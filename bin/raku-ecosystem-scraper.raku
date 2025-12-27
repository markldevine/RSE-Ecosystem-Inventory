#!/var/lib/data/raku/maxzef/bin/raku

use v6.d;
use JSON::Fast;
use Redis;

#| Infrastructure Configuration
constant $VALKEY-HOST = '172.19.2.254'; # valkey-vip.rse.local
constant $VALKEY-PORT = 6379;

#| Naming Conventions (Environmental Infrastructure)
constant $KEY-INDEX       = 'RSE^Raku^zef^index';          
constant $KEY-MOD-PREFIX  = 'RSE^Raku^zef^modules^';       
constant $KEY-BUILD-ORDER = 'RSE^Raku^zef^build-order';    

sub MAIN() {
    say "Connecting to Valkey ($VALKEY-HOST:$VALKEY-PORT)...";
    my $redis = Redis.new("$VALKEY-HOST:$VALKEY-PORT");

    # 1. Load Artifacts (Fetch State from Valkey)
    say "Loading existing state from Valkey Infrastructure...";
    my %artifact-db = load-valkey-state($redis);
    say "Loaded { %artifact-db.elems } cached modules from Valkey.";

    # 2. Fetch Fresh 'Universe' from Zef
    say "Fetching fresh module list from zef repositories...";
    my %live-candidates = fetch-latest-candidates();
    say "Found { %live-candidates.elems } unique modules in current ecosystem.";

    # 3. Diff & Plan Work
    my @work-queue;
    my %final-dataset; 

    say "Comparing fresh candidates against Valkey state...";
    
    for %live-candidates.kv -> $name, $info {
        my $fresh-ver = $info<ver>;
        
        # GUARD: Check existence AND type (Must be Hash)
        if %artifact-db{$name}:exists && %artifact-db{$name} ~~ Hash {
            
            # Safe to access keys now
            my $cached-ver = Version.new(%artifact-db{$name}<ver>);

            if $fresh-ver > $cached-ver {
                say "  [UPDATE] $name: $cached-ver -> $fresh-ver";
                @work-queue.push($info);
            }
            else {
                # Cache Hit
                %final-dataset{$name} = %artifact-db{$name};
            }
        }
        else {
            # New or Corrupt Cache -> Treat as New
            say "  [NEW]    $name";
            @work-queue.push($info);
        }
    }

    say "-" x 40;
    say "Plan: { @work-queue.elems } to process, { %final-dataset.elems } cached.";
    say "-" x 40;

    # 4. Execute Work Queue
    if @work-queue {
        process-queue(@work-queue, %final-dataset, $redis);
    } else {
        say "Valkey is up to date.";
    }

    # 5. Topological Sort
    say "Starting Topological Sort on { %final-dataset.elems } modules...";
    my @sorted-list = topological-sort(%final-dataset);
    
    # 6. Publish Final List to Valkey
    say "Publishing build order to Valkey list: $KEY-BUILD-ORDER";
    update-build-order($redis, @sorted-list);
    
    say "Done.";
    $redis.quit;
}

# ==========================================
# VALKEY INTERACTION
# ==========================================

sub load-valkey-state($redis) {
    my %db;
    say "  - Loading index via /usr/bin/valkey-cli (Quoted Mode)...";

    # 1. Fetch keys (Raw is fine for keys as they are simple strings)
    my $proc-keys = run '/usr/bin/valkey-cli', 
                        '-h', $VALKEY-HOST, 
                        '-p', $VALKEY-PORT.Str, 
                        '--raw', 
                        'SMEMBERS', $KEY-INDEX, 
                        :out, :err;

    my @all-keys = $proc-keys.out.lines.grep(*.chars > 0);
    return %() unless @all-keys;
    
    say "    Found { @all-keys.elems } keys. Fetching data...";

    # 2. Batch Fetch
    for @all-keys.batch(50) -> @batch-keys {
        my @full-keys = @batch-keys.map: { $KEY-MOD-PREFIX ~ $_ };
        
        # We REMOVE '--raw' here. 
        # Standard mode returns: 1) "{\"ver\":\"1.0\"...}"
        # This escapes newlines/quotes, guaranteeing 1 line per item.
        my $proc-data = run '/usr/bin/valkey-cli',
                            '-h', $VALKEY-HOST,
                            '-p', $VALKEY-PORT.Str,
                            'MGET', |@full-keys,
                            :out, :err;

        my @lines = $proc-data.out.lines;
        
        # Filter lines to keep only the values (lines starting with row numbers)
        # Output format:
        # 1) "value"
        # 2) "value"
        # (nil)
        my @values;
        for @lines -> $line {
            # Match standard Redis CLI output: 1) "..."
            if $line ~~ /^ \d+ \) \s+ \" (.*) \" $/ {
                my $content = ~$0;
                # Unescape CLI escaping: \" -> ", \\ -> \
                $content = $content.subst('\"', '"', :g).subst('\\\\', '\\', :g);
                @values.push($content);
            }
            elsif $line.contains('(nil)') {
                @values.push(Nil);
            }
            # Ignore unrelated lines
        }

        # 3. Synchronize Keys with Parsed Values
        # If counts don't match, we skip the batch to avoid corruption
        if @values.elems == @batch-keys.elems {
            for @batch-keys Z @values -> ($name, $json-str) {
                next unless $json-str.defined;
                
                try {
                    my $data = from-json($json-str);
                    # CRITICAL: Ensure we actually got a Hash back
                    if $data ~~ Hash {
                        %db{$name} = $data;
                    }
                }
            }
        }
        print ".";
    }
    say ""; 
    return %db;
}

sub save-module-to-valkey($redis, $name, %data) {
    # 1. Add name to Index Set (Idempotent)
    $redis.sadd($KEY-INDEX, $name);
    
    # 2. Store Metadata as JSON
    my $key = $KEY-MOD-PREFIX ~ $name;
    $redis.set($key, to-json(%data));
}

sub update-build-order($redis, @list) {
    # Atomic replacement of the build list
    $redis.del($KEY-BUILD-ORDER);
    
    # Batch push to avoid packet limits
    for @list.batch(100) -> @chunk {
        $redis.rpush($KEY-BUILD-ORDER, @chunk);
    }
}

# ==========================================
# RETRIEVAL LOGIC
# ==========================================

sub process-queue(@queue, %current-set, $redis) {
    my $total = @queue.elems;
    my $count = 0;

    for @queue -> $candidate {
        $count++;
        my $name = $candidate<name>;
        
        say "[$count/$total] Resolving dependencies for: $name";

        my @deps = get-dependencies($name);
        
        my $record = {
            ver  => $candidate<ver>.Str,
            auth => $candidate<auth>,
            deps => @deps
        };

        # 1. Update In-Memory Set for final sorting
        %current-set{$name} = $record;

        # 2. Persist immediately to Valkey
        save-module-to-valkey($redis, $name, $record);
    }
}

sub fetch-latest-candidates() {
    my @repos = <fez cpan rea>;
    my %modules; 
    for @repos -> $repo {
        say "  - Querying $repo...";
        my $proc = run 'zef', 'list', $repo, :out, :err;
        for $proc.out.lines -> $line {
            if $line ~~ /^ (\S+) \:ver\< (.*?) \> [ \:auth\< (.*?) \> ]? / {
                my $name = ~$0;
                my $ver-str = ~$1;
                my $auth = $2 // '';
                my $ver = Version.new($ver-str);
                
                if %modules{$name}:!exists || $ver > %modules{$name}<ver> {
                    %modules{$name} = { name => $name, ver => $ver, auth => ~$auth };
                }
            }
        }
    }
    return %modules;
}

sub get-dependencies($name) {
    my $proc = run 'zef', 'depends', $name, :out, :err;
    my @deps;
    for $proc.out.lines -> $line {
        if $line ~~ / (\S+) \:ver/ { @deps.push(~$0.split(':').head); }
        elsif $line ~~ /^ \s* (\w+ <[ \w \: ]>*) \s* $/ {
             next if $line.contains("Result:");
             next if $line.contains("===>"); 
             @deps.push(~$0.split(':').head);
        }
    }
    return @deps.unique;
}

# ==========================================
# SORTING LOGIC
# ==========================================

sub topological-sort(%data) {
    my %graph; my %in-degree;
    for %data.keys -> $k { %in-degree{$k} = 0; }
    
    # Build Graph
    for %data.kv -> $dependent, $info {
        my @deps = $info<deps>.list;
        for @deps -> $dep-name {
            if %data{$dep-name}:exists {
                %graph{$dep-name}.push($dependent);
                %in-degree{$dependent}++;
            }
        }
    }
    
    # Process Queue (Items with 0 dependencies)
    my @queue = %in-degree.keys.grep({ %in-degree{$_} == 0 }).sort;
    my @result;
    
    while @queue {
        my $node = @queue.shift;
        @result.push($node);
        if %graph{$node}:exists {
            for %graph{$node}.list -> $neighbor {
                %in-degree{$neighbor}--;
                if %in-degree{$neighbor} == 0 { @queue.push($neighbor); }
            }
            @queue = @queue.sort;
        }
    }
    
    # Cycle Detection
    if @result.elems < %data.elems {
        my %seen = @result.map({ $_ => 1 });
        my @cyclic = %data.keys.grep({ !%seen{$_} }).sort;
        note "WARNING: Cyclic dependencies detected. Appending { @cyclic.elems } items.";
        @result.append(@cyclic);
    }
    return @result;
}
