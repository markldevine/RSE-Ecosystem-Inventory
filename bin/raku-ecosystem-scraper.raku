#!/var/lib/data/raku/maxzef/bin/raku

use v6.d;
use JSON::Fast;
use Redis;

#| Infrastructure Configuration
constant $VALKEY-HOST = '172.19.2.254'; 
constant $VALKEY-PORT = 6379;

#| Naming Conventions
constant $KEY-INDEX       = 'RSE^Raku^zef^index';          
constant $KEY-MOD-PREFIX  = 'RSE^Raku^zef^modules^';       
constant $KEY-BUILD-ORDER = 'RSE^Raku^zef^build-order';    

#| Global Redis container to allow reconnection swapping
class RedisHandle {
    has $.host;
    has $.port;
    has $.client is rw;

    method connect() {
        say "  >> Establishing Redis connection...";
        $!client = Redis.new("$!host:$!port");
    }

    method ensure-connection() {
        unless $!client { self.connect(); }
    }
}

sub MAIN() {
    my $r-handle = RedisHandle.new(host => $VALKEY-HOST, port => $VALKEY-PORT);
    $r-handle.connect();

    # 1. Load Artifacts (Hybrid Approach)
    say "Loading existing state from Valkey Infrastructure...";
    my %artifact-db = load-valkey-state($r-handle);
    say "Loaded { %artifact-db.elems } cached modules from Valkey.";

    # 2. Fetch Fresh 'Universe'
    say "Fetching fresh module list from zef repositories...";
    my %live-candidates = fetch-latest-candidates();
    say "Found { %live-candidates.elems } unique modules in current ecosystem.";

    # 3. Diff & Plan Work
    my @work-queue;
    my %final-dataset; 

    say "Comparing fresh candidates against Valkey state...";
    
    for %live-candidates.kv -> $name, $info {
        my $fresh-ver = $info<ver>;
        
        # Guard against corrupt cache entries (must be Hash)
        if %artifact-db{$name}:exists && %artifact-db{$name} ~~ Hash {
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
            say "  [NEW]    $name";
            @work-queue.push($info);
        }
    }

    say "-" x 40;
    say "Plan: { @work-queue.elems } to process, { %final-dataset.elems } cached.";
    say "-" x 40;

    # 4. Execute Work Queue
    if @work-queue {
        process-queue(@work-queue, %final-dataset, $r-handle);
    } else {
        say "Valkey is up to date.";
    }

    # 5. Topological Sort
    say "Starting Topological Sort on { %final-dataset.elems } modules...";
    my @sorted-list = topological-sort(%final-dataset);
    
    # 6. Publish Final List
    say "Publishing build order to Valkey list: $KEY-BUILD-ORDER";
    update-build-order($r-handle, @sorted-list);
    
    say "Done.";
    $r-handle.client.quit;
}

# ==========================================
# ROBUST DATA LOADING (Hybrid)
# ==========================================

sub load-valkey-state($r-handle) {
    my %db;
    say "  - Loading index via /usr/bin/valkey-cli (Streamed Raw)...";

    # 1. Fetch keys via CLI (Safe for huge lists)
    my $proc-keys = run '/usr/bin/valkey-cli', 
                        '-h', $VALKEY-HOST, 
                        '-p', $VALKEY-PORT.Str, 
                        '--raw', 
                        'SMEMBERS', $KEY-INDEX, 
                        :out, :err;

    my @all-keys = $proc-keys.out.lines.grep(*.chars > 0);
    return %() unless @all-keys;
    
    say "    Found { @all-keys.elems } keys. Fetching data via Native Client...";

    # 2. Fetch Data via Native Client (Safe for JSON decoding)
    #    We use the native client here because we can trust it to handle 
    #    protocol decoding (utf8/blobs) correctly for batch sizes of 50.
    
    for @all-keys.batch(50) -> @batch-keys {
        my @full-keys = @batch-keys.map: { $KEY-MOD-PREFIX ~ $_ };
        
        try {
            # Use the Native Client for MGET
            # Decode Buf -> Str immediately
            my @blobs = $r-handle.client.mget(@full-keys).map({ $_ ~~ Buf ?? $_.decode('utf-8') !! $_ });

            for @batch-keys Z @blobs -> ($name, $json) {
                # Robust parsing check
                if $json.defined && $json ~~ Str && $json.contains('{') {
                    try {
                        my $data = from-json($json);
                        if $data ~~ Hash { %db{$name} = $data; }
                    }
                }
            }
            CATCH {
                default { 
                    # If MGET fails (socket die), force reconnect and retry batch
                    note "    ! Batch fetch failed. Reconnecting...";
                    $r-handle.connect();
                }
            }
        }
        print ".";
    }
    say ""; 
    return %db;
}

# ==========================================
# PROCESSING & SAVING (Auto-Healing)
# ==========================================

sub process-queue(@queue, %current-set, $r-handle) {
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

        # 1. Update Memory
        %current-set{$name} = $record;

        # 2. Persist with Retry Logic
        save-module-robust($r-handle, $name, $record);
    }
}

sub save-module-robust($r-handle, $name, %data) {
    my $max-retries = 3;
    my $attempt = 0;
    
    while $attempt < $max-retries {
        try {
            # Ensure we have a client object
            $r-handle.ensure-connection();
            
            # Perform Writes
            $r-handle.client.sadd($KEY-INDEX, $name);
            $r-handle.client.set($KEY-MOD-PREFIX ~ $name, to-json(%data));
            
            # Success - break loop
            return;
        }
        CATCH {
            default {
                $attempt++;
                note "    ! Connection lost on save ($name). Retry $attempt/$max-retries...";
                try { $r-handle.client.quit; }
                $r-handle.connect(); # Force fresh socket
                sleep 1;
            }
        }
    }
    note "CRITICAL: Failed to save $name after $max-retries attempts.";
}

# ==========================================
# RETRIEVAL HELPERS
# ==========================================

sub fetch-latest-candidates() {
    my @repos = <fez cpan rea>;
    my %modules; 
    for @repos -> $repo {
        say "  - Querying $repo...";
        my $proc = run 'zef', 'list', $repo, :out, :err;
        for $proc.out.lines -> $line {
            if $line ~~ /^ (\S+) \:ver\< (.*?) \> [ \:auth\< (.*?) \> ]? / {
                my $name = ~$0;
                my $ver = Version.new(~$1);
                my $auth = $2 // '';
                
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

sub update-build-order($r-handle, @list) {
    $r-handle.ensure-connection();
    $r-handle.client.del($KEY-BUILD-ORDER);
    for @list.batch(100) -> @chunk {
        $r-handle.client.rpush($KEY-BUILD-ORDER, @chunk);
    }
}

# ==========================================
# TOPOLOGICAL SORT
# ==========================================

sub topological-sort(%data) {
    my %graph; my %in-degree;
    for %data.keys -> $k { %in-degree{$k} = 0; }
    
    for %data.kv -> $dependent, $info {
        my @deps = $info<deps>.list;
        for @deps -> $dep-name {
            if %data{$dep-name}:exists {
                %graph{$dep-name}.push($dependent);
                %in-degree{$dependent}++;
            }
        }
    }
    
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
    
    if @result.elems < %data.elems {
        my %seen = @result.map({ $_ => 1 });
        my @cyclic = %data.keys.grep({ !%seen{$_} }).sort;
        note "WARNING: Cyclic dependencies detected. Appending { @cyclic.elems } items.";
        @result.append(@cyclic);
    }
    return @result;
}
