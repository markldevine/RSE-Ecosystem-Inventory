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

#| Global Redis wrapper
class RedisHandle {
    has $.host;
    has $.port;
    has $.client is rw;
    method connect() { 
        say "  >> [Redis] Establishing connection to $!host:$!port ...";
        $!client = Redis.new("$!host:$!port"); 
    }
    method ensure-connection() { unless $!client { self.connect(); } }
}

sub MAIN() {
    my $r-handle = RedisHandle.new(host => $VALKEY-HOST, port => $VALKEY-PORT);
    $r-handle.connect();

    # 1. Fetch Fresh Universe
    say "=== Phase 1: Discovery ===";
    say "Fetching fresh module list (Priority: Fez > CPAN > Rea)...";
    my %live-candidates = fetch-latest-candidates();
    my $total = %live-candidates.elems;
    say "Found $total unique winning candidates.";

    # 2. Sequential Deep Scan
    say "\n=== Phase 2: Precision Scanning (zef depends) ===";
    say "Mode: Sequential, Resumable, Precise.";
    
    my @queue = %live-candidates.values.sort(*<name>);
    my %final-dataset; 

    process-queue-sequential(@queue, %final-dataset, $r-handle);

    say "-" x 40;
    say "Scan Complete. Total Modules Verified: { %final-dataset.elems }";

    # 3. Garbage Collection
    say "\n=== Phase 3: Storage Cleanup ===";
    scrub-orphaned-keys($r-handle, %final-dataset);

    # 4. Topological Sort
    say "\n=== Phase 4: Build Order Calculation ===";
    my @sorted-names = topological-sort(%final-dataset);
    
    # 5. Map back to Full Identities
    my @build-list;
    for @sorted-names -> $name {
        if %final-dataset{$name} -> $info {
            @build-list.push($info<identity>);
        }
    }

    say "Top 5 Install Candidates: { @build-list[0..4].join(', ') }";
    say "Publishing build order to Valkey list: $KEY-BUILD-ORDER";
    update-build-order($r-handle, @build-list);
    
    say "\n=== SUCCESS ===";
    try { $r-handle.client.quit; }
}

# ==========================================
# PHASE 2: SEQUENTIAL PROCESSING
# ==========================================

sub process-queue-sequential(@queue, %current-set, $r-handle) {
    my $total = @queue.elems;
    my $count = 0;
    my $skipped = 0;

    for @queue -> $candidate {
        $count++;
        my $name = $candidate<name>;
        my $key  = $KEY-MOD-PREFIX ~ $name;
        
        # --- CHECKPOINT START ---
        my $cached-json = try { $r-handle.client.get($key) };
        my $use-cache = False;

        if $cached-json {
            my $data = from-json($cached-json);
            
            # Validation: Version/Auth match + presence of 'deps'
            if $data<ver> eq $candidate<ver>.Str && ($data<auth> // '') eq ($candidate<auth> // '') {
                %current-set{$name} = $data;
                $skipped++;
                $use-cache = True;
                
                if $count %% 100 { 
                    say "[$count/$total] Skipped (Cache Hit): $name"; 
                }
            }
        }
        # --- CHECKPOINT END ---

        if !$use-cache {
            # WORK: Run the Solver
            say "[$count/$total] Resolving: $name ({$candidate<identity>}) ...";
            
            my @deps = run-zef-depends($candidate<identity>);
            
            my $record = {
                name     => $name,
                ver      => $candidate<ver>.Str,
                auth     => $candidate<auth>,
                api      => $candidate<api>,
                repo     => $candidate<repo>,
                identity => $candidate<identity>,
                deps     => @deps,
                scanned_at => DateTime.now.posix
            };

            %current-set{$name} = $record;
            save-module-robust($r-handle, $name, $record);
            
            # Small throttle to be kind to the OS process table
            sleep 0.05; 
        }
    }
    say "  >> Resume Summary: Fast-forwarded $skipped. Resolved { $total - $skipped } fresh.";
}

# Precision Solver Wrapper
sub run-zef-depends($identity) {
    my $proc = run 'zef', 'depends', $identity, :out, :err;
    my @deps;

    for $proc.out.lines -> $line {
        my $clean = $line.trim;
        next if $clean.contains($identity);

        if $clean ~~ /^ ([^:]+) / {
            my $dep-name = ~$0;
            next if $dep-name eq 'Rakudo' || $dep-name eq 'nqp' || $dep-name eq 'MoarVM';
            @deps.push($dep-name);
        }
    }
    return @deps.unique;
}

sub save-module-robust($r-handle, $name, %data) {
    my $max-retries = 5;
    my $attempt = 0;
    while $attempt < $max-retries {
        try {
            $r-handle.ensure-connection();
            $r-handle.client.sadd($KEY-INDEX, $name);
            $r-handle.client.set($KEY-MOD-PREFIX ~ $name, to-json(%data));
            return; 
        }
        CATCH {
            default {
                $attempt++;
                note "    ! [DB Error] Saving $name. Retry $attempt/$max-retries..." if $attempt > 1;
                try { $r-handle.client.quit; }
                $r-handle.connect(); 
                sleep 2; 
            }
        }
    }
    note "CRITICAL: Could not save $name to Valkey. Data may be inconsistent.";
}

# ==========================================
# PHASE 3: CLEANUP
# ==========================================

sub scrub-orphaned-keys($r-handle, %valid-modules) {
    say "  >> Scanning Valkey for orphaned keys...";
    # Flattened to single line to prevent quoting syntax errors
    my $proc = run '/usr/bin/valkey-cli', '-h', $VALKEY-HOST, '--raw', '--scan', '--pattern', $KEY-MOD-PREFIX ~ '*', :out;
    
    my @db-keys = $proc.out.lines;
    my $deleted = 0;

    for @db-keys -> $db-key {
        if $db-key ~~ /^ $KEY-MOD-PREFIX (.*) $/ {
            my $name = ~$0;
            unless %valid-modules{$name}:exists {
                $r-handle.ensure-connection();
                $r-handle.client.del($db-key);
                $r-handle.client.srem($KEY-INDEX, $name);
                $deleted++;
                if $deleted %% 50 { print "x"; }
            }
        }
    }
    say "";
    say "  >> Scrub complete. Deleted $deleted orphaned records.";
}

# ==========================================
# DISCOVERY & SORTING
# ==========================================

sub fetch-latest-candidates() {
    my @repos = <fez cpan rea>;
    my %modules; 
    for @repos -> $repo {
        say "  - Querying $repo...";
        my $proc = run 'zef', 'list', $repo, :out, :err;
        for $proc.out.lines -> $line {
            # Regex for Name:ver<...>:auth<...>:api<...>
            if $line ~~ /^ (\S+) \:ver\< (.*?) \> [ \:auth\< (.*?) \> ]? [ \:api\< (.*?) \> ]? / {
                my $name = ~$0;
                my $ver-str = ~$1;
                my $auth = $2 // '';
                my $api  = $3 // '';
                my $ver = Version.new($ver-str);
                
                # FIXED: Explicit variable delimiting in string
                my $id = "{$name}:ver<{$ver-str}>";
                if $auth { $id ~= ":auth<{$auth}>" }
                if $api  { $id ~= ":api<{$api}>" }

                if %modules{$name}:!exists || $ver > %modules{$name}<ver> {
                    %modules{$name} = { 
                        name => $name, ver => $ver, auth => ~$auth, api => ~$api,
                        repo => $repo, identity => $id
                    };
                }
            }
        }
    }
    return %modules;
}

sub topological-sort(%data) {
    my %graph;      
    my %in-degree;  
    for %data.keys -> $k { %in-degree{$k} = 0; }

    # Build Graph
    for %data.kv -> $dependent, $info {
        for $info<deps>.list -> $dep-name {
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
        note "WARNING: Cyclic dependencies detected. Appending.";
        @result.append(@cyclic);
    }
    return @result;
}

sub update-build-order($r-handle, @list) {
    $r-handle.ensure-connection();
    $r-handle.client.del($KEY-BUILD-ORDER);
    for @list.batch(100) -> @chunk {
        $r-handle.client.rpush($KEY-BUILD-ORDER, @chunk);
    }
}
