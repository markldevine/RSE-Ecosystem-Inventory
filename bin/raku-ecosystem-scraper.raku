#!/var/lib/data/raku/maxzef/bin/raku

#!/usr/bin/env raku

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

#| Global Redis wrapper for auto-reconnection
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

    # 1. Fetch Fresh 'Universe' from Zef Repos
    #    Since we cleaned the DB, we treat everything found as "New".
    say "Fetching fresh module list from zef (fez, cpan, rea)...";
    my %live-candidates = fetch-latest-candidates();
    say "Found { %live-candidates.elems } unique modules in ecosystem.";

    # 2. Process Queue (The Heavy Lifting)
    #    We process ALL modules because we are starting from scratch.
    say "Beginning Deep Dependency Scan (Runtime + Build + Test)...";
    
    my @queue = %live-candidates.values.sort(*<name>);
    my %final-dataset; 

    process-queue(@queue, %final-dataset, $r-handle);

    say "-" x 40;
    say "Scan Complete. Total Modules: { %final-dataset.elems }";

    # 3. Topological Sort
    say "Starting Topological Sort (Build Order Calculation)...";
    my @sorted-list = topological-sort(%final-dataset);
    
    say "Top 5 Install Candidates (No Deps): { @sorted-list[0..4].join(', ') }";

    # 4. Publish Final List
    say "Publishing build order to Valkey list: $KEY-BUILD-ORDER";
    update-build-order($r-handle, @sorted-list);
    
    say "Done.";
    try { $r-handle.client.quit; }
}

# ==========================================
# PROCESSING LOGIC
# ==========================================

sub process-queue(@queue, %current-set, $r-handle) {
    my $total = @queue.elems;
    my $count = 0;

    for @queue -> $candidate {
        $count++;
        my $name = $candidate<name>;
        
        # Identity needed for 'zef info' to be precise
        # e.g. "JSON::Fast:ver<0.19>:auth<zef:timo>"
        my $identity = "$name";
        if $candidate<ver>  { $identity ~= ":ver<$candidate<ver>>" }
        if $candidate<auth> { $identity ~= ":auth<$candidate<auth>>" }

        # Progress logging
        say "[$count/$total] Scanning: $name" if $count %% 10; # Reduce log noise slightly

        # DEEP SCAN: Get Depends, Build-depends, Test-depends
        my @deps = get-deep-dependencies($identity);
        
        my $record = {
            ver  => $candidate<ver>.Str,
            auth => $candidate<auth>,
            deps => @deps
        };

        # 1. Update Memory
        %current-set{$name} = $record;

        # 2. Persist to Valkey (Robustly)
        save-module-robust($r-handle, $name, $record);
    }
}

sub save-module-robust($r-handle, $name, %data) {
    my $max-retries = 3;
    my $attempt = 0;
    
    while $attempt < $max-retries {
        try {
            $r-handle.ensure-connection();
            
            # Atomic Pipeline preferred, but separate commands are fine for this
            $r-handle.client.sadd($KEY-INDEX, $name);
            $r-handle.client.set($KEY-MOD-PREFIX ~ $name, to-json(%data));
            
            return; # Success
        }
        CATCH {
            default {
                $attempt++;
                # Only log if it's a real retry, not just a blip
                if $attempt > 1 {
                    note "    ! Connection issue saving $name. Retry $attempt...";
                }
                try { $r-handle.client.quit; }
                $r-handle.connect(); 
                sleep 1;
            }
        }
    }
    note "CRITICAL: Failed to save $name after $max-retries attempts.";
}

# ==========================================
# ZEF INTERACTION
# ==========================================

sub fetch-latest-candidates() {
    my @repos = <fez cpan rea>;
    my %modules; 

    # We run 'zef list' to populate the local cache and get available versions
    for @repos -> $repo {
        say "  - Querying $repo...";
        my $proc = run 'zef', 'list', $repo, :out, :err;
        
        for $proc.out.lines -> $line {
            # Parse: Name:ver<...>:auth<...>
            if $line ~~ /^ (\S+) \:ver\< (.*?) \> [ \:auth\< (.*?) \> ]? / {
                my $name = ~$0;
                my $ver-str = ~$1;
                my $auth = $2 // '';
                my $ver = Version.new($ver-str);
                
                # Keep highest version
                if %modules{$name}:!exists || $ver > %modules{$name}<ver> {
                    %modules{$name} = { name => $name, ver => $ver, auth => ~$auth };
                }
            }
        }
    }
    return %modules;
}

sub get-deep-dependencies($identity) {
    # We use 'zef info' to get the raw meta-data requirements.
    # This includes Build and Test requirements which are critical for
    # a clean "from scratch" build order.
    
    my $proc = run 'zef', 'info', $identity, :out, :err;
    my @deps;

    for $proc.out.lines -> $line {
        # Match lines like "Depends: Module::A, Module::B"
        # Also match "Build-depends: ..." and "Test-depends: ..."
        if $line ~~ /^(Depends || 'Build-depends' || 'Test-depends') \: \s+ (.*)/ {
            my $list-str = ~$0; 
            
            # Split comma-separated list
            for $list-str.split(',') -> $raw-dep {
                my $clean = $raw-dep.trim;
                
                # Cleanup: "Module::Name:ver<...>" -> "Module::Name"
                # We strip version/auth because our graph is node-to-node based on Name.
                if $clean ~~ /^ (\S+) \s+ / { $clean = ~$0 } # Remove trailing comments like (optional)
                if $clean ~~ /^ ([^:]+) /   { $clean = ~$0 } # Remove :ver/... suffix
                
                # Filter out empty or self
                next if $clean eq '';
                @deps.push($clean);
            }
        }
    }
    return @deps.unique;
}

# ==========================================
# TOPOLOGICAL SORT (Kahn's Algorithm)
# ==========================================

sub topological-sort(%data) {
    my %graph;      
    my %in-degree;  
    
    # 1. Initialize In-Degree 0 for all known modules
    for %data.keys -> $k { %in-degree{$k} = 0; }

    # 2. Build Graph from dependencies
    for %data.kv -> $dependent, $info {
        my @deps = $info<deps>.list;
        
        for @deps -> $dep-name {
            # Only add edges if the dependency is actually in our scrapped list.
            # (Ignores core modules like 'Test' or 'nqp' if they aren't in the list)
            if %data{$dep-name}:exists {
                %graph{$dep-name}.push($dependent);
                %in-degree{$dependent}++;
            }
        }
    }

    # 3. Queue of 0-dependency items (The "Roots")
    #    Sorting alphabetically ensures deterministic output for the base layer.
    my @queue = %in-degree.keys.grep({ %in-degree{$_} == 0 }).sort;
    my @result;

    # 4. Process
    while @queue {
        my $node = @queue.shift;
        @result.push($node);

        if %graph{$node}:exists {
            for %graph{$node}.list -> $neighbor {
                %in-degree{$neighbor}--;
                if %in-degree{$neighbor} == 0 {
                    @queue.push($neighbor);
                }
            }
            # Re-sort queue to maintain deterministic order at every tier
            @queue = @queue.sort; 
        }
    }

    # 5. Cycle Detection
    if @result.elems < %data.elems {
        my %seen = @result.map({ $_ => 1 });
        my @cyclic = %data.keys.grep({ !%seen{$_} }).sort;
        note "WARNING: Cyclic dependencies detected in { @cyclic.elems } modules.";
        note "  (Appending cycles to the end of the build list)";
        @result.append(@cyclic);
    }

    return @result;
}

sub update-build-order($r-handle, @list) {
    $r-handle.ensure-connection();
    $r-handle.client.del($KEY-BUILD-ORDER);
    
    # Push in batches to be network-friendly
    for @list.batch(100) -> @chunk {
        $r-handle.client.rpush($KEY-BUILD-ORDER, @chunk);
    }
}
