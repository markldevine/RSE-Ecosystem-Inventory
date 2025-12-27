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

#| Global Redis wrapper
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

    # 1. Fetch Fresh Universe
    say "Fetching fresh module list (Priority: Fez > CPAN > Rea)...";
    my %live-candidates = fetch-latest-candidates();
    say "Found { %live-candidates.elems } unique winning candidates.";

    # 2. Process Queue with RESUME Logic
    say "Beginning Deep Dependency Scan (Checkpoint Enabled)...";
    
    my @queue = %live-candidates.values.sort(*<name>);
    my %final-dataset; 

    # Pass the total count for better logging
    process-queue(@queue, %final-dataset, $r-handle);

    say "-" x 40;
    say "Scan Complete. Total Modules: { %final-dataset.elems }";

    # 3. Topological Sort
    say "Starting Topological Sort...";
    my @sorted-names = topological-sort(%final-dataset);
    
    # 4. Map back to Identities
    my @build-list;
    for @sorted-names -> $name {
        if %final-dataset{$name} -> $info {
            my $id = $name;
            if $info<ver>  { $id ~= ":ver<$info<ver>>" }
            if $info<auth> { $id ~= ":auth<$info<auth>>" }
            @build-list.push($id);
        }
    }

    say "Top 5 Install Candidates: { @build-list[0..4].join(', ') }";

    # 5. Publish
    say "Publishing pinned build order to Valkey list: $KEY-BUILD-ORDER";
    update-build-order($r-handle, @build-list);
    
    say "Done.";
    try { $r-handle.client.quit; }
}

# ==========================================
# PROCESSING LOGIC (RESUME ENABLED)
# ==========================================

sub process-queue(@queue, %current-set, $r-handle) {
    my $total = @queue.elems;
    my $count = 0;
    my $skipped = 0;

    for @queue -> $candidate {
        $count++;
        my $name = $candidate<name>;
        my $key  = $KEY-MOD-PREFIX ~ $name;

        # ---------------------------------------------------------
        # CHECKPOINT STRATEGY:
        # Check if we already did the work for this EXACT version.
        # ---------------------------------------------------------
        my $cached-json = try { $r-handle.client.get($key) };
        
        if $cached-json {
            my $data = from-json($cached-json);
            
            # Verify it matches the version we want
            if $data<ver> eq $candidate<ver>.Str {
                # CACHE HIT: Load into memory and skip 'zef info'
                %current-set{$name} = $data;
                $skipped++;
                
                # Log occasionally to show fast-forward progress
                if $count %% 200 {
                    say "[$count/$total] Skipped (Already in DB): $name";
                }
                next; 
            }
        }

        # CACHE MISS: We must work.
        # Identity needed for 'zef info'
        my $identity = "$name";
        if $candidate<ver>  { $identity ~= ":ver<$candidate<ver>>" }
        if $candidate<auth> { $identity ~= ":auth<$candidate<auth>>" }

        # Log active scanning more frequently so we know it's alive
        say "[$count/$total] Scanning: $name" if $count %% 10;

        # DEEP SCAN
        my @deps = get-deep-dependencies($identity);
        
        my $record = {
            ver  => $candidate<ver>.Str,
            auth => $candidate<auth>,
            repo => $candidate<repo>,
            deps => @deps
        };

        # Update Memory & Persist
        %current-set{$name} = $record;
        save-module-robust($r-handle, $name, $record);
        
        # Explicitly trigger garbage collection occasionally to help the OOM killer stay away
        if $count %% 100 { full-gc; }
    }
    
    say "  >> Fast-forwarded $skipped modules. Scanned { $total - $skipped } new.";
}

sub save-module-robust($r-handle, $name, %data) {
    my $max-retries = 3;
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
                note "    ! Connection issue saving $name. Retry $attempt..." if $attempt > 1;
                try { $r-handle.client.quit; }
                $r-handle.connect(); 
                sleep 1;
            }
        }
    }
    note "CRITICAL: Failed to save $name after $max-retries attempts.";
}

# ==========================================
# ZEF & SORTING (Unchanged)
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
                my $ver-str = ~$1;
                my $auth = $2 // '';
                my $ver = Version.new($ver-str);
                
                if %modules{$name}:!exists || $ver > %modules{$name}<ver> {
                    %modules{$name} = { name => $name, ver => $ver, auth => ~$auth, repo => $repo };
                }
            }
        }
    }
    return %modules;
}

sub get-deep-dependencies($identity) {
    my $proc = run 'zef', 'info', $identity, :out, :err;
    my @deps;

    for $proc.out.lines -> $line {
        if $line ~~ /^(Depends || 'Build-depends' || 'Test-depends') \: \s+ (.*)/ {
            my $list-str = ~$0; 
            for $list-str.split(',') -> $raw-dep {
                my $clean = $raw-dep.trim;
                if $clean ~~ /^ (\S+) \s+ / { $clean = ~$0 } 
                if $clean ~~ /^ ([^:]+) /   { $clean = ~$0 } 
                next if $clean eq '';
                @deps.push($clean);
            }
        }
    }
    return @deps.unique;
}

sub topological-sort(%data) {
    my %graph;      
    my %in-degree;  
    for %data.keys -> $k { %in-degree{$k} = 0; }
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
