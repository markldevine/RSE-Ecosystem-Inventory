#!/var/lib/data/raku/maxzef/bin/raku

use v6.d;
use JSON::Fast;
use Redis;

#| Infrastructure
constant $VALKEY-HOST = '172.19.2.254'; 
constant $VALKEY-PORT = 6379;

#| Schema Keys
constant $KEY-MOD-PREFIX  = 'RSE^Raku^zef^modules^';       
constant $KEY-BUILD-ORDER = 'RSE^Raku^zef^build-order';    

#| Connection Factory
sub get-redis() { Redis.new("$VALKEY-HOST:$VALKEY-PORT") }

sub MAIN() {
    say "=== Raku Ecosystem Scraper (Idiomatic & Scoped) ===";

    # 1. SCOPE: Discovery (Heavy List Processing)
    #    We compute the list, return it, and let the heavy raw data die.
    my @queue = discover-universe();
    say "Found { @queue.elems } unique candidates to process.";

    # 2. SCOPE: Processing (Iterative & Isolated)
    process-universe(@queue);

    # 3. SCOPE: Assembly (Sorting)
    assemble-build-order();
    
    say "=== SUCCESS ===";
}

# ==========================================
# PHASE 1: DISCOVERY
# ==========================================
sub discover-universe() {
    say ">> Fetching module lists from repositories...";
    my %candidates;

    # Iterate repos sequentially to respect priority
    for <fez cpan rea> -> $repo {
        say "   - Querying $repo...";
        my $proc = run 'zef', 'list', $repo, :out, :err;
        
        for $proc.out.lines -> $line {
            # Parse identity: Name:ver<...>:auth<...>:api<...>
            if $line ~~ /^ (\S+) \:ver\< (.*?) \> [ \:auth\< (.*?) \> ]? [ \:api\< (.*?) \> ]? / {
                my ($name, $v-str, $auth, $api) = (~$0, ~$1, $2//'', $3//'');
                
                my $ver = Version.new($v-str);
                # Strict string interpolation for identity construction
                my $id  = "{$name}:ver<{$v-str}>";
                if $auth { $id ~= ":auth<{$auth}>" }
                if $api  { $id ~= ":api<{$api}>" }

                # Logic: Keep highest version. If equal, first repo wins.
                if %candidates{$name}:!exists || $ver > %candidates{$name}<ver> {
                    %candidates{$name} = {
                        name => $name,
                        ver  => $ver,
                        auth => $auth,
                        api  => $api,
                        repo => $repo,
                        identity => $id
                    };
                }
            }
        }
    }
    # Return sorted list, allowing the hash to be GC'd
    return %candidates.values.sort(*<name>).List;
}

# ==========================================
# PHASE 2: PROCESSING (The Safe Zone)
# ==========================================
sub process-universe(@queue) {
    say ">> Beginning Dependency Scan (Sequential)...";
    my $redis = get-redis();
    my $total = @queue.elems;
    my $i = 0;

    for @queue -> $cand {
        $i++;
        
        # BLOCK SCOPE: Isolate all processing logic here.
        # Variables declared inside 'process-single-module' die when it returns.
        process-single-module($cand, $redis, $i, $total);
    }
    $redis.quit;
}

# This sub is the memory firewall. 
# Anything created in here is eligible for GC immediately after return.
sub process-single-module($cand, $redis, $idx, $total) {
    my $name = $cand<name>;
    my $key  = $KEY-MOD-PREFIX ~ $name;

    # 1. Check Cache (Valkey)
    #    We use a `try` block to safely handle connection blips
    my $cached = try { 
        my $b = $redis.get($key); 
        # Decode Buf -> Str -> Hash
        $b ?? from-json($b.decode) !! Nil 
    };

    if $cached {
        # Validate Cache against current Candidate
        if $cached<ver> eq $cand<ver>.Str && ($cached<auth>//'') eq ($cand<auth>//'') {
            # Log only occasionally to reduce I/O noise
            if $idx %% 50 { say "   [$idx/$total] Skipped (Cache Hit): $name"; }
            return; 
        }
    }

    # 2. Perform Work (zef depends)
    say "   [$idx/$total] Resolving: $name";
    
    # Run process, read lines, capture strings -> all strictly local
    my @deps = run-zef-depends($cand<identity>);

    # 3. Serialize & Store
    my %record = 
        name       => $name,
        ver        => $cand<ver>.Str,
        auth       => $cand<auth>,
        api        => $cand<api>,
        repo       => $cand<repo>,
        identity   => $cand<identity>,
        deps       => @deps,
        scanned_at => DateTime.now.posix;

    $redis.set($key, to-json(%record));
}

sub run-zef-depends($id) {
    # Line-by-line reading is memory efficient
    my $proc = run 'zef', 'depends', $id, :out, :err;
    my @d;
    for $proc.out.lines -> $line {
        my $clean = $line.trim;
        next if $clean.contains($id); # Skip self
        if $clean ~~ /^ ([^:]+) / {
            my $dep = ~$0;
            # Filter core/VM noise
            next if $dep (elem) <Rakudo nqp MoarVM>; 
            @d.push($dep);
        }
    }
    return @d.unique;
}

# ==========================================
# PHASE 3: ASSEMBLY
# ==========================================
sub assemble-build-order() {
    say ">> Assembling Final Build Order...";
    my $redis = get-redis();
    
    # 1. Bulk Load (Only happens once at the very end)
    my %graph;
    
    # Using CLI scan to avoid blocking main thread
    my $proc = run '/usr/bin/valkey-cli', '-h', $VALKEY-HOST, '--raw', 
                   '--scan', '--pattern', $KEY-MOD-PREFIX ~ '*', :out;
    
    my @keys = $proc.out.lines;
    say "   Loading { @keys.elems } records...";

    for @keys -> $k {
        if $k ~~ /^ $KEY-MOD-PREFIX (.*) $/ {
            my $n = ~$0;
            if my $blob = $redis.get($k) {
                %graph{$n} = from-json($blob.decode);
            }
        }
    }

    # 2. Sort
    my @sorted = topological-sort(%graph);
    
    # 3. Publish
    #    Map sorted names back to full identities for installation
    my @final-list = @sorted.map: { %graph{$_}<identity> };
    
    say "   Top 5: { @final-list[0..4].join(', ') }";
    
    $redis.del($KEY-BUILD-ORDER);
    # Push in chunks to stay responsive
    for @final-list.batch(100) -> @chunk {
        $redis.rpush($KEY-BUILD-ORDER, @chunk);
    }
    say ">> Build Order Published.";
    $redis.quit;
}

sub topological-sort(%data) {
    my %g; my %deg;
    for %data.keys -> $k { %deg{$k} = 0 }
    
    # Build Edges
    for %data.values -> $info {
        my $u = $info<name>;
        for $info<deps>.list -> $v {
            if %data{$v}:exists {
                %g{$v}.push($u);
                %deg{$u}++;
            }
        }
    }

    # Kahn's Algo
    my @q = %deg.keys.grep({ %deg{$_} == 0 }).sort;
    my @res;
    
    while @q {
        my $u = @q.shift;
        @res.push($u);
        if %g{$u} {
            for %g{$u}.list -> $v {
                %deg{$v}--;
                if %deg{$v} == 0 { @q.push($v); }
            }
            @q = @q.sort;
        }
    }
    
    # Cycle handling
    if @res < %data {
        my %seen = @res.map({ $_ => 1 });
        @res.append: %data.keys.grep({ !%seen{$_} }).sort;
        note "   WARNING: Cycles detected. Appended remainder.";
    }
    return @res;
}
