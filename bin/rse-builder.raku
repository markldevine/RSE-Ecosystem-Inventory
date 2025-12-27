#!/usr/bin/env raku
use v6.d;
use Redis;
use JSON::Fast;

#| Infrastructure Configuration
constant $TARGET-PREFIX     = '/var/lib/data/raku';
constant $STAGING-DIR       = '/var/lib/data/staging/raku-builds';
constant $VALKEY-HOST       = '172.19.2.254';
constant $VALKEY-PORT       = 6379;
constant $KEY-BUILD-ORDER   = 'RSE^Raku^zef^build-order';

#| Rakudo.org API
constant $DL-API = 'https://www.rakudo.org/dl/rakudo/';

#| Logging
constant $LOG-FILE = "$STAGING-DIR/rse-builder.log";

sub MAIN(Bool :$force = False) {
    prepare-directories();
    log("=== RSE Ecosystem Builder Started ===");

    # 1. Check for Updates
    my ($latest-ver, $download-url) = check-upstream-version();
    my $current-ver = get-current-installed-ver();

    log("Current Installed: $current-ver");
    log("Upstream Latest:   $latest-ver");

    if !$force && $latest-ver eq $current-ver {
        log(">>> System is up to date. Exiting.");
        exit 0;
    }

    log(">>> UPDATE TRIGGERED: $current-ver -> $latest-ver");

    # 2. Download and Stage
    my $tarball = download-release($download-url, $latest-ver);

    # 3. Rotate and Install
    install-release($tarball);

    # 4. Bootstrap/Verify Zef
    verify-zef();

    # 5. Hydrate Ecosystem
    hydrate-ecosystem();

    # 6. Harvest Runtime Artifacts (Docker Prep)
    harvest-libs();

    log("=== BUILD CYCLE COMPLETE: $latest-ver ===");
    say "Build complete. Check $LOG-FILE for details.";
}

# ==========================================
# UTILITIES
# ==========================================

sub prepare-directories() {
    mkdir $STAGING-DIR unless $STAGING-DIR.IO.e;
}

sub log($msg) {
    my $ts = DateTime.now.Str;
    my $line = "[$ts] $msg";
    say $line; # Console
    $LOG-FILE.IO.spurt("$line\n", :append); # File
}

# ==========================================
# VERSION CHECKS
# ==========================================

sub check-upstream-version() {
    log("Querying $DL-API ...");
    
    # Fetch JSON using curl (robust)
    my $proc = run 'curl', '-s', $DL-API, :out;
    my $json = from-json($proc.out.slurp);

    # Filter for: platform=linux, arch=x86_64, toolchain=gcc, type=archive, format=tar.gz, latest=1
    my $candidate = $json.first({
        $_<platform>  eq 'linux' &&
        $_<arch>      eq 'x86_64' &&
        $_<toolchain> eq 'gcc' &&
        $_<type>      eq 'archive' &&
        $_<format>    eq 'tar.gz' &&
        $_<latest>    == 1
    });

    unless $candidate {
        log("CRITICAL: Could not find a matching candidate in rakudo.org JSON.");
        exit 1;
    }

    return ($candidate<ver>, $candidate<url>);
}

sub get-current-installed-ver() {
    return "none" unless "$TARGET-PREFIX/bin/raku".IO.e;
    my $proc = run "$TARGET-PREFIX/bin/raku", '-v', :out;
    # Output: "Welcome to Rakudo v2025.12..."
    if $proc.out.lines.head ~~ /v (\d+ \. \d+)/ {
        return ~$0;
    }
    return "unknown";
}

# ==========================================
# DOWNLOAD & INSTALL
# ==========================================

sub download-release($url, $ver) {
    my $filename = $url.IO.basename;
    my $dest = "$STAGING-DIR/$filename";

    if $dest.IO.e {
        log("Artifact $filename already exists. Skipping download.");
    }
    else {
        log("Downloading $url to $dest ...");
        my $proc = run 'curl', '-L', '-o', $dest, $url, :out, :err;
        if $proc.exitcode != 0 {
            log("Download failed!");
            exit 1;
        }
    }
    return $dest;
}

sub install-release($tarball) {
    # 1. Rotate Old Install
    if "$TARGET-PREFIX".IO.e {
        my $date-suffix = DateTime.now.yyyy-mm-dd;
        my $backup = "$TARGET-PREFIX-$date-suffix";
        log("Rotating existing install to $backup");
        
        # Handle case where backup already exists (multiple runs same day)
        if $backup.IO.e {
            my $ts = DateTime.now.posix;
            $backup = "$backup.$ts";
        }
        
        run 'mv', $TARGET-PREFIX, $backup;
    }

    # 2. Unpack
    log("Unpacking $tarball ...");
    # Unpack to staging dir first to see directory structure
    my $extract-path = "$STAGING-DIR/extracted";
    if $extract-path.IO.e {
        run 'rm', '-rf', $extract-path;
    }
    mkdir $extract-path;

    run 'tar', 'xf', $tarball, '-C', $extract-path;

    # 3. Move to Target
    # The tarball usually contains a single top-level folder like 'rakudo-moar-...'
    my @contents = $extract-path.IO.dir;
    if @contents.elems == 1 && @contents[0].d {
        my $source-dir = @contents[0];
        log("Moving $source-dir to $TARGET-PREFIX");
        run 'mv', $source-dir, $TARGET-PREFIX;
    }
    else {
        log("Unexpected tarball structure. Contents: { @contents.map(*.basename).join(', ') }");
        exit 1;
    }

    # 4. Clean up extraction
    run 'rm', '-rf', $extract-path;
}

sub verify-zef() {
    # Official binaries usually ship with 'zef' in bin/
    if "$TARGET-PREFIX/bin/zef".IO.e {
        log("Zef detected in distribution.");
    }
    else {
        log("Zef NOT found. Bootstrapping...");
        chdir "$STAGING-DIR";
        unless "zef".IO.e {
            run 'git', 'clone', 'https://github.com/ugexe/zef.git';
        }
        chdir "zef";
        run "$TARGET-PREFIX/bin/raku", "-I.", "bin/zef", "install", ".";
        log("Zef bootstrapped.");
    }
}

# ==========================================
# ECOSYSTEM HYDRATION
# ==========================================

sub hydrate-ecosystem() {
    log("Connecting to Valkey ($VALKEY-HOST) for build order...");
    my $redis = Redis.new("$VALKEY-HOST:$VALKEY-PORT");
    
    my @modules-buf = $redis.lrange($KEY-BUILD-ORDER, 0, -1);
    $redis.quit;

    my $total = @modules-buf.elems;
    log("Found $total modules to install.");

    my $i = 0;
    for @modules-buf -> $buf {
        my $mod-name = $buf.decode('utf-8');
        $i++;
        
        log("[$i/$total] Installing $mod-name ...");
        
        # Install command
        # We capture both stdout and stderr to the log file
        my $proc = run "$TARGET-PREFIX/bin/zef", 'install', 
                       '--force-test', # Skip test failures
                       '--/test',      # OPTIONAL: aggressively disable testing if speed is priority
                       $mod-name, 
                       :out, :err;
        
        if $proc.exitcode != 0 {
            log("!!! FAILURE: $mod-name");
            log("    STDERR: " ~ $proc.err.slurp.trim);
        } else {
            # Log successful installs cleanly? 
            # Maybe just keep it minimal to save log space
        }
    }
}

# ==========================================
# HARVEST LIBRARIES
# ==========================================

sub harvest-libs() {
    log("Harvesting runtime libraries for Docker...");
    my $dest = "$TARGET-PREFIX/lib64-runtime";
    mkdir $dest unless $dest.IO.e;
    
    my @libs = <
        libc.so.6 libm.so.6 libpthread.so.0 librt.so.1 libdl.so.2 
        ld-linux-x86-64.so.2 
        libresolv.so.2 libnss_dns.so.2 libnss_files.so.2
        libgcc_s.so.1 libstdc++.so.6
    >;

    for @libs -> $lib {
        my $src = "/lib64/$lib";
        if $src.IO.e {
            copy($src, "$dest/$lib");
        } else {
            log("WARNING: Host library $lib not found.");
        }
    }
    
    my $nss-conf = "$dest/nsswitch.conf";
    $nss-conf.IO.spurt: "hosts: files dns\n";
    log("Harvest complete.");
}
