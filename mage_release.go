//go:build mage

package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"

	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
	"github.com/gofsd/libp2p-kv-raft/pkg/tlscert"
)

// Lint runs golangci-lint (see .golangci.yml) the same way CI does. Needs
// golangci-lint v2 on PATH already -- `go install
// github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2`, pinned
// to the version .golangci.yml's config schema was written against (see
// ci.yml's own doc comment on that pin).
func Lint() error {
	if _, err := exec.LookPath("golangci-lint"); err != nil {
		return fmt.Errorf("golangci-lint not found on PATH -- install with `go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2`: %w", err)
	}
	fmt.Println("Running golangci-lint...")
	cmd := exec.Command("golangci-lint", "run", "./...")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// Test runs only the fast unit tests (ignoring integration and e2e). -race
// is on unconditionally: this is a raft/libp2p codebase with heavy
// goroutine/channel concurrency (transport read loops, raft's own FSM
// apply loop, relay reservation refresh, ...), exactly the kind of code
// where a data race is the failure mode -- rare, environment-dependent
// corruption -- you least want to discover for the first time in
// production. Requires CGO_ENABLED=1 (the race detector's runtime
// support needs cgo); this repo already builds with cgo enabled for
// pkg/ipc's desktop shm transport, so no environment already running
// `mage test` needs a new toolchain to pick this up.
func Test() error {
	fmt.Println("Running Unit Tests...")
	// We pass -short flag so you can optionally skip longer unit tests if needed
	cmd := exec.Command("go", "test", "-v", "-race", "-short", "./...")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// Integration runs both unit tests and integration tests. See Test's doc
// comment for why -race is unconditional here too.
func Integration() error {
	fmt.Println("Running Integration Tests...")
	// -tags=integration tells Go to include files with the '//go:build integration' tag
	cmd := exec.Command("go", "test", "-v", "-race", "-tags=integration", "./...")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// TLS groups self-signed certificate generation behind `mage tls:<method>`
// -- currently just the one target cmd/kvhttp needs, since it now refuses
// to serve /command over plain HTTP at all (see that command's own doc
// comment).
type TLS mg.Namespace

// kvhttpTLSDir returns where GenSelfSigned writes cmd/kvhttp's
// certificate/key pair, and the directory kvhttp itself defaults to
// reading them from (-tls-cert/-tls-key both default to a path under
// here) -- alongside the local node registry (pkg/registry.EnvHome/Open),
// not the repo itself: like every node's own identity.key, this is
// generated, machine-specific material that must never be committed.
func kvhttpTLSDir() (string, error) {
	reg, err := registry.Open()
	if err != nil {
		return "", err
	}
	return filepath.Join(reg.Dir, "kvhttp-tls"), nil
}

// GenSelfSigned generates a fresh self-signed ECDSA certificate/key pair
// for cmd/kvhttp's HTTPS listener (see pkg/tlscert's own doc comment for
// why self-signed/pure-Go), valid for every host/IP in the comma-separated
// hosts argument -- e.g. "localhost,127.0.0.1,203.0.113.4" -- a caller
// connecting to an address missing from this list gets a certificate
// validation error regardless of the cert otherwise being trusted, so
// list every hostname/IP a real caller will actually connect through.
// Overwrites any previously generated pair at the same path. Prints the
// path kvhttp's own -tls-cert/-tls-key flags default to, so `mage
// tls:genselfsigned <hosts>` followed by plain `go run ./cmd/kvhttp` (or
// the deployed binary with no TLS flags at all) picks it up automatically.
// Self-signed means every caller (browser, curl, etc.) must explicitly
// trust this exact certificate first -- there is no CA behind it for a
// client to already trust on its own.
//
// Usage: mage tls:genselfsigned <comma-separated hosts/IPs>
func (TLS) GenSelfSigned(hosts string) error {
	dir, err := kvhttpTLSDir()
	if err != nil {
		return err
	}
	hostList := strings.Split(hosts, ",")
	for i := range hostList {
		hostList[i] = strings.TrimSpace(hostList[i])
	}
	certPath := filepath.Join(dir, "cert.pem")
	keyPath := filepath.Join(dir, "key.pem")
	if err := tlscert.GenerateSelfSigned(certPath, keyPath, hostList, tlscert.DefaultValidFor); err != nil {
		return err
	}
	fmt.Printf("✅ self-signed cert/key generated for [%s]\n   cert: %s\n   key:  %s\n", strings.Join(hostList, ", "), certPath, keyPath)
	return nil
}

// Githooks groups git hook installation behind `mage githooks:<method>`.
type Githooks mg.Namespace

// Install points this repo's core.hooksPath at scripts/git-hooks, so the
// pre-push hook there (which runs `mage e2e:current` for a routine push,
// or the full `mage e2e:release` whenever the push includes a version tag,
// blocking the push if it fails) actually runs -- see that file's own doc
// comment for the SKIP_E2E escape hatch. Idempotent and safe to re-run.
//
// Usage: mage githooks:install
func (Githooks) Install() error {
	root, err := repoRoot()
	if err != nil {
		return err
	}
	hooksDir := filepath.Join(root, "scripts", "git-hooks")
	if err := os.Chmod(filepath.Join(hooksDir, "pre-push"), 0o755); err != nil {
		return fmt.Errorf("githooks: %w", err)
	}
	cmd := exec.Command("git", "config", "core.hooksPath", "scripts/git-hooks")
	cmd.Dir = root
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("githooks: set core.hooksPath: %w", err)
	}
	fmt.Println("✅ core.hooksPath set to scripts/git-hooks -- `mage e2e:current` (or `mage e2e:release` for a version-tag push) now runs before every push")
	return nil
}

// TestAll runs absolutely every test type sequentially
func TestAll() error {
	fmt.Println("Executing complete test matrix...")

	if err := Test(); err != nil {
		return err
	}
	if err := Integration(); err != nil {
		return err
	}
	if err := (E2E{}).All(); err != nil {
		return err
	}

	fmt.Println("🎉 All test suites passed successfully!")
	return nil
}

// TestCurrent runs ONLY the tests that have been created or modified against origin/main
func TestCurrent() error {
	fmt.Println("🔍 Detecting changed or new test files...")

	// 1. Get the list of changed files compared to the main remote branch
	// (You can change "origin/main" to a specific tag like "v1.0.0" if preferred)
	cmd := exec.Command("git", "diff", "origin/main", "--name-only")
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to check git diff: %w. Make sure you have fetched from remote", err)
	}

	// 2. Filter for files that are Go tests and track their packages (directories)
	changedPackages := make(map[string]bool)
	files := strings.Split(out.String(), "\n")

	for _, file := range files {
		file = strings.TrimSpace(file)
		// Only care about Go test files that still exist (weren't deleted)
		if strings.HasSuffix(file, "_test.go") {
			if _, err := os.Stat(file); err == nil {
				// Convert file path to package path (e.g., "calculator/calc_test.go" -> "./calculator")
				dir := filepath.Dir(file)
				pkg := "./" + dir
				changedPackages[pkg] = true
			}
		}
	}

	// 3. If no tests changed, exit early
	if len(changedPackages) == 0 {
		fmt.Println("✅ No new or updated tests found in this version.")
		return nil
	}

	// 4. Build the go test command for the specific packages
	var pkgs []string
	for pkg := range changedPackages {
		pkgs = append(pkgs, pkg)
	}

	fmt.Printf("🚀 Running tests in modified packages: %s\n", strings.Join(pkgs, ", "))

	testArgs := append([]string{"test", "-v"}, pkgs...)
	testCmd := exec.Command("go", testArgs...)
	testCmd.Stdout = os.Stdout
	testCmd.Stderr = os.Stderr

	return testCmd.Run()
}

// Build compiles the project for multiple platforms
func Build() {
	mg.Deps(BuildLinux, BuildWindows, BuildAndroid)
}

// releaseBinaries is every deployable command this repo ships, in the
// desktop/server sense (excludes mobile/kvmobile and web-app, which have
// their own build paths -- BuildAndroid, wasm-pack).
var releaseBinaries = []string{"./cmd/kvnode", "./cmd/kvctl-cli", "./cmd/kvhttp", "./cmd/kvrecover"}

// buildCross compiles releaseBinaries for one GOOS/GOARCH pair into
// dist/<goos>_<goarch>/. Unlike the on-disk store (modernc.org/sqlite, a
// pure-Go, no-cgo driver), pkg/ipc's desktop transport (ipc.go) depends on
// github.com/hidez8891/shm for named shared memory, which is itself cgo --
// so CGO_ENABLED=1 here is required, not optional, and a genuine cross-OS
// build needs the right C cross-compiler set via cc (e.g. Windows from a
// Linux host needs an x86_64-w64-mingw32-gcc on PATH).
func buildCross(goos, goarch, cc string) error {
	root, err := repoRoot()
	if err != nil {
		return err
	}
	outDir := filepath.Join(root, "dist", goos+"_"+goarch)
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}
	ext := ""
	if goos == "windows" {
		ext = ".exe"
	}
	env := append(os.Environ(), "GOOS="+goos, "GOARCH="+goarch, "CGO_ENABLED=1")
	if cc != "" {
		env = append(env, "CC="+cc)
	}
	for _, pkg := range releaseBinaries {
		name := filepath.Base(pkg) + ext
		out := filepath.Join(outDir, name)
		fmt.Printf("Building %s for %s/%s...\n", pkg, goos, goarch)
		cmd := exec.Command("go", "build", "-ldflags=-s -w", "-o", out, pkg)
		cmd.Dir = root
		cmd.Env = env
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("build %s for %s/%s: %w", pkg, goos, goarch, err)
		}
	}
	return nil
}

// BuildLinux compiles releaseBinaries for linux/amd64, natively -- cgo's
// shm dependency (see buildCross's doc comment) means a genuine linux/arm64
// cross build needs an aarch64-linux-gnu-gcc cross-compiler on PATH, which
// isn't assumed to be installed; build natively on an arm64 host instead
// (e.g. an arm64 CI runner) if that target is needed.
func BuildLinux() error {
	return buildCross("linux", "amd64", "")
}

// BuildWindows cross-compiles releaseBinaries for windows/amd64, requiring
// an x86_64-w64-mingw32-gcc cross-compiler on PATH (e.g. `apt-get install
// gcc-mingw-w64-x86-64` on a Debian/Ubuntu CI runner). Compiles clean as of
// thirdparty/libc restoring its windows/* files from upstream (see README's
// "Vendored dependency patch" section) -- but hasn't been run on a real
// Windows machine, only cross-compiled, so treat the resulting .exe as
// unvalidated until someone actually runs it.
func BuildWindows() error {
	return buildCross("windows", "amd64", "x86_64-w64-mingw32-gcc")
}

// BuildAndroid cross-compiles mobile/kvmobile into android-app/app/libs/kvmobile.aar
// via `gomobile bind`, with no identity/leader baked in -- a plain
// "does this still compile for Android" smoke build, the Android
// counterpart to BuildLinux/BuildWindows. `mage e2e:current`/`e2e:all`
// build their own AAR with a real identity and leader multiaddr baked in
// for actual test runs (see pkg/e2erun.buildAndroidAAR); this target
// doesn't produce something a device can usefully run against a real
// cluster.
func BuildAndroid() error {
	fmt.Println("Building Android AAR...")
	root, err := repoRoot()
	if err != nil {
		return err
	}
	aarPath := filepath.Join(root, "android-app", "app", "libs", "kvmobile.aar")
	if err := os.MkdirAll(filepath.Dir(aarPath), 0o755); err != nil {
		return err
	}
	cmd := exec.Command("gomobile", "bind", "-target=android", "-androidapi", "26",
		"-o", aarPath, "./mobile/kvmobile")
	cmd.Dir = root
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("buildandroid: gomobile bind: %w", err)
	}
	fmt.Println("✅ android-app/app/libs/kvmobile.aar built (no identity/leader baked in)")
	return nil
}

// buildAndroidReleaseAAR runs `gomobile bind` baking leaderAddr/relayAddr
// into android-app/app/libs/kvmobile.aar -- the same fixed path
// BuildAndroid/pkg/e2erun.buildAndroidAAR both use, since
// android-app/app/build.gradle.kts's `implementation(files("libs/kvmobile.aar"))`
// only ever reads from there regardless of build type. Unlike
// pkg/e2erun.buildAndroidAAR (which bakes in identitySeedHex so a
// recorded e2e node always comes up as the same deterministic peer id),
// this never sets identitySeedHex: a real Google Play install must
// generate its own fresh identity on first run (see
// mobile/kvmobile.identitySeedHex's doc comment) -- baking in a fixed
// private key would mean every install of the same release shares one
// raft identity. relayAddr defaults to leaderAddr when empty, matching
// mobile/kvmobile.relayMultiaddr's own doc comment ("normally just the
// leader's own multiaddr") and the Node connectivity policy section in
// CLAUDE.md -- a phone can essentially never guarantee it's directly
// dialable, so it needs a relay peer set by default, not just on request.
func buildAndroidReleaseAAR(root, leaderAddr, relayAddr string) error {
	if leaderAddr == "" {
		return fmt.Errorf("leaderAddr is required, e.g. /ip4/<ip>/tcp/4001/p2p/<peerID> -- see configs/bootstrap-nodes.json (mage bootstrapnodes)")
	}
	if relayAddr == "" {
		relayAddr = leaderAddr
	}
	aarPath := filepath.Join(root, "android-app", "app", "libs", "kvmobile.aar")
	if err := os.MkdirAll(filepath.Dir(aarPath), 0o755); err != nil {
		return err
	}
	ldflags := fmt.Sprintf(
		"-X github.com/gofsd/libp2p-kv-raft/mobile/kvmobile.leaderMultiaddr=%s -X github.com/gofsd/libp2p-kv-raft/mobile/kvmobile.relayMultiaddr=%s",
		leaderAddr, relayAddr,
	)
	cmd := exec.Command("gomobile", "bind", "-target=android", "-androidapi", "26",
		"-ldflags", ldflags, "-o", aarPath, "./mobile/kvmobile")
	cmd.Dir = root
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("gomobile bind: %w", err)
	}
	fmt.Printf("✅ android-app/app/libs/kvmobile.aar built (leader=%s relay=%s, fresh identity per install)\n", leaderAddr, relayAddr)
	return nil
}

// requireAndroidKeystore fails fast with a clear error if
// android-app/keystore.properties (gitignored -- see .gitignore's
// "Release signing material" entry and that file's sibling
// android-app/keystore/ dir for the .jks itself, see commit 684f946 for
// how it was provisioned) is missing, rather than letting Gradle's own
// release signingConfig guard (android-app/app/build.gradle.kts) trip
// deep inside a multi-minute gomobile+gradle build.
func requireAndroidKeystore(root string) error {
	if _, err := os.Stat(filepath.Join(root, "android-app", "keystore.properties")); err != nil {
		return fmt.Errorf("android-app/keystore.properties not found -- release signing needs storeFile/storePassword/keyAlias/keyPassword (see android-app/app/build.gradle.kts's doc comment)")
	}
	return nil
}

// BuildAndroidReleaseBundle bakes leaderAddr (and relayAddr, defaulting to
// leaderAddr when passed as "" -- see buildAndroidReleaseAAR's doc
// comment) into a fresh release AAR, then runs `gradlew bundleRelease` to
// produce the signed .aab Google Play Console's upload flow expects --
// Play has required the Android App Bundle format for every new app
// since 2021 (see BuildAndroidReleaseApk for a signed APK instead, e.g.
// for direct/manual distribution outside Play). Requires
// android-app/keystore.properties; fails fast if it's absent.
//
// Usage: mage buildandroidreleasebundle <leaderAddr> [relayAddr|""]
func BuildAndroidReleaseBundle(leaderAddr, relayAddr string) error {
	root, err := repoRoot()
	if err != nil {
		return err
	}
	if err := requireAndroidKeystore(root); err != nil {
		return err
	}
	if err := buildAndroidReleaseAAR(root, leaderAddr, relayAddr); err != nil {
		return err
	}
	androidDir := filepath.Join(root, "android-app")
	cmd := exec.Command(filepath.Join(androidDir, "gradlew"), "bundleRelease")
	cmd.Dir = androidDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("gradlew bundleRelease: %w", err)
	}
	fmt.Println("✅ android-app/app/build/outputs/bundle/release/app-release.aab signed and ready to upload to Google Play Console")
	return nil
}

// BuildAndroidReleaseApk is BuildAndroidReleaseBundle's APK counterpart
// (`gradlew assembleRelease` instead of `bundleRelease`) -- a signed APK
// for direct install/manual distribution outside Play (e.g.
// `adb install -r`), signed with the exact same release keystore/config.
// Google Play itself wants BuildAndroidReleaseBundle's .aab, not this.
//
// Usage: mage buildandroidreleaseapk <leaderAddr> [relayAddr|""]
func BuildAndroidReleaseApk(leaderAddr, relayAddr string) error {
	root, err := repoRoot()
	if err != nil {
		return err
	}
	if err := requireAndroidKeystore(root); err != nil {
		return err
	}
	if err := buildAndroidReleaseAAR(root, leaderAddr, relayAddr); err != nil {
		return err
	}
	androidDir := filepath.Join(root, "android-app")
	cmd := exec.Command(filepath.Join(androidDir, "gradlew"), "assembleRelease")
	cmd.Dir = androidDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("gradlew assembleRelease: %w", err)
	}
	fmt.Println("✅ android-app/app/build/outputs/apk/release/app-release.apk signed")
	return nil
}

// BuildAndRunDocker builds the relay image and recreates the container if it already exists.
// Usage: mage buildandrundocker
func BuildAndRunDocker() error {
	const (
		imageName     = "p2p-relay-app"
		containerName = "p2p-relay-container"
	)

	fmt.Println("🐳 Checking for existing Docker container...")

	// Check if the container exists (running or stopped)
	// 'docker ps -a -q' returns the container ID if found, or empty string if not
	id, _ := sh.Output("docker", "ps", "-a", "-q", "-f", "name=^/"+containerName+"$")

	if id != "" {
		fmt.Printf("🔄 Found existing container (%s). Recreating...\n", containerName)

		// Stop the container if it's currently running
		_ = sh.Run("docker", "stop", containerName)

		// Remove the old container
		if err := sh.Run("docker", "rm", containerName); err != nil {
			return fmt.Errorf("failed to remove old container: %w", err)
		}
		fmt.Println("🗑️  Old container removed successfully.")
	}

	// 1. Build the new Docker image from the current directory
	fmt.Printf("🛠️  Building Docker image: %s...\n", imageName)
	if err := sh.RunV("docker", "build", "-t", imageName, "."); err != nil {
		return fmt.Errorf("docker build failed: %w", err)
	}

	// 2. Run the new container in detached mode (-d)
	fmt.Printf("🚀 Launching new container: %s...\n", containerName)
	if err := sh.RunV("docker", "run", "-d", "--name", containerName, "-p", "4001:4001", imageName); err != nil {
		return fmt.Errorf("failed to run new docker container: %w", err)
	}

	fmt.Fprintln(os.Stderr, "\n=======================================================")
	fmt.Println("✅ Relay container is running in the background!")
	fmt.Println("👉 To view the live logs & Multiaddr, run:")
	fmt.Printf("   docker logs %s\n", containerName)
	fmt.Println("👉 To read the relay.txt file from inside the container, run:")
	fmt.Printf("   docker exec %s cat relay.txt\n", containerName)
	fmt.Fprintln(os.Stderr, "=======================================================")

	return nil
}

// BuildAndRunPodman builds the relay image and recreates the container using Podman.
// Usage: mage buildandrunpodman
func BuildAndRunPodman() error {
	const (
		imageName     = "localhost/p2p-relay-app"
		containerName = "p2p-relay-container"
	)

	fmt.Println("🦭 Checking for existing Podman container...")

	// Check if the container exists (running or stopped)
	// 'podman ps -a -q' returns the container ID if found
	id, _ := sh.Output("podman", "ps", "-a", "-q", "-f", "name=^/"+containerName+"$")

	if id != "" {
		fmt.Printf("🔄 Found existing container (%s). Recreating...\n", containerName)

		// Stop the container if it's currently running
		_ = sh.Run("podman", "stop", containerName)

		// Remove the old container
		if err := sh.Run("podman", "rm", containerName); err != nil {
			return fmt.Errorf("failed to remove old container: %w", err)
		}
		fmt.Println("🗑️  Old container removed successfully.")
	}

	// 1. Build the new Podman image from the current directory
	fmt.Printf("🛠️  Building Podman image: %s...\n", imageName)
	if err := sh.RunV("podman", "build", "-t", imageName, "."); err != nil {
		return fmt.Errorf("podman build failed: %w", err)
	}

	// 2. Run the new container in detached mode (-d)
	fmt.Printf("🚀 Launching new container: %s...\n", containerName)
	fmt.Printf("🚀 Launching new container using Host Networking: %s...\n", containerName)
	if err := sh.RunV("podman", "run", "-d", "--name", containerName, "--net=host", imageName); err != nil {
		return fmt.Errorf("failed to run new podman container: %w", err)
	}

	fmt.Fprintln(os.Stderr, "\n=======================================================")
	fmt.Println("✅ Relay container is running via Podman in the background!")
	fmt.Println("👉 To view the live logs & Multiaddr, run:")
	fmt.Printf("   podman logs %s\n", containerName)
	fmt.Println("👉 To read the relay.txt file from inside the container, run:")
	fmt.Printf("   podman exec %s cat relay.txt\n", containerName)
	fmt.Fprintln(os.Stderr, "=======================================================")

	return nil
}

// Shell attaches an interactive terminal to the running relay container.
// Usage: mage shell
func Shell() error {
	const containerName = "p2p-relay-container"

	// Determine if we should use podman or docker based on what is installed/running
	runtime := "docker"
	if _, err := sh.Output("podman", "--version"); err == nil {
		runtime = "podman"
	}

	fmt.Printf("🐚 Attaching interactive shell into %s (%s)...\n", containerName, runtime)
	fmt.Println("👉 Type 'exit' to disconnect without stopping the relay.")
	fmt.Println("-------------------------------------------------------")

	// sh.RunV automatically handles tying os.Stdin, os.Stdout, and os.Stderr
	// to your terminal so interactive CLI commands work perfectly.
	return sh.RunV(runtime, "exec", "-it", containerName, "/bin/sh")
}
