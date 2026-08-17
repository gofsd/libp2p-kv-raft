# Using `mobile/kvmobile` (the Android client) from another Android app

This doc is a task-oriented reference for embedding this repo's raft/libp2p follower node into a
*different* Android app via the `kvmobile` [gomobile](https://pkg.go.dev/golang.org/x/mobile/cmd/gomobile)
AAR — as opposed to `README.md`, which documents this repo's own architecture, or `android-app/`,
which is the actual reference implementation (a demo app exercising every `kvmobile` call). If
you're generating Kotlin/Gradle code for a caller app, this file plus `android-app/`'s real source
(`MainActivity.kt`, `CommandCatalog.kt`, `CommandDetailScreen.kt`) is enough context — you don't
need this repo's history or `pkg/daemon` internals to get it right.

This file mirrors README.md's "Follower on Android" section closely; if the two ever disagree,
README.md is authoritative (same rule `CommandCatalog.kt`'s own doc comment states for itself).

## Mental model

There is no server to connect to and no network client library in the usual sense. `Start()`
brings up this repo's actual `kvnode` follower daemon **in-process, inside your app's own Android
process** (the exact same Go code `cmd/kvnode` runs on desktop, cross-compiled via `gomobile bind`)
and joins it to a raft cluster over libp2p. From then on:

- Every `kvmobile` call talks to that in-process daemon over `pkg/ipc`'s Android transport
  (`ASharedMemory`, no named rendezvous — this only works because the daemon and your calling code
  are the same OS process; there is no cross-app or cross-device IPC surface here).
- A `Submit`/`SubmitCommand` write is forwarded, inside libp2p, from this device (always a
  follower — a phone can never bootstrap a *fresh* cluster as leader) to whichever peer is
  currently the raft leader, wherever that happens to be running.
- A `Get`/`RangeScan`/`ListCommands`/etc. read answers from this device's own locally replicated
  SQLite-backed state — no network round trip.
- Exactly **one** daemon runs per process, joined to exactly one cluster at a time. There is no
  desktop-style multi-node registry to pick a "current" node from; switching identity/cluster is
  `Stop()` then `Start`/`StartWithKey`/`Join`/`JoinWithKey` against a different `dataDir` or
  `leaderAddr`.
- Unlike desktop, the leader to join has no operator to type it in at runtime — the app has to
  already know it. `leaderMultiaddr` is therefore baked in at **build time** via a Go linker flag
  (`-ldflags -X`), not a runtime parameter. `Join`/`JoinWithKey` are the escape hatch for switching
  to a *different* cluster at runtime (e.g. an address typed in or scanned from a QR code).

## Stability

This surface is **not** covered by the Go-module semver policy `docs/library-usage.md` documents
(that policy is scoped to callers of this module from another *Go* module — `pkg/shmclient`,
`pkg/daemon.Config`/`Run`, `pkg/kvctl`, the capnp wire format). `mobile/kvmobile`'s exported Go
functions (and therefore the Kotlin surface `gomobile bind` generates from them) can change shape
release to release; check `CHANGELOG.md` when upgrading which commit/tag you build the AAR from.
Pin to a specific commit or tag rather than always building off `main` if you want a stable target.

## Getting the AAR

There are two ways to get `kvmobile.aar`. Read both tradeoffs below before picking — Option A is
simpler to set up, but has a real limitation for the most common real-world case (a phone behind
NAT/cellular), not just a convenience difference.

### Option A: download a prebuilt AAR

Every tagged release (`.github/workflows/release.yml`, triggered by `mage major/minor/patch` +
`git push --tags`) builds a **generic, leader-agnostic** `kvmobile.aar` — compiled with no
`-ldflags` at all — and attaches it to the GitHub Release at a stable, versioned URL:

```
https://github.com/gofsd/libp2p-kv-raft/releases/download/<tag>/kvmobile.aar
```

Browse `https://github.com/gofsd/libp2p-kv-raft/releases` for available tags. This needs **no Go
toolchain, no NDK, and no checkout of this repo at all** on your side — the two vendored patches
discussed below (see "Why not `go get`" under Option B) are already compiled into the binary by the
time CI publishes it, so none of that machinery is your problem as a consumer.

**Tradeoff 1 — no `Start`/`StartWithKey`.** Since no `leaderMultiaddr` is baked in, this generic AAR
can **never** use them — both require a build-time leader and throw `"kvmobile: no leader multiaddr
baked in at build time"` otherwise. Always call `Join(dataDir, leaderAddr)` (or `JoinWithKey`)
instead, supplying the leader's address at runtime — `Join` works as the very first call against a
fresh `dataDir`, with no prior `Start` needed (it calls `Stop()` internally first, which is a safe
no-op if nothing is running yet). See "Cluster lifecycle" in the API reference below.

**Tradeoff 2 — no relay peer, ever, for this AAR (read this one carefully).** `relayMultiaddr` is
baked in exactly the same build-time way as `leaderMultiaddr`, and — this is the part that's easy to
miss — it's read by the *same shared code path* `Start` and `Join` both funnel through
(`startAgainst` in `kvmobile.go`, which unconditionally sets `daemon.Config.RelayPeer:
relayMultiaddr`). `Join` takes no relay parameter of its own to override this at runtime. Since
Option A's AAR is built with zero `-ldflags`, `relayMultiaddr` is always empty for it — meaning
**every** device using it, no matter what `leaderAddr` you pass to `Join`, gets no proactive relay
reservation at all. Per this repo's own node-connectivity policy (see "Relay addressing" under
Option B below): any device that can't guarantee it's directly dialable by the rest of the cluster
needs one — and a phone on cellular data essentially never can. Concretely, this means Option A
works reliably when your leader itself is directly dialable (a stable, port-open server — the same
shape as this repo's own `configs/bootstrap-nodes.json` entries) **and** joining devices are either
on the same reachable network or also directly dialable; for a device that's genuinely behind
NAT/carrier-grade NAT and needs a relay reservation to be reachable at all, only Option B (with
`relayMultiaddr` baked in) provides that — there's no way to get it from Option A's AAR at runtime.

Automate the download as part of your Gradle build with the well-known
[`de.undercouch.download`](https://github.com/michel-kraemer/gradle-download-task) plugin:

```kotlin
// app/build.gradle.kts
plugins {
    id("de.undercouch.download") version "5.6.0"
}

val kvmobileVersion = "v0.1.1"   // pin to a real tag from the releases page above
val kvmobileAar = layout.buildDirectory.file("kvmobile/kvmobile-$kvmobileVersion.aar")

tasks.register<de.undercouch.gradle.tasks.download.Download>("downloadKvmobileAar") {
    src("https://github.com/gofsd/libp2p-kv-raft/releases/download/$kvmobileVersion/kvmobile.aar")
    dest(kvmobileAar)
    overwrite(false)   // cached after the first fetch; bump kvmobileVersion to pull a newer one
}

tasks.named("preBuild") {
    dependsOn("downloadKvmobileAar")
}

dependencies {
    implementation(files(kvmobileAar))
}
```

Prefer not to add a third-party Gradle plugin? A plain custom task does the same thing with nothing
but the JDK's own `java.net` classes:

```kotlin
val kvmobileVersion = "v0.1.1"
val kvmobileAarFile = layout.buildDirectory.file("kvmobile/kvmobile-$kvmobileVersion.aar").get().asFile

tasks.register("downloadKvmobileAar") {
    outputs.file(kvmobileAarFile)
    doLast {
        if (!kvmobileAarFile.exists()) {
            kvmobileAarFile.parentFile.mkdirs()
            uri("https://github.com/gofsd/libp2p-kv-raft/releases/download/$kvmobileVersion/kvmobile.aar")
                .toURL().openStream().use { input -> kvmobileAarFile.outputStream().use { input.copyTo(it) } }
        }
    }
}
tasks.named("preBuild") { dependsOn("downloadKvmobileAar") }
dependencies { implementation(files(kvmobileAarFile)) }
```

Either way, `./gradlew assembleDebug` (or any build) now fetches `kvmobile.aar` automatically the
first time it's needed — no manual download-and-copy step, no local Go/NDK toolchain.

#### Alternative: a real Gradle dependency coordinate via an `ivy` repository

The download-task approach above works, but every consumer re-implements its own fetch-and-cache
logic. Gradle also supports pointing a plain `ivy` repository straight at GitHub Releases, so
`kvmobile.aar` resolves like any other `implementation("group:artifact:version")` coordinate — no
custom task, no `files(...)` dependency. `release.yml` already attaches the AAR under the literal
name `kvmobile.aar` (not version-suffixed) to each tag, so the pattern layout below has no
`[module]-[revision]` in it, just `[revision]/[module]`:

```kotlin
// settings.gradle.kts
dependencyResolutionManagement {
    repositories {
        exclusiveContent {
            forRepository {
                ivy {
                    url = uri("https://github.com/gofsd/libp2p-kv-raft/releases/download")
                    patternLayout { artifact("[revision]/[module].[ext]") }
                    metadataSources { artifact() }
                }
            }
            filter { includeModule("dev.gofsd", "kvmobile") }
        }
    }
}
```

```kotlin
// app/build.gradle.kts
dependencies {
    implementation("dev.gofsd:kvmobile:v0.1.1@aar")   // pin to a real tag from the releases page above
}
```

`exclusiveContent`/`filter` scope this ivy repository to exactly the `dev.gofsd:kvmobile` module, so
it never gets consulted (and never leaks a spurious 404 lookup) for any of your app's other, real
Maven Central dependencies. This is exactly the same setup gofsd's other gomobile-bound library,
[`shmring`](https://github.com/gofsd/shmring), documents for its own AAR — same GitHub-Releases-as-
ivy-repo trick, same reason (no Maven Central publish, but still a proper dependency coordinate
instead of a file path).

### Option B: build from source

Needed whenever `Start`/`StartWithKey`'s build-time-baked leader convenience matters, or — the more
common real reason — a joining device needs a relay reservation at all (see Tradeoff 2 above:
Option A's AAR can never set `relayMultiaddr`, full stop, not just "a non-default one"). Also the
only path for `identitySeedHex`/`joinSuffrage` (see the table below). Skip this and use Option A if
every device involved is directly dialable with no relay needed.

#### Why not `go get`

There's no `go get github.com/gofsd/libp2p-kv-raft` + `gomobile bind` against it as an ordinary
dependency from some other Go module — you build from a **full local checkout** (a `git clone`,
pinned to whatever tag/commit you want reproducible builds against).

This isn't just unpolished packaging — it's load-bearing. `go.mod` carries two local `replace`
directives:

```
replace github.com/wlynxg/anet => ./thirdparty/anet
replace modernc.org/libc => ./thirdparty/libc
```

Go's module system only honors a module's own `replace` directives when that module is the *main*
module of the build — they're silently ignored when the module is instead pulled in as someone
else's dependency. Both patches are load-bearing, not cosmetic (see `README.md`'s "Vendored
dependency patch" section for the full detail): `thirdparty/anet` fixes a `//go:linkname` against a
Go 1.25 stdlib symbol whose layout changed upstream — without it, **the build fails to compile**
against Go 1.25 at all. `thirdparty/libc` fixes a `SIGSYS` crash in `modernc.org/sqlite`'s runtime
that Android's seccomp filter triggers the instant SQLite opens its database file — without it, an
Android build **compiles cleanly and then crashes on-device** the moment it touches the store. So
building `mobile/kvmobile` as a bare dependency from a separate module (rather than a checkout where
this repo is the main module) would silently drop both fixes. (Option A's prebuilt AAR sidesteps
this entirely — the patches are already compiled in by the time CI publishes it.)

#### Prerequisites

- **Go 1.25+**, this repo checked out locally as the main module — see "Why not `go get`" above.
- **`gomobile` and `gobind` on `PATH`.** Build both from this repo's own pinned versions (`go.mod`
  declares them as `tool` dependencies) rather than `@latest`, to avoid a version mismatch between
  the two — this is exactly the bug this repo's own CI hit (see `.github/workflows/ci.yml`'s
  `install gobind` step) when only `gomobile` was reachable:

  ```bash
  cd /path/to/libp2p-kv-raft
  go install golang.org/x/mobile/cmd/gomobile
  go install golang.org/x/mobile/cmd/gobind
  export PATH="$(go env GOPATH)/bin:$PATH"
  ```

- **Android NDK, API 26 or higher**, with `ANDROID_NDK_HOME` set. `-androidapi` **must be 26+** —
  the shmring Android transport uses `ASharedMemory_create`, which the NDK headers only declare
  from API 26 onward. Building against a lower target doesn't fail with a clear "API too low"
  error; it silently hides the declaration and fails deep in the linker with a confusing `could
  not determine what C.ASharedMemory_create refers to` instead.
- Your own running `kvnode` leader (or a leader you're joining an existing cluster on) — see
  `README.md`'s "Running a cluster" section for standing one up with `mage addnode`, or
  `cmd/kvnode` directly if you're deploying without `mage`. `kvmobile` never bootstraps a cluster
  itself; it only ever joins one that already exists.

#### Build command

`gomobile bind` compiles `mobile/kvmobile` into an `.aar` your app's Gradle build consumes like any
other Android library dependency. The leader (and, usually, a relay peer — see below) are baked in
via `-ldflags -X`:

```bash
export ANDROID_NDK_HOME=<path-to-ndk>          # e.g. $ANDROID_HOME/ndk/<version>
LEADER_ADDR="/ip4/<leader-ip>/tcp/4001/p2p/<leader-peer-id>"

go tool gomobile bind -target=android -androidapi 26 \
  -ldflags "-X github.com/gofsd/libp2p-kv-raft/mobile/kvmobile.leaderMultiaddr=$LEADER_ADDR" \
  -o kvmobile.aar ./mobile/kvmobile
```

(`go tool gomobile` works without a prior `go install` step if you're inside this repo's own
checkout, since `go.mod` already lists `gomobile` as a tool dependency; the plain `gomobile bind`
form works too once it's on `PATH` per the prerequisites above — both invoke the same binary.)

#### Build-time `-ldflags -X` variables

All are set the same way: `-X github.com/gofsd/libp2p-kv-raft/mobile/kvmobile.<name>=<value>`.
Everything except `leaderMultiaddr` is optional.

| Variable | Required | Format | Purpose |
|---|---|---|---|
| `leaderMultiaddr` | **Yes** | full multiaddr incl. `/p2p/<peerID>` | The cluster this app joins on `Start`/`StartWithKey`. `Start` errors immediately if this was never set. |
| `relayMultiaddr` | No (defaults to `leaderMultiaddr`) | full multiaddr incl. `/p2p/<peerID>` | A circuit-relay v2 node this device reserves a slot through. Almost every real device needs this — see "Relay addressing" below. |
| `identitySeedHex` | No | 128 hex chars (a `pkg/e2edata.Node.PrivateKey`-format seed) | Pins this device to a specific, deterministic peer identity instead of a freshly random one on first run. Mainly useful for reproducible test builds, not normal app releases — leave unset so each real install gets its own fresh identity. |
| `joinSuffrage` | No | `"learner"` (anything else = voter) | Makes `Start`/`StartWithKey`'s automatic join a non-voting raft learner instead of a full voter. Leave unset for a normal app — this exists for test harnesses that don't want a churny, short-lived device's connection dropping the cluster's quorum count. |

#### Relay addressing (read this before shipping)

Per this repo's own node-connectivity policy: **any device that can't guarantee it's directly
dialable by the rest of the cluster must set `relayMultiaddr`.** A phone on cellular data behind
carrier-grade NAT is exactly that case — without it, this device can end up advertising only
addresses the leader can never dial back, and joining or replication stalls. `relayMultiaddr` is
normally just the leader's own address, deployed with `-relay-service` (see `configs/bootstrap-nodes.json`
for this repo's own reference deployment's shape) — set it explicitly if your leader isn't
running with `-relay-service` itself and you have a separate relay node.

#### Building a signed release AAR (this repo's own `android-app/` only)

If you're building *this repo's own* `android-app/` demo (not a separate app), `mage
buildandroidreleasebundle <leaderAddr> [relayAddr]` / `mage buildandroidreleaseapk <leaderAddr>
[relayAddr]` wrap the `gomobile bind` step above plus `gradlew bundleRelease`/`assembleRelease`,
requiring `android-app/keystore.properties` to exist first. For your own separate app, skip this —
just take the `.aar` from the build command above and follow Step 2 below with your own app's own
Gradle project and signing config.

## Step 2: Add the AAR to your app

There's no Maven Central/JitPack publish for `kvmobile`, so the ordinary
`implementation("group:artifact:version")` form only works if you set up the `ivy`-repository
alternative under Option A above (`dev.gofsd:kvmobile:<tag>@aar`). Without that, it's a **file**
dependency: Option A's Gradle task above already ends with `implementation(files(kvmobileAar))`,
resolving to whatever the download task fetched. If you went with Option B instead, drop the AAR
you built into your app module's `libs/` directory and reference it the same way:

```kotlin
dependencies {
    implementation(files("libs/kvmobile.aar"))   // Option B only -- Option A already declared its own dependency above
}
```

Either way, the rest of `android`'s config block is the same:

```kotlin
// app/build.gradle.kts
android {
    defaultConfig {
        minSdk = 26   // ASharedMemory_create's minimum -- see Prerequisites above
        // ...
    }
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
    kotlinOptions {
        jvmTarget = "17"
    }
    ndkVersion = "28.2.13676358"   // only matters if you're also building Option B's AAR yourself
}
```

```xml
<!-- app/src/main/AndroidManifest.xml -->
<uses-permission android:name="android.permission.INTERNET" />
```

That's the entire manifest requirement — `kvmobile` needs no other permission (no storage
permission either; `dataDir` below is expected to be an app-private directory you already have
access to, e.g. `Context.getFilesDir()`).

## Step 3: Call it from Kotlin

### The one threading rule

**Every `kvmobile` call may block** (a shmring IPC round trip to the in-process daemon, sometimes a
full raft round trip for a forwarded write). Never call from the main/UI thread. This repo's own
demo app wraps every call the same way:

```kotlin
import kvmobile.Kvmobile

Thread {
    try {
        val peerID = Kvmobile.start(filesDir.absolutePath)
        runOnUiThread { statusText.text = "Connected as $peerID" }
    } catch (e: Exception) {
        runOnUiThread { statusText.text = "Failed to start: ${e.message}" }
    }
}.start()
```

(A real app would likely use a coroutine/`Dispatchers.IO` instead of a raw `Thread` — the
requirement is only "off the main thread," not this specific mechanism. `android-app/`'s own demo
uses raw `Thread` deliberately to keep the reference implementation dependency-free.)

### Function naming

Go's exported `PascalCase` functions become Kotlin `camelCase` static methods on a generated
`Kvmobile` class in package `kvmobile` (i.e. `Start` → `Kvmobile.start`, `GetOwnAddr` →
`Kvmobile.getOwnAddr`, `SubmitCommand` → `Kvmobile.submitCommand`). Every Go
`(string, error)`/`error` return becomes a plain Kotlin `String`/`Unit` return that throws on
failure — gomobile has no multi-value or `Result`-style return, only exceptions. Every Go `string`
return that's documented as JSON below is a raw JSON string; decode it yourself (`org.json`,
`kotlinx.serialization`, Moshi — `kvmobile` has no opinion on which).

### Error handling

A Go `error` crosses the gomobile boundary as a thrown exception (`go.Universe$proxyerror` at
runtime — catch it as a plain `Exception`, or `Throwable` if you need to catch panics too) whose
`.message` is the Go error's `.Error()` string, always prefixed `"kvmobile: ..."`:

```kotlin
try {
    Kvmobile.submit("key", "value")
} catch (e: Exception) {
    // e.message == "kvmobile: submit: shmclient: ... " (or similar)
}
```

### Bringing the daemon up once

**If you built a custom AAR with `leaderMultiaddr` baked in (Option B)**, call `Start` (or
`StartWithKey`) exactly once for the process's lifetime — it's safe to call again (e.g. from every
`Activity.onCreate`); after the first successful call it just returns the already-running node's
peer id immediately, without re-joining:

```kotlin
class MyApplication : Application() {
    override fun onCreate() {
        super.onCreate()
        Thread {
            try {
                Kvmobile.start(filesDir.absolutePath)
            } catch (e: Exception) {
                // log/report -- see "Common pitfalls" below for why this can fail
            }
        }.start()
    }
}
```

**If you're using Option A's downloaded generic AAR**, `Start`/`StartWithKey` always throw
(`"kvmobile: no leader multiaddr baked in at build time"`, since no `leaderMultiaddr` was ever
compiled in) — call `Join(dataDir, leaderAddr)` instead, with the leader's address supplied by your
own app (a config value, a value typed in by a user, a QR scan, etc.). `Join` works as the very
first call for a fresh `dataDir`, no prior `Start` needed:

```kotlin
class MyApplication : Application() {
    override fun onCreate() {
        super.onCreate()
        val leaderAddr = "/ip4/<leader-ip>/tcp/4001/p2p/<leader-peer-id>"   // from your own app's config
        Thread {
            try {
                Kvmobile.join(filesDir.absolutePath, leaderAddr)
            } catch (e: Exception) {
                // log/report -- see "Common pitfalls" below for why this can fail
            }
        }.start()
    }
}
```

### A complete Submit/Get example

```kotlin
Thread {
    try {
        Kvmobile.submit("user:0042:name", "Ada Lovelace")
        val value = Kvmobile.get("user:0042:name")   // "Ada Lovelace"
        // fixed-width numeric ids keep lexicographic order == numeric order,
        // so this inclusive range covers every "user:NNNN:name" key from 0000 to 0099
        val someUsers = Kvmobile.rangeScan("user:0000:name", "user:0099:name", 100)  // JSON array, see below
        runOnUiThread { /* update UI with value / someUsers */ }
    } catch (e: Exception) {
        runOnUiThread { /* show e.message */ }
    }
}.start()
```

## Full API reference

Grouped exactly like `android-app/`'s own `CommandCatalog.kt`, since that file's own doc comment
promises to mirror this API 1:1. Source file in parens; read that file's doc comments for the full
design rationale behind any entry — this table is deliberately just signatures + one line.

### Cluster lifecycle (`kvmobile.go`, `joinrequest.go`, `joininvite.go`)

| Kotlin call | Returns | Purpose |
|---|---|---|
| `Kvmobile.start(dataDir)` | peer id | Bring the daemon up under `dataDir`, join the build-time `leaderMultiaddr`. Idempotent. |
| `Kvmobile.startWithKey(dataDir, keyHex)` | peer id | Like `start`, but provisions `dataDir`'s identity from an existing `identity.key`-format hex string instead of generating/reusing one. Refuses if `dataDir` already holds a *different* identity. |
| `Kvmobile.join(dataDir, leaderAddr)` | peer id | Switch this device to a *different* cluster at runtime (`leaderAddr` typed in or scanned). Always stops any currently-running daemon first, unlike `start`. |
| `Kvmobile.joinWithKey(dataDir, keyHex, leaderAddr)` | peer id | `join` + `startWithKey`'s identity provisioning combined. |
| `Kvmobile.startPending(dataDir)` | peer id | Bring the daemon up with **no** cluster join at all — prerequisite for the reverse "join-request" flow below. |
| `Kvmobile.startPendingWithKey(dataDir, keyHex)` | peer id | `startPending` + explicit identity. |
| `Kvmobile.getOwnAddr()` | multiaddr string | This device's own current best-advertised address (public, then relay, then anything else). Query live — a relay reservation completes asynchronously, so call again if you get back a private/loopback address. |
| `Kvmobile.createJoinRequest()` | `tokenHex` | Mint a one-time ticket. Combine with `getOwnAddr()` as `"<addr>#<tokenHex>"` (e.g. into a QR code) for some other cluster's voter to redeem via `recruitPeer`. |
| `Kvmobile.cancelJoinRequest(tokenHex)` | — | Clear a pending ticket before it's redeemed. |
| `Kvmobile.recruitPeer(ticket, suffrage)` | `"<peerID> ok"` or `"<peerID> pending"` | The other direction: this device (an existing voter) admits whatever device produced `ticket` (`"<addr>#<tokenHex>"`) directly into its own cluster. `suffrage` is `"voter"` or `"learner"`. |
| `Kvmobile.createJoinInvite(suffrage)` | `tokenHex` | Mint a token granting `suffrage` (`"voter"`/`"learner"`) on *this* device's own cluster, without hand-delivering it. Combine with `getOwnAddr()` as `"<addr>#<tokenHex>"` for another device's `join`/`start` to redeem directly — admitted immediately even if the leader normally requires confirmation. |
| `Kvmobile.revokeJoinInvite(tokenHex)` | — | Delete an invite before it's redeemed. |
| `Kvmobile.stop()` | — | Shut the current daemon down (so a different `dataDir`/identity can be started next). |
| `Kvmobile.delete(dataDir)` | — | Wipe `dataDir` outright. Refuses while a daemon is running against it. |
| `Kvmobile.leave()` | — | Gracefully leave the current cluster (`raft.RemoveServer`) and stop. |
| `Kvmobile.rm()` | — | `leave()` + revoke this device's join standing + delete the joined cluster's local data (never the identity key itself). |
| `Kvmobile.kick(targetPeerID)` | — | Force-remove *another* peer from the current cluster, without its cooperation. This device's own membership is untouched. Only works while the cluster still has quorum. |
| `Kvmobile.listClusters()` | JSON array (0 or 1 entries) | Whichever cluster, if any, this device is currently joined to. |
| `Kvmobile.listClusterMembers()` | JSON array | The current cluster's full live voter/learner/leader membership. |
| `Kvmobile.peerID()` | peer id string | This device's own current peer id (no I/O). |
| `Kvmobile.accessToken()` | token string | This device's deterministic `cmd/kvhttp` bearer token, derived from its own `identity.key`. |

### KV (`kvmobile.go`, `range_scan.go`)

| Kotlin call | Returns | Purpose |
|---|---|---|
| `Kvmobile.submit(key, value)` | — | Write `key`/`value`, forwarded to the current leader if this device isn't it. |
| `Kvmobile.get(key)` | value string | Read `key` from this device's own locally replicated state. |
| `Kvmobile.rangeScan(start, end, limit)` | JSON array of `{"key":"...","value":"..."}` | Every key/value pair in `[start, end]` (inclusive, lexicographic byte order), up to `limit` results (`0` = unlimited). |

### Permits (`kvmobile.go`)

| Kotlin call | Returns | Purpose |
|---|---|---|
| `Kvmobile.requestPermit(kind, targetPeerID, metadata)` | — | `kind` is `"bootstrap"` (see `permitKindFromName`); `"cluster-join"` is confirm/revoke-side. |
| `Kvmobile.confirmPermit(kind, targetPeerID)` | — | |
| `Kvmobile.revokePermit(kind, targetPeerID)` | — | |

There is no log-permit counterpart: `requestLogPermit`/`confirmLogPermit`/
`revokeLogPermit` and the `"peer"` kind were removed from the wire protocol
outright, and `pkg/logrecord` access now runs through the Group/Command ACL
catalog instead (see "Group / Command ACL catalog" below).

### Execute — raft-bypassing peer-to-peer notification (`kvmobile.go`)

| Kotlin call | Returns | Purpose |
|---|---|---|
| `Kvmobile.execute(destPeerID, value)` | — | Send `value` directly to `destPeerID`, best-effort, no raft/durability. |
| `Kvmobile.pollExecute()` | JSON `{"pending":true,"sender_peer_id":"...","value":"..."}` or `{"pending":false}` | One-shot manual drain of this device's notification queue. |
| `Kvmobile.watchExecute(cb)` | — | Continuous delivery instead of polling — see "Callback interfaces" below. |
| `Kvmobile.stopWatchExecute()` | — | Stop this device's own delivery loop (local only). |

### Channel — raw bidirectional byte pipe (`channel.go`)

| Kotlin call | Returns | Purpose |
|---|---|---|
| `Kvmobile.openChannel(peerID, cb)` | `channelID` | Open a persistent pipe to `peerID`. |
| `Kvmobile.listenChannel(cb)` | JSON (claimed channel info) | Accept the next incoming channel open. |
| `Kvmobile.stopListenChannel()` | — | Stop listening (local only). |
| `Kvmobile.sendChannelData(channelID, base64Chunk)` | — | Data crosses base64-encoded both directions — gomobile's string-only boundary can't carry arbitrary binary safely. |
| `Kvmobile.closeChannel(channelID)` | — | Ends the session server-side too (unlike `stopChannel`). |
| `Kvmobile.stopChannel(channelID)` | — | Stop this device's own delivery loop only (local only — the channel/listen may still be live server-side). |

### Log records — `pkg/logrecord` (`kvmobile.go`)

Record shape (both directions): `{"kind":"...","unit_id":"...","timestamp":"RFC3339...","author_peer_id":"...","fields":{...},"narrative":"..."}`.

| Kotlin call | Returns | Purpose |
|---|---|---|
| `Kvmobile.logAppend(kind, unitID, fieldsJSON, narrative)` | — | `fieldsJSON` is a JSON object of string→string, caller-defined. |
| `Kvmobile.logQuery(kind, unitID, since, until, limit)` | JSON array of Record | `since`/`until` RFC3339 or blank; `limit` blank = unlimited. Never returns `null`, `"[]"` when empty. |

### Group / Command ACL catalog (`catalog.go`)

Only a current raft voter may write any `Group`/`Command`/link record — enforced daemon-side, not
client-side. `Group` shape: `{"id":"...","name":"...","public":true|false}`. `Command` shape:
`{"id":"...","name":"...","target_peer_id":"..."}`.

| Kotlin call | Returns | Purpose |
|---|---|---|
| `Kvmobile.createGroup(id, name, public)` | — | `public=true` grants unconditional access to this group's linked commands, no membership needed. Same op as `updateGroup` — a Put. |
| `Kvmobile.updateGroup(id, name, public)` | — | Alias of `createGroup` for the "id already exists" case. |
| `Kvmobile.deleteGroup(id)` | — | Cascades to every link referencing it. |
| `Kvmobile.getGroup(id)` | JSON Group | |
| `Kvmobile.listGroups()` | JSON array of Group | |
| `Kvmobile.createCommand(id, name, targetPeerID)` | — | `targetPeerID` is who executes it. Same Put semantics as `createGroup`. |
| `Kvmobile.updateCommand(id, name, targetPeerID)` | — | |
| `Kvmobile.deleteCommand(id)` | — | |
| `Kvmobile.getCommand(id)` | JSON Command | |
| `Kvmobile.listCommands()` | JSON array of Command | |
| `Kvmobile.addCommandToGroup(commandID, groupID)` | — | A command may belong to several groups. |
| `Kvmobile.removeCommandFromGroup(commandID, groupID)` | — | |
| `Kvmobile.listGroupsForCommand(commandID)` | JSON array of Group | |
| `Kvmobile.addPeerToGroup(peerID, groupID)` | — | |
| `Kvmobile.removePeerFromGroup(peerID, groupID)` | — | |
| `Kvmobile.listGroupsForPeer(peerID)` | JSON array of Group | |

### Dispatch — turning a Command into a request/response flow (`dispatch.go`)

`CommandRequest` shape: `{"instance_id":"...","command_id":"...","requested_by":"...","inputs":"...","requested_at":"RFC3339..."}`.

| Kotlin call | Returns | Purpose |
|---|---|---|
| `Kvmobile.submitCommand(commandID, inputsJSON)` | `instanceID` | Requires this device be permitted for `commandID` (via a shared group). Writes a durable `CommandRequest` and best-effort pokes the command's `targetPeerID`. |
| `Kvmobile.getCommandRequest(commandID, instanceID)` | JSON CommandRequest | |
| `Kvmobile.listCommandRequests(commandID)` | JSON array of CommandRequest | A target device's catch-up path for a poke it might have missed. |
| `Kvmobile.listExecutionsByPeer(peerID)` | JSON array | Every command execution touching `peerID` (as requester or target), most-recent-first, capped at 200. |
| `Kvmobile.appendCommandLog(requesterPeerID, instanceID, fieldsJSON, narrative)` | — | The target's progress report for one dispatch. `requesterPeerID` blank = no poke back. |
| `Kvmobile.queryCommandLog(instanceID, since, until, limit)` | JSON array of Record | |
| `Kvmobile.latestCommandLog(instanceID)` | JSON Record (single) | Just the newest entry. |
| `Kvmobile.watchCommandLog(instanceID, cb)` | — | Continuous delivery of new records — see "Callback interfaces". |
| `Kvmobile.stopWatchCommandLog(instanceID)` | — | |
| `Kvmobile.runCommandDispatcher(commandID, handler)` | — | Standing handler that answers every `CommandRequest` against `commandID` as it arrives — see "Callback interfaces". |
| `Kvmobile.stopCommandDispatcher(commandID)` | — | |

### One-time execution invites (`execinvite.go`)

| Kotlin call | Returns | Purpose |
|---|---|---|
| `Kvmobile.createExecInvite(commandID, inputsJSON)` | `tokenHex` | Combine with this device's own advertised multiaddr yourself (`"<addr>#<tokenHex>"`) — no barcode-rendering here, that's the app's job. |
| `Kvmobile.revokeExecInvite(tokenHex)` | — | |
| `Kvmobile.redeemExecInvite(sourceAddrAndToken)` | result string | `sourceAddrAndToken` is `"<addr>#<tokenHex>"`. |

### Raw escape hatch (`kvmobile.go`, `eventcodec.go`)

| Kotlin call | Returns | Purpose |
|---|---|---|
| `Kvmobile.sendEvent(eventJSON)` | result JSON | Direct access to the underlying `pkg/shmevent` wire event, for anything not covered above. See `pkg/shmevent`'s doc comment (`api/shmevent.capnp`) for the event shape. Same escape hatch this repo's own e2e test suite uses. |
| `Kvmobile.triggerEvent(eventJSON)` | result JSON | Alias for `sendEvent` — same call, named to pair with `encodeEvent`/`decodeEvent` below for a scan-and-confirm flow: encode a form's inputs into a scannable code, decode a scanned code back into JSON for a confirmation prompt, then trigger whichever event the user confirms. |
| `Kvmobile.encodeEvent(eventJSON)` | raw `ByteArray` | Same `eventJSON` shape as `sendEvent`, but returns the signed, capnp-encoded wire bytes instead of dispatching them — e.g. to render as a scannable DataMatrix/QR code (see `android-app/app/src/main/java/com/gofsd/kvdemo/DataMatrixCodec.kt` for a binary-safe ZXing DataMatrix round trip: ZXing's reader/writer are String-only, so arbitrary bytes need an ISO-8859-1 `ByteArray<->String` conversion on both sides). Requires `Start` to have completed (signing a non-`get*Key` op needs this device's own private key). The signature is generated by *this* device but is inert for a scan-and-trigger flow — `decodeEvent` never verifies it, and whichever device eventually calls `sendEvent`/`triggerEvent` re-signs from scratch with its own key. |
| `Kvmobile.decodeEvent(raw)` | result JSON | `encodeEvent`'s inverse: given raw capnp wire bytes (e.g. scanned from a code), returns the same `pkg/e2edata.Event` JSON shape `sendEvent`/`encodeEvent` use. Pure decode — no running daemon needed, doesn't verify the signature. |

`android-app/`'s own command-runner UI (`CommandDetailScreen.kt`) uses all three together: its "Generate DataMatrix" button calls `encodeEvent` on a form's current inputs and renders the result as a DataMatrix code; a persistent scanner (`ScannerHost.kt`, live on every screen) decodes a scanned code via `decodeEvent` and shows a confirm/cancel dialog before ever calling `triggerEvent` — see that app's own source for a complete example, not reproduced here since it's specific to this repo's reference app rather than the general embedding API this document covers.

## Callback interfaces (reverse binding)

`WatchExecute`, `WatchCommandLog`, `RunCommandDispatcher`, `OpenChannel`, and `ListenChannel` are
the only calls that aren't plain string-in/string-out — gomobile has no function-parameter support,
so each takes a Kotlin-implemented interface instead, called back on a Go-owned goroutine (**never**
your calling thread — hop back to the UI thread yourself before touching views, exactly like any
other background-thread callback in Android).

```kotlin
import kvmobile.ExecuteCallback

Kvmobile.watchExecute(object : ExecuteCallback {
    override fun onNotification(senderPeerID: String, value: String) {
        // called once per notification, in delivery order, on Go's own goroutine
        runOnUiThread { /* update UI */ }
    }
})
// ... later:
Kvmobile.stopWatchExecute()
```

```kotlin
import kvmobile.LogCallback

Kvmobile.watchCommandLog(instanceID, object : LogCallback {
    override fun onRecords(recordsJSON: String) {
        // called whenever a poll finds new records since the last one (never with an empty array)
    }
})
```

```kotlin
import kvmobile.CommandDispatchHandler

Kvmobile.runCommandDispatcher(commandID, object : CommandDispatchHandler {
    override fun handle(instanceID: String, commandID: String, requestedBy: String, inputs: String): String {
        // do the actual work commandID represents, then report the result:
        return """{"fields":{"status":"done"},"narrative":"handled by my app"}"""
    }
})
```

```kotlin
import kvmobile.ChannelCallback

val cb = object : ChannelCallback {
    override fun onData(chunk: String) {
        // chunk is base64-encoded
    }
    override fun onClosed(reason: String) {
        // reason is "" for a clean close
    }
}
val channelID = Kvmobile.openChannel(peerID, cb)
```

Calling `watchExecute`/`watchCommandLog`/`runCommandDispatcher` again **replaces** any previously
registered watcher for the same target rather than running two at once. A `WatchExecute`
registration survives a `stop()`/`start()` identity switch with no need to re-register — the loop
just waits while no daemon is running.

## Common pitfalls

- **Calling `Start`/`StartWithKey` with Option A's downloaded generic AAR.** It has no
  `leaderMultiaddr` baked in at all, so both always throw `"kvmobile: no leader multiaddr baked in
  at build time"`. Use `Join`/`JoinWithKey` instead — see "Bringing the daemon up once" above.
- **Expecting Option A's AAR to work behind NAT/cellular.** It also has no `relayMultiaddr` baked
  in, and `Join` has no runtime parameter to supply one — see Tradeoff 2 under "Option A" above. A
  device that can't be dialed directly will silently fail to join/replicate reliably; use Option B
  if any joining device needs a relay reservation.
- **Calling from the main thread.** Every call can block on IPC or a raft round trip; doing this on
  the UI thread risks an ANR. See "The one threading rule" above.
- **`-androidapi` below 26**, or a Gradle `minSdk` below 26. Fails with a confusing linker error
  (`could not determine what C.ASharedMemory_create refers to`), not a clear version error.
- **No `relayMultiaddr` for a device behind NAT/cellular.** `Start`/`Join` will often still
  technically succeed, but ongoing replication/forwarded writes can silently stall. See "Relay
  addressing" above — set it unless you're certain this device is directly dialable.
- **Expecting a runtime leader override.** `leaderMultiaddr` is compiled in; there's no equivalent
  of an app config file or environment variable to change it without rebuilding the AAR. Use
  `Join`/`JoinWithKey` if you need runtime flexibility.
- **Missing `android.permission.INTERNET`.** The only manifest permission this needs, but easy to
  forget since nothing else about the setup looks network-related.
- **Treating `Start`'s peer id as stable across `Delete`.** `Delete(dataDir)` really does erase the
  identity; a subsequent `Start` against the same `dataDir` generates a brand new one (unless you
  pass a key via `StartWithKey`).

## Where to look for more

- `README.md`'s "Follower on Android" section — authoritative narrative, kept current in detail.
- `android-app/` — the actual reference implementation: `MainActivity.kt`/`AppRoot.kt` (bring-up,
  NavHost + persistent scanner), `CommandCatalog.kt` (every call, one `CommandSpec` each),
  `CommandDetailScreen.kt` (the off-thread call pattern, plus the "Generate DataMatrix" button),
  `OutputLog.kt` (fan-in for the three standing-subscription callbacks), `ScannerHost.kt`/
  `MainScannerWidget.kt`/`DataMatrixCodec.kt` (the scanner and its binary-safe DataMatrix codec).
- `CONTRIBUTING.md`'s "Setting up" section for the short version of the prerequisites above.
- `mobile/kvmobile/*.go` — every exported function's own doc comment goes into far more design
  rationale than this file's summary table does.
- `.github/workflows/release.yml` — builds and publishes Option A's generic `kvmobile.aar` on every
  `v*` tag push; `https://github.com/gofsd/libp2p-kv-raft/releases` lists what's actually available.
