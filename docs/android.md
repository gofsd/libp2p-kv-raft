# Android Integration Guide

This guide explains how to integrate the libp2p-kv-raft library into an Android application.

## Storage backend

The replicated key-value store is backed by SQLite via the pure-Go
[modernc.org/sqlite](https://pkg.go.dev/modernc.org/sqlite) driver (see `pkg/store`). Being pure
Go, it needs no CGO toolchain or extra native library of its own to cross-compile for Android --
the NDK requirement below is for `gomobile bind` itself (and for the shmring Android transport's
`ASharedMemory` binding), not for the store.

## 1. Build the Library
Use `gomobile` to generate the Android Archive (AAR).

```bash
# From project root
gomobile bind -target=android -o docs/android/libp2p-raft.aar ./pkg/raft
```

## 2. Project Setup
1. Copy `libp2p-raft.aar` to your Android project's `libs/` folder.
2. In your `build.gradle`, ensure you include the library:
   ```gradle
   dependencies {
       implementation fileTree(dir: 'libs', include: ['*.aar'])
   }
   ```

## 3. Usage (Kotlin)
```kotlin
import raft.Raft
import raft.P2PNode

class P2PManager(val relayAddr: String) {
    private var node: P2PNode? = null

    fun start() {
        try {
            val ctx = Raft.newContext() 
            node = Raft.newP2PNode(ctx, relayAddr)
            val myAddr = node?.getAddress()
            println("P2P Node started: $myAddr")
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }
}
```
