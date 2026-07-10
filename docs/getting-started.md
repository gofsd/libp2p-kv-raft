# libp2p Multiplatform Example Documentation

This project provides a robust P2P setup designed to work across Linux, Android, and Web platforms, even behind strict firewalls and NATs. It uses a Relay/Signaling server to coordinate connections.

---

## 1. Linux CLI Usage

### Build
```bash
go build -o raft-client cmd/client/main.go
```

### Run
To connect, you need the multiaddress of a running Relay Server. **You must replace `<RELAY_IP>` and `<RELAY_PEER_ID>` with the actual values printed by your relay server.**

```bash
# Example (Replace with your server's values):
./raft-client /ip4/1.2.3.4/tcp/4001/p2p/QmYourRelayPeerID
```
The client will output a "Circuit Address" that others can use to connect to it through the relay.

---

## 2. Android Integration (via `gomobile`)

The `pkg/raft` package is designed to be bundled into an Android library (AAR).

### Generate AAR
1. Install [gomobile](https://pkg.go.dev/golang.org/x/mobile/cmd/gomobile).
2. Run bind command:
   ```bash
   gomobile bind -target=android -o libp2p-raft.aar ./pkg/raft
   ```

### Kotlin Implementation
Add the `.aar` to your Android Studio project and use it:
```kotlin
import raft.Raft
import raft.P2PNode

// In your background service or activity
val ctx = Raft.newContext() 
val node = Raft.newP2PNode(ctx, "/ip4/RELAY_IP/tcp/4001/p2p/ID")
val myAddress = node.getAddress()
Log.i("P2P", "My address via relay: $myAddress")
```

---

## 3. Web/WASM Integration

Browsers cannot use raw TCP/UDP. This setup handles Web support via WebSockets and WebRTC through the relay.

### Build WASM
```bash
GOOS=js GOARCH=wasm go build -o main.wasm cmd/client/main.go
```

### JavaScript Implementation
1. Copy `$(go env GOROOT)/misc/wasm/wasm_exec.js` to your web project.
2. Initialize in the browser:
```html
<script src="wasm_exec.js"></script>
<script>
  const go = new Go();
  // Pass the relay address via argv if needed
  go.argv = ["client", "/ip4/RELAY_IP/tcp/4002/ws/p2p/RELAY_ID"];
  
  WebAssembly.instantiateStreaming(fetch("main.wasm"), go.importObject).then((result) => {
    go.run(result.instance);
  });
</script>
```

---

## 4. Relay Server (The Backbone)
The relay must be running on a public IP.
```bash
go run cmd/relay/main.go
```
It listens on:
- `4001` (TCP/QUIC): For Linux/Android/Desktop.
- `4002` (WebSocket): For Web/WASM.
