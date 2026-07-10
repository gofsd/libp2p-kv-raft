# Web/WASM Integration Guide

Since browsers cannot use raw TCP/UDP, this project leverages WebSockets and WebRTC through a libp2p Relay.

## 1. Build for WASM
```bash
GOOS=js GOARCH=wasm go build -o docs/web/main.wasm cmd/client/main.go
```

## 2. Browser Integration
You need the `wasm_exec.js` from your Go installation.
```bash
cp $(go env GOROOT)/misc/wasm/wasm_exec.js docs/web/
```

### index.html
```html
<!DOCTYPE html>
<html>
<head>
    <script src="wasm_exec.js"></script>
</head>
<body>
    <script>
        const go = new Go();
        // Provide relay address as CLI argument
        go.argv = ["client", "/ip4/RELAY_IP/tcp/4002/ws/p2p/RELAY_ID"];
        
        WebAssembly.instantiateStreaming(fetch("main.wasm"), go.importObject).then((result) => {
            go.run(result.instance);
        });
    </script>
</body>
</html>
```
