# Linux CLI Usage Guide

## Build
```bash
go build -o raft-client cmd/client/main.go
```

## Run
Connect to a public relay to join the P2P network.
```bash
./raft-client /ip4/PUBLIC_IP/tcp/4001/p2p/RELAY_ID
```

Your node will print its "Circuit Address". Other peers can use this address to connect to you even if you are both behind strict NATs.
