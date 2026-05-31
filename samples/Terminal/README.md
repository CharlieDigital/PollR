# Terminal Sample

This sample requires running one of the two web samples.

Run a web sample first:

```bash
dotnet run --project samples/WebMinimal --urls http://localhost:5000
```

Then run the terminal sample:

```bash
dotnet run --project samples/Terminal -- http://localhost:5000
```

The terminal sample connects five SSE clients.

- 1 set of two clients on `channel-1`
- 1 set of two clients on `channel-2`
- 1 client on `channel-3`

The TUI also includes a send panel for choosing one of the three channels and posting a message to the running web sample.

## Behavior Note

Because this demonstrates the real behavior, if you exit the TUI and restart while the server is running, the clients will reconnect and start receiving messages again 😎

This is how we would expect new clients joining to catch up.
