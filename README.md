# BlackBox Hello World App

This project provides a minimal HTTP server written in Rust using the [`warp`](https://crates.io/crates/warp) framework. The server responds with `Hello world` on the root path. All previous JavaScript code has been removed, so no Node.js setup is required.

## Building and Running

Ensure you have Rust installed, then run:

```bash
cargo run
```

The server listens on port `3000` by default. Set the `PORT` environment variable to change the port.

Visit `http://localhost:3000/` to see the message.
