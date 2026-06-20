<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="./assets/banner-dark.png">
    <source media="(prefers-color-scheme: light)" srcset="./assets/banner-light.png">
    <img alt="Durable Sleep — Resonate Go SDK" src="./assets/banner-dark.png">
  </picture>
</p>

<p align="center">
  <a href="https://resonatehq.github.io/examples-ci/">
    <img src="https://img.shields.io/endpoint?url=https://resonatehq.github.io/examples-ci/status/example-durable-sleep-go.json" alt="examples-ci status">
  </a>
</p>

# Durable Sleep | Resonate Go SDK

A workflow that suspends for a configurable duration via Resonate's durable timer API. If the worker crashes mid-sleep, the timer promise is recorded server-side and the workflow resumes when the timer fires — long-duration sleeps (hours, days, weeks) survive worker restarts.

> Heads up — `resonate-sdk-go` is pre-release. The SDK has no semver tag yet, so this example pins to a specific commit. Expect API changes until `v0.1.0`.

## What this demonstrates

- Calling `ctx.Sleep(d)` to create a server-backed timer promise inside a workflow.
- Using `future.Await(nil)` to park the workflow goroutine until the timer resolves.
- Running the workflow against **localnet** (no external server) for quick local exploration.
- Running the workflow against a real **Resonate dev server** to demonstrate crash recovery: kill the worker mid-sleep, restart it with the same promise ID, and the workflow resumes rather than restarting.

## The code

```go
type SleepArgs struct {
    Secs int64 `json:"secs"`
}

func sleepingWorkflow(ctx *resonate.Context, args SleepArgs) (string, error) {
    fmt.Printf("sleeping for %d second(s)...\n", args.Secs)

    f, err := ctx.Sleep(time.Duration(args.Secs) * time.Second)
    if err != nil {
        return "", fmt.Errorf("ctx.Sleep: %w", err)
    }
    if err := f.Await(nil); err != nil {
        return "", fmt.Errorf("sleep await: %w", err)
    }

    return fmt.Sprintf("slept for %d second(s)", args.Secs), nil
}
```

`ctx.Sleep` creates a timer promise on the Resonate server and returns a `*Future`. `f.Await(nil)` parks the workflow until the promise resolves. With a real server, this suspension point survives a worker crash: another worker (or this one after restart) picks up from the checkpoint instead of starting over.

> **Note on `time.Duration` in args:** `time.Duration` JSON-marshals as a bare nanosecond integer, which is opaque in stored promise payloads. This example passes duration as `int64` seconds and converts at the call site — the idiomatic Go workaround until the SDK provides a codec-friendly duration type.

## Prerequisites

- Go 1.22+
- **Optional (for crash-recovery demo):** the `resonate` server CLI.
  Install with Homebrew on macOS or Linux:
  ```sh
  brew install resonatehq/tap/resonate
  ```
  Other install paths: <https://docs.resonatehq.io/get-started/quickstart>.

## Setup

```sh
git clone https://github.com/resonatehq-examples/example-durable-sleep-go.git
cd example-durable-sleep-go
go mod download
```

## Run it

### Localnet mode (no server required)

```sh
go run .
```

The binary builds an in-process Resonate instance using `localnet` — no external process needed. The workflow sleeps for 3 seconds (default) and exits.

Pass `-secs=<n>` to change the sleep duration:

```sh
go run . -secs=5
```

> **Localnet limitation:** the server state lives in process memory. A process crash also destroys the timer, so the crash-recovery story is not demonstrable in this mode. Use real-server mode below for that.

### Real-server mode (crash recovery)

In one terminal, start the dev server:

```sh
resonate dev
```

In another, run the example pointing at the server:

```sh
go run . -url=http://localhost:8001 -secs=30
```

The binary defaults to promise ID `durable-sleep-1`. Kill the worker mid-sleep (Ctrl-C) and run the same command again — because the ID is the same, Resonate re-attaches to the existing workflow rather than starting a new one. The workflow completes once the original timer fires.

To use a custom ID (for example, to run two independent workflows against the same server):

```sh
go run . -url=http://localhost:8001 -secs=30 -id=my-sleep-run
```

## What to look for

**Localnet run (default):**

```
[main] using localnet (in-process, no external server required)
[main] note: localnet state is ephemeral — crash recovery requires -url=<server>
[main] invoking workflow id=durable-sleep-1 secs=3
  [workflow] sleeping for 3 second(s)...
  [workflow] sleeping for 3 second(s)...
  [workflow] done — slept for 3 second(s)
[main] OK: slept for 3 second(s)
```

You'll see the workflow line twice — that's replay, not a bug. `ctx.Sleep` parks the workflow on a durable timer promise; when the timer fires, the runtime resumes by re-entering the workflow body from the top, so any `fmt.Println` calls before the `Sleep` run a second time. Wrapping the print in `ctx.Run` would make it idempotent under replay; the example leaves it bare to make the replay visible.

**Real-server run with crash recovery:**

1. Start with `-secs=30` and kill mid-sleep (Ctrl-C).
2. Run the same command again — the default `-id=durable-sleep-1` is unchanged, so Resonate re-attaches to the existing workflow rather than starting a new one.
3. After ~30 seconds total, the result prints even though the worker restarted.

You can inspect live and completed promise state on the dashboard at <http://localhost:8001>.

## File structure

```
example-durable-sleep-go/
├── main.go        workflow definition + entry point
├── go.mod         module declaration + SDK pin
├── go.sum         checksums
├── assets/        README banner images
├── LICENSE        Apache-2.0
└── README.md
```

## Next steps

- **Coming from Temporal?** See [MIGRATING-FROM-TEMPORAL.md](MIGRATING-FROM-TEMPORAL.md) — a side-by-side port of the matching `temporalio/samples-go` example.
- [Get started](https://docs.resonatehq.io/get-started) — install paths + first-program walkthrough.
- [Durable execution concepts](https://docs.resonatehq.io/learn/durable-execution) — what makes invocations durable and how the runtime resumes them.
- [Durable Sleep pattern](https://docs.resonatehq.io/get-started/examples/durable-sleep) — full documentation for this pattern.
- [`example-hello-world-go`](https://github.com/resonatehq-examples/example-hello-world-go) — the simplest starting point: register a function, run it durably, read the result.
- [`example-recursive-factorial-go`](https://github.com/resonatehq-examples/example-recursive-factorial-go) — recursive workflow + worker/client split in Go.

## Community

- Discord: <https://resonatehq.io/discord>
- X: <https://x.com/resonatehqio>
- LinkedIn: <https://linkedin.com/company/resonatehq>
- YouTube: <https://youtube.com/@resonatehq>
- Journal: <https://journal.resonatehq.io>

## License

[Apache-2.0](./LICENSE)
