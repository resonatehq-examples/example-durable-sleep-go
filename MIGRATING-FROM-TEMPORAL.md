# Coming from Temporal: Durable Sleep

This guide maps the [`temporalio/samples-go/sleep-for-days`](https://github.com/temporalio/samples-go/tree/main/sleep-for-days) example to this Resonate example, so you can port the durable-timer pattern without guessing at API differences. The simpler bare-timer reference in that repo is [`samples-go/timer`](https://github.com/temporalio/samples-go/tree/main/timer) — see notes below on how that shape fits here too.

## The pattern

A durable timer lets a workflow suspend for hours or days without holding a thread. In Temporal the timer is `workflow.NewTimer` — a `workflow.Future` that fires after a duration. In this Resonate example the same role is played by `ctx.Sleep(d)`, which creates a server-side timer promise and returns a `*Future` whose `Await` parks the workflow goroutine until the promise resolves.

One structural difference: the Temporal sample (`sleep-for-days`) is not a bare sleep. It loops indefinitely, sending an email on each iteration and then racing a 30-day timer against a `"complete"` signal channel via a `Selector`. The loop exits only when the signal arrives. This example covers the timer half of that pattern. The loop-plus-external-signal half maps to a latent durable promise — that shape is shown in the companion [`example-human-in-the-loop-go`](https://github.com/resonatehq-examples/example-human-in-the-loop-go) example.

## Side by side

### Temporal (`samples-go/sleep-for-days`)

```go
// sleep-for-days/sleepfordays_workflow.go
func SleepForDaysWorkflow(ctx workflow.Context) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
	})

	isComplete := false
	sigChan := workflow.GetSignalChannel(ctx, "complete")

	for !isComplete {
		workflow.ExecuteActivity(ctx, SendEmailActivity, "Sleeping for 30 days")
		selector := workflow.NewSelector(ctx)
		selector.AddFuture(workflow.NewTimer(ctx, time.Hour*24*30), func(f workflow.Future) {})
		selector.AddReceive(sigChan, func(c workflow.ReceiveChannel, more bool) {
			isComplete = true
		})
		selector.Select(ctx)
	}

	return "done", nil
}
```

The timer is `workflow.NewTimer(ctx, time.Hour*24*30)` — a `workflow.Future` added to a `Selector` alongside the signal channel. Each `selector.Select(ctx)` call blocks until whichever branch fires first. If the timer fires the loop continues; if the signal fires `isComplete` flips and the loop exits.

### Resonate (this example)

```go
func sleepingWorkflow(ctx *resonate.Context, args SleepArgs) (string, error) {
	fmt.Printf("  [workflow] sleeping for %d second(s)...\n", args.Secs)

	d := time.Duration(args.Secs) * time.Second
	f, err := ctx.Sleep(d)
	if err != nil {
		return "", fmt.Errorf("ctx.Sleep: %w", err)
	}

	// Await blocks until the timer promise resolves. With a real server this
	// is also the suspension point that survives a worker crash.
	if err := f.Await(nil); err != nil {
		return "", fmt.Errorf("sleep await: %w", err)
	}

	msg := fmt.Sprintf("slept for %d second(s)", args.Secs)
	fmt.Printf("  [workflow] done — %s\n", msg)
	return msg, nil
}
```

`ctx.Sleep(d)` creates a timer promise on the Resonate server and returns a `*Future`. `f.Await(nil)` parks the goroutine until that promise resolves. If the worker is killed while the timer is pending, another worker (or this one after restart) resumes from the promise rather than starting over.

## Concept mapping

| Temporal | Resonate | Notes |
|---|---|---|
| `workflow.NewTimer(ctx, d)` | `ctx.Sleep(d)` | Both accept `time.Duration`. Both create a server-side timer that survives worker restarts when backed by a real server. |
| `workflow.Future` (from `NewTimer`) | `*resonate.Future` (from `ctx.Sleep`) | Both represent a pending result you block on. |
| `selector.AddFuture(f, fn)` + `selector.Select(ctx)` | `f.Await(nil)` | In the sleep-for-days loop, `Select` races the timer against a signal; here the single-timer case collapses to a plain `Await`. |
| `workflow.GetSignalChannel` + `selector.AddReceive` | `ctx.Promise()` + `f.Await` | The signal / completion branch maps to a latent durable promise. See the human-in-the-loop example. |
| `workflow.ExecuteActivity` + `ActivityOptions{StartToCloseTimeout}` | `ctx.Run(fn, args)` | No separate activity type. Function identity is established at `resonate.Register` time, not per-call. For simple calls no options struct is required; pass `RunOpts{Timeout: d}` as the optional third argument when you need a child deadline. |
| `workflow.WithActivityOptions` | `ctx.Run(fn, args, resonate.RunOpts{Timeout: d, RetryPolicy: p})` | `RunOpts` is the direct equivalent of `ActivityOptions` for timeout and retry policy. Pass it as the optional third argument to `ctx.Run`. |
| Worker task queue | `resonate.Config{...}` + `resonate.Register` | The group name (first arg to `localnet.NewLocal("default", &pid)`, or `HTTPOptions.Group` for real-server mode) is the routing equivalent of a Temporal task queue. For a single-worker setup the default group suffices. |
| `client.ExecuteWorkflow` | `sleepFn.Run(ctx, id, args)` | The promise ID passed to `Run` is the idempotency key. Re-running with the same ID re-attaches to the existing workflow rather than starting a new one. |

## Porting it, step by step

1. **Replace the import.** Swap `go.temporal.io/sdk/workflow` for `github.com/resonatehq/resonate-sdk-go`. If you are running in localnet (in-process) mode, also import `github.com/resonatehq/resonate-sdk-go/localnet`; real-server mode needs only the main SDK import.

2. **Change the function signature.** The Temporal workflow receiver is `workflow.Context`; the Resonate receiver is `*resonate.Context`. Add an explicit args struct for any parameters — Resonate passes workflow arguments as a single serialisable value.

3. **Replace the timer.** Every `workflow.NewTimer(ctx, d)` becomes `ctx.Sleep(d)`. Both accept `time.Duration`. Check the error return from `ctx.Sleep` before calling `Await`.

4. **Replace `selector.Select` with `f.Await`.** For the simple single-timer case, drop the `Selector` entirely. `f.Await(nil)` parks the goroutine until the timer promise resolves.

5. **Port the loop and completion signal separately.** The `for !isComplete` loop paired with `workflow.GetSignalChannel` does not have a direct single-call equivalent here. To replicate it: run each loop body as a separate `ctx.Run` step, and replace the signal channel with a latent promise created via `ctx.Promise()`. The external caller resolves that promise to break the loop. See [`example-human-in-the-loop-go`](https://github.com/resonatehq-examples/example-human-in-the-loop-go) for the promise-resolve pattern.

6. **Remove activity boilerplate.** `workflow.WithActivityOptions` and `ActivityOptions{StartToCloseTimeout}` have no counterpart for simple function calls. Call the function directly or wrap it in `ctx.Run` for durability.

7. **Register the workflow.** Use `resonate.Register(r, "sleepingWorkflow", sleepingWorkflow)` in place of Temporal's worker task-queue registration. The returned handle exposes `.Run` for invocation.

8. **Use a stable promise ID.** The ID passed to `sleepFn.Run(ctx, id, args)` is your idempotency key. Re-using the same ID after a crash re-attaches to the existing workflow rather than starting a fresh one. In this example, pass `-id=<value>` on the command line (default `durable-sleep-1`) — run the worker once, kill it, then run it again with the same `-id` to resume from the server-side timer checkpoint.

## What's different (and why)

**Shape of the two programs.** `sleep-for-days` is a loop that runs indefinitely, doing work each iteration and waiting for either a timer or an external signal. This example is a single-sleep workflow that demonstrates the timer primitive in isolation. If you are porting the full loop-until-signalled shape, you need both `ctx.Sleep` (for the timer leg) and a latent promise with `ctx.Promise` (for the signal leg).

**Selector vs. Await.** Temporal's `Selector` is a general-purpose racing primitive — it can race any mix of futures, signals, and cancellations. Resonate does not have a `Selector` equivalent in the Go SDK at this time. For the common single-timer case `f.Await` is sufficient. For racing a timer against an external event, the recommended approach is to structure the code so that the external event resolves a promise that the workflow is already awaiting, rather than racing two independent branches.

**No activity/workflow split.** Temporal distinguishes workflow code (deterministic, replay-safe) from activity code (arbitrary side effects, run by activity workers). This is a deliberate engineering tradeoff: the separate sandbox enforces determinism and provides fine-grained retry and timeout control per activity. Resonate takes a different approach — a plain Go function made durable by `ctx.Run` serves both roles. There is no `@workflow` / `@activity` annotation and no separate activity-worker process; timeout and retry policy are optionally supplied per `ctx.Run` call via `RunOpts`.

**Localnet mode.** This example ships with a `localnet` mode — an in-process transport that needs no external server, useful for exploring the API. Localnet state lives in process memory, so a process crash also loses the timer. The crash-recovery story requires a real Resonate server (`resonate dev`). Temporal does not have an equivalent in-process mode; you always need a server.

**Promise ID as idempotency key.** In Temporal, re-attaching to a running workflow after a crash happens implicitly via the workflow's run ID and the task queue. In Resonate you control the promise ID explicitly. Passing the same ID to `Run` a second time re-attaches to the existing workflow; Resonate deduplicates on that ID server-side.

**Duration encoding in args.** `time.Duration` is an `int64` nanosecond count that JSON-marshals as a bare integer — readable in code but opaque in stored promise payloads. This example passes duration as an explicit `int64` seconds field and converts at the call site. This is the idiomatic workaround until the SDK provides a codec-friendly duration type.

## Notes & coverage

**Bare timer reference.** If you arrived from [`samples-go/timer`](https://github.com/temporalio/samples-go/tree/main/timer) rather than `sleep-for-days`, the mapping is more direct: `timer`'s `workflow.NewTimer(childCtx, threshold)` races a single timer against an activity future. The timer leg maps cleanly to `ctx.Sleep(d)` + `f.Await(nil)`.

**Loop + signal.** Porting the full `sleep-for-days` loop requires the human-in-the-loop pattern for the signal/completion half. In the Go SDK, resolving a promise from outside the workflow is currently lower-level than in other SDKs — call `r.Sender().PromiseSettle(ctx, PromiseSettleReq{ID: id, State: resonate.SettleStateResolved, Value: v})` where `v` is built with `resonate.NewValue(yourResult)` (which stores raw JSON in `Value.Data`). Base64 encoding is an internal wire-layer detail handled by the codec; caller code works with plain Go values via `NewValue`. Alternatively use the `resonate promises resolve` CLI. See [resonate-sdk-go#28](https://github.com/resonatehq/resonate-sdk-go/issues/28) for tracking.

**Replay visibility.** The README notes that the `fmt.Printf` before `ctx.Sleep` prints twice on a localnet run. That is replay: when the timer fires, the runtime re-enters the workflow body from the top. Side effects before a `Sleep` or `ctx.Run` checkpoint will re-execute on replay unless wrapped in their own `ctx.Run` call.

## Further reading

- Concept-level guide (all SDKs): https://docs.resonatehq.io/evaluate/coming-from/temporal
- Temporal sample: https://github.com/temporalio/samples-go/tree/main/sleep-for-days
- Simpler bare-timer reference: https://github.com/temporalio/samples-go/tree/main/timer
- This example's README
