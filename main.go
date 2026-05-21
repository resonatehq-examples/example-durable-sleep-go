// Package main demonstrates durable sleep with the Resonate Go SDK.
//
// A workflow calls [resonate.Context.Sleep] to suspend for a requested
// duration. The suspension is backed by a server-side timer promise: if the
// worker crashes while the timer is pending, another worker (or this one after
// restart) resumes the workflow when the timer fires rather than starting over.
//
// # Modes
//
// By default the program uses localnet — an in-process transport that needs no
// external server. Localnet is convenient for local development and testing, but
// its server state lives in process memory, so a process crash also destroys the
// timer. To demonstrate true crash recovery, start a Resonate dev server and
// point the binary at it:
//
//	resonate dev                       # terminal 1
//	go run . -url=http://localhost:8001 # terminal 2
//
// Kill the worker mid-sleep (Ctrl-C), then run the same command again with the
// same promise ID. The workflow resumes from the server-side timer instead of
// restarting.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	resonate "github.com/resonatehq/resonate-sdk-go"
	"github.com/resonatehq/resonate-sdk-go/localnet"
)

// SleepArgs carries the sleep duration as whole seconds. time.Duration is an
// int64 under the hood and round-trips through JSON as a bare nanosecond count,
// which is opaque in promise payloads. Using an explicit seconds field keeps
// stored promise data readable.
type SleepArgs struct {
	Secs int64 `json:"secs"`
}

// sleepingWorkflow suspends for args.Secs seconds via a durable timer promise,
// then returns a confirmation string. The [resonate.Context.Sleep] call creates
// the timer promise on the server; [Future.Await] parks the workflow goroutine
// until the promise resolves. In the durable execution model, parking is
// implemented by signaling the runtime — another worker can pick up where this
// one left off if this process is killed while the timer is outstanding.
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

func main() {
	serverURL := flag.String("url", "", "Resonate server URL (e.g. http://localhost:8001). Omit to use localnet.")
	sleepSecs := flag.Int64("secs", 3, "How many seconds to sleep.")
	flag.Parse()

	var cfg resonate.Config

	if *serverURL != "" {
		// Real-server mode: crash recovery is fully demonstrable here.
		cfg = resonate.Config{URL: *serverURL}
		fmt.Printf("[main] connecting to server at %s\n", *serverURL)
	} else {
		// Localnet mode: in-process transport, no external server needed.
		// NoopHeartbeat is required — localnet has no HTTP endpoint for the
		// default AsyncHeartbeat to reach.
		pid := "durable-sleep-worker"
		cfg = resonate.Config{
			Network:   localnet.NewLocal("default", &pid),
			Heartbeat: resonate.NoopHeartbeat{},
			TTL:       5 * time.Minute,
		}
		fmt.Println("[main] using localnet (in-process, no external server required)")
		fmt.Println("[main] note: localnet state is ephemeral — crash recovery requires -url=<server>")
	}

	r, err := resonate.New(cfg)
	if err != nil {
		log.Fatalf("resonate.New: %v", err)
	}
	defer func() { _ = r.Stop() }()

	sleepFn, err := resonate.Register(r, "sleepingWorkflow", sleepingWorkflow)
	if err != nil {
		log.Fatalf("Register: %v", err)
	}

	ctx := context.Background()
	// A stable, unique ID per invocation. Resonate deduplicates on this ID, so
	// re-running with the same ID after a crash resumes the existing workflow
	// rather than starting a new one.
	id := fmt.Sprintf("durable-sleep-%d", time.Now().UnixNano())
	args := SleepArgs{Secs: *sleepSecs}

	fmt.Printf("[main] invoking workflow id=%s secs=%d\n", id, args.Secs)

	h, err := sleepFn.Run(ctx, id, args)
	if err != nil {
		log.Fatalf("Run: %v", err)
	}

	result, err := h.Result(ctx)
	if err != nil {
		log.Fatalf("Result: %v", err)
	}

	fmt.Printf("[main] OK: %s\n", result)
}
