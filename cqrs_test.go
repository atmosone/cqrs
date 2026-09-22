// Package cqrs_test contains AI-generated domain, race, benchmark tests
// for [github.com/atmosone/cqrs] toolkit.
// Note: AI generated content!
package cqrs_test

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/atmosone/cqrs"
)

// ---------------------------------------------------------------------------
// Domain test
// ---------------------------------------------------------------------------

const (
	cmdTypeCreate  = cqrs.CommandType("create")
	cmdTypeNoop    = cqrs.CommandType("noop")
	qryTypeGet     = cqrs.QueryType("get")
	evtTypeCreated = cqrs.EventType("created")
)

type createCmd struct{ ID int }

func (createCmd) Type() cqrs.CommandType { return cmdTypeCreate }

type noopCmd struct{}

func (noopCmd) Type() cqrs.CommandType { return cmdTypeNoop }

type getQry struct{ ID int }

func (getQry) Type() cqrs.QueryType { return qryTypeGet }

type createdEvt struct{ ID int }

func (createdEvt) Type() cqrs.EventType { return evtTypeCreated }

func TestCommandRouting(t *testing.T) {
	app := cqrs.New()
	defer app.Close()
	var got int
	app.OnCommand(cmdTypeCreate, func(ctx context.Context, c cqrs.Command) ([]cqrs.Event, error) {
		got = c.(createCmd).ID
		return nil, nil
	})
	if err := app.HandleCommand(context.Background(), createCmd{ID: 42}); err != nil {
		t.Fatalf("HandleCommand: %v", err)
	}
	if got != 42 {
		t.Fatalf("handler got %d, want 42", got)
	}
}

func TestUnimplementedCommand(t *testing.T) {
	app := cqrs.New()
	defer app.Close()
	err := app.HandleCommand(context.Background(), noopCmd{})
	if !errors.Is(err, cqrs.ErrUnimplementedHandler) {
		t.Fatalf("got %v, want ErrUnimplementedHandler", err)
	}
}

func TestCommandAfterClose(t *testing.T) {
	app := cqrs.New()
	app.OnCommand(cmdTypeCreate, func(ctx context.Context, c cqrs.Command) ([]cqrs.Event, error) {
		return nil, nil
	})
	if err := app.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	err := app.HandleCommand(context.Background(), createCmd{})
	if !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("got %v, want io.ErrClosedPipe", err)
	}
}

func TestCloseIdempotent(t *testing.T) {
	app := cqrs.New()
	for i := 0; i < 3; i++ {
		if err := app.Close(); err != nil {
			t.Fatalf("Close #%d: %v", i, err)
		}
	}
}

func TestQueryRouting(t *testing.T) {
	app := cqrs.New()
	defer app.Close()
	app.OnQuery(qryTypeGet, func(ctx context.Context, q cqrs.Query) (cqrs.Result, error) {
		return q.(getQry).ID * 2, nil
	})
	res, err := app.HandleQuery(context.Background(), getQry{ID: 21})
	if err != nil {
		t.Fatalf("HandleQuery: %v", err)
	}
	if res.(int) != 42 {
		t.Fatalf("got %v, want 42", res)
	}
}

func TestQueryPanicRecovered(t *testing.T) {
	app := cqrs.New()
	defer app.Close()
	app.OnQuery(qryTypeGet, func(ctx context.Context, q cqrs.Query) (cqrs.Result, error) {
		panic("boom")
	})
	res, err := app.HandleQuery(context.Background(), getQry{})
	if err != nil {
		t.Fatalf("panic should be recovered, got err: %v", err)
	}
	if res != nil {
		t.Fatalf("want nil result, got %v", res)
	}
}

func TestEventFanout(t *testing.T) {
	app := cqrs.New()
	defer app.Close()
	var count atomic.Int32
	for i := 0; i < 5; i++ {
		app.OnEvent(evtTypeCreated, func(ctx context.Context, e cqrs.Event) {
			count.Add(1)
		})
	}
	app.HandleEvent(context.Background(), createdEvt{ID: 1})
	if got := count.Load(); got != 5 {
		t.Fatalf("got %d handler calls, want 5", got)
	}
}

func TestCommandEmitsEvents(t *testing.T) {
	app := cqrs.New()
	defer app.Close()
	var busCount, streamCount atomic.Int32
	app.OnEvent(evtTypeCreated, func(ctx context.Context, e cqrs.Event) {
		busCount.Add(1)
	})
	stream, err := app.EventStream(context.Background())
	if err != nil {
		t.Fatalf("EventStream: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range stream {
			streamCount.Add(1)
		}
	}()
	app.OnCommand(cmdTypeCreate, func(ctx context.Context, c cqrs.Command) ([]cqrs.Event, error) {
		return []cqrs.Event{createdEvt{ID: 1}, createdEvt{ID: 2}}, nil
	})
	if err := app.HandleCommand(context.Background(), createCmd{}); err != nil {
		t.Fatalf("HandleCommand: %v", err)
	}
	deadline := time.After(time.Second)
	for streamCount.Load() < 2 {
		select {
		case <-deadline:
			t.Fatalf("timeout: got %d stream events, want 2", streamCount.Load())
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if err := app.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	<-done
	if got := busCount.Load(); got != 2 {
		t.Fatalf("bus got %d, want 2", got)
	}
}

func TestProjection(t *testing.T) {
	app := cqrs.New()
	defer app.Close()
	var got atomic.Int32
	p := &testProjection{count: &got}
	app.Build(p)
	app.HandleEvent(context.Background(), createdEvt{ID: 1})
	if got.Load() != 1 {
		t.Fatalf("projection got %d, want 1", got.Load())
	}
}

type testProjection struct{ count *atomic.Int32 }

func (p *testProjection) Types() []cqrs.EventType { return []cqrs.EventType{evtTypeCreated} }
func (p *testProjection) HandleEvent(ctx context.Context, e cqrs.Event) {
	p.count.Add(1)
}

func TestEventStreamAfterClose(t *testing.T) {
	app := cqrs.New()
	app.Close()
	_, err := app.EventStream(context.Background())
	if !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("got %v, want io.ErrClosedPipe", err)
	}
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

func BenchmarkHandleCommand(b *testing.B) {
	app := cqrs.New()
	defer app.Close()
	app.OnCommand(cmdTypeNoop, func(ctx context.Context, c cqrs.Command) ([]cqrs.Event, error) {
		return nil, nil
	})
	cmd := noopCmd{}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := app.HandleCommand(ctx, cmd); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkHandleCommandWithEvent(b *testing.B) {
	app := cqrs.New()
	defer app.Close()
	app.OnCommand(cmdTypeCreate, func(ctx context.Context, c cqrs.Command) ([]cqrs.Event, error) {
		return []cqrs.Event{createdEvt{ID: 1}}, nil
	})
	cmd := createCmd{ID: 1}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := app.HandleCommand(ctx, cmd); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkHandleQuery(b *testing.B) {
	app := cqrs.New()
	defer app.Close()
	app.OnQuery(qryTypeGet, func(ctx context.Context, q cqrs.Query) (cqrs.Result, error) {
		return 42, nil
	})
	q := getQry{}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := app.HandleQuery(ctx, q); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkHandleEvent_NoHandlers(b *testing.B) {
	app := cqrs.New()
	defer app.Close()
	e := createdEvt{}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		app.HandleEvent(ctx, e)
	}
}

func BenchmarkHandleEvent_OneHandler(b *testing.B) {
	app := cqrs.New()
	defer app.Close()
	app.OnEvent(evtTypeCreated, func(ctx context.Context, e cqrs.Event) {})
	e := createdEvt{}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		app.HandleEvent(ctx, e)
	}
}

func BenchmarkEventStreamBroadcast(b *testing.B) {
	app := cqrs.New()
	defer app.Close()
	stream, err := app.EventStream(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		for range stream {
		}
		close(done)
	}()
	e := createdEvt{}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		app.HandleEvent(ctx, e)
	}
	b.StopTimer()
	app.Close()
	<-done
}

func BenchmarkEventStreamOpenClose(b *testing.B) {
	app := cqrs.New()
	defer app.Close()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stream, err := app.EventStream(ctx)
		if err != nil {
			b.Fatal(err)
		}
		_ = stream
	}
}

// ---------------------------------------------------------------------------
//Parallel Benchamrks (checks contention on RWMutex)
// ---------------------------------------------------------------------------

func BenchmarkHandleCommand_Parallel(b *testing.B) {
	app := cqrs.New()
	defer app.Close()

	app.OnCommand(cmdTypeNoop, func(ctx context.Context, c cqrs.Command) ([]cqrs.Event, error) {
		return nil, nil
	})

	cmd := noopCmd{}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = app.HandleCommand(ctx, cmd)
		}
	})
}

func BenchmarkEventStreamBroadcast_Parallel(b *testing.B) {
	app := cqrs.New()
	defer app.Close()
	stream, err := app.EventStream(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		for range stream {
		}
		close(done)
	}()
	e := createdEvt{}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			app.HandleEvent(ctx, e)
		}
	})
	b.StopTimer()
	app.Close()
	<-done
}

func TestHandleCommand_ZeroAllocs(t *testing.T) {
	app := cqrs.New()
	defer app.Close()
	app.OnCommand(cmdTypeNoop, func(ctx context.Context, c cqrs.Command) ([]cqrs.Event, error) {
		return nil, nil
	})
	ctx := context.Background()
	cmd := noopCmd{}
	allocs := testing.AllocsPerRun(1000, func() {
		_ = app.HandleCommand(ctx, cmd)
	})
	if allocs != 0 {
		t.Fatalf("got %v allocs/op, want 0", allocs)
	}
}
