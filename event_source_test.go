package cqrs

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	cmdTypeCreate  = CommandType("create")
	cmdTypeNoop    = CommandType("noop")
	qryTypeGet     = QueryType("get")
	evtTypeCreated = EventType("created")
)

type createCmd struct{ ID int }

func (createCmd) Type() CommandType { return cmdTypeCreate }

type noopCmd struct{}

func (noopCmd) Type() CommandType { return cmdTypeNoop }

type getQry struct{ ID int }

func (getQry) Type() QueryType { return qryTypeGet }

type createdEvt struct{ ID int }

func (createdEvt) Type() EventType { return evtTypeCreated }

// ---------------------------------------------------------------------------
// Race-tests: HandleEvent vs EventStream vs Close
// ---------------------------------------------------------------------------

func TestEventSource_Race_HandleEventAndStream(t *testing.T) {
	es := &eventSource{streams: make(map[chan Event]context.CancelFunc)}
	defer es.Close()

	var wg sync.WaitGroup
	ctx := context.Background()
	stop := make(chan struct{})
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					es.HandleEvent(ctx, createdEvt{})
				}
			}
		}()
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					stream, err := es.EventStream(ctx)
					if err != nil {
						return
					}
					go func() {
						for range stream {
						}
					}()
				}
			}
		}()
	}
	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
}

func TestEventSource_Race_HandleEventAndClose(t *testing.T) {
	es := &eventSource{streams: make(map[chan Event]context.CancelFunc)}
	var wg sync.WaitGroup
	ctx := context.Background()
	var handled atomic.Int64
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				es.HandleEvent(ctx, createdEvt{})
				handled.Add(1)
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(5 * time.Millisecond)
		es.Close()
	}()
	wg.Wait()
	es.HandleEvent(ctx, createdEvt{})
	if err := es.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestEventSource_Race_EventStreamAfterClose(t *testing.T) {
	es := &eventSource{streams: make(map[chan Event]context.CancelFunc)}
	es.Close()

	_, err := es.EventStream(context.Background())
	if err != io.ErrClosedPipe {
		t.Fatalf("got %v, want io.ErrClosedPipe", err)
	}
}

func TestEventSource_ContextCancelClosesStream(t *testing.T) {
	es := &eventSource{streams: make(map[chan Event]context.CancelFunc)}
	defer es.Close()

	ctx, cancel := context.WithCancel(context.Background())
	stream, err := es.EventStream(ctx)
	if err != nil {
		t.Fatalf("EventStream: %v", err)
	}

	cancel()

	select {
	case _, ok := <-stream:
		if ok {
			t.Fatal("stream should be closed after context cancel")
		}
	case <-time.After(time.Second):
		t.Fatal("stream not closed after context cancel")
	}
}

func TestEventSource_SlowConsumerDetached(t *testing.T) {
	es := &eventSource{streams: make(map[chan Event]context.CancelFunc)}
	defer es.Close()
	ctx := context.Background()
	stream, err := es.EventStream(ctx)
	if err != nil {
		t.Fatalf("EventStream: %v", err)
	}
	for i := 0; i < 100; i++ {
		es.HandleEvent(ctx, createdEvt{})
	}
	deadline := time.After(2 * time.Second)
	for {
		select {
		case _, ok := <-stream:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("slow consumer was not detached")
		}
	}
}

func TestEventSource_CloseWaitsForInflight(t *testing.T) {
	es := &eventSource{streams: make(map[chan Event]context.CancelFunc)}
	ctx := context.Background()
	started := make(chan struct{})
	release := make(chan struct{})
	stream, _ := es.EventStream(ctx)
	go func() {
		for range stream {
			close(started)
			<-release
		}
	}()
	go es.HandleEvent(ctx, createdEvt{})
	<-started
	done := make(chan error, 1)
	go func() { done <- es.Close() }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close hung")
	}
	close(release)
}

func BenchmarkEventSource_HandleEvent(b *testing.B) {
	es := &eventSource{streams: make(map[chan Event]context.CancelFunc)}
	defer es.Close()

	ctx := context.Background()
	e := createdEvt{}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		es.HandleEvent(ctx, e)
	}
}

func BenchmarkEventSource_HandleEvent_OneSubscriber(b *testing.B) {
	es := &eventSource{streams: make(map[chan Event]context.CancelFunc)}
	defer es.Close()

	ctx := context.Background()
	stream, _ := es.EventStream(ctx)
	go func() {
		for range stream {
		}
	}()

	e := createdEvt{}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		es.HandleEvent(ctx, e)
	}
}

func BenchmarkEventSource_HandleEvent_Parallel(b *testing.B) {
	es := &eventSource{streams: make(map[chan Event]context.CancelFunc)}
	defer es.Close()

	ctx := context.Background()
	e := createdEvt{}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			es.HandleEvent(ctx, e)
		}
	})
}
