package recallbaseline

import (
	"errors"
	"fmt"
	"sync"
)

// AsyncRecorder keeps operational telemetry off the observed request path.
// A full queue rejects the event rather than delaying the caller.
type AsyncRecorder struct {
	ledger *Ledger
	events chan Event
	done   chan struct{}

	mu        sync.Mutex
	closed    bool
	writeErr  error
	dropped   int64
	writes    int64
	closeOnce sync.Once
	closeErr  error
}

func NewAsyncRecorder(ledger *Ledger, capacity int) *AsyncRecorder {
	if capacity < 1 {
		capacity = 1
	}
	recorder := &AsyncRecorder{
		ledger: ledger,
		events: make(chan Event, capacity),
		done:   make(chan struct{}),
	}
	go recorder.run()
	return recorder
}

func (recorder *AsyncRecorder) Record(event Event) bool {
	if recorder == nil {
		return false
	}
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if recorder.closed {
		return false
	}
	select {
	case recorder.events <- event:
		return true
	default:
		recorder.dropped++
		return false
	}
}

func (recorder *AsyncRecorder) run() {
	defer close(recorder.done)
	for event := range recorder.events {
		if err := recorder.ledger.Record(event); err != nil {
			recorder.mu.Lock()
			recorder.writes++
			if recorder.writeErr == nil {
				recorder.writeErr = err
			}
			recorder.mu.Unlock()
		}
	}
}

func (recorder *AsyncRecorder) Close() error {
	if recorder == nil {
		return nil
	}
	recorder.closeOnce.Do(func() {
		recorder.mu.Lock()
		recorder.closed = true
		close(recorder.events)
		recorder.mu.Unlock()
		<-recorder.done

		recorder.mu.Lock()
		writeErr := recorder.writeErr
		dropped := recorder.dropped
		writes := recorder.writes
		recorder.mu.Unlock()
		lossErr := recorder.ledger.RecordCollectionLoss(dropped, writes)
		closeErr := recorder.ledger.Close()
		if writeErr != nil {
			writeErr = fmt.Errorf("record recall baseline event asynchronously: %w", writeErr)
		}
		recorder.closeErr = errors.Join(writeErr, lossErr, closeErr)
	})
	return recorder.closeErr
}
