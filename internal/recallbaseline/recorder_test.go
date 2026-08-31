package recallbaseline

import (
	"testing"
	"time"

	"github.com/yersonargotev/engram/internal/protocolcontract"
)

func TestAsyncRecorderDrainsWithoutDelayingTheObservedOperation(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	ledger, err := Open(Config{DataDir: dataDir})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	recorder := NewAsyncRecorder(ledger, 4)
	if accepted := recorder.Record(Event{
		Kind: EventOperation, Surface: SurfaceMCP, Operation: "mem_search", Outcome: OutcomeSuccess,
		Latency: KnownLatency(time.Millisecond), DeliveredUTF8Bytes: KnownBytes(3),
	}); !accepted {
		t.Fatal("Record() rejected an event with available capacity")
	}
	if err := recorder.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	ledger, err = Open(Config{DataDir: dataDir})
	if err != nil {
		t.Fatalf("reopen ledger: %v", err)
	}
	defer ledger.Close()
	report, err := ledger.Report(protocolcontract.CompatibilityReport{})
	if err != nil {
		t.Fatalf("Report() error = %v", err)
	}
	if len(report.Operations) != 1 || report.Operations[0].Events != 1 {
		t.Fatalf("drained operations = %+v", report.Operations)
	}
}

func TestAsyncRecorderReportsNonBlockingQueueDrops(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	ledger, err := Open(Config{DataDir: dataDir})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	recorder := &AsyncRecorder{
		ledger: ledger,
		events: make(chan Event, 1),
		done:   make(chan struct{}),
	}
	recorder.events <- Event{Kind: EventOperation, Surface: SurfaceMCP, Operation: "mem_search", Outcome: OutcomeSuccess}
	if accepted := recorder.Record(Event{Kind: EventOperation, Surface: SurfaceMCP, Operation: "mem_context", Outcome: OutcomeSuccess}); accepted {
		t.Fatal("Record() accepted an event after the non-blocking queue was full")
	}
	go recorder.run()
	if err := recorder.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := recorder.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}

	ledger, err = Open(Config{DataDir: dataDir})
	if err != nil {
		t.Fatalf("reopen ledger: %v", err)
	}
	defer ledger.Close()
	report, err := ledger.Report(protocolcontract.CompatibilityReport{})
	if err != nil {
		t.Fatalf("Report() error = %v", err)
	}
	if report.Collection.DroppedEvents != 1 || report.Collection.WriteFailures != 0 {
		t.Fatalf("collection report = %+v", report.Collection)
	}
}
