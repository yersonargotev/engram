package cloudstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yersonargotev/engram/internal/cloud/chunkcodec"
)

const mutationPaginationDriverName = "cloudstore-mutation-pagination-driver"

var mutationPaginationQueries = struct {
	sync.Mutex
	byScenario map[string]int
}{byScenario: make(map[string]int)}

func init() {
	sql.Register(mutationPaginationDriverName, mutationPaginationDriver{})
}

type mutationPaginationDriver struct{}

func (mutationPaginationDriver) Open(scenario string) (driver.Conn, error) {
	return mutationPaginationConn{scenario: scenario}, nil
}

type mutationPaginationConn struct {
	scenario string
}

func (mutationPaginationConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare is not supported")
}

func (mutationPaginationConn) Close() error { return nil }

func (mutationPaginationConn) Begin() (driver.Tx, error) {
	return nil, errors.New("transactions are not supported")
}

func (mutationPaginationConn) CheckNamedValue(*driver.NamedValue) error { return nil }

func (c mutationPaginationConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	mutationPaginationQueries.Lock()
	mutationPaginationQueries.byScenario[c.scenario]++
	mutationPaginationQueries.Unlock()

	source := mutationPaginationSource(c.scenario)
	allowedProjects := namedAllowedProjects(args)
	if strings.Contains(strings.ToLower(query), "max(seq)") {
		latestSeq := int64(0)
		for _, row := range source {
			if projectAllowed(row.project, allowedProjects) && row.seq > latestSeq {
				latestSeq = row.seq
			}
		}
		return &mutationPaginationRows{
			columns: []string{"latest_seq"},
			rows:    [][]driver.Value{{latestSeq}},
		}, nil
	}

	numericArgs := namedInt64Args(args)
	if len(numericArgs) < 2 {
		return nil, fmt.Errorf("expected pagination arguments, got %+v", args)
	}
	sinceSeq := numericArgs[0]
	horizon := int64(^uint64(0) >> 1)
	if len(numericArgs) >= 3 {
		horizon = numericArgs[1]
	}
	limit := numericArgs[len(numericArgs)-1]
	rows := make([][]driver.Value, 0, limit)
	for _, row := range source {
		if row.seq <= sinceSeq || row.seq > horizon || !projectAllowed(row.project, allowedProjects) {
			continue
		}
		rows = append(rows, []driver.Value{
			row.seq,
			row.project,
			row.entity,
			fmt.Sprintf("entity-%03d", row.seq),
			"upsert",
			`{}`,
			time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC).Add(time.Duration(row.seq) * time.Second),
		})
		if int64(len(rows)) == limit {
			break
		}
	}
	return &mutationPaginationRows{
		columns: []string{"seq", "project", "entity", "entity_key", "op", "payload", "occurred_at"},
		rows:    rows,
	}, nil
}

func namedInt64Args(args []driver.NamedValue) []int64 {
	values := make([]int64, 0, len(args))
	for _, arg := range args {
		switch value := arg.Value.(type) {
		case int:
			values = append(values, int64(value))
		case int64:
			values = append(values, value)
		}
	}
	return values
}

func namedAllowedProjects(args []driver.NamedValue) []string {
	for _, arg := range args {
		if projects, ok := arg.Value.([]string); ok {
			return projects
		}
	}
	return nil
}

func projectAllowed(project string, allowedProjects []string) bool {
	if allowedProjects == nil {
		return true
	}
	for _, allowed := range allowedProjects {
		if project == allowed {
			return true
		}
	}
	return false
}

type mutationPaginationSourceRow struct {
	seq     int64
	project string
	entity  string
}

func mutationPaginationSource(scenario string) []mutationPaginationSourceRow {
	const project = "project-a"
	localEntities := []string{"\u00a0PrOmPt\u00a0", "\u00a0DiAgNoStIc_CaPtUrE\u00a0", "\u00a0CaPtUrE_CoNsEnT\u00a0"}
	local := func(seq int64) mutationPaginationSourceRow {
		return mutationPaginationSourceRow{seq: seq, project: project, entity: localEntities[(seq-1)%int64(len(localEntities))]}
	}
	ordinary := func(seq int64) mutationPaginationSourceRow {
		return mutationPaginationSourceRow{seq: seq, project: project, entity: "observation"}
	}

	switch scenario {
	case "bounded":
		rows := make([]mutationPaginationSourceRow, 0, 351)
		for seq := int64(1); seq <= 350; seq++ {
			rows = append(rows, local(seq))
		}
		return append(rows, ordinary(351))
	case "beyond":
		rows := make([]mutationPaginationSourceRow, 0, 50)
		for seq := int64(1); seq <= 50; seq++ {
			rows = append(rows, ordinary(seq))
		}
		return rows
	case "visible":
		rows := []mutationPaginationSourceRow{ordinary(1)}
		for seq := int64(2); seq <= 102; seq++ {
			rows = append(rows, local(seq))
		}
		rows = append(rows, ordinary(103))
		for seq := int64(104); seq <= 204; seq++ {
			rows = append(rows, local(seq))
		}
		return rows
	case "visible-lookahead":
		rows := []mutationPaginationSourceRow{ordinary(1)}
		for seq := int64(2); seq <= 100; seq++ {
			rows = append(rows, local(seq))
		}
		return append(rows, ordinary(101), local(102))
	case "consecutive-visible":
		return []mutationPaginationSourceRow{ordinary(1), ordinary(2), local(3)}
	case "hidden-before-visible":
		rows := make([]mutationPaginationSourceRow, 0, 52)
		for seq := int64(1); seq <= 50; seq++ {
			rows = append(rows, local(seq))
		}
		return append(rows, ordinary(51), ordinary(52))
	default:
		return nil
	}
}

type mutationPaginationRows struct {
	columns []string
	rows    [][]driver.Value
	index   int
}

func (r *mutationPaginationRows) Columns() []string { return r.columns }

func (*mutationPaginationRows) Close() error { return nil }

func (r *mutationPaginationRows) Next(dest []driver.Value) error {
	if r.index >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.index])
	r.index++
	return nil
}

func openMutationPaginationStore(t *testing.T, scenario string) *CloudStore {
	t.Helper()
	db, err := sql.Open(mutationPaginationDriverName, scenario)
	if err != nil {
		t.Fatalf("open pagination database: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return &CloudStore{db: db}
}

func paginationQueryCount(scenario string) int {
	mutationPaginationQueries.Lock()
	defer mutationPaginationQueries.Unlock()
	return mutationPaginationQueries.byScenario[scenario]
}

func TestListMutationsSinceSafeCursorAdjacentVisibleShapes(t *testing.T) {
	for _, tt := range []struct {
		name             string
		scenario         string
		firstVisibleSeq  int64
		continuationSeq  int64
		secondVisibleSeq int64
		terminalSeq      int64
	}{
		{name: "consecutive visible rows", scenario: "consecutive-visible", firstVisibleSeq: 1, continuationSeq: 1, secondVisibleSeq: 2, terminalSeq: 3},
		{name: "hidden rows before first visible", scenario: "hidden-before-visible", firstVisibleSeq: 51, continuationSeq: 51, secondVisibleSeq: 52, terminalSeq: 52},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cs := openMutationPaginationStore(t, tt.scenario)
			first, hasMore, continuationSeq, err := cs.ListMutationsSince(context.Background(), 0, 1, []string{"project-a"})
			if err != nil {
				t.Fatalf("first page: %v", err)
			}
			assertOrdinaryMutationPage(t, first, tt.firstVisibleSeq)
			if !hasMore || continuationSeq != tt.continuationSeq {
				t.Fatalf("first continuation = (%v, %d), want (true, %d)", hasMore, continuationSeq, tt.continuationSeq)
			}

			second, hasMore, latestSeq, err := cs.ListMutationsSince(context.Background(), continuationSeq, 1, []string{"project-a"})
			if err != nil {
				t.Fatalf("second page: %v", err)
			}
			assertOrdinaryMutationPage(t, second, tt.secondVisibleSeq)
			if hasMore || latestSeq != tt.terminalSeq {
				t.Fatalf("second page terminal = (%v, %d), want (false, %d)", hasMore, latestSeq, tt.terminalSeq)
			}
		})
	}
}

func TestListMutationsSinceAdvancesPastHiddenRowsBeforeVisibleLookahead(t *testing.T) {
	for _, tt := range []struct {
		name            string
		allowedProjects []string
	}{
		{name: "without project filter"},
		{name: "with project filter", allowedProjects: []string{"project-a"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cs := openMutationPaginationStore(t, "visible-lookahead")
			first, hasMore, continuationSeq, err := cs.ListMutationsSince(context.Background(), 0, 1, tt.allowedProjects)
			if err != nil {
				t.Fatalf("first page: %v", err)
			}
			assertOrdinaryMutationPage(t, first, 1)
			if !hasMore || continuationSeq != 100 {
				t.Fatalf("first continuation = (%v, %d), want (true, 100) past hidden seq2..100 without skipping visible seq101", hasMore, continuationSeq)
			}

			second, hasMore, latestSeq, err := cs.ListMutationsSince(context.Background(), continuationSeq, 1, tt.allowedProjects)
			if err != nil {
				t.Fatalf("second page: %v", err)
			}
			assertOrdinaryMutationPage(t, second, 101)
			if hasMore || latestSeq != 102 {
				t.Fatalf("second page terminal = (%v, %d), want (false, authoritative 102)", hasMore, latestSeq)
			}
		})
	}
}

func TestListMutationsSinceBoundsRawWorkAndContinuesAllLocalHistory(t *testing.T) {
	for _, tt := range []struct {
		name            string
		allowedProjects []string
	}{
		{name: "without project filter"},
		{name: "with project filter", allowedProjects: []string{"project-a"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cs := openMutationPaginationStore(t, "bounded")
			cursor := int64(0)
			for page, wantCursor := range []int64{101, 202} {
				before := paginationQueryCount("bounded")
				mutations, hasMore, latestSeq, err := cs.ListMutationsSince(context.Background(), cursor, 100, tt.allowedProjects)
				if err != nil {
					t.Fatalf("page %d: ListMutationsSince: %v", page+1, err)
				}
				if len(mutations) != 0 || !hasMore || latestSeq != wantCursor {
					t.Fatalf("page %d = (%+v, %v, %d), want empty continuation cursor %d", page+1, mutations, hasMore, latestSeq, wantCursor)
				}
				if queries := paginationQueryCount("bounded") - before; queries != 2 {
					t.Fatalf("page %d used %d queries, want fixed budget 2", page+1, queries)
				}
				cursor = latestSeq
			}
		})
	}
}

func TestListMutationsSinceReturnsAuthoritativeLatestBelowSince(t *testing.T) {
	for _, allowedProjects := range [][]string{nil, []string{"project-a"}} {
		cs := openMutationPaginationStore(t, "beyond")
		before := paginationQueryCount("beyond")
		mutations, hasMore, latestSeq, err := cs.ListMutationsSince(context.Background(), 100, 100, allowedProjects)
		if err != nil {
			t.Fatalf("ListMutationsSince: %v", err)
		}
		if len(mutations) != 0 || hasMore || latestSeq != 50 {
			t.Fatalf("result = (%+v, %v, %d), want empty terminal latest_seq 50", mutations, hasMore, latestSeq)
		}
		if queries := paginationQueryCount("beyond") - before; queries != 1 {
			t.Fatalf("beyond-latest request used %d queries, want authoritative max query only", queries)
		}
	}
}

func TestListMutationsSincePreservesVisibleRowsAroundCappedLocalTails(t *testing.T) {
	for _, tt := range []struct {
		name            string
		allowedProjects []string
	}{
		{name: "without project filter"},
		{name: "with project filter", allowedProjects: []string{"project-a"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cs := openMutationPaginationStore(t, "visible")
			first, firstHasMore, firstSeq, err := cs.ListMutationsSince(context.Background(), 0, 1, tt.allowedProjects)
			if err != nil {
				t.Fatalf("first page: %v", err)
			}
			assertOrdinaryMutationPage(t, first, 1)
			if !firstHasMore || firstSeq != 101 {
				t.Fatalf("first page continuation = (%v, %d), want (true, 101)", firstHasMore, firstSeq)
			}

			second, secondHasMore, secondSeq, err := cs.ListMutationsSince(context.Background(), firstSeq, 1, tt.allowedProjects)
			if err != nil {
				t.Fatalf("second page: %v", err)
			}
			assertOrdinaryMutationPage(t, second, 103)
			if !secondHasMore || secondSeq != 202 {
				t.Fatalf("second page continuation = (%v, %d), want (true, 202)", secondHasMore, secondSeq)
			}

			terminal, terminalHasMore, terminalSeq, err := cs.ListMutationsSince(context.Background(), secondSeq, 1, tt.allowedProjects)
			if err != nil {
				t.Fatalf("terminal page: %v", err)
			}
			if len(terminal) != 0 || terminalHasMore || terminalSeq != 204 {
				t.Fatalf("terminal page = (%+v, %v, %d), want empty authoritative latest 204", terminal, terminalHasMore, terminalSeq)
			}
		})
	}
}

func assertOrdinaryMutationPage(t *testing.T, mutations []StoredMutation, wantSeq int64) {
	t.Helper()
	if len(mutations) != 1 || mutations[0].Seq != wantSeq || mutations[0].Entity != "observation" {
		t.Fatalf("mutations = %+v, want ordinary seq %d", mutations, wantSeq)
	}
	if chunkcodec.IsLocalOnlyEntity(mutations[0].Entity) {
		t.Fatalf("local-only mutation leaked: %+v", mutations[0])
	}
}
