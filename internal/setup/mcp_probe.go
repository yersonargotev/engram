package setup

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	mcpclient "github.com/mark3labs/mcp-go/client"
	mcptransport "github.com/mark3labs/mcp-go/client/transport"
	mcppkg "github.com/mark3labs/mcp-go/mcp"
)

const mcpProbeStderrLimit = 4096

type mcpProbeResult struct {
	ProtocolVersion string
	Tools           []string
}

// runStdioMCPProbe verifies the protocol surface without calling any tools.
// The subprocess receives an isolated Engram data directory and cannot emit
// Recall baseline or cloud autosync events into the user's configured store.
func runStdioMCPProbe(command string, args []string, timeout time.Duration) (mcpProbeResult, error) {
	if timeout <= 0 {
		return mcpProbeResult{}, fmt.Errorf("probe timeout must be positive")
	}
	dataDir, err := os.MkdirTemp("", "engram-mcp-probe-")
	if err != nil {
		return mcpProbeResult{}, fmt.Errorf("create isolated probe data directory: %w", err)
	}
	defer os.RemoveAll(dataDir)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	environment := mcpProbeEnvironment(dataDir)
	client, err := mcpclient.NewStdioMCPClientWithOptions(
		command,
		environment,
		args,
		mcptransport.WithCommandFunc(func(_ context.Context, command string, env, args []string) (*exec.Cmd, error) {
			cmd := exec.CommandContext(ctx, command, args...)
			cmd.Env = append([]string(nil), env...)
			return cmd, nil
		}),
	)
	if err != nil {
		return mcpProbeResult{}, fmt.Errorf("start MCP server: %w", err)
	}

	stderr := &boundedProbeBuffer{limit: mcpProbeStderrLimit}
	stderrDone := make(chan struct{})
	if reader, ok := mcpclient.GetStderr(client); ok {
		go func() {
			_, _ = io.Copy(stderr, reader)
			close(stderrDone)
		}()
	} else {
		close(stderrDone)
	}
	closed := false
	closeClient := func() error {
		if closed {
			return nil
		}
		closed = true
		err := client.Close()
		<-stderrDone
		return err
	}
	defer func() { _ = closeClient() }()

	initialized, err := client.Initialize(ctx, mcppkg.InitializeRequest{Params: mcppkg.InitializeParams{
		ProtocolVersion: mcppkg.LATEST_PROTOCOL_VERSION,
		ClientInfo:      mcppkg.Implementation{Name: "engram-setup-probe", Version: "1"},
	}})
	if err != nil {
		closeErr := closeClient()
		return mcpProbeResult{}, mcpProbeError(ctx, stderr, fmt.Errorf("initialize MCP server: %w", err), closeErr)
	}
	listed, err := client.ListTools(ctx, mcppkg.ListToolsRequest{})
	if err != nil {
		closeErr := closeClient()
		return mcpProbeResult{}, mcpProbeError(ctx, stderr, fmt.Errorf("list MCP tools: %w", err), closeErr)
	}

	result := mcpProbeResult{
		ProtocolVersion: initialized.ProtocolVersion,
		Tools:           make([]string, 0, len(listed.Tools)),
	}
	for _, tool := range listed.Tools {
		if strings.TrimSpace(tool.Name) == "" {
			closeErr := closeClient()
			return mcpProbeResult{}, mcpProbeError(ctx, stderr, fmt.Errorf("tools/list returned an unnamed tool"), closeErr)
		}
		result.Tools = append(result.Tools, tool.Name)
	}
	if err := closeClient(); err != nil {
		return mcpProbeResult{}, mcpProbeError(ctx, stderr, nil, fmt.Errorf("close MCP probe: %w", err))
	}
	return result, nil
}

func mcpProbeEnvironment(dataDir string) []string {
	environment := make([]string, 0, len(os.Environ())+3)
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if strings.EqualFold(name, "ENGRAM_DATA_DIR") ||
			strings.EqualFold(name, "ENGRAM_CLOUD_AUTOSYNC") ||
			strings.EqualFold(name, "ENGRAM_RECALL_BASELINE") {
			continue
		}
		environment = append(environment, entry)
	}
	return append(environment,
		"ENGRAM_DATA_DIR="+dataDir,
		"ENGRAM_CLOUD_AUTOSYNC=0",
		"ENGRAM_RECALL_BASELINE=0",
	)
}

func mcpProbeError(ctx context.Context, stderr *boundedProbeBuffer, operationErr, closeErr error) error {
	var err error
	if ctx.Err() != nil {
		err = fmt.Errorf("probe timed out: %w", ctx.Err())
	} else if operationErr != nil {
		err = operationErr
	} else {
		err = closeErr
	}
	if detail := strings.TrimSpace(stderr.String()); detail != "" {
		return fmt.Errorf("%w: %s", err, detail)
	}
	if err != nil {
		return err
	}
	return fmt.Errorf("MCP probe failed")
}

type boundedProbeBuffer struct {
	mu    sync.Mutex
	data  []byte
	limit int
}

func (b *boundedProbeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	remaining := b.limit - len(b.data)
	if remaining > 0 {
		if len(p) < remaining {
			remaining = len(p)
		}
		b.data = append(b.data, p[:remaining]...)
	}
	return len(p), nil
}

func (b *boundedProbeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.data)
}
