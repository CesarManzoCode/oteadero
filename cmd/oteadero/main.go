// Command oteadero is the engine binary.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/CesarManzoCode/oteadero/internal/clock"
	"github.com/CesarManzoCode/oteadero/internal/config"
	"github.com/CesarManzoCode/oteadero/internal/eval"
	"github.com/CesarManzoCode/oteadero/internal/rng"
	"github.com/CesarManzoCode/oteadero/internal/rules"
	"github.com/CesarManzoCode/oteadero/internal/server"
	"github.com/CesarManzoCode/oteadero/internal/vfs"
)

const (
	version = "0.1.0-dev"
	usage   = "usage: oteadero serve --config <file> [--export <dir>]\n" +
		"       oteadero evaluate --config <file> --id <id> --rules <file> --coordinator <url> --receiver <url> --listen <addr> [--export <dir>]\n" +
		"       oteadero version\n"

	// shutdownTimeout bounds the wait for requests in flight; a client that
	// stalled in the middle of its body loses the connection after it.
	shutdownTimeout = 10 * time.Second
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run returns the exit code: 0, 1 on failure, 2 on bad usage.
func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	switch args[0] {
	case "version":
		fmt.Fprintf(stdout, "oteadero %s\n", version)
		return 0
	case "serve":
		return serveCommand(args[1:], stdout, stderr)
	case "evaluate":
		return evaluateCommand(args[1:], stdout, stderr)
	}
	fmt.Fprint(stderr, usage)
	return 2
}

func serveCommand(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", "", "configuration file")
	exportDir := flags.String("export", "", "directory of the verification export")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if *configPath == "" || flags.NArg() != 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(stop)
	if err := serve(*configPath, *exportDir, stdout, stop); err != nil {
		fmt.Fprintf(stderr, "oteadero: %v\n", err)
		return 1
	}
	return 0
}

// serve runs the engine until stop delivers or the log fails. GOMAXPROCS is
// left to the runtime, which respects the CPU affinity set by taskset.
func serve(configPath, exportDir string, stdout io.Writer, stop <-chan os.Signal) error {
	fsys := vfs.OS{}
	cfg, err := config.Load(fsys, configPath)
	if err != nil {
		return err
	}
	if cfg.MemoryLimitBytes > 0 {
		debug.SetMemoryLimit(cfg.MemoryLimitBytes)
	}
	// Bind before touching the data directory: a port in use has no effect
	// on the log. Connections wait in the backlog until the end of recovery.
	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return err
	}
	defer ln.Close()

	srv, err := server.New(cfg, server.Deps{FS: fsys, Clock: clock.NewReal(), Rand: rng.NewFromOS(), ExportDir: exportDir})
	if err != nil {
		return err
	}
	rec, err := srv.Start()
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "recovered next_offset=%d truncated_bytes=%d\n", rec.NextOffset, rec.TruncatedBytes)

	httpSrv := &http.Server{Handler: srv.Handler()}
	serveErr := make(chan error, 1)
	go func() { serveErr <- httpSrv.Serve(ln) }()
	fmt.Fprintf(stdout, "listening %s\n", ln.Addr())

	var failure error
	select {
	case <-stop:
	case <-srv.Done():
	case failure = <-serveErr:
	}

	// New requests get 503 shutting_down until the HTTP server stops, and
	// the final checkpoint counts them.
	srv.Drain()
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := httpSrv.Shutdown(ctx); err != nil {
		failure = errors.Join(failure, err, httpSrv.Close())
	}
	return errors.Join(failure, srv.Close())
}

// evaluateOptions are the flags of evaluate.
type evaluateOptions struct {
	config, id, rules, coordinator, receiver, listen, export string
}

func evaluateCommand(args []string, stdout, stderr io.Writer) int {
	var o evaluateOptions
	flags := flag.NewFlagSet("evaluate", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&o.config, "config", "", "configuration file")
	flags.StringVar(&o.id, "id", "", "evaluator id")
	flags.StringVar(&o.rules, "rules", "", "rule file")
	flags.StringVar(&o.coordinator, "coordinator", "", "URL of the lease coordinator")
	flags.StringVar(&o.receiver, "receiver", "", "URL of the notification receiver")
	flags.StringVar(&o.listen, "listen", "", "address of the metrics endpoint")
	flags.StringVar(&o.export, "export", "", "directory of the verification export")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if o.config == "" || o.id == "" || o.rules == "" || o.coordinator == "" || o.receiver == "" || o.listen == "" || flags.NArg() != 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(stop)
	if err := evaluate(o, stdout, stop); err != nil {
		fmt.Fprintf(stderr, "oteadero: %v\n", err)
		return 1
	}
	return 0
}

// evaluate runs one evaluator until stop delivers or something fails. It prints
// listening once recovered and then each change of role.
func evaluate(o evaluateOptions, stdout io.Writer, stop <-chan os.Signal) error {
	fsys := vfs.OS{}
	cfg, err := config.Load(fsys, o.config)
	if err != nil {
		return err
	}
	set, err := rules.Load(fsys, o.rules, cfg.BucketMs)
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", o.listen)
	if err != nil {
		return err
	}
	defer ln.Close()

	deps := eval.Deps{FS: fsys, Clock: clock.NewReal(), Rand: rng.NewFromOS(), ExportDir: o.export}
	node, err := eval.NewNode(cfg, o.id, set, o.coordinator, o.receiver, deps, func(leader bool, epoch uint64) {
		if leader {
			fmt.Fprintf(stdout, "leader epoch=%d\n", epoch)
		} else {
			fmt.Fprintln(stdout, "follower")
		}
	})
	if err != nil {
		return err
	}
	if err := node.Recover(); err != nil {
		return errors.Join(err, node.Close())
	}

	httpSrv := &http.Server{Handler: node.Handler()}
	serveErr := make(chan error, 1)
	go func() { serveErr <- httpSrv.Serve(ln) }()
	fmt.Fprintf(stdout, "listening %s\n", ln.Addr())

	stopped := make(chan struct{})
	failure := make(chan error, 1)
	go func() {
		select {
		case <-stop:
		case err := <-serveErr:
			failure <- err
		}
		close(stopped)
	}()
	err = node.Run(stopped)

	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if serr := httpSrv.Shutdown(ctx); serr != nil {
		err = errors.Join(err, serr, httpSrv.Close())
	}
	select {
	case ferr := <-failure:
		if !errors.Is(ferr, http.ErrServerClosed) {
			err = errors.Join(err, ferr)
		}
	default:
	}
	return errors.Join(err, node.Close())
}
