// Command lodestone is the CLI entry point. The server proper arrives with
// the gRPC work; for now the CLI offers a self-contained semantic-search
// demo over a local text file, exercising the embedding client and HNSW
// index end to end.
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/rattnak/lodestone/internal/client"
	"github.com/rattnak/lodestone/internal/cluster"
	"github.com/rattnak/lodestone/internal/embed"
	"github.com/rattnak/lodestone/internal/gateway"
	"github.com/rattnak/lodestone/internal/index"
	"github.com/rattnak/lodestone/internal/ingest"
	"github.com/rattnak/lodestone/internal/lodelog"
	"github.com/rattnak/lodestone/internal/server"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "serve":
		if err := runServe(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
	case "search":
		if err := runSearch(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
	case "ingest":
		if err := runIngest(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
	case "cluster":
		if err := runCluster(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
	case "dashboard":
		if err := runDashboard(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  lodestone serve     -data <dir> [-addr :7437]
  lodestone cluster   -data <dir> -addr <host:port> [-bootstrap | -seeds <addrs>]
  lodestone dashboard -server <host:port> [-addr :8437]
  lodestone ingest    -data <dir> -server <host:port> [-sources wikipedia,hackernews]
  lodestone search    -file <texts.txt> -query "..." [-k 5] [-ollama URL] [-model NAME]

serve runs a single lodestone node: KV + vector search over gRPC.

cluster runs a distributed node: partitioned, Raft-replicated. Start the first
node with -bootstrap, then add more with -seeds pointing at a running node.

dashboard serves the live web console (telemetry + search) over HTTP, bridging
the node's gRPC API to the browser. Open http://localhost:8437.

ingest streams live events (Wikipedia edits, Hacker News stories) into a local
lodelog, embeds them via Ollama, and upserts documents into the cluster at
-server. Requires a running Ollama with an embedding model:
  ollama pull nomic-embed-text

search is a self-contained demo over a local text file.`)
}

func runCluster(args []string) error {
	fs := flag.NewFlagSet("cluster", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:7437", "this node's reachable address")
	data := fs.String("data", "", "data directory")
	seeds := fs.String("seeds", "", "comma-separated seed node addresses to join")
	partitions := fs.Int("partitions", 8, "partition count (bootstrap only)")
	rf := fs.Int("rf", 3, "replication factor")
	bootstrap := fs.Bool("bootstrap", false, "form a new cluster as a founding node")
	fs.Parse(args)
	if *data == "" {
		return fmt.Errorf("-data is required")
	}

	var seedList []string
	for _, s := range strings.Split(*seeds, ",") {
		if s = strings.TrimSpace(s); s != "" {
			seedList = append(seedList, s)
		}
	}
	node, err := cluster.NewNode(cluster.Options{
		ID: *addr, Dir: *data, Seeds: seedList,
		Partitions: uint32(*partitions), RF: *rf, Bootstrap: *bootstrap,
	})
	if err != nil {
		return err
	}
	srv := cluster.NewClusterServer(node)
	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if len(seedList) > 0 && !*bootstrap {
		jctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		srv.JoinSeeds(jctx, seedList)
		cancel()
	}
	go func() {
		<-ctx.Done()
		fmt.Fprintln(os.Stderr, "shutting down...")
		srv.Stop()
	}()
	fmt.Fprintf(os.Stderr, "lodestone cluster node %s (%d partitions, rf %d), data in %s\n", *addr, *partitions, *rf, *data)
	return srv.Serve(lis)
}

func runDashboard(args []string) error {
	fs := flag.NewFlagSet("dashboard", flag.ExitOnError)
	addr := fs.String("addr", ":8437", "HTTP listen address")
	server := fs.String("server", "localhost:7437", "lodestone server to bridge")
	ollamaURL := fs.String("ollama", embed.DefaultBaseURL, "Ollama base URL (for text search)")
	model := fs.String("model", embed.DefaultModel, "embedding model")
	noEmbed := fs.Bool("no-embed", false, "disable text search (vector-only)")
	fs.Parse(args)

	cl, err := client.Dial(*server)
	if err != nil {
		return err
	}
	defer cl.Close()

	var embedder embed.Embedder
	if !*noEmbed {
		embedder = embed.NewClient(*ollamaURL, *model)
	}
	gw := gateway.New(cl, embedder)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	shown := *addr
	if strings.HasPrefix(shown, ":") {
		shown = "localhost" + shown
	}
	fmt.Fprintf(os.Stderr, "dashboard on http://%s → %s\n", shown, *server)
	return gw.Serve(ctx, *addr)
}

func runIngest(args []string) error {
	fs := flag.NewFlagSet("ingest", flag.ExitOnError)
	data := fs.String("data", "", "directory for the lodelog")
	server := fs.String("server", "localhost:7437", "lodestone server to upsert into")
	sources := fs.String("sources", "wikipedia,hackernews", "comma-separated: wikipedia,hackernews")
	parts := fs.Int("partitions", 8, "lodelog partition count")
	ollamaURL := fs.String("ollama", embed.DefaultBaseURL, "Ollama base URL")
	model := fs.String("model", embed.DefaultModel, "embedding model")
	fs.Parse(args)
	if *data == "" {
		return fmt.Errorf("-data is required")
	}

	lg, err := lodelog.Open(filepath.Join(*data, "lodelog"), *parts)
	if err != nil {
		return err
	}
	defer lg.Close()

	cl, err := client.Dial(*server)
	if err != nil {
		return err
	}
	defer cl.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Source connectors publish into the log.
	pub := ingest.NewPublisher(lg)
	var srcs []ingest.Source
	for _, s := range strings.Split(*sources, ",") {
		switch strings.TrimSpace(s) {
		case "wikipedia":
			srcs = append(srcs, &ingest.WikipediaSource{})
		case "hackernews":
			srcs = append(srcs, &ingest.HackerNewsSource{})
		}
	}
	for _, src := range srcs {
		src := src
		go func() {
			fmt.Fprintf(os.Stderr, "source %s started\n", src.Name())
			if err := src.Run(ctx, pub.Publish); err != nil && ctx.Err() == nil {
				fmt.Fprintf(os.Stderr, "source %s stopped: %v\n", src.Name(), err)
			}
		}()
	}

	// The embedder consumes the log and writes to the cluster.
	emb, err := ingest.NewEmbedder(ingest.Config{
		Log:      lg,
		Embedder: embed.NewClient(*ollamaURL, *model),
		Writer:   cl,
	})
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "ingesting into %s via %s...\n", *server, *model)
	return emb.Run(ctx)
}

func runServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	addr := fs.String("addr", ":7437", "listen address")
	data := fs.String("data", "", "data directory")
	fs.Parse(args)
	if *data == "" {
		return fmt.Errorf("-data is required")
	}

	srv, err := server.New(*data, nil)
	if err != nil {
		return err
	}
	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		return err
	}
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stop
		fmt.Fprintln(os.Stderr, "shutting down...")
		srv.Stop()
	}()
	fmt.Fprintf(os.Stderr, "lodestone serving on %s, data in %s\n", *addr, *data)
	return srv.Serve(lis)
}

func runSearch(args []string) error {
	fs := flag.NewFlagSet("search", flag.ExitOnError)
	file := fs.String("file", "", "text file, one document per line")
	query := fs.String("query", "", "search query")
	k := fs.Int("k", 5, "results to return")
	ollamaURL := fs.String("ollama", embed.DefaultBaseURL, "Ollama base URL")
	model := fs.String("model", embed.DefaultModel, "embedding model")
	fs.Parse(args)
	if *file == "" || *query == "" {
		return fmt.Errorf("-file and -query are required")
	}

	f, err := os.Open(*file)
	if err != nil {
		return err
	}
	defer f.Close()
	var docs []string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		if line := strings.TrimSpace(sc.Text()); line != "" {
			docs = append(docs, line)
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}
	if len(docs) == 0 {
		return fmt.Errorf("%s has no non-empty lines", *file)
	}

	ctx := context.Background()
	client := embed.NewClient(*ollamaURL, *model)

	start := time.Now()
	fmt.Fprintf(os.Stderr, "embedding %d documents via %s...\n", len(docs), *model)
	vecs, err := client.Embed(ctx, docs)
	if err != nil {
		return fmt.Errorf("embedding documents (is Ollama running?): %w", err)
	}
	fmt.Fprintf(os.Stderr, "embedded in %s, building index...\n", time.Since(start).Round(time.Millisecond))

	ix := index.New(index.Options{})
	for i, v := range vecs {
		if err := ix.Add(uint64(i), v); err != nil {
			return err
		}
	}

	qv, err := client.Embed(ctx, []string{*query})
	if err != nil {
		return fmt.Errorf("embedding query: %w", err)
	}
	res, err := ix.Search(qv[0], *k, 0)
	if err != nil {
		return err
	}
	for rank, r := range res {
		doc := docs[r.ID]
		if len(doc) > 120 {
			doc = doc[:117] + "..."
		}
		fmt.Printf("%d. [distance %.4f] %s\n", rank+1, r.Score, doc)
	}
	return nil
}
