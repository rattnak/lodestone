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
	"strings"
	"syscall"
	"time"

	"github.com/rattnak/lodestone/internal/embed"
	"github.com/rattnak/lodestone/internal/index"
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
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  lodestone serve  -data <dir> [-addr :7437]
  lodestone search -file <texts.txt> -query "..." [-k 5] [-ollama URL] [-model NAME]

serve runs a lodestone node: KV + vector search over gRPC.

search is a self-contained demo: embeds each line of the file, indexes the
vectors in HNSW, then answers the query semantically. Requires a running
Ollama with an embedding model:
  ollama pull nomic-embed-text`)
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
