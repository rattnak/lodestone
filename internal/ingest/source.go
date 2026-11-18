package ingest

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// A Source produces events and publishes them into the log via publish. It
// runs until ctx is cancelled. Sources are responsible for their own
// reconnect/backoff; a transient failure should not kill the stream.
type Source interface {
	Name() string
	Run(ctx context.Context, publish func(Event) error) error
}

// Publisher writes events into a lodelog, choosing the partition by event id.
type Publisher struct {
	log interface {
		Append(partition int, payload []byte) (uint64, error)
		Partitions() int
	}
}

func NewPublisher(log interface {
	Append(partition int, payload []byte) (uint64, error)
	Partitions() int
}) *Publisher {
	return &Publisher{log: log}
}

func (p *Publisher) Publish(e Event) error {
	b, err := e.encode()
	if err != nil {
		return err
	}
	_, err = p.log.Append(partitionFor(e.ID, p.log.Partitions()), b)
	return err
}

// --- Wikipedia recent-changes connector (Server-Sent Events) ---

// WikipediaSource streams Wikimedia's public recent-changes feed, a live
// firehose of edits, and emits one event per edit to an article.
type WikipediaSource struct {
	URL    string // defaults to the public stream
	Client *http.Client
}

const wikiStreamURL = "https://stream.wikimedia.org/v2/stream/recentchange"

func (s *WikipediaSource) Name() string { return "wikipedia" }

func (s *WikipediaSource) Run(ctx context.Context, publish func(Event) error) error {
	url := s.URL
	if url == "" {
		url = wikiStreamURL
	}
	client := s.Client
	if client == nil {
		client = &http.Client{}
	}
	for ctx.Err() == nil {
		if err := s.stream(ctx, client, url, publish); err != nil && ctx.Err() == nil {
			// Reconnect after a short backoff; the public stream drops
			// connections routinely.
			select {
			case <-ctx.Done():
			case <-time.After(2 * time.Second):
			}
		}
	}
	return ctx.Err()
}

func (s *WikipediaSource) stream(ctx context.Context, client *http.Client, url string, publish func(Event) error) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "text/event-stream")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("wikipedia stream: status %d", resp.StatusCode)
	}
	return parseSSE(resp.Body, func(data []byte) error {
		var rc struct {
			Type      string `json:"type"`
			Title     string `json:"title"`
			Wiki      string `json:"wiki"`
			ServerURL string `json:"server_url"`
			Comment   string `json:"comment"`
			ID        int64  `json:"id"`
			Namespace int    `json:"namespace"`
			Bot       bool   `json:"bot"`
			Meta      struct {
				URI string `json:"uri"`
				ID  string `json:"id"`
			} `json:"meta"`
		}
		if err := json.Unmarshal(data, &rc); err != nil {
			return nil // skip malformed frames
		}
		// Only real article edits on English Wikipedia, skip bots and
		// non-article namespaces.
		if rc.Wiki != "enwiki" || rc.Namespace != 0 || rc.Bot || (rc.Type != "edit" && rc.Type != "new") {
			return nil
		}
		return publish(Event{
			Source: "wikipedia",
			ID:     rc.Meta.ID,
			Title:  rc.Title,
			Text:   strings.TrimSpace(rc.Comment),
			URL:    rc.Meta.URI,
		})
	})
}

// parseSSE reads a Server-Sent Events body and invokes onData for each
// complete "data:" event payload.
func parseSSE(r io.Reader, onData func([]byte) error) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 1<<20)
	var data strings.Builder
	for sc.Scan() {
		line := sc.Text()
		switch {
		case line == "": // event boundary
			if data.Len() > 0 {
				if err := onData([]byte(data.String())); err != nil {
					return err
				}
				data.Reset()
			}
		case strings.HasPrefix(line, "data:"):
			data.WriteString(strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
		}
	}
	return sc.Err()
}

// --- Hacker News connector (polling the Firebase API) ---

// HackerNewsSource polls the public Hacker News API for new stories and emits
// one event per story. Unlike Wikipedia's push stream, this pulls on an
// interval and tracks the last-seen max item id.
type HackerNewsSource struct {
	BaseURL  string // defaults to the public API
	Interval time.Duration
	Client   *http.Client
}

const hnBaseURL = "https://hacker-news.firebaseio.com/v0"

func (s *HackerNewsSource) Name() string { return "hackernews" }

func (s *HackerNewsSource) Run(ctx context.Context, publish func(Event) error) error {
	base := s.BaseURL
	if base == "" {
		base = hnBaseURL
	}
	interval := s.Interval
	if interval == 0 {
		interval = 15 * time.Second
	}
	client := s.Client
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}

	var seen int64
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		maxID, err := s.maxItem(ctx, client, base)
		if err == nil {
			if seen == 0 {
				seen = maxID - 15 // backfill a little on first poll
			}
			for id := seen + 1; id <= maxID; id++ {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if ev, ok := s.fetchStory(ctx, client, base, id); ok {
					if err := publish(ev); err != nil {
						return err
					}
				}
			}
			seen = maxID
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

func (s *HackerNewsSource) maxItem(ctx context.Context, c *http.Client, base string) (int64, error) {
	var id int64
	err := getJSON(ctx, c, base+"/maxitem.json", &id)
	return id, err
}

func (s *HackerNewsSource) fetchStory(ctx context.Context, c *http.Client, base string, id int64) (Event, bool) {
	var item struct {
		Type  string `json:"type"`
		Title string `json:"title"`
		Text  string `json:"text"`
		URL   string `json:"url"`
		By    string `json:"by"`
		Dead  bool   `json:"dead"`
	}
	if err := getJSON(ctx, c, fmt.Sprintf("%s/item/%d.json", base, id), &item); err != nil {
		return Event{}, false
	}
	if item.Type != "story" || item.Dead || item.Title == "" {
		return Event{}, false
	}
	return Event{
		Source: "hackernews",
		ID:     fmt.Sprint(id),
		Title:  item.Title,
		Text:   item.Text,
		URL:    item.URL,
	}, true
}

func getJSON(ctx context.Context, c *http.Client, url string, v any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: status %d", url, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(v)
}
