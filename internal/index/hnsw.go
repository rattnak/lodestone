// Package index implements approximate nearest-neighbor search over vectors
// using HNSW (Hierarchical Navigable Small World graphs, Malkov & Yashunin
// 2016). Every vector is a node linked to close neighbors; layer 0 holds all
// nodes, and each higher layer keeps an exponentially thinner subset that
// acts as a highway network. A search greedily descends from the sparse top
// toward the dense bottom, so query cost grows logarithmically with the
// collection instead of linearly.
package index

import (
	"container/heap"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sync"
)

// Distance returns a dissimilarity between two vectors: lower is closer.
type Distance func(a, b []float32) float32

// L2 is squared Euclidean distance (the square root is monotonic, so ranking
// does not need it).
func L2(a, b []float32) float32 {
	var s float32
	for i := range a {
		d := a[i] - b[i]
		s += d * d
	}
	return s
}

// Cosine is cosine distance, 1 - cos(a, b). Embedding models typically want
// this metric.
func Cosine(a, b []float32) float32 {
	var dot, na, nb float32
	for i := range a {
		dot += a[i] * b[i]
		na += a[i] * a[i]
		nb += b[i] * b[i]
	}
	if na == 0 || nb == 0 {
		return 1
	}
	return 1 - dot/float32(math.Sqrt(float64(na))*math.Sqrt(float64(nb)))
}

// Options tunes the index. Zero values take the defaults noted per field.
type Options struct {
	// M is the max neighbors per node on upper layers (layer 0 allows 2M).
	// Higher M: better recall, more memory. Default 16.
	M int
	// EfConstruction is the candidate-list width while inserting. Higher:
	// better graph quality, slower builds. Default 200.
	EfConstruction int
	// Distance defaults to Cosine.
	Distance Distance
	// Seed makes level assignment deterministic for tests; 0 seeds randomly.
	Seed int64
}

// Result is one search hit. Score is the distance to the query: lower is
// closer.
type Result struct {
	ID    uint64
	Score float32
}

type node struct {
	id      uint64
	vec     []float32
	links   [][]int32 // neighbor node indices per layer, [0..level]
	deleted bool
}

// Index is an in-memory HNSW graph. Safe for concurrent use; writes are
// serialized, searches run concurrently.
type Index struct {
	mu       sync.RWMutex
	opts     Options
	dim      int
	mL       float64 // level multiplier: 1/ln(M)
	nodes    []node
	byID     map[uint64]int32
	entry    int32 // entry point: a node on the top layer
	maxLevel int
	live     int
	rng      *rand.Rand
}

var (
	ErrDimension = errors.New("index: vector dimension mismatch")
	ErrDuplicate = errors.New("index: id already present")
	ErrNotFound  = errors.New("index: id not found")
)

func New(opts Options) *Index {
	if opts.M <= 0 {
		opts.M = 16
	}
	if opts.EfConstruction <= 0 {
		opts.EfConstruction = 200
	}
	if opts.Distance == nil {
		opts.Distance = Cosine
	}
	seed := opts.Seed
	if seed == 0 {
		seed = rand.Int63()
	}
	return &Index{
		opts:  opts,
		mL:    1 / math.Log(float64(opts.M)),
		byID:  map[uint64]int32{},
		entry: -1,
		rng:   rand.New(rand.NewSource(seed)),
	}
}

// Len returns the number of live (non-deleted) vectors.
func (ix *Index) Len() int {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	return ix.live
}

// Add inserts a vector under id. The vector is not copied; callers must not
// mutate it afterwards.
func (ix *Index) Add(id uint64, vec []float32) error {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if ix.dim == 0 {
		ix.dim = len(vec)
	} else if len(vec) != ix.dim {
		return fmt.Errorf("%w: got %d, index has %d", ErrDimension, len(vec), ix.dim)
	}
	if j, dup := ix.byID[id]; dup {
		if !ix.nodes[j].deleted {
			return fmt.Errorf("%w: %d", ErrDuplicate, id)
		}
		// Re-adding a deleted id: the old node stays a graph waypoint but
		// no longer owns the id; the mapping moves to the fresh node below.
	}

	level := int(math.Floor(-math.Log(ix.rng.Float64()) * ix.mL))
	n := node{id: id, vec: vec, links: make([][]int32, level+1)}
	idx := int32(len(ix.nodes))
	ix.nodes = append(ix.nodes, n)
	ix.byID[id] = idx
	ix.live++

	if ix.entry < 0 {
		ix.entry = idx
		ix.maxLevel = level
		return nil
	}

	// Phase A: greedy descent through layers above the new node's level,
	// keeping only the single closest point per layer.
	ep := ix.entry
	for l := ix.maxLevel; l > level; l-- {
		ep = ix.greedyClosest(vec, ep, l)
	}

	// Phase B: on each shared layer, widen to efConstruction candidates,
	// pick M of them as neighbors, and link both directions.
	eps := []candidate{{idx: ep, dist: ix.opts.Distance(vec, ix.nodes[ep].vec)}}
	for l := min(level, ix.maxLevel); l >= 0; l-- {
		found := ix.searchLayer(vec, eps, ix.opts.EfConstruction, l)
		neighbors := selectClosest(found, ix.opts.M)
		ix.nodes[idx].links[l] = make([]int32, 0, len(neighbors))
		for _, nb := range neighbors {
			ix.nodes[idx].links[l] = append(ix.nodes[idx].links[l], nb.idx)
			ix.linkBack(nb.idx, idx, l)
		}
		eps = found
	}

	if level > ix.maxLevel {
		ix.maxLevel = level
		ix.entry = idx
	}
	return nil
}

// Delete marks id as deleted. The node stays in the graph as a waypoint
// (removing edges would tear routes other nodes depend on) but is never
// returned from searches. This is how hnswlib and FAISS handle deletes too;
// reclaiming space needs a rebuild.
func (ix *Index) Delete(id uint64) error {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	i, ok := ix.byID[id]
	if !ok || ix.nodes[i].deleted {
		return fmt.Errorf("%w: %d", ErrNotFound, id)
	}
	ix.nodes[i].deleted = true
	ix.live--
	return nil
}

// Search returns the k nearest live vectors to query, closest first. ef is
// the search width: recall rises with it at the cost of latency; values
// below k are raised to k, and 0 means "use max(k, 64)".
func (ix *Index) Search(query []float32, k, ef int) ([]Result, error) {
	return ix.SearchFilter(query, k, ef, nil)
}

// SearchFilter is Search restricted to IDs for which allow returns true
// (post-filtering: the graph is traversed unfiltered and disallowed hits are
// dropped from the result set). With a selective filter, raise ef so enough
// allowed candidates survive — a filter keeping 1-in-10 wants ef around 10x.
func (ix *Index) SearchFilter(query []float32, k, ef int, allow func(id uint64) bool) ([]Result, error) {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	if ix.entry < 0 {
		return nil, nil
	}
	if len(query) != ix.dim {
		return nil, fmt.Errorf("%w: got %d, index has %d", ErrDimension, len(query), ix.dim)
	}
	if ef == 0 {
		ef = max(k, 64)
	}
	ef = max(ef, k)

	ep := ix.entry
	for l := ix.maxLevel; l > 0; l-- {
		ep = ix.greedyClosest(query, ep, l)
	}
	eps := []candidate{{idx: ep, dist: ix.opts.Distance(query, ix.nodes[ep].vec)}}
	found := ix.searchLayer(query, eps, ef, 0)

	keep := found[:0]
	for _, c := range found {
		n := &ix.nodes[c.idx]
		if n.deleted || (allow != nil && !allow(n.id)) {
			continue
		}
		keep = append(keep, c)
	}
	res := selectClosest(keep, k)
	out := make([]Result, len(res))
	for i, c := range res {
		out[i] = Result{ID: ix.nodes[c.idx].id, Score: c.dist}
	}
	return out, nil
}

// greedyClosest walks layer l from start, always moving to the closest
// neighbor, until no neighbor improves — the ef=1 descent of the paper.
func (ix *Index) greedyClosest(query []float32, start int32, l int) int32 {
	cur := start
	curDist := ix.opts.Distance(query, ix.nodes[cur].vec)
	for improved := true; improved; {
		improved = false
		for _, nb := range ix.linksAt(cur, l) {
			if d := ix.opts.Distance(query, ix.nodes[nb].vec); d < curDist {
				cur, curDist, improved = nb, d, true
			}
		}
	}
	return cur
}

// searchLayer is the paper's SEARCH-LAYER: best-first expansion over layer l
// keeping the ef closest points seen. Returns candidates sorted closest
// first.
func (ix *Index) searchLayer(query []float32, eps []candidate, ef, l int) []candidate {
	visited := map[int32]bool{}
	frontier := &minHeap{}  // to explore, closest first
	best := &maxHeap{}      // ef best so far, farthest on top
	for _, c := range eps {
		visited[c.idx] = true
		heap.Push(frontier, c)
		heap.Push(best, c)
	}
	for frontier.Len() > 0 {
		c := heap.Pop(frontier).(candidate)
		if best.Len() >= ef && c.dist > (*best)[0].dist {
			break // everything reachable is farther than the worst keeper
		}
		for _, nb := range ix.linksAt(c.idx, l) {
			if visited[nb] {
				continue
			}
			visited[nb] = true
			d := ix.opts.Distance(query, ix.nodes[nb].vec)
			if best.Len() < ef || d < (*best)[0].dist {
				heap.Push(frontier, candidate{idx: nb, dist: d})
				heap.Push(best, candidate{idx: nb, dist: d})
				if best.Len() > ef {
					heap.Pop(best)
				}
			}
		}
	}
	out := make([]candidate, best.Len())
	for i := len(out) - 1; i >= 0; i-- {
		out[i] = heap.Pop(best).(candidate)
	}
	return out
}

func (ix *Index) linksAt(i int32, l int) []int32 {
	if l >= len(ix.nodes[i].links) {
		return nil
	}
	return ix.nodes[i].links[l]
}

// linkBack adds newIdx as a neighbor of existing node at layer l, pruning to
// the layer's capacity (2M on layer 0, M above) by keeping the closest.
func (ix *Index) linkBack(existing, newIdx int32, l int) {
	maxLinks := ix.opts.M
	if l == 0 {
		maxLinks = 2 * ix.opts.M
	}
	links := append(ix.nodes[existing].links[l], newIdx)
	if len(links) > maxLinks {
		cands := make([]candidate, len(links))
		base := ix.nodes[existing].vec
		for i, nb := range links {
			cands[i] = candidate{idx: nb, dist: ix.opts.Distance(base, ix.nodes[nb].vec)}
		}
		keep := selectClosest(cands, maxLinks)
		links = links[:0]
		for _, c := range keep {
			links = append(links, c.idx)
		}
	}
	ix.nodes[existing].links[l] = links
}

type candidate struct {
	idx  int32
	dist float32
}

// selectClosest returns the k closest candidates, sorted ascending.
func selectClosest(cands []candidate, k int) []candidate {
	h := maxHeap{}
	for _, c := range cands {
		if h.Len() < k {
			heap.Push(&h, c)
		} else if c.dist < h[0].dist {
			h[0] = c
			heap.Fix(&h, 0)
		}
	}
	out := make([]candidate, h.Len())
	for i := len(out) - 1; i >= 0; i-- {
		out[i] = heap.Pop(&h).(candidate)
	}
	return out
}

type minHeap []candidate

func (h minHeap) Len() int            { return len(h) }
func (h minHeap) Less(i, j int) bool  { return h[i].dist < h[j].dist }
func (h minHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *minHeap) Push(x any)         { *h = append(*h, x.(candidate)) }
func (h *minHeap) Pop() any {
	old := *h
	c := old[len(old)-1]
	*h = old[:len(old)-1]
	return c
}

type maxHeap []candidate

func (h maxHeap) Len() int            { return len(h) }
func (h maxHeap) Less(i, j int) bool  { return h[i].dist > h[j].dist }
func (h maxHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *maxHeap) Push(x any)         { *h = append(*h, x.(candidate)) }
func (h *maxHeap) Pop() any {
	old := *h
	c := old[len(old)-1]
	*h = old[:len(old)-1]
	return c
}
