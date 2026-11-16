package storage

import (
	"fmt"
	"testing"
)

func TestBloomNoFalseNegatives(t *testing.T) {
	f := newBloomFilter(10_000)
	for i := 0; i < 10_000; i++ {
		h1, h2 := bloomHash([]byte(fmt.Sprintf("member-%d", i)))
		f.add(h1, h2)
	}
	for i := 0; i < 10_000; i++ {
		if !f.mayContain([]byte(fmt.Sprintf("member-%d", i))) {
			t.Fatalf("false negative for member-%d — bloom filters must never do this", i)
		}
	}
}

func TestBloomFalsePositiveRate(t *testing.T) {
	f := newBloomFilter(10_000)
	for i := 0; i < 10_000; i++ {
		h1, h2 := bloomHash([]byte(fmt.Sprintf("member-%d", i)))
		f.add(h1, h2)
	}
	fp := 0
	const probes = 50_000
	for i := 0; i < probes; i++ {
		if f.mayContain([]byte(fmt.Sprintf("absent-%d", i))) {
			fp++
		}
	}
	// 10 bits/key with k=7 predicts ~0.8%; allow slack but catch regressions.
	if rate := float64(fp) / probes; rate > 0.03 {
		t.Fatalf("false-positive rate %.3f%% exceeds 3%%", rate*100)
	}
}

func TestBloomSurvivesSSTableRoundtrip(t *testing.T) {
	m := NewMemtable()
	for i := 0; i < 1000; i++ {
		m.Put([]byte(fmt.Sprintf("key-%d", i)), []byte("v"))
	}
	st := buildTable(t, m.All())
	// The persisted filter must behave identically to the built one: every
	// member passes, and absent keys mostly fail.
	for i := 0; i < 1000; i++ {
		if !st.bloom.mayContain([]byte(fmt.Sprintf("key-%d", i))) {
			t.Fatalf("persisted filter lost key-%d", i)
		}
	}
	fp := 0
	for i := 0; i < 10_000; i++ {
		if st.bloom.mayContain([]byte(fmt.Sprintf("nope-%d", i))) {
			fp++
		}
	}
	if fp > 300 {
		t.Fatalf("persisted filter FP count %d/10000 — likely corrupted on write or read", fp)
	}
}
