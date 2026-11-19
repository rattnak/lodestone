package lodelog

import (
	"fmt"
	"testing"
)

// Append throughput, buffered (no per-record fsync) vs synced. The buffered
// path is the ingestion hot path; producers that must not lose data call Sync
// (or AppendSync) to trade throughput for durability.
//
//	go test ./internal/lodelog/ -bench . -benchtime 2s -run ^$
func BenchmarkAppend(b *testing.B) {
	l, err := Open(b.TempDir(), 1)
	if err != nil {
		b.Fatal(err)
	}
	defer l.Close()
	payload := []byte(fmt.Sprintf("%-200s", "event"))
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := l.Append(0, payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppendSync(b *testing.B) {
	l, err := Open(b.TempDir(), 1)
	if err != nil {
		b.Fatal(err)
	}
	defer l.Close()
	payload := []byte(fmt.Sprintf("%-200s", "event"))
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := l.AppendSync(0, payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRead(b *testing.B) {
	l, _ := Open(b.TempDir(), 1)
	defer l.Close()
	payload := []byte(fmt.Sprintf("%-200s", "event"))
	for i := 0; i < 100_000; i++ {
		l.Append(0, payload)
	}
	l.Sync()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := l.Read(0, uint64(i%90_000), 100); err != nil {
			b.Fatal(err)
		}
	}
}
