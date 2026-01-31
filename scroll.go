package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/charmbracelet/log"
	json "github.com/json-iterator/go"
	"golang.org/x/sync/errgroup"
)

func scroll(ctx context.Context, scrollers []func(context.Context) error) error {
	grp, ctx := errgroup.WithContext(ctx)
	for _, scroller := range scrollers {
		scroller := scroller
		grp.Go(func() error {
			return scroller(ctx)
		})
	}
	return grp.Wait()
}

func (d *dumper) scrollSlice(ctx context.Context, index string, sliceIdx, sliceTotal int) error {
	q := d.scrollQuery(sliceIdx, sliceTotal)

	reqStart := time.Now()
	scrollID, totalHits, more, err := d.scrollRequest(ctx, index+"/_search?scroll="+d.scrollTimeoutES, q)
	d.totalHitsCtr.Report(totalHits)
	defer func() {
		d.clearScrollContext(scrollID)
	}()
	if err != nil || !more {
		return err
	}

	for {
		cancelableSleep(ctx, d.throttlingDuration(time.Since(reqStart)))
		scrollReq := map[string]string{
			"scroll":    d.scrollTimeoutES,
			"scroll_id": scrollID,
		}
		qBytes, err := json.Marshal(scrollReq)
		if err != nil {
			return fmt.Errorf("marshaling scroll request: %w", err)
		}
		q = string(qBytes)
		reqStart = time.Now()
		// do not immediately overwrite the scrollID, in case of error
		// we want to clear the previous one
		newScrollID, _, more, err := d.scrollRequest(ctx, "_search/scroll", q)
		if err != nil {
			return err
		}
		scrollID = newScrollID
		if !more {
			return nil
		}
	}
}

// sendHits sends hits to the output and returns whether the count limit has been reached.
func (d *dumper) sendHits(hits []json.RawMessage) bool {
	scrolled := atomic.LoadUint64(&d.scrolled)
	if d.count > 0 && scrolled >= d.count {
		return true
	}

	for _, hit := range hits {
		d.scrolledCh <- hit
	}

	scrolled = atomic.AddUint64(&d.scrolled, uint64(len(hits)))
	return d.count > 0 && scrolled >= d.count
}

func (d *dumper) clearScrollContext(scrollID string) {
	if scrollID == "" {
		return
	}
	// we want to clear the scroll context even after the Go ctx is canceled, so
	// we use our own ctx.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	status, raw, err := d.cl.Delete(ctx, "_search/scroll/"+scrollID, "", nil)
	if err != nil {
		log.Error("clearing scroll context", "err", err)
	}
	if status != http.StatusOK {
		log.Error("clearing scroll context", "code", status, "response", string(raw))
	}
}

func cancelableSleep(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

func (d *dumper) throttlingDuration(reqDuration time.Duration) time.Duration {
	if d.throttle <= 0 {
		return 0
	}

	delay := time.Duration(d.throttle * float32(reqDuration))

	// make sure we don't sleep for more than the scroll context timeout, or
	// even get too close to it, as we must avoid it expiring
	maxDelay := 3 * d.scrollTimeout / 4
	if delay > maxDelay {
		delay = maxDelay
	}
	return delay
}

func (d *dumper) scrollQuery(sliceIdx, sliceTotal int) string {
	q := d.query
	if sliceTotal > 1 {
		qCopy := make(obj)
		for k := range d.query {
			qCopy[k] = d.query[k]
		}
		qCopy["slice"] = obj{
			"id":  sliceIdx,
			"max": sliceTotal,
		}
		q = qCopy
	}

	b, err := json.Marshal(q)
	if err != nil {
		log.Fatal("marshaling query", "err", err)
	}
	return string(b)
}

type scrollResp interface {
	GetHits() []json.RawMessage
	GetScrollID() string
	GetTotal() uint64
}

type scrollRespMetadata struct {
	Hits struct {
		Total struct {
			Value uint64 `json:"value"`
		} `json:"total"`
		Hits []json.RawMessage `json:"hits"`
	} `json:"hits"`
	ScrollID string `json:"_scroll_id"`
}

func (r scrollRespMetadata) GetHits() []json.RawMessage {
	return r.Hits.Hits
}

func (r scrollRespMetadata) GetScrollID() string {
	return r.ScrollID
}

func (r scrollRespMetadata) GetTotal() uint64 {
	return r.Hits.Total.Value
}

type scrollRespSourceOnly struct {
	Hits struct {
		Total struct {
			Value uint64 `json:"value"`
		} `json:"total"`
		Hits []struct {
			Source json.RawMessage `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
	ScrollID string `json:"_scroll_id"`
}

func (r scrollRespSourceOnly) GetHits() []json.RawMessage {
	hits := make([]json.RawMessage, len(r.Hits.Hits))
	for i, hit := range r.Hits.Hits {
		hits[i] = hit.Source
	}
	return hits
}

func (r scrollRespSourceOnly) GetScrollID() string {
	return r.ScrollID
}

func (r scrollRespSourceOnly) GetTotal() uint64 {
	return r.Hits.Total.Value
}

func (d *dumper) scrollRequest(ctx context.Context, path, query string) (string, uint64, bool, error) {
	var resp scrollResp
	if d.metadata || d.metadataOnly {
		resp = &scrollRespMetadata{}
	} else {
		resp = &scrollRespSourceOnly{}
	}

	status, raw, err := d.cl.Get(ctx, path, query, resp)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			log.Error("sending scroll request", "err", err)
		}
		return "", 0, false, err
	}

	if status != http.StatusOK {
		log.Error("got unexpected status code", "code", status, "response", string(raw))
		return "", 0, false, errors.New("unexpected status code")
	}

	hits := resp.GetHits()
	limitReached := d.sendHits(hits)
	return resp.GetScrollID(), resp.GetTotal(), len(hits) == d.size && !limitReached, err
}

// GroupCounter is a counter that aggregates counts from a given number of
// goroutines. Each goroutine must call Report() exactly once.
type GroupCounter struct {
	cnt     uint64
	pending int
	mu      sync.RWMutex
}

func NewGroupCounter(n int) *GroupCounter {
	return &GroupCounter{
		pending: n,
	}
}

func (gc *GroupCounter) Report(cnt uint64) {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	gc.pending--
	if gc.pending < 0 {
		panic("GroupCounter pending < 0")
	}
	gc.cnt += cnt
}

// Get returns the current count and whether all goroutines have reported.
func (gc *GroupCounter) Get() (uint64, bool) {
	gc.mu.RLock()
	defer gc.mu.RUnlock()
	return gc.cnt, gc.pending == 0
}
