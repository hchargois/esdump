package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/charmbracelet/log"
	json "github.com/json-iterator/go"
	"golang.org/x/sync/errgroup"
)

var errCountReached = errors.New("count reached")

func scroll(ctx context.Context, scrollers []func(context.Context) error) error {
	grp, ctx := errgroup.WithContext(ctx)
	for _, scroller := range scrollers {
		grp.Go(func() error {
			return scroller(ctx)
		})
	}
	err := grp.Wait()
	if errors.Is(err, errCountReached) {
		return nil
	}
	return err
}

func (d *dumper) scrollSlice(ctx context.Context, index string, sliceIdx, sliceTotal int) error {
	q := d.scrollQuery(sliceIdx, sliceTotal)

	reqStart := time.Now()
	scrollID, totalHits, more, err := d.scrollRequest(ctx, index+"/_search?scroll="+d.scrollTimeoutES, q)
	atomic.AddUint64(&d.totalHits, totalHits)
	atomic.AddInt32(&d.totalHitsPending, -1)
	if err != nil || !more {
		d.clearScrollContext(scrollID)
		return err
	}

	d.sleepForThrottling(ctx, time.Since(reqStart))

	var newScrollID string
	for {
		reqStart := time.Now()
		scrollReq := map[string]string{
			"scroll":    d.scrollTimeoutES,
			"scroll_id": scrollID,
		}
		qBytes, err := json.Marshal(scrollReq)
		if err != nil {
			d.clearScrollContext(scrollID)
			return fmt.Errorf("marshaling scroll request: %w", err)
		}
		q = string(qBytes)
		newScrollID, _, more, err = d.scrollRequest(ctx, "_search/scroll", q)
		if err != nil || !more {
			break
		}
		scrollID = newScrollID
		d.sleepForThrottling(ctx, time.Since(reqStart))
	}
	d.clearScrollContext(scrollID)
	return err
}

func (d *dumper) sendHits(hits []json.RawMessage) error {
	scrolled := atomic.LoadUint64(&d.scrolled)
	if d.count > 0 && scrolled >= d.count {
		return errCountReached
	}

	for _, hit := range hits {
		d.scrolledCh <- hit
	}

	scrolled = atomic.AddUint64(&d.scrolled, uint64(len(hits)))
	if d.count > 0 && scrolled >= d.count {
		return errCountReached
	}

	return nil
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

func (d *dumper) sleepForThrottling(ctx context.Context, reqDuration time.Duration) {
	if d.throttle <= 0 {
		return
	}

	delay := time.Duration(d.throttle * float32(reqDuration))

	// make sure we don't sleep for more than the scroll context timeout, or
	// even get too close to it, as we must avoid it expiring
	maxDelay := 3 * d.scrollTimeout / 4
	if delay > maxDelay {
		delay = maxDelay
	}

	// use time.After instead of time.Sleep so it can be canceled by the context
	select {
	case <-ctx.Done():
	case <-time.After(delay):
	}
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
	err = d.sendHits(hits)
	return resp.GetScrollID(), resp.GetTotal(), len(hits) == d.size, err
}
