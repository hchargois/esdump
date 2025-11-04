// Package main provides esdump, a CLI tool to dump Elasticsearch index documents.
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/charmbracelet/log"
	json "github.com/json-iterator/go"
)

// bufferPool is a pool of reusable buffers. Similar to sync.Pool, except it
// reliably keeps objects around forever, contrary to sync.Pool which has a
// tendency of deallocating them much too soon, which means that buffers would
// need to be reallocated and grow from their initial size much too often.
type bufferPool struct {
	elems []*bytes.Buffer
	mu    sync.Mutex
}

func (p *bufferPool) Get() *bytes.Buffer {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.elems) == 0 {
		// start with a reasonably large buffer, as we can expect responses
		// to be quite big
		return bytes.NewBuffer(make([]byte, 0, 64*1024))
	}
	elem := p.elems[len(p.elems)-1]
	p.elems = p.elems[:len(p.elems)-1]
	return elem
}

func (p *bufferPool) Put(elem *bytes.Buffer) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.elems = append(p.elems, elem)
}

type httpClient struct {
	*http.Client
	baseURL string
	bufPool bufferPool
}

func (d *dumper) certPool() *x509.CertPool {
	cp, err := x509.SystemCertPool()
	if err != nil {
		log.Warn("unable to load system cert pool", "err", err)
		cp = x509.NewCertPool()
	}

	if d.verify != "" {
		data, err := os.ReadFile(d.verify)
		if err != nil {
			log.Warn("unable to read CA cert", "file", d.verify, "err", err)
		} else {
			cp.AppendCertsFromPEM(data)
		}
	}
	return cp
}

func (d *dumper) initHTTPClient() {
	var transport *http.Transport
	if defaultTransport, ok := http.DefaultTransport.(*http.Transport); ok {
		transport = defaultTransport.Clone()
	} else {
		transport = &http.Transport{}
	}
	if d.noCompression {
		transport.DisableCompression = true
	}

	transport.TLSClientConfig = &tls.Config{
		MinVersion: tls.VersionTLS12,
	}
	if d.verify == "no" {
		log.Info("skipping TLS verification")
		transport.TLSClientConfig.InsecureSkipVerify = true
	} else {
		transport.TLSClientConfig.RootCAs = d.certPool()
	}

	d.cl = httpClient{
		Client: &http.Client{
			Timeout:   d.httpTimeout,
			Transport: transport,
		},
		baseURL: d.baseURL,
	}
}

// Do sends the request. If dst is non-nil, and the response is 200 OK,  the
// body of the response will be unmarshalled into it, and the returned byte
// array will NOT be returned (i.e. will be nil), in order for the buffer to be
// re-used for subsequent requests. If the response is anything other than 200,
// the byte array of the raw response body will be returned.
func (cl *httpClient) Do(ctx context.Context, method, path string, body string, dst any) (int, []byte, error) {
	var bodyRdr io.Reader
	if body != "" {
		bodyRdr = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, cl.baseURL+path, bodyRdr)
	if err != nil {
		return 0, nil, fmt.Errorf("creating request: %w", err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := cl.Client.Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("sending request: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			log.Warn("closing response body", "err", closeErr)
		}
	}()

	buf := cl.bufPool.Get()
	_, err = buf.ReadFrom(resp.Body)
	if err != nil {
		return 0, nil, fmt.Errorf("reading response body: %w", err)
	}

	bs := buf.Bytes()
	if resp.StatusCode != http.StatusOK {
		return resp.StatusCode, bs, nil
	}

	if dst != nil {
		err = json.Unmarshal(bs, &dst)
		if err != nil {
			return 0, nil, fmt.Errorf("unmarshaling response body %s: %w", string(bs), err)
		}
	}
	buf.Reset()
	cl.bufPool.Put(buf)
	return resp.StatusCode, nil, nil
}

func (cl *httpClient) Get(ctx context.Context, path string, body string, dst any) (int, []byte, error) {
	return cl.Do(ctx, http.MethodGet, path, body, dst)
}

func (cl *httpClient) Delete(ctx context.Context, path string, body string, dst any) (int, []byte, error) {
	return cl.Do(ctx, http.MethodDelete, path, body, dst)
}
