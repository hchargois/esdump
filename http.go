package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/charmbracelet/log"
)

type httpClient struct {
	*http.Client
	baseURL string
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
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if d.noCompression {
		transport.DisableCompression = true
	}

	transport.TLSClientConfig = &tls.Config{}
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

func (cl httpClient) Do(ctx context.Context, method, path string, body string) (int, []byte, error) {
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
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, fmt.Errorf("reading response body: %w", err)
	}
	return resp.StatusCode, respBody, nil
}

func (cl httpClient) Get(ctx context.Context, path string, body string) (int, []byte, error) {
	return cl.Do(ctx, http.MethodGet, path, body)
}

func (cl httpClient) Delete(ctx context.Context, path string, body string) (int, []byte, error) {
	return cl.Do(ctx, http.MethodDelete, path, body)
}
