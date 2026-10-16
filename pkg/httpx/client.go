package httpx

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"time"
)

func NewClient(timeout time.Duration) *http.Client {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &http.Client{
		Timeout:       timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

func Post(ctx context.Context, client *http.Client, url string, headers map[string]string, payload []byte) (int, time.Duration, error) {
	start := time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return 0, time.Since(start), err
	}
	req.Header.Set("Content-Type", "application/json")
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, time.Since(start), err
	}
	defer resp.Body.Close()
	_, err = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	return resp.StatusCode, time.Since(start), err
}
