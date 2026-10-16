package httpx

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestPostDoesNotFollowRedirect(t *testing.T) {
	followed := false
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { followed = true }))
	defer destination.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, destination.URL, http.StatusTemporaryRedirect)
	}))
	defer origin.Close()
	status, _, err := Post(context.Background(), NewClient(time.Second), origin.URL, nil, []byte(`{}`))
	if err != nil || status != http.StatusTemporaryRedirect || followed {
		t.Fatalf("status=%d err=%v followed=%v", status, err, followed)
	}
}
