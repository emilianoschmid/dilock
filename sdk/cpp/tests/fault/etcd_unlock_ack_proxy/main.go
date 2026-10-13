package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"sync/atomic"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

const unlockPath = "/v3lockpb.Lock/Unlock"

type metrics struct {
	unlockRequests atomic.Int64
	unlockApplied  atomic.Int64
	unlockWithheld atomic.Int64
	revokeRequests atomic.Int64
	revokeApplied  atomic.Int64
	revokeAcks     atomic.Int64
	revokeWithheld atomic.Int64
	rangeRequests  atomic.Int64
	rangeApplied   atomic.Int64
	droppedUnlock  atomic.Bool
	droppedRevoke  atomic.Bool
}

func main() {
	upstreamText := os.Getenv("DILOCK_ETCD_UPSTREAM")
	if upstreamText == "" {
		upstreamText = "http://etcd:2379"
	}
	upstream, err := url.Parse(upstreamText)
	if err != nil || upstream.Scheme != "http" || upstream.Host == "" {
		log.Fatal("DILOCK_ETCD_UPSTREAM must be an http:// endpoint")
	}
	state := &metrics{}
	state.droppedUnlock.Store(true)
	state.droppedRevoke.Store(os.Getenv("DILOCK_DROP_REVOKE_ACK") == "1")
	transport := &http2.Transport{
		AllowHTTP: true,
		DialTLSContext: func(ctx context.Context, network, address string, _ *tls.Config) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, address)
		},
	}
	proxy := &httputil.ReverseProxy{
		Transport: transport,
		Director: func(request *http.Request) {
			request.URL.Scheme = upstream.Scheme
			request.URL.Host = upstream.Host
			request.Host = upstream.Host
			if request.URL.Path == unlockPath {
				state.unlockRequests.Add(1)
			}
			if request.URL.Path == "/etcdserverpb.Lease/LeaseRevoke" {
				state.revokeRequests.Add(1)
			}
			if request.URL.Path == "/etcdserverpb.KV/Range" {
				state.rangeRequests.Add(1)
			}
		},
		ModifyResponse: func(response *http.Response) error {
			if response.Request.URL.Path == "/etcdserverpb.KV/Range" {
				body, err := io.ReadAll(response.Body)
				if err != nil {
					return err
				}
				response.Body.Close()
				if response.Trailer.Get("Grpc-Status") == "0" {
					state.rangeApplied.Add(1)
				}
				response.Body = io.NopCloser(strings.NewReader(string(body)))
				return nil
			}
			if response.Request.URL.Path == "/etcdserverpb.Lease/LeaseRevoke" {
				body, err := io.ReadAll(response.Body)
				if err != nil {
					return err
				}
				response.Body.Close()
				if response.Trailer.Get("Grpc-Status") != "0" {
					response.Body = io.NopCloser(strings.NewReader(string(body)))
					return nil
				}
				state.revokeApplied.Add(1)
				if !state.droppedRevoke.CompareAndSwap(true, false) {
					state.revokeAcks.Add(1)
					response.Body = io.NopCloser(strings.NewReader(string(body)))
					return nil
				}
				state.revokeWithheld.Add(1)
				withholdAcknowledgement(response, "injected lost LeaseRevoke acknowledgement after etcd applied the request")
				return nil
			}
			if response.Request.URL.Path != unlockPath {
				return nil
			}

			// Consume the complete upstream gRPC response first. A successful
			// response proves etcd applied the unary request.
			body, err := io.ReadAll(response.Body)
			if err != nil {
				return err
			}
			response.Body.Close()
			if grpcStatus := response.Trailer.Get("Grpc-Status"); grpcStatus != "0" {
				response.Body = io.NopCloser(strings.NewReader(string(body)))
				return nil
			}
			state.unlockApplied.Add(1)
			if !state.droppedUnlock.CompareAndSwap(true, false) {
				response.Body = io.NopCloser(strings.NewReader(string(body)))
				return nil
			}

			// Replace exactly the first successful response with Unavailable.
			state.unlockWithheld.Add(1)
			withholdAcknowledgement(response, "injected lost Unlock acknowledgement after etcd applied the request")
			return nil
		},
		ErrorLog: log.New(os.Stderr, "proxy: ", log.LstdFlags),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/__health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.HandleFunc("/__stats", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]int64{
			"unlock_requests":     state.unlockRequests.Load(),
			"unlock_applied":      state.unlockApplied.Load(),
			"unlock_withheld":     state.unlockWithheld.Load(),
			"revoke_requests":     state.revokeRequests.Load(),
			"revoke_applied":      state.revokeApplied.Load(),
			"revoke_acknowledged": state.revokeAcks.Load(),
			"revoke_withheld":     state.revokeWithheld.Load(),
			"range_requests":      state.rangeRequests.Load(),
			"range_applied":       state.rangeApplied.Load(),
		})
	})
	mux.Handle("/", proxy)
	server := &http.Server{Addr: ":2379", Handler: h2c.NewHandler(mux, &http2.Server{})}
	log.Fatal(server.ListenAndServe())
}

func withholdAcknowledgement(response *http.Response, message string) {
	response.StatusCode = http.StatusOK
	response.Status = "200 OK"
	response.Header = make(http.Header)
	response.Header.Set("Content-Type", "application/grpc")
	response.Header.Set("Trailer", "Grpc-Status, Grpc-Message")
	response.ContentLength = -1
	response.Trailer = http.Header{
		"Grpc-Status":  []string{"14"},
		"Grpc-Message": []string{message},
	}
	response.Body = io.NopCloser(strings.NewReader(""))
}
