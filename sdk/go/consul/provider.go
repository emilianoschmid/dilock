// Package consul implements Dilock's consul-kv-session-v1 profile.
package consul

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	dilock "example.invalid/dilock/core"
)

const profile = "consul-kv-session-v1"

// ProviderOptions fixes the metadata namespace and coordinator incarnation.
type ProviderOptions struct {
	Namespace, Epoch string
	Token string
	HTTPClient *http.Client
	RPCTimeout, CleanupTimeout time.Duration
	DefaultAcquisition dilock.AcquireOptions
	CleanupObserver func(dilock.CleanupObservation)
}

// Provider owns the HTTP client only when it creates the client itself.
type Provider struct {
	endpoint *url.URL
	options ProviderOptions
	client *http.Client
	ownedClient bool
	mu sync.Mutex
	closed bool
	leases map[*Lease]struct{}
}

// NewProvider validates and opens one Consul authority, including metadata.
func NewProvider(ctx context.Context, endpoint string, options ProviderOptions) (*Provider, error) {
	if ctx == nil { return nil, dilock.ErrInvalidOptions }
	if options.Namespace == "" { options.Namespace = "default" }
	if _, err := dilock.Identity(options.Namespace, "validation"); err != nil { return nil, err }
	if len(options.Epoch) == 0 || len(options.Epoch) > 1024 || !utf8.ValidString(options.Epoch) || strings.IndexByte(options.Epoch, 0) >= 0 { return nil, dilock.ErrInvalidOptions }
	if options.Token != "" && (strings.ContainsAny(options.Token, " \t\r\n") || !isPrintableASCII(options.Token)) { return nil, dilock.ErrInvalidOptions }
	if options.RPCTimeout == 0 { options.RPCTimeout = 2*time.Second }
	if options.CleanupTimeout == 0 { options.CleanupTimeout = 2*time.Second }
	if options.RPCTimeout <= 0 || options.CleanupTimeout <= 0 || options.RPCTimeout > 24*time.Hour || options.CleanupTimeout > 24*time.Hour { return nil, dilock.ErrInvalidOptions }
	policy, err := options.DefaultAcquisition.Normalized()
	if err != nil { return nil, err }
	options.DefaultAcquisition = policy
	if _, err = roundTTL(policy.Lease.LeaseDuration); err != nil { return nil, err }
	endpointURL, err := parseEndpoint(endpoint)
	if err != nil { return nil, err }
	client := options.HTTPClient
	owned := false
	if client == nil {
		client = &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		owned = true
	} else {
		copy := *client
		copy.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
		client = &copy
	}
	p := &Provider{endpoint: endpointURL, options: options, client: client, ownedClient: owned, leases: make(map[*Lease]struct{})}
	if err := p.readMetadata(ctx, options.RPCTimeout); err != nil { if owned { client.CloseIdleConnections() }; return nil, err }
	return p, nil
}

// ProvisionNamespace creates exact metadata only with Consul KV CAS=0.
func ProvisionNamespace(ctx context.Context, endpoint string, options ProviderOptions) error {
	if options.Namespace == "" { options.Namespace = "default" }
	if _, err := dilock.Identity(options.Namespace, "validation"); err != nil { return err }
	if len(options.Epoch) == 0 || len(options.Epoch) > 1024 || !utf8.ValidString(options.Epoch) || strings.IndexByte(options.Epoch, 0) >= 0 { return dilock.ErrInvalidOptions }
	if options.RPCTimeout == 0 { options.RPCTimeout = 2*time.Second }
	if options.RPCTimeout <= 0 { return dilock.ErrInvalidOptions }
	base, err := parseEndpoint(endpoint); if err != nil { return err }
	client := options.HTTPClient
	if client == nil { client = &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }} }
        copy := *client
        copy.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
        client = &copy
	p := &Provider{endpoint: base, options: options, client: client}
	meta := map[string]any{"protocol_major": 1, "identity_scheme": "sha256-framed-utf8-v1", "profile": profile, "coordination_epoch": options.Epoch, "fixed_lease_ms": nil}
	body, _ := json.Marshal(meta)
	value, err := p.request(ctx, http.MethodPut, "v1/kv/dilock/namespaces/"+options.Namespace+"/meta", url.Values{"cas": []string{"0"}}, body, options.RPCTimeout, true)
	if err != nil { return err }
	if ok, valid := value.(bool); !valid { return dilock.ErrProtocolMismatch } else if ok { return nil }
	return p.readMetadata(ctx, options.RPCTimeout)
}

func parseEndpoint(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.Path != "" && u.Path != "/" || u.RawQuery != "" || u.Fragment != "" {
		return nil, dilock.ErrInvalidOptions
	}
	u.Path, u.RawPath = "", ""
	return u, nil
}

func (p *Provider) request(parent context.Context, method, path string, query url.Values, body []byte, timeout time.Duration, mutation bool) (any, error) {
	if parent == nil { return nil, dilock.ErrInvalidOptions }
	ctx, cancel := context.WithTimeout(parent, timeout); defer cancel()
	u := *p.endpoint
	u.Path = "/" + strings.TrimLeft(path, "/")
	u.RawQuery = query.Encode()
	var reader io.Reader
	if body != nil { reader = bytes.NewReader(body) }
	req, err := http.NewRequestWithContext(ctx, method, u.String(), reader)
	if err != nil { return nil, classifyRequestError(mutation, err) }
	req.Header.Set("Accept", "application/json")
	if body != nil { req.Header.Set("Content-Type", "application/json") }
	if p.options.Token != "" { req.Header.Set("X-Consul-Token", p.options.Token) }
	resp, err := p.client.Do(req)
	if err != nil { return nil, classifyRequestError(mutation, err) }
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if !mutation && resp.StatusCode == http.StatusNotFound { return nil, dilock.ErrProtocolMismatch }
		if mutation && resp.StatusCode >= 500 { return nil, &dilock.OperationError{Kind: dilock.ErrAcquisitionUnknown, Message: "Consul mutation acknowledgement is unknown"} }
		return nil, fmt.Errorf("Consul HTTP status %d", resp.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 65537))
	if err != nil { return nil, classifyRequestError(mutation, err) }
	if len(raw) > 65536 { return nil, dilock.ErrProtocolMismatch }
	if len(raw) == 0 { return nil, nil }
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		if mutation { return nil, &dilock.OperationError{Kind: dilock.ErrAcquisitionUnknown, Message: "Consul mutation response was malformed", Cause: err} }
		return nil, dilock.ErrProtocolMismatch
	}
	return value, nil
}

func classifyRequestError(mutation bool, cause error) error {
	if mutation { return &dilock.OperationError{Kind: dilock.ErrAcquisitionUnknown, Message: "Consul mutation acknowledgement was not confirmed", Cause: cause} }
	return &dilock.OperationError{Kind: dilock.ErrBackendUnavailable, Message: "Consul HTTP request failed", Cause: cause}
}

func (p *Provider) readMetadata(ctx context.Context, timeout time.Duration) error {
	key := "dilock/namespaces/"+p.options.Namespace+"/meta"
	value, err := p.request(ctx, http.MethodGet, "v1/kv/"+key, url.Values{"consistent": []string{""}}, nil, timeout, false)
	if err != nil { return err }
	rows, ok := value.([]any); if !ok || len(rows) != 1 { return dilock.ErrProtocolMismatch }
	row, ok := rows[0].(map[string]any); if !ok || row["Key"] != key { return dilock.ErrProtocolMismatch }
	encoded, ok := row["Value"].(string); if !ok { return dilock.ErrProtocolMismatch }
	raw, err := base64.StdEncoding.DecodeString(encoded); if err != nil || len(raw) == 0 || len(raw) > 8192 || hasDuplicateJSONField(raw) { return dilock.ErrProtocolMismatch }
	var m map[string]json.RawMessage
	if json.Unmarshal(raw, &m) != nil || len(m) != 5 { return dilock.ErrProtocolMismatch }
	var major int; var scheme, gotProfile, epoch string; var fixed json.RawMessage
	if json.Unmarshal(m["protocol_major"], &major) != nil || json.Unmarshal(m["identity_scheme"], &scheme) != nil || json.Unmarshal(m["profile"], &gotProfile) != nil || json.Unmarshal(m["coordination_epoch"], &epoch) != nil { return dilock.ErrProtocolMismatch }
	fixed = m["fixed_lease_ms"]
	if major != 1 || scheme != "sha256-framed-utf8-v1" || gotProfile != profile || epoch != p.options.Epoch || string(fixed) != "null" { return dilock.ErrProtocolMismatch }
	return nil
}

func hasDuplicateJSONField(raw []byte) bool {
	d := json.NewDecoder(bytes.NewReader(raw)); t, err := d.Token(); if err != nil || t != json.Delim('{') { return true }
	seen := map[string]bool{}
	for d.More() { t, err = d.Token(); if err != nil { return true }; key, ok := t.(string); if !ok || seen[key] { return true }; seen[key] = true; var value json.RawMessage; if d.Decode(&value) != nil { return true } }
	_, err = d.Token(); if err != nil { return true }; var extra any
	return d.Decode(&extra) != io.EOF
}

func isPrintableASCII(s string) bool { for i := 0; i < len(s); i++ { if s[i] < 33 || s[i] > 126 { return false } }; return true }

func roundTTL(ttl time.Duration) (time.Duration, error) {
	if ttl < 10*time.Second || ttl > 24*time.Hour { return 0, dilock.ErrInvalidOptions }
	rounded := ((ttl + time.Second - 1) / time.Second) * time.Second
	if rounded > 24*time.Hour { return 0, dilock.ErrInvalidOptions }
	return rounded, nil
}

// Close releases tracked leases and closes idle connections only when owned.
func (p *Provider) Close() error {
	p.mu.Lock(); if p.closed { p.mu.Unlock(); return nil }; p.closed = true
	leases := make([]*Lease, 0, len(p.leases)); for l := range p.leases { leases = append(leases, l) }; p.mu.Unlock()
	for _, l := range leases { ctx, cancel := context.WithTimeout(context.Background(), p.options.CleanupTimeout); _, _ = l.Release(ctx); cancel() }
	if p.ownedClient { p.client.CloseIdleConnections() }; return nil
}

func (p *Provider) addLease(l *Lease) bool { p.mu.Lock(); defer p.mu.Unlock(); if p.closed { return false }; p.leases[l] = struct{}{}; return true }
func (p *Provider) removeLease(l *Lease) { p.mu.Lock(); delete(p.leases, l); p.mu.Unlock() }
func (p *Provider) observe(outcome dilock.ReleaseOutcome, reason string) { if p.options.CleanupObserver == nil { return }; defer func(){ _ = recover() }(); p.options.CleanupObserver(dilock.CleanupObservation{Backend:"consul", Outcome:outcome, Reason:reason}) }

// Lock creates a reusable local resource handle without network activity.
func (p *Provider) Lock(resource string) (*Handle, error) {
	p.mu.Lock(); closed := p.closed; p.mu.Unlock(); if closed { return nil, dilock.ErrProviderClosed }
	id, err := dilock.Identity(p.options.Namespace, resource); if err != nil { return nil, err }
	return &Handle{provider:p, key:"dilock/locks/"+id}, nil
}

// Handle addresses one canonical draft 1 resource.
type Handle struct { provider *Provider; key string }

// Fence is valid only for the configured epoch and retained key.
type Fence struct { Epoch string; LockIndex uint64 }

func (p *Provider) acquire(ctx context.Context, key string, wait time.Duration, options dilock.Options, waitForLock bool) (*Lease, error) {
	if ctx == nil { return nil, dilock.ErrInvalidOptions }
	policy, _, margin, err := options.Complete(); if err != nil { return nil, err }
	effective, err := roundTTL(policy.LeaseDuration); if err != nil { return nil, err }
	if waitForLock && (wait <= 0 || wait > 24*time.Hour) { return nil, dilock.ErrInvalidOptions }
	started := time.Now(); waitDeadline := started.Add(wait)
	createTimeout := p.options.RPCTimeout
	if waitForLock { if left := time.Until(waitDeadline); left < createTimeout { createTimeout = left } }
	if createTimeout <= 0 { return nil, dilock.ErrWaitTimeout }
	session, sent, err := p.createSession(ctx, effective, createTimeout); if err != nil { return nil, err }
	deadline := sent.Add(effective-margin); lastRenew := sent
	cleanup := func() error { return p.abandon(key, session) }
	for {
		if err := ctx.Err(); err != nil { if cleanup() != nil { return nil, &dilock.OperationError{Kind:dilock.ErrAcquisitionUnknown, Message:"Consul canceled acquisition cleanup unconfirmed", Cause:err} }; return nil, err }
		if !time.Now().Before(deadline) { _ = cleanup(); return nil, dilock.ErrAcquisitionUnknown }
		if waitForLock && !time.Now().Before(waitDeadline) { if cleanup()!=nil { return nil,dilock.ErrAcquisitionUnknown }; return nil,dilock.ErrWaitTimeout }
		budget := p.options.RPCTimeout; if waitForLock { if left:=time.Until(waitDeadline); left<budget { budget=left } }
		v, err := p.request(ctx,http.MethodPut,"v1/kv/"+key,url.Values{"acquire":[]string{session}},[]byte{},budget,true)
		if err != nil { _=cleanup(); return nil,err }
		granted, ok := v.(bool); if !ok { _=cleanup(); return nil,dilock.ErrProtocolMismatch }
		if granted {
			verifyTimeout := p.options.RPCTimeout
			if waitForLock { if left := time.Until(waitDeadline); left < verifyTimeout { verifyTimeout = left } }
			if verifyTimeout <= 0 { _=cleanup(); return nil,dilock.ErrAcquisitionUnknown }
			owner, index, err := p.owner(ctx,key,verifyTimeout); if err != nil || owner!=session || index==0 || !time.Now().Before(deadline) || (waitForLock && !time.Now().Before(waitDeadline)) { _=cleanup(); return nil,dilock.ErrAcquisitionUnknown }
			l := &Lease{provider:p,key:key,session:session,index:index,ttl:effective,margin:margin,deadline:deadline,options:policy, state:dilock.Held}
			if !p.addLease(l) { _=cleanup(); return nil,dilock.ErrProviderClosed }; l.activate(); return l,nil
		}
		if !waitForLock { if cleanup()!=nil { return nil,dilock.ErrAcquisitionUnknown }; return nil,nil }
		pause:=100*time.Millisecond; if left:=time.Until(waitDeadline); left<pause { pause=left }; timer:=time.NewTimer(pause); select { case <-ctx.Done(): timer.Stop(); continue; case <-timer.C: }
		if !time.Now().Before(waitDeadline) { continue }
		if time.Since(lastRenew)>=effective*2/3 { sent=time.Now(); if err:=p.renewSession(ctx,session,p.options.RPCTimeout); err!=nil { _=cleanup(); return nil,dilock.ErrAcquisitionUnknown }; lastRenew=sent; deadline=sent.Add(effective-margin) }
	}
}

func (p *Provider) createSession(ctx context.Context, ttl, timeout time.Duration) (string,time.Time,error) {
	sent:=time.Now(); body,_:=json.Marshal(map[string]any{"Name":"dilock lease","Behavior":"release","LockDelay":"0s","TTL":fmt.Sprintf("%ds",int(ttl/time.Second)),"Checks":[]any{}})
	v,err:=p.request(ctx,http.MethodPut,"v1/session/create",nil,body,timeout,true); if err!=nil{return "",sent,err}
	m,ok:=v.(map[string]any); if !ok{return "",sent,dilock.ErrProtocolMismatch}; id,ok:=m["ID"].(string); if !ok||id==""{return "",sent,&dilock.OperationError{Kind:dilock.ErrAcquisitionUnknown,Message:"Consul may have created a session without returning its identifier"}}; return id,sent,nil
}
func (p *Provider) renewSession(ctx context.Context, id string, timeout time.Duration) error { v,err:=p.request(ctx,http.MethodPut,"v1/session/renew/"+url.PathEscape(id),nil,nil,timeout,true); if err!=nil{return err}; rows,ok:=v.([]any); if !ok||len(rows)!=1{return dilock.ErrProtocolMismatch}; m,ok:=rows[0].(map[string]any); if !ok||m["ID"]!=id{return dilock.ErrProtocolMismatch}; return nil }
func (p *Provider) owner(ctx context.Context,key string,timeout time.Duration)(string,uint64,error){ v,err:=p.request(ctx,http.MethodGet,"v1/kv/"+key,url.Values{"consistent":[]string{""}},nil,timeout,false); if err!=nil{return "",0,err}; rows,ok:=v.([]any); if !ok||len(rows)!=1{return "",0,dilock.ErrProtocolMismatch}; m,ok:=rows[0].(map[string]any); if !ok||m["Key"]!=key{return "",0,dilock.ErrProtocolMismatch}; session,_:=m["Session"].(string); index,ok:=m["LockIndex"].(float64); if !ok||index<0{return "",0,dilock.ErrProtocolMismatch}; return session,uint64(index),nil }
func (p *Provider) abandon(key,session string)error{ _,e:=p.request(context.Background(),http.MethodPut,"v1/kv/"+key,url.Values{"release":[]string{session}},[]byte{},p.options.CleanupTimeout,true); _,d:=p.request(context.Background(),http.MethodPut,"v1/session/destroy/"+url.PathEscape(session),nil,nil,p.options.CleanupTimeout,true); if e!=nil{return e}; return d }

// TryAcquire returns (nil,nil) only for acknowledged contention.
func (h *Handle) TryAcquire(ctx context.Context, options dilock.Options)(*Lease,error){ return h.provider.acquire(ctx,h.key,0,options,false) }
// Acquire waits for the complete finite wait budget.
func (h *Handle) Acquire(ctx context.Context, wait time.Duration, options dilock.Options)(*Lease,error){ return h.provider.acquire(ctx,h.key,wait,options,true) }
// AcquireDefault applies the provider's immutable defaults.
func (h *Handle) AcquireDefault(ctx context.Context)(*Lease,error){ o:=h.provider.options.DefaultAcquisition; return h.Acquire(ctx,o.Wait,o.Lease) }

var errReleaseUnknown = errors.New("Consul cleanup outcome unknown")
