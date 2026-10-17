# Erlang consumer facade for Dilock

Erlang reuses the existing Elixir/BEAM SDK. This directory is an Erlang-facing
consumer facade and examples, not a second Redis, etcd or Consul implementation and not
a separately buildable/published package. Calls delegate to the Elixir modules
in `../elixir`; generated gRPC stubs and all distributed-lock semantics remain
owned by that SDK.

After the Dilock Elixir application and its dependencies are built in a Mix
release or application, add their BEAM paths to the Erlang runtime and compile
`src/dilock_erlang.erl`. Valid Erlang binaries and Unicode charlists for
endpoint, namespace and resource are converted to UTF-8 binaries at the
boundary. Invalid Unicode returns `{error, invalid_unicode}` before calling the
Elixir runtime. The API otherwise returns the same tagged tuples and Elixir
lease process PIDs as the underlying modules.

`with_lease/2,3` scopes acquisition and cleanup in the Erlang facade. If the
work function raises and cleanup returns `unknown`, the facade re-raises the
same Erlang exception class with reason
`{dilock_cleanup_unknown, OriginalReason, unknown}` and preserves the original
stack trace. Other unconfirmed cleanup outcomes use
`{dilock_cleanup_not_acknowledged, OriginalReason, Outcome}`. This keeps the
work failure and uncertain ownership visible together. On a normal return,
only `released` returns `{ok, Value}`. `already_not_owned` means the lease was
no longer owned at cleanup and returns
`{error, {lease_lost, already_not_owned, Value}}`; unknown and other
unacknowledged outcomes use `{error, {cleanup_not_acknowledged, Outcome,
Value}}`. This matches the underlying Elixir API and distinguishes confirmed
ownership loss from an uncertain release. Use
`with_acquired_lease/2` to apply the same behavior to a lease acquired earlier.

For a manually acquired lease, `release/1` returns the backend cleanup outcome.
`close_lease/1` also attempts release before stopping the local lease process,
and returns that release outcome. This prevents a direct close from discarding
the owner monitor while a backend lease may still be active.

Provider options must be a proper keyword list with atom keys. A non-list,
improper list, or malformed entry returns `{error, invalid_configuration}`
before a provider process starts. Only `endpoint`, `namespace`, `epoch`,
`token`, `rpc_timeout_ms`, `cleanup_timeout_ms`, `cleanup_observer`, and the standard
OTP `name` option are accepted; unknown keys are rejected so misspelled safety
settings cannot be silently ignored. `cleanup_observer` must be a unary
function and is invoked asynchronously with a map containing `backend`,
`outcome`, and `reason` when cleanup is not acknowledged. Duplicate keys are
rejected too, so repeated endpoints, epochs, or timeout values cannot select
an ambiguous configuration.

For an OTP application, use `redis_child_spec/1`, `etcd_child_spec/1` or
`consul_child_spec/1` in the
supervisor's child list. They return the backend's standard child-spec map
without starting a provider; the supervisor owns its lifetime. Invalid or
ambiguous provider options return `{error, invalid_configuration}` before a
child spec is created:

```erlang
init([]) ->
    Children = [dilock_erlang:redis_child_spec([
        {endpoint, "redis://127.0.0.1:6379"},
        {namespace, "billing"},
        {epoch, application:get_env(my_app, dilock_epoch, "billing-v1")}
    ])],
    {ok, {#{strategy => one_for_one}, Children}}.
```

The child spec does not provision namespace metadata or connect during
supervisor initialization. The backend validates configuration and metadata
when the supervisor starts the child.

`test/dilock_erlang_tests.erl` is an EUnit consumer test for default options,
identity vectors, validation, standard OTP child specs, thin delegates, and
scope cleanup outcomes.
`examples/dilock_erlang_redis_example.erl` and
`examples/dilock_erlang_etcd_example.erl` show supervised-provider lifetimes
and `try ... after` cleanup. They need real backends and pre-provisioned
metadata. `examples/dilock_erlang_consul_example.erl` demonstrates the Consul
facade and `LockIndex` fence. Run the EUnit suite after changing the facade or
its underlying Elixir runtime.

## Redis quick start

The endpoint and approved epoch are required. Omitting `namespace` selects
`default`; omitting acquisition options uses a five-second wait and
thirty-second lease. Namespace metadata must be provisioned beforehand. This
stand-alone example stops the provider in `after`; in an OTP application,
start it from the application's supervisor and let that supervisor own its
lifetime.

```erlang
run(Endpoint, Epoch, Resource) ->
    {ok, Provider} = dilock_erlang:start_redis([
        {endpoint, Endpoint},
        {epoch, Epoch}
    ]),
    try
        {ok, Lock} = dilock_erlang:lock(Provider, "default", Resource),
        dilock_erlang:with_lease(Lock, fun(Lease) ->
            case dilock_erlang:renew(Lease, 30000) of
                ok -> do_bounded_work();
                {error, _} = Error -> Error
            end
        end)
    after
        gen_server:stop(Provider)
    end.
```

## Explicit policy and backend settings

Provider endpoints, namespace and epoch identify the pre-provisioned authority.
RPC and cleanup budgets are optional backend settings; wait and lease are
per-acquisition settings. The `try ... after` scopes close the lease through
`with_lease/3` and stop this stand-alone provider even when work raises:

```erlang
run_configured(Endpoint, Epoch, Resource) ->
    {ok, Provider} = dilock_erlang:start_redis([
        {endpoint, Endpoint},
        {namespace, "billing"},
        {epoch, Epoch},
        {rpc_timeout_ms, 2000},
        {cleanup_timeout_ms, 3000}
    ]),
    try
        {ok, Lock} = dilock_erlang:lock(Provider, "billing", Resource),
        Policy = #{wait_ms => 12000, lease_ms => 45000},
        dilock_erlang:with_lease(Lock, Policy, fun(Lease) ->
            case dilock_erlang:renew(Lease, 45000) of
                ok -> do_bounded_idempotent_work();
                {error, _} = Error -> Error
            end
        end)
    after
        gen_server:stop(Provider)
    end.
```

Errors and ordinary contention remain distinct tagged results. Lease expiry
cannot stop a paused process; Redis does not provide strong fencing. The etcd
facade exposes an epoch-scoped revision fence; Consul exposes an epoch-scoped
`LockIndex` fence. The protected resource must compare either fence atomically.

For the underlying runtime and backend guarantees, see
[`../elixir/README.md`](../elixir/README.md). The facade does not define an
independent package or claim production support.
