%% Thin Erlang/BEAM facade. Distributed protocol and backend code stay in Elixir.
-module(dilock_erlang).

-export([
    defaults/0,
    start_redis/1,
    start_etcd/1,
    redis_child_spec/1,
    etcd_child_spec/1,
    lock/3,
    try_acquire/1,
    try_acquire/2,
    acquire/1,
    acquire/2,
    ownership/1,
    release/1,
    renew/2,
    close_lease/1,
    with_acquired_lease/2,
    with_lease/2,
    with_lease/3,
    fence/1
]).

%% Match the shared Elixir defaults: five-second wait, thirty-second lease.
defaults() -> 'Elixir.Dilock':defaults().

%% Start these processes under an OTP supervisor in application code.
start_redis(Options) ->
    start_provider(
        'Elixir.Dilock.Redis',
        Options,
        [endpoint, namespace, epoch, rpc_timeout_ms, cleanup_timeout_ms, cleanup_observer, name]
    ).
start_etcd(Options) ->
    start_provider(
        'Elixir.Dilock.Etcd',
        Options,
        [endpoint, namespace, epoch, rpc_timeout_ms, cleanup_timeout_ms, cleanup_observer, name]
    ).

%% Return the backend's standard OTP child specification without starting it.
%% This lets an Erlang application keep provider lifetime under its supervisor.
redis_child_spec(Options) ->
    provider_child_spec(
        'Elixir.Dilock.Redis',
        Options,
        [endpoint, namespace, epoch, rpc_timeout_ms, cleanup_timeout_ms, cleanup_observer, name]
    ).

etcd_child_spec(Options) ->
    provider_child_spec(
        'Elixir.Dilock.Etcd',
        Options,
        [endpoint, namespace, epoch, rpc_timeout_ms, cleanup_timeout_ms, cleanup_observer, name]
    ).

lock(Provider, Namespace, Resource) when is_pid(Provider) ->
    case {utf8(Namespace), utf8(Resource)} of
        {{ok, NamespaceUtf8}, {ok, ResourceUtf8}} ->
            'Elixir.Dilock':lock(Provider, NamespaceUtf8, ResourceUtf8);
        _ ->
            {error, invalid_unicode}
    end.

try_acquire(Lock) -> try_acquire(Lock, defaults()).
try_acquire(Lock, Options) -> 'Elixir.Dilock.Lock':try_acquire(Lock, Options).

acquire(Lock) -> acquire(Lock, defaults()).
acquire(Lock, Options) -> 'Elixir.Dilock.Lock':acquire(Lock, Options).

ownership(Lease) when is_pid(Lease) -> 'Elixir.Dilock.Lease':ownership(Lease).
release(Lease) when is_pid(Lease) -> 'Elixir.Dilock.Lease':release(Lease).
renew(Lease, TtlMilliseconds) when is_pid(Lease), is_integer(TtlMilliseconds) ->
    'Elixir.Dilock.Lease':renew(Lease, TtlMilliseconds).
%% Closing an active lease without cleanup would discard its owner monitor and
%% leave the backend lock until TTL expiry. Make the public close operation
%% release first and return that outcome so callers cannot mistake local process
%% shutdown for acknowledged distributed cleanup.
close_lease(Lease) when is_pid(Lease) ->
    Outcome = safe_release(Lease),
    _ = safe_close(Lease),
    Outcome.

with_lease(Lock, Work) when is_function(Work, 1) ->
    with_lease(Lock, defaults(), Work).
with_lease(Lock, Options, Work) when is_map(Options), is_function(Work, 1) ->
    case acquire(Lock, Options) of
        {ok, Lease} -> with_acquired_lease(Lease, Work);
        Other -> Other
    end.

%% Scope an already-acquired lease. If work raises and bounded release is
%% unconfirmed, change only the exception reason to include both the original
%% reason and the cleanup outcome; keep its class and original stack trace.
with_acquired_lease(Lease, Work) when is_pid(Lease), is_function(Work, 1) ->
    WorkResult =
        try {ok, Work(Lease)}
        catch CatchClass:CatchReason:CatchStack ->
            {raised, CatchClass, CatchReason, CatchStack}
        end,
    Cleanup = safe_release(Lease),
    _ = safe_close(Lease),
    case {WorkResult, Cleanup} of
        {{raised, Class, Reason, Stack}, unknown} ->
            erlang:raise(Class, {dilock_cleanup_unknown, Reason, unknown}, Stack);
        {{raised, Class, Reason, Stack}, released} ->
            erlang:raise(Class, Reason, Stack);
        {{raised, Class, Reason, Stack}, already_not_owned} ->
            erlang:raise(Class, Reason, Stack);
        {{raised, Class, Reason, Stack}, Outcome} ->
            erlang:raise(Class, {dilock_cleanup_not_acknowledged, Reason, Outcome}, Stack);
        {{ok, Value}, released} -> {ok, Value};
        %% The backend confirmed that this acquisition is no longer owned.
        %% Keep that distinct from an unacknowledged cleanup failure, matching
        %% Dilock.with_lease/3 and allowing callers to identify a lost lease.
        {{ok, Value}, already_not_owned} -> {error, {lease_lost, already_not_owned, Value}};
        {{ok, Value}, Outcome} -> {error, {cleanup_not_acknowledged, Outcome, Value}}
    end.

safe_release(Lease) ->
    try release(Lease)
    catch _:_ -> unknown
    end.

safe_close(Lease) ->
    try close_lease_process(Lease)
    catch _:_ -> ok
    end.

close_lease_process(Lease) -> 'Elixir.Dilock.Lease':close(Lease).

fence(Ownership) when is_map(Ownership) -> 'Elixir.Dilock.Etcd':fence(Ownership).

start_provider(Module, Options, AllowedKeys) when is_list(Options) ->
    case normalize_options(Options, AllowedKeys) of
        {ok, Normalized} -> Module:start_link(Normalized);
        {error, _} = Error -> Error
    end;
start_provider(_Module, _Options, _AllowedKeys) ->
    {error, invalid_configuration}.

provider_child_spec(Module, Options, AllowedKeys) when is_list(Options) ->
    case normalize_options(Options, AllowedKeys) of
        {ok, Normalized} -> Module:child_spec(Normalized);
        {error, _} = Error -> Error
    end;
provider_child_spec(_Module, _Options, _AllowedKeys) ->
    {error, invalid_configuration}.

normalize_options(Options, AllowedKeys) ->
    normalize_options(Options, AllowedKeys, []).

normalize_options([], _AllowedKeys, _SeenKeys) -> {ok, []};
normalize_options([{Key, _Value} = Entry | Rest], AllowedKeys, SeenKeys)
        when is_atom(Key) ->
    case lists:member(Key, SeenKeys) of
        true -> {error, invalid_configuration};
        false ->
            case normalize_option(Entry, AllowedKeys) of
                {ok, Normalized} ->
                    case normalize_options(Rest, AllowedKeys, [Key | SeenKeys]) of
                        {ok, Tail} -> {ok, [Normalized | Tail]};
                        {error, _} = Error -> Error
                    end;
                {error, _} = Error -> Error
            end
    end;
normalize_options([Entry | Rest], AllowedKeys, SeenKeys) ->
    case normalize_option(Entry, AllowedKeys) of
        {ok, Normalized} ->
            case normalize_options(Rest, AllowedKeys, SeenKeys) of
                {ok, Tail} -> {ok, [Normalized | Tail]};
                {error, _} = Error -> Error
            end;
        {error, _} = Error -> Error
    end;
normalize_options(_ImproperTail, _AllowedKeys, _SeenKeys) ->
    {error, invalid_configuration}.

normalize_option({Key, Value}, AllowedKeys) when is_atom(Key) ->
    case lists:member(Key, AllowedKeys) of
        false -> {error, invalid_configuration};
        true -> normalize_allowed_option(Key, Value)
    end;
normalize_option(_Entry, _AllowedKeys) -> {error, invalid_configuration}.

normalize_allowed_option(endpoint, Value) -> normalize_string_option(endpoint, Value);
normalize_allowed_option(namespace, Value) -> normalize_string_option(namespace, Value);
normalize_allowed_option(epoch, Value) -> normalize_string_option(epoch, Value);
normalize_allowed_option(cleanup_observer, Value) when is_function(Value, 1) ->
    {ok, {cleanup_observer, Value}};
normalize_allowed_option(cleanup_observer, _Value) -> {error, invalid_configuration};
normalize_allowed_option(Key, Value) -> {ok, {Key, Value}}.

normalize_string_option(Key, Value) when is_binary(Value); is_list(Value) ->
    case utf8(Value) of
        {ok, Utf8} -> {ok, {Key, Utf8}};
        {error, _} = Error -> Error
    end;
normalize_string_option(Key, Value) ->
    {ok, {Key, Value}}.

utf8(Value) when is_binary(Value); is_list(Value) ->
    try unicode:characters_to_binary(Value) of
        Binary when is_binary(Binary) -> {ok, Binary};
        _ -> {error, invalid_unicode}
    catch
        error:_ -> {error, invalid_unicode}
    end;
utf8(_) -> {error, invalid_unicode}.
