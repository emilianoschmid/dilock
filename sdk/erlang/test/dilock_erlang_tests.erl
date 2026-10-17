%% EUnit tests through the public Elixir BEAM modules; no backend is required.
-module(dilock_erlang_tests).
-include_lib("eunit/include/eunit.hrl").

defaults_test() ->
    ?assertEqual(#{wait_ms => 5000, lease_ms => 30000}, dilock_erlang:defaults()).

identity_vector_test() ->
    {ok, Id} = 'Elixir.Dilock.Identity':resource_id(
        <<"default">>, <<"camera/123/reindex">>),
    ?assertEqual(<<"e0cdd484a99b6aee4162335939d2a2d128a490c097069fd85b2fc1e540a940b6">>, Id),
    ?assertEqual(<<"dilock:{e0cdd484a99b6aee4162335939d2a2d128a490c097069fd85b2fc1e540a940b6}:lease">>,
        'Elixir.Dilock.Identity':redis_key(Id)).

invalid_identity_test() ->
    ?assertMatch({error, _}, 'Elixir.Dilock.Identity':resource_id(<<"Upper">>, <<"resource">>)),
    ?assertMatch({error, _}, 'Elixir.Dilock.Identity':resource_id(<<"default">>, <<>>)).

utf8_boundary_test() ->
    {ok, _Lock} = dilock_erlang:lock(self(), "default", "camera/42"),
    %% A supplementary Unicode scalar is encoded as strict UTF-8 at the facade
    %% boundary and remains a valid resource identity input.
    {ok, _UnicodeLock} = dilock_erlang:lock(self(), "default", [16#1F642]),
    ?assertEqual({error, invalid_unicode}, dilock_erlang:lock(self(), [16#D800], "camera/42")),
    ?assertEqual({error, invalid_unicode}, dilock_erlang:lock(self(), "default", [16#D800])),
    ?assertEqual({error, invalid_unicode}, dilock_erlang:lock(self(), <<16#FF>>, "camera/42")),
    ?assertEqual({error, invalid_unicode}, dilock_erlang:lock(self(), "default", <<16#FF>>)),
    ?assertEqual(
        {error, invalid_unicode},
        dilock_erlang:start_redis([
            {endpoint, [16#D800]},
            {namespace, "default"},
            {epoch, "test-epoch"}
        ])
    ),
    ?assertEqual(
        {error, invalid_unicode},
        dilock_erlang:start_etcd([
            {endpoint, <<16#FF>>},
            {namespace, "default"},
            {epoch, "test-epoch"}
        ])
    ),
    %% start_link reports validation errors and also sends an exit signal to its
    %% linked caller. Trap and drain that signal so EUnit can assert the return.
    PreviousTrap = process_flag(trap_exit, true),
    try
        ?assertMatch({error, invalid_configuration}, dilock_erlang:start_redis([{endpoint, 42}])),
        receive {'EXIT', _Pid, invalid_configuration} -> ok after 0 -> ok end
    after
        process_flag(trap_exit, PreviousTrap)
    end.

provider_options_shape_test() ->
    ?assertEqual({error, invalid_configuration}, dilock_erlang:start_redis(not_a_list)),
    ?assertEqual({error, invalid_configuration}, dilock_erlang:start_etcd([{endpoint, "localhost:2379"} | invalid_tail])),
    ?assertEqual({error, invalid_configuration}, dilock_erlang:start_redis([endpoint])),
    ?assertEqual(
        {error, invalid_configuration},
        dilock_erlang:start_redis([{cleanup_timout_ms, 1000}])
    ),
    ?assertEqual(
        {error, invalid_configuration},
        dilock_erlang:start_etcd([{cleanup_timout_ms, 1000}])
    ).

duplicate_provider_option_test() ->
    DuplicateEndpoint = [
        {endpoint, "redis://127.0.0.1:6379"},
        {endpoint, "redis://127.0.0.1:6380"},
        {epoch, "test-epoch"}
    ],
    ?assertEqual({error, invalid_configuration}, dilock_erlang:start_redis(DuplicateEndpoint)),
    ?assertEqual(
        {error, invalid_configuration},
        dilock_erlang:start_etcd([
            {endpoint, "http://127.0.0.1:2379"},
            {namespace, "default"},
            {epoch, "first-epoch"},
            {epoch, "second-epoch"}
        ])
    ),
    ?assertEqual(
        {error, invalid_configuration},
        dilock_erlang:start_redis([
            {endpoint, "redis://127.0.0.1:6379"},
            {epoch, "test-epoch"},
            {rpc_timeout_ms, 1000},
            {rpc_timeout_ms, 2000}
        ])
    ),
    ?assertEqual(
        {error, invalid_configuration},
        dilock_erlang:start_consul([
            {endpoint, "http://127.0.0.1:8500"},
            {epoch, "first-epoch"},
            {token, "first-token"},
            {token, "second-token"}
        ])
    ).

cleanup_observer_option_test() ->
    Observer = fun(_Event) -> ok end,
    RedisSpec = dilock_erlang:redis_child_spec([
        {endpoint, "redis://127.0.0.1:6379"},
        {epoch, "observer-epoch"},
        {cleanup_observer, Observer}
    ]),
    { 'Elixir.Dilock.Provider', start_link, [Options] } = maps:get(start, RedisSpec),
    ?assertEqual(Observer, proplists:get_value(cleanup_observer, Options)),
    EtcdSpec = dilock_erlang:etcd_child_spec([
        {endpoint, "http://127.0.0.1:2379"},
        {epoch, "observer-epoch"},
        {cleanup_observer, Observer}
    ]),
    { 'Elixir.Dilock.Provider', start_link, [EtcdOptions] } = maps:get(start, EtcdSpec),
    ?assertEqual(Observer, proplists:get_value(cleanup_observer, EtcdOptions)),
    ConsulSpec = dilock_erlang:consul_child_spec([
        {endpoint, "http://127.0.0.1:8500/"},
        {epoch, "consul-epoch"},
        {token, "consul-token"}
    ]),
    { 'Elixir.Dilock.Provider', start_link, [ConsulOptions] } = maps:get(start, ConsulSpec),
    ?assertEqual(<<"consul-token">>, proplists:get_value(token, ConsulOptions)),
    ?assertEqual('Elixir.Dilock.Consul', proplists:get_value(backend, ConsulOptions)),
    ?assertEqual(
        {error, invalid_configuration},
        dilock_erlang:redis_child_spec([
            {endpoint, "redis://127.0.0.1:6379"},
            {epoch, "observer-epoch"},
            {cleanup_observer, not_a_function}
        ])
    ),
    ?assertEqual(
        {error, invalid_configuration},
        dilock_erlang:etcd_child_spec([
            {endpoint, "http://127.0.0.1:2379"},
            {epoch, "observer-epoch"},
            {cleanup_observer, fun() -> ok end}
        ])
    ).

with_acquired_lease_reports_unknown_cleanup_test() ->
    Lease = fake_lease(unknown),
    ?assertEqual(
        {error, {cleanup_not_acknowledged, unknown, completed}},
        dilock_erlang:with_acquired_lease(Lease, fun(_Lease) -> completed end)
    ).

with_acquired_lease_rejects_already_not_owned_cleanup_test() ->
    Lease = fake_lease(already_not_owned),
    ?assertEqual(
        {error, {lease_lost, already_not_owned, completed}},
        dilock_erlang:with_acquired_lease(Lease, fun(_Lease) -> completed end)
    ).

with_acquired_lease_preserves_body_error_and_exposes_unknown_cleanup_test() ->
    Lease = fake_lease(unknown),
    ?assertException(
        error,
        {dilock_cleanup_unknown, work_failed, unknown},
        dilock_erlang:with_acquired_lease(
            Lease,
            fun(_Lease) -> erlang:error(work_failed) end
        )
    ).

with_acquired_lease_preserves_body_error_when_cleanup_is_acknowledged_test() ->
    Lease = fake_lease(released),
    ?assertException(
        error,
        work_failed,
        dilock_erlang:with_acquired_lease(
            Lease,
            fun(_Lease) -> erlang:error(work_failed) end
        )
    ).

with_acquired_lease_preserves_body_error_when_cleanup_confirms_not_owned_test() ->
    Lease = fake_lease(already_not_owned),
    ?assertException(
        error,
        work_failed,
        dilock_erlang:with_acquired_lease(
            Lease,
            fun(_Lease) -> erlang:error(work_failed) end
        )
    ).

close_lease_releases_before_stopping_the_owner_monitor_test() ->
    ReleasedLease = fake_lease(released),
    ?assertEqual(released, dilock_erlang:close_lease(ReleasedLease)),
    UnknownLease = fake_lease(unknown),
    ?assertEqual(unknown, dilock_erlang:close_lease(UnknownLease)).

fake_lease(Outcome) ->
    spawn(fun() -> fake_lease_loop(Outcome) end).

fake_lease_loop(Outcome) ->
    receive
        {'$gen_call', {From, Tag}, release} ->
            From ! {Tag, Outcome},
            fake_lease_loop(Outcome);
        {'$gen_call', {From, Tag}, close} ->
            From ! {Tag, ok},
            ok
    end.

public_facade_exports_test() ->
    {module, dilock_erlang} = code:ensure_loaded(dilock_erlang),
    ?assert(erlang:function_exported(dilock_erlang, start_redis, 1)),
    ?assert(erlang:function_exported(dilock_erlang, start_etcd, 1)),
    ?assert(erlang:function_exported(dilock_erlang, start_consul, 1)),
    ?assert(erlang:function_exported(dilock_erlang, redis_child_spec, 1)),
    ?assert(erlang:function_exported(dilock_erlang, etcd_child_spec, 1)),
    ?assert(erlang:function_exported(dilock_erlang, consul_child_spec, 1)),
    ?assert(erlang:function_exported(dilock_erlang, ownership, 1)),
    ?assert(erlang:function_exported(dilock_erlang, with_lease, 2)),
    ?assert(erlang:function_exported(dilock_erlang, with_acquired_lease, 2)),
    ?assert(erlang:function_exported(dilock_erlang, fence, 1)).

provider_child_spec_test() ->
    RedisOptions = [
        {endpoint, "redis://127.0.0.1:6379"},
        {namespace, "jobs"},
        {epoch, "test-epoch"},
        {rpc_timeout_ms, 1200}
    ],
    RedisSpec = dilock_erlang:redis_child_spec(RedisOptions),
    ?assertEqual(worker, maps:get(type, RedisSpec)),
    ?assertMatch({'Elixir.Dilock.Provider', _}, maps:get(id, RedisSpec)),
    { 'Elixir.Dilock.Provider', start_link, [RedisStartOptions] } = maps:get(start, RedisSpec),
    ?assertEqual(<<"redis://127.0.0.1:6379">>, proplists:get_value(endpoint, RedisStartOptions)),
    ?assertEqual(<<"jobs">>, proplists:get_value(namespace, RedisStartOptions)),
    ?assertEqual(<<"test-epoch">>, proplists:get_value(epoch, RedisStartOptions)),
    ?assertEqual('Elixir.Dilock.Redis', proplists:get_value(backend, RedisStartOptions)),
    EtcdSpec = dilock_erlang:etcd_child_spec([
        {endpoint, "http://127.0.0.1:2379"},
        {epoch, "etcd-epoch"}
    ]),
    { 'Elixir.Dilock.Provider', start_link, [EtcdStartOptions] } = maps:get(start, EtcdSpec),
    ?assertEqual('Elixir.Dilock.Etcd', proplists:get_value(backend, EtcdStartOptions)),
    ConsulSpec = dilock_erlang:consul_child_spec([
        {endpoint, "http://127.0.0.1:8500"},
        {namespace, "jobs"},
        {epoch, "consul-epoch"},
        {token, "consul-token"}
    ]),
    { 'Elixir.Dilock.Provider', start_link, [ConsulStartOptions] } = maps:get(start, ConsulSpec),
    ?assertEqual(<<"consul-token">>, proplists:get_value(token, ConsulStartOptions)),
    ?assertEqual('Elixir.Dilock.Consul', proplists:get_value(backend, ConsulStartOptions)),
    ?assertEqual({error, invalid_configuration}, dilock_erlang:redis_child_spec(not_a_list)),
    ?assertEqual(
        {error, invalid_configuration},
        dilock_erlang:etcd_child_spec([{endpoint, "http://127.0.0.1:2379"}, {unknown, true}])
    ).
