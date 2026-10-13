%% The facade uses the shared Elixir/BEAM runtime. The protected resource
%% must atomically enforce the returned epoch/revision fence.
-module(main).
-export([run/3]).

run(Endpoint, Epoch, Resource) ->
    {ok, Provider} = dilock_erlang:start_etcd([
        {endpoint, Endpoint},
        {namespace, "default"},
        {epoch, Epoch}
    ]),
    try
        {ok, Lock} = dilock_erlang:lock(Provider, "default", Resource),
        dilock_erlang:with_lease(Lock, fun(Lease) ->
            Ownership = dilock_erlang:ownership(Lease),
            {ok, Fence} = dilock_erlang:fence(Ownership),
            do_fenced_work(Fence)
        end),
        {ok, MonthlyLock} = dilock_erlang:lock(Provider, "default", "reports/monthly"),
        Policy = #{wait_ms => 12000, lease_ms => 45000},
        dilock_erlang:with_lease(MonthlyLock, Policy, fun(Lease) ->
            Ownership = dilock_erlang:ownership(Lease),
            {ok, Fence} = dilock_erlang:fence(Ownership),
            do_fenced_work(Fence)
        end)
    after
        gen_server:stop(Provider)
    end.

do_fenced_work(_Fence) -> ok.
