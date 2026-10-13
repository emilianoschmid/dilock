%% Minimal consumer; start the provider as a child in production supervision.
-module(dilock_erlang_redis_example).
-export([run/3]).

run(Endpoint, Epoch, Resource) ->
    {ok, Provider} = dilock_erlang:start_redis([
        {endpoint, Endpoint},
        {namespace, "default"},
        {epoch, Epoch}
    ]),
    try
        {ok, Lock} = dilock_erlang:lock(Provider, "default", Resource),
        dilock_erlang:with_lease(Lock, fun(Lease) ->
            ok = dilock_erlang:renew(Lease, 30000),
            do_bounded_work()
        end)
    after
        gen_server:stop(Provider)
    end.

do_bounded_work() -> ok.
