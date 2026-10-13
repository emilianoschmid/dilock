%% The returned fence only helps if the protected resource checks it atomically.
-module(dilock_erlang_etcd_example).
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
        end)
    after
        gen_server:stop(Provider)
    end.

do_fenced_work(_Fence) -> ok.
