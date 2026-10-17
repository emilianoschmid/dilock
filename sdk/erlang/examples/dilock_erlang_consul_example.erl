-module(dilock_erlang_consul_example).
-export([run/3]).

%% Run after explicitly provisioning the matching namespace metadata.
run(Endpoint, Epoch, Resource) ->
    TokenOption = case os:getenv("DILOCK_CONSUL_TOKEN") of
        false -> [];
        Token -> [{token, Token}]
    end,
    {ok, Provider} = dilock_erlang:start_consul([
        {endpoint, Endpoint},
        {namespace, "billing"},
        {epoch, Epoch}
    ] ++ TokenOption),
    try
        {ok, Lock} = dilock_erlang:lock(Provider, "billing", Resource),
        Policy = #{wait_ms => 5000, lease_ms => 10000},
        dilock_erlang:with_lease(Lock, Policy, fun(Lease) ->
            case dilock_erlang:fence(dilock_erlang:ownership(Lease)) of
                {ok, Fence} ->
                    %% The protected resource must atomically enforce this fence.
                    {ok, Fence};
                {error, _} = Error -> Error
            end
        end)
    after
        gen_server:stop(Provider)
    end.
