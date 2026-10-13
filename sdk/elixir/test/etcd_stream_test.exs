defmodule Dilock.EtcdStreamTestAdapter do
  def cancel(stream) do
    send(stream.channel.adapter_payload.test_pid, :stream_cancelled)
    :ok
  end
end

defmodule Dilock.EtcdStreamTestStub do
  def lease_keep_alive(channel) do
    send(channel.adapter_payload.test_pid, :stub_opened_with_channel_only)
    channel.adapter_payload.stream
  end
end

defmodule Dilock.EtcdStreamTest do
  use ExUnit.Case, async: true

  test "single keepalive sends one request, reads one response, then cancels the bidi stream" do
    parent = self()
    response = %{id: 17, ttl: 30}

    interface = %{
      send_request: fn stream, request, _opts ->
        send(parent, {:sent, request})
        stream
      end,
      receive_data: fn _stream, opts ->
        send(parent, {:received, opts})
        {:ok, Stream.map([{:ok, response}], & &1)}
      end
    }

    channel = %GRPC.Channel{
      adapter: Dilock.EtcdStreamTestAdapter,
      adapter_payload: %{test_pid: parent}
    }

    stream = %GRPC.Client.Stream{channel: channel, __interface__: interface}
    channel = %{channel | adapter_payload: %{test_pid: parent, stream: stream}}
    request = %{id: 17}

    assert {:ok, ^response} =
             Dilock.Etcd.Streaming.invoke(
               Dilock.EtcdStreamTestStub,
               :lease_keep_alive,
               channel,
               request,
               37
             )

    assert_received :stub_opened_with_channel_only
    assert_received {:sent, ^request}
    assert_received {:received, receive_options}
    assert Keyword.fetch!(receive_options, :timeout) == 37
    assert_received :stream_cancelled
  end

  test "single keepalive cancels its stream after an empty response" do
    parent = self()

    interface = %{
      send_request: fn stream, _request, _opts -> stream end,
      receive_data: fn _stream, _opts -> {:ok, Stream.map([], & &1)} end
    }

    channel = %GRPC.Channel{
      adapter: Dilock.EtcdStreamTestAdapter,
      adapter_payload: %{test_pid: parent}
    }

    stream = %GRPC.Client.Stream{channel: channel, __interface__: interface}

    assert {:error, :empty_keepalive_stream} = Dilock.Etcd.Streaming.request(stream, %{}, 37)
    assert_received :stream_cancelled
  end
end
