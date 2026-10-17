# frozen_string_literal: true

require 'dilock'

epoch = ENV.fetch('DILOCK_CONSUL_EPOCH')
provider = Dilock::ConsulProvider.connect(
  ENV.fetch('CONSUL_HTTP_ADDR', 'http://127.0.0.1:8500'),
  namespace: ENV.fetch('DILOCK_NAMESPACE', 'default'),
  epoch:,
  token: ENV['CONSUL_HTTP_TOKEN']
)

begin
  provider.lock('example-resource').with_lease do |lease|
    puts "Consul fence: #{lease.fence.epoch}/#{lease.fence.lock_index}"
  end
ensure
  provider.close
end
