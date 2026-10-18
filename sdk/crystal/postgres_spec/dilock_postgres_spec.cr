require "spec"
require "../src/dilock/postgres"

describe Dilock::PostgresProvider do
  it "validates the local contract before native I/O" do
    expect_raises(ArgumentError) do
      Dilock::PostgresProvider.new("", "epoch")
    end
    expect_raises(ArgumentError) do
      Dilock::PostgresProvider.provision_namespace("postgresql://unused", "")
    end
  end
end

if dsn = ENV["DILOCK_POSTGRES_DSN"]?
  describe "Dilock Crystal PostgreSQL C ABI integration" do
    it "contends, releases and hands off across independent providers" do
      namespace = "crystal-pg-#{Random::Secure.hex(6)}"
      epoch = "crystal-pg-epoch-#{Random::Secure.hex(8)}"
      Dilock::PostgresProvider.provision_namespace(dsn, epoch, namespace)

      first_provider = Dilock::PostgresProvider.new(dsn, epoch, namespace)
      second_provider = Dilock::PostgresProvider.new(dsn, epoch, namespace)
      first_handle = first_provider.lock("crystal-postgres-live")
      second_handle = second_provider.lock("crystal-postgres-live")
      first_lease : Dilock::PostgresLease? = nil
      successor : Dilock::PostgresLease? = nil

      begin
        first_lease = first_handle.try_acquire
        first_lease.should_not be_nil
        first_lease.not_nil!.valid?.should be_true
        second_handle.try_acquire.should be_nil
        first_lease.not_nil!.release.should eq(Dilock::PostgresReleaseOutcome::Released)
        first_lease.not_nil!.close
        first_lease = nil

        successor = second_handle.try_acquire
        successor.should_not be_nil
        successor.not_nil!.release.should eq(Dilock::PostgresReleaseOutcome::Released)
      ensure
        first_lease.try(&.close)
        successor.try(&.close)
        first_handle.close
        second_handle.close
        first_provider.close
        second_provider.close
      end
    end
  end
end
