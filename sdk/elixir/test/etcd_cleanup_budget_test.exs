defmodule Dilock.EtcdCleanupBudgetTest do
  use ExUnit.Case, async: true

  test "revoke ACK without Unlock ACK reports already not owned after positive preflight" do
    Process.put(:dilock_fake_now, 1_000)

    now = fn -> Process.get(:dilock_fake_now) end

    unlock = fn budget ->
      assert budget == 100
      Process.put(:dilock_fake_now, 1_065)
      {:error, :unlock_ack_lost}
    end

    revoke = fn budget ->
      assert budget == 35
      Process.put(:dilock_fake_now, 1_090)
      :ok
    end

    assert :already_not_owned = Dilock.Etcd.release_with_budget(100, now, unlock, revoke)
  end

  test "Released requires exact ownership, Unlock ACK, and positive Revoke ACK" do
    now = fn -> 1_250 end
    ownership_check = fn _budget -> :owned end
    unlock = fn _budget -> {:ok, %{}} end
    revoke = fn _budget -> :ok end

    assert :released =
             Dilock.Etcd.release_with_budget(100, now, ownership_check, unlock, revoke)
  end

  test "unlock ACK without positive revoke ACK stays unknown" do
    Process.put(:dilock_fake_now, 1_500)
    now = fn -> Process.get(:dilock_fake_now) end
    unlock = fn _budget -> :ok end
    revoke = fn _budget -> {:error, :revoke_ack_lost} end

    assert :unknown = Dilock.Etcd.release_with_budget(100, now, unlock, revoke)
  end

  test "gRPC Unlock response stays unknown without positive Revoke ACK" do
    Process.put(:dilock_fake_now, 1_550)
    now = fn -> Process.get(:dilock_fake_now) end
    unlock = fn _budget -> {:ok, %{}} end
    revoke = fn _budget -> {:error, :revoke_ack_lost} end

    assert :unknown = Dilock.Etcd.release_with_budget(100, now, unlock, revoke)
  end

  test "Revoke NOT_FOUND reports already not owned even after Unlock ACK" do
    now = fn -> 1_650 end
    ownership_check = fn _budget -> :owned end
    unlock = fn _budget -> :ok end
    revoke = fn _budget -> :already_not_owned end

    assert :already_not_owned =
             Dilock.Etcd.release_with_budget(100, now, ownership_check, unlock, revoke)
  end

  test "release is unknown when neither unlock nor lease revoke is acknowledged" do
    Process.put(:dilock_fake_now, 1_750)
    now = fn -> Process.get(:dilock_fake_now) end
    unlock = fn _budget -> {:error, :unlock_ack_lost} end
    revoke = fn _budget -> {:error, :revoke_ack_lost} end

    assert :unknown = Dilock.Etcd.release_with_budget(100, now, unlock, revoke)
  end

  test "revoke ACK without unlock ACK reports already not owned" do
    Process.put(:dilock_fake_now, 1_900)
    now = fn -> Process.get(:dilock_fake_now) end
    unlock = fn _budget -> raise "unlock failed before acknowledgement" end
    revoke = fn _budget -> :ok end

    assert :already_not_owned = Dilock.Etcd.release_with_budget(100, now, unlock, revoke)
  end

  test "unlock ACK without revoke ACK remains unknown" do
    Process.put(:dilock_fake_now, 2_100)
    now = fn -> Process.get(:dilock_fake_now) end
    unlock = fn _budget -> :ok end
    revoke = fn _budget -> raise "revoke failed after acknowledged unlock" end

    assert :unknown = Dilock.Etcd.release_with_budget(100, now, unlock, revoke)
  end

  test "cleanup skips revoke when unlock consumes the shared deadline" do
    Process.put(:dilock_fake_now, 2_000)

    now = fn -> Process.get(:dilock_fake_now) end

    unlock = fn budget ->
      assert budget == 50
      Process.put(:dilock_fake_now, 2_050)
      {:error, :deadline_exceeded}
    end

    revoke = fn _budget -> flunk("revoke must not start after the cleanup deadline") end

    assert :unknown = Dilock.Etcd.release_with_budget(50, now, unlock, revoke)
  end

  test "release does not unlock a missing key after ownership preflight" do
    now = fn -> 3_000 end

    ownership_check = fn budget ->
      assert budget == 100
      :already_not_owned
    end

    unlock = fn _budget -> flunk("must not unlock a missing key") end

    revoke = fn budget ->
      assert budget == 100
      :ok
    end

    assert :already_not_owned =
             Dilock.Etcd.release_with_budget(100, now, ownership_check, unlock, revoke)
  end

  test "release preflight classifies an absent exact key as already not owned" do
    assert :already_not_owned =
             Dilock.Etcd.classify_lock_ownership(%{kvs: []}, "/locks/k", 123)
  end

  test "unique-lease revoke ACK without exact-key proof reports already-not-owned" do
    now = fn -> 3_500 end
    ownership_check = fn _budget -> :unknown end
    unlock = fn _budget -> flunk("must not unlock without confirmed ownership") end
    revoke = fn _budget -> :ok end

    assert :already_not_owned =
             Dilock.Etcd.release_with_budget(100, now, ownership_check, unlock, revoke)
  end

  test "lease not found confirms already not owned even when preflight was unknown" do
    now = fn -> 3_600 end
    ownership_check = fn _budget -> :unknown end
    unlock = fn _budget -> flunk("must not unlock without confirmed ownership") end
    revoke = fn _budget -> :already_not_owned end

    assert :already_not_owned =
             Dilock.Etcd.release_with_budget(100, now, ownership_check, unlock, revoke)
  end

  test "release does not unlock a key owned by a different lease" do
    now = fn -> 4_000 end
    ownership_check = fn _budget -> :already_not_owned end
    unlock = fn _budget -> flunk("must not unlock the successor's key") end
    revoke = fn _budget -> :ok end

    assert :already_not_owned =
             Dilock.Etcd.release_with_budget(100, now, ownership_check, unlock, revoke)
  end

  test "release preflight preserves a successor key owned by another lease" do
    successor = %{key: "/locks/k", lease: 456}

    assert :already_not_owned =
             Dilock.Etcd.classify_lock_ownership(%{kvs: [successor]}, "/locks/k", 123)
  end

  test "revoking an already expired etcd lease is an acknowledged not-owned result" do
    assert :already_not_owned =
             Dilock.Etcd.classify_revoke_result(
               {:error, %GRPC.RPCError{status: GRPC.Status.not_found()}}
             )
  end

  test "uncertain revoke error stays an error" do
    assert {:error, %GRPC.RPCError{status: 14}} =
             Dilock.Etcd.classify_revoke_result(
               {:error, %GRPC.RPCError{status: GRPC.Status.unavailable()}}
             )
  end

  test "late keepalive cannot revive a lease after its previous deadline" do
    assert Dilock.Etcd.renewal_deadlines_valid?(1_000, 10_000, 1_001) == false
    assert Dilock.Etcd.renewal_deadlines_valid?(2_000, 10_000, 1_999) == true
    assert Dilock.Etcd.renewal_deadlines_valid?(2_000, 1_500, 1_999) == false
  end

  test "etcd ownership verification cannot return a lease after the acquisition wait deadline" do
    assert :ok = Dilock.Etcd.wait_budget_available(2_000, 1_999)
    assert {:error, :acquisition_timeout} = Dilock.Etcd.wait_budget_available(2_000, 2_000)
    assert {:error, :acquisition_timeout} = Dilock.Etcd.wait_budget_available(2_000, 2_001)
  end
end
