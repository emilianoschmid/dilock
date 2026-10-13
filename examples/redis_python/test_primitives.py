"""Adapter unit tests with a scripted command port, NOT Redis integration tests."""
import unittest
from redis_primitives import RedisPrimitiveAdapter, AcquisitionIdentity, _RELEASE, _RENEW

class ScriptedPort:
    def __init__(self, *replies):
        self.replies = list(replies)
        self.calls = []
    async def execute_command(self, *args):
        self.calls.append(args)
        reply = self.replies.pop(0)
        if isinstance(reply, BaseException):
            raise reply
        return reply

class PrimitiveTests(unittest.IsolatedAsyncioTestCase):
    async def test_atomic_claim_and_busy(self):
        port = ScriptedPort(b"OK", None)
        adapter = RedisPrimitiveAdapter(port)
        identity = adapter.new_identity("test", "resource")
        result = await adapter.claim(identity, 1000)
        self.assertTrue(result.accepted)
        self.assertGreaterEqual(result.elapsed_seconds, 0)
        self.assertFalse((await adapter.claim(adapter.new_identity("test", "resource"), 1000)).accepted)
        self.assertEqual(port.calls[0], ("SET", identity.key, identity.owner, "NX", "PX", 1000))

    async def test_canonical_release_and_renew(self):
        port = ScriptedPort(1, 0, 1)
        adapter = RedisPrimitiveAdapter(port)
        identity = adapter.new_identity("test", "resource")
        self.assertTrue(await adapter.release(identity))
        self.assertFalse(await adapter.release(identity))
        self.assertTrue(await adapter.renew(identity, 500))
        self.assertEqual(port.calls[0], ("EVAL", _RELEASE, 1, identity.key, identity.owner))
        self.assertEqual(port.calls[2], ("EVAL", _RENEW, 1, identity.key, identity.owner, 500))

    async def test_transport_error_is_not_busy_and_not_retried(self):
        port = ScriptedPort(ConnectionError("response lost"))
        adapter = RedisPrimitiveAdapter(port)
        with self.assertRaises(ConnectionError):
            await adapter.claim(adapter.new_identity("test", "resource"), 1000)
        self.assertEqual(len(port.calls), 1)

    async def test_validation_precedes_io(self):
        port = ScriptedPort()
        adapter = RedisPrimitiveAdapter(port)
        identity = adapter.new_identity("test", "resource")
        for ttl in [0, -1, True, 1.5, 2_147_483_648]:
            with self.subTest(ttl=ttl), self.assertRaises(ValueError):
                await adapter.claim(identity, ttl)
        with self.assertRaises(ValueError):
            await adapter.release(AcquisitionIdentity(identity.key, "wrong-owner"))
        self.assertEqual(port.calls, [])

    async def test_unexpected_reply_is_not_contention(self):
        port = ScriptedPort(False)
        adapter = RedisPrimitiveAdapter(port)
        with self.assertRaises(RuntimeError):
            await adapter.claim(adapter.new_identity("test", "resource"), 1000)

    def test_identity_and_redaction(self):
        a = RedisPrimitiveAdapter.new_identity("test", "resource")
        b = RedisPrimitiveAdapter.new_identity("test", "resource")
        self.assertEqual(a.key, b.key)
        self.assertNotEqual(a.owner, b.owner)
        self.assertNotIn(a.owner, repr(a))

if __name__ == "__main__":
    unittest.main()
