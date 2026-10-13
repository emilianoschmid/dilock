package org.dilock.etcd;

import io.etcd.jetcd.api.KVGrpc;
import io.etcd.jetcd.api.LeaseGrpc;
import io.etcd.jetcd.api.lock.LockGrpc;
import io.etcd.jetcd.api.lock.LockRequest;
import io.etcd.jetcd.api.lock.LockResponse;
import io.etcd.jetcd.api.lock.UnlockRequest;
import org.junit.jupiter.api.Test;
import org.dilock.redis.DilockException;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EtcdProtocolTest {
    @Test void usesNativeEtcdV3GrpcServiceNames() {
        assertEquals("etcdserverpb.KV/Range", KVGrpc.getRangeMethod().getFullMethodName());
        assertEquals("etcdserverpb.Lease/LeaseGrant", LeaseGrpc.getLeaseGrantMethod().getFullMethodName());
        assertEquals("etcdserverpb.Lease/LeaseRevoke", LeaseGrpc.getLeaseRevokeMethod().getFullMethodName());
        assertEquals("etcdserverpb.Lease/LeaseKeepAlive", LeaseGrpc.getLeaseKeepAliveMethod().getFullMethodName());
        assertEquals("v3lockpb.Lock/Lock", LockGrpc.getLockMethod().getFullMethodName());
        assertEquals("v3lockpb.Lock/Unlock", LockGrpc.getUnlockMethod().getFullMethodName());
    }

    @Test void retainsNativeOpaqueKeyAndLeaseWireFields() {
        assertEquals(1, LockRequest.getDescriptor().findFieldByName("name").getNumber());
        assertEquals(2, LockRequest.getDescriptor().findFieldByName("lease").getNumber());
        assertEquals(2, LockResponse.getDescriptor().findFieldByName("key").getNumber());
        assertEquals(1, UnlockRequest.getDescriptor().findFieldByName("key").getNumber());
    }

    @Test void namespaceMetadataRejectsCoercibleButWrongJsonTypes() {
        assertDoesNotThrow(() -> EtcdProvider.validateMetadataValue(metadata("1", "\"epoch\"").getBytes(StandardCharsets.UTF_8), "epoch"));
        assertProtocolMismatch(metadata("1.0", "\"epoch\""));
        assertProtocolMismatch(metadata("1.5", "\"epoch\""));
        assertProtocolMismatch(metadata("4294967297", "\"epoch\""));
        assertProtocolMismatch(metadata("\"1\"", "\"epoch\""));
        assertProtocolMismatch(metadata("1", "123"));
        assertProtocolMismatch(metadata("1", "\"epoch\"").replace("\"protocol_major\":1", "\"protocol_major\":1,\"protocol_major\":1"));
        assertDoesNotThrow(() -> EtcdProvider.validateMetadataValue(metadata("1", "\"epoch\"").replace(
                "\"fixed_lease_ms\":null", "\"fixed_lease_ms\":null,\"extension\":{\"x\":1,\"x\":2}").getBytes(StandardCharsets.UTF_8), "epoch"));
    }

    private static String metadata(String protocolMajor, String epoch) {
        String raw = "{\"protocol_major\":" + protocolMajor
                + ",\"identity_scheme\":\"sha256-framed-utf8-v1\",\"profile\":\"etcd-native-lock-v1\""
                + ",\"coordination_epoch\":" + epoch + ",\"fixed_lease_ms\":null}";
        return raw;
    }

    private static void assertProtocolMismatch(String raw) {
        DilockException error = assertThrows(DilockException.class, () -> EtcdProvider.validateMetadataValue(raw.getBytes(StandardCharsets.UTF_8), "epoch"));
        assertEquals(DilockException.Kind.PROTOCOL_MISMATCH, error.kind());
    }
}
