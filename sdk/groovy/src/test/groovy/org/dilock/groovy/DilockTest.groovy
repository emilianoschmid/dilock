package org.dilock.groovy

import groovy.transform.CompileStatic
import org.dilock.etcd.EtcdLease
import org.dilock.etcd.EtcdProvider
import org.dilock.redis.AcquireOptions
import org.junit.jupiter.api.Test

import java.util.concurrent.atomic.AtomicBoolean

import static org.junit.jupiter.api.Assertions.*

final class DilockTest {
    @CompileStatic
    private static String etcdAutoRenewConsumer(EtcdProvider provider) {
        Dilock.withEtcdLock(provider, 'resource', true) { EtcdLease lease ->
            lease.assertValid()
            'renewed'
        }
    }

    @CompileStatic
    private static String configuredEtcdAutoRenewConsumer(EtcdProvider provider) {
        Dilock.withEtcdLock(provider, 'resource', new AcquireOptions(12_000L, 45_000L), true) { EtcdLease lease ->
            lease.assertValid()
            'renewed'
        }
    }

    private static final class FakeLease {
        final Closure action

        FakeLease(Closure action) { this.action = action }

        Object release() { action.call() }
    }

    private enum ForeignReleaseOutcome { RELEASED }

    @Test
    void closesResourceAfterNormalBodyReturn() {
        def closed = new AtomicBoolean()
        def resource = [close: { closed.set(true) }] as AutoCloseable

        def result = Dilock.withCloseable(resource) { value ->
            assertSame(resource, value)
            'done'
        }

        assertEquals('done', result)
        assertTrue(closed.get())
    }

    @Test
    void closesTransferredResourceWhenBodyIsNull() {
        def closed = new AtomicBoolean()
        def resource = [close: { closed.set(true) }] as AutoCloseable

        assertThrows(NullPointerException) {
            Dilock.withCloseable(resource, null)
        }

        assertTrue(closed.get(), 'invalid callback must not leak the transferred resource')
    }

    @Test
    void closesResourceWhenBodyThrowsAndPreservesBodyFailure() {
        def closed = new AtomicBoolean()
        def resource = [close: { closed.set(true) }] as AutoCloseable

        def thrown = assertThrows(IllegalStateException) {
            Dilock.withCloseable(resource) { throw new IllegalStateException('expected') }
        }

        assertEquals('expected', thrown.message)
        assertTrue(closed.get())
    }

    @Test
    void suppressesCloseFailureUnderBodyFailure() {
        def resource = [close: { throw new IOException('close failed') }] as AutoCloseable

        def thrown = assertThrows(IllegalStateException) {
            Dilock.withCloseable(resource) { throw new IllegalStateException('body failed') }
        }

        assertEquals('body failed', thrown.message)
        assertEquals('close failed', thrown.suppressed[0].message)
    }

    @Test
    void preservesBodyFailureWhenCloseRethrowsTheSameThrowable() {
        def failure = new IllegalStateException('same failure')
        def resource = [close: { throw failure }] as AutoCloseable

        def thrown = assertThrows(IllegalStateException) {
            Dilock.withCloseable(resource) { throw failure }
        }

        assertSame(failure, thrown)
        assertEquals(0, thrown.suppressed.length)
    }

    @Test
    void leaseScopeSurfacesUnknownReleaseAfterSuccessfulBody() {
        def lease = new FakeLease({ throw new IllegalStateException('release unknown') })

        def thrown = assertThrows(IllegalStateException) {
            Dilock.withLeaseScope(lease) { 'work completed' }
        }

        assertEquals('release unknown', thrown.message)
    }

    @Test
    void leaseScopeReturnsBodyValueAfterReleasedAcknowledgement() {
        def lease = new FakeLease({ org.dilock.etcd.EtcdLease.ReleaseOutcome.RELEASED })

        assertEquals('work completed', Dilock.withLeaseScope(lease) { 'work completed' })
    }

    @Test
    void leaseScopeRejectsUnrelatedEnumNamedReleased() {
        def lease = new FakeLease({ ForeignReleaseOutcome.RELEASED })

        def thrown = assertThrows(IllegalStateException) {
            Dilock.withLeaseScope(lease) { 'work completed' }
        }

        assertTrue(thrown.message.contains('RELEASED'))
    }

    @Test
    void leaseScopePreservesBodyFailureAndSuppressesUnknownRelease() {
        def lease = new FakeLease({ throw new IOException('release unknown') })

        def thrown = assertThrows(IllegalArgumentException) {
            Dilock.withLeaseScope(lease) { throw new IllegalArgumentException('work failed') }
        }

        assertEquals('work failed', thrown.message)
        assertEquals('release unknown', thrown.suppressed[0].message)
    }

    @Test
    void leaseScopeRejectsAlreadyNotOwnedOutcomeAfterSuccessfulWork() {
        def lease = new FakeLease({ org.dilock.etcd.EtcdLease.ReleaseOutcome.ALREADY_NOT_OWNED })

        def thrown = assertThrows(IllegalStateException) {
            Dilock.withLeaseScope(lease) { 'work completed' }
        }

        assertTrue(thrown.message.contains('ALREADY_NOT_OWNED'))
    }

    @Test
    void leaseScopePreservesBodyFailureAndAttachesUnreleasedOutcome() {
        def lease = new FakeLease({ org.dilock.redis.RedisLease.ReleaseOutcome.ALREADY_NOT_OWNED })
        def bodyFailure = new IllegalArgumentException('work failed')

        def thrown = assertThrows(IllegalArgumentException) {
            Dilock.withLeaseScope(lease) { throw bodyFailure }
        }

        assertSame(bodyFailure, thrown)
        assertEquals(1, thrown.suppressed.length)
        assertTrue(thrown.suppressed[0].message.contains('ALREADY_NOT_OWNED'))
    }
}
