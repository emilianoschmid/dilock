package org.dilock.kotlin.worker

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class WorkerProtocolTest {
    @Test
    fun parsesKnownRequestAndPreservesTextFields() {
        val request = parseRequest("""{"id":"r1","op":"acquire","resource":"a/b","wait_ms":0,"lease_ms":2500}""")
        assertEquals("r1", request.path("id").asText())
        assertEquals("a/b", request.path("resource").asText())
        assertEquals(2500, request.path("lease_ms").asLong())
    }

    @Test
    fun rejectsInvalidEnvelopeFieldsAndNumbers() {
        assertFailsWith<IllegalArgumentException> { parseRequest("[]") }
        assertFailsWith<IllegalArgumentException> { parseRequest("""{"id":"r","op":"acquire","resource":"x","wait_ms":-1}""") }
        assertFailsWith<IllegalArgumentException> { parseRequest("""{"id":"r","op":"acquire","resource":"x","surprise":true}""") }
        assertFailsWith<IllegalArgumentException> { parseRequest("""{"id":"r","op":"acquire","resource":"x","rpc_ms":300}""") }
        assertFailsWith<IllegalArgumentException> { parseRequest("""{"id":"r","op":"made_up"}""") }
    }

    @Test
    fun enforcesInputIdentifierLimit() {
        val longId = "x".repeat(129)
        assertFailsWith<IllegalArgumentException> { parseRequest("""{"id":"$longId","op":"hello"}""") }
    }
}
