package test

import io.lantern.observablemodel.Serde
import io.lantern.observablemodel.Test.TestMessage
import org.junit.Assert.*
import org.junit.Test
import java.nio.charset.Charset

class SerdeTest {
    @Test
    fun testString() {
        val serde = Serde()
        val bytes = serde.serialize("brian")
        assertEquals(
            "string should be serialized in optimized text format",
            "Tbrian",
            bytes.toString(Charset.defaultCharset())
        )
        val str = serde.deserialize<String>(bytes)
        assertEquals("round-tripped value should match original", "brian", str)
    }

    @Test
    fun testKryoWithAndWithoutRegistration() {
        var serde = Serde()
        val obj = KryoMessage("name", 100)
        val bytesUnregistered = serde.serialize(obj)
        assertEquals(
            "unregistered object type should be serialized with kryo",
            'K',
            bytesUnregistered[0].toChar()
        )
        assertEquals(
            "round-tripped unregistered kryo object should match original",
            obj,
            serde.deserialize(bytesUnregistered)
        )

        try {
            serde.register(19, KryoMessage::class.java)
            fail("registering an ID below 20 should have failed")
        } catch (e: IllegalArgumentException) {
            // expected
        }
        serde.register(20, KryoMessage::class.java)
        val bytesRegistered = serde.serialize(obj)
        assertEquals(
            "registered object type should be serialized with kryo",
            'K',
            bytesRegistered[0].toChar()
        )
        assertEquals(
            "round-tripped registered kryo object should match original",
            obj,
            serde.deserialize(bytesRegistered)
        )
        assertEquals(
            "round-tripped unregistered kryo object should still match original",
            obj,
            serde.deserialize(bytesUnregistered)
        )
        assertTrue(
            "serialized form of registered object should be shorter than unregistered",
            bytesRegistered.size < bytesUnregistered.size
        )
    }

    @Test
    fun testProtocolBufferObjectWithAndWithoutRegistration() {
        val serde = Serde()
        val obj = TestMessage.newBuilder().setName("name").setNumber(100).build()
        val bytesUnregistered = serde.serialize(obj)
        assertEquals(
            "unregistered object type should be serialized with kryo",
            'K',
            bytesUnregistered[0].toChar()
        )
        assertEquals(
            "round-tripped unregistered kryo object should match original",
            obj,
            serde.deserialize(bytesUnregistered)
        )

        try {
            serde.register(19, TestMessage::class.java)
            fail("registering an ID below 20 should have failed")
        } catch (e: IllegalArgumentException) {
            // expected
        }
        serde.register(20, TestMessage::class.java)
        val bytesRegistered = serde.serialize(obj)
        assertEquals(
            "registered object type should be serialized with protocol buffers",
            'P',
            bytesRegistered[0].toChar()
        )
        assertEquals(
            "round-tripped registered protocol buffer object should match original",
            obj,
            serde.deserialize(bytesRegistered)
        )
        assertEquals(
            "round-tripped unregistered kryo object should still match original",
            obj,
            serde.deserialize(bytesUnregistered)
        )
        assertTrue(
            "serialized form of registered object should be shorter than unregistered",
            bytesRegistered.size < bytesUnregistered.size
        )
    }
}

private data class KryoMessage(val name: String = "", val number: Int = 0)