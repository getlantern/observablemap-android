package test

import io.lantern.observablemodel.Serde
import io.lantern.observablemodel.Test.TestMessage
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
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
    fun testKryoAndMessagePack() {
        var serde = Serde()
        val obj = MsgPackMessage("name", 100, "my data".toByteArray(Charset.defaultCharset()))
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

        serde.register(1, MsgPackMessage::class.java)
        val bytesRegistered = serde.serialize(obj)
        assertEquals(
            "registered object type should be serialized with msgpack",
            'M',
            bytesRegistered[0].toChar()
        )
        assertEquals(
            "round-tripped registered msgpack object should match original",
            obj,
            serde.deserialize(bytesRegistered)
        )
        assertEquals(
            "round-tripped unregistered msgpack object should still match original",
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

        serde.register(1, TestMessage::class.java)
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

private data class MsgPackMessage(
    val name: String = "",
    val number: Int = 0,
    val data: ByteArray = ByteArray(0)
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as MsgPackMessage

        if (name != other.name) return false
        if (number != other.number) return false
        if (!data.contentEquals(other.data)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = name.hashCode()
        result = 31 * result + number
        result = 31 * result + data.contentHashCode()
        return result
    }
}