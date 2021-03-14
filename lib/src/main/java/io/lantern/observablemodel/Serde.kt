package io.lantern.observablemodel

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.protobuf.GeneratedMessageLite
import org.msgpack.jackson.dataformat.MessagePackFactory
import java.io.*
import java.nio.charset.Charset
import java.util.concurrent.ConcurrentHashMap

/**
 * Serde provides a serialization/deserialization mechanism that stores Strings as T<string>,
 * protocol buffers as P<protocol buffer serialized> and all other data as K<kryo serialized>.
 */
internal class Serde() {
    private val kryo = Kryo()
    private val registeredProtocolBufferTypeIds =
        ConcurrentHashMap<Class<GeneratedMessageLite<*, *>>, Int>()
    private val registeredProtocolBufferParsers =
        ConcurrentHashMap<Int, (InputStream) -> GeneratedMessageLite<*, *>>()
    private val registeredMsgPackTypeIds =
        ConcurrentHashMap<Class<*>, Int>()
    private val registeredMsgPackTypes =
        ConcurrentHashMap<Int, Class<*>>()

    init {
        kryo.isRegistrationRequired = false
    }

    @Synchronized
    internal fun <T> register(id: Int, type: Class<T>) {
        if (GeneratedMessageLite::class.java.isAssignableFrom(type)) {
            val pbufType = type as Class<GeneratedMessageLite<*, *>>
            val parseMethod = pbufType.getMethod("parseFrom", InputStream::class.java)
            registeredProtocolBufferTypeIds[pbufType] = id
            registeredProtocolBufferParsers[id] =
                { stream -> parseMethod.invoke(pbufType, stream) as GeneratedMessageLite<*, *> }
        } else {
            registeredMsgPackTypeIds[type] = id
            registeredMsgPackTypes[id] = type
        }
    }

    internal fun serialize(data: Any): ByteArray {
        val out = ByteArrayOutputStream()
        val dataOut = DataOutputStream(out)

        when (data) {
            is String -> {
                // Write strings in optimized format that preserves sort order
                dataOut.write(TEXT)
                dataOut.write(data.toByteArray(charset))
            }
            else -> {
                val msgPackTypeId = data?.let { registeredMsgPackTypeIds[data::class.java] }
                val pbufTypeId = data?.let { registeredProtocolBufferTypeIds[data::class.java] }
                if (msgPackTypeId != null) {
                    // write with msgpack
                    dataOut.write(MSGPACK)
                    dataOut.writeInt(msgPackTypeId)
                    ObjectMapper(MessagePackFactory()).writeValue(dataOut as OutputStream, data)
                } else if (pbufTypeId != null) {
                    // Serialize using protocol buffers
                    dataOut.write(PBUF)
                    dataOut.writeInt(pbufTypeId)
                    (data as GeneratedMessageLite<*, *>).writeTo(dataOut)
                } else {
                    // Write everything else with Kryo
                    dataOut.write(KRYO)
                    val kryoOut = Output(dataOut)
                    kryo.writeClassAndObject(kryoOut, data)
                    kryoOut.close()
                }
            }
        }

        dataOut.close()
        return out.toByteArray()
    }

    internal fun <D> deserialize(bytes: ByteArray): D {
        val dataIn = DataInputStream(ByteArrayInputStream(bytes))
        return when (dataIn.read()) {
            TEXT -> dataIn.readBytes().toString(charset) as D
            MSGPACK -> {
                val typeId = dataIn.readInt()
                msgPackMapper.readValue(dataIn as InputStream, registeredMsgPackTypes[typeId]) as D
            }
            PBUF -> {
                val typeId = dataIn.readInt()
                val pbufParser = registeredProtocolBufferParsers[typeId]
                pbufParser!!(dataIn) as D
            }
            else -> kryo.readClassAndObject(Input(dataIn)) as D
        }
    }

    companion object {
        private val TEXT = 'T'.toInt()
        private val PBUF = 'P'.toInt()
        private val MSGPACK = 'M'.toInt()
        private val KRYO = 'K'.toInt()
        private val charset = Charset.defaultCharset()
        private val msgPackMapper = ObjectMapper(MessagePackFactory())
    }
}