package io.lantern.observablemodel

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Registration
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import com.google.protobuf.GeneratedMessageLite
import java.io.*
import java.nio.charset.Charset
import java.util.concurrent.ConcurrentHashMap

/**
 * Serde provides a serialization/deserialization mechanism that stores Strings as T<string>,
 * protocol buffers as P<protocol buffer serialized> and all other data as K<kryo serialized>.
 */
internal class Serde() {
    private val kryo = Kryo()
    private val registeredProtocolBufferTypes =
        ConcurrentHashMap<Class<GeneratedMessageLite<*, *>>, Int>()
    private val registeredProtocolBufferParsers =
        ConcurrentHashMap<Int, (InputStream) -> GeneratedMessageLite<*, *>>()

    init {
        kryo.isRegistrationRequired = false
    }

    @Synchronized
    internal fun <T> register(id: Int, type: Class<T>) {
        if (id < 20) {
            // Kryo uses ids between 0-9 for primitive types, don't interfere with those. To be safe, leave extra room.
            throw IllegalArgumentException("attempted to register ID below 20")
        }
        if (GeneratedMessageLite::class.java.isAssignableFrom(type)) {
            val pbufType = type as Class<GeneratedMessageLite<*, *>>
            val parseMethod = pbufType.getMethod("parseFrom", InputStream::class.java)
            registeredProtocolBufferTypes[pbufType] = id
            registeredProtocolBufferParsers[id] =
                { stream -> parseMethod.invoke(pbufType, stream) as GeneratedMessageLite<*, *> }
        } else {
            kryo.register(Registration(type, kryo.getDefaultSerializer(type), id))
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
            is GeneratedMessageLite<*, *> -> {
                val pbufTypeId = registeredProtocolBufferTypes[data::class.java]
                if (pbufTypeId != null) {
                    // Serialize using protocol buffers
                    dataOut.write(PROTOCOL_BUFFER)
                    dataOut.writeInt(pbufTypeId)
                    data.writeTo(dataOut)
                } else {
                    // fall back to Kryo serialization for unregistered protocol buffer types
                    dataOut.write(KRYO)
                    val kryoOut = Output(dataOut)
                    kryo.writeClassAndObject(kryoOut, data)
                    kryoOut.close()
                }
            }
            else -> {
                // Write everything else with Kryo
                dataOut.write(KRYO)
                val kryoOut = Output(dataOut)
                kryo.writeClassAndObject(kryoOut, data)
                kryoOut.close()
            }
        }

        dataOut.close()
        return out.toByteArray()
    }

    internal fun <D> deserialize(bytes: ByteArray): D {
        val dataIn = DataInputStream(ByteArrayInputStream(bytes))
        return when (dataIn.read()) {
            TEXT -> dataIn.readBytes().toString(charset) as D
            PROTOCOL_BUFFER -> {
                val pbufTypeId = dataIn.readInt()
                val pbufParser = registeredProtocolBufferParsers[pbufTypeId]
                pbufParser!!(dataIn) as D
            }
            else -> kryo.readClassAndObject(Input(dataIn)) as D
        }
    }

    companion object {
        private val TEXT = 'T'.toInt()
        private val KRYO = 'K'.toInt()
        private val PROTOCOL_BUFFER = 'P'.toInt()
        private val charset = Charset.defaultCharset()
    }
}