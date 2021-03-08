package io.lantern.observablemodel

/**
 * The raw bytes with the ability to obtain the deserialized value
 */
class Raw<T : Any> internal constructor(val bytes: ByteArray, get: Lazy<T>) {
    val value: T by get

    internal constructor(serde: Serde, bytes: ByteArray) : this(
        bytes,
        lazy { serde.deserialize(bytes) })

    internal constructor(bytes: ByteArray, value: T) : this(bytes, lazyOf(value))

    internal constructor(serde: Serde, value: T) : this(serde.serialize(value), value)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Raw<*>

        if (!bytes.contentEquals(other.bytes)) return false

        return true
    }

    override fun hashCode(): Int {
        return bytes.contentHashCode()
    }
}