package io.lantern.observablemodel

import android.content.Context
import androidx.sqlite.db.SupportSQLiteDatabase
import com.googlecode.concurrenttrees.radix.ConcurrentRadixTree
import com.googlecode.concurrenttrees.radix.node.concrete.DefaultCharSequenceNodeFactory
import kotlinx.collections.immutable.PersistentMap
import kotlinx.collections.immutable.persistentHashMapOf
import net.sqlcipher.database.SQLiteDatabase
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import kotlin.collections.ArrayList
import kotlin.collections.HashMap

data class Entry<T>(val path: String, val value: T)

data class Detail<T>(val path: String, val detailPath: String, val value: T)

/**
 * Subscriber for path update events.
 *
 * id - unique identifier for this subscriber
 * pathPrefixes - subscriber will receive notifications for all changes under these path prefixes
 */
abstract class Subscriber<T>(val id: String, vararg val pathPrefixes: String) {
    /**
     * Called when the value at the given path changes
     */
    abstract fun onUpdate(path: String, value: T)

    /**
     * Called when the value at the given path is deleted
     */
    abstract fun onDelete(path: String)
}

/**
 * Observable model provides a simple key/value store with a map-like interface. It allows the
 * registration of observers for any time that a key path changes.
 */
class ObservableModel private constructor(internal val db: SupportSQLiteDatabase) {
    internal val subscribers =
        ConcurrentRadixTree<PersistentMap<String, Subscriber<Any>>>(DefaultCharSequenceNodeFactory())
    internal val subscribersById = ConcurrentHashMap<String, Subscriber<Any>>()
    internal val serde = Serde()

    companion object {
        /**
         * Builds an ObservableModel backed by a SQLite database at the given filePath
         *
         * password - if specified, the database will be encrypted using this password
         * context - encrypted databases can only be built on Android and need a Context
         */
        fun build(
            filePath: String,
            password: String? = null,
            ctx: Context? = null
        ): ObservableModel {
            val db = if (password != null) {
                SQLiteDatabase.loadLibs(ctx)
                SQLiteDatabase.openOrCreateDatabase(filePath, password, null)
            } else {
                android.database.sqlite.SQLiteDatabase.openOrCreateDatabase(filePath, null)
            } as SupportSQLiteDatabase
            if (!db.enableWriteAheadLogging()) {
                throw Exception("Unable to enable write ahead logging")
            }
            // All data is stored in a single table that has a TEXT path and a BLOB value
            db.execSQL("CREATE TABLE IF NOT EXISTS kvstore ([path] TEXT PRIMARY KEY, [value] BLOB)")
            // Create an index on only text values to speed up detail lookups that join on path = value
            db.execSQL("CREATE INDEX IF NOT EXISTS kvstore_value_index ON kvstore(value) WHERE SUBSTR(CAST(value AS TEXT), 1, 1) = 'T'")
            return ObservableModel(db)
        }
    }

    /**
     * Registers a type for optimized serialization. Protocol Buffer types will be serialized with
     * protocol buffers, all others with Kryo.
     *
     * The id identifies the type in serialized values.
     *
     * The id MUST be an integer between 10 and 127.
     * The id MUST be consistent over time - registering the same class under different IDs will
     * cause incompatibilities with previously stored data.
     */
    fun <T> registerType(id: Int, type: Class<T>) {
        serde.register(id, type)
    }

    /**
     * Registers a subscriber for any updates to the paths matching its pathPrefix.
     *
     * If receiveInitial is true, the subscriber will immediately be called for all matching values.
     */
    @Synchronized
    fun <T> subscribe(
        subscriber: Subscriber<T>,
        receiveInitial: Boolean = true
    ) {
        if (subscribersById.putIfAbsent(subscriber.id, subscriber as Subscriber<Any>) != null) {
            throw IllegalArgumentException("subscriber with id ${subscriber.id} already registered")
        }
        subscriber.pathPrefixes.forEach { pathPrefix ->
            val subscribersForPrefix = subscribers.getValueForExactKey(pathPrefix)?.let {
                it.put(
                    subscriber.id,
                    subscriber
                )
            } ?: persistentHashMapOf(subscriber.id to subscriber)
            subscribers.put(pathPrefix, subscribersForPrefix)

            if (receiveInitial) {
                list<T>("${pathPrefix}%").forEach {
                    subscriber.onUpdate(it.path, it.value)
                }
            }
        }
    }

    /**
     * Registers a subscriber for updates to details for the paths matching its pathPrefix.
     *
     * The values corresponding to paths matching the pathPrefix are themselves treated as paths
     * with which to look up the details.
     *
     * For example, given the following data:
     *
     * {"/detail/1": "one",
     *  "/detail/2": "two",
     *  "/list/1": "/detail/2",
     *  "/list/2": "/detail/1"}
     *
     * A details subscription to prefix "/list/" would include ["one", "two"]. It would be notified
     * if the paths /detail/1 or /detail/2 change, or if a new item is added to /list/ or an item
     * is deleted from /list/.
     */
    @Synchronized
    fun <T> subscribeDetails(
        subscriber: Subscriber<T>,
        receiveInitial: Boolean = true
    ) {
        val detailsSubscriber = DetailsSubscriber(this, subscriber)
        subscribe(detailsSubscriber, receiveInitial = false)
        if (receiveInitial) {
            subscriber.pathPrefixes.forEach { pathPrefix ->
                listDetails<T>("${pathPrefix}%").forEach {
                    detailsSubscriber.onUpdate(it.path, it.detailPath, it.value)
                }
            }
        }
    }

    /**
     * Unsubscribes the subscriber identified by subscriberId
     */
    @Synchronized
    fun unsubscribe(subscriberId: String) {
        val subscriber = subscribersById.remove(subscriberId)
        subscriber?.pathPrefixes?.forEach { pathPrefix ->
            val subscribersForPrefix =
                subscribers.getValueForExactKey(pathPrefix)?.remove(subscriber.id)
            if (subscribersForPrefix?.size ?: 0 == 0) {
                subscribers.remove(pathPrefix)
            } else {
                subscribers.put(pathPrefix, subscribersForPrefix)
            }
        }
        when (subscriber) {
            is DetailsSubscriber<*> -> subscriber.subscribersForDetails.values.forEach {
                unsubscribe(
                    it.id
                )
            }
        }
    }

    /**
     * Gets the value at the given path
     */
    fun <T> get(path: String): T? {
        return Transaction(this).get(path)
    }

    /**
     * Lists all values matching the pathQuery. A path query is a path with '%' used as a wildcard.
     *
     * For example, given the following data:
     *
     * {"/detail/1": "one",
     *  "/detail/2": "two"}
     *
     * The pathQuery "/detail/%" would return ["one", "two"]
     */
    fun <T> list(
        pathQuery: String,
        start: Int = 0,
        count: Int = Int.MAX_VALUE,
        reverseSort: Boolean = false
    ): List<Entry<T>> {
        return Transaction(this).list(pathQuery, start, count, reverseSort)
    }

    /**
     * Lists details for paths matching the pathQuery, where details are found by treating the
     * values of the matching paths as paths to look up the details.
     *
     * For example, given the following data:
     *
     * {"/detail/1": "one",
     *  "/detail/2": "two",
     *  "/list/1": "/detail/2",
     *  "/list/2": "/detail/1"}
     *
     * The pathQuery "/list/%" would return ["two", "one"]
     */
    fun <T> listDetails(
        pathQuery: String,
        start: Int = 0,
        count: Int = Int.MAX_VALUE,
        reverseSort: Boolean = false
    ): List<Detail<T>> {
        return Transaction(this).listDetails(pathQuery, start, count, reverseSort)
    }

    /**
     * Mutates the database inside of a transaction.
     *
     * If the callback function throws an exception, the entire transaction is rolled back.
     *
     * If the callback completes without exception, the entire transaction is committed and all
     * listeners of affected key paths are notified.
     */
    fun mutate(fn: (tx: Transaction) -> Unit) {
        try {
            db.beginTransaction()
            val tx = Transaction(this)
            fn(tx)
            db.setTransactionSuccessful()
            tx.publish()
        } finally {
            db.endTransaction()
        }
    }

    @Synchronized
    fun close() {
        db.close()
    }
}

class Transaction internal constructor(private val model: ObservableModel) {
    private val updates = HashMap<String, Any>()
    private val deletions = TreeSet<String>()

    /**
     * Puts the given value at the given path. If the value is null, the path is deleted.
     */
    fun put(path: String, value: Any?) {
        value?.let {
            model.db.execSQL(
                "INSERT INTO kvstore(path, value) VALUES(?, ?) ON CONFLICT(path) DO UPDATE SET value = EXCLUDED.value",
                arrayOf(model.serde.serialize(path), model.serde.serialize(value))
            )
            updates[path] = value
            deletions -= path
        } ?: run {
            delete(path)
        }
    }

    /**
     * Puts all path/value pairs into the model
     */
    fun putAll(map: Map<String, Any?>) {
        map.forEach { (path, value) -> put(path, value) }
    }

    /**
     * Deletes the value at the given path
     */
    fun delete(path: String) {
        model.db.execSQL("DELETE FROM kvstore WHERE path = ?", arrayOf(model.serde.serialize(path)))
        deletions += path
        updates.remove(path)
    }

    fun <T> get(path: String): T? {
        val cursor = model.db.query(
            "SELECT value FROM kvstore WHERE path = ?", arrayOf(model.serde.serialize(path))
        )
        cursor.use { cursor ->
            if (cursor == null || !cursor.moveToNext()) {
                return null
            }
            return model.serde.deserialize(cursor.getBlob(0))
        }
    }

    fun <T> list(
        pathQuery: String,
        start: Int = 0,
        count: Int = Int.MAX_VALUE,
        reverseSort: Boolean = false
    ): List<Entry<T>> {
        val sortOrder = if (reverseSort) "DESC" else "ASC"
        val cursor = model.db.query(
            "SELECT path, value FROM kvstore WHERE path LIKE ? ORDER BY path $sortOrder",
            arrayOf(model.serde.serialize(pathQuery))
        )
        cursor.use { cursor ->
            val result = ArrayList<Entry<T>>()
            if (cursor != null && (start == 0 || cursor.moveToPosition(start - 1))) {
                while (cursor.moveToNext() && result.size < count) {
                    result.add(
                        Entry(
                            model.serde.deserialize(cursor.getBlob(0)),
                            model.serde.deserialize(cursor.getBlob(1))
                        )
                    )
                }
            }
            return result
        }
    }

    fun <T> listDetails(
        pathQuery: String,
        start: Int = 0,
        count: Int = Int.MAX_VALUE,
        reverseSort: Boolean = false
    ): List<Detail<T>> {
        val sortOrder = if (reverseSort) "DESC" else "ASC"
        val cursor = model.db.query(
            "SELECT l.path, d.path, d.value FROM kvstore l INNER JOIN kvstore d ON l.value = d.path WHERE l.path LIKE ? AND SUBSTR(CAST(l.value AS TEXT), 1, 1) = 'T' ORDER BY l.path $sortOrder",
            arrayOf(model.serde.serialize(pathQuery))
        )
        cursor.use { cursor ->
            val result = ArrayList<Detail<T>>()
            if (cursor != null && (start == 0 || cursor.moveToPosition(start - 1))) {
                while (cursor.moveToNext() && result.size < count) {
                    result.add(
                        Detail(
                            model.serde.deserialize(cursor.getBlob(0)),
                            model.serde.deserialize(cursor.getBlob(1)),
                            model.serde.deserialize(cursor.getBlob(2))
                        )
                    )
                }
            }
            return result
        }
    }

    internal fun publish() {
        updates.forEach { (path, value) ->
            model.subscribers.getKeyValuePairsForClosestKeys(path).forEach {
                // TODO: there may be a more efficient way to model this than finding closest keys and then checking prefix matches ourselves
                if (path.startsWith(it.key)) it.value.values.forEach { it.onUpdate(path, value) }
            }
        }

        deletions.forEach { path ->
            model.subscribers.getKeyValuePairsForClosestKeys(path).forEach {
                if (path.startsWith(it.key)) it.value.values.forEach { it.onDelete(path) }
            }
        }
    }
}

internal class DetailsSubscriber<T>(
    private val model: ObservableModel,
    private val originalSubscriber: Subscriber<T>
) : Subscriber<String>(originalSubscriber.id, *originalSubscriber.pathPrefixes) {
    internal val subscribersForDetails = ConcurrentHashMap<String, Subscriber<T>>()

    @Synchronized
    override fun onUpdate(path: String, detailPath: String) {
        val value = model.get<T>(detailPath)
        if (value != null) {
            onUpdate(path, detailPath, value)
        }
    }

    internal fun onUpdate(path: String, detailPath: String, value: T) {
        if (!subscribersForDetails.contains(path)) {
            val subscriberForDetails = SubscriberForDetails<T>(originalSubscriber, path, detailPath)
            subscribersForDetails[path] = subscriberForDetails
            model.subscribe(subscriberForDetails)
        }
        originalSubscriber.onUpdate(path, value!!)
    }

    override fun onDelete(path: String) {
        subscribersForDetails.remove(path)?.let { model.unsubscribe(it.id) }
        originalSubscriber.onDelete(path)
    }
}

internal class SubscriberForDetails<T>(
    private val originalSubscriber: Subscriber<T>,
    private val originalPath: String,
    detailPath: String
) : Subscriber<T>("${originalSubscriber.id}/${detailPath}", detailPath) {
    override fun onUpdate(path: String, value: T) {
        originalSubscriber.onUpdate(originalPath, value)
    }

    override fun onDelete(path: String) {
        originalSubscriber.onDelete(originalPath)
    }
}

