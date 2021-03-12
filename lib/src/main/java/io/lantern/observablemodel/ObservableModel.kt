package io.lantern.observablemodel

import android.content.Context
import android.content.SharedPreferences
import ca.gedge.radixtree.RadixTree
import ca.gedge.radixtree.RadixTreeVisitor
import kotlinx.collections.immutable.PersistentMap
import kotlinx.collections.immutable.persistentHashMapOf
import net.sqlcipher.Cursor
import net.sqlcipher.database.SQLiteDatabase
import java.io.Closeable
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
abstract class RawSubscriber<T : Any>(
    internal val id: String,
    internal vararg val pathPrefixes: String
) {
    /**
     * Called when the value at the given path changes
     */
    abstract fun onUpdate(path: String, raw: Raw<T>)

    /**
     * Called when the value at the given path is deleted
     */
    abstract fun onDelete(path: String)
}

abstract class Subscriber<T : Any>(id: String, vararg pathPrefixes: String) :
    RawSubscriber<T>(id, *pathPrefixes) {
    override fun onUpdate(path: String, raw: Raw<T>) {
        onUpdate(path, raw.value)
    }

    abstract fun onUpdate(path: String, value: T)
}

/**
 * Observable model provides a simple key/value store with a map-like interface. It allows the
 * registration of observers for any time that a key path changes.
 */
class ObservableModel private constructor(db: SQLiteDatabase) : Queryable(db, Serde()), Closeable {
    internal val subscribers = RadixTree<PersistentMap<String, RawSubscriber<Any>>>();
    internal val subscribersById = ConcurrentHashMap<String, RawSubscriber<Any>>()

    companion object {
        /**
         * Builds an ObservableModel backed by an encrypted SQLite database at the given filePath
         *
         * collectionName - the name of the map as stored in the database
         * password - the password used to encrypted the data (the longer the better)
         */
        fun build(
            ctx: Context,
            filePath: String,
            password: String,
        ): ObservableModel {
            SQLiteDatabase.loadLibs(ctx)
            val db = SQLiteDatabase.openOrCreateDatabase(filePath, password, null)
            if (!db.enableWriteAheadLogging()) {
                throw Exception("Unable to enable write ahead logging")
            }
            // All data is stored in a single table that has a TEXT path and a BLOB value
            db.execSQL("CREATE TABLE IF NOT EXISTS data ([path] TEXT PRIMARY KEY, [value] BLOB)")
            // Create an index on only text values to speed up detail lookups that join on path = value
            db.execSQL("CREATE INDEX IF NOT EXISTS data_value_index ON data(value) WHERE SUBSTR(CAST(value AS TEXT), 1, 1) = 'T'")
            // Create a table for full text search
            db.execSQL("CREATE VIRTUAL TABLE IF NOT EXISTS fts USING fts5(value, content=data, tokenize='porter unicode61')")
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
    fun <T : Any> subscribe(
        subscriber: RawSubscriber<T>,
        receiveInitial: Boolean = true
    ) {
        if (subscribersById.putIfAbsent(subscriber.id, subscriber as RawSubscriber<Any>) != null) {
            throw IllegalArgumentException("subscriber with id ${subscriber.id} already registered")
        }
        subscriber.pathPrefixes.forEach { pathPrefix ->
            val subscribersForPrefix = subscribers.get(pathPrefix)?.let {
                it.put(
                    subscriber.id,
                    subscriber
                )
            } ?: persistentHashMapOf(subscriber.id to subscriber)
            subscribers.put(pathPrefix, subscribersForPrefix)

            if (receiveInitial) {
                listRaw<T>("${pathPrefix}%").forEach {
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
    fun <T : Any> subscribeDetails(
        subscriber: RawSubscriber<T>,
        receiveInitial: Boolean = true
    ) {
        val detailsSubscriber = DetailsSubscriber(this, subscriber)
        subscribe(detailsSubscriber, receiveInitial = false)
        if (receiveInitial) {
            subscriber.pathPrefixes.forEach { pathPrefix ->
                listDetailsRaw<T>("${pathPrefix}%").forEach {
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
                subscribers.get(pathPrefix)?.remove(subscriber.id)
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
     * Mutates the database inside of a transaction.
     *
     * If the callback function throws an exception, the entire transaction is rolled back.
     *
     * If the callback completes without exception, the entire transaction is committed and all
     * listeners of affected key paths are notified.
     */
    fun <T> mutate(fn: (tx: Transaction) -> T): T {
        try {
            db.beginTransaction()
            val tx = Transaction(this.db, this.serde, this.subscribers)
            val result = fn(tx)
            db.setTransactionSuccessful()
            tx.publish()
            return result
        } finally {
            db.endTransaction()
        }
    }

    /**
     * Returns a SharedPreferences backed by this model.
     *
     * @param prefix - preference keys are prefixed with this for storage in the model, for example if prefix="/prefs/" and the preference key is "mypref", it would be stored at "/prefs/mypref"
     * @param fallback - an optional fallback SharedPreferences to use for values that aren't found in the model
     */
    fun asSharedPreferences(
        prefix: String = "",
        fallback: SharedPreferences? = null
    ): SharedPreferences {
        return SharedPreferencesAdapter(this, prefix, fallback)
    }

    @Synchronized
    override fun close() {
        db.close()
    }
}

open class Queryable internal constructor(
    protected val db: SQLiteDatabase,
    internal val serde: Serde
) {
    /**
     * Gets the value at the given path
     */
    fun <T : Any> get(path: String): T? {
        val cursor = selectSingle(path)
        cursor.use {
            if (cursor == null || !cursor.moveToNext()) {
                return null
            }
            return serde.deserialize(cursor.getBlob(0))
        }
    }

    /**
     * Gets the raw value at the given path
     */
    fun <T : Any> getRaw(path: String): Raw<T>? {
        val cursor = selectSingle(path)
        cursor.use {
            if (cursor == null || !cursor.moveToNext()) {
                return null
            }
            return Raw(serde, cursor.getBlob(0))
        }
    }

    /**
     * Indicates whether the model contains a value at the given path
     */
    fun contains(path: String): Boolean {
        val cursor = db.rawQuery(
            "SELECT COUNT(path) FROM data WHERE path = ?",
            arrayOf(serde.serialize(path))
        )
        cursor.use {
            return cursor != null && cursor.moveToNext() && cursor.getInt(0) > 0
        }
    }

    private fun selectSingle(path: String): Cursor {
        return db.rawQuery(
            "SELECT value FROM data WHERE path = ?",
            arrayOf(serde.serialize(path))
        )
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
     *
     * By default results are sorted lexicographically by path. If reverseSort is specified, that is
     * reversed.
     *
     * If fullTextSearch is specified, in addition to the pathQuery, rows will be filtered by
     * searching for matches to the fullTextSearch term in the full text index.
     */
    fun <T : Any> list(
        pathQuery: String,
        start: Int = 0,
        count: Int = Int.MAX_VALUE,
        fullTextSearch: String? = null,
        reverseSort: Boolean = false
    ): List<Entry<T>> {
        val result = ArrayList<Entry<T>>()
        doList(pathQuery, start, count, fullTextSearch, reverseSort) { cursor ->
            result.add(
                Entry(
                    serde.deserialize(cursor.getBlob(0)),
                    serde.deserialize(cursor.getBlob(1))
                )
            )
        }
        return result
    }

    /**
     * Like list but returning the raw values
     */
    fun <T : Any> listRaw(
        pathQuery: String,
        start: Int = 0,
        count: Int = Int.MAX_VALUE,
        fullTextSearch: String? = null,
        reverseSort: Boolean = false
    ): List<Entry<Raw<T>>> {
        val result = ArrayList<Entry<Raw<T>>>()
        doList(pathQuery, start, count, fullTextSearch, reverseSort) { cursor ->
            result.add(
                Entry(
                    serde.deserialize(cursor.getBlob(0)),
                    Raw(serde, cursor.getBlob(1))
                )
            )
        }
        return result
    }

    /**
     * Like list but only lists the paths of matching rows.
     */
    fun listPaths(
        pathQuery: String,
        start: Int = 0,
        count: Int = Int.MAX_VALUE,
        fullTextSearch: String? = null,
        reverseSort: Boolean = false
    ): List<String> {
        val result = ArrayList<String>()
        doList(pathQuery, start, count, fullTextSearch, reverseSort) { cursor ->
            result.add(serde.deserialize(cursor.getBlob(0)))
        }
        return result
    }

    private fun doList(
        pathQuery: String,
        start: Int,
        count: Int,
        fullTextSearch: String?,
        reverseSort: Boolean,
        onRow: (cursor: Cursor) -> Unit
    ): Unit {
        val cursor = if (fullTextSearch != null) {
            db.rawQuery(
                "SELECT data.path, data.value FROM fts INNER JOIN data ON fts.rowid = data.rowid WHERE data.path LIKE ? AND fts.value MATCH ? ORDER BY fts.rank LIMIT ? OFFSET ?",
                arrayOf(serde.serialize(pathQuery), fullTextSearch, count, start)
            )
        } else {
            val sortOrder = if (reverseSort) "DESC" else "ASC"
            db.rawQuery(
                "SELECT path, value FROM data WHERE path LIKE ? ORDER BY path $sortOrder LIMIT ? OFFSET ?",
                arrayOf(serde.serialize(pathQuery), count, start)
            )
        }
        cursor.use {
            if (cursor != null) {
                while (cursor.moveToNext()) {
                    onRow(cursor)
                }
            }
        }
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
     *
     * If fullTextSearch is specified, in addition to the pathQuery, detail rows will be filtered by
     * searching for matches to the fullTextSearch term in the full text index corresponding to the
     * detail rows (not the top level list).
     */
    fun <T : Any> listDetails(
        pathQuery: String,
        start: Int = 0,
        count: Int = Int.MAX_VALUE,
        fullTextSearch: String? = null,
        reverseSort: Boolean = false
    ): List<Detail<T>> {
        val result = ArrayList<Detail<T>>()
        doListDetails<T>(
            pathQuery,
            start,
            count,
            fullTextSearch,
            reverseSort
        ) { listPath, detailPath, value ->
            result.add(Detail(listPath, detailPath, serde.deserialize(value)))
        }
        return result
    }

    /**
     * Like listDetails but returning the raw values
     */
    fun <T : Any> listDetailsRaw(
        pathQuery: String,
        start: Int = 0,
        count: Int = Int.MAX_VALUE,
        fullTextSearch: String? = null,
        reverseSort: Boolean = false
    ): List<Detail<Raw<T>>> {
        val result = ArrayList<Detail<Raw<T>>>()
        doListDetails<T>(
            pathQuery,
            start,
            count,
            fullTextSearch,
            reverseSort
        ) { listPath, detailPath, value ->
            result.add(Detail(listPath, detailPath, Raw(serde, value)))
        }
        return result
    }

    private fun <T : Any> doListDetails(
        pathQuery: String,
        start: Int,
        count: Int,
        fullTextSearch: String?,
        reverseSort: Boolean,
        onResult: (listPath: String, detailPath: String, value: ByteArray) -> Unit
    ): Unit {
        val cursor = if (fullTextSearch != null) {
            db.rawQuery(
                "SELECT l.path, d.path, d.value FROM data l INNER JOIN data d ON l.value = d.path INNER JOIN fts ON fts.rowid = d.rowid WHERE l.path LIKE ? AND SUBSTR(CAST(l.value AS TEXT), 1, 1) = 'T' AND fts.value MATCH ? ORDER BY fts.rank LIMIT ? OFFSET ?",
                arrayOf(serde.serialize(pathQuery), fullTextSearch, count, start)
            )
        } else {
            val sortOrder = if (reverseSort) "DESC" else "ASC"
            db.rawQuery(
                "SELECT l.path, d.path, d.value FROM data l INNER JOIN data d ON l.value = d.path WHERE l.path LIKE ? AND SUBSTR(CAST(l.value AS TEXT), 1, 1) = 'T' ORDER BY l.path $sortOrder LIMIT ? OFFSET ?",
                arrayOf(serde.serialize(pathQuery), count, start)
            )
        }
        cursor.use {
            if (cursor != null) {
                while (cursor.moveToNext()) {
                    onResult(
                        serde.deserialize(cursor.getBlob(0)),
                        serde.deserialize(cursor.getBlob(1)),
                        cursor.getBlob(2)
                    )
                }
            }
        }
    }
}

class Transaction internal constructor(
    db: SQLiteDatabase,
    serde: Serde,
    private val subscribers: RadixTree<PersistentMap<String, RawSubscriber<Any>>>
) : Queryable(db, serde) {
    private val updates = HashMap<String, Raw<Any>>()
    private val deletions = TreeSet<String>()

    /**
     * Puts the given value at the given path. If the value is null, the path is deleted.
     *
     * If fullText is populated, the given data will also be full text indexed.
     */
    fun put(path: String, value: Any?, fullText: String? = null) {
        val serializedPath = serde.serialize(path)
        value?.let {
            val bytes = serde.serialize(value)
            db.execSQL(
                "INSERT INTO data(path, value) VALUES(?, ?) ON CONFLICT(path) DO UPDATE SET value = EXCLUDED.value",
                arrayOf(serializedPath, bytes)
            )
            if (fullText != null) {
                db.execSQL(
                    "INSERT INTO fts(rowid, value) VALUES((SELECT rowid FROM data WHERE path = ?), ?)",
                    arrayOf(serializedPath, fullText)
                )
            }
            updates[path] = Raw(bytes, value)
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
    fun <T : Any> delete(path: String, extractFullText: ((T) -> String)? = null) {
        val serializedPath = serde.serialize(path)
        extractFullText?.let {
            val cursor = db.rawQuery(
                "SELECT rowid, value FROM data WHERE path = ?", arrayOf(serializedPath)
            )
            cursor.use {
                if (cursor != null && cursor.moveToNext()) {
                    db.execSQL(
                        "INSERT INTO fts(fts, rowid, value) VALUES('delete', ?, ?)",
                        arrayOf(
                            cursor.getLong(0),
                            extractFullText(serde.deserialize(cursor.getBlob(1)))
                        )
                    )
                }
            }
        }
        db.execSQL("DELETE FROM data WHERE path = ?", arrayOf(serializedPath))
        deletions += path
        updates.remove(path)
    }

    fun delete(path: String) {
        delete<Any>(path, null)
    }

    internal fun publish() {
        updates.forEach { (path, newValue) ->
            subscribers.visit(object :
                RadixTreeVisitor<PersistentMap<String, RawSubscriber<Any>>, Void?> {
                override fun visit(
                    key: String?,
                    value: PersistentMap<String, RawSubscriber<Any>>?
                ): Boolean {
                    if (key == null || !path.startsWith(key)) {
                        return false
                    }
                    value?.values?.forEach { it.onUpdate(path, newValue) }
                    return true
                }

                override fun getResult(): Void? {
                    return null
                }
            })
        }

        deletions.forEach { path ->
            subscribers.visit(object :
                RadixTreeVisitor<PersistentMap<String, RawSubscriber<Any>>, Void?> {
                override fun visit(
                    key: String?,
                    value: PersistentMap<String, RawSubscriber<Any>>?
                ): Boolean {
                    if (key == null || !path.startsWith(key)) {
                        return false
                    }
                    value?.values?.forEach { it.onDelete(path) }
                    return true
                }

                override fun getResult(): Void? {
                    return null
                }
            })
        }
    }
}

internal class DetailsSubscriber<T : Any>(
    private val model: ObservableModel,
    private val originalSubscriber: RawSubscriber<T>
) : RawSubscriber<String>(originalSubscriber.id, *originalSubscriber.pathPrefixes) {
    internal val subscribersForDetails = ConcurrentHashMap<String, RawSubscriber<T>>()

    @Synchronized
    override fun onUpdate(path: String, raw: Raw<String>) {
        val detailPath = raw.value;
        val newValue = model.getRaw<T>(detailPath)
        if (newValue != null) {
            onUpdate(path, detailPath, newValue)
        }
    }

    internal fun onUpdate(path: String, detailPath: String, value: Raw<T>) {
        if (!subscribersForDetails.contains(path)) {
            val subscriberForDetails = SubscriberForDetails<T>(originalSubscriber, path, detailPath)
            subscribersForDetails[path] = subscriberForDetails
            model.subscribe(subscriberForDetails)
        }
        originalSubscriber.onUpdate(path, value)
    }

    override fun onDelete(path: String) {
        subscribersForDetails.remove(path)?.let { model.unsubscribe(it.id) }
        originalSubscriber.onDelete(path)
    }
}

internal class SubscriberForDetails<T : Any>(
    private val originalSubscriber: RawSubscriber<T>,
    private val originalPath: String,
    detailPath: String
) : RawSubscriber<T>("${originalSubscriber.id}/${detailPath}", detailPath) {
    override fun onUpdate(path: String, raw: Raw<T>) {
        originalSubscriber.onUpdate(originalPath, raw)
    }

    override fun onDelete(path: String) {
        originalSubscriber.onDelete(originalPath)
    }
}

