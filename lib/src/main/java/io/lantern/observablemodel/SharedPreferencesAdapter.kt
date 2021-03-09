package io.lantern.observablemodel

import android.content.SharedPreferences
import java.util.*
import kotlin.collections.HashMap

/**
 * Allows accessing an ObservableModel using the SharedPreferences API
 */
internal class SharedPreferencesAdapter(
    val model: ObservableModel,
    val prefix: String,
    val fallback: SharedPreferences?
) :
    SharedPreferences {
    private val listenerIds = Collections.synchronizedList(ArrayList<ListenerId>())

    override fun getAll(): MutableMap<String, *> {
        val all = fallback?.let { it.all as MutableMap<String, Any?> } ?: HashMap()
        model.list<Any>("${prefix}%").forEach { entry ->
            all[unprefixedPath(entry.path)] = entry.value
        }
        return all
    }

    override fun getString(key: String, defValue: String?): String? {
        return model.get<String>(prefixedPath(key)) ?: fallback?.let { it.getString(key, defValue) }
        ?: defValue
    }

    override fun getStringSet(key: String, defValues: MutableSet<String>?): MutableSet<String> {
        TODO("Not yet implemented")
    }

    override fun getInt(key: String, defValue: Int): Int {
        return model.get<Int>(prefixedPath(key)) ?: fallback?.let { it.getInt(key, defValue) }
        ?: defValue
    }

    override fun getLong(key: String, defValue: Long): Long {
        return model.get<Long>(prefixedPath(key)) ?: fallback?.let { it.getLong(key, defValue) }
        ?: defValue
    }

    override fun getFloat(key: String, defValue: Float): Float {
        return model.get<Float>(prefixedPath(key)) ?: fallback?.let { it.getFloat(key, defValue) }
        ?: defValue
    }

    override fun getBoolean(key: String, defValue: Boolean): Boolean {
        return model.get<Boolean>(prefixedPath(key)) ?: fallback?.let {
            it.getBoolean(
                key,
                defValue
            )
        } ?: defValue
    }

    override fun contains(key: String): Boolean {
        return model.get<Any>(prefixedPath(key)) != null || fallback?.contains(key) ?: false
    }

    override fun edit(): SharedPreferences.Editor {
        return SharedPreferencesEditorAdapter(this)
    }

    override fun registerOnSharedPreferenceChangeListener(listener: SharedPreferences.OnSharedPreferenceChangeListener) {
        val subscriber = object : Subscriber<Any>(UUID.randomUUID().toString(), prefix) {
            override fun onUpdate(path: String, value: Any) {
                listener.onSharedPreferenceChanged(
                    this@SharedPreferencesAdapter,
                    unprefixedPath(path)
                )
            }

            override fun onDelete(path: String) {
                listener.onSharedPreferenceChanged(
                    this@SharedPreferencesAdapter,
                    unprefixedPath(path)
                )
            }
        }
        listenerIds.add(ListenerId(listener, subscriber.id))
        model.subscribe(subscriber)
    }

    override fun unregisterOnSharedPreferenceChangeListener(listener: SharedPreferences.OnSharedPreferenceChangeListener?) {
        val it = listenerIds.iterator()
        while (it.hasNext()) {
            val las = it.next()
            if (las.listener == listener) {
                model.unsubscribe(las.subscriberId)
                it.remove()
                return
            }
        }
    }

    internal fun prefixedPath(path: String): String {
        return prefix + path
    }

    internal fun unprefixedPath(path: String): String {
        return path.substring(prefix.length)
    }
}

internal class SharedPreferencesEditorAdapter(val adapter: SharedPreferencesAdapter) :
    SharedPreferences.Editor {
    private val updates = Collections.synchronizedList(ArrayList<(Transaction) -> Unit>())

    override fun putString(key: String, value: String?): SharedPreferences.Editor {
        if (value == null) {
            remove(key)
        } else {
            updates.add { tx ->
                tx.put(adapter.prefixedPath(key), value)
            }
        }
        return this
    }

    override fun putStringSet(key: String, values: MutableSet<String>?): SharedPreferences.Editor {
        TODO("Not yet implemented")
    }

    override fun putInt(key: String, value: Int): SharedPreferences.Editor {
        updates.add { tx ->
            tx.put(adapter.prefixedPath(key), value)
        }
        return this
    }

    override fun putLong(key: String, value: Long): SharedPreferences.Editor {
        updates.add { tx ->
            tx.put(adapter.prefixedPath(key), value)
        }
        return this
    }

    override fun putFloat(key: String, value: Float): SharedPreferences.Editor {
        updates.add { tx ->
            tx.put(adapter.prefixedPath(key), value)
        }
        return this
    }

    override fun putBoolean(key: String, value: Boolean): SharedPreferences.Editor {
        updates.add { tx ->
            tx.put(adapter.prefixedPath(key), value)
        }
        return this
    }

    override fun remove(key: String): SharedPreferences.Editor {
        updates.add { tx ->
            tx.delete(adapter.prefixedPath(key))
        }
        return this
    }

    override fun clear(): SharedPreferences.Editor {
        updates.add { tx ->
            tx.list<Any>("${adapter.prefix}%").forEach { entry ->
                tx.delete(entry.path)
            }
        }
        return this
    }

    override fun commit(): Boolean {
        apply()
        return true
    }

    override fun apply() {
        adapter.model.mutate { tx ->
            updates.forEach { it(tx) }
        }
        updates.clear()
    }

}

private data class ListenerId(
    val listener: SharedPreferences.OnSharedPreferenceChangeListener,
    val subscriberId: String
)