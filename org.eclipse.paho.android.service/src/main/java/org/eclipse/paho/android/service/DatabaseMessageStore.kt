/*******************************************************************************
 * Copyright (c) 1999, 2014 IBM Corp.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Eclipse Distribution License v1.0 which accompany this distribution.
 *
 * The Eclipse Public License is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * and the Eclipse Distribution License is available at
 * http://www.eclipse.org/org/documents/edl-v10.php.
 *
 * Contributors:
 * James Sutton - Removing SQL Injection vunerability (bug 467378)
 */
package org.eclipse.paho.android.service

import org.eclipse.paho.client.mqttv3.MqttMessage

import android.content.ContentValues
import android.content.Context
import android.database.Cursor
import android.database.SQLException
import android.database.sqlite.SQLiteDatabase
import android.database.sqlite.SQLiteOpenHelper

/**
 * Implementation of the [MessageStore] interface, using a SQLite database
 *
 */
internal class DatabaseMessageStore
/**
 * Constructor - create a DatabaseMessageStore to store arrived MQTT message
 *
 * @param service
 * our parent MqttService
 * @param context
 * a context to use for android calls
 */
(service: MqttService, context: Context) : MessageStore {

    // the database
    private var db: SQLiteDatabase? = null

    // a SQLiteOpenHelper specific for this database
    private var mqttDb: MQTTDatabaseHelper? = null

    // a place to send trace data
    private var traceHandler: MqttTraceHandler = service

    /**
     * We need a SQLiteOpenHelper to handle database creation and updating
     *
     */
    private class MQTTDatabaseHelper
    /**
     * Constructor.
     *
     * @param traceHandler
     * @param context
     */
    (private val traceHandler: MqttTraceHandler, context: Context) : SQLiteOpenHelper(context, DATABASE_NAME, null, DATABASE_VERSION) {

        /**
         * When the database is (re)created, create our table
         *
         * @param database
         */
        override fun onCreate(database: SQLiteDatabase) {
            val createArrivedTableStatement = ("CREATE TABLE "
                    + ARRIVED_MESSAGE_TABLE_NAME + "("
                    + MESSAGE_ID + " TEXT PRIMARY KEY, "
                    + CLIENT_HANDLE + " TEXT, "
                    + DESTINATION_NAME + " TEXT, "
                    + PAYLOAD + " BLOB, "
                    + QOS + " INTEGER, "
                    + RETAINED + " TEXT, "
                    + DUPLICATE + " TEXT, " + MTIMESTAMP
                    + " INTEGER" + ");")
            traceHandler!!.traceDebug(TAG, "onCreate {"
                    + createArrivedTableStatement + "}")
            try {
                database.execSQL(createArrivedTableStatement)
                traceHandler.traceDebug(TAG, "created the table")
            } catch (e: SQLException) {
                traceHandler.traceException(TAG, "onCreate", e)
                throw e
            }

        }

        /**
         * To upgrade the database, drop and recreate our table
         *
         * @param db
         * the database
         * @param oldVersion
         * ignored
         * @param newVersion
         * ignored
         */

        override fun onUpgrade(db: SQLiteDatabase, oldVersion: Int, newVersion: Int) {
            traceHandler!!.traceDebug(TAG, "onUpgrade")
            try {
                db.execSQL("DROP TABLE IF EXISTS $ARRIVED_MESSAGE_TABLE_NAME")
            } catch (e: SQLException) {
                traceHandler.traceException(TAG, "onUpgrade", e)
                throw e
            }

            onCreate(db)
            traceHandler.traceDebug(TAG, "onUpgrade complete")
        }

        companion object {
            // TAG used for indentify trace data etc.
            private val TAG = "MQTTDatabaseHelper"

            private val DATABASE_NAME = "mqttAndroidService.db"

            // database version, used to recognise when we need to upgrade
            // (delete and recreate)
            private val DATABASE_VERSION = 1
        }
    }

    init {

        // Open message database
        mqttDb = MQTTDatabaseHelper(traceHandler, context)

        // Android documentation suggests that this perhaps
        // could/should be done in another thread, but as the
        // database is only one table, I doubt it matters...

        traceHandler!!.traceDebug(TAG, "DatabaseMessageStore<init> complete")
    }

    /**
     * Store an MQTT message
     *
     * @param clientHandle
     * identifier for the client storing the message
     * @param topic
     * The topic on which the message was published
     * @param message
     * the arrived MQTT message
     * @return an identifier for the message, so that it can be removed when appropriate
     */
    override fun storeArrived(clientHandle: String, topic: String,
                              message: MqttMessage): String {

        db = mqttDb!!.writableDatabase

        traceHandler!!.traceDebug(TAG, "storeArrived{" + clientHandle + "}, {"
                + message.toString() + "}")

        val payload = message.payload
        val qos = message.qos
        val retained = message.isRetained
        val duplicate = message.isDuplicate

        val values = ContentValues()
        val id = java.util.UUID.randomUUID().toString()
        values.put(MESSAGE_ID, id)
        values.put(CLIENT_HANDLE, clientHandle)
        values.put(DESTINATION_NAME, topic)
        values.put(PAYLOAD, payload)
        values.put(QOS, qos)
        values.put(RETAINED, retained)
        values.put(DUPLICATE, duplicate)
        values.put(MTIMESTAMP, System.currentTimeMillis())
        try {
            db!!.insertOrThrow(ARRIVED_MESSAGE_TABLE_NAME,
                    null, values)
        } catch (e: SQLException) {
            traceHandler.traceException(TAG, "onUpgrade", e)
            throw e
        }

        val count = getArrivedRowCount(clientHandle)
        traceHandler
                .traceDebug(
                        TAG,
                        "storeArrived: inserted message with id of {"
                                + id
                                + "} - Number of messages in database for this clientHandle = "
                                + count)
        return id
    }

    private fun getArrivedRowCount(clientHandle: String): Int {
        var count = 0
        val projection = arrayOf(MESSAGE_ID)
        val selection = "$CLIENT_HANDLE=?"
        val selectionArgs = arrayOfNulls<String>(1)
        selectionArgs[0] = clientHandle
        val c = db!!.query(
                ARRIVED_MESSAGE_TABLE_NAME, // Table Name
                projection, // The columns to return;
                selection, // Columns for WHERE Clause
                selectionArgs, null, null, null// The sort order
        )// The values for the WHERE Cause
        //Don't group the rows
        // Don't filter by row groups

        if (c.moveToFirst()) {
            count = c.getInt(0)
        }
        c.close()
        return count
    }

    /**
     * Delete an MQTT message.
     *
     * @param clientHandle
     * identifier for the client which stored the message
     * @param id
     * the identifying string returned when the message was stored
     *
     * @return true if the message was found and deleted
     */
    override fun discardArrived(clientHandle: String, id: String): Boolean {

        db = mqttDb!!.writableDatabase

        traceHandler!!.traceDebug(TAG, "discardArrived{" + clientHandle + "}, {"
                + id + "}")
        val rows: Int
        val selectionArgs = arrayOfNulls<String>(2)
        selectionArgs[0] = id
        selectionArgs[1] = clientHandle

        try {
            rows = db!!.delete(ARRIVED_MESSAGE_TABLE_NAME,
                    MESSAGE_ID + "=? AND "
                            + CLIENT_HANDLE + "=?",
                    selectionArgs)
        } catch (e: SQLException) {
            traceHandler.traceException(TAG, "discardArrived", e)
            throw e
        }

        if (rows != 1) {
            traceHandler.traceError(TAG,
                    "discardArrived - Error deleting message {" + id
                            + "} from database: Rows affected = " + rows)
            return false
        }
        val count = getArrivedRowCount(clientHandle)
        traceHandler
                .traceDebug(
                        TAG,
                        "discardArrived - Message deleted successfully. - messages in db for this clientHandle $count")
        return true
    }

    /**
     * Get an iterator over all messages stored (optionally for a specific client)
     *
     * @param clientHandle
     * identifier for the client.<br></br>
     * If null, all messages are retrieved
     * @return iterator of all the arrived MQTT messages
     */
    override fun getAllArrivedMessages(
            clientHandle: String): Iterator<MessageStore.StoredMessage> {
        return object : Iterator<MessageStore.StoredMessage> {
            private var c: Cursor? = null
            private var hasNext: Boolean = false
            private val selectionArgs = arrayOf(clientHandle)


            init {
                db = mqttDb!!.writableDatabase
                // anonymous initialiser to start a suitable query
                // and position at the first row, if one exists
                if (clientHandle == null) {
                    c = db!!.query(ARRIVED_MESSAGE_TABLE_NAME, null, null, null, null, null,
                            "mtimestamp ASC")
                } else {
                    c = db!!.query(ARRIVED_MESSAGE_TABLE_NAME, null,
                            "$CLIENT_HANDLE=?",
                            selectionArgs, null, null,
                            "mtimestamp ASC")
                }
                hasNext = c!!.moveToFirst()
            }

            override fun hasNext(): Boolean {
                if (!hasNext) {
                    c!!.close()
                }
                return hasNext
            }

            override fun next(): MessageStore.StoredMessage {
                val messageId = c!!.getString(c!!
                        .getColumnIndex(MESSAGE_ID))
                val clientHandle = c!!.getString(c!!
                        .getColumnIndex(CLIENT_HANDLE))
                val topic = c!!.getString(c!!
                        .getColumnIndex(DESTINATION_NAME))
                val payload = c!!.getBlob(c!!
                        .getColumnIndex(PAYLOAD))
                val qos = c!!.getInt(c!!.getColumnIndex(QOS))
                val retained = java.lang.Boolean.parseBoolean(c!!.getString(c!!
                        .getColumnIndex(RETAINED)))
                val dup = java.lang.Boolean.parseBoolean(c!!.getString(c!!
                        .getColumnIndex(DUPLICATE)))

                // build the result
                val message = MqttMessageHack(payload)
                message.qos = qos
                message.isRetained = retained
                message.isDuplicate = dup

                // move on
                hasNext = c!!.moveToNext()
                return DbStoredData(messageId, clientHandle, topic, message)
            }

        }
    }

    /**
     * Delete all messages (optionally for a specific client)
     *
     * @param clientHandle
     * identifier for the client.<br></br>
     * If null, all messages are deleted
     */
    override fun clearArrivedMessages(clientHandle: String) {

        db = mqttDb!!.writableDatabase
        val selectionArgs = arrayOfNulls<String>(1)
        selectionArgs[0] = clientHandle

        var rows = 0
        if (clientHandle == null) {
            traceHandler!!.traceDebug(TAG,
                    "clearArrivedMessages: clearing the table")
            rows = db!!.delete(ARRIVED_MESSAGE_TABLE_NAME, null, null)
        } else {
            traceHandler!!.traceDebug(TAG,
                    "clearArrivedMessages: clearing the table of "
                            + clientHandle + " messages")
            rows = db!!.delete(ARRIVED_MESSAGE_TABLE_NAME,
                    "$CLIENT_HANDLE=?",
                    selectionArgs)

        }
        traceHandler.traceDebug(TAG, "clearArrivedMessages: rows affected = $rows")
    }

    private inner class DbStoredData internal constructor(override val messageId: String, override val clientHandle: String, override val topic: String,
                                                          override val message: MqttMessage) : MessageStore.StoredMessage

    /**
     * A way to get at the "setDuplicate" method of MqttMessage
     */
    private inner class MqttMessageHack(payload: ByteArray) : MqttMessage(payload) {

        public override fun setDuplicate(dup: Boolean) {
            super.setDuplicate(dup)
        }
    }

    override fun close() {
        if (this.db != null)
            this.db!!.close()

    }

    companion object {

        // TAG used for indentify trace data etc.
        private val TAG = "DatabaseMessageStore"

        // One "private" database column name
        // The other database column names are defined in MqttServiceConstants
        private val MTIMESTAMP = "mtimestamp"

        // the name of the table in the database to which we will save messages
        private val ARRIVED_MESSAGE_TABLE_NAME = "MqttArrivedMessageTable"
    }

}