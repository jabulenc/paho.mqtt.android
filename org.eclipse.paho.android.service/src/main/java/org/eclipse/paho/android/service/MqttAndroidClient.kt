/*******************************************************************************
 * Copyright (c) 1999, 2016 IBM Corp.
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
 * Ian Craggs - Per subscription message handlers bug 466579
 * Ian Craggs - ack control (bug 472172)
 *
 */
package org.eclipse.paho.android.service

import java.io.IOException
import java.io.InputStream
import java.security.KeyManagementException
import java.security.KeyStore
import java.security.KeyStoreException
import java.security.NoSuchAlgorithmException
import java.security.cert.CertificateException
import java.util.concurrent.Executors

import javax.net.ssl.SSLContext
import javax.net.ssl.SSLSocketFactory
import javax.net.ssl.TrustManagerFactory

import org.eclipse.paho.client.mqttv3.DisconnectedBufferOptions
import org.eclipse.paho.client.mqttv3.IMqttActionListener
import org.eclipse.paho.client.mqttv3.IMqttAsyncClient
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken
import org.eclipse.paho.client.mqttv3.IMqttMessageListener
import org.eclipse.paho.client.mqttv3.IMqttToken
import org.eclipse.paho.client.mqttv3.MqttCallback
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended
import org.eclipse.paho.client.mqttv3.MqttClientPersistence
import org.eclipse.paho.client.mqttv3.MqttConnectOptions
import org.eclipse.paho.client.mqttv3.MqttException
import org.eclipse.paho.client.mqttv3.MqttMessage
import org.eclipse.paho.client.mqttv3.MqttPersistenceException
import org.eclipse.paho.client.mqttv3.MqttSecurityException
import org.eclipse.paho.client.mqttv3.MqttToken
import org.eclipse.paho.client.mqttv3.internal.ExceptionHelper

import android.content.BroadcastReceiver
import android.content.ComponentName
import android.content.Context
import android.content.Intent
import android.content.IntentFilter
import android.content.ServiceConnection
import android.os.Build
import android.os.Bundle
import android.os.IBinder
import androidx.localbroadcastmanager.content.LocalBroadcastManager

import android.util.Log
import android.util.SparseArray
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence

/**
 * Enables an android application to communicate with an MQTT server using non-blocking methods.
 *
 *
 * Implementation of the MQTT asynchronous client interface [IMqttAsyncClient] , using the MQTT
 * android service to actually interface with MQTT server. It provides android applications a simple programming interface to all features of the MQTT version 3.1
 * specification including:
 *
 *
 *  * connect
 *  * publish
 *  * subscribe
 *  * unsubscribe
 *  * disconnect
 *
 */
/**
 * Constructor- create an MqttAndroidClient that can be used to communicate
 * with an MQTT server on android
 *
 * @param context
 * used to pass context to the callback.
 * @param serverURI
 * specifies the protocol, host name and port to be used to
 * connect to an MQTT server
 * @param clientId
 * specifies the name by which this connection should be
 * identified to the server
 * @param persistence
 * the persistence class to use to store in-flight message. If
 * null then the default persistence mechanism is used
 * @param ackType
 * how the application wishes to acknowledge a message has been
 * processed.
 */
open class MqttAndroidClient(val myContext: Context, // Connection data
                             protected val serverURIInternal: String,
                             protected val clientIdInternal: String,
                             val persistence: MqttClientPersistence? = null, //The acknowledgment that a message has been processed by the application
                             val messageAck: Ack = Ack.AUTO_ACK) : BroadcastReceiver(), IMqttAsyncClient {

    // Listener for when the service is connected or disconnected
    protected val serviceConnection = MyServiceConnection()

    // The Android Service which will process our mqtt calls
    protected var mqttService: MqttService? = null

    // An identifier for the underlying client connection, which we can pass to
    // the service
    protected var clientHandle: String? = null

    // We hold the various tokens in a collection and pass identifiers for them
    // to the service
    private val tokenMap = SparseArray<IMqttToken>()
    private var tokenNumber = 0
    protected var connectOptions: MqttConnectOptions? = null
    protected var connectToken: IMqttToken? = null

    // The MqttCallback provided by the application
    private var callback: MqttCallback? = null
    private var traceCallback: MqttTraceHandler? = null
    private var traceEnabled = false

    @Volatile
    protected var receiverRegistered = false

    @Volatile
    private var bindedService = false

    internal val bufferedMessageCount: Int
        get() = mqttService?.getBufferedMessageCount(clientHandle!!) ?: -1

    /**
     *
     * The Acknowledgment mode for messages received from [MqttCallback.messageArrived]
     *
     */
    enum class Ack {
        /**
         * As soon as the [MqttCallback.messageArrived] returns,
         * the message has been acknowledged as received .
         */
        AUTO_ACK,

        /**
         * When [MqttCallback.messageArrived] returns, the message
         * will not be acknowledged as received, the application will have to make an acknowledgment call
         * to [MqttAndroidClient] using [MqttAndroidClient.acknowledgeMessage]
         */
        MANUAL_ACK
    }

    /**
     * ServiceConnection to process when we bind to our service
     */
    inner class MyServiceConnection : ServiceConnection {

        override fun onServiceConnected(name: ComponentName, binder: IBinder) {
            mqttService = (binder as MqttServiceBinder).service
            bindedService = true
            // now that we have the service available, we can actually
            // connect...
            val connectTokenInternal = doConnect()
            connectToken = (connectToken as? MqttTokenAndroid)?.apply {
                setDelegate(connectTokenInternal)
            } ?: connectTokenInternal
        }

        override fun onServiceDisconnected(name: ComponentName) {
            mqttService = null
        }
    }

    /**
     * Determines if this client is currently connected to the server.
     *
     * @return `true` if connected, `false` otherwise.
     */
    override fun isConnected(): Boolean {

        return clientHandle != null && mqttService != null && mqttService?.isConnected(clientHandle!!) ?: false
    }

    /**
     * Returns the client ID used by this client.
     *
     *
     * All clients connected to the same server or server farm must have a
     * unique ID.
     *
     *
     * @return the client ID used by this client.
     */
    override fun getClientId(): String {
        return clientIdInternal
    }

    /**
     * Returns the URI address of the server used by this client.
     *
     *
     * The format of the returned String is the same as that used on the
     * constructor.
     *
     *
     * @return the server's address, as a URI String.
     */
    override fun getServerURI(): String {
        return serverURIInternal
    }

    /**
     * Close the client. Releases all resource associated with the client. After
     * the client has been closed it cannot be reused. For instance attempts to
     * connect will fail.
     *
     */
    override fun close() {
        if (mqttService != null) {
            if (clientHandle == null) {
                clientHandle = mqttService?.getClient(serverURIInternal, clientIdInternal, myContext!!.applicationInfo.packageName, persistence!!)
            }
            mqttService?.close(clientHandle!!)
        }
    }

    /**
     * Connects to an MQTT server using the default options.
     *
     *
     * The default options are specified in [MqttConnectOptions] class.
     *
     *
     * @throws MqttException
     * for any connected problems
     * @return token used to track and wait for the connect to complete. The
     * token will be passed to the callback methods if a callback is
     * set.
     * @see .connect
     */
    @Throws(MqttException::class)
    override fun connect(): IMqttToken {
        return connect(null, null)
    }


    /**
     * Connects to an MQTT server using the provided connect options.
     *
     *
     * The connection will be established using the options specified in the
     * [MqttConnectOptions] parameter.
     *
     *
     * @param options
     * a set of connection parameters that override the defaults.
     * @throws MqttException
     * for any connected problems
     * @return token used to track and wait for the connect to complete. The
     * token will be passed to any callback that has been set.
     * @see .connect
     */
    @Throws(MqttException::class)
    override fun connect(options: MqttConnectOptions): IMqttToken {
        return connect(options, null, null)
    }

    /**
     * Connects to an MQTT server using the default options.
     *
     *
     * The default options are specified in [MqttConnectOptions] class.
     *
     *
     * @param userContext
     * optional object used to pass context to the callback. Use null
     * if not required.
     * @param callback
     * optional listener that will be notified when the connect
     * completes. Use null if not required.
     * @throws MqttException
     * for any connected problems
     * @return token used to track and wait for the connect to complete. The
     * token will be passed to any callback that has been set.
     * @see .connect
     */
    @Throws(MqttException::class)
    override fun connect(userContext: Any?, callback: IMqttActionListener?): IMqttToken {
        return connect(MqttConnectOptions(), userContext, callback)
    }

    /**
     * Connects to an MQTT server using the specified options.
     *
     *
     * The server to connect to is specified on the constructor. It is
     * recommended to call [.setCallback] prior to
     * connecting in order that messages destined for the client can be accepted
     * as soon as the client is connected.
     *
     *
     *
     *
     * The method returns control before the connect completes. Completion can
     * be tracked by:
     *
     *
     *  * Waiting on the returned token [IMqttToken.waitForCompletion]
     * or
     *  * Passing in a callback [IMqttActionListener]
     *
     *
     *
     * @param options
     * a set of connection parameters that override the defaults.
     * @param userContext
     * optional object for used to pass context to the callback. Use
     * null if not required.
     * @param callback
     * optional listener that will be notified when the connect
     * completes. Use null if not required.
     * @return token used to track and wait for the connect to complete. The
     * token will be passed to any callback that has been set.
     * @throws MqttException
     * for any connected problems, including communication errors
     */

    @Throws(MqttException::class)
    override fun connect(options: MqttConnectOptions, userContext: Any?,
                         callback: IMqttActionListener?): IMqttToken {

        val token = MqttTokenAndroid(this, userContext,
                callback)

        connectOptions = options
        connectToken = token

        /*
		 * The actual connection depends on the service, which we start and bind
		 * to here, but which we can't actually use until the serviceConnection
		 * onServiceConnected() method has run (asynchronously), so the
		 * connection itself takes place in the onServiceConnected() method
		 */
        if (mqttService == null) { // First time - must bind to the service
            val serviceStartIntent = Intent()
            serviceStartIntent.setClassName(myContext!!, SERVICE_NAME)
            val service = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                myContext?.startForegroundService(serviceStartIntent)
            } else {
                myContext?.startService(serviceStartIntent)
            }
            if (service == null) {
                val listener = token.actionCallback
                listener?.onFailure(token, RuntimeException(
                        "cannot start service $SERVICE_NAME"))
            }

            // We bind with BIND_SERVICE_FLAG (0), leaving us the manage the lifecycle
            // until the last time it is stopped by a call to stopService()
            myContext!!.bindService(serviceStartIntent, serviceConnection,
                    Context.BIND_AUTO_CREATE)

            if (!receiverRegistered) registerReceiver(this)
        } else {
            pool.execute {
                val connectTokenInternal = doConnect()
                connectToken = token.apply {
                    setDelegate(connectTokenInternal)
                }
                //Register receiver to show shoulder tap.
                if (!receiverRegistered) registerReceiver(this@MqttAndroidClient)
            }
        }

        return token
    }

    protected fun registerReceiver(receiver: BroadcastReceiver) {
        val filter = IntentFilter()
        filter.addAction(CALLBACK_TO_ACTIVITY)
        LocalBroadcastManager.getInstance(myContext!!).registerReceiver(receiver, filter)
        receiverRegistered = true
    }

    /**
     * Actually do the mqtt connect operation
     */
    public fun doConnect(): IMqttToken? {
        if (clientHandle == null) {
            clientHandle = mqttService?.getClient(
                    serverURIInternal,
                    clientIdInternal,
                    myContext.applicationInfo.packageName,
                    persistence ?: MqttDefaultFilePersistence(mqttService?.getExternalFilesDir(MqttConnection.TAG)?.absolutePath)
            )
        }
        mqttService?.isTraceEnabled = traceEnabled
        mqttService?.setTraceCallbackId(clientHandle!!)

        val activityToken = storeToken(connectToken)
        return try {
            mqttService?.connect(clientHandle!!, connectOptions!!, null, activityToken)
        } catch (e: MqttException) {
            val listener = connectToken!!.actionCallback
            listener?.onFailure(connectToken, e)
            null
        }
    }

    /**
     * Disconnects from the server.
     *
     *
     * An attempt is made to quiesce the client allowing outstanding work to
     * complete before disconnecting. It will wait for a maximum of 30 seconds
     * for work to quiesce before disconnecting. This method must not be called
     * from inside [MqttCallback] methods.
     *
     *
     * @return token used to track and wait for disconnect to complete. The
     * token will be passed to any callback that has been set.
     * @throws MqttException
     * for problems encountered while disconnecting
     * @see .disconnect
     */
    @Throws(MqttException::class)
    override fun disconnect(): IMqttToken {
        val token = MqttTokenAndroid(this, null, null)
        val activityToken = storeToken(token)
        mqttService?.disconnect(clientHandle!!, null, activityToken)
        return token
    }

    /**
     * Disconnects from the server.
     *
     *
     * An attempt is made to quiesce the client allowing outstanding work to
     * complete before disconnecting. It will wait for a maximum of the
     * specified quiesce time for work to complete before disconnecting. This
     * method must not be called from inside [MqttCallback] methods.
     *
     *
     * @param quiesceTimeout
     * the amount of time in milliseconds to allow for existing work
     * to finish before disconnecting. A value of zero or less means
     * the client will not quiesce.
     * @return token used to track and wait for disconnect to complete. The
     * token will be passed to the callback methods if a callback is
     * set.
     * @throws MqttException
     * for problems encountered while disconnecting
     * @see .disconnect
     */
    @Throws(MqttException::class)
    override fun disconnect(quiesceTimeout: Long): IMqttToken {
        val token = MqttTokenAndroid(this, null, null)
        val activityToken = storeToken(token)
        mqttService?.disconnect(clientHandle!!, quiesceTimeout, null, activityToken)
        return token
    }

    /**
     * Disconnects from the server.
     *
     *
     * An attempt is made to quiesce the client allowing outstanding work to
     * complete before disconnecting. It will wait for a maximum of 30 seconds
     * for work to quiesce before disconnecting. This method must not be called
     * from inside [MqttCallback] methods.
     *
     *
     * @param userContext
     * optional object used to pass context to the callback. Use null
     * if not required.
     * @param callback
     * optional listener that will be notified when the disconnect
     * completes. Use null if not required.
     * @return token used to track and wait for the disconnect to complete. The
     * token will be passed to any callback that has been set.
     * @throws MqttException
     * for problems encountered while disconnecting
     * @see .disconnect
     */
    @Throws(MqttException::class)
    override fun disconnect(userContext: Any,
                            callback: IMqttActionListener): IMqttToken {
        val token = MqttTokenAndroid(this, userContext,
                callback)
        val activityToken = storeToken(token)
        mqttService?.disconnect(clientHandle!!, null, activityToken)
        return token
    }

    /**
     * Disconnects from the server.
     *
     *
     * The client will wait for [MqttCallback] methods to complete. It
     * will then wait for up to the quiesce timeout to allow for work which has
     * already been initiated to complete. For instance when a QoS 2 message has
     * started flowing to the server but the QoS 2 flow has not completed.It
     * prevents new messages being accepted and does not send any messages that
     * have been accepted but not yet started delivery across the network to the
     * server. When work has completed or after the quiesce timeout, the client
     * will disconnect from the server. If the cleanSession flag was set to
     * false and next time it is also set to false in the connection, the
     * messages made in QoS 1 or 2 which were not previously delivered will be
     * delivered this time.
     *
     *
     *
     * This method must not be called from inside [MqttCallback] methods.
     *
     *
     *
     * The method returns control before the disconnect completes. Completion
     * can be tracked by:
     *
     *
     *  * Waiting on the returned token [IMqttToken.waitForCompletion]
     * or
     *  * Passing in a callback [IMqttActionListener]
     *
     *
     * @param quiesceTimeout
     * the amount of time in milliseconds to allow for existing work
     * to finish before disconnecting. A value of zero or less means
     * the client will not quiesce.
     * @param userContext
     * optional object used to pass context to the callback. Use null
     * if not required.
     * @param callback
     * optional listener that will be notified when the disconnect
     * completes. Use null if not required.
     * @return token used to track and wait for the disconnect to complete. The
     * token will be passed to any callback that has been set.
     * @throws MqttException
     * for problems encountered while disconnecting
     */
    @Throws(MqttException::class)
    override fun disconnect(quiesceTimeout: Long, userContext: Any,
                            callback: IMqttActionListener): IMqttToken {
        val token = MqttTokenAndroid(this, userContext,
                callback)
        val activityToken = storeToken(token)
        mqttService?.disconnect(clientHandle!!, quiesceTimeout, null, activityToken)
        return token
    }

    /**
     * Publishes a message to a topic on the server.
     *
     *
     * A convenience method, which will create a new [MqttMessage] object
     * with a byte array payload and the specified QoS, and then publish it.
     *
     *
     * @param topic
     * to deliver the message to, for example "finance/stock/ibm".
     * @param payload
     * the byte array to use as the payload
     * @param qos
     * the Quality of Service to deliver the message at. Valid values
     * are 0, 1 or 2.
     * @param retained
     * whether or not this message should be retained by the server.
     * @return token used to track and wait for the publish to complete. The
     * token will be passed to any callback that has been set.
     * @throws MqttPersistenceException
     * when a problem occurs storing the message
     * @throws IllegalArgumentException
     * if value of QoS is not 0, 1 or 2.
     * @throws MqttException
     * for other errors encountered while publishing the message.
     * For instance, too many messages are being processed.
     * @see .publish
     */
    @Throws(MqttException::class, MqttPersistenceException::class)
    override fun publish(topic: String, payload: ByteArray, qos: Int,
                         retained: Boolean): IMqttDeliveryToken {
        return publish(topic, payload, qos, retained, null, null)
    }

    /**
     * Publishes a message to a topic on the server. Takes an
     * [MqttMessage] message and delivers it to the server at the
     * requested quality of service.
     *
     * @param topic
     * to deliver the message to, for example "finance/stock/ibm".
     * @param message
     * to deliver to the server
     * @return token used to track and wait for the publish to complete. The
     * token will be passed to any callback that has been set.
     * @throws MqttPersistenceException
     * when a problem occurs storing the message
     * @throws IllegalArgumentException
     * if value of QoS is not 0, 1 or 2.
     * @throws MqttException
     * for other errors encountered while publishing the message.
     * For instance client not connected.
     * @see .publish
     */
    @Throws(MqttException::class, MqttPersistenceException::class)
    override fun publish(topic: String, message: MqttMessage): IMqttDeliveryToken {
        return publish(topic, message, null, null)
    }

    /**
     * Publishes a message to a topic on the server.
     *
     *
     * A convenience method, which will create a new [MqttMessage] object
     * with a byte array payload, the specified QoS and retained, then publish it.
     *
     *
     * @param topic
     * to deliver the message to, for example "finance/stock/ibm".
     * @param payload
     * the byte array to use as the payload
     * @param qos
     * the Quality of Service to deliver the message at. Valid values
     * are 0, 1 or 2.
     * @param retained
     * whether or not this message should be retained by the server.
     * @param userContext
     * optional object used to pass context to the callback. Use null
     * if not required.
     * @param callback
     * optional listener that will be notified when message delivery
     * has completed to the requested quality of service
     * @return token used to track and wait for the publish to complete. The
     * token will be passed to any callback that has been set.
     * @throws MqttPersistenceException
     * when a problem occurs storing the message
     * @throws IllegalArgumentException
     * if value of QoS is not 0, 1 or 2.
     * @throws MqttException
     * for other errors encountered while publishing the message.
     * For instance client not connected.
     * @see .publish
     */
    @Throws(MqttException::class, MqttPersistenceException::class)
    override fun publish(topic: String, payload: ByteArray, qos: Int,
                         retained: Boolean, userContext: Any?, callback: IMqttActionListener?): IMqttDeliveryToken {

        val message = MqttMessage(payload)
        message.qos = qos
        message.isRetained = retained
        val token = MqttDeliveryTokenAndroid(
                this, userContext, callback, message)
        val activityToken = storeToken(token)
        val internalToken = mqttService?.publish(clientHandle!!,
                topic, payload, qos, retained, null, activityToken)
        token.setDelegate(internalToken)
        return token
    }

    /**
     * Publishes a message to a topic on the server.
     *
     *
     * Once this method has returned cleanly, the message has been accepted for
     * publication by the client and will be delivered on a background thread.
     * In the event the connection fails or the client stops, Messages will be
     * delivered to the requested quality of service once the connection is
     * re-established to the server on condition that:
     *
     *
     *  * The connection is re-established with the same clientID
     *  * The original connection was made with (@link
     * MqttConnectOptions#setCleanSession(boolean)} set to false
     *  * The connection is re-established with (@link
     * MqttConnectOptions#setCleanSession(boolean)} set to false
     *  * Depending when the failure occurs QoS 0 messages may not be
     * delivered.
     *
     *
     *
     *
     * When building an application, the design of the topic tree should take
     * into account the following principles of topic name syntax and semantics:
     *
     *
     *
     *  * A topic must be at least one character long.
     *  * Topic names are case sensitive. For example, *ACCOUNTS* and
     * *Accounts* are two different topics.
     *  * Topic names can include the space character. For example,
     * *Accounts
     * payable* is a valid topic.
     *  * A leading "/" creates a distinct topic. For example,
     *
    finance* is different from *finance*. finance*
     * matches "+/+" and "/+", but not "+".
     *  * Do not include the null character (Unicode *\x0000*) in any topic.
     *
     *
     *
     *
     * The following principles apply to the construction and content of a topic
     * tree:
     *
     *
     *
     *  * The length is limited to 64k but within that there are no limits to
     * the number of levels in a topic tree.
     *  * There can be any number of root nodes; that is, there can be any
     * number of topic trees.
     *
     *
     *
     * The method returns control before the publish completes. Completion can
     * be tracked by:
     *
     *
     *  * Setting an [IMqttAsyncClient.setCallback] where
     * the [MqttCallback.deliveryComplete] method will
     * be called.
     *  * Waiting on the returned token [MqttToken.waitForCompletion]
     * or
     *  * Passing in a callback [IMqttActionListener] to this method
     *
     *
     * @param topic
     * to deliver the message to, for example "finance/stock/ibm".
     * @param message
     * to deliver to the server
     * @param userContext
     * optional
    object used to pass context to the callback. Use null
     * if not required.
     * @param callback
     * optional listener that will be notified when message delivery
     * has completed to the requested quality of service
     * @ return token used to track and wait for the publish to complete. The
     * token will be passed to callback methods if set.
     * @throws MqttPersistenceException
     * when a problem occurs storing the message
     * @throws IllegalArgumentException
     * if value of QoS is not 0, 1 or 2.
     * @throws MqttException
     * for other errors encountered while publishing the message.
     * For instance, client not connected.
     * @see MqttMessage
     */
    @Throws(MqttException::class, MqttPersistenceException::class)
    override fun publish(topic: String, message: MqttMessage,
                         userContext: Any?, callback: IMqttActionListener?): IMqttDeliveryToken {
        val token = MqttDeliveryTokenAndroid(
                this, userContext, callback, message)
        val activityToken = storeToken(token)
        val internalToken = mqttService?.publish(clientHandle!!,
                topic, message, null, activityToken)
        token.setDelegate(internalToken)
        return token
    }

    /**
     * Subscribe to a topic, which may include wildcards.
     *
     * @param topic
     * the topic to subscribe to, which can include wildcards.
     * @param qos
     * the maximum quality of service at which to subscribe. Messages
     * published at a lower quality of service will be received at
     * the published QoS. Messages published at a higher quality of
     * service will be received using the QoS specified on the
     * subscription.
     * @return token used to track and wait for the subscribe to complete. The
     * token will be passed to callback methods if set.
     * @throws MqttSecurityException
     * for security related problems
     * @throws MqttException
     * for non security related problems
     *
     * @see .subscribe
     */
    @Throws(MqttException::class, MqttSecurityException::class)
    override fun subscribe(topic: String, qos: Int): IMqttToken {
        return subscribe(topic, qos, null, null)
    }

    /**
     * Subscribe to multiple topics, each topic may include wildcards.
     *
     *
     *
     * Provides an optimized way to subscribe to multiple topics compared to
     * subscribing to each one individually.
     *
     *
     * @param topic
     * one or more topics to subscribe to, which can include
     * wildcards
     * @param qos
     * the maximum quality of service at which to subscribe. Messages
     * published at a lower quality of service will be received at
     * the published QoS. Messages published at a higher quality of
     * service will be received using the QoS specified on the
     * subscription.
     * @return token used to track and wait for the subscription to complete. The
     * token will be passed to callback methods if set.
     * @throws MqttSecurityException
     * for security related problems
     * @throws MqttException
     * for non security related problems
     *
     * @see .subscribe
     */
    @Throws(MqttException::class, MqttSecurityException::class)
    override fun subscribe(topic: Array<String>, qos: IntArray): IMqttToken {
        return subscribe(topic, qos, null, null)
    }

    /**
     * Subscribe to a topic, which may include wildcards.
     *
     * @param topic
     * the topic to subscribe to, which can include wildcards.
     * @param qos
     * the maximum quality of service at which to subscribe. Messages
     * published at a lower quality of service will be received at
     * the published QoS. Messages published at a higher quality of
     * service will be received using the QoS specified on the
     * subscription.
     * @param userContext
     * optional object used to pass context to the callback. Use null
     * if not required.
     * @param callback
     * optional listener that will be notified when subscribe has
     * completed
     * @return token used to track and wait for the subscribe to complete. The
     * token will be passed to callback methods if set.
     * @throws MqttException
     * if there was an error when registering the subscription.
     *
     * @see .subscribe
     */
    @Throws(MqttException::class)
    override fun subscribe(topic: String, qos: Int, userContext: Any?,
                           callback: IMqttActionListener?): IMqttToken {
        val token = MqttTokenAndroid(this, userContext,
                callback, arrayOf(topic))
        val activityToken = storeToken(token)
        mqttService?.subscribe(clientHandle!!, topic, qos, null, activityToken)
        return token
    }

    /**
     * Subscribes to multiple topics, each topic may include wildcards.
     *
     *
     * Provides an optimized way to subscribe to multiple topics compared to
     * subscribing to each one individually.
     *
     *
     *
     * The [.setCallback] method should be called before
     * this method, otherwise any received messages will be discarded.
     *
     *
     *
     * If (@link MqttConnectOptions#setCleanSession(boolean)} was set to true,
     * when connecting to the server, the subscription remains in place until
     * either:
     *
     *
     *  * The client disconnects
     *  * An unsubscribe method is called to unsubscribe the topic
     *
     *
     *
     * If (@link MqttConnectOptions#setCleanSession(boolean)} was set to false,
     * when connecting to the server, the subscription remains in place
     * until either:
     *
     *
     *  * An unsubscribe method is called to unsubscribe the topic
     *  * The next time the client connects with cleanSession set to true
     *
     *
     * With cleanSession set to false the MQTT server will store messages
     * on behalf of the client when the client is not connected. The next time
     * the client connects with the **same client ID** the server will
     * deliver the stored messages to the client.
     *
     *
     *
     *
     * The "topic filter" string is used when subscription may contain special
     * characters, which allows you to subscribe to multiple topics at once.
     * <dl>
     * <dt>Topic level separator</dt>
     * <dd>The forward slash (/) is used to separate each level within a topic
     * tree and provide a hierarchical structure to the topic space. The use of
     * the topic level separator is significant when the two wildcard characters
     * are encountered in topics specified by subscribers.</dd>
     *
     * <dt>Multi-level wildcard</dt>
     * <dd>
    </dd></dl> *
     *
     * The number sign (#) is a wildcard character that matches any number of
     * levels within a topic. For example, if you subscribe to <span><span class="filepath">finance/stock/ibm/#</span></span>, you receive messages
     * on these topics:
     *
     *
     *  * <pre>finance/stock/ibm</pre>
     *  * <pre>finance/stock/ibm/closingprice</pre>
     *  * <pre>finance/stock/ibm/currentprice</pre>
     *
     *
     *
     *
     * The multi-level wildcard can represent zero or more levels. Therefore,
     * *finance/#* can also match the singular *finance*, where
     * *#* represents zero levels. The topic level separator is
     * meaningless in this context, because there are no levels to separate.
     *
     *
     *
     *
     * The <span>multi-level</span> wildcard can be specified only on its own or
     * next to the topic level separator character. Therefore, *#* and
     * *finance/#* are both valid, but *finance#* is not valid.
     * <span>The multi-level wildcard must be the last character used within the
     * topic tree. For example, *finance/#* is valid but
     * *finance/#/closingprice* is not valid.</span>
     *
     *
     *
     * <dt>Single-level wildcard</dt>
     * <dd>
     *
     *
     * The plus sign (+) is a wildcard character that matches only one topic
     * level. For example, *finance/stock/+* matches
     * *finance/stock/ibm* and *finance/stock/xyz*, but not
     * *finance/stock/ibm/closingprice*. Also, because the single-level
     * wildcard matches only a single level, *finance/+* does not match
     * *finance*.
     *
     *
     *
     *
     * Use the single-level wildcard at any level in the topic tree, and in
     * conjunction with the multilevel wildcard. Specify the single-level
     * wildcard next to the topic level separator, except when it is specified
     * on its own. Therefore, *+* and *finance/+* are both valid,
     * but *finance+* is not valid. <span>The single-level wildcard can
     * be used at the end of the topic tree or within the topic tree. For
     * example, *finance/+* and *finance/+/ibm* are both
     * valid.</span>
     *
    </dd> *
     *
     *
     *
     * The method returns control before the subscribe completes. Completion can
     * be tracked by:
     *
     *
     *  * Waiting on the supplied token [MqttToken.waitForCompletion]
     * or
     *  * Passing in a callback [IMqttActionListener] to this method
     *
     *
     * @param topic
     * one or more topics to subscribe to, which can include
     * wildcards
     * @param qos
     * the maximum quality of service to subscribe each topic
     * at.Messages published at a lower quality of service will be
     * received at the published QoS. Messages published at a higher
     * quality of service will be received using the QoS specified on
     * the subscription.
     * @param userContext
     * optional object used to pass context to the callback. Use null
     * if not required.
     * @param callback
     * optional listener that will be notified when subscribe has
     * completed
     * @return token used to track and wait for the subscribe to complete. The
     * token will be passed to callback methods if set.
     * @throws MqttException
     * if there was an error registering the subscription.
     * @throws IllegalArgumentException
     * if the two supplied arrays are not the same size.
     */
    @Throws(MqttException::class)
    override fun subscribe(topic: Array<String>, qos: IntArray, userContext: Any?,
                           callback: IMqttActionListener?): IMqttToken {
        val token = MqttTokenAndroid(this, userContext,
                callback, topic)
        val activityToken = storeToken(token)
        return mqttService?.subscribe(clientHandle!!, topic, qos, null, activityToken) ?: token
    }

    /**
     * Subscribe to a topic, which may include wildcards.
     *
     * @see .subscribe
     * @param topicFilter the topic to subscribe to, which can include wildcards.
     * @param qos the maximum quality of service at which to subscribe. Messages
     * published at a lower quality of service will be received at the published
     * QoS.  Messages published at a higher quality of service will be received using
     * the QoS specified on the subscribe.
     * @param userContext optional object used to pass context to the callback. Use
     * null if not required.
     * @param callback optional listener that will be notified when subscribe
     * has completed
     * @param messageListener a callback to handle incoming messages
     * @return token used to track and wait for the subscribe to complete. The token
     * will be passed to callback methods if set.
     * @throws MqttException if there was an error registering the subscription.
     */
    @Throws(MqttException::class)
    override fun subscribe(topicFilter: String, qos: Int, userContext: Any?, callback: IMqttActionListener?, messageListener: IMqttMessageListener): IMqttToken? {

        return subscribe(arrayOf(topicFilter), intArrayOf(qos), userContext, callback, arrayOf(messageListener))
    }

    /**
     * Subscribe to a topic, which may include wildcards.
     *
     * @see .subscribe
     * @param topicFilter the topic to subscribe to, which can include wildcards.
     * @param qos the maximum quality of service at which to subscribe. Messages
     * published at a lower quality of service will be received at the published
     * QoS.  Messages published at a higher quality of service will be received using
     * the QoS specified on the subscribe.
     * @param messageListener a callback to handle incoming messages
     * @return token used to track and wait for the subscribe to complete. The token
     * will be passed to callback methods if set.
     * @throws MqttException if there was an error registering the subscription.
     */
    @Throws(MqttException::class)
    override fun subscribe(topicFilter: String, qos: Int, messageListener: IMqttMessageListener): IMqttToken? {

        return subscribe(topicFilter, qos, null, null, messageListener)
    }


    /**
     * Subscribe to multiple topics, each of which may include wildcards.
     *
     *
     * Provides an optimized way to subscribe to multiple topics compared to
     * subscribing to each one individually.
     *
     * @see .subscribe
     * @param topicFilters one or more topics to subscribe to, which can include wildcards
     * @param qos the maximum quality of service at which to subscribe. Messages
     * published at a lower quality of service will be received at the published
     * QoS.  Messages published at a higher quality of service will be received using
     * the QoS specified on the subscribe.
     * @param messageListeners an array of callbacks to handle incoming messages
     * @return token used to track and wait for the subscribe to complete. The token
     * will be passed to callback methods if set.
     * @throws MqttException if there was an error registering the subscription.
     */
    @Throws(MqttException::class)
    override fun subscribe(topicFilters: Array<String>, qos: IntArray, messageListeners: Array<IMqttMessageListener>): IMqttToken? {

        return subscribe(topicFilters, qos, null, null, messageListeners)
    }


    /**
     * Subscribe to multiple topics, each of which may include wildcards.
     *
     *
     * Provides an optimized way to subscribe to multiple topics compared to
     * subscribing to each one individually.
     *
     * @see .subscribe
     * @param topicFilters one or more topics to subscribe to, which can include wildcards
     * @param qos the maximum quality of service at which to subscribe. Messages
     * published at a lower quality of service will be received at the published
     * QoS.  Messages published at a higher quality of service will be received using
     * the QoS specified on the subscribe.
     * @param userContext optional object used to pass context to the callback. Use
     * null if not required.
     * @param callback optional listener that will be notified when subscribe
     * has completed
     * @param messageListeners an array of callbacks to handle incoming messages
     * @return token used to track and wait for the subscribe to complete. The token
     * will be passed to callback methods if set.
     * @throws MqttException if there was an error registering the subscription.
     */
    @Throws(MqttException::class)
    override fun subscribe(topicFilters: Array<String>, qos: IntArray, userContext: Any?, callback: IMqttActionListener?, messageListeners: Array<IMqttMessageListener>): IMqttToken? {
        val token = MqttTokenAndroid(this, userContext, callback, topicFilters)
        val activityToken = storeToken(token)
        mqttService?.subscribe(clientHandle!!, topicFilters, qos, null, activityToken, messageListeners)

        return null
    }

    override fun getBufferedMessageCount(): Int {
        return bufferedMessageCount
    }

    /**
     * Requests the server unsubscribe the client from a topic.
     *
     * @param topic
     * the topic to unsubscribe from. It must match a topic specified
     * on an earlier subscribe.
     * @return token used to track and wait for the unsubscribe to complete. The
     * token will be passed to callback methods if set.
     * @throws MqttException
     * if there was an error unregistering the subscription.
     *
     * @see .unsubscribe
     */
    @Throws(MqttException::class)
    override fun unsubscribe(topic: String): IMqttToken {
        return unsubscribe(topic, null, null)
    }

    /**
     * Requests the server to unsubscribe the client from one or more topics.
     *
     * @param topic
     * one or more topics to unsubscribe from. Each topic must match
     * one specified on an earlier subscription.
     * @return token used to track and wait for the unsubscribe to complete. The
     * token will be passed to callback methods if set.
     * @throws MqttException
     * if there was an error unregistering the subscription.
     *
     * @see .unsubscribe
     */
    @Throws(MqttException::class)
    override fun unsubscribe(topic: Array<String>): IMqttToken {
        return unsubscribe(topic, null, null)
    }

    /**
     * Requests the server to unsubscribe the client from a topics.
     *
     * @param topic
     * the topic to unsubscribe from. It must match a topic specified
     * on an earlier subscribe.
     * @param userContext
     * optional object used to pass context to the callback. Use null
     * if not required.
     * @param callback
     * optional listener that will be notified when unsubscribe has
     * completed
     * @return token used to track and wait for the unsubscribe to complete. The
     * token will be passed to callback methods if set.
     * @throws MqttException
     * if there was an error unregistering the subscription.
     *
     * @see .unsubscribe
     */
    @Throws(MqttException::class)
    override fun unsubscribe(topic: String, userContext: Any?,
                             callback: IMqttActionListener?): IMqttToken {
        val token = MqttTokenAndroid(this, userContext,
                callback)
        val activityToken = storeToken(token)
        mqttService?.unsubscribe(clientHandle!!, topic, null, activityToken)
        return token
    }

    /**
     * Requests the server to unsubscribe the client from one or more topics.
     *
     *
     * Unsubcribing is the opposite of subscribing. When the server receives the
     * unsubscribe request it looks to see if it can find a matching
     * subscription for the client and then removes it. After this point the
     * server will send no more messages to the client for this subscription.
     *
     *
     *
     * The topic(s) specified on the unsubscribe must match the topic(s)
     * specified in the original subscribe request for the unsubscribe to
     * succeed
     *
     *
     *
     * The method returns control before the unsubscribe completes. Completion
     * can be tracked by:
     *
     *
     *  * Waiting on the returned token [MqttToken.waitForCompletion]
     * or
     *  * Passing in a callback [IMqttActionListener] to this method
     *
     *
     * @param topic
     * one or more topics to unsubscribe from. Each topic must match
     * one specified on an earlier subscription.
     * @param userContext
     * optional object used to pass context to the callback. Use null
     * if not required.
     * @param callback
     * optional listener that will be notified when unsubscribe has
     * completed
     * @return token used to track and wait for the unsubscribe to complete. The
     * token will be passed to callback methods if set.
     * @throws MqttException
     * if there was an error unregistering the subscription.
     */
    @Throws(MqttException::class)
    override fun unsubscribe(topic: Array<String>, userContext: Any?,
                             callback: IMqttActionListener?): IMqttToken {
        val token = MqttTokenAndroid(this, userContext,
                callback)
        val activityToken = storeToken(token)
        mqttService?.unsubscribe(clientHandle!!, topic, null, activityToken)
        return token
    }

    /**
     * Returns the delivery tokens for any outstanding publish operations.
     *
     *
     * If a client has been restarted and there are messages that were in the
     * process of being delivered when the client stopped, this method returns a
     * token for each in-flight message to enable the delivery to be tracked.
     * Alternately the [MqttCallback.deliveryComplete]
     * callback can be used to track the delivery of outstanding messages.
     *
     *
     *
     * If a client connects with cleanSession true then there will be no
     * delivery tokens as the cleanSession option deletes all earlier state. For
     * state to be remembered the client must connect with cleanSession set to
     * false
     *
     *
     * @return zero or more delivery tokens
     */
    override fun getPendingDeliveryTokens(): Array<IMqttDeliveryToken> {
        return mqttService?.getPendingDeliveryTokens(clientHandle!!) ?: emptyArray()
    }

    /**
     * Sets a callback listener to use for events that happen asynchronously.
     *
     *
     * There are a number of events that the listener will be notified about.
     * These include:
     *
     *
     *  * A new message has arrived and is ready to be processed
     *  * The connection to the server has been lost
     *  * Delivery of a message to the server has completed
     *
     *
     *
     * Other events that track the progress of an individual operation such as
     * connect and subscribe can be tracked using the [MqttToken] returned
     * from each non-blocking method or using setting a
     * [IMqttActionListener] on the non-blocking method.
     *
     *
     *
     * @param callback
     * which will be invoked for certain asynchronous events
     *
     * @see MqttCallback
     */
    override fun setCallback(callback: MqttCallback?) {
        this.callback = callback

    }

    /**
     * identify the callback to be invoked when making tracing calls back into
     * the Activity
     *
     * @param traceCallback handler
     */
    fun setTraceCallback(traceCallback: MqttTraceHandler) {
        this.traceCallback = traceCallback
        // mqttService.setTraceCallbackId(traceCallbackId);
    }

    /**
     * turn tracing on and off
     *
     * @param traceEnabled
     * set `true` to enable trace, otherwise, set
     * `false` to disable trace
     */
    fun setTraceEnabled(traceEnabled: Boolean) {
        this.traceEnabled = traceEnabled
        if (mqttService != null)
            mqttService?.isTraceEnabled = traceEnabled
    }

    /**
     *
     *
     * Process incoming Intent objects representing the results of operations
     * and asynchronous activities such as message received
     *
     *
     *
     * **Note:** This is only a public method because the Android
     * APIs require such.<br></br>
     * This method should not be explicitly invoked.
     *
     */
    override fun onReceive(context: Context, intent: Intent) {
        val data = intent.extras

        val handleFromIntent = data!!
                .getString(CALLBACK_CLIENT_HANDLE)

        if (handleFromIntent == null || handleFromIntent != clientHandle) {
            return
        }

        val action = data.getString(CALLBACK_ACTION)

        if (CONNECT_ACTION.equals(action)) {
            connectAction(data)
        } else if (CONNECT_EXTENDED_ACTION.equals(action)) {
            connectExtendedAction(data)
        } else if (MESSAGE_ARRIVED_ACTION.equals(action)) {
            messageArrivedAction(data)
        } else if (SUBSCRIBE_ACTION.equals(action)) {
            subscribeAction(data)
        } else if (UNSUBSCRIBE_ACTION.equals(action)) {
            unSubscribeAction(data)
        } else if (SEND_ACTION.equals(action)) {
            sendAction(data)
        } else if (MESSAGE_DELIVERED_ACTION.equals(action)) {
            messageDeliveredAction(data)
        } else if (ON_CONNECTION_LOST_ACTION
                        .equals(action)) {
            connectionLostAction(data)
        } else if (DISCONNECT_ACTION.equals(action)) {
            disconnected(data)
        } else if (TRACE_ACTION.equals(action)) {
            traceAction(data)
        } else {
            mqttService?.traceError(MqttService.TAG, "Callback action doesn't exist.")
        }

    }

    /**
     * Acknowledges a message received on the
     * [MqttCallback.messageArrived]
     *
     * @param messageId
     * the messageId received from the MqttMessage (To access this
     * field you need to cast [MqttMessage] to
     * [ParcelableMqttMessage])
     * @return whether or not the message was successfully acknowledged
     */
    fun acknowledgeMessage(messageId: String): Boolean {
        if (messageAck == Ack.MANUAL_ACK) {
            val status = mqttService?.acknowledgeMessageArrival(clientHandle!!, messageId)
            return status === Status.OK
        }
        return false

    }

    @Throws(MqttException::class)
    override fun messageArrivedComplete(messageId: Int, qos: Int) {
        throw UnsupportedOperationException()
    }

    override fun setManualAcks(manualAcks: Boolean) {
        throw UnsupportedOperationException()
    }

    /**
     * Process the results of a connection
     *
     * @param data
     */
    private fun connectAction(data: Bundle) {
        val token = connectToken
        removeMqttToken(data)

        simpleAction(token, data)
    }


    /**
     * Process a notification that we have disconnected
     *
     * @param data
     */
    private fun disconnected(data: Bundle) {
        clientHandle = null // avoid reuse!
        val token = removeMqttToken(data)
        if (token != null) {
            (token as MqttTokenAndroid).notifyComplete()
        }
        if (callback != null) {
            callback!!.connectionLost(null)
        }
    }

    /**
     * Process a Connection Lost notification
     *
     * @param data
     */
    private fun connectionLostAction(data: Bundle) {
        if (callback != null) {
            val reason = data
                    .getSerializable(CALLBACK_EXCEPTION) as Exception?
            callback!!.connectionLost(reason)
        }
    }

    private fun connectExtendedAction(data: Bundle) {
        // This is called differently from a normal connect

        if (callback is MqttCallbackExtended) {
            val reconnect = data.getBoolean(CALLBACK_RECONNECT, false)
            val serverURI = data.getString(CALLBACK_SERVER_URI)
            (callback as MqttCallbackExtended).connectComplete(reconnect, serverURI)
        }

    }

    /**
     * Common processing for many notifications
     *
     * @param token
     * the token associated with the action being undertake
     * @param data
     * the result data
     */
    private fun simpleAction(token: IMqttToken?, data: Bundle) {
        if (token != null) {
            val status = data
                    .getSerializable(CALLBACK_STATUS) as Status?
            if (status === Status.OK) {
                (token as MqttTokenAndroid).notifyComplete()
            } else {
                val exceptionThrown = data.getSerializable(CALLBACK_EXCEPTION) as Exception?
                (token as MqttTokenAndroid).notifyFailure(exceptionThrown)
            }
        } else {
            mqttService?.traceError(MqttService.TAG, "simpleAction : token is null")
        }
    }

    /**
     * Process notification of a publish(send) operation
     *
     * @param data
     */
    private fun sendAction(data: Bundle) {
        val token = getMqttToken(data) // get, don't remove - will
        // remove on delivery
        simpleAction(token, data)
    }

    /**
     * Process notification of a subscribe operation
     *
     * @param data
     */
    private fun subscribeAction(data: Bundle) {
        val token = removeMqttToken(data)
        simpleAction(token, data)
    }

    /**
     * Process notification of an unsubscribe operation
     *
     * @param data
     */
    private fun unSubscribeAction(data: Bundle) {
        val token = removeMqttToken(data)
        simpleAction(token, data)
    }

    /**
     * Process notification of a published message having been delivered
     *
     * @param data
     */
    private fun messageDeliveredAction(data: Bundle) {
        val token = removeMqttToken(data)
        if (token != null) {
            if (callback != null) {
                val status = data
                        .getSerializable(CALLBACK_STATUS) as Status?
                if (status === Status.OK && token is IMqttDeliveryToken) {
                    callback!!.deliveryComplete(token as IMqttDeliveryToken?)
                }
            }
        }
    }

    /**
     * Process notification of a message's arrival
     *
     * @param data
     */
    private fun messageArrivedAction(data: Bundle) {
        if (callback != null) {
            val messageId = data
                    .getString(CALLBACK_MESSAGE_ID)
            val destinationName = data
                    .getString(CALLBACK_DESTINATION_NAME)

            val message = data
                    .getParcelable<ParcelableMqttMessage>(CALLBACK_MESSAGE_PARCEL)
            try {
                if (messageAck == Ack.AUTO_ACK) {
                    callback!!.messageArrived(destinationName, message)
                    mqttService?.acknowledgeMessageArrival(clientHandle!!, messageId!!)
                } else {
                    message!!.messageId = messageId
                    callback!!.messageArrived(destinationName, message)
                }

                // let the service discard the saved message details
            } catch (e: Exception) {
                Log.e(TAG, "Error occurred while trying to handle arrived message", e)
            }

        }
    }

    /**
     * Process trace action - pass trace data back to the callback
     *
     * @param data
     */
    private fun traceAction(data: Bundle) {

        if (traceCallback != null) {
            val severity = data.getString(CALLBACK_TRACE_SEVERITY)
            val message = data.getString(CALLBACK_ERROR_MESSAGE)
            val tag = data.getString(CALLBACK_TRACE_TAG)
            if (TRACE_DEBUG.equals(severity))
                traceCallback!!.traceDebug(tag!!, message!!)
            else if (TRACE_ERROR.equals(severity))
                traceCallback!!.traceError(tag!!, message!!)
            else {
                val e = data.getSerializable(CALLBACK_EXCEPTION) as Exception?
                traceCallback!!.traceException(tag!!, message!!, e!!)
            }
        }
    }

    /**
     * @param token
     * identifying an operation
     * @return an identifier for the token which can be passed to the Android
     * Service
     */
    @Synchronized
    private fun storeToken(token: IMqttToken?): String {
        tokenMap.put(tokenNumber, token)
        return Integer.toString(tokenNumber++)
    }

    /**
     * Get a token identified by a string, and remove it from our map
     *
     * @param data
     * @return the token
     */
    @Synchronized
    private fun removeMqttToken(data: Bundle): IMqttToken? {

        val activityToken = data.getString(CALLBACK_ACTIVITY_TOKEN)
        if (activityToken != null) {
            val tokenNumber = Integer.parseInt(activityToken)
            val token = tokenMap.get(tokenNumber)
            tokenMap.delete(tokenNumber)
            return token
        }
        return null
    }

    /**
     * Get a token identified by a string, and remove it from our map
     *
     * @param data
     * @return the token
     */
    @Synchronized
    private fun getMqttToken(data: Bundle): IMqttToken {
        val activityToken = data
                .getString(CALLBACK_ACTIVITY_TOKEN)
        return tokenMap.get(Integer.parseInt(activityToken!!))
    }

    /**
     * Sets the DisconnectedBufferOptions for this client
     * @param bufferOpts the DisconnectedBufferOptions
     */
    override fun setBufferOpts(bufferOpts: DisconnectedBufferOptions) {
        mqttService?.setBufferOpts(clientHandle!!, bufferOpts)
    }

    override fun getBufferedMessage(bufferIndex: Int): MqttMessage? {
        return mqttService?.getBufferedMessage(clientHandle!!, bufferIndex)
    }

    override fun deleteBufferedMessage(bufferIndex: Int) {
        mqttService?.deleteBufferedMessage(clientHandle!!, bufferIndex)
    }

    override fun getInFlightMessageCount(): Int {
        return inFlightMessageCount
    }

    /**
     * Get the SSLSocketFactory using SSL key store and password
     *
     *
     * A convenience method, which will help user to create a SSLSocketFactory
     * object
     *
     *
     * @param keyStore
     * the SSL key store which is generated by some SSL key tool,
     * such as keytool in Java JDK
     * @param password
     * the password of the key store which is set when the key store
     * is generated
     * @return SSLSocketFactory used to connect to the server with SSL
     * authentication
     * @throws MqttSecurityException
     * if there was any error when getting the SSLSocketFactory
     */
    @Throws(MqttSecurityException::class)
    fun getSSLSocketFactory(keyStore: InputStream, password: String): SSLSocketFactory? {
        try {
            var ctx: SSLContext? = null
            var sslSockFactory: SSLSocketFactory? = null
            val ts: KeyStore
            ts = KeyStore.getInstance("BKS")
            ts.load(keyStore, password.toCharArray())
            val tmf = TrustManagerFactory.getInstance("X509")
            tmf.init(ts)
            val tm = tmf.trustManagers
            ctx = SSLContext.getInstance("TLSv1")
            ctx!!.init(null, tm, null)

            sslSockFactory = ctx.socketFactory
            return sslSockFactory

        } catch (e: KeyStoreException) {
            throw MqttSecurityException(e)
        } catch (e: CertificateException) {
            throw MqttSecurityException(e)
        } catch (e: IOException) {
            throw MqttSecurityException(e)
        } catch (e: NoSuchAlgorithmException) {
            throw MqttSecurityException(e)
        } catch (e: KeyManagementException) {
            throw MqttSecurityException(e)
        }

    }

    @Throws(MqttException::class)
    override fun disconnectForcibly() {
        throw UnsupportedOperationException()
    }

    @Throws(MqttException::class)
    override fun disconnectForcibly(disconnectTimeout: Long) {
        throw UnsupportedOperationException()
    }

    @Throws(MqttException::class)
    override fun disconnectForcibly(quiesceTimeout: Long, disconnectTimeout: Long) {
        throw UnsupportedOperationException()
    }

    /**
     * Checks if client or service are null, and then if the client is already connected
     * If none of the above are true, will attempt a reconnect via doConnect
     * @throws MqttException
     */
    @Throws(MqttException::class)
    override fun reconnect() {
        if (clientHandle == null || mqttService == null) {
            throw ExceptionHelper.createMqttException(MqttException.REASON_CODE_UNEXPECTED_ERROR.toInt())
        }
        if (isConnected) {
            throw ExceptionHelper.createMqttException(MqttException.REASON_CODE_CLIENT_CONNECTED.toInt())
        }
        val connectTokenInternal = doConnect()
        connectToken = (connectToken as? MqttTokenAndroid)?.apply {
            setDelegate(connectTokenInternal)
        } ?: connectTokenInternal
    }

    @Throws(MqttException::class)
    override fun removeMessage(token: IMqttDeliveryToken): Boolean {
        // TODO: Add removal to service, then call to remove here.
        return false
    }

    /**
     * Unregister receiver which receives intent from MqttService avoids
     * IntentReceiver leaks.
     */
    fun unregisterResources() {
        if (receiverRegistered) {
            synchronized(this@MqttAndroidClient) {
                LocalBroadcastManager.getInstance(myContext!!).unregisterReceiver(this)
                receiverRegistered = false
            }
            if (bindedService) {
                try {
                    myContext!!.unbindService(serviceConnection)
                    bindedService = false
                } catch (e: IllegalArgumentException) {
                    //Ignore unbind issue.
                }

            }
        }
    }

    /**
     * Register receiver to receiver intent from MqttService. Call this method
     * when activity is hidden and become to show again.
     *
     * @param context
     * - Current activity context.
     */
    fun registerResources() {
        if (!receiverRegistered) {
            registerReceiver(this)
        }
    }

    companion object {
        private val TAG = "MqttAndroidClient"
        private val SERVICE_NAME = "org.eclipse.paho.android.service.MqttService"

        private val BIND_SERVICE_FLAG = 0

        val pool = Executors.newCachedThreadPool()
    }
}
