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
 */
package org.eclipse.paho.android.service

import org.eclipse.paho.client.mqttv3.IMqttActionListener
import org.eclipse.paho.client.mqttv3.IMqttAsyncClient
import org.eclipse.paho.client.mqttv3.IMqttToken
import org.eclipse.paho.client.mqttv3.MqttException
import org.eclipse.paho.client.mqttv3.internal.wire.MqttWireMessage

/**
 *
 *
 * Implementation of the IMqttToken interface for use from within the
 * MqttAndroidClient implementation
 */
internal open class MqttTokenAndroid @JvmOverloads constructor(private val client: MqttAndroidClient,
                                                               private var userContext: Any? = null,
                                                               private var listener: IMqttActionListener? = null,
                                                               topics: Array<String>? = null) : IMqttToken {

    @Volatile
    private var isComplete = false
    @Volatile
    private var sessionPresent: Boolean? = null
    @Volatile
    private var lastException: MqttException? = null
    private val waitObject = Object()
    private val topics: Array<String>
    private var delegate // specifically for getMessageId
            : IMqttToken? = null
    private var pendingException: MqttException? = null

    /**
     * @see org.eclipse.paho.client.mqttv3.IMqttToken.waitForCompletion
     */
    @Throws(MqttException::class)
    override fun waitForCompletion() {
        synchronized(waitObject) {
            try {
                waitObject.wait()
            } catch (e: InterruptedException) {
                // do nothing
            }
        }
        pendingException?.let {
            throw it
        }
    }

    /**
     * @see org.eclipse.paho.client.mqttv3.IMqttToken.waitForCompletion
     */
    @Throws(MqttException::class)
    override fun waitForCompletion(timeout: Long) {
        synchronized(waitObject) {
            try {
                waitObject.wait(timeout)
            } catch (e: InterruptedException) {
                // do nothing
            }
            if (!isComplete) {
                throw MqttException(MqttException.REASON_CODE_CLIENT_TIMEOUT.toInt())
            }
            pendingException?.let {
                throw it
            }
        }
    }

    /**
     * notify successful completion of the operation
     */
    fun notifyComplete() {
        synchronized(waitObject) {
            isComplete = true
            waitObject.notifyAll()
            listener?.onSuccess(this)
        }
    }

    /**
     * notify unsuccessful completion of the operation
     */
    fun notifyFailure(exception: Throwable?) {
        synchronized(waitObject) {
            isComplete = true
            pendingException = if (exception is MqttException) {
                exception
            } else {
                MqttException(exception)
            }
            waitObject.notifyAll()
            if (exception is MqttException) {
                lastException = exception
            }
            listener?.onFailure(this, exception)
        }
    }

    /**
     * @see org.eclipse.paho.client.mqttv3.IMqttToken.isComplete
     */
    override fun isComplete(): Boolean {
        return isComplete
    }

    fun setComplete(complete: Boolean) {
        isComplete = complete
    }

    /**
     * @see org.eclipse.paho.client.mqttv3.IMqttToken.getException
     */
    override fun getException(): MqttException {
        return lastException!!
    }

    fun setException(exception: MqttException?) {
        lastException = exception
    }

    /**
     * @see org.eclipse.paho.client.mqttv3.IMqttToken.getClient
     */
    override fun getClient(): IMqttAsyncClient {
        return client
    }

    /**
     * @see org.eclipse.paho.client.mqttv3.IMqttToken.setActionCallback
     */
    override fun setActionCallback(listener: IMqttActionListener) {
        this.listener = listener
    }

    /**
     * @see org.eclipse.paho.client.mqttv3.IMqttToken.getActionCallback
     */
    override fun getActionCallback(): IMqttActionListener? {
        return listener
    }

    /**
     * @see org.eclipse.paho.client.mqttv3.IMqttToken.getTopics
     */
    override fun getTopics(): Array<String> {
        return topics
    }

    /**
     * @see org.eclipse.paho.client.mqttv3.IMqttToken.setUserContext
     */
    override fun setUserContext(userContext: Any) {
        this.userContext = userContext
    }

    /**
     * @see org.eclipse.paho.client.mqttv3.IMqttToken.getUserContext
     */
    override fun getUserContext(): Any? {
        return userContext
    }

    fun setDelegate(delegate: IMqttToken?) {
        this.delegate = delegate
    }

    fun setSessionPresent(isPresent: Boolean){
        sessionPresent = isPresent
    }

    /**
     * @see org.eclipse.paho.client.mqttv3.IMqttToken.getMessageId
     */
    override fun getMessageId(): Int {
        return if (delegate != null) delegate!!.messageId else 0
    }

    override fun getResponse(): MqttWireMessage {
        return delegate!!.response
    }

    override fun getSessionPresent(): Boolean {
        return sessionPresent ?: delegate?.sessionPresent ?: false
    }

    override fun getGrantedQos(): IntArray {
        return delegate?.grantedQos ?: emptyArray<Int>().toIntArray()
    }
    /**
     * Constructor for use with subscribe operations
     *
     * @param client used to pass MqttAndroidClient object
     * @param userContext used to pass context
     * @param listener optional listener that will be notified when the action completes. Use null if not required.
     * @param topics topics to subscribe to, which can include wildcards.
     */
    /**
     * Standard constructor
     *
     * @param client used to pass MqttAndroidClient object
     * @param userContext used to pass context
     * @param listener optional listener that will be notified when the action completes. Use null if not required.
     */
    init {
        this.topics = topics!!
    }
}