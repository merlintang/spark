/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.ml

import org.apache.spark.scheduler.{LiveListenerBus, SparkListener, SparkListenerEvent}
import org.apache.spark.util.ListenerBus

/**
 * A ML listener bus to forward events to MLListeners. This one will wrap received
 * ML events as MLListenEvent and send them to Spark listener bus.
 */
private[ML] class MLListenerBus (sparkListenerBus: LiveListenerBus)
  extends SparkListener with ListenerBus[MLListener, MLListenerEvent] {


  /**
   * Post a MLListenerEvent to the Spark listener bus asynchronously. This event will be
   * dispatched to all StreamingListeners in the thread of the Spark listener bus.
   */
  def post(event: MLListenerEvent) {
    sparkListenerBus.post(new WrappedMLListenerEvent(event))
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case WrappedMLListenerEvent(e) =>
        postToAll(e)
      case _ =>
    }
  }

  protected override def doPostEvent(
	listener: MLListener,
        event: MLListenerEvent): Unit = {
    event match {
      case modelFitStarted: MLListenerModelFitStarted =>
        listener.onModelFitStarted(modelFitStarted)
      case modelFitEnded: MLListenerModelFitEnd =>
        listener.onModelFitEnd(modelFitEnded)
      case _ =>
    }
  }
  
  /**
   * Register this one with the Spark listener bus so that it can receive ML events and
   * forward them to StreamingListeners.
   */
  def start(): Unit = {
    sparkListenerBus.addToStatusQueue(this)
  }

  /**
   * Unregister this one with the Spark listener bus and all MLListner won't receive any
   * events after that.
   */
  def stop(): Unit = {
    sparkListenerBus.removeListener(this)
  }


  /**
   * Wrapper for StreamingListenerEvent as SparkListenerEvent so that it can be posted to Spark
   * listener bus.
   */
  private case class WrappedMLListenerEvent(mlListenerEvent: MLListenerEvent)
    extends SparkListenerEvent {

    // Do not log streaming events in event log as history server does not support streaming
    // events (SPARK-12140). TODO Once SPARK-12140 is resolved we should set it to true.
    protected[spark] override def logEvent: Boolean = false
  }

}
