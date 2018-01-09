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

import org.apache.spark.annotation.DeveloperApi


/**
 * :: DeveloperApi ::
 * Base trait for events related to StreamingListener
 */
@DeveloperApi
sealed trait MLListenerEvent

@DeveloperApi
case class MLListenerModelFitStarted(time: Long) extends MLListenerEvent

@DeveloperApi
case class MLListenerModelFitEnd(time: Long) extends MLListenerEvent


/**
 * :: DeveloperApi ::
 * A listener interface for receiving information about an ongoing streaming
 * computation.
 */
@DeveloperApi
trait MLListener {

  /** Called when the pipeline has been started to fit data */
  def onModelFitStarted(streamingStarted: MLListenerModelFitStarted) { }

  /** Called when the pipeline has been started to fit data */
  def onModelFitEnd(streamingStarted: MLListenerModelFitEnd) { }

}

