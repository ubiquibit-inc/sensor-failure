/*
 * Copyright (c) 2019.
 *
 * This work, (c) by jason@ubiquibit.com
 *
 * This work is licensed under a
 * Creative Commons Attribution-ShareAlike 4.0 International License.
 *
 * You should have received a copy of the license along with this
 * work.  If not, see <http://creativecommons.org/licenses/by-sa/4.0/>.
 *
 */

package com.ubiquibit.buoy

sealed abstract class WxFeedStatus(description: String){
  def desc: String = description
  override def toString: String = desc.toUpperCase
}

/**
  * Weather data import status
  */
object WxFeedStatus{
  val values: Set[WxFeedStatus] = Set(DOWNLOADED, KAFKALOADING, ERROR, KAFKALOADED)
  def valueOf(str: String): Option[WxFeedStatus] = values.find(_.desc.equalsIgnoreCase(str.trim()))
}

// bunnies & flowers
case object DOWNLOADED extends WxFeedStatus("downloaded") // downloaded to the load directory
case object KAFKALOADING extends WxFeedStatus("kafkaLoading") // importing into Kafka
case object KAFKALOADED extends WxFeedStatus("kafkaLoaded") // imported to kafka from load directory
case object SPARKSTAGED extends WxFeedStatus("sparkloadstaged") // a file has been written to the stage directory and will be picked up the next [[WxStream]]
case object SPARKSTREAMING extends WxFeedStatus("sparkStreaming") // being actively streamed
case object KAFKALIVEIMPORT extends WxFeedStatus("kafkaLiveImport") // stream is up and running and Kafka is being send live updates from NDBC

// wolves & rain clouds
case object ERROR extends WxFeedStatus("error")
case object UNSUPPORTED extends WxFeedStatus("unsupported")
