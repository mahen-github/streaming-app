package com.icc.poc

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.HasOffsetRanges

class KafkaUtilService {
  val retryPolicy = new ExponentialBackoffRetry(1000, 3)
  val path = Constants.KAFKA_CH_ROOT + Constants.ZK_PATH

  private def marshall(offsets: String): Map[Int, Long] = {
    offsets.split(",")
      .map(s => s.split(":"))
      .map { case Array(partition, offset) => (partition.toInt, offset.toLong) }
      .toMap
  }

  private def unmarshall(offsets: Map[Int, Long]) = offsets.map {
    case (partition, offset) => s"${partition}:${offset}"
  } mkString ","

  def app_zk_path = path + "/edhTopic/StreamingAPP"

  private def withCurator[T](f: CuratorFramework => T): T = {
    val client: CuratorFramework = CuratorFrameworkFactory.newClient(Constants.ZK_CONN_STRING, retryPolicy)
    client.start()
    try {
      f(client)
    } finally {
      IOUtils.closeQuietly(client)
    }
  }

  def getOffsets(): Option[Map[Int, Long]] = {
    val data = withCurator { (client) =>
      if (client.checkExists().forPath(app_zk_path) != null) {
      new String(client.getData.forPath(app_zk_path))
      }
      else{
        null
      }
    }
    data match {
      case offsetsRangesStr if (!StringUtils.isBlank(offsetsRangesStr)) => Some(marshall(offsetsRangesStr))
      case _ => None
    }
  }
  def setData(rdd: RDD[(String)]) {
    val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    val data = offsetRanges.map { x => s"${x.partition}:${x.fromOffset}" }.mkString(",")
    withCurator { (client) =>
      if (client.checkExists().forPath(app_zk_path) != null) {
        client.setData().forPath(app_zk_path, data.getBytes)
      } else {
        client.create().forPath(app_zk_path)
      }
    }
  }
}

object KafkaUtilService {

  def configure(): KafkaUtilService = {
    val kafkaService = new KafkaUtilService
    //    kafkaService.setData()
    //    val in = kafkaService.getOffsets
    //    val putElem: PartialFunction[Long, List[Long]] = {
    //      case d: Long if d > 0 => List(d)
    //    }
    //    val arr = List[Long]()
    //    val printMap: PartialFunction[Map[Int, Long], Iterable[Long]] = {
    //      case x => x.values
    //    }
    //    in.collect(printMap).foreach { x => println(x) }
    kafkaService
  }

}