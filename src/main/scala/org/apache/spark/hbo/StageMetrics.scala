package org.apache.spark.hbo

import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.JavaConverters._

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted}

/**
 * Метрики стадий для замеров «цены исправления AQE».
 *
 * Лежит в пакете org.apache.spark, чтобы дотянуться до private[spark]
 * `listenerBus.waitUntilEmpty()`: события листенеров доставляются асинхронно,
 * и без ожидания метрики последних стадий запроса терялись бы.
 */
case class StageStat(
    stageId: Int,
    name: String,
    numTasks: Int,
    durationMs: Long,
    taskTimeMs: Long,
    shuffleWriteBytes: Long,
    shuffleWriteTimeNs: Long,
    shuffleLocalReadBytes: Long,
    shuffleRemoteReadBytes: Long)

class StageMetricsListener extends SparkListener {
  private val stages = new ConcurrentLinkedQueue[StageStat]()

  override def onStageCompleted(e: SparkListenerStageCompleted): Unit = {
    val i = e.stageInfo
    val m = i.taskMetrics
    if (m == null) return
    val duration = (for (s <- i.submissionTime; c <- i.completionTime) yield c - s).getOrElse(0L)
    stages.add(StageStat(
      i.stageId, i.name, i.numTasks, duration,
      m.executorRunTime,
      m.shuffleWriteMetrics.bytesWritten,
      m.shuffleWriteMetrics.writeTime,
      m.shuffleReadMetrics.localBytesRead,
      m.shuffleReadMetrics.remoteBytesRead))
  }

  def reset(): Unit = stages.clear()

  def snapshot(): Seq[StageStat] = stages.asScala.toSeq.sortBy(_.stageId)
}

object StageMetrics {
  /** Дождаться, пока все события листенеров обработаны. */
  def drain(sc: SparkContext): Unit = sc.listenerBus.waitUntilEmpty()
}
