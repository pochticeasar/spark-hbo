package hbo

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.hbo.{StageMetrics, StageMetricsListener}
import org.apache.spark.sql.SparkSession

/**
 * Точка входа. Подключение из PySpark:
 *
 *   spark._jvm.hbo.Hbo.install(spark._jsparkSession, "/tmp/hbo-history.tsv")
 *
 * Правило кладётся в spark.experimental.extraOptimizations: этот батч
 * оптимизатора выполняется последним, поэтому правило видит ровно тот
 * логический план, который пойдёт в физическое планирование, — и отпечатки
 * совпадают с теми, что потом записывает листенер.
 */
object Hbo {
  @volatile var enabled: Boolean = true
  @volatile var lastReport: Report = _
  @volatile private var store: HistoryStore = _
  @volatile private var spark: SparkSession = _

  val interventions = new AtomicInteger(0)
  private val stageListener = new StageMetricsListener

  def install(session: SparkSession, historyPath: String): Unit = synchronized {
    if (store != null) return
    spark = session
    store = HistoryStore.open(historyPath)
    session.experimental.extraOptimizations ++= Seq(new HboJoinRule(session, store))
    session.listenerManager.register(new HboListener(store))
    session.sparkContext.addSparkListener(stageListener)
    println(s"[HBO] installed, history=$historyPath (${store.size} entries)")
  }

  def setEnabled(on: Boolean): Unit = enabled = on

  def clearHistory(): Unit = store.clear()

  def historySize: Int = store.size

  /** Сбросить счётчики перед замером запроса. */
  def resetCounters(): Unit = {
    StageMetrics.drain(spark.sparkContext)
    stageListener.reset()
    interventions.set(0)
  }

  def interventionCount: Int = interventions.get()

  def report: String = String.valueOf(lastReport)

  /**
   * Метрики стадий с момента последнего resetCounters(), одной JSON-строкой:
   * итоги по запросу + разбивка по стадиям.
   */
  def metricsJson: String = {
    StageMetrics.drain(spark.sparkContext)
    val s = stageListener.snapshot()
    def esc(x: String) = x.replace("\\", "\\\\").replace("\"", "\\\"")
    val stagesJson = s.map { st =>
      s"""{"id":${st.stageId},"name":"${esc(st.name.take(60))}","tasks":${st.numTasks},""" +
        s""""duration_ms":${st.durationMs},"task_time_ms":${st.taskTimeMs},""" +
        s""""shuffle_write_bytes":${st.shuffleWriteBytes},"shuffle_write_time_ns":${st.shuffleWriteTimeNs},""" +
        s""""shuffle_local_read_bytes":${st.shuffleLocalReadBytes},"shuffle_remote_read_bytes":${st.shuffleRemoteReadBytes}}"""
    }.mkString(",")
    s"""{"stages":${s.size},""" +
      s""""task_time_ms":${s.map(_.taskTimeMs).sum},""" +
      s""""shuffle_write_bytes":${s.map(_.shuffleWriteBytes).sum},""" +
      s""""shuffle_write_time_ns":${s.map(_.shuffleWriteTimeNs).sum},""" +
      s""""shuffle_local_read_bytes":${s.map(_.shuffleLocalReadBytes).sum},""" +
      s""""shuffle_remote_read_bytes":${s.map(_.shuffleRemoteReadBytes).sum},""" +
      s""""hbo_hints":${interventions.get()},""" +
      s""""report":"${esc(String.valueOf(lastReport))}",""" +
      s""""per_stage":[$stagesJson]}"""
  }
}
