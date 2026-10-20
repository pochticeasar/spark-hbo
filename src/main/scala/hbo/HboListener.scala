package hbo

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, BroadcastQueryStageExec, QueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.exchange.{Exchange, ReusedExchangeExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.util.QueryExecutionListener

/**
 * Сбор истории: после каждого успешного запроса проходим по финальному
 * физическому плану AQE, берём материализованные shuffle-стадии и их
 * реальные размеры (те же runtime-статистики, по которым решает сам AQE),
 * сопоставляем стадию с логическим подпланом через logicalLink и пишем
 * отпечаток -> размер в историю.
 *
 * Заодно сохраняем короткий отчёт о запросе: какие джойны были в исходном
 * и в финальном плане — так видно, где AQE «исправлял» план на лету.
 */
class HboListener(store: HistoryStore) extends QueryExecutionListener {

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    try record(qe, durationNs) catch {
      case e: Throwable => println(s"[HBO] failed to record history: $e")
    }
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = ()

  private def record(qe: QueryExecution, durationNs: Long): Unit = {
    val (initial, finalPlan) = qe.executedPlan match {
      case a: AdaptiveSparkPlanExec => (a.inputPlan, a.executedPlan)
      case p => (p, p)
    }

    // И shuffle-, и broadcast-стадии. Раньше брались только shuffle-стадии, и после того
    // как HBO поставил broadcast, у стороны джойна больше не было shuffle-стадии:
    // запись о ней замораживалась и не обновлялась при изменении данных.
    var recorded = 0
    stages(finalPlan).foreach {
      case s @ (_: ShuffleQueryStageExec | _: BroadcastQueryStageExec) if s.isMaterialized =>
        val exchangeChild = s.plan match {
          case e: Exchange => Some(e.child)
          case r: ReusedExchangeExec => Some(r.child.child)
          case _ => None
        }
        for {
          child <- exchangeChild
          logical <- linkedLogical(child)
          key <- Fingerprint.of(logical)
        } {
          // Храним ЧИСЛО СТРОК, а не байты: shuffle-стадия меряет сериализованные байты,
          // broadcast-стадия — память хэш-таблицы (минимум ~1 МБ даже для пяти строк),
          // и сравнивать их между собой и с порогом нельзя. Строки одинаковы у обеих,
          // а байты правило посчитает тем же способом, что и оценщик Spark.
          s.getRuntimeStatistics.rowCount.foreach { rows =>
            store.put(key, rows.toLong)
            recorded += 1
          }
        }
      case _ =>
    }
    if (recorded > 0) store.flush()

    Hbo.lastReport = Report(
      durationMs = durationNs / 1000000,
      initialJoins = joins(initial),
      finalJoins = joins(finalPlan),
      recordedStages = recorded)
  }

  /** Все query stages, включая вложенные в другие стадии. */
  private def stages(p: SparkPlan): Seq[QueryStageExec] = p match {
    case s: QueryStageExec => s +: stages(s.plan)
    case a: AdaptiveSparkPlanExec => stages(a.executedPlan)
    case other => other.children.flatMap(stages)
  }

  /**
   * Логический узел, из которого спланирован корень физического поддерева.
   * Пропускаем только «обёртки» без своей логики (codegen, колоночные адаптеры):
   * спускаться глубже нельзя — иначе размер отфильтрованного выхода
   * приписался бы, например, нефильтрованному скану.
   */
  private def linkedLogical(p: SparkPlan): Option[LogicalPlan] = p.logicalLink.orElse {
    p match {
      case w: WholeStageCodegenExec => linkedLogical(w.child)
      case i: InputAdapter => linkedLogical(i.child)
      case c: ColumnarToRowExec => linkedLogical(c.child)
      case _ => None
    }
  }

  private def joins(p: SparkPlan): Seq[String] = {
    def walk(n: SparkPlan): Seq[String] = {
      val self = n match {
        case _: SortMergeJoinExec => Seq("SMJ")
        case _: BroadcastHashJoinExec => Seq("BHJ")
        case _: ShuffledHashJoinExec => Seq("SHJ")
        case _ => Nil
      }
      val kids = n match {
        case s: QueryStageExec => Seq(s.plan)
        case a: AdaptiveSparkPlanExec => Seq(a.executedPlan)
        case other => other.children
      }
      self ++ kids.flatMap(walk)
    }
    walk(p)
  }
}

case class Report(durationMs: Long, initialJoins: Seq[String], finalJoins: Seq[String], recordedStages: Int) {
  override def toString: String =
    s"time=${durationMs}ms joins: initial=${initialJoins.mkString(",")} final=${finalJoins.mkString(",")} recorded=$recordedStages"
}
