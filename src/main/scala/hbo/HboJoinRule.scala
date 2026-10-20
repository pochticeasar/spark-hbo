package hbo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Приложение №1 (аналог join operator selection в Ultron):
 * если в истории есть реальный размер стороны джойна и он меньше
 * spark.sql.autoBroadcastJoinThreshold, ставим BROADCAST-хинт ещё до
 * выполнения. Без истории Spark строит sort-merge join, и AQE переключается
 * на broadcast только после того, как обе стороны уже отшаффлены.
 *
 * Правило консервативно: трогает только джойны без пользовательских хинтов,
 * только equi-join и только ту сторону, которую тип джойна позволяет
 * транслировать. Изменение монотонное — размер стороны от смены алгоритма
 * джойна не меняется, поэтому план сходится за одну итерацию.
 *
 * В истории лежит число строк; размер в байтах считается так же, как в оценщике
 * Spark: строки × ширина строки по схеме (EstimationUtils.getSizePerRow). Хинт
 * ставится, только если Spark сам бы НЕ выбрал broadcast (его оценка выше порога),
 * а наблюдение говорит, что сторона помещается.
 */
class HboJoinRule(spark: SparkSession, store: HistoryStore) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!Hbo.enabled) return plan
    val threshold = spark.sessionState.conf.autoBroadcastJoinThreshold
    if (threshold <= 0) return plan

    plan transformUp {
      case j @ Join(left, right, joinType, Some(_), hint)
          if hint.leftHint.isEmpty && hint.rightHint.isEmpty =>

        // наблюдаемый размер в байтах, только если он меняет решение Spark
        def observed(side: LogicalPlan): Option[BigInt] =
          if (side.stats.sizeInBytes <= threshold) None  // Spark и так выберет broadcast
          else Fingerprint.of(side).flatMap(store.get).map { rows =>
            BigInt(rows) * EstimationUtils.getSizePerRow(side.output)
          }.filter(_ <= threshold)

        val leftObs = if (canBuildLeft(joinType)) observed(left) else None
        val rightObs = if (canBuildRight(joinType)) observed(right) else None

        val broadcastHint = Some(HintInfo(strategy = Some(BROADCAST)))
        (leftObs, rightObs) match {
          case (Some(l), Some(r)) if l <= r => log(j, "left", l); j.copy(hint = JoinHint(broadcastHint, None))
          case (_, Some(r)) => log(j, "right", r); j.copy(hint = JoinHint(None, broadcastHint))
          case (Some(l), None) => log(j, "left", l); j.copy(hint = JoinHint(broadcastHint, None))
          case _ => j
        }
    }
  }

  private def canBuildLeft(t: JoinType): Boolean = t match {
    case Inner | Cross | RightOuter => true
    case _ => false
  }

  private def canBuildRight(t: JoinType): Boolean = t match {
    case Inner | Cross | LeftOuter | LeftSemi | LeftAnti | _: ExistenceJoin => true
    case _ => false
  }

  private def log(j: Join, side: String, size: BigInt): Unit = {
    Hbo.interventions.incrementAndGet()
    val est = (if (side == "left") j.left else j.right).stats.sizeInBytes
    println(f"[HBO] broadcast $side side of ${j.joinType}: estimate=${fmt(est)} observed=${fmt(size)}")
  }

  private def fmt(b: BigInt): String =
    if (b > BigInt(1L << 30)) f"${(b.toDouble / (1L << 30))}%.1f GB"
    else if (b > BigInt(1L << 20)) f"${(b.toDouble / (1L << 20))}%.1f MB"
    else f"${(b.toDouble / 1024)}%.1f KB"
}
