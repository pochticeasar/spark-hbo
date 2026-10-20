package hbo

import java.security.MessageDigest

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}

/**
 * Отпечаток логического подплана — ключ в истории.
 *
 * Берём канонизированный план (Catalyst сам нормализует exprId, поэтому
 * один и тот же запрос в разных сессиях даёт одинаковое дерево) и дописываем
 * пути файлов у каждого источника: в канонической форме LogicalRelation их нет,
 * и две parquet-таблицы с одинаковой схемой иначе бы совпали.
 *
 * Литералы остаются в ключе — это аналог literal-sensitive hashing из Ultron.
 *
 * В ключ подмешивается «фактор размера» floor(log2(сумма размеров входов)):
 * если таблицы заметно выросли, старая история перестаёт находиться
 * и запрос снова обучается (тот же приём, что size factor в Ultron).
 */
object Fingerprint {

  /** None — если подплан нельзя надёжно опознать (например, содержит стадии AQE). */
  def of(plan: LogicalPlan): Option[String] = {
    // После перепланирования AQE в дереве появляются LogicalQueryStage —
    // такие подпланы не совпадут с исходным оптимизированным планом, пропускаем.
    if (plan.exists(_.nodeName == "LogicalQueryStage")) return None

    val sources = plan.collectLeaves().map {
      case LogicalRelation(fs: HadoopFsRelation, _, _, _) =>
        val paths = fs.location.rootPaths.map(_.toString).sorted.mkString(",")
        (s"fs[$paths]", fs.sizeInBytes)
      case l: LogicalRelation =>
        (s"rel[${l.relation.getClass.getName}]", l.relation.sizeInBytes)
      case other =>
        (s"leaf[${other.nodeName}]", 0L)
    }

    val totalInput = sources.map(_._2).sum
    val sizeFactor = if (totalInput <= 0) 0 else 63 - java.lang.Long.numberOfLeadingZeros(totalInput)

    val text = plan.canonicalized.treeString(verbose = false) +
      "\n#sources=" + sources.map(_._1).mkString(";") +
      "\n#sizeFactor=" + sizeFactor
    Some(sha1(text))
  }

  private def sha1(s: String): String = {
    val d = MessageDigest.getInstance("SHA-1").digest(s.getBytes("UTF-8"))
    d.map("%02x".format(_)).mkString.take(20)
  }
}
