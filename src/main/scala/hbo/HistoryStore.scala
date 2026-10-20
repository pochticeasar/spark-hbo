package hbo

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

/**
 * Хранилище истории: отпечаток подплана -> наблюдённое число строк на выходе.
 *
 * Живёт на драйвере и переживает перезапуск приложения через TSV-файл.
 * Для прототипа этого достаточно; в «настоящей» версии сюда встанет
 * Redis / Postgres / RocksDB (как Redis в Presto HBO или Softstore в Ultron).
 */
class HistoryStore(val path: Path) {
  private val map = new ConcurrentHashMap[String, java.lang.Long]()

  if (Files.exists(path)) {
    Files.readAllLines(path, StandardCharsets.UTF_8).asScala.foreach { line =>
      line.split('\t') match {
        case Array(k, v) => map.put(k, java.lang.Long.valueOf(v))
        case _ =>
      }
    }
  }

  def get(key: String): Option[Long] = Option(map.get(key)).map(_.longValue())

  def put(key: String, rows: Long): Unit = map.put(key, rows)

  def size: Int = map.size()

  def clear(): Unit = { map.clear(); flush() }

  def flush(): Unit = synchronized {
    val lines = map.asScala.map { case (k, v) => s"$k\t$v" }.toSeq.asJava
    Option(path.getParent).foreach(Files.createDirectories(_))
    Files.write(path, lines, StandardCharsets.UTF_8,
      StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)
  }
}

object HistoryStore {
  def open(path: String): HistoryStore = new HistoryStore(Paths.get(path))
}
