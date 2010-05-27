package sandm.util

import _root_.scala.collection._

object Timer {
	def apply() = {
		val t = new Timer
		t.start
		t
	}
}

class Timer {
  private var _start: Long = 0L
  private var _end: Long = 0L

	def start: Unit = {
		_start = System.currentTimeMillis
	}

  def stop: Long = {
    _end = System.currentTimeMillis
    (_end - _start)
  }

	private var curSplit = 0
	val splits = mutable.Map[Int, (String, Long)]()

	def split(name: String): Unit = {
		if (curSplit == 0)
			splits(curSplit) = (name, elapsed)
		else
			splits(curSplit) = (name, elapsed - splits.values.foldLeft(0l) {
				(acc, s) => acc + s._2
			})
		curSplit += 1
	}

	def elapsed: Long = System.currentTimeMillis - _start
	
	def formatWithElapsed(operation: String) = operation + " took "+elapsed+"ms"
	
	override def toString = "Timer(elapsed="+elapsed+", splits=("+
		splits.keys.toList.sort(_ < _).map { key: Int =>
			val entry = splits(key)
			entry._1 + "=" + entry._2
		}.mkString(", ") +"))"
}
