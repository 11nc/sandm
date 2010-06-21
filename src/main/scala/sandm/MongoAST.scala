package sandm

import _root_.com.mongodb._
import _root_.com.mongodb.{BasicDBObject => DBO}

import _root_.scala.collection.jcl
import _root_.scala.reflect.Manifest
import _root_.scala.util.matching._

object MongoAST {
	sealed abstract class MVal {
	  def isEmpty: Boolean
		def render: Any = this match {
			case MNothing => new DBO
			case MId(oid) => oid
			case MBool(b) => b
			case MString(s) => s
			case MInt(i) => i
			case MJInt(i) => i
			case MNum(n) => n
			case MJNum(n) => n
			case MDecimal(d) => d
			case MRegex(r) => r.pattern
			case MPattern(p) => p
			// careful, this change, while correct, caused a whole bunch of changes in behavior such as friend requests would return everyone if you had no friend requests
      case f @ MField(n, v) => if (!f.isEmpty) new DBO(n.value, v.render) else new DBO
      // case f @ MField(n, v) => if (v != MNothing) new DBO(n.value, v.render) else new DBO
			case MObject(obj) => obj.filter(f => !f.isEmpty).foldLeft(new DBO) { (dbo, f) => 
				dbo.putAll(f.render.asInstanceOf[DBObject])
				dbo 
			}
			case MArray(arr) => arr.filter(v => !v.isEmpty).foldLeft(new jcl.ArrayList[Any]()) { (al, v) => 
				al += v.render
				al
			}.underlying
			case MDbo(dbo) => dbo
		}
	}

	case object MNothing extends MVal {
	  // only time we prune a field is in the event of a None
	  val isEmpty = true
	}

	object MId {
		def apply(oid: ObjectId) = new MId(oid)
		def apply(s: String) = new MId(s)
		def unapply(mid: MId) = Some((mid.oid))
	}
	
	class MId(val oid: ObjectId) extends MVal {
		def this(s: String) = this(ObjectId.massageToObjectId(s))
		val isEmpty = false
	}
	
	case class MString(s: String) extends MVal { val isEmpty = false }
	case class MInt(i: Int) extends MVal { val isEmpty = false }
	case class MJInt(i: java.lang.Integer) extends MVal { val isEmpty = false }
	case class MNum(l: Long) extends MVal { val isEmpty = false }
	case class MJNum(l: java.lang.Long) extends MVal { val isEmpty = false }
	case class MDecimal(d: Double) extends MVal { val isEmpty = false }
	case class MBool(b: Boolean) extends MVal { val isEmpty = false }
	case class MRegex(r: Regex) extends MVal { val isEmpty = false }
	case class MPattern(p: java.util.regex.Pattern) extends MVal { val isEmpty = false }
	
	case class MField(name: MKey, value: MVal) extends MVal { 
	  val isEmpty = value.isEmpty
	}
	
	case class MObject(obj: List[MField]) extends MVal {
	  val isEmpty = false //obj.forall(_.isEmpty)
	}

	case class MArray(arr: List[MVal]) extends MVal {
	  val isEmpty = false //arr.forall(_.isEmpty)
	}
	
	case class MDbo(dbo: DBObject) extends MVal {
	  val isEmpty = false //dbo.toMap.isEmpty
	}
	
	case class MKey(value: String) {
		def +(other: MKey): MKey = MKey(value + "." + other.value)
	}
}
