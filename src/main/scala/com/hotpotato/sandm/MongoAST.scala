package com.hotpotato.sandm

import _root_.com.mongodb._
import _root_.com.mongodb.{BasicDBObject => DBO}

import _root_.scala.collection.jcl
import _root_.scala.reflect.Manifest
import _root_.scala.util.matching._

object MongoAST {
	sealed abstract class MVal {
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
			case MField(n, v) => if (v != MNothing) new DBO(n.value, v.render) else new DBO
			case MObject(obj) => obj.foldLeft(new DBO) { (dbo, f: MField) => 
				dbo.putAll(f.render.asInstanceOf[DBObject])
				dbo 
			}
			case MArray(arr) => arr.foldLeft(new jcl.ArrayList[Any]()) { (al, v) => 
				al += v.render
				al
			}.underlying
			case MDbo(dbo) => dbo
		}
	}

	case object MNothing extends MVal

	object MId {
		def apply(oid: ObjectId) = new MId(oid)
		def apply(s: String) = new MId(s)
		def unapply(mid: MId) = Some((mid.oid))
	}
	
	class MId(val oid: ObjectId) extends MVal {
		def this(s: String) = this(ObjectId.massageToObjectId(s))
	}
	
	case class MString(s: String) extends MVal
	case class MInt(i: Int) extends MVal
	case class MJInt(i: java.lang.Integer) extends MVal
	case class MNum(l: Long) extends MVal
	case class MJNum(l: java.lang.Long) extends MVal
	case class MDecimal(d: Double) extends MVal
	case class MBool(b: Boolean) extends MVal
	case class MRegex(r: Regex) extends MVal
	case class MPattern(p: java.util.regex.Pattern) extends MVal
	case class MField(name: MKey, value: MVal) extends MVal
	case class MObject(obj: List[MField]) extends MVal 
  // {
  //  def /(key: MKey) = MObject(obj.filter(f => f.name == key))
  // }
	case class MArray(arr: List[MVal]) extends MVal
	
	case class MDbo(dbo: DBObject) extends MVal
	
	case class MKey(value: String) {
		def +(other: MKey): MKey = MKey(value + "." + other.value)
	}
}
