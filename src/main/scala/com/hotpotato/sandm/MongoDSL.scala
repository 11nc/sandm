package com.hotpotato.sandm

import _root_.com.mongodb._
import _root_.com.mongodb.{BasicDBObject => DBO}

import _root_.scala.collection.jcl
import _root_.scala.reflect.Manifest
import _root_.scala.util.matching._

import MongoAST._

/** 
 * A DSL to produce and manipulate MongoDB DBObjects.
 */
object MongoDSL {
	implicit def int2mval(x: Int) = MInt(x)
	implicit def optint2mval(x: Option[Int]) = x match {
		case Some(i) => MInt(i)
		case _ => MNothing
	}

	implicit def jint2mval(x: java.lang.Integer) = MJInt(x)
	implicit def optjint2mval(x: Option[java.lang.Integer]) = x match {
		case Some(i) => MJInt(i)
		case _ => MNothing
	}

	implicit def long2mval(x: Long) = MNum(x)
	implicit def optlong2mval(x: Option[Long]) = x match {
		case Some(i) => MNum(i)
		case _ => MNothing
	}

	implicit def jlong2mval(x: java.lang.Long) = MJNum(x)
	implicit def optjlong2mval(x: Option[java.lang.Long]) = x match {
		case Some(i) => MJNum(i)
		case _ => MNothing
	}

	implicit def bigint2mval(x: BigInt) = MNum(x.longValue)
	implicit def optbigint2mval(x: Option[BigInt]) = x match {
		case Some(i) => MNum(i.longValue)
		case _ => MNothing
	}

	implicit def double2mval(x: Double) = MDecimal(x)
	implicit def optdouble2mval(x: Option[Double]) = x match {
		case Some(i) => MDecimal(i)
		case _ => MNothing
	}

	implicit def float2mval(x: Float) = MDecimal(x.toDouble)
	implicit def optfloat2mval(x: Option[Float]) = x match {
		case Some(i) => MDecimal(i.toDouble)
		case _ => MNothing
	}

	implicit def bigdecimal2mval(x: BigDecimal) = MDecimal(x.doubleValue)
	implicit def optbigdecimal2mval(x: Option[BigDecimal]) = x match {
		case Some(i) => MDecimal(i.doubleValue)
		case _ => MNothing
	}

	implicit def bool2mval(x: Boolean) = MBool(x)
	implicit def optbool2mval(x: Option[Boolean]) = x match {
		case Some(i) => MBool(i)
		case _ => MNothing
	}
	
	implicit def regex2mval(x: Regex) = MRegex(x)
	implicit def optregex2mval(x: Option[Regex]) = x match {
		case Some(i) => MRegex(i)
		case _ => MNothing
	}

	implicit def pattern2mval(x: java.util.regex.Pattern) = MPattern(x)
	implicit def optpattern2mval(x: Option[java.util.regex.Pattern]) = x match {
		case Some(i) => MPattern(i)
		case _ => MNothing
	}

	implicit def string2mval(x: String) = MString(x)
	implicit def optstring2mval(x: Option[String]) = x match {
		case Some(i) => MString(i)
		case _ => MNothing
	}
	
	implicit def id2mid(x: ObjectId) = MId(x)
	implicit def optid2mid(x: Option[ObjectId]) = x match {
		case Some(i) => MId(i)
		case _ => MNothing
	}

	implicit def dbo2mdbo(d: DBObject) = MDbo(d)
	implicit def optdbo2mdbo(x: Option[DBObject]) = x match {
		case Some(i) => MDbo(i)
		case _ => MNothing
	}
	
	implicit def optlist2mval[T](l: Option[List[T]])(implicit m: Manifest[T]): MVal = l match {
		case Some(list) => list2mval[T](list)(m)
		case _ => MNothing
	}
	
	// TODO: un-uglify this
	implicit def list2mval[T](l: List[T])(implicit m: Manifest[T]): MVal = m.erasure match {
		case e if e.isAssignableFrom(classOf[Int]) => MArray( l.map(x => int2mval(x.asInstanceOf[Int])) )
		case e if e.isAssignableFrom(classOf[Long]) => MArray( l.map(x => long2mval(x.asInstanceOf[Long])) )
		case e if e.isAssignableFrom(classOf[Double]) => MArray( l.map(x => double2mval(x.asInstanceOf[Double])) )
		case e if e.isAssignableFrom(classOf[Float]) => MArray( l.map(x => float2mval(x.asInstanceOf[Float])) )
		case e if e.isAssignableFrom(classOf[String]) => MArray( l.map(x => string2mval(x.asInstanceOf[String])) )
		case e if e.isAssignableFrom(classOf[Regex]) => MArray( l.map(x => regex2mval(x.asInstanceOf[Regex])) )
		case e if e.isAssignableFrom(classOf[java.util.regex.Pattern]) => MArray( l.map(x => pattern2mval(x.asInstanceOf[java.util.regex.Pattern])) )
    // case e if e.isAssignableFrom(classOf[MVal]) => MArray( l.map(x => x.as[MVal]) ) // not sure why needed
    // case e if e.isAssignableFrom(classOf[UserId]) => MArray( l.map(x => MId(x.as[UserId].id)) )
    // case e if e.isAssignableFrom(classOf[EventId]) => MArray( l.map(x => MId(x.as[EventId].id)) )
    // case e if e.isAssignableFrom(classOf[MsgId]) => MArray( l.map(x => MId(x.as[MsgId].id)) )
    // case e if e.isAssignableFrom(classOf[ObjectId]) => MArray( l.map(x => MId(x.as[ObjectId])) ) // not sure why needed
    // case e if e.isAssignableFrom(classOf[MObject]) => MArray( l.map(x => x.as[MVal]) ) // not sure why needed
		// case e if e.getInterfaces.exists(c => c == classOf[DBObject]) => MArray( l.map(x => MDbo(x.as[DBObject])) )
		case other => try {
			MArray( l.map(x => MDbo(x.asInstanceOf[DBObject])) )
		} catch {
			case t: Throwable => throw new IllegalArgumentException("don't know how to convert "+other+" to MVal: "+t.getMessage)
		}
	}
	
	// supports mapping keys to values
	class KeyWrapper(k: MKey) {
		def ->(v: MVal): MObject = MObject( List( MField(k, v) ) )
		def ->(v: Option[MVal]): MObject = if (v.isDefined) ->(v.get) else ->(MNothing)
	}
	implicit def k2kw(k: MKey) = new KeyWrapper(k)
  implicit def s2kw(s: String) = new KeyWrapper(MKey(s))
	
	// support appending fields together
	class MObjectWrapper(left: MObject) {
		def ~(right: MObject): MObject = {
			val rightNames: List[MKey] = right.obj.map(f => f.name)
			val newLeft: List[MField] = left.obj.filter(f => !rightNames.contains(f.name))
			MObject(newLeft ::: right.obj)
		}
	}
	implicit def mo2mow(o: MObject) = new MObjectWrapper(o)
	
	implicit def strtup2dbowrapper(t: (String, Any)): DBOWrapper = new DBOWrapper(strtup2dbo(t))
	implicit def strtup2dbo(t: (String, Any)): DBObject = new DBO(t._1, t._2)
	
	implicit def dbo2dbowrapper(dbo: DBObject): DBOWrapper = new DBOWrapper(dbo)
	implicit def anyref2dbowrapper(a: Any): DBOWrapper = new DBOWrapper(a.toDBO)
	implicit def dbowrapper2dbo(dbow: DBOWrapper) = dbow.dbo

	case class DBOWrapper(dbo: DBObject) {

		def /(key: MKey): Option[AnyRef] = /(key.value)
    def /(key: String): Option[AnyRef] = if (dbo.containsField(key) && dbo.get(key) != null) Some(dbo.get(key)) else None

		def ?(key: MKey): Boolean = ?(key.value)
    def ?(key: String): Boolean = dbo.containsField(key)

		def <<(other: DBObject): DBObject = {dbo.putAll(other); dbo}
		def <<(other: MObject): DBObject = <<(other.render.asInstanceOf[DBObject])
    def <<(other: (String, MVal)): DBObject = {dbo.put(other._1, other._2.render); dbo}
		def <<(other: Option[DBObject]): DBObject = {
			if (other.isDefined) dbo.putAll(other.get)
			dbo
		}

		def >>(key: MKey): Option[AnyRef] = >>(key.value)
		def >>(key: String): Option[AnyRef] = {
			val v = dbo.removeField(key)
			if (v != null) Some(v) else None
		}
	}

	implicit def option2dbooptionwrapper(opt: Option[Any]): DBOOptionWrapper = new DBOOptionWrapper(opt.map{x => new DBOWrapper(x.toDBO)})
	implicit def dbooptionwrapper2dbo(dbow: DBOOptionWrapper): DBObject = dbow.dbo.getOrElse(new DBOWrapper(new DBO)).dbo
	
	case class DBOOptionWrapper(dbo: Option[DBOWrapper]) {
	  
		def /(key: MKey): Option[AnyRef] = if (dbo.isDefined) dbo.get / (key) else None
    def /(key: String): Option[AnyRef] = if (dbo.isDefined) dbo.get / (key) else None

		def ?(key: MKey): Boolean = if (dbo.isDefined) dbo.get.?(key) else false
		def ?(key: String): Boolean = if (dbo.isDefined) dbo.get.?(key) else false

		def <<(other: DBObject): DBObject = if (dbo.isDefined) dbo.get.<<(other) else other
		def <<(other: MObject): DBObject = if (dbo.isDefined) dbo.get.<<(other) else other.render.asInstanceOf[DBObject]
		def <<(other: (String, MVal)): DBObject = if (dbo.isDefined) dbo.get.<<(other) else other

		def >>(key: MKey): Option[AnyRef] = if (dbo.isDefined) dbo.get >> key else None
		def >>(key: String): Option[AnyRef] = if (dbo.isDefined) dbo.get >> key else None
	}
	
	implicit def anyref2mongonumwrapper(a: Any) = new MongoNumWrapper(a)
	class MongoNumWrapper(a: Any) {
		private val num = a.toString.toDouble
		def jNum2Int = num.toInt
		def jNum2Long = num.toLong
		def jNum2Double = num.toDouble
		def jNum2Float = num.toFloat
	}
	
	implicit def anyopt2oidwrapper(opt: Option[Any]) = new OIDOptionWrapper(opt)
	class OIDOptionWrapper(opt: Option[Any]) {
		val toOID = opt match {
			case Some(a) => new OIDWrapper(a).toOID
			case _ => throw new RuntimeException()
		}
	}
		
	implicit def anyref2oidwrapper(a: Any) = new OIDWrapper(a)
	class OIDWrapper(a: Any) {
		val toOID = ObjectId.massageToObjectId(a)
	}

	implicit def any2dboconverter(a: Any) = new DBOConverter(a)
	class DBOConverter(a: Any) {
		val toDBO = try { a.asInstanceOf[DBObject] }
		 						catch { case t: Throwable => new DBO }
	}
}
