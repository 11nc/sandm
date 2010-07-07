package sandm

import _root_.com.mongodb._
import _root_.com.mongodb.{BasicDBObject => DBO}

import _root_.java.util.regex.Pattern

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
	
	implicit def listint2mval(l: List[Int]): MVal = MArray( l.map(int2mval) )
	implicit def optlistint2mval(l: Option[List[Int]]): MVal = l match {
		case Some(list) => listint2mval(list)
		case _ => MNothing
	}
	implicit def listlong2mval(l: List[Long]): MVal = MArray( l.map(long2mval) )
	implicit def optlistlong2mval(l: Option[List[Long]]): MVal = l match {
		case Some(list) => listlong2mval(list)
		case _ => MNothing
	}
	implicit def listdoub2mval(l: List[Double]): MVal = MArray( l.map(double2mval) )
	implicit def optlistdoub2mval(l: Option[List[Double]]): MVal = l match {
		case Some(list) => listdoub2mval(list)
		case _ => MNothing
	}
	implicit def listfl2mval(l: List[Float]): MVal = MArray( l.map(float2mval) )
	implicit def optlistfl2mval(l: Option[List[Float]]): MVal = l match {
		case Some(list) => listfl2mval(list)
		case _ => MNothing
	}
	implicit def liststr2mval(l: List[String]): MVal = MArray( l.map(string2mval) )
	implicit def optliststr2mval(l: Option[List[String]]): MVal = l match {
		case Some(list) => liststr2mval(list)
		case _ => MNothing
	}
	implicit def listregex2mval(l: List[Regex]): MVal = MArray( l.map(regex2mval) )
	implicit def optlistregex2mval(l: Option[List[Regex]]): MVal = l match {
		case Some(list) => listregex2mval(list)
		case _ => MNothing
	}
	implicit def listpat2mval(l: List[Pattern]): MVal = MArray( l.map(pattern2mval) )
	implicit def optlistpat2mval(l: Option[List[Pattern]]): MVal = l match {
		case Some(list) => listpat2mval(list)
		case _ => MNothing
	}
	implicit def listoid2mval(l: List[ObjectId]): MVal = MArray( l.map(x => MId(x)) )
	implicit def optlistoid2mval(l: Option[List[ObjectId]]): MVal = l match {
		case Some(list) => listoid2mval(list)
		case _ => MNothing
	}
	implicit def listmval2mval(l: List[MVal]): MVal = MArray( l )
	implicit def optlistmval2mval(l: Option[List[MVal]]): MVal = l match {
		case Some(list) => listmval2mval(list)
		case _ => MNothing
	}
	implicit def listdbo2mval(l: List[DBObject]): MVal = MArray( l.map(x => MDbo(x)) )
	implicit def optlistdbo2mval(l: Option[List[DBObject]]): MVal = l match {
		case Some(list) => listdbo2mval(list)
		case _ => MNothing
	}
	
	// // TODO: un-uglify this - there must be a better way
	// implicit def optlist2mval[T](l: Option[List[T]])(implicit m: Manifest[T]): MVal = l match {
	// 	case Some(list) => list2mval[T](list)(m)
	// 	case _ => MNothing
	// }
	// implicit def list2mval[T](l: List[T])(implicit m: Manifest[T]): MVal = m.erasure match {
	// 	case e if e.isAssignableFrom(classOf[Int]) => MArray( l.map(x => int2mval(x.asInstanceOf[Int])) )
	// 	case e if e.isAssignableFrom(classOf[Long]) => MArray( l.map(x => long2mval(x.asInstanceOf[Long])) )
	// 	case e if e.isAssignableFrom(classOf[Double]) => MArray( l.map(x => double2mval(x.asInstanceOf[Double])) )
	// 	case e if e.isAssignableFrom(classOf[Float]) => MArray( l.map(x => float2mval(x.asInstanceOf[Float])) )
	// 	case e if e.isAssignableFrom(classOf[String]) => MArray( l.map(x => string2mval(x.asInstanceOf[String])) )
	// 	case e if e.isAssignableFrom(classOf[Regex]) => MArray( l.map(x => regex2mval(x.asInstanceOf[Regex])) )
	// 	case e if e.isAssignableFrom(classOf[java.util.regex.Pattern]) => MArray( l.map(x => pattern2mval(x.asInstanceOf[java.util.regex.Pattern])) )
	//     case e if e.isAssignableFrom(classOf[MVal]) => MArray( l.map(x => x.asInstanceOf[MVal]) )
	//     case e if e.isAssignableFrom(classOf[ObjectId]) => MArray( l.map(x => MId(x.asInstanceOf[ObjectId])) )
	//     case e if e.isAssignableFrom(classOf[MObject]) => MArray( l.map(x => x.asInstanceOf[MVal]) )
	// 	// case e if e.getInterfaces.exists(c => c == classOf[DBObject]) => MArray( l.map(x => MDbo(x.as[DBObject])) )
	// 	case other => try {
	// 		MArray( l.map(x => MDbo(x.asInstanceOf[DBObject])) )
	// 	} catch {
	// 		case t: Throwable => throw new IllegalArgumentException("don't know how to convert "+other+" to MVal: "+t.getMessage)
	// 	}
	// }
	
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
			val newLeft: List[MField] = left.obj.filter(f => !rightNames.contains(f.name)).filter(f => !f.isEmpty)
			MObject(newLeft ::: right.obj.filter(f => !f.isEmpty))
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
		lazy val jNum2Int = a.toString.toDouble.toInt
		lazy val jNum2JInt = a.asInstanceOf[java.lang.Integer]
		lazy val jNum2Long = a.toString.toDouble.toLong
		lazy val jNum2JLong = a.asInstanceOf[java.lang.Long]
		lazy val jNum2Double = a.toString.toDouble
		lazy val jNum2JDouble = a.asInstanceOf[java.lang.Double]
		lazy val jNum2Float = a.toString.toFloat
		lazy val jNum2JFloat = a.asInstanceOf[java.lang.Float]
	}
	
	implicit def anyopt2oidwrapper(opt: Option[Any]) = new OIDOptionWrapper(opt)
	class OIDOptionWrapper(opt: Option[Any]) {
		lazy val toOID = opt match {
			case Some(a) => new OIDWrapper(a).toOID
			case _ => throw new RuntimeException()
		}
	}
		
	implicit def anyref2oidwrapper(a: Any) = new OIDWrapper(a)
	class OIDWrapper(a: Any) {
		lazy val toOID = ObjectId.massageToObjectId(a)
	}

	implicit def any2dboconverter(a: Any) = new DBOConverter(a)
	class DBOConverter(a: Any) {
		lazy val toDBO = try { a.asInstanceOf[DBObject] } catch { case t: Throwable => new DBO }
	}
}
