package com.hotpotato.sandm

import _root_.scala.collection.jcl

import _root_.com.mongodb._
import _root_.com.mongodb
import mongodb.{BasicDBObject => DBO, MapReduceOutput => MongoMapReduceOutput}
import _root_.com.mongodb.util.JSON

import _root_.com.hotpotato.util.Timer
import _root_.com.hotpotato.util.Utils._

import MongoAST._
import MongoDSL._

abstract class Collection(val coll: DBCollection) {
	
	// reserved_keys 
	val _id = MKey("_id")
	val _ns = MKey("_ns")
	val $ref = MKey("$ref")
	
	// query_keys 
	val $min = MKey("$min")
	val $max = MKey("$max")
	val $query = MKey("$query")
	val $lt = MKey("$lt")
	val $lte = MKey("$lte")
	val $gt = MKey("$gt")
	val $gte = MKey("$gte")
	val $ne = MKey("$ne")
	val $in = MKey("$in")
	val $nin = MKey("$nin")
	val $mod = MKey("$mod")
	val $all = MKey("$all")
	val $size = MKey("$size")
  val $exists = MKey("$exists") 
	val $elemMatch = MKey("$elemMatch")
	val $near = MKey("$near")
	
	// update_keys 
	val $set = MKey("$set")
	val $unset = MKey("$unset")
	val $inc = MKey("$inc")
	val $push = MKey("$push")
	val $pushAll = MKey("$pushAll")
	val $pull = MKey("$pull")
	val $pullAll = MKey("$pullAll")
	val $addToSet = MKey("$addToSet")
	val $each = MKey("$each")
		
	protected val Log = _root_.java.util.logging.Logger.getLogger(coll.getName)

  protected val indexes = scala.collection.mutable.Map[String, () => Unit]()
  
  protected def addSimpleIndex(key: MKey): Unit = {
		val k = (key -> 1).render.asInstanceOf[DBObject]
		val name = DBCollection.genIndexName(k)
		if (indexes isDefinedAt name) throw new RuntimeException("addSimpleIndex: "+name+" is already defined!")
    indexes.update(name, () => { coll.ensureIndex(k, name, false) })
		Log.fine("addSimpleIndex: "+name+" - "+k)		
  }
	
	def ensureSimpleIndex(key: MKey)(implicit logPrefix: String): Unit = {
		val k = (key -> 1).render.asInstanceOf[DBObject]
		val name = DBCollection.genIndexName(k)
		Log.fine("ensureSimpleIndex: using "+name)
		indexes.get(name) match {
		  case Some(f) => f()
		  case _ => Log.warning("ensureSimpleIndex: "+name+" NOT FOUND")
		}
	}
	
	def ensureSimpleIndex(keys: List[MKey])(implicit logPrefix: String): Unit = 
	  keys foreach ensureSimpleIndex

  protected def addUniqueSimpleIndex(key: MKey)(implicit logPrefix: String): Unit = {
		val k = (key -> 1).render.asInstanceOf[DBObject]
		val name = DBCollection.genIndexName(k)
		if (indexes isDefinedAt name) throw new RuntimeException("addUniqueSimpleIndex: "+name+" is already defined!")
    indexes.update(name, () => { coll.ensureIndex(k, name, true) })
		Log.fine("addUniqueSimpleIndex: "+name+" - "+k)
  }
	
	def ensureUniqueSimpleIndex(key: MKey)(implicit logPrefix: String): Unit = {
		val k = (key -> 1).render.asInstanceOf[DBObject]
		val name = DBCollection.genIndexName(k)
		Log.fine("ensureUniqueSimpleIndex: using "+name)
		indexes.get(name) match {
		  case Some(f) => f()
		  case _ => Log.warning("ensureSimpleIndex: "+name+" NOT FOUND")
		}
	}
	
	def ensureUniqueSimpleIndex(keys: List[MKey])(implicit logPrefix: String): Unit = 
	  keys foreach ensureUniqueSimpleIndex
	
	protected def addCompoundIndex(keys: MObject): Unit = {
		val k = keys.render.asInstanceOf[DBObject]
		val name = DBCollection.genIndexName(k)
		if (indexes isDefinedAt name) throw new RuntimeException("addCompoundIndex: "+name+" is already defined!")
    indexes.update(name, () => { coll.ensureIndex(k, name, false) })
		Log.fine("addCompoundIndex: "+name+" - "+k)
	}
	
	def ensureCompoundIndex(keys: MObject)(implicit logPrefix: String): Unit = {
		val k = keys.render.asInstanceOf[DBObject]
		val name = DBCollection.genIndexName(k)
		indexes.get(name) match {
		  case Some(f) => f()
		  case _ => Log.warning("ensureCompoundIndex: "+name+" NOT FOUND")
		}
	}
	
	protected def addUniqueCompoundIndex(keys: MObject): Unit = {
	  val k = keys.render.asInstanceOf[DBObject]
		val name = DBCollection.genIndexName(k)
		if (indexes isDefinedAt name) throw new RuntimeException("addUniqueCompoundIndex: "+name+" is already defined!")
    indexes.update(name, () => { coll.ensureIndex(k, name, true) })
		Log.fine("addUniqueCompoundIndex: "+name+" - "+k)
	}

	def ensureUniqueCompoundIndex(keys: MObject)(implicit logPrefix: String) = {
	  val k = keys.render.asInstanceOf[DBObject]
		val name = DBCollection.genIndexName(k)
		indexes.get(name) match {
		  case Some(f) => f()
		  case _ => Log.warning("ensureUniqueCompoundIndex: "+name+" NOT FOUND")
		}
	}
	
	def count(query: MObject)(implicit logPrefix: String): Long = {
		val t = Timer()
		val q = query.render.asInstanceOf[DBObject]
		val res = coll.getCount(preProcessQuery(q))
		Log.fine("count: "+t.formatWithElapsed(q + " => "+res))
		res
	}
	
	def exists(query: MObject)(implicit logPrefix: String): Boolean = {
		val t = Timer()
		val res = count(query) > 0
		Log.fine(t.formatWithElapsed("exists: "+query.render + " => " + res))
		res
	}
	
	def findOne(id: MVal)(implicit logPrefix: String): Option[DBObject] = {
		val t = Timer()
		val q = id.render
		val res = coll.findOne(q) match {
			case null => None
			case dbo => Some(postProcess(dbo))
		}
		Log.fine(t.formatWithElapsed("findOne: "+q+ " => "+res))
		res
	}

	def findOne(id: MVal, fields: List[MKey])(implicit logPrefix: String): Option[DBObject] = {
		val t = Timer()
		val q = id.render
		val f = fields.map(k => (k -> 1)).foldLeft(new DBO) { 
			(dbo, f) => dbo.putAll(f.render.asInstanceOf[DBObject]); dbo 
		}
		val res = coll.findOne(q, f) match {
			case null => None
			case dbo => Some(postProcess(dbo))
		}
		Log.fine(t.formatWithElapsed("findOne: "+q + " ("+f+") => "+res))
		res
	}

	def findOne(query: MObject)(implicit logPrefix: String): Option[DBObject] = {
		val t = Timer()
		val q = preProcessQuery(query.render.asInstanceOf[DBObject])
		val res = coll.findOne(q) match {
			case null => None
			case dbo => Some(postProcess(dbo))
		}
		Log.fine(t.formatWithElapsed("findOne: " +q+ " => "+res))
		res
	}

	def findOne(query: MObject, fields: List[MKey])(implicit logPrefix: String): Option[DBObject] = {
		val t = Timer()
		val q = preProcessQuery(query.render.asInstanceOf[DBObject])
		val f = fields.map(k => (k -> 1)).foldLeft(new DBO) { 
			(dbo, f) => dbo.putAll(f.render.asInstanceOf[DBObject]); dbo 
		}
		val res = coll.findOne(q, f) match {
			case null => None
			case dbo => Some(postProcess(dbo))
		}
		Log.fine(t.formatWithElapsed("findOne: "+q + " ("+f+") => "+res))
		res
	}
	
	def find(query: MObject)(implicit logPrefix: String): Cursor = {
		val t = Timer()
		val q = preProcessQuery(query.render.asInstanceOf[DBObject])
		val c = Cursor(coll.find(q))
		
		Log.fine {
			val e = c.explain
			val cname: String = box(e.get("cursor")).map(_.toString).getOrElse("BasicCursor")
			val nScanned: Double = box(e.get("nScanned")).map(_.toString.toDouble).getOrElse(0d)
			if (cname.startsWith("BasicCursor") && nScanned > 0d) {
				val qString = JSON.serialize(q)
				val exString = JSON.serialize(e)
				t.formatWithElapsed("find: query:\n------------\n"+qString+"\n------------\nexplain:\n------------\n"+exString+"\n------------")
			} else {
				val qString = JSON.serialize(q)
				t.formatWithElapsed("find: query: "+qString)
			}
		}

		c
	}
	
	def find(query: MObject, fields: List[MKey])(implicit logPrefix: String): Cursor = {
		val t = Timer()
		val q = preProcessQuery(query.render.asInstanceOf[DBObject])
		val f = fields.map(k => (k -> 1)).foldLeft(new DBO) { 
			(dbo, f) => dbo.putAll(f.render.asInstanceOf[DBObject]); dbo 
		}
		val c = Cursor(coll.find(q, f))

		Log.fine {
			val e = c.explain
			val cname: String = box(e.get("cursor")).map(_.toString).getOrElse("BasicCursor")
			val nScanned: Double = box(e.get("nScanned")).map(_.toString.toDouble).getOrElse(0d)
			if (cname.startsWith("BasicCursor") && nScanned > 0d) {
				val qString = JSON.serialize(q)
				val fString = JSON.serialize(f)
				val exString = JSON.serialize(e)
				t.formatWithElapsed("find: query:\n------------\n"+qString+"\n------------\nfields:\n------------\n"+fString+"\n------------\nexplain:\n------------\n"+exString+"\n------------")
			} else {
				val qString = JSON.serialize(q)
				val fString = JSON.serialize(f)
				t.formatWithElapsed("find: query: "+qString+", fields: "+fString)
			}
		}

		c
	}
	
	def findAll: Cursor = Cursor(coll.find)
	
	def findAll(fields: List[MKey])(implicit logPrefix: String): Cursor = {
		val t = Timer()
		val f = fields.map(k => (k -> 1)).foldLeft(new DBO) { 
			(dbo, f) => dbo.putAll(f.render.asInstanceOf[DBObject]); dbo 
		}
		val c = Cursor(coll.find(new DBO, f))
		
		Log.fine {
			val e = c.explain
			val cname: String = box(e.get("cursor")).map(_.toString).getOrElse("BasicCursor")
			val nScanned: Double = box(e.get("nScanned")).map(_.toString.toDouble).getOrElse(0d)
			if (cname.startsWith("BasicCursor") && nScanned > 0d) {
				val fString = JSON.serialize(f)
				val exString = JSON.serialize(e)
				t.formatWithElapsed("find: query: all\n------------\nfields:\n------------\n"+fString+"\n------------\nexplain:\n------------\n"+exString+"\n------------")
			} else {
				val fString = JSON.serialize(f)
				t.formatWithElapsed("find: query: all, fields: "+fString)
			}
		}

		c
  }
	
	def distinct[T](key: MKey, query: MObject): List[T] = {
	  val jlist: java.util.List[_] = coll.distinct(key.value, query.render.asInstanceOf[DBObject])
		jcl.Conversions.convertList(jlist).foldLeft(List[T]()) { (list, v) => v.asInstanceOf[T] :: list }.reverse
	}

	def distinct[T1, T2](key: MKey)(conv: (T1) => T2): List[T2] = {
	  val jlist: java.util.List[_] = coll.distinct(key.value)
		jcl.Conversions.convertList(jlist).foldLeft(List[T2]()) { (list, v) => conv(v.asInstanceOf[T1]) :: list }.reverse
	}
	
	def preProcessQuery(query: DBObject): DBObject = query
	
	def postProcess(dbo: DBObject): DBObject = { dbo.removeField("_ns"); dbo }

	sealed case class Cursor(private val c: DBCursor) {
		def skip(i: Int) = new Cursor(c.skip(i))
		def limit(i: Int) = new Cursor(c.limit(i))
		def sort(keys: MObject) = new Cursor(c.sort(keys.render.asInstanceOf[DBObject]))
		def count = c.count
		def toList = { import jcl.Conversions._
			c.toArray.foldLeft(List[DBObject]()) { (list, dbo) => postProcess(dbo) :: list }.reverse
		}
		def foreach(func: (DBObject) => Unit): Unit = {
			while(c.hasNext) {
				val item = c.next
				func(item)
			}
		}
		def map[T](func: (DBObject) => T): List[T] = {
		  val lb = new scala.collection.mutable.ListBuffer[T]
			while(c.hasNext) {
				val item = c.next
				val newItem = func(item)
				lb += newItem
			}
			lb.toList
		}
		def explain: DBObject = c.explain
		def hint(keys: MObject): Cursor = Cursor(c.hint(keys.render.asInstanceOf[DBObject]))
	}
	
	def save(doc: MObject)(implicit logPrefix: String): DBObject = {
		val t = Timer()
		val obj = doc.render.asInstanceOf[DBObject]		
		coll.save(obj)
		val res = postProcess(obj)
		Log.fine(t.formatWithElapsed("saved: "+res))
		res
	}
	
	def insert(doc: MObject)(implicit logPrefix: String): DBObject = {
		val t = Timer()
		val obj = doc.render.asInstanceOf[DBObject]
		coll.insert(obj)
		val res = postProcess(obj)
		Log.fine(t.formatWithElapsed("inserted: "+res))
		res
	}
	
	def insert(docs: List[MObject])(implicit logPrefix: String): List[DBObject] = {
		val t = Timer()
		val arr = docs.foldLeft(new jcl.ArrayList[DBObject]) { (al, doc) => al += doc.render.asInstanceOf[DBObject]; al }.underlying
		coll.insert(arr)
		import jcl.Conversions._
		val res = arr.foldLeft(List[DBObject]()) { (l, dbo) => postProcess(dbo) :: l }
		Log.fine(t.formatWithElapsed("inserted: "+res))
		res
	}
	
	def update(query: MObject, update: MObject)(implicit logPrefix: String): Unit = {
		val t = Timer()
		val q = query.render.asInstanceOf[DBObject]
		val up = update.render.asInstanceOf[DBObject]
		Log.fine(t.formatWithElapsed("\n\tquery: "+q+"\n\tupdate: "+up))
		coll.update(q, up)
	}
		
	def upsert(query: MObject, update: MObject)(implicit logPrefix: String): Unit = {
		val t = Timer()
		val q = query.render.asInstanceOf[DBObject]
		val up = update.render.asInstanceOf[DBObject]
		Log.fine(t.formatWithElapsed("\n\tquery: "+q+"\n\tupdate: "+up))
		coll.update(q, up, true, false)
	}
	
	def updateMulti(query: MObject, update: MObject)(implicit logPrefix: String): Unit = {
		val t = Timer()
		val q = query.render.asInstanceOf[DBObject]
		val up = update.render.asInstanceOf[DBObject]
		Log.fine(t.formatWithElapsed("\n\tquery: "+q+"\n\tupdate: "+up))
		coll.updateMulti(q, up)
	}

	def upsertMulti(query: MObject, update: MObject)(implicit logPrefix: String): Unit = {
		val t = Timer()
		val q = query.render.asInstanceOf[DBObject]
		val up = update.render.asInstanceOf[DBObject]
		Log.fine(t.formatWithElapsed("\n\tquery: "+q+"\n\tupdate: "+up))
		coll.update(q, up, true, true)
	}
	
	def remove(query: MObject)(implicit logPrefix: String): Unit = {
		val t = Timer()
		val q = query.render.asInstanceOf[DBObject]
		Log.fine(t.formatWithElapsed("remove: " +q))
		coll.remove(q)
	}
	
	def removeAll: Unit = {
		coll.remove(new DBO)
	}
	
	def mapReduce(map: String, reduce: String, query: MObject)(implicit logPrefix: String): MapReduceOutput = mapReduce(map, reduce, null, query)
	
	def mapReduce(map: String, reduce: String, outputColl: String, query: MObject)(implicit logPrefix: String): MapReduceOutput = {
		val t = Timer()
		val q = preProcessQuery(query.render.asInstanceOf[DBObject])
		val out = MapReduceOutput(coll.mapReduce(map, reduce, outputColl, q))
		Log.fine(t.formatWithElapsed("mapReduce: query: "+q))
		out
	}
	
	sealed case class MapReduceOutput(private val mro: MongoMapReduceOutput) {
		def drop = mro.drop
		def results = Cursor(mro.results)
	}
}
