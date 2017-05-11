package lolvang.sockserver

import java.io.{File, FileInputStream, FileOutputStream}
import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}

import lolvang.sockserver.util.TimeAccumulator

import collection.JavaConverters._
import scala.util.Try

/**
  * Class for loading and storing values. The values can be persisted to file by calling store()
  * but they are not automatically persisted at insertion. On startup the Storage class tries to
  * load previously persisted data from the filename given using the load() method.
  * Both of these methods are not error checked as we have no code to recover from failure
  * so it is better to let the program crash to avoid further harm.
  *
  * How this would change in a production setup depends on the intended use case.
  * If data integrity is important we would replace the concurrent map with a database or a memory
  * mapped version of ConcurrentMap like ChronicleMap, but this would force us to lift the
  * strict storage space constraints.
  *
  * If the data was not critical but we still want to be able to dump the data, I would suggest
  * adding client commands to persist and persist to a rolling file schema so that we do not
  * lose data if persistence fails. And add error handling to the store method.
  */
class Storage(filename:String, max_key_size:Int, max_value_size:Int, max_file_size:Int) {
  new File(filename).createNewFile() //create db file if it doesnt exist
  val timer = new TimeAccumulator

  var map:ConcurrentMap[ String, Array[Byte] ] = new ConcurrentHashMap[String,Array[Byte]]()
  var current_size:Int = load()

  def to_str(in:Array[Byte]):String = in.map(_.toChar).mkString("")

  def set(key:String, value:Array[Byte]):String ={
    timer.start()
    var ret = "ok"
    val new_size = current_size + 8 + key.length + value.length
    if(key.length > max_key_size){
      ret = "key_to_long"
    } else if(value.length > max_value_size) {
      ret = "value_to_large"
    } else if(new_size > max_file_size){
      ret = "data_wont_fit_in_storage"
    } else {
      val t = Try({map.put(key,value)})
      ret = if( t.isSuccess){
        current_size = new_size
        "ok"
      } else{
        "storage_error"
      }
    }
    timer.stop()
    ret
  }

  def get(key:String):Option[Array[Byte]] ={
    timer.start()
    val res = Try(Option(map.get(key)))
    timer.stop()
    res.getOrElse(None)
  }

  def del(key:String):String  ={
    timer.start()
    val res = Try({
      val value = map.remove( key )
      current_size -= ( 8 + key.length + value.length )
    })
    assert(current_size>=0)
    timer.stop()
    if(res.isSuccess) "ok" else "storage_error"
  }

  def num_keys:Long = {
    timer.start()
    val r = map.size()
    timer.stop()
    r
  }

  //somewhat unclear is this should give the size of the storage on disk that might not even
  // contain any data yet or the size that it will be id all data in the map is persisted
  // we choose the later.
  def db_size:Long ={
    timer.start()
    val r = Try(new File(filename).length())
    timer.stop()
    current_size
  }

  def store():Unit ={
    val file = new File(filename)
    val fout = new FileOutputStream(file,false)
    val data = map.asScala.map({t=>
      val k = t._1
      val v = t._2
      val buf = ByteBuffer.allocate(8 + k.length + v.length)
        .putInt(k.length)
        .putInt(v.length)
        .put(k.getBytes)
        .put(v)
      buf
    })
    val ret = data.map({b:ByteBuffer=>b.array().length}).sum
    println("Storing to file:")
    println("total size: "+ret)
    println("nr elements " + data.size)
    data.foreach({ b=>
      val arr = b.array()
      fout.write(arr)
    })
    fout.flush()
    fout.close()
    println("file size: %s".format(file.length()))
  }

  def load():Int ={
    map = new ConcurrentHashMap[String,Array[Byte]]()
    val file = new File(filename)
    println("loading from file: ")
    val fin  = new FileInputStream(file)
    val size = file.length().toInt
    val arr = new Array[Byte](size)
    fin.read(arr)
    val buf = ByteBuffer.wrap(arr)
    println("load buffer size: "+ size)
    while(buf.position()  < size){
      val key_size = buf.getInt
      val val_size = buf.getInt
      val key = new Array[Byte](key_size)
      val value = new Array[Byte](val_size)
      buf.get(key)
      buf.get(value)
      map.put(to_str(key),value)
    }
    println("Nr of elements loaded: " +map.keySet().size())
    fin.close()
    size
  }

}
