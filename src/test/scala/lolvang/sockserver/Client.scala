package lolvang.sockserver

import java.io.{InputStream, OutputStream}
import java.net.Socket
import scala.collection.mutable.ArrayBuffer
import scala.util.{Try}

/**
  * Basic client class for tests
  * @param address to connect to
  * @param port to connect to
  */
class Client(address:String, port:Int) {

  val socket:Socket = new Socket(address,port)
  val in:InputStream = socket.getInputStream
  val out:OutputStream = socket.getOutputStream

  def close():Unit ={
    Try(socket.close())
  }

  def get(key:String):Option[Array[Byte]] ={
    out.write("get\t%s\t0\n".format(key).getBytes)
    val (resp,data) = read_response()
    if(resp == "ok")  Some(data)  else None
  }

  def set(key:String, value:Array[Byte]):Boolean ={
    out.write("set\t%s\t%s\n".format(key,value.length).getBytes)
    out.write(value)
    val (resp,data) = read_response()
    resp == "ok"
  }

  def delete(key:String):Boolean ={
    out.write("delete\t%s\t0\n".format(key).getBytes)
    val (resp,data) = read_response()
    resp == "ok"
  }

  def num_connections():Option[Long] ={
    out.write("stats\tnum_connections\t0\n".getBytes)
    val (resp,data) = read_response()
    if(resp=="ok"){
      Some(data.map(_.toChar).mkString.toLong)
    } else {
      None
    }
  }

  def num_keys():Option[Long] ={
    out.write("stats\tnum_keys\t0\n".getBytes)
    val (resp,data) = read_response()
    if(resp=="ok"){
      Some(data.map(_.toChar).mkString.toLong)
    } else {
      None
    }
  }

  def db_size():Option[Long] ={
    out.write("stats\tdb_size\t0\n".getBytes)
    val (resp,data) = read_response()
    if(resp=="ok"){
      Some(data.map(_.toChar).mkString.toLong)
    } else {
      None
    }
  }


  def read_response():(String,Array[Byte]) ={
    val bytes:Array[Byte] = new Array[Byte](8192)
    def read_data(nr:Int):ArrayBuffer[Byte] = {
      var dbuf = ArrayBuffer[Byte]()
      var read = 0
      while(nr > dbuf.length && read != -1 && !socket.isClosed){
        read = in.read(bytes)
        dbuf ++= bytes.slice(0,read)
      }
      dbuf
    }
    var buf = ArrayBuffer[Byte]()
    var read = 0
    var done = false
    do{
      read = in.read(bytes)
      buf ++= bytes.slice(0,read)
      val tmp = mk_string(buf)
      if(buf.contains('\n'.toByte)){
        done = true
      }
    }while(!done && read != -1 && !socket.isClosed)
    val pos = buf.indexOf('\n'.toByte)
    val (head,data) = (buf.take(pos), buf.drop(pos+1))
    val cmd = head.map(_.toChar).mkString.split("\t")
    if(cmd(1).toInt - data.length > 0){
      data ++= read_data(cmd(1).toInt - data.length)
    }
    (cmd(0),data.toArray)
  }

  def mk_string(a:Array[Byte]):String = a.map(_.toChar).mkString
  def mk_string(a:ArrayBuffer[Byte]):String = a.map(_.toChar).mkString

}
