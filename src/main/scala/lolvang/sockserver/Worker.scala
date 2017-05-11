package lolvang.sockserver

import java.io.{IOException, InputStream, OutputStream}
import java.net.Socket

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

class Worker(
  val socket:Socket, server:Server, storage:Storage, max_key:Int, max_data:Int
) extends Runnable {
  val in:InputStream    = socket.getInputStream
  val out:OutputStream  = socket.getOutputStream


  override def run(): Unit = {
    println("Client connected")
    try {
      val bytes:Array[Byte] = new Array[Byte](8192)
      var read = 0
      do{
        val (cmd, param, data) = read_request
        execute(cmd,param,data)
      } while(read != -1 && !socket.isClosed)
    } catch {
      case e: IOException =>
        e.printStackTrace()
    }
    server.nr_connections.decrementAndGet()
    println("Client disconnected")
  }

  def mk_string(a:Array[Byte]):String = a.map(_.toChar).mkString
  def mk_string(a:ArrayBuffer[Byte]):String = a.map(_.toChar).mkString

  def read_request:(String,String,Array[Byte])={
    server.sock_read_timer.start()
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
    val cmd = mk_string(head).split("\t")
    if(cmd.length != 3){
      // we don't know how much data is being sent so we cannot parse it
      // the client should be able to recover from this if it send
      // trivial commands until all the leftover data has been consumed
      // but it is proably easier and safer to just reconnect
      server.sock_read_timer.stop()
      return ("wrong_number_of_parameters","x",Array[Byte]())
    }
    val size = Try(cmd(2).toInt)
    if(size.isFailure){
      // as above leaves data on socket buffers
      server.sock_read_timer.stop()
      ("non_integer_size_parameter","x",Array[Byte]())
    }else if(cmd(1).length > max_key){
      // as above leaves data on socket buffers
      server.sock_read_timer.stop()
      ("key_to_large", "x", Array[Byte]())
    } else if(size.get > max_data){
      // as above leaves data on socket buffers
      server.sock_read_timer.stop()
      ("value_to_large", "x", Array[Byte]())
    } else {
      if(size.get - data.length > 0){
        data ++= read_data(size.get - data.length)
      }
      server.sock_read_timer.stop()
      (cmd(0),cmd(1),data.toArray)
    }
  }

  def reply(mes:String, data:Array[Byte]):Unit ={
    Try({
      out.write(mes.getBytes)
      out.write(data)
    })
  }

  def reply(mes:String):Unit ={
    Try({
      out.write(mes.getBytes)
    })
  }

  def execute(cmd:String,param:String,data:Array[Byte]):Unit ={
    server.exec_timer.start()
    cmd match {
      case "set" =>
        val set = storage.set(param, data)
        reply("%s\t0\n".format(set.replace("\t", " ").toLowerCase))
      case "get" =>
        val get = storage.get(param)
        if(get.isEmpty) {
          reply("key not found\t0\n")
        }else {
          val d = get.getOrElse(Array[Byte]())
          reply("ok\t%s\n".format(d.length), d)
        }
      case "delete" =>
        val del = storage.del(param)
        reply("%s\t0\n".format(del.replace("\t", " ").toLowerCase))
      case "stats" =>
        val nr = stats(param)
        if(nr != -1L){
          val snr = nr.toString
          reply("ok\t%s\n%s".format(snr.length, nr))
        } else{
          reply("unknown_param\t0\n")
        }
      case "key_to_large" =>
        reply("key_to_large\t0\n")
      case "value_to_large" =>
        reply("value_to_large\t0\n")
      case "wrong_number_of_parameters" =>
        reply("wrong_number_of_parameters\t0\n")
      case "non_integer_size_parameter" =>
        reply("non_integer_size_parameter\t0\n")
      case _ =>
        reply("unknown_command\t0\n")
    }
    server.exec_timer.stop()
  }



  def stats(cmd:String):Long ={
    cmd match{
      case "num_connections" =>
        server.nr_connections.get()
      case "num_keys" =>
        storage.num_keys
      case "db_size" =>
        storage.db_size
      case _ =>
        -1L
    }
  }
}
