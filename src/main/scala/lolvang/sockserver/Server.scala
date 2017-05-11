package lolvang.sockserver

/**
  * Created by olvang on 2017-05-08.
  */

import java.net.ServerSocket
import java.net.Socket
import java.io.IOException
import java.util.concurrent.atomic.AtomicInteger

import lolvang.sockserver.util.TimeAccumulator

import scala.util.Try


class Server(val port: Int, storage:Storage, max_key:Int, max_data:Int) extends Runnable {
  var stopped = false
  var serverSocket:ServerSocket = new ServerSocket(port)


  // timers called from worker will complely break for conccurent connections
  // but for timing sequences of comands from a single connection they work fine
  // should be removed or rewritten for a real implemntation
  val thread_spawn_timer = new TimeAccumulator
  val cmd_handle_timer = new TimeAccumulator
  val data_handle_timer = new TimeAccumulator
  val exec_timer = new TimeAccumulator
  val sock_read_timer = new TimeAccumulator

  val nr_connections:AtomicInteger = new AtomicInteger(0)


  override def run():Unit = {
    println("Started Server")
    while ( !stopped ) {
      try {
        val clientSocket = serverSocket.accept
        thread_spawn_timer.start()
        if(nr_connections.incrementAndGet()<=5){
          //creates a new theread for each connection,
          //depending on the usepatterns we might want to replace this with a thread pool
          new Thread(new Worker(clientSocket, this, storage,max_key,max_data)).start()
        } else{
          nr_connections.decrementAndGet()
          clientSocket.close()
        }
      } catch {
        case e: IOException =>
          if (stopped) {
            println("Server Stopped.")
            return
          }
          println("Error accepting client connection")
          e.printStackTrace()
      }
      thread_spawn_timer.stop()
    }
    println("Server Stopped.")
  }


  def stop():Try[Unit] = {
    stopped = true
    Try(serverSocket.close())
  }

}
