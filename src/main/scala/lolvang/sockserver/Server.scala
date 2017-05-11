package lolvang.sockserver

import java.net.ServerSocket
import java.net.Socket
import java.io.IOException
import java.util.concurrent.atomic.AtomicInteger

import lolvang.sockserver.util.TimeAccumulator

import scala.util.Try

/**
  * Server instance, receives incoming connections and spawns Worker threads to handle them
  * Has a fixed max of five concurrent connections. If the server should be used in a setting
  * where many concurrent connections are expected it might be better to have a worker pool
  * that handles input on a given socket and switches to the next in queue as soon as it
  * would wait for further input from the client. This setup would be significantly more
  * complex to implement. If we instead have few but work intensive connections the current
  * setup is probably more efficient.
  */
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
          println("Refused connection, to many clients")
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
