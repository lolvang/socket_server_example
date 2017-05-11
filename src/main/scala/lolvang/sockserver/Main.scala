package lolvang.sockserver

import scala.util.Try

object Main {
  def main(args: Array[String]): Unit = {
    val storage = new Storage("storage.db",100,1000000,25000000)
    val server = new Server(3434, storage,100,1000000)
    Runtime.getRuntime.addShutdownHook(new Thread(){override def run(): Unit ={
      println("Stopping Server")
      storage.store()
      println("Statistics:")
      println("storage.timer "              + storage.timer.get + " ms")
      println("server.thread_spawn_timer "  + server.thread_spawn_timer.get + " ms")
      println("server.cmd_time "            + server.cmd_time.get + " ms")
      println("server.data_time "           + server.data_time.get + " ms")
      println("server.exec_time "           + server.exec_time.get + " ms")
    }})
    new Thread(server).start()
    while(!server.stopped){
      Try(Thread.sleep(20 * 1000))
    }
  }
}
