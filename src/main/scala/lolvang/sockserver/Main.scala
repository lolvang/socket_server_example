package lolvang.sockserver

import scala.util.Try

object Main {
  def main(args: Array[String]): Unit = {
    val storage = new Storage("storage.db",100,1000000,25000000)
    val server = new Server(3434, storage,100,1000000)
    Runtime.getRuntime.addShutdownHook(new Thread(){override def run(): Unit ={
      System.out.println("Stopping Server")
      storage.store()
    }})
    new Thread(server).start()
    while(!server.stopped){
      Try(Thread.sleep(20 * 1000))
    }


  }
}
