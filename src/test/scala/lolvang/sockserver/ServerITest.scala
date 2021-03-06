package lolvang.sockserver

import java.io.File

import org.junit.experimental.categories.Category
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import scala.util.{Random, Try}

/**
  * Created by olvang on 2017-05-08.
  */
@RunWith(classOf[JUnitRunner])
@Category(Array(classOf[IntegrationTest]))
class ServerITest extends FunSuite {
  var storage: Storage = _
  var server: Server = _

  def init(): Unit = {
    new File("test.storage.db").delete()
    storage = new Storage("test.storage.db", 100, 1000000, 25000000)
    server = new Server(3434, storage,100,1000000)
    new Thread(server).start()
  }

  def stop(): Unit = {
    while (server.stop().isFailure) {}
    println("Statistics:")
    println("storage.timer "              + storage.timer.get + " ms")
    println("server.thread_spawn_timer "  + server.thread_spawn_timer.get + " ms")
    println("server.cmd_time "            + server.cmd_time.get + " ms")
    println("server.data_time "           + server.data_time.get + " ms")
    println("server.exec_time "           + server.exec_time.get + " ms")
  }

  def to_str(in: Array[Byte]):String = in.map(_.toChar).mkString("")

  def rnd_str(len:Int):String = Random.alphanumeric.take(len).mkString


  test("stats") {
    init()
    val clients = Range(0, 5).map({ n => new Client("localhost", 3434) })
    val nr_con = clients.map({ c => c.num_connections() })
    val nr_keys = clients.map({ c => c.num_keys() })
    val db_size = clients.map({ c => c.db_size() })
    clients.foreach({ c => c.close() })
    nr_con.foreach({ n =>
      assert(n.isDefined)
      assert(n.get == 5)
    })
    clients.foreach(_.close())
    stop()
  }


  test("set_get_remove") {
    init()
    val elems = 10
    val key_size = 32
    val data_size = 1
    val input = Range(0, elems).map({ r => (rnd_str(key_size), rnd_str(data_size)) })
    val client = new Client("localhost", 3434)
    val s1 = client.db_size()
    val k1 = client.num_keys()
    val sets = input.map({ t => client.set(t._1, t._2.getBytes) })
    val s2 = client.db_size()
    val k2 = client.num_keys()
    storage.store()  //store and load to check persistance
    storage.load()
    val gets = input.map({ t => to_str(client.get(t._1).getOrElse(Array[Byte]())) })
    val s3 = client.db_size()
    val k3 = client.num_keys()
    val rems = input.map({ t => client.delete(t._1) })
    val s4 = client.db_size()
    val k4 = client.num_keys()

    assert(k1.get == k4.get)
    assert(k2.get == k3.get)
    assert(k1.get + elems == k2.get) //all the sets succeeded
    assert(s1.get <= s2.get) // setting elements does not decrease filesize
    assert(s2.get == s3.get) // getting elements does not change filesize
    assert(s3.get >= s4.get) // removing elemnets does not increas filesize
    assert(sets.forall({ b => b })) // all sets succeeded
    assert(input.zip(gets).forall({ t => t._1._2 == t._2 })) // set(a,b)->get(a)->b
    assert(rems.forall({ b => b })) // all deletes succeeded
    client.close()
    stop()
  }

  test("set_get_delete_large_entry") {
    init()
    val key = rnd_str(18)
    val elem = rnd_str(1000000)
    val client = new Client("localhost", 3434)
    println(elem.length)
    val suc = client.set(key, elem.getBytes)
    storage.store()  //store and load to check persistance
    storage.load()
    val get = client.get(key)
    val rem = client.delete(key)
    assert(suc)
    assert(rem)
    assert(to_str(get.get) == elem)
    client.close()
    stop()
  }

  test("too_large_data"){
    init()
    val client = new Client("localhost", 3434)
    val set = client.set(rnd_str(32),rnd_str(1000001).getBytes)
    assert(!set)
    client.close()
    stop()
  }

  test("too_long_key"){
    init()
    val client = new Client("localhost", 3434)
    val set = client.set(rnd_str(101), rnd_str(100).getBytes)
    assert(!set)
    client.close()
    stop()
  }

  test("too_many_connections"){
    init()
    val clients = Range(0,6).map(_=>new Client("localhost", 3434))
    val num_con = clients.map(c=>Try(c.num_connections()))
    assert(num_con.count(_.isSuccess) == 5)
    clients.foreach(_.close())
    stop()
  }

  test("wont_fit_in_storage"){
    init()
    val data = rnd_str(999997).getBytes
    val client = new Client("localhost", 3434)
    //size per element in storage is 8 + key.length + data.length bytes
    //so with key.length = 96 and data.length = 999997 we store 1000001 bytes per entry
    // which and should fail on the 25th
    val ins = Range(0,25).map({_=>client.set(rnd_str(96),data)})
    assert(ins.count({b=>b}) == 24)
    client.close()
    stop()
  }

}
