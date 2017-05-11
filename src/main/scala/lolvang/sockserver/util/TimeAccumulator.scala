package lolvang.sockserver.util

/**
  * Created by olvang on 2017-05-08.
  */
class TimeAccumulator {
  private var acc:Long = _
  private var ts:Long = _
  reset()
  def start():Unit  = synchronized {ts = System.currentTimeMillis()}
  def stop():Unit   = synchronized {acc += System.currentTimeMillis() - ts}
  def get:Long      = acc
  def reset():Unit  = synchronized {acc = 0L; ts = System.currentTimeMillis()}
}
