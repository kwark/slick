package slick.util

trait Pausable {

  def pause(): Unit
  def resume(): Unit

}
