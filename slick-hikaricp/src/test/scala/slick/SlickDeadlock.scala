package slick

import slick.driver.H2Driver.api._
import slick.lifted.ProvenShape

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

object SlickDeadlock extends App {

  class TestTable(tag: Tag) extends Table[(Int)](tag, "TEST") {

    def id: Rep[Int] = column[Int]("ID")
    def * : ProvenShape[(Int)] = id

  }

  val database = Database.forConfig("h2mem1")

  val testTable: TableQuery[TestTable] = TableQuery[TestTable]
  Await.result(database.run(testTable.schema.create), 2.seconds)

  val tasks = 1 to 100 map { i =>
    val action = { testTable += i }
      .flatMap { _ => testTable.length.result }
      .flatMap { _ => DBIO.successful(s"inserted value $i") }

    database.run(action.transactionally)
  }

  println("waiting")
  Await.result(Future.sequence(tasks), Duration.Inf)
  println("done")

}
