package com.novocode.squery.combinator

trait SimpleFunction extends Node {
  val name: String
}

object SimpleFunction {
  def apply[T](fname: String)(implicit tm: TypeMapper[T]): (Seq[Column[_]] => OperatorColumn[T] with SimpleFunction) =
    (paramsC: Seq[Column[_]]) =>
      new OperatorColumn[T]()(tm) with SimpleFunction {
        val name = fname
        def nodeChildren = paramsC.map(n => Node(n)).toList
      }
}

trait SimpleScalarFunction extends Node {
  val name: String
}

object SimpleScalarFunction {
  def apply[T](fname: String)(implicit tm: TypeMapper[T]): (Seq[Column[_]] => OperatorColumn[T] with SimpleScalarFunction) =
    (paramsC: Seq[Column[_]]) =>
      new OperatorColumn[T]()(tm) with SimpleScalarFunction {
        val name = fname
        def nodeChildren = paramsC.map(n => Node(n)).toList
      }
  def nullary[T](fnName: String)(implicit tm: TypeMapper[T]) =
    new OperatorColumn[T]()(tm) with SimpleScalarFunction with NullaryNode { val name = fnName }
}

trait SimpleBinaryOperator extends BinaryNode {
  val name: String
}

object SimpleBinaryOperator {
  def apply[T](fname: String)(implicit tm: TypeMapper[T]): ((Column[_], Column[_]) => OperatorColumn[T] with SimpleBinaryOperator) =
    (leftC: Column[_], rightC: Column[_]) =>
      new OperatorColumn[T]()(tm) with SimpleBinaryOperator {
        val name = fname
        val left = Node(leftC)
        val right = Node(rightC)
      }
}

case class SimpleLiteral(name: String) extends Node {
  val nodeChildren = Nil
}
