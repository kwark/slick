package slick.test.codegen

import scala.concurrent.ExecutionContext.Implicits.global
import slick.ast.{FieldSymbol, Node, Select}
import slick.jdbc.meta.MTable
import slick.test.codegen.generated._
import com.typesafe.slick.testkit.util.TestCodeRunner
import org.junit.Assert._

/** Test files generated by CodeGeneratorTest */
class GeneratedCodeTest extends TestCodeRunner(AllTests)

object GeneratedCodeTest {
  def testCG1 = {
    import CG1._
    import profile.api._
    DBIO.seq(
      schema.create,
      Suppliers += Supplier(1, "1", "2", "3", "4", "5"),
      Suppliers.length.result.map(assertEquals("Size of Suppliers after change", 1, _)),
      Coffees.length.result.map(assertEquals("Size of Coffees", 0, _)),
      MTable.getTables(Some(""), Some(""), None, None).map { tables =>
        val a = tables.find(_.name.name equals "a").get
        val b = tables.find(_.name.name equals "b").get
        assertEquals("# of FKs of 'a' should be 1",
          1, A.baseTableRow.foreignKeys.size)
        assertEquals("# of FKs of 'b' should be 0",
          0, B.baseTableRow.foreignKeys.size)
        val aFk = A.baseTableRow.foreignKeys.head
        val srcColumns = convertColumnsToString(aFk.linearizedSourceColumns.toList)
        val trgColumns = convertColumnsToString(aFk.linearizedTargetColumns.toList)
        assertEquals("FKs should have the same source column", List("k1"), srcColumns)
        assertEquals("FKs should have the same target column", List("f1"), trgColumns)
        assertTrue("FKs should be from 'a' to 'b'", tableName(aFk.sourceTable) == A.baseTableRow.tableName && tableName(aFk.targetTable) == B.baseTableRow.tableName)

        assertEquals("# of FKs of 'c' should be 1", 1, C.baseTableRow.foreignKeys.size)
        assertEquals("# of FKs of 'd' should be 0", 0, D.baseTableRow.foreignKeys.size)
        val cFk = C.baseTableRow.foreignKeys.head
        val cSrcColumns = convertColumnsToString(cFk.linearizedSourceColumns.toList)
        val cTrgColumns = convertColumnsToString(cFk.linearizedTargetColumns.toList)
        assertEquals("FKs should have the same source column", List("k1", "k2"), cSrcColumns)
        assertEquals("FKs should have the same target column", List("f1", "f2"), cTrgColumns)
        assertTrue("FKs should be from 'c' to 'd'", tableName(cFk.sourceTable) == C.baseTableRow.tableName && tableName(cFk.targetTable) == D.baseTableRow.tableName)

        assertEquals("# of unique indices of 'c' should be 0", 0, C.baseTableRow.indexes.size)
        assertEquals("# of unique indices of 'd' should be 1", 1, D.baseTableRow.indexes.size)
        val dIdx = D.baseTableRow.indexes.head
        val dIdxFieldsName = convertColumnsToString(dIdx.on)
        assertTrue("Indices should refer to correct field", dIdxFieldsName sameElements List("f1", "f2"))

        def optionsOfColumn(c: slick.lifted.Rep[_]) =
          c.toNode.asInstanceOf[Select].field.asInstanceOf[FieldSymbol].options.toList
        val k1Options = optionsOfColumn(E.baseTableRow.k1)
        val k2Options = optionsOfColumn(E.baseTableRow.k2)
        val sOptions = optionsOfColumn(E.baseTableRow.s)
        assertTrue("k1 should be AutoInc", k1Options.exists(option => (option equals E.baseTableRow.O.AutoInc)))
        assertTrue("k2 should not be AutoInc", k2Options.forall(option => !(option equals E.baseTableRow.O.AutoInc)))
        assertTrue("s should not be AutoInc", sOptions.forall(option => !(option equals E.baseTableRow.O.AutoInc)))
        // test default values
        assertEquals(None, ERow(1,2).n)
        assertEquals("test", ERow(1,2).s)
        assertEquals("asdf", ERow(1,2,"asdf").s)
      }
    )
  }

  def testCG2 = {
    class Db1 extends CG2 {
      val profile = slick.jdbc.HsqldbProfile
    }
    val Db1 = new Db1
    import Db1._
    import profile.api._
    val s = Supplier(49, "Superior Coffee", "1 Party Place", "Mendocino", "CA", "95460")
    DBIO.seq(
      schema.create,
      Suppliers.length.result.map(assertEquals(0, _)),
      Suppliers += s,
      Suppliers.result.map(assertEquals(List(s), _))
    )
  }

  def testCG3 = {
    import CG3._
    import profile.api._
    val s = Supplier(49, "Superior Coffee", "1 Party Place", "Mendocino", "CA", "95460")
    val m = Model(Some("key"), Some("1000"), Some("model"), Some("en"), Some("0.0.1"), None)
    val mw = MultiwordType(0, 0, Some(0), Some("varchar"))
    assertEquals("Int'" , MultiwordTypes.baseTableRow.smallintUnsigned.toNode.nodeType.toString)
    assertEquals("Int'" , MultiwordTypes.baseTableRow.unsignedSmallint.toNode.nodeType.toString)
    assertEquals("Option[Int']" , MultiwordTypes.baseTableRow.unsignedBigInt.toNode.nodeType.toString)
    assertEquals("Option[String']" , MultiwordTypes.baseTableRow.varChar20.toNode.nodeType.toString)
    DBIO.seq(
      schema.create,
      Suppliers += s,
      Models += m,
      MultiwordTypes += mw,
      Timestamps += new Timestamp(),
      Timestamps.result.head.map{r =>
	val c = new Timestamp()
	val diff = 10*1000 //10 seconds
	(r.ts, r.ts2, r.ts3, r.ts4, r.ts5, r.ts6, r.ts7) match{
		case (c.ts, c.ts2, c.ts3, c.ts4, c.ts5, now, c.ts7) =>  assertTrue(c.ts6.compareTo(r.ts6) <= diff)
	}
      },
      Suppliers.result.map(assertEquals(List(s), _)),
      Models.result.map(assertEquals(List(m), _)),
      MultiwordTypes.result.map(assertEquals(List(mw), _))
    )
  }

  def testCG7 = {
    import CG7._
    import profile.api._
    DBIO.seq(
      schema.create,
      Supps.length.result.map(assertEquals(0, _)),
      Supps += Supplier(1, "1", "2", "3", "4", "5"),
      Supps.length.result.map(assertEquals(1, _)),
      Coffs.length.result.map(assertEquals(0, _))
    )
  }

  def testCG8 = {
    import CG8._
    import profile.api._
    DBIO.seq(
      schema.create,
      SimpleAs.length.result.map(assertEquals(0, _)),
      SimpleAs += SimpleA(CustomTyping.True, "1"),
      SimpleAs.length.result.map(assertEquals(1, _)),
      SimpleAs.result.map(assertEquals(List(SimpleA(CustomTyping.True, "1")), _))
    )
  }

  def testCG9 = {
    import CG9._
    import profile.api._
    def assertAll(all: Seq[ERow]) = {
      assertEquals( 3, all.size )
      assertEquals( Set(1,2,3), all.map(_.k1.get).toSet )
      assertEquals( all.map(_.k2), all.map(_.k1.get) ) // assert auto inc order, should be tested somewhere else as well
    }
    DBIO.seq(
      schema.create,
      E += ERow(1,"foo",Some("bar"),Some(2)),
      E += ERow(2,"foo",Some("bar")),
      E += ERow(3,"foo",Some("bar"),None),
      E.result.map(assertAll),
      sql"select k1, k2, s, n from e".as[ERow].map(assertAll)
    )
  }

  def testCG11 = {
    import CG11._
    import profile.api._
    def assertAll(all: Seq[SimpleA]) = {
      assertEquals( 1, all.size )
      assertEquals(List(SimpleA(Some(1), Some("foo"))), all)
    }
    DBIO.seq(
      schema.create,
      SimpleAs.length.result.map(assertEquals(0, _)),
      SimpleAs += SimpleA(Some(1), Some("foo")),
      SimpleAs.length.result.map(assertEquals(1, _)),
      SimpleAs.result.map(assertEquals(List(SimpleA(Some(1), Some("foo"))), _)),
      sql"select a1, a2 from SIMPLE_AS".as[SimpleA].map(assertAll)
    )
  }

  def testDerby = testEmptyDB
  def testDerbyMem = testEmptyDB
  def testDerbyDisk = testEmptyDB
  def testMySQL = testEmptyDB
  def testSqlServer = testEmptyDB
  def testDB2 = testEmptyDB

  def testEmptyDB = slick.dbio.DBIO.successful(())

  def tableName( node:Node ) : String = {
    import slick.ast._
    node match {
      case TableExpansion(_, tableNode, _) => tableName(tableNode)
      case t: TableNode => t.tableName
    }
  }

  def convertColumnsToString(columns: Seq[Node]) = columns.map(_.asInstanceOf[Select].field.name)
}
