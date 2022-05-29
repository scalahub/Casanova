package org.sh.casanova.cassandra

import com.datastax.oss.driver.api.core.`type`.DataTypes._
import com.datastax.oss.driver.api.core.cql.Row
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}
import org.scalatestplus.mockito.MockitoSugar
import org.sh.casanova.cassandra.models.{History, HistoryT}

import java.util.UUID
import scala.concurrent.ExecutionContext

case class DummyItem(id:String, name:String, age:Int)

object DummyItemStore {
  val idCol = "id"
  val nameCol = "name"
  val ageCol = "age"
}

import org.sh.casanova.cassandra.CassandraStore._
import org.sh.casanova.cassandra.DummyItemStore._

class DummyItemStore(implicit ec:ExecutionContext) extends CassandraStore[DummyItem]("dummy_keyspace", EmbeddedCassandraHelper.session) {

  lazy val baseTableName: String = "dummy"
  lazy val primaryKeys = Seq(
    PrimaryKey(Seq(idCol), Nil),
    PrimaryKey(Seq(nameCol), Seq(idCol))
  )

  lazy val cols: Seq[Col] = Seq(Col(idCol, TEXT), Col(nameCol, TEXT), Col(ageCol, INT))

  override def rowToT(row: Row): DummyItem = DummyItem(row.getString(idCol), row.getString(nameCol), row.getInt(ageCol))

  lazy val insertsT: Seq[Ins[DummyItem]] = Seq(
    Ins(idCol, (b:BS, d:DummyItem, i:Int) => b.setString(i, d.id)),
    Ins(nameCol, (b:BS, d:DummyItem, i:Int) => b.setString(i, d.name)),
    Ins(ageCol, (b:BS, d:DummyItem, i:Int) => b.setInt(i, d.age))
  )
  def get(id:String) = selectWhere(idCol === id)
  def delete(id:String, reason:String, deleteBy:String) = deleteWhere(idCol === id)(reason, deleteBy)
}

class CassandraStoreSpec extends WordSpec with BeforeAndAfterAll with ScalaFutures with Matchers
  with MockitoSugar {

  def randId = UUID.randomUUID().toString

  implicit lazy val executionContext: ExecutionContext = ExecutionContext.Implicits.global

  val dummyItemStore = new DummyItemStore()

  override def beforeAll(): Unit = {
    import dummyItemStore._

    import scala.concurrent.duration._
    eventually(timeout(1.minutes), interval(5.seconds)) {
      createSimpleKeySpaceIfNotExists.futureValue
      createHistoryTable.futureValue
      createTables.futureValue
    }
  }

  "Insert" should {
    "Insert the object correctly" in {
      val (dummyId, dummyName) = (java.util.UUID.randomUUID().toString, "name")
      val dummyItem = DummyItem(dummyId, dummyName, 10)

      val result = for {
        _ <- dummyItemStore.insertT(dummyItem)
        result <- dummyItemStore.get(dummyId)
      } yield result

      result.futureValue shouldBe (List(dummyItem))
    }
  }

  "Insert" should {
    "Abort if primary key already exists" in {
      val (dummyId, dummyName) = (java.util.UUID.randomUUID().toString, "name")
      val dummyItem = DummyItem(dummyId, dummyName, 20)
      val dummyItemAlt = DummyItem(dummyId, "foo", 30)

      val (i1, i2) = {
        for {
          x <- dummyItemStore.insertT(dummyItem)
          y <- dummyItemStore.insertT(dummyItemAlt)
        } yield (x, y)
      }.futureValue

      i1 shouldBe (true)
      i2 shouldBe (false)
      dummyItemStore.get(dummyId).futureValue shouldBe (List(dummyItem))
    }
  }

  "Delete" should {
    "Delete an inserted object" in {
      val (dummyId, dummyName) = (java.util.UUID.randomUUID().toString, "name")
      val dummyItem = DummyItem(dummyId, dummyName, 10)

      val (dummyReason, dummyUpdateBy) = ("update", "test")

      val (list1, list2) = {
        for {
          _ <- dummyItemStore.insertT(dummyItem)
          _ <- dummyItemStore.delete(dummyId, dummyReason, dummyUpdateBy)
          list1 <- dummyItemStore.get(dummyId)
          list2 <- dummyItemStore.selectHistoryWhere(idCol === dummyId).map{ w => w.map(_.t)}
        } yield (list1, list2)
      }.futureValue

      list1 shouldBe Nil
      list2 shouldBe List(dummyItem, dummyItem)
    }
  }

  "Update" should {
    "Update an inserted object" in {
      val dummyId = java.util.UUID.randomUUID().toString
      val dummyName = "name_orig"
      val dummyItem = DummyItem(dummyId, dummyName, 10)
      val dummyItem1 = DummyItem(dummyId, dummyName, 21)

      val (dummyReason, dummyUpdateBy) = ("update", "test")

      val lists = {
        for {
          _ <- dummyItemStore.insertT(dummyItem)
          list1 <- dummyItemStore.get(dummyId)
          _ <- dummyItemStore.updateT(set(ageCol, dummyItem1.age))(idCol === dummyItem.id)(dummyReason, dummyUpdateBy)
          list2 <- dummyItemStore.get(dummyId)
        } yield Seq(list1, list2)
      }.futureValue

      lists(0) shouldBe (List(dummyItem))
      lists(1) shouldBe (List(dummyItem1))
    }
  }

  "UpdateIf" should {
    "Update an inserted object" in {
      val dummyId = java.util.UUID.randomUUID().toString
      val dummyName = "name_orig"
      val dummyItem = DummyItem(dummyId, dummyName, 10)
      val dummyItem1 = DummyItem(dummyId, dummyName, 99)

      val (dummyReason, dummyUpdateBy) = ("update", "test")

      val lists = {
        for {
          _ <- dummyItemStore.insertT(dummyItem)
          list1 <- dummyItemStore.get(dummyId)
          _ <- dummyItemStore.updateT(ageCol <-- 99)(idCol === dummyId, nameCol === dummyName)(dummyReason, dummyUpdateBy)
          list2 <- dummyItemStore.get(dummyId)
        } yield Seq(list1, list2)
      }.futureValue

      lists(0) shouldBe (List(dummyItem))
      lists(1) shouldBe (List(dummyItem1))
    }
  }

  "Select filters" should {
    "Select filtered objects in" in {
      val dummyId1 = randId
      val dummyId2 = randId
      val dummyId3 = randId
      val dummyId4 = randId
      val (dummyName1, dummyName2, dummyName3) = (s"name1$randId", s"name2$randId", s"name3$randId")
      val dummyName4 = dummyName3
      val (dummyItem1, dummyItem2, dummyItem3, dummyItem4) = (DummyItem(dummyId1, dummyName1, 21), DummyItem(dummyId2, dummyName2, 22), DummyItem(dummyId3, dummyName3, 23), DummyItem(dummyId4, dummyName4, 24))

      val lists: Seq[List[DummyItem]] = {
        for {
          _ <- dummyItemStore.insertT(dummyItem1)
          _ <- dummyItemStore.insertT(dummyItem2)
          _ <- dummyItemStore.insertT(dummyItem3)
          _ <- dummyItemStore.insertT(dummyItem4)
          all <- dummyItemStore.selectWhere()
          list1 <- dummyItemStore.selectWhere(whr(idCol, Eq, dummyId1))
          list2 <- dummyItemStore.selectWhere(whr(nameCol, Eq, dummyName2))
          list3 <- dummyItemStore.selectWhere(whr(idCol, Eq, dummyId3), whr(nameCol, Eq, dummyName3))
          list4 <- dummyItemStore.selectWhere(whr(idCol, Eq, dummyId3), whr(nameCol, Eq, dummyName1))
          list5 <- dummyItemStore.selectWhere(whr(nameCol, Eq, dummyName3))
        } yield Seq(all, list1, list2, list3, list4, list5)
      }.futureValue
      Set(dummyItem1, dummyItem2, dummyItem3, dummyItem4).subsetOf(lists(0).toSet) shouldBe (true)
      lists(1) shouldBe (List(dummyItem1))
      lists(2) shouldBe (List(dummyItem2))
      lists(3) shouldBe (List(dummyItem3))
      lists(4) shouldBe (Nil)
      lists(5).toSet shouldBe (Set(dummyItem3, dummyItem4))
    }
  }

  "Multiple updates and delete" should {
    "Update history" in {
      val dummyId = java.util.UUID.randomUUID().toString
      val dummyName = "name0"
      val dummyItem0 = DummyItem(dummyId, dummyName, 10)

      val (dummyAge1, dummyAge2, dummyAge3) = (41, 42, 43)
      val (dummyItem1, dummyItem2, dummyItem3) = (DummyItem(dummyId, dummyName, dummyAge1), DummyItem(dummyId, dummyName, dummyAge2), DummyItem(dummyId, dummyName, dummyAge3))

      val (dummyReason1, dummyReason2, dummyReason3, dummyReason4) = ("reason1", "reason2", "reason3", "reason4")
      val (dummyUpdateBy1, dummyUpdateBy2, dummyUpdateBy3, dummyDeletedBy) = ("update1", "update2", "update3", "delete")

      val (list, histories): (List[DummyItem], List[HistoryT[DummyItem]]) = {
        for {
          _ <- dummyItemStore.insertT(dummyItem0)
          _ <- dummyItemStore.updateT(set(ageCol, dummyAge1))(whr(idCol, Eq, dummyId))(dummyReason1, dummyUpdateBy1)
          _ <- dummyItemStore.updateT(set(ageCol, dummyAge2))(whr(idCol, Eq, dummyId))(dummyReason2, dummyUpdateBy2)
          _ <- dummyItemStore.updateT(set(ageCol, dummyAge3))(whr(idCol, Eq, dummyId))(dummyReason3, dummyUpdateBy3)
          _ <- dummyItemStore.deleteWhere(whr(idCol, Eq, dummyId))(dummyReason4, dummyDeletedBy)
          histories <- dummyItemStore.selectHistoryWhere(whr(idCol, Eq, dummyId))
          list <- dummyItemStore.get(dummyId)
        } yield (list, histories)
      }.futureValue

      list shouldBe (Nil)
      val history = histories.sortBy(_.history.updateTime.toEpochMilli)
      val HistoryT(historyItem0, History(time0, dueToOperation0, updateReason0, updateBy0)) = history(0)
      val HistoryT(historyItem1, History(time1, dueToOperation1, updateReason1, updateBy1)) = history(1)
      val HistoryT(historyItem2, History(time2, dueToOperation2, updateReason2, updateBy2)) = history(2)
      val HistoryT(historyItem3, History(time3, dueToOperation3, updateReason3, updateBy3)) = history(3)
      val HistoryT(historyItem4, History(time4, dueToOperation4, updateReason4, updateBy4)) = history(4)

      (historyItem0, historyItem1, historyItem2, historyItem3, historyItem4) shouldBe ((dummyItem0, dummyItem0, dummyItem1, dummyItem2, dummyItem3))
      (updateReason0, updateReason1, updateReason2, updateReason3, updateReason4) shouldBe (("create", dummyReason1, dummyReason2, dummyReason3, dummyReason4))
      (updateBy0, updateBy1, updateBy2, updateBy3, updateBy4) shouldBe (("auto", dummyUpdateBy1, dummyUpdateBy2, dummyUpdateBy3, dummyDeletedBy))
      (dueToOperation0, dueToOperation1, dueToOperation2, dueToOperation3, dueToOperation4) shouldBe (("create", "update", "update", "update", "delete"))

    }
  }

}
