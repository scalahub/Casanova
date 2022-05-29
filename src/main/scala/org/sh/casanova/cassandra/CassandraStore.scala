package org.sh.casanova.cassandra

import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.`type`.DataTypes.{TEXT, TIMESTAMP}
import com.datastax.oss.driver.api.core.`type`.{DataType, DataTypes}
import com.datastax.oss.driver.api.core.cql._
import com.datastax.oss.driver.api.querybuilder.QueryBuilder.{insertInto, selectFrom, update, _}
import com.datastax.oss.driver.api.querybuilder.delete.{Delete, DeleteSelection}
import com.datastax.oss.driver.api.querybuilder.insert.InsertInto
import com.datastax.oss.driver.api.querybuilder.relation.OngoingWhereClause
import com.datastax.oss.driver.api.querybuilder.schema.{CreateTable, CreateTableStart}
import com.datastax.oss.driver.api.querybuilder.select.Select
import com.datastax.oss.driver.api.querybuilder.update.{Update, UpdateStart, UpdateWithAssignments}
import com.datastax.oss.driver.api.querybuilder.{BuildableQuery, SchemaBuilder}
import org.sh.casanova.cassandra.CassandraStore._
import org.sh.casanova.cassandra.models.{History, HistoryT}

import scala.compat.java8.FutureConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

object CassandraStore {

  case class Col(colName: String, dataType: DataType)

  type PartitionKey = Seq[String]
  type ClusterKeys = Seq[String]
  case class PrimaryKey(partitionKey: PartitionKey, clusterKeys: ClusterKeys) {
    val allKeysNames = (partitionKey ++ clusterKeys).toSet
  }

  type TableName = String
  case class Table(tableName: TableName, primaryKey: PrimaryKey)

  // FOR INSERTS
  type BS = BoundStatement
  type Index = Int
  type BS_A_Index_To_BS[A] = (BS, A, Index) => BS
  case class Ins[A](colName:String, bS_A_Index_To_BS:BS_A_Index_To_BS[A])

  // FOR SELECTS, DELETES and UPDATES
  type BS_Any_Index_To_BS = (BS, Any, Index) => BS

  class Counter(i:Int = 0) {
    private val ctr = new AtomicInteger(i)
    def next:Int = ctr.getAndIncrement
  }

  def usingCounter[A](start:Int)(f: Counter => A) = {
    f(new Counter(start))
  }

  sealed trait Op

  object Eq extends Op
  object Lt extends Op
  object Gt extends Op
  object Le extends Op
  object Ge extends Op
  object Ne extends Op

  def applyWhrOp[T <: OngoingWhereClause[T]](clause:OngoingWhereClause[T], colName:String, op:Op): T = {
    op match {
      case Eq => clause.whereColumn(colName).isEqualTo(bindMarker())
      case Gt => clause.whereColumn(colName).isGreaterThan(bindMarker())
      case Lt => clause.whereColumn(colName).isLessThan(bindMarker())
      case Ge => clause.whereColumn(colName).isGreaterThanOrEqualTo(bindMarker())
      case Le => clause.whereColumn(colName).isLessThanOrEqualTo(bindMarker())
      case Ne => clause.whereColumn(colName).isNotEqualTo(bindMarker())
    }
  }

  class BetterAny(val a:Any) {def as[T] = a.asInstanceOf[T]} /* Shorthand for `asInstanceOf` so we can write val x = b.as[String], which is equivalent to val x = b.asInstanceOf[String] */
  implicit def anyToBetterAny(s: Any) = new BetterAny(s)

  class BetterWhr(col:String) {
    def === (data:Any) = {
      data match {
        case uuid: UUID => whr(col, Eq, uuid)
        case int: Int => whr(col, Eq, int)
        case string: String => whr(col, Eq, string)
        case boolean: Boolean => whr(col, Eq, boolean)
        case instant: Instant => whr(col, Eq, instant)
      }
    }
  }
  implicit def anyToBetterWhr(col: String) = new BetterWhr(col)

  case class Whr(colName:String, bS_Any_Index_To_BS: BS_Any_Index_To_BS, any:Any, op:Op = Eq)

  def whr(col: String, op: Op, data: UUID) = Whr(col, (b:BS, a:Any, i:Int) => b.setUuid(i, a.as[UUID]), data, op)
  def whr(col: String, op: Op, data: String) = Whr(col, (b:BS, a:Any, i:Int) => b.setString(i, a.as[String]), data, op)
  def whr(col: String, op: Op, data: Instant) = Whr(col, (b:BS, a:Any, i:Int) => b.setInstant(i, a.as[Instant]), data, op)
  def whr(col: String, op: Op, data: Int) = Whr(col, (b:BS, a:Any, i:Int) => b.setInt(i, a.as[Int]), data, op)
  def whr(col: String, op: Op, data: Boolean) = Whr(col, (b:BS, a:Any, i:Int) => b.setBoolean(i, a.as[Boolean]), data, op)

  class BetterSet(col:String) {
    def <-- (data:Any) = {
      data match {
        case uuid: UUID => set(col, uuid)
        case int: Int => set(col, int)
        case string: String => set(col, string)
        case boolean: Boolean => set(col, boolean)
        case instant: Instant => set(col, instant)
        case seqStr: Seq[String] => set(col, seqStr)
        case instant: Instant => set(col, instant)
      }
    }
  }
  implicit def anyToBetterSet(col: String) = new BetterSet(col)

  case class Upd(colName:String, bS_Any_Index_To_BS: BS_Any_Index_To_BS, any:Any)

  def set(col:String, data:UUID): Upd = Upd(col, (b:BS, a:Any, i:Int) => b.setUuid(i, a.as[UUID]), data)
  def set(col:String, data:String): Upd = Upd(col, (b:BS, a:Any, i:Int) => b.setString(i, a.as[String]), data)
  def set(col:String, data:Boolean): Upd = Upd(col, (b:BS, a:Any, i:Int) => b.setBoolean(i, a.as[Boolean]), data)
  def set(col:String, data:Instant): Upd = Upd(col, (b:BS, a:Any, i:Int) => b.setInstant(i, a.as[Instant]), data)
  def set(col:String, data:Int): Upd = Upd(col, (b:BS, a:Any, i:Int) => b.setInt(i, a.as[Int]), data)
  def set(col:String, data:Seq[String]): Upd = Upd(col, (b:BS, a:Any, i:Int) => b.setList(i, a.as[Seq[String]].asJava, classOf[String]), data)
  def set(col:String, data:Set[String]): Upd = Upd(col, (b:BS, a:Any, i:Int) => b.setSet(i, a.as[Set[String]].asJava, classOf[String]), data)


      /*
       Ideal syntax:
        dummyItemStore.update(ageCol <-- 99).where(idCol === dummyId and nameCol === dummyName).reason("dummyReason").by("foo")
        update(dummyItemStore).set(ageCol <-- 99).where(idCol === dummyId and nameCol === dummyName).reason("dummyReason").by("foo")
       */

}

abstract class CassandraStore[T](keySpace:String, session:CqlSession)(implicit ec:ExecutionContext) {

  class BetterStatement(stmt:Statement[_]) {
    def asBoolean = execAsync.map(_.wasApplied())
    def execAsync: Future[AsyncResultSet] = session.executeAsync(stmt).toScala
  }

  implicit def toBetterStatement(stmt:Statement[_]) = new BetterStatement(stmt)

  val baseTableName:TableName
  val cols:Seq[Col] // all (partition and non-partition key) columns
  val primaryKeys:Seq[PrimaryKey]

  lazy val basePrimaryKeyNames: Seq[String] = primaryKeys.head.partitionKey ++ primaryKeys.head.clusterKeys

  def findCols(cols:Seq[Col], colNames:Seq[String]):Seq[Col] = {
    colNames.map(colName => cols.find(col => col.colName == colName).getOrElse(throw new Exception(s"No column definition available for column $colName. Available cols [${cols.map(_.colName).toList}]")))
  }

  val DueToCreateOperation = "create"
  val DueToUpdateOperation = "update"
  val DueToDeleteOperation = "delete"

  lazy val historyTableName:String = baseTableName+"_history"

  lazy val (
    historyUUIDCol,
    historyUpdateTimeCol,
    historyUpdateOperationCol,
    historyUpdateReasonCol,
    historyUpdateByCol
    ) = (
    "history_UUID",
    "history_update_time",
    "history_update_operation",
    "history_update_reason",
    "history_update_by"
  )

  lazy val historyCols = Seq(
    Col(historyUUIDCol, DataTypes.UUID),
    Col(historyUpdateTimeCol, TIMESTAMP),
    Col(historyUpdateOperationCol, TEXT),
    Col(historyUpdateReasonCol, TEXT),
    Col(historyUpdateByCol, TEXT)
  )

  // we can only search history using the partition key of the first table.
  lazy val historyPrimaryKey = PrimaryKey(primaryKeys.head.partitionKey, primaryKeys.head.clusterKeys ++ Seq(historyUUIDCol))


  lazy val historyTableCols = cols ++ historyCols

  lazy val tables:Seq[Table] = primaryKeys.zipWithIndex.map{case (primaryKey, i) => Table(baseTableName + i, primaryKey)}

  protected def rowToT(row:Row):T

  def rowToHistoryT(row:Row):HistoryT[T] = HistoryT(
    rowToT(row), History(
      row.getInstant(historyUpdateTimeCol),
      row.getString(historyUpdateOperationCol),
      row.getString(historyUpdateReasonCol),
      row.getString(historyUpdateByCol)
    )
  )

  def resultSetToHistoryT(rs:AsyncResultSet) = resultSetToAny(rowToHistoryT)(rs)

  def resultSetToAny[A](rowToAny:Row => A)(rs:AsyncResultSet) = {
    var result:List[A] = Nil
    rs.currentPage().forEach{ x => result :+= rowToAny(x) }
    result
  }

  @volatile var tablePriKeyMap:Map[Set[String], TableName] = Map()

  def getTableName(implicit wheres:Seq[Whr]):TableName = {
    if (wheres.isEmpty) tables.head.tableName else {
      val colNames = wheres.map(_.colName).toSet
      tablePriKeyMap.get(colNames) match {
        case Some(tableName) => tableName
        case _ =>
          val Table(tableName, _) = tables.find {
            case Table(_, PrimaryKey(partitionKey, clusterKeys)) =>
              val partitionKeys = partitionKey.toSet
              val remainingCols = colNames.diff(partitionKeys)
              val remainingColsSize = remainingCols.size
              lazy val clusterKeysMatch = clusterKeys.zipWithIndex.foldLeft(true) {
                case (isPresent, (clusterKey, index)) => isPresent && (remainingCols.contains(clusterKey) || index >= remainingColsSize)
              }
              partitionKeys.subsetOf(colNames) && remainingCols.subsetOf(clusterKeys.toSet) && clusterKeysMatch
          }.getOrElse(throw new Exception(s"Cannot filter using columns: [${colNames.reduceLeft(_ + ","+ _)}]"))
          tablePriKeyMap += (colNames -> tableName)
          tableName
      }
    }
  }

  def selectFromT(implicit wheres: Seq[Whr]):Select = selectFrom(keySpace, getTableName).all

  def selectFromHistoryT:Select = selectFrom(keySpace, historyTableName).all

  def resultSetToT(rs:AsyncResultSet): List[T] = resultSetToAny(rowToT)(rs)

  def getBoundStatememt(qry:BuildableQuery) = scala.concurrent.blocking(session.prepare(qry.build())).bind()

  def selectAllT: Future[List[T]] = getBoundStatememt(selectFromT(Nil)).execAsync.map(resultSetToT)

  val insertsT:Seq[Ins[T]]

  def upsertT(t:T) = {
    tableNames.map(insertInto(keySpace, _)).foldLeft(BatchStatement.builder(DefaultBatchType.LOGGED)){
      case (builder, insertInto) => builder.addStatement(getInsertStmt(insertsT, t, insertInto))
    }.build().asBoolean
  }

  def wheresPriKeysT(t:T)(keys:Seq[String]) = {
    keys.map{ colName => // for every key
        insertsT.find(_.colName == colName).map{ // find the corresponding insertsT element to get the insert code
          case Ins(a, f1:((BS, T, Index) => BS)) =>
            def f2(bs:BS, any:Any, index:Index) = f1(bs, t, index) // and create the function needed for Whr
            Whr(a, f2, Eq)
        }.getOrElse(throw new Exception(s"No insertsT entry found for column $colName"))
    }
  }

  def insertT(t:T): Future[Boolean] = {
    selectWhere(wheresPriKeysT(t)(basePrimaryKeyNames): _*).flatMap{
      case Nil =>
        upsertT(t).flatMap{
          case true => insertHistory(HistoryT(t, History(Instant.now(), DueToCreateOperation, "create", "auto")))
          case _ => Future.successful(false)
        }
      case _ => Future.successful(false)
    }
  }

  lazy val tableNames = tables.map(_.tableName)

  def insertHistory(h:HistoryT[T]): Future[Boolean] = getInsertStmt(insertsHistoryT, h, insertInto(keySpace, historyTableName)).asBoolean

  lazy val insertsHistoryT: Seq[Ins[HistoryT[T]]] = insertsT.map{
    case Ins(colName, bsTIndexToBs) =>
      new Ins[HistoryT[T]](colName, (bs:BS, ht:HistoryT[T], i:Int) => bsTIndexToBs(bs, ht.t, i))
  } ++ Seq(
    new Ins[HistoryT[T]](historyUUIDCol, (b:BS, h:HistoryT[T], i:Int) => b.setUuid(i, UUID.randomUUID)),
    new Ins[HistoryT[T]](historyUpdateTimeCol, (b:BS, h:HistoryT[T], i:Int) => b.setInstant(i, h.history.updateTime)),
    new Ins[HistoryT[T]](historyUpdateOperationCol, (b:BS, h:HistoryT[T], i:Int) => b.setString(i, h.history.dueToOperation)),
    new Ins[HistoryT[T]](historyUpdateReasonCol, (b:BS, h:HistoryT[T], i:Int) => b.setString(i, h.history.updateReason)),
    new Ins[HistoryT[T]](historyUpdateByCol, (b:BS, h:HistoryT[T], i:Int) => b.setString(i, h.history.updateBy))
  )

  def getInsertStmt[A](inserts:Seq[Ins[A]], a:A, insertInto:InsertInto):BoundStatement = {
    val (colNames, seqBsAIndexToBs) = inserts.map{case Ins(colName, bsAIndexToBs) => (colName, bsAIndexToBs)}.unzip
    val (headColName, tailColNames) = (colNames.head, colNames.tail)
    val regularInsert = insertInto.value(headColName, bindMarker())
    val finalInsert = tailColNames.foldLeft(regularInsert){
      case (i, colName: String) => i.value(colName, bindMarker())
    }
    usingCounter(0) {ctr =>
      seqBsAIndexToBs.foldLeft(getBoundStatememt(finalInsert)){
        case (olderBs, newBsAIndexToBs) =>
          newBsAIndexToBs(olderBs, a, ctr.next)
      }
    }
  }

  def insertAsHistories(seqT:List[T])(dueToOperation:String, updateReason:String, updateBy:String) = {
    val updateTime = Instant.now()
    Future.sequence{
      seqT.map(t => insertHistory(HistoryT(t, History(updateTime, dueToOperation, updateReason, updateBy))))
    }
  }

  def afterCopyingToHistory[A](wheres:Seq[Whr], dueToOperation: String, updateReason:String, updateBy:String)(f: List[T] => A) = {
    selectWhere(wheres : _*).map { items =>
      insertAsHistories(items)(dueToOperation, updateReason, updateBy)
      f(items)
    }
  }

  def batchUpdate[A](updates:Seq[Upd],
                     wheresUpdatesStart:Seq[(Seq[Whr], Either[UpdateStart, (T, DeleteSelection, InsertInto)])]
                     ) = {
    wheresUpdatesStart.foldLeft(BatchStatement.builder(DefaultBatchType.LOGGED)){
      case (builder, (wheres, Left(updateStart))) =>
        builder.addStatement(getUpdateStmtA(updates, wheres, updateStart))
      case (builder, (wheres, Right((t, deleteSelection, insertInto)))) =>
        val newInsertsT: Seq[Ins[T]] = insertsT.map{
          case Ins(colName, bS_A_Index_To_BS) =>
            updates.find(_.colName == colName) match {
              case Some(Upd(_, bS_Any_Index_To_BS, any)) =>
                Ins[T](colName, (bs:BS, a:T, i:Int) => bS_Any_Index_To_BS(bs, any, i))
              case _ =>
                Ins(colName, bS_A_Index_To_BS)
            }
        }
        val deleteTimeStamp = System.currentTimeMillis()
        val insertTimeStamp = deleteTimeStamp + 1
        builder.addStatement(getDeleteStmt(wheres, deleteSelection).setQueryTimestamp(deleteTimeStamp))
        builder.addStatement(getInsertStmt(newInsertsT, t, insertInto).setQueryTimestamp(insertTimeStamp))
    }.build()
  }.asBoolean

  def updateT(updates:Upd*)(wheres:Whr*)(reason:String, updateBy:String): Future[Boolean] = {
    // copy to-be updated rows into history
    val updatesNameSet = updates.map(_.colName).toSet
    afterCopyingToHistory(wheres, DueToUpdateOperation, reason, updateBy) { items =>
      // proceed with actual update
      val wheresUpdateStart = items.flatMap { item =>
        tables.map {
          case Table(tableName, primaryKey) =>
            val intersect = primaryKey.allKeysNames.intersect(updatesNameSet)
            val qry = if (intersect.nonEmpty) {
              Right((item, deleteFrom(keySpace, tableName), insertInto(keySpace, tableName)))
            } else {
              Left(update(keySpace, tableName))
            }
            val newWheres = wheresPriKeysT(item)(primaryKey.partitionKey ++ primaryKey.clusterKeys)
            (newWheres, qry)
        }
      }
      batchUpdate(updates, wheresUpdateStart)
    }.flatten
  }

  def getUpdateStmtA(updates:Seq[Upd], wheres:Seq[Whr], updateStart:UpdateStart): BS = {
    if (updates.isEmpty) throw new Exception("At least one column must be updated")
    if (wheres.isEmpty) throw new Exception("At least one column must be selected")
    val (updColNames, updSeqBsAnyIndexToBs, updSeqAny) = updates.map{case Upd(colName, bsAnyIndexToBs, any) => (colName, bsAnyIndexToBs, any)}.unzip3

    val (headUpdColName, tailUpdColNames) = (updColNames.head, updColNames.tail)
    val update: UpdateWithAssignments = updateStart.setColumn(headUpdColName, bindMarker())

    val midUpdate1: UpdateWithAssignments = tailUpdColNames.foldLeft(update){
      case (update, colName) => update.setColumn(colName, bindMarker())
    }

    val (whereColNameOps, whereSeqBsAnyIndexToBs, whereSeqAny) = splitWheres(wheres)

    val ((headWhereColName, headWhereOp), tailWhereColNameOps) = (whereColNameOps.head, whereColNameOps.tail)
    val midUpdate2: Update = applyWhrOp(midUpdate1, headWhereColName, headWhereOp)

    val updateAfterWhere: Update = tailWhereColNameOps.foldLeft(midUpdate2){
      case (update, (colName, op)) => applyWhrOp(update, colName, op)
    }
    setAnys(updSeqBsAnyIndexToBs ++ whereSeqBsAnyIndexToBs, updSeqAny ++ whereSeqAny, updateAfterWhere)
  }

  def selectWhere(wheres:Whr*) = selectA(wheres, rowToT, selectFromT(wheres))

  def selectHistoryWhere(wheres:Whr*) = selectA(wheres, rowToHistoryT, selectFromHistoryT)

  def countT(wheres:Whr*) = {
    val (colNameOps, seqBsAnyIndexToBs, anys) = splitWheres(wheres)

    val finalSelect:Select = colNameOps.foldLeft(selectFromT(wheres)){
      case (select, (colName, op)) => applyWhrOp(select, colName, op)
    }.countAll()

    setAnys(seqBsAnyIndexToBs, anys, finalSelect).execAsync.map(_.one().getLong(0))
  }

  def splitWheres(wheres:Seq[Whr]): (Seq[(String, Op)], Seq[BS_Any_Index_To_BS], Seq[Any]) = {
    wheres.map{
      case Whr(colName, bsAnyIndexToBs, any, op) => ((colName, op), bsAnyIndexToBs, any)
    }.unzip3
  }

  def selectA[A](wheres:Seq[Whr], rowToA:Row => A, selectFromA:Select) = {
    val (colNameOps, seqBsAnyIndexToBs, anys) = splitWheres(wheres)
    val finalSelect:Select = colNameOps.foldLeft(selectFromA){
      case (select, (colName, op)) => applyWhrOp(select, colName, op)
    }

    setAnys(seqBsAnyIndexToBs, anys, finalSelect).execAsync.map(x => resultSetToAny(rowToA)(x))
  }

  def getDeleteStmt(wheres:Seq[Whr], deleteSelection:DeleteSelection) = {
    if (wheres.isEmpty) throw new Exception("At least one where definition must be given for delete")
    val (colNameOps, seqBsAnyIndexToBs, anys) = splitWheres(wheres)
    val ((headColName, headOp), tailColNameOps) = (colNameOps.head, colNameOps.tail)
    val initialDelete: Delete = applyWhrOp(deleteSelection, headColName, headOp)
    val finalDelete: Delete = tailColNameOps.foldLeft(initialDelete){
      case (currDelete, (colName, op)) => applyWhrOp(currDelete, colName, op)
    }
    setAnys(seqBsAnyIndexToBs, anys, finalDelete)
  }

  def batchDelete[A](wheresDeleteSelections:Seq[(Seq[Whr], DeleteSelection)]) = {
    wheresDeleteSelections.foldLeft(BatchStatement.builder(DefaultBatchType.LOGGED)){
      case (builder, (wheres, deleteSelection)) => builder.addStatement(getDeleteStmt(wheres, deleteSelection))
    }.build()
  }.asBoolean

  /* Sets actual data in place of bind-markers previously inserted into the query */
  def setAnys(seqBsAnyIndexToBs:Seq[BS_Any_Index_To_BS], seqAny:Seq[Any], qry:BuildableQuery):BoundStatement = usingCounter(0){ ctr =>
    (seqBsAnyIndexToBs zip seqAny).foldLeft(getBoundStatememt(qry)) {
      case (oldBsAnyIndexToBs, (newBsAnyIndexToBs, any)) => newBsAnyIndexToBs(oldBsAnyIndexToBs, any, ctr.next)
    }
  }

  /* Deletes from main table that stores instances of T */
  def deleteWhere(wheres:Whr*)(reason:String, deleteBy:String) = {
    // copy to-be updated rows into history
    afterCopyingToHistory(wheres, DueToDeleteOperation, reason, deleteBy) {items =>
      // proceed with actual delete
      val wheresDeleteSelection = items.flatMap{item =>
        tables.map{
          case Table(tableName, primaryKey) =>
            val newWheres = wheresPriKeysT(item)(primaryKey.partitionKey ++ primaryKey.clusterKeys)
            (newWheres, deleteFrom(keySpace, tableName))
        }
      }
      batchDelete(wheresDeleteSelection)
    }.flatten
  }

  def createTables = Future.sequence(tables.map{case Table(tableName, primaryKey) => createTableAny(tableName, primaryKey, cols)})

  def createHistoryTable = createTableAny(historyTableName, historyPrimaryKey, historyTableCols)

  @deprecated("For simple (non-replicated) or testing usage only", "1.0")
  def createSimpleKeySpaceIfNotExists = getBoundStatememt {
    SchemaBuilder.createKeyspace(keySpace)
      .ifNotExists()
      .withSimpleStrategy(1)
  }.asBoolean

  private def createTableAny(tableName:String, primaryKey: PrimaryKey, allCols:Seq[Col]) = {
    val PrimaryKey(partitionKeyNames, clusterKeyNames) = primaryKey
    if ((partitionKeyNames.toSet intersect clusterKeyNames.toSet).nonEmpty) throw new Exception("Partition and cluster keys must be distinct")
    val partitionKeyCols:Seq[Col] = findCols(allCols, partitionKeyNames)
    val clusterKeyCols:Seq[Col] = findCols(allCols, clusterKeyNames)

    val remainingCols:Seq[Col] = allCols.map{
      case Col(colName, colType) if partitionKeyNames.contains(colName) => None
      case Col(colName, colType) if clusterKeyNames.contains(colName) => None
      case any => Some(any)
    }.collect{
      case Some(any) => any
    }

    getBoundStatememt{
      val createTableStart:CreateTableStart = SchemaBuilder.createTable(keySpace, tableName).ifNotExists()

      if (partitionKeyCols.isEmpty) throw new Exception(s"At least one partition key is needed for table $keySpace.$tableName")
      val (Col(rootKeyName, rootKeyType), tail) = (partitionKeyCols.head, partitionKeyCols.tail)
      val createTable:CreateTable = createTableStart.withPartitionKey(rootKeyName, rootKeyType)

      val createTablePartKey:CreateTable = tail.foldLeft(createTable){
        case (currCreateTable, Col(keyName, keyType)) => currCreateTable.withPartitionKey(keyName, keyType)
      }

      val createTableClusterKey:CreateTable = clusterKeyCols.foldLeft(createTablePartKey){
        case (currCreateTable, Col(colName, colType)) => currCreateTable.withClusteringColumn(colName, colType)
      }

      remainingCols.foldLeft(createTableClusterKey){
        case (currCreateTable, Col(colName, colType)) => currCreateTable.withColumn(colName, colType)
      }:CreateTable
    }
  }.asBoolean

}
