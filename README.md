# Casanova
[![Build Status](https://travis-ci.org/scalahub/Casanova.svg?branch=master)](https://travis-ci.org/scalahub/Casanova)

## Cassandra access library

See the example [here](https://github.com/scalahub/Casanova/blob/d950b1e66179497cd6e8276116d7b0cc52952434/src/test/scala/org/sh/casanova/cassandra/CassandraStoreSpec.scala#L26:L47 "here") to get started

In summary, define your store as follows

```
class DummyItemStore(session:CqlSession, implicit ec:ExecutionContext) extends CassandraStore[DummyItem](session) {
    
  // first define abstract vals and defs from CassandraStore
  lazy val keySpace = "dummy_keyspace"
  lazy val numberOfReplicas = 1
  lazy val baseTableName: String = "dummy"
  
  // the below line will create multiple tables, one for each primary key
  lazy val primaryKeys = Seq( 
    PrimaryKey(partitionKey = Seq(idCol), clusterKeys = Nil), // for first table
    PrimaryKey(partitionKey = Seq(nameCol), clusterKeys = Seq(idCol)) // for second table, kept in sync with first
  ) 
  
  lazy val cols = Seq(Col(idCol, TEXT), Col(nameCol, TEXT), Col(ageCol, INT))
  
  override def rowToT(row: Row) = DummyItem(row.getString(idCol), row.getString(nameCol), row.getInt(ageCol))
  
  lazy val insertsT = Seq(
    Ins(idCol, (b:BS, d:DummyItem, i:Int) => b.setString(i, d.id)),
    Ins(nameCol, (b:BS, d:DummyItem, i:Int) => b.setString(i, d.name)),
    Ins(ageCol, (b:BS, d:DummyItem, i:Int) => b.setInt(i, d.age))
  )

  // next define your own methods for further use
  def get(id:String) = selectT(whr(idCol, Eq, id))
  def delete(id:String, reason:String, deleteBy:String) = deleteT(whr(idCol, Eq, id))(reason, deleteBy)
}
```
