package org.sh.casanova.cassandra.models

import java.time.Instant

case class UpdateDetail(by:String, reason:String)

object DefaultUpdateDetail extends UpdateDetail("none", "none")

case class History(updateTime:Instant, dueToOperation:String, updateReason:String, updateBy:String)

case class HistoryT[T](t:T, history:History)

