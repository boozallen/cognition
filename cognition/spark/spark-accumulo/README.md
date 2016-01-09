#Accumulo Data Connector for Spark

This wraps hadoop input formats for spark to create RDDs from Accumulo, leveraging tserver and datanode locality. All RDD functions, such as map and filter, are available on these RDDs.

##RDD Types
There are currently 3 options for rdd formats:
- accumuloRDD -- org.apache.spark.rdd.RDD[(org.apache.accumulo.core.data.Key, org.apache.accumulo.core.data.Value)] : this wraps AccumuloInputFormat
- accumuloRowRDD -- org.apache.spark.rdd.RDD[(org.apache.hadoop.io.Text, org.apache.accumulo.core.util.PeekingIterator[java.util.Map.Entry[org.apache.accumulo.core.data.Key,org.apache.accumulo.core.data.Value]])] : this wraps AccumuloRowInputFormat
- accumuloExpandedRowRDD -- org.apache.spark.rdd.RDD[(org.apache.hadoop.io.Text, scala.collection.immutable.Map[(String, String),String])] : this is a pair with Row ID as a Text and the column families and column qualifiers as pair keys to the value

##Usage -- scala
```scala
import com.boozallen.cognition.spark.accumulo.accumulo._
import com.boozallen.cognition.accumulo.config.AccumuloConfiguration

val config = new AccumuloConfiguration("test", "localhost:2181","root","")
config.setTableName("test")

val accumuloKV = sc.accumuloRDD(config.getConfiguration())
val accumuloRow = sc.accumuloRowRDD(config.getConfiguration())
val accumuloExpandedRow = sc.accumuloExpandedRowRDD(config.getConfiguration())
```
