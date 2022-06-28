
import java.io.InputStreamReader
import java.sql.{DriverManager, ResultSet}
import java.util.Properties
import java.{lang, util}
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks

object Main {
  val prop: Properties = load("spark.properties")

  val url = prop.getProperty("url")
  val properties = new Properties()
  properties.setProperty("driverClassName", prop.getProperty("driverClassName"))
  properties.setProperty("user", prop.getProperty("user"))
  properties.setProperty("password", prop.getProperty("password"))

  val connection = DriverManager.getConnection(url, properties)
  val statement = connection.createStatement

  def init(): Unit = {
    try {
      val databases = statement.executeQuery("show databases")
      println("[databases] ")
      while (databases.next) {
        //输出库名
        val databaseName = databases.getString(1)
        println(databaseName)
      }
      databases.close()

      val tables = statement.executeQuery("show tables")
      try {
        println("[tables]")
        var tableList: List[String] = List()
        while (tables.next) {
          val tableName = tables.getString(1)
          //输出所有表名
          println(tableName)
          tableList = tableList :+ tableName
        }
        tables.close()

        for(s <- tableList){
          println("[columns] table: " + s)
          val resultSet = statement.executeQuery("desc formatted "+s)
          val loop = new Breaks;
          loop.breakable {
            while (resultSet.next) {
              val columnName = resultSet.getString(1)
              val columnType = resultSet.getString(2)
              val comment = resultSet.getString(3)
              if (comment == "") loop.break
              //输出表结构
              print(s"$columnName($comment)\t")
            }
          }
          println()
          resultSet.close()
        }

      } catch {
        case e: Exception => e.printStackTrace()
      }
    }


  }

  def getAllCols(tab: String): lang.Iterable[String] = {
    var scalaList: List[String] = List()
    //查询表结构
    val resultSet = statement.executeQuery("desc formatted " + tab)
    val loop = new Breaks;
    loop.breakable {
      while (resultSet.next) {
        val columnName = resultSet.getString(1)
        val columnType = resultSet.getString(2)
        val comment = resultSet.getString(3)
        if (comment == "") {
          loop.break
        }
        val s = s"$columnName($comment)"
        //        println(s)
        scalaList = scalaList :+ s
      }
    }
    resultSet.close()
    val result = scalaList.asJava
    result
  }

  def getCols(tab: String, sql: String): lang.Iterable[String] = {
    var lst: List[String] = List()
    var allCols: Map[String, String] = Map()

    var i = sql.indexOf(" ")
    var j = sql.indexOf("from") - 1
    while (sql(i) == ' ') i = i + 1;
    while (sql(j) == ' ') j = j - 1;
    val cols = sql.substring(i, j + 1).split(",")
    val resultSet = statement.executeQuery("desc formatted " + tab)
    val loop = new Breaks;
    loop.breakable {
      while (resultSet.next) {
        val columnName = resultSet.getString(1)
        val columnType = resultSet.getString(2)
        val comment = resultSet.getString(3)
        if (comment == "") {
          loop.break
        }
        val s = s"$columnName($comment)"
        allCols += (columnName -> comment)
      }
    }
    resultSet.close()
    for (i <- cols) {
      if (allCols.contains(i)) {
        val s = i + "(" + allCols(i) + ")"
        lst = lst :+ s
      }
    }
    val result = lst.asJava
    result
  }

  def executeSql(sql: String, cols: util.List[String]) = {
    val stmt = connection.createStatement()
    val resultSet = stmt.executeQuery(sql)
    // select sfzhm,xm,xb,csrq,zy from t_rk_jbxx limit 8
    // select * from t_rk_jbxx limit 8
    var t = 0
    val lst = cols.asScala.toList
    var resultList = ListBuffer[ResultSet]()
    while (resultSet.next()) {
      if (t == 0) {
        for (s <- lst) {
          print(s + "\t\t")
        }
      }
      t += 1;
      println()
      for (i <- 1 to cols.size()) {
        print(resultSet.getString(i) + "\t\t")
      }
      resultList += resultSet
    }
    println()
    resultSet.close()
  }

  def load(propertieName: String): Properties = {
    val prop = new Properties();
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName), "UTF-8"))
    prop
  }


}
