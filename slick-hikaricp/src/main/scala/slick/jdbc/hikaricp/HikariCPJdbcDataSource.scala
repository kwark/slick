package slick.jdbc.hikaricp

import java.sql.{Connection, Driver}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock

import com.typesafe.config.Config
import slick.SlickException
import slick.jdbc.{JdbcDataSource, JdbcDataSourceFactory}
import slick.util.ConfigExtensionMethods._
import slick.util.Logging

/** A JdbcDataSource for a HikariCP connection pool.
  * See `slick.jdbc.JdbcBackend#Database.forConfig` for documentation on the config parameters. */
class HikariCPJdbcDataSource(val ds: com.zaxxer.hikari.HikariDataSource, val hconf: com.zaxxer.hikari.HikariConfig)
  extends JdbcDataSource
  with Logging {

  private[this] val poolLock: ReentrantLock = new ReentrantLock()
  private[this] val condition = poolLock.newCondition()
  private[this] var count: Int = 0

  def createConnection(): Connection = {
    poolLock.lock()
    try {
      if (logger.isDebugEnabled) logger.debug("number of connections in use = "+count)
      while (count == hconf.getMaximumPoolSize) {
          condition.await()
      }
      val c = new DelegateConnection(ds.getConnection()) {
        override def close(): Unit = {
          closeConnection(conn)
        }
      }
      count += 1
      if (count == hconf.getMaximumPoolSize) {
        logger.info("Maximum number of connections in use reached, pausing")
        pause()
      }
      c
    } finally {
      poolLock.unlock()
    }
  }

  private[this] def closeConnection(connection: Connection) = {
    poolLock.lock()
    try {
      connection.close()
      count -= 1
      if (count == hconf.getMaximumPoolSize -1) {
        logger.info("Connection freed below maximum, resuming")
        condition.signal()
        resume()
      }
    } finally {
      poolLock.unlock()
    }
  }


  def close(): Unit = ds.close()

}

object HikariCPJdbcDataSource extends JdbcDataSourceFactory {
  import com.zaxxer.hikari._

  def forConfig(c: Config, driver: Driver, name: String, classLoader: ClassLoader): HikariCPJdbcDataSource = {
    if(driver ne null)
      throw new SlickException("An explicit Driver object is not supported by HikariCPJdbcDataSource")
    val hconf = new HikariConfig()

    // Connection settings
    if (c.hasPath("dataSourceClass")) {
      hconf.setDataSourceClassName(c.getString("dataSourceClass"))
    } else {
      Option(c.getStringOr("driverClassName", c.getStringOr("driver"))).map(hconf.setDriverClassName _)
    }
    hconf.setJdbcUrl(c.getStringOr("url", null))
    c.getStringOpt("user").foreach(hconf.setUsername)
    c.getStringOpt("password").foreach(hconf.setPassword)
    c.getPropertiesOpt("properties").foreach(hconf.setDataSourceProperties)

    // Pool configuration
    hconf.setConnectionTimeout(c.getMillisecondsOr("connectionTimeout", 1000))
    hconf.setValidationTimeout(c.getMillisecondsOr("validationTimeout", 1000))
    hconf.setIdleTimeout(c.getMillisecondsOr("idleTimeout", 600000))
    hconf.setMaxLifetime(c.getMillisecondsOr("maxLifetime", 1800000))
    hconf.setLeakDetectionThreshold(c.getMillisecondsOr("leakDetectionThreshold", 0))
    hconf.setInitializationFailFast(c.getBooleanOr("initializationFailFast", false))
    c.getStringOpt("connectionTestQuery").foreach { s =>
      hconf.setJdbc4ConnectionTest(false)
      hconf.setConnectionTestQuery(s)
    }
    c.getStringOpt("connectionInitSql").foreach(hconf.setConnectionInitSql)
    val numThreads = c.getIntOr("numThreads", 20)
    hconf.setMaximumPoolSize(c.getIntOr("maxConnections", numThreads * 5))
    hconf.setMinimumIdle(c.getIntOr("minConnections", numThreads))
    hconf.setPoolName(name)
    hconf.setRegisterMbeans(c.getBooleanOr("registerMbeans", false))

    // Equivalent of ConnectionPreparer
    hconf.setReadOnly(c.getBooleanOr("readOnly", false))
    c.getStringOpt("isolation").map("TRANSACTION_" + _).foreach(hconf.setTransactionIsolation)
    hconf.setCatalog(c.getStringOr("catalog", null))

    val ds = new HikariDataSource(hconf)
    new HikariCPJdbcDataSource(ds, hconf)
  }
}
