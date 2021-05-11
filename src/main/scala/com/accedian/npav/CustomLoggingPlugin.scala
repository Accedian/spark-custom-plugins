package com.accedian.npav

import java.util

import org.apache.log4j.{LogManager, PropertyConfigurator}
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.internal.Logging
import org.apache.spark.{SparkContext, SparkEnv, SparkFiles}


class CustomLoggingPlugin extends SparkPlugin {

  override def executorPlugin(): ExecutorPlugin = new CustomExecutorPlugin

  override def driverPlugin(): DriverPlugin = new CustomDriverPlugin
}

class CustomExecutorPlugin extends ExecutorPlugin with Logging {

  override def init(ctx: PluginContext, extraConf: util.Map[String, String]): Unit = {
    logInfo("init")
    SparkLoggingHelper.reconfigureLogging
  }

  override def shutdown(): Unit = super.shutdown()
}

class CustomDriverPlugin extends DriverPlugin with Logging {
  override def init(sc: SparkContext, pluginContext: PluginContext): util.Map[String, String] = {
    logInfo("init")
    SparkLoggingHelper.reconfigureLogging
    super.init(sc, pluginContext)
  }

}


object SparkLoggingHelper extends Serializable with Logging {
  @volatile var reconfigured = false

  def reconfigureLogging(): Unit = synchronized {
    println("reconfigure logging")
    if (!reconfigured) {
      val logConfigFilename = System.getProperty("log4j.configuration")

      logInfo(s"logConfigFilename = $logConfigFilename")
      if (SparkEnv.get != null && logConfigFilename != null) {
        val absLogFilename = SparkFiles.get(logConfigFilename)
        logInfo(s"absLogFilename = $absLogFilename")
        if (new java.io.File(absLogFilename).exists) {
          logInfo(s"updating log configuration to use $absLogFilename")
          LogManager.resetConfiguration()
          PropertyConfigurator.configure(absLogFilename)
        }
      }

    }
    reconfigured = true
  }

}