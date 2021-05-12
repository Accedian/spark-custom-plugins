package com.accedian.npav

import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.log4j.{LogManager, PropertyConfigurator}
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.internal.Logging
import org.apache.spark.{SparkContext, SparkEnv, SparkFiles}


class CustomLoggingPlugin extends SparkPlugin {

  override def executorPlugin(): ExecutorPlugin = new CustomExecutorLoggingPlugin

  override def driverPlugin(): DriverPlugin = new CustomDriverLoggingPlugin
}

class CustomExecutorLoggingPlugin extends ExecutorPlugin with Logging {

  override def init(ctx: PluginContext, extraConf: util.Map[String, String]): Unit = {
    logInfo("init")

    val thread = new Thread {
      override def run {
        while(!SparkLoggingHelper.reconfigureLogging) {
          Thread.sleep(5000)
        }
      }
    }
    thread.setDaemon(true)
    thread.start
    super.init(ctx, extraConf)
  }

  override def shutdown(): Unit = super.shutdown()
}

class CustomDriverLoggingPlugin extends DriverPlugin with Logging {
  override def init(sc: SparkContext, pluginContext: PluginContext): util.Map[String, String] = {
    logInfo("init")
    SparkLoggingHelper.reconfigureLogging
    super.init(sc, pluginContext)
  }

}


object SparkLoggingHelper extends Serializable with Logging {
  @volatile var reconfigured = false

  def reconfigureLogging(): Boolean = synchronized {
    if (!reconfigured) {

      val logConfigFilename = System.getProperty("log4j.configuration")
      val absoluteConfigFilename: Option[String] = if (logConfigFilename != null) {
        lazy val files = SparkEnv.get.conf.get("spark.files", "")
        lazy val source = files.split(",").find(_.endsWith(logConfigFilename)).orNull
        if (new java.io.File(logConfigFilename).exists) {
          println(s"$logConfigFilename exists")
          Some(logConfigFilename)
        }
        else if (source != null) {
          val dest = SparkFiles.get(logConfigFilename)
          val srcFS = new Path(source).getFileSystem(new Configuration())
          println(s"Copying $source to $dest")
          srcFS.copyToLocalFile(new Path(source), new Path(dest))
          Some(dest)
        }
        else {
          println("log4j config file not found")
          None
        }
      }
      else {
        None
      }

      absoluteConfigFilename.foreach { f =>
        println(s"Updating log configuration to use $f")
        LogManager.resetConfiguration()
        PropertyConfigurator.configure(f)
        reconfigured = true
      }
    }
    reconfigured
  }

}