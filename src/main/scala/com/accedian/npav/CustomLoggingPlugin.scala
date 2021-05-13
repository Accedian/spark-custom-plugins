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
    SparkLoggingHelper.reconfigureLogging
    super.init(ctx, extraConf)
  }

  override def shutdown(): Unit = super.shutdown()
}

class CustomDriverLoggingPlugin extends DriverPlugin with Logging {
  override def init(sc: SparkContext, pluginContext: PluginContext): util.Map[String, String] = {
    SparkLoggingHelper.reconfigureLogging
    super.init(sc, pluginContext)
  }

}


object SparkLoggingHelper extends Serializable with Logging {
  @volatile var reconfigured = false

  def reconfigureLogging(): Boolean = synchronized {
    lazy val logConfigFilename = System.getProperty("log4j.configuration")

    if (!reconfigured && logConfigFilename != null) try {


      if (SparkEnv.get.conf.get("spark.submit.deployMode", "").equals("client")) {
        val th = new Thread {
          override def run(): Unit = {
            while(!new java.io.File(logConfigFilename).exists) {
              Thread.sleep(1000)
            }
            resetLoggingAndConfigure(logConfigFilename)
          }
        }
        th.setDaemon(true)
        th.run()
      } else {
        fetchFile(logConfigFilename).foreach(f => resetLoggingAndConfigure(f))
      }
    }
    catch {
      case ex: Throwable =>
        logWarning("Failed to configure logging", ex)
    }
    reconfigured
  }

  private def resetLoggingAndConfigure(configFilename: String) = {
    println(s"Updating log configuration to use $configFilename")
    LogManager.resetConfiguration()
    PropertyConfigurator.configure(configFilename)
    reconfigured = true
  }

  private def fetchFile(logConfigFilename: String) = {
    lazy val files = SparkEnv.get.conf.get("spark.files", "")
    lazy val source = files.split(",").find(_.endsWith(logConfigFilename)).orNull

    if (new java.io.File(logConfigFilename).exists) {
      // The file is already present in the working directory
      println(s"$logConfigFilename exists")
      Some(logConfigFilename)
    }
    else if (source != null) {
      // Download the file and place it in the working directory
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
}