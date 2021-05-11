package com.accedian.npav

import java.util

import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}

class CustomLoggingPlugin extends SparkPlugin {

  override def executorPlugin(): ExecutorPlugin = new CustomExecutorPlugin

  override def driverPlugin(): DriverPlugin = null
}

class CustomExecutorPlugin extends ExecutorPlugin  {

  override def init(ctx: PluginContext, extraConf: util.Map[String, String]): Unit = {
    println("Startup.....")
  }

  override def shutdown(): Unit = {
    println(s"Shutdown:")
  }
}
