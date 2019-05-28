package org.connected.utils

import scopt.OptionParser

object SparkHdfsFileMerge
{
  case class SparkFMConfig(database :String="", sourceTable:String="", partition:Option[String] = None, targetTable:String="")
}
