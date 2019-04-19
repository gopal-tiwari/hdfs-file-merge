package org.connected.utils

import org.apache.hadoop.fs.{FileSystem, Path}

case class HdfsUtils(fs:FileSystem) {

  def globFiles(globPattern: String) : List[Path] =
    fs.globStatus(new Path(globPattern)).filter(_.isFile).toList.map(_.getPath)

}
