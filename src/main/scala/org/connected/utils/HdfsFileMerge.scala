package org.connected.utils

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.CompressionCodecFactory

class HdfsFileMerge(defaultBufferSize: Int = 10240, defaultConfig: Option[Configuration] = None)
{

  private def toAbsolutePath(path: String) = new File(path).getAbsolutePath

  val config: Configuration = defaultConfig.getOrElse(
    {
      config.addResource(new Path("file:///etc/hadoop/conf/core-site.xml"))
      config.addResource(new Path("file:///etc/hadoop/conf/hdfs-site.xml"))
      config
    }
  )

  val fs: FileSystem = FileSystem.get(config)

  def merge(inputPathGlob: String, outputFilePath: String): Unit =
  {
    HdfsUtils(fs).globFiles(inputPathGlob) match
    {
      case Nil =>
        throw new RuntimeException(s"No files found for glob pattern " + inputPathGlob)
      case lst: List[Path] =>
      {
        val codec = new CompressionCodecFactory(config).getCodec(lst.head)

        val outputPath =
          if (outputFilePath.endsWith(codec.getDefaultExtension))
            new Path(outputFilePath)
          else
            new Path(outputFilePath + codec.getDefaultExtension)

        if (fs.exists(outputPath))
          throw new RuntimeException(s"Output $outputFilePath path already exists.")

        val outputStream = codec.createOutputStream(fs.create(outputPath))
        val buffer = new Array[Byte](defaultBufferSize)
        val totalFiles = lst.size

        lst.zipWithIndex.foreach
        {
          case (path, idx) =>
          {
            println(s"Merging file (${idx + 1}/$totalFiles): $path")

            val inputStream = codec.createInputStream(fs.open(path))
            var length = 0
            while (
            {
              length = inputStream.read(buffer, 0, defaultBufferSize)
              length > 0
            })
              outputStream.write(buffer, 0, length)
            inputStream.close()
          }
        }
        outputStream.flush()
        outputStream.close()
      }
    }
  }
}
