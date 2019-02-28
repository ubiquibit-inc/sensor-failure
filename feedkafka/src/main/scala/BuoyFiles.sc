import java.io.File

val buoyDataDirectory = "/Users/jason/scratch/sensor-failure/data/www.ndbc.noaa.gov/data/realtime2/"

def filenames(dir: String, extension: String): List[String] = {
  val file = new File(dir)
  file.listFiles
    .filter(_.isFile)
    .filter(_.getName.endsWith(extension))
    .map(_.getName)
    .toList
}

def countFilesByExtension(): Unit = {
  BuoyDataFiles.values.foreach { f =>
    val list = filenames(buoyDataDirectory, f.extension)
    println(s"# $f files " + list.length)
  }
}

def countBuoysById(): Unit = {

}

countFilesByExtension()
