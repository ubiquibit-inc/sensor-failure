object BuoyDataFiles extends Enumeration {

  protected case class Val(extension: String) extends super.Val

  implicit def valueToBuoyDataFile(x: Value): Val = x.asInstanceOf[Val]

  val Adcp = Val("adcp")
  val Adcp2 = Val("adcp2")
  val Cwind = Val("cwind")
  val Dart = Val("dart")
  val DataSpec = Val("data_spec")
  val Drift = Val("drift")
  val Hkp = Val("hkp")
  val Ocean = Val("ocean")
  val Rain = Val("rain")
  val Spec = Val("spec")
  val Srad = Val("srad")
  val Supl = Val("supl")
  val Swdir = Val("swdir")
  val Swr1 = Val("swr1")
  val Swr2 = Val("swr2")
  val Text = Val("txt")

}
