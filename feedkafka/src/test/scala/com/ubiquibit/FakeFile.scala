package com.ubiquibit

import java.io.File

class FakeFile(str: String) extends File(str) {
  val strCpy = str

  override def getAbsolutePath: String = strCpy

  override def canRead: Boolean = true

  override def isDirectory: Boolean = false

  override def isFile: Boolean = true

  override def exists(): Boolean = true
}
