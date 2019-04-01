/*
 * Copyright (c) 2019.
 *
 * This work, (c) by jason@ubiquibit.com
 *
 * This work is licensed under a
 * Creative Commons Attribution-ShareAlike 4.0 International License.
 *
 * You should have received a copy of the license along with this
 * work.  If not, see <http://creativecommons.org/licenses/by-sa/4.0/>.
 *
 */

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
