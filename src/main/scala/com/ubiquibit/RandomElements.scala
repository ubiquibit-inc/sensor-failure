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

trait RandomElements extends Serializable{

  private val rand = scala.util.Random

  def randomElemOf[T](seq: Seq[T]): Option[T] = {
    if (seq.isEmpty) None
    else seq lift rand.nextInt(seq.length)
  }

}
