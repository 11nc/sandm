package com.hotpotato.util

object Utils {
  def box[T](t: T): Option[T] = if (t != null) Some(t) else None
}