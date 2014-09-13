package com.yonatankar.compressor.core.structres

import scala.collection.TraversableView.NoBuilder
import scala.collection._
import scala.collection.generic.CanBuildFrom

class LazySeq[+A](orig: => Iterator[A]) extends SeqLike[A, LazySeq[A]] with Seq[A] with Serializable {
  lazy val length = orig.size

  override lazy val size = length

  def iterator = orig

  def apply(idx: Int) = {
    orig.drop(idx).next()
  }

  override def toString() = take(10).mkString("LazySeq(", ",", ", ...)")

  override def filter(predicate: A => Boolean) = new LazySeq(orig filter predicate)

  override protected[this] def newBuilder: mutable.Builder[A, LazySeq[A]] = ???

  override def flatMap[B, That](f: (A) => GenTraversableOnce[B])(implicit bf: CanBuildFrom[LazySeq[A], B, That]): That = new LazySeq(orig flatMap f).asInstanceOf[That]

  override def map[B, That](f: (A) => B)(implicit bf: CanBuildFrom[LazySeq[A], B, That]): That = new LazySeq(orig map f).asInstanceOf[That]

  override def take(n: Int): LazySeq[A] = new LazySeq(orig take n)

  override def drop(n: Int): LazySeq[A] = new LazySeq(orig drop n)
}

object LazySeq {
  def apply[A](orig: => Iterator[A]) = new LazySeq(orig)

  type Coll = LazySeq[_]

  implicit def canBuildFrom[A]: CanBuildFrom[Coll, A, LazySeq[A]] =
    new CanBuildFrom[Coll, A, LazySeq[A]] {
      def apply(from: Coll) = new NoBuilder

      def apply() = new NoBuilder
    }

}
