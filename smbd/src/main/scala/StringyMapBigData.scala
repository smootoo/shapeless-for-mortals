// Copyright (C) 2015 Sam Halliday
// License: http://www.apache.org/licenses/LICENSE-2.0
/**
 * TypeClass (api/impl/syntax) for marshalling objects into
 * `java.util.HashMap<String,Object>` (yay, big data!).
 */
package s4m.smbd

import shapeless._, shapeless.labelled._

/**
 * This exercise involves writing tests, only a skeleton is provided.
 *
 * - Exercise 1.1: derive =BigDataFormat= for sealed traits.
 * - Exercise 1.2: define identity constraints using singleton types.
 */
package object api {
  type StringyMap = java.util.HashMap[String, AnyRef]
  type BigResult[T] = Either[String, T] // aggregating errors doesn't add much
}

package api {
  trait BigDataFormat[T] {
    def label: String
    def toProperties(t: T): StringyMap
    def fromProperties(m: StringyMap): BigResult[T]
  }

  trait SPrimitive[V] {
    def toValue(v: V): AnyRef
    def fromValue(v: AnyRef): V
  }

  // EXERCISE 1.2
  trait BigDataFormatId[T, V] {
    def key: String
    def value(t: T): V
  }
}

package object impl {
  import api._

  // EXERCISE 1.1 goes here

  implicit object StringSPrimitive extends SPrimitive[String] {
    def toValue(v: String): AnyRef = v
    def fromValue(v: AnyRef): String = v.asInstanceOf[String]
  }

  implicit object IntSPrimitive extends SPrimitive[Int] {
    def toValue(v: Int): AnyRef = Integer.valueOf(v)
    def fromValue(v: AnyRef): Int = v.asInstanceOf[Integer].toInt
  }

  implicit object DoubleSPrimitive extends SPrimitive[Double] {
    def toValue(v: Double): AnyRef = java.lang.Double.valueOf(v)
    def fromValue(v: AnyRef): Double = v.asInstanceOf[java.lang.Double].toDouble
  }

  implicit object BooleanSPrimitive extends SPrimitive[Boolean] {
    def toValue(v: Boolean): AnyRef = java.lang.Boolean.valueOf(v)
    def fromValue(v: AnyRef): Boolean = v.asInstanceOf[java.lang.Boolean].booleanValue()
  }


  implicit object hNilBigDataFormat extends BigDataFormat[HNil] {
    def label: String = "HNil" // Not used
    def toProperties(t: HNil): StringyMap = new StringyMap
    def fromProperties(m: StringyMap): BigResult[HNil] = Right(HNil)
  }

  implicit def hListBigDataFormat[Key <: Symbol, Value, Remaining <: HList](
     implicit
     key: Witness.Aux[Key],
     bdfHead: SPrimitive[Value],
     lazyBDFTail: Lazy[BigDataFormat[Remaining]]
   ): BigDataFormat[FieldType[Key, Value] :: Remaining] =
    new BigDataFormat[FieldType[Key, Value] :: Remaining] {
      def label: String = "HeadNTail" // Not used
      def toProperties(t: FieldType[Key, Value] :: Remaining): StringyMap = {
        val headValue = bdfHead.toValue(t.head)
        val stringyMap = lazyBDFTail.value.toProperties(t.tail)
        if (headValue != null) stringyMap.put(key.value.name, headValue)
        stringyMap
      }
      def fromProperties(stringyMap: StringyMap): BigResult[FieldType[Key, Value] :: Remaining] = {
        val head = Option(stringyMap.get(key.value.name)).
          map(v => Right(bdfHead.fromValue(v))).
          getOrElse(Left(s"No value in map for ${key.value.name}"))
        val tail = lazyBDFTail.value.fromProperties(stringyMap)
        for {
          h <- head.right
          t <- tail.right
        } yield field[Key](h) :: t
      }
    }

  implicit object CNilBigDataFormat extends BigDataFormat[CNil] {
    // None of this should be needed
    def label: String = ???
    def toProperties(t: CNil): StringyMap = ???
    def fromProperties(m: StringyMap): BigResult[CNil] = ???
  }

  implicit def coproductBigDataFormat[Name <: Symbol, Head, Tail <: Coproduct](
      implicit
      key: Witness.Aux[Name],
      lazyBFDHead: Lazy[BigDataFormat[Head]],
      lazyBDFTail: Lazy[BigDataFormat[Tail]]
    ): BigDataFormat[FieldType[Name, Head] :+: Tail] =
    new BigDataFormat[FieldType[Name, Head] :+: Tail] {
      def label: String = key.value.name
      def fromProperties(stringyMap: StringyMap): BigResult[FieldType[Name, Head] :+: Tail] =
        if (stringyMap.get("type") == label)
          lazyBFDHead.value.fromProperties(stringyMap).right.map(h => Inl(field[Name](h)))
        else
          lazyBDFTail.value.fromProperties(stringyMap).right.map(t => Inr(t))

      def toProperties(lr: FieldType[Name, Head] :+: Tail) = lr match {
        case Inl(found) => {
          val stringyMap = lazyBFDHead.value.toProperties(found)
          stringyMap.put("type", key.value.name)
          stringyMap
        }
        case Inr(tail) =>
          lazyBDFTail.value.toProperties(tail)
      }
    }

  implicit def familyBigDataFormat[T, Repr](
      implicit
      gen: LabelledGeneric.Aux[T, Repr],
      lazyBDF: Lazy[BigDataFormat[Repr]],
      tpe: Typeable[T]
    ): BigDataFormat[T] = new BigDataFormat[T] {
      def label: String = tpe.describe
      def fromProperties(stringyMap: StringyMap): BigResult[T] =
        lazyBDF.value.fromProperties(stringyMap).right.map(hList => gen.from(hList))
      def toProperties(t: T): StringyMap = lazyBDF.value.toProperties(gen.to(t))
  }
}

package impl {
  import api._

  // EXERCISE 1.2 goes here
}

package object syntax {
  import api._

  implicit class RichBigResult[R](val e: BigResult[R]) extends AnyVal {
    def getOrThrowError: R = e match {
      case Left(error) => throw new IllegalArgumentException(error.mkString(","))
      case Right(r) => r
    }
  }

  /** Syntactic helper for serialisables. */
  implicit class RichBigDataFormat[T](val t: T) extends AnyVal {
    def label(implicit s: BigDataFormat[T]): String = s.label
    def toProperties(implicit s: BigDataFormat[T]): StringyMap = s.toProperties(t)
    def idKey[P](implicit lens: Lens[T, P]): String = ???
    def idValue[P](implicit lens: Lens[T, P]): P = lens.get(t)
  }

  implicit class RichProperties(val props: StringyMap) extends AnyVal {
    def as[T](implicit s: BigDataFormat[T]): T = s.fromProperties(props).getOrThrowError
  }
}
