// Copyright (C) 2015 Sam Halliday
// License: http://www.apache.org/licenses/LICENSE-2.0
package s4m.exercise1

import org.scalatest._
import shapeless._

import s4m.smbd.api._
import s4m.smbd.impl._
import s4m.smbd.syntax._

package api {
  sealed trait Receptacle
  case class Teapot(a: String, b: Int, c: Boolean) extends Receptacle
  case class Bottle(foo: String, bar: Double) extends Receptacle
  case class Glass() extends Receptacle

  class Beans
  case class Coffee(z: Beans)
}

package object formats {
  import s4m.exercise1.api._

  implicit val TeapotF: BigDataFormat[Teapot] = cachedImplicit

  // if you get compiler errors for ReceptacleF, uncomment these to
  // help find which implicit is not being found:
  //implicit val BottleF: BigDataFormat[Bottle] = cachedImplicit
  //implicit val GlassF: BigDataFormat[Glass.type] = cachedImplicit

  implicit val ReceptacleF: BigDataFormat[Receptacle] = cachedImplicit
}

package app {
  import s4m.exercise1.api._
  import s4m.exercise1.formats._

  class StringyMapBigDataSpec extends FlatSpec with Matchers {
    val teapot = Teapot("foo", 42, true)
    val bottle = Bottle("hello", 1.23)
    val glass = Glass()

    "StringyMapBigData" should "marshall a case class" in {
      val stringyMap = teapot.toProperties
      stringyMap.get("a") shouldBe "foo"
      stringyMap.get("b") shouldBe 42
      stringyMap.get("c") shouldBe true
    }

    it should "unmarshall to a case class" in {
      val stringyMap = new StringyMap()
      stringyMap.put("a", "foo")
      stringyMap.put("b", new java.lang.Integer(42))
      stringyMap.put("c", new java.lang.Boolean(true))
      stringyMap.put("foo", "hello")
      stringyMap.put("bar", new java.lang.Double(1.23))

      stringyMap.as[Glass] shouldBe glass
      stringyMap.as[Teapot] shouldBe teapot
      stringyMap.as[Bottle] shouldBe bottle
    }

    it should "correctly fail to unmarshall to a case class" in {
      val stringyMap = new StringyMap()
      stringyMap.put("z", "foo")

      TeapotF.fromProperties(stringyMap) shouldBe Left("No value in map for a")
    }

    it should "round trip sealed traits" in {
      (teapot: Receptacle).toProperties.as[Receptacle] shouldBe teapot
      (bottle: Receptacle).toProperties.as[Receptacle] shouldBe bottle
      (glass: Receptacle).toProperties.as[Receptacle] shouldBe glass
    }

    // uncomment this when the above succed
    it should "fail to marshall a case class with an unsupported value" in {
      // shapeless.test.illTyped(
      //   """implicitly[BigDataFormat[Coffee]]""",
      //   ".*could not find implicit value for evidence parameter of type.*"
      // )
    }

  }
}
