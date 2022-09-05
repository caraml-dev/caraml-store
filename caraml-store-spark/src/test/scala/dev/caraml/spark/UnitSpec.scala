package dev.caraml.spark

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest._
import matchers._

abstract class UnitSpec
    extends AnyFlatSpec
    with should.Matchers
    with OptionValues
    with Inside
    with Inspectors
