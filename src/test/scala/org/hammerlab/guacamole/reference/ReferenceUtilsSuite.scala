package org.hammerlab.guacamole.reference

import org.hammerlab.guacamole.Bases
import org.hammerlab.guacamole.util.{ TestUtil, GuacFunSuite }

class ReferenceUtilsSuite extends GuacFunSuite {

  test("compute base ratios") {
    val referenceSequence = Bases.stringToBases("TCGATCGAAATT").toArray
    val baseFractions = ReferenceUtils.getBaseFraction(referenceSequence)

    TestUtil.assertAlmostEqual(baseFractions(Bases.A), 1 / 3f)
    TestUtil.assertAlmostEqual(baseFractions(Bases.T), 1 / 3f)
    TestUtil.assertAlmostEqual(baseFractions(Bases.G), 1 / 6f)
    TestUtil.assertAlmostEqual(baseFractions(Bases.C), 1 / 6f)

  }

  test("compute gc ratio") {
    val referenceSequence = Bases.stringToBases("TCGATCGAAATT").toArray
    val gcFraction = ReferenceUtils.getGCFraction(referenceSequence)

    TestUtil.assertAlmostEqual(gcFraction, 1 / 3f)
  }

}
