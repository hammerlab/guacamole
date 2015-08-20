package org.hammerlab.guacamole.reference

import org.hammerlab.guacamole.Bases
import org.hammerlab.guacamole.util.{ TestUtil, GuacFunSuite }

class ReferenceUtilsSuite extends GuacFunSuite {

  test("compute base ratios") {
    val referenceSequence = Bases.stringToBases("TCGATCGAAATT").toArray
    val baseRatios = ReferenceUtils.getBaseRatios(referenceSequence)

    TestUtil.assertAlmostEqual(baseRatios(Bases.A), 1 / 3f)
    TestUtil.assertAlmostEqual(baseRatios(Bases.T), 1 / 3f)
    TestUtil.assertAlmostEqual(baseRatios(Bases.G), 1 / 6f)
    TestUtil.assertAlmostEqual(baseRatios(Bases.C), 1 / 6f)

  }

  test("compute gc ratio") {
    val referenceSequence = Bases.stringToBases("TCGATCGAAATT").toArray
    val gcRatio = ReferenceUtils.getGCRatio(referenceSequence)

    TestUtil.assertAlmostEqual(gcRatio, 1 / 3f)
  }

}
