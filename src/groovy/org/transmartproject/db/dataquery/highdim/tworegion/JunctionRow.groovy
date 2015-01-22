package org.transmartproject.db.dataquery.highdim.tworegion

import org.transmartproject.core.dataquery.highdim.tworegion.JunctionValues
import org.transmartproject.db.dataquery.highdim.AbstractDataRow

/**
 * Created by j.hudecek on 4-12-2014.
 */
class JunctionRow   extends AbstractDataRow implements JunctionValues {
    int id

    long upEnd
    String upChromosome
    long upPos
    char upStrand
    long downEnd
    String downChromosome
    long downPos
    char downStrand
    boolean isInFrame

    List<DeTwoRegionJunctionEvent> eventjunctions

    @Override
    String getLabel() {
        return id
    }

    public JunctionRow() {}
    public JunctionRow(DeTwoRegionJunction e) {
        id = e.id
        upEnd = e.upEnd
        upChromosome = e.upChromosome
        upPos = e.upPos
        upStrand = e.upStrand
        downEnd = e.downEnd
        downChromosome = e.downChromosome
        downPos = e.downPos
        downStrand = e.downStrand
        isInFrame = e.isInFrame
    }
}
