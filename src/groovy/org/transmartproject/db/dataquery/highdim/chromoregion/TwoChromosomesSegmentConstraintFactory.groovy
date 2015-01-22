package org.transmartproject.db.dataquery.highdim.chromoregion

import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component
import org.transmartproject.core.dataquery.highdim.dataconstraints.DataConstraint
import org.transmartproject.db.dataquery.highdim.dataconstraints.DisjunctionDataConstraint
import org.transmartproject.db.dataquery.highdim.parameterproducers.ProducerFor

/**
 * Created by j.hudecek on 5-12-2014.
 */
@Component
@Scope("prototype")
class TwoChromosomesSegmentConstraintFactory extends ChromosomeSegmentConstraintFactory {
    String segmentTwoPrefix = 'region.'
    String segmentTwoChromosomeColumn = 'chromosome'
    String segmentTwoStartColumn      = 'start'
    String segmentTwoEndColumn        = 'end'

    @ProducerFor(DataConstraint.DISJUNCTION_CONSTRAINT)
    DisjunctionDataConstraint createTwoRegionsConstraint(Map<String, Object> params) {

        def chr = createChromosomeSegmentConstraint(params)
        def chr2 = createChromosomeSegmentConstraint(params)
        chr2.regionPrefix = segmentTwoPrefix
        chr2.regionChromosomeColumn = segmentTwoChromosomeColumn
        chr2.regionStartColumn = segmentTwoStartColumn
        chr2.regionEndColumn = segmentTwoEndColumn

        return new DisjunctionDataConstraint(constraints:[chr, chr2])
    }
}
