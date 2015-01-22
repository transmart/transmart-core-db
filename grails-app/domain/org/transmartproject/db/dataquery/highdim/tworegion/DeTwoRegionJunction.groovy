package org.transmartproject.db.dataquery.highdim.tworegion

import groovy.transform.EqualsAndHashCode
import org.transmartproject.db.dataquery.highdim.DeSubjectSampleMapping

/**
 * Created by j.hudecek on 4-12-2014.
 */
@EqualsAndHashCode()
class DeTwoRegionJunction implements Serializable {

    long upEnd;
    String upChromosome;
    long upPos;
    char upStrand;
    long downEnd;
    String downChromosome;
    long downPos;
    char downStrand;
    boolean isInFrame;
    long id;
    DeSubjectSampleMapping assay

    static hasMany = [ eventjunctions: DeTwoRegionJunctionEvent]

    static constraints = {
        eventjunctions(nullable: true)
    }

    static mapping = {
        table   schema:    'deapp', name: 'de_two_region_junction'
        version false
        id      column: 'two_region_junction_id'
        //eventjunctions lazy:false

        upEnd            column: 'up_end'
        upChromosome     column: 'up_chr'
        upPos            column: 'up_pos'
        upStrand         column: 'up_strand'
        downEnd          column: 'down_end'
        downChromosome   column: 'down_chr'
        downPos          column: 'down_pos'
        downStrand       column: 'down_strand'
        isInFrame        column: 'is_in_frame'

        /* references */
        assay    column: 'assay_id'

        version false
    }
}
