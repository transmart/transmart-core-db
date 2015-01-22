package org.transmartproject.db.dataquery.highdim.tworegion

import groovy.transform.EqualsAndHashCode

/**
 * Created by j.hudecek on 7-1-2015.
 */
@EqualsAndHashCode()
class DeTwoRegionEventGene  implements Serializable {

    String geneId
    String effect
    long id
    DeTwoRegionEvent event

    static constraints = {
        geneId(nullable: true)
        effect(nullable: true)
    }

    static mapping = {
        table   schema:    'deapp', name: 'de_two_region_event_gene'
        version false
        id      column: 'two_region_event_gene_id'

        geneId   column: 'gene_id'
        effect column: 'effect'
        /* references */
        event    column: 'event_id'
        event fetch: 'join'
    }
}
