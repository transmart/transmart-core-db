package org.transmartproject.db.dataquery.highdim.tworegion

import groovy.transform.EqualsAndHashCode
import org.transmartproject.db.dataquery.highdim.DeSubjectSampleMapping


/**
 * Created by j.hudecek on 4-12-2014.
 */
@EqualsAndHashCode()
class DeTwoRegionEvent implements Serializable {

    String cgaType;
    String soapClass;
    long id;

    static hasMany = [ genes: DeTwoRegionEventGene ]

    static constraints = {
        cgaType(nullable: true)
        soapClass(nullable: true)
        genes(nullable: true)
    }

    static mapping = {
        table   schema:    'deapp', name: 'de_two_region_event'
        version false
        id      column: 'two_region_event_id'
        genes fetch: 'join'

        cgaType   column: 'cga_type'
        soapClass column: 'soap_class'

    }
}
