package org.transmartproject.db.dataquery.highdim.tworegion

import org.transmartproject.core.dataquery.highdim.tworegion.EventValues
import org.transmartproject.core.dataquery.highdim.tworegion.JunctionInEventValues
import org.transmartproject.db.dataquery.highdim.AbstractDataRow
import org.transmartproject.core.dataquery.highdim.tworegion.GenesInEventValues

/**
 * Created by j.hudecek on 4-12-2014.
 */
class EventRow extends AbstractDataRow implements EventValues {
    long id
    String cgaType
    String soapClass

    JunctionInEventValues[] junctions
    GenesInEventValues[] genes

    @Override
    String getLabel() {
        return id
    }

    public EventRow() {}
    public EventRow(DeTwoRegionEvent e) {
        id = e.id
        soapClass = e.soapClass
        cgaType = e.cgaType
    }
}
