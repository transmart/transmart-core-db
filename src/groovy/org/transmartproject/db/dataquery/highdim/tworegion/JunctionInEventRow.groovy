package org.transmartproject.db.dataquery.highdim.tworegion

import org.transmartproject.core.dataquery.highdim.tworegion.JunctionInEventValues
import org.transmartproject.core.dataquery.highdim.tworegion.JunctionValues
import org.transmartproject.db.dataquery.highdim.AbstractDataRow

/**
 * Created by j.hudecek on 4-12-2014.
 */
class JunctionInEventRow  extends AbstractDataRow implements JunctionInEventValues {
    int id
    int readsSpan
    int readsJunction
    int pairsSpan
    int pairsJunction
    int pairsEnd
    int pairsCounter
    int eventId
    double baseFreq
    JunctionValues junction
    EventRow event

    @Override
    String getLabel() {
        return id
    }

    public JunctionInEventRow() {}

    public JunctionInEventRow(DeTwoRegionJunctionEvent e) {
        id = e.id
        readsSpan = e.readsSpan
        readsJunction = e.readsJunction
        pairsSpan = e.pairsSpan
        pairsJunction = e.pairsJunction
        pairsEnd = e.pairsEnd
        pairsCounter = e.pairsCounter
        baseFreq = e.baseFreq
        eventId = e.eventId
    }
}
