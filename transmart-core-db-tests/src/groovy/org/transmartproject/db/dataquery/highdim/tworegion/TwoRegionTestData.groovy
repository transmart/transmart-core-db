/*
 * Copyright © 2013-2014 The Hyve B.V.
 *
 * This file is part of transmart-core-db.
 *
 * Transmart-core-db is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License along with
 * transmart-core-db.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.transmartproject.db.dataquery.highdim.tworegion

import org.transmartproject.db.dataquery.highdim.DeGplInfo
import org.transmartproject.db.dataquery.highdim.DeSubjectSampleMapping
import org.transmartproject.db.dataquery.highdim.HighDimTestData
import org.transmartproject.db.i2b2data.PatientDimension

import static org.hamcrest.MatcherAssert.assertThat
import static org.hamcrest.Matchers.is
import static org.hamcrest.Matchers.notNullValue
import static org.transmartproject.db.dataquery.highdim.HighDimTestData.save

/**
 * Created by j.hudecek on 13-3-14.
 */
class TwoRegionTestData {

    public static final String TRIAL_NAME = '2R_SAMP_TRIAL'

    DeGplInfo platform
    List<PatientDimension> patients
    List<DeSubjectSampleMapping> assays

    List<DeTwoRegionEvent> events
    List<DeTwoRegionJunctionEvent> junctionEvents
    List<DeTwoRegionJunction> junctions
    List<DeTwoRegionEventGene> genes

    public TwoRegionTestData(String conceptCode = 'bogus') {
        // Create VCF platform and assays
        platform = new DeGplInfo(
                title: 'Test two region',
                organism: 'Homo Sapiens',
                markerType: 'two_region')
        platform.id = 'BOGUSGPL2R'
        patients = HighDimTestData.createTestPatients(3, -800, TRIAL_NAME)
        assays = HighDimTestData.createTestAssays(patients, -1400, platform, TRIAL_NAME, conceptCode)

        events = []
        junctions = []
        junctionEvents = [];
        genes = [];
        //1st event: deletion chr1 2-10 - chr3 12-18 + chr10 2-10 - chr13 12-18 + chrX 2-10 - chr3 12-18
        //2nd event: deletion chrX 2-10 - chr3 12-18
        //junction without event chrY 2-10 - chr3 12-18
        def event = new DeTwoRegionEvent()
        event.cgaType = "deletion"
        def junction =  new DeTwoRegionJunction()
        junction.downChromosome = "1"
        junction.downPos = 2
        junction.downEnd = 10
        junction.downStrand = "+"
        junction.upChromosome = "3"
        junction.upPos = 12
        junction.upEnd = 18
        junction.upStrand = "-"
        junction.isInFrame = true
        junction.assay = assays[0]
        junctions.add(junction)
        def junctionEvent = new DeTwoRegionJunctionEvent()
        junctionEvent.event = event
        junctionEvent.junction = junction
        junctionEvent.pairsSpan = 10
        junctionEvents.add(junctionEvent)
        junction.eventjunctions = [junctionEvent]

        junction =  new DeTwoRegionJunction()
        junction.downChromosome = "10"
        junction.downPos = 2
        junction.downEnd = 10
        junction.downStrand = "+"
        junction.upChromosome = "13"
        junction.upPos = 12
        junction.upEnd = 18
        junction.upStrand = "-"
        junction.isInFrame = true
        junction.assay = assays[0]
        junctions.add(junction)
        junctionEvent = new DeTwoRegionJunctionEvent()
        junction.eventjunctions = [junctionEvent]
        junctionEvent.event = event
        junctionEvent.junction = junction
        junctionEvent.pairsSpan = 10
        junctionEvents.add(junctionEvent)
        def event1 = event;
        events.add(event)

        def gene = new DeTwoRegionEventGene(
                geneId: 'TP53',
                effect: "fusion"
        )

        event = new DeTwoRegionEvent()
        event.soapClass = "translocation"
        event.genes = [gene]
        gene.event = event;
        genes.add(gene)

        def junction2 =  new DeTwoRegionJunction()
        junction2.downChromosome = "X"
        junction2.downPos = 2
        junction2.downEnd = 10
        junction2.downStrand = "+"
        junction2.upChromosome = "3"
        junction2.upPos = 12
        junction2.upEnd = 18
        junction2.upStrand = "-"
        junction2.isInFrame = true
        junction2.assay = assays[1]
        junctions.add(junction2)
        junctionEvent = new DeTwoRegionJunctionEvent()
        junction2.eventjunctions = [junctionEvent]
        junctionEvent.event = event
        junctionEvent.junction = junction2
        junctionEvent.pairsSpan = 10
        junctionEvents.add(junctionEvent)
        junctionEvent = new DeTwoRegionJunctionEvent()
        junctionEvent.event = event1
        junctionEvent.junction = junction2
        junctionEvent.pairsSpan = 11
        junctionEvents.add(junctionEvent)
        events.add(event)

        junction =  new DeTwoRegionJunction()
        junction.downChromosome = "Y"
        junction.downPos = 2
        junction.downEnd = 10
        junction.downStrand = "+"
        junction.upChromosome = "3"
        junction.upPos = 12
        junction.upEnd = 18
        junction.upStrand = "-"
        junction.isInFrame = true
        junction.assay = assays[1]
        junctions.add(junction)

    }

    void saveAll() {
        assertThat platform.save(), is(notNullValue(DeGplInfo))
        save patients
        save assays
        save(events)
        save(genes)
        save(junctions)
        save(junctionEvents)
    }
}
