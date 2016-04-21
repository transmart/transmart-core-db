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

package org.transmartproject.db.dataquery.highdim

import com.google.common.collect.Lists
import org.hibernate.SessionFactory
import org.hibernate.criterion.Property
import org.junit.Before
import org.junit.Test
import org.springframework.beans.factory.annotation.Autowired
import org.transmartproject.core.dataquery.assay.Assay
import org.transmartproject.db.biomarker.BioMarkerCoreDb
import org.transmartproject.db.dataquery.highdim.assayconstraints.DefaultTrialNameCriteriaConstraint
import org.transmartproject.db.dataquery.highdim.mrna.DeMrnaAnnotationCoreDb
import org.transmartproject.db.dataquery.highdim.mrna.MrnaModule
import org.transmartproject.db.dataquery.highdim.mrna.MrnaTestData

import static org.hamcrest.MatcherAssert.assertThat
import static org.hamcrest.Matchers.*
import static org.transmartproject.db.test.Matchers.hasSameInterfaceProperties

/**
 * Created by glopes on 11/23/13.
 */
class AssayQueryTests {

    MrnaTestData mrnaTestData = new MrnaTestData()
    AssayTestData testData = AssayTestData.instance

    @Autowired
    HighDimensionResourceService highdim

    SessionFactory sessionFactory

    @Before
    void setUp() {
        testData.saveAll()
        mrnaTestData.saveAll()
        sessionFactory.getCurrentSession().flush()
    }

    @Test
    void testPrepareCriteriaWithConstraints() {
        List results = new AssayQuery([new DefaultTrialNameCriteriaConstraint(trialName: 'SAMPLE_TRIAL_2')]).list()

        assertThat results, containsInAnyOrder(
                testData.assays[6],
                testData.assays[7],
                testData.assays[8])
    }

    @Test
    void testRetrieveAssays() {
        List results = new AssayQuery([new DefaultTrialNameCriteriaConstraint(trialName: 'SAMPLE_TRIAL_2')]).list()

        assertThat results, allOf(
                everyItem(isA(Assay)),
                contains( /* order is asc */
                        hasSameInterfaceProperties(Assay, testData.assays[8]),
                        hasSameInterfaceProperties(Assay, testData.assays[7]),
                        hasSameInterfaceProperties(Assay, testData.assays[6]),
                )
        )
    }


    @Test
    void testMrnaBiomarkers() {
        println("************************************************* starting test 2 " +
                "********************************************")

        HighDimensionDataTypeResourceImpl mrna = highdim.getSubResourceForType('mrna')

        println("************************************************* begin " +
                "********************************************")


//        def result = executeQuery(mrna.openSession(),
//        //def results = session.createQuery( //
//                "select cast(probe.geneId as string) from DeMrnaAnnotationCoreDb as probe where probe.gplId in " +
//                ":platforms"
//        , [platforms: ['BOGUSGPL570']])



        def dbresult = mrna.retrieveBioMarkers(['BOGUSGPL570'])

        List<String> results = Lists.newArrayList(dbresult)

        println("********************************* printing *********************************")
        println("***************** result: $results, ${results.size() ? results[0]?.class : null} ************")
        println("********************** done **********************")


    }

    @Test
    void testHql() {

        println("************************************************* starting test " +
                "********************************************")

        def d = org.hibernate.criterion.DetachedCriteria.forClass(DeMrnaAnnotationCoreDb).setProjection(Property
                .forName("geneId"))

//        //def res = DeMrnaAnnotationCoreDb.findAll(
//        def q = sessionFactory.currentSession.createQuery(
////                "from DeMrnaAnnotationCoreDb probe where cast(probe.geneId as string) in ('hello')"
//                "select cast(probe.geneId as string) from DeMrnaAnnotationCoreDb as probe "
//        )

        def d2 = org.hibernate.criterion.DetachedCriteria.forClass(BioMarkerCoreDb).setProjection(
                Property.forName("id")).add(Property.forName("externalId").in(d))

        def res = sessionFactory.currentSession.createCriteria(BioMarkerCoreDb).add(Property.forName("id").in
                (d2)).list()


//        def res = sessionFactory.currentSession.createQuery(
//                "from BioMarkerCoreDb biomarker where biomarker.externalId in :ids "
//        ).setParameterList("ids", q).list()

        println("********************************* results **********************************")

        println(res)
        println(res ? res[0].class : null)

        println("***done***")

    }

    @Test
    void testPlatforms() {

        println("************************************************* starting test " +
                "********************************************")

        def constraints = [new DefaultTrialNameCriteriaConstraint(trialName: 'SAMPLE_TRIAL_2')]

        MrnaModule

        //todo: make this return HighDimensionDataTypeResourceImpl, Collection<DeSubjectSampleMapping>
        Map<HighDimensionDataTypeResourceImpl, Collection<DeSubjectSampleMapping>> platformMap =
                highdim.getSubResourcesAssayMultiMap(constraints)

        List results

        for (Map.Entry<HighDimensionDataTypeResourceImpl, Collection<DeSubjectSampleMapping>> entry : platformMap.entrySet()) {
            //todo: merge all platform results together
            results = highdim.biomarkersForPlatform(entry.key, entry.value)
            break
        }


        println("********************************* results **********************************")
//        def query = new AssayQuery(constraints)
//        query.projections {
//            distinct 'platform.id'
//        }
//
//        List results = query.list()

        println("********************************* printing *********************************")
        println("***************** result: $results, ${results.size() ? results[0]?.class : null} ************")
        println("********************** done **********************")

    }
}
