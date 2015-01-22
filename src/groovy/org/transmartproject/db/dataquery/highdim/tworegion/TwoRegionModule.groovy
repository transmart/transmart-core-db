package org.transmartproject.db.dataquery.highdim.tworegion

import grails.orm.HibernateCriteriaBuilder
import org.apache.commons.collections.set.ListOrderedSet
import org.hibernate.ScrollableResults
import org.hibernate.engine.SessionImplementor
import org.hibernate.transform.Transformers
import org.springframework.beans.factory.annotation.Autowired
import org.transmartproject.core.dataquery.TabularResult
import org.transmartproject.core.dataquery.highdim.AssayColumn
import org.transmartproject.core.dataquery.highdim.projections.Projection
import org.transmartproject.db.dataquery.MultiTabularResult
import org.transmartproject.db.dataquery.highdim.AbstractHighDimensionDataTypeModule
import org.transmartproject.db.dataquery.highdim.chromoregion.TwoChromosomesSegmentConstraintFactory
import org.transmartproject.db.dataquery.highdim.parameterproducers.AllDataProjectionFactory
import org.transmartproject.db.dataquery.highdim.parameterproducers.DataRetrievalParameterFactory

/**
 * Created by j.hudecek on 4-12-2014.
 */
class TwoRegionModule extends AbstractHighDimensionDataTypeModule {

    final String name = 'two_region'

    final String description = "Two  Variant data"

    final List<String> platformMarkerTypes = ['two_region']

    final Map<String, Class> dataProperties = typesMap(DeTwoRegionEvent,
            ['cgaType', 'soapClass'])

    final Map<String, Class> rowProperties = typesMap(EventRow,
            [])

    @Autowired
    DataRetrievalParameterFactory standardAssayConstraintFactory

    @Autowired
    DataRetrievalParameterFactory standardDataConstraintFactory

    @Autowired
    TwoChromosomesSegmentConstraintFactory chromosomeSegmentConstraintFactory

    @Override
    protected List<DataRetrievalParameterFactory> createAssayConstraintFactories() {
        [standardAssayConstraintFactory]
    }

    @Override
    protected List<DataRetrievalParameterFactory> createDataConstraintFactories() {
        chromosomeSegmentConstraintFactory.segmentPrefix = ''
        chromosomeSegmentConstraintFactory.segmentChromosomeColumn = 'upChromosome'
        chromosomeSegmentConstraintFactory.segmentStartColumn = 'upPos'
        chromosomeSegmentConstraintFactory.segmentEndColumn = 'upEnd'
        chromosomeSegmentConstraintFactory.segmentTwoPrefix = ''
        chromosomeSegmentConstraintFactory.segmentTwoChromosomeColumn = 'downChromosome'
        chromosomeSegmentConstraintFactory.segmentTwoStartColumn = 'downPos'
        chromosomeSegmentConstraintFactory.segmentTwoEndColumn = 'downEnd'
        [
                chromosomeSegmentConstraintFactory
        ]
    }

    @Override
    protected List<DataRetrievalParameterFactory> createProjectionFactories() {
        [
                new AllDataProjectionFactory(dataProperties, rowProperties)
        ]
    }

    @Override
    HibernateCriteriaBuilder prepareDataQuery(Projection projection, SessionImplementor session) {
        HibernateCriteriaBuilder criteriaBuilder =
                createCriteriaBuilder(DeTwoRegionJunction, 'junction', session)

        criteriaBuilder.with {
            createAlias 'eventjunctions', 'eventjunctions', org.hibernate.criterion.CriteriaSpecification.LEFT_JOIN
            createAlias 'eventjunctions.event', 'event', org.hibernate.criterion.CriteriaSpecification.LEFT_JOIN
            createAlias 'eventjunctions.event.genes', 'genes', org.hibernate.criterion.CriteriaSpecification.LEFT_JOIN
            createAlias 'assay', 'assay', org.hibernate.criterion.CriteriaSpecification.LEFT_JOIN

            order 'assay.id', 'asc' // important

            // because we're using this transformer, every column has to have an alias
            instance.setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP)
        }

        criteriaBuilder
    }

    @Override
    TabularResult transformResults(ScrollableResults results, List<AssayColumn> assays, Projection projection) {

        Map assayIndexMap = createAssayIndexMap assays

        new MultiTabularResult(
                rowsDimensionLabel: 'Regions',
                columnsDimensionLabel: 'Sample codes',
                indicesList: assays,
                results: results,
                inSameGroup: {a, b -> a.junction.id == b.junction.id},
                allowMissingColumns: false,
                finalizeGroup: {List list ->
                    def j = new JunctionRow(list[0].junction[0])
                    //all junctions are the same in this group
                    j.eventjunctions = list.findAll({
                        it.eventjunctions[0] != null
                    }).collect({
                        new JunctionInEventRow(it.eventjunctions[0])
                    })
                    def events = list.findAll({
                        it.event[0] != null
                    }).collect({
                        new EventRow(it.event[0])
                    })
                    def genes = list.findAll({
                        it.genes[0] != null
                    }).collect({
                        new GenesInEventRow(it.genes[0])
                    })
                    for (int i = 0; i < j.eventjunctions.size(); i++) {
                        j.eventjunctions[i].event = events.find({
                            j.eventjunctions[i].eventId == it.id
                        })
                        j.eventjunctions[i].junction = j
                        j.eventjunctions[i].event.genes = genes.findAll({
                            it.eventId == j.eventjunctions[i].event.id
                        }).collect()
                    }
                    j
                }
        )
    }
}