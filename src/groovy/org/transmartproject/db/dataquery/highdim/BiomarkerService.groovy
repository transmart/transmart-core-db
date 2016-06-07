package org.transmartproject.db.dataquery.highdim

import org.hibernate.ScrollableResults
import org.hibernate.SessionFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import org.transmartproject.core.IterableResult
import org.transmartproject.db.biomarker.BioMarkerCoreDb
import org.transmartproject.db.biomarker.BioMarkerCorrelMv
import org.transmartproject.db.search.SearchKeywordCoreDb
import org.transmartproject.db.util.ResultIteratorWrappingIterable

import static org.transmartproject.db.util.GormWorkarounds.executeQuery

@Component
class BiomarkerService {

    @Autowired
    SessionFactory sessionFactory

    static void checkOptions(Map options) {
        options?.each {
            switch (it.key) {
                case 'searchKeywords':
                    assert it.value instanceof Boolean
                    break
                case 'related':
                    assert it.value instanceof Boolean
                    break
                case 'limit':
                    assert it.value instanceof Number
                    break
                default:
                    assert false, "$it.key not a valid option"
            }
        }
    }

    private static String joinBiomarkerQueryEntries(List<BMQueryEntry> entries) {
        def tables = []
        def filters = []
        def lastEntry = null
        int tablenum = 1

        entries.each {
            if (it.table && !it.name) {
                it.name = "${it.table.toLowerCase()}_${tablenum++}_"
            }
        }

        // Do some optimization: If a table is being joined on itself by the same attribute we can just skip the join
        // This assumes the property being joined on itself is not nullable, otherwise the results are not the same.
        entries = entries.inject([]) { List<BMQueryEntry> list, BMQueryEntry next ->
            BMQueryEntry prev = list.size() ? list[-1] : null
            if (prev && prev.attr &&
                    prev.table == next.table &&
                    prev.attr == next.filterBy) {
                filters.addAll(prev.filters.collect { it("$next.name.$next.filterBy") })
                //filters << "$prev.name.$prev.attr is not null"
                prev.filters = next.filters
                prev.attr = next.attr
                return list
            } else {
                return list << next
            }
        } as List<BMQueryEntry>

        for (def entry: entries) {
            if (entry.table) {
                tables << "$entry.table as $entry.name"
                if (lastEntry?.attr && entry.filterBy) {
                    filters << "$lastEntry.name.$lastEntry.attr = $entry.name.$entry.filterBy"
                }
                if (entry.filterBy) lastEntry.filters.each {
                    filters << it("$entry.name.$entry.filterBy")
                }
            }
            lastEntry = entry
        }

        return "select $lastEntry.name.$lastEntry.attr\n" +
                "from ${tables.join(', ')}\n" +
                "where ${filters.join(' and ')}"
    }

    static final private class BMQueryEntry {
        def table
        String name, attr, filterBy
        List<Closure> filters = []

        void setTable(tab) {
            if (tab instanceof Class) tab = tab.simpleName
            table = (String) tab
        }
    }

    IterableResult<String> queryBioMarkers(Map options, String biomarkerHql, Map queryParams) {
        checkOptions(options)

        boolean searchKeywords = options?.searchKeywords == null ? false : options.searchKeywords
        boolean related = options?.related == null ? true : options.related
        Number limit = options?.limit ?: null

        // or BioMarkerCoreDb.name
//        "select kw.keyword from BioMarkerCorrelMv correl, SearchKeywordCoreDb kw " +
//                "where correl.associatedBioMarker.id in ($biomarkerHql) " +
//                "and correl.bioMarker.id = kw.bioDataId"

        List<BMQueryEntry> queryEntries = [new BMQueryEntry(
                filters: [{ "$it in ($biomarkerHql)" }]
        ), new BMQueryEntry(
                table: BioMarkerCoreDb,
                attr: 'id',
                filterBy: 'externalId'
        )]

        if (related) {
            queryEntries << new BMQueryEntry(
                    table: BioMarkerCorrelMv,
                    attr: 'bioMarker.id',
                    filterBy: 'associatedBioMarker.id')
        }

        if (searchKeywords) {
            queryEntries << new BMQueryEntry(
                    table: SearchKeywordCoreDb,
                    attr: 'keyword',
                    filterBy: 'bioDataId')
        } else {
            queryEntries << new BMQueryEntry(
                    table: BioMarkerCoreDb,
                    attr: 'name',
                    filterBy: 'id')
        }

        String query = joinBiomarkerQueryEntries(queryEntries)

        if (limit) {
            query += " limit ${limit.longValue()}"
        }

        println("***********************")
        println("query: $query")
        println("***********************")

        ScrollableResults result = executeQuery(sessionFactory.openStatelessSession(), query, queryParams)
        return new ResultIteratorWrappingIterable<String>(result)
    }

}
