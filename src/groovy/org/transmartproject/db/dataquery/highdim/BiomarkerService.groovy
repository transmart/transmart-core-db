package org.transmartproject.db.dataquery.highdim

import groovy.util.logging.Log4j
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

/**
 * The implementation of this class is unfortunately a bit ugly. A list of BMQueryEntry's describes a query that is
 * dynamically built up, and then converted to HQL (which then gets parsed again by Hibernate to an AST and finally
 * to SQL).
 *
 * The reason is that Hibernate criteria queries don't support joins and don't support casts, so they could not be
 * used. The main alternatives would have been to extend the criteria api or to use raw SQL. The former would IMO
 * cost a lot more time and complexity to implement, but would be more general and the right way to solve this. Using
 * raw sql would have required a similar criteria-like layer on top, and have all the portability problems that come
 * with it.
 */

@Component
@Log4j
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
                case 'prefix':
                    assert it.value instanceof String
                    break
                case 'limit':
                    assert it.value instanceof Number
                    break
                default:
                    assert false, "$it.key not a valid option"
            }
        }
    }

    static final private class BMQueryEntry {
        def table
        String name, attr, filterBy
        Closure filterLast, filterNext

        void setTable(tab) {
            if (tab instanceof Class) tab = tab.simpleName
            table = (String) tab
        }
    }

    private static String joinBiomarkerQueryEntries(List<BMQueryEntry> entries) {
        def tables = []
        def filters = []
        def lastTable = null

        int tablenum = 1
        entries.each {
            if (it.table && !it.name) {
                it.name = "${it.table.toLowerCase()}_${tablenum++}_"
            }
        }

        // Do some optimization: If a table is being joined on itself by the same attribute we can just skip the join
        // This currently does not detect identical query entries that are separated by an entry containing only
        // filters but no table.
        entries = entries.inject([]) { List<BMQueryEntry> list, BMQueryEntry next ->
            BMQueryEntry prev = list.size() ? list[-1] : null
            if (prev && prev.table && prev.attr &&
                    prev.table == next.table &&
                    prev.attr == next.filterBy) {

                // Resolve filters that can be resolved now
                if (prev.filterNext) filters << prev.filterNext("$prev.name.$next.filterBy")
                if (next.filterLast) filters << next.filterLast("$prev.name.$prev.attr")

                // Exclude nulls to maintain the same semantics as a self join (null != null in SQL)
                // Unfortunately Postgres is not smart enough to skip this check if e.g. the column is non-nullable
                filters << "$prev.name.$prev.attr is not null"

                // Move attributes from the second table to the first
                prev.filterNext = next.filterNext
                prev.attr = next.attr

                // Do not add the second table to the returned query list
                return list

            } else {
                return list << next
            }
        } as List<BMQueryEntry>

        // Transform the list of entries into HQL fragments in tables and filters
        List<Closure> pendingFilters = []
        for (def entry: entries) {
            if (entry.table) {
                assert entry.attr
                if (pendingFilters) assert entry.filterBy
                pendingFilters.each {
                    filters << it("$entry.name.$entry.filterBy")
                }
                pendingFilters.clear()

                tables << "$entry.table as $entry.name"
                if (lastTable && entry.filterBy) {
                    filters << "$lastTable.name.$lastTable.attr = $entry.name.$entry.filterBy"
                }
            }
            if (entry.filterLast) {
                assert lastTable
                filters << entry.filterLast("$lastTable.name.$lastTable.attr")
            }
            if (entry.filterNext) pendingFilters << entry.filterNext

            if (entry.table) lastTable = entry
        }

        assert lastTable
        return "select $lastTable.name.$lastTable.attr\n" +
                "from ${tables.join(', ')}\n" +
                "where ${filters.join(' and ')}"
    }

    IterableResult<String> queryBioMarkers(Map options, String biomarkerHql, Map queryParams) {
        checkOptions(options)

        boolean searchKeywords = options?.searchKeywords == null ? false : options.searchKeywords
        boolean related = options?.related == null ? true : options.related
        String prefix = options?.prefix
        Number limit = options?.limit ?: null

        List<BMQueryEntry> queryEntries = [new BMQueryEntry(
                filterNext: { "$it in ($biomarkerHql)" }
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

        if (prefix) {
            queryEntries << new BMQueryEntry(
                    filterLast: { "$it like :prefix"}
            )
            queryParams.prefix = prefix+"%"
        }

        String query = joinBiomarkerQueryEntries(queryEntries)

        if (limit) {
            query += " limit ${limit.longValue()}"
        }

        log.debug(query)

        println("***********************")
        println("query: $query")
        println("***********************")

        ScrollableResults result = executeQuery(sessionFactory.openStatelessSession(), query, queryParams)
        return new ResultIteratorWrappingIterable<String>(result)
    }

}
