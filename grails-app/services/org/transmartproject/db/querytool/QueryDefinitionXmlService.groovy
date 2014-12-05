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

package org.transmartproject.db.querytool

import groovy.xml.MarkupBuilder
import org.joda.time.format.ISODateTimeFormat
import org.transmartproject.core.exceptions.InvalidRequestException
import org.transmartproject.core.querytool.*

import static org.transmartproject.core.querytool.DateSpecification.DateColumn
import static org.transmartproject.core.querytool.DateSpecification.DateColumn.*

/**
 * Handles conversions of {@link org.transmartproject.core.querytool
 * .QueryDefinition}s to and from XML strings, as they are stored in
 * qt_query_master.
 */
class QueryDefinitionXmlService implements QueryDefinitionXmlConverter {

    QueryDefinition fromXml(Reader reader) throws InvalidRequestException {
        def xml
        try {
            xml = new XmlSlurper().parse(reader)
        } catch (exception) {
            throw new InvalidRequestException('Malformed XML document: ' +
                    exception.message, exception)
        }

        def convertItem = { item ->
            def data = [ conceptKey: item.item_key ]
            if (item.constrain_by_value.size()) {
                try {
                    def constrain = item.constrain_by_value
                    data.constraint = new ConstraintByValue(
                            valueType: ConstraintByValue.ValueType.valueOf(
                                    constrain.value_type?.toString()),
                            operator: ConstraintByValue.Operator.forValue(
                                    constrain.value_operator.toString()),
                            constraint: constrain.value_constraint?.toString()
                    )
                } catch (err) {
                    throw new InvalidRequestException(
                            'Invalid XML query definition constraint', err)
                }
            }

            new Item(data)
        }

        def convertDateSpecification = { ds ->
            if (ds.size() == 0) {
                return
            }

            DateColumn dateColumn = START_DATE
            if (ds.'@time') {
                try {
                    dateColumn = DateColumn.valueOf(ds.'@time'.text())
                } catch (IllegalArgumentException iae) {
                    throw new InvalidRequestException(
                            'Invalid date column specification', iae)
                }
            }
            Calendar date
            try {
                date = ISODateTimeFormat.dateTime().parseDateTime(ds.text())
            } catch (IllegalArgumentException iae) {
                throw new InvalidRequestException('Invalid date', iae)
            }


            new DateSpecification(
                    inclusive: ds.'@inclusive' != 'NO',
                    dateColumn: dateColumn,
                    date: date)
        }

        def panels = xml.panel.collect { panel ->
            new Panel(
                    invert: panel.invert == '1',
                    items: panel.item.collect(convertItem),
                    dateFrom: convertDateSpecification(panel.panel_date_from),
                    dateTo: convertDateSpecification(panel.panel_date_to))
        }

        if (xml.query_name.size()) {
            return new QueryDefinition(xml.query_name.toString(), panels)
        } else {
            return new QueryDefinition(panels)
        }
    }

    String toXml(QueryDefinition definition) {
        def writer = new StringWriter()
        def xml = new MarkupBuilder(writer)

        /* this XML document is invalid in quite some respects according to
         * the schema, but:
         * 1) that's a subset of what tranSMART used in its requests to CRC
         * 2) i2b2 accepts these documents (not that it matters a lot at this
         * point, since we're not using i2b2's runtime anymore)
         * 3) the schema does not seem correct in many respects; several
         * elements that are supposed to be optional are actually required.
         *
         * It's possible the schema is only used to generate Java classes
         * using JAXB and that there's never any validation against the schema
         */
        xml.query_definition {
            query_name definition.name

            definition.panels.each { Panel panelArg ->
                panel {
                    invert panelArg.invert ? '1' : '0'

                    writeDateSpecification panelArg.dateFrom,
                            'panel_date_from', delegate
                    writeDateSpecification panelArg.dateTo,
                            'panel_date_to', delegate

                    panelArg.items.each { Item itemArg ->
                        item {
                            item_key itemArg.conceptKey

                            if (itemArg.constraint) {
                                constrain_by_value {
                                    value_operator itemArg.constraint.operator.value
                                    value_constraint itemArg.constraint.constraint
                                    value_type itemArg.constraint.valueType.name()
                                }
                            }
                        }
                    }
                }
            }
        }

        writer.toString()
    }

    private void writeDateSpecification(DateSpecification ds, String element, delegate) {
         if (!ds) {
             return
         }

        delegate."$element" time: ds.dateColumn.name,
                inclusive: ds.inclusive.toString(),
                ISODateTimeFormat.dateTime().print(ds.date)
    }
}
