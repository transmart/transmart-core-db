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

package org.transmartproject.db.dataquery.clinical

import com.google.common.collect.HashMultiset
import com.google.common.collect.Lists
import com.google.common.collect.Maps
import com.google.common.collect.Multiset
import groovy.sql.Sql
import org.hibernate.HibernateException
import org.hibernate.ScrollMode
import org.hibernate.ScrollableResults
import grails.gorm.DetachedCriteria
import org.hibernate.engine.SessionImplementor
import org.hibernate.type.Type
import org.transmartproject.core.dataquery.clinical.ClinicalVariable
import org.transmartproject.core.exceptions.InvalidArgumentsException
import org.transmartproject.db.dataquery.clinical.variables.TerminalConceptVariable
import org.transmartproject.db.i2b2data.ConceptDimension
import org.transmartproject.db.i2b2data.ObservationFact
import org.transmartproject.db.i2b2data.PatientDimension


import java.sql.Blob
import java.sql.Clob

import static com.google.common.collect.Lists.partition
import static org.transmartproject.db.util.GormWorkarounds.createCriteriaBuilder
import static org.transmartproject.db.util.GormWorkarounds.getHibernateInCriterion

class TerminalConceptVariablesDataQuery {

    List<TerminalConceptVariable> clinicalVariables

    List<List<String>> conceptCodeList = new ArrayList()

    List<Object> patientIdList = new ArrayList<>()

    Iterable<PatientDimension> patients

    SessionImplementor session

    private boolean inited

    def connection

    void init() {
        fillInTerminalConceptVariables()
        inited = true
    }


    ScrollableResults openResultSet() {

        def criteriaBuilder = createCriteriaBuilder(ObservationFact, 'obs', session)
        criteriaBuilder.with {
            projections {
                distinct 'patient.id'
            }
            order 'patient.id'
        }
        if (patients instanceof PatientQuery) {
            criteriaBuilder.add(getHibernateInCriterion('patient.id', patients.forIds()))
        } else {
            criteriaBuilder.in('patient',  Lists.newArrayList(patients))
        }
        ScrollableResults sr = criteriaBuilder.scroll(ScrollMode.FORWARD_ONLY)
        while (sr.next()) {
            patientIdList.add(sr.getLong(0))
//            patientIdList.add(sr.get(0))
        }



        patientIdList.unique().sort()

        if (!inited) {
            throw new IllegalStateException('init() not called successfully yet')
        }

        int threshold = 30000

        List<String> clinicalVariablesCodes = new ArrayList<>()

        for (TerminalConceptVariable clinicalVariable : clinicalVariables) {
            clinicalVariablesCodes.add(clinicalVariable.code)
        }

        conceptCodeList = partition(clinicalVariablesCodes.unique().sort(), threshold)

        return new TerminalConceptVariablesDataQueryScrollableResults()
    }


    class TerminalConceptVariablesDataQueryScrollableResults implements ScrollableResults {
        ScrollableResults currentResult

        int patientIdCount = 0
        int conceptCodeCount = 0



        void createRequestForEachPatient(int conceptCodeCount, int patientIdCount) {
            if (currentResult != null)
                currentResult.close()

            def criteriaBuilder = createCriteriaBuilder(ObservationFact, 'obs', session)

            criteriaBuilder.with {
                projections {
                    property 'patient.id'
                    property 'conceptCode'
                    property 'valueType'
                    property 'textValue'
                    property 'numberValue'

                }
                order 'conceptCode'
            }

            criteriaBuilder.eq('patient.id', patientIdList.get(patientIdCount))

            criteriaBuilder.in('conceptCode', conceptCodeList.get(conceptCodeCount))

            currentResult = criteriaBuilder.scroll ScrollMode.FORWARD_ONLY
        }


        @Override
        boolean next() {
            if (currentResult == null) {
                createRequestForEachPatient(conceptCodeCount, patientIdCount)
            }

            while (!currentResult.next()) {
                conceptCodeCount++
                if (conceptCodeCount >= conceptCodeList.size()) {
                    conceptCodeCount = 0
                    patientIdCount++
                }
                if (patientIdCount < patientIdList.size()) {
                    createRequestForEachPatient(conceptCodeCount, patientIdCount)
                } else {
                    return false
                }
            }

            return true
        }


        @Override
        void afterLast() {
            throw new HibernateException("")
        }

        @Override
        void beforeFirst() {
            throw new HibernateException("")
        }

        @Override
        boolean first() {
            throw new HibernateException("")

        }

        @Override
        boolean isFirst() {
            throw new HibernateException("")

        }

        @Override
        boolean isLast() {
            throw new HibernateException("")

        }

        @Override
        boolean last() {
            throw new HibernateException("")

        }

        @Override
        boolean scroll(int i) {
            throw new HibernateException("")

        }

        @Override
        boolean setRowNumber(int i) {
            throw new HibernateException("")

        }

        @Override
        boolean previous() throws HibernateException {
            throw new HibernateException("")

        }

        @Override
        void close() throws HibernateException {
            if (currentResult != null) {
                currentResult.close()
            }
        }

        @Override
        Object[] get() throws HibernateException {
            return currentResult?.get()
        }

        @Override
        Object get(int i) throws HibernateException {
            return currentResult?.get(i)
        }

        @Override
        Type getType(int i) {
            return currentResult?.getType(i)
        }

        @Override
        Integer getInteger(int col) throws HibernateException {
            return currentResult?.getInteger(col)
        }

        @Override
        Long getLong(int col) throws HibernateException {
            return currentResult?.getLong(col)
        }

        @Override
        Float getFloat(int col) throws HibernateException {
            return currentResult?.getFloat(col)
        }

        @Override
        Boolean getBoolean(int col) throws HibernateException {
            return currentResult?.getBoolean(col)
        }

        @Override
        Double getDouble(int col) throws HibernateException {
            return currentResult?.getDouble(col)
        }

        @Override
        Short getShort(int col) throws HibernateException {
            return currentResult?.getShort(col)
        }

        @Override
        Byte getByte(int col) throws HibernateException {
            return currentResult?.getByte(col)
        }

        @Override
        Character getCharacter(int col) throws HibernateException {
            return currentResult?.getCharacter(col)
        }

        @Override
        byte[] getBinary(int col) throws HibernateException {
            return currentResult?.getBinary(col)
        }

        @Override
        String getText(int col) throws HibernateException {
            return currentResult?.getText(col)
        }

        @Override
        Blob getBlob(int col) throws HibernateException {
            return currentResult?.getBlob(col)
        }

        @Override
        Clob getClob(int col) throws HibernateException {
            return currentResult?.getClob(col)
        }

        @Override
        String getString(int col) throws HibernateException {
            return currentResult?.getString(col)
        }

        @Override
        BigDecimal getBigDecimal(int col) throws HibernateException {
            return currentResult?.getBigDecimal(col)
        }

        @Override
        BigInteger getBigInteger(int col) throws HibernateException {
            return currentResult?.getBigInteger(col)
        }

        @Override
        Date getDate(int col) throws HibernateException {
            return currentResult?.getDate(col)
        }

        @Override
        Locale getLocale(int col) throws HibernateException {
            return currentResult?.getLocale(col)
        }

        @Override
        Calendar getCalendar(int col) throws HibernateException {
            return currentResult?.getCalendar(col)
        }

        @Override
        TimeZone getTimeZone(int col) throws HibernateException {
            return currentResult?.getTimeZone(col)
        }

        @Override
        int getRowNumber() throws HibernateException {
            throw new HibernateException("")

        }

    }

    private void fillInTerminalConceptVariables() {
        Map<String, TerminalConceptVariable> conceptPaths = Maps.newHashMap()
        Map<String, TerminalConceptVariable> conceptCodes = Maps.newHashMap()

        if (!clinicalVariables) {
            throw new InvalidArgumentsException('No clinical variables specified')
        }

        clinicalVariables.each { ClinicalVariable it ->
            if (!(it instanceof TerminalConceptVariable)) {
                throw new InvalidArgumentsException(
                        'Only terminal concept variables are supported')
            }

            if (it.conceptCode) {
                if (conceptCodes.containsKey(it.conceptCode)) {
                    throw new InvalidArgumentsException("Specified multiple " +
                            "variables with the same concept code: " +
                            it.conceptCode)
                }
                conceptCodes[it.conceptCode] = it
            } else if (it.conceptPath) {
                if (conceptPaths.containsKey(it.conceptPath)) {
                    throw new InvalidArgumentsException("Specified multiple " +
                            "variables with the same concept path: " +
                            it.conceptPath)
                }
                conceptPaths[it.conceptPath] = it
            }
        }


        int conceptPathsSize = conceptPaths.size()
        int conceptCodesSize = conceptCodes.size()

        int threshold = 16000;

        int maxLen = Math.max(conceptPathsSize, conceptCodesSize)
        int numQueries = maxLen / threshold + (maxLen % threshold != 0 ? 1 : 0)


        def conceptPathParts = new Set[numQueries]
        def conceptCodeParts = new Set[numQueries]

        for (int i = 0; i < numQueries; i++) {
            conceptPathParts[i] = new HashSet()
            conceptCodeParts[i] = new HashSet()
        }

        Iterator conceptPathIterator = conceptPaths.keySet().iterator()
        Iterator conceptCodeIterator = conceptCodes.keySet().iterator()

        for (int i = 0; i < numQueries; i++) {
            for (int j = 0; j < threshold; j++) {
                if (conceptPathIterator.hasNext()) {
                    conceptPathParts[i].add(conceptPathIterator.next())
                }

                if (conceptCodeIterator.hasNext()) {
                    conceptCodeParts[i].add(conceptCodeIterator.next())

                }
            }
        }


        def res = new ArrayList()
        for (int i = 0; i < numQueries; i++) {
            def resPart = ConceptDimension.withCriteria {
                projections {
                    property 'conceptPath'
                    property 'conceptCode'

                }

                or {
                    if (conceptPathParts[i]) {
                        'in' 'conceptPath', conceptPathParts[i]
                    }
                    if (conceptCodeParts[i]) {
                        'in' 'conceptCode', conceptCodeParts[i]
                    }
                }
            }
            res.addAll(resPart)
        }


        for (concept in res) {
            String conceptPath = concept[0],
                   conceptCode = concept[1]

            if (conceptPaths[conceptPath]) {
                TerminalConceptVariable variable = conceptPaths[conceptPath]
                variable.conceptCode = conceptCode
            }
            if (conceptCodes[conceptCode]) {
                TerminalConceptVariable variable = conceptCodes[conceptCode]
                variable.conceptPath = conceptPath
            }
            // if both ifs manage we have the variable repeated (specified once
            // with concept code and once with concept path), and we'll catch
            // that further down
        }

        // check we found all the conceptsa
        for (var in conceptPaths.values()) {
            if (var.conceptCode == null) {
                throw new InvalidArgumentsException("Concept path " +
                        "'${var.conceptPath}' did not yield any results")
            }
        }
        for (var in conceptCodes.values()) {
            if (var.conceptPath == null) {
                throw new InvalidArgumentsException("Concept code " +
                        "'${var.conceptCode}' did not yield any results")
            }
        }

        Multiset multiset = HashMultiset.create clinicalVariables
        if (multiset.elementSet().size() < clinicalVariables.size()) {
            throw new InvalidArgumentsException("Repeated variables in the " +
                    "query (though once their concept path was specified and " +
                    "on the second time their concept code was specified): " +
                    multiset.elementSet().findAll {
                        multiset.count(it) > 1
                    })
        }
    }
}