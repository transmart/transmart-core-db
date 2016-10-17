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
import org.hibernate.ScrollMode
import org.hibernate.ScrollableResults
import org.hibernate.engine.SessionImplementor
import org.transmartproject.core.dataquery.clinical.ClinicalVariable
import org.transmartproject.core.exceptions.InvalidArgumentsException
import org.transmartproject.db.dataquery.clinical.variables.TerminalConceptVariable
import org.transmartproject.db.i2b2data.PatientDimension
import org.transmartproject.db.util.DatabaseMultisetStorage

class TerminalConceptVariablesDataQuery {

    List<TerminalConceptVariable> clinicalVariables

    Iterable<PatientDimension> patients

    SessionImplementor session

    private boolean inited

    void init() {
        fillInTerminalConceptVariables()
        inited = true
    }

    ScrollableResults openResultSet() {
        if (!inited) {
            throw new IllegalStateException('init() not called successfully yet')
        }

        def patientIds = patients.collect { it.id }

        def dms = new DatabaseMultisetStorage(session)
        def patientsBagId = dms.saveIntegerData(patientIds)
        def intsTable = dms.getIntegerDataTableName()
        def cvarsBagId = dms.saveStringData(clinicalVariables*.code)
        def strsTable = dms.getStringDataTableName()

        def query = session.createSQLQuery """\
            select patient_num as patient,
                   concept_cd as conceptCode,
                   valtype_cd as valueType,
                   tval_char as textValue,
                   nval_num as numberValue
              from observation_fact
             where     patient_num in (select /*+dynamic_sampling(10)*/ id from $intsTable where mid=:pid)
                   and concept_cd in (select /*+dynamic_sampling(10)*/ id from $strsTable where mid=:cid)
             order by patient, conceptCode
        """.stripIndent()

        query.setInteger('pid', patientsBagId)
        query.setInteger('cid', cvarsBagId)
        query.cacheable = false
        query.readOnly  = true
        query.fetchSize = 10000

        query.scroll ScrollMode.FORWARD_ONLY
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

        // find the concepts
        def dms = new DatabaseMultisetStorage(session)
        def pathsBagId = dms.saveStringData(conceptPaths.keySet())
        def codesBagId = dms.saveStringData(conceptCodes.keySet())
        def strsTable = dms.getStringDataTableName()

        def stmt = session.connection().prepareStatement("""\
            select concept_path,
                   concept_cd
              from concept_dimension
             where concept_path in (select /*+dynamic_sampling(10)*/ id from $strsTable where mid=?)
            union
            select concept_path,
                   concept_cd
              from concept_dimension
             where concept_cd in (select /*+dynamic_sampling(10)*/ id from $strsTable where mid=?)
        """.stripIndent())
        stmt.setInt(1, pathsBagId)
        stmt.setInt(2, codesBagId)

        def res = stmt.executeQuery()
        while (res.next()) {
            String conceptPath = res.getString(1),
                   conceptCode = res.getString(2)

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

        // check we found all the concepts
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
