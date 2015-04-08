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
import com.google.common.collect.Maps
import com.google.common.collect.Multiset
import org.hibernate.Query
import org.hibernate.ScrollMode
import org.hibernate.ScrollableResults
import org.hibernate.engine.SessionImplementor
import org.transmartproject.core.dataquery.clinical.ClinicalVariable
import org.transmartproject.core.exceptions.InvalidArgumentsException
import org.transmartproject.db.dataquery.clinical.variables.TerminalConceptVariable
import org.transmartproject.db.i2b2data.ConceptDimension

class TerminalConceptVariablesDataQuery {

    private static final int FETCH_SIZE = 10000

    List<TerminalConceptVariable> clinicalVariables

    Collection<Long> patientIds

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
		
		String patientInCondition = new String()
		Collection<Long> partialList;
		int remainder = patientIds.size()
		int listIt = 0;
		int listId = 0;
		ArrayList<Collection<Long>> splittedList = new ArrayList<Collection<Long>>();
		
		while(remainder > 1000) {
				partialList = patientIds.subList(listIt,listIt+1000)
				splittedList.add(partialList);
				patientInCondition += "patient.id IN (:patientIds"+listId+") OR "
				listIt = listIt + 1000
				remainder = remainder - 1000
				listId++
		}
		
		
		
		partialList = patientIds.subList(listIt, patientIds.size())
		splittedList.add(partialList);
		patientInCondition += "patient.id IN (:patientIds"+listId+")"
		
		String clinicalVarString = new String()
		Collection<Long> partialListCV;
		int remainderCV = clinicalVariables.size()
		int listItCV = 0;
		int listIdCV = 0;
		ArrayList<Collection<TerminalConceptVariable>> splittedListCV = new ArrayList<Collection<TerminalConceptVariable>>();
		
		while(remainderCV > 1000) {
				splittedListCV.add(clinicalVariables.subList(listItCV,listItCV+1000));
				clinicalVarString += "fact.conceptCode IN (:conceptCodes"+listIdCV+") OR "
				listItCV = listItCV + 1000
				remainderCV = remainderCV - 1000
				listIdCV++
		}
		
		splittedListCV.add(clinicalVariables.subList(listItCV, clinicalVariables.size()))
		clinicalVarString += "fact.conceptCode IN (:conceptCodes"+listIdCV+")"
		
		// see TerminalConceptVariable constants
        // see ObservationFact
        Query query = session.createQuery '''
                SELECT
                    patient.id,
                    conceptCode,
                    valueType,
                    textValue,
                    numberValue
                FROM ObservationFact fact
                WHERE
                    ('''+patientInCondition+''') 
                AND
                    ('''+clinicalVarString+''')
                ORDER BY
                    patient ASC,
                    conceptCode ASC'''

        query.cacheable = false
        query.readOnly  = true
        query.fetchSize = FETCH_SIZE

		for (int a = 0; a <= listId; a++) {
			query.setParameterList "patientIds"+a,   splittedList.get(a)
		}
		
		for (int a = 0; a <= listIdCV; a++) {
			List<TerminalConceptVariable> lCV = splittedListCV.get(a)
			query.setParameterList "conceptCodes"+a, lCV*.conceptCode
		}
        //query.setParameterList 'conceptCodes', clinicalVariables*.conceptCode

        query.scroll ScrollMode.FORWARD_ONLY
    }

    private void fillInTerminalConceptVariables() {
        Map<String, TerminalConceptVariable> conceptPaths = Maps.newHashMap()
        Map<String, TerminalConceptVariable> conceptCodes = Maps.newHashMap()
		
		ArrayList<String> conceptPathsAL = new ArrayList<String>()
		

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
				conceptPathsAL.add(it.conceptPath)
            }
        }

        // find the concepts
        def res = ConceptDimension.withCriteria {
            projections {
                property 'conceptPath'
                property 'conceptCode'
            }

            or {
                if (conceptPaths.keySet()) {
					if (conceptPathsAL.size > 1000) {
						int totalSize = conceptPathsAL.size
						int remainder = totalSize
						int currentIt = 0
						
						while (remainder > 1000) {
							'in' 'conceptPath', conceptPathsAL.subList(currentIt, currentIt+1000)
							currentIt = currentIt + 1000
							remainder = remainder - 1000
						}
						'in' 'conceptPath', conceptPathsAL.subList(currentIt, totalSize)
						
					}
					else
                    	'in' 'conceptPath', conceptPaths.keySet()
                }
                if (conceptCodes.keySet()) {
                    'in' 'conceptCode', conceptCodes.keySet()
                }
            }
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
