/**
 * Copyright (C) 2013
 * by 52 North Initiative for Geospatial Open Source Software GmbH
 *
 * Contact: Andreas Wytzisk
 * 52 North Initiative for Geospatial Open Source Software GmbH
 * Martin-Luther-King-Weg 24
 * 48155 Muenster, Germany
 * info@52north.org
 *
 * This program is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public License version 2 as published by the
 * Free Software Foundation.
 *
 * This program is distributed WITHOUT ANY WARRANTY; even without the implied
 * WARRANTY OF MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program (see gnu-gpl v2.txt). If not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA or
 * visit the Free Software Foundation web page, http://www.fsf.org.
 */
package org.envirocar.analyse;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.envirocar.analyse.entities.InMemoryPoint;
import org.envirocar.analyse.entities.Point;
import org.junit.Test;

public class UpdateStatementTest {

	@Test
	public void testAlgorithm(){
        
		PostgresPointService pointService = new PostgresPointService();
		
		String oldID = "528bd8a3e4b08cecc5a6f403";
		double oldX = 7.6339;
		double oldY = 51.94799;
		double oldSpeed = 20.0087251993326;
		double oldCo2 = 2.12374;
		int oldNumberOfPointsUsedForAggregation = 5;
		int oldNumberOfTracksUsedForAggregation = 7;
		int oldNumberOfPointsUsedForSpeed = 4;
		int oldNumberOfPointsUsedForCo2 = 6;
		String oldLastContributingTrack = "53b5228ee4b01607fa566b78";
		
		Map<String, Object> oldPropertyMap = new HashMap<>();
		
		oldPropertyMap.put("Speed", oldSpeed);
		oldPropertyMap.put("CO2", oldCo2);
		
		Map<String, Integer> oldPropertyPointsUsedForAggregationMap = new HashMap<>();
		
		oldPropertyPointsUsedForAggregationMap.put("Speed", oldNumberOfPointsUsedForSpeed);
		oldPropertyPointsUsedForAggregationMap.put("CO2", oldNumberOfPointsUsedForCo2);
		
		pointService.removePoint(oldID, PostgresPointService.aggregated_MeasurementsTableName);
		
		Point oldPoint = new InMemoryPoint(oldID, oldX, oldY, oldPropertyMap, oldNumberOfPointsUsedForAggregation, oldNumberOfTracksUsedForAggregation, oldLastContributingTrack, oldPropertyPointsUsedForAggregationMap);
		
		/*
		 * add measurement
		 */
		pointService.addToResultSet(oldPoint, false);
		
		String updatedID = "528bd8a3e4b09cecc5a6f445";
		double updatedX = 7.6539;
		double updatedY = 51.95688;
		double updatedSpeed = 35.7;
		double updatedCo2 = 1.654684;
		int updatedNumberOfPointsUsedForAggregation = 34;
		int updatedNumberOfTracksUsedForAggregation = 19;
		int updatedNumberOfPointsUsedForSpeed = 17;
		int updatedNumberOfPointsUsedForCo2 = 23;
		String updatedLastContributingTrack = "53b5448ee4b01607fa566b99";
		
		Map<String, Object> updatedPropertyMap = new HashMap<>();
		
		updatedPropertyMap.put("Speed", updatedSpeed);
		updatedPropertyMap.put("CO2", updatedCo2);
		
		Map<String, Integer> updatedPropertyPointsUsedForAggregationMap = new HashMap<>();
		
		updatedPropertyPointsUsedForAggregationMap.put("Speed", updatedNumberOfPointsUsedForSpeed);
		updatedPropertyPointsUsedForAggregationMap.put("CO2", updatedNumberOfPointsUsedForCo2);
		
		Point updatedPoint = new InMemoryPoint(updatedID, updatedX, updatedY, updatedPropertyMap, updatedNumberOfPointsUsedForAggregation, updatedNumberOfTracksUsedForAggregation, updatedLastContributingTrack, updatedPropertyPointsUsedForAggregationMap);
		 
		pointService.updateResultSet(oldID, updatedPoint);
		
		List<Point> resultSet = pointService.getResultSet();
		
		/*
		 * updated points shall keep their id
		 */
		boolean oldIDStillInResultSet = false;
		
		for (Point point : resultSet) {
			if(point.getID().equals(oldID)){
				oldIDStillInResultSet = true;
				
				assertTrue(point.getX() == updatedX);
				assertTrue(point.getY() == updatedY);
				assertTrue(point.getNumberOfPointsUsedForAggregation() == updatedNumberOfPointsUsedForAggregation);
				assertTrue(point.getNumberOfTracksUsedForAggregation() == updatedNumberOfTracksUsedForAggregation);
				assertTrue(point.getLastContributingTrack().equals(updatedLastContributingTrack));
				
				Map<String, Object> propertyMap = point.getPropertyMap();
				
				assertTrue((double)propertyMap.get("Speed") == updatedSpeed);
				assertTrue((double)propertyMap.get("CO2") == updatedCo2);
				
				Map<String, Integer> propertyPointsUsedForAggregationMap = point.getPropertyPointsUsedForAggregationMap();
				
				assertTrue(propertyPointsUsedForAggregationMap.get("Speed") == updatedNumberOfPointsUsedForSpeed);
				assertTrue(propertyPointsUsedForAggregationMap.get("CO2") == updatedNumberOfPointsUsedForCo2);
				
			}
		}
		
		assertTrue(oldIDStillInResultSet);
	}
	
}
