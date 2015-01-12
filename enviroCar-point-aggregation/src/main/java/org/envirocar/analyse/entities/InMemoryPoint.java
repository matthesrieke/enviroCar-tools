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
package org.envirocar.analyse.entities;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.envirocar.analyse.util.Utils;


public class InMemoryPoint implements Point {

	private String id;
	private double x;
	private double y;
	private Map<String, Object> propertyMap;
	private Map<String, Integer> propertyPointsUsedForAggregationMap;
	private int numberOfPointsUsedForAggregation = 1;
	private List<String> tracksUsedForAggregation;
	private int numberOfTracksUsedForAggregation;
	private String lastContributingTrack;
	
	public InMemoryPoint(String id, double x, double y, Map<String, Object> propertyMap, int numberOfPointsUsedForAggregation, int numberOfTracksUsedForAggregation, String lastContributingTrack, Map<String, Integer> propertyPointsUsedForAggregationMap){
		this.id = id;
		this.x = x;
		this.y = y;	
		this.propertyMap = propertyMap;		
		this.numberOfPointsUsedForAggregation = numberOfPointsUsedForAggregation;	
		this.numberOfTracksUsedForAggregation = numberOfTracksUsedForAggregation;	
		this.lastContributingTrack = lastContributingTrack;	
		this.propertyPointsUsedForAggregationMap = propertyPointsUsedForAggregationMap;
	}
	
	public InMemoryPoint(Point otherPoint){
		this.id = otherPoint.getID();
		this.x = otherPoint.getX();
		this.y = otherPoint.getY();
		this.propertyMap = otherPoint.getPropertyMap();	
		this.numberOfPointsUsedForAggregation = otherPoint.getNumberOfPointsUsedForAggregation();	
		this.numberOfTracksUsedForAggregation = otherPoint.getNumberOfTracksUsedForAggregation();	
		this.lastContributingTrack = otherPoint.getLastContributingTrack();
		tracksUsedForAggregation = otherPoint.getTracksUsedForAggregation();
		this.propertyPointsUsedForAggregationMap = otherPoint.getPropertyPointsUsedForAggregationMap();
	}
	
	public InMemoryPoint(){
		
	}
	
	@Override
	public String getID() {
		return id;
	}

	@Override
	public double getX() {
		return x;
	}

	@Override
	public double getY() {
		return y;
	}

	@Override
	public int getNumberOfPointsUsedForAggregation() {
		return numberOfPointsUsedForAggregation;
	}

	@Override
	public List<String> getTracksUsedForAggregation() {
		return tracksUsedForAggregation;
	}

	@Override
	public void setNumberOfPointsUsedForAggregation(int numberOfPoints) {
		this.numberOfPointsUsedForAggregation = numberOfPoints;
	}

	@Override
	public void addTrackUsedForAggregation(String trackID) {
		tracksUsedForAggregation.add(trackID);
	}

	@Override
	public Map<String, Object> getPropertyMap() {
		return propertyMap;
	}

	@Override
	public void setProperty(String propertyName, Object value) {
		propertyMap.put(propertyName, value);		
	}

	@Override
	public Object getProperty(String propertyName) {		
		return propertyMap.get(propertyName);
	}

	@Override
	public void setID(String id) {
		this.id = id;
	}
	
	@Override
	public int getNumberOfTracksUsedForAggregation() {
		return numberOfTracksUsedForAggregation;
	}

	@Override
	public String getLastContributingTrack() {
		return lastContributingTrack;
	}

	@Override
	public void setNumberOfTracksUsedForAggregation(
			int numberOfTracksUsedForAggregation) {
		this.numberOfTracksUsedForAggregation = numberOfTracksUsedForAggregation;
	}
	
	@Override
	public void setLastContributingTrack(String lastContributingTrack) {
		this.lastContributingTrack = lastContributingTrack;
	}

	@Override
	public void setX(double x) {
		this.x = x;		
	}

	@Override
	public void setY(double y) {
		this.y = y;
	}

	@Override
	public int getNumberOfPointsUsedForAggregation(String propertyName) {

		if (propertyPointsUsedForAggregationMap.containsKey(propertyName)) {
			return propertyPointsUsedForAggregationMap.get(propertyName);
		}
		return 1;
	}

	@Override
	public void setNumberOfPointsUsedForAggregation(int numberOfPoints,
			String propertyName) {
		propertyPointsUsedForAggregationMap.put(propertyName, numberOfPoints);		
	}

	@Override
	public Map<String, Integer> getPropertyPointsUsedForAggregationMap() {		
		return propertyPointsUsedForAggregationMap;
	}

	public static Point fromMap(Map<?, ?> featureMap, String trackID) {
		Object geometryObject = featureMap.get("geometry");
		
		double[] coordinatesXY = new double[2];
		
		if (geometryObject instanceof Map<?, ?>) {
			coordinatesXY = Utils.getCoordinatesXYFromJSON((LinkedHashMap<?, ?>) geometryObject);
		}
		
		Object propertiesObject = featureMap.get("properties");				
		
		if (propertiesObject instanceof Map<?, ?>) {
			Map<?, ?> propertiesMap = (Map<?, ?>) propertiesObject;

			String id = String.valueOf(propertiesMap.get("id"));
			
			Object phenomenonsObject = propertiesMap.get("phenomenons");

			if (phenomenonsObject instanceof Map<?, ?>) {
				Map<?, ?> phenomenonsMap = (Map<?, ?>) phenomenonsObject;

				Map<String, Object> propertiesofInterestMap = Utils.getValuesFromFromJSON(phenomenonsMap);
				
				Point point = new InMemoryPoint(id, coordinatesXY[0], coordinatesXY[1], propertiesofInterestMap, 1, 1, trackID, new HashMap<String, Integer>());
				
				return point;
			}
		}
		
		return null;
	}

}
