package org.envirocar.analyse.entities;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class InMemoryPoint implements Point {

	private String id;
	private double x;
	private double y;
	private Map<String, Object> propertyMap;
	private int numberOfPointsUsedForAggregation = 1;
	private List<String> tracksUsedForAggregation;
	private int numberOfTracksUsedForAggregation;
	private String lastContributingTrack;
	
	public InMemoryPoint(String id, double x, double y, Map<String, Object> propertyMap, String trackID){
		this.id = id;
		this.x = x;
		this.y = y;	
		this.propertyMap = propertyMap;
		tracksUsedForAggregation = new ArrayList<>();
		tracksUsedForAggregation.add(trackID);
	}
	
	public InMemoryPoint(String id, double x, double y, Map<String, Object> propertyMap, int numberOfPointsUsedForAggregation, List<? extends Object> contributingTracks){
		this.id = id;
		this.x = x;
		this.y = y;	
		this.propertyMap = propertyMap;		
		this.numberOfPointsUsedForAggregation = numberOfPointsUsedForAggregation;		
		tracksUsedForAggregation = new ArrayList<>();
		
		for (Object object : contributingTracks) {
			if(object instanceof String){
				tracksUsedForAggregation.add((String)object);
			}
		}
	}
	
	public InMemoryPoint(String id, double x, double y, Map<String, Object> propertyMap, int numberOfPointsUsedForAggregation, int numberOfTracksUsedForAggregation, String lastContributingTrack){
		this.id = id;
		this.x = x;
		this.y = y;	
		this.propertyMap = propertyMap;		
		this.numberOfPointsUsedForAggregation = numberOfPointsUsedForAggregation;	
		this.numberOfTracksUsedForAggregation = numberOfTracksUsedForAggregation;	
		this.lastContributingTrack = lastContributingTrack;	
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

}
