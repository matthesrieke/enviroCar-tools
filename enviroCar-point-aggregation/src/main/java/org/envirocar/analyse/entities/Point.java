package org.envirocar.analyse.entities;

import java.util.List;
import java.util.Map;

public interface Point {

	String getID();
	
	double getX();
	
	double getY();
	
	Map<String, Object> getPropertyMap();
	
	Object getProperty(String propertyName);
	
	int getNumberOfPointsUsedForAggregation();
	
	List<String> getTracksUsedForAggregation();
	
	void setID(String id);
	
	void setProperty(String propertyName, Object value);
	
	void setNumberOfPointsUsedForAggregation(int numberOfPoints);
	
	void addTrackUsedForAggregation(String trackID);

	int getNumberOfTracksUsedForAggregation();

	String getLastContributingTrack();

	void setNumberOfTracksUsedForAggregation(
			int numberOfTracksUsedForAggregation);

	void setLastContributingTrack(String lastContributingTrack);
	
}
