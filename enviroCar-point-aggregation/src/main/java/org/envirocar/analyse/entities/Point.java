/**
 * Copyright (C) 2014
 * by 52 North Initiative for Geospatial Open Source Software GmbH
 *
 * Contact: Andreas Wytzisk
 * 52 North Initiative for Geospatial Open Source Software GmbH
 * Martin-Luther-King-Weg 24
 * 48155 Muenster, Germany
 * info@52north.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
	
	int getNumberOfPointsUsedForAggregation(String propertyName);
	
	List<String> getTracksUsedForAggregation();
	
	void setID(String id);
	
	void setProperty(String propertyName, Object value);
	
	void setNumberOfPointsUsedForAggregation(int numberOfPoints);
	
	void setNumberOfPointsUsedForAggregation(int numberOfPoints, String propertyName);
	
	void addTrackUsedForAggregation(String trackID);

	int getNumberOfTracksUsedForAggregation();

	String getLastContributingTrack();

	void setNumberOfTracksUsedForAggregation(
			int numberOfTracksUsedForAggregation);

	void setLastContributingTrack(String lastContributingTrack);
	
	void setX(double x);
	
	void setY(double y);
	
}
