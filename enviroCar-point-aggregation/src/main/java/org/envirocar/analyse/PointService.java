package org.envirocar.analyse;

import java.util.List;

import org.envirocar.analyse.entities.Point;

public interface PointService {
	
	Point aggregate(Point point, Point aggregationPoint);

	Point getNextPoint(String trackID);

	void getMeasurementsOfTracks(List<String> trackIDs);

	Point getNearestNeighbor(String pointID, double distance);

	void addToResultSet(Point newPoint);
	
	boolean updateResultSet(String idOfPointToBeReplaced, Point replacementPoint);

	List<Point> getResultSet();
	
}
