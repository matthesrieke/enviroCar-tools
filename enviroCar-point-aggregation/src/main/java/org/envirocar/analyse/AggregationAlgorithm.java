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
package org.envirocar.analyse;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.AllowAllHostnameVerifier;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.DefaultHttpClient;
import org.envirocar.analyse.entities.Point;
import org.envirocar.analyse.properties.Properties;
import org.envirocar.analyse.util.PointViaJsonMapIterator;
import org.envirocar.analyse.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

/**
 * Algorithm to aggregate measurements of tracks that are running through a defined bounding box.
 * 
 * @author Benjamin Pross
 *
 */
public class AggregationAlgorithm {

	private static final Logger LOGGER = LoggerFactory.getLogger(AggregationAlgorithm.class);
	
	private Geometry bbox;
	private double distance;
	private PointService pointService;
	private double maxx, maxy, minx, miny;
	
	public AggregationAlgorithm() {
		this(Double.parseDouble(Properties.getProperty("pointDistance")));
	}
	
	public AggregationAlgorithm(double distance) {
		pointService = new PostgresPointService(this.getBbox());
		this.distance = distance;
	}
	
	public AggregationAlgorithm(double minx, double miny, double maxx, double maxy){
		this();
		this.maxx = maxx;
		this.maxy = maxy;
		this.minx = minx;
		this.miny = miny;
		
        Coordinate upperRight = new Coordinate(maxx, maxy);
        Coordinate upperLeft = new Coordinate(minx, maxy);
        Coordinate lowerRight = new Coordinate(maxx, miny);
        Coordinate lowerLeft = new Coordinate(minx, miny);
        
        Coordinate[] coordinates = new Coordinate[] {
                lowerLeft,
                lowerRight,
                upperRight,
                upperLeft,
                lowerLeft
            };
        
        bbox =  Utils.geometryFactory.createPolygon(coordinates);
		this.distance = Double.parseDouble(Properties.getProperty("pointDistance"));
		pointService = new PostgresPointService(this.getBbox());
	}
	
	public void runAlgorithm(Iterator<Point> newPoints, String trackId) {
		if (pointService.trackAlreadyAggregated(trackId)) {
			LOGGER.info("Track already aggregated. skipping. "+trackId);
			return;
		}
		
		Point nextPoint;
		while (newPoints.hasNext()) {
			nextPoint = newPoints.next();
			
			/*
			 * check if point is fit for aggregation (one or more value not null or 0)
			 */
			if(!pointService.isFitForAggregation(nextPoint)){
				LOGGER.info("Skipping original point " + nextPoint.getID() + ". All values are null or 0.");
				continue;
			}

			/*
			 * get nearest neighbor from resultSet
			 */
			
			Point nearestNeighbor = pointService.getNearestNeighbor(
					nextPoint, distance);

//			List<Point> pointList = new ArrayList<>();
//			
//			pointList.add(nextPoint);
			
			if (nearestNeighbor != null) {
				
				/*
				 * check if point is fit for aggregation (one or more value not null or 0)
				 */
				if(!pointService.isFitForAggregation(nearestNeighbor)){
					LOGGER.info("Skipping result set point " + nearestNeighbor.getID() + ". All values are null or 0.");
					continue;
				}
				
//				pointList.add(nearestNeighbor);

				/*
				 * if there is one
				 * 
				 * aggregate values (avg, function should be
				 * replaceable)
				 */
				LOGGER.info("aggregating point: "+ nextPoint.getID());
				pointService.aggregate(nextPoint, nearestNeighbor, trackId);

//				pointList.add(aggregatedPoint);
				
				//TODO remove
//				try {
//					CSVExport.exportAsCSV(pointList, File.createTempFile(count + "-aggregation", ".csv").getAbsolutePath());
//				} catch (IOException e) {
//					LOGGER.error("Could not export resultSet as CSV:", e);
//				}
				/*
				 * PointService replace point in resultSet with aggregated
				 * point
				 */
//				pointService.updateResultSet(nearestNeighbor.getID(),
//						aggregatedPoint);

			} else {
				/*
				 * if there is no nearest neighbor
				 * 
				 * add point to resultSet
				 */					
				LOGGER.info("No nearest neighbor found for " + nextPoint.getID() + ". Adding to resultSet.");
				
				/*
				 * add point to result set, give it a new id
				 */
				pointService.addToResultSet(nextPoint, true);

				//TODO remove
//				try {
//					CSVExport.exportAsCSV(pointList, File.createTempFile(count + "-aggregation", ".csv").getAbsolutePath());
//				} catch (IOException e) {
//					LOGGER.error("Could not export resultSet as CSV:", e);
//				}
			}
		}
	}
	
	
	public void runAlgorithm(final String trackID) throws IOException {
		
		LOGGER.debug("");
		LOGGER.debug("");
		LOGGER.debug("");
		LOGGER.debug("");
		
		LOGGER.debug("Track: " + trackID);
		LOGGER.debug("ResultSet size: " + pointService.getResultSet().size());
		
		LOGGER.debug("");
		LOGGER.debug("");
		LOGGER.debug("");
		LOGGER.debug("");		

		HttpGet get = new HttpGet(Properties.getRequestTrackURL()+trackID);
		
		HttpClient client;
		try {
			client = createClient();
		} catch (KeyManagementException | UnrecoverableKeyException
				| NoSuchAlgorithmException | KeyStoreException e) {
			throw new IllegalStateException(e);
		}
		
		HttpResponse resp = client.execute(get);
		if (resp != null && resp.getEntity() != null
				&& resp.getStatusLine() != null &&
				resp.getStatusLine().getStatusCode() < HttpStatus.SC_MULTIPLE_CHOICES) {
			
			PointViaJsonMapIterator it = new PointViaJsonMapIterator(
					Utils.parseJsonStream(resp.getEntity().getContent()));
			
			runAlgorithm(it, trackID);
		}
		
	}
	
	protected HttpClient createClient() throws IOException, KeyManagementException, UnrecoverableKeyException, NoSuchAlgorithmException, KeyStoreException {
		DefaultHttpClient result = new DefaultHttpClient();
		SchemeRegistry sr = result.getConnectionManager().getSchemeRegistry();

		SSLSocketFactory sslsf = new SSLSocketFactory(new TrustStrategy() {

			@Override
			public boolean isTrusted(X509Certificate[] arg0, String arg1)
					throws CertificateException {
				return true;
			}
		}, new AllowAllHostnameVerifier());

		Scheme httpsScheme2 = new Scheme("https", 443, sslsf);
		sr.register(httpsScheme2);

		return result;
	}
	
	public void runAlgorithm() throws IOException{
		
		/*
		 * get tracks
		 */
				
        List<String> trackIDs = getTrackIDs(minx, miny, maxx, maxy);
		
		/*
		 * foreach track
		 * 
		 */
        
		for (String trackID : trackIDs) {

			runAlgorithm(trackID);
			/* 
			 * continue with next track
			 */
		}
	}
	
	public List<String> getTrackIDs(double minx, double miny, double maxx, double maxy){
		
		List<String> result = new ArrayList<>();
        
		URL url = null;
		try {		
			url = new URL(Properties.getRequestTracksWithinBboxURL() + minx + "," + miny + "," + maxx + "," + maxy);
			
			LOGGER.debug("URL for fetching tracks: " + url.toString());
			
			InputStream in = url.openStream();

			ObjectMapper objMapper = new ObjectMapper();

			Map<?, ?> map = objMapper.readValue(in, Map.class);

			ArrayList<?> tracks = (ArrayList<?>) map.get("tracks");

			LOGGER.info("Number of tracks: " + tracks.size());
			
			for (Object object : tracks) {
				
				if(object instanceof LinkedHashMap<?, ?>){
					String id = String.valueOf(((LinkedHashMap<?, ?>)object).get("id"));
					
					result.add(id);
				}
			}
			
		} catch (MalformedURLException e) {
			LOGGER.error("URL seems to be malformed: " + url);
		} catch (IOException e) {
			LOGGER.error("Could not read from URL: " + url);
		}
		
		return result;
		
	}

	public Geometry getBbox() {
		return bbox;
	}

	public void setBbox(Geometry bbox) {
		this.bbox = bbox;
	}

	public double getDistance() {
		return distance;
	}

	public void setDistance(double distance) {
		this.distance = distance;
	}
	
	public List<Point> getResultSet(){
		return pointService.getResultSet();
	}
	
}
