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

import org.apache.http.client.ClientProtocolException;
import org.envirocar.harvest.ProgressListener;
import org.envirocar.harvest.TrackHarvester;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AggregateAllTracksTest {

	private static final Logger logger = LoggerFactory.getLogger(AggregateAllTracksTest.class);
	
	@Test
	public void harvest() throws ClientProtocolException, IOException {
		final AggregationAlgorithm algo = new AggregationAlgorithm();
		TrackHarvester harv = new TrackHarvester("", new ProgressListener() {
			@Override
			public void onProgressUpdate(float progressPercent) {
				logger.info("Progress: "+progressPercent);
			}
		}) {
			@Override
			public void readAndPushTrack(String id)
					throws ClientProtocolException, IOException {
				try {
					algo.runAlgorithm(id);
				} 
				catch (IOException e) {
					logger.warn(e.getMessage(), e);
				}
			}
		};
		
		harv.harvestTracks();
	}
	
}
