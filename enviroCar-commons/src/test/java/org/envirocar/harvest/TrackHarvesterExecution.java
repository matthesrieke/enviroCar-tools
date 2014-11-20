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
package org.envirocar.harvest;

import java.io.IOException;

import org.apache.http.client.ClientProtocolException;
import org.envirocar.harvest.ProgressListener;
import org.envirocar.harvest.TrackHarvester;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TrackHarvesterExecution {
	
	private static final Logger logger = LoggerFactory.getLogger(TrackHarvesterExecution.class);


	public static void main(String[] args) throws ClientProtocolException,
			IOException {
		String consumerUrl = null;
		if (args != null && args.length > 0) {
			consumerUrl = args[0].trim();
		}
		else {
			throw new IllegalArgumentException("consumerUrl needs to be provided");
		}
		
		ProgressListener l = new ProgressListener() {

			@Override
			public void onProgressUpdate(float progressPercent) {
				logger.info(String.format("%f percent finished", progressPercent));
			}
			
		};
		new TrackHarvester(consumerUrl, l).harvestTracks();
	}
	
//	@Test
	public void execute() throws ClientProtocolException, IOException {
		ProgressListener l = new ProgressListener() {

			@Override
			public void onProgressUpdate(float progressPercent) {
				System.out.println(String.format("%f percent finished", progressPercent));
			}
			
		};
		new TrackHarvester("http://localhost:8080/enviroCar-broker/", l).harvestTracks();
	}
	
}
