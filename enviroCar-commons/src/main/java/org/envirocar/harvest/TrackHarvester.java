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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.envirocar.json.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TrackHarvester extends TrackPublisher {

	private static final Logger logger = LoggerFactory
			.getLogger(TrackHarvester.class);

	private static final String BASE_TRACKS = "https://envirocar.org/api/stable/tracks/";

	private String baseTracks;

	private ProgressListener progressListener;

	private int trackCount;
	private int processedTrackCount;

	private List<TrackFilter> filters = new ArrayList<TrackFilter>();
	
	public TrackHarvester(String consumerUrl, ProgressListener l, String baseTrackUrl) {
		super(consumerUrl);
		this.progressListener = l;
		this.baseTracks = baseTrackUrl;
	}
	
	public TrackHarvester(String consumerUrl, ProgressListener l) {
		this(consumerUrl, l, BASE_TRACKS);
	}

	public void harvestTracks() throws ClientProtocolException,
			IOException {
		HttpClient client = createClient();
		
		trackCount = resolveTrackCount(client);
		
		int page = 1;
		HttpResponse resp = client.execute(createRequest(baseTracks, page));
		Map<?, ?> json = JsonUtil.createJson(resp.getEntity().getContent());
		
		while (((List<?>) json.get("tracks")).size() > 0) {
			logger.info("Processing page {}", page);
			processTracks(json);
			page++;
			resp = client.execute(createRequest(baseTracks, page));
			json = JsonUtil.createJson(resp.getEntity().getContent());
		}
		
		logger.info("finished pushing tracks.");
		
	}

	private int resolveTrackCount(HttpClient client) throws IllegalStateException, IOException {
		int page = 1;
		int trackCount = 0;
		HttpResponse resp = client.execute(createRequest(baseTracks, page));
		Map<?, ?> json = JsonUtil.createJson(resp.getEntity().getContent());
		
		List<?> tmpTrackList = ((List<?>) json.get("tracks"));
		while (tmpTrackList != null && tmpTrackList.size() > 0) {
			logger.info("Retrieving page {}", page);
			trackCount += tmpTrackList.size();
			page++;
			resp = client.execute(createRequest(baseTracks, page));
			json = JsonUtil.createJson(resp.getEntity().getContent());
			tmpTrackList = ((List<?>) json.get("tracks"));
		}
		
		return trackCount;
	}

	private HttpUriRequest createRequest(String url, int page) {
		return new HttpGet(url + "?limit=100&page="+ page);
	}

	private void processTracks(Map<?, ?> json)
			throws ClientProtocolException, IOException {
		List<?> tracks = (List<?>) json.get("tracks");

		for (Object t : tracks) {
			String id = (String) ((Map<?, ?>) t).get("id");
			readAndPushTrack(id);
			processedTrackCount++;
			progressListener.onProgressUpdate(calculateProgress());
		}

	}

	private float calculateProgress() {
		float progress = (processedTrackCount / (float) trackCount) * 100f;
		return progress;
	}

	public void readAndPushTrack(String id)
			throws ClientProtocolException, IOException {
		HttpClient client = createClient();
		HttpResponse resp = client.execute(new HttpGet(baseTracks.concat(id)));
		String content = readContent(resp.getEntity().getContent());

		logger.info("Pushing track '{}' to {}.", id, targetConsumer);
		pushToConsumer(applyFilters(content));
	}

	private String applyFilters(String content) {
		if (this.filters != null && this.filters.size() > 0) {
			for (TrackFilter f : this.filters) {
				if (!f.accepts(content)) {
					return null;
				}
			}
		}
		return content;
	}
	


}
