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
package org.envirocar.analyse.util;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.envirocar.analyse.entities.InMemoryPoint;
import org.envirocar.analyse.entities.Point;


public class PointViaJsonMapIterator implements Iterator<Point> {

	private List<Object> features;
	private int index = 0;
	private String trackID;

	@SuppressWarnings("unchecked")
	public PointViaJsonMapIterator(Map<?, ?> json) {
		this.features = (List<Object>) json.get("features");
		Map<?, ?> properties = (Map<?, ?>) json.get("properties");
		if (properties != null) {
			this.trackID = (String) properties.get("id");
		}

		if (this.features == null || this.features.isEmpty()
				|| this.trackID == null) {
			throw new IllegalArgumentException("Not a valid enviroCar track");
		}
	}

	@Override
	public boolean hasNext() {
		return index < features.size();
	}

	@Override
	public Point next() {
		if (!hasNext()) {
			return null;
		}
		return InMemoryPoint.fromMap((Map<?, ?>) features.get(index++),
				this.trackID);
	}

	@Override
	public void remove() {
	}

	public String getOriginalTrackId() {
		return this.trackID;
	}

}
