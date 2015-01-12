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
package org.envirocar.aggregation;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

public class FileUtil {

	public static Set<String> readConfigFilePerLine(String resourcePath)
			throws IOException {
		URL resURL = FileUtil.class.getResource(resourcePath);
		URLConnection resConn = resURL.openConnection();
		resConn.setUseCaches(false);
		InputStream contents = resConn.getInputStream();

		Scanner sc = new Scanner(contents);
		Set<String> result = new HashSet<String>();

		while (sc.hasNext()) {
			String line = sc.nextLine();
			if ((line != null) && (!line.isEmpty()) && (!line.startsWith("#"))) {
				result.add(line.trim());
			}
		}
		sc.close();

		return result;
	}
}