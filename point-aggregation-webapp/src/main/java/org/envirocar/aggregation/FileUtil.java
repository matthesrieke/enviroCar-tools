/**
 * Copyright 2014 52°North Initiative for Geospatial Open Source
 * Software GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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