/*******************************************************************************
 * Copyright (c) 2014 bankmark UG 
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream; 
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.hive.contrib.mr.GenericMR;
import org.apache.hadoop.hive.contrib.mr.Output;
import org.apache.hadoop.hive.contrib.mr.Reducer;

/**
 * @author Michael Frank <michael.frank@bankmark.de>
 * @version 1.0 25.04.2014
 */
public class Red {

	public static final char SEPARATOR = '\t';

	public static void main(final String[] args) throws Exception {
		boolean TEST = false;

		boolean doGeneric = false;
		int maxItems = Integer.MAX_VALUE;
		for (int i = 0; i < args.length; i++) {
			if (args[i].equalsIgnoreCase("-generic")) {
				doGeneric = true;
		} else if (args[i].equalsIgnoreCase("-test")) {
				TEST = true;
			} else if (args[i].equalsIgnoreCase("-printTestData")) {
				System.out.println(makeTestData());
			} else if (args[i].equalsIgnoreCase("-ITEM_SET_MAX")) {
				maxItems = Integer.parseInt(args[++i]);
			}
		}

		InputStream in;
		OutputStream out;
		if (TEST) {
			String testData = makeTestData();
			System.out.println("INPUT DATA\n=========");
			System.out.println(testData);

			out = new ByteArrayOutputStream();
			in = new ByteArrayInputStream(testData.getBytes());

		} else {
			in = System.in;
			out = System.out;
		}

		if (doGeneric) {
			doGenericMR(in, out, maxItems);
		} else {
			new Red(in, out, maxItems).startCustomReducer();
		}
		if (TEST) {
			System.out.println("\nOUTPUT:");
			System.out.println(new String(((ByteArrayOutputStream) out)
					.toByteArray()));
		}

	}

	private String oldKey = "";
	private ArrayList<String> vals = new ArrayList<String>();
	private BufferedReader in;
	private BufferedWriter out;
	private int maxItems;
	private int maxListSize;

	public Red(InputStream in, OutputStream out, int maxItems) {
		this.in = new BufferedReader(new InputStreamReader(in));
		this.out = new BufferedWriter(new OutputStreamWriter(out));
		this.maxItems = maxItems;
		this.maxListSize = calcMaxListItems(maxItems);
	}

	public void startCustomReducer() throws IOException {
		try {
			BufferedReader in = this.in;
			String line;

			while ((line = in.readLine()) != null) {
				customReducerDoLine(line);
			}
			// finish last batch of accumulated values
			customReduce();
		} finally {
			out.close();
		}

	}

	private void customReducerDoLine(String line) throws IOException {
		line = line.trim();
		int i = line.indexOf(SEPARATOR);
		if (i < 0) { // bad line
			return;
		}
		String key = line.substring(0, i);
		String value = line.substring(i + 1, line.length());
		boolean omitPartition = false;
		if (key.equals(oldKey)) {
			if (!omitPartition) {
				// eager apply size restriction to avoid out of memory error
				if (vals.size() >= maxListSize) {
					// drop list and skip all following items for that key.
					vals = new ArrayList<String>();
					omitPartition = true;
				} else {

					vals.add(value);
				}
			}
		} else {
			// size restriction, omit this partition if it exceeds the specified
			// size
			if (!omitPartition) {
				customReduce(); // flush accumulated values
			}
			omitPartition = false;
			oldKey = key;
			vals = null;
			vals = new ArrayList<String>();
			vals.add(value);
		}
	}

	private static int calcCombinations(int l) {
		return l * (l - 1) / 2;
	}

	private static int calcMaxListItems(int x) {
		return (int) (Math.sqrt(8L * x + 1) + 1) / 2;
	}

	private void customReduce() throws IOException {
		ArrayList<String> vals = this.vals;

		int l = vals.size();
		if (l <= 1) {
			return;
		}
		Collections.sort(vals);
		for (int i = 0; i < l - 1; i++) {
			for (int j = i + 1; j < l; j++) {
				writeRecord(vals.get(i), vals.get(j));
			}
		}
	}

	private void writeRecord(String x, String y) throws IOException {
		out.write(x);
		out.write(SEPARATOR);
		out.write(y);
		out.write('\n');
	}

	private static String makeTestData() {
		String[][] test = new String[][] { { "1", "a" }, { "1", "b" },
				{ "1", "c" }, { "1", "d" }, { "1", "e" }, { "2", "aa" },
				{ "2", "bb" }, { "2", "cc" }, { "3", "abc" } };
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < test.length; i++) {
			for (int j = 0; j < test[i].length; j++) {
				sb.append(test[i][j]);
				if (j < test[i].length - 1)
					sb.append(SEPARATOR);
			}
			if (i < test.length - 1)
				sb.append('\n');
		}
		return sb.toString();
	}

	private static void doGenericMR(final InputStream in,
			final OutputStream out, final int maxItems) throws Exception {
		final int maxListSize = calcMaxListItems(maxItems);
		new GenericMR().reduce(in, out, new Reducer() {

			@Override
			public void reduce(String key, Iterator<String[]> vals, Output out)
					throws Exception {
				ArrayList<String> list = new ArrayList<String>();

				while (vals.hasNext()) {
					// eager apply size restriction to avoid out of memory error
					if (list.size() > maxListSize) {
						return;
					}
					String[] tmp = vals.next(); // []={key, val,...}
					if (tmp.length > 2) {
						throw new IllegalArgumentException(
								"To much values. Expected {key, value} but was:"
										+ Arrays.toString(tmp));
					}
					String val = tmp[1];
					list.add(val);
				}

				// size restriction
				int l = list.size();
				if (l <= 1) {
					return;
				}

				Collections.sort(list);
				for (int i = 0; i < l - 1; i++) {
					for (int j = i + 1; j < l; j++) {
						out.collect(new String[] { list.get(i), list.get(j) });
					}
				}
			}
		});
	}
}
