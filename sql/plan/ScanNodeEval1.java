package edu.buffalo.cse.sql.plan;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import edu.buffalo.cse.sql.Sql;
import edu.buffalo.cse.sql.data.Datum;

public class ScanNodeEval1 {
	ArrayList<String> tableNames = new ArrayList<String>();
	Iterator<String> iterator = null;
	StringTokenizer stk = null;
	String values = null;

	public List<Datum[]> scanNodeFunction(PlanNode planNode) {
		int size = planNode.getSchemaVars().size();
		int counter = 0;
		List<Datum[]> tableList = new ArrayList<Datum[]>();
		Datum[] datum;
		tableNames.add(planNode.toString().substring(6, 7));
		iterator = tableNames.iterator();
		while (iterator.hasNext()) {
			String path = Sql.gtables.get(iterator.next()).getFile().getPath();
			File file = new File(path);
			try {
				String s = null;

				BufferedReader br = new BufferedReader(new FileReader(file));
				try {
					while ((s = br.readLine()) != null) {
						datum = new Datum[size];
						stk = new StringTokenizer(s, ",");
						while (counter < size && stk.hasMoreTokens()) {
							datum[counter] = new Datum.Int(Integer.parseInt(stk
									.nextToken()));
							counter = counter + 1;
						}
						counter = 0;
						tableList.add(datum);
					}

				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			Sql.result_tableSchema = planNode.getSchemaVars().toString()
					.split(",");
			return tableList;
		}
		return null;
	}
}
