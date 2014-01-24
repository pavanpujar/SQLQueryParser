package edu.buffalo.cse.sql.plan;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import edu.buffalo.cse.sql.Sql;
import edu.buffalo.cse.sql.data.Datum;

public class ScanNodeEval {
	StringTokenizer stk = null;
	String values = null;
	String[] row = null;
	String rowValue = null;
	int counter = 0;

	public List<Datum[]> scanNodeFunction(PlanNode planNode, String r) {
		
		int size = planNode.getSchemaVars().size();
		List<Datum[]> tableList = new ArrayList<Datum[]>();
		Datum[] datum;
		String s = r;
		datum = new Datum[size];
		//if (fileName.endsWith(".tbl")) {
			row = s.split("[\\|]");
		//}
		/*else
		{
			row=s.split(",");
		}*/
			counter = 0;
			for (int i = 0; i < row.length; i++) {
				if (row[i].matches("[-+]?\\d+")) {
					datum[counter] = new Datum.Int(Integer.parseInt(row[i]));
					counter++;
				} else if (row[i].matches("[0-9]{4}-[01][0-9]-[0-3][0-9]")) {
					String str[] = row[i].split("[\\'-]");
					datum[counter] = new Datum.Int(Integer.parseInt(str[0]
							+ str[1] + str[2]));
					counter++;
				} else if (row[i]
						.matches("[-+]?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?")) {
					datum[counter] = new Datum.Flt(Double.parseDouble(row[i]));
					counter++;
				} else if (row[i].matches("[a-zA-Z?!,;:#[0-9]*\\-.//\\ ]+")) {
					datum[counter] = new Datum.Str(row[i]);
					counter++;
				}
			}
			tableList.add(datum);
		/*else {
			ArrayList<String> tableNames = new ArrayList<String>();
			Iterator<String> iterator = null;
			StringTokenizer stk = null;
			String values = null;
			int counter = 0;
			tableNames.add(planNode.toString().substring(6, 7));
			iterator = tableNames.iterator();
			while (iterator.hasNext()) {
				String path = Sql.gtables.get(iterator.next()).getFile()
						.getPath();
				File file = new File(path);
				try {
					s = null;
					BufferedReader br = new BufferedReader(new FileReader(file));
					try {
						while ((s = br.readLine()) != null) {
							datum = new Datum[size];
							stk = new StringTokenizer(s, ",");
							while (counter < size && stk.hasMoreTokens()) {
								datum[counter] = new Datum.Int(
										Integer.parseInt(stk.nextToken()));
								counter = counter + 1;
							}
							counter = 0;
							tableList.add(datum);
						}

					} catch (IOException e) {
						e.printStackTrace();
					}
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}
			}
		}*/
		Sql.result_tableSchema = planNode.getSchemaVars().toString().split(",");
		return tableList;
	}
}
