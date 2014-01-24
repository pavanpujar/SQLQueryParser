package edu.buffalo.cse.sql.plan;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import edu.buffalo.cse.sql.Sql;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.Datum.CastError;

public class ProjectionNodeEval {
	ProjectionNode pn;
	int flag = 0;
	List<Datum[]> resultList;
	List<Datum[]> return_list;
	List<Datum[]> ls = new ArrayList<Datum[]>();

	public ProjectionNodeEval(ProjectionNode pNode) {
		this.pn = pNode;
		int columnSize = pn.getColumns().size();
	}

	public List<Datum[]> projectionNode(ProjectionNode pn,String row) throws CastError {
		if (pn.getChild().type.equals(PlanNode.Type.NULLSOURCE)) {
			int ctr = 0;
			int num_Of_Colums = pn.getColumns().size();
			Datum[] resultDatum = new Datum[num_Of_Colums];
			
			while (ctr < num_Of_Colums) {
				String temp = pn.getColumns().get(ctr).expr.toString();
				if (temp.matches("[-+]?[0-9]")) {
					resultDatum[ctr] = new Datum.Int(Integer.parseInt(temp));
				} else if (temp
						.matches("[-+]?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?")) {
					resultDatum[ctr] = new Datum.Flt(Double.parseDouble(temp));
				}
				else if (temp.matches("^'+[a-zA-Z._^%$#!~@,-\\\\\']+'$")) {
					resultDatum[ctr] = new Datum.Str(temp.substring(1,
							temp.length() - 1));
				} else if (temp.matches("true|false")) {
					
					resultDatum[ctr] = new Datum.Bool(
							Boolean.parseBoolean((temp)));
				} else if (temp.matches("^\\([\\w\\s]+\\)$")) {
					
					ScriptEngineManager sem = new ScriptEngineManager();
					ScriptEngine scriptEngine = sem.getEngineByName("js");
					if (temp.contains("AND"))
						temp = temp.replace("AND", "&&");
					if (temp.contains("OR"))
						temp = temp.replace("OR", "||");
					if (temp.contains("NOT"))
						temp = temp.replace("NOT", "!");
					try {
						Boolean obj = (Boolean) scriptEngine.eval(temp);
						resultDatum[ctr] = new Datum.Bool(obj);
					} catch (ScriptException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} else if (temp.matches("^\\([(\\s0-9+*/\\-)?]+\\)$")) {
					
					ScriptEngineManager sem = new ScriptEngineManager();
					ScriptEngine scriptEngine = sem.getEngineByName("js");
					try {
						Double obj = (Double) (scriptEngine.eval(temp));
						resultDatum[ctr] = new Datum.Int(obj.intValue());
					} catch (ScriptException e) {
						e.printStackTrace();
					}
				} else if (temp.matches("^\\([(\\s0.0-9.0+*/\\-)?]+\\)$")) {
					
					ScriptEngineManager sem = new ScriptEngineManager();
					ScriptEngine scriptEngine = sem.getEngineByName("js");
					try {
						Double obj = (Double) (scriptEngine.eval(temp));
						resultDatum[ctr] = new Datum.Flt(obj.floatValue());
					} catch (ScriptException e) {
						e.printStackTrace();
					}
				} else{					
					resultDatum[ctr] = null;}
				ctr++;
			}
			ls.add(resultDatum);
		} else {
			if (pn.getChild().type.equals(PlanNode.Type.JOIN)) {
				JoinNode joinNode = (JoinNode) pn.getChild();
				JoinNodeEval joinNodeEval = new JoinNodeEval(joinNode);
				//resultList = joinNodeEval.joinNode(joinNode,row);
				String columnName = pn.getColumns().toString().trim();
				return_list = projectFn(columnName);
				return return_list;

			} else if (pn.getChild().type.equals(PlanNode.Type.SCAN)) {
				ScanNode scanNode = (ScanNode) pn.getChild();
				ScanNodeEval scanNodeEval = new ScanNodeEval();
				resultList = scanNodeEval.scanNodeFunction(scanNode,row);
				String columnName = pn.getColumns().toString();
				return_list = projectFn(columnName);
				return return_list;

			} else if (pn.getChild().type.equals(PlanNode.Type.SELECT)) {
				SelectionNode selectionNode = (SelectionNode) pn.getChild();
			    SelectNodeEval selectNodeEval = new SelectNodeEval();
				resultList = selectNodeEval.selectionNode(selectionNode,null);
				String columnName = pn.getColumns().toString();
				return_list = projectFn(columnName);
				return return_list;
			}
		}
		return ls;
	}

	public List<Datum[]> projectFn(String columnName) {

		columnName = columnName.substring(1, columnName.length() - 1);
		String[] column = columnName.split(" ");
		for (int i = 0; i < column.length; i++) {
			if (column[i].contains(",")) {
				column[i] = column[i].substring(0, column[i].length() - 1);
			}
		}

		int[] col_no = new int[pn.getColumns().size()];
		int ctr = 0;
		for (int j = 0; j < Sql.result_tableSchema.length; j++) {
			for (int i = 0; i < column.length; i++) {
				if (Sql.result_tableSchema[j].trim().contains(column[i])) {
					col_no[ctr] = j;
					ctr++;
					break;
				}
			}
		}
		if (col_no.length > 1)
			flag = 1;
		Datum[] elementHolder;
		for (int i = 0; i < resultList.size(); i++) {
			elementHolder = new Datum[col_no.length];
			for (int k = 0; k < col_no.length; k++) {
				elementHolder[k] = resultList.get(i)[col_no[k]];
			}
			ls.add(elementHolder);
		}
		return ls;
	}
}
