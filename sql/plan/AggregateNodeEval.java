package edu.buffalo.cse.sql.plan;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.Sql;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.Datum.CastError;
import edu.buffalo.cse.sql.plan.ExprTree.ConstLeaf;
import edu.buffalo.cse.sql.plan.ExprTree.OpCode;

public class AggregateNodeEval {
	int flag = 0;
	int colCount = 0;
	List<Datum[]> ls = new ArrayList<Datum[]>();
	List<Datum[]> return_list = new ArrayList<Datum[]>();
	Datum[] resultDatum = new Datum[colCount];
	Datum[] datumKey;
	String[] groupBy_Keys;
	List<Datum[]> aggregateList = new ArrayList<Datum[]>();
	HashMap<String, Datum[]> hMap = new HashMap<String, Datum[]>();
	HashMap<String, Double> row_Count = new HashMap<String, Double>();
	static Datum sum;
	Iterator<ProjectionNode.Column> iter;
	public List<Datum[]> aggregateFunction(AggregateNode an, String row) throws CastError {
		colCount = an.getAggregates().size();
		datumKey = new Datum[an.getGroupByVars().size()];
		groupBy_Keys = new String[an.getGroupByVars().size()];
		iter = an.getGroupByVars().iterator();
		int i = 0;
		String key = null;
		while (iter.hasNext()) {
			groupBy_Keys[i] = iter.next().toString().split(":")[1].trim();
			i++;
		}
		if (an.getChild().type.equals(PlanNode.Type.AGGREGATE)) {
			AggregateNode aggNode = (AggregateNode) an.getChild();
			ls = aggregateFunction(aggNode, row);
		} else if (an.getChild().type.equals(PlanNode.Type.SCAN)) {
			ScanNodeEval snEval = new ScanNodeEval();
			ls = snEval.scanNodeFunction(an.getChild(), row);
			resultDatum = new Datum[colCount];
			computeAggregate(an,"A");
		} else if (an.getChild().type.equals(PlanNode.Type.SELECT)) {
			SelectionNode sn = (SelectionNode) an.getChild();
			SelectNodeEval selectNodeEval = new SelectNodeEval();
			ls = selectNodeEval.selectionNode(sn, row);
			if (ls == null) {
				return null;
			}
			if(ls.size() > 1){
				List<Datum[]> tempList = new ArrayList<Datum[]>();
				tempList.addAll(ls);
				ls.clear();
				int listSize = tempList.size();
				for(int k = 0; k < listSize; k++){
					ls.add(tempList.get(k));
					for (int j = 0; j < groupBy_Keys.length; j++) {
						datumKey[j] = getColumnValue(groupBy_Keys[j]);
					}
					key = Datum.stringOfRow(datumKey);
					if (!hMap.containsKey(key)) {
						resultDatum = new Datum[colCount];
					}
					if (!row_Count.containsKey(key)) {
						row_Count.put(key, 1.0);
					}
					computeAggregate(an, key);
					ls.clear();
				}
			}
			else{
				if (!ls.isEmpty()) {
					for (int j = 0; j < groupBy_Keys.length; j++) {
						datumKey[j] = getColumnValue(groupBy_Keys[j]);
					}
					key = Datum.stringOfRow(datumKey);
					if (!hMap.containsKey(key)) {
						resultDatum = new Datum[colCount];
					}
					if (!row_Count.containsKey(key)) {
						row_Count.put(key, 1.0);
					}
				}
				computeAggregate(an, key);
			}		
		} else if (an.getChild().type.equals(PlanNode.Type.PROJECT)) {
			ProjectionNode pn = (ProjectionNode) an.getChild();
			ProjectionNodeEval pnEval = new ProjectionNodeEval(pn);
			ls = pnEval.projectionNode(pn, row);
		} else if (an.getChild().type.equals(PlanNode.Type.UNION)) {

		} else if (an.getChild().type.equals(PlanNode.Type.JOIN)) {
			// JoinNode jn = (JoinNode) an.getChild();
			// JoinNodeEval jnEval = new JoinNodeEval(jn);
			// ls = jnEval.joinNode(jn, row);
		} else if (an.getChild().type.equals(PlanNode.Type.NULLSOURCE)) {

		}


		return ls;
	}
	
	private void computeAggregate(AggregateNode an,String key) throws CastError{
		int ind = 0;
		while (ind < colCount && !ls.isEmpty()) {
			if (an.getAggregates().get(ind).aggType.equals(AggregateNode.AType.SUM)) {
				ExprTree exprTree = an.getAggregates().get(ind).expr;
				if (!hMap.containsKey(key)) {
					if (exprTree.size() > 0) {
						resultDatum[ind] = evaluateExpression(exprTree);
					} else {
						resultDatum[ind] = sumFunction(exprTree);
					}
				} 
				else {
					Datum[] currentValue = hMap.get(key);
					if (exprTree.size() > 0) {
						resultDatum[ind] = new Datum.Flt(currentValue[ind].toFloat() + evaluateExpression(exprTree).toFloat());
					} else {
						resultDatum[ind] = new Datum.Flt(currentValue[ind].toFloat() + sumFunction(exprTree).toFloat());
					}
				}
			} else if (an.getAggregates().get(ind).aggType.equals(AggregateNode.AType.MIN)) {
				/*
				 * String col = an.getAggregates().get(ind).expr.toString();
				 * return_list = minFunction(col);
				 */
			} else if (an.getAggregates().get(ind).aggType.equals(AggregateNode.AType.MAX)) {
				/*
				 * String col = an.getAggregates().get(ind).expr.toString();
				 * return_list = maxFunction(col);
				 */
			} else if (an.getAggregates().get(ind).aggType.equals(AggregateNode.AType.COUNT)) {
				resultDatum[ind] = new Datum.Flt(row_Count.get(key));

			} else if (an.getAggregates().get(ind).aggType.equals(AggregateNode.AType.AVG)) {
				ExprTree exprTree = an.getAggregates().get(ind).expr;
				if (!hMap.containsKey(key)) {
					if (exprTree.size() > 0) {
						resultDatum[ind] = evaluateExpression(exprTree);
					} else {
						resultDatum[ind] = sumFunction(exprTree);
					}
				} else {
					Double count = row_Count.get(key);
					Datum[] currentValue = hMap.get(key);
					if (exprTree.size() > 0) {
						resultDatum[ind] = new Datum.Flt(((currentValue[ind].toFloat() * (count - 1)) + evaluateExpression(exprTree).toFloat())	/ count);
					} else {
						resultDatum[ind] = new Datum.Flt(((currentValue[ind].toFloat() * (count - 1)) + sumFunction(exprTree).toFloat()) / count);
					}
				}
			}
			ind++;
			flag = 1;
		}
		if (key != null) {
			Datum[] finalResult = new Datum[resultDatum.length + datumKey.length];
			for(int i = 0; i < resultDatum.length;i++)
			{
				finalResult[i] = resultDatum[i];
			}
			int j = 0;
			for(int i = resultDatum.length; i < finalResult.length; i++){
				finalResult[i] = datumKey[j];
				j++;
			}
			hMap.put(key, finalResult);
			resultDatum = new Datum[colCount];
			row_Count.put(key, row_Count.get(key) + 1.0);
		}

	}

	public List<Datum[]> getResult() {
		List<Datum[]> ret_list = new ArrayList<Datum[]>();
		Iterator<String> iter = hMap.keySet().iterator();
		while (iter.hasNext()) {
			ret_list.add(hMap.get(iter.next()));
		}
		return ret_list;
	}

	public Datum sumFunction(ExprTree exprTree) {
		Datum result = new Datum.Flt(0.0);
		String colName = exprTree.toString();
		int exprLen = exprTree.size();
		if (exprLen == 0) {
			result = getColumnValue(colName);
		}
		return result;
	}

	public List<Datum[]> avgFunction(String colName) {
		Double avg = 0.0;
		Double sum = 0.0;
		int opval = 0;
		int operatorCount = 0;
		List<Datum[]> avgList = new ArrayList<Datum[]>();
		String[] columnName = new String[colName.length()];

		if (colName.length() > 1) {
			colName = colName.substring(1, colName.length() - 1);
			columnName = colName.split(" ");
			for (int i = 0; i < columnName.length; i++) {
				if (columnName[i].trim().matches("[+]")) {
					opval = 1;
					operatorCount++;
					continue;
				} else if (columnName[i].trim().matches("[*]")) {
					opval = 2;
					operatorCount++;
					continue;
				}
			}
		}
		if (columnName[0] == null) {
			int col_num = 0;
			for (int j = 0; j < Sql.result_tableSchema.length; j++) {
				if (Sql.result_tableSchema[j].trim().contains(colName)) {
					col_num = j;
					break;
				}
			}

			for (int i = 0; i < ls.size(); i++) {
				try {
					sum = sum + ls.get(i)[col_num].toFloat();
				} catch (CastError e) {

					e.printStackTrace();
				}
			}
			avg = sum / ls.size();
			if (flag == 0)
				avgList.add(new Datum[] { new Datum.Flt(avg) });
			else {
				avgList.add(new Datum[] { return_list.get(0)[0],
						new Datum.Flt(avg) });
			}
			return avgList;
		} else {
			int ctr = 0;
			int[] col_no = new int[columnName.length - operatorCount];
			for (int k = 0; k < columnName.length; k++) {
				for (int j = 0; j < Sql.result_tableSchema.length; j++) {
					if (Sql.result_tableSchema[j].trim()
							.contains(columnName[k])) {
						col_no[ctr] = j;
						ctr++;
						break;
					}
				}
				if (ctr == (columnName.length - operatorCount))
					break;
			}

			Double rowWiseSum = 0.0;
			for (int i = 0; i < ls.size(); i++) {
				try {
					if (opval == 1) {
						for (int k = 0; k < col_no.length; k++) {
							rowWiseSum = rowWiseSum
									+ ls.get(i)[col_no[k]].toFloat();
						}
						sum = sum + rowWiseSum;
						rowWiseSum = 0.0;
					} else if (opval == 2) {
						rowWiseSum = 1.0;
						for (int k = 0; k < col_no.length; k++) {
							rowWiseSum = rowWiseSum
									* ls.get(i)[col_no[k]].toFloat();
						}
						sum = sum + rowWiseSum;
						rowWiseSum = 0.0;
					}
				} catch (CastError e) {

					e.printStackTrace();
				}
			}
			avg = sum / ls.size();
			if (flag == 0)
				avgList.add(new Datum[] { new Datum.Flt(avg) });
			else {
				avgList.add(new Datum[] { return_list.get(0)[0],
						new Datum.Flt(avg) });
			}
			return avgList;
		}
	}

	public List<Datum[]> minFunction(String colName) {
		int minimum = 2147483647;
		int col_no = 0;
		List<Datum[]> minList = new ArrayList<Datum[]>();
		for (int j = 0; j < Sql.result_tableSchema.length; j++) {
			if (Sql.result_tableSchema[j].trim().contains(colName)) {
				col_no = j;
				break;
			}
		}

		for (int i = 0; i < ls.size(); i++) {
			try {
				if (minimum > ls.get(i)[col_no].toInt())
					minimum = ls.get(i)[col_no].toInt();

			} catch (CastError e) {

				e.printStackTrace();
			}
		}

		minList.add(new Datum[] { new Datum.Int(minimum) });
		return minList;

	}

	public List<Datum[]> maxFunction(String colName) {
		int maximum = 0;
		int col_no = 0;
		List<Datum[]> maxList = new ArrayList<Datum[]>();
		for (int j = 0; j < Sql.result_tableSchema.length; j++) {
			if (Sql.result_tableSchema[j].trim().contains(colName)) {
				col_no = j;
				break;
			}
		}

		for (int i = 0; i < ls.size(); i++) {
			try {
				if (maximum < ls.get(i)[col_no].toInt())
					maximum = ls.get(i)[col_no].toInt();

			} catch (CastError e) {

				e.printStackTrace();
			}
		}

		maxList.add(new Datum[] { new Datum.Int(maximum) });
		return maxList;

	}

	public Datum countFunction() {
		int cnt = 0;
		cnt = ls.size();
		Datum count = new Datum.Int(cnt);
		return count;
	}

	public Datum getColumnValue(String col_Name) {
		int col_num = 0;
		Datum col_Value = new Datum.Int(0);
		for (int j = 0; j < Sql.result_tableSchema.length; j++) {
			if (Sql.result_tableSchema[j].trim().contains(col_Name)) {
				col_num = j;
				break;
			}
		}
		Schema.Type t = ls.get(0)[col_num].getType();
		if (t.equals(Schema.Type.INT)) {
			col_Value = new Datum.Int(0);
			col_Value = ls.get(0)[col_num];
		} else if (t.equals(Schema.Type.FLOAT)) {
			col_Value = new Datum.Flt(0);
			col_Value = ls.get(0)[col_num];
		} else {
			col_Value = new Datum.Str(null);
			col_Value = ls.get(0)[col_num];
		}
		return col_Value;
	}

	public Datum evaluateExpression(ExprTree exprTree) {
		ExprTree exprTreeLeft;
		ExprTree exprTreeRight;
		Datum lValue = new Datum.Int(0);
		Datum rValue = new Datum.Int(0);
		Datum result = new Datum.Int(0);
		exprTreeLeft = exprTree.get(0);
		exprTreeRight = exprTree.get(1);
		if (exprTreeLeft.size() > 0) {
			lValue = evaluateExpression(exprTreeLeft);
		} else {
			if (exprTreeLeft.op.equals(OpCode.VAR)) {
				lValue = getColumnValue(exprTreeLeft.toString());
			} else {
				ConstLeaf constLeaf = (ConstLeaf) exprTreeLeft;
				lValue = constLeaf.v;
			}
		}
		if (exprTreeRight.size() > 0) {
			rValue = evaluateExpression(exprTreeRight);
		} else {
			if (exprTreeRight.op.equals(OpCode.VAR)) {
				rValue = getColumnValue(exprTreeRight.toString());
			} else {
				ConstLeaf constLeaf = (ConstLeaf) exprTreeRight;
				rValue = constLeaf.v;
			}
		}
		OpCode opCode = exprTree.op;
		result = compute(lValue, rValue, opCode);
		return result;
	}

	public Datum compute(Datum left, Datum right, OpCode operator) {
		Datum res = new Datum.Flt(0);
		double result_Value;
		try {
			if (operator.equals(OpCode.SUB)) {

				result_Value = left.toFloat() - right.toFloat();
				res = new Datum.Flt(result_Value);

			} else if (operator.equals(OpCode.MULT)) {
				result_Value = left.toFloat() * right.toFloat();
				res = new Datum.Flt(result_Value);
			} else if (operator.equals(OpCode.ADD)) {
				result_Value = left.toFloat() + right.toFloat();
				res = new Datum.Flt(result_Value);
			} else if (operator.equals(OpCode.DIV)) {
				result_Value = left.toFloat() / right.toFloat();
				res = new Datum.Flt(result_Value);
			}
		} catch (CastError e) {
			e.printStackTrace();
		}
		return res;
	}
}
