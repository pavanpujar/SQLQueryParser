package edu.buffalo.cse.sql.plan;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.Sql;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.Datum.CastError;
import edu.buffalo.cse.sql.plan.JoinNode.JType;

public class JoinNodeEval {
	JoinNode jNode;
	List<Datum[]> listLeftCopy = new ArrayList<Datum[]>();
	List<Datum[]> listRightCopy = new ArrayList<Datum[]>();
	List<Datum[]> left_subset = new ArrayList<Datum[]>();
	List<Datum[]> right_subset = new ArrayList<Datum[]>();
	int left_key = 0;
	int right_key = 0;
	JoinNode tempJoinNode;

	public JoinNodeEval(JoinNode jn) {
		this.jNode = jn;
	}

	public List<Datum[]> joinNode(JoinNode joinNode, String row, ExprTree exprTree) throws CastError {
		tempJoinNode = joinNode;
		List<Datum[]> tableListLeft = new ArrayList<Datum[]>();
		List<Datum[]> tableListRight = new ArrayList<Datum[]>();
		List<Datum[]> resultTable = new ArrayList<Datum[]>();
		Datum[] rDatum;
		if (joinNode.getLHS().type.equals(PlanNode.Type.JOIN)) {
			PlanNode planNode = joinNode.getLHS();
			JoinNode jnNodeLeft = (JoinNode) planNode;
			tableListLeft = joinNode(jnNodeLeft, row, exprTree);
		}
		if (joinNode.getRHS().type.equals(PlanNode.Type.JOIN)) {
			PlanNode planNode = joinNode.getRHS();
			JoinNode jnNodeRight = (JoinNode) planNode;
			tableListRight = joinNode(jnNodeRight, row, exprTree);
		}
		if (joinNode.getLHS().type.equals(PlanNode.Type.SCAN)) {
			ScanNodeEval scanNodeEvalLeft = new ScanNodeEval();
			ScanNode scanNode = (ScanNode) joinNode.getLHS();
			List<Datum[]> tempList = new ArrayList<Datum[]>();
			BufferedReader bufferedReader = null;
			bufferedReader = getReader(scanNode);
			try {
				while ((row = bufferedReader.readLine()) != null) {
					tempList = scanNodeEvalLeft.scanNodeFunction(scanNode, row);
					if (tempList != null && !tempList.isEmpty()) {
						tableListLeft.add(tempList.get(0));
					}
				}
			} catch (IOException e) {

				e.printStackTrace();
			}
			if (joinNode.type.equals(JType.MERGE)) {
				//List<Datum[]> tempList1 = sortList(tableListLeft,exprTree.get(0).toString());
				//Collections.copy(tableListLeft, tempList1);
				listLeftCopy = tableListLeft;
			}
		}
		if (joinNode.getRHS().type.equals(PlanNode.Type.SCAN)) {
			ScanNodeEval scanNodeEvalRight = new ScanNodeEval();
			ScanNode scanNode = (ScanNode) joinNode.getRHS();
			List<Datum[]> tempList = new ArrayList<Datum[]>();
			BufferedReader bufferedReader = null;
			bufferedReader = getReader(scanNode);
			try {
				while ((row = bufferedReader.readLine()) != null) {
					tempList = scanNodeEvalRight
							.scanNodeFunction(scanNode, row);
					if (tempList != null && !tempList.isEmpty()) {
						tableListRight.add(tempList.get(0));
					}
				}
			} catch (IOException e) {

				e.printStackTrace();
			}
			if (joinNode.type.equals(JType.MERGE)) {
				//List<Datum[]> tempList1 = sortList(tableListRight, exprTree.get(1).toString());
				//Collections.copy(tableListRight, tempList1);
				listRightCopy = tableListRight;
				//System.out.println(".");
			}
		}
		if (joinNode.getLHS().type.equals(PlanNode.Type.SELECT)) {
			SelectNodeEval selectNodeEvalLeft = new SelectNodeEval();
			SelectionNode selectNode = (SelectionNode) joinNode.getLHS();
			try {
				if (selectNode.getChild().struct
						.equals(PlanNode.Structure.LEAF)) {
					List<Datum[]> tempList = new ArrayList<Datum[]>();
					BufferedReader bufferedReader = null;
					bufferedReader = getReader(selectNode);
					try {
						while ((row = bufferedReader.readLine()) != null) {
							tempList = selectNodeEvalLeft.selectionNode(
									selectNode, row);
							if (tempList != null && !tempList.isEmpty()) {
								tableListLeft.add(tempList.get(0));
							}
						}
					} catch (IOException e) {

						e.printStackTrace();
					}

				} else {
					tableListLeft = selectNodeEvalLeft.selectionNode(
							selectNode, row);
				}
			} catch (CastError e) {

				e.printStackTrace();
			}
			if (joinNode.type.equals(JType.MERGE)) {
				//List<Datum[]> tempList = sortList(tableListLeft, exprTree.get(0).toString());
				//Collections.copy(tableListLeft, tempList);
				listLeftCopy = tableListLeft;
				if(!selectNode.getSchemaVars().toString().contains(exprTree.get(0).toString())){
					ExprTree tempTree = exprTree.get(0);
					exprTree.set(0, exprTree.get(1));
					exprTree.set(1, tempTree);
				}
			}
		}
		if (joinNode.getRHS().type.equals(PlanNode.Type.SELECT)) {
			SelectNodeEval selectNodeEvalRight = new SelectNodeEval();
			SelectionNode selectNode = (SelectionNode) joinNode.getRHS();
			try {
				if (selectNode.getChild().struct
						.equals(PlanNode.Structure.LEAF)) {
					List<Datum[]> tempList = new ArrayList<Datum[]>();
					BufferedReader bufferedReader = null;
					bufferedReader = getReader(selectNode);
					try {
						while ((row = bufferedReader.readLine()) != null) {
							tempList = selectNodeEvalRight.selectionNode(
									selectNode, row);
							if (tempList != null && !tempList.isEmpty()) {
								tableListRight.add(tempList.get(0));
							}
						}
					} catch (IOException e) {

						e.printStackTrace();
					}

				} else {
					tableListRight = selectNodeEvalRight.selectionNode(
							selectNode, row);
				}
			} catch (CastError e) {
				e.printStackTrace();
			}
			if (joinNode.type.equals(JType.MERGE)) {
				//List<Datum[]> tempList = sortList(tableListRight,exprTree.get(1).toString());
				//Collections.copy(tableListRight, tempList);
				listRightCopy = tableListRight;
				if(!selectNode.getSchemaVars().toString().contains(exprTree.get(1).toString())){
					ExprTree tempTree = exprTree.get(0);
					exprTree.set(0, exprTree.get(1));
					exprTree.set(1, tempTree);
				}
			}
		}
		
		if (joinNode.type.equals(JType.NLJ)) {
			for (int i = 0; i < tableListLeft.size(); i++) {
				for (int j = 0; j < tableListRight.size(); j++) {
					int len1 = tableListLeft.get(0).length;
					int len2 = tableListRight.get(0).length;
					rDatum = new Datum[len1 + len2];
					int counter = 0;
					for (int k = 0; k < len1; k++) {
						rDatum[counter] = tableListLeft.get(i)[k];
						counter = counter + 1;
					}
					for (int k = 0; k < len2; k++) {
						rDatum[counter] = tableListRight.get(j)[k];
						counter = counter + 1;
					}
					resultTable.add(rDatum);
				}
			}
		} else if (joinNode.type.equals(JType.MERGE)) {
			Sql.result_tableSchema = joinNode.getLHS().getSchemaVars().toString().split(",");
			listLeftCopy = sortList(tableListLeft,exprTree.get(0).toString());
			Sql.result_tableSchema = joinNode.getRHS().getSchemaVars().toString().split(",");
			listRightCopy = sortList(tableListRight,exprTree.get(1).toString());
			Sql.result_tableSchema = joinNode.getLHS().getSchemaVars().toString().split(",");
			advance(left_subset, listLeftCopy, left_key, exprTree.get(0).toString(), 0);
			Sql.result_tableSchema = joinNode.getRHS().getSchemaVars().toString().split(",");
			advance(right_subset, listRightCopy, right_key, exprTree.get(1).toString(), 1);
			while (!(left_subset.isEmpty()) && !(right_subset.isEmpty())) {
				if (left_key == right_key) {
					for (int i = 0; i < left_subset.size(); i++) {
						for (int j = 0; j < right_subset.size(); j++) {
							int len1 = left_subset.get(0).length;
							int len2 = right_subset.get(0).length;
							rDatum = new Datum[len1 + len2];
							int counter = 0;
							for (int k = 0; k < len1; k++) {
								rDatum[counter] = left_subset.get(i)[k];
								counter = counter + 1;
							}
							for (int k = 0; k < len2; k++) {
								rDatum[counter] = right_subset.get(j)[k];
								counter = counter + 1;
							}
							resultTable.add(rDatum);
						}
					}
					Sql.result_tableSchema = joinNode.getLHS().getSchemaVars().toString().split(",");
					advance(left_subset, listLeftCopy, left_key, exprTree.get(0).toString(), 0);
					Sql.result_tableSchema = joinNode.getRHS().getSchemaVars().toString().split(",");
					advance(right_subset, listRightCopy, right_key, exprTree.get(1).toString(), 1);
				} else if (left_key < right_key) {
					Sql.result_tableSchema = joinNode.getLHS().getSchemaVars().toString().split(",");
					advance(left_subset, listLeftCopy, left_key, exprTree.get(0).toString(), 0);
				} else {
					Sql.result_tableSchema = joinNode.getRHS().getSchemaVars().toString().split(",");
					advance(right_subset, listRightCopy, right_key, exprTree.get(1).toString(), 1);
				}
			}
		}
		Sql.result_tableSchema = joinNode.getSchemaVars().toString().split(",");
		return resultTable;
	}

	public BufferedReader getReader(PlanNode planNode) {
		BufferedReader br = null;
		String tableName = null;
		String path = new String();
		File file = null;
		if (planNode.type.equals(PlanNode.Type.SELECT)) {
			SelectionNode selectNode = (SelectionNode) planNode;
			String nodeName = selectNode.getChild().toString();
			tableName = nodeName.substring(nodeName.indexOf("[") + 1,
					nodeName.indexOf("("));
			path = Sql.gtables.get(tableName).getFile().getPath();
			file = new File(path);
			try {
				br = new BufferedReader(new FileReader(file));
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		} else if (planNode.type.equals(PlanNode.Type.SCAN)) {
			ScanNode scanNode = (ScanNode) planNode;
			String nodeName = scanNode.toString();
			tableName = nodeName.substring(nodeName.indexOf("[") + 1,
					nodeName.indexOf("("));
			path = Sql.gtables.get(tableName).getFile().getPath();
			file = new File(path);
			try {
				br = new BufferedReader(new FileReader(file));
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}
		return br;
	}


	public List<Datum[]> sortList(List<Datum[]> inputList, String colKey)
			throws CastError {
		int listSize = inputList.size();
		// List<Datum[]> resultTable = new ArrayList<Datum[]>();
		if (listSize > 1) {
			int q = listSize / 2;
			List<Datum[]> leftList = inputList.subList(0, q);
			List<Datum[]> rightList = inputList.subList(q, listSize);
			return merge(sortList(leftList, colKey),
					sortList(rightList, colKey), colKey);
		} else {
			return inputList;
		}
	}

	public List<Datum[]> merge(List<Datum[]> left, List<Datum[]> right,
			String colKey) throws CastError {
		int totElem = left.size() + right.size();
		List<Datum[]> result = new ArrayList<Datum[]>();
		List<Datum[]> lList = new ArrayList<Datum[]>();
		List<Datum[]> rList = new ArrayList<Datum[]>();
		int i, li, ri;
		i = li = ri = 0;
		while (i < totElem) {
			if ((li < left.size()) && (ri < right.size())) {
				lList.add(left.get(li));
				rList.add(right.get(ri));
				Datum datum1 = getColumnValue(colKey, lList);
				Datum datum2 = getColumnValue(colKey, rList);
				if (datum1.toInt() < datum2.toInt()) {
					result.add(left.get(li));
					i++;
					li++;
				} else {
					result.add(right.get(ri));
					i++;
					ri++;
				}
				lList.clear();
				rList.clear();
			} else {
				if (li >= left.size()) {
					while (ri < right.size()) {
						result.add(right.get(ri));
						i++;
						ri++;
					}
				}
				if (ri >= right.size()) {
					while (li < left.size()) {
						result.add(left.get(li));
						li++;
						i++;
					}
				}
			}
		}
		return result;
	}

	public void advance(List<Datum[]> subset, List<Datum[]> sorted, int key,String col, int flg) throws CastError {
		if (flg == 0) {
			int i = 0;
			int listLength = sorted.size();
			if (sorted != null && !sorted.isEmpty()) {
				left_key = getColumnValue(col, sorted).toInt();
			}
			List<Datum[]> temp = new ArrayList<Datum[]>();
			subset.clear();
			while (i < listLength) {
				temp.add(sorted.get(0));
				int val = getColumnValue(col, temp).toInt();
				if (val == left_key) {
					subset.add(temp.get(0));
					temp.clear();
					sorted.remove(0);
					listLength = sorted.size();
					Collections.copy(left_subset, subset);
					Collections.copy(listLeftCopy, sorted);
				} else {
					break;
				}
				i++;
			}
		} else {
			int i = 0;
			int listLength = sorted.size();
			if (sorted != null && !sorted.isEmpty()) {
				right_key = getColumnValue(col, sorted).toInt();
			}
			List<Datum[]> temp = new ArrayList<Datum[]>();
			subset.clear();
			while (i < listLength) {
				temp.add(sorted.get(0));
				int val = getColumnValue(col, temp).toInt();
				if (val == right_key) {
					subset.add(temp.get(0));
					temp.clear();
					sorted.remove(0);
					listLength = sorted.size();
					Collections.copy(right_subset, subset);
					Collections.copy(listRightCopy, sorted);
				} else {
					break;
				}
				i++;
			}
		}
	}

	public Datum getColumnValue(String col_Name, List<Datum[]> ls) {
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
}
