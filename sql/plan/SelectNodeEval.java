package edu.buffalo.cse.sql.plan;

import java.util.ArrayList;
import java.util.List;

import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.Sql;
import edu.buffalo.cse.sql.SqlException;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.Datum.CastError;
import edu.buffalo.cse.sql.plan.ExprTree.ConstLeaf;
import edu.buffalo.cse.sql.plan.ExprTree.OpCode;
import edu.buffalo.cse.sql.plan.JoinNode.JType;

public class SelectNodeEval {
	SelectionNode selectNode;
	int counter;
	List<Datum[]> resultList;
	static int flag = 0;
	/*
	 * public SelectNodeEval(SelectionNode sn) { this.selectNode = sn; counter =
	 * selectNode.getCondition().size(); }
	 */

	String condition;

	public List<Datum[]> selectionNode(SelectionNode selectNode, String row)
			throws CastError {
		List<Datum[]> ls = new ArrayList<Datum[]>();
		if (selectNode.getChild().type.equals(PlanNode.Type.JOIN)) {
			ExprTree exprTree1;
			List<ExprTree> condList = new ArrayList<ExprTree>();
			List<ExprTree> list1 = new ArrayList<ExprTree>();
			exprTree1 = selectNode.condition;
			condList = buildExpressions(exprTree1, list1);
			if (!selectNode.condition.op.equals(OpCode.OR)) {
				SelectNodeRewrite snr = new SelectNodeRewrite(false);
				try {
					if (SelectNodeEval.flag == 0) {
						PlanNode planNode = snr.apply(selectNode);
						SelectNodeEval.flag = 1;
						selectNode = (SelectionNode) planNode;
					}
				} catch (SqlException e) {
					e.printStackTrace();
				}
			}
			JoinNode joinNode = (JoinNode) selectNode.getChild();
			if (selectNode.condition.op.equals(OpCode.EQ)) {
				joinNode.setJoinType(JType.MERGE);
			}
			JoinNodeEval joinNodeEval = new JoinNodeEval(joinNode);
			List<Datum[]> joinResult = joinNodeEval.joinNode(joinNode, row,selectNode.condition);
			List<Datum[]> tempList = new ArrayList<Datum[]>();
			if(!joinNode.type.equals(JType.MERGE)){
				int ctr = joinResult.size();
				for (int i = 0; i < ctr; i++) {
					if (tempList != null) {
						tempList.add(joinResult.get(i));
						tempList = checkCondition(selectNode.getCondition(),tempList);
						if (tempList != null && !tempList.isEmpty()) {
							ls.add(tempList.get(0));
							tempList.clear();
						}
					}
				}
			}
			else{
				ls = joinResult;
			}

		} else if (selectNode.getChild().type.equals(PlanNode.Type.SCAN)) {
			ScanNode scanNode = (ScanNode) selectNode.getChild();
			ScanNodeEval scanNodeEval = new ScanNodeEval();
			resultList = scanNodeEval.scanNodeFunction(scanNode, row);
			ls = checkCondition(selectNode.condition, resultList);

		} else if (selectNode.getChild().type.equals(PlanNode.Type.SELECT)) {

		}
		return ls;
	}

	public List<ExprTree> buildExpressions(ExprTree conditionTree,
			List<ExprTree> exprList) throws CastError {
		ExprTree leftTree = conditionTree.get(0);
		ExprTree rightTree = conditionTree.get(1);
		if (rightTree.op.equals(OpCode.ADD) && rightTree.size() > 0) {
			ConstLeaf constLeaf = (ConstLeaf) rightTree.get(0);
			Datum datum1 = constLeaf.v;
			constLeaf = (ConstLeaf) rightTree.get(1);
			Datum datum2 = constLeaf.v;
			Datum datum3 = new Datum.Int(datum1.toInt() + datum2.toInt());
			rightTree.clear();
			ConstLeaf constLeaf2 = new ConstLeaf(datum3);
			conditionTree.set(1, constLeaf2);
		}
		if (leftTree.size() > 0) {

			buildExpressions(leftTree, exprList);
		}
		if (rightTree.size() > 0) {

			buildExpressions(rightTree, exprList);
		}
		if (leftTree.size() == 0 && rightTree.size() == 0) {
			exprList.add(conditionTree);
		}
		return exprList;
	}

	public List<Datum[]> checkCondition(ExprTree exprTree, List<Datum[]> rList)
			throws CastError {
		resultList = rList;
		List<Datum[]> conditionList = new ArrayList<Datum[]>();
		int[] index = new int[exprTree.size()];
		Datum[] result;
		if (exprTree.isEmpty()) {
			return null;
		}
		if (resultList != null && !resultList.isEmpty()) {
			if (exprTree.op.equals(ExprTree.OpCode.AND)) {
				resultList = checkCondition(exprTree.get(0), resultList);
				if (resultList == null) {
					return null;
				}
				conditionList = checkCondition(exprTree.get(1), resultList);
				if (conditionList == null) {
					return null;
				}
			}
			if (exprTree.op.equals(ExprTree.OpCode.OR)) {
				List<Datum[]> leftConditionList = new ArrayList<Datum[]>();
				leftConditionList = checkCondition(exprTree.get(0), resultList);
				if(leftConditionList==null)
				{
					return null;
				}
				List<Datum[]> rightConditionList = new ArrayList<Datum[]>();
				rightConditionList = checkCondition(exprTree.get(1), resultList);
				if(rightConditionList==null)
				{
					return null;
				}
				conditionList = leftConditionList;
				int flag = 0;
				for (int k = 0; k < rightConditionList.size(); k++) {
					for (int i = 0; i < leftConditionList.size(); i++) {
						if (rightConditionList.get(k).equals(
								leftConditionList.get(i))) {
							flag = 1;
							break;
						}
					}
					if (flag == 0)
						conditionList.add(rightConditionList.get(k));
				}
			}
			if (exprTree.op.equals(ExprTree.OpCode.EQ)) {
				index = new int[exprTree.size()];
				for (int i = 0; i < exprTree.size(); i++) {
					for (int j = 0; j < Sql.result_tableSchema.length; j++) {
						if (Sql.result_tableSchema[j].trim().contains(
								exprTree.get(i).toString().trim())) {
							index[i] = j;
							break;
						}
					}
				}
				Datum datum1 = resultList.get(0)[index[0]];
				Datum datum2 = resultList.get(0)[index[1]];
				Schema.Type type1 = datum1.getType();
				if (exprTree.get(1).op.equals(ExprTree.OpCode.CONST)) {
					ConstLeaf constLeaf = (ConstLeaf) exprTree.get(1);
					datum2 = constLeaf.v;
					if (type1.equals(Schema.Type.INT)) {
						if (datum1.toInt() == datum2.toInt()) {
							result = resultList.get(0);
							conditionList.add(result);
						}
					} else if (type1.equals(Schema.Type.FLOAT)) {
						if (datum1.toFloat() == datum2.toFloat()) {
							result = resultList.get(0);
							conditionList.add(result);
						}
					} else if (type1.equals(Schema.Type.STRING)) {
						if (datum1.toString().equals(datum2.toString())) {
							result = resultList.get(0);
							conditionList.add(result);
						}
					}

				} else {
					if (type1.equals(Schema.Type.INT)) {
						if (datum1.toInt() == datum2.toInt()) {
							result = resultList.get(0);
							conditionList.add(result);
						}
					} else if (type1.equals(Schema.Type.FLOAT)) {
						if (datum1.toFloat() == datum2.toFloat()) {
							result = resultList.get(0);
							conditionList.add(result);
						}
					} else if (type1.equals(Schema.Type.STRING)) {
						if (datum1.toString().equals(datum2.toString())) {
							result = resultList.get(0);
							conditionList.add(result);
						}
					}
				}
			}
			if (exprTree.op.equals(ExprTree.OpCode.GT)) {
				index = new int[exprTree.size()];
				for (int i = 0; i < exprTree.size(); i++) {
					for (int j = 0; j < Sql.result_tableSchema.length; j++) {
						if (Sql.result_tableSchema[j].trim().contains(
								exprTree.get(i).toString().trim())) {
							index[i] = j;
							break;
						}
					}
				}
				Datum datum1 = resultList.get(0)[index[0]];
				Datum datum2 = resultList.get(0)[index[1]];
				Schema.Type type1 = datum1.getType();
				if (exprTree.get(1).op.equals(ExprTree.OpCode.CONST)) {
					ConstLeaf constLeaf = (ConstLeaf) exprTree.get(1);
					datum2 = constLeaf.v;
					if (type1.equals(Schema.Type.INT)) {
						if (datum1.toInt() > datum2.toInt()) {
							result = resultList.get(0);
							conditionList.add(result);
						}
					} else if (type1.equals(Schema.Type.FLOAT)) {
						if (datum1.toFloat() > datum2.toFloat()) {
							result = resultList.get(0);
							conditionList.add(result);
						}
					}

				} else {
					if (type1.equals(Schema.Type.INT)) {
						if (datum1.toInt() > datum2.toInt()) {
							result = resultList.get(0);
							conditionList.add(result);
						}
					} else if (type1.equals(Schema.Type.FLOAT)) {
						if (datum1.toFloat() > datum2.toFloat()) {
							result = resultList.get(0);
							conditionList.add(result);
						}
					}
				}
			}
			if (exprTree.op.equals(ExprTree.OpCode.LT)) {
				index = new int[exprTree.size()];
				for (int i = 0; i < exprTree.size(); i++) {
					for (int j = 0; j < Sql.result_tableSchema.length; j++) {
						if (Sql.result_tableSchema[j].trim().contains(
								exprTree.get(i).toString().trim())) {
							index[i] = j;
							break;
						}
					}
				}
				Datum datum1 = resultList.get(0)[index[0]];
				Datum datum2 = resultList.get(0)[index[1]];
				Schema.Type type1 = datum1.getType();
				if (exprTree.get(1).op.equals(ExprTree.OpCode.CONST)) {
					ConstLeaf constLeaf = (ConstLeaf) exprTree.get(1);
					datum2 = constLeaf.v;
					if (type1.equals(Schema.Type.INT)) {
						if (datum1.toInt() < datum2.toInt()) {
							result = resultList.get(0);
							conditionList.add(result);
						}
					} else if (type1.equals(Schema.Type.FLOAT)) {
						if (datum1.toFloat() < datum2.toFloat()) {
							result = resultList.get(0);
							conditionList.add(result);
						}
					}

				} else {
					if (type1.equals(Schema.Type.INT)) {
						if (datum1.toInt() < datum2.toInt()) {
							result = resultList.get(0);
							conditionList.add(result);
						}
					} else if (type1.equals(Schema.Type.FLOAT)) {
						if (datum1.toFloat() < datum2.toFloat()) {
							result = resultList.get(0);
							conditionList.add(result);
						}
					}
				}
			}
			if (exprTree.op.equals(ExprTree.OpCode.LTE)) {
				index = new int[exprTree.size()];
				for (int i = 0; i < exprTree.size(); i++) {
					for (int j = 0; j < Sql.result_tableSchema.length; j++) {
						if (Sql.result_tableSchema[j].trim().contains(
								exprTree.get(i).toString().trim())) {
							index[i] = j;
							break;
						}
					}
				}
				Datum datum1 = resultList.get(0)[index[0]];
				Datum datum2 = resultList.get(0)[index[1]];
				Schema.Type type1 = datum1.getType();
				if (exprTree.get(1).op.equals(ExprTree.OpCode.CONST)) {
					ConstLeaf constLeaf = (ConstLeaf) exprTree.get(1);
					datum2 = constLeaf.v;
					if (type1.equals(Schema.Type.INT)) {
						if (datum1.toInt() <= datum2.toInt()) {
							result = resultList.get(0);
							conditionList.add(result);
						}
					} else if (type1.equals(Schema.Type.FLOAT)) {
						if (datum1.toFloat() <= datum2.toFloat()) {
							result = resultList.get(0);
							conditionList.add(result);
						}
					}

				} else {
					if (type1.equals(Schema.Type.INT)) {
						if (datum1.toInt() <= datum2.toInt()) {
							result = resultList.get(0);
							conditionList.add(result);
						}
					} else if (type1.equals(Schema.Type.FLOAT)) {
						if (datum1.toFloat() <= datum2.toFloat()) {
							result = resultList.get(0);
							conditionList.add(result);
						}
					}
				}
			}
			if (exprTree.op.equals(ExprTree.OpCode.GTE)) {
				index = new int[exprTree.size()];
				for (int i = 0; i < exprTree.size(); i++) {
					for (int j = 0; j < Sql.result_tableSchema.length; j++) {
						if (Sql.result_tableSchema[j].trim().contains(
								exprTree.get(i).toString().trim())) {
							index[i] = j;
							break;
						}
					}
				}
				Datum datum1 = resultList.get(0)[index[0]];
				Datum datum2 = resultList.get(0)[index[1]];
				Schema.Type type1 = datum1.getType();
				if (exprTree.get(1).op.equals(ExprTree.OpCode.CONST)) {
					ConstLeaf constLeaf = (ConstLeaf) exprTree.get(1);
					datum2 = constLeaf.v;
					if (type1.equals(Schema.Type.INT)) {
						if (datum1.toInt() >= datum2.toInt()) {
							result = resultList.get(0);
							conditionList.add(result);
						}
					} else if (type1.equals(Schema.Type.FLOAT)) {
						if (datum1.toFloat() >= datum2.toFloat()) {
							result = resultList.get(0);
							conditionList.add(result);
						}
					}

				} else {
					if (type1.equals(Schema.Type.INT)) {
						if (datum1.toInt() >= datum2.toInt()) {
							result = resultList.get(0);
							conditionList.add(result);
						}
					} else if (type1.equals(Schema.Type.FLOAT)) {
						if (datum1.toFloat() >= datum2.toFloat()) {
							result = resultList.get(0);
							conditionList.add(result);
						}
					}
				}

			}
			return conditionList;
		} else {
			return null;
		}
	}
}
