package edu.buffalo.cse.sql.plan;

import java.util.ArrayList;
import java.util.List;

import edu.buffalo.cse.sql.Sql;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.Datum.CastError;

public class SelectNodeEval1 {
	SelectionNode selectNode;
	int counter;
	List<Datum[]> resultList;

	public SelectNodeEval1(SelectionNode sn) {
		this.selectNode = sn;
		counter = selectNode.getCondition().size();
	}

	String condition;

	public List<Datum[]> selectionNode(SelectionNode selectNode) throws CastError {
		List<Datum[]> ls = new ArrayList<Datum[]>();
		if (selectNode.getChild().type.equals(PlanNode.Type.JOIN)) {
			JoinNode joinNode = (JoinNode) selectNode.getChild();
			JoinNodeEval1 joinNodeEval = new JoinNodeEval1(joinNode);
			resultList = joinNodeEval.joinNode(joinNode);
			ls = checkCondition(selectNode.getCondition());
		} else if (selectNode.getChild().type.equals(PlanNode.Type.SCAN)) {

		} else if (selectNode.getChild().type.equals(PlanNode.Type.SELECT)) {

		}

		return ls;
	}

	public List<Datum[]> checkCondition(ExprTree exprTree) throws CastError {
		
		List<Datum[]> conditionList = new ArrayList<Datum[]>();
		Datum[] resultDatum;
		if (exprTree.isEmpty())
			return null;

		if (exprTree.op.equals(ExprTree.OpCode.AND)) {
			resultList = checkCondition(exprTree.get(0));
			//List<Datum[]> rightConditionList = new ArrayList<Datum[]>();
			//rightConditionList = checkCondition(exprTree.get(1));
			conditionList = checkCondition(exprTree.get(1));
			//for (int k = 0; k < rightConditionList.size(); k++) {
			//		resultDatum = rightConditionList.get(k);
			//		conditionList.add(resultDatum);
			//}
		}
		if (exprTree.op.equals(ExprTree.OpCode.OR)) {
			List<Datum[]> leftConditionList = new ArrayList<Datum[]>();
			leftConditionList = checkCondition(exprTree.get(0));
			List<Datum[]> rightConditionList = new ArrayList<Datum[]>();
			rightConditionList = checkCondition(exprTree.get(1));
			conditionList = leftConditionList;
			int flag = 0;
			for (int k = 0; k < rightConditionList.size(); k++) {				 
				for(int i = 0; i < leftConditionList.size(); i++) {
					if(rightConditionList.get(k).equals(leftConditionList.get(i)))
					{
						flag = 1;
						break;
					}
				}
				if(flag==0)
					conditionList.add(rightConditionList.get(k));
			}
		}
		if (exprTree.op.equals(ExprTree.OpCode.EQ)) {
			int[] index = new int[exprTree.size()];
			for (int i = 0; i < exprTree.size(); i++) {
				for (int j = 0; j < Sql.result_tableSchema.length; j++) {
					if (Sql.result_tableSchema[j].trim().equals(
							exprTree.get(i).toString().trim())) {
						index[i] = j;
						break;
					}
				}
			}
			for (int k = 0; k < resultList.size(); k++) {
				if (resultList.get(k)[index[0]]
						.equals(resultList.get(k)[index[1]])) {
					resultDatum = resultList.get(k);
					conditionList.add(resultDatum);
				}
			}
		}
		if (exprTree.op.equals(ExprTree.OpCode.GT)) {
			int[] index = new int[exprTree.size()];
			for (int i = 0; i < exprTree.size(); i++) {
				for (int j = 0; j < Sql.result_tableSchema.length; j++) {
					if (Sql.result_tableSchema[j].trim().contains(exprTree.get(i).toString().trim())) {
						index[i] = j;
						break;
					}
				}
			}
			for (int k = 0; k < resultList.size(); k++) {
				if (resultList.get(k)[index[0]].toInt() > resultList.get(k)[index[1]].toInt()) 
				{
					resultDatum = resultList.get(k);
					conditionList.add(resultDatum);
				}
			}
		}
		if (exprTree.op.equals(ExprTree.OpCode.LT)) {
			int[] index = new int[exprTree.size()];
			for (int i = 0; i < exprTree.size(); i++) {
				for (int j = 0; j < Sql.result_tableSchema.length; j++) {
					if (Sql.result_tableSchema[j].trim().contains(exprTree.get(i).toString().trim())) {
						index[i] = j;
						break;
					}
				}
			}
			for (int k = 0; k < resultList.size(); k++) {
				if (resultList.get(k)[index[0]].toInt() < resultList.get(k)[index[1]].toInt()) {
					resultDatum = resultList.get(k);
					conditionList.add(resultDatum);
				}
			}
		}
		return conditionList;
	}
}
