package edu.buffalo.cse.sql.plan;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import edu.buffalo.cse.sql.SqlException;
import edu.buffalo.cse.sql.optimizer.PlanRewrite;
import edu.buffalo.cse.sql.plan.ExprTree.OpCode;
import edu.buffalo.cse.sql.plan.JoinNode.JType;

public class SelectNodeRewrite extends PlanRewrite {

	private static int doJoin = 1;
	private static int doScan = 0;
	Collection<ExprTree> conjuctiveList;
	List<ExprTree> exprList;

	public SelectNodeRewrite(boolean defaultTopDown) {
		super(defaultTopDown);

	}

	@Override
	public PlanNode apply(PlanNode node) throws SqlException {
		if (node.type.equals(PlanNode.Type.SELECT)) {
			SelectionNode sn = (SelectionNode) node;
			if (sn.getChild().type.equals(PlanNode.Type.JOIN)) {
				conjuctiveList = sn.conjunctiveClauses();
				exprList = new ArrayList<ExprTree>(conjuctiveList);
				JoinNode jn = (JoinNode) sn.getChild();
				jn.setJoinType(JType.MERGE);
				if (jn.getLHS().type.equals(PlanNode.Type.JOIN)) {
					jn.setLHS(apply(jn.getLHS(), PlanNode.Type.JOIN, exprList));
				} else {
					ScanNode scanNode = (ScanNode) jn.getLHS();
					scanExpressions(jn, scanNode, doScan, true);
				}
				if (jn.getRHS().type.equals(PlanNode.Type.JOIN)) {
					jn.setRHS(apply(jn.getRHS(), PlanNode.Type.JOIN, exprList));
				} else {
					ScanNode scanNode = (ScanNode) jn.getRHS();
					scanExpressions(jn, scanNode, doScan, false);
				}
				return (PlanNode) scanExpressions(jn, null, doJoin, null);
			}
		}
		return node;
	}

	public PlanNode apply(PlanNode node, PlanNode.Type type,
			Collection<ExprTree> exprList) {
		if (node.type.equals(PlanNode.Type.JOIN)) {
			JoinNode jn = (JoinNode) node;
			jn.setJoinType(JType.MERGE);
			if (jn.getLHS().type.equals(PlanNode.Type.JOIN)) {
				jn.setLHS(apply(jn.getLHS(), jn.getLHS().type, exprList));
			} else {
				ScanNode scanNode = (ScanNode) jn.getLHS();
				scanExpressions(jn, scanNode, doScan, true);
			}
			if (jn.getRHS().type.equals(PlanNode.Type.JOIN)) {
				jn.setRHS(apply(jn.getRHS(), jn.getLHS().type, exprList));
			} else {
				ScanNode scanNode = (ScanNode) jn.getRHS();
				scanExpressions(jn, scanNode, doScan, false);
			}
			return (PlanNode) scanExpressions(jn, null, doJoin, null);
		} else {
			ScanNode scanNode = (ScanNode) node;
			return (PlanNode) scanExpressions(null, scanNode, doScan, null);
		}
	}

	public SelectionNode scanExpressions(JoinNode jn, ScanNode scanNode,
			int joinFlag, Boolean setLeft) {
		Iterator<ExprTree> iter;
		ExprTree exprTree = new ExprTree(OpCode.EQ);
		SelectionNode selectionNode = new SelectionNode(exprTree);
		iter = exprList.iterator();
		ArrayList<ExprTree> expressionContainer = new ArrayList<ExprTree>();
		if (joinFlag == 1) {
			ArrayList<Integer> numArr = new ArrayList<Integer>();
			int j = 0;
			while (iter.hasNext()) {
				exprTree = iter.next();
				if ((exprTree.allVars().size() == 2)
						&& (jn.getSchemaVars().toString().contains(exprTree
								.allVars().toArray()[0].toString()))
						&& (jn.getSchemaVars().toString().contains(exprTree
								.allVars().toArray()[1].toString()))) {
					expressionContainer.add(exprTree);
					numArr.add(j);
				}
				j++;
			}
			if (!expressionContainer.isEmpty()) {
				exprTree = expressionContainer.get(0);
				Object[] arr = numArr.toArray();
				Arrays.sort(arr, Collections.reverseOrder());
				for (int k = 0; k < arr.length; k++) {
					int y = (Integer) arr[k];
					exprList.remove(y);
				}
				for (int i = 1; i < expressionContainer.size(); i++) {
					exprTree = new ExprTree(ExprTree.OpCode.AND, exprTree,
							expressionContainer.get(i));
				}
				selectionNode = new SelectionNode(exprTree);
				selectionNode.setChild(jn);
			}
		} else {
			ArrayList<Integer> numArr = new ArrayList<Integer>();
			int j = 0;
			while (iter.hasNext()) {
				exprTree = iter.next();
				if (exprTree.allVars().size() == 1
						&& (scanNode.schema.toString().contains(exprTree
								.allVars().toArray()[0].toString()))) {
					expressionContainer.add(exprTree);
					numArr.add(j);
				}
				j++;
			}
			if (!expressionContainer.isEmpty()) {
				exprTree = expressionContainer.get(0);
				Object[] arr = numArr.toArray();
				Arrays.sort(arr, Collections.reverseOrder());
				for (int k = 0; k < numArr.size(); k++) {
					int y = (Integer) arr[k];
					exprList.remove(y);
				}
				for (int i = 1; i < expressionContainer.size(); i++) {
					exprTree = new ExprTree(ExprTree.OpCode.AND, exprTree,
							expressionContainer.get(i));
				}
				selectionNode = new SelectionNode(exprTree);
				if (jn != null) {
					if (setLeft) {
						jn.setLHS(selectionNode);
					} else {
						jn.setRHS(selectionNode);
					}
				}
				selectionNode.setChild(scanNode);
			}
		}
		return selectionNode;
	}

}
