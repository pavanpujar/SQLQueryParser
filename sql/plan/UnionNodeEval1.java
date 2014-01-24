package edu.buffalo.cse.sql.plan;

import java.util.ArrayList;
import java.util.List;

import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.Datum.CastError;

public class UnionNodeEval1 {
	UnionNode uNode;

	public UnionNodeEval1(UnionNode unionNode) {
this.uNode=unionNode;
	}

	public List<Datum[]> unionFunction(UnionNode unionNode) throws CastError {
		List<Datum[]> ls = new ArrayList<Datum[]>();
		List<Datum[]> temp_arr = new ArrayList<Datum[]>();

		if (unionNode.lhs.type.equals(PlanNode.Type.PROJECT)) {
			PlanNode pn = unionNode.getLHS();
			ProjectionNodeEval1 pnEval = new ProjectionNodeEval1((ProjectionNode)pn);			
			temp_arr = pnEval.projectionNode((ProjectionNode) pn);			
		}
		if (unionNode.lhs.type.equals(PlanNode.Type.UNION)) {
			temp_arr = unionFunction((UnionNode) unionNode.getLHS());
		}
		if (!temp_arr.isEmpty()) {
			for (int i = 0; i < temp_arr.size(); i++) {
				ls.add(temp_arr.get(i));
			}
		}
		if (unionNode.rhs.type.equals(PlanNode.Type.PROJECT)) {
			PlanNode pn = unionNode.getRHS();
			ProjectionNodeEval1 pnEval = new ProjectionNodeEval1((ProjectionNode)pn);			
			temp_arr = pnEval.projectionNode((ProjectionNode) pn);
		}
		if (unionNode.rhs.type.equals(PlanNode.Type.UNION)) {
			temp_arr = unionFunction((UnionNode) unionNode.getRHS());

		}
		if (!temp_arr.isEmpty()) {
			for (int i = 0; i < temp_arr.size(); i++) {
				ls.add(temp_arr.get(i));
			}
		}
		return ls;
	}
}