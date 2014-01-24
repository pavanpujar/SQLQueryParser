package edu.buffalo.cse.sql.plan;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import edu.buffalo.cse.sql.Sql;
import edu.buffalo.cse.sql.data.Datum;


public class JoinNodeEval1 {
JoinNode jNode;
	public JoinNodeEval1(JoinNode jn) {
		this.jNode=jn;
	}
public List<Datum[]> joinNode(JoinNode joinNode)
{
	List<Datum[]> tableListLeft=new ArrayList<Datum[]>();
	List<Datum[]> tableListRight=new ArrayList<Datum[]>();
	List<Datum[]> resultTable=new ArrayList<Datum[]>();
	Datum[] rDatum;	
	if(joinNode.getLHS().type.equals(PlanNode.Type.JOIN))
	{
		PlanNode planNode=joinNode.getLHS();
		JoinNode jnNodeLeft=(JoinNode)planNode;
		tableListLeft = joinNode(jnNodeLeft);
		
	}
	if(joinNode.getRHS().type.equals(PlanNode.Type.JOIN))
	{
		PlanNode planNode=joinNode.getRHS();
		JoinNode jnNodeRight=(JoinNode)planNode;
		tableListRight = joinNode(jnNodeRight);	
	}
	if(joinNode.getLHS().type.equals(PlanNode.Type.SCAN))
	{
		ScanNodeEval1 scanNodeEvalLeft=new ScanNodeEval1();
		tableListLeft=scanNodeEvalLeft.scanNodeFunction(joinNode.getLHS());
	}
	if(joinNode.getRHS().type.equals(PlanNode.Type.SCAN))
	{
		ScanNodeEval1 scanNodeEvalRight=new ScanNodeEval1();
		tableListRight=scanNodeEvalRight.scanNodeFunction(joinNode.getRHS());		
	}
	for(int i=0;i<tableListLeft.size();i++)
	{
		for(int j=0;j<tableListRight.size();j++)
		{
			int len1=tableListLeft.get(0).length;
			int len2=tableListRight.get(0).length;
			rDatum=new Datum[len1+len2];
			int counter = 0;
			for(int k=0;k<len1;k++){
				rDatum[counter]=tableListLeft.get(i)[k];
				counter=counter+1;
			}
			for(int k=0;k<len2;k++){
				rDatum[counter]=tableListRight.get(j)[k];
				counter=counter+1;
			}
			resultTable.add(rDatum);
		}
	}
	Sql.result_tableSchema=joinNode.getSchemaVars().toString().split(",");
	return resultTable;			
}
}
