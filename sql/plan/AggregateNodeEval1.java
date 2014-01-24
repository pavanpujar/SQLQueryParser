package edu.buffalo.cse.sql.plan;

import java.util.ArrayList;
import java.util.List;

import edu.buffalo.cse.sql.Sql;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.Datum.CastError;

public class AggregateNodeEval1 {
	List<Datum[]> ls = new ArrayList<Datum[]>();
	List<Datum[]> return_list = new ArrayList<Datum[]>();
	int flag = 0;

public List<Datum[]> aggregateFunction(AggregateNode an) throws CastError {
		int ind = 0;
		int cntr = an.getAggregates().size();

		if (an.getChild().type.equals(PlanNode.Type.AGGREGATE)) {
			AggregateNode aggNode = (AggregateNode) an.getChild();
			ls = aggregateFunction(aggNode);
		} else if (an.getChild().type.equals(PlanNode.Type.SCAN)) {
			ScanNodeEval1 snEval = new ScanNodeEval1();
			ls = snEval.scanNodeFunction(an.getChild());
		} else if (an.getChild().type.equals(PlanNode.Type.SELECT)) {
			SelectionNode sn = (SelectionNode) an.getChild();
			SelectNodeEval1 selectNodeEval = new SelectNodeEval1(sn);
			ls = selectNodeEval.selectionNode(sn);
		} else if (an.getChild().type.equals(PlanNode.Type.PROJECT)) {
			ProjectionNode pn = (ProjectionNode) an.getChild();
			ProjectionNodeEval1 pnEval = new ProjectionNodeEval1(pn);
			ls = pnEval.projectionNode(pn);
		} else if (an.getChild().type.equals(PlanNode.Type.UNION)) {

		} else if (an.getChild().type.equals(PlanNode.Type.JOIN)) {
			JoinNode jn = (JoinNode) an.getChild();
			JoinNodeEval1 jnEval = new JoinNodeEval1(jn);
			ls = jnEval.joinNode(jn);
		} else if (an.getChild().type.equals(PlanNode.Type.NULLSOURCE)) {

		}
		while (ind < cntr) {
			if (an.getAggregates().get(ind).aggType
					.equals(AggregateNode.AType.SUM)) {
				String col = an.getAggregates().get(ind).expr.toString();
				return_list = sumFunction(col);
			} 
			else if (an.getAggregates().get(ind).aggType
					.equals(AggregateNode.AType.MIN)) {
				String col = an.getAggregates().get(ind).expr.toString();
				return_list = minFunction(col);

			} else if (an.getAggregates().get(ind).aggType
					.equals(AggregateNode.AType.MAX)) {
				String col = an.getAggregates().get(ind).expr.toString();
				return_list = maxFunction(col);

			} else if (an.getAggregates().get(ind).aggType
					.equals(AggregateNode.AType.COUNT)) {
				return_list = countFunction();

			} else if (an.getAggregates().get(ind).aggType
					.equals(AggregateNode.AType.AVG)) {
				String col = an.getAggregates().get(ind).expr.toString();
				return_list=avgFunction(col);
				}
			ind++;
			flag = 1;
		}
		return return_list;
}
	
	public List<Datum[]> sumFunction(String colName) {
		int sum = 0;
		int opval = 0;
		int operatorCount = 0;
		List<Datum[]> sumList = new ArrayList<Datum[]>();
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
		
		
		if(columnName[0]==null){
			int col_num = 0;
			for (int j = 0; j < Sql.result_tableSchema.length; j++) {
				if (Sql.result_tableSchema[j].trim().contains(colName)) {
					col_num = j;
					break;
				}
			}
	
			for (int i = 0; i < ls.size(); i++) {
				try {
					sum = sum + ls.get(i)[col_num].toInt();
				} catch (CastError e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (flag == 0)
				sumList.add(new Datum[] { new Datum.Int(sum) });
			else {
				sumList.add(new Datum[] { return_list.get(0)[0], new Datum.Int(sum) });
			}
			return sumList;
		}
		
		else{
			int ctr=0;
			int[] col_no = new int[columnName.length-operatorCount];
			for(int k =0;k<columnName.length;k++){
				for (int j = 0; j < Sql.result_tableSchema.length; j++) {				
					if (Sql.result_tableSchema[j].trim().contains(columnName[k])) {
						col_no[ctr] = j;
						ctr++;
						break;
					}					
				}
				if(ctr == (columnName.length-operatorCount))
					break;
			}
			
			int rowWiseSum = 0;
			for (int i = 0; i < ls.size(); i++) {
				try {
					if(opval == 1)
					{
						for(int k = 0; k < col_no.length;k++){
							rowWiseSum = rowWiseSum + ls.get(i)[col_no[k]].toInt();
						}
						sum = sum + rowWiseSum;
						rowWiseSum = 0;
					}
					else if(opval == 2)
					{
						rowWiseSum = 1;
						for(int k = 0; k < col_no.length;k++){
							rowWiseSum = rowWiseSum * ls.get(i)[col_no[k]].toInt();
						}
						sum = sum + rowWiseSum;
						rowWiseSum = 0;
					}
				} catch (CastError e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (flag == 0)
				sumList.add(new Datum[] { new Datum.Int(sum) });
			else {
				sumList.add(new Datum[] { return_list.get(0)[0], new Datum.Int(sum) });
			}
			return sumList;
		}
	}

	public List<Datum[]> avgFunction(String colName) {
		Double avg = 0.0;
		Double sum=0.0;
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
		if(columnName[0]==null){
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
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			avg=sum/ls.size();
			if (flag == 0)
				avgList.add(new Datum[] { new Datum.Flt(avg) });
			else {
				avgList.add(new Datum[] { return_list.get(0)[0], new Datum.Flt(avg) });
			}
			return avgList;
		}
		else{
			int ctr=0;
			int[] col_no = new int[columnName.length-operatorCount];
			for(int k =0;k<columnName.length;k++){
				for (int j = 0; j < Sql.result_tableSchema.length; j++) {				
					if (Sql.result_tableSchema[j].trim().contains(columnName[k])) {
						col_no[ctr] = j;
						ctr++;
						break;
					}					
				}
				if(ctr == (columnName.length-operatorCount))
					break;
			}
			
			Double rowWiseSum = 0.0;
			for (int i = 0; i < ls.size(); i++) {
				try {
					if(opval == 1)
					{
						for(int k = 0; k < col_no.length;k++){
							rowWiseSum = rowWiseSum + ls.get(i)[col_no[k]].toFloat();
						}
						sum = sum + rowWiseSum;
						rowWiseSum = 0.0;
					}
					else if(opval == 2)
					{
						rowWiseSum = 1.0;
						for(int k = 0; k < col_no.length;k++){
							rowWiseSum = rowWiseSum * ls.get(i)[col_no[k]].toFloat();
						}
						sum = sum + rowWiseSum;
						rowWiseSum = 0.0;
					}
				} catch (CastError e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			avg = sum / ls.size();
			if (flag == 0)
				avgList.add(new Datum[] { new Datum.Flt(avg)});
			else {
				avgList.add(new Datum[] {return_list.get(0)[0], new Datum.Flt(avg) });
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
				// TODO Auto-generated catch block
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
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		maxList.add(new Datum[] { new Datum.Int(maximum) });
		return maxList;

	}

	public List<Datum[]> countFunction() {
		int count = 0;
		List<Datum[]> countList = new ArrayList<Datum[]>();
		count = ls.size();
		countList.add(new Datum[] { new Datum.Int(count) });
		return countList;

	}
}
