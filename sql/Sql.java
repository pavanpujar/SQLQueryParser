/* Generated By:JavaCC: Do not edit this line. Sql.java */
package edu.buffalo.cse.sql;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.plan.AggregateNode;
import edu.buffalo.cse.sql.plan.AggregateNode.AggColumn;
import edu.buffalo.cse.sql.plan.AggregateNodeEval1;
import edu.buffalo.cse.sql.plan.ExprTree.OpCode;
import edu.buffalo.cse.sql.plan.AggregateNodeEval;
import edu.buffalo.cse.sql.plan.PlanNode;
import edu.buffalo.cse.sql.plan.ProjectionNode;
import edu.buffalo.cse.sql.plan.ProjectionNodeEval;
import edu.buffalo.cse.sql.plan.ProjectionNodeEval1;
import edu.buffalo.cse.sql.plan.ScanNode;
import edu.buffalo.cse.sql.plan.SelectNodeRewrite;
import edu.buffalo.cse.sql.plan.SelectionNode;
import edu.buffalo.cse.sql.plan.UnionNode;
import edu.buffalo.cse.sql.plan.UnionNodeEval;
import edu.buffalo.cse.sql.plan.UnionNodeEval1;
import edu.buffalo.cse.sql.util.TableBuilder;

public class Sql {
	public static HashMap<String, String> table_Names = new HashMap<String, String>();
	static Boolean explain = false;
	static Boolean index = false;
	static String fileName = null;

	public static void main(String[] args) {
		try {
			List<Datum[]> lresult = new ArrayList<Datum[]>();
			File program = new File(args[0]);
			if (args.length > 1) {
				program = new File(args[args.length - 1]);
				for (int i = 0; i < args.length; i++) {
					if (args[i].equalsIgnoreCase("-explain")) {
						explain = true;
					}
					if (args[i].equalsIgnoreCase("-index")) {
						index = true;
					}
				}
			}
			fileName = "TPCH";
			FileInputStream fis = new FileInputStream(program);
			SqlParser sqlParser = new SqlParser(fis);
			Program prog = sqlParser.Program();
			Iterator<PlanNode> iter = prog.queries.iterator();
			while (iter.hasNext()) {
				lresult = execQuery(prog.tables, iter.next());
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		} catch (SqlException e) {
			e.printStackTrace();
		}
	}

	public static Map<String, Schema.TableFromFile> gtables;
	public static String[] result_tableSchema;

	public static List<Datum[]> execQuery(Map<String, Schema.TableFromFile> tables, PlanNode q)	throws SqlException {
		if(fileName == null){
			gtables=tables;
			List<Datum[]> ls = new ArrayList<Datum[]>();
			if (q.type.equals(PlanNode.Type.AGGREGATE)
					&& q.struct.equals(PlanNode.Structure.UNARY)) {
				AggregateNode an = (AggregateNode) q;
				AggregateNodeEval1 agEval=new AggregateNodeEval1();
				ls = agEval.aggregateFunction(an);	
			}
			 if(q.type.equals(PlanNode.Type.PROJECT) && q.struct.equals(PlanNode.Structure.UNARY)){            
		            ProjectionNode pn = (ProjectionNode) q;
		            ProjectionNodeEval1 pnEval = new ProjectionNodeEval1(pn); 
		            return pnEval.projectionNode(pn);	           
		        }
			 if(q.type.equals(PlanNode.Type.UNION) && q.struct.equals(PlanNode.Structure.BINARY)){            
		            UnionNode un = (UnionNode) q;
		            UnionNodeEval1 unEval = new UnionNodeEval1(un); 
		            return unEval.unionFunction(un);	           
		        }
			 return ls;
			
		}
		else{
			gtables = tables;
			int no_of_tables = tables.keySet().size();
			List<Datum[]> ls = new ArrayList<Datum[]>();
			if (q.type.equals(PlanNode.Type.AGGREGATE)
					&& q.struct.equals(PlanNode.Structure.UNARY)) {
				List<Datum[]> resultList = new ArrayList<Datum[]>();
				AggregateNode an = (AggregateNode) q;
				if (an.getChild().type.equals(PlanNode.Type.SELECT) && explain) {
					SelectionNode sn = (SelectionNode) an.getChild();
					System.out.println("-----------Query before optimization-----------\n");
					System.out.println(an.toString());
					if (sn.getChild().type.equals(PlanNode.Type.JOIN)&&!sn.getCondition().op.equals(OpCode.OR)) {
						SelectNodeRewrite snr = new SelectNodeRewrite(false);
						PlanNode node = snr.apply(sn);
						sn = (SelectionNode) node;
						an.setChild(sn);
					}
					System.out.println("-----------Query after optimization-----------\n");
					System.out.println(an.toString());
				}
				else{
					AggregateNodeEval agEval = new AggregateNodeEval();
					Iterator<String> iterator = null;
					String tablename = new String();
					String path = new String();
					File file = null;
					BufferedReader br = null;
					iterator = gtables.keySet().iterator();
					if (no_of_tables == 1) {
						tablename = iterator.next();
						path = Sql.gtables.get(tablename).getFile().getPath();
						file = new File(path);
						try {
							String str = null;
							br = new BufferedReader(new FileReader(file));
							while ((str = br.readLine()) != null) {
								agEval.aggregateFunction(an, str);

							}
							ls = agEval.getResult();
							br.close();
						} catch (FileNotFoundException e) {
							e.printStackTrace();
						} catch (IOException e) {
							e.printStackTrace();
						}
					} else {
						agEval.aggregateFunction(an, null);
						ls = agEval.getResult();
					}
					List<AggColumn> querySchema = an.getAggregates();
					List<ProjectionNode.Column> gSchema = an.getGroupByVars();
					TableBuilder output = new TableBuilder();
					Iterator<Datum[]> resultIterator = ls.iterator();
					output.newRow();
					for (AggColumn c : querySchema) {
						output.newCell(c.name.toString());
					}
					for (ProjectionNode.Column c : gSchema) {
						output.newCell(c.name);
					}
					output.addDividerLine();
					while (resultIterator.hasNext()) {
						Datum[] row = resultIterator.next();
						output.newRow();
						for (Datum d : row) {
							output.newCell(d.toString());
						}
					}
					System.out.println(output);
				}
			}
			if (q.type.equals(PlanNode.Type.PROJECT)&& q.struct.equals(PlanNode.Structure.UNARY)) {
				ProjectionNode pn = (ProjectionNode) q;
				if (explain) {
					System.out.println("-----------Query before optimization-----------\n");
					System.out.println(pn.toString());
					if(pn.getChild().type.equals(PlanNode.Type.SELECT)){
						SelectionNode sn = (SelectionNode) pn.getChild();
						
						if (sn.getChild().type.equals(PlanNode.Type.JOIN)&&!sn.getCondition().op.equals(OpCode.OR)) {
							SelectNodeRewrite snr = new SelectNodeRewrite(false);
							PlanNode node = snr.apply(sn);
							sn = (SelectionNode)node;
							pn.setChild(sn);
						}					
					}
					System.out.println("-----------Query after optimization-----------\n");
					System.out.println(pn.toString());
				}
				else
				{
					ProjectionNodeEval pnEval = new ProjectionNodeEval(pn);
					return pnEval.projectionNode(pn,null);
				}
			}
			if (q.type.equals(PlanNode.Type.UNION)
					&& q.struct.equals(PlanNode.Structure.BINARY)) {
				UnionNode un = (UnionNode) q;
				UnionNodeEval unEval = new UnionNodeEval(un);
				return unEval.unionFunction(un);
			}
			return ls;

		}
	}

	public static List<List<Datum[]>> execFile(File program) throws Exception {
		List<List<Datum[]>> lresult = new ArrayList<List<Datum[]>>();
		List<Datum[]> ld = new ArrayList<Datum[]>();
		Map<String, Object> result;
		FileInputStream fis = new FileInputStream(program);
		SqlParser sqlParser = new SqlParser(fis);
		Program prog = sqlParser.Program();
		Iterator<PlanNode> iter = prog.queries.iterator();
		while (iter.hasNext()) {
			ld = execQuery(prog.tables, iter.next());
		}
		lresult.add(ld);
		return lresult;
	}
}