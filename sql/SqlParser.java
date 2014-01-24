/* Generated By:JavaCC: Do not edit this line. SqlParser.java */
package edu.buffalo.cse.sql;

import java.util.Collection;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.File;

import edu.buffalo.cse.sql.data.Datum;

import edu.buffalo.cse.sql.plan.ExprTree;
import edu.buffalo.cse.sql.plan.PlanNode;
import edu.buffalo.cse.sql.plan.JoinNode;
import edu.buffalo.cse.sql.plan.UnionNode;
import edu.buffalo.cse.sql.plan.ProjectionNode;
import edu.buffalo.cse.sql.plan.ScanNode;
import edu.buffalo.cse.sql.plan.SelectionNode;
import edu.buffalo.cse.sql.plan.NullSourceNode;
import edu.buffalo.cse.sql.plan.AggregateNode;

public class SqlParser implements SqlParserConstants {

  public static class Target {
    public final String name;
    public final ExprTree expr;
    public final AggregateNode.AType agg;

    public Target(String name, ExprTree expr)
      { this.name = name; this.expr = expr; this.agg = null; }

    public Target(String name, ExprTree expr, AggregateNode.AType agg)
      { this.name = name; this.expr = expr; this.agg = agg; }

    public boolean isAgg() { return agg != null; }
    public ProjectionNode.Column asCol()
      { return new ProjectionNode.Column(name, expr); }

    public AggregateNode.AggColumn asAgg()
      { return new AggregateNode.AggColumn(name, expr, agg); }
  }

  static final public Program Program() throws ParseException {
    Program p = new Program();
    label_1:
    while (true) {
      Statement(p);
      jj_consume_token(EOS);
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case SELECT:
      case CREATE:
        ;
        break;
      default:
        jj_la1[0] = jj_gen;
        break label_1;
      }
    }
    jj_consume_token(0);
        {if (true) return p;}
    throw new Error("Missing return statement in function");
  }

  static final public void Statement(Program p) throws ParseException {
    PlanNode q;
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case SELECT:
      q = Select(p, null);
                            p.addQuery(q);
      break;
    case CREATE:
      Table(p);
      break;
    default:
      jj_la1[1] = jj_gen;
      jj_consume_token(-1);
      throw new ParseException();
    }
  }

  static final public void Table(Program p) throws ParseException {
    Token tableName; Schema.Table schema;
    String file = null;
    String flag=null;
    jj_consume_token(CREATE);
    jj_consume_token(TABLE);
    tableName = jj_consume_token(ID);
    jj_consume_token(LPAREN);
    schema = TableSchema();
    jj_consume_token(RPAREN);
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case FROM:
      jj_consume_token(FROM);
      jj_consume_token(FILE);
      file = StringBase();
      jj_consume_token(USING);
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case CSV:
        jj_consume_token(CSV);
        break;
      case TPCH:
        jj_consume_token(TPCH);
        jj_consume_token(LPAREN);
        jj_consume_token(STRING);
        jj_consume_token(RPAREN);
        break;
      default:
        jj_la1[2] = jj_gen;
        jj_consume_token(-1);
        throw new ParseException();
      }
      break;
    default:
      jj_la1[3] = jj_gen;
      ;
    }
      try {
        p.addTable(tableName.image,
                   new Schema.TableFromFile(new File(file), schema));
      } catch(SqlException e) {
        {if (true) throw new ParseException(e.getMessage());}
      }
  }

  static final public Schema.Table TableSchema() throws ParseException {
    Schema.Table schema = new Schema.Table();
    Schema.Column col;
    col = ColSchema();
                        schema.add(col);
    label_2:
    while (true) {
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case COMMA:
        ;
        break;
      default:
        jj_la1[4] = jj_gen;
        break label_2;
      }
      jj_consume_token(COMMA);
      col = ColSchema();
                                  schema.add(col);
    }
      {if (true) return schema;}
    throw new Error("Missing return statement in function");
  }

  static final public Schema.Column ColSchema() throws ParseException {
    Token colName; Schema.Type type;
    colName = jj_consume_token(ID);
    type = TypeBase();
        {if (true) return new Schema.Column(null, colName.image, type);}
    throw new Error("Missing return statement in function");
  }

  static final public PlanNode Select(Program p, String rangeVariable) throws ParseException {
    List<Target> tgtList;
    PlanNode source = new NullSourceNode(1);
    ExprTree where = null;
    PlanNode unionRHS = null;
    Token unionAll = null;
    List<ExprTree> gbList = null;
    jj_consume_token(SELECT);
    tgtList = SelectTargetList();
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case FROM:
      jj_consume_token(FROM);
      source = SourceList(p);
      break;
    default:
      jj_la1[5] = jj_gen;
      ;
    }
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case WHERE:
      jj_consume_token(WHERE);
      where = Expr();
                                 source =SelectionNode.make(source, where);
      break;
    default:
      jj_la1[6] = jj_gen;
      ;
    }
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case GROUP:
      jj_consume_token(GROUP);
      jj_consume_token(BY);
      gbList = GroupByList();
      break;
    default:
      jj_la1[7] = jj_gen;
      ;
    }
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case UNION:
      jj_consume_token(UNION);
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case ALL:
        unionAll = jj_consume_token(ALL);
        break;
      default:
        jj_la1[8] = jj_gen;
        ;
      }
      unionRHS = Select(p, rangeVariable);
      break;
    default:
      jj_la1[9] = jj_gen;
      ;
    }
      ArrayList<ProjectionNode.Column> pTgtList
                    = new ArrayList<ProjectionNode.Column>();
      ArrayList<AggregateNode.AggColumn> aTgtList
                    = new ArrayList<AggregateNode.AggColumn>();
      for(Target t : tgtList){
        if(t.isAgg()){ aTgtList.add(t.asAgg()); }
        else         { pTgtList.add(t.asCol()); }
      }
      if(aTgtList.size() > 0 || (gbList != null)){
        if(gbList == null) { gbList = new ArrayList<ExprTree>(); }
        List<ExprTree> unmatchedGbList = new ArrayList<ExprTree>();
        unmatchedGbList.addAll(gbList);
        for(int i = 0; i < pTgtList.size(); i++){
          if(!gbList.contains(pTgtList.get(i).expr)){
            {if (true) throw new ParseException("Expression "+
                          pTgtList.get(i).expr.toString()+
                          " is neither an aggregate nor in the group by list");}
          }
          unmatchedGbList.remove(pTgtList.get(i).expr);
        }
        if(unmatchedGbList.size() == 0){
          source =
            AggregateNode.make(rangeVariable, source, pTgtList, aTgtList);
        } else {
          List<ProjectionNode.Column> discardGroupList =
            new ArrayList<ProjectionNode.Column>();
          List<ProjectionNode.Column> passThroughList =
            new ArrayList<ProjectionNode.Column>();
          for(ProjectionNode.Column tgt : pTgtList){
            discardGroupList.add(tgt);
            passThroughList.add(
              new ProjectionNode.Column(tgt.name,
                new ExprTree.VarLeaf(rangeVariable, tgt.name))
            );
          }
          int i = 0;
          for(ExprTree discardedGroup : unmatchedGbList){
            discardGroupList.add(
              new ProjectionNode.Column("DISCARD"+i,discardedGroup)
            );
            i++;
          }
          source =
            ProjectionNode.make(rangeVariable,
              AggregateNode.make(rangeVariable, source,
                                 discardGroupList, aTgtList),
              passThroughList
            );
        }
      } else {
        source = ProjectionNode.make(rangeVariable, source, pTgtList);
      }
      if(unionRHS == null){ {if (true) return source;} }
      else {
        if(unionAll == null) {
          {if (true) return UnionNode.makeDistinct(source, unionRHS);}
        } else {
          {if (true) return UnionNode.make(source, unionRHS);}
        }
      }
    throw new Error("Missing return statement in function");
  }

  static final public List<ExprTree> GroupByList() throws ParseException {
    List<ExprTree> ret = new ArrayList<ExprTree>();
    ExprTree col;
    col = Expr();
                    ret.add(col);
    label_3:
    while (true) {
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case COMMA:
        ;
        break;
      default:
        jj_la1[10] = jj_gen;
        break label_3;
      }
      jj_consume_token(COMMA);
      col = Expr();
                              ret.add(col);
    }
      {if (true) return ret;}
    throw new Error("Missing return statement in function");
  }

  static final public PlanNode SourceList(Program p) throws ParseException {
    PlanNode s1, s2;
    s1 = ExtendedSource(p);
    label_4:
    while (true) {
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case COMMA:
        ;
        break;
      default:
        jj_la1[11] = jj_gen;
        break label_4;
      }
      jj_consume_token(COMMA);
      s2 = ExtendedSource(p);
                                      s1 = JoinNode.make(s1, s2);
    }
      {if (true) return s1;}
    throw new Error("Missing return statement in function");
  }

  static final public PlanNode ExtendedSource(Program p) throws ParseException {
    PlanNode s1, s2;
    ExprTree condition = null;
    s1 = Source(p);
    label_5:
    while (true) {
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case JOIN:
        ;
        break;
      default:
        jj_la1[12] = jj_gen;
        break label_5;
      }
      jj_consume_token(JOIN);
      s2 = Source(p);
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case ON:
        jj_consume_token(ON);
        condition = Expr();
        break;
      default:
        jj_la1[13] = jj_gen;
        ;
      }
          s1 = JoinNode.make(s1, s2);
          if(condition != null){
            s1 = SelectionNode.make(s1, condition);
          }
    }
      {if (true) return s1;}
    throw new Error("Missing return statement in function");
  }

  static final public PlanNode Source(Program p) throws ParseException {
    Token source; Token name = null;
    source = jj_consume_token(ID);
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case AS:
    case ID:
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case AS:
        jj_consume_token(AS);
        break;
      default:
        jj_la1[14] = jj_gen;
        ;
      }
      name = jj_consume_token(ID);
      break;
    default:
      jj_la1[15] = jj_gen;
      ;
    }
        try {
          Schema.Table sch = p.getTable(source.image);
          {if (true) return (name == null) ?
              new ScanNode(source.image, sch) :
              new ScanNode(source.image, name.image, sch);}
        } catch (SqlException e) {
          {if (true) throw new ParseException(e.getMessage());}
        }
    throw new Error("Missing return statement in function");
  }

  static final public List<Target> SelectTargetList() throws ParseException {
    ArrayList<Target> tgtList = new ArrayList<Target>(); Target tgt;
    tgt = SelectTarget();
                           tgtList.add(tgt);
    label_6:
    while (true) {
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case COMMA:
        ;
        break;
      default:
        jj_la1[16] = jj_gen;
        break label_6;
      }
      jj_consume_token(COMMA);
      tgt = SelectTarget();
                             tgtList.add(tgt);
    }
      {if (true) return tgtList;}
    throw new Error("Missing return statement in function");
  }

  static final public Target SelectTarget() throws ParseException {
    ExprTree e; Token id = null; String name; AggregateNode.AType agg = null;
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case DECIMAL:
    case FLOAT:
    case STRING:
    case NOT:
    case LPAREN:
    case TRUE:
    case FALSE:
    case ID:
      e = Expr();
                     name = e.makeName();
      break;
    case SUM:
    case AVG:
    case MIN:
    case MAX:
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case SUM:
        jj_consume_token(SUM);
                  agg = AggregateNode.AType.SUM; name = "Sum";
        break;
      case AVG:
        jj_consume_token(AVG);
                  agg = AggregateNode.AType.AVG; name = "Average";
        break;
      case MIN:
        jj_consume_token(MIN);
                  agg = AggregateNode.AType.MIN; name = "Min";
        break;
      case MAX:
        jj_consume_token(MAX);
                  agg = AggregateNode.AType.MAX; name = "Max";
        break;
      default:
        jj_la1[17] = jj_gen;
        jj_consume_token(-1);
        throw new ParseException();
      }
      jj_consume_token(LPAREN);
      e = Expr();
      jj_consume_token(RPAREN);
      break;
    case COUNT:
      jj_consume_token(COUNT);
      jj_consume_token(LPAREN);
      jj_consume_token(TIMES);
      jj_consume_token(RPAREN);
                  agg = AggregateNode.AType.COUNT; name = "Count";
                  e = new ExprTree.ConstLeaf(1);
      break;
    default:
      jj_la1[18] = jj_gen;
      jj_consume_token(-1);
      throw new ParseException();
    }
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case AS:
    case ID:
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case AS:
        jj_consume_token(AS);
        break;
      default:
        jj_la1[19] = jj_gen;
        ;
      }
      id = jj_consume_token(ID);
                          name = id.image;
      break;
    default:
      jj_la1[20] = jj_gen;
      ;
    }
      {if (true) return new Target(name, e, agg);}
    throw new Error("Missing return statement in function");
  }

  static final public ExprTree Expr() throws ParseException {
    ExprTree e1, e2; ExprTree.OpCode op;
    e1 = UnaryExpr();
    label_7:
    while (true) {
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case PLUS:
      case TIMES:
      case MINUS:
      case DIVIDE:
      case EQ:
      case NEQ:
      case LT:
      case GT:
      case LTE:
      case GTE:
      case AND:
      case OR:
        ;
        break;
      default:
        jj_la1[21] = jj_gen;
        break label_7;
      }
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case PLUS:
      case TIMES:
      case MINUS:
      case DIVIDE:
        op = ArithOp();
        break;
      case AND:
      case OR:
        op = BinOp();
        break;
      case EQ:
      case NEQ:
      case LT:
      case GT:
      case LTE:
      case GTE:
        op = CmpOp();
        break;
      default:
        jj_la1[22] = jj_gen;
        jj_consume_token(-1);
        throw new ParseException();
      }
      e2 = UnaryExpr();
          e1 = new ExprTree(op, e1, e2);
    }
      {if (true) return e1;}
    throw new Error("Missing return statement in function");
  }

  static final public ExprTree.OpCode ArithOp() throws ParseException {
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case PLUS:
      jj_consume_token(PLUS);
                  {if (true) return ExprTree.OpCode.ADD;}
      break;
    case TIMES:
      jj_consume_token(TIMES);
                  {if (true) return ExprTree.OpCode.MULT;}
      break;
    case MINUS:
      jj_consume_token(MINUS);
                  {if (true) return ExprTree.OpCode.SUB;}
      break;
    case DIVIDE:
      jj_consume_token(DIVIDE);
                  {if (true) return ExprTree.OpCode.DIV;}
      break;
    default:
      jj_la1[23] = jj_gen;
      jj_consume_token(-1);
      throw new ParseException();
    }
    throw new Error("Missing return statement in function");
  }

  static final public ExprTree.OpCode BinOp() throws ParseException {
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case AND:
      jj_consume_token(AND);
               {if (true) return ExprTree.OpCode.AND;}
      break;
    case OR:
      jj_consume_token(OR);
               {if (true) return ExprTree.OpCode.OR;}
      break;
    default:
      jj_la1[24] = jj_gen;
      jj_consume_token(-1);
      throw new ParseException();
    }
    throw new Error("Missing return statement in function");
  }

  static final public ExprTree.OpCode CmpOp() throws ParseException {
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case EQ:
      jj_consume_token(EQ);
              {if (true) return ExprTree.OpCode.EQ;}
      break;
    case NEQ:
      jj_consume_token(NEQ);
              {if (true) return ExprTree.OpCode.NEQ;}
      break;
    case LT:
      jj_consume_token(LT);
              {if (true) return ExprTree.OpCode.LT;}
      break;
    case GT:
      jj_consume_token(GT);
              {if (true) return ExprTree.OpCode.GT;}
      break;
    case LTE:
      jj_consume_token(LTE);
              {if (true) return ExprTree.OpCode.LTE;}
      break;
    case GTE:
      jj_consume_token(GTE);
              {if (true) return ExprTree.OpCode.GTE;}
      break;
    default:
      jj_la1[25] = jj_gen;
      jj_consume_token(-1);
      throw new ParseException();
    }
    throw new Error("Missing return statement in function");
  }

  static final public ExprTree UnaryExpr() throws ParseException {
    float f; int i; String s; Token s1; Token s2 = null; ExprTree e;
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case FLOAT:
      f = FloatBase();
                         {if (true) return new ExprTree.ConstLeaf(f);}
      break;
    case DECIMAL:
      i = IntBase();
                         {if (true) return new ExprTree.ConstLeaf(i);}
      break;
    case STRING:
      s = StringBase();
                         {if (true) return new ExprTree.ConstLeaf(s);}
      break;
    case ID:
      s1 = jj_consume_token(ID);
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case PERIOD:
        jj_consume_token(PERIOD);
        s2 = jj_consume_token(ID);
        break;
      default:
        jj_la1[26] = jj_gen;
        ;
      }
        {if (true) return (s2 == null) ? new ExprTree.VarLeaf(s1.image)
                            : new ExprTree.VarLeaf(s1.image, s2.image);}
      break;
    case LPAREN:
      jj_consume_token(LPAREN);
      e = Expr();
      jj_consume_token(RPAREN);
                                     {if (true) return e;}
      break;
    case NOT:
      jj_consume_token(NOT);
      e = Expr();
                         {if (true) return new ExprTree(ExprTree.OpCode.NOT, e);}
      break;
    case TRUE:
      jj_consume_token(TRUE);
                         {if (true) return new ExprTree.ConstLeaf(true);}
      break;
    case FALSE:
      jj_consume_token(FALSE);
                         {if (true) return new ExprTree.ConstLeaf(false);}
      break;
    default:
      jj_la1[27] = jj_gen;
      jj_consume_token(-1);
      throw new ParseException();
    }
    throw new Error("Missing return statement in function");
  }

  static final public Schema.Type TypeBase() throws ParseException {
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case TINT:
      jj_consume_token(TINT);
                   {if (true) return Schema.Type.INT;}
      break;
    case TFLOAT:
      jj_consume_token(TFLOAT);
                   {if (true) return Schema.Type.FLOAT;}
      break;
    case TSTRING:
      jj_consume_token(TSTRING);
                   {if (true) return Schema.Type.STRING;}
      break;
    case TDATE:
      jj_consume_token(TDATE);
                    {if (true) return Schema.Type.INT;}
      break;
    default:
      jj_la1[28] = jj_gen;
      jj_consume_token(-1);
      throw new ParseException();
    }
    throw new Error("Missing return statement in function");
  }

  static final public String StringBase() throws ParseException {
    Token t;
    t = jj_consume_token(STRING);
    String bs = Character.toString((char)(92));
    {if (true) return t.image.substring(1, t.image.length()-1).
      replaceAll(bs+bs+"'", "'").
      replaceAll(bs+bs+bs+bs, bs+bs);}
    throw new Error("Missing return statement in function");
  }

  static final public float FloatBase() throws ParseException {
    Token t;
    t = jj_consume_token(FLOAT);
                  {if (true) return Float.parseFloat(t.image);}
    throw new Error("Missing return statement in function");
  }

  static final public int IntBase() throws ParseException {
    Token t;
    t = jj_consume_token(DECIMAL);
                    {if (true) return Integer.parseInt(t.image);}
    throw new Error("Missing return statement in function");
  }

  static private boolean jj_initialized_once = false;
  /** Generated Token Manager. */
  static public SqlParserTokenManager token_source;
  static SimpleCharStream jj_input_stream;
  /** Current token. */
  static public Token token;
  /** Next token. */
  static public Token jj_nt;
  static private int jj_ntk;
  static private int jj_gen;
  static final private int[] jj_la1 = new int[29];
  static private int[] jj_la1_0;
  static private int[] jj_la1_1;
  static {
      jj_la1_init_0();
      jj_la1_init_1();
   }
   private static void jj_la1_init_0() {
      jj_la1_0 = new int[] {0x2000020,0x2000020,0x300000,0x80,0x0,0x80,0x100,0x200,0x1000,0x800,0x0,0x0,0x800000,0x1000000,0x40,0x40,0x0,0x36000,0xc003e000,0x40,0x40,0x0,0x0,0x0,0x0,0x0,0x0,0xc0000000,0x38000000,};
   }
   private static void jj_la1_init_1() {
      jj_la1_1 = new int[] {0x0,0x0,0x0,0x0,0x4,0x0,0x0,0x0,0x0,0x0,0x4,0x4,0x0,0x0,0x0,0x200000,0x4,0x0,0x3b0001,0x0,0x200000,0xfff0,0xfff0,0xf0,0xc000,0x3f00,0x2,0x3b0001,0x400000,};
   }

  /** Constructor with InputStream. */
  public SqlParser(java.io.InputStream stream) {
     this(stream, null);
  }
  /** Constructor with InputStream and supplied encoding */
  public SqlParser(java.io.InputStream stream, String encoding) {
    if (jj_initialized_once) {
      System.out.println("ERROR: Second call to constructor of static parser.  ");
      System.out.println("       You must either use ReInit() or set the JavaCC option STATIC to false");
      System.out.println("       during parser generation.");
      throw new Error();
    }
    jj_initialized_once = true;
    try { jj_input_stream = new SimpleCharStream(stream, encoding, 1, 1); } catch(java.io.UnsupportedEncodingException e) { throw new RuntimeException(e); }
    token_source = new SqlParserTokenManager(jj_input_stream);
    token = new Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (int i = 0; i < 29; i++) jj_la1[i] = -1;
  }

  /** Reinitialise. */
  static public void ReInit(java.io.InputStream stream) {
     ReInit(stream, null);
  }
  /** Reinitialise. */
  static public void ReInit(java.io.InputStream stream, String encoding) {
    try { jj_input_stream.ReInit(stream, encoding, 1, 1); } catch(java.io.UnsupportedEncodingException e) { throw new RuntimeException(e); }
    token_source.ReInit(jj_input_stream);
    token = new Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (int i = 0; i < 29; i++) jj_la1[i] = -1;
  }

  /** Constructor. */
  public SqlParser(java.io.Reader stream) {
    if (jj_initialized_once) {
      System.out.println("ERROR: Second call to constructor of static parser. ");
      System.out.println("       You must either use ReInit() or set the JavaCC option STATIC to false");
      System.out.println("       during parser generation.");
      throw new Error();
    }
    jj_initialized_once = true;
    jj_input_stream = new SimpleCharStream(stream, 1, 1);
    token_source = new SqlParserTokenManager(jj_input_stream);
    token = new Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (int i = 0; i < 29; i++) jj_la1[i] = -1;
  }

  /** Reinitialise. */
  static public void ReInit(java.io.Reader stream) {
    jj_input_stream.ReInit(stream, 1, 1);
    token_source.ReInit(jj_input_stream);
    token = new Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (int i = 0; i < 29; i++) jj_la1[i] = -1;
  }

  /** Constructor with generated Token Manager. */
  public SqlParser(SqlParserTokenManager tm) {
    if (jj_initialized_once) {
      System.out.println("ERROR: Second call to constructor of static parser. ");
      System.out.println("       You must either use ReInit() or set the JavaCC option STATIC to false");
      System.out.println("       during parser generation.");
      throw new Error();
    }
    jj_initialized_once = true;
    token_source = tm;
    token = new Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (int i = 0; i < 29; i++) jj_la1[i] = -1;
  }

  /** Reinitialise. */
  public void ReInit(SqlParserTokenManager tm) {
    token_source = tm;
    token = new Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (int i = 0; i < 29; i++) jj_la1[i] = -1;
  }

  static private Token jj_consume_token(int kind) throws ParseException {
    Token oldToken;
    if ((oldToken = token).next != null) token = token.next;
    else token = token.next = token_source.getNextToken();
    jj_ntk = -1;
    if (token.kind == kind) {
      jj_gen++;
      return token;
    }
    token = oldToken;
    jj_kind = kind;
    throw generateParseException();
  }


/** Get the next Token. */
  static final public Token getNextToken() {
    if (token.next != null) token = token.next;
    else token = token.next = token_source.getNextToken();
    jj_ntk = -1;
    jj_gen++;
    return token;
  }

/** Get the specific Token. */
  static final public Token getToken(int index) {
    Token t = token;
    for (int i = 0; i < index; i++) {
      if (t.next != null) t = t.next;
      else t = t.next = token_source.getNextToken();
    }
    return t;
  }

  static private int jj_ntk() {
    if ((jj_nt=token.next) == null)
      return (jj_ntk = (token.next=token_source.getNextToken()).kind);
    else
      return (jj_ntk = jj_nt.kind);
  }

  static private java.util.List<int[]> jj_expentries = new java.util.ArrayList<int[]>();
  static private int[] jj_expentry;
  static private int jj_kind = -1;

  /** Generate ParseException. */
  static public ParseException generateParseException() {
    jj_expentries.clear();
    boolean[] la1tokens = new boolean[55];
    if (jj_kind >= 0) {
      la1tokens[jj_kind] = true;
      jj_kind = -1;
    }
    for (int i = 0; i < 29; i++) {
      if (jj_la1[i] == jj_gen) {
        for (int j = 0; j < 32; j++) {
          if ((jj_la1_0[i] & (1<<j)) != 0) {
            la1tokens[j] = true;
          }
          if ((jj_la1_1[i] & (1<<j)) != 0) {
            la1tokens[32+j] = true;
          }
        }
      }
    }
    for (int i = 0; i < 55; i++) {
      if (la1tokens[i]) {
        jj_expentry = new int[1];
        jj_expentry[0] = i;
        jj_expentries.add(jj_expentry);
      }
    }
    int[][] exptokseq = new int[jj_expentries.size()][];
    for (int i = 0; i < jj_expentries.size(); i++) {
      exptokseq[i] = jj_expentries.get(i);
    }
    return new ParseException(token, exptokseq, tokenImage);
  }

  /** Enable tracing. */
  static final public void enable_tracing() {
  }

  /** Disable tracing. */
  static final public void disable_tracing() {
  }

}
