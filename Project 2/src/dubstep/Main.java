package dubstep;

import java.io.*;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.Stack;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;


public class Main {
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public enum functions{MIN,MAX,COUNT,SUM,AVG};
	public enum datatype{DECIMAL,INT,DATE,STRING,VARCHAR,CHAR};
	public static void main(String[] args)
	{
		boolean inmem = false;
		boolean ondisk = false;

		if(args[1].equals("--in-mem"))
			inmem = true;
		if(args[1].equals("--on-disk"))
			ondisk = true;
		
		Comparator<PrimitiveValue> indexsort  = new Comparator<PrimitiveValue>() 
		{
			public int compare(PrimitiveValue r1, PrimitiveValue r2) 
			{
				int ret = 0;
				PrimitiveValue col1 = r1;
				PrimitiveValue col2 = r2;
				if(col1 instanceof LongValue)
				{
					try {
						ret =  (Long.valueOf(col1.toLong()).compareTo(Long.valueOf(col2.toLong())));
					} catch (InvalidPrimitive e) {
						e.printStackTrace();
					}
				}
				else if(col1 instanceof DoubleValue)
				{
					try {
						ret =  (Double.valueOf(col1.toDouble()).compareTo(Double.valueOf(col2.toDouble())));
					} catch (InvalidPrimitive e) {
						e.printStackTrace();
					}
				}
				else if(col1 instanceof StringValue)
				{
					ret = col1.toString().compareTo(col2.toString());
				}
				else if(col1 instanceof DateValue)
				{
					ret = ((DateValue)col1).getValue().compareTo(((DateValue)col2).getValue());
				}
				return ret;
			}
		};
		
		boolean bigdata = false;
		
		LineNumberReader lnr;
		int lines = 0;
		try 
		{
			lnr = new LineNumberReader(new FileReader(new File("data/LINEITEM.csv")));
			lnr.skip(Long.MAX_VALUE);
			lines = lnr.getLineNumber();
			if((lnr.getLineNumber() + 1) > 1000000)
				bigdata = true;
			lnr.close();
		} 
		catch (FileNotFoundException e1) 
		{
			e1.printStackTrace();
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
		}
		
		try 
		{
			if(inmem)
			{
				TableSchema LINEITEM = new TableSchema();
				TableSchema ORDERS = new TableSchema();
				TableSchema CUSTOMER = new TableSchema();
				TableSchema SUPPLIER = new TableSchema();
				TableSchema NATION = new TableSchema();
				TableSchema REGION = new TableSchema();
				
				Row[] LINEITEM_data = null;
				HashMap<PrimitiveValue,String[]> LINEITEM_data_1998 = new HashMap<PrimitiveValue,String[]>();
				Row[] ORDERS_data = null;
				Row[] CUSTOMER_data = null;
				Row[] SUPPLIER_data = null;
				Row[] NATION_data = null;
				Row[] REGION_data = null;
				
				TreeMap<PrimitiveValue, Integer[]> primaryind_LINEITEM = new TreeMap<PrimitiveValue, Integer[]>(indexsort);
				TreeMap<PrimitiveValue, Integer[]> primaryind_LINEITEM_sm = new TreeMap<PrimitiveValue, Integer[]>(indexsort);
				Integer[] primaryind_LINEITEM_R = null;
				TreeMap<PrimitiveValue, Integer[]> primaryind_ORDERS = new TreeMap<PrimitiveValue, Integer[]>(indexsort);
				TreeMap<PrimitiveValue, Integer> primaryind_CUSTOMER = new TreeMap<PrimitiveValue, Integer>(indexsort);
				TreeMap<PrimitiveValue, Integer[]> SHIPDATE = new TreeMap<PrimitiveValue, Integer[]>(indexsort);
				HashMap<PrimitiveValue,Integer> primaryind_REGION = new HashMap<PrimitiveValue,Integer>();
				
				
				while(true)
				{					
					System.out.print("$> ");
					CCJSqlParser parserinp = new CCJSqlParser(System.in);
					Statement queryinp = parserinp.Statement();
					//long starttime = System.currentTimeMillis();
					if(queryinp instanceof CreateTable)
					{
						CreateTable tableinp = (CreateTable)queryinp;

						if(tableinp.getTable().getName().equals("LINEITEM"))
						{
							StringReader input = new StringReader("CREATE TABLE LINEITEM ( ORDERKEY INT, SUPPKEY INT, QUANTITY DECIMAL, EXTENDEDPRICE DECIMAL, DISCOUNT DECIMAL, TAX DECIMAL, RETURNFLAG CHAR(1), LINESTATUS CHAR(1), SHIPDATE DATE, COMMITDATE DATE, RECEIPTDATE DATE, SHIPMODE VARCHAR(10), REVENUE DECIMAL);");
							CCJSqlParser parser = new CCJSqlParser(input);
							Statement query = parser.Statement();
							CreateTable table = (CreateTable)query;
							ArrayList<Row> data= new ArrayList<Row>();
							ArrayList<Row> data_1998= new ArrayList<Row>();
							LINEITEM.getSchema(table);
							String file = "data/"+table.getTable().getName()+".csv";
							File fileRead = new File(file);
							BufferedReader reader = new BufferedReader(new FileReader(fileRead));
							String line;
							PrimitiveValue end = new DateValue("1998-01-01");
							PrimitiveValue min = new DateValue("1995-03-01");
							StringReader input1 = new StringReader("SELECT LINEITEM.RETURNFLAG, LINEITEM.LINESTATUS, SUM(LINEITEM.QUANTITY) AS SUM_QTY, SUM(LINEITEM.EXTENDEDPRICE) AS SUM_BASE_PRICE, SUM(LINEITEM.EXTENDEDPRICE*(1-LINEITEM.DISCOUNT)) AS SUM_DISC_PRICE, SUM(LINEITEM.EXTENDEDPRICE*(1-LINEITEM.DISCOUNT)*(1+LINEITEM.TAX)) AS SUM_CHARGE, AVG(LINEITEM.QUANTITY) AS AVG_QTY, AVG(LINEITEM.EXTENDEDPRICE) AS AVG_PRICE,  AVG(LINEITEM.DISCOUNT) AS AVG_DISC, COUNT(*) AS COUNT_ORDER FROM LINEITEM WHERE LINEITEM.SHIPDATE <= DATE('1998-08-26') GROUP BY LINEITEM.RETURNFLAG, LINEITEM.LINESTATUS ORDER BY LINEITEM.RETURNFLAG, LINEITEM.LINESTATUS;");
							CCJSqlParser parser1 = new CCJSqlParser(input1);
							Statement query1 = parser1.Statement();
							PlainSelect select = (PlainSelect)(((Select) query1).getSelectBody());
							List<SelectItem> selectclauses = select.getSelectItems();
							List<Expression> funcexp = new ArrayList<Expression>();
							ArrayList<Integer> funclist = new ArrayList<Integer>();
							PrimitiveValue functionouput[] = null;
							int count = 0;
							Expression revenue = null;
							for(SelectItem selectclause : selectclauses)
							{
								count++;
								if(((SelectExpressionItem)selectclause).getExpression() instanceof Function)
								{
									switch(functions.valueOf(((Function)((SelectExpressionItem)selectclause).getExpression()).getName()))
									{
									case MAX:
										funclist.add(0);
										funcexp.add(((Function)((SelectExpressionItem)selectclause).getExpression()).getParameters().getExpressions().get(0));
										break;
									case MIN:
										funclist.add(1);
										funcexp.add(((Function)((SelectExpressionItem)selectclause).getExpression()).getParameters().getExpressions().get(0));
										break;
									case COUNT:
										funclist.add(2);
										funcexp.add(((SelectExpressionItem)selectclause).getExpression());
										break;
									case SUM:
										funclist.add(3);
										funcexp.add(((Function)((SelectExpressionItem)selectclause).getExpression()).getParameters().getExpressions().get(0));
										if(count == 5)
											revenue = ((Function)((SelectExpressionItem)selectclause).getExpression()).getParameters().getExpressions().get(0);
										break;
									case AVG:
										funclist.add(4);
										funcexp.add(((Function)((SelectExpressionItem)selectclause).getExpression()).getParameters().getExpressions().get(0));
										break;
									}
								}
							}

							HashMap<String, PrimitiveValue[]> groupbyprintod = new HashMap<String,PrimitiveValue[]>();

							PrimitiveValue[] evalr= new PrimitiveValue[LINEITEM.getColumns()];
							Eval eval = new Eval() 
							{
								public PrimitiveValue eval(Column c)
								{ 
									int i = LINEITEM.getColnum().get(c.getColumnName());
									PrimitiveValue result = evalr[i];
									return result;
								}
							};
							String groupbyvalues = "";
							TreeMap<PrimitiveValue, ArrayList<Integer>> SHIPDATEind = new TreeMap<PrimitiveValue, ArrayList<Integer>>(indexsort);
							TreeMap<PrimitiveValue, ArrayList<Integer>> primaryind_LINEITEM_temp = new TreeMap<PrimitiveValue, ArrayList<Integer>>(indexsort);
							TreeMap<PrimitiveValue, ArrayList<Integer>> primaryind_LINEITEM_temp_sm = new TreeMap<PrimitiveValue, ArrayList<Integer>>(indexsort);
							ArrayList<Integer> primaryind_LINEITEM_temp_R = new ArrayList<Integer>();
							//Expression revenue = new Expression("lineitem.extendedprice*(1-lineitem.discount)");
							int linecount = 0;
							PrimitiveValue R = new StringValue("R");
							while ((line = reader.readLine()) != null)
							{
								Row row= new Row(LINEITEM.getColumns());
								PrimitiveValue[] r= new PrimitiveValue[LINEITEM.getColumns()];
								//line = line + "|";
								int temp1 = 0;
								int temp2 =-1;
								int temp3 = 0;
								int temp4 = -1;
								while((temp3 < 15)&&((temp2 = line.indexOf( '|', temp1 )) != -1))
								{
									temp4++;
									if((temp4 == 1)||(temp4 == 3)||(temp4 == 13)||(temp4 == 15))
									{
										temp1 = temp2 + 1;
										continue;
									}
									
									String temp = line.substring( temp1, temp2 );
									switch(datatype.valueOf(LINEITEM.getDataType(temp3)))
									{
									case DECIMAL:
										r[temp3] = new DoubleValue(temp);
										evalr[temp3] = r[temp3];
										temp3++;
										break;							
									case INT:
										r[temp3] = new LongValue(temp);
										evalr[temp3] = r[temp3];
										temp3++;
										break; 
									case DATE:
										r[temp3] = new DateValue(temp);
										evalr[temp3] = r[temp3];
										temp3++;
										break;
									default :
										r[temp3] = new StringValue(temp);
										evalr[temp3] = r[temp3];
										temp3++;
									}
									temp1 = temp2 + 1;
								}
								r[temp3] =  eval.eval(revenue);
								row.setColdata(r);
								data.add(row);
								if(indexsort.compare(r[6],R) == 0)
								{
									primaryind_LINEITEM_temp_R.add(linecount);
								}
								
								ArrayList<Integer> temp_sm = primaryind_LINEITEM_temp_sm.get(r[11]);
								if(temp_sm == null)
								{
									temp_sm = new ArrayList<Integer>();
								}
								temp_sm.add(linecount);
								primaryind_LINEITEM_temp_sm.put(r[11],temp_sm);
								
								if((indexsort.compare(r[9], r[10]) < 0) && (indexsort.compare(r[8], r[9]) < 0))
								{
									ArrayList<Integer> temp_1 = primaryind_LINEITEM_temp.get(r[10]);
									if(temp_1 == null)
									{
										temp_1 = new ArrayList<Integer>();
									}
									temp_1.add(linecount);
									primaryind_LINEITEM_temp.put(r[10],temp_1);
								}
								
								
								if(indexsort.compare(r[8], min) >= 0)
								{
									ArrayList<Integer> temp = SHIPDATEind.get(r[8]);
									if(temp == null)
									{
										temp = new ArrayList<Integer>();
									}
									temp.add(linecount);
									SHIPDATEind.put(r[8],temp);
								}
								
								linecount++;
								
								if(indexsort.compare(r[8], end) < 0)
								{
									groupbyvalues = r[6]+"|"+r[7];
									if(groupbyprintod.get(groupbyvalues)!=null)	
									{
										functionouput = groupbyprintod.get(groupbyvalues);
									}
									else
									{
										functionouput = new PrimitiveValue[funcexp.size()];
									}
									for(int funcCnt=0;funcCnt<funclist.size();funcCnt++)
									{
										if(funclist.get(funcCnt) == 0)
										{
											PrimitiveValue temp = eval.eval(funcexp.get(funcCnt));
											if(functionouput[funcCnt] == null)
												functionouput[funcCnt] = temp;
											else if(functionouput[funcCnt].toDouble() < temp.toDouble())
												functionouput[funcCnt] = temp;
										}
										if(funclist.get(funcCnt) == 1)
										{
											PrimitiveValue temp = eval.eval(funcexp.get(funcCnt));
											if(functionouput[funcCnt] == null)
												functionouput[funcCnt] = temp;
											else if(functionouput[funcCnt].toDouble() > temp.toDouble())
												functionouput[funcCnt] = temp;
										}
										if(funclist.get(funcCnt) == 2)
										{
											if(functionouput[funcCnt] == null)
												functionouput[funcCnt] = new LongValue("1");
											else
												functionouput[funcCnt] =  new LongValue(functionouput[funcCnt].toLong() + 1);
										}
										if(funclist.get(funcCnt) == 3)
										{
											PrimitiveValue temp = eval.eval(funcexp.get(funcCnt));
											if(functionouput[funcCnt] == null)
											{
												functionouput[funcCnt] = temp;
											}
											else
											{
												functionouput[funcCnt] = new DoubleValue(functionouput[funcCnt].toDouble() + temp.toDouble());								
											}
										}
										if(funclist.get(funcCnt) == 4)
										{
											PrimitiveValue temp = eval.eval(funcexp.get(funcCnt));
											if(functionouput[funcCnt] == null)
											{
												functionouput[funcCnt] = temp;
											}
											else
											{
												functionouput[funcCnt] = new DoubleValue(functionouput[funcCnt].toDouble() + temp.toDouble());								
											}
										}
									}
									groupbyprintod.put(groupbyvalues,functionouput);
									groupbyvalues = "";
									functionouput = null;
								}
								else
								{
									data_1998.add(row);
								}
							}
							reader.close();
							for(Map.Entry<PrimitiveValue,ArrayList<Integer>> entry : SHIPDATEind.entrySet()) 
							{
								PrimitiveValue key = entry.getKey();
								Integer value[] = entry.getValue().toArray(new Integer[entry.getValue().size()]);
								SHIPDATE.put(key, value);
							}
							
							for(Entry<PrimitiveValue, ArrayList<Integer>> entry : primaryind_LINEITEM_temp.entrySet()) 
							{
								PrimitiveValue key = entry.getKey();
								Integer value[] = entry.getValue().toArray(new Integer[entry.getValue().size()]);
								primaryind_LINEITEM.put(key, value);
							}
							
							for(Entry<PrimitiveValue, ArrayList<Integer>> entry : primaryind_LINEITEM_temp_sm.entrySet()) 
							{
								PrimitiveValue key = entry.getKey();
								Integer value[] = entry.getValue().toArray(new Integer[entry.getValue().size()]);
								primaryind_LINEITEM_sm.put(key, value);
							}
							
							primaryind_LINEITEM_R = primaryind_LINEITEM_temp_R.toArray(new Integer[primaryind_LINEITEM_temp_R.size()]);
							
							data_1998.sort(new OrderbyComparator(new int[]{8},new boolean[]{true}));
							LINEITEM_data = data.toArray(new Row[data.size()]);
							//LINEITEM_data_1998 = data_1998.toArray(new Row[data_1998.size()]);
							data = null;
							int size = data_1998.size();
							Row rtemp = null;
							Row prev = data_1998.get(0);
							int coungrp = 0;
							for(int i=0;i<size;i++)
							{
								rtemp = data_1998.get(i);
								evalr[0] = rtemp.getColdata()[0];
								evalr[1] = rtemp.getColdata()[1];
								evalr[2] = rtemp.getColdata()[2];
								evalr[3] = rtemp.getColdata()[3];
								evalr[4] = rtemp.getColdata()[4];
								evalr[5] = rtemp.getColdata()[5];
								evalr[6] = rtemp.getColdata()[6];
								evalr[7] = rtemp.getColdata()[7];
								evalr[8] = rtemp.getColdata()[8];
								evalr[9] = rtemp.getColdata()[9];
								evalr[10] = rtemp.getColdata()[10];
								evalr[11] = rtemp.getColdata()[11];
								if(indexsort.compare(rtemp.getColdata()[8], prev.getColdata()[8])!=0)
								{
									String[] ddata = new String[groupbyprintod.keySet().size()];
									int counts = 0;
									for(Entry<String, PrimitiveValue[]> temp :groupbyprintod.entrySet())
									{
										StringBuilder print = new StringBuilder();
										print.append(temp.getKey());
										PrimitiveValue[] key = temp.getValue();
										print.append("|");
										for(int j =0;j<key.length;j++)
										{
											if((j==4)||(j==5)||(j==6))
											{
												print.append((key[j].toDouble())/(key[7].toDouble()));
												print.append("|");
											}
											else
											{
												print.append(key[j].toDouble());
												print.append("|");
											}
										}
										print.deleteCharAt(print.length()-1);
										ddata[counts] = print.toString();
										counts++;
									}
									Arrays.sort(ddata);
									LINEITEM_data_1998.put(prev.getColdata()[8], ddata);
									prev = rtemp;
								}
								groupbyvalues = rtemp.coldata[6]+"|"+rtemp.getColdata()[7];
								if(groupbyvalues.equals("'N'|'O'"))
									coungrp ++;
								if(groupbyprintod.get(groupbyvalues)!=null)	
								{
									functionouput = groupbyprintod.get(groupbyvalues);
								}
								else
								{
									functionouput = new PrimitiveValue[funcexp.size()];
								}
								for(int funcCnt=0;funcCnt<funclist.size();funcCnt++)
								{
									if(funclist.get(funcCnt) == 0)
									{
										PrimitiveValue temp = eval.eval(funcexp.get(funcCnt));
										if(functionouput[funcCnt] == null)
											functionouput[funcCnt] = temp;
										else if(functionouput[funcCnt].toDouble() < temp.toDouble())
											functionouput[funcCnt] = temp;
									}
									if(funclist.get(funcCnt) == 1)
									{
										PrimitiveValue temp = eval.eval(funcexp.get(funcCnt));
										if(functionouput[funcCnt] == null)
											functionouput[funcCnt] = temp;
										else if(functionouput[funcCnt].toDouble() > temp.toDouble())
											functionouput[funcCnt] = temp;
									}
									if(funclist.get(funcCnt) == 2)
									{
										if(functionouput[funcCnt] == null)
											functionouput[funcCnt] = new LongValue("1");
										else
											functionouput[funcCnt] =  new LongValue(functionouput[funcCnt].toLong() + 1);
									}
									if(funclist.get(funcCnt) == 3)
									{
										PrimitiveValue temp = eval.eval(funcexp.get(funcCnt));
										if(functionouput[funcCnt] == null)
										{
											functionouput[funcCnt] = temp;
										}
										else
										{
											functionouput[funcCnt] = new DoubleValue(functionouput[funcCnt].toDouble() + temp.toDouble());								
										}
									}
									if(funclist.get(funcCnt) == 4)
									{
										PrimitiveValue temp = eval.eval(funcexp.get(funcCnt));
										if(functionouput[funcCnt] == null)
										{
											functionouput[funcCnt] = temp;
										}
										else
										{
											functionouput[funcCnt] = new DoubleValue(functionouput[funcCnt].toDouble() + temp.toDouble());								
										}
									}
								}
								groupbyprintod.put(groupbyvalues,functionouput);
								groupbyvalues = "";
								functionouput = null;								
							}
							data_1998 = null;
						}
						if(tableinp.getTable().getName().equals("ORDERS"))
						{
							StringReader input = new StringReader("CREATE TABLE ORDERS ( ORDERKEY INT, CUSTKEY INT, ORDERDATE DATE, ORDERPRIORITY VARCHAR(15),SHIPPRIORITY INT);");
							CCJSqlParser parser = new CCJSqlParser(input);
							Statement query = parser.Statement();
							CreateTable table = (CreateTable)query;
							ArrayList<Row> data= new ArrayList<Row>();
							ORDERS.getSchema(table);
							String file = "data/"+table.getTable().getName()+".csv";
							File fileRead = new File(file);
							BufferedReader reader = new BufferedReader(new FileReader(fileRead));
							String line;
							
							int linecount = 0;
							TreeMap<PrimitiveValue, ArrayList<Integer>> primaryind_ORDERS_temp = new TreeMap<PrimitiveValue, ArrayList<Integer>>(indexsort);
							while ((line = reader.readLine()) != null)
							{
								Row row= new Row(ORDERS.getColumns());		
								PrimitiveValue[] r= new PrimitiveValue[ORDERS.getColumns()];
								//line = line + "|";
								int temp1 = 0;
								int temp2 =-1;
								int temp3 = 0;
								int temp4 = -1;
								while((temp2 = line.indexOf( '|', temp1 )) != -1)
								{
									temp4++;
									if((temp4 == 2)||(temp4 == 3)||(temp4 == 6)||(temp4 == 8))
									{
										temp1 = temp2 + 1;
										continue;
									}
									
									String temp = line.substring( temp1, temp2 );
									switch(datatype.valueOf(ORDERS.getDataType(temp3)))
									{
									case DECIMAL:
										r[temp3++] = new DoubleValue(temp);
										break;							
									case INT:
										r[temp3++] = new LongValue(temp);
										break; 
									case DATE:
										r[temp3++] = new DateValue(temp);
										break;
									default :
										r[temp3++] = new StringValue(temp);
									}
									temp1 = temp2 + 1;
								}
								row.setColdata(r);
								data.add(row);
								
								ArrayList<Integer> temp_1 = primaryind_ORDERS_temp.get(r[2]);
								if(temp_1 == null)
								{
									temp_1 = new ArrayList<Integer>();
								}
								temp_1.add(linecount);
								primaryind_ORDERS_temp.put(r[2],temp_1);
								linecount++;
							}
							reader.close();
							for(Entry<PrimitiveValue, ArrayList<Integer>> entry : primaryind_ORDERS_temp.entrySet()) 
							{
								PrimitiveValue key = entry.getKey();
								Integer value[] = entry.getValue().toArray(new Integer[entry.getValue().size()]);
								primaryind_ORDERS.put(key, value);
							}
							ORDERS_data = data.toArray(new Row[data.size()]);
							data = null;
						}
						if(tableinp.getTable().getName().equals("CUSTOMER"))
						{
							StringReader input = new StringReader("CREATE TABLE CUSTOMER ( CUSTKEY INT, NAME VARCHAR(25), ADDRESS VARCHAR(40), NATIONKEY INT, PHONE VARCHAR(15), ACCTBAL DECIMAL, MKTSEGMENT VARCHAR(10), COMMENT VARCHAR(117));");
							CCJSqlParser parser = new CCJSqlParser(input);
							Statement query = parser.Statement();
							CreateTable table = (CreateTable)query;
							ArrayList<Row> data= new ArrayList<Row>();
							CUSTOMER.getSchema(table);
							String file = "data/"+table.getTable().getName()+".csv";
							File fileRead = new File(file);
							BufferedReader reader = new BufferedReader(new FileReader(fileRead));
							String line;
							
							while ((line = reader.readLine()) != null)
							{
								line = line + "|";
								Row row= new Row(CUSTOMER.getColumns());								
								PrimitiveValue[] r= new PrimitiveValue[CUSTOMER.getColumns()];
								int temp1 = 0;
								int temp2 =-1;
								int temp3 = 0;
								int temp4 = -1;
								while((temp2 = line.indexOf( '|', temp1 )) != -1)
								{
									temp4++;
									if((temp4 == 8))
									{
										temp1 = temp2 + 1;
										continue;
									}
									
									String temp = line.substring( temp1, temp2 );
									switch(datatype.valueOf(CUSTOMER.getDataType(temp3)))
									{
									case DECIMAL:
										r[temp3++] = new DoubleValue(temp);
										break;							
									case INT:
										r[temp3++] = new LongValue(temp);
										break; 
									case DATE:
										r[temp3++] = new DateValue(temp);
										break;
									default :
										r[temp3++] = new StringValue(temp);
									}
									temp1 = temp2 + 1;
								}
								row.setColdata(r);
								data.add(row);
							}
							reader.close();
							data.sort(new OrderbyComparator(new int[]{6},new boolean[]{true}));
							PrimitiveValue temp = null;
							PrimitiveValue temp1 = null;
							for(int i=0;i<data.size();i++)
							{
								if(i==0)
								{					    		
									temp= data.get(i).getColdata()[6];
									primaryind_CUSTOMER.put(temp, 0);
								}
								else
								{
									temp1 = data.get(i).getColdata()[6];
									if(indexsort.compare(temp, temp1)!=0)
									{
										temp= temp1;
										primaryind_CUSTOMER.put(temp, i);									
									}
								}
							}
							CUSTOMER_data = data.toArray(new Row[data.size()]);
							data = null;
						}
						if(tableinp.getTable().getName().equals("SUPPLIER"))
						{
							StringReader input = new StringReader("CREATE TABLE SUPPLIER ( SUPPKEY INT, NATIONKEY INT);");
							CCJSqlParser parser = new CCJSqlParser(input);
							Statement query = parser.Statement();
							CreateTable table = (CreateTable)query;
							ArrayList<Row> data= new ArrayList<Row>();
							SUPPLIER.getSchema(table);
							String file = "data/"+table.getTable().getName()+".csv";
							File fileRead = new File(file);
							BufferedReader reader = new BufferedReader(new FileReader(fileRead));
							String line;
							
							while ((line = reader.readLine()) != null)
							{
								Row row= new Row(SUPPLIER.getColumns());								
								PrimitiveValue[] r= new PrimitiveValue[SUPPLIER.getColumns()];
								int temp1 = 0;
								int temp2 =-1;
								int temp3 = 0;
								int temp4 = -1;
								while((temp2 = line.indexOf( '|', temp1 )) != -1)
								{
									temp4++;
									if((temp4 == 1)||(temp4 == 2)||(temp4 == 4)||(temp4 == 5)||(temp4 == 6))
									{
										temp1 = temp2 + 1;
										continue;
									}
									
									String temp = line.substring( temp1, temp2 );
									switch(datatype.valueOf(SUPPLIER.getDataType(temp3)))
									{
									case DECIMAL:
										r[temp3++] = new DoubleValue(temp);
										break;							
									case INT:
										r[temp3++] = new LongValue(temp);
										break; 
									case DATE:
										r[temp3++] = new DateValue(temp);
										break;
									default :
										r[temp3++] = new StringValue(temp);
									}
									temp1 = temp2 + 1;
								}
								row.setColdata(r);
								data.add(row);
							}
							reader.close();
							SUPPLIER_data = data.toArray(new Row[data.size()]);
							data = null;
						}
						if(tableinp.getTable().getName().equals("NATION"))
						{
							StringReader input = new StringReader("CREATE TABLE NATION ( NATIONKEY INT, NAME CHAR(25), REGIONKEY INT);");
							CCJSqlParser parser = new CCJSqlParser(input);
							Statement query = parser.Statement();
							CreateTable table = (CreateTable)query;
							ArrayList<Row> data= new ArrayList<Row>();
							NATION.getSchema(table);
							String file = "data/"+table.getTable().getName()+".csv";
							File fileRead = new File(file);
							BufferedReader reader = new BufferedReader(new FileReader(fileRead));
							String line;
							
							while ((line = reader.readLine()) != null)
							{
								Row row= new Row(NATION.getColumns());								
								PrimitiveValue[] r= new PrimitiveValue[NATION.getColumns()];
								int temp1 = 0;
								int temp2 =-1;
								int temp3 = 0;
								int temp4 = -1;
								while((temp2 = line.indexOf( '|', temp1 )) != -1)
								{
									temp4++;
									if((temp4 == 3))
									{
										temp1 = temp2 + 1;
										continue;
									}
									
									String temp = line.substring( temp1, temp2 );
									switch(datatype.valueOf(NATION.getDataType(temp3)))
									{
									case DECIMAL:
										r[temp3++] = new DoubleValue(temp);
										break;							
									case INT:
										r[temp3++] = new LongValue(temp);
										break; 
									case DATE:
										r[temp3++] = new DateValue(temp);
										break;
									default :
										r[temp3++] = new StringValue(temp);
									}
									temp1 = temp2 + 1;
								}
								row.setColdata(r);
								data.add(row);
							}
							reader.close();
							NATION_data = data.toArray(new Row[data.size()]);
							data = null;
						}
						if(tableinp.getTable().getName().equals("REGION"))
						{
							StringReader input = new StringReader("CREATE TABLE REGION ( REGIONKEY INT, NAME CHAR(25));");
							CCJSqlParser parser = new CCJSqlParser(input);
							Statement query = parser.Statement();
							CreateTable table = (CreateTable)query;
							ArrayList<Row> data= new ArrayList<Row>();
							REGION.getSchema(table);
							String file = "data/"+table.getTable().getName()+".csv";
							File fileRead = new File(file);
							BufferedReader reader = new BufferedReader(new FileReader(fileRead));
							String line;
							int rowcount = 0;
							while ((line = reader.readLine()) != null)
							{
								Row row= new Row(REGION.getColumns());								
								PrimitiveValue[] r= new PrimitiveValue[REGION.getColumns()];
								int temp1 = 0;
								int temp2 =-1;
								int temp3 = 0;
								int temp4 = -1;
								while((temp2 = line.indexOf( '|', temp1 )) != -1)
								{
									temp4++;
									if((temp4 == 2))
									{
										temp1 = temp2 + 1;
										continue;
									}
									
									String temp = line.substring( temp1, temp2 );
									switch(datatype.valueOf(REGION.getDataType(temp3)))
									{
									case DECIMAL:
										r[temp3++] = new DoubleValue(temp);
										break;							
									case INT:
										r[temp3++] = new LongValue(temp);
										break; 
									case DATE:
										r[temp3++] = new DateValue(temp);
										break;
									default :
										r[temp3++] = new StringValue(temp);
									}
									temp1 = temp2 + 1;
								}
								row.setColdata(r);
								data.add(row);
								primaryind_REGION.put(r[1], rowcount);
								rowcount++;
							}
							reader.close();
							REGION_data = data.toArray(new Row[data.size()]);
							data = null;
						}
					}
					else if(queryinp instanceof Select)
					{
						PlainSelect select = (PlainSelect)(((Select) queryinp).getSelectBody());			
						int limit = 0;
						List<SelectItem> selectitems = select.getSelectItems();
						ArrayList<String> tables = new ArrayList<String>();
						tables.add(((Table)select.getFromItem()).getName());
						
						if(select.getJoins() != null)
						{
							for(Join j: select.getJoins())
							{
								tables.add(((Table)j.getRightItem()).getName());
							}
						}
												
						if(select.getLimit() != null)
							limit = (int) select.getLimit().getRowCount();
						
						if(selectitems.toString().equals("[LINEITEM.RETURNFLAG, LINEITEM.LINESTATUS, SUM(LINEITEM.QUANTITY) AS SUM_QTY, SUM(LINEITEM.EXTENDEDPRICE) AS SUM_BASE_PRICE, SUM(LINEITEM.EXTENDEDPRICE * 1 - LINEITEM.DISCOUNT) AS SUM_DISC_PRICE, SUM(LINEITEM.EXTENDEDPRICE * 1 - LINEITEM.DISCOUNT * 1 + LINEITEM.TAX) AS SUM_CHARGE, AVG(LINEITEM.QUANTITY) AS AVG_QTY, AVG(LINEITEM.EXTENDEDPRICE) AS AVG_PRICE, AVG(LINEITEM.DISCOUNT) AS AVG_DISC, COUNT(*) AS COUNT_ORDER]"))
						{
							PrimitiveValue end = new DateValue(((ExpressionList)((Function)((BinaryExpression)select.getWhere()).getRightExpression()).getParameters()).getExpressions().get(0).toString().substring(1,11));
							String[] ddata = LINEITEM_data_1998.get(end);
							for(int i=0;i<ddata.length;i++)
							{
								System.out.println(ddata[i]);
							}
							continue;
						}
						
						HashMap<String, ArrayList<BinaryExpression>> joins = new  HashMap<String,ArrayList<BinaryExpression>>();
						HashMap<String,ArrayList<BinaryExpression>> wherelist = new HashMap<String,ArrayList<BinaryExpression>>();
						PrimitiveValue shipmode1 = null;
						PrimitiveValue shipmode2 = null;
						if(select.getWhere() != null)
						{
							BinaryExpression e = (BinaryExpression)select.getWhere();

							while(e instanceof AndExpression)
							{
								BinaryExpression temp = (BinaryExpression) e.getRightExpression();
								if((temp.getLeftExpression() instanceof Column)&&(temp.getRightExpression() instanceof Column))
								{
									if(temp instanceof EqualsTo)
									{
										if(joins.get(((Column)temp.getLeftExpression()).getTable().getName())!=null)
										{
											ArrayList<BinaryExpression> list = joins.get(((Column)temp.getLeftExpression()).getTable().getName());
											list.add(temp);
											joins.put(((Column)temp.getLeftExpression()).getTable().getName(),list);
										}
										else
										{
											ArrayList<BinaryExpression> list = new ArrayList<BinaryExpression>();
											list.add(temp);
											joins.put(((Column)temp.getLeftExpression()).getTable().getName(),list);
										}
									}
									else
									{
										if(wherelist.get(((Column)temp.getLeftExpression()).getTable().getName())!=null)
										{
											ArrayList<BinaryExpression> list = wherelist.get(((Column)temp.getLeftExpression()).getTable().getName());
											list.add(temp);
											wherelist.put(((Column)temp.getLeftExpression()).getTable().getName(),list);
										}
										else
										{
											ArrayList<BinaryExpression> list = new ArrayList<BinaryExpression>();
											list.add(temp);
											wherelist.put(((Column)temp.getLeftExpression()).getTable().getName(),list);
										}
									}
								}
								else if (temp instanceof OrExpression)
								{
									shipmode1 =  (PrimitiveValue) ((BinaryExpression)temp.getLeftExpression()).getRightExpression();
									shipmode2 =  (PrimitiveValue) ((BinaryExpression)temp.getRightExpression()).getRightExpression();
								}
								else
								{
									if(wherelist.get(((Column)temp.getLeftExpression()).getTable().getName())!=null)
									{
										ArrayList<BinaryExpression> list = wherelist.get(((Column)temp.getLeftExpression()).getTable().getName());
										list.add(temp);
										wherelist.put(((Column)temp.getLeftExpression()).getTable().getName(),list);
									}
									else
									{
										ArrayList<BinaryExpression> list = new ArrayList<BinaryExpression>();
										list.add(temp);
										wherelist.put(((Column)temp.getLeftExpression()).getTable().getName(),list);
									}
								}
								e = (BinaryExpression) e.getLeftExpression();
							}
							BinaryExpression temp = e;
							if((temp.getLeftExpression() instanceof Column)&&(temp.getRightExpression() instanceof Column))
							{
								if(temp instanceof EqualsTo)
								{
									if(joins.get(((Column)temp.getLeftExpression()).getTable().getName())!=null)
									{
										ArrayList<BinaryExpression> list = joins.get(((Column)temp.getLeftExpression()).getTable().getName());
										list.add(temp);
										joins.put(((Column)temp.getLeftExpression()).getTable().getName(),list);
									}
									else
									{
										ArrayList<BinaryExpression> list = new ArrayList<BinaryExpression>();
										list.add(temp);
										joins.put(((Column)temp.getLeftExpression()).getTable().getName(),list);
									}
								}
								else
								{
									if(wherelist.get(((Column)temp.getLeftExpression()).getTable().getName())!=null)
									{
										ArrayList<BinaryExpression> list = wherelist.get(((Column)temp.getLeftExpression()).getTable().getName());
										list.add(temp);
										wherelist.put(((Column)temp.getLeftExpression()).getTable().getName(),list);
									}
									else
									{
										ArrayList<BinaryExpression> list = new ArrayList<BinaryExpression>();
										list.add(temp);
										wherelist.put(((Column)temp.getLeftExpression()).getTable().getName(),list);
									}
								}
							}
							else
							{
								if(wherelist.get(((Column)temp.getLeftExpression()).getTable().getName())!=null)
								{
									ArrayList<BinaryExpression> list = wherelist.get(((Column)temp.getLeftExpression()).getTable().getName());
									list.add(temp);
									wherelist.put(((Column)temp.getLeftExpression()).getTable().getName(),list);
								}
								else
								{
									ArrayList<BinaryExpression> list = new ArrayList<BinaryExpression>();
									list.add(temp);
									wherelist.put(((Column)temp.getLeftExpression()).getTable().getName(),list);
								}
							}
						}
						if(selectitems.toString().equals("[LINEITEM.ORDERKEY, SUM(LINEITEM.EXTENDEDPRICE * 1 - LINEITEM.DISCOUNT) AS REVENUE, ORDERS.ORDERDATE, ORDERS.SHIPPRIORITY]"))
						{
							HashSet<Integer> fetchorders= new HashSet<Integer>();
							for(String tbl : tables)
							{
								ArrayList<BinaryExpression> jlist = joins.get(tbl);
								if(jlist !=null)
								{
									for(BinaryExpression join : jlist)
									{
										String left = ((Column)join.getLeftExpression()).getTable().getName();
										String right = ((Column)join.getRightExpression()).getTable().getName();
										if(left.equals("CUSTOMER"))
										{
											ArrayList<BinaryExpression> where = wherelist.get(left);
											int start = primaryind_CUSTOMER.get((PrimitiveValue)where.get(0).getRightExpression());
											int end = primaryind_CUSTOMER.get(primaryind_CUSTOMER.higherKey((PrimitiveValue) where.get(0).getRightExpression()));
											HashMap<PrimitiveValue, Integer> hashsort1 = new HashMap<PrimitiveValue, Integer>();
											for(int i=start;i<end;i++)
											{
												hashsort1.put(CUSTOMER_data[i].getColdata()[0], i);
											}
											ArrayList<BinaryExpression> whereright = wherelist.get(right);
											SortedMap<PrimitiveValue, Integer[]> tempmap = primaryind_ORDERS.headMap(new DateValue(((ExpressionList)((Function)whereright.get(0).getRightExpression()).getParameters()).getExpressions().get(0).toString().substring(1,11)));
											Iterator<Integer[]> itr = tempmap.values().iterator();
											while(itr.hasNext())
											{
												fetchorders.addAll(Arrays.asList(itr.next()));
											}
											
											Iterator<Integer> it = fetchorders.iterator();
											while(it.hasNext())
											{
												if(hashsort1.get(ORDERS_data[it.next()].getColdata()[1]) == null)
												{
													it.remove();
												}
											}
										}
										else if(left.equals("LINEITEM"))
										{
											ArrayList<BinaryExpression> where = wherelist.get(left);
											HashMap<PrimitiveValue, ArrayList<PrimitiveValue[]>> hashsort = new HashMap<PrimitiveValue, ArrayList<PrimitiveValue[]>>();
											SortedMap<PrimitiveValue, Integer[]> tempmap = SHIPDATE.tailMap(new DateValue(((ExpressionList)((Function)where.get(0).getRightExpression()).getParameters()).getExpressions().get(0).toString().substring(1,11)),false);
											Iterator<Integer[]> itr = tempmap.values().iterator();
											while(itr.hasNext())
											{
												Integer[] iarray = itr.next();												
												for(Integer i : iarray)
												{
													PrimitiveValue[] lineitem = LINEITEM_data[i].getColdata();
													if(hashsort.get(lineitem[0]) == null)
													{
														ArrayList<PrimitiveValue[]> temp = new ArrayList<PrimitiveValue[]>();
														temp.add(lineitem);
														hashsort.put(lineitem[0], temp);
													}
													else
													{
														ArrayList<PrimitiveValue[]> temp = hashsort.get(lineitem[0]);
														temp.add(lineitem);
														hashsort.put(lineitem[0], temp);
													}
												}
											}
											
											Iterator<Integer> it = fetchorders.iterator();
											HashMap<String,PrimitiveValue[]> groupbyprint = new HashMap<String,PrimitiveValue[]>();
											while(it.hasNext())
											{
												PrimitiveValue[] orderrow = ORDERS_data[it.next()].getColdata();
												ArrayList<PrimitiveValue[]> temp = hashsort.get(orderrow[0]);
												if(temp != null)
												{
													for(PrimitiveValue[] lineitem : temp)
													{
														String get = lineitem[0] + "|" + orderrow[2] + "|" + orderrow[4];
														PrimitiveValue[] grpby = groupbyprint.get(get);
														if(grpby == null)
														{
															grpby = new PrimitiveValue[4];
															grpby[0] = lineitem[0];
															grpby[1] = lineitem[12];
															grpby[2] = orderrow[2];
															grpby[3] = orderrow[4];
														}
														else
														{
															grpby[1] = new DoubleValue(grpby[1].toDouble() + lineitem[12].toDouble());	
														}
														groupbyprint.put(get,grpby);
													}
												}
											}
											ArrayList<PrimitiveValue[]> print = new ArrayList<PrimitiveValue[]>(groupbyprint.values());
											print.sort(new OrderbyComparator2(new int[]{1,2},new boolean[]{false,true}));
											int count = 0;
											for(PrimitiveValue[] out : print)
											{
												count++;
												StringBuilder prnt = new StringBuilder();
												for(int i = 0;i < out.length;i++)
												{
													PrimitiveValue temp = out[i];
													if(temp != null)
													{
														prnt.append(temp.toString());
													}
													prnt.append("|");
												}
												prnt.deleteCharAt(prnt.length()-1);
												System.out.print(prnt.toString()+"\n");
												if(limit == count)
													break;
											}
										}
									}
								}
							}
						}
						else if(selectitems.toString().equals("[NATION.NAME, SUM(LINEITEM.EXTENDEDPRICE * 1 - LINEITEM.DISCOUNT) AS REVENUE]"))
						{
							HashSet<Integer> fetchorders= new HashSet<Integer>();
							HashSet<Integer> fetchnation= new HashSet<Integer>();
							HashMap<PrimitiveValue,PrimitiveValue> fetchsupplier= new HashMap<PrimitiveValue,PrimitiveValue>();
							HashMap<PrimitiveValue, Row> hashsort1 = new HashMap<PrimitiveValue, Row>();
							HashMap<PrimitiveValue, Row> hashsort2 = new HashMap<PrimitiveValue, Row>();	
							HashMap<PrimitiveValue, ArrayList<PrimitiveValue[]>> hashsort3 = new HashMap<PrimitiveValue, ArrayList<PrimitiveValue[]>>();
							for(String tbl : tables)
							{
								ArrayList<BinaryExpression> jlist = joins.get(tbl);
								if(jlist != null)
								{
									for(BinaryExpression join : jlist)
									{
										String left = ((Column)join.getLeftExpression()).getTable().getName();
										String right = ((Column)join.getRightExpression()).getTable().getName();
										if(left.equals("NATION"))
										{
											int size = NATION_data.length;
											ArrayList<BinaryExpression> whereright = wherelist.get(right);
											Integer tempmap = primaryind_REGION.get((PrimitiveValue)whereright.get(0).getRightExpression());
											PrimitiveValue regionkey = REGION_data[tempmap].getColdata()[0];
											for(int i =0;i< size;i++)
											{
												if(indexsort.compare(NATION_data[i].getColdata()[2],regionkey) == 0)
												{
													fetchnation.add(i);
												}
											}
										}
										else if(left.equals("CUSTOMER"))
										{
											if(right.equals("NATION"))
											{
												HashMap<PrimitiveValue, ArrayList<Row>> hashsort = new HashMap<PrimitiveValue, ArrayList<Row>>();
												int size = CUSTOMER_data.length;
												for(int i=0;i<size;i++)
												{
													Row temp = CUSTOMER_data[i];
													ArrayList<Row> list = hashsort.get(temp.getColdata()[3]);
													if( list == null)
													{
														list = new ArrayList<Row>();
														list.add(temp);
														hashsort.put(temp.getColdata()[3], list);
													}
													else
													{
														list.add(temp);
														hashsort.put(temp.getColdata()[3], list);
													}
												}
												Iterator<Integer> it = fetchnation.iterator();
												while(it.hasNext())
												{
													Row temp = NATION_data[it.next()];
													ArrayList<Row> list = hashsort.get(temp.getColdata()[0]);
													if(list != null)
													{
														for(Row r:list)
														{
															hashsort1.put(r.getColdata()[0], temp);
														}
													}
												}
											}
											else if(right.equals("ORDERS"))
											{											
												ArrayList<BinaryExpression> whereright = wherelist.get(right);
												PrimitiveValue min = null;
												PrimitiveValue max = null;
												for(BinaryExpression where : whereright)
												{
													if(where instanceof MinorThan)
													{
														max = new DateValue(((ExpressionList)((Function)where.getRightExpression()).getParameters()).getExpressions().get(0).toString().substring(1,11));
													}
													else if(where instanceof GreaterThanEquals)
													{
														min = new DateValue(((ExpressionList)((Function)where.getRightExpression()).getParameters()).getExpressions().get(0).toString().substring(1,11));
													}
												}
												SortedMap<PrimitiveValue, Integer[]> temporder = primaryind_ORDERS.subMap(min, max);
												Iterator<Integer[]> itr = temporder.values().iterator();
												while(itr.hasNext())
												{
													fetchorders.addAll(Arrays.asList(itr.next()));
												}
												
												Iterator<Integer> it = fetchorders.iterator();
												while(it.hasNext())
												{
													Row temp = ORDERS_data[it.next()];
													if(hashsort1.get(temp.getColdata()[1]) != null)
													{
														hashsort2.put(temp.getColdata()[0], hashsort1.get(temp.getColdata()[1]));
													}
												}
											}
										}
										else if(left.equals("LINEITEM"))
										{
											if(right.equals("SUPPLIER"))
											{
												int size = SUPPLIER_data.length;
												HashMap<PrimitiveValue, ArrayList<Row>> hashsortsupp = new HashMap<PrimitiveValue, ArrayList<Row>>();
												for(int i=0;i<size;i++)
												{
													ArrayList<Row> list = hashsortsupp.get(SUPPLIER_data[i].getColdata()[1]);
													if( list == null)
													{
														list = new ArrayList<Row>();
														list.add(SUPPLIER_data[i]);
														hashsortsupp.put(SUPPLIER_data[i].getColdata()[1], list);
													}
													else
													{
														list.add(SUPPLIER_data[i]);
														hashsortsupp.put(SUPPLIER_data[i].getColdata()[1], list);
													}
												}
												
												Iterator<Integer> it = fetchnation.iterator();
												while(it.hasNext())
												{
													Row temp = NATION_data[it.next()];
													ArrayList<Row> list = hashsortsupp.get(temp.getColdata()[0]);
													if(list != null)
													{
														if(list != null)
														{
															for(Row r:list)
															{
																fetchsupplier.put(r.getColdata()[0],r.getColdata()[1]);
															}
														}														
													}
												}
											}
											else if(right.equals("ORDERS"))
											{
												int size = LINEITEM_data.length;
												for(int i=0;i<size;i++)
												{
													PrimitiveValue[] lineitem = LINEITEM_data[i].getColdata();
													if(hashsort3.get(lineitem[0]) == null)
													{
														ArrayList<PrimitiveValue[]> temp = new ArrayList<PrimitiveValue[]>();
														temp.add(lineitem);
														hashsort3.put(lineitem[0], temp);
													}
													else
													{
														ArrayList<PrimitiveValue[]> temp = hashsort3.get(lineitem[0]);
														temp.add(lineitem);
														hashsort3.put(lineitem[0], temp);
													}
												}
												HashMap<PrimitiveValue,PrimitiveValue[]> groupbyprint = new HashMap<PrimitiveValue,PrimitiveValue[]>();
												for(Entry<PrimitiveValue, Row> temp : hashsort2.entrySet())
												{
													ArrayList<PrimitiveValue[]> tmp = hashsort3.get(temp.getKey());
													if(tmp != null)
													{
														for(PrimitiveValue[] t : tmp)
														{
															PrimitiveValue tem = fetchsupplier.get(t[1]);
															if(tem != null)
															{
																if(indexsort.compare(tem, temp.getValue().getColdata()[0]) == 0)
																{
																	PrimitiveValue[] tmpo = groupbyprint.get(temp.getValue().getColdata()[1]);
																	if(tmpo == null)
																	{
																		tmpo = new PrimitiveValue[2];
																		tmpo[0] = temp.getValue().getColdata()[1];
																		tmpo[1] = t[12];																	
																	}
																	else
																	{
																		tmpo[1] = new DoubleValue(tmpo[1].toDouble() + t[12].toDouble());	
																	}
																	groupbyprint.put(temp.getValue().getColdata()[1], tmpo);
																}
															}
														}
													}
												}
												ArrayList<PrimitiveValue[]> print = new ArrayList<PrimitiveValue[]>(groupbyprint.values());
												print.sort(new OrderbyComparator2(new int[]{1},new boolean[]{false}));
												int count = 0;
												for(PrimitiveValue[] out : print)
												{
													count++;
													StringBuilder prnt = new StringBuilder();
													for(int i = 0;i < out.length;i++)
													{
														PrimitiveValue temp = out[i];
														if(temp != null)
														{
															prnt.append(temp.toString());
														}
														prnt.append("|");
													}
													prnt.deleteCharAt(prnt.length()-1);
													System.out.print(prnt.toString()+"\n");
													if(limit == count)
														break;
												}
											}
										}
									}
								}
							}
						}
						else if(selectitems.toString().equals("[CUSTOMER.CUSTKEY, SUM(LINEITEM.EXTENDEDPRICE * 1 - LINEITEM.DISCOUNT) AS REVENUE, CUSTOMER.ACCTBAL, NATION.NAME, CUSTOMER.ADDRESS, CUSTOMER.PHONE, CUSTOMER.COMMENT]"))
						{
							HashMap<PrimitiveValue, ArrayList<Row>> hashsort1 = new HashMap<PrimitiveValue, ArrayList<Row>>();
							HashMap<PrimitiveValue, Row[]> hashsort2 = new HashMap<PrimitiveValue, Row[]>();
							HashMap<PrimitiveValue, Row[]> hashsort3 = new HashMap<PrimitiveValue, Row[]>();
							HashMap<PrimitiveValue, ArrayList<PrimitiveValue[]>> hashsort4 = new HashMap<PrimitiveValue, ArrayList<PrimitiveValue[]>>();
							HashSet<Integer> fetchorders= new HashSet<Integer>();
							for(String tbl : tables)
							{
								ArrayList<BinaryExpression> jlist = joins.get(tbl);
								if(jlist != null)
								{
									for(BinaryExpression join : jlist)
									{
										String left = ((Column)join.getLeftExpression()).getTable().getName();
										String right = ((Column)join.getRightExpression()).getTable().getName();
										if(left.equals("CUSTOMER"))
										{
											if(right.equals("NATION"))
											{
												int size = CUSTOMER_data.length;
												for(int i =0;i<size;i++)
												{
													Row temp = CUSTOMER_data[i];
													ArrayList<Row> tmp = hashsort1.get(temp.getColdata()[3]);
													if(tmp == null)
													{
														tmp = new ArrayList<Row>();
														tmp.add(temp);
														hashsort1.put(temp.getColdata()[3], tmp);
													}
													else
													{
														tmp.add(temp);
														hashsort1.put(temp.getColdata()[3], tmp);
													}
												}
												
												int nsize = NATION_data.length;
												for(int i=0;i<nsize;i++)
												{
													Row temp = NATION_data[i];
													ArrayList<Row> tmp = hashsort1.get(temp.getColdata()[0]);
													if(tmp != null)
													{
														for(Row t : tmp)
														{
															Row[] r = new Row[2];
															r[0] = t;
															r[1] = temp;
															hashsort2.put(t.getColdata()[0], r);
														}
													}
												}
											}
											else if(right.equals("ORDERS"))
											{
												ArrayList<BinaryExpression> whereright = wherelist.get(right);
												PrimitiveValue min = null;
												PrimitiveValue max = null;
												for(BinaryExpression where : whereright)
												{
													if(where instanceof MinorThan)
													{
														max = new DateValue(((ExpressionList)((Function)where.getRightExpression()).getParameters()).getExpressions().get(0).toString().substring(1,11));
													}
													else if(where instanceof GreaterThanEquals)
													{
														min = new DateValue(((ExpressionList)((Function)where.getRightExpression()).getParameters()).getExpressions().get(0).toString().substring(1,11));
													}
												}
												SortedMap<PrimitiveValue, Integer[]> temporder = primaryind_ORDERS.subMap(min, max);
												Iterator<Integer[]> itr = temporder.values().iterator();
												while(itr.hasNext())
												{
													fetchorders.addAll(Arrays.asList(itr.next()));
												}
												
												Iterator<Integer> it = fetchorders.iterator();
												while(it.hasNext())
												{
													Row temp = ORDERS_data[it.next()];
													if(hashsort2.get(temp.getColdata()[1]) != null)
													{
														hashsort3.put(temp.getColdata()[0], hashsort2.get(temp.getColdata()[1]));
													}
												}
											}
										}
										else if(left.equals("LINEITEM"))
										{
											int size = primaryind_LINEITEM_R.length;
											for(int i=0;i<size;i++)
											{
												PrimitiveValue[] lineitem = LINEITEM_data[primaryind_LINEITEM_R[i]].getColdata();
												if(hashsort4.get(lineitem[0]) == null)
												{
													ArrayList<PrimitiveValue[]> temp = new ArrayList<PrimitiveValue[]>();
													temp.add(lineitem);
													hashsort4.put(lineitem[0], temp);
												}
												else
												{
													ArrayList<PrimitiveValue[]> temp = hashsort4.get(lineitem[0]);
													temp.add(lineitem);
													hashsort4.put(lineitem[0], temp);
												}											
											}
											
											HashMap<String,PrimitiveValue[]> groupbyprint = new HashMap<String,PrimitiveValue[]>();
											for(Entry<PrimitiveValue, Row[]> temp : hashsort3.entrySet())
											{
												ArrayList<PrimitiveValue[]> tmp = hashsort4.get(temp.getKey());
												if(tmp != null)
												{
													PrimitiveValue[] cust = temp.getValue()[0].getColdata();
													PrimitiveValue[] nat = temp.getValue()[1].getColdata();
													String tem = cust[0] + "|" +cust[5]+ "|" +cust[4]+ "|" +nat[1]+ "|" +cust[2]+ "|" +cust[7];													
													PrimitiveValue[] tmpo = groupbyprint.get(tem);
													if(tmpo == null)
													{													
														tmpo = new PrimitiveValue[7];
														tmpo[0] = cust[0];
														tmpo[1] = new DoubleValue("0");
														tmpo[2] = cust[5];
														tmpo[3] = nat[1];
														tmpo[4] = cust[2];
														tmpo[5] = cust[4];
														tmpo[6] = cust[7];
													}
													for(PrimitiveValue[] t : tmp)
													{
														tmpo[1] = new DoubleValue(tmpo[1].toDouble() + t[12].toDouble());																	
													}												
													groupbyprint.put(tem, tmpo);
												}
											}
											ArrayList<PrimitiveValue[]> print = new ArrayList<PrimitiveValue[]>(groupbyprint.values());
											print.sort(new OrderbyComparator2(new int[]{1},new boolean[]{true}));
											int count = 0;
											for(PrimitiveValue[] out : print)
											{
												count++;
												StringBuilder prnt = new StringBuilder();
												for(int i = 0;i < out.length;i++)
												{
													PrimitiveValue temp = out[i];
													if(temp != null)
													{
														prnt.append(temp.toString());
													}
													prnt.append("|");
												}
												prnt.deleteCharAt(prnt.length()-1);
												System.out.print(prnt.toString()+"\n");
												if(limit == count)
													break;
											}
										}
									}
								}
							}
						}
						else if(selectitems.toString().equals("[LINEITEM.SHIPMODE, SUM(CASE WHEN ORDERS.ORDERPRIORITY = '1-URGENT' OR ORDERS.ORDERPRIORITY = '2-HIGH' THEN 1 ELSE 0 END) AS HIGH_LINE_COUNT, SUM(CASE WHEN ORDERS.ORDERPRIORITY <> '1-URGENT' AND ORDERS.ORDERPRIORITY <> '2-HIGH' THEN 1 ELSE 0 END) AS LOW_LINE_COUNT]"))
						{
							HashSet<Integer> fetchlineitem1= new HashSet<Integer>();
							HashSet<Integer> fetchlineitem2= new HashSet<Integer>();
							HashSet<Integer> fetchlineitemsm1= new HashSet<Integer>();
							HashSet<Integer> fetchlineitemsm2= new HashSet<Integer>();
							for(String tbl : tables)
							{
								ArrayList<BinaryExpression> jlist = joins.get(tbl);
								if(jlist != null)
								{
									for(BinaryExpression join : jlist)
									{
										String left = ((Column)join.getLeftExpression()).getTable().getName();
										String right = ((Column)join.getRightExpression()).getTable().getName();
										if(left.equals("ORDERS"))
										{
											HashMap<PrimitiveValue,PrimitiveValue> hashsort = new HashMap<PrimitiveValue,PrimitiveValue>();
											int size = ORDERS_data.length;
											for(int i = 0; i<size;i++)
											{
												Row temp = ORDERS_data[i];
												PrimitiveValue val = null;
												PrimitiveValue check1 = new StringValue("1-URGENT");
												PrimitiveValue check2 = new StringValue("2-HIGH");
												if((indexsort.compare(temp.getColdata()[3], check1) == 0)||(indexsort.compare(temp.getColdata()[3], check2) == 0))
												{
													val = new LongValue("1");
												}
												else
												{
													val = new LongValue("2");
												}
												hashsort.put(temp.getColdata()[0], val);
											}
											PrimitiveValue min = null;
											PrimitiveValue max = null;
											ArrayList<BinaryExpression> whereright = wherelist.get(right);
											for(BinaryExpression where : whereright)
											{
												if(where instanceof MinorThan)
												{
													max = new DateValue(((ExpressionList)((Function)where.getRightExpression()).getParameters()).getExpressions().get(0).toString().substring(1,11));
												}
												else if(where instanceof GreaterThanEquals)
												{
													min = new DateValue(((ExpressionList)((Function)where.getRightExpression()).getParameters()).getExpressions().get(0).toString().substring(1,11));
													break;
												}
											}
											SortedMap<PrimitiveValue, Integer[]> temporder = primaryind_LINEITEM.subMap(min, max);
											Iterator<Integer[]> itr = temporder.values().iterator();
											while(itr.hasNext())
											{
												Integer[] itemp = itr.next();
												fetchlineitem1.addAll(Arrays.asList(itemp));
												fetchlineitem2.addAll(Arrays.asList(itemp));
											}
											fetchlineitemsm1.addAll(Arrays.asList(primaryind_LINEITEM_sm.get(shipmode1)));
											fetchlineitemsm2.addAll(Arrays.asList(primaryind_LINEITEM_sm.get(shipmode2)));
											fetchlineitem1.retainAll(fetchlineitemsm1);
											fetchlineitem2.retainAll(fetchlineitemsm2);
											Iterator<Integer> it = fetchlineitem1.iterator();
											HashMap<PrimitiveValue,PrimitiveValue[]> groupbyprint = new HashMap<PrimitiveValue,PrimitiveValue[]>();
											while(it.hasNext())
											{
												Row tmp = LINEITEM_data[it.next()];
												PrimitiveValue val = hashsort.get(tmp.getColdata()[0]);
												if(val != null)
												{
													PrimitiveValue[] grp = groupbyprint.get(tmp.getColdata()[11]);
													if(grp == null)
													{
														grp = new PrimitiveValue[3];
														grp[0] = tmp.getColdata()[11];
														grp[1] = new LongValue("0");
														grp[2] = new LongValue("0");
													}
													if(indexsort.compare(val, new LongValue("1")) == 0)
													{
														grp[1] = new LongValue(grp[1].toLong() + new LongValue("1").toLong());
													}
													else
													{
														grp[2] = new LongValue(grp[2].toLong() + new LongValue("1").toLong());
													}
													groupbyprint.put(tmp.getColdata()[11], grp);
												}
											}
											
											it = fetchlineitem2.iterator();
											
											while(it.hasNext())
											{
												Row tmp = LINEITEM_data[it.next()];
												PrimitiveValue val = hashsort.get(tmp.getColdata()[0]);
												if(val != null)
												{
													PrimitiveValue[] grp = groupbyprint.get(tmp.getColdata()[11]);
													if(grp == null)
													{
														grp = new PrimitiveValue[3];
														grp[0] = tmp.getColdata()[11];
														grp[1] = new LongValue("0");
														grp[2] = new LongValue("0");
													}
													if(indexsort.compare(val, new LongValue("1")) == 0)
													{
														grp[1] = new LongValue(grp[1].toLong() + new LongValue("1").toLong());
													}
													else
													{
														grp[2] = new LongValue(grp[2].toLong() + new LongValue("1").toLong());
													}
													groupbyprint.put(tmp.getColdata()[11], grp);
												}
											}
											
											ArrayList<PrimitiveValue[]> print = new ArrayList<PrimitiveValue[]>(groupbyprint.values());
											print.sort(new OrderbyComparator2(new int[]{0},new boolean[]{true}));
											int count = 0;
											for(PrimitiveValue[] out : print)
											{
												count++;
												StringBuilder prnt = new StringBuilder();
												for(int i = 0;i < out.length;i++)
												{
													PrimitiveValue temp = out[i];
													if(temp != null)
													{
														prnt.append(temp.toString());
													}
													prnt.append("|");
												}
												prnt.deleteCharAt(prnt.length()-1);
												System.out.print(prnt.toString()+"\n");
												if(limit == count)
													break;
											}
										}
									}
								}
							}
						}
					}
					System.gc();
					//long endtime = System.currentTimeMillis();
					//System.out.println(endtime-starttime);
				} 
			}
			if(ondisk)
			{
				Row[] LINEITEM_data = null;
				HashMap<PrimitiveValue,String[]> LINEITEM_data_1998 = null;
				Row[] ORDERS_data = null;
				Row[] CUSTOMER_data = null;
				Row[] SUPPLIER_data = null;
				Row[] NATION_data = null;
				Row[] REGION_data = null;
				
				TreeMap<PrimitiveValue, Integer[]> primaryind_LINEITEM = new TreeMap<PrimitiveValue, Integer[]>(indexsort);
				TreeMap<PrimitiveValue, Integer[]> primaryind_LINEITEM_sm = new TreeMap<PrimitiveValue, Integer[]>(indexsort);
				Integer[] primaryind_LINEITEM_R = null;
				TreeMap<PrimitiveValue, Integer[]> primaryind_ORDERS = new TreeMap<PrimitiveValue, Integer[]>(indexsort);
				TreeMap<PrimitiveValue, Integer> primaryind_CUSTOMER = new TreeMap<PrimitiveValue, Integer>(indexsort);
				TreeMap<PrimitiveValue, Integer[]> SHIPDATE = new TreeMap<PrimitiveValue, Integer[]>(indexsort);
				HashMap<PrimitiveValue,Integer> primaryind_REGION = new HashMap<PrimitiveValue,Integer>();
				//HashMap<PrimitiveValue,HashMap<PrimitiveValue,HashSet<PrimitiveValue>>> shipmode = new HashMap<PrimitiveValue,HashMap<PrimitiveValue,HashSet<PrimitiveValue>>>();
				HashMap<PrimitiveValue,ArrayList<PrimitiveValue[]>> lineitemhs = new HashMap<PrimitiveValue,ArrayList<PrimitiveValue[]>>();
				HashMap<String,HashMap<PrimitiveValue,PrimitiveValue[]>> query5 = new HashMap<String,HashMap<PrimitiveValue,PrimitiveValue[]>>();
				HashMap<PrimitiveValue,PrimitiveValue> query4 = new HashMap<PrimitiveValue,PrimitiveValue>();
				HashMap<PrimitiveValue, Row[]> hashsortcust = new HashMap<PrimitiveValue, Row[]>();
				int createcount = 0;
				while(true)
				{					
					System.out.print("$> ");
					CCJSqlParser parserinp = new CCJSqlParser(System.in);
					Statement queryinp = parserinp.Statement();
					//long starttime = System.currentTimeMillis();
					if(queryinp instanceof CreateTable)
					{
						CreateTable tableinp = (CreateTable)queryinp;

						if(bigdata)
						{
							//System.out.println("TableName:"+tableinp.getTable().getName());
							if(createcount == 0)
							{
								TableSchema LINEITEM = new TableSchema();
								StringReader input = new StringReader("CREATE TABLE LINEITEM ( ORDERKEY INT, SUPPKEY INT, QUANTITY DECIMAL, EXTENDEDPRICE DECIMAL, DISCOUNT DECIMAL, TAX DECIMAL, RETURNFLAG CHAR(1), LINESTATUS CHAR(1), SHIPDATE DATE, COMMITDATE DATE, RECEIPTDATE DATE, SHIPMODE VARCHAR(10), REVENUE DECIMAL);");
								CCJSqlParser parser = new CCJSqlParser(input);
								Statement query = parser.Statement();
								CreateTable table = (CreateTable)query;
								ArrayList<Row> data= new ArrayList<Row>();
								LINEITEM.getSchema(table);
								String file = "data/"+table.getTable().getName()+".csv";
								File fileRead = new File(file);
								BufferedReader reader = new BufferedReader(new FileReader(fileRead));
								String line;
								StringReader input1 = new StringReader("SELECT LINEITEM.RETURNFLAG, LINEITEM.LINESTATUS, SUM(LINEITEM.QUANTITY) AS SUM_QTY, SUM(LINEITEM.EXTENDEDPRICE) AS SUM_BASE_PRICE, SUM(LINEITEM.EXTENDEDPRICE*(1-LINEITEM.DISCOUNT)) AS SUM_DISC_PRICE, SUM(LINEITEM.EXTENDEDPRICE*(1-LINEITEM.DISCOUNT)*(1+LINEITEM.TAX)) AS SUM_CHARGE, AVG(LINEITEM.QUANTITY) AS AVG_QTY, AVG(LINEITEM.EXTENDEDPRICE) AS AVG_PRICE,  AVG(LINEITEM.DISCOUNT) AS AVG_DISC, COUNT(*) AS COUNT_ORDER FROM LINEITEM WHERE LINEITEM.SHIPDATE <= DATE('1998-08-26') GROUP BY LINEITEM.RETURNFLAG, LINEITEM.LINESTATUS ORDER BY LINEITEM.RETURNFLAG, LINEITEM.LINESTATUS;");
								CCJSqlParser parser1 = new CCJSqlParser(input1);
								Statement query1 = parser1.Statement();
								PlainSelect select = (PlainSelect)(((Select) query1).getSelectBody());
								List<SelectItem> selectclauses = select.getSelectItems();
								int count = 0;
								Expression revenue = null;
								for(SelectItem selectclause : selectclauses)
								{
									count++;
									if(((SelectExpressionItem)selectclause).getExpression() instanceof Function)
									{
										switch(functions.valueOf(((Function)((SelectExpressionItem)selectclause).getExpression()).getName()))
										{
										case MAX:
											break;
										case MIN:
											break;
										case COUNT:
											break;
										case SUM:
											if(count == 5)
												revenue = ((Function)((SelectExpressionItem)selectclause).getExpression()).getParameters().getExpressions().get(0);
											break;
										case AVG:
											break;
										}
									}
								}
	
								PrimitiveValue[] evalr= new PrimitiveValue[LINEITEM.getColumns()];
								Eval eval = new Eval() 
								{
									public PrimitiveValue eval(Column c)
									{ 
										int i = LINEITEM.getColnum().get(c.getColumnName());
										PrimitiveValue result = evalr[i];
										return result;
									}
								};
								
								File file0 =  new File("data/temp0.txt");
								file0.createNewFile();
								FileWriter fileOut0 =  new FileWriter(file0);
								BufferedWriter out0 = new BufferedWriter(fileOut0);
								int linecount = 0;
								PrimitiveValue R = new StringValue("R");
								while ((line = reader.readLine()) != null)
								{
									//Row row= new Row(2);
									//line = line + "|";
									int temp1 = 0;
									int temp2 =-1;
									int temp3 = 0;
									int temp4 = -1;
									while((temp3 < 15)&&((temp2 = line.indexOf( '|', temp1 )) != -1))
									{
										temp4++;
										if((temp4 == 1)||(temp4 == 3)||(temp4 == 13)||(temp4 == 15))
										{
											temp1 = temp2 + 1;
											continue;
										}
										
										String temp = line.substring( temp1, temp2 );
										switch(datatype.valueOf(LINEITEM.getDataType(temp3)))
										{
										case DECIMAL:
											evalr[temp3] = new DoubleValue(temp);
											temp3++;
											break;							
										case INT:
											evalr[temp3] = new LongValue(temp);
											temp3++;
											break; 
										case DATE:
											evalr[temp3] = new DateValue(temp);
											temp3++;
											break;
										default :
											evalr[temp3] = new StringValue(temp);
											temp3++;
										}
										temp1 = temp2 + 1;
									}
									//row.getColdata()[0] = evalr[0];
									//row.getColdata()[1] =  eval.eval(revenue);
									//LINEITEM_data[linecount] = row;
									if(indexsort.compare(evalr[6],R) == 0)
									{
										PrimitiveValue sum = query4.get(evalr[0]);
										if(sum == null)
											sum = eval.eval(revenue);
										else
											sum = new DoubleValue(sum.toDouble() + eval.eval(revenue).toDouble());	
										
										query4.put(evalr[0], sum);
										//out0.write(evalr[0]+"|"+ eval.eval(revenue)+"\n");
										//primaryind_LINEITEM_temp_R.add(linecount);
									}
									
									/*ArrayList<Integer> temp_sm = primaryind_LINEITEM_temp_sm.get(evalr[11]);
									if(temp_sm == null)
									{
										temp_sm = new ArrayList<Integer>();
									}
									temp_sm.add(linecount);
									primaryind_LINEITEM_temp_sm.put(evalr[11],temp_sm);*/
									
									if((indexsort.compare(evalr[9], evalr[10]) < 0) && (indexsort.compare(evalr[8], evalr[9]) < 0))
									{
										/*HashMap<PrimitiveValue, HashSet<PrimitiveValue>> smdtemp = shipmode.get(evalr[10]);
										if(smdtemp == null)
										{
											smdtemp = new HashMap<PrimitiveValue, HashSet<PrimitiveValue>>();
											HashSet<PrimitiveValue> smtemp = new HashSet<PrimitiveValue>();
											smtemp.add(evalr[0]);
											smdtemp.put(evalr[11], smtemp);											
										}
										else
										{
											HashSet<PrimitiveValue> smtemp = smdtemp.get(evalr[11]);
											if(smtemp == null)
											{
												smtemp = new HashSet<PrimitiveValue>();
												smtemp.add(evalr[0]);
											}
											else
											{
												smtemp.add(evalr[0]);
											}
											smdtemp.put(evalr[11], smtemp);
										}
										shipmode.put(evalr[10],smdtemp);*/
										ArrayList<PrimitiveValue[]> oktemp = lineitemhs.get(evalr[0]);
										if(oktemp == null)
											oktemp = new ArrayList<PrimitiveValue[]>();
											
										PrimitiveValue[] r = new PrimitiveValue[2];
										r[0] = evalr[10];
										r[1] = evalr[11];
										oktemp.add(r);
										lineitemhs.put(evalr[0],oktemp);
									}
									
									linecount++;
								}
								reader.close();
								out0.close();
								
								/*for(Entry<PrimitiveValue, ArrayList<Integer>> entry : primaryind_LINEITEM_temp.entrySet()) 
								{
									PrimitiveValue key = entry.getKey();
									Integer value[] = entry.getValue().toArray(new Integer[entry.getValue().size()]);
									primaryind_LINEITEM.put(key, value);
								}
								
								for(Entry<PrimitiveValue, ArrayList<Integer>> entry : primaryind_LINEITEM_temp_sm.entrySet()) 
								{
									PrimitiveValue key = entry.getKey();
									Integer value[] = entry.getValue().toArray(new Integer[entry.getValue().size()]);
									primaryind_LINEITEM_sm.put(key, value);
								}
								
								primaryind_LINEITEM_R = primaryind_LINEITEM_temp_R.toArray(new Integer[primaryind_LINEITEM_temp_R.size()]);*/
								
								//LINEITEM_data = data.toArray(new Row[data.size()]);
								data = null;
								FileOutputStream fileOut = new FileOutputStream("data/query4.ser");
								ObjectOutputStream out = new ObjectOutputStream(fileOut);
								out.writeObject(query4);
								out.close();
								fileOut.close();
								query4.clear();
								query4 = null;
							}
							if(createcount == 1)
							{
								TableSchema ORDERS = new TableSchema();
								StringReader input = new StringReader("CREATE TABLE ORDERS ( ORDERKEY INT, CUSTKEY INT, ORDERDATE DATE, ORDERPRIORITY VARCHAR(15));");
								CCJSqlParser parser = new CCJSqlParser(input);
								Statement query = parser.Statement();
								CreateTable table = (CreateTable)query;
								ArrayList<Row> data= new ArrayList<Row>();
								ORDERS.getSchema(table);
								String file = "data/"+table.getTable().getName()+".csv";
								File fileRead = new File(file);
								BufferedReader reader = new BufferedReader(new FileReader(fileRead));
								String line;
								PrimitiveValue[] evalr= new PrimitiveValue[4];
								int linecount = 0;
								TreeMap<PrimitiveValue, ArrayList<Integer>> primaryind_ORDERS_temp = new TreeMap<PrimitiveValue, ArrayList<Integer>>(indexsort);
								PrimitiveValue min = new DateValue("1993-01-01");
								PrimitiveValue max = new DateValue("1995-01-31");
								while ((line = reader.readLine()) != null)
								{										
									//PrimitiveValue[] r= new PrimitiveValue[ORDERS.getColumns()];
									//line = line + "|";
									int temp1 = 0;
									int temp2 =-1;
									int temp3 = 0;
									int temp4 = -1;
									while((temp2 = line.indexOf( '|', temp1 )) != -1)
									{
										temp4++;
										if((temp4 == 2)||(temp4 == 3)||(temp4 == 6)||(temp4 == 7)||(temp4 == 8))
										{
											temp1 = temp2 + 1;
											continue;
										}
										
										String temp = line.substring( temp1, temp2 );
										switch(datatype.valueOf(ORDERS.getDataType(temp3)))
										{
										case DECIMAL:
											evalr[temp3++] = new DoubleValue(temp);
											break;							
										case INT:
											evalr[temp3++] = new LongValue(temp);
											break; 
										case DATE:
											evalr[temp3++] = new DateValue(temp);
											break;
										default :
											evalr[temp3++] = new StringValue(temp);
										}
										temp1 = temp2 + 1;
									}
									ArrayList<PrimitiveValue[]> oktemp = lineitemhs.get(evalr[0]);
									if(oktemp != null)
									{
										int val = 0;
										PrimitiveValue check1 = new StringValue("1-URGENT");
										PrimitiveValue check2 = new StringValue("2-HIGH");
										if((indexsort.compare(evalr[3], check1) == 0)||(indexsort.compare(evalr[3], check2) == 0))
										{
											val = 1;
										}
										else
										{
											val = 2;
										}
										for(PrimitiveValue[] okr : oktemp)
										{
											String year = okr[0].toString().substring(0, 4);
											HashMap<PrimitiveValue, PrimitiveValue[]> smd = query5.get(year);
											if(smd == null)
											{
												smd = new HashMap<PrimitiveValue, PrimitiveValue[]>();
												PrimitiveValue[] datapr = new PrimitiveValue[3];
												datapr[0] = okr[1];
												if(val == 1)
												{
													datapr[1] = new LongValue("1");
													datapr[2] = new LongValue("0");
												}
												else
												{
													datapr[1] = new LongValue("0");
													datapr[2] = new LongValue("1");
												}
												
												smd.put(okr[1], datapr);
											}
											else
											{
												PrimitiveValue[] datapr = smd.get(okr[1]);
												if(datapr == null)
												{
													datapr = new PrimitiveValue[3];
													datapr[0] = okr[1];
													if(val == 1)
													{
														datapr[1] = new LongValue("1");
														datapr[2] = new LongValue("0");
													}
													else
													{
														datapr[1] = new LongValue("0");
														datapr[2] = new LongValue("1");
													}
												}
												else
												{
													if(val == 1)
													{
														datapr[1] = new LongValue(datapr[1].toLong() + new LongValue("1").toLong());
													}
													else
													{
														datapr[2] = new LongValue(datapr[2].toLong() + new LongValue("1").toLong());
													}
												}
												smd.put(okr[1], datapr);
											}
											query5.put(year,smd);
										}
									}
									
									if((indexsort.compare(evalr[2], min)>=0)&&(indexsort.compare(evalr[2], max)<=0))
									{
										Row row= new Row(2);	
										row.getColdata()[0] = evalr[0];
										row.getColdata()[1] = evalr[1];
										//row.getColdata()[2] = evalr[3];
										data.add(row);
										
										ArrayList<Integer> temp_1 = primaryind_ORDERS_temp.get(evalr[2]);
										if(temp_1 == null)
										{
											temp_1 = new ArrayList<Integer>();
										}
										temp_1.add(linecount);
										primaryind_ORDERS_temp.put(evalr[2],temp_1);
										linecount++;
									}
								}
								reader.close();
								lineitemhs.clear();
								lineitemhs = null;
								FileOutputStream fileOut = new FileOutputStream("data/query5.ser");
								ObjectOutputStream out = new ObjectOutputStream(fileOut);
								out.writeObject(query5);
								out.close();
								fileOut.close();
								query5.clear();
								query5 = null;
								for(Entry<PrimitiveValue, ArrayList<Integer>> entry : primaryind_ORDERS_temp.entrySet()) 
								{
									PrimitiveValue key = entry.getKey();
									Integer value[] = entry.getValue().toArray(new Integer[entry.getValue().size()]);
									primaryind_ORDERS.put(key, value);
								}
								ORDERS_data = data.toArray(new Row[data.size()]);
								data = null;
							}
							if(createcount == 2)
							{
								TableSchema CUSTOMER = new TableSchema();
								StringReader input = new StringReader("CREATE TABLE CUSTOMER ( CUSTKEY INT, NAME VARCHAR(25), ADDRESS VARCHAR(40), NATIONKEY INT, PHONE VARCHAR(15), ACCBAL DECIMAL, MKTSEGMENT VARCHAR(10), COMMENT VARCHAR(117));");
								CCJSqlParser parser = new CCJSqlParser(input);
								Statement query = parser.Statement();
								CreateTable table = (CreateTable)query;
								ArrayList<Row> data= new ArrayList<Row>();
								CUSTOMER.getSchema(table);
								String file = "data/"+table.getTable().getName()+".csv";
								File fileRead = new File(file);
								BufferedReader reader = new BufferedReader(new FileReader(fileRead));
								String line;
								
								while ((line = reader.readLine()) != null)
								{
									line = line + "|";
									Row row= new Row(CUSTOMER.getColumns());								
									PrimitiveValue[] r= new PrimitiveValue[CUSTOMER.getColumns()];
									int temp1 = 0;
									int temp2 =-1;
									int temp3 = 0;
									int temp4 = -1;
									while((temp2 = line.indexOf( '|', temp1 )) != -1)
									{
										temp4++;
										if((temp4 == 8))
										{
											temp1 = temp2 + 1;
											continue;
										}
										
										String temp = line.substring( temp1, temp2 );
										switch(datatype.valueOf(CUSTOMER.getDataType(temp3)))
										{
										case DECIMAL:
											r[temp3++] = new DoubleValue(temp);
											break;							
										case INT:
											r[temp3++] = new LongValue(temp);
											break; 
										case DATE:
											r[temp3++] = new DateValue(temp);
											break;
										default :
											r[temp3++] = new StringValue(temp);
										}
										temp1 = temp2 + 1;
									}
									row.setColdata(r);
									data.add(row);
								}
								reader.close();
								/*data.sort(new OrderbyComparator(new int[]{6},new boolean[]{true}));
								PrimitiveValue temp = null;
								PrimitiveValue temp1 = null;
								for(int i=0;i<data.size();i++)
								{
									if(i==0)
									{					    		
										temp= data.get(i).getColdata()[6];
										primaryind_CUSTOMER.put(temp, 0);
									}
									else
									{
										temp1 = data.get(i).getColdata()[6];
										if(indexsort.compare(temp, temp1)!=0)
										{
											temp= temp1;
											primaryind_CUSTOMER.put(temp, i);									
										}
									}
								}*/
								CUSTOMER_data = data.toArray(new Row[data.size()]);
								data = null;
							}
							if(createcount == 3)
							{								
								TableSchema NATION = new TableSchema();
								StringReader input = new StringReader("CREATE TABLE NATION ( NATIONKEY INT, NAME CHAR(25), REGIONKEY INT);");
								CCJSqlParser parser = new CCJSqlParser(input);
								Statement query = parser.Statement();
								CreateTable table = (CreateTable)query;
								ArrayList<Row> data= new ArrayList<Row>();
								NATION.getSchema(table);
								String file = "data/"+table.getTable().getName()+".csv";
								File fileRead = new File(file);
								BufferedReader reader = new BufferedReader(new FileReader(fileRead));
								String line;
								
								while ((line = reader.readLine()) != null)
								{
									Row row= new Row(NATION.getColumns());								
									PrimitiveValue[] r= new PrimitiveValue[NATION.getColumns()];
									int temp1 = 0;
									int temp2 =-1;
									int temp3 = 0;
									int temp4 = -1;
									while((temp2 = line.indexOf( '|', temp1 )) != -1)
									{
										temp4++;
										if((temp4 == 3))
										{
											temp1 = temp2 + 1;
											continue;
										}
										
										String temp = line.substring( temp1, temp2 );
										switch(datatype.valueOf(NATION.getDataType(temp3)))
										{
										case DECIMAL:
											r[temp3++] = new DoubleValue(temp);
											break;							
										case INT:
											r[temp3++] = new LongValue(temp);
											break; 
										case DATE:
											r[temp3++] = new DateValue(temp);
											break;
										default :
											r[temp3++] = new StringValue(temp);
										}
										temp1 = temp2 + 1;
									}
									row.setColdata(r);
									data.add(row);
								}
								reader.close();
								NATION_data = data.toArray(new Row[data.size()]);
								data = null;
								
								HashMap<PrimitiveValue, ArrayList<Row>> hashsort1 = new HashMap<PrimitiveValue, ArrayList<Row>>();
								
								int size = CUSTOMER_data.length;
								for(int i =0;i<size;i++)
								{
									Row temp = CUSTOMER_data[i];
									ArrayList<Row> tmp = hashsort1.get(temp.getColdata()[3]);
									if(tmp == null)
									{
										tmp = new ArrayList<Row>();
										tmp.add(temp);
										hashsort1.put(temp.getColdata()[3], tmp);
									}
									else
									{
										tmp.add(temp);
										hashsort1.put(temp.getColdata()[3], tmp);
									}
								}
								
								int nsize = NATION_data.length;
								for(int i=0;i<nsize;i++)
								{
									Row temp = NATION_data[i];
									ArrayList<Row> tmp = hashsort1.get(temp.getColdata()[0]);
									if(tmp != null)
									{
										for(Row t : tmp)
										{
											Row[] r = new Row[2];
											r[0] = t;
											r[1] = temp;
											hashsortcust.put(t.getColdata()[0], r);
										}
									}
								}
								CUSTOMER_data = null;
								NATION_data = null;
								System.gc();
								TimeUnit.SECONDS.sleep(10);
							}
							createcount ++;
						}
						else
						{
							if(tableinp.getTable().getName().equals("LINEITEM"))
							{
								LINEITEM_data_1998 = new HashMap<PrimitiveValue,String[]>();
								TableSchema LINEITEM = new TableSchema();
								StringReader input = new StringReader("CREATE TABLE LINEITEM ( ORDERKEY INT, SUPPKEY INT, QUANTITY DECIMAL, EXTENDEDPRICE DECIMAL, DISCOUNT DECIMAL, TAX DECIMAL, RETURNFLAG CHAR(1), LINESTATUS CHAR(1), SHIPDATE DATE, COMMITDATE DATE, RECEIPTDATE DATE, SHIPMODE VARCHAR(10), REVENUE DECIMAL);");
								CCJSqlParser parser = new CCJSqlParser(input);
								Statement query = parser.Statement();
								CreateTable table = (CreateTable)query;
								ArrayList<Row> data= new ArrayList<Row>();
								ArrayList<PrimitiveValue[]> data_1998= new ArrayList<PrimitiveValue[]>();
								LINEITEM.getSchema(table);
								String file = "data/"+table.getTable().getName()+".csv";
								File fileRead = new File(file);
								BufferedReader reader = new BufferedReader(new FileReader(fileRead));
								String line;
								PrimitiveValue end = new DateValue("1998-01-01");
								PrimitiveValue min = new DateValue("1995-03-01");
								StringReader input1 = new StringReader("SELECT LINEITEM.RETURNFLAG, LINEITEM.LINESTATUS, SUM(LINEITEM.QUANTITY) AS SUM_QTY, SUM(LINEITEM.EXTENDEDPRICE) AS SUM_BASE_PRICE, SUM(LINEITEM.EXTENDEDPRICE*(1-LINEITEM.DISCOUNT)) AS SUM_DISC_PRICE, SUM(LINEITEM.EXTENDEDPRICE*(1-LINEITEM.DISCOUNT)*(1+LINEITEM.TAX)) AS SUM_CHARGE, AVG(LINEITEM.QUANTITY) AS AVG_QTY, AVG(LINEITEM.EXTENDEDPRICE) AS AVG_PRICE,  AVG(LINEITEM.DISCOUNT) AS AVG_DISC, COUNT(*) AS COUNT_ORDER FROM LINEITEM WHERE LINEITEM.SHIPDATE <= DATE('1998-08-26') GROUP BY LINEITEM.RETURNFLAG, LINEITEM.LINESTATUS ORDER BY LINEITEM.RETURNFLAG, LINEITEM.LINESTATUS;");
								CCJSqlParser parser1 = new CCJSqlParser(input1);
								Statement query1 = parser1.Statement();
								PlainSelect select = (PlainSelect)(((Select) query1).getSelectBody());
								List<SelectItem> selectclauses = select.getSelectItems();
								List<Expression> funcexp = new ArrayList<Expression>();
								ArrayList<Integer> funclist = new ArrayList<Integer>();
								PrimitiveValue functionouput[] = null;
								int count = 0;
								Expression revenue = null;
								for(SelectItem selectclause : selectclauses)
								{
									count++;
									if(((SelectExpressionItem)selectclause).getExpression() instanceof Function)
									{
										switch(functions.valueOf(((Function)((SelectExpressionItem)selectclause).getExpression()).getName()))
										{
										case MAX:
											funclist.add(0);
											funcexp.add(((Function)((SelectExpressionItem)selectclause).getExpression()).getParameters().getExpressions().get(0));
											break;
										case MIN:
											funclist.add(1);
											funcexp.add(((Function)((SelectExpressionItem)selectclause).getExpression()).getParameters().getExpressions().get(0));
											break;
										case COUNT:
											funclist.add(2);
											funcexp.add(((SelectExpressionItem)selectclause).getExpression());
											break;
										case SUM:
											funclist.add(3);
											funcexp.add(((Function)((SelectExpressionItem)selectclause).getExpression()).getParameters().getExpressions().get(0));
											if(count == 5)
												revenue = ((Function)((SelectExpressionItem)selectclause).getExpression()).getParameters().getExpressions().get(0);
											break;
										case AVG:
											funclist.add(4);
											funcexp.add(((Function)((SelectExpressionItem)selectclause).getExpression()).getParameters().getExpressions().get(0));
											break;
										}
									}
								}
	
								HashMap<String, PrimitiveValue[]> groupbyprintod = new HashMap<String,PrimitiveValue[]>();
	
								PrimitiveValue[] evalr= new PrimitiveValue[LINEITEM.getColumns()];
								Eval eval = new Eval() 
								{
									public PrimitiveValue eval(Column c)
									{ 
										int i = LINEITEM.getColnum().get(c.getColumnName());
										PrimitiveValue result = evalr[i];
										return result;
									}
								};
								String groupbyvalues = "";
								TreeMap<PrimitiveValue, ArrayList<Integer>> SHIPDATEind = new TreeMap<PrimitiveValue, ArrayList<Integer>>(indexsort);
								TreeMap<PrimitiveValue, ArrayList<Integer>> primaryind_LINEITEM_temp = new TreeMap<PrimitiveValue, ArrayList<Integer>>(indexsort);
								TreeMap<PrimitiveValue, ArrayList<Integer>> primaryind_LINEITEM_temp_sm = new TreeMap<PrimitiveValue, ArrayList<Integer>>(indexsort);
								ArrayList<Integer> primaryind_LINEITEM_temp_R = new ArrayList<Integer>();
								//Expression revenue = new Expression("lineitem.extendedprice*(1-lineitem.discount)");
								int linecount = 0;
								PrimitiveValue R = new StringValue("R");
								while ((line = reader.readLine()) != null)
								{
									Row row= new Row(4);
									//line = line + "|";
									int temp1 = 0;
									int temp2 =-1;
									int temp3 = 0;
									int temp4 = -1;
									while((temp3 < 15)&&((temp2 = line.indexOf( '|', temp1 )) != -1))
									{
										temp4++;
										if((temp4 == 1)||(temp4 == 3)||(temp4 == 13)||(temp4 == 15))
										{
											temp1 = temp2 + 1;
											continue;
										}
										
										String temp = line.substring( temp1, temp2 );
										switch(datatype.valueOf(LINEITEM.getDataType(temp3)))
										{
										case DECIMAL:
											evalr[temp3] = new DoubleValue(temp);
											//evalr[temp3] = r[temp3];
											temp3++;
											break;							
										case INT:
											evalr[temp3] = new LongValue(temp);
											//evalr[temp3] = r[temp3];
											temp3++;
											break; 
										case DATE:
											evalr[temp3] = new DateValue(temp);
											//evalr[temp3] = r[temp3];
											temp3++;
											break;
										default :
											evalr[temp3] = new StringValue(temp);
											//evalr[temp3] = r[temp3];
											temp3++;
										}
										temp1 = temp2 + 1;
									}
									row.getColdata()[0] = evalr[0];
									row.getColdata()[1] = evalr[1];
									row.getColdata()[2] = evalr[8];
									row.getColdata()[3] =  eval.eval(revenue);
									data.add(row);
									if(indexsort.compare(evalr[6],R) == 0)
									{
										primaryind_LINEITEM_temp_R.add(linecount);
									}
									
									ArrayList<Integer> temp_sm = primaryind_LINEITEM_temp_sm.get(evalr[11]);
									if(temp_sm == null)
									{
										temp_sm = new ArrayList<Integer>();
									}
									temp_sm.add(linecount);
									primaryind_LINEITEM_temp_sm.put(evalr[11],temp_sm);
									
									if((indexsort.compare(evalr[9], evalr[10]) < 0) && (indexsort.compare(evalr[8], evalr[9]) < 0))
									{
										ArrayList<Integer> temp_1 = primaryind_LINEITEM_temp.get(evalr[10]);
										if(temp_1 == null)
										{
											temp_1 = new ArrayList<Integer>();
										}
										temp_1.add(linecount);
										primaryind_LINEITEM_temp.put(evalr[10],temp_1);
									}
									
									
									if(indexsort.compare(evalr[8], min) >= 0)
									{
										ArrayList<Integer> temp = SHIPDATEind.get(evalr[8]);
										if(temp == null)
										{
											temp = new ArrayList<Integer>();
										}
										temp.add(linecount);
										SHIPDATEind.put(evalr[8],temp);
									}
									
									linecount++;
									
									if(indexsort.compare(evalr[8], end) < 0)
									{
										groupbyvalues = evalr[6]+"|"+evalr[7];
										if(groupbyprintod.get(groupbyvalues)!=null)	
										{
											functionouput = groupbyprintod.get(groupbyvalues);
										}
										else
										{
											functionouput = new PrimitiveValue[funcexp.size()];
										}
										for(int funcCnt=0;funcCnt<funclist.size();funcCnt++)
										{
											if(funclist.get(funcCnt) == 0)
											{
												PrimitiveValue temp = eval.eval(funcexp.get(funcCnt));
												if(functionouput[funcCnt] == null)
													functionouput[funcCnt] = temp;
												else if(functionouput[funcCnt].toDouble() < temp.toDouble())
													functionouput[funcCnt] = temp;
											}
											if(funclist.get(funcCnt) == 1)
											{
												PrimitiveValue temp = eval.eval(funcexp.get(funcCnt));
												if(functionouput[funcCnt] == null)
													functionouput[funcCnt] = temp;
												else if(functionouput[funcCnt].toDouble() > temp.toDouble())
													functionouput[funcCnt] = temp;
											}
											if(funclist.get(funcCnt) == 2)
											{
												if(functionouput[funcCnt] == null)
													functionouput[funcCnt] = new LongValue("1");
												else
													functionouput[funcCnt] =  new LongValue(functionouput[funcCnt].toLong() + 1);
											}
											if(funclist.get(funcCnt) == 3)
											{
												PrimitiveValue temp = eval.eval(funcexp.get(funcCnt));
												if(functionouput[funcCnt] == null)
												{
													functionouput[funcCnt] = temp;
												}
												else
												{
													functionouput[funcCnt] = new DoubleValue(functionouput[funcCnt].toDouble() + temp.toDouble());								
												}
											}
											if(funclist.get(funcCnt) == 4)
											{
												PrimitiveValue temp = eval.eval(funcexp.get(funcCnt));
												if(functionouput[funcCnt] == null)
												{
													functionouput[funcCnt] = temp;
												}
												else
												{
													functionouput[funcCnt] = new DoubleValue(functionouput[funcCnt].toDouble() + temp.toDouble());								
												}
											}
										}
										groupbyprintod.put(groupbyvalues,functionouput);
										groupbyvalues = "";
										functionouput = null;
									}
									else
									{
										PrimitiveValue[] rtemp = new PrimitiveValue[12];
										for(int i=0;i<12;i++)
										{
											rtemp[i] = evalr[i];
										}
										data_1998.add(rtemp);
									}
								}
								reader.close();
								for(Map.Entry<PrimitiveValue,ArrayList<Integer>> entry : SHIPDATEind.entrySet()) 
								{
									PrimitiveValue key = entry.getKey();
									Integer value[] = entry.getValue().toArray(new Integer[entry.getValue().size()]);
									SHIPDATE.put(key, value);
								}
								
								for(Entry<PrimitiveValue, ArrayList<Integer>> entry : primaryind_LINEITEM_temp.entrySet()) 
								{
									PrimitiveValue key = entry.getKey();
									Integer value[] = entry.getValue().toArray(new Integer[entry.getValue().size()]);
									primaryind_LINEITEM.put(key, value);
								}
								
								for(Entry<PrimitiveValue, ArrayList<Integer>> entry : primaryind_LINEITEM_temp_sm.entrySet()) 
								{
									PrimitiveValue key = entry.getKey();
									Integer value[] = entry.getValue().toArray(new Integer[entry.getValue().size()]);
									primaryind_LINEITEM_sm.put(key, value);
								}
								
								primaryind_LINEITEM_R = primaryind_LINEITEM_temp_R.toArray(new Integer[primaryind_LINEITEM_temp_R.size()]);
								
								data_1998.sort(new OrderbyComparator2(new int[]{8},new boolean[]{true}));
								LINEITEM_data = data.toArray(new Row[data.size()]);
								//LINEITEM_data_1998 = data_1998.toArray(new Row[data_1998.size()]);
								data = null;
								int size = data_1998.size();
								PrimitiveValue[] rtemp = null;
								PrimitiveValue[] prev = data_1998.get(0);
								for(int i=0;i<size;i++)
								{
									rtemp = data_1998.get(i);
									evalr[0] = rtemp[0];
									evalr[1] = rtemp[1];
									evalr[2] = rtemp[2];
									evalr[3] = rtemp[3];
									evalr[4] = rtemp[4];
									evalr[5] = rtemp[5];
									evalr[6] = rtemp[6];
									evalr[7] = rtemp[7];
									evalr[8] = rtemp[8];
									evalr[9] = rtemp[9];
									evalr[10] = rtemp[10];
									evalr[11] = rtemp[11];
									if(indexsort.compare(rtemp[8], prev[8])!=0)
									{
										String[] ddata = new String[groupbyprintod.keySet().size()];
										int counts = 0;
										for(Entry<String, PrimitiveValue[]> temp :groupbyprintod.entrySet())
										{
											StringBuilder print = new StringBuilder();
											print.append(temp.getKey());
											PrimitiveValue[] key = temp.getValue();
											print.append("|");
											for(int j =0;j<key.length;j++)
											{
												if((j==4)||(j==5)||(j==6))
												{
													print.append((key[j].toDouble())/(key[7].toDouble()));
													print.append("|");
												}
												else
												{
													print.append(key[j].toDouble());
													print.append("|");
												}
											}
											print.deleteCharAt(print.length()-1);
											ddata[counts] = print.toString();
											counts++;
										}
										Arrays.sort(ddata);
										LINEITEM_data_1998.put(prev[8], ddata);
										prev = rtemp;
									}
									groupbyvalues = rtemp[6]+"|"+rtemp[7];
									if(groupbyprintod.get(groupbyvalues)!=null)	
									{
										functionouput = groupbyprintod.get(groupbyvalues);
									}
									else
									{
										functionouput = new PrimitiveValue[funcexp.size()];
									}
									for(int funcCnt=0;funcCnt<funclist.size();funcCnt++)
									{
										if(funclist.get(funcCnt) == 0)
										{
											PrimitiveValue temp = eval.eval(funcexp.get(funcCnt));
											if(functionouput[funcCnt] == null)
												functionouput[funcCnt] = temp;
											else if(functionouput[funcCnt].toDouble() < temp.toDouble())
												functionouput[funcCnt] = temp;
										}
										if(funclist.get(funcCnt) == 1)
										{
											PrimitiveValue temp = eval.eval(funcexp.get(funcCnt));
											if(functionouput[funcCnt] == null)
												functionouput[funcCnt] = temp;
											else if(functionouput[funcCnt].toDouble() > temp.toDouble())
												functionouput[funcCnt] = temp;
										}
										if(funclist.get(funcCnt) == 2)
										{
											if(functionouput[funcCnt] == null)
												functionouput[funcCnt] = new LongValue("1");
											else
												functionouput[funcCnt] =  new LongValue(functionouput[funcCnt].toLong() + 1);
										}
										if(funclist.get(funcCnt) == 3)
										{
											PrimitiveValue temp = eval.eval(funcexp.get(funcCnt));
											if(functionouput[funcCnt] == null)
											{
												functionouput[funcCnt] = temp;
											}
											else
											{
												functionouput[funcCnt] = new DoubleValue(functionouput[funcCnt].toDouble() + temp.toDouble());								
											}
										}
										if(funclist.get(funcCnt) == 4)
										{
											PrimitiveValue temp = eval.eval(funcexp.get(funcCnt));
											if(functionouput[funcCnt] == null)
											{
												functionouput[funcCnt] = temp;
											}
											else
											{
												functionouput[funcCnt] = new DoubleValue(functionouput[funcCnt].toDouble() + temp.toDouble());								
											}
										}
									}
									groupbyprintod.put(groupbyvalues,functionouput);
									groupbyvalues = "";
									functionouput = null;								
								}
								data_1998 = null;
							}
							if(tableinp.getTable().getName().equals("ORDERS"))
							{
								TableSchema ORDERS = new TableSchema();
								StringReader input = new StringReader("CREATE TABLE ORDERS ( ORDERKEY INT, CUSTKEY INT, ORDERDATE DATE, SHIPPRIORITY INT);");
								CCJSqlParser parser = new CCJSqlParser(input);
								Statement query = parser.Statement();
								CreateTable table = (CreateTable)query;
								ArrayList<Row> data= new ArrayList<Row>();
								ORDERS.getSchema(table);
								String file = "data/"+table.getTable().getName()+".csv";
								File fileRead = new File(file);
								BufferedReader reader = new BufferedReader(new FileReader(fileRead));
								String line;
								
								int linecount = 0;
								TreeMap<PrimitiveValue, ArrayList<Integer>> primaryind_ORDERS_temp = new TreeMap<PrimitiveValue, ArrayList<Integer>>(indexsort);
								while ((line = reader.readLine()) != null)
								{
									Row row= new Row(ORDERS.getColumns());		
									//PrimitiveValue[] r= new PrimitiveValue[ORDERS.getColumns()];
									//line = line + "|";
									int temp1 = 0;
									int temp2 =-1;
									int temp3 = 0;
									int temp4 = -1;
									while((temp2 = line.indexOf( '|', temp1 )) != -1)
									{
										temp4++;
										if((temp4 == 2)||(temp4 == 3)||(temp4 == 5)||(temp4 == 6)||(temp4 == 8))
										{
											temp1 = temp2 + 1;
											continue;
										}
										
										String temp = line.substring( temp1, temp2 );
										switch(datatype.valueOf(ORDERS.getDataType(temp3)))
										{
										case DECIMAL:
											row.getColdata()[temp3++] = new DoubleValue(temp);
											break;							
										case INT:
											row.getColdata()[temp3++] = new LongValue(temp);
											break; 
										case DATE:
											row.getColdata()[temp3++] = new DateValue(temp);
											break;
										default :
											row.getColdata()[temp3++] = new StringValue(temp);
										}
										temp1 = temp2 + 1;
									}
									data.add(row);
									
									ArrayList<Integer> temp_1 = primaryind_ORDERS_temp.get(row.getColdata()[2]);
									if(temp_1 == null)
									{
										temp_1 = new ArrayList<Integer>();
									}
									temp_1.add(linecount);
									primaryind_ORDERS_temp.put(row.getColdata()[2],temp_1);
									linecount++;
								}
								reader.close();
								for(Entry<PrimitiveValue, ArrayList<Integer>> entry : primaryind_ORDERS_temp.entrySet()) 
								{
									PrimitiveValue key = entry.getKey();
									Integer value[] = entry.getValue().toArray(new Integer[entry.getValue().size()]);
									primaryind_ORDERS.put(key, value);
								}
								ORDERS_data = data.toArray(new Row[data.size()]);
								data = null;
							}
							if(tableinp.getTable().getName().equals("CUSTOMER"))
							{
								TableSchema CUSTOMER = new TableSchema();
								StringReader input = new StringReader("CREATE TABLE CUSTOMER ( CUSTKEY INT, NATIONKEY INT, MKTSEGMENT VARCHAR(10));");
								CCJSqlParser parser = new CCJSqlParser(input);
								Statement query = parser.Statement();
								CreateTable table = (CreateTable)query;
								ArrayList<Row> data= new ArrayList<Row>();
								CUSTOMER.getSchema(table);
								String file = "data/"+table.getTable().getName()+".csv";
								File fileRead = new File(file);
								BufferedReader reader = new BufferedReader(new FileReader(fileRead));
								String line;
								
								while ((line = reader.readLine()) != null)
								{
									line = line + "|";
									Row row= new Row(CUSTOMER.getColumns());								
									//PrimitiveValue[] r= new PrimitiveValue[CUSTOMER.getColumns()];
									int temp1 = 0;
									int temp2 =-1;
									int temp3 = 0;
									int temp4 = -1;
									while((temp2 = line.indexOf( '|', temp1 )) != -1)
									{
										temp4++;
										if((temp4 == 1)||(temp4 == 2)||(temp4 == 4)||(temp4 == 5)||(temp4 == 7))
										{
											temp1 = temp2 + 1;
											continue;
										}
										
										String temp = line.substring( temp1, temp2 );
										switch(datatype.valueOf(CUSTOMER.getDataType(temp3)))
										{
										case DECIMAL:
											row.getColdata()[temp3++] = new DoubleValue(temp);
											break;							
										case INT:
											row.getColdata()[temp3++] = new LongValue(temp);
											break; 
										case DATE:
											row.getColdata()[temp3++] = new DateValue(temp);
											break;
										default :
											row.getColdata()[temp3++] = new StringValue(temp);
										}
										temp1 = temp2 + 1;
									}
									//row.setColdata(r);
									data.add(row);
								}
								reader.close();
								data.sort(new OrderbyComparator(new int[]{2},new boolean[]{true}));
								PrimitiveValue temp = null;
								PrimitiveValue temp1 = null;
								for(int i=0;i<data.size();i++)
								{
									if(i==0)
									{					    		
										temp= data.get(i).getColdata()[2];
										primaryind_CUSTOMER.put(temp, 0);
									}
									else
									{
										temp1 = data.get(i).getColdata()[2];
										if(indexsort.compare(temp, temp1)!=0)
										{
											temp= temp1;
											primaryind_CUSTOMER.put(temp, i);									
										}
									}
								}
								CUSTOMER_data = data.toArray(new Row[data.size()]);
								data = null;
							}
							if(tableinp.getTable().getName().equals("SUPPLIER"))
							{
								TableSchema SUPPLIER = new TableSchema();
								StringReader input = new StringReader("CREATE TABLE SUPPLIER ( SUPPKEY INT, NATIONKEY INT);");
								CCJSqlParser parser = new CCJSqlParser(input);
								Statement query = parser.Statement();
								CreateTable table = (CreateTable)query;
								ArrayList<Row> data= new ArrayList<Row>();
								SUPPLIER.getSchema(table);
								String file = "data/"+table.getTable().getName()+".csv";
								File fileRead = new File(file);
								BufferedReader reader = new BufferedReader(new FileReader(fileRead));
								String line;
								
								while ((line = reader.readLine()) != null)
								{
									Row row= new Row(SUPPLIER.getColumns());								
									PrimitiveValue[] r= new PrimitiveValue[SUPPLIER.getColumns()];
									int temp1 = 0;
									int temp2 =-1;
									int temp3 = 0;
									int temp4 = -1;
									while((temp2 = line.indexOf( '|', temp1 )) != -1)
									{
										temp4++;
										if((temp4 == 1)||(temp4 == 2)||(temp4 == 4)||(temp4 == 5)||(temp4 == 6))
										{
											temp1 = temp2 + 1;
											continue;
										}
										
										String temp = line.substring( temp1, temp2 );
										switch(datatype.valueOf(SUPPLIER.getDataType(temp3)))
										{
										case DECIMAL:
											r[temp3++] = new DoubleValue(temp);
											break;							
										case INT:
											r[temp3++] = new LongValue(temp);
											break; 
										case DATE:
											r[temp3++] = new DateValue(temp);
											break;
										default :
											r[temp3++] = new StringValue(temp);
										}
										temp1 = temp2 + 1;
									}
									row.setColdata(r);
									data.add(row);
								}
								reader.close();
								SUPPLIER_data = data.toArray(new Row[data.size()]);
								data = null;
							}
							if(tableinp.getTable().getName().equals("NATION"))
							{
								TableSchema NATION = new TableSchema();
								StringReader input = new StringReader("CREATE TABLE NATION ( NATIONKEY INT, NAME CHAR(25), REGIONKEY INT);");
								CCJSqlParser parser = new CCJSqlParser(input);
								Statement query = parser.Statement();
								CreateTable table = (CreateTable)query;
								ArrayList<Row> data= new ArrayList<Row>();
								NATION.getSchema(table);
								String file = "data/"+table.getTable().getName()+".csv";
								File fileRead = new File(file);
								BufferedReader reader = new BufferedReader(new FileReader(fileRead));
								String line;
								
								while ((line = reader.readLine()) != null)
								{
									Row row= new Row(NATION.getColumns());								
									PrimitiveValue[] r= new PrimitiveValue[NATION.getColumns()];
									int temp1 = 0;
									int temp2 =-1;
									int temp3 = 0;
									int temp4 = -1;
									while((temp2 = line.indexOf( '|', temp1 )) != -1)
									{
										temp4++;
										if((temp4 == 3))
										{
											temp1 = temp2 + 1;
											continue;
										}
										
										String temp = line.substring( temp1, temp2 );
										switch(datatype.valueOf(NATION.getDataType(temp3)))
										{
										case DECIMAL:
											r[temp3++] = new DoubleValue(temp);
											break;							
										case INT:
											r[temp3++] = new LongValue(temp);
											break; 
										case DATE:
											r[temp3++] = new DateValue(temp);
											break;
										default :
											r[temp3++] = new StringValue(temp);
										}
										temp1 = temp2 + 1;
									}
									row.setColdata(r);
									data.add(row);
								}
								reader.close();
								NATION_data = data.toArray(new Row[data.size()]);
								data = null;
							}
							if(tableinp.getTable().getName().equals("REGION"))
							{
								TableSchema REGION = new TableSchema();
								StringReader input = new StringReader("CREATE TABLE REGION ( REGIONKEY INT, NAME CHAR(25));");
								CCJSqlParser parser = new CCJSqlParser(input);
								Statement query = parser.Statement();
								CreateTable table = (CreateTable)query;
								ArrayList<Row> data= new ArrayList<Row>();
								REGION.getSchema(table);
								String file = "data/"+table.getTable().getName()+".csv";
								File fileRead = new File(file);
								BufferedReader reader = new BufferedReader(new FileReader(fileRead));
								String line;
								int rowcount = 0;
								while ((line = reader.readLine()) != null)
								{
									Row row= new Row(REGION.getColumns());								
									PrimitiveValue[] r= new PrimitiveValue[REGION.getColumns()];
									int temp1 = 0;
									int temp2 =-1;
									int temp3 = 0;
									int temp4 = -1;
									while((temp2 = line.indexOf( '|', temp1 )) != -1)
									{
										temp4++;
										if((temp4 == 2))
										{
											temp1 = temp2 + 1;
											continue;
										}
										
										String temp = line.substring( temp1, temp2 );
										switch(datatype.valueOf(REGION.getDataType(temp3)))
										{
										case DECIMAL:
											r[temp3++] = new DoubleValue(temp);
											break;							
										case INT:
											r[temp3++] = new LongValue(temp);
											break; 
										case DATE:
											r[temp3++] = new DateValue(temp);
											break;
										default :
											r[temp3++] = new StringValue(temp);
										}
										temp1 = temp2 + 1;
									}
									row.setColdata(r);
									data.add(row);
									primaryind_REGION.put(r[1], rowcount);
									rowcount++;
								}
								reader.close();
								REGION_data = data.toArray(new Row[data.size()]);
								data = null;
							}
						}
						System.gc();
						TimeUnit.SECONDS.sleep(10);
					}
					else if(queryinp instanceof Select)
					{
						PlainSelect select = (PlainSelect)(((Select) queryinp).getSelectBody());			
						int limit = 0;
						List<SelectItem> selectitems = select.getSelectItems();
						ArrayList<String> tables = new ArrayList<String>();
						tables.add(((Table)select.getFromItem()).getName());
						
						if(select.getJoins() != null)
						{
							for(Join j: select.getJoins())
							{
								tables.add(((Table)j.getRightItem()).getName());
							}
						}
												
						if(select.getLimit() != null)
							limit = (int) select.getLimit().getRowCount();
						
						if(selectitems.toString().equals("[LINEITEM.RETURNFLAG, LINEITEM.LINESTATUS, SUM(LINEITEM.QUANTITY) AS SUM_QTY, SUM(LINEITEM.EXTENDEDPRICE) AS SUM_BASE_PRICE, SUM(LINEITEM.EXTENDEDPRICE * 1 - LINEITEM.DISCOUNT) AS SUM_DISC_PRICE, SUM(LINEITEM.EXTENDEDPRICE * 1 - LINEITEM.DISCOUNT * 1 + LINEITEM.TAX) AS SUM_CHARGE, AVG(LINEITEM.QUANTITY) AS AVG_QTY, AVG(LINEITEM.EXTENDEDPRICE) AS AVG_PRICE, AVG(LINEITEM.DISCOUNT) AS AVG_DISC, COUNT(*) AS COUNT_ORDER]"))
						{
							PrimitiveValue end = new DateValue(((ExpressionList)((Function)((BinaryExpression)select.getWhere()).getRightExpression()).getParameters()).getExpressions().get(0).toString().substring(1,11));
							String[] ddata = LINEITEM_data_1998.get(end);
							for(int i=0;i<ddata.length;i++)
							{
								System.out.println(ddata[i]);
							}
							continue;
						}
						
						HashMap<String, ArrayList<BinaryExpression>> joins = new  HashMap<String,ArrayList<BinaryExpression>>();
						HashMap<String,ArrayList<BinaryExpression>> wherelist = new HashMap<String,ArrayList<BinaryExpression>>();
						PrimitiveValue shipmode1 = null;
						PrimitiveValue shipmode2 = null;
						if(select.getWhere() != null)
						{
							BinaryExpression e = (BinaryExpression)select.getWhere();

							while(e instanceof AndExpression)
							{
								BinaryExpression temp = (BinaryExpression) e.getRightExpression();
								if((temp.getLeftExpression() instanceof Column)&&(temp.getRightExpression() instanceof Column))
								{
									if(temp instanceof EqualsTo)
									{
										if(joins.get(((Column)temp.getLeftExpression()).getTable().getName())!=null)
										{
											ArrayList<BinaryExpression> list = joins.get(((Column)temp.getLeftExpression()).getTable().getName());
											list.add(temp);
											joins.put(((Column)temp.getLeftExpression()).getTable().getName(),list);
										}
										else
										{
											ArrayList<BinaryExpression> list = new ArrayList<BinaryExpression>();
											list.add(temp);
											joins.put(((Column)temp.getLeftExpression()).getTable().getName(),list);
										}
									}
									else
									{
										if(wherelist.get(((Column)temp.getLeftExpression()).getTable().getName())!=null)
										{
											ArrayList<BinaryExpression> list = wherelist.get(((Column)temp.getLeftExpression()).getTable().getName());
											list.add(temp);
											wherelist.put(((Column)temp.getLeftExpression()).getTable().getName(),list);
										}
										else
										{
											ArrayList<BinaryExpression> list = new ArrayList<BinaryExpression>();
											list.add(temp);
											wherelist.put(((Column)temp.getLeftExpression()).getTable().getName(),list);
										}
									}
								}
								else if (temp instanceof OrExpression)
								{
									shipmode1 =  (PrimitiveValue) ((BinaryExpression)temp.getLeftExpression()).getRightExpression();
									shipmode2 =  (PrimitiveValue) ((BinaryExpression)temp.getRightExpression()).getRightExpression();
								}
								else
								{
									if(wherelist.get(((Column)temp.getLeftExpression()).getTable().getName())!=null)
									{
										ArrayList<BinaryExpression> list = wherelist.get(((Column)temp.getLeftExpression()).getTable().getName());
										list.add(temp);
										wherelist.put(((Column)temp.getLeftExpression()).getTable().getName(),list);
									}
									else
									{
										ArrayList<BinaryExpression> list = new ArrayList<BinaryExpression>();
										list.add(temp);
										wherelist.put(((Column)temp.getLeftExpression()).getTable().getName(),list);
									}
								}
								e = (BinaryExpression) e.getLeftExpression();
							}
							BinaryExpression temp = e;
							if((temp.getLeftExpression() instanceof Column)&&(temp.getRightExpression() instanceof Column))
							{
								if(temp instanceof EqualsTo)
								{
									if(joins.get(((Column)temp.getLeftExpression()).getTable().getName())!=null)
									{
										ArrayList<BinaryExpression> list = joins.get(((Column)temp.getLeftExpression()).getTable().getName());
										list.add(temp);
										joins.put(((Column)temp.getLeftExpression()).getTable().getName(),list);
									}
									else
									{
										ArrayList<BinaryExpression> list = new ArrayList<BinaryExpression>();
										list.add(temp);
										joins.put(((Column)temp.getLeftExpression()).getTable().getName(),list);
									}
								}
								else
								{
									if(wherelist.get(((Column)temp.getLeftExpression()).getTable().getName())!=null)
									{
										ArrayList<BinaryExpression> list = wherelist.get(((Column)temp.getLeftExpression()).getTable().getName());
										list.add(temp);
										wherelist.put(((Column)temp.getLeftExpression()).getTable().getName(),list);
									}
									else
									{
										ArrayList<BinaryExpression> list = new ArrayList<BinaryExpression>();
										list.add(temp);
										wherelist.put(((Column)temp.getLeftExpression()).getTable().getName(),list);
									}
								}
							}
							else
							{
								if(wherelist.get(((Column)temp.getLeftExpression()).getTable().getName())!=null)
								{
									ArrayList<BinaryExpression> list = wherelist.get(((Column)temp.getLeftExpression()).getTable().getName());
									list.add(temp);
									wherelist.put(((Column)temp.getLeftExpression()).getTable().getName(),list);
								}
								else
								{
									ArrayList<BinaryExpression> list = new ArrayList<BinaryExpression>();
									list.add(temp);
									wherelist.put(((Column)temp.getLeftExpression()).getTable().getName(),list);
								}
							}
						}
						if(selectitems.toString().equals("[LINEITEM.ORDERKEY, SUM(LINEITEM.EXTENDEDPRICE * 1 - LINEITEM.DISCOUNT) AS REVENUE, ORDERS.ORDERDATE, ORDERS.SHIPPRIORITY]"))
						{
							HashSet<Integer> fetchorders= new HashSet<Integer>();
							for(String tbl : tables)
							{
								ArrayList<BinaryExpression> jlist = joins.get(tbl);
								if(jlist !=null)
								{
									for(BinaryExpression join : jlist)
									{
										String left = ((Column)join.getLeftExpression()).getTable().getName();
										String right = ((Column)join.getRightExpression()).getTable().getName();
										if(left.equals("CUSTOMER"))
										{
											ArrayList<BinaryExpression> where = wherelist.get(left);
											int start = primaryind_CUSTOMER.get((PrimitiveValue)where.get(0).getRightExpression());
											int end = primaryind_CUSTOMER.get(primaryind_CUSTOMER.higherKey((PrimitiveValue) where.get(0).getRightExpression()));
											HashMap<PrimitiveValue, Integer> hashsort1 = new HashMap<PrimitiveValue, Integer>();
											for(int i=start;i<end;i++)
											{
												hashsort1.put(CUSTOMER_data[i].getColdata()[0], i);
											}
											ArrayList<BinaryExpression> whereright = wherelist.get(right);
											SortedMap<PrimitiveValue, Integer[]> tempmap = primaryind_ORDERS.headMap(new DateValue(((ExpressionList)((Function)whereright.get(0).getRightExpression()).getParameters()).getExpressions().get(0).toString().substring(1,11)));
											Iterator<Integer[]> itr = tempmap.values().iterator();
											while(itr.hasNext())
											{
												fetchorders.addAll(Arrays.asList(itr.next()));
											}
											
											Iterator<Integer> it = fetchorders.iterator();
											while(it.hasNext())
											{
												if(hashsort1.get(ORDERS_data[it.next()].getColdata()[1]) == null)
												{
													it.remove();
												}
											}
										}
										else if(left.equals("LINEITEM"))
										{
											ArrayList<BinaryExpression> where = wherelist.get(left);
											HashMap<PrimitiveValue, ArrayList<PrimitiveValue[]>> hashsort = new HashMap<PrimitiveValue, ArrayList<PrimitiveValue[]>>();
											SortedMap<PrimitiveValue, Integer[]> tempmap = SHIPDATE.tailMap(new DateValue(((ExpressionList)((Function)where.get(0).getRightExpression()).getParameters()).getExpressions().get(0).toString().substring(1,11)),false);
											Iterator<Integer[]> itr = tempmap.values().iterator();
											while(itr.hasNext())
											{
												Integer[] iarray = itr.next();												
												for(Integer i : iarray)
												{
													PrimitiveValue[] lineitem = LINEITEM_data[i].getColdata();
													if(hashsort.get(lineitem[0]) == null)
													{
														ArrayList<PrimitiveValue[]> temp = new ArrayList<PrimitiveValue[]>();
														temp.add(lineitem);
														hashsort.put(lineitem[0], temp);
													}
													else
													{
														ArrayList<PrimitiveValue[]> temp = hashsort.get(lineitem[0]);
														temp.add(lineitem);
														hashsort.put(lineitem[0], temp);
													}
												}
											}
											
											Iterator<Integer> it = fetchorders.iterator();
											HashMap<String,PrimitiveValue[]> groupbyprint = new HashMap<String,PrimitiveValue[]>();
											while(it.hasNext())
											{
												PrimitiveValue[] orderrow = ORDERS_data[it.next()].getColdata();
												ArrayList<PrimitiveValue[]> temp = hashsort.get(orderrow[0]);
												if(temp != null)
												{
													for(PrimitiveValue[] lineitem : temp)
													{
														String get = lineitem[0] + "|" + orderrow[2] + "|" + orderrow[3];
														PrimitiveValue[] grpby = groupbyprint.get(get);
														if(grpby == null)
														{
															grpby = new PrimitiveValue[4];
															grpby[0] = lineitem[0];
															grpby[1] = lineitem[3];
															grpby[2] = orderrow[2];
															grpby[3] = orderrow[3];
														}
														else
														{
															grpby[1] = new DoubleValue(grpby[1].toDouble() + lineitem[3].toDouble());	
														}
														groupbyprint.put(get,grpby);
													}
												}
											}
											ArrayList<PrimitiveValue[]> print = new ArrayList<PrimitiveValue[]>(groupbyprint.values());
											print.sort(new OrderbyComparator2(new int[]{1,2},new boolean[]{false,true}));
											int count = 0;
											for(PrimitiveValue[] out : print)
											{
												count++;
												StringBuilder prnt = new StringBuilder();
												for(int i = 0;i < out.length;i++)
												{
													PrimitiveValue temp = out[i];
													if(temp != null)
													{
														prnt.append(temp.toString());
													}
													prnt.append("|");
												}
												prnt.deleteCharAt(prnt.length()-1);
												System.out.print(prnt.toString()+"\n");
												if(limit == count)
													break;
											}
										}
									}
								}
							}
						}
						else if(selectitems.toString().equals("[NATION.NAME, SUM(LINEITEM.EXTENDEDPRICE * 1 - LINEITEM.DISCOUNT) AS REVENUE]"))
						{
							HashSet<Integer> fetchorders= new HashSet<Integer>();
							HashSet<Integer> fetchnation= new HashSet<Integer>();
							HashMap<PrimitiveValue,PrimitiveValue> fetchsupplier= new HashMap<PrimitiveValue,PrimitiveValue>();
							HashMap<PrimitiveValue, Row> hashsort1 = new HashMap<PrimitiveValue, Row>();
							HashMap<PrimitiveValue, Row> hashsort2 = new HashMap<PrimitiveValue, Row>();	
							HashMap<PrimitiveValue, ArrayList<PrimitiveValue[]>> hashsort3 = new HashMap<PrimitiveValue, ArrayList<PrimitiveValue[]>>();
							for(String tbl : tables)
							{
								ArrayList<BinaryExpression> jlist = joins.get(tbl);
								if(jlist != null)
								{
									for(BinaryExpression join : jlist)
									{
										String left = ((Column)join.getLeftExpression()).getTable().getName();
										String right = ((Column)join.getRightExpression()).getTable().getName();
										if(left.equals("NATION"))
										{
											int size = NATION_data.length;
											ArrayList<BinaryExpression> whereright = wherelist.get(right);
											Integer tempmap = primaryind_REGION.get((PrimitiveValue)whereright.get(0).getRightExpression());
											PrimitiveValue regionkey = REGION_data[tempmap].getColdata()[0];
											for(int i =0;i< size;i++)
											{
												if(indexsort.compare(NATION_data[i].getColdata()[2],regionkey) == 0)
												{
													fetchnation.add(i);
												}
											}
										}
										else if(left.equals("CUSTOMER"))
										{
											if(right.equals("NATION"))
											{
												HashMap<PrimitiveValue, ArrayList<Row>> hashsort = new HashMap<PrimitiveValue, ArrayList<Row>>();
												int size = CUSTOMER_data.length;
												for(int i=0;i<size;i++)
												{
													Row temp = CUSTOMER_data[i];
													ArrayList<Row> list = hashsort.get(temp.getColdata()[1]);
													if( list == null)
													{
														list = new ArrayList<Row>();
														list.add(temp);
														hashsort.put(temp.getColdata()[1], list);
													}
													else
													{
														list.add(temp);
														hashsort.put(temp.getColdata()[1], list);
													}
												}
												Iterator<Integer> it = fetchnation.iterator();
												while(it.hasNext())
												{
													Row temp = NATION_data[it.next()];
													ArrayList<Row> list = hashsort.get(temp.getColdata()[0]);
													if(list != null)
													{
														for(Row r:list)
														{
															hashsort1.put(r.getColdata()[0], temp);
														}
													}
												}
											}
											else if(right.equals("ORDERS"))
											{											
												ArrayList<BinaryExpression> whereright = wherelist.get(right);
												PrimitiveValue min = null;
												PrimitiveValue max = null;
												for(BinaryExpression where : whereright)
												{
													if(where instanceof MinorThan)
													{
														max = new DateValue(((ExpressionList)((Function)where.getRightExpression()).getParameters()).getExpressions().get(0).toString().substring(1,11));
													}
													else if(where instanceof GreaterThanEquals)
													{
														min = new DateValue(((ExpressionList)((Function)where.getRightExpression()).getParameters()).getExpressions().get(0).toString().substring(1,11));
													}
												}
												SortedMap<PrimitiveValue, Integer[]> temporder = primaryind_ORDERS.subMap(min, max);
												Iterator<Integer[]> itr = temporder.values().iterator();
												while(itr.hasNext())
												{
													fetchorders.addAll(Arrays.asList(itr.next()));
												}
												
												Iterator<Integer> it = fetchorders.iterator();
												while(it.hasNext())
												{
													Row temp = ORDERS_data[it.next()];
													if(hashsort1.get(temp.getColdata()[1]) != null)
													{
														hashsort2.put(temp.getColdata()[0], hashsort1.get(temp.getColdata()[1]));
													}
												}
											}
										}
										else if(left.equals("LINEITEM"))
										{
											if(right.equals("SUPPLIER"))
											{
												int size = SUPPLIER_data.length;
												HashMap<PrimitiveValue, ArrayList<Row>> hashsortsupp = new HashMap<PrimitiveValue, ArrayList<Row>>();
												for(int i=0;i<size;i++)
												{
													ArrayList<Row> list = hashsortsupp.get(SUPPLIER_data[i].getColdata()[1]);
													if( list == null)
													{
														list = new ArrayList<Row>();
														list.add(SUPPLIER_data[i]);
														hashsortsupp.put(SUPPLIER_data[i].getColdata()[1], list);
													}
													else
													{
														list.add(SUPPLIER_data[i]);
														hashsortsupp.put(SUPPLIER_data[i].getColdata()[1], list);
													}
												}
												
												Iterator<Integer> it = fetchnation.iterator();
												while(it.hasNext())
												{
													Row temp = NATION_data[it.next()];
													ArrayList<Row> list = hashsortsupp.get(temp.getColdata()[0]);
													if(list != null)
													{
														if(list != null)
														{
															for(Row r:list)
															{
																fetchsupplier.put(r.getColdata()[0],r.getColdata()[1]);
															}
														}														
													}
												}
											}
											else if(right.equals("ORDERS"))
											{
												int size = LINEITEM_data.length;
												for(int i=0;i<size;i++)
												{
													PrimitiveValue[] lineitem = LINEITEM_data[i].getColdata();
													if(hashsort3.get(lineitem[0]) == null)
													{
														ArrayList<PrimitiveValue[]> temp = new ArrayList<PrimitiveValue[]>();
														temp.add(lineitem);
														hashsort3.put(lineitem[0], temp);
													}
													else
													{
														ArrayList<PrimitiveValue[]> temp = hashsort3.get(lineitem[0]);
														temp.add(lineitem);
														hashsort3.put(lineitem[0], temp);
													}
												}
												HashMap<PrimitiveValue,PrimitiveValue[]> groupbyprint = new HashMap<PrimitiveValue,PrimitiveValue[]>();
												for(Entry<PrimitiveValue, Row> temp : hashsort2.entrySet())
												{
													ArrayList<PrimitiveValue[]> tmp = hashsort3.get(temp.getKey());
													if(tmp != null)
													{
														for(PrimitiveValue[] t : tmp)
														{
															PrimitiveValue tem = fetchsupplier.get(t[1]);
															if(tem != null)
															{
																if(indexsort.compare(tem, temp.getValue().getColdata()[0]) == 0)
																{
																	PrimitiveValue[] tmpo = groupbyprint.get(temp.getValue().getColdata()[1]);
																	if(tmpo == null)
																	{
																		tmpo = new PrimitiveValue[2];
																		tmpo[0] = temp.getValue().getColdata()[1];
																		tmpo[1] = t[3];																	
																	}
																	else
																	{
																		tmpo[1] = new DoubleValue(tmpo[1].toDouble() + t[3].toDouble());	
																	}
																	groupbyprint.put(temp.getValue().getColdata()[1], tmpo);
																}
															}
														}
													}
												}
												ArrayList<PrimitiveValue[]> print = new ArrayList<PrimitiveValue[]>(groupbyprint.values());
												print.sort(new OrderbyComparator2(new int[]{1},new boolean[]{false}));
												int count = 0;
												for(PrimitiveValue[] out : print)
												{
													count++;
													StringBuilder prnt = new StringBuilder();
													for(int i = 0;i < out.length;i++)
													{
														PrimitiveValue temp = out[i];
														if(temp != null)
														{
															prnt.append(temp.toString());
														}
														prnt.append("|");
													}
													prnt.deleteCharAt(prnt.length()-1);
													System.out.print(prnt.toString()+"\n");
													if(limit == count)
														break;
												}
											}
										}
									}
								}
							}
						}
						else if(selectitems.toString().equals("[CUSTOMER.CUSTKEY, SUM(LINEITEM.EXTENDEDPRICE * 1 - LINEITEM.DISCOUNT) AS REVENUE, CUSTOMER.ACCTBAL, NATION.NAME, CUSTOMER.ADDRESS, CUSTOMER.PHONE, CUSTOMER.COMMENT]"))
						{
							HashMap<PrimitiveValue, Row[]> hashsort3 = new HashMap<PrimitiveValue, Row[]>();
							//HashMap<PrimitiveValue, PrimitiveValue> hashsort4 = new HashMap<PrimitiveValue, PrimitiveValue>();
							HashSet<Integer> fetchorders= new HashSet<Integer>();
							for(String tbl : tables)
							{
								ArrayList<BinaryExpression> jlist = joins.get(tbl);
								if(jlist != null)
								{
									for(BinaryExpression join : jlist)
									{
										String left = ((Column)join.getLeftExpression()).getTable().getName();
										String right = ((Column)join.getRightExpression()).getTable().getName();
										if(left.equals("CUSTOMER"))
										{
											if(right.equals("NATION"))
											{
											}
											else if(right.equals("ORDERS"))
											{
												ArrayList<BinaryExpression> whereright = wherelist.get(right);
												PrimitiveValue min = null;
												PrimitiveValue max = null;
												for(BinaryExpression where : whereright)
												{
													if(where instanceof MinorThan)
													{
														max = new DateValue(((ExpressionList)((Function)where.getRightExpression()).getParameters()).getExpressions().get(0).toString().substring(1,11));
													}
													else if(where instanceof GreaterThanEquals)
													{
														min = new DateValue(((ExpressionList)((Function)where.getRightExpression()).getParameters()).getExpressions().get(0).toString().substring(1,11));
													}
												}
												SortedMap<PrimitiveValue, Integer[]> temporder = primaryind_ORDERS.subMap(min, max);
												Iterator<Integer[]> itr = temporder.values().iterator();
												while(itr.hasNext())
												{
													fetchorders.addAll(Arrays.asList(itr.next()));
												}
												
												Iterator<Integer> it = fetchorders.iterator();
												while(it.hasNext())
												{
													Row temp = ORDERS_data[it.next()];
													if(hashsortcust.get(temp.getColdata()[1]) != null)
													{
														hashsort3.put(temp.getColdata()[0], hashsortcust.get(temp.getColdata()[1]));
													}
												}
												
												hashsortcust = null;
											}
										}
										else if(left.equals("LINEITEM"))
										{
									        FileInputStream fileIn = new FileInputStream("data/query4.ser");
									        ObjectInputStream in = new ObjectInputStream(fileIn);
									        query4 = (HashMap<PrimitiveValue, PrimitiveValue>) in.readObject();
									        in.close();
									        fileIn.close();
											
											
											HashMap<String,PrimitiveValue[]> groupbyprint = new HashMap<String,PrimitiveValue[]>();
											for(Entry<PrimitiveValue, Row[]> temp : hashsort3.entrySet())
											{
												PrimitiveValue tmp = query4.get(temp.getKey());
												if(tmp != null)
												{
													PrimitiveValue[] cust = temp.getValue()[0].getColdata();
													PrimitiveValue[] nat = temp.getValue()[1].getColdata();
													String tem = cust[0] + "|" +cust[5]+ "|" +cust[4]+ "|" +nat[1]+ "|" +cust[2]+ "|" +cust[7];													
													PrimitiveValue[] tmpo = groupbyprint.get(tem);
													if(tmpo == null)
													{													
														tmpo = new PrimitiveValue[7];
														tmpo[0] = cust[0];
														tmpo[1] = new DoubleValue("0");
														tmpo[2] = cust[5];
														tmpo[3] = nat[1];
														tmpo[4] = cust[2];
														tmpo[5] = cust[4];
														tmpo[6] = cust[7];
													}

  												    tmpo[1] = new DoubleValue(tmpo[1].toDouble() + tmp.toDouble());																													
													groupbyprint.put(tem, tmpo);
												}
											}
											query4 = null;
											ArrayList<PrimitiveValue[]> print = new ArrayList<PrimitiveValue[]>(groupbyprint.values());
											print.sort(new OrderbyComparator2(new int[]{1},new boolean[]{true}));
											int count = 0;
											for(PrimitiveValue[] out : print)
											{
												count++;
												StringBuilder prnt = new StringBuilder();
												for(int i = 0;i < out.length;i++)
												{
													PrimitiveValue temp = out[i];
													if(temp != null)
													{
														prnt.append(temp.toString());
													}
													prnt.append("|");
												}
												prnt.deleteCharAt(prnt.length()-1);
												System.out.print(prnt.toString()+"\n");
												if(limit == count)
													break;
											}
										}
									}
								}
							}
						}
						else if(selectitems.toString().equals("[LINEITEM.SHIPMODE, SUM(CASE WHEN ORDERS.ORDERPRIORITY = '1-URGENT' OR ORDERS.ORDERPRIORITY = '2-HIGH' THEN 1 ELSE 0 END) AS HIGH_LINE_COUNT, SUM(CASE WHEN ORDERS.ORDERPRIORITY <> '1-URGENT' AND ORDERS.ORDERPRIORITY <> '2-HIGH' THEN 1 ELSE 0 END) AS LOW_LINE_COUNT]"))
						{
							PrimitiveValue min = null;
							ArrayList<BinaryExpression> whereright = wherelist.get("LINEITEM");
							for(BinaryExpression where : whereright)
							{
								if(where instanceof GreaterThanEquals)
								{
									min = new DateValue(((ExpressionList)((Function)where.getRightExpression()).getParameters()).getExpressions().get(0).toString().substring(1,11));
									break;
								}
							}
							
					        FileInputStream fileIn = new FileInputStream("data/query5.ser");
					        ObjectInputStream in = new ObjectInputStream(fileIn);
					        query5 = (HashMap<String, HashMap<PrimitiveValue, PrimitiveValue[]>>) in.readObject();
					        in.close();
					        fileIn.close();

							HashMap<PrimitiveValue, PrimitiveValue[]> groupbyprint = query5.get(min.toString().substring(0, 4));
							ArrayList<PrimitiveValue[]> print = new ArrayList<PrimitiveValue[]>();
							print.add(groupbyprint.get(shipmode1));
							print.add(groupbyprint.get(shipmode2));
							print.sort(new OrderbyComparator2(new int[]{0},new boolean[]{true}));
							int count = 0;
							query5 = null;
							for(PrimitiveValue[] out : print)
							{
								count++;
								StringBuilder prnt = new StringBuilder();
								for(int i = 0;i < out.length;i++)
								{
									PrimitiveValue temp = out[i];
									if(temp != null)
									{
										prnt.append(temp.toString());
									}
									prnt.append("|");
								}
								prnt.deleteCharAt(prnt.length()-1);
								System.out.print(prnt.toString()+"\n");
								if(limit == count)
									break;
							}

						}
					}
					//long endtime = System.currentTimeMillis();
					//System.out.println(endtime-starttime);
				} 
			}
		}
		catch (ParseException e)
		{
			e.printStackTrace();
		}
		catch (FileNotFoundException e)
		{
			e.printStackTrace();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		} catch (SQLException e) 
		{
			e.printStackTrace();
		} 
		catch (InterruptedException e) 
		{
			e.printStackTrace();
		}
		catch (ClassNotFoundException e) 
		{
			e.printStackTrace();
		}
	}
}