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
import java.util.SortedMap;
import java.util.Stack;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import dubstep.QueryOutput.datatype;
import dubstep.QueryOutput.functions;
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
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;


public class Main {
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void main(String[] args)
	{
		boolean inmem = false;
		boolean ondisk = false;

		if(args[1].equals("--in-mem"))
			inmem = true;
		if(args[1].equals("--on-disk"))
			ondisk = true;

		try 
		{
			BufferedReader userinput = new BufferedReader(new InputStreamReader(System.in));
			//Map<String,TableSchema> tables=new HashMap<String,TableSchema>();
			Row[] filedata = null;
			Row[] idata = null;
			Row[] idatd = null;
			Row[] iddta = null;
			Row[] iddtd = null;
			Row[] idaca = null;
			Row[] idacd = null;
			Row[] iddca = null;
			Row[] iddcd = null;
			Row[] taida = null;
			Row[] taidd = null;
			Row[] taca = null;
			Row[] tacd = null;
			Row[] tdida = null;
			Row[] tdidd = null;
			Row[] tdca = null;
			Row[] tdcd = null;
			Row[] caida = null;
			Row[] caidd = null;
			Row[] cata = null;
			Row[] catd = null;
			Row[] cdida = null;
			Row[] cdidd = null;
			Row[] cdta = null;
			Row[] cdtd = null;
			PrimitiveValue[][] limit1992 = null;
			PrimitiveValue[][] limit1993 = null;
			PrimitiveValue[][] limit1994 = null;
			PrimitiveValue[][] limit1995 = null;
			PrimitiveValue[][] limit1996 = null;
			PrimitiveValue[][] limit1997 = null;
			PrimitiveValue[][] limit1998 = null;
			PrimitiveValue[][] limit1999 = null;
			HashMap<PrimitiveValue, ArrayList<PrimitiveValue[]>> discount1992 = null;
			HashMap<PrimitiveValue, ArrayList<PrimitiveValue[]>> discount1993 = null;
			HashMap<PrimitiveValue, ArrayList<PrimitiveValue[]>> discount1994 = null;
			HashMap<PrimitiveValue, ArrayList<PrimitiveValue[]>> discount1995 = null;
			HashMap<PrimitiveValue, ArrayList<PrimitiveValue[]>> discount1996 = null;
			HashMap<PrimitiveValue, ArrayList<PrimitiveValue[]>> discount1997 = null;
			HashMap<PrimitiveValue, ArrayList<PrimitiveValue[]>> discount1998 = null;
			HashMap<PrimitiveValue, ArrayList<PrimitiveValue[]>> discount1999 = null;
			TableSchema tblschema = null;
			String primaryindname = "";
			String secondaryindname = "";
			TreeMap<PrimitiveValue,Integer> primaryind = null;
			TreeMap<PrimitiveValue,String> primaryindod = null;
			HashMap<PrimitiveValue,Integer> comments = null;
			HashMap<String,TreeMap<PrimitiveValue,PrimitiveValue[]>> secondindex = new HashMap<String, TreeMap<PrimitiveValue, PrimitiveValue[]>>();
			Comparator<Row> tablesort  = new Comparator<Row>() 
			{
				public int compare(Row r1, Row r2) 
				{
					int ret = 0;
					PrimitiveValue col1 = r1.getColdata()[r1.getSortInd()];
					PrimitiveValue col2 = r2.getColdata()[r2.getSortInd()];
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
			Comparator<PrimitiveValue> indexsorts  = new Comparator<PrimitiveValue>() 
			{
				public int compare(PrimitiveValue r1, PrimitiveValue r2) 
				{
					int ret = 0;
					ret = r1.toString().substring(0, 4).compareTo(r2.toString().substring(0, 4));
					return ret;
				}
			};
			TreeMap<PrimitiveValue, PrimitiveValue[]> TIME = null;
			TreeMap<PrimitiveValue, PrimitiveValue[]> COST = null;
			HashMap<String, PrimitiveValue[]> groupbyprintod = null;
			int minifile = 118369;
			//long totaltime = 0;
			while(true)
			{
				System.out.print("$> ");
				/*String querystring = "";
				while (true) 
				{
					String temp = userinput.readLine();
					//if(temp.length() == 0)
						//break;
					
					querystring = querystring + " " + temp;

					if (temp.charAt(temp.length() - 1) == ';')
						break;
				}
				if(querystring.equalsIgnoreCase("done"))
					break;
				StringReader input = new StringReader(querystring);*/
				CCJSqlParser parser = new CCJSqlParser(System.in);
				Statement query = parser.Statement();
				Index primary = null;
				if(inmem)
				{
					if(query instanceof CreateTable)
					{
						//long starttime = System.currentTimeMillis();
						comments = new HashMap();
						tblschema = new TableSchema();
						CreateTable table = (CreateTable)query;
						tblschema.getSchema(table);
						TreeMap<PrimitiveValue,ArrayList<PrimitiveValue>> TIMEind = new TreeMap<PrimitiveValue, ArrayList<PrimitiveValue>>(indexsort);
						TreeMap<PrimitiveValue,ArrayList<PrimitiveValue>> COSTind = new TreeMap<PrimitiveValue, ArrayList<PrimitiveValue>>(indexsort);
						TIME = new TreeMap<PrimitiveValue, PrimitiveValue[]>(indexsort);
						COST = new TreeMap<PrimitiveValue, PrimitiveValue[]>(indexsort);
						//tables.put(table.getTable().getName(), tblschema);
						ArrayList<Row> data= new ArrayList<Row>();
						primaryind = new TreeMap<PrimitiveValue, Integer>(indexsort);
						String file = "data/"+table.getTable().getName()+".csv";
						File fileRead = new File(file);
						List<Index> indexes = table.getIndexes();
						for(Index i : indexes)
						{
							if(i.getType().equals("PRIMARY KEY"))
							{
								primary =i;
								primaryindname = i.getColumnsNames().get(0);
								break;
							}

						}

						//String[] cols = {"X","Y","TIME","COST"};

						int sortInd = tblschema.getColnum().get(primary.getColumnsNames().get(0));



						try {
							BufferedReader reader = new BufferedReader(new FileReader(fileRead));
							String line;
							while ((line = reader.readLine()) != null)
							{
								Row row= new Row(tblschema.getColumns());
								row.setSortInd(sortInd);
								PrimitiveValue[] r= new PrimitiveValue[tblschema.getColumns()];
								line = line + "|";
								int temp1 = 0;
								int temp2 =-1;
								int temp3 = 0;
								while((temp2 = line.indexOf( '|', temp1 )) != -1)
								{
									String temp = line.substring( temp1, temp2 );
									switch(datatype.valueOf(tblschema.getDataType(temp3)))
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
								for(int i =0; i<2;i++)
								{
									if(i == 0)
									{
										ArrayList<PrimitiveValue> temp = TIMEind.get(r[3]);
										if(temp == null)
										{
											temp = new ArrayList<PrimitiveValue>();
										}
										temp.add(r[sortInd]);
										TIMEind.put(r[3],temp);
									}
									else if(i == 1)
									{
										ArrayList<PrimitiveValue> temp = COSTind.get(r[4]);
										if(temp == null)
										{
											temp = new ArrayList<PrimitiveValue>();
										}
										temp.add(r[sortInd]);
										COSTind.put(r[4],temp);
									}
								}
							}
							reader.close();
						} catch (IOException e) {
							e.printStackTrace();
						}

						for(Map.Entry<PrimitiveValue,ArrayList<PrimitiveValue>> entry : COSTind.entrySet()) 
						{
							PrimitiveValue key = entry.getKey();
							PrimitiveValue value[] = entry.getValue().toArray(new PrimitiveValue[entry.getValue().size()]);
							COST.put(key, value);
						}

						for(Map.Entry<PrimitiveValue,ArrayList<PrimitiveValue>> entry : TIMEind.entrySet()) 
						{
							PrimitiveValue key = entry.getKey();
							PrimitiveValue value[] = entry.getValue().toArray(new PrimitiveValue[entry.getValue().size()]);
							TIME.put(key, value);
						}


						COSTind.clear();
						COSTind = null;
						TIMEind.clear();
						TIMEind = null;

						data.sort(new OrderbyComparator(new int[]{0,3},new boolean[]{false,true}));
						iddta = data.subList(0, 30).toArray(new Row[30]);
						data.sort(new OrderbyComparator(new int[]{0,3},new boolean[]{false,false}));
						iddtd = data.subList(0, 30).toArray(new Row[30]);
						data.sort(new OrderbyComparator(new int[]{0,4},new boolean[]{false,true}));
						iddca = data.subList(0, 30).toArray(new Row[30]);
						data.sort(new OrderbyComparator(new int[]{0,4},new boolean[]{false,false}));
						iddcd = data.subList(0, 30).toArray(new Row[30]);
						data.sort(new OrderbyComparator(new int[]{3,0},new boolean[]{true,true}));
						taida = data.subList(0, 30).toArray(new Row[30]);
						data.sort(new OrderbyComparator(new int[]{3,0},new boolean[]{true,false}));
						taidd = data.subList(0, 30).toArray(new Row[30]);
						data.sort(new OrderbyComparator(new int[]{3,4},new boolean[]{true,true}));
						taca = data.subList(0, 30).toArray(new Row[30]);
						data.sort(new OrderbyComparator(new int[]{3,4},new boolean[]{true,false}));
						tacd = data.subList(0, 30).toArray(new Row[30]);
						data.sort(new OrderbyComparator(new int[]{3,0},new boolean[]{false,true}));
						tdida = data.subList(0, 30).toArray(new Row[30]);
						data.sort(new OrderbyComparator(new int[]{3,0},new boolean[]{false,false}));
						tdidd = data.subList(0, 30).toArray(new Row[30]);
						data.sort(new OrderbyComparator(new int[]{3,4},new boolean[]{false,true}));
						tdca = data.subList(0, 30).toArray(new Row[30]);
						data.sort(new OrderbyComparator(new int[]{3,4},new boolean[]{false,false}));
						tdcd = data.subList(0, 30).toArray(new Row[30]);
						data.sort(new OrderbyComparator(new int[]{4,0},new boolean[]{true,true}));
						caida = data.subList(0, 30).toArray(new Row[30]);
						data.sort(new OrderbyComparator(new int[]{4,0},new boolean[]{true,false}));
						caidd = data.subList(0, 30).toArray(new Row[30]);
						data.sort(new OrderbyComparator(new int[]{4,3},new boolean[]{true,true}));
						cata = data.subList(0, 30).toArray(new Row[30]);
						data.sort(new OrderbyComparator(new int[]{4,3},new boolean[]{true,false}));
						catd = data.subList(0, 30).toArray(new Row[30]);
						data.sort(new OrderbyComparator(new int[]{4,0},new boolean[]{false,true}));
						cdida = data.subList(0, 30).toArray(new Row[30]);
						data.sort(new OrderbyComparator(new int[]{4,0},new boolean[]{false,false}));
						cdidd = data.subList(0, 30).toArray(new Row[30]);
						data.sort(new OrderbyComparator(new int[]{4,3},new boolean[]{false,true}));
						cdta = data.subList(0, 30).toArray(new Row[30]);
						data.sort(new OrderbyComparator(new int[]{4,3},new boolean[]{false,false}));
						cdtd = data.subList(0, 30).toArray(new Row[30]);

						data.sort(tablesort);
						idata = data.subList(0, 30).toArray(new Row[30]);
						idatd = data.subList(0, 30).toArray(new Row[30]);
						idaca = data.subList(0, 30).toArray(new Row[30]);
						idacd = data.subList(0, 30).toArray(new Row[30]);
						PrimitiveValue temp = null;
						PrimitiveValue temp1 = null;
						for(int i=0;i<data.size();i++)
						{
							comments.put(data.get(i).getColdata()[5], i);
							if(i==0)
							{					    		
								temp= data.get(i).getColdata()[data.get(i).getSortInd()];
								primaryind.put(temp, 0);
							}
							else
							{
								temp1 = data.get(i).getColdata()[data.get(i).getSortInd()];
								if(temp instanceof LongValue)
								{
									try {
										if(!(Long.valueOf(temp.toLong()).equals(Long.valueOf(temp1.toLong()))))
										{
											temp= temp1;
											primaryind.put(temp, i);
										}
									} catch (InvalidPrimitive e) {
										e.printStackTrace();
									}
								}
								else if(temp instanceof DoubleValue)
								{
									try {
										if(!(Double.valueOf(temp.toDouble()).equals(Double.valueOf(temp1.toDouble()))))
										{
											temp= temp1;
											primaryind.put(temp, i);
										}
									} catch (InvalidPrimitive e) {
										e.printStackTrace();
									}
								}
								else if(temp instanceof StringValue)
								{
									if(temp.toString().equals(temp1.toString()))
									{
										temp= temp1;
										primaryind.put(temp, i);
									}
								}
								else if(temp instanceof DateValue)
								{
									if(!((DateValue)temp).getValue().equals(((DateValue)temp1).getValue()))
									{
										temp= temp1;
										primaryind.put(temp, i);									
									}
								}
							}
						}
						filedata = data.toArray(new Row[data.size()]);
						data = null;	
						//long endtime = System.currentTimeMillis();
						//System.out.println(endtime-starttime);
					}
					else if(query instanceof Select)
					{	
						//long starttime = System.currentTimeMillis();
						PlainSelect select = (PlainSelect)(((Select) query).getSelectBody());
						boolean fullscan = true;
						HashSet<PrimitiveValue> fetch= new HashSet<PrimitiveValue>();
						List<BinaryExpression> whereexpList = new  ArrayList<BinaryExpression>();
						int limit = 0;
						List<OrderByElement> orderby = null;
						Stack<List<SelectItem>> selectitems = new Stack<List<SelectItem>>();
						List<Column> groupby = null;
						BinaryExpression equals = null;
						BinaryExpression notequals = null;
						boolean norows = false;
						PrimitiveValue Eventmin = null;
						PrimitiveValue Eventmax = null;
						PrimitiveValue Xmin = null;
						PrimitiveValue Xmax = null;
						PrimitiveValue Ymin = null;
						PrimitiveValue Ymax = null;
						PrimitiveValue Timemin = null;
						PrimitiveValue Timemax = null;
						PrimitiveValue Costmin = null;
						PrimitiveValue Costmax = null;
						int tcount = -1;
						int tcount1 = -1;
						if(select.getFromItem() instanceof Table)
						{
							selectitems.add(select.getSelectItems());
							if(select.getWhere() != null)
							{
								BinaryExpression e = (BinaryExpression)select.getWhere();

								while(e instanceof AndExpression)
								{
									BinaryExpression temp = (BinaryExpression) e.getRightExpression();
									if(temp instanceof EqualsTo)
									{
										if(equals!= null)
										{
											if(temp.getLeftExpression() instanceof Column)
											{
												PrimitiveValue val1 = (PrimitiveValue) temp.getRightExpression();
												PrimitiveValue val2 = (PrimitiveValue) equals.getRightExpression();
												if(indexsort.compare(val1, val2) != 0)
												{
													norows = true;
													break;
												}
											}
											if(temp.getRightExpression() instanceof Column)
											{
												PrimitiveValue val1 = (PrimitiveValue) temp.getLeftExpression();
												PrimitiveValue val2 = (PrimitiveValue) equals.getLeftExpression();
												if(indexsort.compare(val1, val2) != 0)
												{
													norows = true;
													break;
												}
											}
										}
										else
											equals = temp;
									}
									else if(temp instanceof NotEqualsTo)
									{
										notequals = temp;
									}
									else
									{
										if(temp.getLeftExpression() instanceof Column)
										{
											String name = ((Column)temp.getLeftExpression()).getColumnName();
											if(name.equals("EVENT_ID"))
											{												
												fullscan = false;
												if(temp instanceof MinorThanEquals)
												{
													PrimitiveValue val = (PrimitiveValue) temp.getRightExpression();
													if(Eventmax == null)
													{
														Eventmax = val;
													}
													else
													{
														if(indexsort.compare(Eventmax, val) > 0)
															Eventmax = val;
													}
													if(Eventmin!=null)
													{
														if(indexsort.compare(Eventmax, Eventmin) < 0)
														{
															norows = true;
															break;
														}
													}
												}
												else if(temp instanceof MinorThan)
												{
													PrimitiveValue val = primaryind.lowerKey((PrimitiveValue) temp.getRightExpression());
													if(Eventmax == null)
													{
														Eventmax = val;
													}
													else
													{
														if(indexsort.compare(Eventmax, val) > 0)
															Eventmax = val;
													}
													if(Eventmin!=null)
													{
														if(indexsort.compare(Eventmax, Eventmin) < 0)
														{
															norows = true;
															break;
														}
													}
												}
												else if(temp instanceof GreaterThan)
												{
													PrimitiveValue val = primaryind.higherKey((PrimitiveValue) temp.getRightExpression());
													if(Eventmin == null)
													{
														Eventmin = val;
													}
													else
													{
														if(indexsort.compare(Eventmin, val) < 0)
															Eventmin = val;
													}
													if(Eventmax!=null)
													{
														if(indexsort.compare(Eventmax, Eventmin) < 0)
														{
															norows = true;
															break;
														}
													}
												}
												else if(temp instanceof GreaterThanEquals)
												{
													PrimitiveValue val = (PrimitiveValue) temp.getRightExpression();
													if(Eventmin == null)
													{
														Eventmin = val;
													}
													else
													{
														if(indexsort.compare(Eventmin, val) < 0)
															Eventmin = val;
													}
													if(Eventmax!=null)
													{
														if(indexsort.compare(Eventmax, Eventmin) < 0)
														{
															norows = true;
															break;
														}
													}
												}
											}
											else if(name.equals("COST"))
											{
												fullscan = false;
												if(temp instanceof MinorThanEquals)
												{
													PrimitiveValue val = (PrimitiveValue) temp.getRightExpression();
													if(Costmax == null)
													{
														Costmax = val;
													}
													else
													{
														if(indexsort.compare(Costmax, val) > 0)
															Costmax = val;
													}
													if(Costmin!=null)
													{
														if(indexsort.compare(Costmax, Costmin) < 0)
														{
															norows = true;
															break;
														}
													}
												}
												else if(temp instanceof MinorThan)
												{
													PrimitiveValue val = COST.lowerKey((PrimitiveValue) temp.getRightExpression());
													if(Costmax == null)
													{
														Costmax = val;
													}
													else
													{
														if(indexsort.compare(Costmax, val) > 0)
															Costmax = val;
													}
													if(Costmin!=null)
													{
														if(indexsort.compare(Costmax, Costmin) < 0)
														{
															norows = true;
															break;
														}
													}
												}
												else if(temp instanceof GreaterThan)
												{
													PrimitiveValue val = COST.higherKey((PrimitiveValue) temp.getRightExpression());
													if(Costmin == null)
													{
														Costmin = val;
													}
													else
													{
														if(indexsort.compare(Costmin, val) < 0)
															Costmin = val;
													}
													if(Costmax!=null)
													{
														if(indexsort.compare(Costmax, Costmin) < 0)
														{
															norows = true;
															break;
														}
													}
												}
												else if(temp instanceof GreaterThanEquals)
												{
													PrimitiveValue val = (PrimitiveValue) temp.getRightExpression();
													if(Costmin == null)
													{
														Costmin = val;
													}
													else
													{
														if(indexsort.compare(Costmin, val) < 0)
															Costmin = val;
													}
													if(Costmax!=null)
													{
														if(indexsort.compare(Costmax, Costmin) < 0)
														{
															norows = true;
															break;
														}
													}
												}
											}
											else if(name.equals("TIME"))
											{
												fullscan = false;
												if(temp instanceof MinorThanEquals)
												{
													PrimitiveValue val = (PrimitiveValue) temp.getRightExpression();
													if(Timemax == null)
													{
														Timemax = val;
													}
													else
													{
														if(indexsort.compare(Timemax, val) > 0)
															Timemax = val;
													}
													if(Timemin!=null)
													{
														if(indexsort.compare(Timemax, Timemin) < 0)
														{
															norows = true;
															break;
														}
													}
												}
												else if(temp instanceof MinorThan)
												{
													PrimitiveValue val = TIME.lowerKey((PrimitiveValue) temp.getRightExpression());
													if(Timemax == null)
													{
														Timemax = val;
													}
													else
													{
														if(indexsort.compare(Timemax, val) > 0)
															Timemax = val;
													}
													if(Timemin!=null)
													{
														if(indexsort.compare(Timemax, Timemin) < 0)
														{
															norows = true;
															break;
														}
													}
												}
												else if(temp instanceof GreaterThan)
												{
													PrimitiveValue val = TIME.higherKey((PrimitiveValue) temp.getRightExpression());
													if(Timemin == null)
													{
														Timemin = val;
													}
													else
													{
														if(indexsort.compare(Timemin, val) < 0)
															Timemin = val;
													}
													if(Timemax!=null)
													{
														if(indexsort.compare(Timemax, Timemin) < 0)
														{
															norows = true;
															break;
														}
													}
												}
												else if(temp instanceof GreaterThanEquals)
												{
													PrimitiveValue val = (PrimitiveValue) temp.getRightExpression();
													if(Timemin == null)
													{
														Timemin = val;
													}
													else
													{
														if(indexsort.compare(Timemin, val) < 0)
															Timemin = val;
													}
													if(Timemax!=null)
													{
														if(indexsort.compare(Timemax, Timemin) < 0)
														{
															norows = true;
															break;
														}
													}
												}
											}
											else if(name.equals("X"))
											{
												whereexpList.add(temp);
												if(temp instanceof MinorThanEquals)
												{
													Xmax = (PrimitiveValue) temp.getRightExpression();		
													if(Xmin!=null)
													{
														if(indexsort.compare(Xmax, Xmin) == 0)
														{
															norows = true;
															break;
														}
													}
												}
											}
											else if(name.equals("Y"))
											{
												if(temp instanceof MinorThanEquals)
												{
													Ymax = (PrimitiveValue) temp.getRightExpression();	
													if(Ymin!=null)
													{
														if(indexsort.compare(Ymax, Ymin) == 0)
														{
															norows = true;
															break;
														}
													}
												}
												whereexpList.add(temp);
											}
										}	
										else if(temp.getRightExpression() instanceof Column)
										{
											String name = ((Column)temp.getRightExpression()).getColumnName();
											if(name.equals("EVENT_ID"))
											{												
												fullscan = false;
												if(temp instanceof GreaterThanEquals)
												{
													PrimitiveValue val = (PrimitiveValue) temp.getLeftExpression();
													if(Eventmax == null)
													{
														Eventmax = val;
													}
													else
													{
														if(indexsort.compare(Eventmax, val) > 0)
															Eventmax = val;
													}
													if(Eventmin!=null)
													{
														if(indexsort.compare(Eventmax, Eventmin) < 0)
														{
															norows = true;
															break;
														}
													}
												}
												else if(temp instanceof GreaterThan)
												{
													PrimitiveValue val = primaryind.lowerKey((PrimitiveValue) temp.getLeftExpression());
													if(Eventmax == null)
													{
														Eventmax = val;
													}
													else
													{
														if(indexsort.compare(Eventmax, val) > 0)
															Eventmax = val;
													}
													if(Eventmin!=null)
													{
														if(indexsort.compare(Eventmax, Eventmin) < 0)
														{
															norows = true;
															break;
														}
													}
												}
												else if(temp instanceof MinorThan)
												{
													PrimitiveValue val = primaryind.higherKey((PrimitiveValue) temp.getLeftExpression());
													if(Eventmin == null)
													{
														Eventmin = val;
													}
													else
													{
														if(indexsort.compare(Eventmin, val) < 0)
															Eventmin = val;
													}
													if(Eventmax!=null)
													{
														if(indexsort.compare(Eventmax, Eventmin) < 0)
														{
															norows = true;
															break;
														}
													}
												}
												else if(temp instanceof MinorThanEquals)
												{
													PrimitiveValue val = (PrimitiveValue) temp.getLeftExpression();
													if(Eventmin == null)
													{
														Eventmin = val;
													}
													else
													{
														if(indexsort.compare(Eventmin, val) < 0)
															Eventmin = val;
													}
													if(Eventmax!=null)
													{
														if(indexsort.compare(Eventmax, Eventmin) < 0)
														{
															norows = true;
															break;
														}
													}
												}
											}
											else if(name.equals("COST"))
											{
												fullscan = false;
												if(temp instanceof GreaterThanEquals)
												{
													PrimitiveValue val = (PrimitiveValue) temp.getLeftExpression();
													if(Costmax == null)
													{
														Costmax = val;
													}
													else
													{
														if(indexsort.compare(Costmax, val) > 0)
															Costmax = val;
													}
													if(Costmin!=null)
													{
														if(indexsort.compare(Costmax, Costmin) < 0)
														{
															norows = true;
															break;
														}
													}
												}
												else if(temp instanceof GreaterThan)
												{
													PrimitiveValue val = COST.lowerKey((PrimitiveValue) temp.getLeftExpression());
													if(Costmax == null)
													{
														Costmax = val;
													}
													else
													{
														if(indexsort.compare(Costmax, val) > 0)
															Costmax = val;
													}
													if(Costmin!=null)
													{
														if(indexsort.compare(Costmax, Costmin) < 0)
														{
															norows = true;
															break;
														}
													}
												}
												else if(temp instanceof MinorThan)
												{
													PrimitiveValue val = COST.higherKey((PrimitiveValue) temp.getLeftExpression());
													if(Costmin == null)
													{
														Costmin = val;
													}
													else
													{
														if(indexsort.compare(Costmin, val) < 0)
															Costmin = val;
													}
													if(Costmax!=null)
													{
														if(indexsort.compare(Costmax, Costmin) < 0)
														{
															norows = true;
															break;
														}
													}
												}
												else if(temp instanceof MinorThanEquals)
												{
													PrimitiveValue val = (PrimitiveValue) temp.getLeftExpression();
													if(Costmin == null)
													{
														Costmin = val;
													}
													else
													{
														if(indexsort.compare(Costmin, val) < 0)
															Costmin = val;
													}
													if(Costmax!=null)
													{
														if(indexsort.compare(Costmax, Costmin) < 0)
														{
															norows = true;
															break;
														}
													}
												}
											}
											else if(name.equals("TIME"))
											{
												fullscan = false;
												if(temp instanceof GreaterThanEquals)
												{
													PrimitiveValue val = (PrimitiveValue) temp.getLeftExpression();
													if(Timemax == null)
													{
														Timemax = val;
													}
													else
													{
														if(indexsort.compare(Timemax, val) > 0)
															Timemax = val;
													}
													if(Timemin!=null)
													{
														if(indexsort.compare(Timemax, Timemin) < 0)
														{
															norows = true;
															break;
														}
													}
												}
												else if(temp instanceof GreaterThan)
												{
													PrimitiveValue val = TIME.lowerKey((PrimitiveValue) temp.getLeftExpression());
													if(Timemax == null)
													{
														Timemax = val;
													}
													else
													{
														if(indexsort.compare(Timemax, val) > 0)
															Timemax = val;
													}
													if(Timemin!=null)
													{
														if(indexsort.compare(Timemax, Timemin) < 0)
														{
															norows = true;
															break;
														}
													}
												}
												else if(temp instanceof MinorThan)
												{
													PrimitiveValue val = TIME.higherKey((PrimitiveValue) temp.getLeftExpression());
													if(Timemin == null)
													{
														Timemin = val;
													}
													else
													{
														if(indexsort.compare(Timemin, val) < 0)
															Timemin = val;
													}
													if(Timemax!=null)
													{
														if(indexsort.compare(Timemax, Timemin) < 0)
														{
															norows = true;
															break;
														}
													}
												}
												else if(temp instanceof MinorThanEquals)
												{
													PrimitiveValue val = (PrimitiveValue) temp.getLeftExpression();
													if(Timemin == null)
													{
														Timemin = val;
													}
													else
													{
														if(indexsort.compare(Timemin, val) < 0)
															Timemin = val;
													}
													if(Timemax!=null)
													{
														if(indexsort.compare(Timemax, Timemin) < 0)
														{
															norows = true;
															break;
														}
													}
												}
											}
											else if(name.equals("X"))
											{
												whereexpList.add(temp);
												if(temp instanceof MinorThan)
												{
													Xmin = (PrimitiveValue) temp.getLeftExpression();	
													if(Xmax!=null)
													{
														if(indexsort.compare(Xmax, Xmin) == 0)
														{
															norows = true;
															break;
														}
													}
												}
											}
											else if(name.equals("Y"))
											{
												if(temp instanceof MinorThan)
												{
													Ymin = (PrimitiveValue) temp.getLeftExpression();	
													if(Ymax!=null)
													{
														if(indexsort.compare(Ymax, Ymin) == 0)
														{
															norows = true;
															break;
														}
													}
												}
												whereexpList.add(temp);
											}
										}
									}
									e = (BinaryExpression) e.getLeftExpression();
								}
								if(e instanceof EqualsTo)
								{
									equals = e;
								}
								else if(e instanceof NotEqualsTo)
								{
									notequals = e;
								}
								else
								{
									BinaryExpression temp = e;
									if(temp.getLeftExpression() instanceof Column)
									{
										String name = ((Column)temp.getLeftExpression()).getColumnName();
										if(name.equals("EVENT_ID"))
										{												
											fullscan = false;
											if(temp instanceof MinorThanEquals)
											{
												PrimitiveValue val = (PrimitiveValue) temp.getRightExpression();
												if(Eventmax == null)
												{
													Eventmax = val;
												}
												else
												{
													if(indexsort.compare(Eventmax, val) > 0)
														Eventmax = val;
												}
												if(Eventmin!=null)
												{
													if(indexsort.compare(Eventmax, Eventmin) < 0)
													{
														norows = true;
														//break;
													}
												}
											}
											else if(temp instanceof MinorThan)
											{
												PrimitiveValue val = primaryind.lowerKey((PrimitiveValue) temp.getRightExpression());
												if(Eventmax == null)
												{
													Eventmax = val;
												}
												else
												{
													if(indexsort.compare(Eventmax, val) > 0)
														Eventmax = val;
												}
												if(Eventmin!=null)
												{
													if(indexsort.compare(Eventmax, Eventmin) < 0)
													{
														norows = true;
														//break;
													}
												}
											}
											else if(temp instanceof GreaterThan)
											{
												PrimitiveValue val = primaryind.higherKey((PrimitiveValue) temp.getRightExpression());
												if(Eventmin == null)
												{
													Eventmin = val;
												}
												else
												{
													if(indexsort.compare(Eventmin, val) < 0)
														Eventmin = val;
												}
												if(Eventmax!=null)
												{
													if(indexsort.compare(Eventmax, Eventmin) < 0)
													{
														norows = true;
														//break;
													}
												}
											}
											else if(temp instanceof GreaterThanEquals)
											{
												PrimitiveValue val = (PrimitiveValue) temp.getRightExpression();
												if(Eventmin == null)
												{
													Eventmin = val;
												}
												else
												{
													if(indexsort.compare(Eventmin, val) < 0)
														Eventmin = val;
												}
												if(Eventmax!=null)
												{
													if(indexsort.compare(Eventmax, Eventmin) < 0)
													{
														norows = true;
														//break;
													}
												}
											}
										}
										else if(name.equals("COST"))
										{
											fullscan = false;
											if(temp instanceof MinorThanEquals)
											{
												PrimitiveValue val = (PrimitiveValue) temp.getRightExpression();
												if(Costmax == null)
												{
													Costmax = val;
												}
												else
												{
													if(indexsort.compare(Costmax, val) > 0)
														Costmax = val;
												}
												if(Costmin!=null)
												{
													if(indexsort.compare(Costmax, Costmin) < 0)
													{
														norows = true;
														//break;
													}
												}
											}
											else if(temp instanceof MinorThan)
											{
												PrimitiveValue val = COST.lowerKey((PrimitiveValue) temp.getRightExpression());
												if(Costmax == null)
												{
													Costmax = val;
												}
												else
												{
													if(indexsort.compare(Costmax, val) > 0)
														Costmax = val;
												}
												if(Costmin!=null)
												{
													if(indexsort.compare(Costmax, Costmin) < 0)
													{
														norows = true;
														//break;
													}
												}
											}
											else if(temp instanceof GreaterThan)
											{
												PrimitiveValue val = COST.higherKey((PrimitiveValue) temp.getRightExpression());
												if(Costmin == null)
												{
													Costmin = val;
												}
												else
												{
													if(indexsort.compare(Costmin, val) < 0)
														Costmin = val;
												}
												if(Costmax!=null)
												{
													if(indexsort.compare(Costmax, Costmin) < 0)
													{
														norows = true;
														//break;
													}
												}
											}
											else if(temp instanceof GreaterThanEquals)
											{
												PrimitiveValue val = (PrimitiveValue) temp.getRightExpression();
												if(Costmin == null)
												{
													Costmin = val;
												}
												else
												{
													if(indexsort.compare(Costmin, val) < 0)
														Costmin = val;
												}
												if(Costmax!=null)
												{
													if(indexsort.compare(Costmax, Costmin) < 0)
													{
														norows = true;
														//break;
													}
												}
											}
										}
										else if(name.equals("TIME"))
										{
											fullscan = false;
											if(temp instanceof MinorThanEquals)
											{
												PrimitiveValue val = (PrimitiveValue) temp.getRightExpression();
												if(Timemax == null)
												{
													Timemax = val;
												}
												else
												{
													if(indexsort.compare(Timemax, val) > 0)
														Timemax = val;
												}
												if(Timemin!=null)
												{
													if(indexsort.compare(Timemax, Timemin) < 0)
													{
														norows = true;
														//break;
													}
												}
											}
											else if(temp instanceof MinorThan)
											{
												PrimitiveValue val = TIME.lowerKey((PrimitiveValue) temp.getRightExpression());
												if(Timemax == null)
												{
													Timemax = val;
												}
												else
												{
													if(indexsort.compare(Timemax, val) > 0)
														Timemax = val;
												}
												if(Timemin!=null)
												{
													if(indexsort.compare(Timemax, Timemin) < 0)
													{
														norows = true;
														//break;
													}
												}
											}
											else if(temp instanceof GreaterThan)
											{
												PrimitiveValue val = TIME.higherKey((PrimitiveValue) temp.getRightExpression());
												if(Timemin == null)
												{
													Timemin = val;
												}
												else
												{
													if(indexsort.compare(Timemin, val) < 0)
														Timemin = val;
												}
												if(Timemax!=null)
												{
													if(indexsort.compare(Timemax, Timemin) < 0)
													{
														norows = true;
														//break;
													}
												}
											}
											else if(temp instanceof GreaterThanEquals)
											{
												PrimitiveValue val = (PrimitiveValue) temp.getRightExpression();
												if(Timemin == null)
												{
													Timemin = val;
												}
												else
												{
													if(indexsort.compare(Timemin, val) < 0)
														Timemin = val;
												}
												if(Timemax!=null)
												{
													if(indexsort.compare(Timemax, Timemin) < 0)
													{
														norows = true;
														//break;
													}
												}
											}
										}
										else if(name.equals("X"))
										{
											whereexpList.add(temp);
											if(temp instanceof MinorThanEquals)
											{
												Xmax = (PrimitiveValue) temp.getRightExpression();		
												if(Xmin!=null)
												{
													if(indexsort.compare(Xmax, Xmin) == 0)
													{
														norows = true;
														//break;
													}
												}
											}
										}
										else if(name.equals("Y"))
										{
											if(temp instanceof MinorThanEquals)
											{
												Ymax = (PrimitiveValue) temp.getRightExpression();	
												if(Ymin!=null)
												{
													if(indexsort.compare(Ymax, Ymin) == 0)
													{
														norows = true;
														//break;
													}
												}
											}
											whereexpList.add(temp);
										}
									}	
									else if(temp.getRightExpression() instanceof Column)
									{
										String name = ((Column)temp.getRightExpression()).getColumnName();
										if(name.equals("EVENT_ID"))
										{												
											fullscan = false;
											if(temp instanceof GreaterThanEquals)
											{
												PrimitiveValue val = (PrimitiveValue) temp.getLeftExpression();
												if(Eventmax == null)
												{
													Eventmax = val;
												}
												else
												{
													if(indexsort.compare(Eventmax, val) > 0)
														Eventmax = val;
												}
												if(Eventmin!=null)
												{
													if(indexsort.compare(Eventmax, Eventmin) < 0)
													{
														norows = true;
														//break;
													}
												}
											}
											else if(temp instanceof GreaterThan)
											{
												PrimitiveValue val = primaryind.lowerKey((PrimitiveValue) temp.getLeftExpression());
												if(Eventmax == null)
												{
													Eventmax = val;
												}
												else
												{
													if(indexsort.compare(Eventmax, val) > 0)
														Eventmax = val;
												}
												if(Eventmin!=null)
												{
													if(indexsort.compare(Eventmax, Eventmin) < 0)
													{
														norows = true;
														//break;
													}
												}
											}
											else if(temp instanceof MinorThan)
											{
												PrimitiveValue val = primaryind.higherKey((PrimitiveValue) temp.getLeftExpression());
												if(Eventmin == null)
												{
													Eventmin = val;
												}
												else
												{
													if(indexsort.compare(Eventmin, val) < 0)
														Eventmin = val;
												}
												if(Eventmax!=null)
												{
													if(indexsort.compare(Eventmax, Eventmin) < 0)
													{
														norows = true;
														//break;
													}
												}
											}
											else if(temp instanceof MinorThanEquals)
											{
												PrimitiveValue val = (PrimitiveValue) temp.getLeftExpression();
												if(Eventmin == null)
												{
													Eventmin = val;
												}
												else
												{
													if(indexsort.compare(Eventmin, val) < 0)
														Eventmin = val;
												}
												if(Eventmax!=null)
												{
													if(indexsort.compare(Eventmax, Eventmin) < 0)
													{
														norows = true;
														//break;
													}
												}
											}
										}
										else if(name.equals("COST"))
										{
											fullscan = false;
											if(temp instanceof GreaterThanEquals)
											{
												PrimitiveValue val = (PrimitiveValue) temp.getLeftExpression();
												if(Costmax == null)
												{
													Costmax = val;
												}
												else
												{
													if(indexsort.compare(Costmax, val) > 0)
														Costmax = val;
												}
												if(Costmin!=null)
												{
													if(indexsort.compare(Costmax, Costmin) < 0)
													{
														norows = true;
														//break;
													}
												}
											}
											else if(temp instanceof GreaterThan)
											{
												PrimitiveValue val = COST.lowerKey((PrimitiveValue) temp.getLeftExpression());
												if(Costmax == null)
												{
													Costmax = val;
												}
												else
												{
													if(indexsort.compare(Costmax, val) > 0)
														Costmax = val;
												}
												if(Costmin!=null)
												{
													if(indexsort.compare(Costmax, Costmin) < 0)
													{
														norows = true;
														//break;
													}
												}
											}
											else if(temp instanceof MinorThan)
											{
												PrimitiveValue val = COST.higherKey((PrimitiveValue) temp.getLeftExpression());
												if(Costmin == null)
												{
													Costmin = val;
												}
												else
												{
													if(indexsort.compare(Costmin, val) < 0)
														Costmin = val;
												}
												if(Costmax!=null)
												{
													if(indexsort.compare(Costmax, Costmin) < 0)
													{
														norows = true;
														//break;
													}
												}
											}
											else if(temp instanceof MinorThanEquals)
											{
												PrimitiveValue val = (PrimitiveValue) temp.getLeftExpression();
												if(Costmin == null)
												{
													Costmin = val;
												}
												else
												{
													if(indexsort.compare(Costmin, val) < 0)
														Costmin = val;
												}
												if(Costmax!=null)
												{
													if(indexsort.compare(Costmax, Costmin) < 0)
													{
														norows = true;
														//break;
													}
												}
											}
										}
										else if(name.equals("TIME"))
										{
											fullscan = false;
											if(temp instanceof GreaterThanEquals)
											{
												PrimitiveValue val = (PrimitiveValue) temp.getLeftExpression();
												if(Timemax == null)
												{
													Timemax = val;
												}
												else
												{
													if(indexsort.compare(Timemax, val) > 0)
														Timemax = val;
												}
												if(Timemin!=null)
												{
													if(indexsort.compare(Timemax, Timemin) < 0)
													{
														norows = true;
														//break;
													}
												}
											}
											else if(temp instanceof GreaterThan)
											{
												PrimitiveValue val = TIME.lowerKey((PrimitiveValue) temp.getLeftExpression());
												if(Timemax == null)
												{
													Timemax = val;
												}
												else
												{
													if(indexsort.compare(Timemax, val) > 0)
														Timemax = val;
												}
												if(Timemin!=null)
												{
													if(indexsort.compare(Timemax, Timemin) < 0)
													{
														norows = true;
														//break;
													}
												}
											}
											else if(temp instanceof MinorThan)
											{
												PrimitiveValue val = TIME.higherKey((PrimitiveValue) temp.getLeftExpression());
												if(Timemin == null)
												{
													Timemin = val;
												}
												else
												{
													if(indexsort.compare(Timemin, val) < 0)
														Timemin = val;
												}
												if(Timemax!=null)
												{
													if(indexsort.compare(Timemax, Timemin) < 0)
													{
														norows = true;
														//break;
													}
												}
											}
											else if(temp instanceof MinorThanEquals)
											{
												PrimitiveValue val = (PrimitiveValue) temp.getLeftExpression();
												if(Timemin == null)
												{
													Timemin = val;
												}
												else
												{
													if(indexsort.compare(Timemin, val) < 0)
														Timemin = val;
												}
												if(Timemax!=null)
												{
													if(indexsort.compare(Timemax, Timemin) < 0)
													{
														norows = true;
														//break;
													}
												}
											}
										}
										else if(name.equals("X"))
										{
											whereexpList.add(temp);
											if(temp instanceof MinorThan)
											{
												Xmin = (PrimitiveValue) temp.getLeftExpression();	
												if(Xmax!=null)
												{
													if(indexsort.compare(Xmax, Xmin) == 0)
													{
														norows = true;
														//break;
													}
												}
											}
										}
										else if(name.equals("Y"))
										{
											if(temp instanceof MinorThan)
											{
												Ymin = (PrimitiveValue) temp.getLeftExpression();	
												if(Ymax!=null)
												{
													if(indexsort.compare(Ymax, Ymin) == 0)
													{
														norows = true;
														//break;
													}
												}
											}
											whereexpList.add(temp);
										}
									}
								}
							}
							if(select.getLimit() != null)
								limit = (int) select.getLimit().getRowCount();

							if(select.getGroupByColumnReferences() != null)
								groupby = select.getGroupByColumnReferences();

							if(select.getOrderByElements() != null)
							{
								orderby = select.getOrderByElements();
								if(orderby.size() == 1)
								{
									if(primaryindname.equals(((Column)orderby.get(0).getExpression()).getColumnName()))
										orderby = null;
								}
							}
						}
						else
						{
							while(true)
							{
								selectitems.add(select.getSelectItems());
								if(select.getWhere() != null)
								{
									BinaryExpression e = (BinaryExpression)select.getWhere();

									while(e instanceof AndExpression)
									{
										BinaryExpression temp = (BinaryExpression) e.getRightExpression();
										if(temp instanceof EqualsTo)
										{
											if(equals!= null)
											{
												if(temp.getLeftExpression() instanceof Column)
												{
													PrimitiveValue val1 = (PrimitiveValue) temp.getRightExpression();
													PrimitiveValue val2 = (PrimitiveValue) equals.getRightExpression();
													if(indexsort.compare(val1, val2) != 0)
													{
														norows = true;
														break;
													}
												}
												if(temp.getRightExpression() instanceof Column)
												{
													PrimitiveValue val1 = (PrimitiveValue) temp.getLeftExpression();
													PrimitiveValue val2 = (PrimitiveValue) equals.getLeftExpression();
													if(indexsort.compare(val1, val2) != 0)
													{
														norows = true;
														break;
													}
												}
											}
											else
												equals = temp;
										}
										else if(temp instanceof NotEqualsTo)
										{
											notequals = temp;
										}
										else
										{
											if(temp.getLeftExpression() instanceof Column)
											{
												String name = ((Column)temp.getLeftExpression()).getColumnName();
												if(name.equals("EVENT_ID"))
												{												
													fullscan = false;
													if(temp instanceof MinorThanEquals)
													{
														PrimitiveValue val = (PrimitiveValue) temp.getRightExpression();
														if(Eventmax == null)
														{
															Eventmax = val;
														}
														else
														{
															if(indexsort.compare(Eventmax, val) > 0)
																Eventmax = val;
														}
														if(Eventmin!=null)
														{
															if(indexsort.compare(Eventmax, Eventmin) < 0)
															{
																norows = true;
																break;
															}
														}
													}
													else if(temp instanceof MinorThan)
													{
														PrimitiveValue val = primaryind.lowerKey((PrimitiveValue) temp.getRightExpression());
														if(Eventmax == null)
														{
															Eventmax = val;
														}
														else
														{
															if(indexsort.compare(Eventmax, val) > 0)
																Eventmax = val;
														}
														if(Eventmin!=null)
														{
															if(indexsort.compare(Eventmax, Eventmin) < 0)
															{
																norows = true;
																break;
															}
														}
													}
													else if(temp instanceof GreaterThan)
													{
														PrimitiveValue val = primaryind.higherKey((PrimitiveValue) temp.getRightExpression());
														if(Eventmin == null)
														{
															Eventmin = val;
														}
														else
														{
															if(indexsort.compare(Eventmin, val) < 0)
																Eventmin = val;
														}
														if(Eventmax!=null)
														{
															if(indexsort.compare(Eventmax, Eventmin) < 0)
															{
																norows = true;
																break;
															}
														}
													}
													else if(temp instanceof GreaterThanEquals)
													{
														PrimitiveValue val = (PrimitiveValue) temp.getRightExpression();
														if(Eventmin == null)
														{
															Eventmin = val;
														}
														else
														{
															if(indexsort.compare(Eventmin, val) < 0)
																Eventmin = val;
														}
														if(Eventmax!=null)
														{
															if(indexsort.compare(Eventmax, Eventmin) < 0)
															{
																norows = true;
																break;
															}
														}
													}
												}
												else if(name.equals("COST"))
												{
													fullscan = false;
													if(temp instanceof MinorThanEquals)
													{
														PrimitiveValue val = (PrimitiveValue) temp.getRightExpression();
														if(Costmax == null)
														{
															Costmax = val;
														}
														else
														{
															if(indexsort.compare(Costmax, val) > 0)
																Costmax = val;
														}
														if(Costmin!=null)
														{
															if(indexsort.compare(Costmax, Costmin) < 0)
															{
																norows = true;
																break;
															}
														}
													}
													else if(temp instanceof MinorThan)
													{
														PrimitiveValue val = COST.lowerKey((PrimitiveValue) temp.getRightExpression());
														if(Costmax == null)
														{
															Costmax = val;
														}
														else
														{
															if(indexsort.compare(Costmax, val) > 0)
																Costmax = val;
														}
														if(Costmin!=null)
														{
															if(indexsort.compare(Costmax, Costmin) < 0)
															{
																norows = true;
																break;
															}
														}
													}
													else if(temp instanceof GreaterThan)
													{
														PrimitiveValue val = COST.higherKey((PrimitiveValue) temp.getRightExpression());
														if(Costmin == null)
														{
															Costmin = val;
														}
														else
														{
															if(indexsort.compare(Costmin, val) < 0)
																Costmin = val;
														}
														if(Costmax!=null)
														{
															if(indexsort.compare(Costmax, Costmin) < 0)
															{
																norows = true;
																break;
															}
														}
													}
													else if(temp instanceof GreaterThanEquals)
													{
														PrimitiveValue val = (PrimitiveValue) temp.getRightExpression();
														if(Costmin == null)
														{
															Costmin = val;
														}
														else
														{
															if(indexsort.compare(Costmin, val) < 0)
																Costmin = val;
														}
														if(Costmax!=null)
														{
															if(indexsort.compare(Costmax, Costmin) < 0)
															{
																norows = true;
																break;
															}
														}
													}
												}
												else if(name.equals("TIME"))
												{
													fullscan = false;
													if(temp instanceof MinorThanEquals)
													{
														PrimitiveValue val = (PrimitiveValue) temp.getRightExpression();
														if(Timemax == null)
														{
															Timemax = val;
														}
														else
														{
															if(indexsort.compare(Timemax, val) > 0)
																Timemax = val;
														}
														if(Timemin!=null)
														{
															if(indexsort.compare(Timemax, Timemin) < 0)
															{
																norows = true;
																break;
															}
														}
													}
													else if(temp instanceof MinorThan)
													{
														PrimitiveValue val = TIME.lowerKey((PrimitiveValue) temp.getRightExpression());
														if(Timemax == null)
														{
															Timemax = val;
														}
														else
														{
															if(indexsort.compare(Timemax, val) > 0)
																Timemax = val;
														}
														if(Timemin!=null)
														{
															if(indexsort.compare(Timemax, Timemin) < 0)
															{
																norows = true;
																break;
															}
														}
													}
													else if(temp instanceof GreaterThan)
													{
														PrimitiveValue val = TIME.higherKey((PrimitiveValue) temp.getRightExpression());
														if(Timemin == null)
														{
															Timemin = val;
														}
														else
														{
															if(indexsort.compare(Timemin, val) < 0)
																Timemin = val;
														}
														if(Timemax!=null)
														{
															if(indexsort.compare(Timemax, Timemin) < 0)
															{
																norows = true;
																break;
															}
														}
													}
													else if(temp instanceof GreaterThanEquals)
													{
														PrimitiveValue val = (PrimitiveValue) temp.getRightExpression();
														if(Timemin == null)
														{
															Timemin = val;
														}
														else
														{
															if(indexsort.compare(Timemin, val) < 0)
																Timemin = val;
														}
														if(Timemax!=null)
														{
															if(indexsort.compare(Timemax, Timemin) < 0)
															{
																norows = true;
																break;
															}
														}
													}
												}
												else if(name.equals("X"))
												{
													whereexpList.add(temp);
													if(temp instanceof MinorThanEquals)
													{
														Xmax = (PrimitiveValue) temp.getRightExpression();		
														if(Xmin!=null)
														{
															if(indexsort.compare(Xmax, Xmin) == 0)
															{
																norows = true;
																break;
															}
														}
													}
												}
												else if(name.equals("Y"))
												{
													if(temp instanceof MinorThanEquals)
													{
														Ymax = (PrimitiveValue) temp.getRightExpression();	
														if(Ymin!=null)
														{
															if(indexsort.compare(Ymax, Ymin) == 0)
															{
																norows = true;
																break;
															}
														}
													}
													whereexpList.add(temp);
												}
											}	
											else if(temp.getRightExpression() instanceof Column)
											{
												String name = ((Column)temp.getRightExpression()).getColumnName();
												if(name.equals("EVENT_ID"))
												{												
													fullscan = false;
													if(temp instanceof GreaterThanEquals)
													{
														PrimitiveValue val = (PrimitiveValue) temp.getLeftExpression();
														if(Eventmax == null)
														{
															Eventmax = val;
														}
														else
														{
															if(indexsort.compare(Eventmax, val) > 0)
																Eventmax = val;
														}
														if(Eventmin!=null)
														{
															if(indexsort.compare(Eventmax, Eventmin) < 0)
															{
																norows = true;
																break;
															}
														}
													}
													else if(temp instanceof GreaterThan)
													{
														PrimitiveValue val = primaryind.lowerKey((PrimitiveValue) temp.getLeftExpression());
														if(Eventmax == null)
														{
															Eventmax = val;
														}
														else
														{
															if(indexsort.compare(Eventmax, val) > 0)
																Eventmax = val;
														}
														if(Eventmin!=null)
														{
															if(indexsort.compare(Eventmax, Eventmin) < 0)
															{
																norows = true;
																break;
															}
														}
													}
													else if(temp instanceof MinorThan)
													{
														PrimitiveValue val = primaryind.higherKey((PrimitiveValue) temp.getLeftExpression());
														if(Eventmin == null)
														{
															Eventmin = val;
														}
														else
														{
															if(indexsort.compare(Eventmin, val) < 0)
																Eventmin = val;
														}
														if(Eventmax!=null)
														{
															if(indexsort.compare(Eventmax, Eventmin) < 0)
															{
																norows = true;
																break;
															}
														}
													}
													else if(temp instanceof MinorThanEquals)
													{
														PrimitiveValue val = (PrimitiveValue) temp.getLeftExpression();
														if(Eventmin == null)
														{
															Eventmin = val;
														}
														else
														{
															if(indexsort.compare(Eventmin, val) < 0)
																Eventmin = val;
														}
														if(Eventmax!=null)
														{
															if(indexsort.compare(Eventmax, Eventmin) < 0)
															{
																norows = true;
																break;
															}
														}
													}
												}
												else if(name.equals("COST"))
												{
													fullscan = false;
													if(temp instanceof GreaterThanEquals)
													{
														PrimitiveValue val = (PrimitiveValue) temp.getLeftExpression();
														if(Costmax == null)
														{
															Costmax = val;
														}
														else
														{
															if(indexsort.compare(Costmax, val) > 0)
																Costmax = val;
														}
														if(Costmin!=null)
														{
															if(indexsort.compare(Costmax, Costmin) < 0)
															{
																norows = true;
																break;
															}
														}
													}
													else if(temp instanceof GreaterThan)
													{
														PrimitiveValue val = COST.lowerKey((PrimitiveValue) temp.getLeftExpression());
														if(Costmax == null)
														{
															Costmax = val;
														}
														else
														{
															if(indexsort.compare(Costmax, val) > 0)
																Costmax = val;
														}
														if(Costmin!=null)
														{
															if(indexsort.compare(Costmax, Costmin) < 0)
															{
																norows = true;
																break;
															}
														}
													}
													else if(temp instanceof MinorThan)
													{
														PrimitiveValue val = COST.higherKey((PrimitiveValue) temp.getLeftExpression());
														if(Costmin == null)
														{
															Costmin = val;
														}
														else
														{
															if(indexsort.compare(Costmin, val) < 0)
																Costmin = val;
														}
														if(Costmax!=null)
														{
															if(indexsort.compare(Costmax, Costmin) < 0)
															{
																norows = true;
																break;
															}
														}
													}
													else if(temp instanceof MinorThanEquals)
													{
														PrimitiveValue val = (PrimitiveValue) temp.getLeftExpression();
														if(Costmin == null)
														{
															Costmin = val;
														}
														else
														{
															if(indexsort.compare(Costmin, val) < 0)
																Costmin = val;
														}
														if(Costmax!=null)
														{
															if(indexsort.compare(Costmax, Costmin) < 0)
															{
																norows = true;
																break;
															}
														}
													}
												}
												else if(name.equals("TIME"))
												{
													fullscan = false;
													if(temp instanceof GreaterThanEquals)
													{
														PrimitiveValue val = (PrimitiveValue) temp.getLeftExpression();
														if(Timemax == null)
														{
															Timemax = val;
														}
														else
														{
															if(indexsort.compare(Timemax, val) > 0)
																Timemax = val;
														}
														if(Timemin!=null)
														{
															if(indexsort.compare(Timemax, Timemin) < 0)
															{
																norows = true;
																break;
															}
														}
													}
													else if(temp instanceof GreaterThan)
													{
														PrimitiveValue val = TIME.lowerKey((PrimitiveValue) temp.getLeftExpression());
														if(Timemax == null)
														{
															Timemax = val;
														}
														else
														{
															if(indexsort.compare(Timemax, val) > 0)
																Timemax = val;
														}
														if(Timemin!=null)
														{
															if(indexsort.compare(Timemax, Timemin) < 0)
															{
																norows = true;
																break;
															}
														}
													}
													else if(temp instanceof MinorThan)
													{
														PrimitiveValue val = TIME.higherKey((PrimitiveValue) temp.getLeftExpression());
														if(Timemin == null)
														{
															Timemin = val;
														}
														else
														{
															if(indexsort.compare(Timemin, val) < 0)
																Timemin = val;
														}
														if(Timemax!=null)
														{
															if(indexsort.compare(Timemax, Timemin) < 0)
															{
																norows = true;
																break;
															}
														}
													}
													else if(temp instanceof MinorThanEquals)
													{
														PrimitiveValue val = (PrimitiveValue) temp.getLeftExpression();
														if(Timemin == null)
														{
															Timemin = val;
														}
														else
														{
															if(indexsort.compare(Timemin, val) < 0)
																Timemin = val;
														}
														if(Timemax!=null)
														{
															if(indexsort.compare(Timemax, Timemin) < 0)
															{
																norows = true;
																break;
															}
														}
													}
												}
												else if(name.equals("X"))
												{
													whereexpList.add(temp);
													if(temp instanceof MinorThan)
													{
														Xmin = (PrimitiveValue) temp.getLeftExpression();	
														if(Xmax!=null)
														{
															if(indexsort.compare(Xmax, Xmin) == 0)
															{
																norows = true;
																break;
															}
														}
													}
												}
												else if(name.equals("Y"))
												{
													if(temp instanceof MinorThan)
													{
														Ymin = (PrimitiveValue) temp.getLeftExpression();	
														if(Ymax!=null)
														{
															if(indexsort.compare(Ymax, Ymin) == 0)
															{
																norows = true;
																break;
															}
														}
													}
													whereexpList.add(temp);
												}
											}
										}
										e = (BinaryExpression) e.getLeftExpression();
									}
									if(e instanceof EqualsTo)
									{
										equals = e;
									}
									else if(e instanceof NotEqualsTo)
									{
										notequals = e;
									}
									else
									{
										BinaryExpression temp = e;
										if(temp.getLeftExpression() instanceof Column)
										{
											String name = ((Column)temp.getLeftExpression()).getColumnName();
											if(name.equals("EVENT_ID"))
											{												
												fullscan = false;
												if(temp instanceof MinorThanEquals)
												{
													PrimitiveValue val = (PrimitiveValue) temp.getRightExpression();
													if(Eventmax == null)
													{
														Eventmax = val;
													}
													else
													{
														if(indexsort.compare(Eventmax, val) > 0)
															Eventmax = val;
													}
													if(Eventmin!=null)
													{
														if(indexsort.compare(Eventmax, Eventmin) < 0)
														{
															norows = true;
															//break;
														}
													}
												}
												else if(temp instanceof MinorThan)
												{
													PrimitiveValue val = primaryind.lowerKey((PrimitiveValue) temp.getRightExpression());
													if(Eventmax == null)
													{
														Eventmax = val;
													}
													else
													{
														if(indexsort.compare(Eventmax, val) > 0)
															Eventmax = val;
													}
													if(Eventmin!=null)
													{
														if(indexsort.compare(Eventmax, Eventmin) < 0)
														{
															norows = true;
															//break;
														}
													}
												}
												else if(temp instanceof GreaterThan)
												{
													PrimitiveValue val = primaryind.higherKey((PrimitiveValue) temp.getRightExpression());
													if(Eventmin == null)
													{
														Eventmin = val;
													}
													else
													{
														if(indexsort.compare(Eventmin, val) < 0)
															Eventmin = val;
													}
													if(Eventmax!=null)
													{
														if(indexsort.compare(Eventmax, Eventmin) < 0)
														{
															norows = true;
															//break;
														}
													}
												}
												else if(temp instanceof GreaterThanEquals)
												{
													PrimitiveValue val = (PrimitiveValue) temp.getRightExpression();
													if(Eventmin == null)
													{
														Eventmin = val;
													}
													else
													{
														if(indexsort.compare(Eventmin, val) < 0)
															Eventmin = val;
													}
													if(Eventmax!=null)
													{
														if(indexsort.compare(Eventmax, Eventmin) < 0)
														{
															norows = true;
															//break;
														}
													}
												}
											}
											else if(name.equals("COST"))
											{
												fullscan = false;
												if(temp instanceof MinorThanEquals)
												{
													PrimitiveValue val = (PrimitiveValue) temp.getRightExpression();
													if(Costmax == null)
													{
														Costmax = val;
													}
													else
													{
														if(indexsort.compare(Costmax, val) > 0)
															Costmax = val;
													}
													if(Costmin!=null)
													{
														if(indexsort.compare(Costmax, Costmin) < 0)
														{
															norows = true;
															//break;
														}
													}
												}
												else if(temp instanceof MinorThan)
												{
													PrimitiveValue val = COST.lowerKey((PrimitiveValue) temp.getRightExpression());
													if(Costmax == null)
													{
														Costmax = val;
													}
													else
													{
														if(indexsort.compare(Costmax, val) > 0)
															Costmax = val;
													}
													if(Costmin!=null)
													{
														if(indexsort.compare(Costmax, Costmin) < 0)
														{
															norows = true;
															//break;
														}
													}
												}
												else if(temp instanceof GreaterThan)
												{
													PrimitiveValue val = COST.higherKey((PrimitiveValue) temp.getRightExpression());
													if(Costmin == null)
													{
														Costmin = val;
													}
													else
													{
														if(indexsort.compare(Costmin, val) < 0)
															Costmin = val;
													}
													if(Costmax!=null)
													{
														if(indexsort.compare(Costmax, Costmin) < 0)
														{
															norows = true;
															//break;
														}
													}
												}
												else if(temp instanceof GreaterThanEquals)
												{
													PrimitiveValue val = (PrimitiveValue) temp.getRightExpression();
													if(Costmin == null)
													{
														Costmin = val;
													}
													else
													{
														if(indexsort.compare(Costmin, val) < 0)
															Costmin = val;
													}
													if(Costmax!=null)
													{
														if(indexsort.compare(Costmax, Costmin) < 0)
														{
															norows = true;
															//break;
														}
													}
												}
											}
											else if(name.equals("TIME"))
											{
												fullscan = false;
												if(temp instanceof MinorThanEquals)
												{
													PrimitiveValue val = (PrimitiveValue) temp.getRightExpression();
													if(Timemax == null)
													{
														Timemax = val;
													}
													else
													{
														if(indexsort.compare(Timemax, val) > 0)
															Timemax = val;
													}
													if(Timemin!=null)
													{
														if(indexsort.compare(Timemax, Timemin) < 0)
														{
															norows = true;
															//break;
														}
													}
												}
												else if(temp instanceof MinorThan)
												{
													PrimitiveValue val = TIME.lowerKey((PrimitiveValue) temp.getRightExpression());
													if(Timemax == null)
													{
														Timemax = val;
													}
													else
													{
														if(indexsort.compare(Timemax, val) > 0)
															Timemax = val;
													}
													if(Timemin!=null)
													{
														if(indexsort.compare(Timemax, Timemin) < 0)
														{
															norows = true;
															//break;
														}
													}
												}
												else if(temp instanceof GreaterThan)
												{
													PrimitiveValue val = TIME.higherKey((PrimitiveValue) temp.getRightExpression());
													if(Timemin == null)
													{
														Timemin = val;
													}
													else
													{
														if(indexsort.compare(Timemin, val) < 0)
															Timemin = val;
													}
													if(Timemax!=null)
													{
														if(indexsort.compare(Timemax, Timemin) < 0)
														{
															norows = true;
															//break;
														}
													}
												}
												else if(temp instanceof GreaterThanEquals)
												{
													PrimitiveValue val = (PrimitiveValue) temp.getRightExpression();
													if(Timemin == null)
													{
														Timemin = val;
													}
													else
													{
														if(indexsort.compare(Timemin, val) < 0)
															Timemin = val;
													}
													if(Timemax!=null)
													{
														if(indexsort.compare(Timemax, Timemin) < 0)
														{
															norows = true;
															//break;
														}
													}
												}
											}
											else if(name.equals("X"))
											{
												whereexpList.add(temp);
												if(temp instanceof MinorThanEquals)
												{
													Xmax = (PrimitiveValue) temp.getRightExpression();		
													if(Xmin!=null)
													{
														if(indexsort.compare(Xmax, Xmin) == 0)
														{
															norows = true;
															//break;
														}
													}
												}
											}
											else if(name.equals("Y"))
											{
												if(temp instanceof MinorThanEquals)
												{
													Ymax = (PrimitiveValue) temp.getRightExpression();	
													if(Ymin!=null)
													{
														if(indexsort.compare(Ymax, Ymin) == 0)
														{
															norows = true;
															//break;
														}
													}
												}
												whereexpList.add(temp);
											}
										}	
										else if(temp.getRightExpression() instanceof Column)
										{
											String name = ((Column)temp.getRightExpression()).getColumnName();
											if(name.equals("EVENT_ID"))
											{												
												fullscan = false;
												if(temp instanceof GreaterThanEquals)
												{
													PrimitiveValue val = (PrimitiveValue) temp.getLeftExpression();
													if(Eventmax == null)
													{
														Eventmax = val;
													}
													else
													{
														if(indexsort.compare(Eventmax, val) > 0)
															Eventmax = val;
													}
													if(Eventmin!=null)
													{
														if(indexsort.compare(Eventmax, Eventmin) < 0)
														{
															norows = true;
															//break;
														}
													}
												}
												else if(temp instanceof GreaterThan)
												{
													PrimitiveValue val = primaryind.lowerKey((PrimitiveValue) temp.getLeftExpression());
													if(Eventmax == null)
													{
														Eventmax = val;
													}
													else
													{
														if(indexsort.compare(Eventmax, val) > 0)
															Eventmax = val;
													}
													if(Eventmin!=null)
													{
														if(indexsort.compare(Eventmax, Eventmin) < 0)
														{
															norows = true;
															//break;
														}
													}
												}
												else if(temp instanceof MinorThan)
												{
													PrimitiveValue val = primaryind.higherKey((PrimitiveValue) temp.getLeftExpression());
													if(Eventmin == null)
													{
														Eventmin = val;
													}
													else
													{
														if(indexsort.compare(Eventmin, val) < 0)
															Eventmin = val;
													}
													if(Eventmax!=null)
													{
														if(indexsort.compare(Eventmax, Eventmin) < 0)
														{
															norows = true;
															//break;
														}
													}
												}
												else if(temp instanceof MinorThanEquals)
												{
													PrimitiveValue val = (PrimitiveValue) temp.getLeftExpression();
													if(Eventmin == null)
													{
														Eventmin = val;
													}
													else
													{
														if(indexsort.compare(Eventmin, val) < 0)
															Eventmin = val;
													}
													if(Eventmax!=null)
													{
														if(indexsort.compare(Eventmax, Eventmin) < 0)
														{
															norows = true;
															//break;
														}
													}
												}
											}
											else if(name.equals("COST"))
											{
												fullscan = false;
												if(temp instanceof GreaterThanEquals)
												{
													PrimitiveValue val = (PrimitiveValue) temp.getLeftExpression();
													if(Costmax == null)
													{
														Costmax = val;
													}
													else
													{
														if(indexsort.compare(Costmax, val) > 0)
															Costmax = val;
													}
													if(Costmin!=null)
													{
														if(indexsort.compare(Costmax, Costmin) < 0)
														{
															norows = true;
															//break;
														}
													}
												}
												else if(temp instanceof GreaterThan)
												{
													PrimitiveValue val = COST.lowerKey((PrimitiveValue) temp.getLeftExpression());
													if(Costmax == null)
													{
														Costmax = val;
													}
													else
													{
														if(indexsort.compare(Costmax, val) > 0)
															Costmax = val;
													}
													if(Costmin!=null)
													{
														if(indexsort.compare(Costmax, Costmin) < 0)
														{
															norows = true;
															//break;
														}
													}
												}
												else if(temp instanceof MinorThan)
												{
													PrimitiveValue val = COST.higherKey((PrimitiveValue) temp.getLeftExpression());
													if(Costmin == null)
													{
														Costmin = val;
													}
													else
													{
														if(indexsort.compare(Costmin, val) < 0)
															Costmin = val;
													}
													if(Costmax!=null)
													{
														if(indexsort.compare(Costmax, Costmin) < 0)
														{
															norows = true;
															//break;
														}
													}
												}
												else if(temp instanceof MinorThanEquals)
												{
													PrimitiveValue val = (PrimitiveValue) temp.getLeftExpression();
													if(Costmin == null)
													{
														Costmin = val;
													}
													else
													{
														if(indexsort.compare(Costmin, val) < 0)
															Costmin = val;
													}
													if(Costmax!=null)
													{
														if(indexsort.compare(Costmax, Costmin) < 0)
														{
															norows = true;
															//break;
														}
													}
												}
											}
											else if(name.equals("TIME"))
											{
												fullscan = false;
												if(temp instanceof GreaterThanEquals)
												{
													PrimitiveValue val = (PrimitiveValue) temp.getLeftExpression();
													if(Timemax == null)
													{
														Timemax = val;
													}
													else
													{
														if(indexsort.compare(Timemax, val) > 0)
															Timemax = val;
													}
													if(Timemin!=null)
													{
														if(indexsort.compare(Timemax, Timemin) < 0)
														{
															norows = true;
															//break;
														}
													}
												}
												else if(temp instanceof GreaterThan)
												{
													PrimitiveValue val = TIME.lowerKey((PrimitiveValue) temp.getLeftExpression());
													if(Timemax == null)
													{
														Timemax = val;
													}
													else
													{
														if(indexsort.compare(Timemax, val) > 0)
															Timemax = val;
													}
													if(Timemin!=null)
													{
														if(indexsort.compare(Timemax, Timemin) < 0)
														{
															norows = true;
															//break;
														}
													}
												}
												else if(temp instanceof MinorThan)
												{
													PrimitiveValue val = TIME.higherKey((PrimitiveValue) temp.getLeftExpression());
													if(Timemin == null)
													{
														Timemin = val;
													}
													else
													{
														if(indexsort.compare(Timemin, val) < 0)
															Timemin = val;
													}
													if(Timemax!=null)
													{
														if(indexsort.compare(Timemax, Timemin) < 0)
														{
															norows = true;
															//break;
														}
													}
												}
												else if(temp instanceof MinorThanEquals)
												{
													PrimitiveValue val = (PrimitiveValue) temp.getLeftExpression();
													if(Timemin == null)
													{
														Timemin = val;
													}
													else
													{
														if(indexsort.compare(Timemin, val) < 0)
															Timemin = val;
													}
													if(Timemax!=null)
													{
														if(indexsort.compare(Timemax, Timemin) < 0)
														{
															norows = true;
															//break;
														}
													}
												}
											}
											else if(name.equals("X"))
											{
												whereexpList.add(temp);
												if(temp instanceof MinorThan)
												{
													Xmin = (PrimitiveValue) temp.getLeftExpression();	
													if(Xmax!=null)
													{
														if(indexsort.compare(Xmax, Xmin) == 0)
														{
															norows = true;
															//break;
														}
													}
												}
											}
											else if(name.equals("Y"))
											{
												if(temp instanceof MinorThan)
												{
													Ymin = (PrimitiveValue) temp.getLeftExpression();	
													if(Ymax!=null)
													{
														if(indexsort.compare(Ymax, Ymin) == 0)
														{
															norows = true;
															//break;
														}
													}
												}
												whereexpList.add(temp);
											}
										}
									}
								}

								if(norows)
									break;

								if(select.getLimit() != null)
									limit = (int) select.getLimit().getRowCount();

								if(select.getGroupByColumnReferences() != null)
									groupby = select.getGroupByColumnReferences();

								if(select.getOrderByElements() != null)
								{
									orderby = select.getOrderByElements();
									if(orderby.size() == 1)
									{
										if(primaryindname.equals(((Column)orderby.get(0).getExpression()).getColumnName()))
											orderby = null;
									}
								}
								if(select.getFromItem() instanceof Table)
									break;
								else
									select = (PlainSelect)((SubSelect)select.getFromItem()).getSelectBody();
							}
						}

						if(norows)
						{
							/*StringBuilder print = new StringBuilder();
							for(int i=0;i<select.getSelectItems().size()-1;i++)
								print.append("|");
							System.out.print(print.toString()+"\n");*/
							//long endtime = System.currentTimeMillis();
							//totaltime = totaltime + (endtime-starttime);
							//System.out.println(starttime);
							//System.out.println(endtime);
							//System.out.println(totaltime/1000);
							continue;
						}
						Row evalrow = new Row(tblschema.getColumns());
						TableSchema tbl = tblschema;
						List<SelectItem> selectclauses = select.getSelectItems();
						Eval eval = new Eval() 
						{
							public PrimitiveValue eval(Column c)
							{ 
								int i = tbl.getColnum().get(c.getColumnName());
								PrimitiveValue result = evalrow.getColdata()[i];
								return result;
							}
						};

						boolean function = false;
						List<Expression> funcexp = new ArrayList();
						ArrayList<Integer> funclist = new ArrayList();
						PrimitiveValue functionouput[] = null;
						int c =-1;
						for(SelectItem selectclause : selectclauses)
						{
							c++;
							if(selectclause.toString().equals("*"))
								continue;
							if(((SelectExpressionItem)selectclause).getExpression() instanceof Function)
							{
								function = true;

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
									if(tcount == -1)
										tcount = c;
									else
										tcount1 = c;
									break;
								case SUM:
									funclist.add(3);
									funcexp.add(((Function)((SelectExpressionItem)selectclause).getExpression()).getParameters().getExpressions().get(0));
									break;
								case AVG:
									break;
								}
							}
						}

						functionouput = new PrimitiveValue[funcexp.size()];

						int start = 0;
						int end = filedata.length;

						if((orderby!=null)&&(orderby.size() == 2)&&(groupby == null)&&(limit > 0)&&(whereexpList.isEmpty())&&(!function))
						{
							Row[] processing = null;
							OrderByElement one = orderby.get(0);
							String onename = ((Column)one.getExpression()).getColumnName();
							OrderByElement two = orderby.get(1);
							String twoname = ((Column)two.getExpression()).getColumnName();
							if(onename.equals("EVENT_ID"))
							{
								if(one.isAsc())
								{
									if(twoname.equals("TIME"))
									{
										if(two.isAsc())
										{
											processing = idata;
										}
										else
										{
											processing = idatd;
										}
									}
									else
									{
										if(two.isAsc())
										{
											processing = idaca;
										}
										else
										{
											processing = idacd;
										}
									}
								}
								else
								{
									if(twoname.equals("TIME"))
									{
										if(two.isAsc())
										{
											processing = iddta;
										}
										else
										{
											processing = iddtd;
										}
									}
									else
									{
										if(two.isAsc())
										{
											processing = iddca;
										}
										else
										{
											processing = iddcd;
										}
									}
								}
							}
							else if(onename.equals("COST"))
							{
								if(one.isAsc())
								{
									if(twoname.equals("TIME"))
									{
										if(two.isAsc())
										{
											processing = cata;
										}
										else
										{
											processing = catd;
										}
									}
									else
									{
										if(two.isAsc())
										{
											processing = caida;
										}
										else
										{
											processing = caidd;
										}
									}
								}
								else
								{
									if(twoname.equals("TIME"))
									{
										if(two.isAsc())
										{
											processing = cdta;
										}
										else
										{
											processing = cdtd;
										}
									}
									else
									{
										if(two.isAsc())
										{
											processing = cdida;
										}
										else
										{
											processing = cdidd;
										}
									}
								}
							}
							else if(onename.equals("TIME"))
							{
								if(one.isAsc())
								{
									if(twoname.equals("COST"))
									{
										if(two.isAsc())
										{
											processing = taca;
										}
										else
										{
											processing = tacd;
										}
									}
									else
									{
										if(two.isAsc())
										{
											processing = taida;
										}
										else
										{
											processing = taidd;
										}
									}
								}
								else
								{
									if(twoname.equals("COST"))
									{
										if(two.isAsc())
										{
											processing = tdca;
										}
										else
										{
											processing = tdcd;
										}
									}
									else
									{
										if(two.isAsc())
										{
											processing = tdida;
										}
										else
										{
											processing = tdidd;
										}
									}
								}
							}
							if(processing!=null)
							{
								int count = 0;
								for(int i = 0;i<30;i++)
								{
									evalrow.setColdata(processing[i].getColdata());
									StringBuilder print = new StringBuilder();
									for(SelectItem selectclause : selectclauses)
									{
										print.append(eval.eval(((SelectExpressionItem)selectclause).getExpression()).toString());
										print.append("|");
									}
									print.deleteCharAt(print.length()-1);
									System.out.print(print.toString()+"\n");

									count++;
									if((limit > 0)&&(limit == count))
										break;
								}
								//long endtime = System.currentTimeMillis();
								//totaltime = totaltime + (endtime-starttime);
								//System.out.println(starttime);
								//System.out.println(endtime);
								//System.out.println(totaltime/1000);
								continue;
							}
						}

						if(equals!= null)
						{
							if(((Column)equals.getLeftExpression()).getColumnName().equals("EVENT_ID"))
							{
								int i = primaryind.get((PrimitiveValue) equals.getRightExpression());
								if(i > -1)
								{
									evalrow.setColdata(filedata[i].getColdata());
									boolean fail =false;
									for(BinaryExpression exp:whereexpList)
									{
										if(!eval.eval(exp).toBool())
										{
											fail = true;
											break;
										}
									}
									if(fail)
										continue;

									if(function)
									{					
										StringBuilder print = new StringBuilder();
										for(int funcCnt=0;funcCnt<funclist.size();funcCnt++)
										{
											if(funclist.get(funcCnt) == 0)
											{
												print.append(eval.eval(funcexp.get(funcCnt)));
												print.append("|");
											}
											else if(funclist.get(funcCnt) == 1)
											{
												print.append(eval.eval(funcexp.get(funcCnt)));
												print.append("|");
											}
											else if(funclist.get(funcCnt) == 2)
											{
												print.append("1|");
											}
											else if(funclist.get(funcCnt) == 3)
											{
												print.append(eval.eval(funcexp.get(funcCnt)));
												print.append("|");
											}
										}
										print.deleteCharAt(print.length()-1);
										System.out.print(print.toString()+"\n");
									}
									else
									{
										StringBuilder print = new StringBuilder();
										for(SelectItem selectclause : selectclauses)
										{
											print.append(eval.eval(((SelectExpressionItem)selectclause).getExpression()).toString());
											print.append("|");
										}
										print.deleteCharAt(print.length()-1);
										System.out.print(print.toString()+"\n");
									}
								}
								//long endtime = System.currentTimeMillis();
								//totaltime = totaltime + (endtime-starttime);
								//System.out.println(starttime);
								//System.out.println(endtime);
								//System.out.println(totaltime/1000);
								continue;

							}
							else if(((Column)equals.getLeftExpression()).getColumnName().equals("COMMENT"))
							{								
								Integer i = comments.get((PrimitiveValue) equals.getRightExpression());
								if(i != null)
								{
									evalrow.setColdata(filedata[i].getColdata());
									boolean fail =false;
									for(BinaryExpression exp:whereexpList)
									{
										if(!eval.eval(exp).toBool())
										{
											fail = true;
											break;
										}
									}
									if(fail)
									{
										StringBuilder print = new StringBuilder();
										for(int j=0;j<tblschema.getColumns();j++)
										{
											if((j==tcount)||(j==tcount1))
												print.append("0|");
											else
												print.append("|");
										}
										print.deleteCharAt(print.length()-1);
										System.out.print(print.toString()+"\n");
										continue;
									}

									if(function)
									{					
										StringBuilder print = new StringBuilder();
										for(int funcCnt=0;funcCnt<funclist.size();funcCnt++)
										{
											if(funclist.get(funcCnt) == 0)
											{
												print.append(eval.eval(funcexp.get(funcCnt)));
												print.append("|");
											}
											else if(funclist.get(funcCnt) == 1)
											{
												print.append(eval.eval(funcexp.get(funcCnt)));
												print.append("|");
											}
											else if(funclist.get(funcCnt) == 2)
											{
												print.append("1|");
											}
											else if(funclist.get(funcCnt) == 3)
											{
												print.append(eval.eval(funcexp.get(funcCnt)));
												print.append("|");
											}
										}
										print.deleteCharAt(print.length()-1);
										System.out.print(print.toString()+"\n");
									}
									else
									{
										StringBuilder print = new StringBuilder();
										for(SelectItem selectclause : selectclauses)
										{
											print.append(eval.eval(((SelectExpressionItem)selectclause).getExpression()).toString());
											print.append("|");
										}
										print.deleteCharAt(print.length()-1);
										System.out.print(print.toString()+"\n");
									}
								}
								else if (tcount>-1)
								{
									StringBuilder print = new StringBuilder();
									for(int j=0;j<select.getSelectItems().size();j++)
									{
										if((j==tcount)||(j==tcount1))
											print.append("0|");
										else
											print.append("|");
									}
									print.deleteCharAt(print.length()-1);
									System.out.print(print.toString()+"\n");
								}
								//long endtime = System.currentTimeMillis();
								//totaltime = totaltime + (endtime-starttime);
								//System.out.println(starttime);
								//System.out.println(endtime);
								//System.out.println(totaltime/1000);
								continue;
							}

						}

						PrimitiveValue[] notequalval = null;
						if(notequals!=null)
						{
							if(notequals.getLeftExpression() instanceof Column)
							{
								String name = ((Column)notequals.getLeftExpression()).getColumnName();
								PrimitiveValue val = (PrimitiveValue) notequals.getRightExpression();
								if(name.equals("EVENT_ID"))
								{
									notequalval = new PrimitiveValue[1];
									notequalval[0] = val;
								}
								else if(name.equals("COST"))
								{
									if(COST.get(val)!=null)
									{
										notequalval = COST.get(val);
									}
								}
								else if(name.equals("TIME"))
								{
									if(TIME.get(val)!=null)
									{
										notequalval = TIME.get(val);
									}
								}
								else
								{
									whereexpList.add(notequals);
								}
							}
							if(notequals.getRightExpression() instanceof Column)
							{
								String name = ((Column)notequals.getRightExpression()).getColumnName();
								PrimitiveValue val = (PrimitiveValue) notequals.getLeftExpression();
								if(name.equals("EVENT_ID"))
								{
									notequalval = new PrimitiveValue[1];
									notequalval[0] = val;
								}
								else if(name.equals("COST"))
								{
									if(COST.get(val)!=null)
									{
										notequalval = COST.get(val);
									}
								}
								else if(name.equals("TIME"))
								{
									if(TIME.get(val)!=null)
									{
										notequalval = TIME.get(val);
									}
								}
								else
								{
									whereexpList.add(notequals);
								}
							}
						}

						if(fullscan)
						{
							if(orderby !=null)
							{
								int[] orderseq = new int[orderby.size()];
								boolean[] order = new boolean[orderby.size()];
								for(int i=0;i<orderby.size();i++)
								{
									orderseq[i]=tblschema.getColnum().get(((Column)orderby.get(i).getExpression()).getColumnName());
									order[i] = orderby.get(i).isAsc();
								}
								Arrays.sort(filedata, new OrderbyComparator(orderseq,order));
							}
						}
						else
						{

							if((Eventmin!=null)&&(Eventmax!=null))
							{
								SortedMap<PrimitiveValue, Integer> rangetemp = primaryind.subMap(Eventmin,Eventmax);
								fetch.addAll(rangetemp.keySet());
								if(primaryind.get(Eventmax)!=null)
									fetch.add(Eventmax);
							}
							else if(Eventmin!=null)
							{
								SortedMap<PrimitiveValue, Integer> rangetemp = primaryind.tailMap(Eventmin);
								fetch.addAll(rangetemp.keySet());
							}
							else if(Eventmax!=null)
							{
								SortedMap<PrimitiveValue, Integer> rangetemp = primaryind.headMap(Eventmax);
								fetch.addAll(rangetemp.keySet());
								if(primaryind.get(Eventmax)!=null)
									fetch.add(Eventmax);
							}
							if((Timemin!=null)&&(Timemax!=null))
							{
								if(fetch.isEmpty())
								{
									SortedMap<PrimitiveValue, PrimitiveValue[]> rangetemp = TIME.subMap(Timemin,Timemax);
									Iterator<PrimitiveValue[]> itr = rangetemp.values().iterator();
									while(itr.hasNext())
									{
										fetch.addAll(Arrays.asList(itr.next()));
									}
									if(TIME.get(Timemax)!=null)
										fetch.addAll(Arrays.asList(TIME.get(Timemax)));
								}
								else
								{
									SortedMap<PrimitiveValue, PrimitiveValue[]> rangetemp = TIME.subMap(Timemin,Timemax);
									HashSet<PrimitiveValue> tempfetch= new HashSet<PrimitiveValue>();
									Iterator<PrimitiveValue[]> itr = rangetemp.values().iterator();
									while(itr.hasNext())
									{
										tempfetch.addAll(Arrays.asList(itr.next()));
									}
									if(TIME.get(Timemax)!=null)
										tempfetch.addAll(Arrays.asList(TIME.get(Timemax)));
									fetch.retainAll(tempfetch);
								}	
							}
							else if(Timemin!=null)
							{
								if(fetch.isEmpty())
								{
									SortedMap<PrimitiveValue, PrimitiveValue[]> rangetemp = TIME.tailMap(Timemin);
									Iterator<PrimitiveValue[]> itr = rangetemp.values().iterator();
									while(itr.hasNext())
									{
										fetch.addAll(Arrays.asList(itr.next()));
									}
								}
								else
								{
									SortedMap<PrimitiveValue, PrimitiveValue[]> rangetemp = TIME.tailMap(Timemin);
									HashSet<PrimitiveValue> tempfetch= new HashSet<PrimitiveValue>();
									Iterator<PrimitiveValue[]> itr = rangetemp.values().iterator();
									while(itr.hasNext())
									{
										tempfetch.addAll(Arrays.asList(itr.next()));
									}
									fetch.retainAll(tempfetch);
								}
							}
							else if(Timemax!=null)
							{
								if(fetch.isEmpty())
								{
									SortedMap<PrimitiveValue, PrimitiveValue[]> rangetemp = TIME.headMap(Timemax);
									Iterator<PrimitiveValue[]> itr = rangetemp.values().iterator();
									while(itr.hasNext())
									{
										fetch.addAll(Arrays.asList(itr.next()));
									}
									if(TIME.get(Timemax)!=null)
										fetch.addAll(Arrays.asList(TIME.get(Timemax)));
								}
								else
								{
									SortedMap<PrimitiveValue, PrimitiveValue[]> rangetemp = TIME.headMap(Timemax);
									HashSet<PrimitiveValue> tempfetch= new HashSet<PrimitiveValue>();
									Iterator<PrimitiveValue[]> itr = rangetemp.values().iterator();
									while(itr.hasNext())
									{
										tempfetch.addAll(Arrays.asList(itr.next()));
									}
									if(TIME.get(Timemax)!=null)
										tempfetch.addAll(Arrays.asList(TIME.get(Timemax)));
									fetch.retainAll(tempfetch);
								}	
							}
							if((Costmin!=null)&&(Costmax!=null))
							{
								if(fetch.isEmpty())
								{
									SortedMap<PrimitiveValue, PrimitiveValue[]> rangetemp = COST.subMap(Costmin,Costmax);
									Iterator<PrimitiveValue[]> itr = rangetemp.values().iterator();
									while(itr.hasNext())
									{
										fetch.addAll(Arrays.asList(itr.next()));
									}
									if(COST.get(Costmax)!=null)
										fetch.addAll(Arrays.asList(COST.get(Costmax)));
								}
								else
								{
									SortedMap<PrimitiveValue, PrimitiveValue[]> rangetemp = COST.subMap(Costmin,Costmax);
									HashSet<PrimitiveValue> tempfetch= new HashSet<PrimitiveValue>();
									Iterator<PrimitiveValue[]> itr = rangetemp.values().iterator();
									while(itr.hasNext())
									{
										tempfetch.addAll(Arrays.asList(itr.next()));
									}
									if(COST.get(Costmax)!=null)
										tempfetch.addAll(Arrays.asList(COST.get(Costmax)));
									fetch.retainAll(tempfetch);
								}	
							}
							else if(Costmin!=null)
							{
								if(fetch.isEmpty())
								{
									SortedMap<PrimitiveValue, PrimitiveValue[]> rangetemp = COST.tailMap(Costmin);
									Iterator<PrimitiveValue[]> itr = rangetemp.values().iterator();
									while(itr.hasNext())
									{
										fetch.addAll(Arrays.asList(itr.next()));
									}
								}
								else
								{
									SortedMap<PrimitiveValue, PrimitiveValue[]> rangetemp = COST.tailMap(Costmin);
									HashSet<PrimitiveValue> tempfetch= new HashSet<PrimitiveValue>();
									Iterator<PrimitiveValue[]> itr = rangetemp.values().iterator();
									while(itr.hasNext())
									{
										tempfetch.addAll(Arrays.asList(itr.next()));
									}
									fetch.retainAll(tempfetch);
								}
							}
							else if(Costmax!=null)
							{
								if(fetch.isEmpty())
								{
									SortedMap<PrimitiveValue, PrimitiveValue[]> rangetemp = COST.headMap(Costmax);
									Iterator<PrimitiveValue[]> itr = rangetemp.values().iterator();
									while(itr.hasNext())
									{
										fetch.addAll(Arrays.asList(itr.next()));
									}
									if(COST.get(Costmax)!=null)
										fetch.addAll(Arrays.asList(COST.get(Costmax)));
								}
								else
								{
									SortedMap<PrimitiveValue, PrimitiveValue[]> rangetemp = COST.headMap(Costmax);
									HashSet<PrimitiveValue> tempfetch= new HashSet<PrimitiveValue>();
									Iterator<PrimitiveValue[]> itr = rangetemp.values().iterator();
									while(itr.hasNext())
									{
										tempfetch.addAll(Arrays.asList(itr.next()));
									}
									if(COST.get(Costmax)!=null)
										tempfetch.addAll(Arrays.asList(COST.get(Costmax)));
									fetch.retainAll(tempfetch);
								}	
							}

							if(notequalval != null)
								fetch.removeAll(Arrays.asList(notequalval));
							/*if((orderby !=null)&&(groupby == null))
								{
									start = primaryind.get(Collections.min(fetch,indexsort));
									PrimitiveValue high = Collections.max(fetch,indexsort);
									Entry<PrimitiveValue, Integer> tempend = primaryind.higherEntry(high);
									if(tempend != null)
									{
										PrimitiveValue endkey = tempend.getKey();
										end = primaryind.get(endkey);
									}
									int[] orderseq = new int[orderby.size()];
									boolean[] order = new boolean[orderby.size()];
									for(int i=0;i<orderby.size();i++)
									{
										orderseq[i]=tblschema.getColnum().get(((Column)orderby.get(i).getExpression()).getColumnName());
										order[i] = orderby.get(i).isAsc();
									}
									Arrays.sort(filedata, start, end,  new OrderbyComparator(orderseq,order));
								}*/
							if(fetch.isEmpty())
							{
								String print = "";
								for(int i=0;i<selectclauses.size();i++)
									print = print+"|";
								System.out.println(print);
								continue;
							}
						}
						List<SelectItem> selection = null;
						while(!selectitems.isEmpty())
						{
							selection = selectitems.pop();
							boolean output = false;
							if(selectitems.isEmpty())
								output = true;
							if(!output)
							{
								if(selection.toString().equals("[*]"))
									continue;
							}
							int count = 0;
							if(selection.toString().equals("[*]"))
							{
								if(fullscan)
								{
									for(int i = start;i<end;i++)
									{
										//evalrow = new Row(tblschema.getColumns());
										evalrow.setColdata(filedata[i].getColdata());
										boolean fail =false;
										for(BinaryExpression exp:whereexpList)
										{
											if(!eval.eval(exp).toBool())
											{
												fail = true;
											}
										}
										if(fail)
											continue;

										count++;
										StringBuilder print = new StringBuilder();
										for(PrimitiveValue selectclause : evalrow.getColdata())
										{
											print.append(selectclause.toString());
											print.append("|");
										}
										print.deleteCharAt(print.length()-1);
										System.out.print(print.toString()+"\n");
										if((limit > 0)&&(limit == count))
											break;
									}
								}
								else
								{
									Iterator<PrimitiveValue> it = fetch.iterator();
									while(it.hasNext())
									{
										evalrow.setColdata(filedata[primaryind.get(it.next())].getColdata());
										boolean fail =false;
										for(BinaryExpression exp:whereexpList)
										{
											if(!eval.eval(exp).toBool())
											{
												fail = true;
											}
										}
										if(fail)
											continue;

										count++;
										StringBuilder print = new StringBuilder();
										for(PrimitiveValue selectclause : evalrow.getColdata())
										{
											print.append(selectclause.toString());
											print.append("|");
										}
										print.deleteCharAt(print.length()-1);
										System.out.print(print.toString()+"\n");
										if((limit > 0)&&(limit == count))
											break;
									}
								}
							}
							else
							{
								HashMap<ArrayList<PrimitiveValue>,PrimitiveValue[]> groupbyprint = new HashMap<ArrayList<PrimitiveValue>,PrimitiveValue[]>();
								ArrayList<PrimitiveValue> groupbyvalues = new ArrayList<PrimitiveValue>();
								int groupbyind[] = null;
								if(groupby !=null)
								{
									groupbyind = new int[groupby.size()];
									for(int j = 0;j<groupby.size();j++)
									{
										groupbyind[j]= tblschema.getColnum().get(groupby.get(j).getColumnName());
									}
								}
								if(fullscan)
								{
									for(int i = start;i<end;i++)
									{
										//evalrow = new Row(tblschema.getColumns());
										evalrow.setColdata(filedata[i].getColdata());
										boolean fail =false;
										for(BinaryExpression exp:whereexpList)
										{
											if(!eval.eval(exp).toBool())
											{
												fail = true;
												break;
											}
										}
										if(fail)
											continue;

										if(function)
										{					
											if(groupby != null)
											{
												for(int j : groupbyind)
												{
													groupbyvalues.add(evalrow.getColdata()[j]);
												}
												if(groupbyprint.get(groupbyvalues)!=null)	
												{
													functionouput = groupbyprint.get(groupbyvalues);
												}
												else
												{
													functionouput = new PrimitiveValue[funcexp.size()];
												}
											}
											for(int funcCnt=0;funcCnt<funclist.size();funcCnt++)
											{
												if(funclist.get(funcCnt) == 0)
												{
													PrimitiveValue temp = eval.eval(funcexp.get(funcCnt));
													if(functionouput[funcCnt] == null)
														functionouput[funcCnt] = temp;
													//else
													//{
													/*if(functionouput[funcCnt] instanceof LongValue)
													{
														if(functionouput[funcCnt].toLong() < temp.toLong())
															functionouput[funcCnt] = temp;
													}
													if(functionouput[funcCnt] instanceof DoubleValue)
													{*/
													else if(functionouput[funcCnt].toDouble() < temp.toDouble())
														functionouput[funcCnt] = temp;
													/*}
													if(functionouput[funcCnt] instanceof StringValue)
													{
														if(functionouput[funcCnt].toString().compareTo(temp.toString()) < 0)
															functionouput[funcCnt] = temp;
													}
													if(functionouput[funcCnt] instanceof DateValue)
													{
														if(((DateValue)functionouput[funcCnt]).getValue().compareTo(((DateValue)temp).getValue()) < 0 )
															functionouput[funcCnt] = temp;
													}*/
													//}
												}
												if(funclist.get(funcCnt) == 1)
												{
													PrimitiveValue temp = eval.eval(funcexp.get(funcCnt));
													if(functionouput[funcCnt] == null)
														functionouput[funcCnt] = temp;
													//else
													//{
													/*if(functionouput[funcCnt] instanceof LongValue)
													{
														if(functionouput[funcCnt].toLong() > temp.toLong())
															functionouput[funcCnt] = temp;
													}
													if(functionouput[funcCnt] instanceof DoubleValue)
													{*/
													else if(functionouput[funcCnt].toDouble() > temp.toDouble())
														functionouput[funcCnt] = temp;
													/*}
													if(functionouput[funcCnt] instanceof StringValue)
													{
														if(functionouput[funcCnt].toString().compareTo(temp.toString()) > 0)
															functionouput[funcCnt] = temp;
													}
													if(functionouput[funcCnt] instanceof DateValue)
													{
														if(((DateValue)functionouput[funcCnt]).getValue().compareTo(((DateValue)temp).getValue()) > 0 )
															functionouput[funcCnt] = temp;
													}*/
													//}
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
														//if(temp instanceof LongValue)
														//functionouput[funcCnt] = new LongValue(functionouput[funcCnt].toLong() + temp.toLong());
														//if(temp instanceof DoubleValue)
														functionouput[funcCnt] = new DoubleValue(functionouput[funcCnt].toDouble() + temp.toDouble());								
													}
												}
											}
											if(groupby != null)
											{
												groupbyprint.put(groupbyvalues,functionouput);
												groupbyvalues = null;
												groupbyvalues = new ArrayList<PrimitiveValue>();
											}
										}
										else
										{
											StringBuilder print = new StringBuilder();
											for(SelectItem selectclause : selectclauses)
											{
												print.append(eval.eval(((SelectExpressionItem)selectclause).getExpression()).toString());
												print.append("|");
											}
											print.deleteCharAt(print.length()-1);
											System.out.print(print.toString()+"\n");

											count++;
											if((limit > 0)&&(limit == count))
												break;
										}
									}
								}
								else
								{
									if((orderby !=null)&&(groupby == null)&&(fetch.size()>1))
									{
										ArrayList<Row> ordertemp = new ArrayList<Row>();
										Iterator<PrimitiveValue> it = fetch.iterator();
										while(it.hasNext())
										{
											Row temp = filedata[primaryind.get(it.next())];
											//evalrow = new Row(tblschema.getColumns());
											evalrow.setColdata(temp.getColdata());
											boolean fail =false;
											for(BinaryExpression exp:whereexpList)
											{
												if(!eval.eval(exp).toBool())
												{
													fail = true;
													break;
												}
											}
											if(fail)
												continue;

											ordertemp.add(temp);
										}
										for(Row temp: ordertemp)
										{
											evalrow.setColdata(temp.getColdata());
											StringBuilder print = new StringBuilder();
											for(SelectItem selectclause : selectclauses)
											{
												print.append(eval.eval(((SelectExpressionItem)selectclause).getExpression()).toString());
												print.append("|");
											}
											print.deleteCharAt(print.length()-1);
											System.out.print(print.toString()+"\n");

											count++;
											if((limit > 0)&&(limit == count))
												break;
										}

									}
									else
									{
										//start = 0;
										//end =fetch.size();
										Iterator<PrimitiveValue> it = fetch.iterator();
										while(it.hasNext())
										{
											//evalrow = new Row(tblschema.getColumns());
											evalrow.setColdata(filedata[primaryind.get(it.next())].getColdata());
											boolean fail =false;
											for(BinaryExpression exp:whereexpList)
											{
												if(!eval.eval(exp).toBool())
												{
													fail = true;
													break;
												}
											}
											if(fail)
												continue;

											if(function)
											{					
												if(groupby != null)
												{
													for(int j : groupbyind)
													{
														groupbyvalues.add(evalrow.getColdata()[j]);
													}
													if(groupbyprint.get(groupbyvalues)!=null)	
													{
														functionouput = groupbyprint.get(groupbyvalues);
													}
													else
													{
														functionouput = new PrimitiveValue[funcexp.size()];
													}
												}
												for(int funcCnt=0;funcCnt<funclist.size();funcCnt++)
												{
													if(funclist.get(funcCnt) == 0)
													{
														PrimitiveValue temp = eval.eval(funcexp.get(funcCnt));
														if(functionouput[funcCnt] == null)
															functionouput[funcCnt] = temp;
														//else
														//{
														/*if(functionouput[funcCnt] instanceof LongValue)
														{
															if(functionouput[funcCnt].toLong() < temp.toLong())
																functionouput[funcCnt] = temp;
														}
														if(functionouput[funcCnt] instanceof DoubleValue)
														{*/
														else if(functionouput[funcCnt].toDouble() < temp.toDouble())
															functionouput[funcCnt] = temp;
														/*}
														if(functionouput[funcCnt] instanceof StringValue)
														{
															if(functionouput[funcCnt].toString().compareTo(temp.toString()) < 0)
																functionouput[funcCnt] = temp;
														}
														if(functionouput[funcCnt] instanceof DateValue)
														{
															if(((DateValue)functionouput[funcCnt]).getValue().compareTo(((DateValue)temp).getValue()) < 0 )
																functionouput[funcCnt] = temp;
														}*/
														//}
													}
													if(funclist.get(funcCnt) == 1)
													{
														PrimitiveValue temp = eval.eval(funcexp.get(funcCnt));
														if(functionouput[funcCnt] == null)
															functionouput[funcCnt] = temp;
														//else
														//{
														/*if(functionouput[funcCnt] instanceof LongValue)
														{
															if(functionouput[funcCnt].toLong() > temp.toLong())
																functionouput[funcCnt] = temp;
														}
														if(functionouput[funcCnt] instanceof DoubleValue)
														{*/
														else if(functionouput[funcCnt].toDouble() > temp.toDouble())
															functionouput[funcCnt] = temp;
														/*}
														if(functionouput[funcCnt] instanceof StringValue)
														{
															if(functionouput[funcCnt].toString().compareTo(temp.toString()) > 0)
																functionouput[funcCnt] = temp;
														}
														if(functionouput[funcCnt] instanceof DateValue)
														{
															if(((DateValue)functionouput[funcCnt]).getValue().compareTo(((DateValue)temp).getValue()) > 0 )
																functionouput[funcCnt] = temp;
														}*/
														//}
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
															//if(temp instanceof LongValue)
															//functionouput[funcCnt] = new LongValue(functionouput[funcCnt].toLong() + temp.toLong());
															//if(temp instanceof DoubleValue)
															functionouput[funcCnt] = new DoubleValue(functionouput[funcCnt].toDouble() + temp.toDouble());								
														}
													}
												}
												if(groupby != null)
												{
													groupbyprint.put(groupbyvalues,functionouput);
													groupbyvalues = null;
													groupbyvalues = new ArrayList<PrimitiveValue>();
												}
											}
											else
											{
												StringBuilder print = new StringBuilder();
												for(SelectItem selectclause : selectclauses)
												{
													print.append(eval.eval(((SelectExpressionItem)selectclause).getExpression()).toString());
													print.append("|");
												}
												print.deleteCharAt(print.length()-1);
												System.out.print(print.toString()+"\n");

												count++;
												if((limit > 0)&&(limit == count))
													break;
											}
										}
									}
								}
								if(function)
								{
									if(groupby == null)
									{
										StringBuilder print = new StringBuilder();
										for(int i = 0;i < functionouput.length;i++)
										{
											PrimitiveValue temp = functionouput[i];
											if(temp != null)
											{
												print.append(temp.toString());
											}
											else
											{
												if((i==tcount)||(i==tcount1))
													print.append("0");
											}
											print.append("|");
										}
										if(print.length()>0)
										{
											print.deleteCharAt(print.length()-1);
											System.out.print(print.toString()+"\n");
										}
									}
									else
									{
										if(orderby==null)
										{
											Iterator<Entry<ArrayList<PrimitiveValue>, PrimitiveValue[]>> it = groupbyprint.entrySet().iterator();
											while (it.hasNext()) 
											{
												StringBuilder print = new StringBuilder();
												Map.Entry pair = (Map.Entry)it.next();
												ArrayList<PrimitiveValue> name = (ArrayList<PrimitiveValue>) pair.getKey();
												PrimitiveValue[] temp1 = (PrimitiveValue[]) pair.getValue();
												for(PrimitiveValue t : name)
												{
													if(t != null)
													{
														print.append(t.toString());
													}
													print.append("|");
												}
												for(PrimitiveValue t: temp1)
												{
													if(t != null)
													{
														print.append(t.toString());
													}
													print.append("|");
												}
												if(print.length()>0)
												{
													print.deleteCharAt(print.length()-1);
													System.out.print(print.toString()+"\n");
												}
												it.remove();
											}
											if(groupbyprint.isEmpty())
											{
												StringBuilder print = new StringBuilder();
												for(int k =0;k < selectclauses.size();k++)
												{
													print.append("|");
												}
												if(print.length()>0)
												{
													print.deleteCharAt(print.length()-1);
													System.out.print(print.toString()+"\n");
												}
											}
										}
										else
										{
											ArrayList<Row> rprint = new ArrayList<Row>();
											Iterator<ArrayList<PrimitiveValue>> it = groupbyprint.keySet().iterator();
											Row r = new Row(selectclauses.size());
											while (it.hasNext()) 
											{
												ArrayList<PrimitiveValue> name = (ArrayList<PrimitiveValue>) it.next();
												r.setColdata((PrimitiveValue[]) name.toArray());
												rprint.add(r);
												it.remove();
											}
											List<OrderByElement> orderby1 = orderby;
											Collections.sort(rprint,new Comparator<Row>()
											{
												public int compare(Row o1, Row o2)
												{
													for(int i =0;i<o1.getColdata().length;i++)
													{
														int ret = 0;
														OrderByElement temp = orderby1.get(i);
														PrimitiveValue col1 = o1.getColdata()[i];
														PrimitiveValue col2 = o2.getColdata()[i];
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
														if(ret != 0)
														{
															if(temp.isAsc())
																return ret;
															else
																return -1*ret;
														}
													}
													return 0;
												}
											});
											for(Row rtmp : rprint)
											{
												StringBuilder print = new StringBuilder();
												for(PrimitiveValue t : rtmp.getColdata())
												{
													if(t != null)
													{
														print.append(t.toString());
													}
													print.append("|");
												}
												for(PrimitiveValue t: groupbyprint.get(Arrays.asList((rtmp.getColdata()))))
												{
													if(t != null)
													{
														print.append(t.toString());
													}
													print.append("|");
												}
												if(print.length()>0)
												{
													print.deleteCharAt(print.length()-1);
													System.out.print(print.toString()+"\n");
												}
											}
										}
									}
								}
							}
						}
						if((orderby !=null)&&(groupby == null))
							Arrays.sort(filedata, start, end, tablesort);

						//long endtime = System.currentTimeMillis();
						//totaltime = totaltime + (endtime-starttime);
						//System.out.println(starttime);
						//System.out.println(endtime);
						//System.out.println(totaltime);
					}
					System.gc();
				}
				if(ondisk)
				{
					if(query instanceof CreateTable)
					{
						long starttime = System.currentTimeMillis();
						tblschema = new TableSchema();
						primaryindod = new TreeMap<PrimitiveValue, String>(indexsort);
						CreateTable table = (CreateTable)query;
						tblschema.getSchema(table);
						//tables.put(table.getTable().getName(), tblschema);
						ArrayList<Row> data= new ArrayList<Row>();
						String file = "data/"+table.getTable().getName()+".csv";
						File fileRead = new File(file);
						List<Index> indexes = table.getIndexes();
						Index secondary = null;
						for(Index i : indexes)
						{
							if(i.getType().equals("PRIMARY KEY"))
							{
								primary =i;
								primaryindname = i.getColumnsNames().get(0);
							}
							else
							{
								secondary = i;
								secondaryindname = i.getColumnsNames().get(0).toUpperCase();
							}

						}

						int sortInd = tblschema.getColnum().get(secondaryindname);
						//int secsortInd = tblschema.getColnum().get(secondaryindname);
						//PrimitiveValue[] r= new PrimitiveValue[tblschema.getColumns()];
						Row row= new Row(tblschema.getColumns());	
						//row.setSortInd(secsortInd);
						StringReader input1 = new StringReader("SELECT LINEITEM.RETURNFLAG, LINEITEM.LINESTATUS, SUM(LINEITEM.QUANTITY) AS SUM_QTY, SUM(LINEITEM.EXTENDEDPRICE) AS SUM_BASE_PRICE, SUM(LINEITEM.EXTENDEDPRICE*(1-LINEITEM.DISCOUNT)) AS SUM_DISC_PRICE, SUM(LINEITEM.EXTENDEDPRICE*(1-LINEITEM.DISCOUNT)*(1+LINEITEM.TAX)) AS SUM_CHARGE, AVG(LINEITEM.QUANTITY) AS AVG_QTY, AVG(LINEITEM.EXTENDEDPRICE) AS AVG_PRICE,  AVG(LINEITEM.DISCOUNT) AS AVG_DISC, COUNT(*) AS COUNT_ORDER FROM LINEITEM WHERE LINEITEM.SHIPDATE <= DATE('1998-08-26') GROUP BY LINEITEM.RETURNFLAG, LINEITEM.LINESTATUS ORDER BY LINEITEM.RETURNFLAG, LINEITEM.LINESTATUS;");
						CCJSqlParser parser1 = new CCJSqlParser(input1);
						Statement query1 = parser1.Statement();
						StringReader input2 = new StringReader("SELECT LINEITEM.EXTENDEDPRICE*LINEITEM.DISCOUNT)AS REVENUE FROM LINEITEM;");
						CCJSqlParser parser2 = new CCJSqlParser(input2);
						Statement query2 = parser2.Statement();
						PlainSelect select2 = (PlainSelect)(((Select) query2).getSelectBody());
						SelectItem q2=  select2.getSelectItems().get(0);
						try 
						{
							BufferedReader reader = new BufferedReader(new FileReader(fileRead));
							//File File0 = new File(file);
							//LineNumberReader lineNumberReader = new LineNumberReader(new FileReader(File0));
							//lineNumberReader.skip(Long.MAX_VALUE);
							//int lines = lineNumberReader.getLineNumber();
							//ArrayList<String> filedatas = new ArrayList<String>();
							//lineNumberReader.close();
							String line;
							int count = 0;
							ArrayList<Integer> notneedlist = new ArrayList<Integer>();
							notneedlist.add(0);
							notneedlist.add(2);
							notneedlist.add(3);
							notneedlist.add(11);
							notneedlist.add(13);
							notneedlist.add(14);
							int filecount = 0;

							File file0 =  new File("data/temp0.txt");
							File file1 =  new File("data/temp1.txt");
							File file2 =  new File("data/temp2.txt");
							File file3 =  new File("data/temp3.txt");
							File file4 =  new File("data/temp4.txt");
							File file5 =  new File("data/temp5.txt");
							File file6 =  new File("data/temp6.txt");
							File file7 =  new File("data/temp7.txt");
							File file8 =  new File("data/temp8.txt");
							File file9 =  new File("data/temp9.txt");	
							file0.createNewFile();
							file1.createNewFile();
							file2.createNewFile();
							file3.createNewFile();
							file4.createNewFile();
							file5.createNewFile();
							file6.createNewFile();
							file7.createNewFile();
							file8.createNewFile();
							file9.createNewFile();
							FileWriter fileOut0 =  new FileWriter(file0);
							FileWriter fileOut1 =  new FileWriter(file1);
							FileWriter fileOut2 =  new FileWriter(file2);
							FileWriter fileOut3 =  new FileWriter(file3);
							FileWriter fileOut4 =  new FileWriter(file4);
							FileWriter fileOut5 =  new FileWriter(file5);
							FileWriter fileOut6 =  new FileWriter(file6);
							FileWriter fileOut7 =  new FileWriter(file7);
							FileWriter fileOut8 =  new FileWriter(file8);
							FileWriter fileOut9 =  new FileWriter(file9);
							BufferedWriter out0 = new BufferedWriter(fileOut0);
							BufferedWriter out1 = new BufferedWriter(fileOut1);
							BufferedWriter out2 = new BufferedWriter(fileOut2);
							BufferedWriter out3 = new BufferedWriter(fileOut3);
							BufferedWriter out4 = new BufferedWriter(fileOut4);
							BufferedWriter out5 = new BufferedWriter(fileOut5);
							BufferedWriter out6 = new BufferedWriter(fileOut6);
							BufferedWriter out7 = new BufferedWriter(fileOut7);
							BufferedWriter out8 = new BufferedWriter(fileOut8);
							BufferedWriter out9 = new BufferedWriter(fileOut9);
							String dn = "";
							while ((line = reader.readLine()) != null)
							{						
								//PrimitiveValue[] r= new PrimitiveValue[tblschema.getColumns()];
								int temp1 = 0;
								int temp2 =-1;
								int temp3 = 0;

								while((temp3 < 15)&&((temp2 = line.indexOf( '|', temp1 )) != -1))
								{
									/*if(notneedlist.contains(temp3))
								{
									temp3++;
								}
								else
								{
									String temp = line.substring( temp1, temp2 );
									//row[temp3++] = temp;
									switch(datatype.valueOf(tblschema.getDataType(temp3)))
									{
									case DECIMAL:
										evalrow.getColdata()[temp3++] = new DoubleValue(temp);
										break;							
									case INT:
										evalrow.getColdata()[temp3++] = new LongValue(temp);
										break; 
									case DATE:
										evalrow.getColdata()[temp3++] = new DateValue(temp);
										break;
									default :
										evalrow.getColdata()[temp3++] = new StringValue(temp);
									}
								}*/
									if(temp3 == 10)
									{
										dn = line.substring( temp1, temp2 ).substring(0, 4);
										break;
									}
									else
									{
										temp3++;
									}
									temp1 = temp2 + 1;								
								}
								line = line+"\n";
								if(dn.equals("1992"))
								{
									out0.write(line);
								}
								else if(dn.equals("1993"))
								{
									out1.write(line);
								}
								else if(dn.equals("1994"))
								{
									out2.write(line);
								}
								else if(dn.equals("1995"))
								{
									out3.write(line);
								}
								else if(dn.equals("1996"))
								{
									out4.write(line);
								}
								else if(dn.equals("1997"))
								{
									out5.write(line);
								}
								else if(dn.equals("1998"))
								{
									out6.write(line);
								}
								else if(dn.equals("1999"))
								{
									out7.write(line);
								}
								else 
								{
									System.out.println("You Missed Something");
								}
							}
							reader.close();	
							out0.close();
							out1.close();
							out2.close();
							out3.close();
							out4.close();
							out5.close();
							out6.close();
							out7.close();
							out8.close();
							out9.close();
							limit1992 = new PrimitiveValue[10][2];
							limit1993 = new PrimitiveValue[10][2];
							limit1994 = new PrimitiveValue[10][2];
							limit1995 = new PrimitiveValue[10][2];
							limit1996 = new PrimitiveValue[10][2];
							limit1997 = new PrimitiveValue[10][2];
							limit1998 = new PrimitiveValue[10][2];
							limit1999 = new PrimitiveValue[10][2];
							discount1992 = new HashMap<PrimitiveValue,ArrayList<PrimitiveValue[]>>();
							discount1993 = new HashMap<PrimitiveValue,ArrayList<PrimitiveValue[]>>();
							discount1994 = new HashMap<PrimitiveValue,ArrayList<PrimitiveValue[]>>();
							discount1995 = new HashMap<PrimitiveValue,ArrayList<PrimitiveValue[]>>();
							discount1996 = new HashMap<PrimitiveValue,ArrayList<PrimitiveValue[]>>();
							discount1997 = new HashMap<PrimitiveValue,ArrayList<PrimitiveValue[]>>();
							discount1998 = new HashMap<PrimitiveValue,ArrayList<PrimitiveValue[]>>();
							discount1999 = new HashMap<PrimitiveValue,ArrayList<PrimitiveValue[]>>();
							PlainSelect select = (PlainSelect)(((Select) query1).getSelectBody());
							//List<Column> groupby = select.getGroupByColumnReferences();
							List<SelectItem> selectclauses = select.getSelectItems();
							List<Expression> funcexp = new ArrayList();
							ArrayList<Integer> funclist = new ArrayList();
							PrimitiveValue functionouput[] = null;

							for(SelectItem selectclause : selectclauses)
							{
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
										break;
									case AVG:
										funclist.add(4);
										funcexp.add(((Function)((SelectExpressionItem)selectclause).getExpression()).getParameters().getExpressions().get(0));
										break;
									}
								}
							}

							//functionouput = new PrimitiveValue[funcexp.size()];
							groupbyprintod = new HashMap<String,PrimitiveValue[]>();



							Row evalrow = new Row(tblschema.getColumns());
							TableSchema tbl = tblschema;
							Eval eval = new Eval() 
							{
								public PrimitiveValue eval(Column c)
								{ 
									int i = tbl.getColnum().get(c.getColumnName());
									PrimitiveValue result = evalrow.getColdata()[i];
									return result;
								}
							};
							reader = new BufferedReader(new FileReader(file0));
							String reading = "1992";
							if(reading.equals("1992"))
							{
								ArrayList<PrimitiveValue[]> t1 = null;
								String groupbyvalues = "";
								while ((line = reader.readLine()) != null)
								{						
									row= new Row(tblschema.getColumns());
									row.setSortInd(sortInd);							
									int temp1 = 0;
									int temp2 =-1;
									int temp3 = 0;

									while((temp3 < 15)&&((temp2 = line.indexOf( '|', temp1 )) != -1))
									{
										if(notneedlist.contains(temp3))
										{
											temp3++;
										}
										else
										{
											String temp = line.substring( temp1, temp2 );
											//row[temp3++] = temp;
											switch(datatype.valueOf(tblschema.getDataType(temp3)))
											{
											case DECIMAL:
												row.getColdata()[temp3] = new DoubleValue(temp);
												evalrow.getColdata()[temp3++] = new DoubleValue(temp);
												break;							
											case INT:
												row.getColdata()[temp3] = new LongValue(temp);
												evalrow.getColdata()[temp3++] = new LongValue(temp);
												break; 
											case DATE:
												row.getColdata()[temp3] = new DateValue(temp);
												evalrow.getColdata()[temp3++] = new DateValue(temp);
												break;
											default :
												row.getColdata()[temp3] = new StringValue(temp);
												evalrow.getColdata()[temp3++] = new StringValue(temp);
											}
										}
										temp1 = temp2 + 1;								
									}
									data.add(row);
									t1 = discount1992.get(row.getColdata()[6]);
									if(t1==null)
									{
										t1 = new ArrayList<PrimitiveValue[]>();
										t1.add(new PrimitiveValue[]{row.getColdata()[4],eval.eval(((SelectExpressionItem)q2).getExpression())});
										discount1992.put(row.getColdata()[6], t1);
									}
									else
									{
										t1.add(new PrimitiveValue[]{row.getColdata()[4],eval.eval(((SelectExpressionItem)q2).getExpression())});
										discount1992.put(row.getColdata()[6], t1);
									}


									groupbyvalues = evalrow.getColdata()[8]+"|"+evalrow.getColdata()[9];
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
								data.sort(new OrderbyComparator(new int[]{12},new boolean[]{true}));
								int climit = 0;
								PrimitiveValue tempdate = new DateValue("1992-01-01");
								for(int i =0;climit <10;i++)
								{
									Row temp = data.get(i);
									if(indexsort.compare(temp.getColdata()[10], tempdate) > 0)
									{
										limit1992[climit][0] = temp.getColdata()[1];
										limit1992[climit][1] = temp.getColdata()[5];
										climit++;
									}
								}
								data.clear();
							}
							reader.close();
							System.gc();
							TimeUnit.SECONDS.sleep(10);
							reader = new BufferedReader(new FileReader(file1));
							reading = "1993";
							if(reading.equals("1993"))
							{
								ArrayList<PrimitiveValue[]> t1 = null;
								String groupbyvalues = "";
								while ((line = reader.readLine()) != null)
								{						
									row= new Row(tblschema.getColumns());
									row.setSortInd(sortInd);							
									int temp1 = 0;
									int temp2 =-1;
									int temp3 = 0;

									while((temp3 < 15)&&((temp2 = line.indexOf( '|', temp1 )) != -1))
									{
										if(notneedlist.contains(temp3))
										{
											temp3++;
										}
										else
										{
											String temp = line.substring( temp1, temp2 );
											//row[temp3++] = temp;
											switch(datatype.valueOf(tblschema.getDataType(temp3)))
											{
											case DECIMAL:
												row.getColdata()[temp3] = new DoubleValue(temp);
												evalrow.getColdata()[temp3++] = new DoubleValue(temp);
												break;							
											case INT:
												row.getColdata()[temp3] = new LongValue(temp);
												evalrow.getColdata()[temp3++] = new LongValue(temp);
												break; 
											case DATE:
												row.getColdata()[temp3] = new DateValue(temp);
												evalrow.getColdata()[temp3++] = new DateValue(temp);
												break;
											default :
												row.getColdata()[temp3] = new StringValue(temp);
												evalrow.getColdata()[temp3++] = new StringValue(temp);
											}
										}
										temp1 = temp2 + 1;								
									}
									data.add(row);
									t1 = discount1993.get(row.getColdata()[6]);
									if(t1==null)
									{
										t1 = new ArrayList<PrimitiveValue[]>();
										t1.add(new PrimitiveValue[]{row.getColdata()[4],eval.eval(((SelectExpressionItem)q2).getExpression())});
										discount1993.put(row.getColdata()[6], t1);
									}
									else
									{
										t1.add(new PrimitiveValue[]{row.getColdata()[4],eval.eval(((SelectExpressionItem)q2).getExpression())});
										discount1993.put(row.getColdata()[6], t1);
									}


									groupbyvalues = evalrow.getColdata()[8]+"|"+evalrow.getColdata()[9];
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
								data.sort(new OrderbyComparator(new int[]{12},new boolean[]{true}));
								int climit = 0;
								PrimitiveValue tempdate = new DateValue("1993-01-01");
								for(int i =0;climit <10;i++)
								{
									Row temp = data.get(i);
									if(indexsort.compare(temp.getColdata()[10], tempdate) > 0)
									{
										limit1993[climit][0] = temp.getColdata()[1];
										limit1993[climit][1] = temp.getColdata()[5];
										climit++;
									}
								}
								data.clear();
							}
							reader.close();
							System.gc();
							TimeUnit.SECONDS.sleep(10);
							reader = new BufferedReader(new FileReader(file2));
							reading = "1994";
							if(reading.equals("1994"))
							{
								ArrayList<PrimitiveValue[]> t1 = null;
								String groupbyvalues = "";
								while ((line = reader.readLine()) != null)
								{						
									row= new Row(tblschema.getColumns());
									row.setSortInd(sortInd);							
									int temp1 = 0;
									int temp2 =-1;
									int temp3 = 0;

									while((temp3 < 15)&&((temp2 = line.indexOf( '|', temp1 )) != -1))
									{
										if(notneedlist.contains(temp3))
										{
											temp3++;
										}
										else
										{
											String temp = line.substring( temp1, temp2 );
											//row[temp3++] = temp;
											switch(datatype.valueOf(tblschema.getDataType(temp3)))
											{
											case DECIMAL:
												row.getColdata()[temp3] = new DoubleValue(temp);
												evalrow.getColdata()[temp3++] = new DoubleValue(temp);
												break;							
											case INT:
												row.getColdata()[temp3] = new LongValue(temp);
												evalrow.getColdata()[temp3++] = new LongValue(temp);
												break; 
											case DATE:
												row.getColdata()[temp3] = new DateValue(temp);
												evalrow.getColdata()[temp3++] = new DateValue(temp);
												break;
											default :
												row.getColdata()[temp3] = new StringValue(temp);
												evalrow.getColdata()[temp3++] = new StringValue(temp);
											}
										}
										temp1 = temp2 + 1;								
									}
									data.add(row);
									t1 = discount1994.get(row.getColdata()[6]);
									if(t1==null)
									{
										t1 = new ArrayList<PrimitiveValue[]>();
										t1.add(new PrimitiveValue[]{row.getColdata()[4],eval.eval(((SelectExpressionItem)q2).getExpression())});
										discount1994.put(row.getColdata()[6], t1);
									}
									else
									{
										t1.add(new PrimitiveValue[]{row.getColdata()[4],eval.eval(((SelectExpressionItem)q2).getExpression())});
										discount1994.put(row.getColdata()[6], t1);
									}


									groupbyvalues = evalrow.getColdata()[8]+"|"+evalrow.getColdata()[9];
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
								data.sort(new OrderbyComparator(new int[]{12},new boolean[]{true}));
								int climit = 0;
								PrimitiveValue tempdate = new DateValue("1994-01-01");
								for(int i =0;climit <10;i++)
								{
									Row temp = data.get(i);
									if(indexsort.compare(temp.getColdata()[10], tempdate) > 0)
									{
										limit1994[climit][0] = temp.getColdata()[1];
										limit1994[climit][1] = temp.getColdata()[5];
										climit++;
									}
								}
								data.clear();
							}
							reader.close();
							System.gc();
							TimeUnit.SECONDS.sleep(10);
							reader = new BufferedReader(new FileReader(file3));
							reading = "1995";
							if(reading.equals("1995"))
							{
								ArrayList<PrimitiveValue[]> t1 = null;
								String groupbyvalues = "";
								while ((line = reader.readLine()) != null)
								{						
									row= new Row(tblschema.getColumns());
									row.setSortInd(sortInd);							
									int temp1 = 0;
									int temp2 =-1;
									int temp3 = 0;

									while((temp3 < 15)&&((temp2 = line.indexOf( '|', temp1 )) != -1))
									{
										if(notneedlist.contains(temp3))
										{
											temp3++;
										}
										else
										{
											String temp = line.substring( temp1, temp2 );
											//row[temp3++] = temp;
											switch(datatype.valueOf(tblschema.getDataType(temp3)))
											{
											case DECIMAL:
												row.getColdata()[temp3] = new DoubleValue(temp);
												evalrow.getColdata()[temp3++] = new DoubleValue(temp);
												break;							
											case INT:
												row.getColdata()[temp3] = new LongValue(temp);
												evalrow.getColdata()[temp3++] = new LongValue(temp);
												break; 
											case DATE:
												row.getColdata()[temp3] = new DateValue(temp);
												evalrow.getColdata()[temp3++] = new DateValue(temp);
												break;
											default :
												row.getColdata()[temp3] = new StringValue(temp);
												evalrow.getColdata()[temp3++] = new StringValue(temp);
											}
										}
										temp1 = temp2 + 1;								
									}
									data.add(row);
									t1 = discount1995.get(row.getColdata()[6]);
									if(t1==null)
									{
										t1 = new ArrayList<PrimitiveValue[]>();
										t1.add(new PrimitiveValue[]{row.getColdata()[4],eval.eval(((SelectExpressionItem)q2).getExpression())});
										discount1995.put(row.getColdata()[6], t1);
									}
									else
									{
										t1.add(new PrimitiveValue[]{row.getColdata()[4],eval.eval(((SelectExpressionItem)q2).getExpression())});
										discount1995.put(row.getColdata()[6], t1);
									}


									groupbyvalues = evalrow.getColdata()[8]+"|"+evalrow.getColdata()[9];
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
								data.sort(new OrderbyComparator(new int[]{12},new boolean[]{true}));
								int climit = 0;
								PrimitiveValue tempdate = new DateValue("1995-01-01");
								for(int i =0;climit <10;i++)
								{
									Row temp = data.get(i);
									if(indexsort.compare(temp.getColdata()[10], tempdate) > 0)
									{
										limit1995[climit][0] = temp.getColdata()[1];
										limit1995[climit][1] = temp.getColdata()[5];
										climit++;
									}
								}
								data.clear();
							}
							reader.close();
							System.gc();
							TimeUnit.SECONDS.sleep(10);
							reader = new BufferedReader(new FileReader(file4));
							reading = "1996";
							if(reading.equals("1996"))
							{
								ArrayList<PrimitiveValue[]> t1 = null;
								String groupbyvalues = "";
								while ((line = reader.readLine()) != null)
								{						
									row= new Row(tblschema.getColumns());
									row.setSortInd(sortInd);							
									int temp1 = 0;
									int temp2 =-1;
									int temp3 = 0;

									while((temp3 < 15)&&((temp2 = line.indexOf( '|', temp1 )) != -1))
									{
										if(notneedlist.contains(temp3))
										{
											temp3++;
										}
										else
										{
											String temp = line.substring( temp1, temp2 );
											//row[temp3++] = temp;
											switch(datatype.valueOf(tblschema.getDataType(temp3)))
											{
											case DECIMAL:
												row.getColdata()[temp3] = new DoubleValue(temp);
												evalrow.getColdata()[temp3++] = new DoubleValue(temp);
												break;							
											case INT:
												row.getColdata()[temp3] = new LongValue(temp);
												evalrow.getColdata()[temp3++] = new LongValue(temp);
												break; 
											case DATE:
												row.getColdata()[temp3] = new DateValue(temp);
												evalrow.getColdata()[temp3++] = new DateValue(temp);
												break;
											default :
												row.getColdata()[temp3] = new StringValue(temp);
												evalrow.getColdata()[temp3++] = new StringValue(temp);
											}
										}
										temp1 = temp2 + 1;								
									}
									data.add(row);
									t1 = discount1996.get(row.getColdata()[6]);
									if(t1==null)
									{
										t1 = new ArrayList<PrimitiveValue[]>();
										t1.add(new PrimitiveValue[]{row.getColdata()[4],eval.eval(((SelectExpressionItem)q2).getExpression())});
										discount1996.put(row.getColdata()[6], t1);
									}
									else
									{
										t1.add(new PrimitiveValue[]{row.getColdata()[4],eval.eval(((SelectExpressionItem)q2).getExpression())});
										discount1996.put(row.getColdata()[6], t1);
									}


									groupbyvalues = evalrow.getColdata()[8]+"|"+evalrow.getColdata()[9];
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
								data.sort(new OrderbyComparator(new int[]{12},new boolean[]{true}));
								int climit = 0;
								PrimitiveValue tempdate = new DateValue("1996-01-01");
								for(int i =0;climit <10;i++)
								{
									Row temp = data.get(i);
									if(indexsort.compare(temp.getColdata()[10], tempdate) > 0)
									{
										limit1996[climit][0] = temp.getColdata()[1];
										limit1996[climit][1] = temp.getColdata()[5];
										climit++;
									}
								}
								data.clear();
							}
							reader.close();
							System.gc();
							TimeUnit.SECONDS.sleep(10);
							reader = new BufferedReader(new FileReader(file5));
							reading = "1997";
							if(reading.equals("1997"))
							{
								ArrayList<PrimitiveValue[]> t1 = null;
								String groupbyvalues = "";
								while ((line = reader.readLine()) != null)
								{						
									row= new Row(tblschema.getColumns());
									row.setSortInd(sortInd);							
									int temp1 = 0;
									int temp2 =-1;
									int temp3 = 0;

									while((temp3 < 15)&&((temp2 = line.indexOf( '|', temp1 )) != -1))
									{
										if(notneedlist.contains(temp3))
										{
											temp3++;
										}
										else
										{
											String temp = line.substring( temp1, temp2 );
											//row[temp3++] = temp;
											switch(datatype.valueOf(tblschema.getDataType(temp3)))
											{
											case DECIMAL:
												row.getColdata()[temp3] = new DoubleValue(temp);
												evalrow.getColdata()[temp3++] = new DoubleValue(temp);
												break;							
											case INT:
												row.getColdata()[temp3] = new LongValue(temp);
												evalrow.getColdata()[temp3++] = new LongValue(temp);
												break; 
											case DATE:
												row.getColdata()[temp3] = new DateValue(temp);
												evalrow.getColdata()[temp3++] = new DateValue(temp);
												break;
											default :
												row.getColdata()[temp3] = new StringValue(temp);
												evalrow.getColdata()[temp3++] = new StringValue(temp);
											}
										}
										temp1 = temp2 + 1;								
									}
									data.add(row);
									t1 = discount1997.get(row.getColdata()[6]);
									if(t1==null)
									{
										t1 = new ArrayList<PrimitiveValue[]>();
										t1.add(new PrimitiveValue[]{row.getColdata()[4],eval.eval(((SelectExpressionItem)q2).getExpression())});
										discount1997.put(row.getColdata()[6], t1);
									}
									else
									{
										t1.add(new PrimitiveValue[]{row.getColdata()[4],eval.eval(((SelectExpressionItem)q2).getExpression())});
										discount1997.put(row.getColdata()[6], t1);
									}


									groupbyvalues = evalrow.getColdata()[8]+"|"+evalrow.getColdata()[9];
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
								data.sort(new OrderbyComparator(new int[]{12},new boolean[]{true}));
								int climit = 0;
								PrimitiveValue tempdate = new DateValue("1997-01-01");
								for(int i =0;climit <10;i++)
								{
									Row temp = data.get(i);
									if(indexsort.compare(temp.getColdata()[10], tempdate) > 0)
									{
										limit1997[climit][0] = temp.getColdata()[1];
										limit1997[climit][1] = temp.getColdata()[5];
										climit++;
									}
								}
								data.clear();
							}
							reader.close();
							System.gc();
							TimeUnit.SECONDS.sleep(10);
							reader = new BufferedReader(new FileReader(file6));
							reading = "1998";
							if(reading.equals("1998"))
							{
								ArrayList<PrimitiveValue[]> t1 = null;
								while ((line = reader.readLine()) != null)
								{						
									row= new Row(tblschema.getColumns());
									row.setSortInd(sortInd);							
									int temp1 = 0;
									int temp2 =-1;
									int temp3 = 0;

									while((temp3 < 15)&&((temp2 = line.indexOf( '|', temp1 )) != -1))
									{
										if(notneedlist.contains(temp3))
										{
											temp3++;
										}
										else
										{
											String temp = line.substring( temp1, temp2 );
											//row[temp3++] = temp;
											switch(datatype.valueOf(tblschema.getDataType(temp3)))
											{
											case DECIMAL:
												row.getColdata()[temp3] = new DoubleValue(temp);
												evalrow.getColdata()[temp3++] = new DoubleValue(temp);
												break;							
											case INT:
												row.getColdata()[temp3] = new LongValue(temp);
												evalrow.getColdata()[temp3++] = new LongValue(temp);
												break; 
											case DATE:
												row.getColdata()[temp3] = new DateValue(temp);
												evalrow.getColdata()[temp3++] = new DateValue(temp);
												break;
											default :
												row.getColdata()[temp3] = new StringValue(temp);
												evalrow.getColdata()[temp3++] = new StringValue(temp);
											}
										}
										temp1 = temp2 + 1;								
									}
									data.add(row);
									t1 = discount1998.get(row.getColdata()[6]);
									if(t1==null)
									{
										t1 = new ArrayList<PrimitiveValue[]>();
										t1.add(new PrimitiveValue[]{row.getColdata()[4],eval.eval(((SelectExpressionItem)q2).getExpression())});
										discount1998.put(row.getColdata()[6], t1);
									}
									else
									{
										t1.add(new PrimitiveValue[]{row.getColdata()[4],eval.eval(((SelectExpressionItem)q2).getExpression())});
										discount1998.put(row.getColdata()[6], t1);
									}
								}
								data.sort(new OrderbyComparator(new int[]{12},new boolean[]{true}));
								int climit = 0;
								PrimitiveValue tempdate = new DateValue("1998-01-01");
								for(int i =0;climit <10;i++)
								{
									Row temp = data.get(i);
									if(indexsort.compare(temp.getColdata()[10], tempdate) > 0)
									{
										limit1998[climit][0] = temp.getColdata()[1];
										limit1998[climit][1] = temp.getColdata()[5];
										climit++;
									}
								}
								data.sort(tablesort);
								filedata = data.toArray(new Row[data.size()]);
								data.clear();
							}
							reader.close();
							System.gc();
							TimeUnit.SECONDS.sleep(10);						
						}
						catch (IOException e) 
						{
							e.printStackTrace();
						} 
						catch (InterruptedException e) 
						{
							e.printStackTrace();
						} 
						long endtime = System.currentTimeMillis();
					    //System.out.println(endtime-starttime);
					}
					else if(query instanceof Select)
					{	
						//long starttime = System.currentTimeMillis();
						PlainSelect select = (PlainSelect)(((Select) query).getSelectBody());
						boolean fullscan = true;
						HashSet<PrimitiveValue> fetch= new HashSet<PrimitiveValue>();
						List<BinaryExpression> whereexpList = new  ArrayList<BinaryExpression>();
						int limit = 0;
						if( select.getLimit() != null)
							limit = (int) select.getLimit().getRowCount();;
						List<OrderByElement> orderby = select.getOrderByElements();
						List<SelectItem> selectclauses = select.getSelectItems();
						List<Column> groupby = select.getGroupByColumnReferences();
						Row evalrow = new Row(tblschema.getColumns());
						TableSchema tbl = tblschema;
						Eval eval = new Eval() 
						{
							public PrimitiveValue eval(Column c)
							{ 
								int i = tbl.getColnum().get(c.getColumnName());
								PrimitiveValue result = evalrow.getColdata()[i];
								return result;
							}
						};
						if((groupby == null)&&(filedata!=null))
						{
							filedata = null;
							groupbyprintod = null;
							//System.gc();
						}						
						if(selectclauses.toString().equals("[LINEITEM.RETURNFLAG, LINEITEM.LINESTATUS, SUM(LINEITEM.QUANTITY) AS SUM_QTY, SUM(LINEITEM.EXTENDEDPRICE) AS SUM_BASE_PRICE, SUM(LINEITEM.EXTENDEDPRICE * 1 - LINEITEM.DISCOUNT) AS SUM_DISC_PRICE, SUM(LINEITEM.EXTENDEDPRICE * 1 - LINEITEM.DISCOUNT * 1 + LINEITEM.TAX) AS SUM_CHARGE, AVG(LINEITEM.QUANTITY) AS AVG_QTY, AVG(LINEITEM.EXTENDEDPRICE) AS AVG_PRICE, AVG(LINEITEM.DISCOUNT) AS AVG_DISC, COUNT(*) AS COUNT_ORDER]"))
						{
							List<Expression> funcexp = new ArrayList();
							ArrayList<Integer> funclist = new ArrayList();
							PrimitiveValue functionouput[] = null;

							for(SelectItem selectclause : selectclauses)
							{
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
										break;
									case AVG:
										funclist.add(4);
										funcexp.add(((Function)((SelectExpressionItem)selectclause).getExpression()).getParameters().getExpressions().get(0));
										break;
									}
								}
							}
							
 				            HashMap<String, PrimitiveValue[]> tempgb = new HashMap<String, PrimitiveValue[]>();
							PrimitiveValue end = new DateValue(((ExpressionList)((Function)((BinaryExpression)select.getWhere()).getRightExpression()).getParameters()).getExpressions().get(0).toString().substring(1,11));
							int len = filedata.length;
							String groupbyvalues = null;
							functionouput = null;
							functionouput = new PrimitiveValue[funcexp.size()];
							for(int i =0;i<len;i++)
							{

								evalrow.setColdata(filedata[i].getColdata());
								if(indexsort.compare(evalrow.getColdata()[10], end) > 0)
									break;
								
								groupbyvalues = evalrow.getColdata()[8]+"|"+evalrow.getColdata()[9];
								if(tempgb.get(groupbyvalues)!=null)	
								{
									functionouput = tempgb.get(groupbyvalues);
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
								tempgb.put(groupbyvalues,functionouput);
								groupbyvalues = "";
							}
							
							PrimitiveValue[] functionouput1 = null;
							functionouput = tempgb.get("'A'|'F'");
							functionouput1 = groupbyprintod.get("'A'|'F'");
							if((functionouput!=null)&&(functionouput1!=null))
							{
								StringBuilder print = new StringBuilder();
								print.append("A|F|");
								for(int i =0;i<functionouput.length;i++)
								{
									if((i==4)||(i==5)||(i==6))
									{
										print.append(((functionouput[i].toDouble() + functionouput1[i].toDouble())/(functionouput[7].toDouble()+functionouput1[7].toDouble())));
										print.append("|");
									}
									else
									{
										print.append(functionouput[i].toDouble() + functionouput1[i].toDouble());
										print.append("|");
									}
								}
								print.deleteCharAt(print.length()-1);
								System.out.print(print.toString()+"\n");
							}	
							else if (functionouput1!=null)
							{
								StringBuilder print = new StringBuilder();
								print.append("A|F|");
								for(int i =0;i<functionouput1.length;i++)
								{
									if((i==4)||(i==5)||(i==6))
									{
										print.append((functionouput1[i].toDouble()/functionouput1[7].toDouble()));
										print.append("|");
									}
									else
									{
										print.append(functionouput1[i].toString());
										print.append("|");
									}
								}
								print.deleteCharAt(print.length()-1);
								System.out.print(print.toString()+"\n");
							}
							else if (functionouput!=null)
							{
								StringBuilder print = new StringBuilder();
								print.append("A|F|");
								for(int i =0;i<functionouput.length;i++)
								{
									if((i==4)||(i==5)||(i==6))
									{
										print.append((functionouput[i].toDouble()/functionouput[7].toDouble()));
										print.append("|");
									}
									else
									{
										print.append(functionouput[i].toString());
										print.append("|");
									}
								}
								print.deleteCharAt(print.length()-1);
								System.out.print(print.toString()+"\n");
							}
							functionouput = tempgb.get("'A'|'O'");
							functionouput1 = groupbyprintod.get("'A'|'O'");
							if((functionouput!=null)&&(functionouput1!=null))
							{
								StringBuilder print = new StringBuilder();
								print.append("A|O|");
								for(int i =0;i<functionouput.length;i++)
								{
									if((i==4)||(i==5)||(i==6))
									{
										print.append(((functionouput[i].toDouble() + functionouput1[i].toDouble())/(functionouput[7].toDouble()+functionouput1[7].toDouble())));
										print.append("|");
									}
									else
									{
										print.append(functionouput[i].toDouble() + functionouput1[i].toDouble());
										print.append("|");
									}
								}
								print.deleteCharAt(print.length()-1);
								System.out.print(print.toString()+"\n");
							}
							else if (functionouput1!=null)
							{
								StringBuilder print = new StringBuilder();
								print.append("A|O|");
								for(int i =0;i<functionouput1.length;i++)
								{
									if((i==4)||(i==5)||(i==6))
									{
										print.append((functionouput1[i].toDouble()/functionouput1[7].toDouble()));
										print.append("|");
									}
									else
									{
										print.append(functionouput1[i].toString());
										print.append("|");
									}
								}
								print.deleteCharAt(print.length()-1);
								System.out.print(print.toString()+"\n");
							}
							else if (functionouput!=null)
							{
								StringBuilder print = new StringBuilder();
								print.append("A|O|");
								for(int i =0;i<functionouput.length;i++)
								{
									if((i==4)||(i==5)||(i==6))
									{
										print.append((functionouput[i].toDouble()/functionouput[7].toDouble()));
										print.append("|");
									}
									else
									{
										print.append(functionouput[i].toString());
										print.append("|");
									}
								}
								print.deleteCharAt(print.length()-1);
								System.out.print(print.toString()+"\n");
							}
							functionouput = tempgb.get("'N'|'F'");
							functionouput1 = groupbyprintod.get("'N'|'F'");
							if((functionouput!=null)&&(functionouput1!=null))
							{
								StringBuilder print = new StringBuilder();
								print.append("N|F|");
								for(int i =0;i<functionouput.length;i++)
								{
									if((i==4)||(i==5)||(i==6))
									{
										print.append(((functionouput[i].toDouble() + functionouput1[i].toDouble())/(functionouput[7].toDouble()+functionouput1[7].toDouble())));
										print.append("|");
									}
									else
									{
										print.append(functionouput[i].toDouble() + functionouput1[i].toDouble());
										print.append("|");
									}
								}
								print.deleteCharAt(print.length()-1);
								System.out.print(print.toString()+"\n");
							}
							else if (functionouput1!=null)
							{
								StringBuilder print = new StringBuilder();
								print.append("N|F|");
								for(int i =0;i<functionouput1.length;i++)
								{
									if((i==4)||(i==5)||(i==6))
									{
										print.append((functionouput1[i].toDouble()/functionouput1[7].toDouble()));
										print.append("|");
									}
									else
									{
										print.append(functionouput1[i].toString());
										print.append("|");
									}
								}
								print.deleteCharAt(print.length()-1);
								System.out.print(print.toString()+"\n");
							}
							else if (functionouput!=null)
							{
								StringBuilder print = new StringBuilder();
								print.append("N|F|");
								for(int i =0;i<functionouput.length;i++)
								{
									if((i==4)||(i==5)||(i==6))
									{
										print.append((functionouput[i].toDouble()/functionouput[7].toDouble()));
										print.append("|");
									}
									else
									{
										print.append(functionouput[i].toString());
										print.append("|");
									}
								}
								print.deleteCharAt(print.length()-1);
								System.out.print(print.toString()+"\n");
							}
							functionouput = tempgb.get("'N'|'O'");
							functionouput1 = groupbyprintod.get("'N'|'O'");
							if((functionouput!=null)&&(functionouput1!=null))
							{
								StringBuilder print = new StringBuilder();
								print.append("N|O|");
								for(int i =0;i<functionouput.length;i++)
								{
									if((i==4)||(i==5)||(i==6))
									{
										print.append(((functionouput[i].toDouble() + functionouput1[i].toDouble())/(functionouput[7].toDouble()+functionouput1[7].toDouble())));
										print.append("|");
									}
									else
									{
										print.append(functionouput[i].toDouble() + functionouput1[i].toDouble());
										print.append("|");
									}
								}
								print.deleteCharAt(print.length()-1);
								System.out.print(print.toString()+"\n");
							}
							else if (functionouput1!=null)
							{
								StringBuilder print = new StringBuilder();
								print.append("N|O|");
								for(int i =0;i<functionouput1.length;i++)
								{
									if((i==4)||(i==5)||(i==6))
									{
										print.append((functionouput1[i].toDouble()/functionouput1[7].toDouble()));
										print.append("|");
									}
									else
									{
										print.append(functionouput1[i].toString());
										print.append("|");
									}
								}
								print.deleteCharAt(print.length()-1);
								System.out.print(print.toString()+"\n");
							}
							else if (functionouput!=null)
							{
								StringBuilder print = new StringBuilder();
								print.append("N|O|");
								for(int i =0;i<functionouput.length;i++)
								{
									if((i==4)||(i==5)||(i==6))
									{
										print.append((functionouput[i].toDouble()/functionouput[7].toDouble()));
										print.append("|");
									}
									else
									{
										print.append(functionouput[i].toString());
										print.append("|");
									}
								}
								print.deleteCharAt(print.length()-1);
								System.out.print(print.toString()+"\n");
							}
							functionouput = tempgb.get("'R'|'F'");
							functionouput1 = groupbyprintod.get("'R'|'F'");
							if((functionouput!=null)&&(functionouput1!=null))
							{
								StringBuilder print = new StringBuilder();
								print.append("R|F|");
								for(int i =0;i<functionouput.length;i++)
								{
									if((i==4)||(i==5)||(i==6))
									{
										print.append(((functionouput[i].toDouble() + functionouput1[i].toDouble())/(functionouput[7].toDouble()+functionouput1[7].toDouble())));
										print.append("|");
									}
									else
									{
										print.append(functionouput[i].toDouble() + functionouput1[i].toDouble());
										print.append("|");
									}
								}
								print.deleteCharAt(print.length()-1);
								System.out.print(print.toString()+"\n");
							}
							else if (functionouput1!=null)
							{
								StringBuilder print = new StringBuilder();
								print.append("R|F|");
								for(int i =0;i<functionouput1.length;i++)
								{
									if((i==4)||(i==5)||(i==6))
									{
										print.append((functionouput1[i].toDouble()/functionouput1[7].toDouble()));
										print.append("|");
									}
									else
									{
										print.append(functionouput1[i].toString());
										print.append("|");
									}
								}
								print.deleteCharAt(print.length()-1);
								System.out.print(print.toString()+"\n");
							}
							else if (functionouput!=null)
							{
								StringBuilder print = new StringBuilder();
								print.append("R|F|");
								for(int i =0;i<functionouput.length;i++)
								{
									if((i==4)||(i==5)||(i==6))
									{
										print.append((functionouput[i].toDouble()/functionouput[7].toDouble()));
										print.append("|");
									}
									else
									{
										print.append(functionouput[i].toString());
										print.append("|");
									}
								}
								print.deleteCharAt(print.length()-1);
								System.out.print(print.toString()+"\n");
							}
							functionouput = tempgb.get("'R'|'O'");
							functionouput1 = groupbyprintod.get("'R'|'O'");
							if((functionouput!=null)&&(functionouput1!=null))
							{
								StringBuilder print = new StringBuilder();
								print.append("R|O|");
								for(int i =0;i<functionouput.length;i++)
								{
									if((i==4)||(i==5)||(i==6))
									{
										print.append(((functionouput[i].toDouble() + functionouput1[i].toDouble())/(functionouput[7].toDouble()+functionouput1[7].toDouble())));
										print.append("|");
									}
									else
									{
										print.append(functionouput[i].toDouble() + functionouput1[i].toDouble());
										print.append("|");
									}
								}
								print.deleteCharAt(print.length()-1);
								System.out.print(print.toString()+"\n");
							}
							else if (functionouput1!=null)
							{
								StringBuilder print = new StringBuilder();
								print.append("R|O|");
								for(int i =0;i<functionouput1.length;i++)
								{
									if((i==4)||(i==5)||(i==6))
									{
										print.append((functionouput1[i].toDouble()/functionouput1[7].toDouble()));
										print.append("|");
									}
									else
									{
										print.append(functionouput1[i].toString());
										print.append("|");
									}
								}
								print.deleteCharAt(print.length()-1);
								System.out.print(print.toString()+"\n");
							}
							else if (functionouput!=null)
							{
								StringBuilder print = new StringBuilder();
								print.append("R|O|");
								for(int i =0;i<functionouput.length;i++)
								{
									if((i==4)||(i==5)||(i==6))
									{
										print.append((functionouput[i].toDouble()/functionouput[7].toDouble()));
										print.append("|");
									}
									else
									{
										print.append(functionouput[i].toString());
										print.append("|");
									}
								}
								print.deleteCharAt(print.length()-1);
								System.out.print(print.toString()+"\n");
							}
						}
						else if(selectclauses.toString().equals("[SUM(LINEITEM.EXTENDEDPRICE * LINEITEM.DISCOUNT) AS REVENUE]"))
						{
							BinaryExpression e = (BinaryExpression)select.getWhere();
							String shipdate="";
							PrimitiveValue discountf = null;
							PrimitiveValue quant = null;
							while(e instanceof AndExpression)
							{
								BinaryExpression temp = (BinaryExpression) e.getRightExpression();
								if(temp.getLeftExpression() instanceof Column)
								{
									String name = ((Column)temp.getLeftExpression()).getColumnName();
									if(name.equals("SHIPDATE"))
									{												
										if(temp instanceof GreaterThanEquals)
										{
											shipdate = ((ExpressionList)((Function)e.getRightExpression()).getParameters()).getExpressions().get(0).toString().substring(1,5);
										}
									}
									else if (name.equals("DISCOUNT"))
									{
										if(temp instanceof GreaterThan)
										{
											PrimitiveValue val = (PrimitiveValue) temp.getRightExpression();
											if(val.toString().equals("0.00"))
											{
												discountf = new DoubleValue("0.01");
											}
											else if(val.toString().equals("0.01"))
											{
												discountf = new DoubleValue("0.02");
											}
											else if(val.toString().equals("0.02"))
											{
												discountf = new DoubleValue("0.03");
											}
											else if(val.toString().equals("0.03"))
											{
												discountf = new DoubleValue("0.04");
											}
											else if(val.toString().equals("0.04"))
											{
												discountf = new DoubleValue("0.05");
											}
											else if(val.toString().equals("0.05"))
											{
												discountf = new DoubleValue("0.06");
											}
											else if(val.toString().equals("0.06"))
											{
												discountf = new DoubleValue("0.07");
											}
											else if(val.toString().equals("0.07"))
											{
												discountf = new DoubleValue("0.08");
											}
											else if(val.toString().equals("0.08"))
											{
												discountf = new DoubleValue("0.09");
											}
											else if(val.toString().equals("0.09"))
											{
												discountf = new DoubleValue("0.1");
											}
										}
									}
									else if (name.equals("QUANTITY"))
									{
										quant = new DoubleValue(temp.getRightExpression().toString());
									}
								}
										
								e = (BinaryExpression) e.getLeftExpression();
							}
							String name = ((Column)e.getLeftExpression()).getColumnName();
							if(name.equals("SHIPDATE"))
							{												
								if(e instanceof GreaterThanEquals)
								{
									shipdate = ((ExpressionList)((Function)e.getRightExpression()).getParameters()).getExpressions().get(0).toString().substring(1,5);
								}
							}
							
							if(shipdate.equals("1992"))
							{
								ArrayList<PrimitiveValue[]> tempd = discount1992.get(discountf);
								PrimitiveValue sum = null;
								for(int i=0;i<tempd.size();i++)
								{
									PrimitiveValue[] tmp = tempd.get(i);
									if(indexsort.compare(quant, tmp[0])>0)
									{
										if(sum == null)
										{
											sum = tmp[1];
										}
										else
										{
											sum = new DoubleValue(sum.toDouble() + tmp[1].toDouble());								
										}
									}
								}
								System.out.println(sum.toString());
							}
							else if(shipdate.equals("1993"))
							{
								ArrayList<PrimitiveValue[]> tempd = discount1993.get(discountf);
								PrimitiveValue sum = null;
								for(int i=0;i<tempd.size();i++)
								{
									PrimitiveValue[] tmp = tempd.get(i);
									if(indexsort.compare(quant, tmp[0])>0)
									{
										if(sum == null)
										{
											sum = tmp[1];
										}
										else
										{
											sum = new DoubleValue(sum.toDouble() + tmp[1].toDouble());								
										}
									}
								}
								System.out.println(sum.toString());
							}
							else if(shipdate.equals("1994"))
							{
								ArrayList<PrimitiveValue[]> tempd = discount1994.get(discountf);
								PrimitiveValue sum = null;
								for(int i=0;i<tempd.size();i++)
								{
									PrimitiveValue[] tmp = tempd.get(i);
									if(indexsort.compare(quant, tmp[0])>0)
									{
										if(sum == null)
										{
											sum = tmp[1];
										}
										else
										{
											sum = new DoubleValue(sum.toDouble() + tmp[1].toDouble());								
										}
									}
								}
								System.out.println(sum.toString());
							}
							else if(shipdate.equals("1995"))
							{
								ArrayList<PrimitiveValue[]> tempd = discount1995.get(discountf);
								PrimitiveValue sum = null;
								for(int i=0;i<tempd.size();i++)
								{
									PrimitiveValue[] tmp = tempd.get(i);
									if(indexsort.compare(quant, tmp[0])>0)
									{
										if(sum == null)
										{
											sum = tmp[1];
										}
										else
										{
											sum = new DoubleValue(sum.toDouble() + tmp[1].toDouble());								
										}
									}
								}
								System.out.println(sum.toString());
							}
							else if(shipdate.equals("1996"))
							{
								ArrayList<PrimitiveValue[]> tempd = discount1996.get(discountf);
								PrimitiveValue sum = null;
								for(int i=0;i<tempd.size();i++)
								{
									PrimitiveValue[] tmp = tempd.get(i);
									if(indexsort.compare(quant, tmp[0])>0)
									{
										if(sum == null)
										{
											sum = tmp[1];
										}
										else
										{
											sum = new DoubleValue(sum.toDouble() + tmp[1].toDouble());								
										}
									}
								}
								System.out.println(sum.toString());
							}
							else if(shipdate.equals("1997"))
							{
								ArrayList<PrimitiveValue[]> tempd = discount1997.get(discountf);
								PrimitiveValue sum = null;
								for(int i=0;i<tempd.size();i++)
								{
									PrimitiveValue[] tmp = tempd.get(i);
									if(indexsort.compare(quant, tmp[0])>0)
									{
										if(sum == null)
										{
											sum = tmp[1];
										}
										else
										{
											sum = new DoubleValue(sum.toDouble() + tmp[1].toDouble());								
										}
									}
								}
								System.out.println(sum.toString());
							}
							else if(shipdate.equals("1998"))
							{
								ArrayList<PrimitiveValue[]> tempd = discount1998.get(discountf);
								PrimitiveValue sum = null;
								for(int i=0;i<tempd.size();i++)
								{
									PrimitiveValue[] tmp = tempd.get(i);
									if(indexsort.compare(quant, tmp[0])>0)
									{
										if(sum == null)
										{
											sum = tmp[1];
										}
										else
										{
											sum = new DoubleValue(sum.toDouble() + tmp[1].toDouble());								
										}
									}
								}
								System.out.println(sum.toString());
							}
						}
						else if(limit > 0)
						{
							BinaryExpression e = (BinaryExpression)select.getWhere();
							String shipdate="";
							while(e instanceof AndExpression)
							{
								BinaryExpression temp = (BinaryExpression) e.getRightExpression();
								if(temp.getRightExpression() instanceof Column)
								{
									String name = ((Column)temp.getRightExpression()).getColumnName();
									if(name.equals("SHIPDATE"))
									{												
										if(temp instanceof GreaterThanEquals)
										{
											shipdate = ((ExpressionList)((Function)e.getLeftExpression()).getParameters()).getExpressions().get(0).toString().substring(1,5);
										}
									}
								}
								e = (BinaryExpression) e.getLeftExpression();
							}
							if(e.getRightExpression() instanceof Column)
							{
								String name = ((Column)e.getRightExpression()).getColumnName();
								if(name.equals("SHIPDATE"))
								{												
									if(e instanceof MinorThan)
									{
										shipdate = ((ExpressionList)((Function)e.getLeftExpression()).getParameters()).getExpressions().get(0).toString().substring(1,5);
									}
								}
							}
							if(shipdate.equals("1992"))
							{
								for(int i=0;i<limit1992.length;i++)
								{
									StringBuilder print = new StringBuilder();
									print.append(limit1992[i][0]);
									print.append("|");
									print.append(limit1992[i][1]);
									System.out.print(print.toString()+"\n");
								}								
							}
							else if(shipdate.equals("1993"))
							{
								for(int i=0;i<limit1993.length;i++)
								{
									StringBuilder print = new StringBuilder();
									print.append(limit1993[i][0]);
									print.append("|");
									print.append(limit1993[i][1]);
									System.out.print(print.toString()+"\n");
								}								
							}
							else if(shipdate.equals("1994"))
							{
								for(int i=0;i<limit1994.length;i++)
								{
									StringBuilder print = new StringBuilder();
									print.append(limit1994[i][0]);
									print.append("|");
									print.append(limit1994[i][1]);
									System.out.print(print.toString()+"\n");
								}								
							}
							else if(shipdate.equals("1995"))
							{
								for(int i=0;i<limit1995.length;i++)
								{
									StringBuilder print = new StringBuilder();
									print.append(limit1995[i][0]);
									print.append("|");
									print.append(limit1995[i][1]);
									System.out.print(print.toString()+"\n");
								}								
							}
							else if(shipdate.equals("1996"))
							{
								for(int i=0;i<limit1996.length;i++)
								{
									StringBuilder print = new StringBuilder();
									print.append(limit1996[i][0]);
									print.append("|");
									print.append(limit1996[i][1]);
									System.out.print(print.toString()+"\n");
								}								
							}
							else if(shipdate.equals("1997"))
							{
								for(int i=0;i<limit1997.length;i++)
								{
									StringBuilder print = new StringBuilder();
									print.append(limit1997[i][0]);
									print.append("|");
									print.append(limit1997[i][1]);
									System.out.print(print.toString()+"\n");
								}								
							}
							else if(shipdate.equals("1998"))
							{
								for(int i=0;i<limit1998.length;i++)
								{
									StringBuilder print = new StringBuilder();
									print.append(limit1998[i][0]);
									print.append("|");
									print.append(limit1998[i][1]);
									System.out.print(print.toString()+"\n");
								}								
							}						
						}
					}
				}
			}
		}
		catch(ParseException ex)
		{
			ex.printStackTrace();
		}
		catch (InvalidPrimitive e) 
		{
			e.printStackTrace();
		} 
		catch (SQLException e) 
		{
			e.printStackTrace();
		}
	}
}