<h3>Overview: Optimize your implementation to support join algorithms.</h3>

This checkpoint is, in effect, a more rigorous form of Checkpoints 1. The requirements are identical: We give you a query and some data, you evaluate the query on the data and give us a response as quickly as possible.

We'll be expecting a more feature-complete submission. Your code will be evaluated on more queries from TPC-H benchmark, which exercises a broader range of SQL features than the previous checkpoints did.

<h3>Join Ordering</h3>
The order in which you join tables together is incredibly important, and can change the runtime of your query by multiple orders of magnitude.  Picking between different join orderings is incredibly important!  However, to do so, you will need statistics about the data, something that won't really be feasible until later.  Instead, here's a present for those of you paying attention.  The tables in each FROM clause are ordered so that you will get our recommended join order by building a left-deep plan going in-order of the relation list (something that many of you are doing already), and (for hybrid hash joins) using the left-hand-side relation to build your hash table.

<h3>Query Rewriting</h3>
In the prior checkpoints, you were encouraged to parse SQL into a relational algebra tree.  This checkpoint is where that design choice will begins to pay off.  We've discussed expression equivalences in relational algebra, and identified several that are always good (e.g., pushing down selection operators). The reference implementation uses some simple recursion to identify patterns of expressions that can be optimized and rewrite them.  For example, if I wanted to define a new HashJoin operator, I might go through and replace every qualifying Selection operator sitting on top of a CrossProduct operator with a HashJoin.


The checkpoint 3 reference implementation optimizer implements three improvements:

Sort-Merge Join: An implementation of sort-merge join for use on out-of-memory joins.

1-Pass Hash Join: An implementation of the in-memory hash join algorithm.

Join Specialization: Rewrite Selection + CrossProduct operators into Hash Join operators

<h3>Interface</h3>
Your code will be evaluated in exactly the same way as Project 1. Data will drawn from a 1GB (SF 1) TPC-H dataset.

    java -cp build:jsqlparser.jar 
       dubstep.Main 
       --data [data] 
       [sqlfile1] [sqlfile2] ...
       
This example uses the following directories and files:

[data]: Table data stored in '|' separated files. As before, table names match the names provided in the matching CREATE TABLE with the .dat suffix.

[sqlfileX]: A file containing CREATE TABLE and SELECT statements, defining the schema of the dataset and the query to process
