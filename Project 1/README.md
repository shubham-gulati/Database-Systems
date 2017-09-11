<h3>Overview: Select/Project/Aggregate, New SQL features, Limited Memory, Faster Performance</h3>

Queries may include a ORDER BY clause. Because sort is a blocking, or 2-pass operator, you will need to handle both the case where you can fit everything into memory and the case where you can not.

Queries may include a LIMIT clause, a GROUP BY clause, and/or a FROM-nested subquery.

CREATE TABLE statements may now include INDEX and/or PRIMARY KEY directives.

You will be expected to process queries faster.

<h3>Sorting and Grouping Data</h3>
Sort is a blocking operator. Before it emits even one row, it needs to see the entire dataset. If you have enough memory to hold the entire input to be sorted, then you can just use Java's built-in Collections.sort method. However, for at least a few queries you will likely not have enough memory to keep everything available. In that case, a good option is to use the 2-pass sort algorithm that we discussed in class.

Group-by aggregates are also a blocking operator. If you run out of memory for the groups, you will need to implement a memory-aware grouping operator. One idea is to re-use the sort operator to group values together and use the sorted grouping technique that we discussed in class.

<h3>Preprocessing</h3>
Your code will be tested in 2 phases. In the first phase, you will have 1GB of memory and 2 minutes with each CREATE TABLE statement. In the second phase, you will have 150MB of memory and 5 minutes with each CREATE TABLE statement. The reference implementation uses this time to build indexes over the data – in-memory and/or on-disk, depending on phase. Students in prior years have come up with other creative ways to use this time.

CREATE TABLE statements will include index suggestions, both via unique PRIMARY KEY attributes and non-unique INDEX fields. You can get access to both through the getIndexes() method of CreateTable
