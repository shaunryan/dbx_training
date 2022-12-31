// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC ##Schema on Read
// MAGIC 
// MAGIC Schema on read is an important and complex topic. This is notebook doesn't delve into that dicussion but covers the practical facets of reading data from the data lake loaded specifically in this Azure architecture using data factory.
// MAGIC 
// MAGIC When Azure Data Factory writes data in text or json format the schema of that data is an inherant property of the data file itself. There is no schema meta data definition supplied in the data lake separately e.g. there is no table definition or XSD definition... we haven't got that far yet. Furthermore data files written yesterday may have a different schema than the files written today. 
// MAGIC 
// MAGIC Either because:
// MAGIC 
// MAGIC - The data recieved today has more fields than were populated yesterday and null fields in a format like json may just be excluded rather than be stated as null
// MAGIC - The source application changed and the schema changed
// MAGIC 
// MAGIC On top of that some files may have no schema definition at all! This is because there was no data for a given incremental period and data factory writes an empty file. If there is no data then that file cannot have a schema... it's just an empty file! This is useful because we can provision the data into the data lake without the burden of a schema at low cost very easily. However when we want to **prepare the data into information** we must have a schema - we perform a **schema on read**
// MAGIC 
// MAGIC So what does this mean in practical terms:
// MAGIC - **we can't load data without a schema!** If you try this it will result in production failures loading data over multiple paritions, because the schema will be inferred by files containing data and those without data will fail to adhere to this schema or schemas inferred may be violated on other files! It may work on a single file or a few files because spark is very good at inferring schema's. This is very useful in the development process since it's easy to get the data in for profiling to start with to figure out what the schema should be particularly with Big Data.
// MAGIC - furthermore **we should declare a schema** when preparing the data otherwise we compromise the information value proposition of the entire solution - rubbish in, rubbish out... there's also lots of other more boring computer science reasons like performance, etc, etc...
// MAGIC 
// MAGIC How do we declare a schema? There are various ways.
// MAGIC - For structured data we can create a table and take the schema from the table and apply it to text file when loading
// MAGIC - We can declare the schema in script using scala
// MAGIC - Supply a standard sample of data to load from which you can take the schema and apply to bigger sets of data
// MAGIC 
// MAGIC They all have pro's and cons and are useful or required in different scenarios.