# data-profiling
a set of scripts to pull meta data and data profiling metrics from relational database systems

The collection of scripts and SQL-code which can be tailored to collect specific information about tables and columns within databases.
It facilitates the bulk and rapid collection of high level and common metadata and provides a great starting point for identifiying and inventory of your database objects.

The scripts are intended to be used within your own particular client user interface and assumes you are able to connect to the database from which you want to collect data.   It is also up to you to dump the results out to whatever output meduim you want.   Generally by saving the results as a spreadsheet, csv, or cutting and pasting the results from your user interface.   This allows the greatest degree of flexibility.  

This repostiory is not meant to provide very deep data profiling capabilities,  other tools such as Informatica, Talend, InfoShere and others can do that much better.  

We get this most of the meta data from the internal data dictionary of the database system.   Some meta data can only be obtained by querying the the tables themselves.   Since performance can often be a problem when profiling large tables, we don't try to do this in bulk for an entire database.  This is where the more expensive and sophisticated tools are most useful.

Data profiling is a process
---------------------------
Data Profiling generally consists of a series of steps that dig deeper and deeper into the details of the data sets.
The high level steps addressed by this repository include: 

  1.  Inventory of databases (schemas)
  1.  Inventory and metadata of tables within the databases
  1.  Inventory and metadata of columns with the tables
  1.  For each column we can then dig into the details ( i.e. frequency distribution or list of values for columns)
  
Databases Platforms
-------------------
Since each database platform has it's own propiatary data dictionary, each platform has it's owns set of scripts.
The following databases have scripts in this repository:

  	* Oracle
  	* SQLServer
  	* MySQL
  	* Sybase IQ
  	* Netezza
  	* Hive
    * AWS Redshift
 
  
