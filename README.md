# csv2parquet


## csv2parquet
Wrapper script to convert CSV files to Parquet format using the excellent [DuckDB](https://duckdb.org/)

Since DuckDB does such an excellent job this is just a simple wrapper in python so I don't forget the precise command to use.

Assumes a true CSV (comma separated, not tab or semicolon separated; although this could be made configurable), the parquet file is written out with ZSTD codec.

The read_csv_auto by duckdb does an excellent job of guessing the appropriate type for a column,
so no need sofar to provide some means of explicitly casting columns to a certain type.

DuckDB converts the file in streaming fashion, not by loading the entire file first. 
So converting large files should not pose any issues (short of bugs maybe).

# parquet_info
A short script that outputs some metadata and the schema of a parquet file.
Useful if you get a parquet file from somewhere and want to quickly check its contents.

## Using DuckDB itself
If you have the DuckDB CLI at hand another quick solution 
(for showing the raw parquet schema information) is to execute this in DuckDB CLI:<br/>
`SELECT * FROM parquet_schema('filename.parquet')`<br/>

If you want to see what the parquet file will look like in DuckDB, use:<br/>
`create view test as select * from 'filename.parquet';`<br/>
`describe table test;`

This will also set a view on the parquet file which you can use as a regular table.
So to get the number of entries in the parquet file after this, simply do:<br/>
`select count(*) test;`

