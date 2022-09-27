Data generated using Databricks 10.4:

```sql
CREATE TABLE default.json_stats_on_row_type
 (col struct<x bigint>) 
USING DELTA 
LOCATION 's3://bucket/databricks-compatibility-test-json_stats_on_row_type' 
TBLPROPERTIES (
 delta.checkpointInterval = 2,
 delta.checkpoint.writeStatsAsJson = false,
 delta.checkpoint.writeStatsAsStruct = true
);

INSERT INTO default.json_stats_on_row_type SELECT named_struct('x', 1);
INSERT INTO default.json_stats_on_row_type SELECT named_struct('x', 2);

ALTER TABLE default.json_stats_on_row_type SET TBLPROPERTIES (
 'delta.checkpoint.writeStatsAsJson' = true, 
 'delta.checkpoint.writeStatsAsStruct' = false
);
```
