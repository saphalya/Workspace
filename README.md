# Workspace
Apache Beam
All the beam code has been developed to run in Google Dataflow Runner.
1. BigQueryReadPOCex1 --> Read from Big Query and write to a file in google storage
2. ReadFileWriteBQPOC --> Read from a file ad write to Big Query table
3. KVJoinBQPOC --> Key value pair joining
4. JoinTableBQPOC --> Read two table in Big Query, join them and then write them to a file
5. DiffJoinsPOC --> Read two table in Big Query. The tables are joined using different SQL joins and the resultant are written to a file

Table Schema used JoinTableBQPOC and DiffJoinsPOC are as follow

a) PlayerCategory: Category:STRING, Basepayment:STRING

b) Player: name:STRING, age:STRING, phone:STRING, category:STRING

c) PlayerTeam: phone:STRING, city:STRING, team:STRING
