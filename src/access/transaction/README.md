TransactionManager gives a TransactionID to a QueryPlan. 
It tracks commited transactions using Commit Log (CLOG).
It offers a Snapshot to the QueryPlan freezing the database in time.
HeapScan or other read functions ask TransactionManager if xid was commited,
and if it was HeapScan updates flags in TupleHeader, so it doesn't 
need to ask again.
