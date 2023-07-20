# Document on OceanBase Wait Event

## latch:dtl channel mgr hashmap lock wait

all DTL channels are managed by DTL channel manager using a hash map. Threads enter "dtl channel mgr hashmap lock wait" when concurrently insert/delete/get/iterate the hashmap.

## latch:dtl channel list lock
DTL channels are organized as a list. Lock the list when new channel is added or removed.

## latch:dtl free buffer list lock
Unused DTL cached buffer info structure is organized as a free list. Lock the list when buffer is allocated from the free list or returned to the free list.

## latch:pl debug lock wait

pl debug let you can debug the following program units (PL/SQL programs): anonymous blocks, packages, procedures, functions, and triggers. Threads enter "pl debug lock wait" when concurrently debug pl/sql.

## latch:pl sys breakpoint lock wait

when debug a pl , use sys breakpoint to pause the PL/SQL programs. Threads enter "pl sys breakpoint lock wait" when concurrently debug the pl/sql program.

## latch:dbms_job task lock wait

dbms_job create a job task to execute user-defined job. Threads enter "dbms_job task lock wait" when concurrently when run/add/execute immediate a job.

## latch:dbms_job master lock wait

dbms_job use dbms_job master to schedule all tasks. Threads enter "dbms_job master lock wait" when concurrently when register a job to master.


## latch:plan cache evict lock
There is a concurrency problem between the regularly triggered ps cache evict task and
the manually triggered flush ps cache evict task. There is also a concurrency problem between
the flush ps cache evict tasks triggered at the same time in different sessions.
The same ps statement may be added to the closed list by different ps cache evict tasks
at the same time. The mutex is used here to make all flush ps cache evict tasks execute serially

## latch:shared hash join cond lock wait
Shared hash join algorithm need to sync multiple threads when building the shared hash table. A mutex is used to sync all threads. This lock is used to protect the mutex.

## mutex: shared hash join cond wait
Shared hash join algorithm need to sync multiple threads when building the shared hash table. A mutex is used to sync all threads.

## latch: sql work area profile list lock wait
SQL Work Area Profile maintain a list of memory work area profiles, which is shared by all SQL threads. Use this lock to provide concurrency safety.

## latch: sql work area stat map lock wait
SQL Work Area Stat is maintained as a hash table. which is accessed by all SQL threads. Use this lock to provide concurrency safety.

## latch: sql memory manager mutex lock
SQL Memory Manager is a global object shared by all SQL threads. Use this lock to provide concurrency safety for its internal data structures such as global memory bounds.

## latch: load data rpc asyn callback lock
Loaddata function loads data in parallel RPCs. It use this lock to serialize multi async loaddata RPC callbacks.

## latch: merge dynamic sampling piece message lock
When dynamic sampling is used for parallel query with range reshuffle, threads wait when concurrent sampling message returned to the coordinator.

## latch:granule iterator task queue lock
SQL granule iterator task queue let parallel query tasks get their tasks from a shared pool. threads wait when threads concurrently obtaining next task from the queue.


## latch: dtl receive channel provider access lock
DTL receive channel provider is accessed by all PX tasks in a DFO.

## latch: parralel execution tenant target lock
parallel target value is updated by concurrent parallel queries. Threads wait here when they enter or exit the query process simultaneously.

## latch: parallel execution worker stat lock
px worker link its stat into the virtual view `__all_virtual_px_worker_stat` when the worker is running. This lock provide concurrency safety.

## latch: session query lock
At most one query can run in a session in the same time. session query lock guarante this.

## latch: session thread data lock
Data in a session can be accessed by multiple threads at the same time. For example, when a query is updating session thread data, V$ACTIVE_SESSION_HISTORY may also access it at the same time.

## latch: maintain session pool lock
Idle session are cached in a session pool. This lock is used to prevent creating session pool simultaneously.

## latch: sequence value alloc lock
A sequence value cache is access by multiple threads when allocating sequence value.

## latch: sequence cache lock
This lock prevent deleting sequence cache when other threads reading the cache.

## latch: inner connection pool lock
Lock for inner connection

## latch: tenant resource mgr list lock
Lock for tenant resource manager

## latch: tc free list lock
Thread local cache free list lock

## latch:dbms_schedler task lock wait

dbms_schedler create a job task to execute user-defined job. Threads enter "dbms_schedler task lock wait" when concurrently when run/add/execute immediate a job.

## latch:dbms_schedler master lock wait

dbms_schedler use dbms_schedler master to schedule all tasks. Threads enter "dbms_schedler master lock wait" when concurrently when register a job to master.

## latch:tenant worker lock wait

Threads enter "tenant worker lock wait" when add/remove worker from tenant worker list.

## latch:major_freeze service lock wait

major_freeze service is the enter for launching major freeze and executing admin merge operations. Threads enter 'major_freeze service lock wait' when concurrently executing launching major_freeze、clear_merge_error etc.

## latch:major_freeze lock wait

major_freeze service highly depends on 'leader'. we expect that when occuring switch_role, it can finish quickly. Due to the long time
of major_freeze usually, we hope that theads enter 'major_freeze lock wait' when concurrently executing launching major_freeze and switch_role

## latch:major_freeze switch lock wait

thread enter 'major_freeze switch lock wait' when concurrently executing switch_to_leader and switch_to_follower. Although we guess that
this concurrent situation may happen rarely

## latch:zone merge manager read lock wait

zone merge manager is used to manage all operations on zone_merge_info and global_merge_info. To get a higher performance, we use ObZoneMergeMgrGuard to solve 'write-read' concurrent situation. When start write operation, it will get a snapshot of zone merge mgr, and all the ops will execute on the snapshot. Threads enter 'zone merge manager read lock wait' when concurrently executing re-write
snapshot to zone merge mgr and read from zone merge mgr.

## latch:zone merge manager write lock wait

thread enter 'zone merge manager write lock wait' when concurrently executing 'write-write' operations.

## latch: auto increment table node init lock wait

For each auto-increment column, there is a cached table node and stored in the hashmap. The initialization and clearing of the table node will controlled by the lock.

## latch: auto increment alloc lock wait

Thread allocate auto-increment value wait.
In order to ensure the uniqueness of the value generated by the auto-increment column, the operation of allocating the auto-increment value must be atomic.

## latch: auto increment sync lock wait

Synchronous auto-increment column value when there is an insert into auto-increment column with specified value request.

## latch: auto increment global service lock wait

Global auto-increment service operation lock, the central node needs to maintain cache information and ensure atomic operations.

## latch: auto increment get leader lock wait

Global auto-increment service guaranteed the only leader address in global. Thread to refresh the table location cache and update the leader address acquire the lock.

## latch: work dag lock wait

Dag is used to handle various tasks, such as Compaction and Migration. The operations like adding task to Dag should be mutually exclusive.

## latch: work dag net lock wait

DagNet contains many Dag, which are executed in a specific order. The operations like adding child dag to DagNet should be mutually exclusive.

## latch: sys task stat lock wait

SysTaskStatMgr records current sys task info, and the operations like adding task, updating task and deleting task should be mutually exclusive.

## latch: info mgr lock wait

InfoMgr is used to record infos such as warning infos of Dag that running failed, and the read/write operations on InfoMgr should be mutually exclusive.

## latch: merger dump lock wait

When error occurs in minor/major compaction, we need to dump all sstables for anaylazing, and the operations in merge dumper should be mutually exclusive.

## latch: tablet merge info lock wait

Updating context infos to tablet merge info in parallel compaction task should be mutually exclusive.

## latch: wash out lock wait

The wash thread on kvcache store and updating cache priority should be mutually exclusive.

## latch: kv cache inst lock wait

The read and write operation on kvcache inst_map should be mutually exclusive.

## latch: kv cache list lock wait

The read and write operation on kvcache list_map in inst_map should be mutually exclusive.

## latch: thread store lock wait

The read and write operation on kvcache hazard version thread_stores_ should be mutually exclusive.

## latch: global kv cache config lock wait

The read and write operation on kvcache configs in global kvcache instance should be mutual exclusive.

## latch: global io config lock wait

The read and write operation on configs in global io manager instance should be mutually exclusive.

## rwlock: tenant io manage lock wait

The read and write operation on tenant map in global io manager instance should be mutually exclusive.

## spinlock: io fault detector lock wait

The read and write operation on diagnose info in global io detector instance should be mutually exclusive.

## latch: table api interface lock

table api query session lock. avoid concurrent access to same session.

## spinlock: palf sw last submit log info lock wait

The read and write operation on last submit log info in sliding window of palf should be mutually exclusive.

## spinlock: palf sw committed info lock wait

The read and write operation on committed info in sliding window of palf should be mutually exclusive.

## spinlock: palf sw last slide log info lock wait

The read and write operation on last slide log info in sliding window of palf should be mutually exclusive.

## spinlock: palf sw fetch log info lock wait

The read and write operation on fetch log info in sliding window of palf should be mutually exclusive.

## spinlock: palf sw match lsn map lock wait

The read and write operation on sw match lsn map in sliding window of palf should be mutually exclusive.

## spinlock: palf sw location cache cb lock wait

The read and write operation on sw location cache cb in sliding window of palf should be mutually exclusive.

## spinlock: palf cm config data lock wait

The read and write operation on config data in config mgr of palf should be mutually exclusive.

## spinlock: palf cm parent info lock wait

The read and write operation on parent info in config mgr of palf should be mutually exclusive.

## spinlock: palf cm child info lock wait

The read and write operation on child info in config mgr of palf should be mutually exclusive.

## RWLock: palf handle impl lock wait

The read and write operation on state of palf handle impl should be mutually exclusive. This lock protects basic state of a palf handle impl instance.

## spinglock: palf log engine lock wait

The read and write operation on log storage meta info in multiple threads should be mutually exclusive.

## spinglock: palf env lock wait

The read and write operation on palf env meta info in multiple threads should be mutually exclusive.

## spinglock: role change service lock wait

Registering sub role change handler to role change handler should be mutually exclusive.

## latch: obcdc progress recycle lock wait

The read and write operation on progress indices in progress controller of cdc connector should be mutually exclusive.

## latch: obcdc metainfo lock wait

The read and write operation on meteinfo in meta manager of cdc connector should be mutually exclusive.

## latch: obcdc trans ctx lock wait

The read and write operation on transaction context in cdc connector should be mutually exclusive.

## latch: obcdc svr blacklist wait

The read and write operation on server black list in cdc connector should be mutually exclusive.

## latch: obcdc sqlserver lock wait

The read and write operation on server list in sql server provider of cdc connector should be mutually exclusive.

## latch: obcdc timezone getter lock wait

The read and write operation on timezone info in cdc connector should be mutually exclusive.

## latch: obcdc fetchlog arpc lock wait

The read and write operation on state and rpc request in FetchLogARpc in cdc connector should be mutually exclusive.

## latch: obcdc fetchstream container lock wait

The read and write operation on fetchstream container in cdc connector should be mutually exclusive.

## latch: external server blacklist lock wait

The read and write operation on external server black list in logroute service should be mutually exclusive.

## latch: replay status lock wait

The read and write operation on configs in replay status should be mutually exclusive.

## spinlock: replay status task lock wait

The read and write operation on configs in replay status basic task should be mutually exclusive.

## mutex: max apply scn lock wait

Updating or getting max apply scn in apply status should be mutually exclusive.

## spinlock: gc handler lock wait

The read and write operation on configs in gc handler should be mutually exclusive.

## latch: cdcservice clientlsctx lock wait

The read and write operation on source(RemoteLogParent) in ClientLSCtx should be mutually exclusive.
## latch: hb respnses lock wait

The read and write operation on hb_responses_ should be mutually exclusive.
## latch: all servers info in table lock wait

The read and write operation on all_servers_info_in_table_ should be mutually exclusive.

## thread_cond: log external storage io task lock wait
The read and write operation on ObLogExternalStorageIOTaskCtx should be mutually exclusive.

## rwlock: log external storage io handler rw lock wait
The read and write operation on ObLogExternalStorageIOHandler should be mutually exclusive.

## rwlock: log external storage io handler spin lock wait
The read and write operation on ObLogExternalStorageIOHandler should be mutually exclusive.
