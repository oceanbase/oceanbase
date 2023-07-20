/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifdef STAT_EVENT_ADD_DEF
// NETWORK
STAT_EVENT_ADD_DEF(RPC_PACKET_IN, "rpc packet in", ObStatClassIds::NETWORK, "rpc packet in", 10000, true, true)
STAT_EVENT_ADD_DEF(RPC_PACKET_IN_BYTES, "rpc packet in bytes", ObStatClassIds::NETWORK, "rpc packet in bytes", 10001, false, true)
STAT_EVENT_ADD_DEF(RPC_PACKET_OUT, "rpc packet out", ObStatClassIds::NETWORK, "rpc packet out", 10002, true, true)
STAT_EVENT_ADD_DEF(RPC_PACKET_OUT_BYTES, "rpc packet out bytes", ObStatClassIds::NETWORK, "rpc packet out bytes", 10003, false, true)
STAT_EVENT_ADD_DEF(RPC_DELIVER_FAIL, "rpc deliver fail", ObStatClassIds::NETWORK, "rpc deliver fail", 10004, false, true)
STAT_EVENT_ADD_DEF(RPC_NET_DELAY, "rpc net delay", ObStatClassIds::NETWORK, "rpc net delay", 10005, false, true)
STAT_EVENT_ADD_DEF(RPC_NET_FRAME_DELAY, "rpc net frame delay", ObStatClassIds::NETWORK, "rpc net frame delay", 10006, false, true)
STAT_EVENT_ADD_DEF(MYSQL_PACKET_IN, "mysql packet in", ObStatClassIds::NETWORK, "mysql packet in", 10007, false, true)
STAT_EVENT_ADD_DEF(MYSQL_PACKET_IN_BYTES, "mysql packet in bytes", ObStatClassIds::NETWORK, "mysql packet in bytes", 10008, false, true)
STAT_EVENT_ADD_DEF(MYSQL_PACKET_OUT, "mysql packet out", ObStatClassIds::NETWORK, "mysql packet out", 10009, false, true)
STAT_EVENT_ADD_DEF(MYSQL_PACKET_OUT_BYTES, "mysql packet out bytes", ObStatClassIds::NETWORK, "mysql packet out bytes", 10010, false, true)
STAT_EVENT_ADD_DEF(MYSQL_DELIVER_FAIL, "mysql deliver fail", ObStatClassIds::NETWORK, "mysql deliver fail", 10011, false, true)
STAT_EVENT_ADD_DEF(RPC_COMPRESS_ORIGINAL_PACKET_CNT, "rpc compress original packet cnt", ObStatClassIds::NETWORK, "rpc compress original packet cnt", 10012, false, true)
STAT_EVENT_ADD_DEF(RPC_COMPRESS_COMPRESSED_PACKET_CNT, "rpc compress compressed packet cnt", ObStatClassIds::NETWORK, "rpc compress compressed pacekt cnt", 10013, false, true)
STAT_EVENT_ADD_DEF(RPC_COMPRESS_ORIGINAL_SIZE, "rpc compress original size", ObStatClassIds::NETWORK, "rpc compress original size", 10014, false, true)
STAT_EVENT_ADD_DEF(RPC_COMPRESS_COMPRESSED_SIZE, "rpc compress compressed size", ObStatClassIds::NETWORK, "rpc compress compressed size", 10015, false, true)

STAT_EVENT_ADD_DEF(RPC_STREAM_COMPRESS_ORIGINAL_PACKET_CNT, "rpc stream compress original packet cnt", ObStatClassIds::NETWORK, "rpc stream compress original packet cnt", 10016, false, true)
STAT_EVENT_ADD_DEF(RPC_STREAM_COMPRESS_COMPRESSED_PACKET_CNT, "rpc stream compress compressed packet cnt", ObStatClassIds::NETWORK, "rpc stream compress compressed pacekt cnt", 10017, false, true)
STAT_EVENT_ADD_DEF(RPC_STREAM_COMPRESS_ORIGINAL_SIZE, "rpc stream compress original size", ObStatClassIds::NETWORK, "rpc stream compress original size", 10018, false, true)
STAT_EVENT_ADD_DEF(RPC_STREAM_COMPRESS_COMPRESSED_SIZE, "rpc stream compress compressed size", ObStatClassIds::NETWORK, "rpc stream compress compressed size", 10019, false, true)

// QUEUE
// STAT_EVENT_ADD_DEF(REQUEST_QUEUED_COUNT, "REQUEST_QUEUED_COUNT", QUEUE, "REQUEST_QUEUED_COUNT")
STAT_EVENT_ADD_DEF(REQUEST_ENQUEUE_COUNT, "request enqueue count", ObStatClassIds::QUEUE, "request enqueue count", 20000, false, true)
STAT_EVENT_ADD_DEF(REQUEST_DEQUEUE_COUNT, "request dequeue count", ObStatClassIds::QUEUE, "request dequeue count", 20001, false, true)
STAT_EVENT_ADD_DEF(REQUEST_QUEUE_TIME, "request queue time", ObStatClassIds::QUEUE, "request queue time", 20002, false, true)

// TRANS
STAT_EVENT_ADD_DEF(TRANS_COMMIT_LOG_SYNC_TIME, "trans commit log sync time", ObStatClassIds::TRANS, "trans commit log sync time", 30000, false, true)
STAT_EVENT_ADD_DEF(TRANS_COMMIT_LOG_SYNC_COUNT, "trans commit log sync count", ObStatClassIds::TRANS, "trans commit log sync count", 30001, false, true)
STAT_EVENT_ADD_DEF(TRANS_COMMIT_LOG_SUBMIT_COUNT, "trans commit log submit count", ObStatClassIds::TRANS, "trans commit log submit count", 30002, false, true)
STAT_EVENT_ADD_DEF(TRANS_SYSTEM_TRANS_COUNT, "trans system trans count", ObStatClassIds::TRANS, "trans system trans count", 30003, false, true)
STAT_EVENT_ADD_DEF(TRANS_USER_TRANS_COUNT, "trans user trans count", ObStatClassIds::TRANS, "trans user trans count", 30004, false, true)
STAT_EVENT_ADD_DEF(TRANS_START_COUNT, "trans start count", ObStatClassIds::TRANS, "trans start count", 30005, false, true)
STAT_EVENT_ADD_DEF(TRANS_TOTAL_USED_TIME, "trans total used time", ObStatClassIds::TRANS, "trans total used time", 30006, false, true)
STAT_EVENT_ADD_DEF(TRANS_COMMIT_COUNT, "trans commit count", ObStatClassIds::TRANS, "trans commit count", 30007, false, true)
STAT_EVENT_ADD_DEF(TRANS_COMMIT_TIME, "trans commit time", ObStatClassIds::TRANS, "trans commit time", 30008, false, true)
STAT_EVENT_ADD_DEF(TRANS_ROLLBACK_COUNT, "trans rollback count", ObStatClassIds::TRANS, "trans rollback count", 30009, false, true)
STAT_EVENT_ADD_DEF(TRANS_ROLLBACK_TIME, "trans rollback time", ObStatClassIds::TRANS, "trans rollback time", 30010, false, true)
STAT_EVENT_ADD_DEF(TRANS_TIMEOUT_COUNT, "trans timeout count", ObStatClassIds::TRANS, "trans timeout count", 30011, false, true)
STAT_EVENT_ADD_DEF(TRANS_LOCAL_COUNT, "trans local trans count", ObStatClassIds::TRANS, "trans local trans count", 30012, false, true)
STAT_EVENT_ADD_DEF(TRANS_DIST_COUNT, "trans distribute trans count", ObStatClassIds::TRANS, "trans dist trans count", 30013, false, true)
//STAT_EVENT_ADD_DEF(TRANS_DISTRIBUTED_STMT_COUNT, "trans distributed stmt count", TRANS, "trans distributed stmt count", 30014)
//STAT_EVENT_ADD_DEF(TRANS_LOCAL_STMT_COUNT, "trans local stmt count", TRANS, "trans local stmt count", 30015)
//STAT_EVENT_ADD_DEF(TRANS_REMOTE_STMT_COUNT, "trans remote stmt count", TRANS, "trans remote stmt count", 30016)
STAT_EVENT_ADD_DEF(TRANS_READONLY_COUNT, "trans without participant count", ObStatClassIds::TRANS, "trans without participant count", 30017, false, true)
STAT_EVENT_ADD_DEF(REDO_LOG_REPLAY_COUNT, "redo log replay count", ObStatClassIds::TRANS, "redo log replay count", 30018, false, true)
STAT_EVENT_ADD_DEF(REDO_LOG_REPLAY_TIME, "redo log replay time", ObStatClassIds::TRANS, "redo log replay time", 30019, false, true)
STAT_EVENT_ADD_DEF(PREPARE_LOG_REPLAY_COUNT, "prepare log replay count", ObStatClassIds::TRANS, "prepare log replay count", 30020, false, true)
STAT_EVENT_ADD_DEF(PREPARE_LOG_REPLAY_TIME, "prepare log replay time", ObStatClassIds::TRANS, "prepare log replay time", 30021, false, true)
STAT_EVENT_ADD_DEF(COMMIT_LOG_REPLAY_COUNT, "commit log replay count", ObStatClassIds::TRANS, "commit log replay count", 30022, false, true)
STAT_EVENT_ADD_DEF(COMMIT_LOG_REPLAY_TIME, "commit log replay time", ObStatClassIds::TRANS, "commit log replay time", 30023, false, true)
STAT_EVENT_ADD_DEF(ABORT_LOG_REPLAY_COUNT, "abort log replay count", ObStatClassIds::TRANS, "abort log replay count", 30024, false, true)
STAT_EVENT_ADD_DEF(ABORT_LOG_REPLAY_TIME, "abort log replay time", ObStatClassIds::TRANS, "abort log replay time", 30025, false, true)
STAT_EVENT_ADD_DEF(CLEAR_LOG_REPLAY_COUNT, "clear log replay count", ObStatClassIds::TRANS, "clear log replay count", 30026, false, true)
STAT_EVENT_ADD_DEF(CLEAR_LOG_REPLAY_TIME, "clear log replay time", ObStatClassIds::TRANS, "clear log replay time", 30027, false, true)
STAT_EVENT_ADD_DEF(GTS_REQUEST_TOTAL_COUNT, "gts request total count", ObStatClassIds::TRANS, "gts request total count", 30053, false, true)
STAT_EVENT_ADD_DEF(GTS_ACQUIRE_TOTAL_TIME, "gts acquire total time", ObStatClassIds::TRANS, "gts acquire total time", 30054, false, true)
STAT_EVENT_ADD_DEF(GTS_ACQUIRE_TOTAL_WAIT_COUNT, "gts acquire total wait count", ObStatClassIds::TRANS, "gts acquire total wait count", 30056, false, true)
STAT_EVENT_ADD_DEF(GTS_WAIT_ELAPSE_TOTAL_TIME, "gts wait elapse total time", ObStatClassIds::TRANS, "gts wait elapse total time", 30063, false, true)
STAT_EVENT_ADD_DEF(GTS_WAIT_ELAPSE_TOTAL_COUNT, "gts wait elapse total count", ObStatClassIds::TRANS, "gts wait elapse total count", 30064, false, true)
STAT_EVENT_ADD_DEF(GTS_WAIT_ELAPSE_TOTAL_WAIT_COUNT, "gts wait elapse total wait count", ObStatClassIds::TRANS, "gts wait elapse total wait count", 30065, false, true)
STAT_EVENT_ADD_DEF(GTS_RPC_COUNT, "gts rpc count", ObStatClassIds::TRANS, "gts rpc count", 30066, false, true)
STAT_EVENT_ADD_DEF(GTS_TRY_ACQUIRE_TOTAL_COUNT, "gts try acquire total count", ObStatClassIds::TRANS, "gts try acquire total count", 30067, false, true)
STAT_EVENT_ADD_DEF(GTS_TRY_WAIT_ELAPSE_TOTAL_COUNT, "gts try wait elapse total count", ObStatClassIds::TRANS, "gts try wait elapse total count", 30068, false, true)
STAT_EVENT_ADD_DEF(TRANS_ELR_ENABLE_COUNT, "trans early lock release enable count", ObStatClassIds::TRANS, "trans early lock releaes enable count", 30077, false, true)
STAT_EVENT_ADD_DEF(TRANS_ELR_UNABLE_COUNT, "trans early lock release unable count", ObStatClassIds::TRANS, "trans early lock releaes unable count", 30078, false, true)
STAT_EVENT_ADD_DEF(READ_ELR_ROW_COUNT, "read elr row count", ObStatClassIds::TRANS, "read elr row count", 30079, false, true)
STAT_EVENT_ADD_DEF(TRANS_LOCAL_TOTAL_USED_TIME, "local trans total used time", ObStatClassIds::TRANS, "local trans total used time", 30080, false, true)
STAT_EVENT_ADD_DEF(TRANS_DIST_TOTAL_USED_TIME, "distributed trans total used time", ObStatClassIds::TRANS, "distributed trans total used time", 30081, false, true)
STAT_EVENT_ADD_DEF(TX_DATA_HIT_MINI_CACHE_COUNT, "tx data hit mini cache count", ObStatClassIds::TRANS, "tx data hit mini cache count", 30082, false, true)
STAT_EVENT_ADD_DEF(TX_DATA_HIT_KV_CACHE_COUNT, "tx data hit kv cache count", ObStatClassIds::TRANS, "tx data hit kv cache count", 30083, false, true)
STAT_EVENT_ADD_DEF(TX_DATA_READ_TX_CTX_COUNT, "tx data read tx ctx count", ObStatClassIds::TRANS, "tx data read tx ctx count", 30084, false, true)
STAT_EVENT_ADD_DEF(TX_DATA_READ_TX_DATA_MEMTABLE_COUNT, "tx data read tx data memtable count", ObStatClassIds::TRANS, "tx data read tx data memtable count", 30085, false, true)
STAT_EVENT_ADD_DEF(TX_DATA_READ_TX_DATA_SSTABLE_COUNT, "tx data read tx data sstable count", ObStatClassIds::TRANS, "tx data read tx data sstable count", 30086, false, true)

// SQL
//STAT_EVENT_ADD_DEF(PLAN_CACHE_HIT, "PLAN_CACHE_HIT", SQL, "PLAN_CACHE_HIT")
//STAT_EVENT_ADD_DEF(PLAN_CACHE_MISS, "PLAN_CACHE_MISS", SQL, "PLAN_CACHE_MISS")
STAT_EVENT_ADD_DEF(SQL_SELECT_COUNT, "sql select count", ObStatClassIds::SQL, "sql select count", 40000, false, true)
STAT_EVENT_ADD_DEF(SQL_SELECT_TIME, "sql select time", ObStatClassIds::SQL, "sql select time", 40001, false, true)
STAT_EVENT_ADD_DEF(SQL_INSERT_COUNT, "sql insert count", ObStatClassIds::SQL, "sql insert count", 40002, false, true)
STAT_EVENT_ADD_DEF(SQL_INSERT_TIME, "sql insert time", ObStatClassIds::SQL, "sql insert time", 40003, false, true)
STAT_EVENT_ADD_DEF(SQL_REPLACE_COUNT, "sql replace count", ObStatClassIds::SQL, "sql replace count", 40004, false, true)
STAT_EVENT_ADD_DEF(SQL_REPLACE_TIME, "sql replace time", ObStatClassIds::SQL, "sql replace time", 40005, false, true)
STAT_EVENT_ADD_DEF(SQL_UPDATE_COUNT, "sql update count", ObStatClassIds::SQL, "sql update count", 40006, false, true)
STAT_EVENT_ADD_DEF(SQL_UPDATE_TIME, "sql update time", ObStatClassIds::SQL, "sql update time", 40007, false, true)
STAT_EVENT_ADD_DEF(SQL_DELETE_COUNT, "sql delete count", ObStatClassIds::SQL, "sql delete count", 40008, false, true)
STAT_EVENT_ADD_DEF(SQL_DELETE_TIME, "sql delete time", ObStatClassIds::SQL, "sql delete time", 40009, false, true)
STAT_EVENT_ADD_DEF(SQL_OTHER_COUNT, "sql other count", ObStatClassIds::SQL, "sql other count", 40018, false, true)
STAT_EVENT_ADD_DEF(SQL_OTHER_TIME, "sql other time", ObStatClassIds::SQL, "sql other time", 40019, false, true)
STAT_EVENT_ADD_DEF(SQL_PS_PREPARE_COUNT, "ps prepare count", ObStatClassIds::SQL, "ps prepare count", 40020, false, true)
STAT_EVENT_ADD_DEF(SQL_PS_PREPARE_TIME, "ps prepare time", ObStatClassIds::SQL, "ps prepare time", 40021, false, true)
STAT_EVENT_ADD_DEF(SQL_PS_EXECUTE_COUNT, "ps execute count", ObStatClassIds::SQL, "ps execute count", 40022, false, true)
STAT_EVENT_ADD_DEF(SQL_PS_CLOSE_COUNT, "ps close count", ObStatClassIds::SQL, "ps close count", 40023, false, true)
STAT_EVENT_ADD_DEF(SQL_PS_CLOSE_TIME, "ps close time", ObStatClassIds::SQL, "ps close time", 40024, false, true)

STAT_EVENT_ADD_DEF(SQL_OPEN_CURSORS_CURRENT, "opened cursors current", ObStatClassIds::SQL, "opened cursors current", 40030, false, true)
STAT_EVENT_ADD_DEF(SQL_OPEN_CURSORS_CUMULATIVE, "opened cursors cumulative", ObStatClassIds::SQL, "opened cursors cumulative", 40031, false, true)

STAT_EVENT_ADD_DEF(SQL_LOCAL_COUNT, "sql local count", ObStatClassIds::SQL, "sql local count", 40010, false, true)
STAT_EVENT_ADD_DEF(SQL_REMOTE_COUNT, "sql remote count", ObStatClassIds::SQL, "sql remote count", 40011, false, true)
STAT_EVENT_ADD_DEF(SQL_DISTRIBUTED_COUNT, "sql distributed count", ObStatClassIds::SQL, "sql distributed count", 40012, false, true)
STAT_EVENT_ADD_DEF(ACTIVE_SESSIONS, "active sessions", ObStatClassIds::SQL, "active sessions", 40013, false, true)
STAT_EVENT_ADD_DEF(SQL_SINGLE_QUERY_COUNT, "single query count", ObStatClassIds::SQL, "single query count", 40014, false, true)
STAT_EVENT_ADD_DEF(SQL_MULTI_QUERY_COUNT, "multiple query count", ObStatClassIds::SQL, "multiple query count", 40015, false, true)
STAT_EVENT_ADD_DEF(SQL_MULTI_ONE_QUERY_COUNT, "multiple query with one stmt count", ObStatClassIds::SQL, "multiple query with one stmt count", 40016, false, true)

STAT_EVENT_ADD_DEF(SQL_INNER_SELECT_COUNT, "sql inner select count", ObStatClassIds::SQL, "sql inner select count", 40100, false, true)
STAT_EVENT_ADD_DEF(SQL_INNER_SELECT_TIME, "sql inner select time", ObStatClassIds::SQL, "sql inner select time", 40101, false, true)
STAT_EVENT_ADD_DEF(SQL_INNER_INSERT_COUNT, "sql inner insert count", ObStatClassIds::SQL, "sql inner insert count", 40102, false, true)
STAT_EVENT_ADD_DEF(SQL_INNER_INSERT_TIME, "sql inner insert time", ObStatClassIds::SQL, "sql inner insert time", 40103, false, true)
STAT_EVENT_ADD_DEF(SQL_INNER_REPLACE_COUNT, "sql inner replace count", ObStatClassIds::SQL, "sql inner replace count", 40104, false, true)
STAT_EVENT_ADD_DEF(SQL_INNER_REPLACE_TIME, "sql inner replace time", ObStatClassIds::SQL, "sql inner replace time", 40105, false, true)
STAT_EVENT_ADD_DEF(SQL_INNER_UPDATE_COUNT, "sql inner update count", ObStatClassIds::SQL, "sql inner update count", 40106, false, true)
STAT_EVENT_ADD_DEF(SQL_INNER_UPDATE_TIME, "sql inner update time", ObStatClassIds::SQL, "sql inner update time", 40107, false, true)
STAT_EVENT_ADD_DEF(SQL_INNER_DELETE_COUNT, "sql inner delete count", ObStatClassIds::SQL, "sql inner delete count", 40108, false, true)
STAT_EVENT_ADD_DEF(SQL_INNER_DELETE_TIME, "sql inner delete time", ObStatClassIds::SQL, "sql inner delete time", 40109, false, true)
STAT_EVENT_ADD_DEF(SQL_INNER_OTHER_COUNT, "sql inner other count", ObStatClassIds::SQL, "sql inner other count", 40110, false, true)
STAT_EVENT_ADD_DEF(SQL_INNER_OTHER_TIME, "sql inner other time", ObStatClassIds::SQL, "sql inner other time", 40111, false, true)
STAT_EVENT_ADD_DEF(SQL_USER_LOGONS_CUMULATIVE, "user logons cumulative", ObStatClassIds::SQL, "user logons cumulative", 40112, false, true)
STAT_EVENT_ADD_DEF(SQL_USER_LOGOUTS_CUMULATIVE, "user logouts cumulative", ObStatClassIds::SQL, "user logouts cumulative", 40113, false, true)
STAT_EVENT_ADD_DEF(SQL_USER_LOGONS_FAILED_CUMULATIVE, "user logons failed cumulative", ObStatClassIds::SQL, "user logons failed cumulative", 40114, false, true)
STAT_EVENT_ADD_DEF(SQL_USER_LOGONS_COST_TIME_CUMULATIVE, "user logons time cumulative", ObStatClassIds::SQL, "user logons time cumulative", 40115, false, true)
STAT_EVENT_ADD_DEF(SQL_LOCAL_TIME, "sql local execute time", ObStatClassIds::SQL, "sql local execute time", 40116, false, true)
STAT_EVENT_ADD_DEF(SQL_REMOTE_TIME, "sql remote execute time", ObStatClassIds::SQL, "sql remote execute time", 40117, false, true)
STAT_EVENT_ADD_DEF(SQL_DISTRIBUTED_TIME, "sql distributed execute time", ObStatClassIds::SQL, "sql distributed execute time", 40118, false, true)

// CACHE
STAT_EVENT_ADD_DEF(ROW_CACHE_HIT, "row cache hit", ObStatClassIds::CACHE, "row cache hit", 50000, true, true)
STAT_EVENT_ADD_DEF(ROW_CACHE_MISS, "row cache miss", ObStatClassIds::CACHE, "row cache miss", 50001, true, true)
STAT_EVENT_ADD_DEF(BLOOM_FILTER_CACHE_HIT, "bloom filter cache hit", ObStatClassIds::CACHE, "bloom filter cache hit", 50004, true, true)
STAT_EVENT_ADD_DEF(BLOOM_FILTER_CACHE_MISS, "bloom filter cache miss", ObStatClassIds::CACHE, "bloom filter cache miss", 50005, true, true)
STAT_EVENT_ADD_DEF(BLOOM_FILTER_FILTS, "bloom filter filts", ObStatClassIds::CACHE, "bloom filter filts", 50006, true, true)
STAT_EVENT_ADD_DEF(BLOOM_FILTER_PASSES, "bloom filter passes", ObStatClassIds::CACHE, "bloom filter passes", 50007, true, true)
STAT_EVENT_ADD_DEF(BLOCK_CACHE_HIT, "block cache hit", ObStatClassIds::CACHE, "block cache hit", 50008, true, true)
STAT_EVENT_ADD_DEF(BLOCK_CACHE_MISS, "block cache miss", ObStatClassIds::CACHE, "block cache miss", 50009, true, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_HIT, "location cache hit", ObStatClassIds::CACHE, "location cache hit", 50010, false, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_MISS, "location cache miss", ObStatClassIds::CACHE, "location cache miss", 50011, false, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_WAIT, "location cache wait", ObStatClassIds::CACHE, "location cache wait", 50012, false, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_PROXY_HIT, "location cache get hit from proxy virtual table", ObStatClassIds::CACHE, "location cache get hit from proxy virtual table", 50013, false, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_PROXY_MISS, "location cache get miss from proxy virtual table", ObStatClassIds::CACHE, "location cache get miss from proxy virtual table", 50014, false, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_CLEAR_LOCATION, "location cache nonblock renew", ObStatClassIds::CACHE, "location cache nonblock renew", 50015, false, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_CLEAR_LOCATION_IGNORED, "location cache nonblock renew ignored", ObStatClassIds::CACHE, "location cache nonblock renew ignored", 50016, false, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_NONBLOCK_HIT, "location nonblock get hit", ObStatClassIds::CACHE, "location nonblock get hit", 50017, false, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_NONBLOCK_MISS, "location nonblock get miss", ObStatClassIds::CACHE, "location nonblock get miss", 50018, false, true)
//STAT_EVENT_ADD_DEF(LOCATION_CACHE_RPC_CHECK_LEADER, "location check leader count", ObStatClassIds::CACHE, "location check leader count", 50019, true, true)
//STAT_EVENT_ADD_DEF(LOCATION_CACHE_RPC_CHECK_MEMBER, "location check member count", ObStatClassIds::CACHE, "location check member count", 50020, true, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_RPC_CHECK, "location cache rpc renew count", ObStatClassIds::CACHE, "location cache rpc renew count", 50021, false, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_RENEW, "location cache renew", ObStatClassIds::CACHE, "location cache renew", 50022, false, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_RENEW_IGNORED, "location cache renew ignored", ObStatClassIds::CACHE, "location cache renew ignored", 50023, false, true)
//STAT_EVENT_ADD_DEF(MMAP_COUNT, "mmap count", ObStatClassIds::CACHE, "mmap count", 50024, true, true)
//STAT_EVENT_ADD_DEF(MUNMAP_COUNT, "munmap count", ObStatClassIds::CACHE, "munmap count", 50025, true, true)
//STAT_EVENT_ADD_DEF(MMAP_SIZE, "mmap size", ObStatClassIds::CACHE, "mmap size", 50026, true, true)
//STAT_EVENT_ADD_DEF(MUNMAP_SIZE, "munmap size", ObStatClassIds::CACHE, "munmap size", 50027, true, true)
STAT_EVENT_ADD_DEF(KVCACHE_SYNC_WASH_TIME, "kvcache sync wash time", ObStatClassIds::CACHE, "kvcache sync wash time", 50028, false, true)
STAT_EVENT_ADD_DEF(KVCACHE_SYNC_WASH_COUNT, "kvcache sync wash count", ObStatClassIds::CACHE, "kvcache sync wash count", 50029, false, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_RPC_RENEW_FAIL, "location cache rpc renew fail count", ObStatClassIds::CACHE, "location cache rpc renew fail count", 50030, false, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_SQL_RENEW, "location cache sql renew count", ObStatClassIds::CACHE, "location cache sql renew count", 50031, false, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_IGNORE_RPC_RENEW, "location cache ignore rpc renew count", ObStatClassIds::CACHE, "location cache ignore rpc renew count", 50032, false, true)
STAT_EVENT_ADD_DEF(FUSE_ROW_CACHE_HIT, "fuse row cache hit", ObStatClassIds::CACHE, "fuse row cache hit", 50033, true, true)
STAT_EVENT_ADD_DEF(FUSE_ROW_CACHE_MISS, "fuse row cache miss", ObStatClassIds::CACHE, "fuse row cache miss", 50034, true, true)
STAT_EVENT_ADD_DEF(SCHEMA_CACHE_HIT, "schema cache hit", ObStatClassIds::CACHE, "schema cache hit", 50035, false, true)
STAT_EVENT_ADD_DEF(SCHEMA_CACHE_MISS, "schema cache miss", ObStatClassIds::CACHE, "schema cache miss", 50036, false, true)
STAT_EVENT_ADD_DEF(TABLET_LS_CACHE_HIT, "tablet ls cache hit", ObStatClassIds::CACHE, "tablet ls cache hit", 50037, false, true)
STAT_EVENT_ADD_DEF(TABLET_LS_CACHE_MISS, "tablet ls cache miss", ObStatClassIds::CACHE, "tablet ls cache miss", 50038, false, true)
// obsoleted part from INDEX_CLOG_CACHE_HIT to USER_TABLE_STAT_CACHE_MISS
STAT_EVENT_ADD_DEF(INDEX_CLOG_CACHE_HIT, "index clog cache hit", ObStatClassIds::CACHE, "index clog cache hit", 50039, false, true)
STAT_EVENT_ADD_DEF(INDEX_CLOG_CACHE_MISS, "index clog cache miss", ObStatClassIds::CACHE, "index clog cache miss", 50040, false, true)
STAT_EVENT_ADD_DEF(USER_TAB_COL_STAT_CACHE_HIT, "user tab col stat cache hit", ObStatClassIds::CACHE, "user tab col stat cache hit", 50041, false, true)
STAT_EVENT_ADD_DEF(USER_TAB_COL_STAT_CACHE_MISS, "user tab col stat cache miss", ObStatClassIds::CACHE, "user tab col stat cache miss", 50042, false, true)
STAT_EVENT_ADD_DEF(USER_TABLE_STAT_CACHE_HIT, "user table stat cache hit", ObStatClassIds::CACHE, "user table stat cache hit", 50043, false, true)
STAT_EVENT_ADD_DEF(USER_TABLE_STAT_CACHE_MISS, "user table stat cache miss", ObStatClassIds::CACHE, "user table stat cache miss", 50044, false, true)
// obsoleted part from CLOG_CACHE_HIT to USER_TABLE_STAT_CACHE_MISS
STAT_EVENT_ADD_DEF(OPT_TABLE_STAT_CACHE_HIT, "opt table stat cache hit", ObStatClassIds::CACHE, "opt table stat cache hit", 50045, false, true)
STAT_EVENT_ADD_DEF(OPT_TABLE_STAT_CACHE_MISS, "opt table stat cache miss", ObStatClassIds::CACHE, "opt table stat cache miss", 50046, false, true)
STAT_EVENT_ADD_DEF(OPT_COLUMN_STAT_CACHE_HIT, "opt column stat cache hit", ObStatClassIds::CACHE, "opt column stat cache hit", 50047, false, true)
STAT_EVENT_ADD_DEF(OPT_COLUMN_STAT_CACHE_MISS, "opt column stat cache miss", ObStatClassIds::CACHE, "opt column stat cache miss", 50048, false, true)
STAT_EVENT_ADD_DEF(TMP_PAGE_CACHE_HIT, "tmp page cache hit", ObStatClassIds::CACHE, "tmp page cache hit", 50049, false, true)
STAT_EVENT_ADD_DEF(TMP_PAGE_CACHE_MISS, "tmp page cache miss", ObStatClassIds::CACHE, "tmp page cache miss", 50050, false, true)
STAT_EVENT_ADD_DEF(TMP_BLOCK_CACHE_HIT, "tmp block cache hit", ObStatClassIds::CACHE, "tmp block cache hit", 50051, false, true)
STAT_EVENT_ADD_DEF(TMP_BLOCK_CACHE_MISS, "tmp block cache miss", ObStatClassIds::CACHE, "tmp block cache miss", 50052, false, true)
STAT_EVENT_ADD_DEF(SECONDARY_META_CACHE_HIT, "secondary meta cache hit", ObStatClassIds::CACHE, "secondary meta cache hit", 50053, false, true)
STAT_EVENT_ADD_DEF(SECONDARY_META_CACHE_MISS, "secondary meta cache miss", ObStatClassIds::CACHE, "secondary meta cache miss", 50054, false, true)
STAT_EVENT_ADD_DEF(OPT_DS_STAT_CACHE_HIT, "opt ds stat cache hit", ObStatClassIds::CACHE, "opt ds stat cache hit", 50055, false, true)
STAT_EVENT_ADD_DEF(OPT_DS_STAT_CACHE_MISS, "opt ds stat cache miss", ObStatClassIds::CACHE, "opt ds stat cache miss", 50056, false, true)
STAT_EVENT_ADD_DEF(STORAGE_META_CACHE_HIT, "storage meta cache hit", ObStatClassIds::CACHE, "storage meta cache hit", 50057, true, true)
STAT_EVENT_ADD_DEF(STORAGE_META_CACHE_MISS, "storage meta cache miss", ObStatClassIds::CACHE, "storage meta cache miss", 50058, true, true)
STAT_EVENT_ADD_DEF(TABLET_CACHE_HIT, "tablet cache hit", ObStatClassIds::CACHE, "tablet cache hit", 50059, true, true)
STAT_EVENT_ADD_DEF(TABLET_CACHE_MISS, "tablet cache miss", ObStatClassIds::CACHE, "tablet cache miss", 50060, true, true)

STAT_EVENT_ADD_DEF(SCHEMA_HISTORY_CACHE_HIT, "schema history cache hit", ObStatClassIds::CACHE, "schema cache history hit", 50061, false, true)
STAT_EVENT_ADD_DEF(SCHEMA_HISTORY_CACHE_MISS, "schema history cache miss", ObStatClassIds::CACHE, "schema cache history miss", 50062, false, true)

// STORAGE
//STAT_EVENT_ADD_DEF(MEMSTORE_LOGICAL_READS, "MEMSTORE_LOGICAL_READS", STORAGE, "MEMSTORE_LOGICAL_READS")
//STAT_EVENT_ADD_DEF(MEMSTORE_LOGICAL_BYTES, "MEMSTORE_LOGICAL_BYTES", STORAGE, "MEMSTORE_LOGICAL_BYTES")
//STAT_EVENT_ADD_DEF(SSTABLE_LOGICAL_READS, "SSTABLE_LOGICAL_READS", STORAGE, "SSTABLE_LOGICAL_READS")
//STAT_EVENT_ADD_DEF(SSTABLE_LOGICAL_BYTES, "SSTABLE_LOGICAL_BYTES", STORAGE, "SSTABLE_LOGICAL_BYTES")
STAT_EVENT_ADD_DEF(IO_READ_COUNT, "io read count", ObStatClassIds::STORAGE, "io read count", 60000, true, true)
STAT_EVENT_ADD_DEF(IO_READ_DELAY, "io read delay", ObStatClassIds::STORAGE, "io read delay", 60001, true, true)
STAT_EVENT_ADD_DEF(IO_READ_BYTES, "io read bytes", ObStatClassIds::STORAGE, "io read bytes", 60002, true, true)
STAT_EVENT_ADD_DEF(IO_WRITE_COUNT, "io write count", ObStatClassIds::STORAGE, "io write count", 60003, true, true)
STAT_EVENT_ADD_DEF(IO_WRITE_DELAY, "io write delay", ObStatClassIds::STORAGE, "io write delay", 60004, true, true)
STAT_EVENT_ADD_DEF(IO_WRITE_BYTES, "io write bytes", ObStatClassIds::STORAGE, "io write bytes", 60005, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_SCAN_COUNT, "memstore scan count", ObStatClassIds::STORAGE, "memstore scan count", 60006, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_SCAN_SUCC_COUNT, "memstore scan succ count", ObStatClassIds::STORAGE, "memstore scan succ count", 60007, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_SCAN_FAIL_COUNT, "memstore scan fail count", ObStatClassIds::STORAGE, "memstore scan fail count", 60008, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_GET_COUNT, "memstore get count", ObStatClassIds::STORAGE, "memstore get count", 60009, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_GET_SUCC_COUNT, "memstore get succ count", ObStatClassIds::STORAGE, "memstore get succ count", 60010, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_GET_FAIL_COUNT, "memstore get fail count", ObStatClassIds::STORAGE, "memstore get fail count", 60011, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_APPLY_COUNT, "memstore apply count", ObStatClassIds::STORAGE, "memstore apply count", 60012, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_APPLY_SUCC_COUNT, "memstore apply succ count", ObStatClassIds::STORAGE, "memstore apply succ count", 60013, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_APPLY_FAIL_COUNT, "memstore apply fail count", ObStatClassIds::STORAGE, "memstore apply fail count", 60014, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_GET_TIME, "memstore get time", ObStatClassIds::STORAGE, "memstore get time", 60016, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_SCAN_TIME, "memstore scan time", ObStatClassIds::STORAGE, "memstore scan time", 60017, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_APPLY_TIME, "memstore apply time", ObStatClassIds::STORAGE, "memstore apply time", 60018, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_READ_LOCK_SUCC_COUNT, "memstore read lock succ count", ObStatClassIds::STORAGE, "memstore read lock succ count", 60019, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_READ_LOCK_FAIL_COUNT, "memstore read lock fail count", ObStatClassIds::STORAGE, "memstore read lock fail count", 60020, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_WRITE_LOCK_SUCC_COUNT, "memstore write lock succ count", ObStatClassIds::STORAGE, "memstore write lock succ count", 60021, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_WRITE_LOCK_FAIL_COUNT, "memstore write lock fail count", ObStatClassIds::STORAGE, "memstore write lock fail count", 60022, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_WAIT_WRITE_LOCK_TIME, "memstore wait write lock time", ObStatClassIds::STORAGE, "memstore wait write lock time", 60023, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_WAIT_READ_LOCK_TIME, "memstore wait read lock time", ObStatClassIds::STORAGE, "memstore wait read lock time", 60024, true, true)
STAT_EVENT_ADD_DEF(IO_READ_MICRO_INDEX_COUNT, "io read micro index count", ObStatClassIds::STORAGE, "io read micro index count", 60025, false, true)
STAT_EVENT_ADD_DEF(IO_READ_MICRO_INDEX_BYTES, "io read micro index bytes", ObStatClassIds::STORAGE, "io read micro index bytes", 60026, false, true)
STAT_EVENT_ADD_DEF(IO_READ_PREFETCH_MICRO_COUNT, "io prefetch micro block count", ObStatClassIds::STORAGE, "io prefetch micro block count", 60027, false, true)
STAT_EVENT_ADD_DEF(IO_READ_PREFETCH_MICRO_BYTES, "io prefetch micro block bytes", ObStatClassIds::STORAGE, "io prefetch micro block bytes", 60028, false, true)
STAT_EVENT_ADD_DEF(IO_READ_UNCOMP_MICRO_COUNT, "io read uncompress micro block count", ObStatClassIds::STORAGE, "io read uncompress micro block count", 60029, false, true)
STAT_EVENT_ADD_DEF(IO_READ_UNCOMP_MICRO_BYTES, "io read uncompress micro block bytes", ObStatClassIds::STORAGE, "io read uncompress micro block bytes", 60030, false, true)
STAT_EVENT_ADD_DEF(STORAGE_READ_ROW_COUNT, "storage read row count", ObStatClassIds::STORAGE, "storage read row count", 60031, false, true)
STAT_EVENT_ADD_DEF(STORAGE_DELETE_ROW_COUNT, "storage delete row count", ObStatClassIds::STORAGE, "storage delete row count", 60032, false, true)
STAT_EVENT_ADD_DEF(STORAGE_INSERT_ROW_COUNT, "storage insert row count", ObStatClassIds::STORAGE, "storage insert row count", 60033, false, true)
STAT_EVENT_ADD_DEF(STORAGE_UPDATE_ROW_COUNT, "storage update row count", ObStatClassIds::STORAGE, "storage update row count", 60034, false, true)
STAT_EVENT_ADD_DEF(MEMSTORE_ROW_PURGE_COUNT, "memstore row purge count", ObStatClassIds::STORAGE, "memstore row purge count", 60037, false, true)
STAT_EVENT_ADD_DEF(MEMSTORE_ROW_COMPACTION_COUNT, "memstore row compaction count", ObStatClassIds::STORAGE, "memstore row compaction count", 60038, false, true)

STAT_EVENT_ADD_DEF(IO_READ_QUEUE_DELAY, "io read queue delay", ObStatClassIds::STORAGE, "io read queue delay", 60039, false, true)
STAT_EVENT_ADD_DEF(IO_WRITE_QUEUE_DELAY, "io write queue delay", ObStatClassIds::STORAGE, "io write queue delay", 60040, false, true)
STAT_EVENT_ADD_DEF(IO_READ_CB_QUEUE_DELAY, "io read callback queuing delay", ObStatClassIds::STORAGE, "io read callback queuing delay", 60041, false, true)
STAT_EVENT_ADD_DEF(IO_READ_CB_PROCESS_DELAY, "io read callback process delay", ObStatClassIds::STORAGE, "io read callback process delay", 60042, false, true)
STAT_EVENT_ADD_DEF(BANDWIDTH_IN_THROTTLE, "bandwidth in throttle size", ObStatClassIds::STORAGE, "bandwidth in throttle size", 60051, false, true)
STAT_EVENT_ADD_DEF(BANDWIDTH_OUT_THROTTLE, "bandwidth out throttle size", ObStatClassIds::STORAGE, "bandwidth out throttle size", 60052, false, true)
STAT_EVENT_ADD_DEF(MEMSTORE_READ_ROW_COUNT, "memstore read row count", ObStatClassIds::STORAGE, "memstore read row count", 60056, true, true)
STAT_EVENT_ADD_DEF(SSSTORE_READ_ROW_COUNT, "ssstore read row count", ObStatClassIds::STORAGE, "ssstore read row count", 60057, true, true)

STAT_EVENT_ADD_DEF(MEMSTORE_WRITE_LOCK_WAKENUP_COUNT, "memstore write lock wakenup count in lock_wait_mgr", ObStatClassIds::STORAGE, "memstore write lock wakenup count in lock_wait_mgr", 60068, false, true)


STAT_EVENT_ADD_DEF(EXIST_ROW_EFFECT_READ, "exist row effect read", ObStatClassIds::STORAGE, "exist row effect read", 60075, false, true)
STAT_EVENT_ADD_DEF(EXIST_ROW_EMPTY_READ, "exist row empty read", ObStatClassIds::STORAGE, "exist row empty read", 60076, false, true)
STAT_EVENT_ADD_DEF(GET_ROW_EFFECT_READ, "get row effect read", ObStatClassIds::STORAGE, "get row effect read", 60077, false, true)
STAT_EVENT_ADD_DEF(GET_ROW_EMPTY_READ, "get row empty read", ObStatClassIds::STORAGE, "get row empty read", 60078, false, true)
STAT_EVENT_ADD_DEF(SCAN_ROW_EFFECT_READ, "scan row effect read", ObStatClassIds::STORAGE, "scan row effect read", 60079, false, true)
STAT_EVENT_ADD_DEF(SCAN_ROW_EMPTY_READ, "scan row empty read", ObStatClassIds::STORAGE, "scan row empty read", 60080, false, true)
STAT_EVENT_ADD_DEF(BANDWIDTH_IN_SLEEP_US, "bandwidth in sleep us", ObStatClassIds::STORAGE, "bandwidth in sleep us", 60081, false, true)
STAT_EVENT_ADD_DEF(BANDWIDTH_OUT_SLEEP_US, "bandwidth out sleep us", ObStatClassIds::STORAGE, "bandwidth out sleep us", 60082, false, true)

STAT_EVENT_ADD_DEF(MEMSTORE_WRITE_LOCK_WAIT_TIMEOUT_COUNT, "memstore write lock wait timeout count", ObStatClassIds::STORAGE, "memstore write wait timeout count", 60083, false, true)

STAT_EVENT_ADD_DEF(DATA_BLOCK_READ_CNT, "accessed data micro block count", ObStatClassIds::STORAGE, "accessed data micro block count", 60084, true, true)
STAT_EVENT_ADD_DEF(DATA_BLOCK_CACHE_HIT, "data micro block cache hit", ObStatClassIds::STORAGE, "data micro block cache hit", 60085, true, true)
STAT_EVENT_ADD_DEF(INDEX_BLOCK_READ_CNT, "accessed index micro block count", ObStatClassIds::STORAGE, "accessed index micro block count", 60086, true, true)
STAT_EVENT_ADD_DEF(INDEX_BLOCK_CACHE_HIT, "index micro block cache hit", ObStatClassIds::STORAGE, "index micro block cache hit", 60087, true, true)
STAT_EVENT_ADD_DEF(BLOCKSCAN_BLOCK_CNT, "blockscaned data micro block count", ObStatClassIds::STORAGE, "blockscaned data micro block count", 60088, true, true)
STAT_EVENT_ADD_DEF(BLOCKSCAN_ROW_CNT, "blockscaned row count", ObStatClassIds::STORAGE, "blockscaned row count", 60089, true, true)
STAT_EVENT_ADD_DEF(PUSHDOWN_STORAGE_FILTER_ROW_CNT, "storage filtered row count", ObStatClassIds::STORAGE, "storage filter row count", 60090, true, true)

// backup & restore
STAT_EVENT_ADD_DEF(BACKUP_IO_READ_COUNT, "backup io read count", ObStatClassIds::STORAGE, "backup io read count", 69000, true, true)
STAT_EVENT_ADD_DEF(BACKUP_IO_READ_BYTES, "backup io read bytes", ObStatClassIds::STORAGE, "backup io read bytes", 69001, true, true)
STAT_EVENT_ADD_DEF(BACKUP_IO_WRITE_COUNT, "backup io write count", ObStatClassIds::STORAGE, "backup io write count", 69002, true, true)
STAT_EVENT_ADD_DEF(BACKUP_IO_WRITE_BYTES, "backup io write bytes", ObStatClassIds::STORAGE, "backup io write bytes", 69003, true, true)
STAT_EVENT_ADD_DEF(BACKUP_DELETE_COUNT, "backup delete count", ObStatClassIds::STORAGE, "backup delete count", 69010, true, true)

STAT_EVENT_ADD_DEF(BACKUP_IO_READ_DELAY, "backup io read delay", ObStatClassIds::STORAGE, "backup io read delay", 69012, true, true)
STAT_EVENT_ADD_DEF(BACKUP_IO_WRITE_DELAY, "backup io write delay", ObStatClassIds::STORAGE, "backup io write delay", 69013, true, true)
STAT_EVENT_ADD_DEF(COS_IO_READ_DELAY, "cos io read delay", ObStatClassIds::STORAGE, "cos io read delay", 69014, true, true)
STAT_EVENT_ADD_DEF(COS_IO_WRITE_DELAY, "cos io write delay", ObStatClassIds::STORAGE, "cos io write delay", 69015, true, true)
STAT_EVENT_ADD_DEF(COS_IO_LS_DELAY, "cos io list delay", ObStatClassIds::STORAGE, "cos io list delay", 69016, true, true)
STAT_EVENT_ADD_DEF(BACKUP_DELETE_DELAY, "backup delete delay", ObStatClassIds::STORAGE, "backup delete delay", 69017, true, true)

STAT_EVENT_ADD_DEF(BACKUP_IO_LS_COUNT, "backup io list count", ObStatClassIds::STORAGE, "backup io list count", 69019, true, true)
STAT_EVENT_ADD_DEF(BACKUP_IO_READ_FAIL_COUNT, "backup io read failed count", ObStatClassIds::STORAGE, "backup io read failed count", 69020, true, true)
STAT_EVENT_ADD_DEF(BACKUP_IO_WRITE_FAIL_COUNT, "backup io write failed count", ObStatClassIds::STORAGE, "backup io write failed count", 69021, true, true)
STAT_EVENT_ADD_DEF(BACKUP_IO_DEL_FAIL_COUNT, "backup io delete failed count", ObStatClassIds::STORAGE, "backup io delete failed count", 69022, true, true)
STAT_EVENT_ADD_DEF(BACKUP_IO_LS_FAIL_COUNT, "backup io list failed count", ObStatClassIds::STORAGE, "backup io list failed count", 69023, true, true)

STAT_EVENT_ADD_DEF(BACKUP_TAGGING_COUNT, "backup io tagging count", ObStatClassIds::STORAGE, "backup io tagging count", 69024, true, true)
STAT_EVENT_ADD_DEF(BACKUP_TAGGING_FAIL_COUNT, "backup io tagging failed count", ObStatClassIds::STORAGE, "backup io tagging count", 69025, true, true)

// DEBUG
STAT_EVENT_ADD_DEF(REFRESH_SCHEMA_COUNT, "refresh schema count", ObStatClassIds::DEBUG, "refresh schema count", 70000, false, true)
STAT_EVENT_ADD_DEF(REFRESH_SCHEMA_TIME, "refresh schema time", ObStatClassIds::DEBUG, "refresh schema time", 70001, false, true)
STAT_EVENT_ADD_DEF(INNER_SQL_CONNECTION_EXECUTE_COUNT, "inner sql connection execute count", ObStatClassIds::DEBUG, "inner sql connection execute count", 70002, false, true)
STAT_EVENT_ADD_DEF(INNER_SQL_CONNECTION_EXECUTE_TIME, "inner sql connection execute time", ObStatClassIds::DEBUG, "inner sql connection execute time", 70003, false, true)
STAT_EVENT_ADD_DEF(LS_ALL_TABLE_OPERATOR_GET_COUNT, "log stream table operator get count", ObStatClassIds::DEBUG, "log stream table operator get count", 70006, false, true)
STAT_EVENT_ADD_DEF(LS_ALL_TABLE_OPERATOR_GET_TIME, "log stream table operator get time", ObStatClassIds::DEBUG, "log stream table operator get time", 70007, false, true)

//CLOG 80001 ~ 90000
STAT_EVENT_ADD_DEF(CLOG_RPC_COUNT, "clog rpc count", ObStatClassIds::CLOG, "clog rpc count", 80023, false, true)
STAT_EVENT_ADD_DEF(CLOG_RPC_REQUEST_HANDLE_TIME, "clog rpc request handle time", ObStatClassIds::CLOG, "clog rpc request handle time", 80025, false, true)
STAT_EVENT_ADD_DEF(CLOG_RPC_REQUEST_COUNT, "clog rpc request count", ObStatClassIds::CLOG, "clog rpc request count", 80026, false, true)
STAT_EVENT_ADD_DEF(CLOG_WRITE_COUNT, "clog write count", ObStatClassIds::CLOG, "clog write count", 80040, false, true)
STAT_EVENT_ADD_DEF(CLOG_WRITE_TIME, "clog write time", ObStatClassIds::CLOG, "clog write time", 80041, false, true)
STAT_EVENT_ADD_DEF(CLOG_TRANS_LOG_TOTAL_SIZE, "clog trans log total size", ObStatClassIds::CLOG, "clog trans log total size", 80057, false, true)

// CLOG.EXTLOG 81001 ~ 90000
STAT_EVENT_ADD_DEF(CLOG_EXTLOG_FETCH_LOG_SIZE, "external log service fetch log size", ObStatClassIds::CLOG, "external log service fetch log size", 81001, false, true)
STAT_EVENT_ADD_DEF(CLOG_EXTLOG_FETCH_LOG_COUNT, "external log service fetch log count", ObStatClassIds::CLOG, "external log service fetch log count", 81002, false, true)
STAT_EVENT_ADD_DEF(CLOG_EXTLOG_FETCH_RPC_COUNT, "external log service fetch rpc count", ObStatClassIds::CLOG, "external log service fetch rpc count", 81003, false, true)

//CLOG.REPLAY


//CLOG.PROXY


// ELECTION

//OBSERVER

// rootservice
STAT_EVENT_ADD_DEF(RS_RPC_SUCC_COUNT, "success rpc process", ObStatClassIds::RS, "success rpc process", 110015, false, true)
STAT_EVENT_ADD_DEF(RS_RPC_FAIL_COUNT, "failed rpc process", ObStatClassIds::RS, "failed rpc process", 110016, false, true)
STAT_EVENT_ADD_DEF(RS_BALANCER_SUCC_COUNT, "balancer succ execute count", ObStatClassIds::RS, "balanacer succ execute count", 110017, false, true)
STAT_EVENT_ADD_DEF(RS_BALANCER_FAIL_COUNT, "balancer failed execute count", ObStatClassIds::RS, "balanacer failed execute count", 110018, false, true)

// TABLE API (19xxxx)
// -- retrieve 1900xx
STAT_EVENT_ADD_DEF(TABLEAPI_RETRIEVE_COUNT, "single retrieve execute count", ObStatClassIds::TABLEAPI, "retrieve count", 190001, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_RETRIEVE_TIME, "single retrieve execute time", ObStatClassIds::TABLEAPI, "retrieve time", 190002, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_RETRIEVE_COUNT, "multi retrieve execute count", ObStatClassIds::TABLEAPI, "multi retrieve count", 190003, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_RETRIEVE_TIME, "multi retrieve execute time", ObStatClassIds::TABLEAPI, "multi retrieve time", 190004, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_RETRIEVE_ROW, "multi retrieve rows", ObStatClassIds::TABLEAPI, "multi retrieve rows", 190005, true, true)
// -- insert 1901xx
STAT_EVENT_ADD_DEF(TABLEAPI_INSERT_COUNT, "single insert execute count", ObStatClassIds::TABLEAPI, "insert count", 190101, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_INSERT_TIME, "single insert execute time", ObStatClassIds::TABLEAPI, "insert time", 190102, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_INSERT_COUNT, "multi insert execute count", ObStatClassIds::TABLEAPI, "multi insert count", 190103, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_INSERT_TIME, "multi insert execute time", ObStatClassIds::TABLEAPI, "multi insert time", 190104, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_INSERT_ROW, "multi insert rows", ObStatClassIds::TABLEAPI, "multi insert rows", 190105, true, true)
// -- delete 1902xx
STAT_EVENT_ADD_DEF(TABLEAPI_DELETE_COUNT, "single delete execute count", ObStatClassIds::TABLEAPI, "delete count", 190201, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_DELETE_TIME, "single delete execute time", ObStatClassIds::TABLEAPI, "delete time", 190202, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_DELETE_COUNT, "multi delete execute count", ObStatClassIds::TABLEAPI, "multi delete count", 190203, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_DELETE_TIME, "multi delete execute time", ObStatClassIds::TABLEAPI, "multi delete time", 190204, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_DELETE_ROW, "multi delete rows", ObStatClassIds::TABLEAPI, "multi delete rows", 190205, true, true)
// -- update 1903xx
STAT_EVENT_ADD_DEF(TABLEAPI_UPDATE_COUNT, "single update execute count", ObStatClassIds::TABLEAPI, "update count", 190301, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_UPDATE_TIME, "single update execute time", ObStatClassIds::TABLEAPI, "update time", 190302, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_UPDATE_COUNT, "multi update execute count", ObStatClassIds::TABLEAPI, "multi update count", 190303, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_UPDATE_TIME, "multi update execute time", ObStatClassIds::TABLEAPI, "multi update time", 190304, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_UPDATE_ROW, "multi update rows", ObStatClassIds::TABLEAPI, "multi update rows", 190305, true, true)
// -- insert_or_update 1904xx
STAT_EVENT_ADD_DEF(TABLEAPI_INSERT_OR_UPDATE_COUNT, "single insert_or_update execute count", ObStatClassIds::TABLEAPI, "insert_or_update count", 190401, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_INSERT_OR_UPDATE_TIME, "single insert_or_update execute time", ObStatClassIds::TABLEAPI, "insert_or_update time", 190402, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_INSERT_OR_UPDATE_COUNT, "multi insert_or_update execute count", ObStatClassIds::TABLEAPI, "multi insert_or_update count", 190403, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_INSERT_OR_UPDATE_TIME, "multi insert_or_update execute time", ObStatClassIds::TABLEAPI, "multi insert_or_update time", 190404, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_INSERT_OR_UPDATE_ROW, "multi insert_or_update rows", ObStatClassIds::TABLEAPI, "multi insert_or_update rows", 190405, true, true)
// -- replace 1905xx
STAT_EVENT_ADD_DEF(TABLEAPI_REPLACE_COUNT, "single replace execute count", ObStatClassIds::TABLEAPI, "replace count", 190501, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_REPLACE_TIME, "single replace execute time", ObStatClassIds::TABLEAPI, "replace time", 190502, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_REPLACE_COUNT, "multi replace execute count", ObStatClassIds::TABLEAPI, "multi replace count", 190503, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_REPLACE_TIME, "multi replace execute time", ObStatClassIds::TABLEAPI, "multi replace time", 190504, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_REPLACE_ROW, "multi replace rows", ObStatClassIds::TABLEAPI, "multi replace rows", 190505, true, true)
// -- complex_batch_retrieve 1906xx
STAT_EVENT_ADD_DEF(TABLEAPI_BATCH_RETRIEVE_COUNT, "batch retrieve execute count", ObStatClassIds::TABLEAPI, "batch retrieve count", 190601, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_BATCH_RETRIEVE_TIME, "batch retrieve execute time", ObStatClassIds::TABLEAPI, "batch retrieve time", 190602, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_BATCH_RETRIEVE_ROW, "batch retrieve rows", ObStatClassIds::TABLEAPI, "batch retrieve rows", 190603, true, true)
// -- complex_batch_operation 1907xx
STAT_EVENT_ADD_DEF(TABLEAPI_BATCH_HYBRID_COUNT, "batch hybrid execute count", ObStatClassIds::TABLEAPI, "batch hybrid count", 190701, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_BATCH_HYBRID_TIME, "batch hybrid execute time", ObStatClassIds::TABLEAPI, "batch hybrid time", 190702, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_BATCH_HYBRID_INSERT_OR_UPDATE_ROW, "batch hybrid insert_or_update rows", ObStatClassIds::TABLEAPI, "batch hybrid insert_or_update rows", 190707, true, true)
// -- other 1908xx
STAT_EVENT_ADD_DEF(TABLEAPI_LOGIN_COUNT, "table api login count", ObStatClassIds::TABLEAPI, "login count", 190801, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_TSC_VIOLATE_COUNT, "table api OB_TRANSACTION_SET_VIOLATION count", ObStatClassIds::TABLEAPI, "OB_TRANSACTION_SET_VIOLATION count", 190802, true, true)
// -- query 1909xx
STAT_EVENT_ADD_DEF(TABLEAPI_QUERY_COUNT, "query count", ObStatClassIds::TABLEAPI, "query count", 190901, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_QUERY_TIME, "query time", ObStatClassIds::TABLEAPI, "query time", 190902, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_QUERY_ROW, "query row count", ObStatClassIds::TABLEAPI, "query row count", 190903, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_QUERY_AND_MUTATE_COUNT, "query_and_mutate count", ObStatClassIds::TABLEAPI, "query_and_mutate count", 190904, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_QUERY_AND_MUTATE_TIME, "query_and_mutate time", ObStatClassIds::TABLEAPI, "query_and_mutate time", 190905, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_QUERY_AND_MUTATE_ROW, "query_and_mutate row count", ObStatClassIds::TABLEAPI, "query_and_mutate row count", 190906, true, true)
// -- hbase api 1910xx
STAT_EVENT_ADD_DEF(HBASEAPI_SCAN_COUNT, "hbase scan count", ObStatClassIds::TABLEAPI, "scan count", 191001, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_SCAN_TIME, "hbase scan time", ObStatClassIds::TABLEAPI, "scan time", 191002, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_SCAN_ROW, "hbase scan row count", ObStatClassIds::TABLEAPI, "scan row count", 191003, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_PUT_COUNT, "hbase put count", ObStatClassIds::TABLEAPI, "put count", 191004, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_PUT_TIME, "hbase put time", ObStatClassIds::TABLEAPI, "put time", 191005, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_PUT_ROW, "hbase put row count", ObStatClassIds::TABLEAPI, "put row count", 191006, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_DELETE_COUNT, "hbase delete count", ObStatClassIds::TABLEAPI, "delete count", 191007, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_DELETE_TIME, "hbase delete time", ObStatClassIds::TABLEAPI, "delete time", 191008, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_DELETE_ROW, "hbase delete row count", ObStatClassIds::TABLEAPI, "delete row count", 191009, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_APPEND_COUNT, "hbase append count", ObStatClassIds::TABLEAPI, "append count", 191010, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_APPEND_TIME, "hbase append time", ObStatClassIds::TABLEAPI, "append time", 191011, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_APPEND_ROW, "hbase append row count", ObStatClassIds::TABLEAPI, "append row count", 191012, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_INCREMENT_COUNT, "hbase increment count", ObStatClassIds::TABLEAPI, "increment count", 191013, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_INCREMENT_TIME, "hbase increment time", ObStatClassIds::TABLEAPI, "increment time", 191014, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_INCREMENT_ROW, "hbase increment row count", ObStatClassIds::TABLEAPI, "increment row count", 191015, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_CHECK_PUT_COUNT, "hbase check_put count", ObStatClassIds::TABLEAPI, "check_put count", 191016, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_CHECK_PUT_TIME, "hbase check_put time", ObStatClassIds::TABLEAPI, "check_put time", 191017, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_CHECK_PUT_ROW, "hbase check_put row count", ObStatClassIds::TABLEAPI, "check_put row count", 191018, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_CHECK_DELETE_COUNT, "hbase check_delete count", ObStatClassIds::TABLEAPI, "check_delete count", 191019, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_CHECK_DELETE_TIME, "hbase check_delete time", ObStatClassIds::TABLEAPI, "check_delete time", 191020, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_CHECK_DELETE_ROW, "hbase check_delete row count", ObStatClassIds::TABLEAPI, "check_delete row count", 191021, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_HYBRID_COUNT, "hbase hybrid count", ObStatClassIds::TABLEAPI, "hybrid count", 191022, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_HYBRID_TIME, "hbase hybrid time", ObStatClassIds::TABLEAPI, "hybrid time", 191023, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_HYBRID_ROW, "hbase hybrid row count", ObStatClassIds::TABLEAPI, "hybrid row count", 191024, true, true)

// -- table increment 1911xx
STAT_EVENT_ADD_DEF(TABLEAPI_INCREMENT_COUNT, "single increment execute count", ObStatClassIds::TABLEAPI, "increment count", 191101, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_INCREMENT_TIME, "single increment execute time", ObStatClassIds::TABLEAPI, "increment time", 191102, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_INCREMENT_COUNT, "multi increment execute count", ObStatClassIds::TABLEAPI, "multi increment count", 191103, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_INCREMENT_TIME, "multi increment execute time", ObStatClassIds::TABLEAPI, "multi increment time", 191104, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_INCREMENT_ROW, "multi increment rows", ObStatClassIds::TABLEAPI, "multi increment rows", 191105, true, true)

// -- table append 1912xx
STAT_EVENT_ADD_DEF(TABLEAPI_APPEND_COUNT, "single append execute count", ObStatClassIds::TABLEAPI, "append count", 191201, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_APPEND_TIME, "single append execute time", ObStatClassIds::TABLEAPI, "append time", 191202, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_APPEND_COUNT, "multi append execute count", ObStatClassIds::TABLEAPI, "multi append count", 191203, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_APPEND_TIME, "multi append execute time", ObStatClassIds::TABLEAPI, "multi append time", 191204, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_APPEND_ROW, "multi append rows", ObStatClassIds::TABLEAPI, "multi append rows", 191205, true, true)

// sys_time_model related (20xxxx)
STAT_EVENT_ADD_DEF(SYS_TIME_MODEL_DB_TIME, "DB time", ObStatClassIds::SYS, "DB time", 200001, false, true)
STAT_EVENT_ADD_DEF(SYS_TIME_MODEL_DB_CPU, "DB CPU", ObStatClassIds::SYS, "DB CPU", 200002, false, true)
STAT_EVENT_ADD_DEF(SYS_TIME_MODEL_DB_INNER_TIME, "DB inner sql time", ObStatClassIds::SYS, "DB time", 200003, false, true)
STAT_EVENT_ADD_DEF(SYS_TIME_MODEL_DB_INNER_CPU, "DB inner sql CPU", ObStatClassIds::SYS, "DB CPU", 200004, false, true)
STAT_EVENT_ADD_DEF(SYS_TIME_MODEL_BKGD_TIME, "background elapsed time", ObStatClassIds::SYS, "background elapsed time", 200005, false, true)
STAT_EVENT_ADD_DEF(SYS_TIME_MODEL_BKGD_CPU, "background cpu time", ObStatClassIds::SYS, "background cpu time", 200006, false, true)
STAT_EVENT_ADD_DEF(SYS_TIME_MODEL_BACKUP_CPU, "(backup/restore) cpu time", ObStatClassIds::SYS, "(backup/restore) cpu time", 200007, false, true)
STAT_EVENT_ADD_DEF(SYS_TIME_MODEL_ENCRYPT_TIME, "Tablespace encryption elapsed time", ObStatClassIds::SYS, "Tablespace encryption elapsed time", 200008, false, true)
STAT_EVENT_ADD_DEF(SYS_TIME_MODEL_ENCRYPT_CPU, "Tablespace encryption cpu time", ObStatClassIds::SYS, "Tablespace encryption cpu time", 200009, false, true)

//end
STAT_EVENT_ADD_DEF(STAT_EVENT_ADD_END, "event add end", ObStatClassIds::DEBUG, "event add end", 1, false, false)

#endif

#ifdef STAT_EVENT_SET_DEF
// NETWORK
STAT_EVENT_SET_DEF(STANDBY_FETCH_LOG_BYTES, "standby fetch log bytes", ObStatClassIds::NETWORK, "standby fetch log bytes" , 110000, false, true)
STAT_EVENT_SET_DEF(STANDBY_FETCH_LOG_BANDWIDTH_LIMIT, "standby fetch log bandwidth limit", ObStatClassIds::NETWORK, "standby fetch log bandwidth limit" , 110001, false, true)

// QUEUE

// TRANS

// SQL

// CACHE
STAT_EVENT_SET_DEF(LOCATION_CACHE_SIZE, "location cache size", ObStatClassIds::CACHE, "location cache size", 120000, false, true)
STAT_EVENT_SET_DEF(TABLET_LS_CACHE_SIZE, "tablet ls cache size", ObStatClassIds::CACHE, "tablet ls cache size", 120001, false, true)
STAT_EVENT_SET_DEF(OPT_TAB_STAT_CACHE_SIZE, "table stat cache size", ObStatClassIds::CACHE, "table stat cache size", 120002, false, true)
STAT_EVENT_SET_DEF(OPT_TAB_COL_STAT_CACHE_SIZE, "table col stat cache size", ObStatClassIds::CACHE, "table col stat cache size", 120003, false, true)
STAT_EVENT_SET_DEF(INDEX_BLOCK_CACHE_SIZE, "index block cache size", ObStatClassIds::CACHE, "index block cache size", 120004, false, true)
STAT_EVENT_SET_DEF(SYS_BLOCK_CACHE_SIZE, "sys block cache size", ObStatClassIds::CACHE, "sys block cache size", 120005, false, true)
STAT_EVENT_SET_DEF(USER_BLOCK_CACHE_SIZE, "user block cache size", ObStatClassIds::CACHE, "user block cache size", 120006, false, true)
STAT_EVENT_SET_DEF(USER_ROW_CACHE_SIZE, "user row cache size", ObStatClassIds::CACHE, "user row cache size", 120008, false, true)
STAT_EVENT_SET_DEF(BLOOM_FILTER_CACHE_SIZE, "bloom filter cache size", ObStatClassIds::CACHE, "bloom filter cache size", 120009, false, true)

// STORAGE
STAT_EVENT_SET_DEF(ACTIVE_MEMSTORE_USED, "active memstore used", ObStatClassIds::STORAGE, "active memstore used", 130000, false, true)
STAT_EVENT_SET_DEF(TOTAL_MEMSTORE_USED, "total memstore used", ObStatClassIds::STORAGE, "total memstore used", 130001, false, true)
STAT_EVENT_SET_DEF(MAJOR_FREEZE_TRIGGER, "major freeze trigger", ObStatClassIds::STORAGE, "major freeze trigger", 130002, false, true)
STAT_EVENT_SET_DEF(MEMSTORE_LIMIT, "memstore limit", ObStatClassIds::STORAGE, "memstore limit", 130004, false, true)

// RESOURCE
//STAT_EVENT_SET_DEF(SRV_DISK_SIZE, "SRV_DISK_SIZE", RESOURCE, "SRV_DISK_SIZE")
//STAT_EVENT_SET_DEF(DISK_USAGE, "DISK_USAGE", RESOURCE, "DISK_USAGE")
//STAT_EVENT_SET_DEF(SRV_MEMORY_SIZE, "SRV_MEMORY_SIZE", RESOURCE, "SRV_MEMORY_SIZE")
STAT_EVENT_SET_DEF(MIN_MEMORY_SIZE, "min memory size", ObStatClassIds::RESOURCE, "min memory size", 140001, false, true)
STAT_EVENT_SET_DEF(MAX_MEMORY_SIZE, "max memory size", ObStatClassIds::RESOURCE, "max memory size", 140002, false, true)
STAT_EVENT_SET_DEF(MEMORY_USAGE, "memory usage", ObStatClassIds::RESOURCE, "memory usage", 140003, false, true)
//STAT_EVENT_SET_DEF(SRV_CPUS, "SRV_CPUS", RESOURCE, "SRV_CPUS")
STAT_EVENT_SET_DEF(MIN_CPUS, "min cpus", ObStatClassIds::RESOURCE, "min cpus", 140004, false, true)
STAT_EVENT_SET_DEF(MAX_CPUS, "max cpus", ObStatClassIds::RESOURCE, "max cpus", 140005, false, true)
STAT_EVENT_SET_DEF(CPU_USAGE, "cpu usage", ObStatClassIds::RESOURCE, "cpu usage", 140006, false, true)
STAT_EVENT_SET_DEF(DISK_USAGE, "disk usage", ObStatClassIds::RESOURCE, "disk usage", 140007, false, true)
STAT_EVENT_SET_DEF(MEMORY_USED_SIZE, "observer memory used size", ObStatClassIds::RESOURCE, "observer memory used size", 140008, false, true)
STAT_EVENT_SET_DEF(MEMORY_FREE_SIZE, "observer memory free size", ObStatClassIds::RESOURCE, "observer memory free size", 140009, false, true)
STAT_EVENT_SET_DEF(IS_MINI_MODE, "is mini mode", ObStatClassIds::RESOURCE, "is mini mode", 140010, false, true)
STAT_EVENT_SET_DEF(MEMORY_HOLD_SIZE, "observer memory hold size", ObStatClassIds::RESOURCE, "observer memory hold size", 140011, false, true)
STAT_EVENT_SET_DEF(WORKER_TIME, "worker time", ObStatClassIds::RESOURCE, "worker time", 140012, false, true)
STAT_EVENT_SET_DEF(CPU_TIME, "cpu time", ObStatClassIds::RESOURCE, "cpu time", 140013, false, true)

//CLOG

// DEBUG
STAT_EVENT_SET_DEF(OBLOGGER_WRITE_SIZE, "oblogger log bytes", ObStatClassIds::DEBUG, "oblogger log bytes", 160001, false, true)
STAT_EVENT_SET_DEF(ELECTION_WRITE_SIZE, "election log bytes", ObStatClassIds::DEBUG, "election log bytes", 160002, false, true)
STAT_EVENT_SET_DEF(ROOTSERVICE_WRITE_SIZE, "rootservice log bytes", ObStatClassIds::DEBUG, "rootservice log bytes", 160003, false, true)
STAT_EVENT_SET_DEF(OBLOGGER_TOTAL_WRITE_COUNT, "oblogger total log count", ObStatClassIds::DEBUG, "oblogger total log count", 160004, false, true)
STAT_EVENT_SET_DEF(ELECTION_TOTAL_WRITE_COUNT, "election total log count", ObStatClassIds::DEBUG, "election total log count", 160005, false, true)
STAT_EVENT_SET_DEF(ROOTSERVICE_TOTAL_WRITE_COUNT, "rootservice total log count", ObStatClassIds::DEBUG, "rootservice total log count", 160006, false, true)
STAT_EVENT_SET_DEF(ASYNC_ERROR_LOG_DROPPED_COUNT, "async error log dropped count", ObStatClassIds::DEBUG, "async error log dropped count", 160019, false, true)
STAT_EVENT_SET_DEF(ASYNC_WARN_LOG_DROPPED_COUNT, "async warn log dropped count", ObStatClassIds::DEBUG, "async warn log dropped count", 160020, false, true)
STAT_EVENT_SET_DEF(ASYNC_INFO_LOG_DROPPED_COUNT, "async info log dropped count", ObStatClassIds::DEBUG, "async info log dropped count", 160021, false, true)
STAT_EVENT_SET_DEF(ASYNC_TRACE_LOG_DROPPED_COUNT, "async trace log dropped count", ObStatClassIds::DEBUG, "async trace log dropped count", 160022, false, true)
STAT_EVENT_SET_DEF(ASYNC_DEBUG_LOG_DROPPED_COUNT, "async debug log dropped count", ObStatClassIds::DEBUG, "async debug log dropped count", 160023, false, true)
STAT_EVENT_SET_DEF(ASYNC_LOG_FLUSH_SPEED, "async log flush speed", ObStatClassIds::DEBUG, "async log flush speed", 160024, false, true)
STAT_EVENT_SET_DEF(ASYNC_GENERIC_LOG_WRITE_COUNT, "async generic log write count", ObStatClassIds::DEBUG, "async generic log write count", 160025, false, true)
STAT_EVENT_SET_DEF(ASYNC_USER_REQUEST_LOG_WRITE_COUNT, "async user request log write count", ObStatClassIds::DEBUG, "async user request log write count", 160026, false, true)
STAT_EVENT_SET_DEF(ASYNC_DATA_MAINTAIN_LOG_WRITE_COUNT, "async data maintain log write count", ObStatClassIds::DEBUG, "async data maintain log write count", 160027, false, true)
STAT_EVENT_SET_DEF(ASYNC_ROOT_SERVICE_LOG_WRITE_COUNT, "async root service log write count", ObStatClassIds::DEBUG, "async root service log write count", 160028, false, true)
STAT_EVENT_SET_DEF(ASYNC_SCHEMA_LOG_WRITE_COUNT, "async schema log write count", ObStatClassIds::DEBUG, "async schema log write count", 160029, false, true)
STAT_EVENT_SET_DEF(ASYNC_FORCE_ALLOW_LOG_WRITE_COUNT, "async force allow log write count", ObStatClassIds::DEBUG, "async force allow log write count", 160030, false, true)
STAT_EVENT_SET_DEF(ASYNC_GENERIC_LOG_DROPPED_COUNT, "async generic log dropped count", ObStatClassIds::DEBUG, "async generic log dropped count", 160031, false, true)
STAT_EVENT_SET_DEF(ASYNC_USER_REQUEST_LOG_DROPPED_COUNT, "async user request log dropped count", ObStatClassIds::DEBUG, "async user request log dropped count", 160032, false, true)
STAT_EVENT_SET_DEF(ASYNC_DATA_MAINTAIN_LOG_DROPPED_COUNT, "async data maintain log dropped count", ObStatClassIds::DEBUG, "async data maintain log dropped count", 160033, false, true)
STAT_EVENT_SET_DEF(ASYNC_ROOT_SERVICE_LOG_DROPPED_COUNT, "async root service log dropped count", ObStatClassIds::DEBUG, "async root service log dropped count", 160034, false, true)
STAT_EVENT_SET_DEF(ASYNC_SCHEMA_LOG_DROPPED_COUNT, "async schema log dropped count", ObStatClassIds::DEBUG, "async schema log dropped count", 160035, false, true)
STAT_EVENT_SET_DEF(ASYNC_FORCE_ALLOW_LOG_DROPPED_COUNT, "async force allow log dropped count", ObStatClassIds::DEBUG, "async force allow log dropped count", 160036, false, true)


//OBSERVER
STAT_EVENT_SET_DEF(OBSERVER_PARTITION_TABLE_UPATER_USER_QUEUE_SIZE, "observer partition table updater user table queue size", ObStatClassIds::OBSERVER, "observer partition table updater user table queue size", 170001, false, true)
STAT_EVENT_SET_DEF(OBSERVER_PARTITION_TABLE_UPATER_SYS_QUEUE_SIZE, "observer partition table updater system table queue size", ObStatClassIds::OBSERVER, "observer partition table updater system table queue size", 170002, false, true)
STAT_EVENT_SET_DEF(OBSERVER_PARTITION_TABLE_UPATER_CORE_QUEUE_SIZE, "observer partition table updater core table queue size", ObStatClassIds::OBSERVER, "observer partition table updater core table queue size", 170003, false, true)

// rootservice
STAT_EVENT_SET_DEF(RS_START_SERVICE_TIME, "rootservice start time", ObStatClassIds::RS, "rootservice start time", 180001, false, true)
// END
STAT_EVENT_SET_DEF(STAT_EVENT_SET_END, "event set end", ObStatClassIds::DEBUG, "event set end", 300000, false, false)
#endif

#ifndef OB_STAT_EVENT_DEFINE_H_
#define OB_STAT_EVENT_DEFINE_H_

#include "lib/time/ob_time_utility.h"
#include "lib/statistic_event/ob_stat_class.h"


namespace oceanbase
{
namespace common
{
static const int64_t MAX_STAT_EVENT_NAME_LENGTH = 64;
static const int64_t MAX_STAT_EVENT_PARAM_LENGTH = 64;

struct ObStatEventIds
{
  enum ObStatEventIdEnum
  {
#define STAT_EVENT_ADD_DEF(def, name, stat_class, display_name, stat_id, summary_in_session, can_visible) def,
#include "lib/statistic_event/ob_stat_event.h"
#undef STAT_EVENT_ADD_DEF
#define STAT_EVENT_SET_DEF(def, name, stat_class, display_name, stat_id, summary_in_session, can_visible) def,
#include "lib/statistic_event/ob_stat_event.h"
#undef STAT_EVENT_SET_DEF
  };
};

struct ObStatEventAddStat
{
  //value for a stat
  int64_t stat_value_;
  ObStatEventAddStat();
  int add(const ObStatEventAddStat &other);
  int add(int64_t value);
  int64_t get_stat_value();
  void reset();
  inline bool is_valid() const { return true; }
};

inline int64_t ObStatEventAddStat::get_stat_value()
{
  return stat_value_;
}

struct ObStatEventSetStat
{
  //value for a stat
  int64_t stat_value_;
  int64_t set_time_;
  ObStatEventSetStat();
  int add(const ObStatEventSetStat &other);
  void set(int64_t value);
  int64_t get_stat_value();
  void reset();
  inline bool is_valid() const { return stat_value_ > 0; }
};

inline int64_t ObStatEventSetStat::get_stat_value()
{
  return stat_value_;
}

inline void ObStatEventSetStat::set(int64_t value)
{
  stat_value_ = value;
  set_time_ = ObTimeUtility::current_time();
}

struct ObStatEvent
{
  char name_[MAX_STAT_EVENT_NAME_LENGTH];
  int64_t stat_class_;
  char display_name_[MAX_STAT_EVENT_NAME_LENGTH];
  int64_t stat_id_;
  bool summary_in_session_;
  bool can_visible_;
};


extern const ObStatEvent OB_STAT_EVENTS[];

}
}

#endif /* OB_WAIT_EVENT_DEFINE_H_ */
