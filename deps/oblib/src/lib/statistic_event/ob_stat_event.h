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
STAT_EVENT_ADD_DEF(RPC_PACKET_IN, "rpc packet in", ObStatClassIds::NETWORK, 10000, true, true)
STAT_EVENT_ADD_DEF(RPC_PACKET_IN_BYTES, "rpc packet in bytes", ObStatClassIds::NETWORK, 10001, false, true)
STAT_EVENT_ADD_DEF(RPC_PACKET_OUT, "rpc packet out", ObStatClassIds::NETWORK, 10002, true, true)
STAT_EVENT_ADD_DEF(RPC_PACKET_OUT_BYTES, "rpc packet out bytes", ObStatClassIds::NETWORK, 10003, false, true)
STAT_EVENT_ADD_DEF(RPC_DELIVER_FAIL, "rpc deliver fail", ObStatClassIds::NETWORK, 10004, false, true)
STAT_EVENT_ADD_DEF(RPC_NET_DELAY, "rpc net delay", ObStatClassIds::NETWORK, 10005, false, true)
STAT_EVENT_ADD_DEF(RPC_NET_FRAME_DELAY, "rpc net frame delay", ObStatClassIds::NETWORK, 10006, false, true)
STAT_EVENT_ADD_DEF(MYSQL_PACKET_IN, "mysql packet in", ObStatClassIds::NETWORK, 10007, false, true)
STAT_EVENT_ADD_DEF(MYSQL_PACKET_IN_BYTES, "mysql packet in bytes", ObStatClassIds::NETWORK, 10008, false, true)
STAT_EVENT_ADD_DEF(MYSQL_PACKET_OUT, "mysql packet out", ObStatClassIds::NETWORK, 10009, false, true)
STAT_EVENT_ADD_DEF(MYSQL_PACKET_OUT_BYTES, "mysql packet out bytes", ObStatClassIds::NETWORK, 10010, false, true)
STAT_EVENT_ADD_DEF(MYSQL_DELIVER_FAIL, "mysql deliver fail", ObStatClassIds::NETWORK, 10011, false, true)
STAT_EVENT_ADD_DEF(RPC_COMPRESS_ORIGINAL_PACKET_CNT, "rpc compress original packet cnt", ObStatClassIds::NETWORK, 10012, false, true)
STAT_EVENT_ADD_DEF(RPC_COMPRESS_COMPRESSED_PACKET_CNT, "rpc compress compressed packet cnt", ObStatClassIds::NETWORK, 10013, false, true)
STAT_EVENT_ADD_DEF(RPC_COMPRESS_ORIGINAL_SIZE, "rpc compress original size", ObStatClassIds::NETWORK, 10014, false, true)
STAT_EVENT_ADD_DEF(RPC_COMPRESS_COMPRESSED_SIZE, "rpc compress compressed size", ObStatClassIds::NETWORK, 10015, false, true)

STAT_EVENT_ADD_DEF(RPC_STREAM_COMPRESS_ORIGINAL_PACKET_CNT, "rpc stream compress original packet cnt", ObStatClassIds::NETWORK, 10016, false, true)
STAT_EVENT_ADD_DEF(RPC_STREAM_COMPRESS_COMPRESSED_PACKET_CNT, "rpc stream compress compressed packet cnt", ObStatClassIds::NETWORK, 10017, false, true)
STAT_EVENT_ADD_DEF(RPC_STREAM_COMPRESS_ORIGINAL_SIZE, "rpc stream compress original size", ObStatClassIds::NETWORK, 10018, false, true)
STAT_EVENT_ADD_DEF(RPC_STREAM_COMPRESS_COMPRESSED_SIZE, "rpc stream compress compressed size", ObStatClassIds::NETWORK, 10019, false, true)

// QUEUE
// STAT_EVENT_ADD_DEF(REQUEST_QUEUED_COUNT, "REQUEST_QUEUED_COUNT", QUEUE, "REQUEST_QUEUED_COUNT")
STAT_EVENT_ADD_DEF(REQUEST_ENQUEUE_COUNT, "request enqueue count", ObStatClassIds::QUEUE, 20000, false, true)
STAT_EVENT_ADD_DEF(REQUEST_DEQUEUE_COUNT, "request dequeue count", ObStatClassIds::QUEUE, 20001, false, true)
STAT_EVENT_ADD_DEF(REQUEST_QUEUE_TIME, "request queue time", ObStatClassIds::QUEUE, 20002, false, true)

// TRANS
STAT_EVENT_ADD_DEF(TRANS_COMMIT_LOG_SYNC_TIME, "trans commit log sync time", ObStatClassIds::TRANS, 30000, false, true)
STAT_EVENT_ADD_DEF(TRANS_COMMIT_LOG_SYNC_COUNT, "trans commit log sync count", ObStatClassIds::TRANS, 30001, false, true)
STAT_EVENT_ADD_DEF(TRANS_COMMIT_LOG_SUBMIT_COUNT, "trans commit log submit count", ObStatClassIds::TRANS, 30002, false, true)
STAT_EVENT_ADD_DEF(TRANS_SYSTEM_TRANS_COUNT, "trans system trans count", ObStatClassIds::TRANS, 30003, false, true)
STAT_EVENT_ADD_DEF(TRANS_USER_TRANS_COUNT, "trans user trans count", ObStatClassIds::TRANS, 30004, false, true)
STAT_EVENT_ADD_DEF(TRANS_START_COUNT, "trans start count", ObStatClassIds::TRANS, 30005, false, true)
STAT_EVENT_ADD_DEF(TRANS_TOTAL_USED_TIME, "trans total used time", ObStatClassIds::TRANS, 30006, false, true)
STAT_EVENT_ADD_DEF(TRANS_COMMIT_COUNT, "trans commit count", ObStatClassIds::TRANS, 30007, false, true)
STAT_EVENT_ADD_DEF(TRANS_COMMIT_TIME, "trans commit time", ObStatClassIds::TRANS, 30008, false, true)
STAT_EVENT_ADD_DEF(TRANS_ROLLBACK_COUNT, "trans rollback count", ObStatClassIds::TRANS, 30009, false, true)
STAT_EVENT_ADD_DEF(TRANS_ROLLBACK_TIME, "trans rollback time", ObStatClassIds::TRANS, 30010, false, true)
STAT_EVENT_ADD_DEF(TRANS_TIMEOUT_COUNT, "trans timeout count", ObStatClassIds::TRANS, 30011, false, true)
STAT_EVENT_ADD_DEF(TRANS_LOCAL_COUNT, "trans local trans count", ObStatClassIds::TRANS, 30012, false, true)
STAT_EVENT_ADD_DEF(TRANS_DIST_COUNT, "trans distribute trans count", ObStatClassIds::TRANS, 30013, false, true)
//STAT_EVENT_ADD_DEF(TRANS_DISTRIBUTED_STMT_COUNT, "trans distributed stmt count", TRANS, 30014)
//STAT_EVENT_ADD_DEF(TRANS_LOCAL_STMT_COUNT, "trans local stmt count", TRANS, 30015)
//STAT_EVENT_ADD_DEF(TRANS_REMOTE_STMT_COUNT, "trans remote stmt count", TRANS, 30016)
STAT_EVENT_ADD_DEF(TRANS_READONLY_COUNT, "trans without participant count", ObStatClassIds::TRANS, 30017, false, true)
STAT_EVENT_ADD_DEF(REDO_LOG_REPLAY_COUNT, "redo log replay count", ObStatClassIds::TRANS, 30018, false, true)
STAT_EVENT_ADD_DEF(REDO_LOG_REPLAY_TIME, "redo log replay time", ObStatClassIds::TRANS, 30019, false, true)
STAT_EVENT_ADD_DEF(PREPARE_LOG_REPLAY_COUNT, "prepare log replay count", ObStatClassIds::TRANS, 30020, false, true)
STAT_EVENT_ADD_DEF(PREPARE_LOG_REPLAY_TIME, "prepare log replay time", ObStatClassIds::TRANS, 30021, false, true)
STAT_EVENT_ADD_DEF(COMMIT_LOG_REPLAY_COUNT, "commit log replay count", ObStatClassIds::TRANS, 30022, false, true)
STAT_EVENT_ADD_DEF(COMMIT_LOG_REPLAY_TIME, "commit log replay time", ObStatClassIds::TRANS, 30023, false, true)
STAT_EVENT_ADD_DEF(ABORT_LOG_REPLAY_COUNT, "abort log replay count", ObStatClassIds::TRANS, 30024, false, true)
STAT_EVENT_ADD_DEF(ABORT_LOG_REPLAY_TIME, "abort log replay time", ObStatClassIds::TRANS, 30025, false, true)
STAT_EVENT_ADD_DEF(CLEAR_LOG_REPLAY_COUNT, "clear log replay count", ObStatClassIds::TRANS, 30026, false, true)
STAT_EVENT_ADD_DEF(CLEAR_LOG_REPLAY_TIME, "clear log replay time", ObStatClassIds::TRANS, 30027, false, true)
STAT_EVENT_ADD_DEF(GTS_REQUEST_TOTAL_COUNT, "gts request total count", ObStatClassIds::TRANS, 30053, false, true)
STAT_EVENT_ADD_DEF(GTS_ACQUIRE_TOTAL_TIME, "gts acquire total time", ObStatClassIds::TRANS, 30054, false, true)
STAT_EVENT_ADD_DEF(GTS_ACQUIRE_TOTAL_WAIT_COUNT, "gts acquire total wait count", ObStatClassIds::TRANS, 30056, false, true)
STAT_EVENT_ADD_DEF(GTS_WAIT_ELAPSE_TOTAL_TIME, "gts wait elapse total time", ObStatClassIds::TRANS, 30063, false, true)
STAT_EVENT_ADD_DEF(GTS_WAIT_ELAPSE_TOTAL_COUNT, "gts wait elapse total count", ObStatClassIds::TRANS, 30064, false, true)
STAT_EVENT_ADD_DEF(GTS_WAIT_ELAPSE_TOTAL_WAIT_COUNT, "gts wait elapse total wait count", ObStatClassIds::TRANS, 30065, false, true)
STAT_EVENT_ADD_DEF(GTS_RPC_COUNT, "gts rpc count", ObStatClassIds::TRANS, 30066, false, true)
STAT_EVENT_ADD_DEF(GTS_TRY_ACQUIRE_TOTAL_COUNT, "gts try acquire total count", ObStatClassIds::TRANS, 30067, false, true)
STAT_EVENT_ADD_DEF(GTS_TRY_WAIT_ELAPSE_TOTAL_COUNT, "gts try wait elapse total count", ObStatClassIds::TRANS, 30068, false, true)
STAT_EVENT_ADD_DEF(TRANS_ELR_ENABLE_COUNT, "trans early lock release enable count", ObStatClassIds::TRANS, 30077, false, true)
STAT_EVENT_ADD_DEF(TRANS_ELR_UNABLE_COUNT, "trans early lock release unable count", ObStatClassIds::TRANS, 30078, false, true)
STAT_EVENT_ADD_DEF(READ_ELR_ROW_COUNT, "read elr row count", ObStatClassIds::TRANS, 30079, false, true)
STAT_EVENT_ADD_DEF(TRANS_LOCAL_TOTAL_USED_TIME, "local trans total used time", ObStatClassIds::TRANS, 30080, false, true)
STAT_EVENT_ADD_DEF(TRANS_DIST_TOTAL_USED_TIME, "distributed trans total used time", ObStatClassIds::TRANS, 30081, false, true)
STAT_EVENT_ADD_DEF(TX_DATA_HIT_MINI_CACHE_COUNT, "tx data hit mini cache count", ObStatClassIds::TRANS, 30082, false, true)
STAT_EVENT_ADD_DEF(TX_DATA_HIT_KV_CACHE_COUNT, "tx data hit kv cache count", ObStatClassIds::TRANS, 30083, false, true)
STAT_EVENT_ADD_DEF(TX_DATA_READ_TX_CTX_COUNT, "tx data read tx ctx count", ObStatClassIds::TRANS, 30084, false, true)
STAT_EVENT_ADD_DEF(TX_DATA_READ_TX_DATA_MEMTABLE_COUNT, "tx data read tx data memtable count", ObStatClassIds::TRANS, 30085, false, true)
STAT_EVENT_ADD_DEF(TX_DATA_READ_TX_DATA_SSTABLE_COUNT, "tx data read tx data sstable count", ObStatClassIds::TRANS, 30086, false, true)

// SQL
//STAT_EVENT_ADD_DEF(PLAN_CACHE_HIT, "PLAN_CACHE_HIT", SQL, "PLAN_CACHE_HIT")
//STAT_EVENT_ADD_DEF(PLAN_CACHE_MISS, "PLAN_CACHE_MISS", SQL, "PLAN_CACHE_MISS")
STAT_EVENT_ADD_DEF(SQL_SELECT_COUNT, "sql select count", ObStatClassIds::SQL, 40000, false, true)
STAT_EVENT_ADD_DEF(SQL_SELECT_TIME, "sql select time", ObStatClassIds::SQL, 40001, false, true)
STAT_EVENT_ADD_DEF(SQL_INSERT_COUNT, "sql insert count", ObStatClassIds::SQL, 40002, false, true)
STAT_EVENT_ADD_DEF(SQL_INSERT_TIME, "sql insert time", ObStatClassIds::SQL, 40003, false, true)
STAT_EVENT_ADD_DEF(SQL_REPLACE_COUNT, "sql replace count", ObStatClassIds::SQL, 40004, false, true)
STAT_EVENT_ADD_DEF(SQL_REPLACE_TIME, "sql replace time", ObStatClassIds::SQL, 40005, false, true)
STAT_EVENT_ADD_DEF(SQL_UPDATE_COUNT, "sql update count", ObStatClassIds::SQL, 40006, false, true)
STAT_EVENT_ADD_DEF(SQL_UPDATE_TIME, "sql update time", ObStatClassIds::SQL, 40007, false, true)
STAT_EVENT_ADD_DEF(SQL_DELETE_COUNT, "sql delete count", ObStatClassIds::SQL, 40008, false, true)
STAT_EVENT_ADD_DEF(SQL_DELETE_TIME, "sql delete time", ObStatClassIds::SQL, 40009, false, true)
STAT_EVENT_ADD_DEF(SQL_OTHER_COUNT, "sql other count", ObStatClassIds::SQL, 40018, false, true)
STAT_EVENT_ADD_DEF(SQL_OTHER_TIME, "sql other time", ObStatClassIds::SQL, 40019, false, true)
STAT_EVENT_ADD_DEF(SQL_PS_PREPARE_COUNT, "ps prepare count", ObStatClassIds::SQL, 40020, false, true)
STAT_EVENT_ADD_DEF(SQL_PS_PREPARE_TIME, "ps prepare time", ObStatClassIds::SQL, 40021, false, true)
STAT_EVENT_ADD_DEF(SQL_PS_EXECUTE_COUNT, "ps execute count", ObStatClassIds::SQL, 40022, false, true)
STAT_EVENT_ADD_DEF(SQL_PS_CLOSE_COUNT, "ps close count", ObStatClassIds::SQL, 40023, false, true)
STAT_EVENT_ADD_DEF(SQL_PS_CLOSE_TIME, "ps close time", ObStatClassIds::SQL, 40024, false, true)

STAT_EVENT_ADD_DEF(SQL_OPEN_CURSORS_CURRENT, "opened cursors current", ObStatClassIds::SQL, 40030, true, true)
STAT_EVENT_ADD_DEF(SQL_OPEN_CURSORS_CUMULATIVE, "opened cursors cumulative", ObStatClassIds::SQL, 40031, true, true)

STAT_EVENT_ADD_DEF(SQL_LOCAL_COUNT, "sql local count", ObStatClassIds::SQL, 40010, false, true)
STAT_EVENT_ADD_DEF(SQL_REMOTE_COUNT, "sql remote count", ObStatClassIds::SQL, 40011, false, true)
STAT_EVENT_ADD_DEF(SQL_DISTRIBUTED_COUNT, "sql distributed count", ObStatClassIds::SQL, 40012, false, true)
STAT_EVENT_ADD_DEF(ACTIVE_SESSIONS, "active sessions", ObStatClassIds::SQL, 40013, false, true)
STAT_EVENT_ADD_DEF(SQL_SINGLE_QUERY_COUNT, "single query count", ObStatClassIds::SQL, 40014, false, true)
STAT_EVENT_ADD_DEF(SQL_MULTI_QUERY_COUNT, "multiple query count", ObStatClassIds::SQL, 40015, false, true)
STAT_EVENT_ADD_DEF(SQL_MULTI_ONE_QUERY_COUNT, "multiple query with one stmt count", ObStatClassIds::SQL, 40016, false, true)

STAT_EVENT_ADD_DEF(SQL_INNER_SELECT_COUNT, "sql inner select count", ObStatClassIds::SQL, 40100, false, true)
STAT_EVENT_ADD_DEF(SQL_INNER_SELECT_TIME, "sql inner select time", ObStatClassIds::SQL, 40101, false, true)
STAT_EVENT_ADD_DEF(SQL_INNER_INSERT_COUNT, "sql inner insert count", ObStatClassIds::SQL, 40102, false, true)
STAT_EVENT_ADD_DEF(SQL_INNER_INSERT_TIME, "sql inner insert time", ObStatClassIds::SQL, 40103, false, true)
STAT_EVENT_ADD_DEF(SQL_INNER_REPLACE_COUNT, "sql inner replace count", ObStatClassIds::SQL, 40104, false, true)
STAT_EVENT_ADD_DEF(SQL_INNER_REPLACE_TIME, "sql inner replace time", ObStatClassIds::SQL, 40105, false, true)
STAT_EVENT_ADD_DEF(SQL_INNER_UPDATE_COUNT, "sql inner update count", ObStatClassIds::SQL, 40106, false, true)
STAT_EVENT_ADD_DEF(SQL_INNER_UPDATE_TIME, "sql inner update time", ObStatClassIds::SQL, 40107, false, true)
STAT_EVENT_ADD_DEF(SQL_INNER_DELETE_COUNT, "sql inner delete count", ObStatClassIds::SQL, 40108, false, true)
STAT_EVENT_ADD_DEF(SQL_INNER_DELETE_TIME, "sql inner delete time", ObStatClassIds::SQL, 40109, false, true)
STAT_EVENT_ADD_DEF(SQL_INNER_OTHER_COUNT, "sql inner other count", ObStatClassIds::SQL, 40110, false, true)
STAT_EVENT_ADD_DEF(SQL_INNER_OTHER_TIME, "sql inner other time", ObStatClassIds::SQL, 40111, false, true)
STAT_EVENT_ADD_DEF(SQL_USER_LOGONS_CUMULATIVE, "user logons cumulative", ObStatClassIds::SQL, 40112, false, true)
STAT_EVENT_ADD_DEF(SQL_USER_LOGOUTS_CUMULATIVE, "user logouts cumulative", ObStatClassIds::SQL, 40113, false, true)
STAT_EVENT_ADD_DEF(SQL_USER_LOGONS_FAILED_CUMULATIVE, "user logons failed cumulative", ObStatClassIds::SQL, 40114, false, true)
STAT_EVENT_ADD_DEF(SQL_USER_LOGONS_COST_TIME_CUMULATIVE, "user logons time cumulative", ObStatClassIds::SQL, 40115, false, true)
STAT_EVENT_ADD_DEF(SQL_LOCAL_TIME, "sql local execute time", ObStatClassIds::SQL, 40116, false, true)
STAT_EVENT_ADD_DEF(SQL_REMOTE_TIME, "sql remote execute time", ObStatClassIds::SQL, 40117, false, true)
STAT_EVENT_ADD_DEF(SQL_DISTRIBUTED_TIME, "sql distributed execute time", ObStatClassIds::SQL, 40118, false, true)

// CACHE
STAT_EVENT_ADD_DEF(ROW_CACHE_HIT, "row cache hit", ObStatClassIds::CACHE, 50000, true, true)
STAT_EVENT_ADD_DEF(ROW_CACHE_MISS, "row cache miss", ObStatClassIds::CACHE, 50001, true, true)
STAT_EVENT_ADD_DEF(BLOOM_FILTER_CACHE_HIT, "bloom filter cache hit", ObStatClassIds::CACHE, 50004, true, true)
STAT_EVENT_ADD_DEF(BLOOM_FILTER_CACHE_MISS, "bloom filter cache miss", ObStatClassIds::CACHE, 50005, true, true)
STAT_EVENT_ADD_DEF(BLOOM_FILTER_FILTS, "bloom filter filts", ObStatClassIds::CACHE, 50006, true, true)
STAT_EVENT_ADD_DEF(BLOOM_FILTER_PASSES, "bloom filter passes", ObStatClassIds::CACHE, 50007, true, true)
STAT_EVENT_ADD_DEF(BLOCK_CACHE_HIT, "block cache hit", ObStatClassIds::CACHE, 50008, true, true)
STAT_EVENT_ADD_DEF(BLOCK_CACHE_MISS, "block cache miss", ObStatClassIds::CACHE, 50009, true, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_HIT, "location cache hit", ObStatClassIds::CACHE, 50010, false, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_MISS, "location cache miss", ObStatClassIds::CACHE, 50011, false, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_WAIT, "location cache wait", ObStatClassIds::CACHE, 50012, false, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_PROXY_HIT, "location cache get hit from proxy virtual table", ObStatClassIds::CACHE, 50013, false, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_PROXY_MISS, "location cache get miss from proxy virtual table", ObStatClassIds::CACHE, 50014, false, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_CLEAR_LOCATION, "location cache nonblock renew", ObStatClassIds::CACHE, 50015, false, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_CLEAR_LOCATION_IGNORED, "location cache nonblock renew ignored", ObStatClassIds::CACHE, 50016, false, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_NONBLOCK_HIT, "location nonblock get hit", ObStatClassIds::CACHE, 50017, false, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_NONBLOCK_MISS, "location nonblock get miss", ObStatClassIds::CACHE, 50018, false, true)
//STAT_EVENT_ADD_DEF(LOCATION_CACHE_RPC_CHECK_LEADER, "location check leader count", ObStatClassIds::CACHE, 50019, true, true)
//STAT_EVENT_ADD_DEF(LOCATION_CACHE_RPC_CHECK_MEMBER, "location check member count", ObStatClassIds::CACHE, 50020, true, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_RPC_CHECK, "location cache rpc renew count", ObStatClassIds::CACHE, 50021, false, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_RENEW, "location cache renew", ObStatClassIds::CACHE, 50022, false, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_RENEW_IGNORED, "location cache renew ignored", ObStatClassIds::CACHE, 50023, false, true)
//STAT_EVENT_ADD_DEF(MMAP_COUNT, "mmap count", ObStatClassIds::CACHE, 50024, true, true)
//STAT_EVENT_ADD_DEF(MUNMAP_COUNT, "munmap count", ObStatClassIds::CACHE, 50025, true, true)
//STAT_EVENT_ADD_DEF(MMAP_SIZE, "mmap size", ObStatClassIds::CACHE, 50026, true, true)
//STAT_EVENT_ADD_DEF(MUNMAP_SIZE, "munmap size", ObStatClassIds::CACHE, 50027, true, true)
STAT_EVENT_ADD_DEF(KVCACHE_SYNC_WASH_TIME, "kvcache sync wash time", ObStatClassIds::CACHE, 50028, false, true)
STAT_EVENT_ADD_DEF(KVCACHE_SYNC_WASH_COUNT, "kvcache sync wash count", ObStatClassIds::CACHE, 50029, false, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_RPC_RENEW_FAIL, "location cache rpc renew fail count", ObStatClassIds::CACHE, 50030, false, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_SQL_RENEW, "location cache sql renew count", ObStatClassIds::CACHE, 50031, false, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_IGNORE_RPC_RENEW, "location cache ignore rpc renew count", ObStatClassIds::CACHE, 50032, false, true)
STAT_EVENT_ADD_DEF(FUSE_ROW_CACHE_HIT, "fuse row cache hit", ObStatClassIds::CACHE, 50033, true, true)
STAT_EVENT_ADD_DEF(FUSE_ROW_CACHE_MISS, "fuse row cache miss", ObStatClassIds::CACHE, 50034, true, true)
STAT_EVENT_ADD_DEF(SCHEMA_CACHE_HIT, "schema cache hit", ObStatClassIds::CACHE, 50035, false, true)
STAT_EVENT_ADD_DEF(SCHEMA_CACHE_MISS, "schema cache miss", ObStatClassIds::CACHE, 50036, false, true)
STAT_EVENT_ADD_DEF(TABLET_LS_CACHE_HIT, "tablet ls cache hit", ObStatClassIds::CACHE, 50037, false, true)
STAT_EVENT_ADD_DEF(TABLET_LS_CACHE_MISS, "tablet ls cache miss", ObStatClassIds::CACHE, 50038, false, true)
// obsoleted part from INDEX_CLOG_CACHE_HIT to USER_TABLE_STAT_CACHE_MISS
STAT_EVENT_ADD_DEF(INDEX_CLOG_CACHE_HIT, "index clog cache hit", ObStatClassIds::CACHE, 50039, false, true)
STAT_EVENT_ADD_DEF(INDEX_CLOG_CACHE_MISS, "index clog cache miss", ObStatClassIds::CACHE, 50040, false, true)
STAT_EVENT_ADD_DEF(USER_TAB_COL_STAT_CACHE_HIT, "user tab col stat cache hit", ObStatClassIds::CACHE, 50041, false, true)
STAT_EVENT_ADD_DEF(USER_TAB_COL_STAT_CACHE_MISS, "user tab col stat cache miss", ObStatClassIds::CACHE, 50042, false, true)
STAT_EVENT_ADD_DEF(USER_TABLE_STAT_CACHE_HIT, "user table stat cache hit", ObStatClassIds::CACHE, 50043, false, true)
STAT_EVENT_ADD_DEF(USER_TABLE_STAT_CACHE_MISS, "user table stat cache miss", ObStatClassIds::CACHE, 50044, false, true)
// obsoleted part from CLOG_CACHE_HIT to USER_TABLE_STAT_CACHE_MISS
STAT_EVENT_ADD_DEF(OPT_TABLE_STAT_CACHE_HIT, "opt table stat cache hit", ObStatClassIds::CACHE, 50045, false, true)
STAT_EVENT_ADD_DEF(OPT_TABLE_STAT_CACHE_MISS, "opt table stat cache miss", ObStatClassIds::CACHE, 50046, false, true)
STAT_EVENT_ADD_DEF(OPT_COLUMN_STAT_CACHE_HIT, "opt column stat cache hit", ObStatClassIds::CACHE, 50047, false, true)
STAT_EVENT_ADD_DEF(OPT_COLUMN_STAT_CACHE_MISS, "opt column stat cache miss", ObStatClassIds::CACHE, 50048, false, true)
STAT_EVENT_ADD_DEF(TMP_PAGE_CACHE_HIT, "tmp page cache hit", ObStatClassIds::CACHE, 50049, false, true)
STAT_EVENT_ADD_DEF(TMP_PAGE_CACHE_MISS, "tmp page cache miss", ObStatClassIds::CACHE, 50050, false, true)
STAT_EVENT_ADD_DEF(TMP_BLOCK_CACHE_HIT, "tmp block cache hit", ObStatClassIds::CACHE, 50051, false, true)
STAT_EVENT_ADD_DEF(TMP_BLOCK_CACHE_MISS, "tmp block cache miss", ObStatClassIds::CACHE, 50052, false, true)
STAT_EVENT_ADD_DEF(SECONDARY_META_CACHE_HIT, "secondary meta cache hit", ObStatClassIds::CACHE, 50053, false, true)
STAT_EVENT_ADD_DEF(SECONDARY_META_CACHE_MISS, "secondary meta cache miss", ObStatClassIds::CACHE, 50054, false, true)
STAT_EVENT_ADD_DEF(OPT_DS_STAT_CACHE_HIT, "opt ds stat cache hit", ObStatClassIds::CACHE, 50055, false, true)
STAT_EVENT_ADD_DEF(OPT_DS_STAT_CACHE_MISS, "opt ds stat cache miss", ObStatClassIds::CACHE, 50056, false, true)
STAT_EVENT_ADD_DEF(STORAGE_META_CACHE_HIT, "storage meta cache hit", ObStatClassIds::CACHE, 50057, true, true)
STAT_EVENT_ADD_DEF(STORAGE_META_CACHE_MISS, "storage meta cache miss", ObStatClassIds::CACHE, 50058, true, true)
STAT_EVENT_ADD_DEF(TABLET_CACHE_HIT, "tablet cache hit", ObStatClassIds::CACHE, 50059, true, true)
STAT_EVENT_ADD_DEF(TABLET_CACHE_MISS, "tablet cache miss", ObStatClassIds::CACHE, 50060, true, true)

STAT_EVENT_ADD_DEF(SCHEMA_HISTORY_CACHE_HIT, "schema history cache hit", ObStatClassIds::CACHE, 50061, false, true)
STAT_EVENT_ADD_DEF(SCHEMA_HISTORY_CACHE_MISS, "schema history cache miss", ObStatClassIds::CACHE, 50062, false, true)

// STORAGE
//STAT_EVENT_ADD_DEF(MEMSTORE_LOGICAL_READS, "MEMSTORE_LOGICAL_READS", STORAGE, "MEMSTORE_LOGICAL_READS")
//STAT_EVENT_ADD_DEF(MEMSTORE_LOGICAL_BYTES, "MEMSTORE_LOGICAL_BYTES", STORAGE, "MEMSTORE_LOGICAL_BYTES")
//STAT_EVENT_ADD_DEF(SSTABLE_LOGICAL_READS, "SSTABLE_LOGICAL_READS", STORAGE, "SSTABLE_LOGICAL_READS")
//STAT_EVENT_ADD_DEF(SSTABLE_LOGICAL_BYTES, "SSTABLE_LOGICAL_BYTES", STORAGE, "SSTABLE_LOGICAL_BYTES")
STAT_EVENT_ADD_DEF(IO_READ_COUNT, "io read count", ObStatClassIds::STORAGE, 60000, true, true)
STAT_EVENT_ADD_DEF(IO_READ_DELAY, "io read delay", ObStatClassIds::STORAGE, 60001, true, true)
STAT_EVENT_ADD_DEF(IO_READ_BYTES, "io read bytes", ObStatClassIds::STORAGE, 60002, true, true)
STAT_EVENT_ADD_DEF(IO_WRITE_COUNT, "io write count", ObStatClassIds::STORAGE, 60003, true, true)
STAT_EVENT_ADD_DEF(IO_WRITE_DELAY, "io write delay", ObStatClassIds::STORAGE, 60004, true, true)
STAT_EVENT_ADD_DEF(IO_WRITE_BYTES, "io write bytes", ObStatClassIds::STORAGE, 60005, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_SCAN_COUNT, "memstore scan count", ObStatClassIds::STORAGE, 60006, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_SCAN_SUCC_COUNT, "memstore scan succ count", ObStatClassIds::STORAGE, 60007, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_SCAN_FAIL_COUNT, "memstore scan fail count", ObStatClassIds::STORAGE, 60008, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_GET_COUNT, "memstore get count", ObStatClassIds::STORAGE, 60009, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_GET_SUCC_COUNT, "memstore get succ count", ObStatClassIds::STORAGE, 60010, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_GET_FAIL_COUNT, "memstore get fail count", ObStatClassIds::STORAGE, 60011, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_APPLY_COUNT, "memstore apply count", ObStatClassIds::STORAGE, 60012, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_APPLY_SUCC_COUNT, "memstore apply succ count", ObStatClassIds::STORAGE, 60013, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_APPLY_FAIL_COUNT, "memstore apply fail count", ObStatClassIds::STORAGE, 60014, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_GET_TIME, "memstore get time", ObStatClassIds::STORAGE, 60016, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_SCAN_TIME, "memstore scan time", ObStatClassIds::STORAGE, 60017, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_APPLY_TIME, "memstore apply time", ObStatClassIds::STORAGE, 60018, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_READ_LOCK_SUCC_COUNT, "memstore read lock succ count", ObStatClassIds::STORAGE, 60019, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_READ_LOCK_FAIL_COUNT, "memstore read lock fail count", ObStatClassIds::STORAGE, 60020, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_WRITE_LOCK_SUCC_COUNT, "memstore write lock succ count", ObStatClassIds::STORAGE, 60021, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_WRITE_LOCK_FAIL_COUNT, "memstore write lock fail count", ObStatClassIds::STORAGE, 60022, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_WAIT_WRITE_LOCK_TIME, "memstore wait write lock time", ObStatClassIds::STORAGE, 60023, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_WAIT_READ_LOCK_TIME, "memstore wait read lock time", ObStatClassIds::STORAGE, 60024, true, true)
STAT_EVENT_ADD_DEF(IO_READ_MICRO_INDEX_COUNT, "io read micro index count", ObStatClassIds::STORAGE, 60025, false, true)
STAT_EVENT_ADD_DEF(IO_READ_MICRO_INDEX_BYTES, "io read micro index bytes", ObStatClassIds::STORAGE, 60026, false, true)
STAT_EVENT_ADD_DEF(IO_READ_PREFETCH_MICRO_COUNT, "io prefetch micro block count", ObStatClassIds::STORAGE, 60027, false, true)
STAT_EVENT_ADD_DEF(IO_READ_PREFETCH_MICRO_BYTES, "io prefetch micro block bytes", ObStatClassIds::STORAGE, 60028, false, true)
STAT_EVENT_ADD_DEF(IO_READ_UNCOMP_MICRO_COUNT, "io read uncompress micro block count", ObStatClassIds::STORAGE, 60029, false, true)
STAT_EVENT_ADD_DEF(IO_READ_UNCOMP_MICRO_BYTES, "io read uncompress micro block bytes", ObStatClassIds::STORAGE, 60030, false, true)
STAT_EVENT_ADD_DEF(STORAGE_READ_ROW_COUNT, "storage read row count", ObStatClassIds::STORAGE, 60031, false, true)
STAT_EVENT_ADD_DEF(STORAGE_DELETE_ROW_COUNT, "storage delete row count", ObStatClassIds::STORAGE, 60032, false, true)
STAT_EVENT_ADD_DEF(STORAGE_INSERT_ROW_COUNT, "storage insert row count", ObStatClassIds::STORAGE, 60033, false, true)
STAT_EVENT_ADD_DEF(STORAGE_UPDATE_ROW_COUNT, "storage update row count", ObStatClassIds::STORAGE, 60034, false, true)
STAT_EVENT_ADD_DEF(MEMSTORE_ROW_PURGE_COUNT, "memstore row purge count", ObStatClassIds::STORAGE, 60037, false, true)
STAT_EVENT_ADD_DEF(MEMSTORE_ROW_COMPACTION_COUNT, "memstore row compaction count", ObStatClassIds::STORAGE, 60038, false, true)

STAT_EVENT_ADD_DEF(IO_READ_QUEUE_DELAY, "io read queue delay", ObStatClassIds::STORAGE, 60039, false, true)
STAT_EVENT_ADD_DEF(IO_WRITE_QUEUE_DELAY, "io write queue delay", ObStatClassIds::STORAGE, 60040, false, true)
STAT_EVENT_ADD_DEF(IO_READ_CB_QUEUE_DELAY, "io read callback queuing delay", ObStatClassIds::STORAGE, 60041, false, true)
STAT_EVENT_ADD_DEF(IO_READ_CB_PROCESS_DELAY, "io read callback process delay", ObStatClassIds::STORAGE, 60042, false, true)
STAT_EVENT_ADD_DEF(BANDWIDTH_IN_THROTTLE, "bandwidth in throttle size", ObStatClassIds::STORAGE, 60051, false, true)
STAT_EVENT_ADD_DEF(BANDWIDTH_OUT_THROTTLE, "bandwidth out throttle size", ObStatClassIds::STORAGE, 60052, false, true)
STAT_EVENT_ADD_DEF(MEMSTORE_READ_ROW_COUNT, "memstore read row count", ObStatClassIds::STORAGE, 60056, true, true)
STAT_EVENT_ADD_DEF(SSSTORE_READ_ROW_COUNT, "ssstore read row count", ObStatClassIds::STORAGE, 60057, true, true)

STAT_EVENT_ADD_DEF(MEMSTORE_WRITE_LOCK_WAKENUP_COUNT, "memstore write lock wakenup count in lock_wait_mgr", ObStatClassIds::STORAGE, 60068, false, true)


STAT_EVENT_ADD_DEF(EXIST_ROW_EFFECT_READ, "exist row effect read", ObStatClassIds::STORAGE, 60075, false, true)
STAT_EVENT_ADD_DEF(EXIST_ROW_EMPTY_READ, "exist row empty read", ObStatClassIds::STORAGE, 60076, false, true)
STAT_EVENT_ADD_DEF(GET_ROW_EFFECT_READ, "get row effect read", ObStatClassIds::STORAGE, 60077, false, true)
STAT_EVENT_ADD_DEF(GET_ROW_EMPTY_READ, "get row empty read", ObStatClassIds::STORAGE, 60078, false, true)
STAT_EVENT_ADD_DEF(SCAN_ROW_EFFECT_READ, "scan row effect read", ObStatClassIds::STORAGE, 60079, false, true)
STAT_EVENT_ADD_DEF(SCAN_ROW_EMPTY_READ, "scan row empty read", ObStatClassIds::STORAGE, 60080, false, true)
STAT_EVENT_ADD_DEF(BANDWIDTH_IN_SLEEP_US, "bandwidth in sleep us", ObStatClassIds::STORAGE, 60081, false, true)
STAT_EVENT_ADD_DEF(BANDWIDTH_OUT_SLEEP_US, "bandwidth out sleep us", ObStatClassIds::STORAGE, 60082, false, true)

STAT_EVENT_ADD_DEF(MEMSTORE_WRITE_LOCK_WAIT_TIMEOUT_COUNT, "memstore write lock wait timeout count", ObStatClassIds::STORAGE, 60083, false, true)

STAT_EVENT_ADD_DEF(DATA_BLOCK_READ_CNT, "accessed data micro block count", ObStatClassIds::STORAGE, 60084, true, true)
STAT_EVENT_ADD_DEF(DATA_BLOCK_CACHE_HIT, "data micro block cache hit", ObStatClassIds::STORAGE, 60085, true, true)
STAT_EVENT_ADD_DEF(INDEX_BLOCK_READ_CNT, "accessed index micro block count", ObStatClassIds::STORAGE, 60086, true, true)
STAT_EVENT_ADD_DEF(INDEX_BLOCK_CACHE_HIT, "index micro block cache hit", ObStatClassIds::STORAGE, 60087, true, true)
STAT_EVENT_ADD_DEF(BLOCKSCAN_BLOCK_CNT, "blockscaned data micro block count", ObStatClassIds::STORAGE, 60088, true, true)
STAT_EVENT_ADD_DEF(BLOCKSCAN_ROW_CNT, "blockscaned row count", ObStatClassIds::STORAGE, 60089, true, true)
STAT_EVENT_ADD_DEF(PUSHDOWN_STORAGE_FILTER_ROW_CNT, "storage filtered row count", ObStatClassIds::STORAGE, 60090, true, true)

// backup & restore
STAT_EVENT_ADD_DEF(BACKUP_IO_READ_COUNT, "backup io read count", ObStatClassIds::STORAGE, 69000, true, true)
STAT_EVENT_ADD_DEF(BACKUP_IO_READ_BYTES, "backup io read bytes", ObStatClassIds::STORAGE, 69001, true, true)
STAT_EVENT_ADD_DEF(BACKUP_IO_WRITE_COUNT, "backup io write count", ObStatClassIds::STORAGE, 69002, true, true)
STAT_EVENT_ADD_DEF(BACKUP_IO_WRITE_BYTES, "backup io write bytes", ObStatClassIds::STORAGE, 69003, true, true)
STAT_EVENT_ADD_DEF(BACKUP_DELETE_COUNT, "backup delete count", ObStatClassIds::STORAGE, 69010, true, true)

STAT_EVENT_ADD_DEF(BACKUP_IO_READ_DELAY, "backup io read delay", ObStatClassIds::STORAGE, 69012, true, true)
STAT_EVENT_ADD_DEF(BACKUP_IO_WRITE_DELAY, "backup io write delay", ObStatClassIds::STORAGE, 69013, true, true)
STAT_EVENT_ADD_DEF(COS_IO_READ_DELAY, "cos io read delay", ObStatClassIds::STORAGE, 69014, true, true)
STAT_EVENT_ADD_DEF(COS_IO_WRITE_DELAY, "cos io write delay", ObStatClassIds::STORAGE, 69015, true, true)
STAT_EVENT_ADD_DEF(COS_IO_LS_DELAY, "cos io list delay", ObStatClassIds::STORAGE, 69016, true, true)
STAT_EVENT_ADD_DEF(BACKUP_DELETE_DELAY, "backup delete delay", ObStatClassIds::STORAGE, 69017, true, true)

STAT_EVENT_ADD_DEF(BACKUP_IO_LS_COUNT, "backup io list count", ObStatClassIds::STORAGE, 69019, true, true)
STAT_EVENT_ADD_DEF(BACKUP_IO_READ_FAIL_COUNT, "backup io read failed count", ObStatClassIds::STORAGE, 69020, true, true)
STAT_EVENT_ADD_DEF(BACKUP_IO_WRITE_FAIL_COUNT, "backup io write failed count", ObStatClassIds::STORAGE, 69021, true, true)
STAT_EVENT_ADD_DEF(BACKUP_IO_DEL_FAIL_COUNT, "backup io delete failed count", ObStatClassIds::STORAGE, 69022, true, true)
STAT_EVENT_ADD_DEF(BACKUP_IO_LS_FAIL_COUNT, "backup io list failed count", ObStatClassIds::STORAGE, 69023, true, true)

STAT_EVENT_ADD_DEF(BACKUP_TAGGING_COUNT, "backup io tagging count", ObStatClassIds::STORAGE, 69024, true, true)
STAT_EVENT_ADD_DEF(BACKUP_TAGGING_FAIL_COUNT, "backup io tagging failed count", ObStatClassIds::STORAGE, 69025, true, true)

STAT_EVENT_ADD_DEF(COS_IO_WRITE_BYTES, "cos io write bytes", ObStatClassIds::STORAGE, 69026, true, true)
STAT_EVENT_ADD_DEF(COS_IO_WRITE_COUNT, "cos io write count", ObStatClassIds::STORAGE, 69027, true, true)
STAT_EVENT_ADD_DEF(COS_DELETE_COUNT, "cos delete count", ObStatClassIds::STORAGE, 69028, true, true)
STAT_EVENT_ADD_DEF(COS_DELETE_DELAY, "cos delete delay", ObStatClassIds::STORAGE, 69029, true, true)
STAT_EVENT_ADD_DEF(COS_IO_LS_LIMIT_COUNT, "cos list io limit count", ObStatClassIds::STORAGE, 69030, true, true)
STAT_EVENT_ADD_DEF(COS_IO_LS_COUNT, "cos io list count", ObStatClassIds::STORAGE, 69031, true, true)
STAT_EVENT_ADD_DEF(COS_IO_READ_COUNT, "cos io read count", ObStatClassIds::STORAGE, 69032, true, true)
STAT_EVENT_ADD_DEF(COS_IO_READ_BYTES, "cos io read bytes", ObStatClassIds::STORAGE, 69033, true, true)

// DEBUG
STAT_EVENT_ADD_DEF(REFRESH_SCHEMA_COUNT, "refresh schema count", ObStatClassIds::DEBUG, 70000, false, true)
STAT_EVENT_ADD_DEF(REFRESH_SCHEMA_TIME, "refresh schema time", ObStatClassIds::DEBUG, 70001, false, true)
STAT_EVENT_ADD_DEF(INNER_SQL_CONNECTION_EXECUTE_COUNT, "inner sql connection execute count", ObStatClassIds::DEBUG, 70002, false, true)
STAT_EVENT_ADD_DEF(INNER_SQL_CONNECTION_EXECUTE_TIME, "inner sql connection execute time", ObStatClassIds::DEBUG, 70003, false, true)
STAT_EVENT_ADD_DEF(LS_ALL_TABLE_OPERATOR_GET_COUNT, "log stream table operator get count", ObStatClassIds::DEBUG, 70006, false, true)
STAT_EVENT_ADD_DEF(LS_ALL_TABLE_OPERATOR_GET_TIME, "log stream table operator get time", ObStatClassIds::DEBUG, 70007, false, true)

//CLOG 80001 ~ 90000
STAT_EVENT_ADD_DEF(PALF_WRITE_IO_COUNT, "palf write io count to disk",  ObStatClassIds::CLOG, 80001, true, true)
STAT_EVENT_ADD_DEF(PALF_WRITE_SIZE, "palf write size to disk",  ObStatClassIds::CLOG, 80002, true, true)
STAT_EVENT_ADD_DEF(PALF_WRITE_TIME, "palf write total time to disk", ObStatClassIds::CLOG, 80003, true, true)
STAT_EVENT_ADD_DEF(PALF_READ_COUNT_FROM_CACHE, "palf read count from cache", ObStatClassIds::CLOG, 80004, true, true)
STAT_EVENT_ADD_DEF(PALF_READ_SIZE_FROM_CACHE, "palf read size from cache", ObStatClassIds::CLOG, 80005, true, true)
STAT_EVENT_ADD_DEF(PALF_READ_TIME_FROM_CACHE, "palf read total time from cache", ObStatClassIds::CLOG, 80006, true, true)
STAT_EVENT_ADD_DEF(PALF_READ_IO_COUNT_FROM_DISK, "palf read io count from disk", ObStatClassIds::CLOG, 80007, true, true)
STAT_EVENT_ADD_DEF(PALF_READ_SIZE_FROM_DISK, "palf read size from disk", ObStatClassIds::CLOG, 80008, true, true)
STAT_EVENT_ADD_DEF(PALF_READ_TIME_FROM_DISK, "palf read total time from disk", ObStatClassIds::CLOG, 80009, true, true)
STAT_EVENT_ADD_DEF(PALF_HANDLE_RPC_REQUEST_COUNT, "palf handle rpc request count", ObStatClassIds::CLOG, 80010, true, true)
STAT_EVENT_ADD_DEF(ARCHIVE_READ_LOG_SIZE, "archive read log size", ObStatClassIds::CLOG, 80011, true, true)
STAT_EVENT_ADD_DEF(ARCHIVE_WRITE_LOG_SIZE, "archive write log size", ObStatClassIds::CLOG, 80012, true, true)
STAT_EVENT_ADD_DEF(RESTORE_READ_LOG_SIZE, "restore read log size", ObStatClassIds::CLOG, 80013, true, true)
STAT_EVENT_ADD_DEF(RESTORE_WRITE_LOG_SIZE, "restore write log size", ObStatClassIds::CLOG, 80014, true, true)
STAT_EVENT_ADD_DEF(CLOG_TRANS_LOG_TOTAL_SIZE, "clog trans log total size", ObStatClassIds::CLOG, 80057, false, true)

// CLOG.EXTLOG 81001 ~ 90000
STAT_EVENT_ADD_DEF(CLOG_EXTLOG_FETCH_LOG_SIZE, "external log service fetch log size", ObStatClassIds::CLOG, 81001, false, true)
STAT_EVENT_ADD_DEF(CLOG_EXTLOG_FETCH_LOG_COUNT, "external log service fetch log count", ObStatClassIds::CLOG, 81002, false, true)
STAT_EVENT_ADD_DEF(CLOG_EXTLOG_FETCH_RPC_COUNT, "external log service fetch rpc count", ObStatClassIds::CLOG, 81003, false, true)

// CLOG.REPLAY


// CLOG.PROXY

// ELECTION

//OBSERVER

// rootservice
STAT_EVENT_ADD_DEF(RS_RPC_SUCC_COUNT, "success rpc process", ObStatClassIds::RS, 110015, false, true)
STAT_EVENT_ADD_DEF(RS_RPC_FAIL_COUNT, "failed rpc process", ObStatClassIds::RS, 110016, false, true)
STAT_EVENT_ADD_DEF(RS_BALANCER_SUCC_COUNT, "balancer succ execute count", ObStatClassIds::RS, 110017, false, true)
STAT_EVENT_ADD_DEF(RS_BALANCER_FAIL_COUNT, "balancer failed execute count", ObStatClassIds::RS, 110018, false, true)

// TABLE API (19xxxx)
// -- retrieve 1900xx
STAT_EVENT_ADD_DEF(TABLEAPI_RETRIEVE_COUNT, "single retrieve execute count", ObStatClassIds::TABLEAPI, 190001, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_RETRIEVE_TIME, "single retrieve execute time", ObStatClassIds::TABLEAPI, 190002, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_RETRIEVE_COUNT, "multi retrieve execute count", ObStatClassIds::TABLEAPI, 190003, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_RETRIEVE_TIME, "multi retrieve execute time", ObStatClassIds::TABLEAPI, 190004, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_RETRIEVE_ROW, "multi retrieve rows", ObStatClassIds::TABLEAPI, 190005, true, true)
// -- insert 1901xx
STAT_EVENT_ADD_DEF(TABLEAPI_INSERT_COUNT, "single insert execute count", ObStatClassIds::TABLEAPI, 190101, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_INSERT_TIME, "single insert execute time", ObStatClassIds::TABLEAPI, 190102, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_INSERT_COUNT, "multi insert execute count", ObStatClassIds::TABLEAPI, 190103, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_INSERT_TIME, "multi insert execute time", ObStatClassIds::TABLEAPI, 190104, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_INSERT_ROW, "multi insert rows", ObStatClassIds::TABLEAPI, 190105, true, true)
// -- delete 1902xx
STAT_EVENT_ADD_DEF(TABLEAPI_DELETE_COUNT, "single delete execute count", ObStatClassIds::TABLEAPI, 190201, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_DELETE_TIME, "single delete execute time", ObStatClassIds::TABLEAPI, 190202, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_DELETE_COUNT, "multi delete execute count", ObStatClassIds::TABLEAPI, 190203, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_DELETE_TIME, "multi delete execute time", ObStatClassIds::TABLEAPI, 190204, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_DELETE_ROW, "multi delete rows", ObStatClassIds::TABLEAPI, 190205, true, true)
// -- update 1903xx
STAT_EVENT_ADD_DEF(TABLEAPI_UPDATE_COUNT, "single update execute count", ObStatClassIds::TABLEAPI, 190301, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_UPDATE_TIME, "single update execute time", ObStatClassIds::TABLEAPI, 190302, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_UPDATE_COUNT, "multi update execute count", ObStatClassIds::TABLEAPI, 190303, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_UPDATE_TIME, "multi update execute time", ObStatClassIds::TABLEAPI, 190304, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_UPDATE_ROW, "multi update rows", ObStatClassIds::TABLEAPI, 190305, true, true)
// -- insert_or_update 1904xx
STAT_EVENT_ADD_DEF(TABLEAPI_INSERT_OR_UPDATE_COUNT, "single insert_or_update execute count", ObStatClassIds::TABLEAPI, 190401, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_INSERT_OR_UPDATE_TIME, "single insert_or_update execute time", ObStatClassIds::TABLEAPI, 190402, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_INSERT_OR_UPDATE_COUNT, "multi insert_or_update execute count", ObStatClassIds::TABLEAPI, 190403, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_INSERT_OR_UPDATE_TIME, "multi insert_or_update execute time", ObStatClassIds::TABLEAPI, 190404, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_INSERT_OR_UPDATE_ROW, "multi insert_or_update rows", ObStatClassIds::TABLEAPI, 190405, true, true)
// -- replace 1905xx
STAT_EVENT_ADD_DEF(TABLEAPI_REPLACE_COUNT, "single replace execute count", ObStatClassIds::TABLEAPI, 190501, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_REPLACE_TIME, "single replace execute time", ObStatClassIds::TABLEAPI, 190502, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_REPLACE_COUNT, "multi replace execute count", ObStatClassIds::TABLEAPI, 190503, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_REPLACE_TIME, "multi replace execute time", ObStatClassIds::TABLEAPI, 190504, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_REPLACE_ROW, "multi replace rows", ObStatClassIds::TABLEAPI, 190505, true, true)
// -- complex_batch_retrieve 1906xx
STAT_EVENT_ADD_DEF(TABLEAPI_BATCH_RETRIEVE_COUNT, "batch retrieve execute count", ObStatClassIds::TABLEAPI, 190601, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_BATCH_RETRIEVE_TIME, "batch retrieve execute time", ObStatClassIds::TABLEAPI, 190602, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_BATCH_RETRIEVE_ROW, "batch retrieve rows", ObStatClassIds::TABLEAPI, 190603, true, true)
// -- complex_batch_operation 1907xx
STAT_EVENT_ADD_DEF(TABLEAPI_BATCH_HYBRID_COUNT, "batch hybrid execute count", ObStatClassIds::TABLEAPI, 190701, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_BATCH_HYBRID_TIME, "batch hybrid execute time", ObStatClassIds::TABLEAPI, 190702, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_BATCH_HYBRID_INSERT_OR_UPDATE_ROW, "batch hybrid insert_or_update rows", ObStatClassIds::TABLEAPI, 190707, true, true)
// -- other 1908xx
STAT_EVENT_ADD_DEF(TABLEAPI_LOGIN_COUNT, "table api login count", ObStatClassIds::TABLEAPI, 190801, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_TSC_VIOLATE_COUNT, "table api OB_TRANSACTION_SET_VIOLATION count", ObStatClassIds::TABLEAPI, 190802, true, true)
// -- query 1909xx
STAT_EVENT_ADD_DEF(TABLEAPI_QUERY_COUNT, "query count", ObStatClassIds::TABLEAPI, 190901, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_QUERY_TIME, "query time", ObStatClassIds::TABLEAPI, 190902, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_QUERY_ROW, "query row count", ObStatClassIds::TABLEAPI, 190903, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_QUERY_AND_MUTATE_COUNT, "query_and_mutate count", ObStatClassIds::TABLEAPI, 190904, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_QUERY_AND_MUTATE_TIME, "query_and_mutate time", ObStatClassIds::TABLEAPI, 190905, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_QUERY_AND_MUTATE_ROW, "query_and_mutate row count", ObStatClassIds::TABLEAPI, 190906, true, true)
// -- hbase api 1910xx
STAT_EVENT_ADD_DEF(HBASEAPI_SCAN_COUNT, "hbase scan count", ObStatClassIds::TABLEAPI, 191001, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_SCAN_TIME, "hbase scan time", ObStatClassIds::TABLEAPI, 191002, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_SCAN_ROW, "hbase scan row count", ObStatClassIds::TABLEAPI, 191003, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_PUT_COUNT, "hbase put count", ObStatClassIds::TABLEAPI, 191004, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_PUT_TIME, "hbase put time", ObStatClassIds::TABLEAPI, 191005, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_PUT_ROW, "hbase put row count", ObStatClassIds::TABLEAPI, 191006, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_DELETE_COUNT, "hbase delete count", ObStatClassIds::TABLEAPI, 191007, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_DELETE_TIME, "hbase delete time", ObStatClassIds::TABLEAPI, 191008, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_DELETE_ROW, "hbase delete row count", ObStatClassIds::TABLEAPI, 191009, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_APPEND_COUNT, "hbase append count", ObStatClassIds::TABLEAPI, 191010, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_APPEND_TIME, "hbase append time", ObStatClassIds::TABLEAPI, 191011, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_APPEND_ROW, "hbase append row count", ObStatClassIds::TABLEAPI, 191012, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_INCREMENT_COUNT, "hbase increment count", ObStatClassIds::TABLEAPI, 191013, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_INCREMENT_TIME, "hbase increment time", ObStatClassIds::TABLEAPI, 191014, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_INCREMENT_ROW, "hbase increment row count", ObStatClassIds::TABLEAPI, 191015, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_CHECK_PUT_COUNT, "hbase check_put count", ObStatClassIds::TABLEAPI, 191016, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_CHECK_PUT_TIME, "hbase check_put time", ObStatClassIds::TABLEAPI, 191017, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_CHECK_PUT_ROW, "hbase check_put row count", ObStatClassIds::TABLEAPI, 191018, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_CHECK_DELETE_COUNT, "hbase check_delete count", ObStatClassIds::TABLEAPI, 191019, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_CHECK_DELETE_TIME, "hbase check_delete time", ObStatClassIds::TABLEAPI, 191020, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_CHECK_DELETE_ROW, "hbase check_delete row count", ObStatClassIds::TABLEAPI, 191021, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_HYBRID_COUNT, "hbase hybrid count", ObStatClassIds::TABLEAPI, 191022, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_HYBRID_TIME, "hbase hybrid time", ObStatClassIds::TABLEAPI, 191023, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_HYBRID_ROW, "hbase hybrid row count", ObStatClassIds::TABLEAPI, 191024, true, true)

// -- table increment 1911xx
STAT_EVENT_ADD_DEF(TABLEAPI_INCREMENT_COUNT, "single increment execute count", ObStatClassIds::TABLEAPI, 191101, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_INCREMENT_TIME, "single increment execute time", ObStatClassIds::TABLEAPI, 191102, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_INCREMENT_COUNT, "multi increment execute count", ObStatClassIds::TABLEAPI, 191103, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_INCREMENT_TIME, "multi increment execute time", ObStatClassIds::TABLEAPI, 191104, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_INCREMENT_ROW, "multi increment rows", ObStatClassIds::TABLEAPI, 191105, true, true)

// -- table append 1912xx
STAT_EVENT_ADD_DEF(TABLEAPI_APPEND_COUNT, "single append execute count", ObStatClassIds::TABLEAPI, 191201, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_APPEND_TIME, "single append execute time", ObStatClassIds::TABLEAPI, 191202, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_APPEND_COUNT, "multi append execute count", ObStatClassIds::TABLEAPI, 191203, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_APPEND_TIME, "multi append execute time", ObStatClassIds::TABLEAPI, 191204, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_APPEND_ROW, "multi append rows", ObStatClassIds::TABLEAPI, 191205, true, true)

// sys_time_model related (20xxxx)
STAT_EVENT_ADD_DEF(SYS_TIME_MODEL_DB_TIME, "DB time", ObStatClassIds::SYS, 200001, false, true)
STAT_EVENT_ADD_DEF(SYS_TIME_MODEL_DB_CPU, "DB CPU", ObStatClassIds::SYS, 200002, false, true)
STAT_EVENT_ADD_DEF(SYS_TIME_MODEL_DB_INNER_TIME, "DB inner sql time", ObStatClassIds::SYS, 200003, false, true)
STAT_EVENT_ADD_DEF(SYS_TIME_MODEL_DB_INNER_CPU, "DB inner sql CPU", ObStatClassIds::SYS, 200004, false, true)
STAT_EVENT_ADD_DEF(SYS_TIME_MODEL_BKGD_TIME, "background elapsed time", ObStatClassIds::SYS, 200005, false, true)
STAT_EVENT_ADD_DEF(SYS_TIME_MODEL_BKGD_CPU, "background cpu time", ObStatClassIds::SYS, 200006, false, true)
STAT_EVENT_ADD_DEF(SYS_TIME_MODEL_BACKUP_CPU, "(backup/restore) cpu time", ObStatClassIds::SYS, 200007, false, true)
STAT_EVENT_ADD_DEF(SYS_TIME_MODEL_ENCRYPT_TIME, "Tablespace encryption elapsed time", ObStatClassIds::SYS, 200008, false, true)
STAT_EVENT_ADD_DEF(SYS_TIME_MODEL_ENCRYPT_CPU, "Tablespace encryption cpu time", ObStatClassIds::SYS, 200009, false, true)

// wr and ash related  (2100xx)
STAT_EVENT_ADD_DEF(WR_SNAPSHOT_ELAPSE_TIME, "wr snapshot task elapse time", ObStatClassIds::WR, 210001, false, true)
STAT_EVENT_ADD_DEF(WR_SNAPSHOT_CPU_TIME, "wr snapshot task cpu time", ObStatClassIds::WR, 210002, false, true)
STAT_EVENT_ADD_DEF(WR_PURGE_ELAPSE_TIME, "wr purge task elapse time", ObStatClassIds::WR, 210003, false, true)
STAT_EVENT_ADD_DEF(WR_PURGE_CPU_TIME, "wr purge task cpu time", ObStatClassIds::WR, 210004, false, true)
STAT_EVENT_ADD_DEF(WR_SCHEDULAR_ELAPSE_TIME, "wr schedular elapse time", ObStatClassIds::WR, 210005, false, true)
STAT_EVENT_ADD_DEF(WR_SCHEDULAR_CPU_TIME, "wr schedular cpu time", ObStatClassIds::WR, 210006, false, true)
STAT_EVENT_ADD_DEF(WR_USER_SUBMIT_SNAPSHOT_ELAPSE_TIME, "wr user submit snapshot elapse time", ObStatClassIds::WR, 210007, false, true)
STAT_EVENT_ADD_DEF(WR_USER_SUBMIT_SNAPSHOT_CPU_TIME, "wr user submit snapshot cpu time", ObStatClassIds::WR, 210008, false, true)
STAT_EVENT_ADD_DEF(WR_COLLECTED_ASH_ROW_COUNT, "wr collected active session history row count", ObStatClassIds::WR, 210009, false, true)
STAT_EVENT_ADD_DEF(ASH_SCHEDULAR_ELAPSE_TIME, "ash schedular elapse time", ObStatClassIds::WR, 210010, false, true)

//end
STAT_EVENT_ADD_DEF(STAT_EVENT_ADD_END, "event add end", ObStatClassIds::DEBUG, 1, false, false)

#endif

#ifdef STAT_EVENT_SET_DEF
// NETWORK
STAT_EVENT_SET_DEF(STANDBY_FETCH_LOG_BYTES, "standby fetch log bytes", ObStatClassIds::NETWORK, 110000, false, true)
STAT_EVENT_SET_DEF(STANDBY_FETCH_LOG_BANDWIDTH_LIMIT, "standby fetch log bandwidth limit", ObStatClassIds::NETWORK, 110001, false, true)

// QUEUE

// TRANS

// SQL

// CACHE
STAT_EVENT_SET_DEF(LOCATION_CACHE_SIZE, "location cache size", ObStatClassIds::CACHE, 120000, false, true)
STAT_EVENT_SET_DEF(TABLET_LS_CACHE_SIZE, "tablet ls cache size", ObStatClassIds::CACHE, 120001, false, true)
STAT_EVENT_SET_DEF(OPT_TAB_STAT_CACHE_SIZE, "table stat cache size", ObStatClassIds::CACHE, 120002, false, true)
STAT_EVENT_SET_DEF(OPT_TAB_COL_STAT_CACHE_SIZE, "table col stat cache size", ObStatClassIds::CACHE, 120003, false, true)
STAT_EVENT_SET_DEF(INDEX_BLOCK_CACHE_SIZE, "index block cache size", ObStatClassIds::CACHE, 120004, false, true)
STAT_EVENT_SET_DEF(SYS_BLOCK_CACHE_SIZE, "sys block cache size", ObStatClassIds::CACHE, 120005, false, true)
STAT_EVENT_SET_DEF(USER_BLOCK_CACHE_SIZE, "user block cache size", ObStatClassIds::CACHE, 120006, false, true)
STAT_EVENT_SET_DEF(USER_ROW_CACHE_SIZE, "user row cache size", ObStatClassIds::CACHE, 120008, false, true)
STAT_EVENT_SET_DEF(BLOOM_FILTER_CACHE_SIZE, "bloom filter cache size", ObStatClassIds::CACHE, 120009, false, true)

// STORAGE
STAT_EVENT_SET_DEF(ACTIVE_MEMSTORE_USED, "active memstore used", ObStatClassIds::STORAGE, 130000, false, true)
STAT_EVENT_SET_DEF(TOTAL_MEMSTORE_USED, "total memstore used", ObStatClassIds::STORAGE, 130001, false, true)
STAT_EVENT_SET_DEF(MAJOR_FREEZE_TRIGGER, "major freeze trigger", ObStatClassIds::STORAGE, 130002, false, true)
STAT_EVENT_SET_DEF(MEMSTORE_LIMIT, "memstore limit", ObStatClassIds::STORAGE, 130004, false, true)

// RESOURCE
//STAT_EVENT_SET_DEF(SRV_DISK_SIZE, "SRV_DISK_SIZE", RESOURCE, "SRV_DISK_SIZE")
//STAT_EVENT_SET_DEF(DISK_USAGE, "DISK_USAGE", RESOURCE, "DISK_USAGE")
//STAT_EVENT_SET_DEF(SRV_MEMORY_SIZE, "SRV_MEMORY_SIZE", RESOURCE, "SRV_MEMORY_SIZE")
STAT_EVENT_SET_DEF(MIN_MEMORY_SIZE, "min memory size", ObStatClassIds::RESOURCE, 140001, false, true)
STAT_EVENT_SET_DEF(MAX_MEMORY_SIZE, "max memory size", ObStatClassIds::RESOURCE, 140002, false, true)
STAT_EVENT_SET_DEF(MEMORY_USAGE, "memory usage", ObStatClassIds::RESOURCE, 140003, false, true)
//STAT_EVENT_SET_DEF(SRV_CPUS, "SRV_CPUS", RESOURCE, "SRV_CPUS")
STAT_EVENT_SET_DEF(MIN_CPUS, "min cpus", ObStatClassIds::RESOURCE, 140004, false, true)
STAT_EVENT_SET_DEF(MAX_CPUS, "max cpus", ObStatClassIds::RESOURCE, 140005, false, true)
STAT_EVENT_SET_DEF(CPU_USAGE, "cpu usage", ObStatClassIds::RESOURCE, 140006, false, true)
STAT_EVENT_SET_DEF(DISK_USAGE, "disk usage", ObStatClassIds::RESOURCE, 140007, false, true)
STAT_EVENT_SET_DEF(MEMORY_USED_SIZE, "observer memory used size", ObStatClassIds::RESOURCE, 140008, false, true)
STAT_EVENT_SET_DEF(MEMORY_FREE_SIZE, "observer memory free size", ObStatClassIds::RESOURCE, 140009, false, true)
STAT_EVENT_SET_DEF(IS_MINI_MODE, "is mini mode", ObStatClassIds::RESOURCE, 140010, false, true)
STAT_EVENT_SET_DEF(MEMORY_HOLD_SIZE, "observer memory hold size", ObStatClassIds::RESOURCE, 140011, false, true)
STAT_EVENT_SET_DEF(WORKER_TIME, "worker time", ObStatClassIds::RESOURCE, 140012, false, true)
STAT_EVENT_SET_DEF(CPU_TIME, "cpu time", ObStatClassIds::RESOURCE, 140013, false, true)
STAT_EVENT_SET_DEF(MEMORY_LIMIT, "effective observer memory limit", ObStatClassIds::RESOURCE, 140014, false, true)
STAT_EVENT_SET_DEF(SYSTEM_MEMORY, "effective system memory", ObStatClassIds::RESOURCE, 140015, false, true)
STAT_EVENT_SET_DEF(HIDDEN_SYS_MEMORY, "effective hidden sys memory", ObStatClassIds::RESOURCE, 140016, false, true)
STAT_EVENT_SET_DEF(MAX_SESSION_NUM, "max session num", ObStatClassIds::RESOURCE, 140017, false, true)

//CLOG

// DEBUG
STAT_EVENT_SET_DEF(OBLOGGER_WRITE_SIZE, "oblogger log bytes", ObStatClassIds::DEBUG, 160001, false, true)
STAT_EVENT_SET_DEF(ELECTION_WRITE_SIZE, "election log bytes", ObStatClassIds::DEBUG, 160002, false, true)
STAT_EVENT_SET_DEF(ROOTSERVICE_WRITE_SIZE, "rootservice log bytes", ObStatClassIds::DEBUG, 160003, false, true)
STAT_EVENT_SET_DEF(OBLOGGER_TOTAL_WRITE_COUNT, "oblogger total log count", ObStatClassIds::DEBUG, 160004, false, true)
STAT_EVENT_SET_DEF(ELECTION_TOTAL_WRITE_COUNT, "election total log count", ObStatClassIds::DEBUG, 160005, false, true)
STAT_EVENT_SET_DEF(ROOTSERVICE_TOTAL_WRITE_COUNT, "rootservice total log count", ObStatClassIds::DEBUG, 160006, false, true)
STAT_EVENT_SET_DEF(ASYNC_ERROR_LOG_DROPPED_COUNT, "async error log dropped count", ObStatClassIds::DEBUG, 160019, false, true)
STAT_EVENT_SET_DEF(ASYNC_WARN_LOG_DROPPED_COUNT, "async warn log dropped count", ObStatClassIds::DEBUG, 160020, false, true)
STAT_EVENT_SET_DEF(ASYNC_INFO_LOG_DROPPED_COUNT, "async info log dropped count", ObStatClassIds::DEBUG, 160021, false, true)
STAT_EVENT_SET_DEF(ASYNC_TRACE_LOG_DROPPED_COUNT, "async trace log dropped count", ObStatClassIds::DEBUG, 160022, false, true)
STAT_EVENT_SET_DEF(ASYNC_DEBUG_LOG_DROPPED_COUNT, "async debug log dropped count", ObStatClassIds::DEBUG, 160023, false, true)
STAT_EVENT_SET_DEF(ASYNC_LOG_FLUSH_SPEED, "async log flush speed", ObStatClassIds::DEBUG, 160024, false, true)
STAT_EVENT_SET_DEF(ASYNC_GENERIC_LOG_WRITE_COUNT, "async generic log write count", ObStatClassIds::DEBUG, 160025, false, true)
STAT_EVENT_SET_DEF(ASYNC_USER_REQUEST_LOG_WRITE_COUNT, "async user request log write count", ObStatClassIds::DEBUG, 160026, false, true)
STAT_EVENT_SET_DEF(ASYNC_DATA_MAINTAIN_LOG_WRITE_COUNT, "async data maintain log write count", ObStatClassIds::DEBUG, 160027, false, true)
STAT_EVENT_SET_DEF(ASYNC_ROOT_SERVICE_LOG_WRITE_COUNT, "async root service log write count", ObStatClassIds::DEBUG, 160028, false, true)
STAT_EVENT_SET_DEF(ASYNC_SCHEMA_LOG_WRITE_COUNT, "async schema log write count", ObStatClassIds::DEBUG, 160029, false, true)
STAT_EVENT_SET_DEF(ASYNC_FORCE_ALLOW_LOG_WRITE_COUNT, "async force allow log write count", ObStatClassIds::DEBUG, 160030, false, true)
STAT_EVENT_SET_DEF(ASYNC_GENERIC_LOG_DROPPED_COUNT, "async generic log dropped count", ObStatClassIds::DEBUG, 160031, false, true)
STAT_EVENT_SET_DEF(ASYNC_USER_REQUEST_LOG_DROPPED_COUNT, "async user request log dropped count", ObStatClassIds::DEBUG, 160032, false, true)
STAT_EVENT_SET_DEF(ASYNC_DATA_MAINTAIN_LOG_DROPPED_COUNT, "async data maintain log dropped count", ObStatClassIds::DEBUG, 160033, false, true)
STAT_EVENT_SET_DEF(ASYNC_ROOT_SERVICE_LOG_DROPPED_COUNT, "async root service log dropped count", ObStatClassIds::DEBUG, 160034, false, true)
STAT_EVENT_SET_DEF(ASYNC_SCHEMA_LOG_DROPPED_COUNT, "async schema log dropped count", ObStatClassIds::DEBUG, 160035, false, true)
STAT_EVENT_SET_DEF(ASYNC_FORCE_ALLOW_LOG_DROPPED_COUNT, "async force allow log dropped count", ObStatClassIds::DEBUG, 160036, false, true)


//OBSERVER
STAT_EVENT_SET_DEF(OBSERVER_PARTITION_TABLE_UPATER_USER_QUEUE_SIZE, "observer partition table updater user table queue size", ObStatClassIds::OBSERVER, 170001, false, true)
STAT_EVENT_SET_DEF(OBSERVER_PARTITION_TABLE_UPATER_SYS_QUEUE_SIZE, "observer partition table updater system table queue size", ObStatClassIds::OBSERVER, 170002, false, true)
STAT_EVENT_SET_DEF(OBSERVER_PARTITION_TABLE_UPATER_CORE_QUEUE_SIZE, "observer partition table updater core table queue size", ObStatClassIds::OBSERVER, 170003, false, true)

// rootservice
STAT_EVENT_SET_DEF(RS_START_SERVICE_TIME, "rootservice start time", ObStatClassIds::RS, 180001, false, true)
// END
STAT_EVENT_SET_DEF(STAT_EVENT_SET_END, "event set end", ObStatClassIds::DEBUG, 300000, false, false)
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
#define STAT_EVENT_ADD_DEF(def, name, stat_class, stat_id, summary_in_session, can_visible) def,
#include "lib/statistic_event/ob_stat_event.h"
#undef STAT_EVENT_ADD_DEF
#define STAT_EVENT_SET_DEF(def, name, stat_class, stat_id, summary_in_session, can_visible) def,
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
  int64_t stat_id_;
  bool summary_in_session_;
  bool can_visible_;
};


extern const ObStatEvent OB_STAT_EVENTS[];

}
}

#endif /* OB_WAIT_EVENT_DEFINE_H_ */
