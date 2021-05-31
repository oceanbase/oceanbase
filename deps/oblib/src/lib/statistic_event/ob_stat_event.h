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
STAT_EVENT_ADD_DEF(
    RPC_PACKET_IN_BYTES, "rpc packet in bytes", ObStatClassIds::NETWORK, "rpc packet in bytes", 10001, true, true)
STAT_EVENT_ADD_DEF(RPC_PACKET_OUT, "rpc packet out", ObStatClassIds::NETWORK, "rpc packet out", 10002, true, true)
STAT_EVENT_ADD_DEF(
    RPC_PACKET_OUT_BYTES, "rpc packet out bytes", ObStatClassIds::NETWORK, "rpc packet out bytes", 10003, true, true)
STAT_EVENT_ADD_DEF(RPC_DELIVER_FAIL, "rpc deliver fail", ObStatClassIds::NETWORK, "rpc deliver fail", 10004, true, true)
STAT_EVENT_ADD_DEF(RPC_NET_DELAY, "rpc net delay", ObStatClassIds::NETWORK, "rpc net delay", 10005, true, true)
STAT_EVENT_ADD_DEF(
    RPC_NET_FRAME_DELAY, "rpc net frame delay", ObStatClassIds::NETWORK, "rpc net frame delay", 10006, true, true)
STAT_EVENT_ADD_DEF(MYSQL_PACKET_IN, "mysql packet in", ObStatClassIds::NETWORK, "mysql packet in", 10007, true, true)
STAT_EVENT_ADD_DEF(
    MYSQL_PACKET_IN_BYTES, "mysql packet in bytes", ObStatClassIds::NETWORK, "mysql packet in bytes", 10008, true, true)
STAT_EVENT_ADD_DEF(MYSQL_PACKET_OUT, "mysql packet out", ObStatClassIds::NETWORK, "mysql packet out", 10009, true, true)
STAT_EVENT_ADD_DEF(MYSQL_PACKET_OUT_BYTES, "mysql packet out bytes", ObStatClassIds::NETWORK, "mysql packet out bytes",
    10010, true, true)
STAT_EVENT_ADD_DEF(
    MYSQL_DELIVER_FAIL, "mysql deliver fail", ObStatClassIds::NETWORK, "mysql deliver fail", 10011, true, true)
STAT_EVENT_ADD_DEF(RPC_COMPRESS_ORIGINAL_PACKET_CNT, "rpc compress original packet cnt", ObStatClassIds::NETWORK,
    "rpc compress original packet cnt", 10012, true, true)
STAT_EVENT_ADD_DEF(RPC_COMPRESS_COMPRESSED_PACKET_CNT, "rpc compress compressed packet cnt", ObStatClassIds::NETWORK,
    "rpc compress compressed pacekt cnt", 10013, true, true)
STAT_EVENT_ADD_DEF(RPC_COMPRESS_ORIGINAL_SIZE, "rpc compress original size", ObStatClassIds::NETWORK,
    "rpc compress original size", 10014, true, true)
STAT_EVENT_ADD_DEF(RPC_COMPRESS_COMPRESSED_SIZE, "rpc compress compressed size", ObStatClassIds::NETWORK,
    "rpc compress compressed size", 10015, true, true)

STAT_EVENT_ADD_DEF(RPC_STREAM_COMPRESS_ORIGINAL_PACKET_CNT, "rpc stream compress original packet cnt",
    ObStatClassIds::NETWORK, "rpc stream compress original packet cnt", 10016, true, true)
STAT_EVENT_ADD_DEF(RPC_STREAM_COMPRESS_COMPRESSED_PACKET_CNT, "rpc stream compress compressed packet cnt",
    ObStatClassIds::NETWORK, "rpc stream compress compressed pacekt cnt", 10017, true, true)
STAT_EVENT_ADD_DEF(RPC_STREAM_COMPRESS_ORIGINAL_SIZE, "rpc stream compress original size", ObStatClassIds::NETWORK,
    "rpc stream compress original size", 10018, true, true)
STAT_EVENT_ADD_DEF(RPC_STREAM_COMPRESS_COMPRESSED_SIZE, "rpc stream compress compressed size", ObStatClassIds::NETWORK,
    "rpc stream compress compressed size", 10019, true, true)

// QUEUE
// STAT_EVENT_ADD_DEF(REQUEST_QUEUED_COUNT, "REQUEST_QUEUED_COUNT", QUEUE, "REQUEST_QUEUED_COUNT")
STAT_EVENT_ADD_DEF(
    REQUEST_ENQUEUE_COUNT, "request enqueue count", ObStatClassIds::QUEUE, "request enqueue count", 20000, true, true)
STAT_EVENT_ADD_DEF(
    REQUEST_DEQUEUE_COUNT, "request dequeue count", ObStatClassIds::QUEUE, "request dequeue count", 20001, true, true)
STAT_EVENT_ADD_DEF(
    REQUEST_QUEUE_TIME, "request queue time", ObStatClassIds::QUEUE, "request queue time", 20002, true, true)

// TRANS
STAT_EVENT_ADD_DEF(TRANS_COMMIT_LOG_SYNC_TIME, "trans commit log sync time", ObStatClassIds::TRANS,
    "trans commit log sync time", 30000, true, true)
STAT_EVENT_ADD_DEF(TRANS_COMMIT_LOG_SYNC_COUNT, "trans commit log sync count", ObStatClassIds::TRANS,
    "trans commit log sync count", 30001, true, true)
STAT_EVENT_ADD_DEF(TRANS_COMMIT_LOG_SUBMIT_COUNT, "trans commit log submit count", ObStatClassIds::TRANS,
    "trans commit log submit count", 30002, true, true)
STAT_EVENT_ADD_DEF(TRANS_SYSTEM_TRANS_COUNT, "trans system trans count", ObStatClassIds::TRANS,
    "trans system trans count", 30003, true, true)
STAT_EVENT_ADD_DEF(TRANS_USER_TRANS_COUNT, "trans user trans count", ObStatClassIds::TRANS, "trans user trans count",
    30004, true, true)
STAT_EVENT_ADD_DEF(
    TRANS_START_COUNT, "trans start count", ObStatClassIds::TRANS, "trans start count", 30005, true, true)
STAT_EVENT_ADD_DEF(
    TRANS_TOTAL_USED_TIME, "trans total used time", ObStatClassIds::TRANS, "trans total used time", 30006, true, true)
STAT_EVENT_ADD_DEF(
    TRANS_COMMIT_COUNT, "trans commit count", ObStatClassIds::TRANS, "trans commit count", 30007, true, true)
STAT_EVENT_ADD_DEF(
    TRANS_COMMIT_TIME, "trans commit time", ObStatClassIds::TRANS, "trans commit time", 30008, true, true)
STAT_EVENT_ADD_DEF(
    TRANS_ROLLBACK_COUNT, "trans rollback count", ObStatClassIds::TRANS, "trans rollback count", 30009, true, true)
STAT_EVENT_ADD_DEF(
    TRANS_ROLLBACK_TIME, "trans rollback time", ObStatClassIds::TRANS, "trans rollback time", 30010, true, true)
STAT_EVENT_ADD_DEF(
    TRANS_TIMEOUT_COUNT, "trans timeout count", ObStatClassIds::TRANS, "trans timeout count", 30011, true, true)
STAT_EVENT_ADD_DEF(TRANS_SINGLE_PARTITION_COUNT, "trans single partition count", ObStatClassIds::TRANS,
    "trans single partition count", 30012, true, true)
STAT_EVENT_ADD_DEF(TRANS_MULTI_PARTITION_COUNT, "trans multi partition count", ObStatClassIds::TRANS,
    "trans multi partition count", 30013, true, true)
// STAT_EVENT_ADD_DEF(TRANS_DISTRIBUTED_STMT_COUNT, "trans distributed stmt count", TRANS, "trans distributed stmt
// count", 30014) STAT_EVENT_ADD_DEF(TRANS_LOCAL_STMT_COUNT, "trans local stmt count", TRANS, "trans local stmt count",
// 30015) STAT_EVENT_ADD_DEF(TRANS_REMOTE_STMT_COUNT, "trans remote stmt count", TRANS, "trans remote stmt count",
// 30016)
STAT_EVENT_ADD_DEF(TRANS_ZERO_PARTITION_COUNT, "trans zero partition count", ObStatClassIds::TRANS,
    "trans zero partition count", 30017, true, true)
STAT_EVENT_ADD_DEF(
    REDO_LOG_REPLAY_COUNT, "redo log replay count", ObStatClassIds::TRANS, "redo log replay count", 30018, true, true)
STAT_EVENT_ADD_DEF(
    REDO_LOG_REPLAY_TIME, "redo log replay time", ObStatClassIds::TRANS, "redo log replay time", 30019, true, true)
STAT_EVENT_ADD_DEF(PREPARE_LOG_REPLAY_COUNT, "prepare log replay count", ObStatClassIds::TRANS,
    "prepare log replay count", 30020, true, true)
STAT_EVENT_ADD_DEF(PREPARE_LOG_REPLAY_TIME, "prepare log replay time", ObStatClassIds::TRANS, "prepare log replay time",
    30021, true, true)
STAT_EVENT_ADD_DEF(COMMIT_LOG_REPLAY_COUNT, "commit log replay count", ObStatClassIds::TRANS, "commit log replay count",
    30022, true, true)
STAT_EVENT_ADD_DEF(COMMIT_LOG_REPLAY_TIME, "commit log replay time", ObStatClassIds::TRANS, "commit log replay time",
    30023, true, true)
STAT_EVENT_ADD_DEF(ABORT_LOG_REPLAY_COUNT, "abort log replay count", ObStatClassIds::TRANS, "abort log replay count",
    30024, true, true)
STAT_EVENT_ADD_DEF(
    ABORT_LOG_REPLAY_TIME, "abort log replay time", ObStatClassIds::TRANS, "abort log replay time", 30025, true, true)
STAT_EVENT_ADD_DEF(CLEAR_LOG_REPLAY_COUNT, "clear log replay count", ObStatClassIds::TRANS, "clear log replay count",
    30026, true, true)
STAT_EVENT_ADD_DEF(
    CLEAR_LOG_REPLAY_TIME, "clear log replay time", ObStatClassIds::TRANS, "clear log replay time", 30027, true, true)
STAT_EVENT_ADD_DEF(
    SP_REDO_LOG_CB_COUNT, "sp redo log cb count", ObStatClassIds::TRANS, "sp redo log cb count", 30028, true, true)
STAT_EVENT_ADD_DEF(
    SP_REDO_LOG_CB_TIME, "sp redo log cb time", ObStatClassIds::TRANS, "sp redo log cb time", 30029, true, true)
STAT_EVENT_ADD_DEF(SP_COMMIT_LOG_CB_COUNT, "sp commit log cb count", ObStatClassIds::TRANS, "sp commit log cb count",
    30030, true, true)
STAT_EVENT_ADD_DEF(
    SP_COMMIT_LOG_CB_TIME, "sp commit log cb time", ObStatClassIds::TRANS, "sp commit log cb time", 30031, true, true)
STAT_EVENT_ADD_DEF(
    REDO_LOG_CB_COUNT, "redo log cb count", ObStatClassIds::TRANS, "redo log cb count", 30032, true, true)
STAT_EVENT_ADD_DEF(REDO_LOG_CB_TIME, "redo log cb time", ObStatClassIds::TRANS, "redo log cb time", 30033, true, true)
STAT_EVENT_ADD_DEF(
    PREPARE_LOG_CB_COUNT, "prepare log cb count", ObStatClassIds::TRANS, "prepare log cb count", 30034, true, true)
STAT_EVENT_ADD_DEF(
    PREPARE_LOG_CB_TIME, "prepare log cb time", ObStatClassIds::TRANS, "prepare log cb time", 30035, true, true)
STAT_EVENT_ADD_DEF(
    COMMIT_LOG_CB_COUNT, "commit log cb count", ObStatClassIds::TRANS, "commit log cb count", 30036, true, true)
STAT_EVENT_ADD_DEF(
    COMMIT_LOG_CB_TIME, "commit log cb time", ObStatClassIds::TRANS, "commit log cb time", 30037, true, true)
STAT_EVENT_ADD_DEF(
    ABORT_LOG_CB_COUNT, "abort log cb count", ObStatClassIds::TRANS, "abort log cb count", 30038, true, true)
STAT_EVENT_ADD_DEF(
    ABORT_LOG_CB_TIME, "abort log cb time", ObStatClassIds::TRANS, "abort log cb time", 30039, true, true)
STAT_EVENT_ADD_DEF(
    CLEAR_LOG_CB_COUNT, "clear log cb count", ObStatClassIds::TRANS, "clear log cb count", 30040, true, true)
STAT_EVENT_ADD_DEF(
    CLEAR_LOG_CB_TIME, "clear log cb time", ObStatClassIds::TRANS, "clear log cb time", 30041, true, true)
STAT_EVENT_ADD_DEF(TRANS_MT_END_COUNT, "trans memtable end count", ObStatClassIds::TRANS, "trans memtable end count",
    30042, true, true)
STAT_EVENT_ADD_DEF(
    TRANS_MT_END_TIME, "trans memtable end time", ObStatClassIds::TRANS, "trans memtable end time", 30043, true, true)
STAT_EVENT_ADD_DEF(TRANS_CALLBACK_SQL_COUNT, "trans callback sql count", ObStatClassIds::TRANS,
    "trans callback sql count", 30044, true, true)
STAT_EVENT_ADD_DEF(TRANS_CALLBACK_SQL_TIME, "trans callback sql time", ObStatClassIds::TRANS, "trans callback sql time",
    30045, true, true)
STAT_EVENT_ADD_DEF(TRANS_STRONG_CONSISTENCY_STMT_TIMEOUT_COUNT, "strong consistency stmt timeout count",
    ObStatClassIds::TRANS, "strong consistency stmt timeout count", 30046, true, true)
STAT_EVENT_ADD_DEF(TRANS_SLAVE_READ_STMT_TIMEOUT_COUNT, "slave read stmt timeout count", ObStatClassIds::TRANS,
    "slave read stmt timeout count", 30047, true, true)
STAT_EVENT_ADD_DEF(TRANS_SLAVE_READ_STMT_RETRY_COUNT, "slave read stmt retry count", ObStatClassIds::TRANS,
    "slave read stmt retry count", 30048, true, true)
STAT_EVENT_ADD_DEF(TRANS_FILL_REDO_LOG_COUNT, "trans fill redo log count", ObStatClassIds::TRANS,
    "trans fill redo log count", 30049, true, true)
STAT_EVENT_ADD_DEF(TRANS_FILL_REDO_LOG_TIME, "trans fill redo log time", ObStatClassIds::TRANS,
    "trans fill redo log time", 30050, true, true)
STAT_EVENT_ADD_DEF(TRANS_SUBMIT_LOG_COUNT, "trans submit log count", ObStatClassIds::TRANS, "trans submit log count",
    30051, true, true)
STAT_EVENT_ADD_DEF(
    TRANS_SUBMIT_LOG_TIME, "trans submit log time", ObStatClassIds::TRANS, "trans submit log time", 30052, true, true)
STAT_EVENT_ADD_DEF(GTS_REQUEST_TOTAL_COUNT, "gts request total count", ObStatClassIds::TRANS, "gts request total count",
    30053, true, true)
STAT_EVENT_ADD_DEF(GTS_ACQUIRE_TOTAL_TIME, "gts acquire total time", ObStatClassIds::TRANS, "gts acquire total time",
    30054, true, true)
STAT_EVENT_ADD_DEF(GTS_ACQUIRE_TOTAL_COUNT, "gts acquire total count", ObStatClassIds::TRANS, "gts acquire total count",
    30055, true, true)
STAT_EVENT_ADD_DEF(GTS_ACQUIRE_TOTAL_WAIT_COUNT, "gts acquire total wait count", ObStatClassIds::TRANS,
    "gts acquire total wait count", 30056, true, true)
STAT_EVENT_ADD_DEF(TRANS_STMT_TOTAL_COUNT, "trans stmt total count", ObStatClassIds::TRANS, "trans stmt total count",
    30057, true, true)
STAT_EVENT_ADD_DEF(TRANS_STMT_INTERVAL_TIME, "trans stmt interval time", ObStatClassIds::TRANS,
    "trans stmt interval time", 30058, true, true)
STAT_EVENT_ADD_DEF(TRANS_BATCH_COMMIT_COUNT, "trans batch commit count", ObStatClassIds::TRANS,
    "trans batch commit count", 30059, true, true)
STAT_EVENT_ADD_DEF(TRANS_RELOCATE_ROW_COUNT, "trans relocate row count", ObStatClassIds::TRANS,
    "trans relocate row count", 30060, true, true)
STAT_EVENT_ADD_DEF(TRANS_RELOCATE_TOTAL_TIME, "trans relocate total time", ObStatClassIds::TRANS,
    "trans relocate total time", 30061, true, true)
STAT_EVENT_ADD_DEF(TRANS_MULTI_PARTITION_UPDATE_STMT_COUNT, "trans multi partition update stmt count",
    ObStatClassIds::TRANS, "trans multi partition update stmt count", 30062, true, true)
STAT_EVENT_ADD_DEF(GTS_WAIT_ELAPSE_TOTAL_TIME, "gts wait elapse total time", ObStatClassIds::TRANS,
    "gts wait elapse total time", 30063, true, true)
STAT_EVENT_ADD_DEF(GTS_WAIT_ELAPSE_TOTAL_COUNT, "gts wait elapse total count", ObStatClassIds::TRANS,
    "gts wait elapse total count", 30064, true, true)
STAT_EVENT_ADD_DEF(GTS_WAIT_ELAPSE_TOTAL_WAIT_COUNT, "gts wait elapse total wait count", ObStatClassIds::TRANS,
    "gts wait elapse total wait count", 30065, true, true)
STAT_EVENT_ADD_DEF(GTS_RPC_COUNT, "gts rpc count", ObStatClassIds::TRANS, "gts rpc count", 30066, true, true)
STAT_EVENT_ADD_DEF(GTS_TRY_ACQUIRE_TOTAL_COUNT, "gts try acquire total count", ObStatClassIds::TRANS,
    "gts try acquire total count", 30067, true, true)
STAT_EVENT_ADD_DEF(GTS_TRY_WAIT_ELAPSE_TOTAL_COUNT, "gts try wait elapse total count", ObStatClassIds::TRANS,
    "gts try wait elapse total count", 30068, true, true)
STAT_EVENT_ADD_DEF(HA_GTS_HANDLE_GET_REQUEST_COUNT, "ha gts handle get request count", ObStatClassIds::TRANS,
    "ha gts handle get request count", 30069, true, true)
STAT_EVENT_ADD_DEF(HA_GTS_SEND_GET_RESPONSE_COUNT, "ha gts send get response count", ObStatClassIds::TRANS,
    "ha gts send get response count", 30070, true, true)
STAT_EVENT_ADD_DEF(HA_GTS_HANDLE_PING_REQUEST_COUNT, "ha gts handle ping request count", ObStatClassIds::TRANS,
    "ha gts handle ping request count", 30071, true, true)
STAT_EVENT_ADD_DEF(HA_GTS_HANDLE_PING_RESPONSE_COUNT, "ha gts handle ping response count", ObStatClassIds::TRANS,
    "ha gts handle ping response count", 30072, true, true)

// SQL
// STAT_EVENT_ADD_DEF(PLAN_CACHE_HIT, "PLAN_CACHE_HIT", SQL, "PLAN_CACHE_HIT")
// STAT_EVENT_ADD_DEF(PLAN_CACHE_MISS, "PLAN_CACHE_MISS", SQL, "PLAN_CACHE_MISS")
STAT_EVENT_ADD_DEF(SQL_SELECT_COUNT, "sql select count", ObStatClassIds::SQL, "sql select count", 40000, true, true)
STAT_EVENT_ADD_DEF(SQL_SELECT_TIME, "sql select time", ObStatClassIds::SQL, "sql select time", 40001, true, true)
STAT_EVENT_ADD_DEF(SQL_INSERT_COUNT, "sql insert count", ObStatClassIds::SQL, "sql insert count", 40002, true, true)
STAT_EVENT_ADD_DEF(SQL_INSERT_TIME, "sql insert time", ObStatClassIds::SQL, "sql insert time", 40003, true, true)
STAT_EVENT_ADD_DEF(SQL_REPLACE_COUNT, "sql replace count", ObStatClassIds::SQL, "sql replace count", 40004, true, true)
STAT_EVENT_ADD_DEF(SQL_REPLACE_TIME, "sql replace time", ObStatClassIds::SQL, "sql replace time", 40005, true, true)
STAT_EVENT_ADD_DEF(SQL_UPDATE_COUNT, "sql update count", ObStatClassIds::SQL, "sql update count", 40006, true, true)
STAT_EVENT_ADD_DEF(SQL_UPDATE_TIME, "sql update time", ObStatClassIds::SQL, "sql update time", 40007, true, true)
STAT_EVENT_ADD_DEF(SQL_DELETE_COUNT, "sql delete count", ObStatClassIds::SQL, "sql delete count", 40008, true, true)
STAT_EVENT_ADD_DEF(SQL_DELETE_TIME, "sql delete time", ObStatClassIds::SQL, "sql delete time", 40009, true, true)
STAT_EVENT_ADD_DEF(SQL_OTHER_COUNT, "sql other count", ObStatClassIds::SQL, "sql other count", 40018, true, true)
STAT_EVENT_ADD_DEF(SQL_OTHER_TIME, "sql other time", ObStatClassIds::SQL, "sql other time", 40019, true, true)
STAT_EVENT_ADD_DEF(SQL_PS_PREPARE_COUNT, "ps prepare count", ObStatClassIds::SQL, "ps prepare count", 40020, true, true)
STAT_EVENT_ADD_DEF(SQL_PS_PREPARE_TIME, "ps prepare time", ObStatClassIds::SQL, "ps prepare time", 40021, true, true)
STAT_EVENT_ADD_DEF(SQL_PS_EXECUTE_COUNT, "ps execute count", ObStatClassIds::SQL, "ps execute count", 40022, true, true)
STAT_EVENT_ADD_DEF(SQL_PS_CLOSE_COUNT, "ps close count", ObStatClassIds::SQL, "ps close count", 40023, true, true)
STAT_EVENT_ADD_DEF(SQL_PS_CLOSE_TIME, "ps close time", ObStatClassIds::SQL, "ps close time", 40024, true, true)

STAT_EVENT_ADD_DEF(SQL_OPEN_CURSORS_CURRENT, "opened cursors current", ObStatClassIds::SQL, "opened cursors current",
    40030, true, true)
STAT_EVENT_ADD_DEF(SQL_OPEN_CURSORS_CUMULATIVE, "opened cursors cumulative", ObStatClassIds::SQL,
    "opened cursors cumulative", 40031, true, true)

STAT_EVENT_ADD_DEF(SQL_LOCAL_COUNT, "sql local count", ObStatClassIds::SQL, "sql local count", 40010, true, true)
// STAT_EVENT_ADD_DEF(SQL_LOCAL_TIME, "SQL_LOCAL_TIME", SQL, "SQL_LOCAL_TIME")
STAT_EVENT_ADD_DEF(SQL_REMOTE_COUNT, "sql remote count", ObStatClassIds::SQL, "sql remote count", 40011, true, true)
// STAT_EVENT_ADD_DEF(SQL_REMOTE_TIME, "SQL_REMOTE_TIME", SQL, "SQL_REMOTE_TIME")
STAT_EVENT_ADD_DEF(
    SQL_DISTRIBUTED_COUNT, "sql distributed count", ObStatClassIds::SQL, "sql distributed count", 40012, true, true)
// STAT_EVENT_ADD_DEF(SQL_DISTRIBUTED_TIME, "SQL_DISTRIBUTED_TIME", SQL, "SQL_DISTRIBUTED_TIME")
STAT_EVENT_ADD_DEF(ACTIVE_SESSIONS, "active sessions", ObStatClassIds::SQL, "active sessions", 40013, false, true)
STAT_EVENT_ADD_DEF(
    SQL_SINGLE_QUERY_COUNT, "single query count", ObStatClassIds::SQL, "single query count", 40014, true, true)
STAT_EVENT_ADD_DEF(
    SQL_MULTI_QUERY_COUNT, "multiple query count", ObStatClassIds::SQL, "multiple query count", 40015, true, true)
STAT_EVENT_ADD_DEF(SQL_MULTI_ONE_QUERY_COUNT, "multiple query with one stmt count", ObStatClassIds::SQL,
    "multiple query with one stmt count", 40016, true, true)

STAT_EVENT_ADD_DEF(
    SQL_INNER_SELECT_COUNT, "sql inner select count", ObStatClassIds::SQL, "sql inner select count", 40100, true, true)
STAT_EVENT_ADD_DEF(
    SQL_INNER_SELECT_TIME, "sql inner select time", ObStatClassIds::SQL, "sql inner select time", 40101, true, true)
STAT_EVENT_ADD_DEF(
    SQL_INNER_INSERT_COUNT, "sql inner insert count", ObStatClassIds::SQL, "sql inner insert count", 40102, true, true)
STAT_EVENT_ADD_DEF(
    SQL_INNER_INSERT_TIME, "sql inner insert time", ObStatClassIds::SQL, "sql inner insert time", 40103, true, true)
STAT_EVENT_ADD_DEF(SQL_INNER_REPLACE_COUNT, "sql inner replace count", ObStatClassIds::SQL, "sql inner replace count",
    40104, true, true)
STAT_EVENT_ADD_DEF(
    SQL_INNER_REPLACE_TIME, "sql inner replace time", ObStatClassIds::SQL, "sql inner replace time", 40105, true, true)
STAT_EVENT_ADD_DEF(
    SQL_INNER_UPDATE_COUNT, "sql inner update count", ObStatClassIds::SQL, "sql inner update count", 40106, true, true)
STAT_EVENT_ADD_DEF(
    SQL_INNER_UPDATE_TIME, "sql inner update time", ObStatClassIds::SQL, "sql inner update time", 40107, true, true)
STAT_EVENT_ADD_DEF(
    SQL_INNER_DELETE_COUNT, "sql inner delete count", ObStatClassIds::SQL, "sql inner delete count", 40108, true, true)
STAT_EVENT_ADD_DEF(
    SQL_INNER_DELETE_TIME, "sql inner delete time", ObStatClassIds::SQL, "sql inner delete time", 40109, true, true)
STAT_EVENT_ADD_DEF(
    SQL_INNER_OTHER_COUNT, "sql inner other count", ObStatClassIds::SQL, "sql inner other count", 40110, true, true)
STAT_EVENT_ADD_DEF(
    SQL_INNER_OTHER_TIME, "sql inner other time", ObStatClassIds::SQL, "sql inner other time", 40111, true, true)

// CACHE
STAT_EVENT_ADD_DEF(ROW_CACHE_HIT, "row cache hit", ObStatClassIds::CACHE, "row cache hit", 50000, true, true)
STAT_EVENT_ADD_DEF(ROW_CACHE_MISS, "row cache miss", ObStatClassIds::CACHE, "row cache miss", 50001, true, true)
STAT_EVENT_ADD_DEF(
    BLOCK_INDEX_CACHE_HIT, "block index cache hit", ObStatClassIds::CACHE, "block index cache hit", 50002, true, true)
STAT_EVENT_ADD_DEF(BLOCK_INDEX_CACHE_MISS, "block index cache miss", ObStatClassIds::CACHE, "block index cache miss",
    50003, true, true)
STAT_EVENT_ADD_DEF(BLOOM_FILTER_CACHE_HIT, "bloom filter cache hit", ObStatClassIds::CACHE, "bloom filter cache hit",
    50004, true, true)
STAT_EVENT_ADD_DEF(BLOOM_FILTER_CACHE_MISS, "bloom filter cache miss", ObStatClassIds::CACHE, "bloom filter cache miss",
    50005, true, true)
STAT_EVENT_ADD_DEF(
    BLOOM_FILTER_FILTS, "bloom filter filts", ObStatClassIds::CACHE, "bloom filter filts", 50006, true, true)
STAT_EVENT_ADD_DEF(
    BLOOM_FILTER_PASSES, "bloom filter passes", ObStatClassIds::CACHE, "bloom filter passes", 50007, true, true)
STAT_EVENT_ADD_DEF(BLOCK_CACHE_HIT, "block cache hit", ObStatClassIds::CACHE, "block cache hit", 50008, true, true)
STAT_EVENT_ADD_DEF(BLOCK_CACHE_MISS, "block cache miss", ObStatClassIds::CACHE, "block cache miss", 50009, true, true)
STAT_EVENT_ADD_DEF(
    LOCATION_CACHE_HIT, "location cache hit", ObStatClassIds::CACHE, "location cache hit", 50010, true, true)
STAT_EVENT_ADD_DEF(
    LOCATION_CACHE_MISS, "location cache miss", ObStatClassIds::CACHE, "location cache miss", 50011, true, true)
STAT_EVENT_ADD_DEF(
    LOCATION_CACHE_WAIT, "location cache wait", ObStatClassIds::CACHE, "location cache wait", 50012, true, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_PROXY_HIT, "location cache get hit from proxy virtual table", ObStatClassIds::CACHE,
    "location cache get hit from proxy virtual table", 50013, true, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_PROXY_MISS, "location cache get miss from proxy virtual table", ObStatClassIds::CACHE,
    "location cache get miss from proxy virtual table", 50014, true, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_CLEAR_LOCATION, "location cache nonblock renew", ObStatClassIds::CACHE,
    "location cache nonblock renew", 50015, true, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_CLEAR_LOCATION_IGNORED, "location cache nonblock renew ignored",
    ObStatClassIds::CACHE, "location cache nonblock renew ignored", 50016, true, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_NONBLOCK_HIT, "location nonblock get hit", ObStatClassIds::CACHE,
    "location nonblock get hit", 50017, true, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_NONBLOCK_MISS, "location nonblock get miss", ObStatClassIds::CACHE,
    "location nonblock get miss", 50018, true, true)
// STAT_EVENT_ADD_DEF(LOCATION_CACHE_RPC_CHECK_LEADER, "location check leader count", ObStatClassIds::CACHE, "location
// check leader count", 50019, true, true) STAT_EVENT_ADD_DEF(LOCATION_CACHE_RPC_CHECK_MEMBER, "location check member
// count", ObStatClassIds::CACHE, "location check member count", 50020, true, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_RPC_CHECK, "location cache rpc renew count", ObStatClassIds::CACHE,
    "location cache rpc renew count", 50021, true, true)
STAT_EVENT_ADD_DEF(
    LOCATION_CACHE_RENEW, "location cache renew", ObStatClassIds::CACHE, "location cache renew", 50022, true, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_RENEW_IGNORED, "location cache renew ignored", ObStatClassIds::CACHE,
    "location cache renew ignored", 50023, true, true)
STAT_EVENT_ADD_DEF(MMAP_COUNT, "mmap count", ObStatClassIds::CACHE, "mmap count", 50024, true, true)
STAT_EVENT_ADD_DEF(MUNMAP_COUNT, "munmap count", ObStatClassIds::CACHE, "munmap count", 50025, true, true)
STAT_EVENT_ADD_DEF(MMAP_SIZE, "mmap size", ObStatClassIds::CACHE, "mmap size", 50026, true, true)
STAT_EVENT_ADD_DEF(MUNMAP_SIZE, "munmap size", ObStatClassIds::CACHE, "munmap size", 50027, true, true)
STAT_EVENT_ADD_DEF(KVCACHE_SYNC_WASH_TIME, "kvcache sync wash time", ObStatClassIds::CACHE, "kvcache sync wash time",
    50028, true, true)
STAT_EVENT_ADD_DEF(KVCACHE_SYNC_WASH_COUNT, "kvcache sync wash count", ObStatClassIds::CACHE, "kvcache sync wash count",
    50029, true, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_RPC_RENEW_FAIL, "location cache rpc renew fail count", ObStatClassIds::CACHE,
    "location cache rpc renew fail count", 50030, true, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_SQL_RENEW, "location cache sql renew count", ObStatClassIds::CACHE,
    "location cache sql renew count", 50031, true, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_IGNORE_RPC_RENEW, "location cache ignore rpc renew count", ObStatClassIds::CACHE,
    "location cache ignore rpc renew count", 50032, true, true)
STAT_EVENT_ADD_DEF(
    FUSE_ROW_CACHE_HIT, "fuse row cache hit", ObStatClassIds::CACHE, "fuse row cache hit", 50033, true, true)
STAT_EVENT_ADD_DEF(
    FUSE_ROW_CACHE_MISS, "fuse row cache miss", ObStatClassIds::CACHE, "fuse row cache miss", 50034, true, true)
STAT_EVENT_ADD_DEF(SCHEMA_CACHE_HIT, "schema cache hit", ObStatClassIds::CACHE, "schema cache hit", 50035, true, true)
STAT_EVENT_ADD_DEF(
    SCHEMA_CACHE_MISS, "schema cache miss", ObStatClassIds::CACHE, "schema cache miss", 50036, true, true)
STAT_EVENT_ADD_DEF(CLOG_CACHE_HIT, "clog cache hit", ObStatClassIds::CACHE, "clog cache hit", 50037, true, true)
STAT_EVENT_ADD_DEF(CLOG_CACHE_MISS, "clog cache miss", ObStatClassIds::CACHE, "clog cache miss", 50038, true, true)
STAT_EVENT_ADD_DEF(
    INDEX_CLOG_CACHE_HIT, "index clog cache hit", ObStatClassIds::CACHE, "index clog cache hit", 50039, true, true)
STAT_EVENT_ADD_DEF(
    INDEX_CLOG_CACHE_MISS, "index clog cache miss", ObStatClassIds::CACHE, "index clog cache miss", 50040, true, true)
STAT_EVENT_ADD_DEF(USER_TAB_COL_STAT_CACHE_HIT, "user tab col stat cache hit", ObStatClassIds::CACHE,
    "user tab col stat cache hit", 50041, true, true)
STAT_EVENT_ADD_DEF(USER_TAB_COL_STAT_CACHE_MISS, "user tab col stat cache miss", ObStatClassIds::CACHE,
    "user tab col stat cache miss", 50042, true, true)
STAT_EVENT_ADD_DEF(USER_TABLE_STAT_CACHE_HIT, "user table stat cache hit", ObStatClassIds::CACHE,
    "user table stat cache hit", 50043, true, true)
STAT_EVENT_ADD_DEF(USER_TABLE_STAT_CACHE_MISS, "user table stat cache miss", ObStatClassIds::CACHE,
    "user table stat cache miss", 50044, true, true)
STAT_EVENT_ADD_DEF(OPT_TABLE_STAT_CACHE_HIT, "opt table stat cache hit", ObStatClassIds::CACHE,
    "opt table stat cache hit", 50045, true, true)
STAT_EVENT_ADD_DEF(OPT_TABLE_STAT_CACHE_MISS, "opt table stat cache miss", ObStatClassIds::CACHE,
    "opt table stat cache miss", 50046, true, true)
STAT_EVENT_ADD_DEF(OPT_COLUMN_STAT_CACHE_HIT, "opt column stat cache hit", ObStatClassIds::CACHE,
    "opt column stat cache hit", 50047, true, true)
STAT_EVENT_ADD_DEF(OPT_COLUMN_STAT_CACHE_MISS, "opt column stat cache miss", ObStatClassIds::CACHE,
    "opt column stat cache miss", 50048, true, true)
STAT_EVENT_ADD_DEF(
    TMP_PAGE_CACHE_HIT, "tmp page cache hit", ObStatClassIds::CACHE, "tmp page cache hit", 50049, true, true)
STAT_EVENT_ADD_DEF(
    TMP_PAGE_CACHE_MISS, "tmp page cache miss", ObStatClassIds::CACHE, "tmp page cache miss", 50050, true, true)
STAT_EVENT_ADD_DEF(
    TMP_BLOCK_CACHE_HIT, "tmp block cache hit", ObStatClassIds::CACHE, "tmp block cache hit", 50051, true, true)
STAT_EVENT_ADD_DEF(
    TMP_BLOCK_CACHE_MISS, "tmp block cache miss", ObStatClassIds::CACHE, "tmp block cache miss", 50052, true, true)

// STORAGE
// STAT_EVENT_ADD_DEF(MEMSTORE_LOGICAL_READS, "MEMSTORE_LOGICAL_READS", STORAGE, "MEMSTORE_LOGICAL_READS")
// STAT_EVENT_ADD_DEF(MEMSTORE_LOGICAL_BYTES, "MEMSTORE_LOGICAL_BYTES", STORAGE, "MEMSTORE_LOGICAL_BYTES")
// STAT_EVENT_ADD_DEF(SSTABLE_LOGICAL_READS, "SSTABLE_LOGICAL_READS", STORAGE, "SSTABLE_LOGICAL_READS")
// STAT_EVENT_ADD_DEF(SSTABLE_LOGICAL_BYTES, "SSTABLE_LOGICAL_BYTES", STORAGE, "SSTABLE_LOGICAL_BYTES")
STAT_EVENT_ADD_DEF(IO_READ_COUNT, "io read count", ObStatClassIds::STORAGE, "io read count", 60000, true, true)
STAT_EVENT_ADD_DEF(IO_READ_DELAY, "io read delay", ObStatClassIds::STORAGE, "io read delay", 60001, true, true)
STAT_EVENT_ADD_DEF(IO_READ_BYTES, "io read bytes", ObStatClassIds::STORAGE, "io read bytes", 60002, true, true)
STAT_EVENT_ADD_DEF(IO_WRITE_COUNT, "io write count", ObStatClassIds::STORAGE, "io write count", 60003, true, true)
STAT_EVENT_ADD_DEF(IO_WRITE_DELAY, "io write delay", ObStatClassIds::STORAGE, "io write delay", 60004, true, true)
STAT_EVENT_ADD_DEF(IO_WRITE_BYTES, "io write bytes", ObStatClassIds::STORAGE, "io write bytes", 60005, true, true)
STAT_EVENT_ADD_DEF(
    MEMSTORE_SCAN_COUNT, "memstore scan count", ObStatClassIds::STORAGE, "memstore scan count", 60006, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_SCAN_SUCC_COUNT, "memstore scan succ count", ObStatClassIds::STORAGE,
    "memstore scan succ count", 60007, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_SCAN_FAIL_COUNT, "memstore scan fail count", ObStatClassIds::STORAGE,
    "memstore scan fail count", 60008, true, true)
STAT_EVENT_ADD_DEF(
    MEMSTORE_GET_COUNT, "memstore get count", ObStatClassIds::STORAGE, "memstore get count", 60009, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_GET_SUCC_COUNT, "memstore get succ count", ObStatClassIds::STORAGE,
    "memstore get succ count", 60010, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_GET_FAIL_COUNT, "memstore get fail count", ObStatClassIds::STORAGE,
    "memstore get fail count", 60011, true, true)
STAT_EVENT_ADD_DEF(
    MEMSTORE_APPLY_COUNT, "memstore apply count", ObStatClassIds::STORAGE, "memstore apply count", 60012, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_APPLY_SUCC_COUNT, "memstore apply succ count", ObStatClassIds::STORAGE,
    "memstore apply succ count", 60013, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_APPLY_FAIL_COUNT, "memstore apply fail count", ObStatClassIds::STORAGE,
    "memstore apply fail count", 60014, true, true)
STAT_EVENT_ADD_DEF(
    MEMSTORE_ROW_COUNT, "memstore row count", ObStatClassIds::STORAGE, "memstore row count", 60015, true, true)
STAT_EVENT_ADD_DEF(
    MEMSTORE_GET_TIME, "memstore get time", ObStatClassIds::STORAGE, "memstore get time", 60016, true, true)
STAT_EVENT_ADD_DEF(
    MEMSTORE_SCAN_TIME, "memstore scan time", ObStatClassIds::STORAGE, "memstore scan time", 60017, true, true)
STAT_EVENT_ADD_DEF(
    MEMSTORE_APPLY_TIME, "memstore apply time", ObStatClassIds::STORAGE, "memstore apply time", 60018, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_READ_LOCK_SUCC_COUNT, "memstore read lock succ count", ObStatClassIds::STORAGE,
    "memstore read lock succ count", 60019, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_READ_LOCK_FAIL_COUNT, "memstore read lock fail count", ObStatClassIds::STORAGE,
    "memstore read lock fail count", 60020, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_WRITE_LOCK_SUCC_COUNT, "memstore write lock succ count", ObStatClassIds::STORAGE,
    "memstore write lock succ count", 60021, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_WRITE_LOCK_FAIL_COUNT, "memstore write lock fail count", ObStatClassIds::STORAGE,
    "memstore write lock fail count", 60022, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_WAIT_WRITE_LOCK_TIME, "memstore wait write lock time", ObStatClassIds::STORAGE,
    "memstore wait write lock time", 60023, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_WAIT_READ_LOCK_TIME, "memstore wait read lock time", ObStatClassIds::STORAGE,
    "memstore wait read lock time", 60024, true, true)
STAT_EVENT_ADD_DEF(IO_READ_MICRO_INDEX_COUNT, "io read micro index count", ObStatClassIds::STORAGE,
    "io read micro index count", 60025, true, true)
STAT_EVENT_ADD_DEF(IO_READ_MICRO_INDEX_BYTES, "io read micro index bytes", ObStatClassIds::STORAGE,
    "io read micro index bytes", 60026, true, true)
STAT_EVENT_ADD_DEF(IO_READ_PREFETCH_MICRO_COUNT, "io prefetch micro block count", ObStatClassIds::STORAGE,
    "io prefetch micro block count", 60027, true, true)
STAT_EVENT_ADD_DEF(IO_READ_PREFETCH_MICRO_BYTES, "io prefetch micro block bytes", ObStatClassIds::STORAGE,
    "io prefetch micro block bytes", 60028, true, true)
STAT_EVENT_ADD_DEF(IO_READ_UNCOMP_MICRO_COUNT, "io read uncompress micro block count", ObStatClassIds::STORAGE,
    "io read uncompress micro block count", 60029, true, true)
STAT_EVENT_ADD_DEF(IO_READ_UNCOMP_MICRO_BYTES, "io read uncompress micro block bytes", ObStatClassIds::STORAGE,
    "io read uncompress micro block bytes", 60030, true, true)
STAT_EVENT_ADD_DEF(STORAGE_READ_ROW_COUNT, "storage read row count", ObStatClassIds::STORAGE, "storage read row count",
    60031, true, true)
STAT_EVENT_ADD_DEF(STORAGE_DELETE_ROW_COUNT, "storage delete row count", ObStatClassIds::STORAGE,
    "storage delete row count", 60032, true, true)
STAT_EVENT_ADD_DEF(STORAGE_INSERT_ROW_COUNT, "storage insert row count", ObStatClassIds::STORAGE,
    "storage insert row count", 60033, true, true)
STAT_EVENT_ADD_DEF(STORAGE_UPDATE_ROW_COUNT, "storage update row count", ObStatClassIds::STORAGE,
    "storage update row count", 60034, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_MUTATOR_REPLAY_TIME, "memstore mutator replay time", ObStatClassIds::STORAGE,
    "memstore mutator replay time", 60035, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_MUTATOR_REPLAY_COUNT, "memstore mutator replay count", ObStatClassIds::STORAGE,
    "memstore mutator replay count", 60036, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_ROW_PURGE_COUNT, "memstore row purge count", ObStatClassIds::STORAGE,
    "memstore row purge count", 60037, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_ROW_COMPACTION_COUNT, "memstore row compaction count", ObStatClassIds::STORAGE,
    "memstore row compaction count", 60038, true, true)

STAT_EVENT_ADD_DEF(
    IO_READ_QUEUE_DELAY, "io read queue delay", ObStatClassIds::STORAGE, "io read queue delay", 60039, true, true)
STAT_EVENT_ADD_DEF(
    IO_WRITE_QUEUE_DELAY, "io write queue delay", ObStatClassIds::STORAGE, "io write queue delay", 60040, true, true)
STAT_EVENT_ADD_DEF(IO_READ_CB_QUEUE_DELAY, "io read callback queuing delay", ObStatClassIds::STORAGE,
    "io read callback queuing delay", 60041, true, true)
STAT_EVENT_ADD_DEF(IO_READ_CB_PROCESS_DELAY, "io read callback process delay", ObStatClassIds::STORAGE,
    "io read callback process delay", 60042, true, true)
STAT_EVENT_ADD_DEF(
    WARM_UP_REQUEST_COUNT, "warm up request count", ObStatClassIds::STORAGE, "warm up request count", 60043, true, true)
STAT_EVENT_ADD_DEF(WARM_UP_REQUEST_SCAN_COUNT, "warm up request scan count", ObStatClassIds::STORAGE,
    "warm up request scan count", 60044, true, true)
STAT_EVENT_ADD_DEF(WARM_UP_REQUEST_GET_COUNT, "warm up request get count", ObStatClassIds::STORAGE,
    "warm up request get count", 60045, true, true)
STAT_EVENT_ADD_DEF(WARM_UP_REQUEST_MULTI_GET_COUNT, "warm up request multi get count", ObStatClassIds::STORAGE,
    "warm up request multi get count", 60046, true, true)
STAT_EVENT_ADD_DEF(WARM_UP_REQUEST_SEND_COUNT, "warm up request send count", ObStatClassIds::STORAGE,
    "warm up request send count", 60047, true, true)
STAT_EVENT_ADD_DEF(WARM_UP_REQUEST_SEND_SIZE, "warm up request send size", ObStatClassIds::STORAGE,
    "warm up request send size", 60048, true, true)
STAT_EVENT_ADD_DEF(WARM_UP_REQUEST_IN_DROP_COUNT, "warm up request in drop count", ObStatClassIds::STORAGE,
    "warm up request in drop count", 60049, true, true)
STAT_EVENT_ADD_DEF(WARM_UP_REQUEST_OUT_DROP_COUNT, "warm up request out drop count", ObStatClassIds::STORAGE,
    "warm up request out drop count", 60050, true, true)
STAT_EVENT_ADD_DEF(BANDWIDTH_IN_THROTTLE, "bandwidth in throttle size", ObStatClassIds::STORAGE,
    "bandwidth in throttle size", 60051, true, true)
STAT_EVENT_ADD_DEF(BANDWIDTH_OUT_THROTTLE, "bandwidth out throttle size", ObStatClassIds::STORAGE,
    "bandwidth out throttle size", 60052, true, true)
STAT_EVENT_ADD_DEF(WARM_UP_REQUEST_IGNORE_COUNT, "warm up request ignored when send queue is full",
    ObStatClassIds::STORAGE, "warm up request ignored when send queue is full", 60053, true, true)
STAT_EVENT_ADD_DEF(WARM_UP_REQUEST_ROWKEY_CHECK_COUNT, "warm up request rowkey check count", ObStatClassIds::STORAGE,
    "warm up request rowkey check count", 60054, true, true)
STAT_EVENT_ADD_DEF(WARM_UP_REQUEST_MULTI_SCAN_COUNT, "warm up request multi scan count", ObStatClassIds::STORAGE,
    "warm up request multi scan count", 60055, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_READ_ROW_COUNT, "memstore read row count", ObStatClassIds::STORAGE,
    "memstore read row count", 60056, true, true)
STAT_EVENT_ADD_DEF(SSSTORE_READ_ROW_COUNT, "ssstore read row count", ObStatClassIds::STORAGE, "ssstore read row count",
    60057, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_BIG_GAP_ROW_COUNT, "memstore big gap row count", ObStatClassIds::STORAGE,
    "memstore big gap row count", 60058, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_SMALL_GAP_ROW_COUNT, "memstore small gap row count", ObStatClassIds::STORAGE,
    "memstore small gap row count", 60059, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_BIG_GAP_COUNT, "memstore big gap count", ObStatClassIds::STORAGE, "memstore big gap count",
    60060, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_SMALL_GAP_COUNT, "memstore small gap count", ObStatClassIds::STORAGE,
    "memstore small gap count", 60061, true, true)
STAT_EVENT_ADD_DEF(SSSTORE_BIG_GAP_ROW_COUNT, "ssstore big gap row count", ObStatClassIds::STORAGE,
    "ssstore big gap row count", 60062, true, true)
STAT_EVENT_ADD_DEF(SSSTORE_SMALL_GAP_ROW_COUNT, "ssstore small gap row count", ObStatClassIds::STORAGE,
    "ssstore small gap row count", 60063, true, true)
STAT_EVENT_ADD_DEF(
    SSSTORE_BIG_GAP_COUNT, "ssstore big gap count", ObStatClassIds::STORAGE, "ssstore big gap count", 60064, true, true)
STAT_EVENT_ADD_DEF(SSSTORE_SMALL_GAP_COUNT, "ssstore small gap count", ObStatClassIds::STORAGE,
    "ssstore small gap count", 60065, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_RANGE_PURGE_COUNT, "memstore range purge count", ObStatClassIds::STORAGE,
    "memstore range purge count", 60067, true, true)

STAT_EVENT_ADD_DEF(MEMSTORE_WRITE_LOCK_WAKENUP_COUNT, "memstore write lock wakenup count in lock_wait_mgr",
    ObStatClassIds::STORAGE, "memstore write lock wakenup count in lock_wait_mgr", 60068, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_SDR_SUBMIT_COUNT, "memstore active purge submit sdr count", ObStatClassIds::STORAGE,
    "memstore active purge submit sdr count", 60069, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_SDR_SUCC_COUNT, "memstore active purge handle sdr succ count", ObStatClassIds::STORAGE,
    "memstore handle sdr succ count", 60070, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_SDR_FAIL_COUNT, "memstore active purge handle sdr fail count", ObStatClassIds::STORAGE,
    "memstore handle sdr fail count", 60071, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_SDR_RETRY_COUNT, "memstore active purge handle sdr retry count", ObStatClassIds::STORAGE,
    "memstore handle sdr retry count", 60072, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_SDR_DELAY, "memstore active purge handle sdr delay", ObStatClassIds::STORAGE,
    "memstore active purge handle sdr delay", 60073, true, true)

STAT_EVENT_ADD_DEF(ROWID_HIT, "rowid hit count", ObStatClassIds::STORAGE, "rowid hit count", 60074, true, true)

STAT_EVENT_ADD_DEF(
    EXIST_ROW_EFFECT_READ, "exist row effect read", ObStatClassIds::STORAGE, "exist row effect read", 60075, true, true)
STAT_EVENT_ADD_DEF(
    EXIST_ROW_EMPTY_READ, "exist row empty read", ObStatClassIds::STORAGE, "exist row empty read", 60076, true, true)
STAT_EVENT_ADD_DEF(
    GET_ROW_EFFECT_READ, "get row effect read", ObStatClassIds::STORAGE, "get row effect read", 60077, true, true)
STAT_EVENT_ADD_DEF(
    GET_ROW_EMPTY_READ, "get row empty read", ObStatClassIds::STORAGE, "get row empty read", 60078, true, true)
STAT_EVENT_ADD_DEF(
    SCAN_ROW_EFFECT_READ, "scan row effect read", ObStatClassIds::STORAGE, "scan row effect read", 60079, true, true)
STAT_EVENT_ADD_DEF(
    SCAN_ROW_EMPTY_READ, "scan row empty read", ObStatClassIds::STORAGE, "scan row empty read", 60080, true, true)
STAT_EVENT_ADD_DEF(
    BANDWIDTH_IN_SLEEP_US, "bandwidth in sleep us", ObStatClassIds::STORAGE, "bandwidth in sleep us", 60081, true, true)
STAT_EVENT_ADD_DEF(BANDWIDTH_OUT_SLEEP_US, "bandwidth out sleep us", ObStatClassIds::STORAGE, "bandwidth out sleep us",
    60082, true, true)

STAT_EVENT_ADD_DEF(MEMSTORE_WRITE_LOCK_WAIT_TIMEOUT_COUNT, "memstore write lock wait timeout count",
    ObStatClassIds::STORAGE, "memstore write wait timeout count", 60083, true, true)

// DEBUG
STAT_EVENT_ADD_DEF(
    REFRESH_SCHEMA_COUNT, "refresh schema count", ObStatClassIds::DEBUG, "refresh schema count", 70000, true, true)
STAT_EVENT_ADD_DEF(
    REFRESH_SCHEMA_TIME, "refresh schema time", ObStatClassIds::DEBUG, "refresh schema time", 70001, true, true)
STAT_EVENT_ADD_DEF(INNER_SQL_CONNECTION_EXECUTE_COUNT, "inner sql connection execute count", ObStatClassIds::DEBUG,
    "inner sql connection execute count", 70002, true, true)
STAT_EVENT_ADD_DEF(INNER_SQL_CONNECTION_EXECUTE_TIME, "inner sql connection execute time", ObStatClassIds::DEBUG,
    "inner sql connection execute time", 70003, true, true)
STAT_EVENT_ADD_DEF(PARTITION_TABLE_OPERATOR_GET_COUNT, "partition table operator get count", ObStatClassIds::DEBUG,
    "partition table operator get count", 70004, true, true)
STAT_EVENT_ADD_DEF(PARTITION_TABLE_OPERATOR_GET_TIME, "partition table operator get time", ObStatClassIds::DEBUG,
    "partition table operator get time", 70005, true, true)

// CLOG 80001 ~ 90000
STAT_EVENT_ADD_DEF(CLOG_SW_SUBMITTED_LOG_COUNT, "submitted to sliding window log count", ObStatClassIds::CLOG,
    "submitted to sliding window log count", 80001, true, true)
STAT_EVENT_ADD_DEF(CLOG_SW_SUBMITTED_LOG_SIZE, "submitted to sliding window log size", ObStatClassIds::CLOG,
    "submitted to sliding window log size", 80002, true, true)
STAT_EVENT_ADD_DEF(CLOG_INDEX_LOG_FLUSHED_LOG_COUNT, "index log flushed count", ObStatClassIds::CLOG,
    "index log flushed count", 80003, true, true)
STAT_EVENT_ADD_DEF(CLOG_INDEX_LOG_FLUSHED_LOG_SIZE, "index log flushed clog size", ObStatClassIds::CLOG,
    "index log flushed clog size", 80004, true, true)
STAT_EVENT_ADD_DEF(
    CLOG_FLUSHED_COUNT, "clog flushed count", ObStatClassIds::CLOG, "clog flushed count", 80005, true, true)
STAT_EVENT_ADD_DEF(CLOG_FLUSHED_SIZE, "clog flushed size", ObStatClassIds::CLOG, "clog flushed size", 80006, true, true)
STAT_EVENT_ADD_DEF(CLOG_READ_SIZE, "clog read size", ObStatClassIds::CLOG, "clog read size", 80007, true, true)
STAT_EVENT_ADD_DEF(CLOG_READ_COUNT, "clog read count", ObStatClassIds::CLOG, "clog read count", 80008, true, true)
STAT_EVENT_ADD_DEF(
    CLOG_DISK_READ_SIZE, "clog disk read size", ObStatClassIds::CLOG, "clog disk read size", 80009, true, true)
STAT_EVENT_ADD_DEF(
    CLOG_DISK_READ_COUNT, "clog disk read count", ObStatClassIds::CLOG, "clog disk read count", 80010, true, true)
STAT_EVENT_ADD_DEF(
    CLOG_DISK_READ_TIME, "clog disk read time", ObStatClassIds::CLOG, "clog disk read time", 80011, true, true)
STAT_EVENT_ADD_DEF(
    CLOG_FETCH_LOG_SIZE, "clog fetch log size", ObStatClassIds::CLOG, "clog fetch log size", 80012, true, true)
STAT_EVENT_ADD_DEF(
    CLOG_FETCH_LOG_COUNT, "clog fetch log count", ObStatClassIds::CLOG, "clog fetch log count", 80013, true, true)
STAT_EVENT_ADD_DEF(CLOG_FETCH_LOG_BY_LOCATION_SIZE, "clog fetch log by localtion size", ObStatClassIds::CLOG,
    "clog fetch log by localtion size", 80014, true, true)
STAT_EVENT_ADD_DEF(CLOG_FETCH_LOG_BY_LOCATION_COUNT, "clog fetch log by location count", ObStatClassIds::CLOG,
    "clog fetch log by localtion size", 80015, true, true)
STAT_EVENT_ADD_DEF(CLOG_READ_REQUEST_SUCC_SIZE, "clog read request succ size", ObStatClassIds::CLOG,
    "clog read request succ size", 80016, true, true)
STAT_EVENT_ADD_DEF(CLOG_READ_REQUEST_SUCC_COUNT, "clog read request succ count", ObStatClassIds::CLOG,
    "clog read request succ count", 80017, true, true)
STAT_EVENT_ADD_DEF(CLOG_READ_REQUEST_FAIL_COUNT, "clog read request fail count", ObStatClassIds::CLOG,
    "clog read request fail count", 80018, true, true)
STAT_EVENT_ADD_DEF(CLOG_LEADER_CONFIRM_TIME, "clog leader confirm time", ObStatClassIds::CLOG,
    "clog leader confirm time", 80019, true, true)
STAT_EVENT_ADD_DEF(CLOG_FLUSH_TASK_GENERATE_COUNT, "clog flush task generate count", ObStatClassIds::CLOG,
    "clog flush task generate count", 80020, true, true)
STAT_EVENT_ADD_DEF(CLOG_FLUSH_TASK_RELEASE_COUNT, "clog flush task release count", ObStatClassIds::CLOG,
    "clog flush task release count", 80021, true, true)
STAT_EVENT_ADD_DEF(
    CLOG_RPC_DELAY_TIME, "clog rpc delay time", ObStatClassIds::CLOG, "clog rpc delay time", 80022, true, true)
STAT_EVENT_ADD_DEF(CLOG_RPC_COUNT, "clog rpc count", ObStatClassIds::CLOG, "clog rpc count", 80023, true, true)
STAT_EVENT_ADD_DEF(CLOG_NON_KV_CACHE_HIT_COUNT, "clog non kv cache hit count", ObStatClassIds::CLOG,
    "clog non kv cache hit count", 80024, true, true)
STAT_EVENT_ADD_DEF(CLOG_RPC_REQUEST_HANDLE_TIME, "clog rpc request handle time", ObStatClassIds::CLOG,
    "clog rpc request handle time", 80025, true, true)
STAT_EVENT_ADD_DEF(
    CLOG_RPC_REQUEST_COUNT, "clog rpc request count", ObStatClassIds::CLOG, "clog rpc request count", 80026, true, true)
STAT_EVENT_ADD_DEF(
    CLOG_CACHE_HIT_COUNT, "clog cache hit count", ObStatClassIds::CLOG, "clog cache hit count", 80027, true, true)
STAT_EVENT_ADD_DEF(
    CLOG_STATE_LOOP_COUNT, "clog state loop count", ObStatClassIds::CLOG, "clog state loop count", 80028, true, true)
STAT_EVENT_ADD_DEF(
    CLOG_STATE_LOOP_TIME, "clog state loop time", ObStatClassIds::CLOG, "clog state loop time", 80029, true, true)
STAT_EVENT_ADD_DEF(
    CLOG_REPLAY_LOOP_COUNT, "clog replay loop count", ObStatClassIds::CLOG, "clog state loop count", 80030, true, true)
STAT_EVENT_ADD_DEF(
    CLOG_REPLAY_LOOP_TIME, "clog replay loop time", ObStatClassIds::CLOG, "clog state loop time", 80031, true, true)
STAT_EVENT_ADD_DEF(CLOG_TO_LEADER_ACTIVE_COUNT, "clog to leader active count", ObStatClassIds::CLOG,
    "clog to leader active count", 80032, true, true)
STAT_EVENT_ADD_DEF(CLOG_TO_LEADER_ACTIVE_TIME, "clog to leader active time", ObStatClassIds::CLOG,
    "clog to leader active time", 80033, true, true)
STAT_EVENT_ADD_DEF(
    ON_SUCCESS_COUNT, "on success cb count", ObStatClassIds::CLOG, "on success cb count", 80034, true, true)
STAT_EVENT_ADD_DEF(ON_SUCCESS_TIME, "on success cb time", ObStatClassIds::CLOG, "on success cb time", 80035, true, true)
STAT_EVENT_ADD_DEF(
    ON_LEADER_REVOKE_COUNT, "on leader revoke count", ObStatClassIds::CLOG, "on leader revoke count", 80036, true, true)
STAT_EVENT_ADD_DEF(
    ON_LEADER_REVOKE_TIME, "on leader revoke time", ObStatClassIds::CLOG, "on leader revoke time", 80037, true, true)
STAT_EVENT_ADD_DEF(ON_LEADER_TAKEOVER_COUNT, "on leader takeover count", ObStatClassIds::CLOG,
    "on leader takeover count", 80038, true, true)
STAT_EVENT_ADD_DEF(ON_LEADER_TAKEOVER_TIME, "on leader takeover time", ObStatClassIds::CLOG, "on leader takeover time",
    80039, true, true)
STAT_EVENT_ADD_DEF(CLOG_WRITE_COUNT, "clog write count", ObStatClassIds::CLOG, "clog write count", 80040, true, true)
STAT_EVENT_ADD_DEF(CLOG_WRITE_TIME, "clog write time", ObStatClassIds::CLOG, "clog write time", 80041, true, true)
STAT_EVENT_ADD_DEF(ILOG_WRITE_COUNT, "ilog write count", ObStatClassIds::CLOG, "ilog write count", 80042, true, true)
STAT_EVENT_ADD_DEF(ILOG_WRITE_TIME, "ilog write time", ObStatClassIds::CLOG, "ilog write time", 80043, true, true)
STAT_EVENT_ADD_DEF(CLOG_FLUSHED_TIME, "clog flushed time", ObStatClassIds::CLOG, "clog flushed time", 80044, true, true)
STAT_EVENT_ADD_DEF(
    CLOG_TASK_CB_COUNT, "clog task cb count", ObStatClassIds::CLOG, "clog task cb count", 80045, true, true)
STAT_EVENT_ADD_DEF(
    CLOG_CB_QUEUE_TIME, "clog cb queue time", ObStatClassIds::CLOG, "clog cb queue time", 80046, true, true)
STAT_EVENT_ADD_DEF(CLOG_ACK_COUNT, "clog ack count", ObStatClassIds::CLOG, "clog ack count", 80049, true, true)
STAT_EVENT_ADD_DEF(CLOG_ACK_TIME, "clog ack time", ObStatClassIds::CLOG, "clog ack time", 80050, true, true)
STAT_EVENT_ADD_DEF(
    CLOG_FIRST_ACK_COUNT, "clog first ack count", ObStatClassIds::CLOG, "clog first ack count", 80051, true, true)
STAT_EVENT_ADD_DEF(
    CLOG_FIRST_ACK_TIME, "clog first ack time", ObStatClassIds::CLOG, "clog first ack time", 80052, true, true)
STAT_EVENT_ADD_DEF(CLOG_LEADER_CONFIRM_COUNT, "clog leader confirm count", ObStatClassIds::CLOG,
    "clog leader confirm count", 80053, true, true)
STAT_EVENT_ADD_DEF(
    CLOG_BATCH_CB_COUNT, "clog batch cb count", ObStatClassIds::CLOG, "clog batch cb count", 80054, true, true)
STAT_EVENT_ADD_DEF(CLOG_BATCH_CB_QUEUE_TIME, "clog batch cb queue time", ObStatClassIds::CLOG,
    "clog batch cb queue time", 80055, true, true)
STAT_EVENT_ADD_DEF(CLOG_MEMSTORE_MUTATOR_TOTAL_SIZE, "clog memstore mutator total size", ObStatClassIds::CLOG,
    "clog memstore mutator total size", 80056, true, true)
STAT_EVENT_ADD_DEF(CLOG_TRANS_LOG_TOTAL_SIZE, "clog trans log total size", ObStatClassIds::CLOG,
    "clog trans log total size", 80057, true, true)
STAT_EVENT_ADD_DEF(CLOG_SUBMIT_LOG_TOTAL_SIZE, "clog submit log total size", ObStatClassIds::CLOG,
    "clog submit log total size", 80058, true, true)
STAT_EVENT_ADD_DEF(CLOG_BATCH_BUFFER_TOTAL_SIZE, "clog batch buffer total size", ObStatClassIds::CLOG,
    "clog batch buffer total size", 80059, true, true)
STAT_EVENT_ADD_DEF(ILOG_BATCH_BUFFER_TOTAL_SIZE, "ilog batch buffer total size", ObStatClassIds::CLOG,
    "ilog batch buffer total size", 80060, true, true)
STAT_EVENT_ADD_DEF(
    CLOG_FILE_TOTAL_SIZE, "clog file total size", ObStatClassIds::CLOG, "clog file total size", 80061, true, true)
STAT_EVENT_ADD_DEF(
    ILOG_FILE_TOTAL_SIZE, "ilog file total size", ObStatClassIds::CLOG, "ilog file total size", 80062, true, true)
STAT_EVENT_ADD_DEF(CLOG_BATCH_SUBMITTED_COUNT, "clog batch submitted count", ObStatClassIds::CLOG,
    "clog batch submitted count", 80063, true, true)
STAT_EVENT_ADD_DEF(CLOG_BATCH_COMMITTED_COUNT, "clog batch committed count", ObStatClassIds::CLOG,
    "clog batch committed count", 80064, true, true)

// CLOG.EXTLOG 81001 ~ 90000
STAT_EVENT_ADD_DEF(CLOG_EXTLOG_FETCH_LOG_SIZE, "external log service fetch log size", ObStatClassIds::CLOG,
    "external log service fetch log size", 81001, true, true)
STAT_EVENT_ADD_DEF(CLOG_EXTLOG_FETCH_LOG_COUNT, "external log service fetch log count", ObStatClassIds::CLOG,
    "external log service fetch log count", 81002, true, true)
STAT_EVENT_ADD_DEF(CLOG_EXTLOG_FETCH_RPC_COUNT, "external log service fetch rpc count", ObStatClassIds::CLOG,
    "external log service fetch rpc count", 81003, true, true)
STAT_EVENT_ADD_DEF(CLOG_EXTLOG_HEARTBEAT_RPC_COUNT, "external log service heartbeat rpc count", ObStatClassIds::CLOG,
    "external log service heartbeat rpc count", 81004, true, true)
STAT_EVENT_ADD_DEF(CLOG_EXTLOG_HEARTBEAT_PARTITION_COUNT, "external log service heartbeat partition count",
    ObStatClassIds::CLOG, "external log service heartbeat partition count", 81005, true, true)

// CLOG.REPLAY
STAT_EVENT_ADD_DEF(CLOG_SUCC_REPLAY_TRANS_LOG_COUNT, "replay engine success replay transaction log count",
    ObStatClassIds::CLOG, "replay engine success replay transaction log count", 82001, true, true)
STAT_EVENT_ADD_DEF(CLOG_SUCC_REPLAY_TRANS_LOG_TIME, "replay engine success replay transaction log time",
    ObStatClassIds::CLOG, "replay engine success replay transaction log time", 82002, true, true)
STAT_EVENT_ADD_DEF(CLOG_FAIL_REPLAY_TRANS_LOG_COUNT, "replay engine fail replay transaction log count",
    ObStatClassIds::CLOG, "replay engine fail replay transaction log count", 82003, true, true)
STAT_EVENT_ADD_DEF(CLOG_FAIL_REPLAY_TRANS_LOG_TIME, "replay engine fail replay transaction log time",
    ObStatClassIds::CLOG, "replay engine fail replay transaction log time", 82004, true, true)
STAT_EVENT_ADD_DEF(CLOG_SUCC_REPLAY_START_MEMBERSHIP_LOG_COUNT,
    "replay engine success replay start_membership log count", ObStatClassIds::CLOG,
    "replay engine success replay start_membership log count", 82005, true, true)
STAT_EVENT_ADD_DEF(CLOG_SUCC_REPLAY_START_MEMBERSHIP_LOG_TIME, "replay engine success replay start_membership log time",
    ObStatClassIds::CLOG, "replay engine success replay start_membership log time", 82006, true, true)
STAT_EVENT_ADD_DEF(CLOG_FAIL_REPLAY_START_MEMBERSHIP_LOG_COUNT, "replay engine fail replay start_membership log count",
    ObStatClassIds::CLOG, "replay engine fail replay start_membership log count", 82007, true, true)
STAT_EVENT_ADD_DEF(CLOG_FAIL_REPLAY_START_MEMBERSHIP_LOG_TIME, "replay engine fail replay start_membership log time",
    ObStatClassIds::CLOG, "replay engine fail replay start_membership log time", 82008, true, true)
STAT_EVENT_ADD_DEF(CLOG_SUCC_REPLAY_OFFLINE_COUNT, "replay engine success replay offline replica count",
    ObStatClassIds::CLOG, "replay engine success replay offline replica count", 82009, true, true)
STAT_EVENT_ADD_DEF(CLOG_FAIL_REPLAY_OFFLINE_COUNT, "replay engine fail replay offline replica count",
    ObStatClassIds::CLOG, "replay engine fail replay offline replica count", 82010, true, true)
STAT_EVENT_ADD_DEF(CLOG_REPLAY_RETRY_COUNT, "replay engine retry replay task count", ObStatClassIds::CLOG,
    "replay engine retry replay task count", 82011, true, true)
STAT_EVENT_ADD_DEF(CLOG_HANDLE_REPLAY_TASK_COUNT, "replay engine handle replay task count", ObStatClassIds::CLOG,
    "replay engine handle replay task count", 82012, true, true)
STAT_EVENT_ADD_DEF(CLOG_HANDLE_REPLAY_TIME, "replay engine total handle time", ObStatClassIds::CLOG,
    "replay engine total handle time", 82013, true, true)

STAT_EVENT_ADD_DEF(CLOG_REPLAY_TASK_SUBMIT_COUNT, "replay engine submitted replay task count", ObStatClassIds::CLOG,
    "replay engine submitted replay task count", 82014, true, true)
STAT_EVENT_ADD_DEF(CLOG_REPLAY_TASK_TRANS_SUBMIT_COUNT, "replay engine submitted transaction replay task count",
    ObStatClassIds::CLOG, "replay engine submitted transaction replay task count", 82015, true, true)
STAT_EVENT_ADD_DEF(CLOG_REPLAY_TASK_FREEZE_SUBMIT_COUNT, "replay engine submitted freeze replay task count",
    ObStatClassIds::CLOG, "replay engine submitted freeze replay task count", 82016, true, true)
STAT_EVENT_ADD_DEF(CLOG_REPLAY_TASK_OFFLINE_SUBMIT_COUNT, "replay engine submitted offline replay task count",
    ObStatClassIds::CLOG, "replay engine submitted offline replay task count", 82017, true, true)
STAT_EVENT_ADD_DEF(CLOG_REPLAY_TASK_SPLIT_SUBMIT_COUNT, "replay engine submitted split replay task count",
    ObStatClassIds::CLOG, "replay engine submitted split replay task count", 82018, true, true)
STAT_EVENT_ADD_DEF(CLOG_REPLAY_TASK_START_MEMBERSHIP_SUBMIT_COUNT,
    "replay engine submitted start membership replay task count", ObStatClassIds::CLOG,
    "replay engine submitted start membership replay task count", 82019, true, true)
STAT_EVENT_ADD_DEF(CLOG_SUCC_REPLAY_SPLIT_LOG_COUNT, "replay engine success replay split log count",
    ObStatClassIds::CLOG, "replay engine success replay split log count", 82020, true, true)
STAT_EVENT_ADD_DEF(CLOG_FAIL_REPLAY_SPLIT_LOG_COUNT, "replay engine fail replay split log count", ObStatClassIds::CLOG,
    "replay engine fail replay split log count", 82021, true, true)
STAT_EVENT_ADD_DEF(CLOG_REPLAY_TASK_META_SUBMIT_COUNT, "replay engine submitted partition meta replay task count",
    ObStatClassIds::CLOG, "replay engine submitted partition meta replay task count", 82022, true, true)
STAT_EVENT_ADD_DEF(CLOG_SUCC_REPLAY_META_LOG_COUNT, "replay engine success replay partition meta log count",
    ObStatClassIds::CLOG, "replay engine success replay partition meta log count", 82023, true, true)
STAT_EVENT_ADD_DEF(CLOG_FAIL_REPLAY_META_LOG_COUNT, "replay engine fail replay partition meta log count",
    ObStatClassIds::CLOG, "replay engine fail replay partition meta log count", 82024, true, true)
STAT_EVENT_ADD_DEF(CLOG_REPLAY_TASK_FLASHBACK_SUBMIT_COUNT, "replay engine submitted flashback replay task count",
    ObStatClassIds::CLOG, "replay engine submitted flashback replay task count", 82025, true, true)
STAT_EVENT_ADD_DEF(CLOG_SUCC_REPLAY_FLASHBACK_LOG_COUNT, "replay engine success replay flashback replica count",
    ObStatClassIds::CLOG, "replay engine success replay flashback replica count", 82026, true, true)
STAT_EVENT_ADD_DEF(CLOG_FAIL_REPLAY_FLASHBACK_LOG_COUNT, "replay engine fail replay flashback replica count",
    ObStatClassIds::CLOG, "replay engine fail replay flashback replica count", 82027, true, true)
STAT_EVENT_ADD_DEF(CLOG_REPLAY_TASK_ADD_PARTITION_TO_PG_SUBMIT_COUNT,
    "replay engine submitted add partition to pg replay task count", ObStatClassIds::CLOG,
    "replay engine submitted add partition to pg replay task count", 82028, true, true)
STAT_EVENT_ADD_DEF(CLOG_SUCC_REPLAY_ADD_PARTITION_TO_PG_LOG_COUNT,
    "replay engine success replay add partition to pg count", ObStatClassIds::CLOG,
    "replay engine success replay add partition to pg count", 82029, true, true)
STAT_EVENT_ADD_DEF(CLOG_FAIL_REPLAY_ADD_PARTITION_TO_PG_LOG_COUNT,
    "replay engine fail replay add partition to pg count", ObStatClassIds::CLOG,
    "replay engine fail replay add partition to pg count", 82030, true, true)
STAT_EVENT_ADD_DEF(CLOG_REPLAY_TASK_REMOVE_PG_PARTITION_SUBMIT_COUNT,
    "replay engine submitted remove pg partition replay task count", ObStatClassIds::CLOG,
    "replay engine submitted remove pg partition replay task count", 82031, true, true)
STAT_EVENT_ADD_DEF(CLOG_SUCC_REPLAY_REMOVE_PG_PARTITION_LOG_COUNT,
    "replay engine success replay remove pg partition count", ObStatClassIds::CLOG,
    "replay engine success replay remove pg partition count", 82032, true, true)
STAT_EVENT_ADD_DEF(CLOG_FAIL_REPLAY_REMOVE_PG_PARTITION_LOG_COUNT,
    "replay engine fail replay remove pg partition count", ObStatClassIds::CLOG,
    "replay engine fail replay remove pg partition count", 82033, true, true)
STAT_EVENT_ADD_DEF(CLOG_HANDLE_SUBMIT_TASK_COUNT, "replay engine handle submit task count", ObStatClassIds::CLOG,
    "replay engine handle submit task count", 82034, true, true)
STAT_EVENT_ADD_DEF(CLOG_HANDLE_SUBMIT_TASK_SIZE, "replay engine handle submit task size", ObStatClassIds::CLOG,
    "replay engine handle submit task size", 82035, true, true)
STAT_EVENT_ADD_DEF(CLOG_HANDLE_SUBMIT_TIME, "replay engine handle submit time", ObStatClassIds::CLOG,
    "replay engine handle submit time", 82036, true, true)

// ELECTION
STAT_EVENT_ADD_DEF(ELECTION_CHANGE_LEAER_COUNT, "election change leader count", ObStatClassIds::ELECT,
    "election change leader count", 90001, false, true)
STAT_EVENT_ADD_DEF(ELECTION_LEADER_REVOKE_COUNT, "election leader revoke count", ObStatClassIds::ELECT,
    "election leader revoke count", 90002, false, true)

// OBSERVER
STAT_EVENT_ADD_DEF(OBSERVER_PARTITION_TABLE_UPDATER_BATCH_COUNT, "observer partition table updater batch count",
    ObStatClassIds::OBSERVER, "observer partition table updater batch count", 100001, false, true)
STAT_EVENT_ADD_DEF(OBSERVER_PARTITION_TABLE_UPDATE_COUNT, "observer partition table updater count",
    ObStatClassIds::OBSERVER, "observer partition table updater count", 100002, false, true)
STAT_EVENT_ADD_DEF(OBSERVER_PARTITION_TABLE_UPDATER_PROCESS_TIME, "observer partition table updater process time",
    ObStatClassIds::OBSERVER, "observer partition table updater process time", 100003, false, true)
STAT_EVENT_ADD_DEF(OBSERVER_PARTITION_TABLE_UPDATER_DROP_COUNT, "observer partition table updater drop task count",
    ObStatClassIds::OBSERVER, "observer partition table updater drop task count", 100004, false, true)
STAT_EVENT_ADD_DEF(OBSERVER_PARTITION_TABLE_UPDATER_REPUT_COUNT,
    "observer partition table updater repurt to queue count", ObStatClassIds::OBSERVER,
    "observer partition table updater reput to queue count", 100005, false, true)
STAT_EVENT_ADD_DEF(OBSERVER_PARTITION_TABLE_UPDATER_FAIL_TIMES, "observer partition table updater execute fail times",
    ObStatClassIds::OBSERVER, "observer partition table updater fail times", 10006, false, true)
STAT_EVENT_ADD_DEF(OBSERVER_PARTITION_TABLE_UPDATER_FINISH_COUNT,
    "observer partition table updater success execute count", ObStatClassIds::OBSERVER,
    "observer partition table updater execute success count", 100007, false, true)
STAT_EVENT_ADD_DEF(OBSERVER_PARTITION_TABLE_UPDATER_TOTAL_COUNT, "observer partition table updater total task count",
    ObStatClassIds::OBSERVER, "observer partition table updater total task count", 100008, false, true)

// rootservice
STAT_EVENT_ADD_DEF(RS_PARTITION_MIGRATE_COUNT, "partition migrate count", ObStatClassIds::RS, "partition migrate count",
    110001, false, true)
STAT_EVENT_ADD_DEF(RS_PARTITION_MIGRATE_TIME, "partition migrate time", ObStatClassIds::RS, "partition migrate time",
    110002, false, true)
STAT_EVENT_ADD_DEF(
    RS_PARTITION_ADD_COUNT, "partition add count", ObStatClassIds::RS, "partition add count", 110003, false, true)
STAT_EVENT_ADD_DEF(
    RS_PARTITION_ADD_TIME, "partition add time", ObStatClassIds::RS, "partition add time", 110004, false, true)
STAT_EVENT_ADD_DEF(RS_PARTITION_REBUILD_COUNT, "partition rebuild count", ObStatClassIds::RS, "partition rebuild count",
    110005, false, true)
STAT_EVENT_ADD_DEF(RS_PARTITION_REBUILD_TIME, "partition rebuild time", ObStatClassIds::RS, "partition rebuild time",
    110006, false, true)
STAT_EVENT_ADD_DEF(RS_PARTITION_TRANSFORM_COUNT, "partition transform count", ObStatClassIds::RS,
    "partition transform count", 110007, false, true)
STAT_EVENT_ADD_DEF(RS_PARTITION_TRANSFORM_TIME, "partition transform time", ObStatClassIds::RS,
    "partition transform time", 110008, false, true)
STAT_EVENT_ADD_DEF(RS_PARTITION_REMOVE_COUNT, "partition remove count", ObStatClassIds::RS, "partition remove count",
    110009, false, true)
STAT_EVENT_ADD_DEF(
    RS_PARTITION_REMOVE_TIME, "partition remove time", ObStatClassIds::RS, "partition remove time", 110010, false, true)
STAT_EVENT_ADD_DEF(RS_PARTITION_CHANGE_MEMBER_COUNT, "partition change member count", ObStatClassIds::RS,
    "partition change member count", 110011, false, true)
STAT_EVENT_ADD_DEF(RS_PARTITION_CHANGE_MEMBER_TIME, "partition change member time", ObStatClassIds::RS,
    "partition change member time", 110012, false, true)
STAT_EVENT_ADD_DEF(RS_SWITCH_LEADER_SUCC, "succ switch leader count", ObStatClassIds::RS, "succ switch leader count",
    110013, false, true)
STAT_EVENT_ADD_DEF(RS_SWITCH_LEADER_FAIL, "failed switch leader count", ObStatClassIds::RS,
    "failed switch leader count", 110014, false, true)
STAT_EVENT_ADD_DEF(
    RS_RPC_SUCC_COUNT, "success rpc process", ObStatClassIds::RS, "success rpc process", 110015, false, true)
STAT_EVENT_ADD_DEF(
    RS_RPC_FAIL_COUNT, "failed rpc process", ObStatClassIds::RS, "failed rpc process", 110016, false, true)
STAT_EVENT_ADD_DEF(RS_BALANCER_SUCC_COUNT, "balancer succ execute count", ObStatClassIds::RS,
    "balanacer succ execute count", 110017, false, true)
STAT_EVENT_ADD_DEF(RS_BALANCER_FAIL_COUNT, "balancer failed execute count", ObStatClassIds::RS,
    "balanacer failed execute count", 110018, false, true)
STAT_EVENT_ADD_DEF(RS_TASK_FAIL_COUNT, "rebalance task failed execute count", ObStatClassIds::RS,
    "rebalance task failed execute count", 110019, false, true)
STAT_EVENT_ADD_DEF(RS_PARTITION_MODIFY_QUORUM_COUNT, "partition modify quorum count", ObStatClassIds::RS,
    "partition modify quorum count", 110020, false, true)
STAT_EVENT_ADD_DEF(RS_PARTITION_MODIFY_QUORUM_TIME, "partition modify quorum time", ObStatClassIds::RS,
    "partition modify quorum time", 110021, false, true)
STAT_EVENT_ADD_DEF(RS_COPY_GLOBAL_INDEX_SSTABLE_COUNT, "copy global index sstable count", ObStatClassIds::RS,
    "copy global index sstable count", 110022, false, true)
STAT_EVENT_ADD_DEF(RS_COPY_GLOBAL_INDEX_SSTABLE_TIME, "copy global index sstable time", ObStatClassIds::RS,
    "copy global index sstable time", 110023, false, true)
STAT_EVENT_ADD_DEF(RS_COPY_LOCAL_INDEX_SSTABLE_COUNT, "copy local index sstable count", ObStatClassIds::RS,
    "copy local index sstable count", 110024, false, true)
STAT_EVENT_ADD_DEF(RS_COPY_LOCAL_INDEX_SSTABLE_TIME, "copy local index sstable time", ObStatClassIds::RS,
    "copy local index sstable time", 110025, false, true)
STAT_EVENT_ADD_DEF(RS_STANDBY_CUTDATA_TASK_COUNT, "standby cutdata task count", ObStatClassIds::RS,
    "standby cutdata task count", 110026, false, true)
STAT_EVENT_ADD_DEF(RS_STANDBY_CUTDATA_TASK_TIME, "standby cutdata task count", ObStatClassIds::RS,
    "standby cutdata task count", 110027, false, true)

// TABLE API (19xxxx)
// -- retrieve 1900xx
STAT_EVENT_ADD_DEF(TABLEAPI_RETRIEVE_COUNT, "single retrieve execute count", ObStatClassIds::TABLEAPI, "retrieve count",
    190001, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_RETRIEVE_TIME, "single retrieve execute time", ObStatClassIds::TABLEAPI, "retrieve time",
    190002, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_RETRIEVE_COUNT, "multi retrieve execute count", ObStatClassIds::TABLEAPI,
    "multi retrieve count", 190003, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_RETRIEVE_TIME, "multi retrieve execute time", ObStatClassIds::TABLEAPI,
    "multi retrieve time", 190004, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_RETRIEVE_ROW, "multi retrieve rows", ObStatClassIds::TABLEAPI, "multi retrieve rows",
    190005, true, true)
// -- insert 1901xx
STAT_EVENT_ADD_DEF(
    TABLEAPI_INSERT_COUNT, "single insert execute count", ObStatClassIds::TABLEAPI, "insert count", 190101, true, true)
STAT_EVENT_ADD_DEF(
    TABLEAPI_INSERT_TIME, "single insert execute time", ObStatClassIds::TABLEAPI, "insert time", 190102, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_INSERT_COUNT, "multi insert execute count", ObStatClassIds::TABLEAPI,
    "multi insert count", 190103, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_INSERT_TIME, "multi insert execute time", ObStatClassIds::TABLEAPI,
    "multi insert time", 190104, true, true)
STAT_EVENT_ADD_DEF(
    TABLEAPI_MULTI_INSERT_ROW, "multi insert rows", ObStatClassIds::TABLEAPI, "multi insert rows", 190105, true, true)
// -- delete 1902xx
STAT_EVENT_ADD_DEF(
    TABLEAPI_DELETE_COUNT, "single delete execute count", ObStatClassIds::TABLEAPI, "delete count", 190201, true, true)
STAT_EVENT_ADD_DEF(
    TABLEAPI_DELETE_TIME, "single delete execute time", ObStatClassIds::TABLEAPI, "delete time", 190202, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_DELETE_COUNT, "multi delete execute count", ObStatClassIds::TABLEAPI,
    "multi delete count", 190203, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_DELETE_TIME, "multi delete execute time", ObStatClassIds::TABLEAPI,
    "multi delete time", 190204, true, true)
STAT_EVENT_ADD_DEF(
    TABLEAPI_MULTI_DELETE_ROW, "multi delete rows", ObStatClassIds::TABLEAPI, "multi delete rows", 190205, true, true)
// -- update 1903xx
STAT_EVENT_ADD_DEF(
    TABLEAPI_UPDATE_COUNT, "single update execute count", ObStatClassIds::TABLEAPI, "update count", 190301, true, true)
STAT_EVENT_ADD_DEF(
    TABLEAPI_UPDATE_TIME, "single update execute time", ObStatClassIds::TABLEAPI, "update time", 190302, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_UPDATE_COUNT, "multi update execute count", ObStatClassIds::TABLEAPI,
    "multi update count", 190303, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_UPDATE_TIME, "multi update execute time", ObStatClassIds::TABLEAPI,
    "multi update time", 190304, true, true)
STAT_EVENT_ADD_DEF(
    TABLEAPI_MULTI_UPDATE_ROW, "multi update rows", ObStatClassIds::TABLEAPI, "multi update rows", 190305, true, true)
// -- insert_or_update 1904xx
STAT_EVENT_ADD_DEF(TABLEAPI_INSERT_OR_UPDATE_COUNT, "single insert_or_update execute count", ObStatClassIds::TABLEAPI,
    "insert_or_update count", 190401, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_INSERT_OR_UPDATE_TIME, "single insert_or_update execute time", ObStatClassIds::TABLEAPI,
    "insert_or_update time", 190402, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_INSERT_OR_UPDATE_COUNT, "multi insert_or_update execute count",
    ObStatClassIds::TABLEAPI, "multi insert_or_update count", 190403, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_INSERT_OR_UPDATE_TIME, "multi insert_or_update execute time",
    ObStatClassIds::TABLEAPI, "multi insert_or_update time", 190404, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_INSERT_OR_UPDATE_ROW, "multi insert_or_update rows", ObStatClassIds::TABLEAPI,
    "multi insert_or_update rows", 190405, true, true)
// -- replace 1905xx
STAT_EVENT_ADD_DEF(TABLEAPI_REPLACE_COUNT, "single replace execute count", ObStatClassIds::TABLEAPI, "replace count",
    190501, true, true)
STAT_EVENT_ADD_DEF(
    TABLEAPI_REPLACE_TIME, "single replace execute time", ObStatClassIds::TABLEAPI, "replace time", 190502, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_REPLACE_COUNT, "multi replace execute count", ObStatClassIds::TABLEAPI,
    "multi replace count", 190503, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_REPLACE_TIME, "multi replace execute time", ObStatClassIds::TABLEAPI,
    "multi replace time", 190504, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_REPLACE_ROW, "multi replace rows", ObStatClassIds::TABLEAPI, "multi replace rows",
    190505, true, true)
// -- complex_batch_retrieve 1906xx
STAT_EVENT_ADD_DEF(TABLEAPI_BATCH_RETRIEVE_COUNT, "batch retrieve execute count", ObStatClassIds::TABLEAPI,
    "batch retrieve count", 190601, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_BATCH_RETRIEVE_TIME, "batch retrieve execute time", ObStatClassIds::TABLEAPI,
    "batch retrieve time", 190602, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_BATCH_RETRIEVE_ROW, "batch retrieve rows", ObStatClassIds::TABLEAPI, "batch retrieve rows",
    190603, true, true)
// -- complex_batch_operation 1907xx
STAT_EVENT_ADD_DEF(TABLEAPI_BATCH_HYBRID_COUNT, "batch hybrid execute count", ObStatClassIds::TABLEAPI,
    "batch hybrid count", 190701, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_BATCH_HYBRID_TIME, "batch hybrid execute time", ObStatClassIds::TABLEAPI,
    "batch hybrid time", 190702, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_BATCH_HYBRID_RETRIEVE_ROW, "batch hybrid retrieve rows", ObStatClassIds::TABLEAPI,
    "batch hybrid retrieve rows", 190703, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_BATCH_HYBRID_INSERT_ROW, "batch hybrid insert rows", ObStatClassIds::TABLEAPI,
    "batch hybrid insert rows", 190704, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_BATCH_HYBRID_DELETE_ROW, "batch hybrid delete rows", ObStatClassIds::TABLEAPI,
    "batch hybrid delete rows", 190705, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_BATCH_HYBRID_UPDATE_ROW, "batch hybrid update rows", ObStatClassIds::TABLEAPI,
    "batch hybrid update rows", 190706, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_BATCH_HYBRID_INSERT_OR_UPDATE_ROW, "batch hybrid insert_or_update rows",
    ObStatClassIds::TABLEAPI, "batch hybrid insert_or_update rows", 190707, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_BATCH_HYBRID_REPLACE_ROW, "batch hybrid replace rows", ObStatClassIds::TABLEAPI,
    "batch hybrid replace rows", 190708, true, true)
// -- other 1908xx
STAT_EVENT_ADD_DEF(
    TABLEAPI_LOGIN_COUNT, "table api login count", ObStatClassIds::TABLEAPI, "login count", 190801, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_TSC_VIOLATE_COUNT, "table api OB_TRANSACTION_SET_VIOLATION count", ObStatClassIds::TABLEAPI,
    "OB_TRANSACTION_SET_VIOLATION count", 190802, true, true)
// -- query 1909xx
STAT_EVENT_ADD_DEF(TABLEAPI_QUERY_COUNT, "query count", ObStatClassIds::TABLEAPI, "query count", 190901, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_QUERY_TIME, "query time", ObStatClassIds::TABLEAPI, "query time", 190902, true, true)
STAT_EVENT_ADD_DEF(
    TABLEAPI_QUERY_ROW, "query row count", ObStatClassIds::TABLEAPI, "query row count", 190903, true, true)
// -- hbase api 1910xx
STAT_EVENT_ADD_DEF(HBASEAPI_SCAN_COUNT, "hbase scan count", ObStatClassIds::TABLEAPI, "scan count", 191001, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_SCAN_TIME, "hbase scan time", ObStatClassIds::TABLEAPI, "scan time", 191002, true, true)
STAT_EVENT_ADD_DEF(
    HBASEAPI_SCAN_ROW, "hbase scan row count", ObStatClassIds::TABLEAPI, "scan row count", 191003, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_PUT_COUNT, "hbase put count", ObStatClassIds::TABLEAPI, "put count", 191004, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_PUT_TIME, "hbase put time", ObStatClassIds::TABLEAPI, "put time", 191005, true, true)
STAT_EVENT_ADD_DEF(
    HBASEAPI_PUT_ROW, "hbase put row count", ObStatClassIds::TABLEAPI, "put row count", 191006, true, true)
STAT_EVENT_ADD_DEF(
    HBASEAPI_DELETE_COUNT, "hbase delete count", ObStatClassIds::TABLEAPI, "delete count", 191007, true, true)
STAT_EVENT_ADD_DEF(
    HBASEAPI_DELETE_TIME, "hbase delete time", ObStatClassIds::TABLEAPI, "delete time", 191008, true, true)
STAT_EVENT_ADD_DEF(
    HBASEAPI_DELETE_ROW, "hbase delete row count", ObStatClassIds::TABLEAPI, "delete row count", 191009, true, true)
STAT_EVENT_ADD_DEF(
    HBASEAPI_APPEND_COUNT, "hbase append count", ObStatClassIds::TABLEAPI, "append count", 191010, true, true)
STAT_EVENT_ADD_DEF(
    HBASEAPI_APPEND_TIME, "hbase append time", ObStatClassIds::TABLEAPI, "append time", 191011, true, true)
STAT_EVENT_ADD_DEF(
    HBASEAPI_APPEND_ROW, "hbase append row count", ObStatClassIds::TABLEAPI, "append row count", 191012, true, true)
STAT_EVENT_ADD_DEF(
    HBASEAPI_INCREMENT_COUNT, "hbase increment count", ObStatClassIds::TABLEAPI, "increment count", 191013, true, true)
STAT_EVENT_ADD_DEF(
    HBASEAPI_INCREMENT_TIME, "hbase increment time", ObStatClassIds::TABLEAPI, "increment time", 191014, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_INCREMENT_ROW, "hbase increment row count", ObStatClassIds::TABLEAPI, "increment row count",
    191015, true, true)
STAT_EVENT_ADD_DEF(
    HBASEAPI_CHECK_PUT_COUNT, "hbase check_put count", ObStatClassIds::TABLEAPI, "check_put count", 191016, true, true)
STAT_EVENT_ADD_DEF(
    HBASEAPI_CHECK_PUT_TIME, "hbase check_put time", ObStatClassIds::TABLEAPI, "check_put time", 191017, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_CHECK_PUT_ROW, "hbase check_put row count", ObStatClassIds::TABLEAPI, "check_put row count",
    191018, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_CHECK_DELETE_COUNT, "hbase check_delete count", ObStatClassIds::TABLEAPI,
    "check_delete count", 191019, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_CHECK_DELETE_TIME, "hbase check_delete time", ObStatClassIds::TABLEAPI, "check_delete time",
    191020, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_CHECK_DELETE_ROW, "hbase check_delete row count", ObStatClassIds::TABLEAPI,
    "check_delete row count", 191021, true, true)
STAT_EVENT_ADD_DEF(
    HBASEAPI_HYBRID_COUNT, "hbase hybrid count", ObStatClassIds::TABLEAPI, "hybrid count", 191022, true, true)
STAT_EVENT_ADD_DEF(
    HBASEAPI_HYBRID_TIME, "hbase hybrid time", ObStatClassIds::TABLEAPI, "hybrid time", 191023, true, true)
STAT_EVENT_ADD_DEF(
    HBASEAPI_HYBRID_ROW, "hbase hybrid row count", ObStatClassIds::TABLEAPI, "hybrid row count", 191024, true, true)

// -- table increment 1911xx
STAT_EVENT_ADD_DEF(TABLEAPI_INCREMENT_COUNT, "single increment execute count", ObStatClassIds::TABLEAPI,
    "increment count", 191101, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_INCREMENT_TIME, "single increment execute time", ObStatClassIds::TABLEAPI, "increment time",
    191102, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_INCREMENT_COUNT, "multi increment execute count", ObStatClassIds::TABLEAPI,
    "multi increment count", 191103, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_INCREMENT_TIME, "multi increment execute time", ObStatClassIds::TABLEAPI,
    "multi increment time", 191104, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_INCREMENT_ROW, "multi increment rows", ObStatClassIds::TABLEAPI,
    "multi increment rows", 191105, true, true)

// -- table append 1912xx
STAT_EVENT_ADD_DEF(
    TABLEAPI_APPEND_COUNT, "single append execute count", ObStatClassIds::TABLEAPI, "append count", 191201, true, true)
STAT_EVENT_ADD_DEF(
    TABLEAPI_APPEND_TIME, "single append execute time", ObStatClassIds::TABLEAPI, "append time", 191202, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_APPEND_COUNT, "multi append execute count", ObStatClassIds::TABLEAPI,
    "multi append count", 191203, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_APPEND_TIME, "multi append execute time", ObStatClassIds::TABLEAPI,
    "multi append time", 191204, true, true)
STAT_EVENT_ADD_DEF(
    TABLEAPI_MULTI_APPEND_ROW, "multi append rows", ObStatClassIds::TABLEAPI, "multi append rows", 191205, true, true)

// sys_time_model related (20xxxx)
STAT_EVENT_ADD_DEF(SYS_TIME_MODEL_DB_TIME, "DB time", ObStatClassIds::SYS, "DB time", 200001, false, true)
STAT_EVENT_ADD_DEF(SYS_TIME_MODEL_DB_CPU, "DB CPU", ObStatClassIds::SYS, "DB CPU", 200002, false, true)
STAT_EVENT_ADD_DEF(
    SYS_TIME_MODEL_DB_INNER_TIME, "DB inner sql time", ObStatClassIds::SYS, "DB time", 200003, false, true)
STAT_EVENT_ADD_DEF(SYS_TIME_MODEL_DB_INNER_CPU, "DB inner sql CPU", ObStatClassIds::SYS, "DB CPU", 200004, false, true)
STAT_EVENT_ADD_DEF(SYS_TIME_MODEL_BKGD_TIME, "background elapsed time", ObStatClassIds::SYS, "background elapsed time",
    200005, false, true)
STAT_EVENT_ADD_DEF(
    SYS_TIME_MODEL_BKGD_CPU, "background cpu time", ObStatClassIds::SYS, "background cpu time", 200006, false, true)
STAT_EVENT_ADD_DEF(SYS_TIME_MODEL_BACKUP_CPU, "(backup/restore) cpu time", ObStatClassIds::SYS,
    "(backup/restore) cpu time", 200007, false, true)
STAT_EVENT_ADD_DEF(SYS_TIME_MODEL_ENCRYPT_TIME, "Tablespace encryption elapsed time", ObStatClassIds::SYS,
    "Tablespace encryption elapsed time", 200008, false, true)
STAT_EVENT_ADD_DEF(SYS_TIME_MODEL_ENCRYPT_CPU, "Tablespace encryption cpu time", ObStatClassIds::SYS,
    "Tablespace encryption cpu time", 200009, false, true)

// end
STAT_EVENT_ADD_DEF(STAT_EVENT_ADD_END, "event add end", ObStatClassIds::DEBUG, "event add end", 1, false, false)

#endif

#ifdef STAT_EVENT_SET_DEF
// NETWORK

// QUEUE

// TRANS

// SQL

// CACHE
STAT_EVENT_SET_DEF(
    LOCATION_CACHE_SIZE, "location cache size", ObStatClassIds::CACHE, "location cache size", 120000, false, true)
STAT_EVENT_SET_DEF(CLOG_CACHE_SIZE, "clog cache size", ObStatClassIds::CACHE, "clog cache size", 120001, false, true)
STAT_EVENT_SET_DEF(
    INDEX_CLOG_CACHE_SIZE, "index clog cache size", ObStatClassIds::CACHE, "index clog cache size", 120002, false, true)
STAT_EVENT_SET_DEF(USER_TABLE_COL_STAT_CACHE_SIZE, "user table col stat cache size", ObStatClassIds::CACHE,
    "user table col stat cache size", 120003, false, true)
STAT_EVENT_SET_DEF(INDEX_CACHE_SIZE, "index cache size", ObStatClassIds::CACHE, "index cache size", 120004, false, true)
STAT_EVENT_SET_DEF(
    SYS_BLOCK_CACHE_SIZE, "sys block cache size", ObStatClassIds::CACHE, "sys block cache size", 120005, false, true)
STAT_EVENT_SET_DEF(
    USER_BLOCK_CACHE_SIZE, "user block cache size", ObStatClassIds::CACHE, "user block cache size", 120006, false, true)
STAT_EVENT_SET_DEF(
    SYS_ROW_CACHE_SIZE, "sys row cache size", ObStatClassIds::CACHE, "sys row cache size", 120007, false, true)
STAT_EVENT_SET_DEF(
    USER_ROW_CACHE_SIZE, "user row cache size", ObStatClassIds::CACHE, "user row cache size", 120008, false, true)
STAT_EVENT_SET_DEF(BLOOM_FILTER_CACHE_SIZE, "bloom filter cache size", ObStatClassIds::CACHE, "bloom filter cache size",
    120009, false, true)
STAT_EVENT_SET_DEF(BLOOM_FILTER_CACHE_STRATEGY, "bloom filter cache strategy", ObStatClassIds::CACHE,
    "bloom filter cache strategy", 120010, false, true)

// STORAGE
STAT_EVENT_SET_DEF(
    ACTIVE_MEMSTORE_USED, "active memstore used", ObStatClassIds::STORAGE, "active memstore used", 130000, false, true)
STAT_EVENT_SET_DEF(
    TOTAL_MEMSTORE_USED, "total memstore used", ObStatClassIds::STORAGE, "total memstore used", 130001, false, true)
STAT_EVENT_SET_DEF(
    MAJOR_FREEZE_TRIGGER, "major freeze trigger", ObStatClassIds::STORAGE, "major freeze trigger", 130002, false, true)
STAT_EVENT_SET_DEF(MEMSTORE_LIMIT, "memstore limit", ObStatClassIds::STORAGE, "memstore limit", 130004, false, true)

// RESOURCE
// STAT_EVENT_SET_DEF(SRV_DISK_SIZE, "SRV_DISK_SIZE", RESOURCE, "SRV_DISK_SIZE")
// STAT_EVENT_SET_DEF(DISK_USAGE, "DISK_USAGE", RESOURCE, "DISK_USAGE")
// STAT_EVENT_SET_DEF(SRV_MEMORY_SIZE, "SRV_MEMORY_SIZE", RESOURCE, "SRV_MEMORY_SIZE")
STAT_EVENT_SET_DEF(MIN_MEMORY_SIZE, "min memory size", ObStatClassIds::RESOURCE, "min memory size", 140001, false, true)
STAT_EVENT_SET_DEF(MAX_MEMORY_SIZE, "max memory size", ObStatClassIds::RESOURCE, "max memory size", 140002, false, true)
STAT_EVENT_SET_DEF(MEMORY_USAGE, "memory usage", ObStatClassIds::RESOURCE, "memory usage", 140003, false, true)
// STAT_EVENT_SET_DEF(SRV_CPUS, "SRV_CPUS", RESOURCE, "SRV_CPUS")
STAT_EVENT_SET_DEF(MIN_CPUS, "min cpus", ObStatClassIds::RESOURCE, "min cpus", 140004, false, true)
STAT_EVENT_SET_DEF(MAX_CPUS, "max cpus", ObStatClassIds::RESOURCE, "max cpus", 140005, false, true)
STAT_EVENT_SET_DEF(CPU_USAGE, "cpu usage", ObStatClassIds::RESOURCE, "cpu usage", 140006, false, true)
STAT_EVENT_SET_DEF(DISK_USAGE, "disk usage", ObStatClassIds::RESOURCE, "disk usage", 140007, false, true)
STAT_EVENT_SET_DEF(MEMORY_USED_SIZE, "observer memory used size", ObStatClassIds::RESOURCE, "observer memory used size",
    140008, false, true)
STAT_EVENT_SET_DEF(MEMORY_FREE_SIZE, "observer memory free size", ObStatClassIds::RESOURCE, "observer memory free size",
    140009, false, true)
STAT_EVENT_SET_DEF(IS_MINI_MODE, "is mini mode", ObStatClassIds::RESOURCE, "is mini mode", 140010, false, true)
STAT_EVENT_SET_DEF(MEMORY_HOLD_SIZE, "observer memory hold size", ObStatClassIds::RESOURCE, "observer memory hold size",
    140011, false, true)

// CLOG
STAT_EVENT_SET_DEF(
    CLOG_DISK_FREE_SIZE, "clog disk free size", ObStatClassIds::CLOG, "clog disk free size", 150001, false, true)
STAT_EVENT_SET_DEF(
    CLOG_DISK_FREE_RATIO, "clog disk free ratio", ObStatClassIds::CLOG, "clog disk free ratio", 150002, false, true)
STAT_EVENT_SET_DEF(CLOG_LAST_CHECK_LOG_FILE_COLLECT_TIME, "clog last check log file collect time", ObStatClassIds::CLOG,
    "clog last check log file collect time", 150003, false, true)

// DEBUG
STAT_EVENT_SET_DEF(
    OBLOGGER_WRITE_SIZE, "oblogger log bytes", ObStatClassIds::DEBUG, "oblogger log bytes", 160001, false, true)
STAT_EVENT_SET_DEF(
    ELECTION_WRITE_SIZE, "election log bytes", ObStatClassIds::DEBUG, "election log bytes", 160002, false, true)
STAT_EVENT_SET_DEF(ROOTSERVICE_WRITE_SIZE, "rootservice log bytes", ObStatClassIds::DEBUG, "rootservice log bytes",
    160003, false, true)
STAT_EVENT_SET_DEF(OBLOGGER_TOTAL_WRITE_COUNT, "oblogger total log count", ObStatClassIds::DEBUG,
    "oblogger total log count", 160004, false, true)
STAT_EVENT_SET_DEF(ELECTION_TOTAL_WRITE_COUNT, "election total log count", ObStatClassIds::DEBUG,
    "election total log count", 160005, false, true)
STAT_EVENT_SET_DEF(ROOTSERVICE_TOTAL_WRITE_COUNT, "rootservice total log count", ObStatClassIds::DEBUG,
    "rootservice total log count", 160006, false, true)
STAT_EVENT_SET_DEF(ASYNC_TINY_LOG_WRITE_COUNT, "async tiny log write count", ObStatClassIds::DEBUG,
    "async tiny log write count", 160007, false, true)
STAT_EVENT_SET_DEF(ASYNC_NORMAL_LOG_WRITE_COUNT, "async normal log write count", ObStatClassIds::DEBUG,
    "async normal log write count", 160008, false, true)
STAT_EVENT_SET_DEF(ASYNC_LARGE_LOG_WRITE_COUNT, "async large log write count", ObStatClassIds::DEBUG,
    "async large log write count", 160009, false, true)
STAT_EVENT_SET_DEF(ASYNC_TINY_LOG_DROPPED_COUNT, "async tiny log dropped count", ObStatClassIds::DEBUG,
    "async tiny log dropped count", 160010, false, true)
STAT_EVENT_SET_DEF(ASYNC_NORMAL_LOG_DROPPED_COUNT, "async normal log dropped count", ObStatClassIds::DEBUG,
    "async normal log dropped count", 160011, false, true)
STAT_EVENT_SET_DEF(ASYNC_LARGE_LOG_DROPPED_COUNT, "async large log dropped count", ObStatClassIds::DEBUG,
    "async large log dropped count", 160012, false, true)
STAT_EVENT_SET_DEF(ASYNC_ACTIVE_TINY_LOG_COUNT, "async active tiny log count", ObStatClassIds::DEBUG,
    "async active tiny log count", 160013, false, true)
STAT_EVENT_SET_DEF(ASYNC_ACTIVE_NORMAL_LOG_COUNT, "async active normal log count", ObStatClassIds::DEBUG,
    "async active normal log count", 160014, false, true)
STAT_EVENT_SET_DEF(ASYNC_ACTIVE_LARGE_LOG_COUNT, "async active large log count", ObStatClassIds::DEBUG,
    "async active large log count", 160015, false, true)
STAT_EVENT_SET_DEF(ASYNC_RELEASED_TINY_LOG_COUNT, "async released tiny log count", ObStatClassIds::DEBUG,
    "async released tiny log count", 160016, false, true)
STAT_EVENT_SET_DEF(ASYNC_RELEASED_NORMAL_LOG_COUNT, "async released normal log count", ObStatClassIds::DEBUG,
    "async released normal log count", 160017, false, true)
STAT_EVENT_SET_DEF(ASYNC_RELEASED_LARGE_LOG_COUNT, "async released large log count", ObStatClassIds::DEBUG,
    "async released large log count", 160018, false, true)
STAT_EVENT_SET_DEF(ASYNC_ERROR_LOG_DROPPED_COUNT, "async error log dropped count", ObStatClassIds::DEBUG,
    "async error log dropped count", 160019, false, true)
STAT_EVENT_SET_DEF(ASYNC_WARN_LOG_DROPPED_COUNT, "async warn log dropped count", ObStatClassIds::DEBUG,
    "async warn log dropped count", 160020, false, true)
STAT_EVENT_SET_DEF(ASYNC_INFO_LOG_DROPPED_COUNT, "async info log dropped count", ObStatClassIds::DEBUG,
    "async info log dropped count", 160021, false, true)
STAT_EVENT_SET_DEF(ASYNC_TRACE_LOG_DROPPED_COUNT, "async trace log dropped count", ObStatClassIds::DEBUG,
    "async trace log dropped count", 160022, false, true)
STAT_EVENT_SET_DEF(ASYNC_DEBUG_LOG_DROPPED_COUNT, "async debug log dropped count", ObStatClassIds::DEBUG,
    "async debug log dropped count", 160023, false, true)
STAT_EVENT_SET_DEF(
    ASYNC_LOG_FLUSH_SPEED, "async log flush speed", ObStatClassIds::DEBUG, "async log flush speed", 160024, false, true)
STAT_EVENT_SET_DEF(ASYNC_GENERIC_LOG_WRITE_COUNT, "async generic log write count", ObStatClassIds::DEBUG,
    "async generic log write count", 160025, false, true)
STAT_EVENT_SET_DEF(ASYNC_USER_REQUEST_LOG_WRITE_COUNT, "async user request log write count", ObStatClassIds::DEBUG,
    "async user request log write count", 160026, false, true)
STAT_EVENT_SET_DEF(ASYNC_DATA_MAINTAIN_LOG_WRITE_COUNT, "async data maintain log write count", ObStatClassIds::DEBUG,
    "async data maintain log write count", 160027, false, true)
STAT_EVENT_SET_DEF(ASYNC_ROOT_SERVICE_LOG_WRITE_COUNT, "async root service log write count", ObStatClassIds::DEBUG,
    "async root service log write count", 160028, false, true)
STAT_EVENT_SET_DEF(ASYNC_SCHEMA_LOG_WRITE_COUNT, "async schema log write count", ObStatClassIds::DEBUG,
    "async schema log write count", 160029, false, true)
STAT_EVENT_SET_DEF(ASYNC_FORCE_ALLOW_LOG_WRITE_COUNT, "async force allow log write count", ObStatClassIds::DEBUG,
    "async force allow log write count", 160030, false, true)
STAT_EVENT_SET_DEF(ASYNC_GENERIC_LOG_DROPPED_COUNT, "async generic log dropped count", ObStatClassIds::DEBUG,
    "async generic log dropped count", 160031, false, true)
STAT_EVENT_SET_DEF(ASYNC_USER_REQUEST_LOG_DROPPED_COUNT, "async user request log dropped count", ObStatClassIds::DEBUG,
    "async user request log dropped count", 160032, false, true)
STAT_EVENT_SET_DEF(ASYNC_DATA_MAINTAIN_LOG_DROPPED_COUNT, "async data maintain log dropped count",
    ObStatClassIds::DEBUG, "async data maintain log dropped count", 160033, false, true)
STAT_EVENT_SET_DEF(ASYNC_ROOT_SERVICE_LOG_DROPPED_COUNT, "async root service log dropped count", ObStatClassIds::DEBUG,
    "async root service log dropped count", 160034, false, true)
STAT_EVENT_SET_DEF(ASYNC_SCHEMA_LOG_DROPPED_COUNT, "async schema log dropped count", ObStatClassIds::DEBUG,
    "async schema log dropped count", 160035, false, true)
STAT_EVENT_SET_DEF(ASYNC_FORCE_ALLOW_LOG_DROPPED_COUNT, "async force allow log dropped count", ObStatClassIds::DEBUG,
    "async force allow log dropped count", 160036, false, true)
STAT_EVENT_SET_DEF(ASYNC_TINY_LOG_WRITE_COUNT_FOR_ERR, "async tiny log write count for error log",
    ObStatClassIds::DEBUG, "async tiny log write count for error log", 160037, false, true)
STAT_EVENT_SET_DEF(ASYNC_NORMAL_LOG_WRITE_COUNT_FOR_ERR, "async normal log write count for error log",
    ObStatClassIds::DEBUG, "async normal log write count for error log", 160038, false, true)
STAT_EVENT_SET_DEF(ASYNC_LARGE_LOG_WRITE_COUNT_FOR_ERR, "async large log write count for err/or log",
    ObStatClassIds::DEBUG, "async large log write count for error log", 160039, false, true)
STAT_EVENT_SET_DEF(ASYNC_TINY_LOG_DROPPED_COUNT_FOR_ERR, "async tiny log dropped count for error log",
    ObStatClassIds::DEBUG, "async tiny log dropped count for error log", 160040, false, true)
STAT_EVENT_SET_DEF(ASYNC_NORMAL_LOG_DROPPED_COUNT_FOR_ERR, "async normal log dropped count for error log",
    ObStatClassIds::DEBUG, "async normal log dropped count for error log", 160041, false, true)
STAT_EVENT_SET_DEF(ASYNC_LARGE_LOG_DROPPED_COUNT_FOR_ERR, "async large log dropped count for error log",
    ObStatClassIds::DEBUG, "async large log dropped count for error log", 160042, false, true)
STAT_EVENT_SET_DEF(ASYNC_ACTIVE_TINY_LOG_COUNT_FOR_ERR, "async active tiny log count for error log",
    ObStatClassIds::DEBUG, "async active tiny log count for error log", 160043, false, true)
STAT_EVENT_SET_DEF(ASYNC_ACTIVE_NORMAL_LOG_COUNT_FOR_ERR, "async active normal log count for error log",
    ObStatClassIds::DEBUG, "async active normal log count for error log", 160044, false, true)
STAT_EVENT_SET_DEF(ASYNC_ACTIVE_LARGE_LOG_COUNT_FOR_ERR, "async active large log count for error log",
    ObStatClassIds::DEBUG, "async active large log count for error log", 160045, false, true)
STAT_EVENT_SET_DEF(ASYNC_RELEASED_TINY_LOG_COUNT_FOR_ERR, "async released tiny log count for error log",
    ObStatClassIds::DEBUG, "async released tiny log count for error log", 160046, false, true)
STAT_EVENT_SET_DEF(ASYNC_RELEASED_NORMAL_LOG_COUNT_FOR_ERR, "async released normal log count for error log",
    ObStatClassIds::DEBUG, "async released normal log count for error log", 160047, false, true)
STAT_EVENT_SET_DEF(ASYNC_RELEASED_LARGE_LOG_COUNT_FOR_ERR, "async released large log count for error log",
    ObStatClassIds::DEBUG, "async released large log countfor error log", 160048, false, true)

// OBSERVER
STAT_EVENT_SET_DEF(OBSERVER_PARTITION_TABLE_UPATER_USER_QUEUE_SIZE,
    "observer partition table updater user table queue size", ObStatClassIds::OBSERVER,
    "observer partition table updater user table queue size", 170001, false, true)
STAT_EVENT_SET_DEF(OBSERVER_PARTITION_TABLE_UPATER_SYS_QUEUE_SIZE,
    "observer partition table updater system table queue size", ObStatClassIds::OBSERVER,
    "observer partition table updater system table queue size", 170002, false, true)
STAT_EVENT_SET_DEF(OBSERVER_PARTITION_TABLE_UPATER_CORE_QUEUE_SIZE,
    "observer partition table updater core table queue size", ObStatClassIds::OBSERVER,
    "observer partition table updater core table queue size", 170003, false, true)

// rootservice
STAT_EVENT_SET_DEF(
    RS_START_SERVICE_TIME, "rootservice start time", ObStatClassIds::RS, "rootservice start time", 180001, false, true)
STAT_EVENT_SET_DEF(RS_TASK_QUEUE_SIZE_HIGH_WAIT, "rootservice waiting task count with high priority",
    ObStatClassIds::RS, "rootservice waiting task count with high priority", 180002, false, true)
STAT_EVENT_SET_DEF(RS_TASK_QUEUE_SIZE_HIGH_SCHED, "rootservice scheduled task count with high priority",
    ObStatClassIds::RS, "rootservice scheduled task count with high priority", 180003, false, true)
STAT_EVENT_SET_DEF(RS_TASK_QUEUE_SIZE_LOW_WAIT, "rootservice waiting task count with low priority", ObStatClassIds::RS,
    "rootservice waiting task count with low priority", 180004, false, true)
STAT_EVENT_SET_DEF(RS_TASK_QUEUE_SIZE_LOW_SCHED, "rootservice scheduled task count with low priority",
    ObStatClassIds::RS, "rootservice scheduled task count with low priority", 180005, false, true)
// END
STAT_EVENT_SET_DEF(STAT_EVENT_SET_END, "event set end", ObStatClassIds::DEBUG, "event set end", 300000, false, false)
#endif

#ifndef OB_STAT_EVENT_DEFINE_H_
#define OB_STAT_EVENT_DEFINE_H_

#include "lib/time/ob_time_utility.h"
#include "lib/statistic_event/ob_stat_class.h"

namespace oceanbase {
namespace common {
static const int64_t MAX_STAT_EVENT_NAME_LENGTH = 64;
static const int64_t MAX_STAT_EVENT_PARAM_LENGTH = 64;

struct ObStatEventIds {
  enum ObStatEventIdEnum {
#define STAT_EVENT_ADD_DEF(def, name, stat_class, display_name, stat_id, summary_in_session, can_visible) def,
#include "lib/statistic_event/ob_stat_event.h"
#undef STAT_EVENT_ADD_DEF
#define STAT_EVENT_SET_DEF(def, name, stat_class, display_name, stat_id, summary_in_session, can_visible) def,
#include "lib/statistic_event/ob_stat_event.h"
#undef STAT_EVENT_SET_DEF
  };
};

struct ObStatEventAddStat {
  // stat no
  int64_t stat_no_;
  // value for a stat
  int64_t stat_value_;
  ObStatEventAddStat();
  int add(const ObStatEventAddStat& other);
  int add(int64_t value);
  int64_t get_stat_value();
  void reset();
  inline bool is_valid() const
  {
    return true;
  }
};

inline int64_t ObStatEventAddStat::get_stat_value()
{
  return stat_value_;
}

struct ObStatEventSetStat {
  // stat no
  int64_t stat_no_;
  // value for a stat
  int64_t stat_value_;
  int64_t set_time_;
  ObStatEventSetStat();
  int add(const ObStatEventSetStat& other);
  void set(int64_t value);
  int64_t get_stat_value();
  void reset();
  inline bool is_valid() const
  {
    return stat_value_ > 0;
  }
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

struct ObStatEvent {
  char name_[MAX_STAT_EVENT_NAME_LENGTH];
  int64_t stat_class_;
  char display_name_[MAX_STAT_EVENT_NAME_LENGTH];
  int64_t stat_id_;
  bool summary_in_session_;
  bool can_visible_;
};

extern const ObStatEvent OB_STAT_EVENTS[];

}  // namespace common
}  // namespace oceanbase

#endif /* OB_WAIT_EVENT_DEFINE_H_ */
