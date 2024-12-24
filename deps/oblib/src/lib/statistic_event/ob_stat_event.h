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
/**
 * STAT_EVENT_ADD_DEF(def, name, stat_class, stat_id, summary_in_session, can_visible, enable)
 * STAT_EVENT_SET_DEF(def, name, stat_class, stat_id, summary_in_session, can_visible, enable)
 * @param def name of this stat
 * @param name Name of this stat. Display on v$statname.
 * @param stat_class Every stats belongs to a class on deps/oblib/src/lib/statistic_event/ob_stat_class.h
 * @param stat_id Identifier of an stat ATTENTION: please add id placeholder on master.
 * @param summary_in_session Stat recorded for user session process mark this flag true.
 * @param can_visible Indicate whether this stat can be queried on gv$sysstat and gv$sesstat.
 * @param enable Indicate whether this stat is enabled. Marked it false it you merely need it as an placeholder.
 * NOTICE: do not reuse stat id or rename stat event!
*/

// NETWORK
STAT_EVENT_ADD_DEF(RPC_PACKET_IN, "rpc packet in", ObStatClassIds::NETWORK, 10000, true, true, true)
STAT_EVENT_ADD_DEF(RPC_PACKET_IN_BYTES, "rpc packet in bytes", ObStatClassIds::NETWORK, 10001, false, true, true)
STAT_EVENT_ADD_DEF(RPC_PACKET_OUT, "rpc packet out", ObStatClassIds::NETWORK, 10002, true, true, true)
STAT_EVENT_ADD_DEF(RPC_PACKET_OUT_BYTES, "rpc packet out bytes", ObStatClassIds::NETWORK, 10003, false, true, true)
STAT_EVENT_ADD_DEF(RPC_DELIVER_FAIL, "rpc deliver fail", ObStatClassIds::NETWORK, 10004, false, true, true)
STAT_EVENT_ADD_DEF(RPC_NET_DELAY, "rpc net delay", ObStatClassIds::NETWORK, 10005, false, true, true)
STAT_EVENT_ADD_DEF(RPC_NET_FRAME_DELAY, "rpc net frame delay", ObStatClassIds::NETWORK, 10006, false, true, true)
STAT_EVENT_ADD_DEF(MYSQL_PACKET_IN, "mysql packet in", ObStatClassIds::NETWORK, 10007, false, true, true)
STAT_EVENT_ADD_DEF(MYSQL_PACKET_IN_BYTES, "mysql packet in bytes", ObStatClassIds::NETWORK, 10008, false, true, true)
STAT_EVENT_ADD_DEF(MYSQL_PACKET_OUT, "mysql packet out", ObStatClassIds::NETWORK, 10009, false, true, true)
STAT_EVENT_ADD_DEF(MYSQL_PACKET_OUT_BYTES, "mysql packet out bytes", ObStatClassIds::NETWORK, 10010, false, true, true)
STAT_EVENT_ADD_DEF(MYSQL_DELIVER_FAIL, "mysql deliver fail", ObStatClassIds::NETWORK, 10011, false, true, true)
STAT_EVENT_ADD_DEF(RPC_COMPRESS_ORIGINAL_PACKET_CNT, "rpc compress original packet cnt", ObStatClassIds::NETWORK, 10012, false, true, true)
STAT_EVENT_ADD_DEF(RPC_COMPRESS_COMPRESSED_PACKET_CNT, "rpc compress compressed packet cnt", ObStatClassIds::NETWORK, 10013, false, true, true)
STAT_EVENT_ADD_DEF(RPC_COMPRESS_ORIGINAL_SIZE, "rpc compress original size", ObStatClassIds::NETWORK, 10014, false, true, true)
STAT_EVENT_ADD_DEF(RPC_COMPRESS_COMPRESSED_SIZE, "rpc compress compressed size", ObStatClassIds::NETWORK, 10015, false, true, true)

STAT_EVENT_ADD_DEF(RPC_STREAM_COMPRESS_ORIGINAL_PACKET_CNT, "rpc stream compress original packet cnt", ObStatClassIds::NETWORK, 10016, false, true, true)
STAT_EVENT_ADD_DEF(RPC_STREAM_COMPRESS_COMPRESSED_PACKET_CNT, "rpc stream compress compressed packet cnt", ObStatClassIds::NETWORK, 10017, false, true, true)
STAT_EVENT_ADD_DEF(RPC_STREAM_COMPRESS_ORIGINAL_SIZE, "rpc stream compress original size", ObStatClassIds::NETWORK, 10018, false, true, true)
STAT_EVENT_ADD_DEF(RPC_STREAM_COMPRESS_COMPRESSED_SIZE, "rpc stream compress compressed size", ObStatClassIds::NETWORK, 10019, false, true, true)

// QUEUE
STAT_EVENT_ADD_DEF(REQUEST_QUEUED_COUNT, "REQUEST_QUEUED_COUNT", QUEUE, "REQUEST_QUEUED_COUNT", true, true, false)
STAT_EVENT_ADD_DEF(REQUEST_ENQUEUE_COUNT, "request enqueue count", ObStatClassIds::QUEUE, 20000, false, true, true)
STAT_EVENT_ADD_DEF(REQUEST_DEQUEUE_COUNT, "request dequeue count", ObStatClassIds::QUEUE, 20001, false, true, true)
STAT_EVENT_ADD_DEF(REQUEST_QUEUE_TIME, "request queue time", ObStatClassIds::QUEUE, 20002, false, true, true)

// TRANS
STAT_EVENT_ADD_DEF(TRANS_COMMIT_LOG_SYNC_TIME, "trans commit log sync time", ObStatClassIds::TRANS, 30000, false, true, true)
STAT_EVENT_ADD_DEF(TRANS_COMMIT_LOG_SYNC_COUNT, "trans commit log sync count", ObStatClassIds::TRANS, 30001, false, true, true)
STAT_EVENT_ADD_DEF(TRANS_COMMIT_LOG_SUBMIT_COUNT, "trans commit log submit count", ObStatClassIds::TRANS, 30002, false, true, true)
STAT_EVENT_ADD_DEF(TRANS_SYSTEM_TRANS_COUNT, "trans system trans count", ObStatClassIds::TRANS, 30003, false, true, true)
STAT_EVENT_ADD_DEF(TRANS_USER_TRANS_COUNT, "trans user trans count", ObStatClassIds::TRANS, 30004, false, true, true)
STAT_EVENT_ADD_DEF(TRANS_START_COUNT, "trans start count", ObStatClassIds::TRANS, 30005, false, true, true)
STAT_EVENT_ADD_DEF(TRANS_TOTAL_USED_TIME, "trans total used time", ObStatClassIds::TRANS, 30006, false, true, true)
STAT_EVENT_ADD_DEF(TRANS_COMMIT_COUNT, "trans commit count", ObStatClassIds::TRANS, 30007, false, true, true)
STAT_EVENT_ADD_DEF(TRANS_COMMIT_TIME, "trans commit time", ObStatClassIds::TRANS, 30008, false, true, true)
STAT_EVENT_ADD_DEF(TRANS_ROLLBACK_COUNT, "trans rollback count", ObStatClassIds::TRANS, 30009, false, true, true)
STAT_EVENT_ADD_DEF(TRANS_ROLLBACK_TIME, "trans rollback time", ObStatClassIds::TRANS, 30010, false, true, true)
STAT_EVENT_ADD_DEF(TRANS_TIMEOUT_COUNT, "trans timeout count", ObStatClassIds::TRANS, 30011, false, true, true)
STAT_EVENT_ADD_DEF(TRANS_LOCAL_COUNT, "trans local trans count", ObStatClassIds::TRANS, 30012, false, true, true)
STAT_EVENT_ADD_DEF(TRANS_DIST_COUNT, "trans distribute trans count", ObStatClassIds::TRANS, 30013, false, true, true)
STAT_EVENT_ADD_DEF(TRANS_DISTRIBUTED_STMT_COUNT, "trans distributed stmt count", TRANS, 30014, true, true, false)
STAT_EVENT_ADD_DEF(TRANS_LOCAL_STMT_COUNT, "trans local stmt count", TRANS, 30015, true, true, false)
STAT_EVENT_ADD_DEF(TRANS_REMOTE_STMT_COUNT, "trans remote stmt count", TRANS, 30016, true, true, false)
STAT_EVENT_ADD_DEF(TRANS_READONLY_COUNT, "trans without participant count", ObStatClassIds::TRANS, 30017, false, true, true)
STAT_EVENT_ADD_DEF(REDO_LOG_REPLAY_COUNT, "redo log replay count", ObStatClassIds::TRANS, 30018, false, true, true)
STAT_EVENT_ADD_DEF(REDO_LOG_REPLAY_TIME, "redo log replay time", ObStatClassIds::TRANS, 30019, false, true, true)
STAT_EVENT_ADD_DEF(PREPARE_LOG_REPLAY_COUNT, "prepare log replay count", ObStatClassIds::TRANS, 30020, false, true, true)
STAT_EVENT_ADD_DEF(PREPARE_LOG_REPLAY_TIME, "prepare log replay time", ObStatClassIds::TRANS, 30021, false, true, true)
STAT_EVENT_ADD_DEF(COMMIT_LOG_REPLAY_COUNT, "commit log replay count", ObStatClassIds::TRANS, 30022, false, true, true)
STAT_EVENT_ADD_DEF(COMMIT_LOG_REPLAY_TIME, "commit log replay time", ObStatClassIds::TRANS, 30023, false, true, true)
STAT_EVENT_ADD_DEF(ABORT_LOG_REPLAY_COUNT, "abort log replay count", ObStatClassIds::TRANS, 30024, false, true, true)
STAT_EVENT_ADD_DEF(ABORT_LOG_REPLAY_TIME, "abort log replay time", ObStatClassIds::TRANS, 30025, false, true, true)
STAT_EVENT_ADD_DEF(CLEAR_LOG_REPLAY_COUNT, "clear log replay count", ObStatClassIds::TRANS, 30026, false, true, true)
STAT_EVENT_ADD_DEF(CLEAR_LOG_REPLAY_TIME, "clear log replay time", ObStatClassIds::TRANS, 30027, false, true, true)
STAT_EVENT_ADD_DEF(GTS_REQUEST_TOTAL_COUNT, "gts request total count", ObStatClassIds::TRANS, 30053, false, true, true)
STAT_EVENT_ADD_DEF(GTS_ACQUIRE_TOTAL_TIME, "gts acquire total time", ObStatClassIds::TRANS, 30054, false, true, true)
STAT_EVENT_ADD_DEF(GTS_ACQUIRE_TOTAL_WAIT_COUNT, "gts acquire total wait count", ObStatClassIds::TRANS, 30056, false, true, true)
STAT_EVENT_ADD_DEF(GTS_WAIT_ELAPSE_TOTAL_TIME, "gts wait elapse total time", ObStatClassIds::TRANS, 30063, false, true, true)
STAT_EVENT_ADD_DEF(GTS_WAIT_ELAPSE_TOTAL_COUNT, "gts wait elapse total count", ObStatClassIds::TRANS, 30064, false, true, true)
STAT_EVENT_ADD_DEF(GTS_WAIT_ELAPSE_TOTAL_WAIT_COUNT, "gts wait elapse total wait count", ObStatClassIds::TRANS, 30065, false, true, true)
STAT_EVENT_ADD_DEF(GTS_RPC_COUNT, "gts rpc count", ObStatClassIds::TRANS, 30066, false, true, true)
STAT_EVENT_ADD_DEF(GTS_TRY_ACQUIRE_TOTAL_COUNT, "gts try acquire total count", ObStatClassIds::TRANS, 30067, false, true, true)
STAT_EVENT_ADD_DEF(GTS_TRY_WAIT_ELAPSE_TOTAL_COUNT, "gts try wait elapse total count", ObStatClassIds::TRANS, 30068, false, true, true)
STAT_EVENT_ADD_DEF(TRANS_ELR_ENABLE_COUNT, "trans early lock release enable count", ObStatClassIds::TRANS, 30077, false, true, true)
STAT_EVENT_ADD_DEF(TRANS_ELR_UNABLE_COUNT, "trans early lock release unable count", ObStatClassIds::TRANS, 30078, false, true, true)
STAT_EVENT_ADD_DEF(READ_ELR_ROW_COUNT, "read elr row count", ObStatClassIds::TRANS, 30079, false, true, true)
STAT_EVENT_ADD_DEF(TRANS_LOCAL_TOTAL_USED_TIME, "local trans total used time", ObStatClassIds::TRANS, 30080, false, true, true)
STAT_EVENT_ADD_DEF(TRANS_DIST_TOTAL_USED_TIME, "distributed trans total used time", ObStatClassIds::TRANS, 30081, false, true, true)
STAT_EVENT_ADD_DEF(TX_DATA_HIT_MINI_CACHE_COUNT, "tx data hit mini cache count", ObStatClassIds::TRANS, 30082, false, true, true)
STAT_EVENT_ADD_DEF(TX_DATA_HIT_KV_CACHE_COUNT, "tx data hit kv cache count", ObStatClassIds::TRANS, 30083, false, true, true)
STAT_EVENT_ADD_DEF(TX_DATA_READ_TX_CTX_COUNT, "tx data read tx ctx count", ObStatClassIds::TRANS, 30084, false, true, true)
STAT_EVENT_ADD_DEF(TX_DATA_READ_TX_DATA_MEMTABLE_COUNT, "tx data read tx data memtable count", ObStatClassIds::TRANS, 30085, false, true, true)
STAT_EVENT_ADD_DEF(TX_DATA_READ_TX_DATA_SSTABLE_COUNT, "tx data read tx data sstable count", ObStatClassIds::TRANS, 30086, false, true, true)
// XA TRANS
STAT_EVENT_ADD_DEF(XA_START_TOTAL_COUNT, "xa start total count", ObStatClassIds::TRANS, 30200, false, true, true)
STAT_EVENT_ADD_DEF(XA_START_TOTAL_USED_TIME, "xa start total used time", ObStatClassIds::TRANS, 30201, false, true, true)
STAT_EVENT_ADD_DEF(XA_START_REMOTE_COUNT, "xa start with rpc total count", ObStatClassIds::TRANS, 30202, false, true, true)
STAT_EVENT_ADD_DEF(XA_START_FAIL_COUNT, "failed xa start total count", ObStatClassIds::TRANS, 30203, false, true, true)
STAT_EVENT_ADD_DEF(XA_END_TOTAL_COUNT, "xa end total count", ObStatClassIds::TRANS, 30204, false, true, true)
STAT_EVENT_ADD_DEF(XA_END_TOTAL_USED_TIME, "xa end total used count", ObStatClassIds::TRANS, 30205, false, true, true)
STAT_EVENT_ADD_DEF(XA_END_REMOTE_COUNT, "xa end with rpc total count", ObStatClassIds::TRANS, 30206, false, true, true)
STAT_EVENT_ADD_DEF(XA_END_FAIL_COUNT, "failed xa end total count", ObStatClassIds::TRANS, 30207, false, true, true)
STAT_EVENT_ADD_DEF(XA_PREPARE_TOTAL_COUNT, "xa prepare total count", ObStatClassIds::TRANS, 30208, false, true, true)
STAT_EVENT_ADD_DEF(XA_PREPARE_TOTAL_USED_TIME, "xa prepare total used time", ObStatClassIds::TRANS, 30209, false, true, true)
STAT_EVENT_ADD_DEF(XA_PREPARE_REMOTE_COUNT, "xa prepare with rpc total count", ObStatClassIds::TRANS, 30210, false, true, true)
STAT_EVENT_ADD_DEF(XA_PREPARE_FAIL_COUNT, "failed xa prepare total count", ObStatClassIds::TRANS, 30211, false, true, true)
STAT_EVENT_ADD_DEF(XA_COMMIT_TOTAL_COUNT, "xa commit total count", ObStatClassIds::TRANS, 30212, false, true, true)
STAT_EVENT_ADD_DEF(XA_COMMIT_TOTAL_USED_TIME, "xa commit total used time", ObStatClassIds::TRANS, 30213, false, true, true)
STAT_EVENT_ADD_DEF(XA_COMMIT_REMOTE_COUNT, "xa commit with rpc total count", ObStatClassIds::TRANS, 30214, false, true, true)
STAT_EVENT_ADD_DEF(XA_COMMIT_FAIL_COUNT, "failed xa commit total count", ObStatClassIds::TRANS, 30215, false, true, true)
STAT_EVENT_ADD_DEF(XA_ROLLBACK_TOTAL_COUNT, "xa rollback total count", ObStatClassIds::TRANS, 30216, false, true, true)
STAT_EVENT_ADD_DEF(XA_ROLLBACK_TOTAL_USED_TIME, "xa rollback total used time", ObStatClassIds::TRANS, 30217, false, true, true)
STAT_EVENT_ADD_DEF(XA_ROLLBACK_REMOTE_COUNT, "xa rollback with rpc total count", ObStatClassIds::TRANS, 30218, false, true, true)
STAT_EVENT_ADD_DEF(XA_ROLLBACK_FAIL_COUNT, "failed xa rollback total count", ObStatClassIds::TRANS, 30219, false, true, true)
STAT_EVENT_ADD_DEF(XA_TRANS_START_COUNT, "started xa trans count", ObStatClassIds::TRANS, 30220, false, true, true)
STAT_EVENT_ADD_DEF(XA_READ_ONLY_TRANS_TOTAL_COUNT, "read only xa trans total count", ObStatClassIds::TRANS, 30221, false, true, true)
STAT_EVENT_ADD_DEF(XA_ONE_PHASE_COMMIT_TOTAL_COUNT, "xa trans with one phase commit total count", ObStatClassIds::TRANS, 30222, false, true, true)
STAT_EVENT_ADD_DEF(XA_INNER_SQL_TOTAL_COUNT, "inner sql total count in xa statement", ObStatClassIds::TRANS, 30223, false, true, true)
STAT_EVENT_ADD_DEF(XA_INNER_SQL_TEN_MS_COUNT, "total count of inner sql (latency >= 10ms) in xa statement", ObStatClassIds::TRANS, 30224, false, true, true)
STAT_EVENT_ADD_DEF(XA_INNER_SQL_TWENTY_MS_COUNT, "total count of inner sql (latency >= 20ms) in xa statement", ObStatClassIds::TRANS, 30225, false, true, true)
STAT_EVENT_ADD_DEF(XA_INNER_SQL_TOTAL_USED_TIME, "inner sql total used time in xa statement", ObStatClassIds::TRANS, 30226, false, true, true)
STAT_EVENT_ADD_DEF(XA_INNER_RPC_TOTAL_COUNT, "inner rpc total count in xa statement", ObStatClassIds::TRANS, 30227, false, true, true)
STAT_EVENT_ADD_DEF(XA_INNER_RPC_TEN_MS_COUNT, "total count of inner rpc (latency >= 10ms) in xa statement", ObStatClassIds::TRANS, 30228, false, true, true)
STAT_EVENT_ADD_DEF(XA_INNER_RPC_TWENTY_MS_COUNT, "total count of inner rpc (latency >= 20ms) in xa statement", ObStatClassIds::TRANS, 30229, false, true, true)
STAT_EVENT_ADD_DEF(XA_INNER_RPC_TOTAL_USED_TIME, "inner rpc total used time in xa statement", ObStatClassIds::TRANS, 30230, false, true, true)
// DBLINK TRANS
STAT_EVENT_ADD_DEF(DBLINK_TRANS_COUNT, "dblink trans total count", ObStatClassIds::TRANS, 30231, false, true, true)
STAT_EVENT_ADD_DEF(DBLINK_TRANS_FAIL_COUNT, "failed dblink trans total count", ObStatClassIds::TRANS, 30232, false, true, true)
STAT_EVENT_ADD_DEF(DBLINK_TRANS_PROMOTION_COUNT, "dblink trans promotion total count", ObStatClassIds::TRANS, 30233, false, true, true)
STAT_EVENT_ADD_DEF(DBLINK_TRANS_CALLBACK_COUNT, "dblink trans callback total count", ObStatClassIds::TRANS, 30234, false, true, true)
STAT_EVENT_ADD_DEF(DBLINK_TRANS_COMMIT_COUNT, "dblink trans commit total count", ObStatClassIds::TRANS, 30235, false, true, true)
STAT_EVENT_ADD_DEF(DBLINK_TRANS_COMMIT_USED_TIME, "dblink trans commit total used time", ObStatClassIds::TRANS, 30236, false, true, true)
STAT_EVENT_ADD_DEF(DBLINK_TRANS_COMMIT_FAIL_COUNT, "failed dblink trans commit total count", ObStatClassIds::TRANS, 30237, false, true, true)
STAT_EVENT_ADD_DEF(DBLINK_TRANS_ROLLBACK_COUNT, "dblink trans rollback total count", ObStatClassIds::TRANS, 30238, false, true, true)
STAT_EVENT_ADD_DEF(DBLINK_TRANS_ROLLBACK_USED_TIME, "dblink trans rollback total used time", ObStatClassIds::TRANS, 30239, false, true, true)
STAT_EVENT_ADD_DEF(DBLINK_TRANS_ROLLBACK_FAIL_COUNT, "failed dblink trans rollback total count", ObStatClassIds::TRANS, 30240, false, true, true)

// SQL
STAT_EVENT_ADD_DEF(PLAN_CACHE_HIT, "PLAN_CACHE_HIT", SQL, "PLAN_CACHE_HIT", true, true, false)
STAT_EVENT_ADD_DEF(PLAN_CACHE_MISS, "PLAN_CACHE_MISS", SQL, "PLAN_CACHE_MISS", true, true, false)
STAT_EVENT_ADD_DEF(SQL_SELECT_COUNT, "sql select count", ObStatClassIds::SQL, 40000, false, true, true)
STAT_EVENT_ADD_DEF(SQL_SELECT_TIME, "sql select time", ObStatClassIds::SQL, 40001, false, true, true)
STAT_EVENT_ADD_DEF(SQL_INSERT_COUNT, "sql insert count", ObStatClassIds::SQL, 40002, false, true, true)
STAT_EVENT_ADD_DEF(SQL_INSERT_TIME, "sql insert time", ObStatClassIds::SQL, 40003, false, true, true)
STAT_EVENT_ADD_DEF(SQL_REPLACE_COUNT, "sql replace count", ObStatClassIds::SQL, 40004, false, true, true)
STAT_EVENT_ADD_DEF(SQL_REPLACE_TIME, "sql replace time", ObStatClassIds::SQL, 40005, false, true, true)
STAT_EVENT_ADD_DEF(SQL_UPDATE_COUNT, "sql update count", ObStatClassIds::SQL, 40006, false, true, true)
STAT_EVENT_ADD_DEF(SQL_UPDATE_TIME, "sql update time", ObStatClassIds::SQL, 40007, false, true, true)
STAT_EVENT_ADD_DEF(SQL_DELETE_COUNT, "sql delete count", ObStatClassIds::SQL, 40008, false, true, true)
STAT_EVENT_ADD_DEF(SQL_DELETE_TIME, "sql delete time", ObStatClassIds::SQL, 40009, false, true, true)
STAT_EVENT_ADD_DEF(SQL_OTHER_COUNT, "sql other count", ObStatClassIds::SQL, 40018, false, true, true)
STAT_EVENT_ADD_DEF(SQL_OTHER_TIME, "sql other time", ObStatClassIds::SQL, 40019, false, true, true)
STAT_EVENT_ADD_DEF(SQL_PS_PREPARE_COUNT, "ps prepare count", ObStatClassIds::SQL, 40020, false, true, true)
STAT_EVENT_ADD_DEF(SQL_PS_PREPARE_TIME, "ps prepare time", ObStatClassIds::SQL, 40021, false, true, true)
STAT_EVENT_ADD_DEF(SQL_PS_EXECUTE_COUNT, "ps execute count", ObStatClassIds::SQL, 40022, false, true, true)
STAT_EVENT_ADD_DEF(SQL_PS_CLOSE_COUNT, "ps close count", ObStatClassIds::SQL, 40023, false, true, true)
STAT_EVENT_ADD_DEF(SQL_PS_CLOSE_TIME, "ps close time", ObStatClassIds::SQL, 40024, false, true, true)
STAT_EVENT_ADD_DEF(SQL_COMMIT_COUNT, "sql commit count", ObStatClassIds::SQL, 40025, false, true, true)
STAT_EVENT_ADD_DEF(SQL_COMMIT_TIME, "sql commit time", ObStatClassIds::SQL, 40026, false, true, true)
STAT_EVENT_ADD_DEF(SQL_ROLLBACK_COUNT, "sql rollback count", ObStatClassIds::SQL, 40027, false, true, true)
STAT_EVENT_ADD_DEF(SQL_ROLLBACK_TIME, "sql rollback time", ObStatClassIds::SQL, 40028, false, true, true)

STAT_EVENT_ADD_DEF(SQL_OPEN_CURSORS_CURRENT, "opened cursors current", ObStatClassIds::SQL, 40030, true, true, true)
STAT_EVENT_ADD_DEF(SQL_OPEN_CURSORS_CUMULATIVE, "opened cursors cumulative", ObStatClassIds::SQL, 40031, true, true, true)

STAT_EVENT_ADD_DEF(SQL_LOCAL_COUNT, "sql local count", ObStatClassIds::SQL, 40010, false, true, true)
STAT_EVENT_ADD_DEF(SQL_REMOTE_COUNT, "sql remote count", ObStatClassIds::SQL, 40011, false, true, true)
STAT_EVENT_ADD_DEF(SQL_DISTRIBUTED_COUNT, "sql distributed count", ObStatClassIds::SQL, 40012, false, true, true)
STAT_EVENT_ADD_DEF(ACTIVE_SESSIONS, "active sessions", ObStatClassIds::SQL, 40013, false, true, true)
STAT_EVENT_ADD_DEF(SQL_SINGLE_QUERY_COUNT, "single query count", ObStatClassIds::SQL, 40014, false, true, true)
STAT_EVENT_ADD_DEF(SQL_MULTI_QUERY_COUNT, "multiple query count", ObStatClassIds::SQL, 40015, false, true, true)
STAT_EVENT_ADD_DEF(SQL_MULTI_ONE_QUERY_COUNT, "multiple query with one stmt count", ObStatClassIds::SQL, 40016, false, true, true)

STAT_EVENT_ADD_DEF(SQL_INNER_SELECT_COUNT, "sql inner select count", ObStatClassIds::SQL, 40100, false, true, true)
STAT_EVENT_ADD_DEF(SQL_INNER_SELECT_TIME, "sql inner select time", ObStatClassIds::SQL, 40101, false, true, true)
STAT_EVENT_ADD_DEF(SQL_INNER_INSERT_COUNT, "sql inner insert count", ObStatClassIds::SQL, 40102, false, true, true)
STAT_EVENT_ADD_DEF(SQL_INNER_INSERT_TIME, "sql inner insert time", ObStatClassIds::SQL, 40103, false, true, true)
STAT_EVENT_ADD_DEF(SQL_INNER_REPLACE_COUNT, "sql inner replace count", ObStatClassIds::SQL, 40104, false, true, true)
STAT_EVENT_ADD_DEF(SQL_INNER_REPLACE_TIME, "sql inner replace time", ObStatClassIds::SQL, 40105, false, true, true)
STAT_EVENT_ADD_DEF(SQL_INNER_UPDATE_COUNT, "sql inner update count", ObStatClassIds::SQL, 40106, false, true, true)
STAT_EVENT_ADD_DEF(SQL_INNER_UPDATE_TIME, "sql inner update time", ObStatClassIds::SQL, 40107, false, true, true)
STAT_EVENT_ADD_DEF(SQL_INNER_DELETE_COUNT, "sql inner delete count", ObStatClassIds::SQL, 40108, false, true, true)
STAT_EVENT_ADD_DEF(SQL_INNER_DELETE_TIME, "sql inner delete time", ObStatClassIds::SQL, 40109, false, true, true)
STAT_EVENT_ADD_DEF(SQL_INNER_OTHER_COUNT, "sql inner other count", ObStatClassIds::SQL, 40110, false, true, true)
STAT_EVENT_ADD_DEF(SQL_INNER_OTHER_TIME, "sql inner other time", ObStatClassIds::SQL, 40111, false, true, true)
STAT_EVENT_ADD_DEF(SQL_USER_LOGONS_CUMULATIVE, "user logons cumulative", ObStatClassIds::SQL, 40112, false, true, true)
STAT_EVENT_ADD_DEF(SQL_USER_LOGOUTS_CUMULATIVE, "user logouts cumulative", ObStatClassIds::SQL, 40113, false, true, true)
STAT_EVENT_ADD_DEF(SQL_USER_LOGONS_FAILED_CUMULATIVE, "user logons failed cumulative", ObStatClassIds::SQL, 40114, false, true, true)
STAT_EVENT_ADD_DEF(SQL_USER_LOGONS_COST_TIME_CUMULATIVE, "user logons time cumulative", ObStatClassIds::SQL, 40115, false, true, true)
STAT_EVENT_ADD_DEF(SQL_LOCAL_TIME, "sql local execute time", ObStatClassIds::SQL, 40116, false, true, true)
STAT_EVENT_ADD_DEF(SQL_REMOTE_TIME, "sql remote execute time", ObStatClassIds::SQL, 40117, false, true, true)
STAT_EVENT_ADD_DEF(SQL_DISTRIBUTED_TIME, "sql distributed execute time", ObStatClassIds::SQL, 40118, false, true, true)
STAT_EVENT_ADD_DEF(SQL_FAIL_COUNT, "sql fail count", ObStatClassIds::SQL, 40119, false, true, true)
STAT_EVENT_ADD_DEF(SQL_INNER_LOCAL_COUNT, "inner sql local count", ObStatClassIds::SQL, 40120, false, true, true)
STAT_EVENT_ADD_DEF(SQL_INNER_REMOTE_COUNT, "inner sql remote count", ObStatClassIds::SQL, 40121, false, true, true)
STAT_EVENT_ADD_DEF(SQL_INNER_DISTRIBUTED_COUNT, "inner sql distributed count", ObStatClassIds::SQL, 40122, false, true, true)
STAT_EVENT_ADD_DEF(SQL_INNER_LOCAL_TIME, "inner sql local execute time", ObStatClassIds::SQL, 40123, false, true, false)
STAT_EVENT_ADD_DEF(SQL_INNER_REMOTE_TIME, "inner sql remote execute time", ObStatClassIds::SQL, 40124, false, true, false)
STAT_EVENT_ADD_DEF(SQL_INNER_DISTRIBUTED_TIME, "inner sql distributed execute time", ObStatClassIds::SQL, 40125, false, true, false)

// CACHE
STAT_EVENT_ADD_DEF(ROW_CACHE_HIT, "row cache hit", ObStatClassIds::CACHE, 50000, true, true, true)
STAT_EVENT_ADD_DEF(ROW_CACHE_MISS, "row cache miss", ObStatClassIds::CACHE, 50001, true, true, true)
STAT_EVENT_ADD_DEF(BLOOM_FILTER_CACHE_HIT, "bloom filter cache hit", ObStatClassIds::CACHE, 50004, true, true, true)
STAT_EVENT_ADD_DEF(BLOOM_FILTER_CACHE_MISS, "bloom filter cache miss", ObStatClassIds::CACHE, 50005, true, true, true)
STAT_EVENT_ADD_DEF(BLOOM_FILTER_FILTS, "bloom filter filts", ObStatClassIds::CACHE, 50006, true, true, true)
STAT_EVENT_ADD_DEF(BLOOM_FILTER_PASSES, "bloom filter passes", ObStatClassIds::CACHE, 50007, true, true, true)
STAT_EVENT_ADD_DEF(BLOCK_CACHE_HIT, "block cache hit", ObStatClassIds::CACHE, 50008, true, true, true)
STAT_EVENT_ADD_DEF(BLOCK_CACHE_MISS, "block cache miss", ObStatClassIds::CACHE, 50009, true, true, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_HIT, "location cache hit", ObStatClassIds::CACHE, 50010, false, true, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_MISS, "location cache miss", ObStatClassIds::CACHE, 50011, false, true, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_WAIT, "location cache wait", ObStatClassIds::CACHE, 50012, false, true, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_PROXY_HIT, "location cache get hit from proxy virtual table", ObStatClassIds::CACHE, 50013, false, true, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_PROXY_MISS, "location cache get miss from proxy virtual table", ObStatClassIds::CACHE, 50014, false, true, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_CLEAR_LOCATION, "location cache nonblock renew", ObStatClassIds::CACHE, 50015, false, true, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_CLEAR_LOCATION_IGNORED, "location cache nonblock renew ignored", ObStatClassIds::CACHE, 50016, false, true, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_NONBLOCK_HIT, "location nonblock get hit", ObStatClassIds::CACHE, 50017, false, true, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_NONBLOCK_MISS, "location nonblock get miss", ObStatClassIds::CACHE, 50018, false, true, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_RPC_CHECK_LEADER, "location check leader count", ObStatClassIds::CACHE, 50019, true, true, false)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_RPC_CHECK_MEMBER, "location check member count", ObStatClassIds::CACHE, 50020, true, true, false)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_RPC_CHECK, "location cache rpc renew count", ObStatClassIds::CACHE, 50021, false, true, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_RENEW, "location cache renew", ObStatClassIds::CACHE, 50022, false, true, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_RENEW_IGNORED, "location cache renew ignored", ObStatClassIds::CACHE, 50023, false, true, true)
STAT_EVENT_ADD_DEF(MMAP_COUNT, "mmap count", ObStatClassIds::CACHE, 50024, true, true, false)
STAT_EVENT_ADD_DEF(MUNMAP_COUNT, "munmap count", ObStatClassIds::CACHE, 50025, true, true, false)
STAT_EVENT_ADD_DEF(MMAP_SIZE, "mmap size", ObStatClassIds::CACHE, 50026, true, true, false)
STAT_EVENT_ADD_DEF(MUNMAP_SIZE, "munmap size", ObStatClassIds::CACHE, 50027, true, true, false)
STAT_EVENT_ADD_DEF(KVCACHE_SYNC_WASH_TIME, "kvcache sync wash time", ObStatClassIds::CACHE, 50028, false, true, true)
STAT_EVENT_ADD_DEF(KVCACHE_SYNC_WASH_COUNT, "kvcache sync wash count", ObStatClassIds::CACHE, 50029, false, true, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_RPC_RENEW_FAIL, "location cache rpc renew fail count", ObStatClassIds::CACHE, 50030, false, true, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_SQL_RENEW, "location cache sql renew count", ObStatClassIds::CACHE, 50031, false, true, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_IGNORE_RPC_RENEW, "location cache ignore rpc renew count", ObStatClassIds::CACHE, 50032, false, true, true)
STAT_EVENT_ADD_DEF(FUSE_ROW_CACHE_HIT, "fuse row cache hit", ObStatClassIds::CACHE, 50033, true, true, true)
STAT_EVENT_ADD_DEF(FUSE_ROW_CACHE_MISS, "fuse row cache miss", ObStatClassIds::CACHE, 50034, true, true, true)
STAT_EVENT_ADD_DEF(SCHEMA_CACHE_HIT, "schema cache hit", ObStatClassIds::CACHE, 50035, false, true, true)
STAT_EVENT_ADD_DEF(SCHEMA_CACHE_MISS, "schema cache miss", ObStatClassIds::CACHE, 50036, false, true, true)
STAT_EVENT_ADD_DEF(TABLET_LS_CACHE_HIT, "tablet ls cache hit", ObStatClassIds::CACHE, 50037, false, true, true)
STAT_EVENT_ADD_DEF(TABLET_LS_CACHE_MISS, "tablet ls cache miss", ObStatClassIds::CACHE, 50038, false, true, true)
// obsoleted part from INDEX_CLOG_CACHE_HIT to USER_TABLE_STAT_CACHE_MISS
STAT_EVENT_ADD_DEF(INDEX_CLOG_CACHE_HIT, "index clog cache hit", ObStatClassIds::CACHE, 50039, false, true, true)
STAT_EVENT_ADD_DEF(INDEX_CLOG_CACHE_MISS, "index clog cache miss", ObStatClassIds::CACHE, 50040, false, true, true)
STAT_EVENT_ADD_DEF(USER_TAB_COL_STAT_CACHE_HIT, "user tab col stat cache hit", ObStatClassIds::CACHE, 50041, false, true, true)
STAT_EVENT_ADD_DEF(USER_TAB_COL_STAT_CACHE_MISS, "user tab col stat cache miss", ObStatClassIds::CACHE, 50042, false, true, true)
STAT_EVENT_ADD_DEF(USER_TABLE_STAT_CACHE_HIT, "user table stat cache hit", ObStatClassIds::CACHE, 50043, false, true, true)
STAT_EVENT_ADD_DEF(USER_TABLE_STAT_CACHE_MISS, "user table stat cache miss", ObStatClassIds::CACHE, 50044, false, true, true)
// obsoleted part from CLOG_CACHE_HIT to USER_TABLE_STAT_CACHE_MISS
STAT_EVENT_ADD_DEF(OPT_TABLE_STAT_CACHE_HIT, "opt table stat cache hit", ObStatClassIds::CACHE, 50045, false, true, true)
STAT_EVENT_ADD_DEF(OPT_TABLE_STAT_CACHE_MISS, "opt table stat cache miss", ObStatClassIds::CACHE, 50046, false, true, true)
STAT_EVENT_ADD_DEF(OPT_COLUMN_STAT_CACHE_HIT, "opt column stat cache hit", ObStatClassIds::CACHE, 50047, false, true, true)
STAT_EVENT_ADD_DEF(OPT_COLUMN_STAT_CACHE_MISS, "opt column stat cache miss", ObStatClassIds::CACHE, 50048, false, true, true)
STAT_EVENT_ADD_DEF(TMP_PAGE_CACHE_HIT, "tmp page cache hit", ObStatClassIds::CACHE, 50049, false, true, true)
STAT_EVENT_ADD_DEF(TMP_PAGE_CACHE_MISS, "tmp page cache miss", ObStatClassIds::CACHE, 50050, false, true, true)
STAT_EVENT_ADD_DEF(TMP_BLOCK_CACHE_HIT, "tmp block cache hit", ObStatClassIds::CACHE, 50051, false, true, true)
STAT_EVENT_ADD_DEF(TMP_BLOCK_CACHE_MISS, "tmp block cache miss", ObStatClassIds::CACHE, 50052, false, true, true)
STAT_EVENT_ADD_DEF(SECONDARY_META_CACHE_HIT, "secondary meta cache hit", ObStatClassIds::CACHE, 50053, false, true, true)
STAT_EVENT_ADD_DEF(SECONDARY_META_CACHE_MISS, "secondary meta cache miss", ObStatClassIds::CACHE, 50054, false, true, true)
STAT_EVENT_ADD_DEF(OPT_DS_STAT_CACHE_HIT, "opt ds stat cache hit", ObStatClassIds::CACHE, 50055, false, true, true)
STAT_EVENT_ADD_DEF(OPT_DS_STAT_CACHE_MISS, "opt ds stat cache miss", ObStatClassIds::CACHE, 50056, false, true, true)
STAT_EVENT_ADD_DEF(STORAGE_META_CACHE_HIT, "storage meta cache hit", ObStatClassIds::CACHE, 50057, true, true, true)
STAT_EVENT_ADD_DEF(STORAGE_META_CACHE_MISS, "storage meta cache miss", ObStatClassIds::CACHE, 50058, true, true, true)
STAT_EVENT_ADD_DEF(TABLET_CACHE_HIT, "tablet cache hit", ObStatClassIds::CACHE, 50059, true, true, true)
STAT_EVENT_ADD_DEF(TABLET_CACHE_MISS, "tablet cache miss", ObStatClassIds::CACHE, 50060, true, true, true)

STAT_EVENT_ADD_DEF(SCHEMA_HISTORY_CACHE_HIT, "schema history cache hit", ObStatClassIds::CACHE, 50061, false, true, true)
STAT_EVENT_ADD_DEF(SCHEMA_HISTORY_CACHE_MISS, "schema history cache miss", ObStatClassIds::CACHE, 50062, false, true, true)
STAT_EVENT_ADD_DEF(OPT_SYSTEM_STAT_CACHE_HIT, "opt system stat cache hit", ObStatClassIds::CACHE, 50063, false, true, true)
STAT_EVENT_ADD_DEF(OPT_SYSTEM_STAT_CACHE_MISS, "opt system stat cache miss", ObStatClassIds::CACHE, 50064, false, true, true)

STAT_EVENT_ADD_DEF(LOG_KV_CACHE_HIT, "log kv cache hit", ObStatClassIds::CACHE, 50065, false, true, true)
STAT_EVENT_ADD_DEF(LOG_KV_CACHE_MISS, "log kv cache miss", ObStatClassIds::CACHE, 50066, false, true, true)
STAT_EVENT_ADD_DEF(DATA_BLOCK_CACHE_MISS, "data block cache miss", ObStatClassIds::CACHE, 50067, true, true, true)
STAT_EVENT_ADD_DEF(INDEX_BLOCK_CACHE_MISS, "index block cache miss", ObStatClassIds::CACHE, 50068, true, true, true)
STAT_EVENT_ADD_DEF(MULTI_VERSION_FUSE_ROW_CACHE_HIT, "multi version fuse row cache hit", ObStatClassIds::CACHE, 50069, true, true, true)
STAT_EVENT_ADD_DEF(MULTI_VERSION_FUSE_ROW_CACHE_MISS, "multi version fuse row cache miss", ObStatClassIds::CACHE, 50070, true, true, true)
STAT_EVENT_ADD_DEF(BACKUP_INDEX_CACHE_HIT, "backup index cache hit", ObStatClassIds::CACHE, 50071, true, true, true)
STAT_EVENT_ADD_DEF(BACKUP_INDEX_CACHE_MISS, "backup index cache miss", ObStatClassIds::CACHE, 50072, true, true, true)
STAT_EVENT_ADD_DEF(BACKUP_META_CACHE_HIT, "backup meta cache hit", ObStatClassIds::CACHE, 50073, true, true, true)
STAT_EVENT_ADD_DEF(BACKUP_META_CACHE_MISS, "backup meta cache miss", ObStatClassIds::CACHE, 50074, true, true, true)

// STORAGE
STAT_EVENT_ADD_DEF(MEMSTORE_LOGICAL_READS, "MEMSTORE_LOGICAL_READS", STORAGE, "MEMSTORE_LOGICAL_READS", true, true, false)
STAT_EVENT_ADD_DEF(MEMSTORE_LOGICAL_BYTES, "MEMSTORE_LOGICAL_BYTES", STORAGE, "MEMSTORE_LOGICAL_BYTES", true, true, false)
STAT_EVENT_ADD_DEF(SSTABLE_LOGICAL_READS, "SSTABLE_LOGICAL_READS", STORAGE, "SSTABLE_LOGICAL_READS", true, true, false)
STAT_EVENT_ADD_DEF(SSTABLE_LOGICAL_BYTES, "SSTABLE_LOGICAL_BYTES", STORAGE, "SSTABLE_LOGICAL_BYTES", true, true, false)
STAT_EVENT_ADD_DEF(IO_READ_COUNT, "io read count", ObStatClassIds::STORAGE, 60000, true, true, true)
STAT_EVENT_ADD_DEF(IO_READ_DELAY, "io read delay", ObStatClassIds::STORAGE, 60001, true, true, true)
STAT_EVENT_ADD_DEF(IO_READ_BYTES, "io read bytes", ObStatClassIds::STORAGE, 60002, true, true, true)
STAT_EVENT_ADD_DEF(IO_WRITE_COUNT, "io write count", ObStatClassIds::STORAGE, 60003, true, true, true)
STAT_EVENT_ADD_DEF(IO_WRITE_DELAY, "io write delay", ObStatClassIds::STORAGE, 60004, true, true, true)
STAT_EVENT_ADD_DEF(IO_WRITE_BYTES, "io write bytes", ObStatClassIds::STORAGE, 60005, true, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_SCAN_COUNT, "memstore scan count", ObStatClassIds::STORAGE, 60006, true, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_SCAN_SUCC_COUNT, "memstore scan succ count", ObStatClassIds::STORAGE, 60007, true, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_SCAN_FAIL_COUNT, "memstore scan fail count", ObStatClassIds::STORAGE, 60008, true, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_GET_COUNT, "memstore get count", ObStatClassIds::STORAGE, 60009, true, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_GET_SUCC_COUNT, "memstore get succ count", ObStatClassIds::STORAGE, 60010, true, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_GET_FAIL_COUNT, "memstore get fail count", ObStatClassIds::STORAGE, 60011, true, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_APPLY_COUNT, "memstore apply count", ObStatClassIds::STORAGE, 60012, true, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_APPLY_SUCC_COUNT, "memstore apply succ count", ObStatClassIds::STORAGE, 60013, true, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_APPLY_FAIL_COUNT, "memstore apply fail count", ObStatClassIds::STORAGE, 60014, true, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_GET_TIME, "memstore get time", ObStatClassIds::STORAGE, 60016, true, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_SCAN_TIME, "memstore scan time", ObStatClassIds::STORAGE, 60017, true, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_APPLY_TIME, "memstore apply time", ObStatClassIds::STORAGE, 60018, true, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_READ_LOCK_SUCC_COUNT, "memstore read lock succ count", ObStatClassIds::STORAGE, 60019, true, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_READ_LOCK_FAIL_COUNT, "memstore read lock fail count", ObStatClassIds::STORAGE, 60020, true, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_WRITE_LOCK_SUCC_COUNT, "memstore write lock succ count", ObStatClassIds::STORAGE, 60021, true, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_WRITE_LOCK_FAIL_COUNT, "memstore write lock fail count", ObStatClassIds::STORAGE, 60022, true, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_WAIT_WRITE_LOCK_TIME, "memstore wait write lock time", ObStatClassIds::STORAGE, 60023, true, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_WAIT_READ_LOCK_TIME, "memstore wait read lock time", ObStatClassIds::STORAGE, 60024, true, true, true)
STAT_EVENT_ADD_DEF(IO_READ_MICRO_INDEX_COUNT, "io read micro index count", ObStatClassIds::STORAGE, 60025, false, true, true)
STAT_EVENT_ADD_DEF(IO_READ_MICRO_INDEX_BYTES, "io read micro index bytes", ObStatClassIds::STORAGE, 60026, false, true, true)
STAT_EVENT_ADD_DEF(IO_READ_PREFETCH_MICRO_COUNT, "io prefetch micro block count", ObStatClassIds::STORAGE, 60027, false, true, true)
STAT_EVENT_ADD_DEF(IO_READ_PREFETCH_MICRO_BYTES, "io prefetch micro block bytes", ObStatClassIds::STORAGE, 60028, false, true, true)
STAT_EVENT_ADD_DEF(IO_READ_UNCOMP_MICRO_COUNT, "io read uncompress micro block count", ObStatClassIds::STORAGE, 60029, false, true, true)
STAT_EVENT_ADD_DEF(IO_READ_UNCOMP_MICRO_BYTES, "io read uncompress micro block bytes", ObStatClassIds::STORAGE, 60030, false, true, true)
STAT_EVENT_ADD_DEF(STORAGE_READ_ROW_COUNT, "storage read row count", ObStatClassIds::STORAGE, 60031, false, true, true)
STAT_EVENT_ADD_DEF(STORAGE_DELETE_ROW_COUNT, "storage delete row count", ObStatClassIds::STORAGE, 60032, false, true, true)
STAT_EVENT_ADD_DEF(STORAGE_INSERT_ROW_COUNT, "storage insert row count", ObStatClassIds::STORAGE, 60033, false, true, true)
STAT_EVENT_ADD_DEF(STORAGE_UPDATE_ROW_COUNT, "storage update row count", ObStatClassIds::STORAGE, 60034, false, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_ROW_PURGE_COUNT, "memstore row purge count", ObStatClassIds::STORAGE, 60037, false, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_ROW_COMPACTION_COUNT, "memstore row compaction count", ObStatClassIds::STORAGE, 60038, false, true, true)
STAT_EVENT_ADD_DEF(IO_READ_QUEUE_DELAY, "io read queue delay", ObStatClassIds::STORAGE, 60039, false, true, true)
STAT_EVENT_ADD_DEF(IO_WRITE_QUEUE_DELAY, "io write queue delay", ObStatClassIds::STORAGE, 60040, false, true, true)
STAT_EVENT_ADD_DEF(IO_READ_CB_QUEUE_DELAY, "io read callback queuing delay", ObStatClassIds::STORAGE, 60041, false, true, true)
STAT_EVENT_ADD_DEF(IO_READ_CB_PROCESS_DELAY, "io read callback process delay", ObStatClassIds::STORAGE, 60042, false, true, true)
STAT_EVENT_ADD_DEF(BANDWIDTH_IN_THROTTLE, "bandwidth in throttle size", ObStatClassIds::STORAGE, 60051, false, true, true)
STAT_EVENT_ADD_DEF(BANDWIDTH_OUT_THROTTLE, "bandwidth out throttle size", ObStatClassIds::STORAGE, 60052, false, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_READ_ROW_COUNT, "memstore read row count", ObStatClassIds::STORAGE, 60056, true, true, true)
STAT_EVENT_ADD_DEF(SSSTORE_READ_ROW_COUNT, "ssstore read row count", ObStatClassIds::STORAGE, 60057, true, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_WRITE_BYTES, "memstore write bytes", ObStatClassIds::STORAGE, 60058, true, true, true)

STAT_EVENT_ADD_DEF(MEMSTORE_WRITE_LOCK_WAKENUP_COUNT, "memstore write lock wakenup count in lock_wait_mgr", ObStatClassIds::STORAGE, 60068, false, true, true)


STAT_EVENT_ADD_DEF(EXIST_ROW_EFFECT_READ, "exist row effect read", ObStatClassIds::STORAGE, 60075, false, true, true)
STAT_EVENT_ADD_DEF(EXIST_ROW_EMPTY_READ, "exist row empty read", ObStatClassIds::STORAGE, 60076, false, true, true)
STAT_EVENT_ADD_DEF(GET_ROW_EFFECT_READ, "get row effect read", ObStatClassIds::STORAGE, 60077, false, true, true)
STAT_EVENT_ADD_DEF(GET_ROW_EMPTY_READ, "get row empty read", ObStatClassIds::STORAGE, 60078, false, true, true)
STAT_EVENT_ADD_DEF(SCAN_ROW_EFFECT_READ, "scan row effect read", ObStatClassIds::STORAGE, 60079, false, true, true)
STAT_EVENT_ADD_DEF(SCAN_ROW_EMPTY_READ, "scan row empty read", ObStatClassIds::STORAGE, 60080, false, true, true)
STAT_EVENT_ADD_DEF(BANDWIDTH_IN_SLEEP_US, "bandwidth in sleep us", ObStatClassIds::STORAGE, 60081, false, true, true)
STAT_EVENT_ADD_DEF(BANDWIDTH_OUT_SLEEP_US, "bandwidth out sleep us", ObStatClassIds::STORAGE, 60082, false, true, true)

STAT_EVENT_ADD_DEF(MEMSTORE_WRITE_LOCK_WAIT_TIMEOUT_COUNT, "memstore write lock wait timeout count", ObStatClassIds::STORAGE, 60083, false, true, true)

STAT_EVENT_ADD_DEF(DATA_BLOCK_READ_CNT, "accessed data micro block count", ObStatClassIds::STORAGE, 60084, true, true, true)
STAT_EVENT_ADD_DEF(DATA_BLOCK_CACHE_HIT, "data micro block cache hit", ObStatClassIds::STORAGE, 60085, true, true, true)
STAT_EVENT_ADD_DEF(INDEX_BLOCK_READ_CNT, "accessed index micro block count", ObStatClassIds::STORAGE, 60086, true, true, true)
STAT_EVENT_ADD_DEF(INDEX_BLOCK_CACHE_HIT, "index micro block cache hit", ObStatClassIds::STORAGE, 60087, true, true, true)
STAT_EVENT_ADD_DEF(BLOCKSCAN_BLOCK_CNT, "blockscaned data micro block count", ObStatClassIds::STORAGE, 60088, true, true, true)
STAT_EVENT_ADD_DEF(BLOCKSCAN_ROW_CNT, "blockscaned row count", ObStatClassIds::STORAGE, 60089, true, true, true)
STAT_EVENT_ADD_DEF(PUSHDOWN_STORAGE_FILTER_ROW_CNT, "storage filtered row count", ObStatClassIds::STORAGE, 60090, true, true, true)
STAT_EVENT_ADD_DEF(MINOR_SSSTORE_READ_ROW_COUNT, "minor ssstore read row count", ObStatClassIds::STORAGE, 60091, true, true, true)
STAT_EVENT_ADD_DEF(MAJOR_SSSTORE_READ_ROW_COUNT, "major ssstore read row count", ObStatClassIds::STORAGE, 60092, true, true, true)
STAT_EVENT_ADD_DEF(STORAGE_WRITING_THROTTLE_TIME, "storage waiting throttle time", ObStatClassIds::STORAGE, 60093, true, true, true)
STAT_EVENT_ADD_DEF(IO_READ_DEVICE_TIME, "io read execute time", ObStatClassIds::STORAGE, 60094, true, true, true)
STAT_EVENT_ADD_DEF(IO_WRITE_DEVICE_TIME, "io write execute time", ObStatClassIds::STORAGE, 60095, true, true, true)

// backup & restore
STAT_EVENT_ADD_DEF(BACKUP_IO_READ_COUNT, "backup io read count", ObStatClassIds::STORAGE, 69000, true, true, true)
STAT_EVENT_ADD_DEF(BACKUP_IO_READ_BYTES, "backup io read bytes", ObStatClassIds::STORAGE, 69001, true, true, true)
STAT_EVENT_ADD_DEF(BACKUP_IO_WRITE_COUNT, "backup io write count", ObStatClassIds::STORAGE, 69002, true, true, true)
STAT_EVENT_ADD_DEF(BACKUP_IO_WRITE_BYTES, "backup io write bytes", ObStatClassIds::STORAGE, 69003, true, true, true)
STAT_EVENT_ADD_DEF(BACKUP_DELETE_COUNT, "backup delete count", ObStatClassIds::STORAGE, 69010, true, true, true)

STAT_EVENT_ADD_DEF(BACKUP_IO_READ_DELAY, "backup io read delay", ObStatClassIds::STORAGE, 69012, true, true, true)
STAT_EVENT_ADD_DEF(BACKUP_IO_WRITE_DELAY, "backup io write delay", ObStatClassIds::STORAGE, 69013, true, true, true)
STAT_EVENT_ADD_DEF(COS_IO_READ_DELAY, "cos io read delay", ObStatClassIds::STORAGE, 69014, true, true, true)
STAT_EVENT_ADD_DEF(COS_IO_WRITE_DELAY, "cos io write delay", ObStatClassIds::STORAGE, 69015, true, true, true)
STAT_EVENT_ADD_DEF(COS_IO_LS_DELAY, "cos io list delay", ObStatClassIds::STORAGE, 69016, true, true, true)
STAT_EVENT_ADD_DEF(BACKUP_DELETE_DELAY, "backup delete delay", ObStatClassIds::STORAGE, 69017, true, true, true)

STAT_EVENT_ADD_DEF(BACKUP_IO_LS_COUNT, "backup io list count", ObStatClassIds::STORAGE, 69019, true, true, true)
STAT_EVENT_ADD_DEF(BACKUP_IO_READ_FAIL_COUNT, "backup io read failed count", ObStatClassIds::STORAGE, 69020, true, true, true)
STAT_EVENT_ADD_DEF(BACKUP_IO_WRITE_FAIL_COUNT, "backup io write failed count", ObStatClassIds::STORAGE, 69021, true, true, true)
STAT_EVENT_ADD_DEF(BACKUP_IO_DEL_FAIL_COUNT, "backup io delete failed count", ObStatClassIds::STORAGE, 69022, true, true, true)
STAT_EVENT_ADD_DEF(BACKUP_IO_LS_FAIL_COUNT, "backup io list failed count", ObStatClassIds::STORAGE, 69023, true, true, true)

STAT_EVENT_ADD_DEF(BACKUP_TAGGING_COUNT, "backup io tagging count", ObStatClassIds::STORAGE, 69024, true, true, true)
STAT_EVENT_ADD_DEF(BACKUP_TAGGING_FAIL_COUNT, "backup io tagging failed count", ObStatClassIds::STORAGE, 69025, true, true, true)

STAT_EVENT_ADD_DEF(COS_IO_WRITE_BYTES, "cos io write bytes", ObStatClassIds::STORAGE, 69026, true, true, true)
STAT_EVENT_ADD_DEF(COS_IO_WRITE_COUNT, "cos io write count", ObStatClassIds::STORAGE, 69027, true, true, true)
STAT_EVENT_ADD_DEF(COS_DELETE_COUNT, "cos delete count", ObStatClassIds::STORAGE, 69028, true, true, true)
STAT_EVENT_ADD_DEF(COS_DELETE_DELAY, "cos delete delay", ObStatClassIds::STORAGE, 69029, true, true, true)
STAT_EVENT_ADD_DEF(COS_IO_LS_LIMIT_COUNT, "cos list io limit count", ObStatClassIds::STORAGE, 69030, true, true, true)
STAT_EVENT_ADD_DEF(COS_IO_LS_COUNT, "cos io list count", ObStatClassIds::STORAGE, 69031, true, true, true)
STAT_EVENT_ADD_DEF(COS_IO_READ_COUNT, "cos io read count", ObStatClassIds::STORAGE, 69032, true, true, true)
STAT_EVENT_ADD_DEF(COS_IO_READ_BYTES, "cos io read bytes", ObStatClassIds::STORAGE, 69033, true, true, true)

// DEBUG
STAT_EVENT_ADD_DEF(REFRESH_SCHEMA_COUNT, "refresh schema count", ObStatClassIds::DEBUG, 70000, false, true, true)
STAT_EVENT_ADD_DEF(REFRESH_SCHEMA_TIME, "refresh schema time", ObStatClassIds::DEBUG, 70001, false, true, true)
STAT_EVENT_ADD_DEF(INNER_SQL_CONNECTION_EXECUTE_COUNT, "inner sql connection execute count", ObStatClassIds::DEBUG, 70002, false, true, true)
STAT_EVENT_ADD_DEF(INNER_SQL_CONNECTION_EXECUTE_TIME, "inner sql connection execute time", ObStatClassIds::DEBUG, 70003, false, true, true)
STAT_EVENT_ADD_DEF(LS_ALL_TABLE_OPERATOR_GET_COUNT, "log stream table operator get count", ObStatClassIds::DEBUG, 70006, false, true, true)
STAT_EVENT_ADD_DEF(LS_ALL_TABLE_OPERATOR_GET_TIME, "log stream table operator get time", ObStatClassIds::DEBUG, 70007, false, true, true)

//CLOG 80001 ~ 90000
STAT_EVENT_ADD_DEF(PALF_WRITE_IO_COUNT, "palf write io count to disk",  ObStatClassIds::CLOG, 80001, true, true, true)
STAT_EVENT_ADD_DEF(PALF_WRITE_SIZE, "palf write size to disk",  ObStatClassIds::CLOG, 80002, true, true, true)
STAT_EVENT_ADD_DEF(PALF_WRITE_TIME, "palf write total time to disk", ObStatClassIds::CLOG, 80003, true, true, true)
STAT_EVENT_ADD_DEF(PALF_READ_COUNT_FROM_HOT_CACHE, "palf read count from hot cache", ObStatClassIds::CLOG, 80004, true, true, true) // FARM COMPAT WHITELIST
STAT_EVENT_ADD_DEF(PALF_READ_SIZE_FROM_HOT_CACHE, "palf read size from hot cache", ObStatClassIds::CLOG, 80005, true, true, true) // FARM COMPAT WHITELIST
STAT_EVENT_ADD_DEF(PALF_READ_TIME_FROM_HOT_CACHE, "palf read total time from hot cache", ObStatClassIds::CLOG, 80006, true, true, true) // FARM COMPAT WHITELIST
STAT_EVENT_ADD_DEF(PALF_READ_IO_COUNT_FROM_DISK, "palf read io count from disk", ObStatClassIds::CLOG, 80007, true, true, true)
STAT_EVENT_ADD_DEF(PALF_READ_SIZE_FROM_DISK, "palf read size from disk", ObStatClassIds::CLOG, 80008, true, true, true)
STAT_EVENT_ADD_DEF(PALF_READ_TIME_FROM_DISK, "palf read total time from disk", ObStatClassIds::CLOG, 80009, true, true, true)
STAT_EVENT_ADD_DEF(PALF_HANDLE_RPC_REQUEST_COUNT, "palf handle rpc request count", ObStatClassIds::CLOG, 80010, true, true, true)
STAT_EVENT_ADD_DEF(ARCHIVE_READ_LOG_SIZE, "archive read log size", ObStatClassIds::CLOG, 80011, true, true, true)
STAT_EVENT_ADD_DEF(ARCHIVE_WRITE_LOG_SIZE, "archive write log size", ObStatClassIds::CLOG, 80012, true, true, true)
STAT_EVENT_ADD_DEF(RESTORE_READ_LOG_SIZE, "restore read log size", ObStatClassIds::CLOG, 80013, true, true, true)
STAT_EVENT_ADD_DEF(RESTORE_WRITE_LOG_SIZE, "restore write log size", ObStatClassIds::CLOG, 80014, true, true, true)
STAT_EVENT_ADD_DEF(CLOG_TRANS_LOG_TOTAL_SIZE, "clog trans log total size", ObStatClassIds::CLOG, 80057, false, true, true)
STAT_EVENT_ADD_DEF(LOG_STORAGE_COMPRESS_ORIGINAL_SIZE, "log storage compress original size", ObStatClassIds::CLOG, 80058, false, true, true)
STAT_EVENT_ADD_DEF(LOG_STORAGE_COMPRESS_COMPRESSED_SIZE, "log storage compress compressed size", ObStatClassIds::CLOG, 80059, false, true, true)

// CLOG.EXTLOG 81001 ~ 90000
STAT_EVENT_ADD_DEF(CLOG_EXTLOG_FETCH_LOG_SIZE, "external log service fetch log size", ObStatClassIds::CLOG, 81001, false, true, true)
STAT_EVENT_ADD_DEF(CLOG_EXTLOG_FETCH_LOG_COUNT, "external log service fetch log count", ObStatClassIds::CLOG, 81002, false, true, true)
STAT_EVENT_ADD_DEF(CLOG_EXTLOG_FETCH_RPC_COUNT, "external log service fetch rpc count", ObStatClassIds::CLOG, 81003, false, true, true)

// CLOG.REPLAY


// CLOG.PROXY

// ELECTION

//OBSERVER

// rootservice
STAT_EVENT_ADD_DEF(RS_RPC_SUCC_COUNT, "success rpc process", ObStatClassIds::RS, 110015, false, true, true)
STAT_EVENT_ADD_DEF(RS_RPC_FAIL_COUNT, "failed rpc process", ObStatClassIds::RS, 110016, false, true, true)
STAT_EVENT_ADD_DEF(RS_BALANCER_SUCC_COUNT, "balancer succ execute count", ObStatClassIds::RS, 110017, false, true, true)
STAT_EVENT_ADD_DEF(RS_BALANCER_FAIL_COUNT, "balancer failed execute count", ObStatClassIds::RS, 110018, false, true, true)

// TABLE API (19xxxx)
// -- retrieve 1900xx
STAT_EVENT_ADD_DEF(TABLEAPI_RETRIEVE_COUNT, "single retrieve execute count", ObStatClassIds::TABLEAPI, 190001, true, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_RETRIEVE_TIME, "single retrieve execute time", ObStatClassIds::TABLEAPI, 190002, true, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_RETRIEVE_COUNT, "multi retrieve execute count", ObStatClassIds::TABLEAPI, 190003, true, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_RETRIEVE_TIME, "multi retrieve execute time", ObStatClassIds::TABLEAPI, 190004, true, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_RETRIEVE_ROW, "multi retrieve rows", ObStatClassIds::TABLEAPI, 190005, true, true, true)
// -- insert 1901xx
STAT_EVENT_ADD_DEF(TABLEAPI_INSERT_COUNT, "single insert execute count", ObStatClassIds::TABLEAPI, 190101, true, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_INSERT_TIME, "single insert execute time", ObStatClassIds::TABLEAPI, 190102, true, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_INSERT_COUNT, "multi insert execute count", ObStatClassIds::TABLEAPI, 190103, true, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_INSERT_TIME, "multi insert execute time", ObStatClassIds::TABLEAPI, 190104, true, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_INSERT_ROW, "multi insert rows", ObStatClassIds::TABLEAPI, 190105, true, true, true)
// -- delete 1902xx
STAT_EVENT_ADD_DEF(TABLEAPI_DELETE_COUNT, "single delete execute count", ObStatClassIds::TABLEAPI, 190201, true, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_DELETE_TIME, "single delete execute time", ObStatClassIds::TABLEAPI, 190202, true, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_DELETE_COUNT, "multi delete execute count", ObStatClassIds::TABLEAPI, 190203, true, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_DELETE_TIME, "multi delete execute time", ObStatClassIds::TABLEAPI, 190204, true, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_DELETE_ROW, "multi delete rows", ObStatClassIds::TABLEAPI, 190205, true, true, true)
// -- update 1903xx
STAT_EVENT_ADD_DEF(TABLEAPI_UPDATE_COUNT, "single update execute count", ObStatClassIds::TABLEAPI, 190301, true, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_UPDATE_TIME, "single update execute time", ObStatClassIds::TABLEAPI, 190302, true, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_UPDATE_COUNT, "multi update execute count", ObStatClassIds::TABLEAPI, 190303, true, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_UPDATE_TIME, "multi update execute time", ObStatClassIds::TABLEAPI, 190304, true, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_UPDATE_ROW, "multi update rows", ObStatClassIds::TABLEAPI, 190305, true, true, true)
// -- insert_or_update 1904xx
STAT_EVENT_ADD_DEF(TABLEAPI_INSERT_OR_UPDATE_COUNT, "single insert_or_update execute count", ObStatClassIds::TABLEAPI, 190401, true, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_INSERT_OR_UPDATE_TIME, "single insert_or_update execute time", ObStatClassIds::TABLEAPI, 190402, true, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_INSERT_OR_UPDATE_COUNT, "multi insert_or_update execute count", ObStatClassIds::TABLEAPI, 190403, true, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_INSERT_OR_UPDATE_TIME, "multi insert_or_update execute time", ObStatClassIds::TABLEAPI, 190404, true, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_INSERT_OR_UPDATE_ROW, "multi insert_or_update rows", ObStatClassIds::TABLEAPI, 190405, true, true, true)
// -- replace 1905xx
STAT_EVENT_ADD_DEF(TABLEAPI_REPLACE_COUNT, "single replace execute count", ObStatClassIds::TABLEAPI, 190501, true, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_REPLACE_TIME, "single replace execute time", ObStatClassIds::TABLEAPI, 190502, true, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_REPLACE_COUNT, "multi replace execute count", ObStatClassIds::TABLEAPI, 190503, true, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_REPLACE_TIME, "multi replace execute time", ObStatClassIds::TABLEAPI, 190504, true, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_REPLACE_ROW, "multi replace rows", ObStatClassIds::TABLEAPI, 190505, true, true, true)
// -- complex_batch_retrieve 1906xx
STAT_EVENT_ADD_DEF(TABLEAPI_BATCH_RETRIEVE_COUNT, "batch retrieve execute count", ObStatClassIds::TABLEAPI, 190601, true, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_BATCH_RETRIEVE_TIME, "batch retrieve execute time", ObStatClassIds::TABLEAPI, 190602, true, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_BATCH_RETRIEVE_ROW, "batch retrieve rows", ObStatClassIds::TABLEAPI, 190603, true, true, true)
// -- complex_batch_operation 1907xx
STAT_EVENT_ADD_DEF(TABLEAPI_BATCH_HYBRID_COUNT, "batch hybrid execute count", ObStatClassIds::TABLEAPI, 190701, true, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_BATCH_HYBRID_TIME, "batch hybrid execute time", ObStatClassIds::TABLEAPI, 190702, true, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_BATCH_HYBRID_INSERT_OR_UPDATE_ROW, "batch hybrid insert_or_update rows", ObStatClassIds::TABLEAPI, 190707, true, true, true)
// -- other 1908xx
STAT_EVENT_ADD_DEF(TABLEAPI_LOGIN_COUNT, "table api login count", ObStatClassIds::TABLEAPI, 190801, true, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_TSC_VIOLATE_COUNT, "table api OB_TRANSACTION_SET_VIOLATION count", ObStatClassIds::TABLEAPI, 190802, true, true, true)
// -- query 1909xx
STAT_EVENT_ADD_DEF(TABLEAPI_QUERY_COUNT, "query count", ObStatClassIds::TABLEAPI, 190901, true, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_QUERY_TIME, "query time", ObStatClassIds::TABLEAPI, 190902, true, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_QUERY_ROW, "query row count", ObStatClassIds::TABLEAPI, 190903, true, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_QUERY_AND_MUTATE_COUNT, "query_and_mutate count", ObStatClassIds::TABLEAPI, 190904, true, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_QUERY_AND_MUTATE_TIME, "query_and_mutate time", ObStatClassIds::TABLEAPI, 190905, true, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_QUERY_AND_MUTATE_ROW, "query_and_mutate row count", ObStatClassIds::TABLEAPI, 190906, true, true, true)
// -- hbase api 1910xx
STAT_EVENT_ADD_DEF(HBASEAPI_SCAN_COUNT, "hbase scan count", ObStatClassIds::TABLEAPI, 191001, true, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_SCAN_TIME, "hbase scan time", ObStatClassIds::TABLEAPI, 191002, true, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_SCAN_ROW, "hbase scan row count", ObStatClassIds::TABLEAPI, 191003, true, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_PUT_COUNT, "hbase put count", ObStatClassIds::TABLEAPI, 191004, true, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_PUT_TIME, "hbase put time", ObStatClassIds::TABLEAPI, 191005, true, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_PUT_ROW, "hbase put row count", ObStatClassIds::TABLEAPI, 191006, true, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_DELETE_COUNT, "hbase delete count", ObStatClassIds::TABLEAPI, 191007, true, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_DELETE_TIME, "hbase delete time", ObStatClassIds::TABLEAPI, 191008, true, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_DELETE_ROW, "hbase delete row count", ObStatClassIds::TABLEAPI, 191009, true, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_APPEND_COUNT, "hbase append count", ObStatClassIds::TABLEAPI, 191010, true, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_APPEND_TIME, "hbase append time", ObStatClassIds::TABLEAPI, 191011, true, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_APPEND_ROW, "hbase append row count", ObStatClassIds::TABLEAPI, 191012, true, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_INCREMENT_COUNT, "hbase increment count", ObStatClassIds::TABLEAPI, 191013, true, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_INCREMENT_TIME, "hbase increment time", ObStatClassIds::TABLEAPI, 191014, true, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_INCREMENT_ROW, "hbase increment row count", ObStatClassIds::TABLEAPI, 191015, true, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_CHECK_PUT_COUNT, "hbase check_put count", ObStatClassIds::TABLEAPI, 191016, true, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_CHECK_PUT_TIME, "hbase check_put time", ObStatClassIds::TABLEAPI, 191017, true, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_CHECK_PUT_ROW, "hbase check_put row count", ObStatClassIds::TABLEAPI, 191018, true, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_CHECK_DELETE_COUNT, "hbase check_delete count", ObStatClassIds::TABLEAPI, 191019, true, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_CHECK_DELETE_TIME, "hbase check_delete time", ObStatClassIds::TABLEAPI, 191020, true, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_CHECK_DELETE_ROW, "hbase check_delete row count", ObStatClassIds::TABLEAPI, 191021, true, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_HYBRID_COUNT, "hbase hybrid count", ObStatClassIds::TABLEAPI, 191022, true, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_HYBRID_TIME, "hbase hybrid time", ObStatClassIds::TABLEAPI, 191023, true, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_HYBRID_ROW, "hbase hybrid row count", ObStatClassIds::TABLEAPI, 191024, true, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_CHECK_MUTATE_COUNT, "hbase check_mutate count", ObStatClassIds::TABLEAPI, 191025, true, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_CHECK_MUTATE_TIME, "hbase check_mutate time", ObStatClassIds::TABLEAPI, 191026, true, true, true)
STAT_EVENT_ADD_DEF(HBASEAPI_CHECK_MUTATE_ROW, "hbase check_mutate row count", ObStatClassIds::TABLEAPI, 191027, true, true, true)

// -- table increment 1911xx
STAT_EVENT_ADD_DEF(TABLEAPI_INCREMENT_COUNT, "single increment execute count", ObStatClassIds::TABLEAPI, 191101, true, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_INCREMENT_TIME, "single increment execute time", ObStatClassIds::TABLEAPI, 191102, true, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_INCREMENT_COUNT, "multi increment execute count", ObStatClassIds::TABLEAPI, 191103, true, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_INCREMENT_TIME, "multi increment execute time", ObStatClassIds::TABLEAPI, 191104, true, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_INCREMENT_ROW, "multi increment rows", ObStatClassIds::TABLEAPI, 191105, true, true, true)

// -- table append 1912xx
STAT_EVENT_ADD_DEF(TABLEAPI_APPEND_COUNT, "single append execute count", ObStatClassIds::TABLEAPI, 191201, true, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_APPEND_TIME, "single append execute time", ObStatClassIds::TABLEAPI, 191202, true, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_APPEND_COUNT, "multi append execute count", ObStatClassIds::TABLEAPI, 191203, true, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_APPEND_TIME, "multi append execute time", ObStatClassIds::TABLEAPI, 191204, true, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_APPEND_ROW, "multi append rows", ObStatClassIds::TABLEAPI, 191205, true, true, true)

// -- table put 1913xx
STAT_EVENT_ADD_DEF(TABLEAPI_PUT_COUNT, "single put execute count", ObStatClassIds::TABLEAPI, 191301, true, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_PUT_TIME, "single put execute time", ObStatClassIds::TABLEAPI, 191302, true, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_PUT_COUNT, "multi put execute count", ObStatClassIds::TABLEAPI, 191303, true, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_PUT_TIME, "multi put execute time", ObStatClassIds::TABLEAPI, 191304, true, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_MULTI_PUT_ROW, "multi put rows", ObStatClassIds::TABLEAPI, 191305, true, true, true)

// -- table trigger 1914xx
STAT_EVENT_ADD_DEF(TABLEAPI_GROUP_TRIGGER_COUNT, "group commit trigger execute count", ObStatClassIds::TABLEAPI, 191401, true, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_GROUP_TRIGGER_TIME, "group commit trigger execute execute time", ObStatClassIds::TABLEAPI, 191402, true, true, true)
STAT_EVENT_ADD_DEF(TABLEAPI_GROUP_TRIGGER_ROW, "group commit trigger execute rows", ObStatClassIds::TABLEAPI, 191403, true, true, true)

// -- redis api 1915xx - 1917xx
STAT_EVENT_ADD_DEF(REDISAPI_LINDEX_COUNT, "redis lindex count", ObStatClassIds::REDISAPI, 191501, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_LINDEX_TIME, "redis lindex time", ObStatClassIds::REDISAPI, 191502, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_LSET_COUNT, "redis lset count", ObStatClassIds::REDISAPI, 191503, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_LSET_TIME, "redis lset time", ObStatClassIds::REDISAPI, 191504, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_LRANGE_COUNT, "redis lrange count", ObStatClassIds::REDISAPI, 191505, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_LRANGE_TIME, "redis lrange time", ObStatClassIds::REDISAPI, 191506, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_LTRIM_COUNT, "redis ltrim count", ObStatClassIds::REDISAPI, 191507, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_LTRIM_TIME, "redis ltrim time", ObStatClassIds::REDISAPI, 191508, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_LPUSH_COUNT, "redis lpush count", ObStatClassIds::REDISAPI, 191509, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_LPUSH_TIME, "redis lpush time", ObStatClassIds::REDISAPI, 191510, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_LPUSHX_COUNT, "redis lpushx count", ObStatClassIds::REDISAPI, 191511, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_LPUSHX_TIME, "redis lpushx time", ObStatClassIds::REDISAPI, 191512, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_RPUSH_COUNT, "redis rpush count", ObStatClassIds::REDISAPI, 191513, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_RPUSH_TIME, "redis rpush time", ObStatClassIds::REDISAPI, 191514, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_RPUSHX_COUNT, "redis rpushx count", ObStatClassIds::REDISAPI, 191515, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_RPUSHX_TIME, "redis rpushx time", ObStatClassIds::REDISAPI, 191516, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_LPOP_COUNT, "redis lpop count", ObStatClassIds::REDISAPI, 191517, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_LPOP_TIME, "redis lpop time", ObStatClassIds::REDISAPI, 191518, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_RPOP_COUNT, "redis rpop count", ObStatClassIds::REDISAPI, 191519, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_RPOP_TIME, "redis rpop time", ObStatClassIds::REDISAPI, 191520, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_LREM_COUNT, "redis lrem count", ObStatClassIds::REDISAPI, 191521, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_LREM_TIME, "redis lrem time", ObStatClassIds::REDISAPI, 191522, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_RPOPLPUSH_COUNT, "redis rpoplpush count", ObStatClassIds::REDISAPI, 191523, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_RPOPLPUSH_TIME, "redis rpoplpush time", ObStatClassIds::REDISAPI, 191524, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_LINSERT_COUNT, "redis linsert count", ObStatClassIds::REDISAPI, 191525, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_LINSERT_TIME, "redis linsert time", ObStatClassIds::REDISAPI, 191526, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_LLEN_COUNT, "redis llen count", ObStatClassIds::REDISAPI, 191527, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_LLEN_TIME, "redis llen time", ObStatClassIds::REDISAPI, 191528, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_SDIFF_COUNT, "redis sdiff count", ObStatClassIds::REDISAPI, 191529, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_SDIFF_TIME, "redis sdiff time", ObStatClassIds::REDISAPI, 191530, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_SDIFFSTORE_COUNT, "redis sdiffstore count", ObStatClassIds::REDISAPI, 191531, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_SDIFFSTORE_TIME, "redis sdiffstore time", ObStatClassIds::REDISAPI, 191532, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_SINTER_COUNT, "redis sinter count", ObStatClassIds::REDISAPI, 191533, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_SINTER_TIME, "redis sinter time", ObStatClassIds::REDISAPI, 191534, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_SINTERSTORE_COUNT, "redis sinterstore count", ObStatClassIds::REDISAPI, 191535, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_SINTERSTORE_TIME, "redis sinterstore time", ObStatClassIds::REDISAPI, 191536, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_SUNION_COUNT, "redis sunion count", ObStatClassIds::REDISAPI, 191537, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_SUNION_TIME, "redis sunion time", ObStatClassIds::REDISAPI, 191538, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_SUNIONSTORE_COUNT, "redis sunionstore count", ObStatClassIds::REDISAPI, 191539, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_SUNIONSTORE_TIME, "redis sunionstore time", ObStatClassIds::REDISAPI, 191540, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_SADD_COUNT, "redis sadd count", ObStatClassIds::REDISAPI, 191541, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_SADD_TIME, "redis sadd time", ObStatClassIds::REDISAPI, 191542, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_SCARD_COUNT, "redis scard count", ObStatClassIds::REDISAPI, 191543, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_SCARD_TIME, "redis scard time", ObStatClassIds::REDISAPI, 191544, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_SISMEMBER_COUNT, "redis sismember count", ObStatClassIds::REDISAPI, 191545, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_SISMEMBER_TIME, "redis sismember time", ObStatClassIds::REDISAPI, 191546, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_SMEMBERS_COUNT, "redis smembers count", ObStatClassIds::REDISAPI, 191547, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_SMEMBERS_TIME, "redis smembers time", ObStatClassIds::REDISAPI, 191548, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_SMOVE_COUNT, "redis smove count", ObStatClassIds::REDISAPI, 191549, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_SMOVE_TIME, "redis smove time", ObStatClassIds::REDISAPI, 191550, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_SPOP_COUNT, "redis spop count", ObStatClassIds::REDISAPI, 191551, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_SPOP_TIME, "redis spop time", ObStatClassIds::REDISAPI, 191552, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_SRANDMEMBER_COUNT, "redis srandmember count", ObStatClassIds::REDISAPI, 191553, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_SRANDMEMBER_TIME, "redis srandmember time", ObStatClassIds::REDISAPI, 191554, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_SREM_COUNT, "redis srem count", ObStatClassIds::REDISAPI, 191555, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_SREM_TIME, "redis srem time", ObStatClassIds::REDISAPI, 191556, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_ZADD_COUNT, "redis zadd count", ObStatClassIds::REDISAPI, 191557, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_ZADD_TIME, "redis zadd time", ObStatClassIds::REDISAPI, 191558, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_ZCARD_COUNT, "redis zcard count", ObStatClassIds::REDISAPI, 191559, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_ZCARD_TIME, "redis zcard time", ObStatClassIds::REDISAPI, 191560, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_ZREM_COUNT, "redis zrem count", ObStatClassIds::REDISAPI, 191561, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_ZREM_TIME, "redis zrem time", ObStatClassIds::REDISAPI, 191562, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_ZINCRBY_COUNT, "redis zincrby count", ObStatClassIds::REDISAPI, 191563, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_ZINCRBY_TIME, "redis zincrby time", ObStatClassIds::REDISAPI, 191564, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_ZSCORE_COUNT, "redis zscore count", ObStatClassIds::REDISAPI, 191565, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_ZSCORE_TIME, "redis zscore time", ObStatClassIds::REDISAPI, 191566, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_ZRANK_COUNT, "redis zrank count", ObStatClassIds::REDISAPI, 191567, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_ZRANK_TIME, "redis zrank time", ObStatClassIds::REDISAPI, 191568, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_ZREVRANK_COUNT, "redis zrevrank count", ObStatClassIds::REDISAPI, 191569, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_ZREVRANK_TIME, "redis zrevrank time", ObStatClassIds::REDISAPI, 191570, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_ZRANGE_COUNT, "redis zrange count", ObStatClassIds::REDISAPI, 191571, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_ZRANGE_TIME, "redis zrange time", ObStatClassIds::REDISAPI, 191572, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_ZREVRANGE_COUNT, "redis zrevrange count", ObStatClassIds::REDISAPI, 191573, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_ZREVRANGE_TIME, "redis zrevrange time", ObStatClassIds::REDISAPI, 191574, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_ZREMRANGEBYRANK_COUNT, "redis zremrangebyrank count", ObStatClassIds::REDISAPI, 191575, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_ZREMRANGEBYRANK_TIME, "redis zremrangebyrank time", ObStatClassIds::REDISAPI, 191576, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_ZCOUNT_COUNT, "redis zcount count", ObStatClassIds::REDISAPI, 191577, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_ZCOUNT_TIME, "redis zcount time", ObStatClassIds::REDISAPI, 191578, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_ZRANGEBYSCORE_COUNT, "redis zrangebyscore count", ObStatClassIds::REDISAPI, 191579, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_ZRANGEBYSCORE_TIME, "redis zrangebyscore time", ObStatClassIds::REDISAPI, 191580, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_ZREVRANGEBYSCORE_COUNT, "redis zrevrangebyscore count", ObStatClassIds::REDISAPI, 191581, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_ZREVRANGEBYSCORE_TIME, "redis zrevrangebyscore time", ObStatClassIds::REDISAPI, 191582, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_ZREMRANGEBYSCORE_COUNT, "redis zremrangebyscore count", ObStatClassIds::REDISAPI, 191583, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_ZREMRANGEBYSCORE_TIME, "redis zremrangebyscore time", ObStatClassIds::REDISAPI, 191584, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_ZINTERSTORE_COUNT, "redis zinterstore count", ObStatClassIds::REDISAPI, 191585, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_ZINTERSTORE_TIME, "redis zinterstore time", ObStatClassIds::REDISAPI, 191586, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_ZUNIONSTORE_COUNT, "redis zunionstore count", ObStatClassIds::REDISAPI, 191587, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_ZUNIONSTORE_TIME, "redis zunionstore time", ObStatClassIds::REDISAPI, 191588, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_HSET_COUNT, "redis hset count", ObStatClassIds::REDISAPI, 191589, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_HSET_TIME, "redis hset time", ObStatClassIds::REDISAPI, 191590, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_HMSET_COUNT, "redis hmset count", ObStatClassIds::REDISAPI, 191591, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_HMSET_TIME, "redis hmset time", ObStatClassIds::REDISAPI, 191592, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_HSETNX_COUNT, "redis hsetnx count", ObStatClassIds::REDISAPI, 191593, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_HSETNX_TIME, "redis hsetnx time", ObStatClassIds::REDISAPI, 191594, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_HGET_COUNT, "redis hget count", ObStatClassIds::REDISAPI, 191595, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_HGET_TIME, "redis hget time", ObStatClassIds::REDISAPI, 191596, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_HMGET_COUNT, "redis hmget count", ObStatClassIds::REDISAPI, 191597, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_HMGET_TIME, "redis hmget time", ObStatClassIds::REDISAPI, 191598, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_HGETALL_COUNT, "redis hgetall count", ObStatClassIds::REDISAPI, 191599, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_HGETALL_TIME, "redis hgetall time", ObStatClassIds::REDISAPI, 191600, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_HVALS_COUNT, "redis hvals count", ObStatClassIds::REDISAPI, 191601, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_HVALS_TIME, "redis hvals time", ObStatClassIds::REDISAPI, 191602, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_HKEYS_COUNT, "redis hkeys count", ObStatClassIds::REDISAPI, 191603, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_HKEYS_TIME, "redis hkeys time", ObStatClassIds::REDISAPI, 191604, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_HEXISTS_COUNT, "redis hexists count", ObStatClassIds::REDISAPI, 191605, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_HEXISTS_TIME, "redis hexists time", ObStatClassIds::REDISAPI, 191606, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_HDEL_COUNT, "redis hdel count", ObStatClassIds::REDISAPI, 191607, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_HDEL_TIME, "redis hdel time", ObStatClassIds::REDISAPI, 191608, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_HINCRBY_COUNT, "redis hincrby count", ObStatClassIds::REDISAPI, 191609, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_HINCRBY_TIME, "redis hincrby time", ObStatClassIds::REDISAPI, 191610, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_HINCRBYFLOAT_COUNT, "redis hincrbyfloat count", ObStatClassIds::REDISAPI, 191611, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_HINCRBYFLOAT_TIME, "redis hincrbyfloat time", ObStatClassIds::REDISAPI, 191612, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_HLEN_COUNT, "redis hlen count", ObStatClassIds::REDISAPI, 191613, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_HLEN_TIME, "redis hlen time", ObStatClassIds::REDISAPI, 191614, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_GETSET_COUNT, "redis getset count", ObStatClassIds::REDISAPI, 191615, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_GETSET_TIME, "redis getset time", ObStatClassIds::REDISAPI, 191616, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_SETBIT_COUNT, "redis setbit count", ObStatClassIds::REDISAPI, 191617, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_SETBIT_TIME, "redis setbit time", ObStatClassIds::REDISAPI, 191618, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_INCR_COUNT, "redis incr count", ObStatClassIds::REDISAPI, 191619, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_INCR_TIME, "redis incr time", ObStatClassIds::REDISAPI, 191620, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_INCRBY_COUNT, "redis incrby count", ObStatClassIds::REDISAPI, 191621, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_INCRBY_TIME, "redis incrby time", ObStatClassIds::REDISAPI, 191622, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_DECR_COUNT, "redis decr count", ObStatClassIds::REDISAPI, 191623, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_DECR_TIME, "redis decr time", ObStatClassIds::REDISAPI, 191624, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_DECRBY_COUNT, "redis decrby count", ObStatClassIds::REDISAPI, 191625, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_DECRBY_TIME, "redis decrby time", ObStatClassIds::REDISAPI, 191626, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_APPEND_COUNT, "redis append count", ObStatClassIds::REDISAPI, 191627, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_APPEND_TIME, "redis append time", ObStatClassIds::REDISAPI, 191628, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_BITCOUNT_COUNT, "redis bitcount count", ObStatClassIds::REDISAPI, 191629, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_BITCOUNT_TIME, "redis bitcount time", ObStatClassIds::REDISAPI, 191630, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_GET_COUNT, "redis get count", ObStatClassIds::REDISAPI, 191631, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_GET_TIME, "redis get time", ObStatClassIds::REDISAPI, 191632, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_GETBIT_COUNT, "redis getbit count", ObStatClassIds::REDISAPI, 191633, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_GETBIT_TIME, "redis getbit time", ObStatClassIds::REDISAPI, 191634, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_GETRANGE_COUNT, "redis getrange count", ObStatClassIds::REDISAPI, 191635, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_GETRANGE_TIME, "redis getrange time", ObStatClassIds::REDISAPI, 191636, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_INCRBYFLOAT_COUNT, "redis incrbyfloat count", ObStatClassIds::REDISAPI, 191637, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_INCRBYFLOAT_TIME, "redis incrbyfloat time", ObStatClassIds::REDISAPI, 191638, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_MGET_COUNT, "redis mget count", ObStatClassIds::REDISAPI, 191639, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_MGET_TIME, "redis mget time", ObStatClassIds::REDISAPI, 191640, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_MSET_COUNT, "redis mset count", ObStatClassIds::REDISAPI, 191641, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_MSET_TIME, "redis mset time", ObStatClassIds::REDISAPI, 191642, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_SET_COUNT, "redis set count", ObStatClassIds::REDISAPI, 191643, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_SET_TIME, "redis set time", ObStatClassIds::REDISAPI, 191644, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_PSETEX_COUNT, "redis psetex count", ObStatClassIds::REDISAPI, 191645, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_PSETEX_TIME, "redis psetex time", ObStatClassIds::REDISAPI, 191646, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_SETEX_COUNT, "redis setex count", ObStatClassIds::REDISAPI, 191647, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_SETEX_TIME, "redis setex time", ObStatClassIds::REDISAPI, 191648, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_SETNX_COUNT, "redis setnx count", ObStatClassIds::REDISAPI, 191649, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_SETNX_TIME, "redis setnx time", ObStatClassIds::REDISAPI, 191650, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_SETRANGE_COUNT, "redis setrange count", ObStatClassIds::REDISAPI, 191651, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_SETRANGE_TIME, "redis setrange time", ObStatClassIds::REDISAPI, 191652, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_STRLEN_COUNT, "redis strlen count", ObStatClassIds::REDISAPI, 191653, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_STRLEN_TIME, "redis strlen time", ObStatClassIds::REDISAPI, 191654, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_TTL_COUNT, "redis ttl count", ObStatClassIds::REDISAPI, 191655, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_TTL_TIME, "redis ttl time", ObStatClassIds::REDISAPI, 191656, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_PTTL_COUNT, "redis pttl count", ObStatClassIds::REDISAPI, 191657, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_PTTL_TIME, "redis pttl time", ObStatClassIds::REDISAPI, 191658, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_EXPIRE_COUNT, "redis expire count", ObStatClassIds::REDISAPI, 191659, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_EXPIRE_TIME, "redis expire time", ObStatClassIds::REDISAPI, 191660, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_PEXPIRE_COUNT, "redis pexpire count", ObStatClassIds::REDISAPI, 191661, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_PEXPIRE_TIME, "redis pexpire time", ObStatClassIds::REDISAPI, 191662, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_EXPIREAT_COUNT, "redis expireat count", ObStatClassIds::REDISAPI, 191663, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_EXPIREAT_TIME, "redis expireat time", ObStatClassIds::REDISAPI, 191664, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_PEXPIREAT_COUNT, "redis pexpireat count", ObStatClassIds::REDISAPI, 191665, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_PEXPIREAT_TIME, "redis pexpireat time", ObStatClassIds::REDISAPI, 191666, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_DEL_COUNT, "redis del count", ObStatClassIds::REDISAPI, 191667, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_DEL_TIME, "redis del time", ObStatClassIds::REDISAPI, 191668, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_EXISTS_COUNT, "redis exists count", ObStatClassIds::REDISAPI, 191669, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_EXISTS_TIME, "redis exists time", ObStatClassIds::REDISAPI, 191670, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_TYPE_COUNT, "redis type count", ObStatClassIds::REDISAPI, 191671, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_TYPE_TIME, "redis type time", ObStatClassIds::REDISAPI, 191672, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_PERSIST_COUNT, "redis persist count", ObStatClassIds::REDISAPI, 191673, true, true, false)
STAT_EVENT_ADD_DEF(REDISAPI_PERSIST_TIME, "redis persist time", ObStatClassIds::REDISAPI, 191674, true, true, false)

// sys_time_model related (20xxxx)
STAT_EVENT_ADD_DEF(SYS_TIME_MODEL_DB_TIME, "DB time", ObStatClassIds::SYS, 200001, false, true, true)
STAT_EVENT_ADD_DEF(SYS_TIME_MODEL_DB_CPU, "DB CPU", ObStatClassIds::SYS, 200002, false, true, true)
STAT_EVENT_ADD_DEF(SYS_TIME_MODEL_DB_INNER_TIME, "DB inner sql time", ObStatClassIds::SYS, 200003, false, true, false)
STAT_EVENT_ADD_DEF(SYS_TIME_MODEL_DB_INNER_CPU, "DB inner sql CPU", ObStatClassIds::SYS, 200004, false, true, false)
STAT_EVENT_ADD_DEF(SYS_TIME_MODEL_BKGD_TIME, "background elapsed time", ObStatClassIds::SYS, 200005, false, true, true)
STAT_EVENT_ADD_DEF(SYS_TIME_MODEL_BKGD_CPU, "background cpu time", ObStatClassIds::SYS, 200006, false, true, true)
STAT_EVENT_ADD_DEF(SYS_TIME_MODEL_BACKUP_CPU, "(backup/restore) cpu time", ObStatClassIds::SYS, 200007, false, true, false)
STAT_EVENT_ADD_DEF(SYS_TIME_MODEL_ENCRYPT_TIME, "Tablespace encryption elapsed time", ObStatClassIds::SYS, 200008, false, true, false)
STAT_EVENT_ADD_DEF(SYS_TIME_MODEL_ENCRYPT_CPU, "Tablespace encryption cpu time", ObStatClassIds::SYS, 200009, false, true, false)
STAT_EVENT_ADD_DEF(SYS_TIME_MODEL_NON_IDLE_WAIT_TIME, "non idle wait time", ObStatClassIds::SYS, 200010, true, true, true)
STAT_EVENT_ADD_DEF(SYS_TIME_MODEL_IDLE_WAIT_TIME, "idle wait time", ObStatClassIds::SYS, 200011, true, true, true)
STAT_EVENT_ADD_DEF(SYS_TIME_MODEL_BKGD_DB_TIME, "background database time", ObStatClassIds::SYS, 200012, false, true, true)
STAT_EVENT_ADD_DEF(SYS_TIME_MODEL_BKGD_NON_IDLE_WAIT_TIME, "background database non-idle wait time", ObStatClassIds::SYS, 200013, false, true, true)
STAT_EVENT_ADD_DEF(SYS_TIME_MODEL_BKGD_IDLE_WAIT_TIME, "background database idle wait time", ObStatClassIds::SYS, 200014, false, true, true)
STAT_EVENT_ADD_DEF(DIAGNOSTIC_INFO_ALLOC_COUNT, "diagnostic info object allocated count", ObStatClassIds::SYS, 200015, false, true, false)
STAT_EVENT_ADD_DEF(DIAGNOSTIC_INFO_ALLOC_FAIL_COUNT, "diagnostic info object allocate failure count", ObStatClassIds::SYS, 200016, false, true, false)
STAT_EVENT_ADD_DEF(DIAGNOSTIC_INFO_RETURN_COUNT, "diagnostic info object returned count", ObStatClassIds::SYS, 200017, false, true, false)
STAT_EVENT_ADD_DEF(DIAGNOSTIC_INFO_RETURN_FAIL_COUNT, "diagnostic info object return failure count", ObStatClassIds::SYS, 200018, false, true, false)

// wr and ash related  (2100xx)
STAT_EVENT_ADD_DEF(WR_SNAPSHOT_ELAPSE_TIME, "wr snapshot task elapse time", ObStatClassIds::WR, 210001, false, true, true)
STAT_EVENT_ADD_DEF(WR_SNAPSHOT_CPU_TIME, "wr snapshot task cpu time", ObStatClassIds::WR, 210002, false, true, true)
STAT_EVENT_ADD_DEF(WR_PURGE_ELAPSE_TIME, "wr purge task elapse time", ObStatClassIds::WR, 210003, false, true, true)
STAT_EVENT_ADD_DEF(WR_PURGE_CPU_TIME, "wr purge task cpu time", ObStatClassIds::WR, 210004, false, true, true)
STAT_EVENT_ADD_DEF(WR_SCHEDULAR_ELAPSE_TIME, "wr schedular elapse time", ObStatClassIds::WR, 210005, false, true, true)
STAT_EVENT_ADD_DEF(WR_SCHEDULAR_CPU_TIME, "wr schedular cpu time", ObStatClassIds::WR, 210006, false, true, true)
STAT_EVENT_ADD_DEF(WR_USER_SUBMIT_SNAPSHOT_ELAPSE_TIME, "wr user submit snapshot elapse time", ObStatClassIds::WR, 210007, false, true, true)
STAT_EVENT_ADD_DEF(WR_USER_SUBMIT_SNAPSHOT_CPU_TIME, "wr user submit snapshot cpu time", ObStatClassIds::WR, 210008, false, true, true)
STAT_EVENT_ADD_DEF(WR_COLLECTED_ASH_ROW_COUNT, "wr collected active session history row count", ObStatClassIds::WR, 210009, false, true, true)
STAT_EVENT_ADD_DEF(ASH_SCHEDULAR_ELAPSE_TIME, "ash schedular elapse time", ObStatClassIds::WR, 210010, false, true, true)
STAT_EVENT_ADD_DEF(ASH_SCHEDULAR_CPU_TIME, "ash schedular cpu time", ObStatClassIds::WR, 210011, false, true, true)

// sqlstat relat (2200xx)
STAT_EVENT_ADD_DEF(CCWAIT_TIME, "concurrency wait total time", ObStatClassIds::SYS, 220001, true, true, true)
STAT_EVENT_ADD_DEF(USER_IO_WAIT_TIME, "user io wait total time", ObStatClassIds::SYS, 220002, true, true, true)
STAT_EVENT_ADD_DEF(APWAIT_TIME, "application wait total time", ObStatClassIds::SYS, 220003, true, true, true)
STAT_EVENT_ADD_DEF(SCHEDULE_WAIT_TIME, "schedule wait total time", ObStatClassIds::SYS, 220004, true, true, true)
STAT_EVENT_ADD_DEF(NETWORK_WAIT_TIME, "network wait total time", ObStatClassIds::SYS, 220005, true, true, true)

// shared-storage local_cache(2400xx)
STAT_EVENT_ADD_DEF(SS_MICRO_CACHE_HIT, "ss_micro_cache hit count", ObStatClassIds::CACHE, 240001, true, true, true)
STAT_EVENT_ADD_DEF(SS_MICRO_CACHE_MISS, "ss_micro_cache miss count", ObStatClassIds::CACHE, 240002, true, true, true)
STAT_EVENT_ADD_DEF(SS_MICRO_CACHE_FAIL_ADD, "ss_micro_cache failure count of adding cache", ObStatClassIds::CACHE, 240003, true, true, true)
STAT_EVENT_ADD_DEF(SS_MICRO_CACHE_FAIL_GET, "ss_micro_cache failure count of getting cache", ObStatClassIds::CACHE, 240004, true, true, true)
STAT_EVENT_ADD_DEF(SS_MICRO_CACHE_MC_PREWARM, "ss_micro_cache major_compaction prewarm count", ObStatClassIds::CACHE, 240005, true, true, true)
STAT_EVENT_ADD_DEF(SS_MICRO_CACHE_HA_PREWARM, "ss_micro_cache migrate prewarm count", ObStatClassIds::CACHE, 240006, true, true, true)
STAT_EVENT_ADD_DEF(SS_MICRO_CACHE_REPLICA_PREWARM, "ss_micro_cache replica prewarm count", ObStatClassIds::CACHE, 240007, true, true, true)
STAT_EVENT_ADD_DEF(SS_MICRO_CACHE_DDL_PREWARM, "ss_micro_cache ddl prewarm count", ObStatClassIds::CACHE, 240008, true, true, true)
STAT_EVENT_ADD_DEF(SS_MICRO_CACHE_HOLD_COUNT, "ss_micro_cache total hold micro_block count", ObStatClassIds::CACHE, 240009, true, true, true)
STAT_EVENT_ADD_DEF(SS_MICRO_CACHE_HOLD_SIZE, "ss_micro_cache total hold micro_block size", ObStatClassIds::CACHE, 240010, true, true, true)
STAT_EVENT_ADD_DEF(SS_MICRO_CACHE_HIT_BYTES, "ss_micro_cache hit total bytes", ObStatClassIds::CACHE, 240011, true, true, true)
STAT_EVENT_ADD_DEF(SS_MICRO_CACHE_MISS_BYTES, "ss_micro_cache miss total bytes", ObStatClassIds::CACHE, 240012, true, true, true)
STAT_EVENT_ADD_DEF(SS_MICRO_CACHE_MC_PREWARM_BYTES, "ss_micro_cache major_compaction prewarm bytes", ObStatClassIds::CACHE, 240013, true, true, true)
STAT_EVENT_ADD_DEF(SS_MICRO_CACHE_HA_PREWARM_BYTES, "ss_micro_cache migrate prewarm bytes", ObStatClassIds::CACHE, 240014, true, true, true)
STAT_EVENT_ADD_DEF(SS_MICRO_CACHE_REPLICA_PREWARM_BYTES, "ss_micro_cache replica prewarm bytes", ObStatClassIds::CACHE, 240015, true, true, true)
STAT_EVENT_ADD_DEF(SS_MICRO_CACHE_DDL_PREWARM_BYTES, "ss_micro_cache ddl prewarm bytes", ObStatClassIds::CACHE, 240016, true, true, true)
STAT_EVENT_ADD_DEF(SS_TMPFILE_CACHE_HIT, "ss_tmpfile_cache hit count", ObStatClassIds::CACHE, 240017, true, true, true)
STAT_EVENT_ADD_DEF(SS_TMPFILE_CACHE_MISS, "ss_tmpfile_cache miss count", ObStatClassIds::CACHE, 240018, true, true, true)
STAT_EVENT_ADD_DEF(SS_MICRO_CACHE_USED_DISK_SIZE, "ss_micro_cache total used disk size", ObStatClassIds::CACHE, 240019, true, true, true)
STAT_EVENT_ADD_DEF(SS_MAJOR_MACRO_CACHE_HIT, "ss_major_macro_cache hit count", ObStatClassIds::CACHE, 240020, true, true, true)
STAT_EVENT_ADD_DEF(SS_MAJOR_MACRO_CACHE_MISS, "ss_major_macro_cache miss count", ObStatClassIds::CACHE, 240021, true, true, true)

STAT_EVENT_ADD_DEF(OBJECT_STORAGE_IO_HEAD_COUNT, "object storage io head count", ObStatClassIds::STORAGE, 240022, true, true, true)
STAT_EVENT_ADD_DEF(OBJECT_STORAGE_IO_HEAD_FAIL_COUNT, "object storage io head fail count", ObStatClassIds::STORAGE, 240023, true, true, true)

//end
STAT_EVENT_ADD_DEF(STAT_EVENT_ADD_END, "event add end", ObStatClassIds::DEBUG, 1, false, false, true)

#endif

#ifdef STAT_EVENT_SET_DEF
// NETWORK
STAT_EVENT_SET_DEF(STANDBY_FETCH_LOG_BYTES, "standby fetch log bytes", ObStatClassIds::NETWORK, 110000, false, true, true)
STAT_EVENT_SET_DEF(STANDBY_FETCH_LOG_BANDWIDTH_LIMIT, "standby fetch log bandwidth limit", ObStatClassIds::NETWORK, 110001, false, true, true)

// QUEUE

// TRANS

// SQL

// CACHE
STAT_EVENT_SET_DEF(LOCATION_CACHE_SIZE, "location cache size", ObStatClassIds::CACHE, 120000, false, true, false)
STAT_EVENT_SET_DEF(TABLET_LS_CACHE_SIZE, "tablet ls cache size", ObStatClassIds::CACHE, 120001, false, true, true)
STAT_EVENT_SET_DEF(OPT_TAB_STAT_CACHE_SIZE, "table stat cache size", ObStatClassIds::CACHE, 120002, false, true, true)
STAT_EVENT_SET_DEF(OPT_TAB_COL_STAT_CACHE_SIZE, "table col stat cache size", ObStatClassIds::CACHE, 120003, false, true, true)
STAT_EVENT_SET_DEF(INDEX_BLOCK_CACHE_SIZE, "index block cache size", ObStatClassIds::CACHE, 120004, false, true, true)
STAT_EVENT_SET_DEF(SYS_BLOCK_CACHE_SIZE, "sys block cache size", ObStatClassIds::CACHE, 120005, false, true, false)
STAT_EVENT_SET_DEF(USER_BLOCK_CACHE_SIZE, "user block cache size", ObStatClassIds::CACHE, 120006, false, true, true)
STAT_EVENT_SET_DEF(USER_ROW_CACHE_SIZE, "user row cache size", ObStatClassIds::CACHE, 120008, false, true, true)
STAT_EVENT_SET_DEF(BLOOM_FILTER_CACHE_SIZE, "bloom filter cache size", ObStatClassIds::CACHE, 120009, false, true, true)
STAT_EVENT_SET_DEF(LOG_KV_CACHE_SIZE, "log kv cache size", ObStatClassIds::CACHE, 120010, false, true, true)

// STORAGE
STAT_EVENT_SET_DEF(ACTIVE_MEMSTORE_USED, "active memstore used", ObStatClassIds::STORAGE, 130000, false, true, true)
STAT_EVENT_SET_DEF(TOTAL_MEMSTORE_USED, "total memstore used", ObStatClassIds::STORAGE, 130001, false, true, true)
STAT_EVENT_SET_DEF(MAJOR_FREEZE_TRIGGER, "major freeze trigger", ObStatClassIds::STORAGE, 130002, false, true, true)
STAT_EVENT_SET_DEF(MEMSTORE_LIMIT, "memstore limit", ObStatClassIds::STORAGE, 130004, false, true, true)

// RESOURCE
STAT_EVENT_SET_DEF(SRV_DISK_SIZE, "SRV_DISK_SIZE", RESOURCE, "SRV_DISK_SIZE", false, true, false)
STAT_EVENT_SET_DEF(SRV_MEMORY_SIZE, "SRV_MEMORY_SIZE", RESOURCE, "SRV_MEMORY_SIZE", false, true, false)
STAT_EVENT_SET_DEF(MIN_MEMORY_SIZE, "min memory size", ObStatClassIds::RESOURCE, 140001, false, true, true)
STAT_EVENT_SET_DEF(MAX_MEMORY_SIZE, "max memory size", ObStatClassIds::RESOURCE, 140002, false, true, true)
STAT_EVENT_SET_DEF(MEMORY_USAGE, "memory usage", ObStatClassIds::RESOURCE, 140003, false, true, true)
STAT_EVENT_SET_DEF(SRV_CPUS, "SRV_CPUS", RESOURCE, "SRV_CPUS", false, true, false)
STAT_EVENT_SET_DEF(MIN_CPUS, "min cpus", ObStatClassIds::RESOURCE, 140004, false, true, true)
STAT_EVENT_SET_DEF(MAX_CPUS, "max cpus", ObStatClassIds::RESOURCE, 140005, false, true, true)
STAT_EVENT_SET_DEF(CPU_USAGE, "cpu usage", ObStatClassIds::RESOURCE, 140006, false, true, true)
STAT_EVENT_SET_DEF(DISK_USAGE, "disk usage", ObStatClassIds::RESOURCE, 140007, false, true, true)
STAT_EVENT_SET_DEF(MEMORY_USED_SIZE, "observer memory used size", ObStatClassIds::RESOURCE, 140008, false, true, true)
STAT_EVENT_SET_DEF(MEMORY_FREE_SIZE, "observer memory free size", ObStatClassIds::RESOURCE, 140009, false, true, true)
STAT_EVENT_SET_DEF(IS_MINI_MODE, "is mini mode", ObStatClassIds::RESOURCE, 140010, false, true, true)
STAT_EVENT_SET_DEF(MEMORY_HOLD_SIZE, "observer memory hold size", ObStatClassIds::RESOURCE, 140011, false, true, true)
STAT_EVENT_SET_DEF(WORKER_TIME, "worker time", ObStatClassIds::RESOURCE, 140012, false, true, true)
STAT_EVENT_SET_DEF(CPU_TIME, "cpu time", ObStatClassIds::RESOURCE, 140013, false, true, true)
STAT_EVENT_SET_DEF(MEMORY_LIMIT, "effective observer memory limit", ObStatClassIds::RESOURCE, 140014, false, true, true)
STAT_EVENT_SET_DEF(SYSTEM_MEMORY, "effective system memory", ObStatClassIds::RESOURCE, 140015, false, true, true)
STAT_EVENT_SET_DEF(HIDDEN_SYS_MEMORY, "effective hidden sys memory", ObStatClassIds::RESOURCE, 140016, false, true, true)
STAT_EVENT_SET_DEF(MAX_SESSION_NUM, "max session num", ObStatClassIds::RESOURCE, 140017, false, true, true)
STAT_EVENT_SET_DEF(KV_CACHE_HOLD, "kvcache hold", ObStatClassIds::RESOURCE, 140018, false, true, true)
STAT_EVENT_SET_DEF(UNMANAGED_MEMORY_SIZE, "unmanaged memory size", ObStatClassIds::RESOURCE, 140019, false, true, true)

//CLOG

// DEBUG
STAT_EVENT_SET_DEF(OBLOGGER_WRITE_SIZE, "oblogger log bytes", ObStatClassIds::DEBUG, 160001, false, true, true)
STAT_EVENT_SET_DEF(ELECTION_WRITE_SIZE, "election log bytes", ObStatClassIds::DEBUG, 160002, false, true, true)
STAT_EVENT_SET_DEF(ROOTSERVICE_WRITE_SIZE, "rootservice log bytes", ObStatClassIds::DEBUG, 160003, false, true, true)
STAT_EVENT_SET_DEF(OBLOGGER_TOTAL_WRITE_COUNT, "oblogger total log count", ObStatClassIds::DEBUG, 160004, false, true, true)
STAT_EVENT_SET_DEF(ELECTION_TOTAL_WRITE_COUNT, "election total log count", ObStatClassIds::DEBUG, 160005, false, true, true)
STAT_EVENT_SET_DEF(ROOTSERVICE_TOTAL_WRITE_COUNT, "rootservice total log count", ObStatClassIds::DEBUG, 160006, false, true, true)
STAT_EVENT_SET_DEF(ASYNC_ERROR_LOG_DROPPED_COUNT, "async error log dropped count", ObStatClassIds::DEBUG, 160019, false, true, true)
STAT_EVENT_SET_DEF(ASYNC_WARN_LOG_DROPPED_COUNT, "async warn log dropped count", ObStatClassIds::DEBUG, 160020, false, true, true)
STAT_EVENT_SET_DEF(ASYNC_INFO_LOG_DROPPED_COUNT, "async info log dropped count", ObStatClassIds::DEBUG, 160021, false, true, true)
STAT_EVENT_SET_DEF(ASYNC_TRACE_LOG_DROPPED_COUNT, "async trace log dropped count", ObStatClassIds::DEBUG, 160022, false, true, true)
STAT_EVENT_SET_DEF(ASYNC_DEBUG_LOG_DROPPED_COUNT, "async debug log dropped count", ObStatClassIds::DEBUG, 160023, false, true, true)
STAT_EVENT_SET_DEF(ASYNC_LOG_FLUSH_SPEED, "async log flush speed", ObStatClassIds::DEBUG, 160024, false, true, true)
STAT_EVENT_SET_DEF(ASYNC_GENERIC_LOG_WRITE_COUNT, "async generic log write count", ObStatClassIds::DEBUG, 160025, false, true, true)
STAT_EVENT_SET_DEF(ASYNC_USER_REQUEST_LOG_WRITE_COUNT, "async user request log write count", ObStatClassIds::DEBUG, 160026, false, true, true)
STAT_EVENT_SET_DEF(ASYNC_DATA_MAINTAIN_LOG_WRITE_COUNT, "async data maintain log write count", ObStatClassIds::DEBUG, 160027, false, true, true)
STAT_EVENT_SET_DEF(ASYNC_ROOT_SERVICE_LOG_WRITE_COUNT, "async root service log write count", ObStatClassIds::DEBUG, 160028, false, true, true)
STAT_EVENT_SET_DEF(ASYNC_SCHEMA_LOG_WRITE_COUNT, "async schema log write count", ObStatClassIds::DEBUG, 160029, false, true, true)
STAT_EVENT_SET_DEF(ASYNC_FORCE_ALLOW_LOG_WRITE_COUNT, "async force allow log write count", ObStatClassIds::DEBUG, 160030, false, true, true)
STAT_EVENT_SET_DEF(ASYNC_GENERIC_LOG_DROPPED_COUNT, "async generic log dropped count", ObStatClassIds::DEBUG, 160031, false, true, true)
STAT_EVENT_SET_DEF(ASYNC_USER_REQUEST_LOG_DROPPED_COUNT, "async user request log dropped count", ObStatClassIds::DEBUG, 160032, false, true, true)
STAT_EVENT_SET_DEF(ASYNC_DATA_MAINTAIN_LOG_DROPPED_COUNT, "async data maintain log dropped count", ObStatClassIds::DEBUG, 160033, false, true, true)
STAT_EVENT_SET_DEF(ASYNC_ROOT_SERVICE_LOG_DROPPED_COUNT, "async root service log dropped count", ObStatClassIds::DEBUG, 160034, false, true, true)
STAT_EVENT_SET_DEF(ASYNC_SCHEMA_LOG_DROPPED_COUNT, "async schema log dropped count", ObStatClassIds::DEBUG, 160035, false, true, true)
STAT_EVENT_SET_DEF(ASYNC_FORCE_ALLOW_LOG_DROPPED_COUNT, "async force allow log dropped count", ObStatClassIds::DEBUG, 160036, false, true, true)


//OBSERVER
STAT_EVENT_SET_DEF(OBSERVER_PARTITION_TABLE_UPATER_USER_QUEUE_SIZE, "observer partition table updater user table queue size", ObStatClassIds::OBSERVER, 170001, false, true, true)
STAT_EVENT_SET_DEF(OBSERVER_PARTITION_TABLE_UPATER_SYS_QUEUE_SIZE, "observer partition table updater system table queue size", ObStatClassIds::OBSERVER, 170002, false, true, true)
STAT_EVENT_SET_DEF(OBSERVER_PARTITION_TABLE_UPATER_CORE_QUEUE_SIZE, "observer partition table updater core table queue size", ObStatClassIds::OBSERVER, 170003, false, true, true)

// rootservice
STAT_EVENT_SET_DEF(RS_START_SERVICE_TIME, "rootservice start time", ObStatClassIds::RS, 180001, false, true, false)

// das
STAT_EVENT_SET_DEF(DAS_PARALLEL_TENANT_MEMORY_USAGE, "the memory use of all DAS parallel task", ObStatClassIds::SQL, 230001, false, true, true)
STAT_EVENT_SET_DEF(DAS_PARALLEL_TENANT_TASK_CNT, "the count of DAS parallel task", ObStatClassIds::SQL, 230002, false, true, true)

// shared-storage local_cache(start from 245001)
STAT_EVENT_SET_DEF(SS_MICRO_CACHE_USED_MEM_SIZE, "ss_micro_cache micro_meta used memory size", ObStatClassIds::CACHE, 245001, false, true, true)
STAT_EVENT_SET_DEF(SS_MICRO_CACHE_ALLOC_DISK_SIZE, "ss_micro_cache total alloc disk size", ObStatClassIds::CACHE, 245002, false, true, true)
STAT_EVENT_SET_DEF(SS_LOCAL_CACHE_TMPFILE_ALLOC_SIZE, "ss_local_cache tmpfile alloc disk size", ObStatClassIds::CACHE, 245003, false, true, true)
STAT_EVENT_SET_DEF(SS_LOCAL_CACHE_META_ALLOC_SIZE, "ss_local_cache meta file alloc disk size", ObStatClassIds::CACHE, 245004, false, true, true)
STAT_EVENT_SET_DEF(SS_LOCAL_CACHE_INCREMENTAL_DATA_ALLOC_SIZE, "ss_local_cache incremental data alloc disk size", ObStatClassIds::CACHE, 245005, false, true, true)
STAT_EVENT_SET_DEF(SS_LOCAL_CACHE_TMPFILE_USED_DISK_SIZE_W, "ss_local_cache tmpfile write cache used disk size", ObStatClassIds::CACHE, 245006, false, true, true)
STAT_EVENT_SET_DEF(SS_LOCAL_CACHE_TMPFILE_USED_DISK_SIZE_R, "ss_local_cache tmpfile read cache used disk size", ObStatClassIds::CACHE, 245007, false, true, true)
STAT_EVENT_SET_DEF(SS_LOCAL_CACHE_META_USED_DISK_SIZE, "ss_local_cache meta file used disk size", ObStatClassIds::CACHE, 245008, false, true, true)
STAT_EVENT_SET_DEF(SS_LOCAL_CACHE_INCREMENTAL_DATA_USED_DISK_SIZE, "ss_local_cache incremental data used disk size", ObStatClassIds::CACHE, 245009, false, true, true)
STAT_EVENT_SET_DEF(SS_LOCAL_CACHE_MAJOR_MACRO_USED_DISK_SIZE, "ss_local_cache major macro used disk size", ObStatClassIds::CACHE, 245010, false, true, true)


// END
STAT_EVENT_SET_DEF(STAT_EVENT_SET_END, "event set end", ObStatClassIds::DEBUG, 300000, false, false, true)
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
#define STAT_DEF_true(def, name, stat_class, stat_id, summary_in_session, can_visible) def,
#define STAT_DEF_false(def, name, stat_class, stat_id, summary_in_session, can_visible)

#define STAT_EVENT_ADD_DEF(def, name, stat_class, stat_id, summary_in_session, can_visible, enable)\
STAT_DEF_##enable(def, name, stat_class, stat_id, summary_in_session, can_visible)
#include "lib/statistic_event/ob_stat_event.h"
#undef STAT_EVENT_ADD_DEF
#define STAT_EVENT_SET_DEF(def, name, stat_class, stat_id, summary_in_session, can_visible, enable)\
STAT_DEF_##enable(def, name, stat_class, stat_id, summary_in_session, can_visible)
#include "lib/statistic_event/ob_stat_event.h"
#undef STAT_EVENT_SET_DEF
#undef STAT_DEF_true
#undef STAT_DEF_false
  };
};

struct ObStatEventAddStat
{
  //value for a stat
  int64_t stat_value_;
  ObStatEventAddStat();
  int add(const ObStatEventAddStat &other);
  int add(int64_t value);
  int atomic_add(int64_t value);
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
