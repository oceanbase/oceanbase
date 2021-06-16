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

#ifndef OCEANBASE_COMMON_OB_DEFINE_H_
#define OCEANBASE_COMMON_OB_DEFINE_H_
// common system headers
#include <stdint.h>  // for int64_t etc.
#include <sys/syscall.h>
#include <unistd.h>
#include <netinet/in.h>
// basic headers, do not add other headers here
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase {
namespace common {
const int64_t OB_ALL_SERVER_CNT = INT64_MAX;
const uint16_t OB_COMPACT_COLUMN_INVALID_ID = UINT16_MAX;
const int64_t OB_INVALID_TIMESTAMP = -1;
const uint64_t OB_INVALID_ID = UINT64_MAX;
const int64_t OB_INVALID_DISK_ID = -1;
const int32_t OB_INVALID_FD = -1;
const int64_t OB_LATEST_VERSION = 0;
const uint32_t OB_INVALID_FILE_ID = UINT32_MAX;
const int64_t OB_INVALID_DATA_FILE_ID = -1;
const int64_t OB_VIRTUAL_DATA_FILE_ID = 0;
const uint64_t OB_INVALID_ARCHIVE_FILE_ID = UINT64_MAX;
const int64_t NO_PARTITION_ID_FLAG = -2;
const int16_t OB_COMPACT_INVALID_INDEX = -1;
const int OB_INVALID_INDEX = -1;
const int64_t OB_INVALID_INDEX_INT64 = -1;
const int OB_INVALID_SIZE = -1;
const int OB_INVALID_COUNT = -1;
const int OB_INVALID_PTHREAD_KEY = -1;
const int64_t OB_INVALID_VERSION = -1;
const int64_t OB_INVALID_STMT_ID = -1;
const int64_t OB_INVALID_PARTITION_ID = 65535;
const int64_t OB_MIN_CLUSTER_ID = 1;
const int64_t OB_MAX_CLUSTER_ID = 4294901759;
const int64_t OB_INVALID_CLUSTER_ID = -1;
const int64_t OB_INVALID_ORG_CLUSTER_ID = 0;
const int64_t OB_MAX_ITERATOR = 16;
const int64_t MAX_IP_ADDR_LENGTH = INET6_ADDRSTRLEN;
const int64_t MAX_IP_PORT_LENGTH = MAX_IP_ADDR_LENGTH + 6;
const int64_t MAX_IP_PORT_SQL_LENGTH = MAX_IP_ADDR_LENGTH + 12;
const int64_t OB_MAX_SQL_ID_LENGTH = 32;
const int64_t MAX_ZONE_LENGTH = 128;
const int64_t MAX_REGION_LENGTH = 128;
const int64_t MAX_GTS_NAME_LENGTH = 128;
const int32_t MAX_ZONE_NUM = 64;
const int64_t SINGLE_ZONE_DEPLOYMENT_TENANT_SYS_QUORUM = 3;
const int64_t MAX_OPERATOR_NAME_LENGTH = 32;
const int64_t MAX_LONG_OPS_NAME_LENGTH = 128;
const int64_t MAX_LONG_OPS_TARGET_LENGTH = 128;
const int64_t MAX_LONG_OPS_MESSAGE_LENGTH = 500;
const int64_t MAX_ZONE_LIST_LENGTH = MAX_ZONE_LENGTH * MAX_ZONE_NUM;
const int64_t MAX_ZONE_STATUS_LENGTH = 16;
const int64_t MAX_REPLICA_STATUS_LENGTH = 64;
const int64_t MAX_TENANT_STATUS_LENGTH = 64;
const int64_t MAX_RESOURCE_POOL_NAME_LEN = 128;
const int32_t MAX_REPLICA_COUNT_PER_ZONE = 5;
const int32_t MAX_REPLICA_COUNT_TOTAL = MAX_ZONE_NUM * MAX_REPLICA_COUNT_PER_ZONE;
const int64_t MAX_RESOURCE_POOL_LENGTH = 128;
const int64_t MAX_RESOURCE_POOL_COUNT_OF_TENANT = 16;
const int64_t MAX_RESOURCE_POOL_LIST_LENGTH = MAX_RESOURCE_POOL_LENGTH * MAX_RESOURCE_POOL_COUNT_OF_TENANT;
const int64_t MAX_UNIT_CONFIG_LENGTH = 128;
const int64_t MAX_UNIT_STATUS_LENGTH = 128;
const int64_t MAX_PATH_SIZE = 1024;
const int64_t MAX_DISK_ALIAS_NAME = 128;
const int64_t MAX_DISKGROUP_NAME = 128;
const int64_t DEFAULT_BUF_LENGTH = 4096;
const int64_t MAX_MEMBER_LIST_LENGTH = MAX_ZONE_NUM * (MAX_IP_PORT_LENGTH + 17 /* timestamp length*/ + 1);
const int64_t OB_MAX_MEMBER_NUMBER = 7;
const int64_t OB_MAX_CHILD_MEMBER_NUMBER = 15;
const int64_t OB_DEFAULT_MEMBER_NUMBER = 3;
const int64_t MAX_VALUE_LENGTH = 4096;
const int64_t MAX_LLC_BITMAP_LENGTH = 4096;
const int64_t MAX_ROOTSERVICE_EVENT_NAME_LENGTH = 256;
const int64_t MAX_ROOTSERVICE_EVENT_VALUE_LENGTH = 256;
const int64_t MAX_ROOTSERVICE_EVENT_DESC_LENGTH = 64;
const int64_t MAX_ROOTSERVICE_EVENT_EXTRA_INFO_LENGTH = 512;
const int64_t MAX_ELECTION_EVENT_DESC_LENGTH = 64;
const int64_t MAX_ELECTION_EVENT_EXTRA_INFO_LENGTH = 512;
const int64_t MAX_BUFFER_SIZE = 1024 * 1024;
const int64_t MAX_MULTI_GET_CACHE_AWARE_ROW_NUM = 100;
const int64_t OB_DEFAULT_TABLET_SIZE = (1 << 27);
const int64_t OB_DEFAULT_PCTFREE = 10;
const int64_t OB_MAX_PCTFREE = 50;
const int64_t OB_MAX_PCTUSED = 99;
const int64_t OB_MAX_TRANS = 255;
const int64_t OB_MAX_DISK_TYPE_LENGTH = 32;
const int64_t OB_MAX_IO_BENCH_RESULT_LENGTH = 1024;
const int64_t OB_MAX_LOCAL_VARIABLE_SIZE = 8L << 10;
const char* const OB_EMPTY_STR = "";
const char* const OB_MAX_USED_FILE_ID_PREFIX = "max_used_file_id";
const uint64_t SEARRAY_INIT_NUM = 4;
const int64_t OB_DEFAULT_TABLE_DOP = 1;

typedef int64_t ObDateTime;
typedef int64_t ObPreciseDateTime;
typedef ObPreciseDateTime ObModifyTime;
typedef ObPreciseDateTime ObCreateTime;
typedef uint64_t ObPsStmtId;

const int32_t NOT_CHECK_FLAG = 0;
const int64_t MAX_SERVER_COUNT = 1024;
const uint64_t OB_SERVER_USER_ID = 0;
const int64_t OB_MAX_INDEX_PER_TABLE = 128;
const int64_t OB_MAX_SSTABLE_PER_TABLE = OB_MAX_INDEX_PER_TABLE + 1;
const int64_t OB_MAX_SQL_LENGTH = 64 * 1024;
const int64_t OB_SHORT_SQL_LENGTH = 1 * 1024;                  // 1KB
const int64_t OB_MEDIUM_SQL_LENGTH = 2 * OB_SHORT_SQL_LENGTH;  // 2KB
const int64_t OB_MAX_PROXY_SQL_STORE_LENGTH = 8 * 1024;        // 8KB
const int64_t OB_MAX_SERVER_ADDR_SIZE = 128;
const int64_t OB_MAX_JOIN_INFO_NUMBER = 10;
const int64_t OB_MAX_USER_ROW_KEY_LENGTH = 16 * 1024L;  // 16K
const int64_t OB_MAX_ROW_KEY_LENGTH = 17 * 1024L;       // 1K for extra varchar columns of root table
const int64_t OB_MAX_RANGE_LENGTH = 2 * OB_MAX_ROW_KEY_LENGTH;
const int64_t OB_MAX_ROW_KEY_SPLIT = 32;
const int64_t OB_USER_MAX_ROWKEY_COLUMN_NUMBER = 64;
const int64_t OB_MAX_ROWKEY_COLUMN_NUMBER = 2 * OB_USER_MAX_ROWKEY_COLUMN_NUMBER;
const int64_t OB_MAX_VIEW_COLUMN_NAME_LENGTH_MYSQL = 64;
const int64_t OB_MAX_COLUMN_NAME_LENGTH = 128;  // Compatible with oracle, OB code logic is greater than Times TODO:
const int64_t OB_MAX_COLUMN_NAME_BUF_LENGTH = OB_MAX_COLUMN_NAME_LENGTH + 1;
const int64_t OB_MAX_COLUMN_NAMES_LENGTH = 2 * 1024;
const int64_t OB_MAX_APP_NAME_LENGTH = 128;
const int64_t OB_MAX_OPERATOR_PROPERTY_LENGTH = 4 * 1024;
const int64_t OB_MAX_DATA_SOURCE_NAME_LENGTH = 128;
const int64_t OB_TRIGGER_TYPE_LENGTH = 32;
const int64_t OB_MAX_YUNTI_USER_LENGTH = 128;
const int64_t OB_MAX_YUNTI_GROUP_LENGTH = 128;
const int64_t OB_MAX_INSTANCE_NAME_LENGTH = 128;
const int64_t OB_MAX_HOST_NAME_LENGTH = 128;
const int64_t OB_MAX_HOST_NUM = 128;
const int64_t OB_MAX_MS_TYPE_LENGTH = 10;
const int64_t OB_DEFAULT_MAX_PARALLEL_COUNT = 32;
const int64_t OB_RPC_SCAN_DEFAULT_MEM_LIMIT = 1024 * 1024 * 512;
const int64_t OB_RPC_SCAN_MIN_MEM_LIMIT = 2 * 1024 * 1024;
const int64_t OB_MAX_DEBUG_MSG_LEN = 1024;
const int64_t OB_MAX_COMPRESSOR_NAME_LENGTH = 128;
const int64_t OB_MAX_HEADER_COMPRESSOR_NAME_LENGTH = 96;
const int64_t OB_MAX_STORE_FORMAT_NAME_LENGTH = 128;
const int64_t OB_MAX_PARSER_NAME_LENGTH = 128;
const int64_t OB_MAX_SUBQUERY_LAYER_NUM = 64;
const int64_t OB_MAX_SET_STMT_SIZE = 256;
const int64_t OB_MAX_WINDOW_FUNCTION_NUM = 32;
const uint64_t OB_DEFAULT_GROUP_CONCAT_MAX_LEN = 1024;
const uint64_t OB_DEFAULT_GROUP_CONCAT_MAX_LEN_FOR_ORACLE =
    32767;  // Same as OB_MAX_ORACLE_VARCHAR_LENGTH, expanded to 32767
const int64_t OB_DEFAULT_OB_INTERM_RESULT_MEM_LIMIT = 2L * 1024L * 1024L * 1024L;
// The maximum table name length that the user can specify
const int64_t OB_MAX_USER_TABLE_NAME_LENGTH_MYSQL =
    64;  // Compatible with mysql, the OB code logic is greater than the time error
const int64_t OB_MAX_USER_TABLE_NAME_LENGTH_ORACLE =
    128;  // Compatible with Oracle, error is reported when the logic is greater than
// The actual maximum table name length of table_schema (the index table will have an additional prefix, so the actual
// length is greater than OB_MAX_USER_TABLE_NAME_LENGTH)
const int64_t OB_MAX_TABLE_NAME_LENGTH = 256;
const int64_t OB_MAX_TABLE_NAME_BUF_LENGTH = OB_MAX_TABLE_NAME_LENGTH + 1;
const int64_t OB_MAX_PLAN_EXPLAIN_NAME_LENGTH = 256;
const int64_t OB_MAX_TABLE_TYPE_LENGTH = 64;
const int64_t OB_MAX_INFOSCHEMA_TABLE_NAME_LENGTH = 64;
const int64_t OB_MAX_FILE_NAME_LENGTH = MAX_PATH_SIZE;
const int64_t OB_MAX_TENANT_NAME_LENGTH = 64;
const int64_t OB_MAX_TENANT_NAME_LENGTH_STORE = 128;
const int64_t OB_MAX_TENANT_INFO_LENGTH = 4096;
const int64_t OB_MAX_PART_COLUMNS = 16;
const int64_t OB_MAX_PARTITION_NAME_LENGTH = 64;
const int64_t OB_MAX_PARTITION_DESCRIPTION_LENGTH = 1024;
const int64_t OB_MAX_PARTITION_COMMENT_LENGTH = 1024;
const int64_t OB_MAX_PARTITION_METHOD_LENGTH = 18;
const int64_t OB_MAX_PARTITION_EXPR_LENGTH = 4096;
const int64_t OB_MAX_B_PARTITION_EXPR_LENGTH = 8192;
const int64_t OB_MAX_B_HIGH_BOUND_VAL_LENGTH = OB_MAX_B_PARTITION_EXPR_LENGTH;
const int64_t OB_MAX_NODEGROUP_LENGTH = 12;
// change from 128 to 64, according to production definition document
const int64_t OB_MAX_RESERVED_POINT_TYPE_LENGTH = 32;
const int64_t OB_MAX_RESERVED_POINT_NAME_LENGTH = 128;

// for recybin
const int64_t OB_MAX_OBJECT_NAME_LENGTH = 128;    // should include index_name
const int64_t OB_MAX_ORIGINAL_NANE_LENGTH = 256;  // max length of tenant_name, table_name, db_name

const int64_t OB_MAX_CHAR_LEN = 3;
const int64_t OB_MAX_TRIGGER_NAME_LENGTH = 128;        // Compatible with Oracle
const int64_t OB_MAX_WHEN_CONDITION_LENGTH = 4000;     // Compatible with Oracle
const int64_t OB_MAX_UPDATE_COLUMNS_LENGTH = 4000;     // Compatible with Oracle
const int64_t OB_MAX_TRIGGER_BODY_LENGTH = 64 * 1024;  // In Oracle, it is the LONG type, but there is a problem with
                                                       // the large object type used in the OB internal table.
const int64_t OB_MAX_DBLINK_NAME_LENGTH = 128;         // Compatible with Oracle
const int64_t OB_MAX_QB_NAME_LENGTH = 20;  // Compatible with Oracle, hint specifies the length of the maximum qb_name.
const int64_t OB_MAX_SEQUENCE_NAME_LENGTH =
    128;  // Compatible with Oracle, error is reported when the logic is greater than
const int64_t OB_MAX_KEYSTORE_NAME_LENGTH = 128;
const int64_t OB_MAX_DATABASE_NAME_LENGTH =
    128;  // Not compatible with mysql (mysql is 64), the logic is greater than when an error is reported
const int64_t OB_MAX_DATABASE_NAME_BUF_LENGTH = OB_MAX_DATABASE_NAME_LENGTH + 1;
const int64_t OB_MAX_TABLEGROUP_NAME_LENGTH =
    128;  // OB code logic is greater than or equal to an error, so modify it to 65
const int64_t OB_MAX_ALIAS_NAME_LENGTH =
    255;  // Compatible with mysql, 255 visible characters. Plus 256 bytes at the end of 0
const int64_t OB_MAX_CONSTRAINT_NAME_LENGTH =
    128;  // Compatible with Oracle, error is reported when the logic is greater than
const int64_t OB_MAX_CONSTRAINT_EXPR_LENGTH = 2048;
const int64_t OB_MAX_TABLESPACE_NAME_LENGTH = 128;
const int64_t OB_FIRST_PARTTITION_ID = 0;
const int64_t OB_MAX_UDF_NAME_LENGTH = 64;
const int64_t OB_MAX_DL_NAME_LENGTH = 128;
const int64_t OB_MAX_USER_NAME_LENGTH =
    64;  // Not compatible with Oracle (Oracle is 128), the logic is greater than when an error is reported
const int64_t OB_MAX_USER_NAME_BUF_LENGTH = OB_MAX_USER_NAME_LENGTH + 1;
const int64_t OB_MAX_USER_NAME_LENGTH_STORE = 128;
const int64_t OB_MAX_INFOSCHEMA_GRANTEE_LEN = 81;
const int64_t OB_MAX_USER_INFO_LENGTH = 4096;
const int64_t OB_MAX_COMMAND_LENGTH = 4096;
const int64_t OB_MAX_SESSION_STATE_LENGTH = 128;
const int64_t OB_MAX_SESSION_INFO_LENGTH = 128;
const int64_t OB_MAX_VERSION_LENGTH = 256;
const int64_t COLUMN_CHECKSUM_LENGTH = 8 * 1024;
const int64_t OB_MAX_SYS_PARAM_INFO_LENGTH = 1024;
const int64_t OB_MAX_FUNC_EXPR_LENGTH = 128;
const int64_t OB_MAX_CACHE_NAME_LENGTH = 127;
const int64_t OB_MAX_WAIT_EVENT_NAME_LENGTH = 64;
const int64_t OB_MAX_WAIT_EVENT_PARAM_LENGTH = 64;
const int64_t OB_MAX_TWO_OPERATOR_EXPR_LENGTH = 256;
const int64_t OB_MAX_OPERATOR_NAME_LENGTH = 128;
const int64_t OB_MAX_SECTION_NAME_LENGTH = 128;
const int64_t OB_MAX_FLAG_NAME_LENGTH = 128;
const int64_t OB_MAX_FLAG_VALUE_LENGTH = 512;
const int64_t OB_MAX_TOKEN_BUFFER_LENGTH = 80;
const int64_t OB_MAX_PACKET_LENGTH = 1 << 26;                       // max packet length, 64MB
const int64_t OB_MAX_PACKET_BUFFER_LENGTH = (1 << 26) - (1 << 20);  // buffer length for max packet, 63MB
const int64_t OB_MAX_ROW_NUMBER_PER_QUERY = 65536;
const int64_t OB_MAX_BATCH_NUMBER = 100;
const int64_t OB_MAX_TABLET_LIST_NUMBER = 64;
const int64_t OB_MAX_DISK_NUMBER = 32;
const int64_t OB_MAX_TIME_STR_LENGTH = 64;
const int64_t OB_IP_STR_BUFF = MAX_IP_ADDR_LENGTH;  // TODO:   uniform IP/PORR length
const int64_t OB_IP_PORT_STR_BUFF = 64;
const int64_t OB_RANGE_STR_BUFSIZ = 512;
const int64_t OB_MAX_FETCH_CMD_LENGTH = 2048;
const int64_t OB_MAX_EXPIRE_INFO_STRING_LENGTH = 4096;
const int64_t OB_MAX_PART_FUNC_EXPR_LENGTH = 4096;
const int64_t OB_MAX_PART_FUNC_BIN_EXPR_LENGTH = 2 * OB_MAX_PART_FUNC_EXPR_LENGTH;
const int64_t OB_MAX_PART_FUNC_BIN_EXPR_STRING_LENGTH = 2 * OB_MAX_PART_FUNC_BIN_EXPR_LENGTH + 1;
const int64_t OB_MAX_CALCULATOR_SERIALIZE_LENGTH = 8 * 1024;  // 8k
const int64_t OB_MAX_THREAD_AIO_BUFFER_MGR_COUNT = 32;
const int64_t OB_MAX_GET_ROW_NUMBER = 10240;
const uint64_t OB_FULL_ROW_COLUMN_ID = 0;
const uint64_t OB_DELETE_ROW_COLUMN_ID = 0;
const int64_t OB_DIRECT_IO_ALIGN_BITS = 9;
const int64_t OB_DIRECT_IO_ALIGN = 1 << OB_DIRECT_IO_ALIGN_BITS;
const int64_t OB_MAX_COMPOSITE_SYMBOL_COUNT = 256;
const int64_t OB_SERVER_STATUS_LENGTH = 64;
const int64_t OB_SERVER_VERSION_LENGTH = 256;
const int64_t OB_CLUSTER_VERSION_LENGTH = OB_SERVER_VERSION_LENGTH;  // xx.xx.xx
const int64_t OB_SERVER_TYPE_LENGTH = 64;
const int64_t OB_MAX_HOSTNAME_LENGTH = 60;
const int64_t OB_MAX_USERNAME_LENGTH = 32;
const int64_t OB_MAX_PASSWORD_LENGTH = 128;
// After each sha1 is 41 characters, the incremental backup is up to 64 times, and the maximum password required for
// recovery is 64*(41+1)=2,688
const int64_t OB_MAX_PASSWORD_ARRAY_LENGTH = 4096;
const int64_t OB_MAX_ERROR_MSG_LEN = 512;
const int64_t OB_MAX_RESULT_MESSAGE_LENGTH = 1024;
const int64_t OB_MAX_DEFINER_LENGTH = OB_MAX_USER_NAME_LENGTH_STORE + OB_MAX_HOST_NAME_LENGTH + 1;  // user@host
const int64_t OB_MAX_SECURITY_TYPE_LENGTH = 7;  // definer or invoker
const int64_t OB_MAX_READ_ONLY_STATE_LENGTH = 16;
// At present, the log module reads and writes the buffer using OB_MAX_LOG_BUFFER_SIZE, the length of the transaction
// submitted to the log module is required to be less than the length of the log module can read and write the log,
// minus the length of the log header, the BLOCK header and the EOF, here is defined a length minus 1024B
const int64_t OB_MAX_LOG_ALLOWED_SIZE = 1965056L;  // OB_MAX_LOG_BUFFER_SIZE - 1024B
const int64_t OB_MAX_LOG_BUFFER_SIZE = 1966080L;   // 1.875MB
const int64_t OB_MAX_TRIGGER_VCHAR_PARAM_LENGTH = 128;
const int64_t OB_TRIGGER_MSG_LENGTH =
    3 * MAX_IP_ADDR_LENGTH + OB_TRIGGER_TYPE_LENGTH + 3 * OB_MAX_TRIGGER_VCHAR_PARAM_LENGTH;
const int64_t OB_MAX_TRACE_ID_BUFFER_SIZE = 64;
const int64_t OB_MAX_TRACE_INFO_BUFFER_SIZE = (1 << 12);
const int64_t OB_MAX_TRANS_ID_BUFFER_SIZE = 512;
const int32_t OB_MIN_SAFE_COPY_COUNT = 3;
const int32_t OB_SAFE_COPY_COUNT = 3;
const int32_t OB_DEFAULT_REPLICA_NUM = 3;
const int32_t OB_DEC_AND_LOCK = 2626;                                /* used by remote_plan in ObPsStore */
const int32_t OB_MAX_SCHEMA_VERSION_INTERVAL = 40 * 1000 * 1000;     // 40s
const int64_t UPDATE_SCHEMA_ADDITIONAL_INTERVAL = 5 * 1000 * 1000L;  // 5s

const int32_t OB_MAX_SUB_GET_REQUEST_NUM = 256;
const int32_t OB_DEFAULT_MAX_GET_ROWS_PER_SUBREQ = 20;

const int64_t OB_MPI_MAX_PARTITION_NUM = 128;
const int64_t OB_MPI_MAX_TASK_NUM = 256;

const int64_t OB_MAX_TABLE_NUM_PER_STMT = 256;
const int64_t OB_TMP_BUF_SIZE_256 = 256;
const int64_t OB_SCHEMA_MGR_MAX_USED_TID_MAP_BUCKET_NUM = 64;
const int64_t OB_MAX_SLAVE_READ_DELAY_TS = 5 * 1000 * 1000;
const int64_t OB_SKIP_RANGE_LIMIT = 256;

// plan cache
const int64_t OB_PC_NOT_PARAM_COUNT = 8;
const int64_t OB_PC_SPECIAL_PARAM_COUNT = 16;
const int64_t OB_PC_RAW_PARAM_COUNT = 128;
const int64_t OB_PLAN_CACHE_BUCKET_NUMBER = 49157;  // calculated by cal_next_prime()
const int64_t OB_PLAN_CACHE_PERCENTAGE = 5;
const int64_t OB_PLAN_CACHE_EVICT_HIGH_PERCENTAGE = 90;
const int64_t OB_PLAN_CACHE_EVICT_LOW_PERCENTAGE = 50;
const int64_t OB_PC_WEIGHT_NUMERATOR = 1000000000;

// schedule info
static const int64_t OB_MAX_SCHED_INFO_LENGTH = 16 * 1024L;  // Scheduling information

// time zone info
const int64_t OB_MAX_TZ_ABBR_LEN = 32;  // according to statistics
const int64_t OB_MAX_TZ_NAME_LEN = 64;  // according to statistics
const int64_t OB_INVALID_TZ_ID = -1;
const int64_t OB_INVALID_TZ_TRAN_TIME = INT64_MIN;
const int64_t OB_MAX_SNAPSHOT_DELAY_TIME = 5 * 1000 * 1000;  // Maximum time delay for machine reading
// OceanBase Log Synchronization Type
const int64_t OB_LOG_NOSYNC = 0;
const int64_t OB_LOG_SYNC = 1;
const int64_t OB_LOG_DELAYED_SYNC = 2;
const int64_t OB_LOG_NOT_PERSISTENT = 4;

const int64_t OB_MAX_UPS_LEASE_DURATION_US = INT64_MAX;

const int64_t OB_EXECABLE = 1;
const int64_t OB_WRITEABLE = 2;
const int64_t OB_READABLE = 4;
const int64_t OB_SCHEMA_START_VERSION = 100;
const int64_t OB_SYS_PARAM_ROW_KEY_LENGTH = 192;
const int64_t OB_MAX_SYS_PARAM_NAME_LENGTH = 128;
const int64_t OB_MAX_SYS_PARAM_VALUE_LENGTH = 1024;
const int64_t OB_MAX_SYS_PARAM_NUM = 500;
const int64_t OB_MAX_PREPARE_STMT_NUM_PER_SESSION = 512;
const int64_t OB_MAX_VAR_NUM_PER_SESSION = 1024;
// The maximum time set by the user through hint/set session.ob_query_timeout/set session.ob_tx_timeout is 102 years
// The purpose of this is to avoid that when the user enters a value that is too large, adding the current timestamp
// causes the MAX_INT64 to overflow
const int64_t OB_MAX_USER_SPECIFIED_TIMEOUT = 102L * 365L * 24L * 60L * 60L * 1000L * 1000L;
const int64_t OB_MAX_PROCESS_TIMEOUT = 5L * 60L * 1000L * 1000L;              // 5m
const int64_t OB_DEFAULT_SESSION_TIMEOUT = 100L * 1000L * 1000L;              // 10s
const int64_t OB_DEFAULT_STMT_TIMEOUT = 30L * 1000L * 1000L;                  // 30s
const int64_t OB_DEFAULT_INTERNAL_TABLE_QUERY_TIMEOUT = 10L * 1000L * 1000L;  // 10s
const int64_t OB_DEFAULT_STREAM_WAIT_TIMEOUT = 10L * 1000L * 1000L;           // 10s
const int64_t OB_DEFAULT_STREAM_RESERVE_TIME = 2L * 1000L * 1000L;            // 2s
const int64_t OB_DEFAULT_JOIN_BATCH_COUNT = 10000;
const int64_t OB_AIO_TIMEOUT_US = 5L * 1000L * 1000L;  // 5s
const int64_t OB_DEFAULT_TENANT_COUNT = 100000;        // 10w
const int64_t OB_ONLY_SYS_TENANT_COUNT = 2;
const int64_t OB_MAX_SERVER_SESSION_CNT = 32767;
const int64_t OB_MAX_SERVER_TENANT_CNT = 1000;
const int64_t OB_RECYCLE_MACRO_BLOCK_DURATION = 10 * 60 * 1000 * 1000LL;      // 10 minutes
const int64_t OB_MINOR_FREEZE_TEAM_UP_INTERVAL = 2 * 60 * 60 * 1000 * 1000L;  // 2 hours
const int64_t OB_MAX_PARTITION_NUM_PER_SERVER = 500000;                       // Version 3.1 is limited to 50w
const int64_t OB_MINI_MODE_MAX_PARTITION_NUM_PER_SERVER = 10000;
const int64_t OB_MAX_PG_NUM_PER_SERVER =
    10000;  // The total number of PG single servers 2.x version is first limited to 1w
const int64_t OB_MAX_SA_PARTITION_NUM_PER_SERVER =
    500000;  // stand alone partition single server 3.1 version is limited to 50w
const int64_t OB_MAX_PG_PARTITION_COUNT_PER_PG =
    100000;  // The maximum number of partitions in a single PG is limited to 10w
const int64_t OB_MAX_TIME = 3020399000000;
// Max add partition member timeout.
// Used to make sure no member added after lease expired + %OB_MAX_ADD_MEMBER_TIMEOUT
const int64_t OB_MAX_ADD_MEMBER_TIMEOUT = 60L * 1000L * 1000L;  // 1 minute
const int64_t OB_MAX_PACKET_FLY_TS = 100 * 1000L;               // 100ms
const int64_t OB_MAX_PACKET_DECODE_TS = 10 * 1000L;
// Oceanbase network protocol
/*  4bytes  4bytes  4bytes   4bytes
 * -----------------------------------
 * | flag |  dlen  | chid | reserved |
 * -----------------------------------
 */
const uint32_t OB_NET_HEADER_LENGTH = 16;  // 16 bytes packet header
const uint32_t OB_MAX_RPC_PACKET_LENGTH = (2L << 30) - (1 << 20);

const int OB_TBNET_PACKET_FLAG = 0x416e4574;
const int OB_SERVER_ADDR_STR_LEN = 128;  // used for buffer size of easy_int_addr_to_str

/*   3bytes   1 byte
 * ------------------
 * |   len  |  seq  |
 * ------------------
 */
const int64_t OB_MYSQL_HEADER_LENGTH = 4; /** 3bytes length + 1byte seq*/
const int64_t INVALID_CLUSTER_ID = -1;

/*      3bytes     1 byte      3bytes
 * -----------------------------------------
 * |  compr_len  |  seq  |  len_before_comp
 * -----------------------------------------
 */
const int64_t OB_MYSQL_COMPRESSED_HEADER_SIZE = OB_MYSQL_HEADER_LENGTH + 3; /* compression header size */

//-----------------------------------oceanbase 2.0 c/s protocol----------------------//
const uint16_t OB20_PROTOCOL_MAGIC_NUM = 0x20AB;
const int64_t OB20_PROTOCOL_HEADER_LENGTH = 24;
const int64_t OB20_PROTOCOL_TAILER_LENGTH = 4;  // for CRC32
const int64_t OB20_PROTOCOL_HEADER_TAILER_LENGTH = OB20_PROTOCOL_HEADER_LENGTH + OB20_PROTOCOL_TAILER_LENGTH;
const int64_t OB20_PROTOCOL_EXTRA_INFO_LENGTH = 4;  // for the length of extra info
const int16_t OB20_PROTOCOL_VERSION_VALUE = 20;

enum ObCSProtocolType {
  OB_INVALID_CS_TYPE = 0,
  OB_MYSQL_CS_TYPE,           // mysql standard protocol
  OB_MYSQL_COMPRESS_CS_TYPE,  // mysql compress protocol
  OB_2_0_CS_TYPE,             // oceanbase 2.0 protocol
};

inline const char* get_cs_protocol_type_name(const ObCSProtocolType type)
{
  switch (type) {
    case OB_INVALID_CS_TYPE:
      return "OB_INVALID_CS_TYPE";
    case OB_MYSQL_CS_TYPE:
      return "OB_MYSQL_CS_TYPE";
    case OB_MYSQL_COMPRESS_CS_TYPE:
      return "OB_MYSQL_COMPRESS_CS_TYPE";
    case OB_2_0_CS_TYPE:
      return "OB_2_0_CS_TYPE";
    default:
      return "OB_UNKNOWN_CS_TYPE";
  }
}

const int64_t OB_UPS_START_MAJOR_VERSION = 2;
const int64_t OB_UPS_START_MINOR_VERSION = 1;

const int64_t OB_NEWEST_DATA_VERSION = -2;

const int32_t OB_CONNECTION_FREE_TIME_S = 240;

#define INVALID_FD -1

/// @see ob_object.cpp and ob_expr_obj.cpp
const float OB_FLOAT_EPSINON = static_cast<float>(1e-6);
const double OB_DOUBLE_EPSINON = 1e-14;

const double OB_DOUBLE_PI = 3.141592653589793;

const uint64_t OB_UPS_MAX_MINOR_VERSION_NUM = 2048;
const int64_t OB_MAX_COMPACTSSTABLE_NUM = 64;
const int32_t OB_UPS_LIMIT_RATIO = 2;

const int64_t OB_MERGED_VERSION_INIT = 1;

const int64_t OB_TRACE_BUFFER_SIZE = 4 * 1024;  // 4k
const int64_t OB_TRACE_STAT_BUFFER_SIZE = 200;  // 200

const int64_t OB_MAX_VERSION_COUNT = 32;  // max version count
const int64_t OB_MAX_VERSION_COUNT_FOR_MERGE = 16;
const int64_t OB_EASY_HANDLER_COST_TIME = 10 * 1000;  // 10ms
const int64_t OB_EASY_MEMORY_LIMIT = 4L << 30;        // 4G

const int64_t OB_MAX_SCHEMA_BUF_SIZE = 10L * 1024L * 1024L;     // 10MB
const int64_t OB_MAX_PART_LIST_SIZE = 10L * 1024L * 1024L;      // 10MB
const int64_t OB_MAX_TABLE_ID_LIST_SIZE = 10L * 1024L * 1024L;  // 10MB

enum ObServerRole {
  OB_INVALID = 0,
  OB_ROOTSERVER = 1,    // rs
  OB_CHUNKSERVER = 2,   // cs
  OB_MERGESERVER = 3,   // ms
  OB_UPDATESERVER = 4,  // ups
  OB_PROXYSERVER = 5,
  OB_SERVER = 6,
  OB_PROXY = 7,
  OB_OBLOG = 8,  // liboblog
};

enum ObServerManagerOp {
  OB_SHUTDOWN = 1,
  OB_RESTART = 2,
  OB_ADD = 3,
  OB_DELETE = 4,
};

const int OB_FAKE_MS_PORT = 2828;
const uint64_t OB_MAX_PS_PARAM_COUNT = 65535;
const uint64_t OB_MAX_PS_FIELD_COUNT = 65535;
// OB_ALL_MAX_COLUMN_ID must <= 65535, it is used in ob_cs_create_plan.h
const uint64_t OB_ALL_MAX_COLUMN_ID = 65535;
// internal columns id
const uint64_t OB_NOT_EXIST_COLUMN_ID = 0;
const uint64_t OB_HIDDEN_PK_INCREMENT_COLUMN_ID = 1;  // hidden pk contain 3 column (seq, cluster_id, partition_id)
const uint64_t OB_CREATE_TIME_COLUMN_ID = 2;
const uint64_t OB_MODIFY_TIME_COLUMN_ID = 3;
const uint64_t OB_HIDDEN_PK_CLUSTER_COLUMN_ID = 4;
const uint64_t OB_HIDDEN_PK_PARTITION_COLUMN_ID = 5;
const uint64_t OB_HIDDEN_ROWID_COLUMN_ID = 6;
const uint64_t OB_HIDDEN_TRANS_VERSION_COLUMN_ID = 7;
const uint64_t OB_HIDDEN_SQL_SEQUENCE_COLUMN_ID = 8;
const uint64_t OB_HIDDEN_SESSION_ID_COLUMN_ID = 9;
const uint64_t OB_HIDDEN_SESS_CREATE_TIME_COLUMN_ID = 10;
const uint64_t OB_HIDDEN_LOGICAL_ROWID_COLUMN_ID = 11;
const int64_t OB_END_RESERVED_COLUMN_ID_NUM = 16;
const uint64_t OB_APP_MIN_COLUMN_ID = 16;
const uint64_t OB_ACTION_FLAG_COLUMN_ID = OB_ALL_MAX_COLUMN_ID - OB_END_RESERVED_COLUMN_ID_NUM + 1; /* 65520 */
const uint64_t OB_MAX_TMP_COLUMN_ID = OB_ALL_MAX_COLUMN_ID - OB_END_RESERVED_COLUMN_ID_NUM;

const char* const OB_UPDATE_MSG_FMT = "Rows matched: %ld  Changed: %ld  Warnings: %ld";
const char* const OB_INSERT_MSG_FMT = "Records: %ld  Duplicates: %ld  Warnings: %ld";
const char* const OB_LOAD_DATA_MSG_FMT = "Records: %ld  Deleted: %ld  Skipped: %ld  Warnings: %ld";
const char OB_PADDING_CHAR = ' ';
const char OB_PADDING_BINARY = '\0';
const char* const OB_VALUES = "__values";
// hidden primary key name
const char* const OB_HIDDEN_PK_INCREMENT_COLUMN_NAME = "__pk_increment";  // hidden
const char* const OB_HIDDEN_PK_CLUSTER_COLUMN_NAME = "__pk_cluster_id";
const char* const OB_HIDDEN_PK_PARTITION_COLUMN_NAME = "__pk_partition_id";
const char* const OB_HIDDEN_SESSION_ID_COLUMN_NAME = "SYS_SESSION_ID";              // oracle temporary table
const char* const OB_HIDDEN_SESS_CREATE_TIME_COLUMN_NAME = "SYS_SESS_CREATE_TIME";  // oracle temporary table

// hidden rowid name
const char* const OB_HIDDEN_ROWID_COLUMN_NAME = "__ob_rowid";
const int32_t OB_HIDDEN_ROWID_COLUMN_LENGTH = 16;

const char* const OB_HIDDEN_LOGICAL_ROWID_COLUMN_NAME = "ROWID";
const int32_t OB_HIDDEN_LOGICAL_ROWID_COLUMN_LENGTH = 5;

// internal index prefix
const char* const OB_INDEX_PREFIX = "__idx_";

// internal user
const char* const OB_INTERNAL_USER = "__ob_server";

const char* const OB_SERVER_ROLE_VAR_NAME = "__ob_server_role";
// trace id
const char* const OB_TRACE_ID_VAR_NAME = "__ob_trace_id";

// backup and restore
const int64_t OB_MAX_CLUSTER_NAME_LENGTH = OB_MAX_APP_NAME_LENGTH;
const int64_t OB_MAX_URI_LENGTH = 2048;
const int64_t OB_MAX_RETRY_TIMES = 3;
const int64_t OB_AGENT_MAX_RETRY_TIME = 5 * 60 * 1000 * 1000;  // 300s
const int64_t OB_AGENT_SINGLE_SLEEP_SECONDS = 5;               // 5s
// Currently the flashback database is not maintained incanation, it will always be 1. TODO: fix it
const int64_t OB_START_INCARNATION = 1;
const char* const OB_STRING_SSTABLE_META = "sstable_meta";
const char* const OB_STRING_TABLE_KEYS = "table_keys";
const char* const OB_STRING_PARTITION_META = "partition_meta";
const char* const OB_STRING_PART_LIST = "part_list";
const static int64_t OB_MAX_URI_HEADER_LENGTH = 1024;
const int64_t OB_MAX_REPLICAS_INFO = 1024;
const int64_t OB_INNER_TABLE_DEFAULT_KEY_LENTH = 1024;
const int64_t OB_INNER_TABLE_DEFAULT_VALUE_LENTH = 4096;
const int64_t OB_INNER_TABLE_BACKUP_TYPE_LENTH = 1;
const int64_t OB_DEFAULT_STATUS_LENTH = 64;
const int64_t OB_DEFAULT_LOG_INFO_LENGTH = 64;
const int64_t OB_DEFAULT_OUTPUT_DEVICE_TYPE_LENTH = 64;
const int64_t OB_INNER_TABLE_BACKUP_CLEAN_TYPE_LENGTH = 64;
const int64_t OB_INNER_TABLE_BACKUP_CLEAN_PARAMETER_LENGTH = 256;
const int64_t OB_INNER_TABLE_BACKUP_CLEAN_COMMENT_LENGHT = 2048;
const int64_t OB_INNER_TABLE_BACKUP_TASK_CLUSTER_FORMAT_LENGTH = 64;
const char* const OB_OSS_PREFIX = "oss://";
const char* const OB_FILE_PREFIX = "file://";
const char* const OB_COS_PREFIX = "cos://";
const char* const OB_RESOURCE_UNIT_DEFINITION = "resource_unit_definition";
const char* const OB_RESOURCE_POOL_DEFINITION = "resource_pool_definition";
const char* const OB_CREATE_TENANT_DEFINITION = "create_tenant_definition";
const char* const OB_CREATE_DATABASE_DEFINITION = "create_database_definition";
const char* const OB_CREATE_USER_DEINITION = "create_user_definition";
const char* const OB_SYSTEM_VARIABLE_DEFINITION = "system_variable_definition";
const char* const OB_TENANT_PARAMETER_DEFINITION = "tenant_parameter_definition";
const char* const OB_USER_PRIVILEGE_DEFINITION = "user_privilege_definition";
const char* const OB_CREATE_TABLEGROUP_DEFINITON = "create_tablegroup_definition";
const char* const OB_DATA_TABLE_IDS_LIST = "data_table_ids_list";
const char* const OB_INDEX_TABLE_IDS_LIST = "index_table_ids_list";
const char* const OB_TABLEGROUP_IDS_LIST = "tablegroup_ids_list";
const char* const OB_FOREIGN_KEY_IDS_LIST = "foreign_key_ids_list";
const char* const OB_FOREIGN_KEY_DEFINITION = "foreign_key_definition";
const char* const OB_ROUTINE_IDS_LIST = "routine_ids_list";
const char* const OB_CREATE_ROUTINE_DEFINITION = "create_routine_definition";
const char* const OB_PACKAGE_IDS_LIST = "package_ids_list";
const char* const OB_CREATE_PACKAGE_DEFINITION = "create_package_definition";
const char* const OB_UDT_IDS_LIST = "udt_ids_list";
const char* const OB_CREATE_UDT_DEFINITION = "create_udt_definition";
const char* const OB_TRIGGER_IDS_LIST = "trigger_ids_list";
const char* const OB_TRIGGER_DEFINITION = "trigger_definition";
const char* const OB_TABLESPACE_IDS_LIST = "tablespace_ids_list";
const char* const OB_CREATE_TABLESPACE_DEFINITION = "create_tablespace_definition";
const char* const OB_SEQUENCE_DEFINITION_IDS = "sequence_definition_ids";
const char* const OB_SEQUENCE_DEFINITION = "sequence_definition";
const char* const OB_RECYCLE_OBJECT_LIST = "recycle_objects_list";
const char* const OB_DROPPED_TABLE_ID_LIST = "dropped_table_ids_list";
const char* const OB_TABLEGROUP = "tablegroup";
const char* const OB_STRING_PARTITION_GROUP_META = "partition_group_meta";
const char* const OB_SECURITY_AUDIT_IDS_LIST = "security_audit_ids_list";
const char* const OB_SECURITY_AUDIT_DEFINITION = "security_audit_definition";
const char* const OB_SYNONYM_IDS_LIST = "synonym_ids_list";
const char* const OB_CREATE_SYNONYM_DEFINITION = "create_synonym_definition";
const char* const OB_TIMEZONE_INFO_DEFINITION = "timezone_info_definition";

enum ObCopySSTableType {
  OB_COPY_SSTABLE_TYPE_INVALID,
  OB_COPY_SSTABLE_TYPE_GLOBAL_INDEX,
  OB_COPY_SSTABLE_TYPE_LOCAL_INDEX,
  OB_COPY_SSTABLE_TYPE_RESTORE_FOLLOWER,
};

///////////////////////////////////////////////////////////
//                 SYSTEM TABLES                         //
///////////////////////////////////////////////////////////
// SYTEM TABLES ID (0, 500), they should not be mutated
const uint64_t OB_NOT_EXIST_TABLE_TID = 0;
///////////////////////////////////////////////////////////
//                 VIRUTAL TABLES                        //
///////////////////////////////////////////////////////////
// virtual table ID for SHOW statements start from 601
const uint64_t OB_LAST_SHOW_TID = 611;
///////////////////////////////////////////////////////////
//            ini schema                                 //
///////////////////////////////////////////////////////////
const char* const OB_BACKUP_SCHEMA_FILE_PATTERN = "etc/%s.schema.bin";

////////////////////////////////////////////////////////////
//                  schema variables length               //
////////////////////////////////////////////////////////////
const int64_t TEMP_ROWKEY_LENGTH = 64;
const int64_t SERVER_TYPE_LENGTH = 16;
const int64_t SERVER_STAT_LENGTH = 64;
const int64_t TABLE_MAX_KEY_LENGTH = 128;
const int64_t TABLE_MAX_VALUE_LENGTH = 128;
const int64_t MAX_ZONE_INFO_LENGTH = 4096;
const int64_t UPS_SESSION_TYPE_LENGTH = 64;
const int64_t UPS_MEMTABLE_LOG_LENGTH = 128;
const int64_t COLUMN_TYPE_LENGTH = 64;
const int64_t COLUMN_NULLABLE_LENGTH = 4;
const int64_t COLUMN_KEY_LENGTH = 4;
const int64_t COLUMN_DEFAULT_LENGTH = 4 * 1024;
const int64_t COLUMN_EXTRA_LENGTH = 4 * 1024;
const int64_t DATABASE_DEFINE_LENGTH = 4 * 1024;
const int64_t TABLE_DEFINE_LENGTH = 4 * 1024;
const int64_t TABLEGROUP_DEFINE_LENGTH = 4 * 1024;
const int64_t TENANT_DEFINE_LENGTH = 4 * 1024;
const int64_t ROW_FORMAT_LENGTH = 10;
const int64_t MAX_ENGINE_LENGTH = 64;
const int64_t MAX_CHARSET_LENGTH = 128;
const int64_t MAX_CHARSET_DESCRIPTION_LENGTH = 64;
const int64_t MAX_COLLATION_LENGTH = 128;
const int64_t MAX_TABLE_STATUS_CREATE_OPTION_LENGTH = 1024;
const int64_t MAX_BOOL_STR_LENGTH = 4;
const int64_t INDEX_SUB_PART_LENGTH = 256;
const int64_t INDEX_PACKED_LENGTH = 256;
const int64_t INDEX_NULL_LENGTH = 128;
const int64_t MAX_GRANT_LENGTH = 1024;
const int64_t MAX_SQL_PATH_LENGTH = 512;
const int64_t MAX_TENANT_COMMENT_LENGTH = 4096;
const int64_t MAX_LOCALITY_LENGTH = 4096;
const int64_t MAX_DATABASE_COMMENT_LENGTH = 2048;
const int64_t MAX_TABLE_COMMENT_LENGTH = 4096;
const int64_t MAX_INDEX_COMMENT_LENGTH = 2048;
const int64_t MAX_TABLEGROUP_COMMENT_LENGTH = 4096;
const int64_t MAX_VERSION_LENGTH = 128;
const int64_t MAX_FREEZE_STATUS_LENGTH = 64;
const int64_t MAX_FREEZE_SUBMIT_STATUS_LENGTH = 64;
const int64_t MAX_REPLAY_LOG_TYPE_LENGTH = 64;
// columns
const int64_t MAX_TABLE_CATALOG_LENGTH = 4096;
const int64_t MAX_COLUMN_COMMENT_LENGTH = 2048;  // Consistent with mysql, changed from 1024 to 2048
const int64_t MAX_COLUMN_KEY_LENGTH = 3;
const int64_t MAX_NUMERIC_PRECISION_LENGTH = 9;
const int64_t MAX_NUMERIC_SCALE_LENGTH = 9;
const int64_t MAX_COLUMN_PRIVILEGE_LENGTH = 200;
const int64_t MAX_PRIVILEGE_CONTEXT_LENGTH = 80;
const int64_t MAX_INFOSCHEMA_COLUMN_PRIVILEGE_LENGTH = 64;
const int64_t MAX_COLUMN_YES_NO_LENGTH = 3;
const int64_t MAX_COLUMN_VARCHAR_LENGTH = 262143;
const int64_t MAX_COLUMN_CHAR_LENGTH = 255;

// Oracle
const int64_t MAX_ORACLE_COMMENT_LENGTH = 4000;

// Oracle MAX_ENABLED_ROLES The maximum number of effective roles granted to users
const int64_t MAX_ENABLED_ROLES = 148;
const int64_t MAX_ORACLE_NAME_LENGTH = 30;

// Oracle SA
const int64_t MAX_ORACLE_SA_COMPONENTS_SHORT_NAME_LENGTH = 30;
const int64_t MAX_ORACLE_SA_COMPONENTS_LONG_NAME_LENGTH = 80;
const int64_t MAX_ORACLE_SA_COMPONENTS_PARENT_NAME_LENGTH = 30;
const int64_t MAX_ORACLE_SA_LABEL_TYPE_LENGTH = 15;

////////////////////////////////////////////////////////////
//             table id range definition                  //
////////////////////////////////////////////////////////////

// : must keep same with generate_inner_table_schema.py
// don't use share/inner_table/ob_inner_table_schema.h to avoid dependence.
const int64_t OB_SCHEMA_CODE_VERSION = 1;
const uint64_t OB_MAX_CORE_TABLE_ID = 100;
const uint64_t OB_MIN_SYS_INDEX_TABLE_ID = 9997;
const uint64_t OB_MAX_SYS_TABLE_ID = 10000;
const uint64_t OB_MAX_MYSQL_VIRTUAL_TABLE_ID = 15000;
const uint64_t OB_MIN_VIRTUAL_TABLE_ID = 15001;
const uint64_t OB_MAX_VIRTUAL_TABLE_ID = 20000;
// The overall system view interval is (20000, 30000)
// OB (MySQL) system view interval (20000, 25000)
// Oracle system view interval (25000, 30000)
const uint64_t OB_MAX_SYS_VIEW_ID = 30000;
const uint64_t OB_MAX_MYSQL_SYS_VIEW_ID = 25000;
const uint64_t OB_MIN_USER_TABLE_ID = 50000;
const uint64_t OB_MIN_GENERATED_COLUMN_ID = 2000;
const uint64_t OB_MIN_MV_COLUMN_ID = 10000;
const uint64_t OB_MIN_SHADOW_COLUMN_ID = 32767;
const uint64_t OB_MAX_SYS_POOL_ID = 100;

// for cte, cte table opens up a separate id space, which does not conflict with other id
const uint64_t OB_MIN_CTE_TABLE_ID = 49500;
const uint64_t OB_MAX_CTE_TABLE_ID = 49999;
const uint64_t OB_MIN_CTE_COLUMN_ID = 0;
const uint64_t OB_CTE_DATABASE_ID = 131313131;

// ddl related
const char* const OB_SYS_USER_NAME = "root";
const char* const OB_ORA_SYS_USER_NAME = "SYS";
const char* const OB_ORA_LBACSYS_NAME = "LBACSYS";
const char* const OB_ORA_AUDITOR_NAME = "ORAAUDITOR";
const char* const OB_ORA_CONNECT_ROLE_NAME = "CONNECT";
const char* const OB_ORA_RESOURCE_ROLE_NAME = "RESOURCE";
const char* const OB_ORA_DBA_ROLE_NAME = "DBA";
const char* const OB_ORA_PUBLIC_ROLE_NAME = "PUBLIC";
const char* const OB_RESTORE_USER_NAME = "__oceanbase_inner_restore_user";
const char* const OB_DRC_USER_NAME = "__oceanbase_inner_drc_user";
const char* const OB_SYS_TENANT_NAME = "sys";
const char* const OB_GTS_TENANT_NAME = "gts";
const char* const OB_SYS_HOST_NAME = "%";
const char* const OB_DEFAULT_HOST_NAME = "%";
const char* const OB_MONITOR_TENANT_NAME = "monitor";
const char* const OB_DIAG_TENANT_NAME = "diag";
// for sync ddl (ClusterID_TenantID_SchemaVersion)
const char* const OB_DDL_ID_VAR_NAME = "__oceanbase_ddl_id";
const int64_t OB_MAX_DDL_ID_STR_LENGTH = 64;

// The default user name of the standby database to log in to the main database
const char* const OB_STANDBY_USER_NAME = "__oceanbase_inner_standby_user";

const double TENANT_RESERVE_MEM_RATIO = 0.1;
const int64_t DEFAULT_MAX_SYS_MEMORY = 16L << 30;
const int64_t DEFAULT_MIN_SYS_MEMORY = 12L << 30;
const int64_t LEAST_MEMORY_SIZE = 2L << 30;
const int64_t SYS_MAX_ALLOCATE_MEMORY = 1L << 34;

// mem factor
const double SQL_AUDIT_MEM_FACTOR = 0.1;
const double MIN_SYS_TENANT_MEM_FACTOR =
    0.25;  // There is no need to set too large, the functions of the tenant system will be restricted later
const double MAX_SYS_TENANT_MEM_FACTOR = 0.3;
const double MONITOR_MEM_FACTOR = 0.01;
const double KVCACHE_FACTOR = TENANT_RESERVE_MEM_RATIO;

const double DEFAULT_MAX_SYS_CPU = 5.;
const double MIN_SYS_TENANT_QUOTA = 2.5;
const double MIN_TENANT_QUOTA = .5;
const double EXT_LOG_TENANT_CPU = 4.;
const int64_t EXT_LOG_TENANT_MEMORY_LIMIT = 4L << 30;
const double OB_MONITOR_CPU = 1.;
const double OB_DTL_CPU = 5.;
const double OB_DIAG_CPU = 1.0;
const double OB_DATA_CPU = 2.5;
const double OB_RS_CPU = 1.0;
const double OB_SVR_BLACKLIST_CPU = 1.0;
const int64_t OB_DIAG_MEMORY = 2L << 30;
const int64_t OB_RS_MEMORY = 2L << 30;

const uint64_t OB_INVALID_TENANT_ID = 0;
const uint64_t OB_SYS_TENANT_ID = 1;
const uint64_t OB_GTS_TENANT_ID = 2;
const uint64_t OB_SERVER_TENANT_ID = 500;
const uint64_t OB_ELECT_TENANT_ID = 501;
const uint64_t OB_LOC_CORE_TENANT_ID = 502;
const uint64_t OB_LOC_ROOT_TENANT_ID = 503;
const uint64_t OB_LOC_SYS_TENANT_ID = 504;
const uint64_t OB_LOC_USER_TENANT_ID = 505;
const uint64_t OB_EXT_LOG_TENANT_ID = 506;
const uint64_t OB_MONITOR_TENANT_ID = 507;
const uint64_t OB_DTL_TENANT_ID = 508;
const uint64_t OB_DATA_TENANT_ID = 509;
const uint64_t OB_RS_TENANT_ID = 510;
const uint64_t OB_GTS_SOURCE_TENANT_ID = 511;
const uint64_t OB_SVR_BLACKLIST_TENANT_ID = 512;
const uint64_t OB_DIAG_TENANT_ID = 999;
const uint64_t OB_MAX_RESERVED_TENANT_ID = 1000;

const uint64_t OB_SYS_USER_ID = 1;
const uint64_t OB_EMPTY_USER_ID = 2;
const uint64_t OB_ORA_SYS_USER_ID = 3;
const uint64_t OB_ORA_LBACSYS_USER_ID = 4;
const uint64_t OB_ORA_AUDITOR_USER_ID = 5;
const uint64_t OB_ORA_CONNECT_ROLE_ID = 6;
const uint64_t OB_ORA_RESOURCE_ROLE_ID = 7;
const uint64_t OB_ORA_DBA_ROLE_ID = 8;
const uint64_t OB_ORA_PUBLIC_ROLE_ID = 9;
const char* const OB_PROXYRO_USERNAME = "proxyro";
const uint64_t OB_SYS_TABLEGROUP_ID = 1;
const char* const OB_SYS_TABLEGROUP_NAME = "oceanbase";
const uint64_t OB_SYS_DATABASE_ID = 1;
const char* const OB_SYS_DATABASE_NAME = "oceanbase";
const uint64_t OB_INFORMATION_SCHEMA_ID = 2;
const char* const OB_INFORMATION_SCHEMA_NAME = "information_schema";
const uint64_t OB_MYSQL_SCHEMA_ID = 3;
const char* const OB_MYSQL_SCHEMA_NAME = "mysql";
const uint64_t OB_RECYCLEBIN_SCHEMA_ID = 4;
const char* const OB_RECYCLEBIN_SCHEMA_NAME = "__recyclebin";  // hidden
const uint64_t OB_PUBLIC_SCHEMA_ID = 5;
const char* const OB_PUBLIC_SCHEMA_NAME = "__public";  // hidden
const char* const OB_TEST_SCHEMA_NAME = "test";
const char* const OB_ORA_SYS_SCHEMA_NAME = "SYS";
const uint64_t OB_ORA_SYS_DATABASE_ID = 6;
const uint64_t OB_ORA_LBACSYS_DATABASE_ID = 7;
const uint64_t OB_ORA_AUDITOR_DATABASE_ID = 8;
const char* const OB_ORA_PUBLIC_SCHEMA_NAME = "PUBLIC";

// sys unit associated const
const uint64_t OB_SYS_UNIT_CONFIG_ID = 1;
const char* const OB_SYS_UNIT_CONFIG_NAME = "sys_unit_config";
const uint64_t OB_SYS_RESOURCE_POOL_ID = 1;
const uint64_t OB_SYS_UNIT_ID = 1;
const uint64_t OB_INIT_SERVER_ID = 1;
// gts unit associate const
const uint64_t OB_GTS_UNIT_CONFIG_ID = 100;
const char* const OB_GTS_UNIT_CONFIG_NAME = "gts_unit_config";
const uint64_t OB_GTS_RESOURCE_POOL_ID = 100;
const uint64_t OB_GTS_UNIT_ID = 100;
// gts quorum
const int64_t OB_GTS_QUORUM = 3;
const char* const OB_ORIGINAL_GTS_NAME = "primary_gts_instance";
const uint64_t OB_ORIGINAL_GTS_ID = 100;
// standby unit config template
const char* const OB_STANDBY_UNIT_CONFIG_TEMPLATE_NAME = "standby_unit_config_template";

const uint64_t OB_SCHEMATA_TID = 2001;
const char* const OB_SCHEMATA_TNAME = "schemata";
const char* const OB_MYSQL50_TABLE_NAME_PREFIX = "#mysql50#";

const uint64_t OB_USER_TENANT_ID = 1000;
const uint64_t OB_USER_TABLEGROUP_ID = 1000;
const uint64_t OB_USER_DATABASE_ID = 1000;
const uint64_t OB_USER_ID = 1000;
const uint64_t OB_USER_UNIT_CONFIG_ID = 1000;
const uint64_t OB_USER_RESOURCE_POOL_ID = 1000;
const uint64_t OB_USER_UNIT_ID = 1000;
const uint64_t OB_USER_SEQUENCE_ID = 0;
const uint64_t OB_USER_OUTLINE_ID = 1000;
const uint64_t OB_USER_SYNONYM_ID = 0;
const uint64_t OB_USER_PLAN_BASELINE_ID = 0;
const uint64_t OB_USER_START_DATABASE_ID =
    1050;  // OB_USER_DATABASE_ID = 1000; 50 are reserved to initialize the user database added by default when the
           // tenant is initialized, such as the test library
const uint64_t OB_USER_RG_ID = 1000;
const uint64_t OB_USER_UDF_ID = 0;
const uint64_t OB_USER_CONSTRAINT_ID = 1000;
const uint64_t OB_USER_UDT_ID = 1000;
const uint64_t OB_USER_ROUTINE_ID = 1000;
const uint64_t OB_USER_PACKAGE_ID = 1000;
const uint64_t OB_MYSQL_TENANT_INNER_KEYSTORE_ID = 100;  // This id is used by the MySQL tenant's built-in keystore.
const uint64_t OB_USER_KEYSTORE_ID = 1000;
const uint64_t OB_USER_MASTERKEY_ID = 1000;
const uint64_t OB_USER_TABLESPACE_ID = 1000;
const uint64_t OB_USER_AUDIT_ID = 1000;

/*
 * trigger package need be parsed specially because it support identifier with ':' prefixed
 * like ':OLD' / ':NEW'. we need differ normal package and trigger package very easily.
 * trigger package is mocked by trigger, we cannot find them from normal package schema,
 * we need fetch them from trigger schema, so trigger id and trigger package id should be
 * convert to each other very easily.
 * we make these following rules:
 * 1. set the highest bit of PURE trigger id to 1.
 * 2. trigger should be even integer, not odd.
 * 3. set trigger spec package id to trigger id.
 * 4. set trigger body package id to trigger id plus 1.
 */
const uint64_t OB_USER_TRIGGER_ID_MASK = 0x8000000000;
const uint64_t OB_USER_TRIGGER_ID = OB_USER_TRIGGER_ID_MASK + 1000;
const uint64_t OB_USER_DBLINK_ID = 16;

const char* const OB_PRIMARY_INDEX_NAME = "PRIMARY";

const int64_t OB_MAX_CONFIG_URL_LENGTH = 512;

const double OB_UNIT_MIN_CPU = 0.1;
const int64_t OB_UNIT_MIN_MEMORY = 256LL * 1024LL * 1024LL;     // 256M
const int64_t OB_UNIT_MIN_DISK_SIZE = 512LL * 1024LL * 1024LL;  // 512MB
const int64_t OB_UNIT_MIN_IOPS = 128;
const int64_t OB_UNIT_MIN_SESSION_NUM = 64;

const int64_t OB_MIGRATE_ACTION_LENGTH = 64;
const int64_t OB_MIGRATE_REPLICA_STATE_LENGTH = 64;
const int64_t OB_SYS_TASK_TYPE_LENGTH = 64;
const int64_t OB_MAX_TASK_COMMENT_LENGTH = 512;
const int64_t MAX_RECONNECTION_INTERVAL = 5 * 1000 * 1000;    // 5s
const int64_t MAX_LOGIC_MIGRATE_TIME_OUT = 30 * 1000 * 1000;  // 30s
const int64_t OB_MODULE_NAME_LENGTH = 64;
const int64_t OB_RET_STR_LENGTH = 64;
const int64_t OB_STATUS_STR_LENGTH = 64;
const int64_t OB_DAG_WARNING_INFO_LENGTH = 512;

// for erasure code
const int64_t OB_MAX_EC_STRIPE_COUNT = 32;

// for array log print
const int64_t OB_LOG_KEEP_SIZE = 512;
// no need keep size for async
const int64_t OB_ASYNC_LOG_KEEP_SIZE = 0;
const char* const OB_LOG_ELLIPSIS = "...";

const char* const DEFAULT_REGION_NAME = "default_region";

// for obproxy
const char* const OB_MYSQL_CLIENT_MODE = "__mysql_client_type";
const char* const OB_MYSQL_CLIENT_OBPROXY_MODE_NAME = "__ob_proxy";
const char* const OB_MYSQL_CONNECTION_ID = "__connection_id";
const char* const OB_MYSQL_GLOBAL_VARS_VERSION = "__global_vars_version";
const char* const OB_MYSQL_PROXY_CONNECTION_ID = "__proxy_connection_id";
const char* const OB_MYSQL_PROXY_SESSION_CREATE_TIME_US = "__proxy_session_create_time_us";
const char* const OB_MYSQL_CLUSTER_NAME = "__cluster_name";
const char* const OB_MYSQL_CLUSTER_ID = "__cluster_id";
const char* const OB_MYSQL_CLIENT_IP = "__client_ip";
const char* const OB_MYSQL_CAPABILITY_FLAG = "__proxy_capability_flag";
const char* const OB_MYSQL_PROXY_SESSION_VARS = "__proxy_session_vars";
const char* const OB_MYSQL_SCRAMBLE = "__proxy_scramble";
const char* const OB_MYSQL_PROXY_VEERSION = "__proxy_version";

// for java client
const char* const OB_MYSQL_JAVA_CLIENT_MODE_NAME = "__ob_java_client";

enum ObClientMode {
  OB_MIN_CLIENT_MODE = 0,

  OB_JAVA_CLIENT_MODE,
  OB_PROXY_CLIENT_MODE,
  // add others ...

  OB_MAX_CLIENT_MODE,
};

// for obproxy debug
#define OBPROXY_DEBUG 0
const char* const OB_SYS_TENANT_LOCALITY_STRATEGY = "sys_tenant_locality_strategy";
const char* const OB_AUTO_LOCALITY_STRATEGY = "auto_locality_strategy";
const char* const OB_3ZONES_IN_2REGIONS_LOCALITY_STRATEGY = "3zone3-in-2regions";

#if OBPROXY_DEBUG
const char* const OB_MYSQL_PROXY_SESSION_ID = "session_id";
const char* const OB_MYSQL_PROXY_TIMESTAMP = "proxy_time_stamp";
const char* const OB_MYSQL_PROXY_SYNC_VERSION = "proxy_sync_version";

const char* const OB_MYSQL_SERVER_SESSION_ID = "server_session_id";
const char* const OB_MYSQL_SERVER_SQL = "sql";
const char* const OB_MYSQL_SERVER_HANDLE_TIMESTAMP = "server_handle_time_stamp";
const char* const OB_MYSQL_SERVER_RECEIVED_TIMESTAMP = "server_receive_time_stamp";
#endif

#define OB_PACKAGE_ID_SHIFT 8
OB_INLINE uint64_t extract_package_id(uint64_t global_type_id)
{
  return (int64_t)global_type_id >> OB_PACKAGE_ID_SHIFT;
}

OB_INLINE int64_t extract_type_id(uint64_t global_type_id)
{
  return global_type_id & (~(UINT64_MAX << OB_PACKAGE_ID_SHIFT));
}

OB_INLINE uint64_t combine_pl_type_id(uint64_t package_id, int64_t type_idx)
{
  return (package_id << OB_PACKAGE_ID_SHIFT | type_idx);
}

#define OB_ROOTSERVICE_EPOCH_SHIFT 40
OB_INLINE int64_t extract_rootservice_epoch(uint64_t sequence_id)
{
  return (int64_t)(sequence_id >> OB_ROOTSERVICE_EPOCH_SHIFT);
}

OB_INLINE uint64_t extract_pure_sequence_id(uint64_t sequence_id)
{
  return sequence_id & (~(UINT64_MAX << OB_ROOTSERVICE_EPOCH_SHIFT));
}

OB_INLINE uint64_t combine_sequence_id(int64_t rootservice_epoch, uint64_t pure_sequence_id)
{
  return (rootservice_epoch << OB_ROOTSERVICE_EPOCH_SHIFT | pure_sequence_id);
}

// The following is the occupation of pure_object_id
#define OB_TENANT_ID_SHIFT 40
#define OB_TABLEGROUP_FLAG_SHIFT 39
#define OB_LINK_TABLE_FLAG_SHIFT 38
const uint64_t OB_TABLEGROUP_MASK = ((uint64_t)0x1 << OB_TABLEGROUP_FLAG_SHIFT);
const uint64_t OB_LINK_TABLE_MASK = ((uint64_t)0x1 << OB_LINK_TABLE_FLAG_SHIFT);
const uint64_t OB_TRANS_TABLE_MASK = OB_TABLEGROUP_MASK | OB_LINK_TABLE_MASK;
const uint64_t OB_TABLE_FLAG_MASK = OB_TABLEGROUP_MASK | OB_LINK_TABLE_MASK;

/*
 * The current general encoding scheme for object_id is:
 * tenant_id (24 bits) + pure_object_id (40 bits)
 * There are many functions that occupy some bits in object_id to represent specific semantics.
 * When more and more bits are occupied, the space of pure_object_id will be severely compressed,
 * so consider compressing as much as possible.
 * For the current tablegroup and link_table, the 39th and 38th bits need to be occupied. At this time,
 *  if there is a third function that also needs to be occupied, there are two solutions:
 * 1. Directly occupy the 37th position.
 * 2. Reuse the 39th and 38th bits:
 *  a. 10 means tablegroup;
 *  b. 01 means link_table;
 *  c. 11 represents the transaction status table partition;
 *  d. 00 means ordinary table.
 * Solution 1: N bits can only represent N kinds of marks.
 * Solution 2: N bits can represent 2^N-1 kinds of tags, provided that all tags are mutually exclusive.
 *  For example, it is impossible for a table_id to be a tablegroup or a link_table.
 * The current choice is Solution 2. After all, pure_object_id only has 40 bits,
 *  so the space is limited and it is necessary to save as much as possible.
 */

OB_INLINE uint64_t extract_tenant_id(uint64_t id)
{
  return id >> OB_TENANT_ID_SHIFT;
}

OB_INLINE uint64_t extract_pure_id(uint64_t id)
{
  return id & (~(UINT64_MAX << OB_TENANT_ID_SHIFT));
}

OB_INLINE uint64_t combine_id(uint64_t tenant_id, uint64_t pure_id)
{
  return (tenant_id << OB_TENANT_ID_SHIFT) | extract_pure_id(pure_id);
}

OB_INLINE uint64_t replace_tenant_id(uint64_t tenant_id, uint64_t old_id)
{
  return (tenant_id << OB_TENANT_ID_SHIFT) | extract_pure_id(old_id);
}

// dblink.

/*
 * normal table:
 * | tenant_id | tablegroup flag |  dblink flag | table_id |
 * ---------------------------------------------------------
 * |  24 bits  |   1 bit keep 1  | 1 bit keep 0 |  38 bits |
 *
 * link table:
 * | tenant_id | tablegroup flag |  dblink flag | dblink_id | table_id |
 * ---------------------------------------------------------------------
 * |  24 bits  |   1 bit keep 0  | 1 bit keep 1 |  30 bits  |  8 bits  |
 *
 * tenant_id in link table id is local tenant, not the tenant in remote cluster.
 * why not use remote tenant id then we can get information from remote cluster
 * by table_id?
 * because not all kinds of database have table id, such as mysql, so we must
 * get information by database_name and table_name.
 * table_id is generated in every stmt, temporary.
 */

#define OB_DBLINK_ID_SHIFT 8
const uint64_t OB_DBLINK_ID_MASK = (uint64_t)0x3FFFFFFF;
const uint64_t OB_MAX_PURE_DBLINK_ID = OB_DBLINK_ID_MASK;
const uint64_t OB_MAX_PURE_LINK_TABLE_ID = (uint64_t)0xFF;

OB_INLINE bool is_link_table_id(uint64_t table_id)
{
  return table_id != OB_INVALID_ID && (table_id & OB_TABLE_FLAG_MASK) == OB_LINK_TABLE_MASK;
}

OB_INLINE uint64_t combine_link_table_id(uint64_t dblink_id, uint64_t pure_link_table_id)
{
  uint64_t tenant_id = extract_tenant_id(dblink_id);
  uint64_t pure_dblink_id = extract_pure_id(dblink_id);
  uint64_t link_table_id = OB_INVALID_ID;
  if (pure_dblink_id <= OB_MAX_PURE_DBLINK_ID && pure_link_table_id <= OB_MAX_PURE_LINK_TABLE_ID) {
    link_table_id = (tenant_id << OB_TENANT_ID_SHIFT) | (OB_LINK_TABLE_MASK) | (pure_dblink_id << OB_DBLINK_ID_SHIFT) |
                    (pure_link_table_id);
  }
  return link_table_id;
}

OB_INLINE uint64_t extract_dblink_id(uint64_t link_table_id)
{
  uint64_t dblink_id = 0;
  if (is_link_table_id(link_table_id)) {
    uint64_t tenant_id = extract_tenant_id(link_table_id);
    uint64_t pure_dblink_id = (link_table_id >> OB_DBLINK_ID_SHIFT) & OB_DBLINK_ID_MASK;
    dblink_id = combine_id(tenant_id, pure_dblink_id);
  }
  return dblink_id;
}

OB_INLINE uint64_t extract_table_id(uint64_t link_table_id)
{
  return link_table_id & (~(UINT64_MAX << OB_DBLINK_ID_SHIFT));
}

OB_INLINE bool is_tablegroup_id(uint64_t tablegroup_id)
{
  return (tablegroup_id & OB_TABLE_FLAG_MASK) == OB_TABLEGROUP_MASK;
}

OB_INLINE bool is_new_tablegroup_id(uint64_t tablegroup_id)
{
  return tablegroup_id != OB_INVALID_ID && (tablegroup_id & OB_TABLE_FLAG_MASK) == OB_TABLEGROUP_MASK;
}

OB_INLINE uint64_t transform_trans_table_id(uint64_t id)
{
  return id | OB_TRANS_TABLE_MASK;
}

OB_INLINE bool is_trans_table_id(uint64_t id)
{
  return (id & OB_TABLE_FLAG_MASK) == OB_TRANS_TABLE_MASK;
}

const char* const OB_RANDOM_PRIMARY_ZONE = "RANDOM";

#define OB_PART_ID_SHIFT 32
#define OB_PART_IDS_BITNUM 28

const uint64_t OB_TWOPART_BEGIN_MASK =
    (((uint64_t)0x1 << OB_PART_IDS_BITNUM) | ((uint64_t)0x1 << (OB_PART_IDS_BITNUM + OB_PART_ID_SHIFT)));

// for PARTITION_LEVEL_TWO int64_t(int32_t,int32_t) partition_id, part_version_num is begin with 1.
const uint64_t OB_TWO_PART_MASK = (0xF << OB_PART_IDS_BITNUM);

// combine part_id,subpart_id which is with PARTITION_LEVEL_TWO_MASK
OB_INLINE int64_t combine_part_id(int64_t part_id, int64_t subpart_id)
{
  return ((uint64_t)part_id << OB_PART_ID_SHIFT) | subpart_id;
}

OB_INLINE int64_t is_twopart(int32_t part_id, int32_t assit_id)
{
  return ((uint64_t)part_id & OB_TWO_PART_MASK) && ((uint64_t)assit_id & OB_TWO_PART_MASK);
}

OB_INLINE int64_t is_twopart(int64_t partition_id)
{
  return ((uint64_t)partition_id & OB_TWO_PART_MASK) &&
         (((uint64_t)partition_id >> OB_PART_ID_SHIFT) & OB_TWO_PART_MASK);
}

OB_INLINE int64_t generate_phy_part_id(int64_t part_idx, int64_t sub_part_idx)
{
  return (part_idx < 0 || sub_part_idx < 0)
             ? -1
             : (((uint64_t)part_idx << OB_PART_ID_SHIFT) | sub_part_idx) | OB_TWOPART_BEGIN_MASK;
}

// get subpart_id with PARTITION_LEVEL_TWO_MASK
OB_INLINE int64_t extract_subpart_id(int64_t id)
{
  return id & (~(UINT64_MAX << OB_PART_ID_SHIFT));
}

// get part_id with PARTITION_LEVEL_TWO_MASK
OB_INLINE int64_t extract_part_id(int64_t id)
{
  return id >> OB_PART_ID_SHIFT;
}

// get part idx from one level partid
OB_INLINE int64_t extract_idx_from_partid(int64_t id)
{
  return (id & (~(UINT64_MAX << OB_PART_IDS_BITNUM)));
}

// get part space from one level partid
OB_INLINE int64_t extract_space_from_partid(int64_t id)
{
  return (id >> OB_PART_IDS_BITNUM);
}

// get part_idx
OB_INLINE int64_t extract_part_idx(int64_t id)
{
  return extract_idx_from_partid(extract_part_id(id));
}

// get sub_part_idx
OB_INLINE int64_t extract_subpart_idx(int64_t id)
{
  return extract_idx_from_partid(extract_subpart_id(id));
}

// generate partition idx in table
OB_INLINE int64_t generate_partition_idx(int64_t id, int64_t sub_part_num)
{
  int64_t idx = 0;
  if (sub_part_num <= 0) {
    idx = id;
  } else {
    idx = extract_part_idx(id) * sub_part_num + extract_subpart_idx(id);
  }
  return idx;
}

OB_INLINE bool compare_partition_id(int64_t l_id, int64_t r_id)
{
  bool less = false;
  if (extract_part_id(l_id) < extract_part_id(r_id)) {
    less = true;
  } else if (extract_part_id(l_id) == extract_part_id(r_id) && extract_subpart_id(l_id) < extract_subpart_id(r_id)) {
    less = true;
  } else {
  }
  return less;
}
OB_INLINE bool is_core_table(const uint64_t tid)
{
  const uint64_t id = extract_pure_id(tid);
  const uint64_t tenant_id = extract_tenant_id(tid);
  return (OB_INVALID_ID != id) && (id <= OB_MAX_CORE_TABLE_ID) && (OB_SYS_TENANT_ID == tenant_id);
}

OB_INLINE bool is_sys_table(const uint64_t tid)
{
  const uint64_t id = extract_pure_id(tid);
  return (0 < id && id <= OB_MAX_SYS_TABLE_ID);
}

OB_INLINE bool is_virtual_table(const uint64_t tid)
{
  const uint64_t id = extract_pure_id(tid);
  return (id > OB_MAX_SYS_TABLE_ID && id <= OB_MAX_VIRTUAL_TABLE_ID);
}

OB_INLINE bool is_ora_virtual_table(const uint64_t tid)
{
  const uint64_t id = extract_pure_id(tid);
  return (id > OB_MAX_MYSQL_VIRTUAL_TABLE_ID && id <= OB_MAX_VIRTUAL_TABLE_ID);
}

OB_INLINE bool is_shadow_column(const uint64_t column_id)
{
  return column_id > common::OB_MIN_SHADOW_COLUMN_ID;
}

// Temporary table_id allocated when SQL is processing join
OB_INLINE bool is_join_table(const uint64_t tid)
{
  return tid >= UINT64_MAX - 1000;
}

OB_INLINE bool is_cte_table(const uint64_t tid)
{
  const uint64_t id = extract_pure_id(tid);
  return (id >= OB_MIN_CTE_TABLE_ID && id <= OB_MAX_CTE_TABLE_ID);
}

OB_INLINE bool is_fake_table(const uint64_t tid)
{
  return (is_join_table(tid) || is_cte_table(tid));
}

OB_INLINE bool is_sys_view(const uint64_t tid)
{
  const uint64_t id = extract_pure_id(tid);
  return (id > OB_MAX_VIRTUAL_TABLE_ID && id <= OB_MAX_SYS_VIEW_ID);
}

OB_INLINE bool is_ora_sys_view_table(const uint64_t tid)
{
  const uint64_t id = extract_pure_id(tid);
  return (id > OB_MAX_MYSQL_SYS_VIEW_ID && id <= OB_MAX_SYS_VIEW_ID);
}

OB_INLINE bool is_reserved_id(const uint64_t tid)
{
  const uint64_t id = extract_pure_id(tid);
  return (id > OB_MAX_SYS_VIEW_ID && id <= OB_MIN_USER_TABLE_ID);
}

OB_INLINE bool is_inner_db(const uint64_t db_id)
{
  const uint64_t id = extract_pure_id(db_id);
  return (id < OB_USER_DATABASE_ID);
}

OB_INLINE bool is_sys_database_id(const uint64_t database_id)
{
  uint64_t pure_database_id = extract_pure_id(database_id);
  return (OB_SYS_DATABASE_ID == pure_database_id || OB_INFORMATION_SCHEMA_ID == pure_database_id ||
          OB_MYSQL_SCHEMA_ID == pure_database_id || OB_ORA_SYS_DATABASE_ID == pure_database_id);
}

OB_INLINE bool is_inner_table(const uint64_t tid)
{
  const uint64_t id = extract_pure_id(tid);
  return (id <= OB_MIN_USER_TABLE_ID);
}

OB_INLINE bool is_ora_sys_user(const uint64_t uid)
{
  const uint64_t id = extract_pure_id(uid);
  return (id == OB_ORA_SYS_USER_ID);
}

OB_INLINE bool is_ora_lbacsys_user(const uint64_t uid)
{
  const uint64_t id = extract_pure_id(uid);
  return (id == OB_ORA_LBACSYS_USER_ID);
}

OB_INLINE bool is_ora_auditor_user(const uint64_t uid)
{
  const uint64_t id = extract_pure_id(uid);
  return (id == OB_ORA_AUDITOR_USER_ID);
}

OB_INLINE bool is_ora_connect_role(const uint64_t uid)
{
  const uint64_t id = extract_pure_id(uid);
  return (id == OB_ORA_CONNECT_ROLE_ID);
}

OB_INLINE bool is_ora_resource_role(const uint64_t uid)
{
  const uint64_t id = extract_pure_id(uid);
  return (id == OB_ORA_RESOURCE_ROLE_ID);
}

OB_INLINE bool is_ora_dba_role(const uint64_t uid)
{
  const uint64_t id = extract_pure_id(uid);
  return (id == OB_ORA_DBA_ROLE_ID);
}

OB_INLINE bool is_ora_public_role(const uint64_t uid)
{
  const uint64_t id = extract_pure_id(uid);
  return (id == OB_ORA_PUBLIC_ROLE_ID);
}

OB_INLINE bool is_inner_table_with_partition(const uint64_t tid)
{
  const uint64_t id = extract_pure_id(tid);
  return (id <= OB_MIN_SYS_INDEX_TABLE_ID);
}
// This method mainly judges internal tables that have nothing to do with ordinary tenants
OB_INLINE bool is_inner_table_without_user(const uint64_t tid)
{
  const uint64_t id = extract_pure_id(tid);
  const uint64_t tenant_id = extract_tenant_id(tid);
  return (id <= OB_MAX_SYS_TABLE_ID && tenant_id < OB_MAX_RESERVED_TENANT_ID);
}

OB_INLINE bool is_bootstrap_resource_pool(const uint64_t resource_pool_id)
{
  return (OB_SYS_RESOURCE_POOL_ID == resource_pool_id || OB_GTS_RESOURCE_POOL_ID == resource_pool_id);
}

// ob_malloc & ob_tc_malloc
const int64_t OB_MALLOC_NORMAL_BLOCK_SIZE = (1LL << 13);             // 8KB
const int64_t OB_MALLOC_MIDDLE_BLOCK_SIZE = (1LL << 16);             // 64KB
const int64_t OB_MALLOC_BIG_BLOCK_SIZE = (1LL << 21) - (1LL << 10);  // 2MB (-1KB)

const int64_t OB_MAX_MYSQL_RESPONSE_PACKET_SIZE = OB_MALLOC_BIG_BLOCK_SIZE;

const int64_t MAX_FRAME_SIZE = OB_MALLOC_BIG_BLOCK_SIZE;

/// Maximum number of elements/columns a row can contain
const int64_t OB_USER_ROW_MAX_COLUMNS_COUNT = 512;
const int64_t OB_ROW_MAX_COLUMNS_COUNT =
    OB_USER_ROW_MAX_COLUMNS_COUNT + 2 * OB_USER_MAX_ROWKEY_COLUMN_NUMBER;  // used in ObRow
const int64_t OB_DEFAULT_COL_DEC_NUM = common::OB_ROW_MAX_COLUMNS_COUNT / 80;
const int64_t OB_DEFAULT_MULTI_GET_ROWKEY_NUM = 8;
const int64_t OB_MAX_TIMESTAMP_LENGTH = 32;
const int64_t OB_COMMON_MEM_BLOCK_SIZE = 64 * 1024;
const int64_t OB_MAX_USER_ROW_LENGTH = 1572864L;  // 1.5M
const int64_t OB_MAX_ROW_LENGTH = OB_MAX_USER_ROW_LENGTH + 64L * 1024L /*for root table extra columns*/;
const int64_t OB_MAX_ROW_LENGTH_IN_MEMTABLE = 60L * 1024 * 1024;  // 60M
const int64_t OB_MAX_MONITOR_INFO_LENGTH = 65535;
const int64_t OB_MAX_CHAR_LENGTH = 256;               // Compatible with mysql, unit character mysql is 256
const int64_t OB_MAX_ORACLE_CHAR_LENGTH_BYTE = 2000;  // Compatible with oracle, unit byte oracle is 2000
const int64_t OB_MAX_ORACLE_VARCHAR_LENGTH =
    32767;  // Compatible with oracle, VARCHAR's max length is 4k, PL and SQL unified extended to 32767
// Compatible with oracle, the maximum length of RAW type column in SQL layer is 2000 byte
const int64_t OB_MAX_ORACLE_RAW_SQL_COL_LENGTH = 2000;
// Compatible with oracle, the maximum length of PL layer RAW type variable is 32767 byte
const int64_t OB_MAX_ORACLE_RAW_PL_VAR_LENGTH = 32767;
const int64_t OB_MAX_VARCHAR_LENGTH = 1024L * 1024L;  // Unit byte
const int64_t OB_MAX_BIT_LENGTH = 64;                 // Compatible with mysql, 64 bit
const int64_t OB_MAX_SET_ELEMENT_NUM = 64;            // Compatible with mysql8.0, the number of values
const int64_t OB_MAX_INTERVAL_VALUE_LENGTH = 255;     // Compatible with mysql, unit character
const int64_t OB_MAX_ENUM_ELEMENT_NUM = 65535;        // Compatible with mysql8.0, the number of enum values

const int64_t OB_MAX_VARCHAR_LENGTH_KEY = 16 * 1024L;  // KEY key varchar maximum length limit
const int64_t OB_OLD_MAX_VARCHAR_LENGTH = 64 * 1024;   // for compatible purpose
// For compatibility we set max default value as 256K bytes/64K chars.
// Otherwise inner core tables schema would changes that hard to upgrade.
const int64_t OB_MAX_DEFAULT_VALUE_LENGTH = 256 * 1024L;
const int64_t OB_MAX_BINARY_LENGTH = 255;
const int64_t OB_MAX_VARBINARY_LENGTH = 64 * 1024L;
const int64_t OB_MAX_EXTENDED_TYPE_INFO_LENGTH = OB_MAX_VARBINARY_LENGTH;  // TODO: large object
const int64_t OB_MAX_DECIMAL_PRECISION = 65;
const int64_t OB_MIN_DECIMAL_PRECISION = 1;
const int64_t OB_MAX_DECIMAL_SCALE = 30;
const int64_t OB_MIN_NUMBER_PRECISION = 1;         // Number in Oracle: p:[1, 38]
const int64_t OB_MAX_NUMBER_PRECISION = 38;        // Number in Oracle: p:[1, 38]
const int64_t OB_MAX_NUMBER_PRECISION_INNER = 40;  // Number in Oracle: p can reach 40 if not define by user
const int64_t OB_MIN_NUMBER_SCALE = -84;           // Number in Oracle: s:[-84, 127]
const int64_t OB_MAX_NUMBER_SCALE = 127;           // Number in Oracle: s:[-84, 127]
const int64_t OB_DECIMAL_NOT_SPECIFIED = -1;
const int64_t OB_MIN_NUMBER_FLOAT_PRECISION = 1;  // Float in Oracle: p[1, 126]
const int64_t OB_MAX_NUMBER_FLOAT_PRECISION = 126;
const double OB_PRECISION_BINARY_TO_DECIMAL_FACTOR = 0.30103;
const double OB_PRECISION_DECIMAL_TO_BINARY_FACTOR = 3.32193;

const int64_t OB_MAX_DOUBLE_FLOAT_SCALE = 30;
const int64_t OB_MAX_DOUBLE_FLOAT_PRECISION = 53;  // why?? mysql is 255 TODO
const int64_t OB_MAX_FLOAT_PRECISION = 24;
const int64_t OB_MAX_INTEGER_DISPLAY_WIDTH = 255;  // TINYINT, SMALLINT, MEDIUMINT, INT, BIGINT
const int64_t OB_MAX_DOUBLE_FLOAT_DISPLAY_WIDTH = 255;
const int64_t OB_MAX_COLUMN_NUMBER = OB_ROW_MAX_COLUMNS_COUNT;  // used in ObSchemaManagerV2
const int64_t OB_MAX_PARTITION_KEY_COLUMN_NUMBER = OB_MAX_ROWKEY_COLUMN_NUMBER;
const int64_t OB_MAX_USER_DEFINED_COLUMNS_COUNT = OB_ROW_MAX_COLUMNS_COUNT - OB_APP_MIN_COLUMN_ID;
const int64_t OB_CAST_TO_VARCHAR_MAX_LENGTH = 256;
const int64_t OB_CAST_BUFFER_LENGTH = 256;
const int64_t OB_PREALLOCATED_NUM = 21;  // half of 42
const int64_t OB_PREALLOCATED_COL_ID_NUM = 4;
const int64_t OB_MAX_DATE_PRECISION = 0;
const int64_t OB_MAX_DATETIME_PRECISION = 6;
const int64_t OB_MAX_TIMESTAMP_TZ_PRECISION = 9;

// TODO lob handle length will be much shorter in 2.0
const int64_t OB_MAX_LOB_INLINE_LENGTH = OB_MAX_VARCHAR_LENGTH;
const int64_t OB_MAX_LOB_HANDLE_LENGTH = 2 * 1024L;
const int64_t OB_MAX_TINYTEXT_LENGTH = 256;                  // mysql (1LL << 8)
const int64_t OB_MAX_TEXT_LENGTH = 64 * 1024L;               // mysql (1LL << 16)
const int64_t OB_MAX_MEDIUMTEXT_LENGTH = 16 * 1024 * 1024L;  // mysql (1LL << 24)
const int64_t OB_MAX_LONGTEXT_LENGTH = 48 * 1024 * 1024L;    // mysql (1LL << 32)
const int64_t OB_MAX_MEDIUMTEXT_LENGTH_OLD = 256 * 1024L;    // for compatibility
const int64_t OB_MAX_LONGTEXT_LENGTH_OLD = 512 * 1024L;      // for compatibility

const char* const SYS_DATE = "$SYS_DATE";
const char* const OB_DEFAULT_COMPRESS_FUNC_NAME = "none";
const char* const OB_DEFAULT_FULLTEXT_PARSER_NAME = "TAOBAO_CHN";

const int64_t OB_MYSQL_LOGIN_USER_NAME_MAX_LEN = 48;
const int64_t OB_MAX_CONFIG_NAME_LEN = 128;
const int64_t OB_MAX_CONFIG_VALUE_LEN = 64 * 1024;
const int64_t OB_MAX_CONFIG_TYPE_LENGTH = 128;
const int64_t OB_MAX_CONFIG_INFO_LEN = 4096;
const int64_t OB_MAX_CONFIG_SECTION_LEN = 128;
const int64_t OB_MAX_CONFIG_VISIBLE_LEVEL_LEN = 64;
const int64_t OB_MAX_CONFIG_SCOPE_LEN = 64;
const int64_t OB_MAX_CONFIG_SOURCE_LEN = 64;
const int64_t OB_MAX_CONFIG_EDIT_LEVEL_LEN = 128;
const int64_t OB_MAX_CONFIG_NUMBER = 1024;
const int64_t OB_MAX_EXTRA_CONFIG_LENGTH = 4096;
const int64_t OB_TABLET_MAX_REPLICA_COUNT = 6;

// all_outline related
const int64_t OB_MAX_OUTLINE_CATEGORY_NAME_LENGTH = 64;
const int64_t OB_MAX_OUTLINE_SIGNATURE_LENGTH = OB_MAX_VARBINARY_LENGTH;
const int64_t OB_MAX_OUTLINE_PARAMS_LENGTH = OB_MAX_VARBINARY_LENGTH;
const int64_t OB_MAX_HINT_FORMAT_LENGTH = 16;

// procedure related
const int64_t OB_MAX_PROC_ENV_LENGTH = 2048;
const int64_t OB_MAX_PROC_PARAM_COUNT = 65536;

// udt related
static const int64_t OB_MAX_TYPE_ATTR_COUNT = 65536;

const int64_t OB_AUTO_PROGRESSIVE_MERGE_NUM = 100;
const int64_t OB_DEFAULT_PROGRESSIVE_MERGE_NUM = 0;
const int64_t OB_DEFAULT_PROGRESSIVE_MERGE_ROUND = 1;
const int64_t OB_DEFAULT_STORAGE_FORMAT_VERSION = 3;
const int64_t OB_DEFAULT_MACRO_BLOCK_SIZE = 2 << 20;           // 2MB
const int64_t OB_DEFAULT_SSTABLE_BLOCK_SIZE = 16 * 1024;       // 16KB
const int64_t OB_DEFAULT_MAX_TABLET_SIZE = 256 * 1024 * 1024;  // 256MB
const int64_t OB_MAX_MACRO_BLOCK_TYPE = 16;
const int32_t OB_DEFAULT_CHARACTER_SET = 33;         // UTF8
const int64_t OB_MYSQL_PACKET_BUFF_SIZE = 6 * 1024;  // 6KB
const int64_t OB_MAX_THREAD_NUM = 4096;
const int64_t OB_RESERVED_THREAD_NUM = 128;  // Naked threads created with pthread_create, such as easy
const int32_t OB_MAX_SYS_BKGD_THREAD_NUM = 64;
#if __x86_64__
const int64_t OB_MAX_CPU_NUM = 64;
#elif __aarch64__
const int64_t OB_MAX_CPU_NUM = 128;
#endif
const int64_t OB_MAX_STATICS_PER_TABLE = 128;

const uint64_t OB_DEFAULT_INDEX_ATTRIBUTES_SET = 0;
const uint64_t OB_DEFAULT_INDEX_VISIBILITY = 0;  // 0 means visible;1 means invisible

const int64_t OB_INDEX_WRITE_START_DELAY = 20 * 1000 * 1000;  // 20s

const int64_t MAX_SQL_ERR_MSG_LENGTH = 256;
const int64_t MSG_SIZE = MAX_SQL_ERR_MSG_LENGTH;
const int64_t OB_DUMP_ROOT_TABLE_TYPE = 1;
const int64_t OB_DUMP_UNUSUAL_TABLET_TYPE = 2;
const int64_t OB_MAX_SYS_VAR_NON_STRING_VAL_LENGTH = 128;
const int64_t OB_MAX_SYS_VAR_VAL_LENGTH = 4096;  // original 128 is too small

// mini minor merge related parameters
const int64_t OB_MIN_MINOR_SSTABLE_ROW_COUNT = 2000000;  // L0 -> L1 row count threashold
const int64_t OB_DEFAULT_COMPACTION_AMPLIFICATION_FACTOR =
    25;  // / mini_sstable_total > minor_sstable_total * OB_DEFAULT_COMPACTION_AMPLIFICATION_FACTOR / 100

// bitset defines
const int64_t OB_DEFAULT_BITSET_SIZE = OB_MAX_TABLE_NUM_PER_STMT;
const int64_t OB_DEFAULT_BITSET_SIZE_FOR_BASE_COLUMN = 64;
const int64_t OB_DEFAULT_BITSET_SIZE_FOR_ALIAS_COLUMN = 32;
const int64_t OB_MAX_BITSET_SIZE = OB_ROW_MAX_COLUMNS_COUNT;
const int64_t OB_DEFAULT_STATEMEMT_LEVEL_COUNT = 16;

// max number of existing ObIStores for each partition,
// which contains ssstore, memstore and frozen stores
const int64_t DEFAULT_STORE_CNT_IN_STORAGE = 8;
const int64_t MAX_SSTABLE_CNT_IN_STORAGE = 64;
const int64_t RESERVED_STORE_CNT_IN_STORAGE =
    8;  // Avoid mistakenly triggering minor or major freeze to cause the problem of unsuccessful merge.
const int64_t MAX_FROZEN_MEMSTORE_CNT_IN_STORAGE = 7;
// some frozen memstores and one active memstore
// Only limited to minor freeze, major freeze is not subject to this restriction
const int64_t MAX_MEMSTORE_CNT_IN_STORAGE = MAX_FROZEN_MEMSTORE_CNT_IN_STORAGE + 1;
const int64_t MAX_TABLE_CNT_IN_STORAGE = MAX_SSTABLE_CNT_IN_STORAGE + MAX_MEMSTORE_CNT_IN_STORAGE;
const int64_t OB_MAX_PARTITION_NUM_MYSQL = 8192;
const int64_t OB_MAX_PARTITION_NUM_ORACLE = 65536;

// Used to indicate the visible range of configuration items and whether to restart after modification to take effect
const char* const OB_CONFIG_SECTION_DEFAULT = "DEFAULT";
const char* const OB_CONFIG_VISIBLE_LEVEL_USER = "USER";
const char* const OB_CONFIG_VISIBLE_LEVEL_SYS = "SYS";
const char* const OB_CONFIG_VISIBLE_LEVEL_DBA = "DBA";
const char* const OB_CONFIG_VISIBLE_LEVEL_MEMORY = "MEMORY";

// Precision in user data type
const int16_t MAX_SCALE_FOR_TEMPORAL = 6;
const int16_t MIN_SCALE_FOR_TEMPORAL = 0;
const int16_t MAX_SCALE_FOR_ORACLE_TEMPORAL = 9;
const int16_t DEFAULT_SCALE_FOR_INTEGER = 0;
const int16_t DEFAULT_NUMBER_PRECISION_FOR_INTEGER = 38;
const int16_t DEFAULT_NUMBER_SCALE_FOR_INTEGER = 0;
const int16_t DEFAULT_SCALE_FOR_BIT = 0;
const int16_t DEFAULT_LENGTH_FOR_NUMERIC = -1;
const int16_t DEFAULT_SCALE_FOR_DATE = 0;
const int16_t DEFAULT_SCALE_FOR_YEAR = 0;
const int16_t SCALE_UNKNOWN_YET = -1;
const int16_t ORA_NUMBER_SCALE_UNKNOWN_YET = OB_MIN_NUMBER_SCALE - 1;
const int16_t PRECISION_UNKNOWN_YET = -1;
const int16_t LENGTH_UNKNOWN_YET = -1;
const int16_t DEFAULT_PRECISION_FOR_BOOL = 1;
const int16_t DEFAULT_PRECISION_FOR_TEMPORAL = -1;
const int16_t DEFAULT_LENGTH_FOR_TEMPORAL = -1;
const int16_t DEFAULT_PRECISION_FOR_STRING = -1;
const int16_t DEFAULT_SCALE_FOR_STRING = -1;
const int16_t DEFAULT_SCALE_FOR_TEXT = 0;
const int16_t DEFAULT_SCALE_FOR_ORACLE_FRACTIONAL_SECONDS =
    6;  // SEE : https://docs.oracle.com/cd/B19306_01/server.102/b14225/ch4datetime.htm
const int16_t DEFUALT_PRECISION_FOR_INTERVAL = 2;

#define NUMBER_SCALE_UNKNOWN_YET (lib::is_oracle_mode() ? ORA_NUMBER_SCALE_UNKNOWN_YET : SCALE_UNKNOWN_YET)

const int64_t OB_MAX_FAILLIST_LENGTH = 1024;

const int64_t OB_MAX_MODE_CNT = 2;
const uint32_t BATCH_RPC_PORT_DELTA = 0;
const uint32_t HIGH_PRIO_RPC_PORT_DELTA = 0;

const int64_t UNIQ_TASK_QUEUE_BATCH_EXECUTE_NUM = 128;

const char* const OB_SSL_CA_FILE = "wallet/ca.pem";
const char* const OB_SSL_CERT_FILE = "wallet/server-cert.pem";
const char* const OB_SSL_KEY_FILE = "wallet/server-key.pem";

enum ObDmlType {
  OB_DML_UNKNOW = 0,
  OB_DML_REPLACE = 1,
  OB_DML_INSERT = 2,
  OB_DML_UPDATE = 3,
  OB_DML_DELETE = 4,
  OB_DML_MERGED = 5,
  OB_DML_NUM,
};

// check whether an id is valid
OB_INLINE bool is_valid_id(const uint64_t id)
{
  return (OB_INVALID_ID != id);
}
// check whether an index is valid
OB_INLINE bool is_valid_idx(const int64_t idx)
{
  return (0 <= idx);
}

// check whether an tenant_id is valid
OB_INLINE bool is_valid_tenant_id(const uint64_t tenant_id)
{
  return (0 < tenant_id) && (OB_INVALID_ID != tenant_id);
}

// Tenants who can use gts, system tenants do not use gts, to avoid circular dependencies
OB_INLINE bool is_valid_no_sys_tenant_id(const uint64_t tenant_id)
{
  return is_valid_tenant_id(tenant_id) && (OB_SYS_TENANT_ID != tenant_id);
}

OB_INLINE bool is_valid_gts_id(const uint64_t gts_id)
{
  return OB_INVALID_ID != gts_id && 0 != gts_id;
}

// check whether an cluster_id is valid
OB_INLINE bool is_valid_cluster_id(const int64_t cluster_id)
{
  return (cluster_id >= OB_MIN_CLUSTER_ID && cluster_id <= OB_MAX_CLUSTER_ID);
}

// check whether an tenant_id is virtual
OB_INLINE bool is_virtual_tenant_id(const uint64_t tenant_id)
{
  return (OB_SYS_TENANT_ID < tenant_id && tenant_id <= OB_MAX_RESERVED_TENANT_ID);
}

OB_INLINE bool is_virtual_tenant_for_memory(const uint64_t tenant_id)
{
  return is_virtual_tenant_id(tenant_id) &&
         (OB_EXT_LOG_TENANT_ID == tenant_id || OB_RS_TENANT_ID == tenant_id || OB_DIAG_TENANT_ID == tenant_id);
}

enum ObNameCaseMode {
  OB_NAME_CASE_INVALID = -1,
  OB_ORIGIN_AND_SENSITIVE = 0,       // stored using lettercase, and name comparisons are case sensitive
  OB_LOWERCASE_AND_INSENSITIVE = 1,  // stored using lowercase, and name comparisons are case insensitive
  OB_ORIGIN_AND_INSENSITIVE = 2,     // stored using lettercase, and name comparisons are case insensitive
  OB_NAME_CASE_MAX,
};

enum ObFreezeStatus {
  INIT_STATUS = 0,
  PREPARED_SUCCEED,
  COMMIT_SUCCEED,
  FREEZE_STATUS_MAX,
};

enum ObModifyQuorumType {
  WITH_MODIFY_QUORUM = 0,
  WITHOUT_MODIFY_QUORUM,
  MAX_MODIFY_QUORUM_TYPE,
};

class ObModifyQuorumTypeChecker {
public:
  static bool is_valid_type(const ObModifyQuorumType modify_quorum_type)
  {
    return modify_quorum_type >= WITH_MODIFY_QUORUM && modify_quorum_type < MAX_MODIFY_QUORUM_TYPE;
  }
};

/*
 * |--- 4 bits ---|--- 2 bits ---|--- 2 bits ---| LSB
 * |---  clog  ---|-- SSStore ---|--- MemStore--| LSB
 */
const int64_t MEMSTORE_BITS_SHIFT = 0;
const int64_t SSSTORE_BITS_SHIFT = 2;
const int64_t CLOG_BITS_SHIFT = 4;
const int64_t REPLICA_TYPE_MEMSTORE_MASK = (0x3UL << MEMSTORE_BITS_SHIFT);
const int64_t REPLICA_TYPE_SSSTORE_MASK = (0x3UL << SSSTORE_BITS_SHIFT);
const int64_t REPLICA_TYPE_CLOG_MASK = (0xFUL << CLOG_BITS_SHIFT);
// replica type associated with memstore
const int64_t WITH_MEMSTORE = 0;
const int64_t WITHOUT_MEMSTORE = 1;
// replica type associated with ssstore
const int64_t WITH_SSSTORE = 0 << SSSTORE_BITS_SHIFT;
const int64_t WITHOUT_SSSTORE = 1 << SSSTORE_BITS_SHIFT;
// replica type associated with clog
const int64_t SYNC_CLOG = 0 << CLOG_BITS_SHIFT;
const int64_t ASYNC_CLOG = 1 << CLOG_BITS_SHIFT;

// Need to manually maintain the replica_type_to_str function in utility.cpp,
// Currently there are only three types: REPLICA_TYPE_FULL, REPLICA_TYPE_READONLY, and REPLICA_TYPE_LOGONLY
enum ObReplicaType {
  // Almighty copy: is a member of paxos; has ssstore; has memstore
  REPLICA_TYPE_FULL = (SYNC_CLOG | WITH_SSSTORE | WITH_MEMSTORE),  // 0
                                                                   // Backup copy: Paxos member; ssstore; no memstore
  REPLICA_TYPE_BACKUP = (SYNC_CLOG | WITH_SSSTORE |
                         WITHOUT_MEMSTORE),  // 1
                                             // Memory copy; no ssstore; memstore
                                             // REPLICA_TYPE_MMONLY = (SYNC_CLOG | WITHOUT_SSSTORE | WITH_MEMSTORE), //
                                             // 4 Journal copy: Paxos member; no ssstore; no memstore
  REPLICA_TYPE_LOGONLY =
      (SYNC_CLOG | WITHOUT_SSSTORE | WITHOUT_MEMSTORE),  // 5
                                                         // Read-only copy: not a member of paxos; ssstore; memstore
  REPLICA_TYPE_READONLY =
      (ASYNC_CLOG | WITH_SSSTORE | WITH_MEMSTORE),  // 16
                                                    // Incremental copy: not a member of paxos; no ssstore; memstore
  REPLICA_TYPE_MEMONLY = (ASYNC_CLOG | WITHOUT_SSSTORE | WITH_MEMSTORE),  // 20
                                                                          // invalid value
  REPLICA_TYPE_MAX,
};

class ObReplicaTypeCheck {
public:
  static bool is_replica_type_valid(const int32_t replica_type)
  {
    return REPLICA_TYPE_FULL == replica_type || REPLICA_TYPE_LOGONLY == replica_type ||
           REPLICA_TYPE_READONLY == replica_type;
  }
  static bool is_can_elected_replica(const int32_t replica_type)
  {
    return (REPLICA_TYPE_FULL == replica_type);
  }
  static bool is_readonly_replica(const int32_t replica_type)
  {
    return (REPLICA_TYPE_READONLY == replica_type);
  }
  static bool is_log_replica(const int32_t replica_type)
  {
    return (REPLICA_TYPE_LOGONLY == replica_type);
  }
  static bool is_paxos_replica_V2(const int32_t replica_type)
  {
    return (replica_type >= REPLICA_TYPE_FULL && replica_type <= REPLICA_TYPE_LOGONLY);
  }
  static bool is_paxos_replica(const int32_t replica_type)
  {
    return (replica_type >= REPLICA_TYPE_FULL && replica_type <= REPLICA_TYPE_LOGONLY);
  }
  static bool is_writable_replica(const int32_t replica_type)
  {
    return (REPLICA_TYPE_FULL == replica_type);
  }
  static bool is_readable_replica(const int32_t replica_type)
  {
    return (REPLICA_TYPE_FULL == replica_type || REPLICA_TYPE_READONLY == replica_type);
  }
  static bool is_replica_with_memstore(const ObReplicaType replica_type)
  {
    return (0 == (replica_type & REPLICA_TYPE_MEMSTORE_MASK));
  }
  static bool is_replica_with_ssstore(const ObReplicaType replica_type)
  {
    return (0 == (replica_type & REPLICA_TYPE_SSSTORE_MASK));
  }
  static bool is_replica_need_split(const ObReplicaType replica_type)
  {
    return (REPLICA_TYPE_FULL == replica_type || REPLICA_TYPE_READONLY == replica_type);
  }
  static bool can_as_data_source(const int32_t dest_replica_type, const int32_t src_replica_type)
  {
    return (
        dest_replica_type == src_replica_type ||
        REPLICA_TYPE_FULL == src_replica_type);  // TODO temporarily only supports the same type or F as the data source
  }
  // Currently only copies of F and R can be used for machine reading, not L
  static bool can_slave_read_replica(const int32_t replica_type)
  {
    return (REPLICA_TYPE_FULL == replica_type || REPLICA_TYPE_READONLY == replica_type);
  }

  static bool change_replica_op_allow(const ObReplicaType source, const ObReplicaType target)
  {
    bool bool_ret = false;

    if (REPLICA_TYPE_FULL == source) {
      bool_ret = true;
    } else if (REPLICA_TYPE_READONLY == source && REPLICA_TYPE_FULL == target) {
      bool_ret = true;
    } else if (REPLICA_TYPE_LOGONLY == source && REPLICA_TYPE_FULL == target) {
      bool_ret = false;  // TODO: fix it
    }

    return bool_ret;
  }
};

class ObMemstorePercentCheck {
public:
  static bool is_memstore_percent_valid(const int64_t memstore_percent)
  {
    return 0 == memstore_percent || 100 == memstore_percent;
  }
};

enum ObReplicaOpPriority { PRIO_HIGH = 0, PRIO_LOW = 1, PRIO_MID = 2, PRIO_INVALID };

inline bool is_replica_op_priority_valid(const ObReplicaOpPriority priority)
{
  return priority >= ObReplicaOpPriority::PRIO_HIGH && priority < ObReplicaOpPriority::PRIO_INVALID;
}

enum ObMetaTableMode {
  METATABLE_MODE_SYS_ONLY = 0,
  METATABLE_MODE_DOUBLE_WRITE = 1,
  METATABLE_MODE_TENANT_ONLY = 2,
  METATABLE_MODE_INVALID,
};

char* lbt();

enum ObConsistencyLevel {
  INVALID_CONSISTENCY = -1,
  FROZEN = 1,
  WEAK,
  STRONG,
};

enum ObCompatibilityMode {
  OCEANBASE_MODE = -1,
  MYSQL_MODE = 0,
  ORACLE_MODE,
};

enum ObOrderType {
  ASC = 0,
  DESC = -1,
};

enum ObJITEnableMode {
  OFF = 0,
  AUTO = 1,
  FORCE = 2,
};

}  // end namespace common
}  // end namespace oceanbase

// For the serialize function pos is both an input parameter and an output parameter,
// serialize writes the serialized data from (buf+pos),
// Update pos after writing is completed. If the data after writing exceeds (buf+buf_len),
// serialize returned failed.
//
// For the deserialize function pos is both an input parameter and an output parameter,
// deserialize reads data from (buf+pos) for deserialization,
// Update pos after completion. If the data required for deserialization exceeds (buf+data_len),
// deserialize returned failed.

#define NEED_SERIALIZE_AND_DESERIALIZE                                    \
  int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;    \
  int deserialize(const char* buf, const int64_t data_len, int64_t& pos); \
  int64_t get_serialize_size(void) const

#define INLINE_NEED_SERIALIZE_AND_DESERIALIZE                                    \
  inline int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;    \
  inline int deserialize(const char* buf, const int64_t data_len, int64_t& pos); \
  inline int64_t get_serialize_size(void) const

#define VIRTUAL_NEED_SERIALIZE_AND_DESERIALIZE                                    \
  virtual int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;    \
  virtual int deserialize(const char* buf, const int64_t data_len, int64_t& pos); \
  virtual int64_t get_serialize_size(void) const

#define PURE_VIRTUAL_NEED_SERIALIZE_AND_DESERIALIZE                                   \
  virtual int serialize(char* buf, const int64_t buf_len, int64_t& pos) const = 0;    \
  virtual int deserialize(const char* buf, const int64_t data_len, int64_t& pos) = 0; \
  virtual int64_t get_serialize_size(void) const = 0

#define DEFINE_SERIALIZE(TypeName) int TypeName::serialize(char* buf, const int64_t buf_len, int64_t& pos) const

#define DEFINE_DESERIALIZE(TypeName) int TypeName::deserialize(const char* buf, const int64_t data_len, int64_t& pos)

#define DEFINE_GET_SERIALIZE_SIZE(TypeName) int64_t TypeName::get_serialize_size(void) const

#define DATABUFFER_SERIALIZE_INFO data_buffer_.get_data(), data_buffer_.get_capacity(), data_buffer_.get_position()

#define DIO_ALIGN_SIZE 512
#define DIO_READ_ALIGN_SIZE 4096
#define DIO_ALLOCATOR_CACHE_BLOCK_SIZE (OB_DEFAULT_MACRO_BLOCK_SIZE + DIO_READ_ALIGN_SIZE)
#define CORO_INIT_PRIORITY 120
#define MALLOC_INIT_PRIORITY 128
#define NORMAL_INIT_PRIORITY (MALLOC_INIT_PRIORITY + 1)

// judge int64_t multiplication whether overflow
inline bool is_multi_overflow64(int64_t a, int64_t b)
{
  bool ret = false;
  if (0 == b || 0 == a) {
    ret = false;
  }
  // min / -1 will overflow, so can't use the next rule to judge
  else if (-1 == b) {
    if (INT64_MIN == a) {
      ret = true;
    } else {
      ret = false;
    }
  } else if (a > 0 && b > 0) {
    ret = INT64_MAX / b < a;
  } else if (a < 0 && b < 0) {
    ret = INT64_MAX / b > a;
  } else if (a > 0 && b < 0) {
    ret = INT64_MIN / b < a;
  } else {
    ret = INT64_MIN / b > a;
  }
  return ret;
}
#define IS_MULTI_OVERFLOW64(a, b) is_multi_overflow64(a, b)

#define IS_ADD_OVERFLOW64(a, b, ret)                                               \
  ((0 == ((static_cast<uint64_t>(a) >> 63) ^ (static_cast<uint64_t>(b) >> 63))) && \
      (1 == ((static_cast<uint64_t>(a) >> 63) ^ (static_cast<uint64_t>(ret) >> 63))))

#ifdef __ENABLE_PRELOAD__
#include "lib/utility/ob_preload.h"
#endif

struct ObNumberDesc {
  explicit ObNumberDesc() : desc_(0)
  {}
  explicit ObNumberDesc(const uint32_t desc) : desc_(desc)
  {}
  explicit ObNumberDesc(const uint8_t len, uint8_t cap, uint8_t flag, uint8_t exp, uint8_t sign)
      : len_(len), cap_(cap), flag_(flag), exp_(exp), sign_(sign)
  {}
  union {
    uint32_t desc_;
    struct {
      uint8_t len_;
      uint8_t cap_;
      uint8_t flag_;
      union {
        uint8_t se_;
        struct {
          uint8_t exp_ : 7;
          uint8_t sign_ : 1;
        };
      };
    };
  };
};

#define DEFINE_ALLOCATOR_WRAPPER                                             \
  class IAllocator {                                                         \
  public:                                                                    \
    virtual ~IAllocator(){};                                                 \
    virtual void* alloc(const int64_t size) = 0;                             \
    virtual void* alloc(const int64_t size, const lib::ObMemAttr& attr) = 0; \
  };                                                                         \
  template <class T>                                                         \
  class TAllocator : public IAllocator {                                     \
  public:                                                                    \
    explicit TAllocator(T& allocator) : allocator_(allocator){};             \
    void* alloc(const int64_t size)                                          \
    {                                                                        \
      return allocator_.alloc(size);                                         \
    };                                                                       \
    void* alloc(const int64_t size, const lib::ObMemAttr& attr)              \
    {                                                                        \
      UNUSED(attr);                                                          \
      return alloc(size);                                                    \
    };                                                                       \
                                                                             \
  private:                                                                   \
    T& allocator_;                                                           \
  };

// need define:
//   ObRow row;
//   ObObj obj;
#define ROW_CELL_SET_VAL(table_id, type, val)                                                \
  obj.set_##type(val);                                                                       \
  if (OB_SUCCESS == ret && OB_SUCCESS != (ret = row.set_cell(table_id, ++column_id, obj))) { \
    _OB_LOG(WARN, "failed to set cell=%s, ret=%d", to_cstring(obj), ret);                    \
  }

inline int64_t& get_tid_cache()
{
  // NOTE: cache tid
  static __thread int64_t tid = -1;
  return tid;
}

inline bool& get_ignore_mem_limit()
{
  static __thread bool ignore_mem_limit = false;
  return ignore_mem_limit;
}

inline uint32_t& get_writing_throttling_sleep_interval()
{
  static __thread uint32_t writing_throttling_sleep_interval = 0;
  return writing_throttling_sleep_interval;
}

// should be called after fork/daemon
inline void reset_tid_cache()
{
  get_tid_cache() = -1;
}

inline int64_t ob_gettid()
{
  int64_t& tid = get_tid_cache();
  if (OB_UNLIKELY(tid <= 0)) {
    tid = static_cast<int64_t>(syscall(__NR_gettid));
  }
  return tid;
}
#define GETTID() ob_gettid()
#define gettid GETTID

// for explain
#define LEFT_BRACKET "("
#define RIGHT_BRACKET ")"
#define COMMA_REVERSE ",Reverse"
#define BRACKET_REVERSE "(Reverse)"

#define ORALCE_LITERAL_PREFIX_DATE "DATE"
#define ORALCE_LITERAL_PREFIX_TIMESTAMP "TIMESTAMP"
#define ORACLE_LITERAL_PREFIX_INTERVAL "INTERVAL"
inline bool is_x86()
{
#if defined(__x86_64__)
  return true;
#else
  return false;
#endif
}

#endif  // OCEANBASE_COMMON_DEFINE_H_
