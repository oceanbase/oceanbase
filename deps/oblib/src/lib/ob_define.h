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
#include <stdio.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <netinet/in.h>
// basic headers, do not add other headers here
#include "lib/coro/co_var.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/alloc/alloc_assist.h"
#include "lib/ob_errno.h"

namespace oceanbase
{
namespace common
{

// ******************* Object ID Type Definition ********************
// All objects have common ID Type, including Table/Partition and so on.
// Define 'uint64_t' as Object ID Type for code-compatibility temporarily.
// TODO: Use 'int64_t' instead when all objects are ready.
// See: docs on yuque rootservice/lvnlgi

// Common Object ID Type
typedef uint64_t ObObjectID;

// Table Object ID Type
typedef ObObjectID ObTableID;

// Level-One Partition Object ID
typedef ObObjectID ObPartID;

// Level-Two Partition Object ID
typedef ObObjectID ObSubPartID;
// ****************************************************************


const int64_t OB_ALL_SERVER_CNT = INT64_MAX;
const uint16_t OB_COMPACT_COLUMN_INVALID_ID = UINT16_MAX;
const int64_t OB_INVALID_TIMESTAMP = -1;
const int64_t OB_INVALID_LOG_ID = -1;
const uint64_t OB_INVALID_ID = UINT64_MAX;
const int64_t OB_INVALID_DISK_ID = -1;
const int32_t OB_INVALID_FD = -1;
const int64_t OB_INVALID_SVR_SEQ = -1;
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
const int64_t OB_INVALID_PARTITION_ID = 0;
const int64_t OB_MIN_CLUSTER_ID = 1;
const int64_t OB_MAX_CLUSTER_ID = 4294901759;
const int64_t OB_INVALID_CLUSTER_ID = -1;
const int64_t OB_INVALID_ORG_CLUSTER_ID = 0;
const int64_t OB_MAX_ITERATOR = 16;
const int64_t MAX_IP_ADDR_LENGTH = INET6_ADDRSTRLEN;
const int64_t MAX_IP_PORT_LENGTH = MAX_IP_ADDR_LENGTH + 6;
const int64_t MAX_IP_PORT_SQL_LENGTH = MAX_IP_ADDR_LENGTH + 12;
const uint64_t MAX_IFNAME_LENGTH = 128;
const int64_t OB_MAX_SNAPSHOT_SOURCE_LENGTH = 128;
const int64_t OB_MAX_SQL_ID_LENGTH = 32;
const int64_t OB_MAX_CLIENT_INFO_LENGTH = 64;
const int64_t OB_MAX_MOD_NAME_LENGTH = 48;
const int64_t OB_MAX_ACT_NAME_LENGTH = 32;
const int64_t OB_MAX_UUID_LENGTH = 16;
const int64_t OB_MAX_UUID_STR_LENGTH = 36;
const int64_t OB_MAX_CON_INFO_STR_LENGTH = 512;
const int64_t MAX_LOAD_DATA_MESSAGE_LENGTH = 4096;
const int64_t MAX_ZONE_LENGTH = 128;
const int64_t MAX_REGION_LENGTH = 128;
const int64_t MAX_GTS_NAME_LENGTH = 128;
const int32_t MAX_ZONE_NUM = 64;
const int32_t DEFAULT_ZONE_COUNT = 5;
const int64_t MAX_OPERATOR_NAME_LENGTH = 32;
const int64_t MAX_LONG_OPS_NAME_LENGTH = 128;
const int64_t MAX_LONG_OPS_TARGET_LENGTH = 128;
const int64_t MAX_LONG_OPS_MESSAGE_LENGTH = 512;
const int64_t MAX_LS_STATE_LENGTH = 16;
const int64_t MAX_LOCK_ID_BUF_LENGTH = 64;
const int64_t MAX_LOCK_ROWKEY_BUF_LENGTH = 512;
const int64_t MAX_LOCK_MODE_BUF_LENGTH = 8;
const int64_t MAX_LOCK_OBJ_TYPE_BUF_LENGTH = 16;
const int64_t MAX_LOCK_OP_TYPE_BUF_LENGTH = 32;
const int64_t MAX_LOCK_OP_STATUS_BUF_LENGTH = 16;
const int64_t MAX_LOCK_OP_EXTRA_INFO_LENGTH = 256;
const int64_t MAX_LOCK_DETECT_PARAM_LENGTH = 512;
const int64_t MAX_SERVICE_TYPE_BUF_LENGTH = 32;
const int64_t MAX_CHECKPOINT_TYPE_BUF_LENGTH = 32;
const int64_t MAX_FREEZE_CHECKPOINT_LOCATION_BUF_LENGTH = 16;
const int64_t MAX_ZONE_LIST_LENGTH = MAX_ZONE_LENGTH * MAX_ZONE_NUM;
const int64_t MAX_ZONE_STATUS_LENGTH = 16;
const int64_t MAX_REPLICA_STATUS_LENGTH = 64;
const int64_t MAX_REPLICA_TYPE_LENGTH = 16;
const int64_t MAX_DISASTER_RECOVERY_TASK_TYPE_LENGTH = 64;
const int64_t MAX_ARB_REPLICA_TASK_TYPE_LENGTH = 32;
const int64_t MAX_TENANT_STATUS_LENGTH = 64;
const int64_t MAX_RESOURCE_POOL_NAME_LEN = 128;
const int32_t MAX_REPLICA_COUNT_PER_ZONE = 5;
const int32_t MAX_REPLICA_COUNT_TOTAL = MAX_ZONE_NUM
                                        *MAX_REPLICA_COUNT_PER_ZONE;
const int64_t MAX_RESOURCE_POOL_LENGTH = 128;
const int64_t MAX_RESOURCE_POOL_COUNT_OF_TENANT = 16;
const int64_t MAX_RESOURCE_POOL_LIST_LENGTH = MAX_RESOURCE_POOL_LENGTH
    * MAX_RESOURCE_POOL_COUNT_OF_TENANT;
const int64_t MAX_UNIT_CONFIG_LENGTH = 128;
const int64_t MAX_UNIT_STATUS_LENGTH = 128;
const int64_t MAX_PATH_SIZE = 1024;
const int64_t MAX_DISK_ALIAS_NAME = 128;
const int64_t MAX_DISKGROUP_NAME = 128;
const int64_t DEFAULT_BUF_LENGTH = 4096;
const int64_t MAX_SINGLE_MEMBER_LENGTH = (MAX_IP_PORT_LENGTH + 17 /* timestamp length*/  + 1);
const int64_t MAX_MEMBER_LIST_LENGTH = MAX_ZONE_NUM * (MAX_IP_PORT_LENGTH + 17 /* timestamp length*/  + 1);
const int64_t OB_MAX_MEMBER_NUMBER = 7;
const int64_t OB_MAX_GLOBAL_LEARNER_NUMBER = 2000;
const int64_t MAX_LEARNER_LIST_LENGTH = OB_MAX_GLOBAL_LEARNER_NUMBER * (MAX_IP_PORT_LENGTH + 17 /* timestamp length*/  + 1);
const int64_t OB_MAX_CHILD_MEMBER_NUMBER = 15;
const int64_t OB_MAX_CHILD_MEMBER_NUMBER_IN_FOLLOWER = 5;
const int64_t OB_DEFAULT_MEMBER_NUMBER = 3;
const int64_t MAX_VALUE_LENGTH = 4096;
const int64_t MAX_LLC_BITMAP_LENGTH = 4096;
const int64_t MAX_CLUSTER_EVENT_NAME_LENGTH = 256;
const int64_t MAX_CLUSTER_EVENT_VALUE_LENGTH = 4096;
const int64_t MAX_ROOTSERVICE_EVENT_NAME_LENGTH = 256;
const int64_t MAX_ROOTSERVICE_EVENT_VALUE_LENGTH = 512;
const int64_t MAX_ROOTSERVICE_EVENT_DESC_LENGTH = 64;
const int64_t MAX_ROOTSERVICE_EVENT_EXTRA_INFO_LENGTH = 512;
const int64_t MAX_TENANT_EVENT_NAME_LENGTH = 256;
const int64_t MAX_TENANT_EVENT_VALUE_LENGTH = 4096;
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
const int64_t OB_MAX_IOPS = 1000L * 1000L * 1000L;
const int64_t OB_MAX_LOCAL_VARIABLE_SIZE = 8L << 10;
const char *const OB_EMPTY_STR = "";
const char *const OB_NEXT_RESTART_SEQUENCE_PREFIX = "next_restart_sequence_";
const uint64_t SEARRAY_INIT_NUM = 4;
const int64_t OB_DEFAULT_TABLE_DOP = 1;
const int64_t OB_DEFAULT_META_OBJ_PERCENTAGE_LIMIT = 10;
const uint64_t OB_DEFAULT_COLUMN_SRS_ID = 0xffffffffffffffe0;
const int64_t OB_MAX_SPAN_LENGTH = 1024;
const int64_t OB_MAX_SPAN_TAG_LENGTH = 8 * 1024L;
const int64_t OB_MAX_REF_TYPE_LENGTH = 10;
const int64_t OB_MAX_LS_FLAG_LENGTH = 2048;
// The timeout provided to the storage layer will be reduced by 100ms
const int64_t ESTIMATE_PS_RESERVE_TIME = 100 * 1000;
const uint64_t MAX_STMT_TYPE_NAME_LENGTH = 128;

// See ObDeviceHealthStatus for more information
const int64_t OB_MAX_DEVICE_HEALTH_STATUS_STR_LENGTH = 20;

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
const int64_t OB_TINY_SQL_LENGTH = 128;
const int64_t OB_SHORT_SQL_LENGTH = 1 * 1024; // 1KB
const int64_t OB_MEDIUM_SQL_LENGTH = 2 * OB_SHORT_SQL_LENGTH; // 2KB
const int64_t OB_MAX_PROXY_SQL_STORE_LENGTH = 8 * 1024; // 8KB
const int64_t OB_MAX_SERVER_ADDR_SIZE = 128;
const int64_t OB_MAX_JOIN_INFO_NUMBER = 10;
const int64_t OB_MAX_USER_ROW_KEY_LENGTH = 16 * 1024L; // 16K
const int64_t OB_MAX_ROW_KEY_LENGTH = 17 * 1024L; // 1K for extra varchar columns of root table
const int64_t OB_MAX_RANGE_LENGTH = 2 * OB_MAX_ROW_KEY_LENGTH;
const int64_t OB_MAX_ROW_KEY_SPLIT = 32;
const int64_t OB_USER_MAX_ROWKEY_COLUMN_NUMBER = 64;
const int64_t OB_MAX_ROWKEY_COLUMN_NUMBER = 2 * OB_USER_MAX_ROWKEY_COLUMN_NUMBER;
const int64_t OB_MAX_VIEW_COLUMN_NAME_LENGTH_MYSQL  = 64;
const int64_t OB_MAX_COLUMN_NAME_LENGTH = 128; // Compatible with oracle, OB code logic is greater than Times TODO:xiyu
const int64_t OB_MAX_COLUMN_NAME_BINARY_LENGTH = 512; //OB_MAX_COLUMN_NAME_LENGTH * 4 (max character bytes)
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
const int64_t OB_MAX_NAMED_WINDOW_FUNCTION_NUM = 127;
const uint64_t OB_DEFAULT_GROUP_CONCAT_MAX_LEN = 1024;
const uint64_t OB_DEFAULT_GROUP_CONCAT_MAX_LEN_FOR_ORACLE = 32767; //Same as OB_MAX_ORACLE_VARCHAR_LENGTH, expanded to 32767
const int64_t OB_DEFAULT_OB_INTERM_RESULT_MEM_LIMIT = 2L * 1024L * 1024L * 1024L;
// The maximum table name length that the user can specify
const int64_t OB_MAX_USER_TABLE_NAME_LENGTH_MYSQL = 64;  // Compatible with mysql, the OB code logic is greater than the time error
const int64_t OB_MAX_USER_TABLE_NAME_LENGTH_ORACLE = 128; // Compatible with Oracle, error is reported when the logic is greater than
// The actual maximum table name length of table_schema (the index table will have an additional prefix, so the actual length is greater than OB_MAX_USER_TABLE_NAME_LENGTH)
const int64_t OB_MAX_TABLE_NAME_LENGTH = 256;
const int64_t OB_MAX_TABLE_NAME_BINARY_LENGTH = 2048; // See OB_MAX_DATABASE_NAME_BINARY_LENGTH explaination
const int64_t OB_MAX_SCHEMA_REF_INFO = 4096;
const int64_t OB_MAX_TABLE_NAME_BUF_LENGTH = OB_MAX_TABLE_NAME_LENGTH + 1;
const int64_t OB_MAX_PLAN_EXPLAIN_NAME_LENGTH= 256;
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
//change from 128 to 64, according to production definition document
const int64_t OB_MAX_RESERVED_POINT_TYPE_LENGTH = 32;
const int64_t OB_MAX_RESERVED_POINT_NAME_LENGTH = 128;
const int64_t OB_MAX_EXTRA_ROWKEY_COLUMN_NUMBER = 2; //storage extra rowkey column number, it contains trans version column and sql sequence column
const int64_t OB_INNER_MAX_ROWKEY_COLUMN_NUMBER = OB_MAX_ROWKEY_COLUMN_NUMBER + OB_MAX_EXTRA_ROWKEY_COLUMN_NUMBER;
const int64_t OB_MAX_TENANT_SNAPSHOT_NAME_LENGTH = 64;
const int64_t OB_MAX_TENANT_SNAPSHOT_NAME_LENGTH_STORE = 128;


//for recybin
const int64_t OB_MAX_OBJECT_NAME_LENGTH = 128; //should include index_name
const int64_t OB_MAX_ORIGINAL_NANE_LENGTH = 256; //max length of tenant_name, table_name, db_name

const int64_t OB_MAX_CHAR_LEN = 3;
const int64_t OB_MAX_POINTER_ADDR_LEN = 32;
const int64_t OB_MAX_TRIGGER_NAME_LENGTH = 128;  // Compatible with Oracle
const int64_t OB_MAX_WHEN_CONDITION_LENGTH = 4000;  // Compatible with Oracle
const int64_t OB_MAX_UPDATE_COLUMNS_LENGTH = 4000;  // Compatible with Oracle
const int64_t OB_MAX_TRIGGER_BODY_LENGTH = 64 * 1024;  // In Oracle, it is the LONG type, but there is a problem with the large object type used in the OB internal table.
const int64_t OB_MAX_DBLINK_NAME_LENGTH = 128;  // Compatible with Oracle
const int64_t OB_MAX_DOMIN_NAME_LENGTH = 240;   // max length of domin name, refer to max domin name of oracle
const int64_t OB_MAX_QB_NAME_LENGTH = 20;  // Compatible with Oracle, hint specifies the length of the maximum qb_name.
const int64_t OB_MAX_SEQUENCE_NAME_LENGTH = 128; // Compatible with Oracle, error is reported when the logic is greater than
const int64_t OB_MAX_KEYSTORE_NAME_LENGTH = 128;
const int64_t OB_MAX_DATABASE_NAME_LENGTH = 128; // Not compatible with mysql (mysql is 64), the logic is greater than when an error is reported
const int64_t OB_MAX_DATABASE_NAME_BINARY_LENGTH = 2048; // Should be OB_MAX_DATABASE_NAME_LENGTH * 4(max char bytes),
                                                         // reserve some bytes thus OB_MAX_DATABASE_NAME_LENGTH changes will probably not influence it
                                                         // it is defined in primary key, and can not change randomly.
const int64_t OB_MAX_DATABASE_NAME_BUF_LENGTH = OB_MAX_DATABASE_NAME_LENGTH + 1;
const int64_t OB_MAX_TABLEGROUP_NAME_LENGTH = 128; // OB code logic is greater than or equal to an error, so modify it to 65
const int64_t OB_MAX_ALIAS_NAME_LENGTH = 255;// Compatible with mysql, 255 visible characters. Plus 256 bytes at the end of 0
const int64_t OB_MAX_CONSTRAINT_NAME_LENGTH_ORACLE = 128;  // Compatible with Oracle, error is reported when the logic is greater than
const int64_t OB_MAX_CONSTRAINT_NAME_LENGTH_MYSQL = 64; // Compatible with mysql, error is reported when the logic is greater than
const int64_t OB_MAX_CONSTRAINT_EXPR_LENGTH = 2048;
const int64_t OB_MAX_TABLESPACE_NAME_LENGTH = 128;
const int64_t OB_MAX_UDF_NAME_LENGTH = 64;
const int64_t OB_MAX_DL_NAME_LENGTH = 128;
const int64_t OB_MAX_USER_NAME_LENGTH = 64; // Not compatible with Oracle (Oracle is 128), the logic is greater than when an error is reported
const int64_t OB_MAX_USER_NAME_BUF_LENGTH = OB_MAX_USER_NAME_LENGTH + 1;
const int64_t OB_MAX_USER_NAME_LENGTH_STORE = 128;
const int64_t OB_MAX_INFOSCHEMA_GRANTEE_LEN = 81;
const int64_t OB_MAX_USER_INFO_LENGTH = 4096;
const int64_t OB_MAX_COMMAND_LENGTH = 4096;
const int64_t OB_MAX_SESSION_STATE_LENGTH = 128;
const int64_t OB_MAX_SESSION_INFO_LENGTH = 128;
const int64_t OB_MAX_TRANS_STATE_LENGTH = 32;
const int64_t OB_MAX_DUP_TABLE_TABLET_SET_ATTR_LENGTH = 16;
const int64_t OB_MAX_DUP_TABLE_TABLET_SET_STATE_LENGTH = 16;
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
const int64_t OB_MAX_PACKET_LENGTH = 1 << 26; // max packet length, 64MB
const int64_t OB_MAX_PACKET_BUFFER_LENGTH = (1 << 26) - (1 << 20); // buffer length for max packet, 63MB
const int64_t OB_MAX_ROW_NUMBER_PER_QUERY = 65536;
const int64_t OB_MAX_BATCH_NUMBER = 100;
const int64_t OB_MAX_TABLET_LIST_NUMBER = 64;
const int64_t OB_MAX_DISK_NUMBER = 32;
const int64_t OB_MAX_TIME_STR_LENGTH = 64;
const int64_t OB_IP_STR_BUFF = MAX_IP_ADDR_LENGTH; //TODO: xiyu  uniform IP/PORR length
const int64_t OB_IP_PORT_STR_BUFF = 64;
const int64_t OB_RANGE_STR_BUFSIZ = 512;
const int64_t OB_MAX_FETCH_CMD_LENGTH = 2048;
const int64_t OB_MAX_EXPIRE_INFO_STRING_LENGTH = 4096;
const int64_t OB_MAX_PART_FUNC_EXPR_LENGTH = 4096;
const int64_t OB_MAX_PART_FUNC_BIN_EXPR_LENGTH = 2 * OB_MAX_PART_FUNC_EXPR_LENGTH;
const int64_t OB_MAX_PART_FUNC_BIN_EXPR_STRING_LENGTH = 2 * OB_MAX_PART_FUNC_BIN_EXPR_LENGTH + 1;
const int64_t OB_MAX_CALCULATOR_SERIALIZE_LENGTH = 8 * 1024; // 8k
const int64_t OB_MAX_THREAD_AIO_BUFFER_MGR_COUNT = 32;
const int64_t OB_MAX_GET_ROW_NUMBER = 10240;
const uint64_t OB_FULL_ROW_COLUMN_ID = 0;
const uint64_t OB_DELETE_ROW_COLUMN_ID = 0;
const int64_t OB_DIRECT_IO_ALIGN = 4096;
const int64_t OB_MAX_COMPOSITE_SYMBOL_COUNT = 256;
const int64_t OB_SERVER_STATUS_LENGTH = 64;
const int64_t OB_SERVER_VERSION_LENGTH = 256;
const int64_t OB_CLUSTER_VERSION_LENGTH = OB_SERVER_VERSION_LENGTH;//xx.xx.xx
const int64_t OB_SERVER_TYPE_LENGTH = 64;
const int64_t OB_MAX_HOSTNAME_LENGTH = 60;
const int64_t OB_MAX_USERNAME_LENGTH = 32;
const int64_t OB_MAX_PASSWORD_LENGTH = 128;
const int64_t OB_MAX_PASSWORD_BUF_LENGTH = OB_MAX_PASSWORD_LENGTH + 1;
// After each sha1 is 41 characters, the incremental backup is up to 64 times, and the maximum password required for recovery is 64*(41+1)=2,688
const int64_t OB_MAX_ENCRYPTED_PASSWORD_LENGTH = OB_MAX_PASSWORD_LENGTH * 4;
const int64_t OB_MAX_PASSWORD_ARRAY_LENGTH = 4096;
const int64_t OB_MAX_ERROR_MSG_LEN = 512;
const int64_t OB_MAX_RESULT_MESSAGE_LENGTH = 1024;
const int64_t OB_MAX_DEFINER_LENGTH= OB_MAX_USER_NAME_LENGTH_STORE + OB_MAX_HOST_NAME_LENGTH + 1; //user@host
const int64_t OB_MAX_SECURITY_TYPE_LENGTH = 7; //definer or invoker
const int64_t OB_MAX_READ_ONLY_STATE_LENGTH = 16;
//At present, the log module reads and writes the buffer using OB_MAX_LOG_BUFFER_SIZE, the length of the transaction submitted to the log module is required to be less than the length of the log module can read and write the log, minus the length of the log header, the BLOCK header and the EOF, here is defined a length minus 1024B
const int64_t OB_MAX_LOG_ALLOWED_SIZE = 1965056L; //OB_MAX_LOG_BUFFER_SIZE - 1024B
const int64_t OB_MAX_LOG_BUFFER_SIZE = 1966080L;  // 1.875MB
const int64_t OB_MAX_TRIGGER_VCHAR_PARAM_LENGTH = 128;
const int64_t OB_TRIGGER_MSG_LENGTH = 3 * MAX_IP_ADDR_LENGTH
                                      + OB_TRIGGER_TYPE_LENGTH + 3 * OB_MAX_TRIGGER_VCHAR_PARAM_LENGTH;
const int64_t OB_MAX_TRACE_ID_BUFFER_SIZE = 64;
const int64_t OB_MAX_TRACE_INFO_BUFFER_SIZE = (1 << 12);
const int64_t OB_MAX_TRANS_ID_BUFFER_SIZE = 512;
const int32_t OB_MIN_SAFE_COPY_COUNT = 3;
const int32_t OB_SAFE_COPY_COUNT = 3;
const int32_t OB_DEFAULT_REPLICA_NUM = 3;
const int32_t OB_DEC_AND_LOCK = 2626; /* used by remoe_plan in ObPsStore */
const int32_t OB_MAX_SCHEMA_VERSION_INTERVAL = 40 * 1000 * 1000;  // 40s
const int64_t UPDATE_SCHEMA_ADDITIONAL_INTERVAL = 5 * 1000 * 1000L; //5s

const int32_t OB_MAX_SUB_GET_REQUEST_NUM = 256;
const int32_t OB_DEFAULT_MAX_GET_ROWS_PER_SUBREQ = 20;

const int64_t OB_MPI_MAX_PARTITION_NUM = 128;
const int64_t OB_MPI_MAX_TASK_NUM = 256;

const int64_t OB_MAX_TABLE_NUM_PER_STMT = 256;
const int32_t OB_TMP_BUF_SIZE_256 = 256;
const int64_t OB_SCHEMA_MGR_MAX_USED_TID_MAP_BUCKET_NUM = 64;
const int64_t OB_MAX_SLAVE_READ_DELAY_TS = 5 * 1000 * 1000;

const int64_t OB_MAX_DIRECTORY_NAME_LENGTH = 128; // Compatible with Oracle
const int64_t OB_MAX_DIRECTORY_PATH_LENGTH = 4000; // Compatible with Oracle
const uint64_t OB_MAX_INTERVAL_PARTITIONS = 1048575; // interval parted table max partitions
const int64_t OB_MAX_BALANCE_GROUP_NAME_LENGTH = 512;

//plan cache
const int64_t OB_PC_NOT_PARAM_COUNT = 8;
const int64_t OB_PC_SPECIAL_PARAM_COUNT = 16;
const int64_t OB_PC_RAW_PARAM_COUNT = 128;
const int64_t OB_PLAN_CACHE_BUCKET_NUMBER = 49157;// calculated by cal_next_prime()
const int64_t OB_PLAN_CACHE_PERCENTAGE = 5;
const int64_t OB_PLAN_CACHE_EVICT_HIGH_PERCENTAGE = 90;
const int64_t OB_PLAN_CACHE_EVICT_LOW_PERCENTAGE = 50;
const int64_t OB_PC_WEIGHT_NUMERATOR = 1000000000;

//schedule info
static const int64_t OB_MAX_SCHED_INFO_LENGTH = 16 * 1024L; //Scheduling information

//time zone info
const int64_t OB_MAX_TZ_ABBR_LEN = 32;//according to statistics
const int64_t OB_MAX_TZ_NAME_LEN = 64;//according to statistics
const int64_t OB_INVALID_TZ_ID = -1;
const int64_t OB_INVALID_TZ_TRAN_TIME = INT64_MIN;
const int64_t OB_MAX_SNAPSHOT_DELAY_TIME = 5*1000*1000; //Maximum time delay for machine reading
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
const uint32_t INVALID_SESSID = UINT32_MAX;
const int64_t OB_MAX_VAR_NUM_PER_SESSION = 1024;
// The maximum time set by the user through hint/set session.ob_query_timeout/set session.ob_tx_timeout is 102 years
// The purpose of this is to avoid that when the user enters a value that is too large, adding the current timestamp causes the MAX_INT64 to overflow
const int64_t OB_MAX_USER_SPECIFIED_TIMEOUT =  102L * 365L * 24L * 60L * 60L * 1000L * 1000L;
const int64_t OB_MAX_PROCESS_TIMEOUT = 5L * 60L * 1000L * 1000L; // 5m
const int64_t OB_DEFAULT_SESSION_TIMEOUT = 100L * 1000L * 1000L; // 10s
const int64_t OB_DEFAULT_STMT_TIMEOUT = 30L * 1000L * 1000L; // 30s
const int64_t OB_DEFAULT_INTERNAL_TABLE_QUERY_TIMEOUT = 10L * 1000L * 1000L; // 10s
const int64_t OB_DEFAULT_STREAM_WAIT_TIMEOUT = 30L * 1000L * 1000L; // 30s
const int64_t OB_DEFAULT_STREAM_RESERVE_TIME = 2L * 1000L * 1000L; // 2s
const int64_t OB_DEFAULT_JOIN_BATCH_COUNT = 10000;
const int64_t OB_AIO_TIMEOUT_US = 5L * 1000L * 1000L; //5s
const int64_t OB_DEFAULT_TENANT_COUNT = 100000; //10w
const int64_t OB_ONLY_SYS_TENANT_COUNT = 2;
const int64_t OB_MAX_SERVER_SESSION_CNT = 32767;
const int64_t OB_MAX_SERVER_TENANT_CNT = 1000;
const int64_t OB_RECYCLE_MACRO_BLOCK_DURATION = 10 * 60 * 1000 * 1000LL; // 10 minutes
const int64_t OB_MINOR_FREEZE_TEAM_UP_INTERVAL = 2 * 60 * 60 * 1000 * 1000L; // 2 hours
// for define
const int64_t OB_MAX_SPECIAL_LS_NUM = 1 + 1; // 1 for broadcast ls and 1 for sys ls
const int64_t OB_MAX_LS_NUM_PER_TENANT_PER_SERVER_CAN_BE_SET = 1024; // the maximum of _max_ls_cnt_per_server
const int64_t OB_MAX_LS_NUM_PER_TENANT_PER_SERVER = (10 * (OB_MAX_SPECIAL_LS_NUM + OB_MAX_MEMBER_NUMBER) > OB_MAX_LS_NUM_PER_TENANT_PER_SERVER_CAN_BE_SET ?
                                                     OB_MAX_LS_NUM_PER_TENANT_PER_SERVER_CAN_BE_SET : 10 * (OB_MAX_SPECIAL_LS_NUM + OB_MAX_MEMBER_NUMBER)); // magnification is 10x
const int64_t OB_MAX_LS_NUM_PER_TENANT_PER_SERVER_FOR_SMALL_TENANT = 8;   // the tenant that smaller than 4G will be limit to 8
const int64_t OB_MAX_TIME = 3020399000000;
// Max add partition member timeout.
// Used to make sure no member added after lease expired + %OB_MAX_ADD_MEMBER_TIMEOUT
const int64_t OB_MAX_ADD_MEMBER_TIMEOUT = 60L * 1000L * 1000L; // 1 minute
const int64_t OB_MAX_PACKET_FLY_TS = 100 * 1000L; // 100ms
const int64_t OB_MAX_PACKET_DECODE_TS = 10 * 1000L;
//Oceanbase network protocol
/*  4bytes  4bytes  4bytes   4bytes
 * -----------------------------------
 * | flag |  dlen  | chid | reserved |
 * -----------------------------------
 */
const uint32_t OB_NET_HEADER_LENGTH = 16;            // 16 bytes packet header
const uint32_t OB_MAX_RPC_PACKET_LENGTH = (1L << 24);

const int OB_TBNET_PACKET_FLAG = 0x416e4574;
const int OB_SERVER_ADDR_STR_LEN = 128; //used for buffer size of easy_int_addr_to_str

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
const int64_t OB_MYSQL_COMPRESSED_HEADER_SIZE = OB_MYSQL_HEADER_LENGTH + 3;  /* compression header size */


//-----------------------------------oceanbase 2.0 c/s protocol----------------------//
const uint16_t OB20_PROTOCOL_MAGIC_NUM = 0x20AB;
const int64_t OB20_PROTOCOL_HEADER_LENGTH = 24;
const int64_t OB20_PROTOCOL_TAILER_LENGTH = 4;  // for CRC32
const int64_t OB20_PROTOCOL_HEADER_TAILER_LENGTH = OB20_PROTOCOL_HEADER_LENGTH + OB20_PROTOCOL_TAILER_LENGTH;
const int64_t OB20_PROTOCOL_EXTRA_INFO_LENGTH = 4;  // for the length of extra info
const int16_t OB20_PROTOCOL_VERSION_VALUE = 20;

const int OB_THREAD_NAME_BUF_LEN = 16;

enum ObCSProtocolType
{
  OB_INVALID_CS_TYPE = 0,
  OB_MYSQL_CS_TYPE,           // mysql standard protocol
  OB_MYSQL_COMPRESS_CS_TYPE,  // mysql compress protocol
  OB_2_0_CS_TYPE,             // oceanbase 2.0 protocol
};

enum ObClientType
{
  OB_CLIENT_INVALID_TYPE = 0,
  OB_CLIENT_JDBC,             // JDBC client
  OB_CLIENT_OCI,              // ob lib client
  OB_CLIENT_NON_STANDARD      // non-standard client
};

inline const char *get_cs_protocol_type_name(const ObCSProtocolType type) {
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

const int32_t OB_TRACE_BUFFER_SIZE = 4 * 1024; //4k
const int64_t OB_TRACE_STAT_BUFFER_SIZE= 200; //200


const int64_t OB_MAX_VERSION_COUNT = 32;// max version count
const int64_t OB_MAX_VERSION_COUNT_FOR_MERGE = 4;
const int64_t OB_EASY_HANDLER_COST_TIME = 10 * 1000; // 10ms
const int64_t OB_EASY_MEMORY_LIMIT = 4L << 30; // 4G

const int64_t OB_MAX_SCHEMA_BUF_SIZE = 10L * 1024L * 1024L;//10MB
const int64_t OB_MAX_PART_LIST_SIZE = 10L * 1024L * 1024L;//10MB
const int64_t OB_MAX_TABLE_ID_LIST_SIZE = 10L * 1024L * 1024L;//10MB

const int64_t OB_MAX_SCHEDULER_JOB_NAME_LENGTH = 128;

enum ObServerRole
{
  OB_INVALID = 0,
  OB_ROOTSERVER = 1,  // rs
  OB_CHUNKSERVER = 2, // cs
  OB_MERGESERVER = 3, // ms
  OB_UPDATESERVER = 4, // ups
  OB_PROXYSERVER = 5,
  OB_SERVER = 6,
  OB_PROXY = 7,
  OB_OBLOG = 8, // liboblog
};

enum ObServerManagerOp
{
  OB_SHUTDOWN = 1, OB_RESTART = 2, OB_ADD = 3, OB_DELETE = 4,
};

const int OB_FAKE_MS_PORT = 2828;
const uint64_t OB_MAX_PS_PARAM_COUNT = 65535;
const uint64_t OB_MAX_PS_FIELD_COUNT = 65535;
// OB_ALL_MAX_COLUMN_ID must <= 65535, it is used in ob_cs_create_plan.h
const uint64_t OB_ALL_MAX_COLUMN_ID = 65535;
// internal columns id
const uint64_t OB_NOT_EXIST_COLUMN_ID = 0;
const uint64_t OB_HIDDEN_PK_INCREMENT_COLUMN_ID = 1;  // hidden pk is a tablet-level autoinc seq
const uint64_t OB_CREATE_TIME_COLUMN_ID = 2;
const uint64_t OB_MODIFY_TIME_COLUMN_ID = 3;
const uint64_t OB_MOCK_LINK_TABLE_PK_COLUMN_ID = 4;

const uint64_t OB_HIDDEN_ROWID_COLUMN_ID = 6;
const uint64_t OB_HIDDEN_TRANS_VERSION_COLUMN_ID = 7;
const uint64_t OB_HIDDEN_SQL_SEQUENCE_COLUMN_ID = 8;
const uint64_t OB_HIDDEN_SESSION_ID_COLUMN_ID = 9;
const uint64_t OB_HIDDEN_SESS_CREATE_TIME_COLUMN_ID = 10;
const uint64_t OB_HIDDEN_LOGICAL_ROWID_COLUMN_ID = 11;
const uint64_t OB_HIDDEN_GROUP_IDX_COLUMN_ID = 13; // used for batch nlj
const uint64_t OB_HIDDEN_FILE_ID_COLUMN_ID = 14; // used for external table
const uint64_t OB_HIDDEN_LINE_NUMBER_COLUMN_ID = 15; // used for external table
const int64_t OB_END_RESERVED_COLUMN_ID_NUM = 16;
const uint64_t OB_APP_MIN_COLUMN_ID = 16;

const uint64_t OB_ACTION_FLAG_COLUMN_ID = OB_ALL_MAX_COLUMN_ID
                                          - OB_END_RESERVED_COLUMN_ID_NUM + 1; /* 65520 */
// materialized view log
const uint64_t OB_MLOG_SEQ_NO_COLUMN_ID = OB_ALL_MAX_COLUMN_ID
                                          - OB_END_RESERVED_COLUMN_ID_NUM + 2; /* 65521 */
const uint64_t OB_MLOG_DML_TYPE_COLUMN_ID = OB_ALL_MAX_COLUMN_ID
                                          - OB_END_RESERVED_COLUMN_ID_NUM + 3; /* 65522 */
const uint64_t OB_MLOG_OLD_NEW_COLUMN_ID = OB_ALL_MAX_COLUMN_ID
                                           - OB_END_RESERVED_COLUMN_ID_NUM + 4; /* 65523 */
const uint64_t OB_MLOG_ROWID_COLUMN_ID = OB_ALL_MAX_COLUMN_ID
                                         - OB_END_RESERVED_COLUMN_ID_NUM + 5; /* 65524 */
const uint64_t OB_MIN_MLOG_SPECIAL_COLUMN_ID = OB_MLOG_SEQ_NO_COLUMN_ID;
const uint64_t OB_MAX_MLOG_SPECIAL_COLUMN_ID = OB_MLOG_ROWID_COLUMN_ID;

const char *const OB_MLOG_SEQ_NO_COLUMN_NAME = "SEQUENCE$$";
const char *const OB_MLOG_DML_TYPE_COLUMN_NAME = "DMLTYPE$$";
const char *const OB_MLOG_OLD_NEW_COLUMN_NAME = "OLD_NEW$$";
const char *const OB_MLOG_ROWID_COLUMN_NAME = "M_ROW$$";

const uint64_t OB_MAX_TMP_COLUMN_ID = OB_ALL_MAX_COLUMN_ID
                                      - OB_END_RESERVED_COLUMN_ID_NUM;
const int32_t OB_COUNT_AGG_PD_COLUMN_ID = INT32_MAX - 1;
const int64_t OB_MAX_AUTOINC_SEQ_VALUE = (1L << 40) - 1; // max value for 40bit

const char *const OB_UPDATE_MSG_FMT = "Rows matched: %ld  Changed: %ld  Warnings: %ld";
const char *const OB_INSERT_MSG_FMT = "Records: %ld  Duplicates: %ld  Warnings: %ld";
const char *const OB_LOAD_DATA_MSG_FMT = "Records: %ld  Deleted: %ld  Skipped: %ld  Warnings: %ld";
const char OB_PADDING_CHAR = ' ';
const char OB_PADDING_BINARY = '\0';
const char *const OB_VALUES = "__values";
// hidden primary key name
const char *const OB_HIDDEN_PK_INCREMENT_COLUMN_NAME = "__pk_increment"; //hidden
const char *const OB_MOCK_LINK_TABLE_PK_COLUMN_NAME = "__link_table_pkey"; //hidden
const char *const OB_HIDDEN_SESSION_ID_COLUMN_NAME = "SYS_SESSION_ID"; //oracle temporary table
const char *const OB_HIDDEN_SESS_CREATE_TIME_COLUMN_NAME = "SYS_SESS_CREATE_TIME"; //oracle temporary table
const char *const OB_HIDDEN_FILE_ID_COLUMN_NAME = "__file_id"; // used for external table
const char *const OB_HIDDEN_LINE_NUMBER_COLUMN_NAME = "__line_number"; // used for external table

// hidden rowid name
const char *const OB_HIDDEN_ROWID_COLUMN_NAME = "__ob_rowid";
const int32_t OB_HIDDEN_ROWID_COLUMN_LENGTH = 16;

const char *const OB_HIDDEN_LOGICAL_ROWID_COLUMN_NAME = "ROWID";
const int32_t OB_HIDDEN_LOGICAL_ROWID_COLUMN_NAME_LENGTH = 5;

const char *const OB_HIDDEN_LOGICAL_ROWID_INDEX_NAME = "ROWID_INDEX";
const int32_t OB_HIDDEN_LOGICAL_ROWID_INDEX_NAME_LENGTH = 11;

// internal index prefix
const char *const OB_INDEX_PREFIX = "__idx_";
// internal materialized view log prefix
const char *const OB_MLOG_PREFIX_MYSQL = "mlog$_";
const char *const OB_MLOG_PREFIX_ORACLE = "MLOG$_";

// internal user
const char *const OB_INTERNAL_USER = "__ob_server";

const char *const OB_SERVER_ROLE_VAR_NAME = "__ob_server_role";
//trace id
const char *const OB_TRACE_ID_VAR_NAME = "__ob_trace_id";

//balance partition sharding
const char *const OB_PARTITION_SHARDING_NONE = "NONE";
const char *const OB_PARTITION_SHARDING_PARTITION = "PARTITION";
const char *const OB_PARTITION_SHARDING_ADAPTIVE = "ADAPTIVE";

// fulltext search
const char *const OB_DOC_ID_COLUMN_NAME = "__doc_id";
const char *const OB_WORD_SEGMENT_COLUMN_NAME_PREFIX = "__word_segment";
const char *const OB_WORD_COUNT_COLUMN_NAME_PREFIX = "__word_count";
const char *const OB_DOC_LENGTH_COLUMN_NAME_PREFIX = "__doc_length";
const int64_t OB_DOC_ID_COLUMN_BYTE_LENGTH = (sizeof(uint64_t) * 2);
constexpr int64_t OB_WORD_SEGMENT_COLUMN_NAME_PREFIX_LEN = sizeof("__word_segment") - 1;
constexpr int64_t OB_WORD_COUNT_COLUMN_NAME_PREFIX_LEN = sizeof("__word_count") - 1;
const char OB_FT_COL_ID_DELIMITER = '_';

// backup and restore
const int64_t OB_MAX_CLUSTER_NAME_LENGTH = OB_MAX_APP_NAME_LENGTH;
const int64_t OB_MAX_URI_LENGTH = 2048;
const int64_t OB_MAX_RETRY_TIMES = 3;
const int64_t OB_AGENT_MAX_RETRY_TIME = 5 * 60 * 1000 * 1000;//300s
const int64_t OB_AGENT_SINGLE_SLEEP_SECONDS = 5;//5s
// Currently the flashback database is not maintained incanation, it will always be 1. TODO(rongxuan): fix it
const int64_t OB_START_INCARNATION = 1;
const char *const OB_STRING_SSTABLE_META = "sstable_meta";
const char *const OB_STRING_TABLE_KEYS = "table_keys";
const char *const OB_STRING_PARTITION_META = "partition_meta";
const char *const OB_STRING_PART_LIST = "part_list";
const static int64_t OB_MAX_URI_HEADER_LENGTH = 1024;
const int64_t OB_MAX_REPLICAS_INFO  = 1024;
const int64_t OB_INNER_TABLE_DEFAULT_KEY_LENTH = 1024;
const int64_t OB_INNER_TABLE_DEFAULT_VALUE_LENTH = 4096;
const int64_t OB_INNER_TABLE_BACKUP_TYPE_LENTH = 64;
const int64_t OB_DEFAULT_STATUS_LENTH = 64;
const int64_t OB_DEFAULT_LOG_INFO_LENGTH = 64;
const int64_t OB_DEFAULT_OUTPUT_DEVICE_TYPE_LENTH = 64;
const int64_t OB_INNER_TABLE_BACKUP_CLEAN_TYPE_LENGTH = 64;
const int64_t OB_INNER_TABLE_BACKUP_CLEAN_PARAMETER_LENGTH = 256;
const int64_t OB_INNER_TABLE_BACKUP_TASK_CLUSTER_FORMAT_LENGTH = 64;
const int64_t OB_INNER_TABLE_BACKUP_LEVEL_LENGTH = 64;
const int64_t OB_INNER_TABLE_BACKUP_DEFAULT_FIELD_LENGTH = 256;
const int64_t OB_MAX_BACKUP_PTAH_LIST_LENGTH = 8 * 1024;
const int64_t OB_MAX_EXECUTOR_TENANT_LENGTH = 64 * 1024;

const char *const OB_LOCAL_PREFIX = "local://";
const char *const OB_OSS_PREFIX = "oss://";
const char *const OB_FILE_PREFIX = "file://";
const char *const OB_COS_PREFIX = "cos://";
const char *const OB_S3_PREFIX = "s3://";
const char *const OB_S3_APPENDABLE_FORMAT_META = "FORMAT_META";
const char *const OB_S3_APPENDABLE_SEAL_META = "SEAL_META";
const char *const OB_S3_APPENDABLE_FRAGMENT_PREFIX = "@APD_PART@";
const int64_t OB_STORAGE_LIST_MAX_NUM = 1000;
const char *const OB_RESOURCE_UNIT_DEFINITION = "resource_unit_definition";
const char *const OB_RESOURCE_POOL_DEFINITION = "resource_pool_definition";
const char *const OB_CREATE_TENANT_DEFINITION = "create_tenant_definition";
const char *const OB_CREATE_DATABASE_DEFINITION = "create_database_definition";
const char *const OB_CREATE_USER_DEINITION = "create_user_definition";
const char *const OB_SYSTEM_VARIABLE_DEFINITION = "system_variable_definition";
const char *const OB_TENANT_PARAMETER_DEFINITION = "tenant_parameter_definition";
const char *const OB_USER_PRIVILEGE_DEFINITION = "user_privilege_definition";
const char *const OB_CREATE_TABLEGROUP_DEFINITON = "create_tablegroup_definition";
const char *const OB_DATA_TABLE_IDS_LIST = "data_table_ids_list";
const char *const OB_INDEX_TABLE_IDS_LIST = "index_table_ids_list";
const char *const OB_TABLEGROUP_IDS_LIST = "tablegroup_ids_list";
const char *const OB_FOREIGN_KEY_IDS_LIST = "foreign_key_ids_list";
const char *const OB_FOREIGN_KEY_DEFINITION = "foreign_key_definition";
const char *const OB_ROUTINE_IDS_LIST = "routine_ids_list";
const char *const OB_CREATE_ROUTINE_DEFINITION = "create_routine_definition";
const char *const OB_PACKAGE_IDS_LIST = "package_ids_list";
const char *const OB_CREATE_PACKAGE_DEFINITION = "create_package_definition";
const char *const OB_UDT_IDS_LIST = "udt_ids_list";
const char *const OB_CREATE_UDT_DEFINITION = "create_udt_definition";
const char *const OB_TRIGGER_IDS_LIST = "trigger_ids_list";
const char *const OB_TRIGGER_DEFINITION = "trigger_definition";
const char *const OB_TABLESPACE_IDS_LIST = "tablespace_ids_list";
const char *const OB_CREATE_TABLESPACE_DEFINITION = "create_tablespace_definition";
const char *const OB_SEQUENCE_DEFINITION_IDS = "sequence_definition_ids";
const char *const OB_SEQUENCE_DEFINITION = "sequence_definition";
const char *const OB_RECYCLE_OBJECT_LIST = "recycle_objects_list";
const char *const OB_DROPPED_TABLE_ID_LIST = "dropped_table_ids_list";
const char *const OB_TABLEGROUP = "tablegroup";
const char *const OB_STRING_PARTITION_GROUP_META = "partition_group_meta";
const char *const OB_SECURITY_AUDIT_IDS_LIST = "security_audit_ids_list";
const char *const OB_SECURITY_AUDIT_DEFINITION = "security_audit_definition";
const char *const OB_SYNONYM_IDS_LIST = "synonym_ids_list";
const char *const OB_CREATE_SYNONYM_DEFINITION = "create_synonym_definition";
const char *const OB_TIMEZONE_INFO_DEFINITION = "timezone_info_definition";
const char *const OB_MASKED_STR = "***";

enum ObCopySSTableType
{
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
//            ini schema                                 //
///////////////////////////////////////////////////////////
const char *const OB_BACKUP_SCHEMA_FILE_PATTERN = "etc/%s.schema.bin";

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
const int64_t INDEX_SUB_PART_LENGTH= 256;
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
//columns
const int64_t MAX_TABLE_CATALOG_LENGTH = 4096;
const int64_t MAX_COLUMN_COMMENT_LENGTH = 2048;
const int64_t MAX_COLUMN_COMMENT_CHAR_LENGTH = 1024;
const int64_t MAX_COLUMN_KEY_LENGTH = 3;
const int64_t MAX_NUMERIC_PRECISION_LENGTH = 9;
const int64_t MAX_NUMERIC_SCALE_LENGTH = 9;
const int64_t MAX_COLUMN_PRIVILEGE_LENGTH = 200;
const int64_t MAX_PRIVILEGE_CONTEXT_LENGTH = 80;
const int64_t MAX_INFOSCHEMA_COLUMN_PRIVILEGE_LENGTH = 64;
const int64_t MAX_COLUMN_YES_NO_LENGTH = 3;
const int64_t MAX_COLUMN_VARCHAR_LENGTH = 262143;
const int64_t MAX_COLUMN_CHAR_LENGTH = 255;
//column group
const uint64_t INVALID_COLUMN_GROUP_ID = 0;
const uint64_t DEFAULT_TYPE_COLUMN_GROUP_ID = 1; // reserve 2~999
const uint64_t COLUMN_GROUP_START_ID = 1000;
const uint64_t DEFAULT_CUSTOMIZED_CG_NUM = 2;
const int64_t OB_CG_NAME_PREFIX_LENGTH = 5; // length of cg prefix like "__cg_"
const int64_t OB_MAX_COLUMN_GROUP_NAME_LENGTH = OB_MAX_COLUMN_NAME_LENGTH * OB_MAX_CHAR_LEN + OB_CG_NAME_PREFIX_LENGTH; //(max_column_name_length(128) * ob_max_char_len(3)) + prefix
const int64_t MAX_NAME_CHAR_LEN = 64;

//Oracle
const int64_t MAX_ORACLE_COMMENT_LENGTH = 4000;

//Oracle MAX_ENABLED_ROLES The maximum number of effective roles granted to users
const int64_t MAX_ENABLED_ROLES = 148;
const int64_t MAX_ORACLE_NAME_LENGTH = 30;

//Oracle SA
const int64_t MAX_ORACLE_SA_COMPONENTS_SHORT_NAME_LENGTH = 30;
const int64_t MAX_ORACLE_SA_COMPONENTS_LONG_NAME_LENGTH = 80;
const int64_t MAX_ORACLE_SA_COMPONENTS_PARENT_NAME_LENGTH = 30;
const int64_t MAX_ORACLE_SA_LABEL_TYPE_LENGTH = 15;

////////////////////////////////////////////////////////////
//             table id range definition                  //
////////////////////////////////////////////////////////////
const uint64_t OB_MIN_GENERATED_COLUMN_ID = 2000;
const uint64_t OB_MIN_MV_COLUMN_ID = 10000;
const uint64_t OB_MIN_SHADOW_COLUMN_ID = 32767;
const uint64_t OB_MAX_SYS_POOL_ID = 100;

// ddl related
const char *const OB_SYS_USER_NAME = "root";
const char *const OB_ORA_SYS_USER_NAME = "SYS";
const char *const OB_ORA_LBACSYS_NAME = "LBACSYS";
const char *const OB_ORA_AUDITOR_NAME = "ORAAUDITOR";
const char *const OB_ORA_CONNECT_ROLE_NAME = "CONNECT";
const char *const OB_ORA_RESOURCE_ROLE_NAME = "RESOURCE";
const char *const OB_ORA_DBA_ROLE_NAME = "DBA";
const char *const OB_ORA_PUBLIC_ROLE_NAME = "PUBLIC";
const char *const OB_ORA_STANDBY_REPLICATION_ROLE_NAME = "STANDBY_REPLICATION";
const char *const OB_RESTORE_USER_NAME = "__oceanbase_inner_restore_user";
const char *const OB_DRC_USER_NAME = "__oceanbase_inner_drc_user";
const char *const OB_SYS_TENANT_NAME = "sys";
const char *const OB_FAKE_TENANT_NAME = "fake_tenant";
const char *const OB_GTS_TENANT_NAME = "gts";
const char *const OB_SYS_HOST_NAME = "%";
const char *const OB_DEFAULT_HOST_NAME = "%";
// const char *const OB_MONITOR_TENANT_NAME = "monitor";
const char *const OB_DIAG_TENANT_NAME = "diag";
//for sync ddl (ClusterID_TenantID_SchemaVersion)
const char *const OB_DDL_ID_VAR_NAME = "__oceanbase_ddl_id";
const int64_t OB_MAX_DDL_ID_STR_LENGTH = 64;
#ifdef ERRSIM
const int64_t OB_MAX_DDL_SINGLE_REPLICA_BUILD_TIMEOUT = 30L * 60L * 1000L * 1000L; // 30 minutes
#else
const int64_t OB_MAX_DDL_SINGLE_REPLICA_BUILD_TIMEOUT = 7L * 24L * 60L * 60L * 1000L * 1000L; // 7days
#endif

const int64_t OB_MAX_PARTITION_SHARDING_LENGTH = 10;

// The default user name of the standby database to log in to the main database
const char *const OB_STANDBY_USER_NAME = "__oceanbase_inner_standby_user";

const double TENANT_RESERVE_MEM_RATIO = 0.1;
const int64_t LEAST_MEMORY_SIZE = 1L << 30;
const int64_t LEAST_MEMORY_SIZE_FOR_NORMAL_MODE = 2L << 30;
const int64_t SYS_MAX_ALLOCATE_MEMORY = 1L << 34;

// mem factor
const double SQL_AUDIT_MEM_FACTOR = 0.1;
const double MONITOR_MEM_FACTOR = 0.01;
const double KVCACHE_FACTOR = TENANT_RESERVE_MEM_RATIO;

const double MIN_TENANT_QUOTA = .5;
const double OB_DTL_CPU = 5.;
const double OB_DATA_CPU = 2.5;

const uint64_t OB_INVALID_TENANT_ID = 0;
const uint64_t OB_SYS_TENANT_ID = 1;
const uint64_t OB_GTS_TENANT_ID = 2;
const uint64_t OB_SERVER_TENANT_ID = 500;
const uint64_t OB_DTL_TENANT_ID = 508;
const uint64_t OB_DATA_TENANT_ID = 509;
const uint64_t OB_GTS_SOURCE_TENANT_ID = 511;
const uint64_t OB_SVR_BLACKLIST_TENANT_ID = 512;
const uint64_t OB_MAX_RESERVED_TENANT_ID = 1000;
const uint64_t OB_USER_TENANT_ID = 1000;

// sys unit associated const
const uint64_t OB_SYS_RESOURCE_POOL_ID = 1;
const uint64_t OB_SYS_UNIT_ID = 1;
const uint64_t OB_INIT_SERVER_ID = 1;
const uint64_t OB_INIT_DDL_TASK_ID = 1;
const uint64_t OB_SYS_UNIT_GROUP_ID = 1;
const uint64_t OB_INIT_REWRITE_RULE_VERSION = 1;
const uint64_t OB_USER_UNIT_CONFIG_ID = 1000;
const uint64_t OB_USER_RESOURCE_POOL_ID = 1000;
const uint64_t OB_USER_UNIT_ID = 1000;
const uint64_t OB_USER_UNIT_GROUP_ID = 1000;
//standby unit config tmplate
const char * const OB_STANDBY_UNIT_CONFIG_TEMPLATE_NAME = "standby_unit_config_template";
const char* const OB_MYSQL50_TABLE_NAME_PREFIX = "#mysql50#";

const int64_t OB_SCHEMA_CODE_VERSION = 1;

// xiyu: must keep same with generate_inner_table_schema.py
// don't use share/inner_table/ob_inner_table_schema.h to avoid dependence.

/*
 * ################################################################################
 *
 * OBJECT_ID FOR INNER OBJECTS (0, 500000)
 *
 * For more details: see docs on yuque product_functionality_review/fgcxak
 *
 * To avolid confict, border for each range should not be used.
 *
 * ################################################################################
 */
const uint64_t OB_INVALID_OBJECT_ID           = 0;
const uint64_t OB_MAX_INNER_OBJECT_ID         = 500000;
const uint64_t OB_MIN_USER_OBJECT_ID          = 500000;
// `test` database is created when create tenant, it's just a user defined object.
const uint64_t OB_INITIAL_TEST_DATABASE_ID    = OB_MAX_INNER_OBJECT_ID + 1;

OB_INLINE bool is_inner_object_id(const uint64_t object_id)
{
  return object_id > OB_INVALID_OBJECT_ID && object_id < OB_MAX_INNER_OBJECT_ID;
}
/*
 * ################################################################################
 * OBJECT_ID FOR TABLE (0, 200000)
 *
 * (0, 100)         : Core Table
 * (0, 10000)       : System Table
 * (10000, 15000)   : MySQL Virtual Table
 * (15000, 20000)   : Oracle Virtual Table
 * (20000, 25000)   : MySQL System View
 * (25000, 30000)   : Oracle System View
 * (30000, 49400)   : Reserved
 * (49400, 49500)   : Inner tablet for LS
 * (49500, 50000)   : CTE Table
 * (50000, 60000)   : Lob meta table
 * (60000, 70000)   : Lob piece table
 * (70000, 100000)  : Reserved
 * (100000, 200000) : System table Index
 *
 * ATTENTION!!! If reserved range is used, the following files should be checked.
 * - src/share/inner_table/ob_inner_table_schema_def.py
 * - src/share/inner_table/generate_inner_table_schema.py
 * - deps/oblib/src/common/ob_tablet_id.h
 * ################################################################################
 */
// (0, 10000) for system table
const uint64_t OB_MAX_CORE_TABLE_ID           = 100;
const uint64_t OB_MAX_SYS_TABLE_ID            = 10000;
// (10000, 15000) for mysql virtual table
const uint64_t OB_MAX_MYSQL_VIRTUAL_TABLE_ID  = 15000;
// (15000, 20000) for oracle virtual table
const uint64_t OB_MAX_VIRTUAL_TABLE_ID        = 20000;
// (20000, 25000) for mysql sys view
const uint64_t OB_MAX_MYSQL_SYS_VIEW_ID       = 25000;
// (25000, 30000) for oracle sys view
const uint64_t OB_MAX_SYS_VIEW_ID             = 30000;
// (30000, 49400) is reserved
// (49400, 49500) for LS inner table / tablet
const uint64_t OB_MIN_LS_INNER_TABLE_ID       = 49400;
const uint64_t OB_MAX_LS_INNER_TABLE_ID       = 49500;
// (49500, 49999) for cte, cte table opens up a separate id space, which does not conflict with other id
const uint64_t OB_MIN_CTE_TABLE_ID            = 49500;
const uint64_t OB_MAX_CTE_TABLE_ID            = 49999;
// (50000, 60000) for inner lob meta table
// (60000, 70000) for inner lob piece table
const uint64_t OB_MIN_SYS_LOB_META_TABLE_ID   = 50000;
const uint64_t OB_MAX_CORE_LOB_META_TABLE_ID  = 50100;
const uint64_t OB_MIN_SYS_LOB_PIECE_TABLE_ID  = 60000;
const uint64_t OB_MAX_CORE_LOB_PIECE_TABLE_ID = 60100;
const uint64_t OB_MAX_SYS_LOB_PIECE_TABLE_ID  = 70000;
// (70000, 100000) is reserved
// (100000, 200000) for sys table index
const uint64_t OB_MIN_SYS_TABLE_INDEX_ID      = 100000;
const uint64_t OB_MAX_CORE_TABLE_INDEX_ID     = 101000;
const uint64_t OB_MAX_SYS_TABLE_INDEX_ID      = 200000;
const uint64_t OB_MAX_INNER_TABLE_ID          = 200000;

OB_INLINE bool is_system_table(const uint64_t tid)
{
  return (tid > OB_INVALID_OBJECT_ID && tid < OB_MAX_SYS_TABLE_ID);
}

// includes virtual table's index
OB_INLINE bool is_mysql_virtual_table(const uint64_t tid)
{
  return (tid > OB_MAX_SYS_TABLE_ID && tid < OB_MAX_MYSQL_VIRTUAL_TABLE_ID);
}

// includes virtual table's index
OB_INLINE bool is_ora_virtual_table(const uint64_t tid)
{
  return (tid > OB_MAX_MYSQL_VIRTUAL_TABLE_ID && tid < OB_MAX_VIRTUAL_TABLE_ID);
}

// includes virtual table's index
OB_INLINE bool is_virtual_table(const uint64_t tid)
{
  return (tid > OB_MAX_SYS_TABLE_ID && tid < OB_MAX_VIRTUAL_TABLE_ID);
}

OB_INLINE bool is_mysql_sys_view_table(const uint64_t tid)
{
  return (tid > OB_MAX_VIRTUAL_TABLE_ID && tid < OB_MAX_MYSQL_SYS_VIEW_ID);
}

OB_INLINE bool is_ora_sys_view_table(const uint64_t tid)
{
  return (tid > OB_MAX_MYSQL_SYS_VIEW_ID && tid < OB_MAX_SYS_VIEW_ID);
}

OB_INLINE bool is_sys_view(const uint64_t tid)
{
  return (tid > OB_MAX_VIRTUAL_TABLE_ID && tid < OB_MAX_SYS_VIEW_ID);
}

OB_INLINE bool is_ls_reserved_table(const uint64_t tid)
{
  return (tid > OB_MIN_LS_INNER_TABLE_ID && tid < OB_MAX_LS_INNER_TABLE_ID);
}

OB_INLINE bool is_cte_table(const uint64_t tid)
{
  return (tid > OB_MIN_CTE_TABLE_ID && tid < OB_MAX_CTE_TABLE_ID);
}

OB_INLINE bool is_core_lob_meta_table(const uint64_t tid)
{
  return (tid > OB_MIN_SYS_LOB_META_TABLE_ID) && (tid < OB_MAX_CORE_LOB_META_TABLE_ID);
}

OB_INLINE bool is_core_lob_piece_table(const uint64_t tid)
{
  return (tid > OB_MIN_SYS_LOB_PIECE_TABLE_ID) && (tid < OB_MAX_CORE_LOB_PIECE_TABLE_ID);
}

OB_INLINE bool is_core_lob_table(const uint64_t tid)
{
  return (is_core_lob_meta_table(tid) || is_core_lob_piece_table(tid));
}

OB_INLINE bool is_sys_lob_meta_table(const uint64_t tid)
{
  return (tid > OB_MIN_SYS_LOB_META_TABLE_ID) && (tid < OB_MIN_SYS_LOB_PIECE_TABLE_ID);
}

OB_INLINE bool is_sys_lob_piece_table(const uint64_t tid)
{
  return (tid > OB_MIN_SYS_LOB_PIECE_TABLE_ID) && (tid < OB_MAX_SYS_LOB_PIECE_TABLE_ID);
}

OB_INLINE bool is_sys_lob_table(const uint64_t tid)
{
  return (is_sys_lob_meta_table(tid) || is_sys_lob_piece_table(tid));
}

OB_INLINE bool is_core_index_table(const uint64_t tid)
{
  return (tid > OB_MIN_SYS_TABLE_INDEX_ID) && (tid < OB_MAX_CORE_TABLE_INDEX_ID);
}

OB_INLINE bool is_sys_index_table(const uint64_t tid)
{
  return (tid > OB_MIN_SYS_TABLE_INDEX_ID) && (tid < OB_MAX_SYS_TABLE_INDEX_ID);
}

// This function includes core table and its index、lob table
OB_INLINE bool is_core_table(const uint64_t tid)
{
  return (tid > OB_INVALID_OBJECT_ID && tid < OB_MAX_CORE_TABLE_ID)
         || is_core_index_table(tid)
         || is_core_lob_table(tid);
}

// This function includes system table and its index、lob table
OB_INLINE bool is_sys_table(const uint64_t tid)
{
  return is_system_table(tid)
         || is_sys_index_table(tid)
         || is_sys_lob_table(tid);
}

OB_INLINE bool is_reserved_table_id(const uint64_t tid)
{
  return (tid > OB_MAX_SYS_VIEW_ID && tid <= OB_MIN_LS_INNER_TABLE_ID)
         || (tid > OB_MAX_SYS_LOB_PIECE_TABLE_ID && OB_MIN_SYS_TABLE_INDEX_ID);
}

OB_INLINE bool is_inner_table(const uint64_t tid)
{
  return (tid > OB_INVALID_OBJECT_ID && tid < OB_MAX_INNER_TABLE_ID);
}

/*
 * ################################################################################
 * OBJECT_ID FOR USER/ROLE (200000, 201000)
 * ################################################################################
 */
const uint64_t OB_MIN_INNER_USER_ID    = 200000;
const uint64_t OB_SYS_USER_ID          = OB_MIN_INNER_USER_ID + 1;
const uint64_t OB_EMPTY_USER_ID        = OB_MIN_INNER_USER_ID + 2;
const uint64_t OB_ORA_SYS_USER_ID      = OB_MIN_INNER_USER_ID + 3;
const uint64_t OB_ORA_LBACSYS_USER_ID  = OB_MIN_INNER_USER_ID + 4;
const uint64_t OB_ORA_AUDITOR_USER_ID  = OB_MIN_INNER_USER_ID + 5;
const uint64_t OB_ORA_CONNECT_ROLE_ID  = OB_MIN_INNER_USER_ID + 6;
const uint64_t OB_ORA_RESOURCE_ROLE_ID = OB_MIN_INNER_USER_ID + 7;
const uint64_t OB_ORA_DBA_ROLE_ID      = OB_MIN_INNER_USER_ID + 8;
const uint64_t OB_ORA_PUBLIC_ROLE_ID   = OB_MIN_INNER_USER_ID + 9;
const uint64_t OB_AUDIT_MOCK_USER_ID   = OB_MIN_INNER_USER_ID + 10;
const uint64_t OB_ORA_STANDBY_REPLICATION_ROLE_ID = OB_MIN_INNER_USER_ID + 11;
const char * const OB_PROXYRO_USERNAME = "proxyro";
const uint64_t OB_MAX_INNER_USER_ID    = 201000;

OB_INLINE bool is_inner_user_or_role(const uint64_t uid)
{
  return (uid > OB_MIN_INNER_USER_ID && uid < OB_MAX_INNER_USER_ID);
}

OB_INLINE bool is_root_user(const uint64_t uid)
{
  return (uid == OB_SYS_USER_ID);
}

OB_INLINE bool is_empty_user(const uint64_t uid)
{
  return (uid == OB_EMPTY_USER_ID);
}

OB_INLINE bool is_ora_sys_user(const uint64_t uid)
{
  return (uid == OB_ORA_SYS_USER_ID);
}

OB_INLINE bool is_ora_lbacsys_user(const uint64_t uid)
{
  return (uid == OB_ORA_LBACSYS_USER_ID);
}

OB_INLINE bool is_ora_auditor_user(const uint64_t uid)
{
  return (uid == OB_ORA_AUDITOR_USER_ID);
}

OB_INLINE bool is_ora_connect_role(const uint64_t uid)
{
  return (uid == OB_ORA_CONNECT_ROLE_ID);
}

OB_INLINE bool is_ora_resource_role(const uint64_t uid)
{
  return (uid == OB_ORA_RESOURCE_ROLE_ID);
}

OB_INLINE bool is_ora_dba_role(const uint64_t uid)
{
  return (uid == OB_ORA_DBA_ROLE_ID);
}

OB_INLINE bool is_ora_public_role(const uint64_t uid)
{
  return (uid == OB_ORA_PUBLIC_ROLE_ID);
}

OB_INLINE bool is_ora_standby_replication_role(const uint64_t uid)
{
  return (uid == OB_ORA_STANDBY_REPLICATION_ROLE_ID);
}

/*
 * ################################################################################
 * OBJECT_ID FOR DATABASE (201000, 202000)
 * ################################################################################
 */
const uint64_t OB_MIN_INNER_DATABASE_ID       = 201000;
const uint64_t OB_SYS_DATABASE_ID             = OB_MIN_INNER_DATABASE_ID + 1;
const uint64_t OB_INFORMATION_SCHEMA_ID       = OB_MIN_INNER_DATABASE_ID + 2;
const uint64_t OB_MYSQL_SCHEMA_ID             = OB_MIN_INNER_DATABASE_ID + 3;
const uint64_t OB_RECYCLEBIN_SCHEMA_ID        = OB_MIN_INNER_DATABASE_ID + 4;
const uint64_t OB_PUBLIC_SCHEMA_ID            = OB_MIN_INNER_DATABASE_ID + 5;
const uint64_t OB_ORA_SYS_DATABASE_ID         = OB_MIN_INNER_DATABASE_ID + 6;
const uint64_t OB_ORA_LBACSYS_DATABASE_ID     = OB_MIN_INNER_DATABASE_ID + 7;
const uint64_t OB_ORA_AUDITOR_DATABASE_ID     = OB_MIN_INNER_DATABASE_ID + 8;
// use only if the 'use database' command is not executed.
const uint64_t OB_MOCK_DEFAULT_DATABASE_ID = OB_MIN_INNER_DATABASE_ID + 9;
const uint64_t OB_CTE_DATABASE_ID             = OB_MIN_INNER_DATABASE_ID + 10;
const uint64_t OB_MAX_INNER_DATABASE_ID       = 202000;

const char* const OB_SYS_DATABASE_NAME             = "oceanbase";
const char* const OB_INFORMATION_SCHEMA_NAME       = "information_schema";
const char* const OB_MYSQL_SCHEMA_NAME             = "mysql";
const char* const OB_RECYCLEBIN_SCHEMA_NAME        = "__recyclebin"; //hidden
const char* const OB_PUBLIC_SCHEMA_NAME            = "__public";     //hidden
const char* const OB_ORA_SYS_SCHEMA_NAME           = "SYS";
const char* const OB_MOCK_DEFAULT_DATABASE_NAME = "__outline_default_db";
const char* const OB_TEST_SCHEMA_NAME              = "test";

OB_INLINE bool is_oceanbase_sys_database_id(const uint64_t database_id)
{
  return OB_SYS_DATABASE_ID == database_id;
}

OB_INLINE bool is_information_schema_database_id(const uint64_t database_id)
{
  return OB_INFORMATION_SCHEMA_ID == database_id;
}

OB_INLINE bool is_mysql_database_id(const uint64_t database_id)
{
  return OB_MYSQL_SCHEMA_ID == database_id;
}

OB_INLINE bool is_oracle_sys_database_id(const int64_t database_id)
{
  return OB_ORA_SYS_DATABASE_ID == database_id;
}

OB_INLINE bool is_recyclebin_database_id(const uint64_t database_id)
{
  return OB_RECYCLEBIN_SCHEMA_ID == database_id;
}

OB_INLINE bool is_public_database_id(const uint64_t database_id)
{
  return OB_PUBLIC_SCHEMA_ID == database_id;
}

OB_INLINE bool is_outline_database_id(const uint64_t database_id)
{
  return OB_MOCK_DEFAULT_DATABASE_ID == database_id;
}

OB_INLINE bool is_inner_db(const uint64_t db_id)
{
  return (db_id > OB_MIN_INNER_DATABASE_ID && db_id < OB_MAX_INNER_DATABASE_ID);
}

OB_INLINE bool is_mysql_sys_database_id(const uint64_t database_id)
{
  return is_oceanbase_sys_database_id(database_id)
          || is_information_schema_database_id(database_id)
          || is_mysql_database_id(database_id);
}

OB_INLINE bool is_sys_database_id(const uint64_t database_id)
{
  return is_mysql_sys_database_id(database_id)
         || is_oracle_sys_database_id(database_id);
}

/*
 * ################################################################################
 * OBJECT_ID FOR TABLEGROUP (202000, 202100)
 * ################################################################################
 */
const uint64_t OB_MIN_INNER_TABLEGROUP_ID = 202000;
const uint64_t OB_SYS_TABLEGROUP_ID       = OB_MIN_INNER_TABLEGROUP_ID + 1;
const char* const OB_SYS_TABLEGROUP_NAME  = "oceanbase";
const uint64_t OB_MAX_INNER_TABLEGROUP_ID = 202100;

OB_INLINE bool is_sys_tablegroup_id(const uint64_t tablegroup_id)
{
  return tablegroup_id > OB_MIN_INNER_TABLEGROUP_ID
         && tablegroup_id <OB_MAX_INNER_TABLEGROUP_ID;
}

/*
 * ################################################################################
 * OBJECT_ID FOR KEYSTORE/MASTER KEY (202100, 202200)
 * ################################################################################
 */
const uint64_t OB_MIN_INNER_KEYSTORE_ID          = 202100;
const uint64_t OB_MYSQL_TENANT_INNER_KEYSTORE_ID = OB_MIN_INNER_KEYSTORE_ID + 1;
const uint64_t OB_MAX_INNER_KEYSTORE_ID          = 202200;

OB_INLINE bool is_mysql_inner_keystore_id(const uint64_t keystore_id)
{
  return OB_MYSQL_TENANT_INNER_KEYSTORE_ID == keystore_id;
}

OB_INLINE bool is_inner_keystore_id(const uint64_t keystore_id)
{
  return keystore_id > OB_MIN_INNER_KEYSTORE_ID
         && keystore_id < OB_MAX_INNER_KEYSTORE_ID;
}

/*
 * ################################################################################
 * OBJECT_ID FOR PROFILE (202200, 202300)
 * ################################################################################
 */
const uint64_t OB_MIN_INNER_PROFILE_ID           = 202200;
const uint64_t OB_ORACLE_TENANT_INNER_PROFILE_ID = OB_MIN_INNER_PROFILE_ID + 1;
const uint64_t OB_MAX_INNER_PROFILE_ID           = 202300;

OB_INLINE bool is_oracle_inner_profile_id(const uint64_t profile_id)
{
  return OB_ORACLE_TENANT_INNER_PROFILE_ID == profile_id;
}

OB_INLINE bool is_inner_profile_id(const uint64_t profile_id)
{
  return profile_id > OB_MIN_INNER_PROFILE_ID
         && profile_id < OB_MAX_INNER_PROFILE_ID;
}

/*
 * ################################################################################
 * OBJECT_ID RESERVED FOR OTHER SCHEMA OBJECTS (202300, 300000)
 * ################################################################################
 */

/*
 * ################################################################################
 * OBJECT_ID RESERVED FOR UDT/PROCEDURE/PACKAGE/TRIGGER (300000, 500000)
 * ################################################################################
 */

/*!
 * TODO:
 * PL will encode object_id with high 3bits & low 8bits to differ from kinds of objects.
 * Temporarily, PL object_id should be restricted to the low 53bits to avoid conficts.
 *
 * these flags only used for mocked package which generated by trigger and object type.
 * it`s not a real object id, and no effective for real object id. only used in pl/sql mocked package.
 * we only mask these flag when generate mocked package, it all in memory.
 * NOTICE: do not write these flag to system table, also do not dependence these flags. it maybe deleted in future.
 */
// 63bit : use for mock trigger package id
const uint64_t OB_MOCK_TRIGGER_PACKAGE_ID_MASK = 0x4000000000000000;
// 62bit : use for mock object type package id
const uint64_t OB_MOCK_OBJECT_PACAKGE_ID_MASK = 0x2000000000000000;
// 64bit : use for mock package spec/body mask, 0 means spec, 1 means body
const uint64_t OB_MOCK_PACKAGE_BODY_ID_MASK = 0x8000000000000000;
// 61bit : use for mock dblink udt id
const uint64_t OB_MOCK_DBLINK_UDT_ID_MASK = 0x1000000000000000;
/* low 21bits used as package type id */
#define OB_MOCK_MASK_SHIFT  40
#define OB_PACKAGE_ID_SHIFT 24
OB_INLINE uint64_t extract_package_id(uint64_t global_type_id)
{
  uint64_t mask = OB_MOCK_PACKAGE_BODY_ID_MASK |
                  OB_MOCK_TRIGGER_PACKAGE_ID_MASK |
                  OB_MOCK_OBJECT_PACAKGE_ID_MASK |
                  OB_MOCK_DBLINK_UDT_ID_MASK;
  uint64_t mock_val = global_type_id & (mask >> OB_MOCK_MASK_SHIFT);
  uint64_t package_id = ((int64_t)global_type_id >> OB_PACKAGE_ID_SHIFT) |
                        (mock_val << OB_MOCK_MASK_SHIFT);
  return package_id;
}

OB_INLINE int64_t extract_type_id(uint64_t global_type_id)
{
  return global_type_id & (~(UINT64_MAX << (OB_PACKAGE_ID_SHIFT - 4)));
}

OB_INLINE uint64_t combine_pl_type_id(uint64_t package_id, int64_t type_idx)
{
  uint64_t mask = OB_MOCK_PACKAGE_BODY_ID_MASK |
                  OB_MOCK_TRIGGER_PACKAGE_ID_MASK |
                  OB_MOCK_OBJECT_PACAKGE_ID_MASK |
                  OB_MOCK_DBLINK_UDT_ID_MASK;
  type_idx |= ((uint64_t)(package_id & mask) >> OB_MOCK_MASK_SHIFT);
  return (package_id << OB_PACKAGE_ID_SHIFT | type_idx);
}
const uint64_t OB_MIN_SYS_PL_UDT_ID = 300000; /*(300000-310000] reserved by system UDT ID*/
const uint64_t OB_MIN_SYS_PL_OBJECT_ID = 310000;
const uint64_t OB_MAX_SYS_PL_OBJECT_ID = 500000;
//TODO: restrict the max avaliable pl object id to avoid confict.
const uint64_t OB_MAX_USER_PL_OBJECT_ID = (OB_MOCK_OBJECT_PACAKGE_ID_MASK >> OB_PACKAGE_ID_SHIFT);

OB_INLINE bool is_inner_pl_udt_id(const uint64_t object_id)
{
  return object_id > OB_MIN_SYS_PL_UDT_ID
         && object_id < OB_MIN_SYS_PL_OBJECT_ID;
}
OB_INLINE bool is_inner_pl_object_id(const uint64_t object_id)
{
  return object_id > OB_MIN_SYS_PL_UDT_ID
         && object_id < OB_MAX_SYS_PL_OBJECT_ID;
}

OB_INLINE bool is_dblink_type_id(uint64_t type_id)
{
  return type_id != common::OB_INVALID_ID
          && ((type_id <<  OB_MOCK_MASK_SHIFT) & OB_MOCK_DBLINK_UDT_ID_MASK) != 0;
}


/* ################################################################################ */

const char* const OB_PRIMARY_INDEX_NAME = "PRIMARY";

const int64_t OB_MAX_CONFIG_URL_LENGTH = 512;
const int64_t OB_MAX_ADMIN_COMMAND_LENGTH = 1000;
const int64_t OB_MAX_ARBITRATION_SERVICE_NAME_LENGTH = 256;
const int64_t OB_MAX_ARBITRATION_SERVICE_LENGTH = 512;

const int64_t OB_MIGRATE_ACTION_LENGTH = 64;
const int64_t OB_MIGRATE_REPLICA_STATE_LENGTH = 64;
const int64_t OB_SYS_TASK_TYPE_LENGTH = 64;
const int64_t OB_MAX_TASK_COMMENT_LENGTH = 512;
const int64_t MAX_RECONNECTION_INTERVAL = 5 * 1000 * 1000;  //5s
const int64_t MAX_LOGIC_MIGRATE_TIME_OUT = 30 * 1000 * 1000; //30s
const int64_t OB_MODULE_NAME_LENGTH = 64;
const int64_t OB_RET_STR_LENGTH = 64;
const int64_t OB_STATUS_STR_LENGTH = 64;
const int64_t OB_DAG_WARNING_INFO_LENGTH = 512;
const int64_t OB_DAG_KEY_LENGTH = 128;
const int64_t OB_DAG_COMMET_LENGTH = 512;
const int64_t OB_DAG_SCHEDULER_INFO_LENGTH = 64;
const int64_t OB_MERGE_TYPE_STR_LENGTH = 64;
const int64_t OB_MERGE_STATUS_STR_LENGTH = 15;
const int64_t OB_DIAGNOSE_INFO_LENGTH = 1024;
const int64_t OB_PARALLEL_MERGE_INFO_LENGTH = 512;
const int64_t OB_COMPACTION_EVENT_STR_LENGTH = 1024;
const int64_t OB_PART_TABLE_INFO_LENGTH = 512;
const int64_t OB_MACRO_ID_INFO_LENGTH = 256;
const int64_t OB_COMPACTION_COMMENT_STR_LENGTH = 1024;
const int64_t OB_COMPACTION_INFO_LENGTH = 128;
const int64_t OB_MERGE_LEVEL_STR_LENGTH = 64;
const int64_t OB_MERGE_ROLE_STR_LENGTH = 64;
const int64_t OB_MERGE_COMMENT_INNER_STR_LENGTH = 800;

// for erasure code
const int64_t OB_MAX_EC_STRIPE_COUNT = 32;

// for array log print
const int64_t OB_LOG_KEEP_SIZE = 512;
//no need keep size for async
const int64_t OB_ASYNC_LOG_KEEP_SIZE = 0;
const char* const OB_LOG_ELLIPSIS = "...";


const char *const DEFAULT_REGION_NAME = "default_region";

// The connect attribute key value prefix that the obproxy transparently transmits to the observer
const char *const OB_PROXY_TRANSPARENT_TRANSMIT_PREFIX__ = "__ob_client_";

// The connect attribute key that the proxy transparently transmits to the observer,
// in order to prevent the sql request thread from deadlocking (such as dblink sql request)
const char *const OB_SQL_REQUEST_LEVEL = "__ob_client_sql_request_level";

// The connect attribute value that the proxy transparently transmits to the observer,
// in order to prevent the sql request thread from deadlocking (such as dblink sql request)
const char *const OB_SQL_REQUEST_LEVEL0 = "__sql_request_L0";
const char *const OB_SQL_REQUEST_LEVEL1 = "__sql_request_L1";
const char *const OB_SQL_REQUEST_LEVEL2 = "__sql_request_L2";
const char *const OB_SQL_REQUEST_LEVEL3 = "__sql_request_L3";

// for obproxy
const char *const OB_MYSQL_CLIENT_MODE = "__mysql_client_type";
const char *const OB_MYSQL_CLIENT_OBPROXY_MODE_NAME = "__ob_proxy";
const char *const OB_MYSQL_CONNECTION_ID = "__connection_id";
const char *const OB_MYSQL_GLOBAL_VARS_VERSION = "__global_vars_version";
const char *const OB_MYSQL_PROXY_CONNECTION_ID = "__proxy_connection_id";
// add client_session_id, addr_port & client session create time us
const char *const OB_MYSQL_CLIENT_SESSION_ID = "__client_session_id";
const char *const OB_MYSQL_CLIENT_ADDR_PORT = "__client_addr_port";
const char *const OB_MYSQL_CLIENT_CONNECT_TIME_US = "__client_connect_time";
const char *const OB_MYSQL_PROXY_SESSION_CREATE_TIME_US = "__proxy_session_create_time_us";
const char *const OB_MYSQL_CLUSTER_NAME = "__cluster_name";
const char *const OB_MYSQL_CLUSTER_ID = "__cluster_id";
const char *const OB_MYSQL_CLIENT_IP = "__client_ip";
const char *const OB_MYSQL_CAPABILITY_FLAG = "__proxy_capability_flag";
const char *const OB_MYSQL_PROXY_SESSION_VARS = "__proxy_session_vars";
const char *const OB_MYSQL_SCRAMBLE = "__proxy_scramble";
const char *const OB_MYSQL_PROXY_VEERSION = "__proxy_version";

const char *const OB_MYSQL_CLIENT_VERSION = "__ob_client_version";
const char *const OB_MYSQL_CLIENT_NAME = "__ob_client_name";

const char *const OB_MYSQL_JDBC_CLIENT_NAME = "OceanBase Connector/J";
const char *const OB_MYSQL_OCI_CLIENT_NAME = "OceanBase Connector/C";
// for java client
const char *const OB_MYSQL_JAVA_CLIENT_MODE_NAME = "__ob_java_client";
const char *const OB_MYSQL_OCI_CLIENT_MODE_NAME = "__ob_libobclient";
const char *const OB_MYSQL_JDBC_CLIENT_MODE_NAME = "__ob_jdbc_client";
const char *const OB_MYSQL_CLIENT_PROXY_USER_NAME = "__ob_client_proxy_user_name";
const char *const OB_MYSQL_CLIENT_ATTRIBUTE_CAPABILITY_FLAG = "__ob_client_attribute_capability_flag";

enum ObClientMode
{
  OB_MIN_CLIENT_MODE = 0,

  OB_JAVA_CLIENT_MODE,
  OB_PROXY_CLIENT_MODE,
  OB_OCI_CLIENT_MODE,
  OB_JDBC_CLIENT_MODE,
  // add others ...

  OB_MAX_CLIENT_MODE,
};

// for obproxy debug
#define OBPROXY_DEBUG 0
const char *const OB_SYS_TENANT_LOCALITY_STRATEGY = "sys_tenant_locality_strategy";
const char *const OB_AUTO_LOCALITY_STRATEGY = "auto_locality_strategy";
const char *const OB_3ZONES_IN_2REGIONS_LOCALITY_STRATEGY = "3zone3-in-2regions";

#if OBPROXY_DEBUG
const char* const OB_MYSQL_PROXY_SESSION_ID = "session_id";
const char* const OB_MYSQL_PROXY_TIMESTAMP = "proxy_time_stamp";
const char* const OB_MYSQL_PROXY_SYNC_VERSION = "proxy_sync_version";

const char* const OB_MYSQL_SERVER_SESSION_ID = "server_session_id";
const char* const OB_MYSQL_SERVER_SQL = "sql";
const char* const OB_MYSQL_SERVER_HANDLE_TIMESTAMP = "server_handle_time_stamp";
const char* const OB_MYSQL_SERVER_RECEIVED_TIMESTAMP = "server_receive_time_stamp";
#endif

//TODO: rootservice's sequence_id for schema refresh will be removed later.
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

/*
 * In 4.x version, each user tenant(except sys tenant) has its own meta tenant.
 * User tenant's tenant_id and its meta tenant's tenant_id will be distinguished by 0th bit.
 * 1) If tenant_id = OB_SYS_TENANT_ID, it's sys tenant.
 * 2) If tenant_id is odd, it's meta tenant.
 * 3) If tenant_id is even, it't user tenant.
 * see more docs on yuque rootservice/cnxdv7#pIAUC
 */
OB_INLINE bool is_sys_tenant(const uint64_t tenant_id)
{
  return OB_SYS_TENANT_ID == tenant_id;
}

OB_INLINE bool is_server_tenant(const uint64_t tenant_id)
{
  return OB_SERVER_TENANT_ID == tenant_id;
}

//check whether an tenant_id is virtual
OB_INLINE bool is_virtual_tenant_id(const uint64_t tenant_id)
{
  return (OB_SYS_TENANT_ID < tenant_id && tenant_id <= OB_MAX_RESERVED_TENANT_ID);
}

OB_INLINE bool is_not_virtual_tenant_id(const uint64_t tenant_id)
{
  return !is_virtual_tenant_id(tenant_id);
}

const uint64_t META_TENANT_MASK = (uint64_t)0x1;
OB_INLINE bool is_meta_tenant(const uint64_t tenant_id)
{
  return !is_sys_tenant(tenant_id)
         && !is_virtual_tenant_id(tenant_id)
         && 1 == (tenant_id & META_TENANT_MASK);
}

OB_INLINE bool is_user_tenant(const uint64_t tenant_id)
{
  return !is_sys_tenant(tenant_id)
         && !is_virtual_tenant_id(tenant_id)
         && 0 == (tenant_id & META_TENANT_MASK);
}

OB_INLINE uint64_t gen_user_tenant_id(const uint64_t tenant_id)
{
  uint64_t new_tenant_id = OB_INVALID_TENANT_ID;
  if (is_virtual_tenant_id(tenant_id)) {
    new_tenant_id = OB_INVALID_TENANT_ID; //invalid
  } else if (is_sys_tenant(tenant_id) || is_user_tenant(tenant_id)) {
    new_tenant_id = tenant_id;
  } else {
    new_tenant_id = tenant_id + 1;
  }
  return new_tenant_id;
}

OB_INLINE uint64_t gen_meta_tenant_id(const uint64_t tenant_id)
{
  uint64_t new_tenant_id = OB_INVALID_TENANT_ID;
  if (is_virtual_tenant_id(tenant_id)) {
    new_tenant_id = OB_INVALID_TENANT_ID; //invalid
  } else if (is_sys_tenant(tenant_id) || is_meta_tenant(tenant_id)) {
    new_tenant_id = tenant_id;
  } else {
    new_tenant_id = tenant_id - 1;
  }
  return new_tenant_id;
}

// Only work for private table which meta_record_in_sys = True.
// For private table which meta_record_in_sys = False, gen_meta_tenant_id() should be called.
OB_INLINE uint64_t get_private_table_exec_tenant_id(const uint64_t tenant_id)
{
  uint64_t ret_tenant_id = OB_INVALID_TENANT_ID;
  if (is_sys_tenant(tenant_id)) {
    ret_tenant_id = tenant_id;
  } else if (is_meta_tenant(tenant_id)) {
    ret_tenant_id = OB_SYS_TENANT_ID;
  } else {
    ret_tenant_id = gen_meta_tenant_id(tenant_id);
  }
  return ret_tenant_id;
}

#define COMBINE_ID_HIGH_SHIFT 24
#define COMBINE_ID_LOW_SHIFT 40
OB_INLINE uint64_t combine_two_ids(uint64_t high_id, uint64_t low_id)
{
  uint64_t pure_high_id = high_id & (~(UINT64_MAX << COMBINE_ID_HIGH_SHIFT));
  uint64_t pure_low_id = low_id & (~(UINT64_MAX << COMBINE_ID_LOW_SHIFT));
  return (pure_high_id << COMBINE_ID_LOW_SHIFT) | (pure_low_id);
}

const char *const OB_RANDOM_PRIMARY_ZONE = "RANDOM";

OB_INLINE bool is_mlog_reference_column(const uint64_t column_id)
{
  return (common::OB_MLOG_ROWID_COLUMN_ID == column_id);
}

OB_INLINE bool is_mlog_special_column(const uint64_t column_id)
{
  return (column_id >= common::OB_MIN_MLOG_SPECIAL_COLUMN_ID
          && column_id <= common::OB_MAX_MLOG_SPECIAL_COLUMN_ID);
}

OB_INLINE bool is_shadow_column(const uint64_t column_id)
{
  return (column_id > common::OB_MIN_SHADOW_COLUMN_ID)
          && !is_mlog_special_column(column_id);
}

OB_INLINE bool is_bootstrap_resource_pool(const uint64_t resource_pool_id)
{
  return (OB_SYS_RESOURCE_POOL_ID == resource_pool_id);
}

// ob_malloc & ob_tc_malloc
const int64_t OB_MALLOC_NORMAL_BLOCK_SIZE = (1LL << 13) - 256;                 // 8KB
const int64_t OB_MALLOC_MIDDLE_BLOCK_SIZE = (1LL << 16) - 128;                 // 64KB
const int64_t OB_MALLOC_BIG_BLOCK_SIZE = (1LL << 21) - ACHUNK_PRESERVE_SIZE;// 2MB (-17KB)
const int64_t OB_MALLOC_REQ_NORMAL_BLOCK_SIZE = (240LL << 10);                 // 240KB

const int64_t OB_MAX_MYSQL_RESPONSE_PACKET_SIZE = OB_MALLOC_BIG_BLOCK_SIZE;

const int64_t MAX_FRAME_SIZE = OB_MALLOC_BIG_BLOCK_SIZE;

/// Maximum number of elements/columns a row can contain
const int64_t OB_USER_ROW_MAX_COLUMNS_COUNT = 4096;
const int64_t OB_ROW_MAX_COLUMNS_COUNT =
    OB_USER_ROW_MAX_COLUMNS_COUNT + 2 * OB_USER_MAX_ROWKEY_COLUMN_NUMBER; // used in ObRow
const int64_t OB_ROW_DEFAULT_COLUMNS_COUNT = 32;
const int64_t OB_DEFAULT_COL_DEC_NUM = common::OB_ROW_MAX_COLUMNS_COUNT / 80;
const int64_t OB_DEFAULT_MULTI_GET_ROWKEY_NUM = 8;
const int64_t OB_MAX_TIMESTAMP_LENGTH = 32;
// nls_date_format = 'yyyy-mm-dd hh24:mi:ss.ff TZR TZD' max length of TZR is 38, max length of TZD is 6
// 27 + 38 + 1 + 6 = 72; set OB_MAX_TIMESTAMP_TZ_LENGTH = 128 in case add new time zone with a long name
const int64_t OB_MAX_TIMESTAMP_TZ_LENGTH = 128;
const int64_t OB_COMMON_MEM_BLOCK_SIZE = 64 * 1024;
const int64_t OB_MAX_USER_ROW_LENGTH = 1572864L; // 1.5M
const int64_t OB_MAX_ROW_LENGTH = OB_MAX_USER_ROW_LENGTH
                                         + 64L * 1024L/*for root table extra columns*/;
const int64_t OB_MAX_ROW_LENGTH_IN_MEMTABLE = 60L * 1024 * 1024; //60M
const int64_t OB_MAX_MONITOR_INFO_LENGTH = 65535;
const int64_t OB_MAX_CHAR_LENGTH = 256; // Compatible with mysql, unit character mysql is 256
const int64_t OB_MAX_MYSQL_VARCHAR_LENGTH = 65535; // Compatible with mysql, unit character mysql is 256
const int64_t OB_MAX_ORACLE_CHAR_LENGTH_BYTE = 2000; // Compatible with oracle, unit byte oracle is 2000
const int64_t OB_MAX_ORACLE_PL_CHAR_LENGTH_BYTE = 32767; // Compatible with oracle pl/sql, unit byte oracle is 32767
const int64_t OB_MAX_ORACLE_VARCHAR_LENGTH = 32767; // Compatible with oracle, VARCHAR's max length is 4k, PL and SQL unified extended to 32767
// Compatible with oracle, the maximum length of RAW type column in SQL layer is 2000 byte
const int64_t OB_MAX_ORACLE_RAW_SQL_COL_LENGTH = 2000;
// Compatible with oracle, the maximum length of PL layer RAW type variable is 32767 byte
const int64_t OB_MAX_ORACLE_RAW_PL_VAR_LENGTH = 32767;
const int64_t OB_MAX_VARCHAR_LENGTH = 1024L * 1024L; // Unit byte
const int64_t OB_MAX_BIT_LENGTH = 64; // Compatible with mysql, 64 bit
const int64_t OB_MAX_SET_ELEMENT_NUM = 64; // Compatible with mysql8.0, the number of values
const int64_t OB_MAX_INTERVAL_VALUE_LENGTH = 255; // Compatible with mysql, unit character
const int64_t OB_MAX_ENUM_ELEMENT_NUM = 65535; // Compatible with mysql8.0, the number of enum values
const int64_t OB_MAX_QUALIFIED_COLUMN_NAME_LENGTH = 4096; // Compatible with oracle
const int64_t OB_MAX_VARCHAR_LENGTH_KEY = 16 * 1024L;  //KEY key varchar maximum length limit
const int64_t OB_OLD_MAX_VARCHAR_LENGTH = 64 * 1024; // for compatible purpose
// For compatibility we set max default value as 256K bytes/64K chars.
// Otherwise inner core tables schema would changes that hard to upgrade.
const int64_t OB_MAX_DEFAULT_VALUE_LENGTH = 256 * 1024L;
const int64_t OB_MAX_BINARY_LENGTH = 255;
const int64_t OB_MAX_VARBINARY_LENGTH = 64 * 1024L;
const int64_t OB_MAX_EXTENDED_TYPE_INFO_LENGTH = OB_MAX_VARBINARY_LENGTH;//TODO(yts): large object
const int64_t OB_MAX_DECIMAL_PRECISION = 65;
const int64_t OB_MAX_DECIMAL_POSSIBLE_PRECISION = 81;
const int64_t OB_MIN_DECIMAL_PRECISION = 1;
const int64_t OB_MAX_DECIMAL_SCALE = 30;
const int64_t OB_MIN_NUMBER_PRECISION = 1;          //Number in Oracle: p:[1, 38]
const int64_t OB_MAX_NUMBER_PRECISION = 38;          //Number in Oracle: p:[1, 38]
const int64_t OB_MAX_NUMBER_PRECISION_INNER = 40;    //Number in Oracle: p can reach 40 if not define by user
const int64_t OB_MIN_NUMBER_SCALE = -84;             //Number in Oracle: s:[-84, 127]
const int64_t OB_MAX_NUMBER_SCALE = 127;             //Number in Oracle: s:[-84, 127]

// len_ = 2, se_ = 192: 2 digits(e.g: 111.111)
const uint32_t NUM_DESC_2DIGITS_POSITIVE_DECIMAL = 0xc0000002;
// len_ = 1, se_ = 191: 1 digit fragment(e.g 0.111)
const uint32_t NUM_DESC_1DIGIT_POSITIVE_FRAGMENT = 0xbf000001;
// len_ = 1, se_ = 192: 1 digit integer(e.g 111)
const uint32_t NUM_DESC_1DIGIT_POSITIVE_INTEGER = 0xc0000001;
// len_ = 2, se_ = 64: 2 digits(e.g: -111.111)
const uint32_t NUM_DESC_2DIGITS_NEGATIVE_DECIMAL = 0x40000002;
// len_ = 1, se_ = 65: 1 digit fragment(e.g -0.111)
const uint32_t NUM_DESC_1DIGIT_NEGATIVE_FRAGMENT = 0x41000001;
// len_ = 1, se_ = 64: 1 digit integer(e.g -111)
const uint32_t NUM_DESC_1DIGIT_NEGATIVE_INTEGER = 0x40000001;


const int64_t OB_DECIMAL_NOT_SPECIFIED = -1;
const int64_t OB_MIN_NUMBER_FLOAT_PRECISION = 1;     //Float in Oracle: p[1, 126]
const int64_t OB_MAX_NUMBER_FLOAT_PRECISION = 126;
const double OB_PRECISION_BINARY_TO_DECIMAL_FACTOR = 0.30103;
const double OB_PRECISION_DECIMAL_TO_BINARY_FACTOR = 3.32193;
const int64_t OB_DECIMAL_LONGLONG_DIGITS = 22;

const int64_t OB_MAX_DOUBLE_FLOAT_SCALE = 30;
const int64_t OB_NOT_FIXED_SCALE = OB_MAX_DOUBLE_FLOAT_SCALE + 1;
const int64_t OB_MAX_DOUBLE_FLOAT_PRECISION = 53;//why?? mysql is 255 TODO::@yanhua
const int64_t OB_MAX_FLOAT_PRECISION = 24;
const int64_t OB_MAX_INTEGER_DISPLAY_WIDTH = 255; //TINYINT, SMALLINT, MEDIUMINT, INT, BIGINT
const int64_t OB_MAX_DOUBLE_FLOAT_DISPLAY_WIDTH = 255;
const int64_t OB_MAX_COLUMN_NUMBER = OB_ROW_MAX_COLUMNS_COUNT; // used in ObSchemaManagerV2
const int64_t OB_MAX_PARTITION_KEY_COLUMN_NUMBER = OB_MAX_ROWKEY_COLUMN_NUMBER;
const int64_t OB_MAX_USER_DEFINED_COLUMNS_COUNT =
    OB_ROW_MAX_COLUMNS_COUNT - OB_APP_MIN_COLUMN_ID;
const int64_t OB_CAST_TO_VARCHAR_MAX_LENGTH = 256;
const int64_t OB_CAST_TO_JSON_SCALAR_LENGTH = 256;
const int64_t OB_CAST_BUFFER_LENGTH = 256;
const int64_t OB_PREALLOCATED_NUM = 21;  // half of 42
const int64_t OB_PREALLOCATED_COL_ID_NUM = 4;
const int64_t OB_MAX_DATE_PRECISION = 0;
const int64_t OB_MAX_DATETIME_PRECISION = 6;
const int64_t OB_MAX_TIMESTAMP_TZ_PRECISION = 9;

// decimal int related
const int16_t MAX_PRECISION_DECIMAL_INT_32  = 9;
const int16_t MAX_PRECISION_DECIMAL_INT_64  = 18;
const int16_t MAX_PRECISION_DECIMAL_INT_128 = 38;
const int16_t MAX_PRECISION_DECIMAL_INT_256 = 76;
const int16_t MAX_PRECISION_DECIMAL_INT_512 = 154;

const int16_t MAX_SIGNED_INTEGER_PRECISION = 18;

// TODO@hanhui lob handle length will be much shorter in 2.0
const int64_t OB_MAX_LOB_INLINE_LENGTH = OB_MAX_VARCHAR_LENGTH;
const int64_t OB_MAX_LOB_HANDLE_LENGTH = 512L;
const int64_t OB_MAX_TINYTEXT_LENGTH = 256;  // mysql (1LL << 8)
const int64_t OB_MAX_TEXT_LENGTH = 64 * 1024L;  // mysql (1LL << 16)
const int64_t OB_MAX_MEDIUMTEXT_LENGTH = 16 *  1024 * 1024L;  // mysql (1LL << 24)
const int64_t OB_MAX_LONGTEXT_LENGTH = 512 * 1024 * 1024L - 1; // 2^29-1,for datum.len_ only has 29 bit // mysql (1LL << 32)
const int64_t OB_MAX_MEDIUMTEXT_LENGTH_OLD = 256 * 1024L;  // for compatibility
const int64_t OB_MAX_LONGTEXT_LENGTH_OLD = 512 * 1024L;  // for compatibility

const int64_t OB_MIN_LOB_CHUNK_SIZE = 1024; // 1K
const int64_t OB_MAX_LOB_CHUNK_SIZE = 256 * 1024; // 256K
const int64_t OB_DEFAULT_LOB_CHUNK_SIZE = OB_MAX_LOB_CHUNK_SIZE;

const int64_t OB_MIN_LOB_INROW_THRESHOLD = 0; // 0 means disable inrow lob
const int64_t OB_MAX_LOB_INROW_THRESHOLD = OB_MAX_USER_ROW_LENGTH / 2; // 1.5M/2
const int64_t OB_DEFAULT_LOB_INROW_THRESHOLD = 4096; // 4K

// this's used for ob_default_lob_inrow_threshold system variable
// used to set the default inrow threshold for newly created tables
const int64_t OB_SYS_VAR_DEFAULT_LOB_INROW_THRESHOLD = 8192; // 8K

const int64_t OB_MAX_CAST_CHAR_VARCHAR_LENGTH = 512;
const int64_t OB_MAX_CAST_CHAR_TEXT_LENGTH = 16383;
const int64_t OB_MAX_CAST_CHAR_MEDIUMTEXT_LENGTH = 4194303;

const char *const SYS_DATE = "$SYS_DATE";
const char *const OB_DEFAULT_COMPRESS_FUNC_NAME = "none";
const char *const OB_DEFAULT_FULLTEXT_PARSER_NAME = "space";

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

//all_outline related
const int64_t OB_MAX_OUTLINE_CATEGORY_NAME_LENGTH = 64;
const int64_t OB_MAX_OUTLINE_SIGNATURE_LENGTH = OB_MAX_VARBINARY_LENGTH;
const int64_t OB_MAX_OUTLINE_PARAMS_LENGTH = OB_MAX_VARBINARY_LENGTH;
const int64_t OB_MAX_HINT_FORMAT_LENGTH = 16;

//procedure related
const int64_t OB_MAX_PROC_ENV_LENGTH = 2048;
const int64_t OB_MAX_PROC_PARAM_COUNT = 65536;

//udt related
static const int64_t OB_MAX_TYPE_ATTR_COUNT = 65536;

const int64_t OB_AUTO_PROGRESSIVE_MERGE_NUM = 100;
const int64_t OB_DEFAULT_PROGRESSIVE_MERGE_NUM = 0;
const int64_t OB_DEFAULT_PROGRESSIVE_MERGE_ROUND = 1;
const int64_t OB_DEFAULT_STORAGE_FORMAT_VERSION = 3;
const int64_t OB_DEFAULT_MACRO_BLOCK_SIZE = 2 << 20; // 2MB
const int64_t OB_DEFAULT_SSTABLE_BLOCK_SIZE = 16 * 1024; // 16KB
const int64_t OB_DEFAULT_MAX_TABLET_SIZE = 256 * 1024 * 1024; // 256MB
const int64_t OB_MAX_MACRO_BLOCK_TYPE = 16;
const int32_t OB_DEFAULT_CHARACTER_SET = 33; //UTF8
const int64_t OB_MYSQL_PACKET_BUFF_SIZE = 6 * 1024; //6KB
const int64_t OB_MAX_THREAD_NUM = 4096;
const int64_t OB_RESERVED_THREAD_NUM = 128; // Naked threads created with pthread_create, such as easy
const int32_t OB_MAX_SYS_BKGD_THREAD_NUM = 64;
#if __x86_64__
const int64_t OB_MAX_CPU_NUM = 64;
#elif __aarch64__
const int64_t OB_MAX_CPU_NUM = 128;
#endif
const int64_t OB_MAX_STATICS_PER_TABLE = 128;

const uint64_t OB_DEFAULT_INDEX_ATTRIBUTES_SET = 0;
const uint64_t OB_DEFAULT_INDEX_VISIBILITY = 0;//0 menas visible;1 means invisible

const uint64_t OB_MAX_BULK_JOIN_ROWS = 1000;

const int64_t OB_INDEX_WRITE_START_DELAY = 20 * 1000 * 1000; //20s

const int64_t MAX_SQL_ERR_MSG_LENGTH = 256;
const int64_t MSG_SIZE = MAX_SQL_ERR_MSG_LENGTH;
const int64_t OB_DUMP_ROOT_TABLE_TYPE = 1;
const int64_t OB_DUMP_UNUSUAL_TABLET_TYPE = 2;
const int64_t OB_MAX_SYS_VAR_NON_STRING_VAL_LENGTH = 128;
const int64_t OB_MAX_SYS_VAR_VAL_LENGTH = 4096;//original 128 is too small
const int64_t OB_MAX_TCP_INVITED_NODES_LENGTH = 64 * 1024; // 64K

// bitset defines
const int64_t OB_DEFAULT_BITSET_SIZE = OB_MAX_TABLE_NUM_PER_STMT;
const int64_t OB_DEFAULT_BITSET_SIZE_FOR_BASE_COLUMN = 64;
const int64_t OB_DEFAULT_BITSET_SIZE_FOR_ALIAS_COLUMN = 32;
const int64_t OB_MAX_BITSET_SIZE = OB_ROW_MAX_COLUMNS_COUNT;
const int64_t OB_DEFAULT_STATEMEMT_LEVEL_COUNT = 16;
const int64_t OB_DEFAULT_BITSET_SIZE_FOR_DFM = 64;

// max number of existing ObIStores for each partition,
// which contains ssstore, memstore and frozen stores
const int64_t DEFAULT_STORE_CNT_IN_STORAGE = 8;
const int64_t MAX_SSTABLE_CNT_IN_STORAGE = 64;
const int64_t RESERVED_STORE_CNT_IN_STORAGE = 8; // Avoid mistakenly triggering minor or major freeze to cause the problem of unsuccessful merge.
const int64_t DIAGNOSE_TABLE_CNT_IN_STORAGE = 12;
const int64_t MAX_FROZEN_MEMSTORE_CNT_IN_STORAGE = 7;
const int64_t BASIC_MEMSTORE_CNT = 8;
const int64_t MAX_MEMSTORE_CNT = 16;
// some frozen memstores and one active memstore
// Only limited to minor freeze, major freeze is not subject to this restriction
const int64_t MAX_MEMSTORE_CNT_IN_STORAGE = MAX_FROZEN_MEMSTORE_CNT_IN_STORAGE + 1;
const int64_t MAX_TX_DATA_TABLE_STATE_LENGTH = 20;
const int64_t MAX_TX_DATA_STATE_LENGTH = 16;
const int64_t MAX_UNDO_LIST_CHAR_LENGTH = 4096;
const int64_t MAX_TX_OP_CHAR_LENGTH = 4096;
const int64_t MAX_TABLE_CNT_IN_STORAGE = MAX_SSTABLE_CNT_IN_STORAGE + MAX_MEMSTORE_CNT;
const int64_t OB_MAX_PARTITION_NUM_MYSQL = 8192;
const int64_t OB_MAX_PARTITION_NUM_ORACLE = 65536;

//Used to indicate the visible range of configuration items and whether to restart after modification to take effect
const char *const OB_CONFIG_SECTION_DEFAULT = "DEFAULT";
const char *const OB_CONFIG_VISIBLE_LEVEL_USER = "USER";
const char *const OB_CONFIG_VISIBLE_LEVEL_SYS = "SYS";
const char *const OB_CONFIG_VISIBLE_LEVEL_DBA = "DBA";
const char *const OB_CONFIG_VISIBLE_LEVEL_MEMORY = "MEMORY";

//Precision in user data type
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
const int16_t DEFAULT_SCALE_FOR_ORACLE_FRACTIONAL_SECONDS = 6;  //SEE : https://docs.oracle.com/cd/B19306_01/server.102/b14225/ch4datetime.htm
const int16_t DEFUALT_PRECISION_FOR_INTERVAL = 2;

#define NUMBER_SCALE_UNKNOWN_YET (lib::is_oracle_mode() ? ORA_NUMBER_SCALE_UNKNOWN_YET: SCALE_UNKNOWN_YET)
//TDE相关参数
const int64_t MAX_ENCRYPTION_SECRET_LENGTH = 256;
const int64_t MAX_DECRYPTION_SECRET_LENGTH = 256;
const int64_t MAX_ENCRYPTION_ALGORITHM_LENGTH = 256;
const char *const OB_MASTER_KEY_FILE = "wallet/master-key.bin";

const int64_t OB_MAX_FAILLIST_LENGTH = 1024;

const int64_t OB_MAX_MODE_CNT = 2;
const uint32_t BATCH_RPC_PORT_DELTA = 0;
const uint32_t HIGH_PRIO_RPC_PORT_DELTA = 0;

const int64_t UNIQ_TASK_QUEUE_BATCH_EXECUTE_NUM = 128;

const char *const OB_SSL_CA_FILE = "wallet/ca.pem";
const char *const OB_SSL_CERT_FILE = "wallet/server-cert.pem";
const char *const OB_SSL_KEY_FILE = "wallet/server-key.pem";

const char *const OB_SSL_SM_SIGN_CERT_FILE = "wallet/SS.cert.pem";
const char *const OB_SSL_SM_SIGN_KEY_FILE  = "wallet/SS.key.pem";
const char *const OB_SSL_SM_ENC_CERT_FILE  = "wallet/SE.cert.pem";
const char *const OB_SSL_SM_ENC_KEY_FILE   = "wallet/SE.key.pem";

const int64_t MAX_CLUSTER_IDX_VALUE = 32;

//Application context
const int64_t OB_MAX_CONTEXT_STRING_LENGTH = 128;
const int64_t OB_MAX_CONTEXT_TYPE_LENGTH = 22;
const int64_t OB_MAX_CONTEXT_TRACKING_LENGTH = 3;
const int64_t OB_MAX_CONTEXT_VALUE_LENGTH = 4000;
const int64_t OB_MAX_CONTEXT_CLIENT_IDENTIFIER_LENGTH = 65;
const int64_t OB_MAX_CONTEXT_CLIENT_IDENTIFIER_LENGTH_IN_SESSION = 64;

// Resource limit calculator
const int64_t MAX_RESOURCE_NAME_LEN = 128;
const int64_t MAX_CONSTRAINT_NAME_LEN = 128;

// log row value options
const char *const OB_LOG_ROW_VALUE_PARTIAL_LOB = "partial_lob";
const char *const OB_LOG_ROW_VALUE_PARTIAL_JSON = "partial_json";
const char *const OB_LOG_ROW_VALUE_PARTIAL_ALL = "partial_all";
// json partial update expr flag
enum ObJsonPartialUpdateFlag
{
  OB_JSON_PARTIAL_UPDATE_ALLOW = 1 << 0,
  OB_JSON_PARTIAL_UPDATE_LAST_EXPR = 1 << 1,
  OB_JSON_PARTIAL_UPDATE_FIRST_EXPR = 1 << 2,
};

enum ObDmlType
{
  OB_DML_UNKNOW = 0,
  OB_DML_REPLACE = 1,
  OB_DML_INSERT = 2,
  OB_DML_UPDATE = 3,
  OB_DML_DELETE = 4,
  OB_DML_MERGED = 5,
  OB_DML_NUM,
};

//check whether an id is valid
OB_INLINE bool is_valid_id(const uint64_t id)
{
  return (OB_INVALID_ID != id);
}
//check whether an index is valid
OB_INLINE bool is_valid_idx(const int64_t idx)
{
  return (0 <= idx);
}

// check whether a server_id is valid
OB_INLINE bool is_valid_server_id(const uint64_t server_id)
{
  return (0 < server_id) && (OB_INVALID_ID != server_id);
}

//check whether an tenant_id is valid
OB_INLINE bool is_valid_tenant_id(const uint64_t tenant_id)
{
  return (0 < tenant_id) && (OB_INVALID_ID != tenant_id) && (OB_INVALID_TENANT_ID != tenant_id);
}

//Tenants who can use gts, system tenants do not use gts, to avoid circular dependencies
OB_INLINE bool is_valid_no_sys_tenant_id(const uint64_t tenant_id)
{
  return is_valid_tenant_id(tenant_id) && (OB_SYS_TENANT_ID != tenant_id);
}

OB_INLINE bool is_valid_gts_id(const uint64_t gts_id)
{
  return OB_INVALID_ID != gts_id && 0 != gts_id;
}

//check whether an cluster_id is valid
OB_INLINE bool is_valid_cluster_id(const int64_t cluster_id)
{
  return (cluster_id >= OB_MIN_CLUSTER_ID && cluster_id <= OB_MAX_CLUSTER_ID);
}

OB_INLINE bool is_virtual_tenant_for_memory(const uint64_t tenant_id)
{
  return is_virtual_tenant_id(tenant_id);
}

enum ObNameCaseMode
{
  OB_NAME_CASE_INVALID = -1,
  OB_ORIGIN_AND_SENSITIVE = 0,//stored using lettercase, and name comparisons are case sensitive
  OB_LOWERCASE_AND_INSENSITIVE = 1,//stored using lowercase, and name comparisons are case insensitive
  OB_ORIGIN_AND_INSENSITIVE = 2,//stored using lettercase, and name comparisons are case insensitive
  OB_NAME_CASE_MAX,
};

enum ObFreezeStatus
{
  INIT_STATUS = 0,
  PREPARED_SUCCEED,
  COMMIT_SUCCEED,
  FREEZE_STATUS_MAX,
};

/*
 * |---- 2 bits ---|--- 4 bits ---|--- 2 bits ---|--- 2 bits ---| LSB
 * |-- encryption--|---  clog  ---|-- SSStore ---|--- MemStore--| LSB
 */
const int64_t MEMSTORE_BITS_SHIFT = 0;
const int64_t SSSTORE_BITS_SHIFT = 2;
const int64_t CLOG_BITS_SHIFT = 4;
const int64_t ENCRYPTION_BITS_SHIFT = 8;
const int64_t REPLICA_TYPE_MEMSTORE_MASK = (0x3UL << MEMSTORE_BITS_SHIFT);
const int64_t REPLICA_TYPE_SSSTORE_MASK = (0x3UL << SSSTORE_BITS_SHIFT);
const int64_t REPLICA_TYPE_CLOG_MASK = (0xFUL << CLOG_BITS_SHIFT);
const int64_t REPLICA_TYPE_ENCRYPTION_MASK = (0x3UL << ENCRYPTION_BITS_SHIFT);
// replica type associated with memstore
const int64_t WITH_MEMSTORE = 0;
const int64_t WITHOUT_MEMSTORE = 1;
// replica type associated with ssstore
const int64_t WITH_SSSTORE = 0 << SSSTORE_BITS_SHIFT;
const int64_t WITHOUT_SSSTORE = 1 << SSSTORE_BITS_SHIFT;
// replica type associated with clog
const int64_t SYNC_CLOG = 0 << CLOG_BITS_SHIFT;
const int64_t ASYNC_CLOG = 1 << CLOG_BITS_SHIFT;
// replica type associated with encryption
const int64_t WITHOUT_ENCRYPTION = 0 << ENCRYPTION_BITS_SHIFT;
const int64_t WITH_ENCRYPTION = 1 << ENCRYPTION_BITS_SHIFT;

// tracepoint, refer to OB_MAX_CONFIG_xxx
const int64_t OB_MAX_TRACEPOINT_NAME_LEN = 128;
const int64_t OB_MAX_TRACEPOINT_DESCRIBE_LEN = 4096;

// Need to manually maintain the replica_type_to_str function in utility.cpp,
// Currently there are only three types: REPLICA_TYPE_FULL, REPLICA_TYPE_READONLY, and REPLICA_TYPE_LOGONLY
enum ObReplicaType
{
  // Almighty copy: is a member of paxos; has ssstore; has memstore
  REPLICA_TYPE_FULL = (SYNC_CLOG | WITH_SSSTORE | WITH_MEMSTORE), // 0
  // Backup copy: Paxos member; ssstore; no memstore
  REPLICA_TYPE_BACKUP = (SYNC_CLOG | WITH_SSSTORE | WITHOUT_MEMSTORE), // 1
  // Memory copy; no ssstore; memstore
  //REPLICA_TYPE_MMONLY = (SYNC_CLOG | WITHOUT_SSSTORE | WITH_MEMSTORE), // 4
  // Journal copy: Paxos member; no ssstore; no memstore
  REPLICA_TYPE_LOGONLY = (SYNC_CLOG | WITHOUT_SSSTORE | WITHOUT_MEMSTORE), // 5
  // Read-only copy: not a member of paxos; ssstore; memstore
  REPLICA_TYPE_READONLY = (ASYNC_CLOG | WITH_SSSTORE | WITH_MEMSTORE), // 16
  // Incremental copy: not a member of paxos; no ssstore; memstore
  REPLICA_TYPE_MEMONLY = (ASYNC_CLOG | WITHOUT_SSSTORE | WITH_MEMSTORE), // 20
  // TODO by yunlong: is it proper to use ASYNC_CLOG
  // Arbitration copy: a member of paxos; no ssstore; no memstore
  REPLICA_TYPE_ARBITRATION = (ASYNC_CLOG | WITHOUT_SSSTORE | WITHOUT_MEMSTORE), // 21
  // Encrypted log copy: encrypted; paxos member; no sstore; no memstore
  REPLICA_TYPE_ENCRYPTION_LOGONLY = (WITH_ENCRYPTION | SYNC_CLOG | WITHOUT_SSSTORE | WITHOUT_MEMSTORE), // 261
  // invalid value
  REPLICA_TYPE_MAX,
};

static inline int replica_type_to_string(const ObReplicaType replica_type, char *name_str, const int64_t str_len)
{
  int ret = OB_SUCCESS;
  switch(replica_type) {
  case REPLICA_TYPE_FULL: {
    strncpy(name_str ,"FULL", str_len);
    break;
  }
  case REPLICA_TYPE_BACKUP: {
    strncpy(name_str ,"BACKUP", str_len);
    break;
  }
  case REPLICA_TYPE_LOGONLY: {
    strncpy(name_str ,"LOGONLY", str_len);
    break;
  }
  case REPLICA_TYPE_READONLY: {
    strncpy(name_str ,"READONLY", str_len);
    break;
  }
  case REPLICA_TYPE_MEMONLY: {
    strncpy(name_str ,"MEMONLY", str_len);
    break;
  }
  case REPLICA_TYPE_ENCRYPTION_LOGONLY: {
    strncpy(name_str ,"ENCRYPTION_LOGONLY", str_len);
    break;
  }
  default: {
    ret = OB_INVALID_ARGUMENT;
    strncpy(name_str ,"INVALID", str_len);
    break;
  } // default
  } // switch
  return ret;
}

class ObReplicaTypeCheck
{
public:
  static bool is_replica_type_valid(const int32_t replica_type)
  {
    return REPLICA_TYPE_FULL == replica_type
           || REPLICA_TYPE_READONLY == replica_type;
  }
  static bool is_can_elected_replica(const int32_t replica_type)
  {
    return is_paxos_replica_V2(replica_type);
  }
  static bool is_full_replica(const int32_t replica_type)
  {
    return (REPLICA_TYPE_FULL == replica_type);
  }
  static bool is_readonly_replica(const int32_t replica_type)
  {
    return (REPLICA_TYPE_READONLY == replica_type);
  }
  static bool is_log_replica(const int32_t replica_type)
  {
    return (REPLICA_TYPE_LOGONLY == replica_type || REPLICA_TYPE_ENCRYPTION_LOGONLY == replica_type);
  }
  static bool is_paxos_replica_V2(const int32_t replica_type)
  {
    return (replica_type >= REPLICA_TYPE_FULL && replica_type <= REPLICA_TYPE_LOGONLY)
           || (REPLICA_TYPE_ENCRYPTION_LOGONLY == replica_type);
  }
  static bool is_paxos_replica(const int32_t replica_type)
  {
    return (replica_type >= REPLICA_TYPE_FULL && replica_type <= REPLICA_TYPE_LOGONLY)
            || (REPLICA_TYPE_ENCRYPTION_LOGONLY == replica_type);
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
    return  (dest_replica_type == src_replica_type
             || REPLICA_TYPE_FULL == src_replica_type); // TODO temporarily only supports the same type or F as the data source
  }
  //Currently only copies of F and R can be used for machine reading, not L
  static bool can_slave_read_replica(const int32_t replica_type)
  {
    return (REPLICA_TYPE_FULL == replica_type || REPLICA_TYPE_READONLY == replica_type);
  }

  static bool change_replica_op_allow(const ObReplicaType source, const ObReplicaType target)
  {
    bool bool_ret = false;

    if (REPLICA_TYPE_LOGONLY == source || REPLICA_TYPE_LOGONLY == target) {
      bool_ret = false;
    } else if (REPLICA_TYPE_FULL == source) {
      bool_ret = true;
    } else if (REPLICA_TYPE_READONLY == source && REPLICA_TYPE_FULL == target) {
      bool_ret = true;
    }
    return bool_ret;
  }
};

class ObMemstorePercentCheck
{
public:
  static bool is_memstore_percent_valid(const int64_t memstore_percent)
  {
    return 0 == memstore_percent || 100 == memstore_percent;
  }
};

enum ObReplicaOpPriority
{
  PRIO_HIGH = 0,
  PRIO_LOW = 1,
  PRIO_MID = 2,
  PRIO_INVALID
};

inline bool is_replica_op_priority_valid(const ObReplicaOpPriority priority)
{
  return priority >= ObReplicaOpPriority::PRIO_HIGH && priority < ObReplicaOpPriority::PRIO_INVALID;
}

char *lbt();

enum ObConsistencyLevel
{
  INVALID_CONSISTENCY = -1,
  FROZEN = 1,
  WEAK,
  STRONG,
};

enum ObCompatibilityMode
{
  OCEANBASE_MODE = -1,
  MYSQL_MODE = 0,
  ORACLE_MODE,
};

enum ObOrderType
{
  ASC = 0,
  DESC = -1,
};

enum ObJITEnableMode
{
  OFF = 0,
  AUTO = 1,
  FORCE = 2,
};

enum ObCursorSharingMode
{
  FORCE_MODE = 0,
  EXACT_MODE = 1
};

enum ObWFRemoveMode
{
  REMOVE_INVALID = 0,
  REMOVE_STATISTICS = 1,
  REMOVE_EXTRENUM = 2
};

enum ObTraceGranularity
{
  QUERY_LEVEL = 0,
  TRANS_LEVEL = 1
};

const uint64_t OB_LISTENER_GID = 0;

} // end namespace common
} // end namespace oceanbase


// For the serialize function pos is both an input parameter and an output parameter,
// serialize writes the serialized data from (buf+pos),
// Update pos after writing is completed. If the data after writing exceeds (buf+buf_len),
// serialize returned failed.
//
// For the deserialize function pos is both an input parameter and an output parameter,
// deserialize reads data from (buf+pos) for deserialization,
// Update pos after completion. If the data required for deserialization exceeds (buf+data_len),
// deserialize returned failed.

#define NEED_SERIALIZE_AND_DESERIALIZE \
  int serialize(char* buf, const int64_t buf_len, int64_t& pos) const; \
  int deserialize(const char* buf, const int64_t data_len, int64_t& pos); \
  int64_t get_serialize_size(void) const

#define INLINE_NEED_SERIALIZE_AND_DESERIALIZE \
  inline int serialize(char* buf, const int64_t buf_len, int64_t& pos) const; \
  inline int deserialize(const char* buf, const int64_t data_len, int64_t& pos); \
  inline int64_t get_serialize_size(void) const

#define VIRTUAL_NEED_SERIALIZE_AND_DESERIALIZE \
  virtual int serialize(char* buf, const int64_t buf_len, int64_t& pos) const; \
  virtual int deserialize(const char* buf, const int64_t data_len, int64_t& pos); \
  virtual int64_t get_serialize_size(void) const

#define PURE_VIRTUAL_NEED_SERIALIZE_AND_DESERIALIZE \
  virtual int serialize(char* buf, const int64_t buf_len, int64_t& pos) const = 0; \
  virtual int deserialize(const char* buf, const int64_t data_len, int64_t& pos) = 0; \
  virtual int64_t get_serialize_size(void) const = 0

#define DEFINE_SERIALIZE(TypeName) \
  int TypeName::serialize(char* buf, const int64_t buf_len, int64_t& pos) const

#define DEFINE_DESERIALIZE(TypeName) \
  int TypeName::deserialize(const char* buf, const int64_t data_len, int64_t& pos)

#define DEFINE_GET_SERIALIZE_SIZE(TypeName) \
  int64_t TypeName::get_serialize_size(void) const

#define DATABUFFER_SERIALIZE_INFO \
  data_buffer_.get_data(), data_buffer_.get_capacity(), data_buffer_.get_position()

#define DIO_ALIGN_SIZE 4096
#define DIO_READ_ALIGN_SIZE 4096
#define DIO_ALLOCATOR_CACHE_BLOCK_SIZE (OB_DEFAULT_MACRO_BLOCK_SIZE + DIO_READ_ALIGN_SIZE)
#define MALLOC_INIT_PRIORITY 128
#define NORMAL_INIT_PRIORITY (MALLOC_INIT_PRIORITY + 1)

//judge int64_t multiplication whether overflow
inline bool is_multi_overflow64(int64_t a, int64_t b)
{
  bool ret = false;
  if (0 == b || 0 == a) {
    ret = false;
  }
  //min / -1 will overflow, so can't use the next rule to judge
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

#define IS_ADD_OVERFLOW64(a, b, ret) \
  ((0 == ((static_cast<uint64_t>(a) >> 63) ^ (static_cast<uint64_t>(b) >> 63))) \
   && (1 == ((static_cast<uint64_t>(a) >> 63) ^ (static_cast<uint64_t>(ret) >> 63))))

#ifdef __ENABLE_PRELOAD__
#include "lib/utility/ob_preload.h"
#endif

struct ObNumberDesc
{
  explicit ObNumberDesc(): desc_(0) {}
  explicit ObNumberDesc(const uint32_t desc): desc_(desc) {}
  explicit ObNumberDesc(const uint8_t len, uint8_t flag, uint8_t exp, uint8_t sign)
                       : len_(len), reserved_(0), flag_(flag), exp_(exp), sign_(sign) {}

  bool is_2d_positive_decimal()
  {
    return (desc_ == oceanbase::common::NUM_DESC_2DIGITS_POSITIVE_DECIMAL);
  }
  bool is_1d_positive_fragment()
  {
    return (desc_ == oceanbase::common::NUM_DESC_1DIGIT_POSITIVE_FRAGMENT);
  }
  bool is_1d_positive_integer()
  {
    return (desc_ == oceanbase::common::NUM_DESC_1DIGIT_POSITIVE_INTEGER);
  }

  bool is_2d_negative_decimal()
  {
    return (desc_ == oceanbase::common::NUM_DESC_2DIGITS_NEGATIVE_DECIMAL);
  }
  bool is_1d_negative_fragment()
  {
    return (desc_ == oceanbase::common::NUM_DESC_1DIGIT_NEGATIVE_FRAGMENT);
  }
  bool is_1d_negative_integer()
  {
    return (desc_ == oceanbase::common::NUM_DESC_1DIGIT_NEGATIVE_INTEGER);
  }

  union
  {
    uint32_t desc_;
    struct
    {
      uint8_t len_;
      uint8_t reserved_;
      uint8_t flag_;
      union
      {
        uint8_t se_;
        struct
        {
          uint8_t exp_:7;
          uint8_t sign_:1;
        };
      };
    };
  };
};

#define DEFINE_ALLOCATOR_WRAPPER \
  class IAllocator \
  { \
  public: \
    virtual ~IAllocator() {}; \
    virtual void *alloc(const int64_t size) = 0; \
    virtual void *alloc(const int64_t size, const lib::ObMemAttr &attr) = 0; \
  }; \
  template <class T> \
  class TAllocator : public IAllocator \
  { \
  public: \
    explicit TAllocator(T &allocator) : allocator_(allocator) {}; \
    void *alloc(const int64_t size)  { return allocator_.alloc(size); }; \
    void *alloc(const int64_t size, const lib::ObMemAttr &attr) \
    {\
      UNUSED(attr);\
      return alloc(size);\
    }; \
  private: \
    T &allocator_; \
  };

// need define:
//   ObRow row;
//   ObObj obj;
#define ROW_CELL_SET_VAL(table_id, type, val) \
  obj.set_##type(val);\
  if (OB_SUCCESS == ret  \
      && OB_SUCCESS != (ret = row.set_cell(table_id, ++column_id, obj))) \
  {\
    _OB_LOG(WARN, "failed to set cell=%s, ret=%d", to_cstring(obj), ret); \
  }

OB_INLINE int64_t &get_tid_cache()
{
  // NOTE: cache tid
  struct TID {
    TID() : v_(-1) {}
    int64_t v_;
  };
  RLOCAL_INLINE(TID, tid);
  return (&tid)->v_;
}

OB_INLINE int64_t &get_seq()
{
  RLOCAL_INLINE(int64_t, seq);
  return seq;
}

OB_INLINE bool &tl_need_speed_limit()
{
  RLOCAL_INLINE(bool, tl_need_speed_limit);
  return tl_need_speed_limit;
}

OB_INLINE uint32_t &get_writing_throttling_sleep_interval()
{
  RLOCAL_INLINE(uint32_t, writing_throttling_sleep_interval);
  return writing_throttling_sleep_interval;
}

// should be called after fork/daemon
OB_INLINE void reset_tid_cache()
{
  get_tid_cache() = -1;
}

OB_INLINE int64_t ob_gettid()
{
  int64_t &tid = get_tid_cache();
  if (OB_UNLIKELY(tid <= 0)) {
    tid = static_cast<int64_t>(syscall(__NR_gettid));
  }
  return tid;
}

OB_INLINE uint64_t& ob_get_tenant_id()
{
  thread_local uint64_t tenant_id = 0;
  return tenant_id;
}

OB_INLINE char* ob_get_tname()
{
  thread_local char tname[oceanbase::OB_THREAD_NAME_BUF_LEN] = {0};
  return tname;
}

OB_INLINE char* ob_get_origin_thread_name()
{
  thread_local char ori_tname[oceanbase::OB_THREAD_NAME_BUF_LEN] = {0};
  return ori_tname;
}

static const char* PARALLEL_DDL_THREAD_NAME = "DDLPQueueTh";
static const char* REPLAY_SERVICE_THREAD_NAME = "ReplaySrv";

// There are many clusters in arbitration server, we need a field identify the different clusters.
OB_INLINE int64_t &ob_get_cluster_id()
{
  RLOCAL(int64_t, cluster_id);
  return cluster_id;
}

OB_INLINE int64_t &ob_get_arb_tenant_id()
{
  RLOCAL(int64_t, arb_tenant_id);
  return arb_tenant_id;
}
extern __thread uint64_t tl_thread_tenant_id;
OB_INLINE uint64_t ob_thread_tenant_id()
{
  return tl_thread_tenant_id;
}
OB_INLINE uint64_t ob_set_thread_tenant_id(uint64_t tenant_id)
{
  return tl_thread_tenant_id = tenant_id;
}

#define GETTID() ob_gettid()
#define GETTNAME() ob_get_tname()
#define GET_TENANT_ID() ob_get_tenant_id()
#define gettid GETTID
#define GET_CLUSTER_ID() ob_get_cluster_id()
#define GET_ARB_TENANT_ID() ob_get_arb_tenant_id()

//for explain
#define LEFT_BRACKET "("
#define RIGHT_BRACKET ")"
#define COMMA_REVERSE ",Reverse"
#define BRACKET_REVERSE "(Reverse)"

#define LITERAL_PREFIX_DATE      "DATE"
#define LITERAL_PREFIX_TIMESTAMP "TIMESTAMP"
#define ORACLE_LITERAL_PREFIX_INTERVAL "INTERVAL"
#define MYSQL_LITERAL_PREFIX_TIME "TIME"
inline bool is_x86() {
#if defined(__x86_64__)
  return true;
#else
  return false;
#endif
}
#define __maybe_unused  __attribute__((unused))
#define DO_PRAGMA(x) _Pragma(#x)
#define DISABLE_WARNING_GCC_PUSH _Pragma("GCC diagnostic push")
#define DISABLE_WARNING_GCC(option) DO_PRAGMA(GCC diagnostic ignored option)
#define DISABLE_WARNING_GCC_POP _Pragma("GCC diagnostic pop")
#define DISABLE_WARNING_GCC_ATTRIBUTES DISABLE_WARNING_GCC("-Wattributes")

extern "C" {
extern int ob_pthread_cond_wait(pthread_cond_t *__restrict __cond,
                                pthread_mutex_t *__restrict __mutex);
extern int ob_pthread_cond_timedwait(pthread_cond_t *__restrict __cond,
                                     pthread_mutex_t *__restrict __mutex,
                                     const struct timespec *__restrict __abstime);
}


#endif // OCEANBASE_COMMON_DEFINE_H_
