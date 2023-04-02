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
 *
 * Common Header
 */

#ifndef OCEANBASE_LIBOBCDC_COMMON_H__
#define OCEANBASE_LIBOBCDC_COMMON_H__

#include <stdint.h>

namespace oceanbase
{
namespace libobcdc
{

#define LOG_STAT(level, format_str, ...) OBLOG_LOG(level, "STAT: " format_str, ##__VA_ARGS__)

#define DEFAULT_LOG_DIR "./log/"
#define DEFAULT_LOG_FILE DEFAULT_LOG_DIR "libobcdc.log"
#define DEFAULT_STDERR_LOG_FILE DEFAULT_LOG_DIR "libobcdc.log.stderr"
#define DEFAULT_LOG_FILE_NAME "libobcdc.log"
#define DEFAULT_STDERR_LOG_FILE_NAME "libobcdc.log.stderr"
#define DEFAULT_TIMEZONE "+8:00"
#define DEFAULT_PID_FILE_DIR "./run/"
#define DEFAULT_PID_FILE DEFAULT_PID_FILE_DIR "libobcdc.pid"
#define DEFAULT_CONFIG_FPATN "etc/libobcdc.conf"
#define DEFAULT_TIMEZONE_INFO_FPATH "etc/timezone_info.conf"

#define DEFAULT_PENDING_TRANS_INFO_FILE "./log/pending_trans_info.log"

static const int64_t MAX_LOG_FILE_SIZE = 1 << 28;
static const int64_t MAX_MEMORY_USAGE_PERCENT = 80;
static const int64_t DEFAULT_QUEUE_SIZE = 100000;
static const int64_t DEFAULT_START_SEQUENCE_NUM = 0;
static const int64_t MAX_CACHED_TRANS_CTX_COUNT = 10 * 10000;
static const int64_t RELOAD_CONFIG_INTERVAL = 10 * 1000 * 1000;
static const int64_t PRINT_GLOBAL_FLOW_CONTROL_INTERVAL = 5 * 1000 * 1000;
static const int64_t SINGLE_INSTANCE_NUMBER = 1;                  // single instance
static const int64_t GET_SCHEMA_TIMEOUT_ON_START_UP = 7200LL * 1000 * 1000;     // Start moment, get schema timeout

// column id of table __all_ddl_operation(used by libobcdc)
static const uint64_t ALL_DDL_OPERATION_TABLE_SCHEMA_VERSION_COLUMN_ID = 18;
static const uint64_t ALL_DDL_OPERATION_TABLE_TENANT_ID_COLUMN_ID = 19;
static const uint64_t ALL_DDL_OPERATION_TABLE_DATABASE_ID_COLUMN_ID = 21;
static const uint64_t ALL_DDL_OPERATION_TABLE_TABLEGROUP_ID_COLUMN_ID = 23;
static const uint64_t ALL_DDL_OPERATION_TABLE_TABLE_ID_COLUMN_ID = 24;
static const uint64_t ALL_DDL_OPERATION_TABLE_OPERATION_TYPE_COLUMN_ID = 26;
static const uint64_t ALL_DDL_OPERATION_TABLE_DDL_STMT_STR_COLUMN_ID = 27;
static const uint64_t ALL_DDL_OPERATION_TABLE_EXEC_TENANT_ID_COLUMN_ID = 28;

} // namespace libobcdc
} // namespace oceanbase

#endif
