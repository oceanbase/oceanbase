/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_LOG_MINER_UNITTEST_UTILS_H_
#define OCEANBASE_LOG_MINER_UNITTEST_UTILS_H_

#include "ob_log_miner_br.h"
#include "rpc/obmysql/ob_mysql_global.h"
#include "gtest/gtest.h"
#define private public
#include "ob_log_miner_record.h"
#undef private
namespace oceanbase
{
namespace oblogminer
{
ObLogMinerBR *build_logminer_br(binlogBuf *new_bufs,
              binlogBuf *old_bufs,
              RecordType type,
              lib::Worker::CompatMode compat_mode,
              const char *db_name,
              const char *table_name,
              const int arg_count, ...);

ObLogMinerBR *build_logminer_br(binlogBuf *new_bufs,
              binlogBuf *old_bufs,
              RecordType type,
              lib::Worker::CompatMode compat_mode,
              const char *db_name,
              const char *table_name,
              const char *encoding,
              const int arg_count, ...);

void destroy_miner_br(ObLogMinerBR *&br);

ObLogMinerRecord *build_logminer_record(ObIAllocator &alloc,
                  lib::Worker::CompatMode	compat_mode,
                  uint64_t tenant_id,
                  int64_t orig_cluster_id,
                  const char *tenant_name,
                  const char *database_name,
                  const char *tbl_name,
                  int64_t trans_id,
                  const char* const * pks,
                  const int64_t pk_cnt,
                  const char* const * uks,
                  const int64_t uk_cnt,
                  const char* row_unique_id,
                  RecordType record_type,
                  int64_t commit_ts_us,
                  const char * redo_stmt,
                  const char * undo_stmt);

void destroy_miner_record(ObLogMinerRecord *&rec);

}
}

#endif