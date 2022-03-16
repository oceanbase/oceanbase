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

#ifndef OCEANBASE_LIBOBLOG_OB_LOG_PART_TRANS_PARSER_H_
#define OCEANBASE_LIBOBLOG_OB_LOG_PART_TRANS_PARSER_H_

#include "lib/utility/ob_macro_utils.h"
#include "ob_log_tenant.h"              // ObLogTenant
#include <stdint.h>

namespace oceanbase
{
namespace liboblog
{
class PartTransTask;

class IObLogPartTransParser
{
public:
  virtual ~IObLogPartTransParser() {}

  enum { DATA_OP_TIMEOUT = 200 * 1000 };

public:
  virtual int parse(PartTransTask &task, volatile bool &stop_flag) = 0;

  virtual int parse(ObLogEntryTask &task, volatile bool &stop_flag) = 0;
};


////////////////////////////////////// ObLogPartTransParser //////////////////////////////////////
// thread safe

class MutatorRow;
class IObLogBRPool;
class IObLogMetaManager;
class ObLogPartTransParser : public IObLogPartTransParser
{
public:
  ObLogPartTransParser();
  virtual ~ObLogPartTransParser();

public:
  virtual int parse(PartTransTask &task, volatile bool &stop_flag);
  virtual int parse(ObLogEntryTask &task, volatile bool &stop_flag);

public:
  int init(IObLogBRPool *br_pool,
      IObLogMetaManager *meta_manager,
      const int64_t cluster_id);
  void destroy();

private:
  bool is_rollback_savepoint_stmt_(MutatorRow &row) const
  {
    return row.is_rollback_stmt();
  }
  int handle_ddl_part_rollback_savepoint_(const uint64_t row_index,
      PartTransTask &part_trans_task,
      MutatorRow &row);
  int handle_dml_part_rollback_savepoint_(const uint64_t row_index,
      PartTransTask &part_trans_task,
      ObLogEntryTask &log_entry_task,
      MutatorRow &row);
  int parse_ddl_redo_log_(PartTransTask &task, volatile bool &stop_flag);
  int parse_stmts_(ObLogTenant *tenant,
      const char *redo_data,
      const int64_t redo_data_len,
      ObLogEntryTask &redo_log_entry_task,
      PartTransTask &task,
      uint64_t &row_index,
      volatile bool &stop_flag);
  // 1. Non-PG partitions do not filter row data for now. TODO: Turn on later
  // 2. For PG partitions, filter out non-whitelisted and common index data based on TableIDCache filtering
  // 3. Do not filter DDL partition data to avoid transaction data and DDL data dependency
  int filter_row_data_(ObLogTenant *tenant,
      const char *redo_data,
      const int64_t redo_data_len,
      const int64_t cur_pos,
      PartTransTask &task,
      bool &need_filter,
      int32_t &row_size,
      volatile bool &stop_flag);
  // DDL data/non-PG partitioned data all need to be deserialized in whole rows, no filtering
  bool should_not_filter_row_(PartTransTask &task);
  int parse_ddl_stmts_(const uint64_t row_index, MutatorRow &row, PartTransTask &task);
  int parse_dml_stmts_(const uint64_t row_index,
      MutatorRow &row,
      ObLogEntryTask &redo_log_entry_task,
      PartTransTask &part_trans_task,
      const bool is_rollback = false);

private:
  bool              inited_;
  IObLogBRPool      *br_pool_;
  IObLogMetaManager *meta_manager_;

  // The cluster ID of this cluster
  // Set as the unique ID of the DDL
  int64_t           cluster_id_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogPartTransParser);
};

} /* liboblog */
} /* oceanbase */
#endif
