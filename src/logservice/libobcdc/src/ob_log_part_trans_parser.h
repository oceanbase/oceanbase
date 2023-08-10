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
 * Partitioned transaction parser that translate partition transaction into statements
 */

#ifndef OCEANBASE_LIBOBCDC_OB_LOG_PART_TRANS_PARSER_H_
#define OCEANBASE_LIBOBCDC_OB_LOG_PART_TRANS_PARSER_H_

#include "lib/utility/ob_macro_utils.h"
#include "ob_log_all_ddl_operation_schema_info.h" // ObLogAllDdlOperationSchemaInfo
#include "ob_log_tenant.h"              // ObLogTenant
#include <stdint.h>

namespace oceanbase
{
namespace libobcdc
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
  int init(
      IObLogBRPool *br_pool,
      IObLogMetaManager *meta_manager,
      const int64_t cluster_id);
  void destroy();

private:
  int check_row_need_rollback_(
      const PartTransTask &part_trans_task,
      const MutatorRow &row,
      bool &need_rollback);
  int parse_ddl_redo_log_(PartTransTask &task, volatile bool &stop_flag);
  int parse_stmts_(
      ObLogTenant *tenant,
      const RedoLogMetaNode &redo_log_node,
      ObLogEntryTask &redo_log_entry_task,
      PartTransTask &task,
      uint64_t &row_index,
      volatile bool &stop_flag);
  // try parse mutator_header to get mutator type(support if ob_version >= 320)
  // and move forward cur_pos to skip header if header is supported
  //
  // @param   [in]    buf            redo data to deserialize
  // @param   [in]    buf_len        redo data length
  // @param   [in]    cur_pos        pos before deserialize and will move_forward pos if header is valid
  // @param   [out]   mutator_type   mutator_type, default MUTATOR_ROW
  // @retval  OB_SUCCESS             get mutator type success
  // @retval  other_err_code         get mutator type fail
  int parse_mutator_header_(
      const char *buf,
      const int64_t buf_len,
      int64_t &cur_pos,
      memtable::MutatorType &mutator_type,
      common::ObTabletID &tablet_id);
  // deserialize table lock and move forward cur_pos
  int filter_mutator_table_lock_(const char *buf, const int64_t buf_len, int64_t &cur_pos);
  int get_table_info_of_tablet_(
      const ObLogTenant *tenant,
      const PartTransTask &part_trans_task,
      const ObTabletID &tablet_id,
      ObCDCTableInfo &table_info);
  // 1. Non-PG partitions do not filter row data for now. TODO: Turn on later
  // 2. For PG partitions, filter out non-whitelisted and common index data based on TableIDCache filtering
  // 3. Do not filter DDL partition data to avoid transaction data and DDL data dependency
  int filter_row_data_(
      ObLogTenant *tenant,
      const char *redo_data,
      const int64_t redo_data_len,
      const int64_t cur_pos,
      const ObCDCTableInfo &table_info,
      PartTransTask &task,
      bool &need_filter,
      int32_t &row_size,
      volatile bool &stop_flag);
  // DDL data/non-PG partitioned data all need to be deserialized in whole rows, no filtering
  bool should_not_filter_row_(PartTransTask &task);

  int parse_ddl_stmts_(
      const uint64_t row_index,
      const ObLogAllDdlOperationSchemaInfo &all_ddl_operation_table_schema,
      MutatorRow &row,
      PartTransTask &task,
      volatile bool &stop_flag);
  int parse_ddl_lob_aux_stmts_(
      const uint64_t table_id,
      const uint64_t row_index,
      MutatorRow &row,
      PartTransTask &part_trans_task);
  int parse_dml_stmts_(
      const uint64_t table_id,
      const uint64_t row_index,
      MutatorRow &row,
      ObLogEntryTask &redo_log_entry_task,
      PartTransTask &part_trans_task);
  const transaction::ObTxSEQ &get_row_seq_(PartTransTask &task, MutatorRow &row) const;
  int alloc_mutator_row_(
      PartTransTask &part_trans_task,
      ObLogEntryTask &redo_log_entry_task,
      MutatorRow *&row);
  void free_mutator_row_(
      PartTransTask &part_trans_task,
      ObLogEntryTask &redo_log_entry_task,
      MutatorRow *&row);
  int parse_mutator_row_(
      ObLogTenant *tenant,
      const ObTabletID &tablet_id,
      const char *redo_data,
      const int64_t redo_data_len,
      int64_t &pos,
      PartTransTask &part_trans_task,
      ObLogEntryTask &redo_log_entry_task,
      MutatorRow *&row,
      ObCDCTableInfo &table_info,
      bool &is_ignored);
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

} // namespace libobcdc
} // namespace oceanbase
#endif
