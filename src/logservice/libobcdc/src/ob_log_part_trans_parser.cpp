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

#define USING_LOG_PREFIX OBLOG_PARSER

#include "ob_log_part_trans_parser.h"

#include "lib/allocator/page_arena.h"   // PageArena
#include "ob_log_part_trans_task.h"     // PartTransTask
#include "ob_log_binlog_record_pool.h"  // IObLogBRPool
#include "ob_log_meta_manager.h"        // IObLogMetaManager
#include "ob_log_instance.h"            // TCTX
#include "ob_log_config.h"              // TCONF
#include "ob_cdc_lob_aux_table_parse.h" // parse_aux_lob_meta_table_row
#include "src/storage/ddl/ob_ddl_redo_log_row_iterator.h"

using namespace oceanbase::common;
using namespace oceanbase::share::schema;
using namespace oceanbase::transaction;
using namespace oceanbase::memtable;
using namespace oceanbase::storage;

namespace oceanbase
{
namespace libobcdc
{

ObLogPartTransParser::ObLogPartTransParser() :
    inited_(false),
    br_pool_(NULL),
    meta_manager_(NULL),
    all_ddl_operation_table_schema_info_(),
    cluster_id_(OB_INVALID_CLUSTER_ID),
    total_log_size_(0),
    remaining_log_size_(0),
    last_stat_time_(OB_INVALID_TIMESTAMP)
{}

ObLogPartTransParser::~ObLogPartTransParser()
{
  destroy();
}

void ObLogPartTransParser::destroy()
{
  inited_ = false;
  cluster_id_ = OB_INVALID_CLUSTER_ID;
  br_pool_ = NULL;
  meta_manager_ = NULL;
  all_ddl_operation_table_schema_info_.reset();
}

int ObLogPartTransParser::init(
    IObLogBRPool *br_pool,
    IObLogMetaManager *meta_manager,
    const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("parser has been initialized", K(inited_));
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(br_pool_ = br_pool)
      || OB_ISNULL(meta_manager_ = meta_manager)
      || OB_UNLIKELY(OB_INVALID_CLUSTER_ID == cluster_id)) {
    LOG_ERROR("invalid argument", K(br_pool), K(meta_manager), K(cluster_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(all_ddl_operation_table_schema_info_.init())) {
    LOG_ERROR("init all ddl operation table schema info failed", KR(ret));
  } else {
    cluster_id_ = cluster_id;
    inited_ = true;
    last_stat_time_ = get_timestamp();
    LOG_INFO("init PartTransParser succ", K(cluster_id));
  }
  return ret;
}

void ObLogPartTransParser::print_stat_info()
{
  int64_t current_timestamp = get_timestamp();
  int64_t local_last_stat_time = last_stat_time_;
  int64_t delta_time = current_timestamp - local_last_stat_time;
  double delta_second = static_cast<double>(delta_time) / static_cast<double>(_SEC_);
  int64_t total_traffic = 0;
  int64_t remaining_traffic = 0;
  int64_t filtered_out_traffic = 0;
  if (0 < delta_second) {
    total_traffic = static_cast<int64_t>(static_cast<double>(total_log_size_) / delta_second);
    remaining_traffic = static_cast<int64_t>(static_cast<double>(remaining_log_size_) / delta_second);
    filtered_out_traffic = static_cast<int64_t>(static_cast<double>(total_log_size_ - remaining_log_size_) / delta_second);
  }

  // Update last statistic value
  last_stat_time_ = current_timestamp;
  total_log_size_ = 0;
  remaining_log_size_ = 0;

  _LOG_INFO("[STAT] [PARSER] TOTAL_TRAFFIC=%s/sec, REMAINING_TRAFFIC=%s/sec, FILTERED_OUT_TRAFFIC=%s/sec",
      SIZE_TO_STR(total_traffic), SIZE_TO_STR(remaining_traffic), SIZE_TO_STR(filtered_out_traffic));
}

int ObLogPartTransParser::parse(PartTransTask &task, const bool is_build_baseline, volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not init", KR(ret), K(inited_));
  } else if (OB_UNLIKELY(! (task.is_ddl_trans() || task.is_ls_op_trans()))) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("task type is not supported", KR(ret), K(task));
  } else if (OB_UNLIKELY(! task.is_task_info_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid task", KR(ret), K(task));
  } else {
    const SortedRedoLogList &sorted_redo_list = task.get_sorted_redo_list();
    // Parse Redo logs if they exist
    if (sorted_redo_list.log_num_ > 0 && OB_FAIL(parse_ddl_redo_log_(task, is_build_baseline, stop_flag))) {
      LOG_ERROR("parse_ddl_redo_log_ fail", KR(ret), K(task), K(is_build_baseline));
    }
  }

  return ret;
}

int ObLogPartTransParser::parse(ObLogEntryTask &task, volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  PartTransTask *part_trans_task = NULL;
  const bool is_build_baseline = false;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init", K(inited_));
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(! task.is_valid())) {
    LOG_ERROR("invalid task", K(task));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(part_trans_task = static_cast<PartTransTask *>(task.get_host()))) {
    LOG_ERROR("part_trans_task is NULL", K(part_trans_task));
    ret = OB_ERR_UNEXPECTED;
  } else {
    ObLogTenant *tenant = NULL;
    ObLogTenantGuard guard;
    // Incremental within LogEntryTask
    uint64_t row_index = 0;
    const uint64_t tenant_id = part_trans_task->get_tenant_id();

    // DDL data/non-PG partitioned data need to be deserialized in whole rows, not filtered
    // otherwise need to get tenant structure and perform filtering
    if (OB_SUCC(ret)) {
      if (! should_not_filter_row_(*part_trans_task)) {
        if (OB_FAIL(TCTX.get_tenant_guard(tenant_id, guard))) {
          // Tenants must exist here
          LOG_ERROR("get_tenant_guard fail", KR(ret), K(tenant_id), KPC(part_trans_task));
        } else if (OB_ISNULL(tenant = guard.get_tenant())) {
          LOG_ERROR("tenant is null", K(tenant_id), K(tenant), K(task));
          ret = OB_ERR_UNEXPECTED;
        } else {
          // succ
        }
      }
    }

    if (OB_SUCC(ret)) {
      const DmlRedoLogNode *redo_node = task.get_redo_log_node();

      if (OB_ISNULL(redo_node)) {
        LOG_ERROR("redo_node is NULL");
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_UNLIKELY(! redo_node->is_valid())) {
        LOG_ERROR("redo_node is invalid", "redo_node", redo_node);
        ret = OB_INVALID_DATA;
        // Calibrate data for completeness
      } else if (OB_UNLIKELY(! redo_node->check_data_integrity())) {
        ret = OB_INVALID_DATA;
        LOG_ERROR("redo data is not valid", KR(ret), KPC(redo_node));
      } else if (redo_node->is_direct_load_inc_log()) {
        if (OB_FAIL(parse_direct_load_inc_stmts_(tenant, *redo_node, task, *part_trans_task, row_index, stop_flag))) {
          LOG_ERROR("parse_ddl_stmts_ fail", KR(ret), K(tenant), KPC(redo_node), K(task), K(row_index));
        } else {
          ATOMIC_AAF(&total_log_size_, redo_node->get_data_len());
          LOG_DEBUG("[PARSE] LogEntryTask parse ddl redo log succ", K(task));
        }
      } else {
        if (OB_FAIL(parse_stmts_(tenant, *redo_node, is_build_baseline, task, *part_trans_task,
            row_index, stop_flag))) {
          LOG_ERROR("parse_stmts_ fail", KR(ret), K(tenant), KPC(redo_node), K(task), K(row_index));
        } else {
          ATOMIC_AAF(&total_log_size_, redo_node->get_data_len());
          LOG_DEBUG("[PARSE] LogEntryTask parse redo log succ", K(task));
        }
      }
    }
  }

  return ret;
}

int ObLogPartTransParser::parse_ddl_redo_log_(PartTransTask &task, const bool is_build_baseline, volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  int64_t redo_num = 0;
  SortedRedoLogList &sorted_redo_list = task.get_sorted_redo_list();
  DdlRedoLogNode *redo_node = static_cast<DdlRedoLogNode *>(sorted_redo_list.head_);
  const uint64_t tenant_id = task.get_tenant_id();

  if (OB_UNLIKELY(! sorted_redo_list.is_valid())) {
    LOG_ERROR("redo log list is invalid", K(sorted_redo_list), K(task));
    ret = OB_ERR_UNEXPECTED;
  } else {
    // Used to assign row_index to DML and DDL statements, partitioned transaction statements are ordered, starting from 0
    uint64_t row_index = 0;
    ObLogTenant *tenant = NULL;
    ObLogTenantGuard guard;
    // just declear here
    ObLogEntryTask invalid_redo_log_entry_task(task);

    // DDL data/non-PG partitioned data need to be deserialized in whole rows, not filtered
    // otherwise need to get tenant structure and perform filtering
    if (! should_not_filter_row_(task) && !is_build_baseline) {
      if (OB_FAIL(TCTX.get_tenant_guard(tenant_id, guard))) {
        // tenant must exist here
        LOG_ERROR("get_tenant_guard fail", KR(ret), K(tenant_id));
      } else if (OB_ISNULL(tenant = guard.get_tenant())) {
        LOG_ERROR("tenant is null", K(tenant_id), K(tenant), K(task));
        ret = OB_ERR_UNEXPECTED;
      } else {
        // succ
      }
    }

    if (OB_SUCC(ret)) {
      while (OB_SUCCESS == ret && NULL != redo_node) {
        LOG_DEBUG("parse redo log", "redo_node", *redo_node);

        if (OB_UNLIKELY(! redo_node->is_valid())) {
          LOG_ERROR("redo_node is invalid", "redo_node", *redo_node, "redo_index", redo_num);
          ret = OB_INVALID_DATA;
        }
        // Calibrate data for completeness
        else if (OB_UNLIKELY(! redo_node->check_data_integrity())) {
          LOG_ERROR("redo data is not valid", KPC(redo_node));
          ret = OB_INVALID_DATA;
        } else if (OB_FAIL(parse_stmts_(tenant, *redo_node, is_build_baseline,
            invalid_redo_log_entry_task, task, row_index, stop_flag))) {
          LOG_ERROR("parse_stmts_ fail", KR(ret), K(tenant), "redo_node", *redo_node,
              K(is_build_baseline), K(task), K(row_index));
        } else {
          redo_num += redo_node->get_log_num();
          redo_node = static_cast<DdlRedoLogNode *>(redo_node->get_next());
        }
      } // while
    }
  }

  return ret;
}

int ObLogPartTransParser::parse_stmts_(
    ObLogTenant *tenant,
    const RedoLogMetaNode &redo_log_node,
    const bool is_build_baseline,
    ObLogEntryTask &redo_log_entry_task,
    PartTransTask &task,
    uint64_t &row_index,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  const char *redo_data = redo_log_node.get_data();
  const int64_t redo_data_len = redo_log_node.get_data_len();

  if ((OB_ISNULL(tenant) && !is_build_baseline) || OB_ISNULL(redo_data) || OB_UNLIKELY(redo_data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), KPC(tenant), K(redo_data), K(redo_data_len), K(task), K(redo_log_entry_task));
  } else {
    const bool is_ddl_trans = task.is_ddl_trans();
    int64_t pos = 0;

    // parse statement
    while (OB_SUCC(ret) && pos < redo_data_len) {
      bool need_filter_row = false;
      int32_t row_size = 0;
      int64_t begin_pos = pos;
      MutatorType mutator_type = MutatorType::MUTATOR_ROW; // default type to mutator_row
      common::ObTabletID tablet_id;

      if (OB_FAIL(parse_mutator_header_(redo_data, redo_data_len, pos, mutator_type, tablet_id))) {
        LOG_ERROR("failed to get mutator_type", KR(ret), K(pos), K(task), K(mutator_type));
      } else if (MutatorType::MUTATOR_TABLE_LOCK == mutator_type) {
        // handler MutatorType::MUTATOR_TABLE_LOCK
        // skip table_lock data
        if (OB_FAIL(filter_mutator_table_lock_(redo_data, redo_data_len, pos))) {
          LOG_ERROR("failed to filter mutator table lock data", KR(ret), K(redo_data_len), K(pos));
        }
      } else if (MutatorType::MUTATOR_ROW == mutator_type) {
        bool is_ignored = false;
        MemtableMutatorRow *row = NULL;
        ObCDCTableInfo table_info;

        if (OB_FAIL(parse_memtable_mutator_row_(
            tenant,
            tablet_id,
            redo_data,
            redo_data_len,
            is_build_baseline,
            pos,
            task,
            redo_log_entry_task,
            row,
            table_info,
            is_ignored))) {
          LOG_ERROR("parse_memtable_mutator_row_ failed", KR(ret),
              "tls_id", task.get_tls_id(),
              "trans_id", task.get_trans_id(),
              K(tablet_id), K(redo_log_entry_task), K(is_build_baseline), K(row_index));
        } else if (! is_ignored) {
          // parse row data
          if (is_ddl_trans) {
            if (!is_build_baseline && is_all_ddl_operation_lob_aux_tablet(task.get_ls_id(), tablet_id)) {
              LOG_INFO("is_all_ddl_operation_lob_aux_tablet", "tls_id", task.get_tls_id(),
                  "trans_id", task.get_trans_id(), K(tablet_id));

              if (OB_FAIL(parse_ddl_lob_aux_stmts_(table_info.get_table_id(), row_index, *row, task))) {
                LOG_ERROR("parse_ddl_lob_aux_stmts_ failed", KR(ret), "tls_id", task.get_tls_id(),
                  "trans_id", task.get_trans_id(), K(tablet_id));
              }
              // data in non ddl table already filtered while parse_mutator_row_
            } else if (OB_FAIL(parse_ddl_stmts_(
                row_index,
                all_ddl_operation_table_schema_info_,
                is_build_baseline,
                *row,
                task,
                stop_flag))) {
              LOG_ERROR("parse_ddl_stmts_ fail", KR(ret), K(row_index), K(tablet_id), K(*row), K(task));
            }
          } else if (OB_FAIL(parse_dml_stmts_(
              table_info.get_table_id(),
              row_index,
              *row,
              redo_log_entry_task,
              task))) {
            LOG_ERROR("parse_dml_stmts_ fail", KR(ret), K(row_index), K(*row), K(redo_log_entry_task), K(task));
          } else {
            ATOMIC_AAF(&remaining_log_size_, pos - begin_pos);
          }

          if (OB_SUCC(ret)) {
            ++row_index;
          }
        } // need_ignore_row=false
      } else if (MutatorType::MUTATOR_ROW_EXT_INFO == mutator_type) {
        if (OB_FAIL(handle_mutator_ext_info_log_(
            tenant,
            tablet_id,
            redo_data,
            redo_data_len,
            pos,
            task,
            redo_log_entry_task))) {
          LOG_ERROR("handle_mutator_ext_info_log_ failed", KR(ret),
              "tls_id", task.get_tls_id(),
              "trans_id", task.get_trans_id(),
              K(tablet_id), K(redo_log_entry_task), K(row_index));
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_ERROR("not support mutator type", KR(ret), K(mutator_type));
      }
    }
  }

  return ret;
}

int ObLogPartTransParser::parse_direct_load_inc_stmts_(
    ObLogTenant *tenant,
    const RedoLogMetaNode &redo_log_node,
    ObLogEntryTask &redo_log_entry_task,
    PartTransTask &task,
    uint64_t &row_index,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  const char *ddl_redo_data = redo_log_node.get_data();
  const int64_t ddl_redo_data_len = redo_log_node.get_data_len();

  if (OB_ISNULL(tenant) || OB_ISNULL(ddl_redo_data) || OB_UNLIKELY(ddl_redo_data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), KPC(tenant), K(ddl_redo_data), K(ddl_redo_data_len), K(task), K(redo_log_entry_task));
  } else {
    ObDDLRedoLog ddl_redo_log;
    int64_t pos = 0;
    if (OB_FAIL(ddl_redo_log.deserialize(ddl_redo_data, ddl_redo_data_len, pos))) {
      LOG_ERROR("ObDDLRedoLog deserialize failed", KR(ret), K(ddl_redo_log), K(ddl_redo_data_len), K(pos));
    } else {
      const common::ObTabletID tablet_id = ddl_redo_log.get_redo_info().table_key_.get_tablet_id();
      ObArenaAllocator allocator;
      ObDDLRedoLogRowIterator ddl_redo_log_row_iterator(allocator, tenant->get_tenant_id());
      if (OB_FAIL(ddl_redo_log_row_iterator.init(ddl_redo_log.get_redo_info().data_buffer_))){
        LOG_ERROR("direct_load_log_row_iterator init failed", KR(ret), K(ddl_redo_log));
      } else {
        while (OB_SUCCESS == ret) {
          bool is_ignored = false;
          MacroBlockMutatorRow *row = nullptr;
          const blocksstable::ObDatumRow *datum_row = nullptr;
          const ObStoreRowkey *row_key = nullptr;
          transaction::ObTxSEQ seq;
          blocksstable::ObDmlRowFlag dml_flag;
          ObCDCTableInfo table_info;

          if (OB_FAIL(ddl_redo_log_row_iterator.get_next_row(datum_row, row_key, seq, dml_flag))) {
            if (OB_ITER_END != ret) {
              LOG_ERROR("get next macroblock row failed", KR(ret));
            }
          } else if (OB_ISNULL(datum_row) || OB_ISNULL(row_key)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("datum_row or row_key is nullptr", K(datum_row), K(row_key));
          } else if (OB_FAIL(alloc_macroblock_mutator_row_(task, redo_log_entry_task, datum_row, row_key, seq, dml_flag, row))) {
            LOG_ERROR("alloc macroblock mutator row failed", KR(ret), K(task), K(redo_log_entry_task), KPC(datum_row),
                KPC(row_key), K(seq), K(dml_flag), K(row));
          } else if (OB_FAIL(parse_macroblock_mutator_row_(tenant, tablet_id, task, redo_log_entry_task, row, table_info, is_ignored))) {
            LOG_ERROR("parse macroblock mutator row failed", K(tenant), K(tablet_id), K(task), K(redo_log_entry_task),
                KPC(row), K(table_info), K(is_ignored), K(datum_row), K(row_key), K(seq), K(dml_flag), K(ddl_redo_log));
          } else if (!is_ignored && OB_FAIL(parse_dml_stmts_(table_info.get_table_id(), row_index, *row,
              redo_log_entry_task, task))) {
            LOG_ERROR("parse_dml_stmts failed", KR(ret), K(table_info), K(row_index), KPC(row), K(redo_log_entry_task), K(task),
                K(datum_row), K(row_key), K(seq), K(dml_flag), K(ddl_redo_log));
          } else {
            ++row_index;
          }
        } // for each row

        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
    }
  }

  return ret;
}

int ObLogPartTransParser::parse_mutator_header_(
    const char *buf,
    const int64_t buf_len,
    int64_t &cur_pos,
    memtable::MutatorType &mutator_type,
    common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  int64_t pos = cur_pos;
  ObMutatorRowHeader row_header;
  row_header.reset();

  if (OB_ISNULL(buf) || OB_UNLIKELY(pos < 0 || pos > buf_len)) {
    LOG_ERROR("invalid arguments", K(pos), K(buf_len));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(row_header.deserialize(buf, buf_len, pos))) {
    if (OB_NOT_SUPPORTED == ret) {
      LOG_DEBUG("NOT SUPPORT MUTATOR ROW HEADER, may server version below 320, will ignore header and treat data as MUTATOR_ROW", KR(ret));
      mutator_type = memtable::MutatorType::MUTATOR_ROW;
      ret = OB_SUCCESS;
    } else {
      LOG_ERROR("failed to deserialize row header", KR(ret), K(buf_len), K(pos));
    }
  } else {
    mutator_type = row_header.mutator_type_;
    tablet_id = row_header.tablet_id_;
    cur_pos = pos;
  }

  return ret;
}

int ObLogPartTransParser::filter_mutator_table_lock_(const char *buf, const int64_t buf_len, int64_t &cur_pos)
{
  int ret = OB_SUCCESS;
  int64_t pos = cur_pos;
  ObMutatorTableLock table_lock;
  table_lock.reset();
  if (OB_ISNULL(buf) || OB_UNLIKELY(pos < 0 || pos > buf_len)) {
    LOG_ERROR("invalid arguments", K(buf_len), K(pos));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(table_lock.deserialize(buf, buf_len, pos))) {
    LOG_ERROR("failed to deserialize table lock", KR(ret), K(buf_len), K(pos));
  } else {
    cur_pos = pos;
  }
  return ret;
}

int ObLogPartTransParser::parse_memtable_mutator_row_(
    ObLogTenant *tenant,
    const ObTabletID &tablet_id,
    const char *redo_data,
    const int64_t redo_data_len,
    const bool is_build_baseline,
    int64_t &pos,
    PartTransTask &part_trans_task,
    ObLogEntryTask &redo_log_entry_task,
    MemtableMutatorRow *&row,
    ObCDCTableInfo &table_info,
    bool &is_ignored)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(alloc_memtable_mutator_row_(part_trans_task, redo_log_entry_task, row))) {
    LOG_ERROR("alloc_memtable_mutator_row_ failed", KR(ret), K(part_trans_task), K(redo_log_entry_task));
  } else if (OB_FAIL(row->deserialize(redo_data, redo_data_len, pos))) {
    LOG_ERROR("deserialize mutator row fail", KR(ret), KPC(row), K(redo_data_len), K(pos));
  } else if (OB_FAIL(check_row_need_ignore_(is_build_baseline, tenant, part_trans_task, redo_log_entry_task,
      *row, tablet_id, table_info, is_ignored))) {
    LOG_ERROR("check_row_need_ignore_ failed", KR(ret), K(is_build_baseline), K(part_trans_task),
        K(redo_log_entry_task), KPC(row));
  }

  if (OB_FAIL(ret) || is_ignored) {
    free_memtable_mutator_row_(part_trans_task, redo_log_entry_task, row);
  } else if (OB_ISNULL(row)) {
    ret = OB_INVALID_DATA;
  }

  return ret;
}

int ObLogPartTransParser::alloc_memtable_mutator_row_(
    PartTransTask &part_trans_task,
    ObLogEntryTask &redo_log_entry_task,
    MemtableMutatorRow *&row)
{
  int ret = OB_SUCCESS;
  void *mutator_row_buf = NULL;
  row = NULL;
  const bool is_ddl_trans = part_trans_task.is_ddl_trans();

  if (is_ddl_trans) {
    mutator_row_buf = part_trans_task.alloc(sizeof(MemtableMutatorRow));
  } else {
    mutator_row_buf = redo_log_entry_task.alloc(sizeof(MemtableMutatorRow));
  }

  if (OB_ISNULL(row = static_cast<MemtableMutatorRow *>(mutator_row_buf))) {
    LOG_ERROR("alloc memory for MemtableMutatorRow fail", K(sizeof(MemtableMutatorRow)));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    // FIXME: Destroy MemtableMutatorRow from regular channels and free memory
    // Currently destroyed in DmlStmtTask and DdlStmtTask, but no memory is freed
    // Since this memory is allocated by the Allocator of the PartTransTask, it is guaranteed not to leak
    if (is_ddl_trans) {
      new (row) MemtableMutatorRow(part_trans_task.get_allocator());
    } else {
      new (row) MemtableMutatorRow(redo_log_entry_task.get_allocator());
    }
  }

  return ret;
}

void ObLogPartTransParser::free_memtable_mutator_row_(
    PartTransTask &part_trans_task,
    ObLogEntryTask &redo_log_entry_task,
    MemtableMutatorRow *&row)
{
  if (OB_NOT_NULL(row)) {
    row->~MemtableMutatorRow();
    if (part_trans_task.is_ddl_trans()) {
      part_trans_task.free(row);
    } else {
      redo_log_entry_task.free(row);
    }
    row = NULL;
  }
}

int ObLogPartTransParser::get_table_info_of_tablet_(
    const ObLogTenant *tenant,
    const PartTransTask &part_trans_task,
    const ObTabletID &tablet_id,
    ObCDCTableInfo &table_info)
{
  int ret = OB_SUCCESS;
  const bool is_sys_ls_trans = part_trans_task.get_tls_id().is_sys_log_stream();

  if (is_sys_ls_trans) {
    table_info.reset(tablet_id.id(), ObTableType::SYSTEM_TABLE);
  } else {
    // For DDL trans etc we will not get tenant structure, refrence should_not_filter_row_
    if (OB_ISNULL(tenant)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("tenant is NULL", KR(ret));
    } else if (OB_FAIL(tenant->get_table_info_of_tablet(tablet_id, table_info))) {
      LOG_ERROR("get_table_info_of_tablet for dml_tx failed", KR(ret), K(tablet_id),
          "tenant_ls_id", part_trans_task.get_tls_id(),
          KPC(tenant));
    }
  }

  return ret;
}

int ObLogPartTransParser::check_row_need_rollback_(
    const PartTransTask &part_trans_task,
    const MutatorRow &row,
    bool &need_rollback)
{
  int ret = OB_SUCCESS;
  need_rollback = false;
  const RollbackList &rollback_list = part_trans_task.get_rollback_list();
  const auto &row_seq_no = row.get_seq_no();
  const RollbackNode *rollback_node = rollback_list.head_;

  while (OB_SUCC(ret) && OB_NOT_NULL(rollback_node) && ! need_rollback) {
    need_rollback = rollback_node->should_rollback_stmt(row_seq_no);
    rollback_node = rollback_node->get_next();
  }

  return ret;
}

// To support filtering of table data within PG, the filtering algorithm is as follows:
// 1. PG-DML transaction parses out the row_size and table_id first, avoiding deserializing the entire row and causing performance overhead
// 2. Query the TableIDCache based on table_id, if it exists, then the data is required
// 3. When it does not exist, the table_id may be blacklisted data or a future table that cannot be filtered
// 4. parse out row_size, table_id, rowkey, table_version
// 5. cur_schema_version based on table_version and PartMgr processing:
//   (1) When table_version > cur_schema_version, it means it is a future table, then you need to wait for
//     PartMgr processing to push up the schema version, until it is greater than or equal to tabel_version,
//     then query TableIDCache again, if it exists, it is needed, otherwise it is filtered
//   (2) When table_version <= cur_schema_version, it means it is no longer a future table, then filter it out
int ObLogPartTransParser::filter_row_data_(
    ObLogTenant *tenant,
    const char *redo_data,
    const int64_t redo_data_len,
    const int64_t cur_pos,
    const ObCDCTableInfo &table_info,
    PartTransTask &task,
    bool &need_filter_row,
    int32_t &row_size,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  // No filtering by default
  need_filter_row = false;
  row_size = 0;
  const uint64_t tenant_id = task.get_tenant_id();
  // Temporary row data structure to avoid allocation of row data memory
  // TODO allocator
  MemtableMutatorRow row(task.get_allocator());

  if (OB_ISNULL(redo_data) || OB_UNLIKELY(redo_data_len <= 0) || OB_UNLIKELY(cur_pos < 0) || OB_UNLIKELY(! table_info.is_valid())) {
    LOG_ERROR("invalid argument", K(redo_data), K(task), K(redo_data_len), K(cur_pos), K(table_info));
    ret = OB_INVALID_ARGUMENT;
  } else if (should_not_filter_row_(task)) {
    // DDL data/non-PG partitioned data all need to be deserialized in whole rows, no filtering
    need_filter_row = false;
  } else {
    const uint64_t table_id = table_info.get_table_id();
    int64_t pos = cur_pos;
    int64_t table_version = 0;
    bool is_exist = false;

    // Filtering requires that the tenant must be valid
    if (OB_ISNULL(tenant)) {
      LOG_ERROR("tenant is null", K(tenant_id), K(tenant));
      ret = OB_ERR_UNEXPECTED;
    } else {
      IObLogPartMgr &part_mgr = tenant->get_part_mgr();
      // Note: In the TableIDCache based on table_id, you should get the current schema version in advance,
      // because the schema version will keep changing, and getting the schema version first will have the following bad case:
      // Assume data: table_id=1001, table_version=100 cur_schema_version=90
      // 1. future table, based on table_id query TableIDCache does not exist
      // 2. get schema version, at this point cur_schema_version=100
      // 3. Parse to get table_version, because table_version <= cur_schema_version will result in false filtering out
      const int64_t part_mgr_cur_schema_verison = tenant->get_schema_version();

      if (OB_FAIL(row.deserialize_first(redo_data, redo_data_len, pos, row_size))) {
        LOG_ERROR("deserialize row_size and table_id fail", KR(ret), K(row), K(redo_data_len), K(pos),
            K(row_size), K(table_id));
      } else if (table_info.is_index_table()) {
        // filter redo in index table
        need_filter_row = true;
      } else if (OB_FAIL(part_mgr.is_exist_table_id_cache(table_id, is_exist))) {
        LOG_ERROR("part_mgr is_exist_table_id_cache fail", KR(ret), K(table_id), K(is_exist));
      } else if (is_exist) {
        // Located in the whitelist, data does not need to be filtered
        need_filter_row = false;
      } else {
        /* TODO modify : table_version is not valid, don't need to check it
        // Not present, may need to filter or future table
        if (TCONF.test_mode_on) {
          static int cnt = 0;
          int64_t block_time_us = TCONF.test_mode_block_parser_filter_row_data_sec * _SEC_;
          // Only the first statement blocks
          if (block_time_us > 0 && 0 == cnt) {
            LOG_INFO("[FILTER_ROW] [TEST_MODE_ON] block to filter row",
                K(block_time_us), K(table_id), K(row_size), K(cur_pos), K(need_filter_row), K(cnt));
            ++cnt;
            ob_usleep((useconds_t)block_time_us);
          }
        }


        // Continue parsing to get table_version
        if (OB_FAIL(row.deserialize_second(redo_data, redo_data_len, pos, table_version))) {
          LOG_ERROR("deserialize table_version fail", KR(ret), K(row), K(redo_data_len), K(pos),
              K(row_size), K(table_id), K(table_version));
        } else {
           // There will be no data with table_version=0 in the current row, if it occurs only an error will be reported and won't exit
          if (OB_UNLIKELY(table_version <= 0)) {
            // TODO error code ?
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("desrialize row data, table version is less than 0, unexcepted", KR(ret),
                K(table_id), K(table_version), K(part_mgr_cur_schema_verison), K(row), K(task));
          }
              KR(ret), K(tenant_id), K(table_id), K(table_version), K(part_mgr_cur_schema_verison));

          if (table_version <= part_mgr_cur_schema_verison) {
            // Blacklisted data needs to be filtered out
            need_filter_row = true;
          } else {
            RETRY_FUNC(stop_flag, part_mgr, handle_future_table, table_id, table_version, DATA_OP_TIMEOUT, is_exist);

            if (OB_SUCC(ret)) {
              if (! is_exist) {
                need_filter_row = true;
              } else {
                need_filter_row = false;
              }
            }
          }
        }
        */
      }
      LOG_DEBUG("[FILTER_ROW]", K(tenant_id), K(table_id), K(need_filter_row),
          K(table_id), K(row_size), K(table_version), K(cur_pos), K(pos));
    }
  }

  return ret;
}

bool ObLogPartTransParser::should_not_filter_row_(PartTransTask &task)
{
  bool bool_ret = false;

  bool_ret = task.is_ls_op_trans();

  return bool_ret;
}

// Parsing DDL statements
// Construct DDL Binlog Record directly
int ObLogPartTransParser::parse_ddl_stmts_(
    const uint64_t row_index,
    const ObLogAllDdlOperationSchemaInfo &all_ddl_operation_table_schema,
    const bool is_build_baseline,
    MutatorRow &row,
    PartTransTask &task,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_) || OB_UNLIKELY(OB_INVALID_ID == row_index)) {
    LOG_ERROR("invalid argument", K(inited_), K(row_index));
    ret = OB_INVALID_ARGUMENT;
  } else {
    DdlStmtTask *stmt_task = static_cast<DdlStmtTask *>(task.alloc(sizeof(DdlStmtTask)));
    ObLogBR *br = NULL;
    int64_t update_schema_version = 0;
    ITableMeta *ddl_table_meta = meta_manager_->get_ddl_table_meta();

    if (OB_ISNULL(stmt_task)) {
      LOG_ERROR("allocate memory for DdlStmtTask fail", "size", sizeof(DdlStmtTask));
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(br_pool_->alloc(br, &task))) {
      LOG_ERROR("alloc binlog record from pool fail", KR(ret), K(br_pool_));
    } else if (OB_ISNULL(br)) {
      LOG_ERROR("alloc binlog record fail", K(br));
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(br->set_table_meta(ddl_table_meta))) {
      LOG_ERROR("set table meta fail", KR(ret), K(br), K(ddl_table_meta));
    } else {
      new (stmt_task) DdlStmtTask(task, row);
      uint64_t exec_tennat_id = OB_INVALID_TENANT_ID;

      // Parsing DDL statement information
      bool is_valid_ddl = false;
      if (OB_FAIL(stmt_task->parse_ddl_info(br, row_index, all_ddl_operation_table_schema, is_build_baseline,
              is_valid_ddl, update_schema_version, exec_tennat_id, stop_flag))) {
        LOG_ERROR("parse_ddl_info fail", KR(ret), K(*stmt_task), K(br), K(row_index), K(is_build_baseline),
            K(is_valid_ddl), K(update_schema_version), K(exec_tennat_id));
      } else if (! is_valid_ddl) {
        // Discard invalid DDL statement tasks
        stmt_task->~DdlStmtTask();
        task.free(stmt_task);
        stmt_task = NULL;

        // recycle Binlog Record
        br_pool_->free(br);
        br  = NULL;
      } else if (OB_FAIL(task.add_ddl_stmt(row_index, stmt_task))) {
        LOG_ERROR("add stmt into trans task fail", KR(ret), K(task), K(row_index),
            "stmt_task", *stmt_task);
      } else {
        // succ
      }

      // Update Schema version with or without DDL statements
      if (OB_SUCC(ret)) {
        task.update_local_schema_version(update_schema_version);

        if (OB_UNLIKELY(OB_INVALID_TENANT_ID == exec_tennat_id)) {
          LOG_ERROR("exec_tennat_id is invalid", K(exec_tennat_id), K(task));
          ret = OB_INVALID_ARGUMENT;
        } else {
          task.set_exec_tenant_id(exec_tennat_id);
        }
      }
    }

    if (OB_SUCCESS != ret) {
      if (NULL != stmt_task) {
        stmt_task->~DdlStmtTask();
        task.free(stmt_task);
        stmt_task = NULL;
      }

      if (NULL != br) {
        br_pool_->free(br);
        br = NULL;
      }
    }
  }

  return ret;
}

int ObLogPartTransParser::parse_ddl_lob_aux_stmts_(
    const uint64_t table_id,
    const uint64_t row_index,
    MutatorRow &row,
    PartTransTask &part_trans_task)
{
  int ret = OB_SUCCESS;
  ObLogEntryTask invalid_log_entry_task(part_trans_task);
  // For DDL Lob Aux meta: DmlStmtTask needs to allocate memory based on PartTransTask
  DmlStmtTask *stmt_task = static_cast<DmlStmtTask *>(part_trans_task.alloc(sizeof(DmlStmtTask)));
  ObCDCGlobalInfo &globl_info = TCTX.global_info_;
  const ObCDCLobAuxTableSchemaInfo &lob_aux_table_schema_info = globl_info.get_lob_aux_table_schema_info();

  if (OB_ISNULL(stmt_task)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocate memory for DmlStmtTask fail", KR(ret), "Dmlsize", sizeof(DmlStmtTask));
  } else {
    new (stmt_task) DmlStmtTask(part_trans_task, invalid_log_entry_task, row);
    ColValueList *rowkey_cols = nullptr;
    ColValueList *new_cols = nullptr;
    ColValueList *old_cols = nullptr;

    if (OB_ISNULL(stmt_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("stmt_task is nullptr", KR(ret), K(stmt_task));
    } else if (FALSE_IT(stmt_task->set_table_id(table_id))) {
    } else if (OB_FAIL(part_trans_task.add_ddl_lob_aux_stmt(row_index, stmt_task))) {
      LOG_ERROR("add_ddl_lob_aux_stmt into trans task fail", KR(ret), K(part_trans_task),
          K(row_index), "stmt_task", *stmt_task);
    } else if (OB_FAIL(stmt_task->parse_aux_meta_table_cols(lob_aux_table_schema_info))) {
      LOG_ERROR("parse_aux_meta_table_cols failed", KR(ret));
    } else if (OB_FAIL(stmt_task->get_cols(&rowkey_cols, &new_cols, &old_cols, nullptr/*new_lob_ctx_cols*/))) {
      LOG_ERROR("get_cols fail", KR(ret), K(stmt_task));
    } else {
      const int64_t commit_version = part_trans_task.get_trans_commit_version();
      const uint64_t tenant_id = part_trans_task.get_tenant_id();
      const transaction::ObTransID &trans_id = part_trans_task.get_trans_id();
      ObLobId lob_id;
      const transaction::ObTxSEQ &row_seq_no = stmt_task->get_row_seq_no();
      const char *lob_data = nullptr;
      int64_t lob_data_len = 0;
      ObCDCLobAuxMetaStorager &lob_aux_meta_storager = TCTX.lob_aux_meta_storager_;

      if (OB_FAIL(ObCDCLobAuxMetaParse::parse_aux_lob_meta_table_row(*new_cols, lob_id, lob_data, lob_data_len))) {
        LOG_ERROR("parse_aux_lob_meta_table_row failed", KR(ret));
      } else {
        LobAuxMetaKey lob_aux_meta_key(commit_version, tenant_id, trans_id, table_id, lob_id, row_seq_no);

        if (OB_FAIL(lob_aux_meta_storager.put(lob_aux_meta_key, "insert", lob_data, lob_data_len))) {
          LOG_ERROR("lob_aux_meta_storager put failed", KR(ret), K(lob_aux_meta_key));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    if (NULL != stmt_task) {
      stmt_task->~DmlStmtTask();
      part_trans_task.free(stmt_task);
      stmt_task = NULL;
    }
  }

  return ret;
}

// parse DML statement
//
// After constructing the DML statement, add it directly to the chain table, without parsing the column data and without constructing the Binlog Record
// 1. to save memory
// 2. the corresponding Schema must be obtained in order to parse the column data correctly
int ObLogPartTransParser::parse_dml_stmts_(
    const uint64_t table_id,
    const uint64_t row_index,
    MutatorRow &row,
    ObLogEntryTask &redo_log_entry_task,
    PartTransTask &task)
{
  int ret = OB_SUCCESS;
  // DmlStmtTask needs to allocate memory based on LogEntryTask
  DmlStmtTask *stmt_task = static_cast<DmlStmtTask *>(redo_log_entry_task.alloc(sizeof(DmlStmtTask)));

  if (OB_ISNULL(stmt_task)) {
    LOG_ERROR("allocate memory for DmlStmtTask fail", "Dmlsize", sizeof(DmlStmtTask));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    new (stmt_task) DmlStmtTask(task, redo_log_entry_task, row);
    stmt_task->set_table_id(table_id);

    if (OB_ISNULL(stmt_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("stmt_task is nullptr", KR(ret), K(stmt_task));
    } else if (OB_FAIL(redo_log_entry_task.add_stmt(row_index, stmt_task))) {
      LOG_ERROR("add stmt into trans task fail", KR(ret), K(task), K(row_index), "stmt_task", *stmt_task);
    } else {
      // Update the Local Schema version of PartTransTask
      task.update_local_schema_version(stmt_task->get_table_version());

      LOG_DEBUG("add_stmt succ", K(row_index), KPC(stmt_task));
    }
  }

  if (OB_FAIL(ret)) {
    if (NULL != stmt_task) {
      stmt_task->~DmlStmtTask();
      redo_log_entry_task.free(stmt_task);
      stmt_task = NULL;
    }
  }

  return ret;
}

const transaction::ObTxSEQ ObLogPartTransParser::get_row_seq_(PartTransTask &task, MutatorRow &row) const
{
  //return task.is_cluster_version_before_320() ? row.sql_no_ : row.seq_no_;
  return row.get_seq_no();
}

int ObLogPartTransParser::alloc_macroblock_mutator_row_(
    PartTransTask &part_trans_task,
    ObLogEntryTask &redo_log_entry_task,
    const blocksstable::ObDatumRow *datum_row,
    const common::ObStoreRowkey *row_key,
    transaction::ObTxSEQ &seq_no,
    blocksstable::ObDmlRowFlag &dml_flag,
    MacroBlockMutatorRow *&row)
{
  int ret = OB_SUCCESS;
  row = NULL;

  if (OB_ISNULL(row = static_cast<MacroBlockMutatorRow *>(redo_log_entry_task.alloc(sizeof(MacroBlockMutatorRow))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory for MacroBlockMutatorRow fail", KR(ret), K(sizeof(MacroBlockMutatorRow)));
  } else if (FALSE_IT(new (row) MacroBlockMutatorRow(redo_log_entry_task.get_allocator(), dml_flag, seq_no))) {
  } else if (OB_FAIL(row->init(redo_log_entry_task.get_allocator(), *datum_row, *row_key))) {
    LOG_ERROR("macroblock row init fail", KR(ret), KPC(datum_row), KPC(row_key));
  } else {
    // succ
  }

  return ret;
}

void ObLogPartTransParser::free_macroblock_mutator_row_(
    PartTransTask &part_trans_task,
    ObLogEntryTask &redo_log_entry_task,
    MacroBlockMutatorRow *&row)
{
  if (OB_NOT_NULL(row)) {
    row->~MacroBlockMutatorRow();
    redo_log_entry_task.free(row);
    row = NULL;
  }
}

int ObLogPartTransParser::parse_macroblock_mutator_row_(
    ObLogTenant *tenant,
    const ObTabletID &tablet_id,
    PartTransTask &part_trans_task,
    ObLogEntryTask &redo_log_entry_task,
    MacroBlockMutatorRow *&row,
    ObCDCTableInfo &table_info,
    bool &is_ignored)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_row_need_ignore_(false /* is_build_baseline */, tenant, part_trans_task,
      redo_log_entry_task, *row, tablet_id, table_info, is_ignored))) {
    LOG_ERROR("check_row_need_ignore_ failed", KR(ret), K(tablet_id), K(part_trans_task),
        K(redo_log_entry_task), KPC(row));
  }

  if (OB_FAIL(ret) || is_ignored) {
    free_macroblock_mutator_row_(part_trans_task, redo_log_entry_task, row);
  } else if (OB_ISNULL(row)) {
    ret = OB_INVALID_DATA;
  } else {
    row->set_table_id(table_info.get_table_id());
  }

  return ret;
}

int ObLogPartTransParser::check_row_need_ignore_(
    const bool is_build_baseline,
    ObLogTenant *tenant,
    PartTransTask &part_trans_task,
    ObLogEntryTask &redo_log_entry_task,
    MutatorRow &row,
    const ObTabletID &tablet_id,
    ObCDCTableInfo &table_info,
    bool &is_ignored)
{
  int ret = OB_SUCCESS;
  IObLogPartMgr &part_mgr = tenant->get_part_mgr();
  is_ignored = false;
  bool need_rollback = false;
  bool need_filter = false;
  bool is_in_table_id_cache = false;
  const char *filter_reason = NULL;

  if (OB_FAIL(check_row_need_rollback_(part_trans_task, row, need_rollback))) {
    LOG_ERROR("check_row_need_rollback_ failed", KR(ret), K(part_trans_task), K(redo_log_entry_task), K(row));
  } else if (need_rollback) {
    LOG_DEBUG("rollback row by RollbackToSavepoint",
        "tls_id", part_trans_task.get_tls_id(),
        "trans_id", part_trans_task.get_trans_id(),
        "row_seq_no", row.get_seq_no());
  } else if (OB_FAIL(get_table_info_of_tablet_(tenant, part_trans_task, tablet_id, table_info))) {
    LOG_ERROR("get_table_info_of_tablet_ failed", KR(ret), K(tablet_id), K(part_trans_task), K(redo_log_entry_task), K(row));
  } else if (table_info.is_index_table()) {
    need_filter = true;
    filter_reason = "INDEX_TABLE";
  // DDL Table and LOB_AUX TABLE for DDL Table should not ignore
  } else if (part_trans_task.is_ddl_trans()
      && ! (is_ddl_tablet(part_trans_task.get_ls_id(), tablet_id)
          || is_all_ddl_operation_lob_aux_tablet(part_trans_task.get_ls_id(), tablet_id))) {
    need_filter = true;
    filter_reason = "NON_DDL_RELATED_TABLE";
  } else if (part_trans_task.is_ddl_trans() && is_build_baseline
      && is_all_ddl_operation_lob_aux_tablet(part_trans_task.get_ls_id(), tablet_id)) {
    need_filter = true;
    filter_reason = "DDL_OPERATION_LOB_AUX_TABLE_IN_BUILD_BASELINE";
  } else if (part_trans_task.is_ddl_trans()) {
    // do nothing, ddl trans should not be filtered
  } else {
    IObLogPartMgr &part_mgr = tenant->get_part_mgr();
    if (OB_FAIL(part_mgr.is_exist_table_id_cache(table_info.get_table_id(), is_in_table_id_cache))) {
      LOG_ERROR("check is_exist_table_id_cache failed", KR(ret),
          "tls_id", part_trans_task.get_tls_id(),
          "trans_id", part_trans_task.get_trans_id(),
          K(tablet_id), K(table_info));
    } else {
      need_filter = ! is_in_table_id_cache;
      filter_reason = "NOT_EXIST_IN_TB_ID_CACHE";
    }
  }

  if (need_filter) {
    LOG_DEBUG("filter mutator row",
        "tls_id", part_trans_task.get_tls_id(),
        "trans_id", part_trans_task.get_trans_id(),
        K(tablet_id),
        K(table_info),
        K(filter_reason),
        K(is_build_baseline));
  }

  if (OB_SUCC(ret)) {
    is_ignored = need_rollback || need_filter;
  }

  return ret;
}

int ObLogPartTransParser::parse_ext_info_log_mutator_row_(
    ObLogTenant *tenant,
    const char *redo_data,
    const int64_t redo_data_len,
    int64_t &pos,
    PartTransTask &part_trans_task,
    ObLogEntryTask &redo_log_entry_task,
    MemtableMutatorRow *&row,
    bool &is_ignored)
{
  int ret = OB_SUCCESS;
  is_ignored = false;
  row = nullptr;
  bool need_rollback = false;

  if (OB_FAIL(alloc_memtable_mutator_row_(part_trans_task, redo_log_entry_task, row))) {
    LOG_ERROR("alloc_memtable_mutator_row_ failed", KR(ret), K(part_trans_task), K(redo_log_entry_task));
  } else if (OB_FAIL(row->deserialize(redo_data, redo_data_len, pos))) {
    LOG_ERROR("deserialize mutator row fail", KR(ret), KPC(row), K(redo_data_len), K(pos));
  } else if (OB_FAIL(check_row_need_rollback_(part_trans_task, *row, need_rollback))) {
    LOG_ERROR("check_row_need_rollback_ failed", KR(ret), K(part_trans_task), K(redo_log_entry_task), KPC(row));
  } else if (need_rollback) {
    LOG_DEBUG("rollback row by RollbackToSavepoint",
        "tls_id", part_trans_task.get_tls_id(),
        "trans_id", part_trans_task.get_trans_id(),
        "row_seq_no", row->seq_no_);
  } else if (part_trans_task.is_ddl_trans()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part tans task is ddl not expected", KR(ret), K(part_trans_task));
  }

  if (OB_SUCC(ret)) {
    is_ignored = need_rollback;
  }

  if (OB_FAIL(ret) || is_ignored) {
    free_memtable_mutator_row_(part_trans_task, redo_log_entry_task, row);
    row = nullptr;
  } else if (OB_ISNULL(row)) {
    ret = OB_INVALID_DATA;
  }

  return ret;
}

int ObLogPartTransParser::handle_mutator_ext_info_log_(
    ObLogTenant *tenant,
    const ObTabletID &tablet_id,
    const char *redo_data,
    const int64_t redo_data_len,
    int64_t &pos,
    PartTransTask &part_trans_task,
    ObLogEntryTask &redo_log_entry_task)
{
  int ret = OB_SUCCESS;
  bool is_ignored = false;
  MemtableMutatorRow *row = nullptr;
  if (OB_FAIL(parse_ext_info_log_mutator_row_(
      tenant,
      redo_data,
      redo_data_len,
      pos,
      part_trans_task,
      redo_log_entry_task,
      row,
      is_ignored))) {
    LOG_ERROR("parse_mutator_row_ failed", KR(ret),
        "tls_id", part_trans_task.get_tls_id(),
        "trans_id", part_trans_task.get_trans_id(),
        K(tablet_id));
  } else if (! is_ignored) {
    const int64_t commit_version = part_trans_task.get_trans_commit_version();
    const uint64_t tenant_id = tenant->get_tenant_id();
    const transaction::ObTransID &trans_id = part_trans_task.get_trans_id();
    const uint64_t table_id = 0;
    ObLobId lob_id; // empty
    transaction::ObTxSEQ row_seq_no = row->get_seq_no();
    ObString ext_info_log;
    ObCDCLobAuxMetaStorager &lob_aux_meta_storager = TCTX.lob_aux_meta_storager_;
    LobAuxMetaKey lob_aux_meta_key(commit_version, tenant_id, trans_id, table_id, lob_id, row_seq_no);
    if (OB_FAIL(row->parse_ext_info_log(ext_info_log))) {
      LOG_WARN("parse_ext_info_log fail", KR(ret));
    } else if (OB_FAIL(lob_aux_meta_storager.put(lob_aux_meta_key, "ext_info_log", ext_info_log.ptr(), ext_info_log.length()))) {
      LOG_ERROR("lob_aux_meta_storager put failed", KR(ret), K(lob_aux_meta_key));
    } else {
      LOG_DEBUG("put ext info log success", K(lob_aux_meta_key), "log_length", ext_info_log.length());
    }
  } else {
    LOG_INFO("ext info log is ignored", K(tablet_id),
        "tenant_id", tenant->get_tenant_id(),
        "tls_id", part_trans_task.get_tls_id(),
        "trans_id", part_trans_task.get_trans_id());
  }
  if (OB_NOT_NULL(row)) {
    free_memtable_mutator_row_(part_trans_task, redo_log_entry_task, row);
  }
  return ret;
}

}
}
