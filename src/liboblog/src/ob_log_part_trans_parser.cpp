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

#define USING_LOG_PREFIX OBLOG_PARSER

#include "ob_log_part_trans_parser.h"

#include "ob_log_part_trans_task.h"     // PartTransTask
#include "ob_log_binlog_record_pool.h"  // IObLogBRPool
#include "ob_log_meta_manager.h"        // IObLogMetaManager
#include "ob_log_instance.h"            // TCTX
#include "ob_log_config.h"              // TCONF
#include "ob_log_row_data_index.h"      // ObLogRowDataIndex

using namespace oceanbase::common;
using namespace oceanbase::transaction;
using namespace oceanbase::memtable;

namespace oceanbase
{
namespace liboblog
{

ObLogPartTransParser::ObLogPartTransParser() :
    inited_(false),
    br_pool_(NULL),
    meta_manager_(NULL),
    cluster_id_(OB_INVALID_CLUSTER_ID)
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
}

int ObLogPartTransParser::init(IObLogBRPool *br_pool,
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
  } else {
    cluster_id_ = cluster_id;
    inited_ = true;

    LOG_INFO("init PartTransParser succ", K(cluster_id));
  }
  return ret;
}

int ObLogPartTransParser::parse(PartTransTask &task, volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init", K(inited_));
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(! task.is_ddl_trans())) {
    LOG_ERROR("task type is not supported", K(task));
    ret = OB_NOT_SUPPORTED;
  } else if (OB_UNLIKELY(! task.is_task_info_valid())) {
    LOG_ERROR("invalid task", K(task));
    ret = OB_INVALID_ARGUMENT;
  } else {
    SortedRedoLogList &sorted_redo_list = task.get_sorted_redo_list();

    // Parse Redo logs if they exist
    if (sorted_redo_list.log_num_ > 0 && OB_FAIL(parse_ddl_redo_log_(task, stop_flag))) {
      LOG_ERROR("parse_ddl_redo_log_ fail", KR(ret), K(task));
    }
  }

  return ret;
}

int ObLogPartTransParser::parse(ObLogEntryTask &task, volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  PartTransTask *part_trans_task = NULL;

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
    // Incremental within PartTransTask
    uint64_t &row_no = part_trans_task->get_row_no();
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
      const DmlRedoLogNode &redo_node = task.get_redo_log_node();

      if (OB_UNLIKELY(! redo_node.is_valid())) {
        LOG_ERROR("redo_node is invalid", "redo_node", redo_node);
        ret = OB_INVALID_DATA;
        // Calibrate data for completeness
      } else if (OB_UNLIKELY(! redo_node.check_data_integrity())) {
        LOG_ERROR("redo data is not valid", K(redo_node));
        ret = OB_INVALID_DATA;
      } else if (OB_FAIL(parse_stmts_(tenant, redo_node.data_, redo_node.size_, task, *part_trans_task, row_no, stop_flag))) {
        LOG_ERROR("parse_stmts_ fail", KR(ret), K(tenant), "redo_node", redo_node, K(task), K(row_no));
      } else {
        LOG_DEBUG("[PARSE] log_entry_task parse succ", K(task));
      }
    }
  }

  return ret;
}

int ObLogPartTransParser::parse_ddl_redo_log_(PartTransTask &task, volatile bool &stop_flag)
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
    ObLogEntryTask redo_log_entry_task;

    // DDL data/non-PG partitioned data need to be deserialized in whole rows, not filtered
    // otherwise need to get tenant structure and perform filtering
    if (! should_not_filter_row_(task)) {
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
          // Verify that the Redo log serial number is accurate
        } else if (OB_UNLIKELY(redo_node->start_log_no_ != redo_num)) {
          LOG_ERROR("redo log_no is incorrect", "start_redo_no", redo_node->start_log_no_,
              "expected_redo_no", redo_num, KPC(redo_node));
          ret = OB_INVALID_DATA;
        }
        // Calibrate data for completeness
        else if (OB_UNLIKELY(! redo_node->check_data_integrity())) {
          LOG_ERROR("redo data is not valid", KPC(redo_node));
          ret = OB_INVALID_DATA;
        } else if (OB_FAIL(parse_stmts_(tenant, redo_node->data_, redo_node->size_, redo_log_entry_task, task, row_index, stop_flag))) {
          LOG_ERROR("parse_stmts_ fail", KR(ret), K(tenant), "redo_node", *redo_node, K(task), K(row_index));
        } else {
          redo_num += redo_node->get_log_num();
          redo_node = static_cast<DdlRedoLogNode *>(redo_node->next_);
        }
      } // while
    }
  }

  return ret;
}

int ObLogPartTransParser::parse_stmts_(ObLogTenant *tenant,
    const char *redo_data,
    const int64_t redo_data_len,
    ObLogEntryTask &redo_log_entry_task,
    PartTransTask &task,
    uint64_t &row_index,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(redo_data) || OB_UNLIKELY(redo_data_len <= 0)) {
    LOG_ERROR("invalid argument", K(redo_data), K(redo_data_len), K(task), K(redo_log_entry_task));
    ret = OB_INVALID_ARGUMENT;
  } else {
    bool is_ddl_trans = task.is_ddl_trans();
    int64_t pos = 0;

    // parse statement
    while (OB_SUCCESS == ret && pos < redo_data_len) {
      bool need_filter_row = false;
      int32_t row_size = 0;

      if (OB_FAIL(filter_row_data_(tenant, redo_data, redo_data_len, pos, task, need_filter_row, row_size, stop_flag))) {
        LOG_ERROR("filter_row_data_ fail", KR(ret), K(tenant), K(pos), K(task), K(need_filter_row), K(row_size));
      } else if (need_filter_row) {
        // filter this row, move pos to next row
        pos += row_size;
      } else {
        // parse row
        void *mutator_row_buf = NULL;
        MutatorRow *row = NULL;
        if (is_ddl_trans) {
          mutator_row_buf = task.alloc(sizeof(MutatorRow));
        } else {
          mutator_row_buf = redo_log_entry_task.alloc(sizeof(MutatorRow));
        }

        if (OB_ISNULL(row = static_cast<MutatorRow *>(mutator_row_buf))) {
          LOG_ERROR("alloc memory for MutatorRow fail", K(sizeof(MutatorRow)));
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          // FIXME: Destroy MutatorRow from regular channels and free memory
          // Currently destroyed in DmlStmtTask and DdlStmtTask, but no memory is freed
          // Since this memory is allocated by the Allocator of the PartTransTask, it is guaranteed not to leak
          if (is_ddl_trans) {
            new (row) MutatorRow(task.get_allocator());
          } else {
            new (row) MutatorRow(redo_log_entry_task.get_allocator());
          }

          // Deserialising row data
          if (OB_FAIL(row->deserialize(redo_data, redo_data_len, pos))) {
            LOG_ERROR("deserialize mutator row fail", KR(ret), KPC(row), K(redo_data_len), K(pos));
          }
          // First determine if it is a rollback savepoint by the Row
          else if (is_rollback_savepoint_stmt_(*row)) {
            if (is_ddl_trans) {
              if (OB_FAIL(handle_ddl_part_rollback_savepoint_(row_index, task, *row))) {
                LOG_ERROR("handle_ddl_part_rollback_savepoint_ failed", KR(ret), K(row_index), K(task), KPC(row));
              }
            } else {
              // Non-incrementing row_index
              if (OB_FAIL(handle_dml_part_rollback_savepoint_(row_index, task, redo_log_entry_task, *row))) {
                LOG_ERROR("handle_dml_part_rollback_savepoint_ failed", KR(ret), K(row_index), K(task),
                    K(redo_log_entry_task), KPC(row));
              }
            }

            if (is_ddl_trans) {
              row->~MutatorRow();
              task.free(row);
              row = NULL;
            }
          }
          // For DDL partitions, only parse data from the same table and filter data from other unrelated
          // tables such as index tables; prevent the generation of non-DDL type statements.
          //
          // For DML partitioning, you cannot parse only the table data, because Sequencer needs the data
          // of the unique index table for dependency analysis and to ensure data consistency.
          else if (is_ddl_trans && task.get_partition().get_table_id() != row->table_id_) {
            row->~MutatorRow();
            task.free(row);
            row = NULL;
          } else {
            // parse row data
            if (is_ddl_trans) {
              if (OB_FAIL(parse_ddl_stmts_(row_index, *row, task))) {
                LOG_ERROR("parse_ddl_stmts_ fail", KR(ret), K(row_index), K(*row), K(task));
              }
            } else if (OB_FAIL(parse_dml_stmts_(row_index, *row, redo_log_entry_task, task))) {
              LOG_ERROR("parse_dml_stmts_ fail", KR(ret), K(row_index), K(*row), K(redo_log_entry_task), K(task));
            }

            if (OB_SUCC(ret)) {
              ++row_index;
            }
          }
        }
      } // need_filter_row=false
    }
  }

  return ret;
}

// 1. savepoint is an internal transaction concept, a bitpoint set by the unrolled part of the transaction, for statement-level rollback within the transaction
// 2.
// 2. The OB rolls back a transaction statement to the set savepoint point by rolling back the savepoint, for example:
//    (begin)(1)(2)(3)(sp1)(4)(5)(rollback sp1)(6)(commit)
//    execute result of this transaction:(1)(2)(3)(6)
//
// 3. savepoint usage example:
//    (begin)(sp1)(1)(2)(sp2)(3)(rollback sp2)(commit) => (1)(2)
//    (begin)(sp1)(1)(2)(sp2)(3)(rollback sp1)(commit) => null
//    (begin)(1)(sp1)(2)(sp2)(3)(rollback sp1)(commit) => (1)
//    (begin)(1)(sp1)(2)(sp2)(3)(rollback sp2)(commit) => (1)(2)
//    (begin)(1)(sp1)(2)(sp2)(3)(rollback sp2)(4)(rollback sp1)(5) => (1)(5)
//
// 4. liboblog outputs bin record by parsing the clog log, which only records rollback statements in the clog log, not savepoint statements
// savepoint information is stored in the memory of the coordinator of the transaction, and the transaction will fail if the coordinator is down.
// rollback statements are recorded in the clog by flag and sql_no to indicate that the statement is a rollback statement and rolls back to a stmt
//
// 5. so there is no need to process the savepoint statement (which is not recorded in the clog), only the rollback savepoint statement task order traversal, dropping the sql_no is greater than the sql_no recorded in the rollback savepoint statement
//
// Only DDL partition calls are supported
int ObLogPartTransParser::handle_ddl_part_rollback_savepoint_(const uint64_t row_index,
    PartTransTask &task,
    MutatorRow &row)
{
  int ret = OB_SUCCESS;
  const int32_t flag = row.flag_;
  const int32_t sql_no = row.sql_no_;

  if (OB_UNLIKELY(! task.is_ddl_trans())) {
    LOG_ERROR("task is not ddl trans, unexcepted", K(row_index), K(task), K(row));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(1 != flag) || OB_UNLIKELY(sql_no < 0) || OB_UNLIKELY(OB_INVALID_ID == row_index)) {
    LOG_ERROR("invalid argument", K(flag), K(sql_no), K(row_index), K(row));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(task.revert_by_rollback_savepoint(row_index, sql_no))) {
    LOG_ERROR("task revert_stmt_by_rollback_savepoint failed", KR(ret), K(sql_no), K(task));
  } else {
    // succ
    LOG_DEBUG("handle rollback savepoint succ", K(row), K(task));
  }

  return ret;
}

int ObLogPartTransParser::handle_dml_part_rollback_savepoint_(const uint64_t row_index,
    PartTransTask &part_trans_task,
    ObLogEntryTask &log_entry_task,
    MutatorRow &row)
{
  int ret = OB_SUCCESS;
  const bool is_rollback = true;

  if (OB_FAIL(parse_dml_stmts_(row_index, row, log_entry_task, part_trans_task, is_rollback))) {
    LOG_ERROR("parse_dml_stmts_ fail", KR(ret), K(row_index), K(row), K(log_entry_task), K(part_trans_task),
        K(is_rollback));
  } else {
    LOG_DEBUG("handle dml rollback savepoint succ", K(row), K(row_index), K(log_entry_task), K(part_trans_task),
        K(is_rollback));
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
int ObLogPartTransParser::filter_row_data_(ObLogTenant *tenant,
    const char *redo_data,
    const int64_t redo_data_len,
    const int64_t cur_pos,
    PartTransTask &task,
    bool &need_filter_row,
    int32_t &row_size,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  // No filtering by default
  need_filter_row = false;
  row_size = 0;
  const bool is_pg = task.is_pg();
  const uint64_t tenant_id = task.get_tenant_id();
  // Temporary row data structure to avoid allocation of row data memory
  // TODO allocator
  MutatorRow row(task.get_allocator());

  if (OB_ISNULL(redo_data) || OB_UNLIKELY(redo_data_len <= 0) || OB_UNLIKELY(cur_pos < 0)) {
    LOG_ERROR("invalid argument", K(redo_data), K(task), K(redo_data_len), K(cur_pos));
    ret = OB_INVALID_ARGUMENT;
  } else if (should_not_filter_row_(task)) {
    // DDL data/non-PG partitioned data all need to be deserialized in whole rows, no filtering
    need_filter_row = false;
  } else {
    int64_t pos = cur_pos;
    uint64_t table_id = OB_INVALID_ID;
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

      if (OB_FAIL(row.deserialize_first(redo_data, redo_data_len, pos, row_size, table_id))) {
        LOG_ERROR("deserialize row_size and table_id fail", KR(ret), K(row), K(redo_data_len), K(pos),
            K(row_size), K(table_id), K(is_pg));
      } else if (OB_FAIL(part_mgr.is_exist_table_id_cache(table_id, is_exist))) {
        LOG_ERROR("part_mgr is_exist_table_id_cache fail", KR(ret), K(table_id), K(is_pg), K(is_exist));
      } else if (is_exist) {
        // Located in the whitelist, data does not need to be filtered
        need_filter_row = false;
      } else {
        // Not present, may need to filter or future table
        if (TCONF.test_mode_on) {
          static int cnt = 0;
          int64_t block_time_us = TCONF.test_mode_block_parser_filter_row_data_sec * _SEC_;
          // Only the first statement blocks
          if (block_time_us > 0 && 0 == cnt) {
            LOG_INFO("[FILTER_ROW] [TEST_MODE_ON] block to filter row",
                K(block_time_us), K(table_id), K(is_pg), K(row_size), K(cur_pos), K(need_filter_row), K(cnt));
            ++cnt;
            usleep((useconds_t)block_time_us);
          }
        }

        // Continue parsing to get table_version
        if (OB_FAIL(row.deserialize_second(redo_data, redo_data_len, pos, table_version))) {
          LOG_ERROR("deserialize table_version fail", KR(ret), K(row), K(redo_data_len), K(pos),
              K(row_size), K(table_id), K(is_pg), K(table_version));
        } else {
           // There will be no data with table_version=0 in the current row, if it occurs only an error will be reported and won't exit
          if (OB_UNLIKELY(table_version <= 0)) {
            LOG_ERROR("desrialize row data, table version is less than 0, unexcepted",
                K(table_id), K(is_pg), K(table_version), K(task), K(part_mgr_cur_schema_verison));
          }

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
      }
      LOG_DEBUG("[FILTER_ROW]", K(tenant_id), K(table_id), K(need_filter_row),
          "table_pure_id", extract_pure_id(table_id),
          K(row_size), K(table_version), K(cur_pos), K(pos));
    }
  }

  return ret;
}

bool ObLogPartTransParser::should_not_filter_row_(PartTransTask &task)
{
  bool bool_ret = false;

  bool_ret = task.is_ddl_trans() || (! task.is_pg());

  return bool_ret;
}

// Parsing DDL statements
// Construct DDL Binlog Record directly
int ObLogPartTransParser::parse_ddl_stmts_(const uint64_t row_index, MutatorRow &row, PartTransTask &task)
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
    } else if (OB_FAIL(br_pool_->alloc(false/*is_serilized*/, br, &task))) {
      LOG_ERROR("alloc binlog record from pool fail", KR(ret), K(br_pool_));
    } else if (OB_ISNULL(br)) {
      LOG_ERROR("alloc binlog record fail", K(br));
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(br->set_table_meta(ddl_table_meta))) {
      LOG_ERROR("set table meta fail", KR(ret), K(br), K(ddl_table_meta));
    } else {
      new (stmt_task) DdlStmtTask(task, row, cluster_id_);
      uint64_t exec_tennat_id = OB_INVALID_TENANT_ID;

      // Parsing DDL statement information
      bool is_valid_ddl = false;
      if (OB_FAIL(stmt_task->parse_ddl_info(br, row_index, is_valid_ddl, update_schema_version, exec_tennat_id))) {
        LOG_ERROR("parse_ddl_info fail", KR(ret), K(*stmt_task), K(br), K(row_index), K(is_valid_ddl),
            K(update_schema_version), K(exec_tennat_id));
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

// parse DML statement
//
// After constructing the DML statement, add it directly to the chain table, without parsing the column data and without constructing the Binlog Record
// 1. to save memory
// 2. the corresponding Schema must be obtained in order to parse the column data correctly
int ObLogPartTransParser::parse_dml_stmts_(const uint64_t row_index,
    MutatorRow &row,
    ObLogEntryTask &redo_log_entry_task,
    PartTransTask &task,
    const bool is_rollback)
{
  int ret = OB_SUCCESS;
  // DmlStmtTask needs to allocate memory based on LogEntryTask
  DmlStmtTask *stmt_task = static_cast<DmlStmtTask *>(redo_log_entry_task.alloc(sizeof(DmlStmtTask)));
  // Row indexes exist globally and require memory allocation based on PartTransTask
  ObLogRowDataIndex *row_data_index = static_cast<ObLogRowDataIndex *>(task.alloc(sizeof(ObLogRowDataIndex)));
  const uint64_t tenant_id = task.get_tenant_id();
  const char *participant_key_str = task.get_participant_key_str();
  const uint64_t log_id = redo_log_entry_task.get_log_id();
  const int32_t log_offset = redo_log_entry_task.get_log_offset();
  const int32_t row_sql_no = row.sql_no_;

  if (OB_ISNULL(stmt_task) || OB_ISNULL(row_data_index)) {
    LOG_ERROR("allocate memory for DmlStmtTask or ObLogRowDataIndex fail", "Dmlsize", sizeof(DmlStmtTask),
        "RowIndexSize", sizeof(ObLogRowDataIndex));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    new (row_data_index) ObLogRowDataIndex();

    if (OB_FAIL(row_data_index->init(tenant_id, participant_key_str, log_id, log_offset, row_index, is_rollback, row_sql_no))) {
      LOG_ERROR("row_data_index init fail", KR(ret), K(tenant_id), K(row_data_index),
          K(participant_key_str), K(log_id), K(log_offset), K(row_index), K(is_rollback), K(row_sql_no),
          K(task), K(redo_log_entry_task));
    } else {
      new (stmt_task) DmlStmtTask(task, redo_log_entry_task, *row_data_index, row);

      row_data_index->set_host(&task);

      if (OB_FAIL(redo_log_entry_task.add_stmt(row_index, stmt_task))) {
        LOG_ERROR("add stmt into trans task fail", KR(ret), K(task), K(row_index), "stmt_task", *stmt_task);
      } else {
        // Update the Local Schema version of PartTransTask
        task.update_local_schema_version(stmt_task->get_table_version());

        LOG_DEBUG("add_stmt succ", KPC(stmt_task), K(redo_log_entry_task));
      }
    }
  }

  if (OB_FAIL(ret)) {
    if (NULL != stmt_task) {
      stmt_task->~DmlStmtTask();
      redo_log_entry_task.free(stmt_task);
      stmt_task = NULL;
    }

    if (NULL != row_data_index) {
      row_data_index->~ObLogRowDataIndex();
      task.free(row_data_index);
      row_data_index = NULL;
    }
  }

  return ret;
}
}
}
