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

#define USING_LOG_PREFIX STORAGE
#include "ob_build_index_task.h"
#include "lib/utility/ob_tracepoint.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/ob_ddl_checksum.h"
#include "share/ob_ddl_error_message_table_operator.h"
#include "share/ob_get_compat_mode.h"
#include "share/ob_ddl_task_executor.h"
#include "share/schema/ob_tenant_schema_service.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "storage/compaction/ob_column_checksum_calculator.h"
#include "storage/ddl/ob_ddl_redo_log_writer.h"
#include "storage/ddl/ob_complement_data_task.h"
#include "storage/ob_i_table.h"
#include "observer/ob_server_struct.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "storage/ob_sstable_struct.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx/ob_trans_service.h"

using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;
using namespace oceanbase::compaction;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::observer;
using namespace oceanbase::omt;
using namespace oceanbase::palf;

ObUniqueIndexChecker::ObUniqueIndexChecker()
  : is_inited_(false), tenant_id_(OB_INVALID_TENANT_ID), ls_id_(), tablet_id_(),
    index_schema_(NULL), data_table_schema_(NULL), execution_id_(-1), snapshot_version_(0), task_id_(0),
    is_scan_index_(false)
{
}

int ObUniqueIndexChecker::init(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const bool is_scan_index,
    const ObTableSchema *data_table_schema,
    const ObTableSchema *index_schema,
    const int64_t task_id,
    const int64_t execution_id,
    const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObUniqueIndexChecker has already been inited", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid()
      || NULL == data_table_schema
      || NULL == index_schema
      || task_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(ls_id), K(tablet_id), KPC(data_table_schema), KPC(index_schema), K(task_id));
  } else {
    is_inited_ = true;
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    tablet_id_ = tablet_id;
    data_table_schema_ = data_table_schema;
    index_schema_ = index_schema;
    task_id_ = task_id;
    execution_id_ = execution_id;
    snapshot_version_ = snapshot_version;
    is_scan_index_ = is_scan_index;
  }
  return ret;
}

int ObUniqueIndexChecker::calc_column_checksum(
    const common::ObIArray<bool> &need_reshape,
    const ObColDescIArray &cols_desc,
    const ObIArray<int32_t> &output_projector,
    ObLocalScan &iterator,
    common::ObIArray<int64_t> &column_checksum,
    int64_t &row_count)
{
  int ret = OB_SUCCESS;
  const int64_t column_cnt = output_projector.count();
  row_count = 0;
  column_checksum.reuse();
  if (OB_UNLIKELY(column_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(column_cnt));
  } else if (OB_FAIL(column_checksum.reserve(column_cnt))) {
    STORAGE_LOG(WARN, "fail to reserve column", K(ret), K(column_cnt));
  } else {
    const ObDatumRow *row = NULL;
    const ObDatumRow *unused_row = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt; ++i) {
      if (OB_FAIL(column_checksum.push_back(0))) {
        STORAGE_LOG(WARN, "fail to push back column checksum", K(ret));
      }
    }
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iterator.get_next_row(row, unused_row))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          STORAGE_LOG(WARN, "fail to get next row", K(ret));
        }
      } else if (OB_ISNULL(row)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "store row must not be NULL", K(ret), KP(row));
      } else if (column_cnt != row->get_column_count()) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "column cnt not as expected", K(ret), K(column_cnt), "row_val_column_cnt",
            row->count_);
      } else {
        ++row_count;
        for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt; ++i) {
          column_checksum.at(i) += row->storage_datums_[i].checksum(0);
        }
        dag_yield();
      }
    }
  }
  return ret;
}

int ObUniqueIndexChecker::scan_table_with_column_checksum(
    const ObScanTableParam &param,
    ObIArray<int64_t> &column_checksum,
    int64_t &row_count)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObLocalScan, local_scan) {
    if (OB_UNLIKELY(!param.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid arguments", K(ret), K(param));
    } else {
      transaction::ObTransService *trans_service = nullptr;
      ObTabletTableIterator iterator;
      ObQueryFlag query_flag(ObQueryFlag::Forward,
          true, /*is daily merge scan*/
          true, /*is read multiple macro block*/
          false, /*sys task scan, read one macro block in single io*/
          false, /*is full row scan?*/
          false,
          false);
      query_flag.skip_read_lob_ = 1;
      ObDatumRange range;
      bool allow_not_ready = false;
      ObArray<bool> need_reshape;
      ObLSHandle ls_handle;
      range.set_whole_range();
      if (OB_ISNULL(trans_service = MTL(transaction::ObTransService*))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("trans_service is null", K(ret));
      } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
        LOG_WARN("fail to get log stream", K(ret), K(ls_id_));
      } else if (OB_UNLIKELY(nullptr == ls_handle.get_ls())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, ls must not be nullptr", K(ret));
      } else if (OB_FAIL(ls_handle.get_ls()->get_tablet_svr()->get_read_tables(tablet_id_,
                                                                               param.snapshot_version_,
                                                                               iterator, allow_not_ready))) {
        if (OB_REPLICA_NOT_READABLE == ret) {
          ret = OB_EAGAIN;
        } else {
          LOG_WARN("snapshot version has been discarded", K(ret));
        }
      } else if (OB_FAIL(local_scan.init(*param.col_ids_, *param.org_col_ids_, *param.output_projector_,
              *param.data_table_schema_, param.snapshot_version_, trans_service, *param.index_schema_, true/*output org cols only*/))) {
        LOG_WARN("init local scan failed", K(ret));
      } else if (OB_FAIL(local_scan.table_scan(*param.data_table_schema_, ls_id_, tablet_id_, iterator, query_flag, range, nullptr))) {
        LOG_WARN("fail to table scan", K(ret));
      } else {
        const ObColDescIArray &out_cols = *param.org_col_ids_;
        for (int64_t i = 0; OB_SUCC(ret) && i < out_cols.count(); i++) {
          const int64_t col_id = out_cols.at(i).col_id_;
          const ObColumnSchemaV2 *col = nullptr;
          if (OB_ISNULL(col = param.data_table_schema_->get_column_schema(col_id))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to get column schema", K(ret), K(param), K(col_id));
          } else {
            const bool col_need_reshape = !param.is_scan_index_ && col->is_virtual_generated_column()
              && col->get_meta_type().is_fixed_len_char_type();
            if (OB_FAIL(need_reshape.push_back(col_need_reshape))) {
              LOG_WARN("failed to push back is virtual col", K(ret));
            }
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(calc_column_checksum(need_reshape, *param.col_ids_, *param.output_projector_, local_scan, column_checksum, row_count))) {
          LOG_WARN("fail to calc column checksum", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObUniqueIndexChecker::generate_index_output_param(
    const ObTableSchema &data_table_schema,
    const ObTableSchema &index_schema,
    ObArray<ObColDesc> &col_ids,
    ObArray<ObColDesc> &org_col_ids,
    ObArray<int32_t> &output_projector)
{
  int ret = OB_SUCCESS;
  col_ids.reuse();
  output_projector.reuse();
  if (OB_UNLIKELY(!data_table_schema.is_valid() || !index_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(data_table_schema), K(index_schema));
  } else {
    // add data table rowkey
    const ObRowkeyInfo &rowkey_info = data_table_schema.get_rowkey_info();
    const ObRowkeyColumn *rowkey_column = NULL;
    ObColDesc col_desc;
    for (int32_t i = 0; OB_SUCC(ret) && i < rowkey_info.get_size(); ++i) {
      if (OB_ISNULL(rowkey_column = rowkey_info.get_column(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("The rowkey column is NULL", K(ret), K(i), K(rowkey_info));
      } else {
        col_desc.col_id_ = rowkey_column->column_id_;
        col_desc.col_type_ = rowkey_column->type_;
        col_desc.col_order_ = rowkey_column->order_;
        if (OB_FAIL(col_ids.push_back(col_desc))) {
          STORAGE_LOG(WARN, "fail to push back column desc", K(ret));
        }
      }
    }

    // add index table other columns
    if (OB_SUCC(ret)) {
      if (OB_FAIL(get_index_columns_without_virtual_generated_and_shadow_columns(data_table_schema, index_schema, org_col_ids))) {
        LOG_WARN("get index columns failed", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < org_col_ids.count(); ++i) {
        const ObColDesc &index_col_desc = org_col_ids.at(i);
        int64_t j = 0;
        for (j = 0; OB_SUCC(ret) && j < col_ids.count(); ++j) {
          if (index_col_desc.col_id_ == col_ids.at(j).col_id_) {
            break;
          }
        }
        if (j == col_ids.count()) {
          if (OB_FAIL(col_ids.push_back(index_col_desc))) {
            STORAGE_LOG(WARN, "fail to push back index col desc", K(ret));
          }
        }
      }
    }

    // generate output projector
    for (int64_t i = 0; OB_SUCC(ret) && i < org_col_ids.count(); ++i) {
      int64_t j = 0;
      for (j = 0; OB_SUCC(ret) && j < col_ids.count(); ++j) {
        if (col_ids.at(j).col_id_ == org_col_ids.at(i).col_id_) {
          break;
        }
      }
      if (j == col_ids.count()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "error unexpected, output col does not exist in index table columns", K(ret));
      } else if (OB_FAIL(output_projector.push_back(static_cast<int32_t>(j)))) {
        STORAGE_LOG(WARN, "fail to push back output projector", K(ret));
      }
    }

    STORAGE_LOG(INFO, "output index projector", K(output_projector), K(col_ids), K(org_col_ids));
  }

  return ret;
}

int ObUniqueIndexChecker::get_index_columns_without_virtual_generated_and_shadow_columns(
    const ObTableSchema &data_table_schema,
    const ObTableSchema &index_table_schema,
    ObIArray<ObColDesc> &col_ids)
{
  int ret = OB_SUCCESS;
  ObArray<ObColDesc> index_table_columns;
  col_ids.reset();
  if (OB_FAIL(index_table_schema.get_column_ids(index_table_columns))) {
    STORAGE_LOG(WARN, "fail to get column ids", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < index_table_columns.count(); ++i) {
    const ObColumnSchemaV2 *column_schema = nullptr;
    bool is_output = true;
    if (!is_shadow_column(index_table_columns.at(i).col_id_)) {
      if (OB_ISNULL(column_schema = data_table_schema.get_column_schema(index_table_columns.at(i).col_id_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, column schema must not nullptr", K(ret));
      } else {
        is_output = !column_schema->is_virtual_generated_column();
      }
    } else {
      is_output = false;
    }

    if (OB_SUCC(ret) && is_output) {
      if (OB_FAIL(col_ids.push_back(index_table_columns.at(i)))) {
        LOG_WARN("push back origin col ids", K(ret));
      }
    }
  }
  return ret;
}

int ObUniqueIndexChecker::scan_main_table_with_column_checksum(
    const ObTableSchema &data_table_schema,
    const ObTableSchema &index_schema,
    const int64_t snapshot_version,
    ObIArray<int64_t> &column_checksum,
    int64_t &row_count)
{
  int ret = OB_SUCCESS;
  ObArray<share::schema::ObColDesc> col_ids;
  ObArray<share::schema::ObColDesc> org_col_ids;
  ObArray<int32_t> output_projector;
  if (OB_UNLIKELY(!data_table_schema.is_valid() || !index_schema.is_valid() || snapshot_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(data_table_schema), K(index_schema), K(snapshot_version));
  } else if (OB_FAIL(generate_index_output_param(data_table_schema, index_schema,
      col_ids, org_col_ids, output_projector))) {
    STORAGE_LOG(WARN, "fail to generate index output param", K(ret));
  } else {
    ObScanTableParam param;
    param.data_table_schema_ = &data_table_schema;
    param.index_schema_ = &index_schema;
    param.snapshot_version_ = snapshot_version;
    param.col_ids_ = &col_ids;
    param.org_col_ids_ = &org_col_ids;
    param.output_projector_ = &output_projector;
    param.is_scan_index_ = false;
    STORAGE_LOG(INFO, "scan main table column checksum", K(col_ids), K(org_col_ids));
    if (OB_FAIL(scan_table_with_column_checksum(param, column_checksum, row_count))) {
      STORAGE_LOG(WARN, "fail to scan table with column checksum", K(ret));
    }
    LOG_INFO("scan main table column checksum", K(org_col_ids), K(column_checksum));
  }
  return ret;
}

int ObUniqueIndexChecker::scan_index_table_with_column_checksum(
    const ObTableSchema &data_table_schema,
    const ObTableSchema &index_schema,
    const int64_t snapshot_version,
    ObIArray<int64_t> &column_checksum,
    int64_t &row_count)
{
  int ret = OB_SUCCESS;
  UNUSED(data_table_schema);
  ObArray<ObColDesc> column_ids;
  ObArray<int64_t> tmp_column_checksum;
  if (OB_UNLIKELY(!index_schema.is_valid() || snapshot_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(index_schema), K(snapshot_version));
  } else if (OB_FAIL(index_schema.get_column_ids(column_ids))) {
    STORAGE_LOG(WARN, "fail to get column ids", K(ret), "index_id", index_schema.get_table_id());
  } else {
    ObArray<int32_t> output_projector;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
      if (OB_FAIL(output_projector.push_back(static_cast<int32_t>(i)))) {
        STORAGE_LOG(WARN, "fail to push back output projector", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      ObScanTableParam param;
      param.data_table_schema_ = &index_schema;
      param.index_schema_ = &index_schema;
      param.snapshot_version_ = snapshot_version;
      param.col_ids_ = &column_ids;
      param.org_col_ids_ = &column_ids;
      param.output_projector_ = &output_projector;
      param.is_scan_index_ = true;
      STORAGE_LOG(INFO, "scan index table column checksum", K(column_ids));
      if (OB_FAIL(scan_table_with_column_checksum(param, tmp_column_checksum, row_count))) {
        STORAGE_LOG(WARN, "fail to scan table with column checksum", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
          const ObColumnSchemaV2 *column_schema = nullptr;
          if (!is_shadow_column(column_ids.at(i).col_id_)) {
            if (OB_ISNULL(column_schema = data_table_schema.get_column_schema(column_ids.at(i).col_id_))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("error unexpected, column schema must not be nullptr", K(ret));
            } else if (!column_schema->is_virtual_generated_column()) {
              if (OB_FAIL(column_checksum.push_back(tmp_column_checksum.at(i)))) {
                LOG_WARN("push back column id failed", K(ret));
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObUniqueIndexChecker::check_global_index(ObIDag *dag)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dag)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), KP(dag));
  } else {
    ObArray<int64_t> column_checksum;
    int64_t row_count = 0;
    if (OB_SUCC(ret) && !dag->has_set_stop()) {
      if (!is_scan_index_) {
        if (OB_FAIL(scan_main_table_with_column_checksum(*data_table_schema_, *index_schema_,
            snapshot_version_, column_checksum, row_count))) {
          STORAGE_LOG(WARN, "fail to scan main table with column checksum", K(ret));
        }
      } else {
        if (OB_FAIL(scan_index_table_with_column_checksum(*data_table_schema_, *index_schema_,
            snapshot_version_, column_checksum, row_count))) {
          STORAGE_LOG(WARN, "fail to scan index table with column checksum", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && !dag->has_set_stop()) {
      if (OB_FAIL(report_column_checksum(column_checksum, is_scan_index_ ? index_schema_->get_table_id() : data_table_schema_->get_table_id()))) {
        STORAGE_LOG(WARN, "fail to report column checksum", K(ret));
      }
    }
  }
  return ret;
}

int ObUniqueIndexChecker::report_column_checksum(
    const common::ObIArray<int64_t> &column_checksum,
    const int64_t report_table_id)
{
  int ret = OB_SUCCESS;
  ObArray<ObColDesc> tmp_column_ids;
  ObArray<ObColDesc> column_ids;
  if (OB_FAIL(index_schema_->get_column_ids(tmp_column_ids))) {
    STORAGE_LOG(WARN, "fail to get columns ids", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_column_ids.count(); ++i) {
      if (!is_shadow_column(tmp_column_ids.at(i).col_id_)) {
        const ObColumnSchemaV2 *column_schema = nullptr;
        if (OB_ISNULL(column_schema = data_table_schema_->get_column_schema(tmp_column_ids.at(i).col_id_))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("error unexpected, get column schema failed", K(ret));
        } else if (!column_schema->is_virtual_generated_column()) {
          if (OB_FAIL(column_ids.push_back(tmp_column_ids.at(i)))) {
            LOG_WARN("push back column id failed", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (column_ids.count() != column_checksum.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, column id count mismatch", K(ret), K(column_ids), K(column_checksum));
      }
    }
    ObArray<ObDDLChecksumItem> checksum_items;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
      const ObColumnSchemaV2 *column_schema = data_table_schema_->get_column_schema(column_ids.at(i).col_id_);
      if (NULL == column_schema) {
        column_schema = index_schema_->get_column_schema(column_ids.at(i).col_id_);
      }
      if (OB_ISNULL(column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "error unexpected, column schema must not be NULL", K(ret), K(column_ids.at(i).col_id_));
      /*} else if (column_schema->is_shadow_column()
          || column_schema->is_generated_column()
          || !column_schema->is_column_stored_in_sstable()) {
        STORAGE_LOG(INFO, "column do not need to compare checksum", K(column_ids.at(i).col_id_));*/
      } else {
        ObDDLChecksumItem item;
        item.execution_id_ = execution_id_;
        item.tenant_id_ = tenant_id_;
        item.table_id_ = report_table_id;
        item.ddl_task_id_ = task_id_;
        item.column_id_ = column_ids.at(i).col_id_;
        item.task_id_ = -tablet_id_.id();
        item.checksum_ = column_checksum.at(i);
        if (OB_FAIL(checksum_items.push_back(item))) {
          LOG_WARN("fail to push back item", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObDDLChecksumOperator::update_checksum(checksum_items, *GCTX.sql_proxy_))) {
        LOG_WARN("fail to update checksum", K(ret));
      }
    }
  }
  return ret;
}

int ObUniqueIndexChecker::check_unique_index(ObIDag *dag)
{
  int ret = OB_SUCCESS;
  bool need_report_error_msg = true;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObUniqueIndexChecker has not been inited", K(ret));
  } else if (OB_ISNULL(dag)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(dag));
  } else {
    MTL_SWITCH(tenant_id_) {
      ObLSHandle ls_handle;
      if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
        LOG_WARN("fail to get log stream", K(ret), K(ls_id_));
      } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle, tablet_id_, tablet_handle_))) {
        LOG_WARN("fail to get tablet", K(ret), K(tablet_id_), K(tablet_handle_));
      } else if (index_schema_->is_domain_index()) {
        STORAGE_LOG(INFO, "do not need to check unique for domain index", "index_id", index_schema_->get_table_id());
      } else {
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(wait_trans_end(dag))) {
          LOG_WARN("fail to wait trans end", K(ret));
        } else if (OB_FAIL(check_global_index(dag))) {
          LOG_WARN("fail to check global index", K(ret));
        }
      }
    } else {
      LOG_WARN("switch to tenant guard failed", K(ret));
    }
  }
  if (OB_SUCCESS != ret && share::ObIDDLTask::in_ddl_retry_white_list(ret)) {
    need_report_error_msg = false;
  }
  if (is_inited_ && need_report_error_msg) {
    int tmp_ret = OB_SUCCESS;
    int report_ret_code = OB_SUCCESS;
    const ObAddr &self_addr = GCTX.self_addr();
    bool keep_report_err_msg = true;
    LOG_INFO("begin to report build index status & ddl error message", K(index_schema_->get_table_id()), K(*index_schema_), K(tablet_id_));
    while (!dag->has_set_stop() && keep_report_err_msg) {
      int64_t task_id = 0;
      if (OB_SUCCESS != (tmp_ret = ObDDLErrorMessageTableOperator::get_index_task_id(*GCTX.sql_proxy_, *index_schema_, task_id))) {
        if (OB_ITER_END == tmp_ret) {
          keep_report_err_msg = false;
          LOG_INFO("get task id failed, check whether index building task is cancled", K(ret), K(tmp_ret), KPC(index_schema_));
        } else {
          LOG_INFO("get task id failed, but retry to get it", K(ret), K(tmp_ret), KPC(index_schema_));
        }
      } else if (OB_SUCCESS != (tmp_ret = ObDDLErrorMessageTableOperator::generate_index_ddl_error_message(
          ret, *index_schema_, task_id, tablet_id_.id(), self_addr, *GCTX.sql_proxy_, "\0", report_ret_code))) {
        LOG_WARN("fail to generate index ddl error message", K(ret), K(tmp_ret), KPC(index_schema_), K(tablet_id_), K(self_addr));
        ob_usleep(RETRY_INTERVAL);
        dag_yield();
      } else {
        if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret && OB_ERR_DUPLICATED_UNIQUE_KEY == report_ret_code) {
          //error message of OB_ERR_PRIMARY_KEY_DUPLICATE is not compatiable with oracle, so use a new error code
          ret = OB_ERR_DUPLICATED_UNIQUE_KEY;
        }
        keep_report_err_msg = false;
      }

      if (OB_TMP_FAIL(tmp_ret) && keep_report_err_msg) {
        bool is_tenant_dropped = false;
        if (OB_TMP_FAIL(GSCHEMASERVICE.check_if_tenant_has_been_dropped(tenant_id_, is_tenant_dropped))) {
          LOG_WARN("check if tenant has been dropped failed", K(tmp_ret), K(tenant_id_));
        } else if (is_tenant_dropped) {
          keep_report_err_msg = false;
          LOG_INFO("break when tenant dropped", K(tmp_ret), KPC(index_schema_), K(tablet_id_), K(self_addr));
        }
      }
    }
  }
  return ret;
}

int ObUniqueIndexChecker::wait_trans_end(ObIDag *dag)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLSService *ls_service = MTL(ObLSService *);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObUniqueIndexChecker has not been inited", K(ret));
  } else if (OB_FAIL(ls_service->get_ls(ObLSID(ls_id_), ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("get ls failed", K(ret), K(ls_id_));
  } else {
    const int64_t now = ObTimeUtility::current_time();
    const int64_t timeout_us = 1000L * 1000L * 60L; // 1 min
    while (OB_SUCC(ret) && !dag->has_set_stop()) {
      transaction::ObTransID pending_tx_id;
      if (OB_FAIL(ls_handle.get_ls()->check_modify_time_elapsed(tablet_id_, now, pending_tx_id))) {
        // when timeout with EAGAIN, ddl scheduler of root service will retry
        if (OB_EAGAIN == ret && ObTimeUtility::current_time() - now < timeout_us) {
          ret = OB_SUCCESS;
          ob_usleep(RETRY_INTERVAL);
          dag_yield();
        } else {
          LOG_WARN("fail to check modify time elapsed", K(ret));
          break;
        }
      } else {
        break;
      }
    }
  }
  return ret;
}

/* ObUniqueCheckingDag */

ObUniqueCheckingDag::ObUniqueCheckingDag()
  : ObIDag(ObDagType::DAG_TYPE_UNIQUE_CHECKING), is_inited_(false), tenant_id_(OB_INVALID_TENANT_ID), tablet_id_(), is_scan_index_(false),
    schema_guard_(share::schema::ObSchemaMgrItem::MOD_UNIQ_CHECK), index_schema_(nullptr), data_table_schema_(nullptr), callback_(nullptr),
    execution_id_(-1), snapshot_version_(0), compat_mode_(lib::Worker::CompatMode::INVALID)
{
}

ObUniqueCheckingDag::~ObUniqueCheckingDag()
{
  if (NULL != callback_) {
    ob_free(callback_);
    callback_ = NULL;
  }
}

int ObUniqueCheckingDag::init(
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const bool is_scan_index,
    const uint64_t index_table_id,
    const int64_t schema_version,
    const int64_t task_id,
    const int64_t execution_id,
    const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  ObMultiVersionSchemaService *schema_service = nullptr;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObUniqueCheckingDag has already been inited", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || !ls_id.is_valid() || !tablet_id.is_valid()
      || OB_INVALID_ID == index_table_id || schema_version < 0 || task_id <= 0
      || execution_id < 0 || snapshot_version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(tenant_id), K(ls_id), K(tablet_id),
        K(index_table_id), K(schema_version), K(task_id), K(execution_id), K(snapshot_version));
  } else {
    MTL_SWITCH(tenant_id) {
      if (OB_ISNULL(schema_service = MTL(ObTenantSchemaService *)->get_schema_service())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get schema service failed", K(ret));
      } else if (OB_FAIL(schema_service->get_tenant_schema_guard(tenant_id, schema_guard_, schema_version))) {
        STORAGE_LOG(WARN, "fail to get schema guard", K(ret), K(schema_version));
      } else if (OB_FAIL(schema_guard_.check_formal_guard())) {
        LOG_WARN("schema_guard is not formal", K(ret), K(tablet_id));
      } else if (OB_FAIL(schema_guard_.get_table_schema(tenant_id, index_table_id, index_schema_))) {
        STORAGE_LOG(WARN, "fail to get table schema", K(ret));
      } else if (OB_ISNULL(index_schema_)) {
        ret = OB_TABLE_NOT_EXIST;
        STORAGE_LOG(WARN, "fail to get table schema", K(ret), K(index_table_id));
      } else if (OB_FAIL(schema_guard_.get_table_schema(tenant_id, index_schema_->get_data_table_id(), data_table_schema_))) {
        STORAGE_LOG(WARN, "fail to get table schema", K(ret));
      } else if (OB_ISNULL(data_table_schema_)) {
        ret = OB_TABLE_NOT_EXIST;
        STORAGE_LOG(WARN, "data table not exist", K(ret));
      } else if (OB_FAIL(ObCompatModeGetter::get_table_compat_mode(tenant_id, index_table_id, compat_mode_))) {
        LOG_WARN("failed to get compat mode", K(ret), K(index_table_id));
      } else {
        is_inited_ = true;
        tenant_id_ = tenant_id;
        ls_id_ = ls_id;
        tablet_id_ = tablet_id;
        is_scan_index_ = is_scan_index;
        schema_service_ = schema_service;
        execution_id_ = execution_id;
        snapshot_version_ = snapshot_version;
        task_id_ = task_id;
      }
    } else {
      LOG_WARN("switch to tenant failed", K(ret), K(index_table_id), K(tenant_id));
    }
  }
  return ret;
}

int ObUniqueCheckingDag::alloc_unique_checking_prepare_task(ObIUniqueCheckingCompleteCallback *callback)
{
  int ret = OB_SUCCESS;
  ObUniqueCheckingPrepareTask *prepare_task = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObUniqueCheckingDag has not been inited", K(ret));
  } else if (OB_FAIL(alloc_task(prepare_task))) {
    STORAGE_LOG(WARN, "fail to alloc task", K(ret));
  } else if (OB_FAIL(prepare_task->init(callback))) {
    STORAGE_LOG(WARN, "fail to init prepare task", K(ret));
  } else if (OB_FAIL(add_task(*prepare_task))) {
    STORAGE_LOG(WARN, "fail to add task", K(ret));
  }
  return ret;
}

int ObUniqueCheckingDag::alloc_local_index_task_callback(
    ObLocalUniqueIndexCallback *&callback)
{
  int ret = OB_SUCCESS;
  void *buf = NULL;
  callback = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObUniqueCheckingDag has not been inited", K(ret));
  } else if (OB_ISNULL(buf = ob_malloc(sizeof(ObLocalUniqueIndexCallback),
      ObModIds::OB_CS_BUILD_INDEX))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to allocate memory", K(ret));
  } else if (OB_ISNULL(callback = new (buf) ObLocalUniqueIndexCallback())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to placement new local index callback", K(ret));
  } else {
    callback_ = callback;
  }

  return ret;
}

int ObUniqueCheckingDag::alloc_global_index_task_callback(
    const ObTabletID &tablet_id, const uint64_t index_id,
    const uint64_t data_table_id, const int64_t schema_version,
    const int64_t task_id,
    ObGlobalUniqueIndexCallback *&callback)
{
  int ret = OB_SUCCESS;
  void *buf = NULL;
  callback = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObUniqueCheckingDag has not been inited", K(ret));
  } else if (OB_UNLIKELY(!tablet_id.is_valid() || OB_INVALID_ID == index_id || OB_INVALID_ID == data_table_id || schema_version <= 0 || task_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(tablet_id), K(index_id), K(data_table_id), K(schema_version), K(task_id));
  } else if (OB_ISNULL(buf = ob_malloc(sizeof(ObGlobalUniqueIndexCallback),
      ObModIds::OB_CS_BUILD_INDEX))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to allocate memory", K(ret));
  } else if (OB_ISNULL(callback = new (buf) ObGlobalUniqueIndexCallback(tenant_id_, tablet_id, index_id, data_table_id, schema_version, task_id))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to placement new local index callback", K(ret));
  } else {
    callback_ = callback;
  }

  return ret;
}

int64_t ObUniqueCheckingDag::hash() const
{
  int tmp_ret = OB_SUCCESS;
  int64_t hash_val = 0;
  if (NULL == index_schema_) {
    tmp_ret = OB_ERR_SYS;
    STORAGE_LOG_RET(ERROR, tmp_ret, "index schema must not be NULL", K(tmp_ret));
  } else {
    hash_val = tablet_id_.hash() + index_schema_->get_table_id();
  }
  return hash_val;
}

int ObUniqueCheckingDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  int64_t index_id = 0;
  if (NULL != index_schema_) {
    index_id = index_schema_->get_table_id();
  }
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                static_cast<int64_t>(tablet_id_.id()), index_id))) {
    STORAGE_LOG(WARN, "failed to fill info param", K(ret));
  }
  return ret;
}

int ObUniqueCheckingDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t index_id = 0;
  if (NULL != index_schema_) {
    index_id = index_schema_->get_table_id();
  }

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, "tablet_id=%s index_id=%ld", to_cstring(tablet_id_), index_id))) {
    STORAGE_LOG(WARN, "failed to fill dag key", K(ret), K(tablet_id_), K(index_id));
  }
  return ret;
}

bool ObUniqueCheckingDag::ignore_warning()
{
  return OB_EAGAIN == dag_ret_
    || OB_NEED_RETRY == dag_ret_
    || OB_TASK_EXPIRED == dag_ret_;
}

bool ObUniqueCheckingDag::operator==(const ObIDag &other) const
{
  int tmp_ret = OB_SUCCESS;
  bool is_equal = false;
  if (OB_UNLIKELY(this == &other)) {
    is_equal = true;
  } else if (get_type() == other.get_type()) {
    const ObUniqueCheckingDag &dag = static_cast<const ObUniqueCheckingDag &>(other);
    if (NULL == index_schema_ || NULL == dag.index_schema_) {
      tmp_ret = OB_ERR_SYS;
      STORAGE_LOG_RET(ERROR, tmp_ret, "index schema must not be NULL", K(tmp_ret), KP(index_schema_),
          KP(dag.index_schema_));
    } else {
      is_equal = tablet_id_ == dag.tablet_id_
        && index_schema_->get_table_id() == dag.index_schema_->get_table_id();
    }
  }
  return is_equal;
}

ObUniqueCheckingPrepareTask::ObUniqueCheckingPrepareTask()
  : ObITask(TASK_TYPE_UNIQUE_CHECKING_PREPARE), is_inited_(false), index_schema_(NULL),
    data_table_schema_(NULL), callback_(NULL)
{
}

int ObUniqueCheckingPrepareTask::init(ObIUniqueCheckingCompleteCallback *callback)
{
  int ret = OB_SUCCESS;
  ObUniqueCheckingDag *dag = NULL;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObUniqueCheckingPrepareTask has already been inited", K(ret));
  } else if (OB_ISNULL(dag = static_cast<ObUniqueCheckingDag *>(get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "error unexpected, dag must not be NULL", K(ret));
  } else if (OB_ISNULL(index_schema_ = dag->get_index_schema())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), KP(index_schema_));
  } else if (OB_ISNULL(data_table_schema_ = dag->get_data_table_schema())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid data table schema", K(ret));
  } else {
    callback_ = callback;
    is_inited_ = true;
  }
  return ret;
}

int ObUniqueCheckingPrepareTask::process()
{
  int ret = OB_SUCCESS;
  ObUniqueCheckingDag *dag = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObUniqueCheckingPrepareTask has not been inited", K(ret));
  } else if (OB_ISNULL(dag = static_cast<ObUniqueCheckingDag *>(get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "error unexpected, dag must not be NULL", K(ret));
  } else if (OB_FAIL(generate_unique_checking_task(dag))) {
    STORAGE_LOG(WARN, "fail to generate unique checking task", K(ret));
  }
  return ret;
}

int ObUniqueCheckingPrepareTask::generate_unique_checking_task(ObUniqueCheckingDag *dag)
{
  int ret = OB_SUCCESS;
  ObSimpleUniqueCheckingTask *checking_task = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObIndexPrepareTask has not been inited", K(ret));
  } else if (OB_ISNULL(dag)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), KP(dag));
  } else if (OB_FAIL(dag->alloc_task(checking_task))) {
    STORAGE_LOG(WARN, "fail to alloc checking task", K(ret));
  } else if (OB_FAIL(checking_task->init(dag->get_tenant_id(), data_table_schema_, index_schema_, callback_))) {
    STORAGE_LOG(WARN, "fail to init unique checking task", K(ret));
  } else if (OB_FAIL(add_child(*checking_task))) {
    STORAGE_LOG(WARN, "fail to add child for prepare task", K(ret));
  } else if (OB_FAIL(dag->add_task(*checking_task))) {
    STORAGE_LOG(WARN, "fail to add unique checking task", K(ret));
  }
  return ret;
}

ObSimpleUniqueCheckingTask::ObSimpleUniqueCheckingTask()
  : ObITask(TASK_TYPE_SIMPLE_UNIQUE_CHECKING), is_inited_(false), unique_checker_(),
    index_schema_(NULL), data_table_schema_(NULL), tablet_id_(), callback_(NULL)
{
}

int ObSimpleUniqueCheckingTask::init(
    const uint64_t tenant_id,
    const ObTableSchema *data_table_schema,
    const ObTableSchema *index_schema,
    ObIUniqueCheckingCompleteCallback *callback)
{
  int ret = OB_SUCCESS;
  ObUniqueCheckingDag *dag = NULL;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObSimpleUniqueCheckingTask has already been inited", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_ISNULL(data_table_schema) || OB_ISNULL(index_schema) || OB_ISNULL(callback)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(tenant_id), KP(data_table_schema), KP(index_schema), KP(callback));
  } else if (OB_ISNULL(dag = static_cast<ObUniqueCheckingDag *>(get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "error unexpected, dag must not be NULL", K(ret));
  } else {
    tenant_id_ = tenant_id;
    tablet_id_ = dag->get_tablet_id();
    if (OB_UNLIKELY(!tablet_id_.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid arguments", K(ret), K(tablet_id_));
    } else if (OB_FAIL(unique_checker_.init(tenant_id_,
                                            dag->get_ls_id(),
                                            tablet_id_,
                                            dag->get_is_scan_index(),
                                            data_table_schema,
                                            index_schema,
                                            dag->get_task_id(),
                                            dag->get_execution_id(),
                                            dag->get_snapshot_version()))) {
      STORAGE_LOG(WARN, "fail to init unique index checker", K(ret));
    } else {
      index_schema_ = index_schema;
      data_table_schema_ = data_table_schema;
      callback_ = callback;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObSimpleUniqueCheckingTask::process()
{
  int ret = OB_SUCCESS;
  int ret_code = OB_SUCCESS;
  ObUniqueCheckingDag *dag = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObSimpleUniqueCheckingTask has not been inited", K(ret));
  } else if (OB_ISNULL(dag = static_cast<ObUniqueCheckingDag *>(get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "error unexpected, dag must not be NULL", K(ret));
  } else if (OB_FAIL(unique_checker_.check_unique_index(dag))) {
    STORAGE_LOG(WARN, "fail to check unique index", K(ret));
  }
  ret_code = ret;
  // overwrite ret
  if (NULL != callback_) {
    if (NULL != index_schema_) {
      STORAGE_LOG(INFO, "unique checking callback", K(tablet_id_), "index_id", index_schema_->get_table_id());
    }
    if (OB_FAIL(callback_->operator()(ret_code))) {
      STORAGE_LOG(WARN, "fail to check unique index response", K(ret));
    }
  }
  return ret;
}

ObGlobalUniqueIndexCallback::ObGlobalUniqueIndexCallback(
    const uint64_t tenant_id, const common::ObTabletID &tablet_id, const uint64_t index_id, const uint64_t data_table_id, const int64_t schema_version, const int64_t task_id)
  : tenant_id_(tenant_id), tablet_id_(tablet_id), index_id_(index_id), data_table_id_(data_table_id), schema_version_(schema_version), task_id_(task_id)
{
}

int ObGlobalUniqueIndexCallback::operator()(const int ret_code)
{
  int ret = OB_SUCCESS;
  obrpc::ObCalcColumnChecksumResponseArg arg;
  ObAddr rs_addr;
  arg.tablet_id_ = tablet_id_;
  arg.target_table_id_ = index_id_;
  arg.ret_code_ = ret_code;
  arg.source_table_id_ = data_table_id_;
  arg.schema_version_ = schema_version_;
  arg.task_id_ = task_id_;
  arg.tenant_id_ = tenant_id_;
#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = OB_E(EventTable::EN_DDL_REPORT_REPLICA_BUILD_STATUS_FAIL) OB_SUCCESS;
      LOG_INFO("report replica build status errsim", K(ret));
    }
#endif
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(GCTX.rs_rpc_proxy_) || OB_ISNULL(GCTX.rs_mgr_)) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "innner system error, rootserver rpc proxy or rs mgr must not be NULL", K(ret), K(GCTX));
  } else if (OB_FAIL(GCTX.rs_mgr_->get_master_root_server(rs_addr))) {
    STORAGE_LOG(WARN, "fail to get rootservice address", K(ret));
  } else if (OB_FAIL(GCTX.rs_rpc_proxy_->to(rs_addr).calc_column_checksum_response(arg))) {
    STORAGE_LOG(WARN, "fail to check unique index response", K(ret), K(arg));
  } else {
    STORAGE_LOG(INFO, "send column checksum response", K(arg));
  }
  return ret;
}

ObLocalUniqueIndexCallback::ObLocalUniqueIndexCallback()
{
}

int ObLocalUniqueIndexCallback::operator()(const int ret_code)
{
  int ret = OB_SUCCESS;
  UNUSED(ret_code);
  return ret;
}
