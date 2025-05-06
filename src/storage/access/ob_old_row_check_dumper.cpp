/*************************************************************************
  * Copyright (c) 2025 OceanBase
  * OceanBase is licensed under Mulan PubL v2.
  * You can use this software according to the terms and conditions of the Mulan PubL v2
  * You may obtain a copy of Mulan PubL v2 at:
  *          http://license.coscl.org.cn/MulanPubL-2.0
  * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
  * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
  * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
  * See the Mulan PubL v2 for more details.
  * File Name   : ob_old_row_check_dumper.cpp
  * Created  on : 04/27/2025
 ************************************************************************/
#define USING_LOG_PREFIX STORAGE
#include "ob_old_row_check_dumper.h"
#include "share/schema/ob_table_dml_param.h"
#include "share/ob_force_print_log.h"
#include "ob_single_merge.h"
#include "storage/tablet/ob_tablet.h"

namespace oceanbase
{
namespace storage
{
ObOldRowCheckDumper::ObOldRowCheckDumper(ObDMLRunningCtx &run_ctx, const blocksstable::ObDatumRow &datum_row)
  : run_ctx_(run_ctx),
    datum_row_(datum_row),
    out_col_pros_(),
    allocator_(common::ObMemAttr(MTL_ID(), "DumpDIAGInfo")),
    rowkey_helper_(allocator_),
    access_param_(),
    access_ctx_(),
    datum_rowkey_()
{}

int ObOldRowCheckDumper::prepare_read_ctx()
{
  int ret = OB_SUCCESS;
  ObStoreCtx &store_ctx = run_ctx_.store_ctx_;
  ObRelativeTable &data_table = run_ctx_.relative_table_;
  ObColDescIArray &col_descs = const_cast<ObColDescIArray&>(*run_ctx_.col_descs_);
  const int64_t schema_rowkey_cnt = data_table.get_rowkey_column_num();
  ObTableStoreIterator &table_iter = *data_table.tablet_iter_.table_iter();
  ObQueryFlag query_flag(ObQueryFlag::Forward,
      false, /*is daily merge scan*/
      false, /*is read multiple macro block*/
      false, /*sys task scan, read one macro block in single io*/
      false /*is full row scan?*/,
      false,
      false);
  query_flag.read_latest_ = ObQueryFlag::OBSF_MASK_READ_LATEST;
  common::ObVersionRange trans_version_rang;
  trans_version_rang.base_version_ = 0;
  trans_version_rang.multi_version_start_ = 0;
  trans_version_rang.snapshot_version_ = store_ctx.mvcc_acc_ctx_.get_snapshot_version().get_val_for_tx();
  const share::schema::ObTableSchemaParam *schema_param = data_table.get_schema_param();
  const ObITableReadInfo *read_info = &schema_param->get_read_info();
  for (int64_t i = 0; OB_SUCC(ret) && i < read_info->get_request_count(); i++) {
    if (OB_FAIL(out_col_pros_.push_back(i))) {
      STORAGE_LOG(WARN, "Failed to push back col project", K(ret), K(i));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(rowkey_helper_.prepare_datum_rowkey(datum_row_, schema_rowkey_cnt, col_descs, datum_rowkey_))) {
    STORAGE_LOG(WARN, "Failed to prepare rowkey", K(ret), K(datum_row_), K(datum_rowkey_));
  } else if (OB_FAIL(access_ctx_.init(query_flag, store_ctx, allocator_, trans_version_rang))) {
    STORAGE_LOG(WARN, "Fail to init access ctx", K(ret));
  } else {
    access_param_.iter_param_.table_id_ = data_table.get_table_id();
    access_param_.iter_param_.tablet_id_ = data_table.tablet_iter_.get_tablet()->get_tablet_meta().tablet_id_;
    if (nullptr != data_table.tablet_iter_.get_tablet()) {
      access_param_.iter_param_.ls_id_ = data_table.tablet_iter_.get_tablet()->get_tablet_meta().ls_id_;
    }
    access_param_.iter_param_.read_info_ = read_info;
    access_param_.iter_param_.cg_read_infos_ = schema_param->get_cg_read_infos();
    access_param_.iter_param_.out_cols_project_ = &out_col_pros_;
    access_param_.iter_param_.set_tablet_handle(data_table.get_tablet_handle());
    access_param_.iter_param_.need_trans_info_ = true;
    access_param_.is_inited_ = true;
  }

  return ret;
}

int ObOldRowCheckDumper::dump_diag_tables()
{
  int ret = OB_SUCCESS;

  ObTableStoreIterator &table_iter = *run_ctx_.relative_table_.tablet_iter_.table_iter();
  ObStoreRowIterator *getter = nullptr;
  ObITable *table = nullptr;
  const ObDatumRow *row = nullptr;

  FLOG_INFO("[DUMP DIAG] Try to find the specified rowkey within all tables", K(datum_row_), K(table_iter));
  FLOG_INFO("Prepare the diag env to dump the rows", K(datum_rowkey_), K(access_ctx_.trans_version_range_));

  table_iter.resume();
  while (OB_SUCC(ret)) {
    bool row_found = false;
    if (OB_FAIL(table_iter.get_next(table))) {
      if (OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "failed to get next tables", K(ret));
      }
      break;
    } else if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "table must not be null", K(ret), K(table_iter));
    } else if (OB_FAIL(table->get(access_param_.iter_param_, access_ctx_, datum_rowkey_, getter))) {
      STORAGE_LOG(WARN, "Failed to get param", K(ret), KPC(table));
    } else if (OB_FAIL(getter->get_next_row(row))) {
      STORAGE_LOG(WARN, "Failed to get next row", K(ret), K(table));
    } else if (row->row_flag_.is_not_exist()){
      FLOG_INFO("Cannot found rowkey in the table", KPC(row), K(table->get_key()));
    } else {
      row_found = true;
      FLOG_INFO("Found rowkey in the sstable", KPC(row), K(table->get_key()));
    }

    if (OB_SUCC(ret) && table->is_sstable()) {
      access_ctx_.query_flag_.set_not_use_row_cache();
      getter->reuse();
      if (OB_FAIL(getter->init(access_param_.iter_param_, access_ctx_, table, &datum_rowkey_))) {
        STORAGE_LOG(WARN, "Failed to init getter", K(ret));
      } else if (OB_FAIL(getter->get_next_row(row))) {
        STORAGE_LOG(WARN, "Failed to get next row", K(ret), KPC(table));
      } else if (row->row_flag_.is_not_exist()) {
        if (row_found) {
          FLOG_INFO("Attention! Cannot found rowkey in the table without row cache", KPC(row), K(table->get_key()));
        }
      } else if (!row_found) {
        FLOG_INFO("Attention! Found rowkey in the sstable without row cache", KPC(row), K(table->get_key()));
      }
      access_ctx_.query_flag_.set_use_row_cache();
    }
    // ignore error in the loop
    ret = OB_SUCCESS;
    if (OB_NOT_NULL(getter)) {
      getter->~ObStoreRowIterator();
      getter = nullptr;
    }
  }
  return OB_SUCCESS;
}

int ObOldRowCheckDumper::dump_diag_merge()
{
  int ret = OB_SUCCESS;

  ObRelativeTable &data_table = run_ctx_.relative_table_;

  FLOG_INFO("[DUMP DIAG] prepare to use single merge to find row", K(datum_rowkey_), K(access_param_));
  ObSingleMerge *get_merge = nullptr;
  ObGetTableParam get_table_param;
  ObDatumRow *row = nullptr;
  void *buf = nullptr;
  bool row_found = false;
  if (OB_FAIL(get_table_param.tablet_iter_.assign(data_table.tablet_iter_))) {
    STORAGE_LOG(WARN, "Failed to assign tablet iterator", K(ret));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObSingleMerge)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "Failed to alloc memory for single merge", K(ret));
  } else if (FALSE_IT(get_merge = new(buf)ObSingleMerge())) {
  } else if (OB_FAIL(get_merge->init(access_param_, access_ctx_, get_table_param))) {
    STORAGE_LOG(WARN, "Failed to init single get merge", K(ret));
  } else if (OB_FAIL(get_merge->open(datum_rowkey_))) {
    STORAGE_LOG(WARN, "Failed to open single merge", K(ret));
  } else if (FALSE_IT(get_merge->disable_fill_default())) {
  } else {
    if (OB_SUCC(get_merge->get_next_row(row))) {
      row_found = true;
      FLOG_INFO("Found one row for the rowkey with single merge", KPC(row));
    }
    ret = OB_SUCCESS;
    access_ctx_.use_fuse_row_cache_ = false;
    get_merge->reuse();
    if (OB_FAIL(get_merge->open(datum_rowkey_))) {
      STORAGE_LOG(WARN, "Failed to open single merge", K(ret));
    } else if (FALSE_IT(get_merge->disable_fill_default())) {
    } else {
      if (OB_SUCC(get_merge->get_next_row(row))) {
        if (!row_found) {
          FLOG_INFO("Attention!! Found one row for the rowkey with single merge and disable fuse row cache", KPC(row));
        }
      } else if (row_found) {
        FLOG_INFO("Attention!! Cannot Found one row for the rowkey with single merge and disable fuse row cache", KPC(row));
      }
    }
    access_ctx_.use_fuse_row_cache_ = true;
  }
  if (OB_NOT_NULL(get_merge)) {
    get_merge->~ObSingleMerge();
    get_merge = nullptr;
  }
  return OB_SUCCESS;
}

int ObOldRowCheckDumper::dump_diag_log()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(prepare_read_ctx())) {
    STORAGE_LOG(WARN, "Failed to prepare dump read ctx", K(ret));
  } else if (OB_FAIL(dump_diag_tables())) {
    STORAGE_LOG(WARN, "Failed to dump diag info for tables", K(ret));
  } else if (OB_FAIL(dump_diag_merge())) {
    STORAGE_LOG(WARN, "Failed to dump diag info for single merge", K(ret));
  } else {
#ifdef ENABLE_DEBUG_LOG
    ObStoreCtx &store_ctx = run_ctx_.store_ctx_;
    // print single row check info
    if (store_ctx.mvcc_acc_ctx_.tx_id_.is_valid()) {
      transaction::ObTransService *trx = MTL(transaction::ObTransService *);
      if (OB_NOT_NULL(trx)
          && NULL != trx->get_defensive_check_mgr()) {
        (void)trx->get_defensive_check_mgr()->dump(store_ctx.mvcc_acc_ctx_.tx_id_);
      }
    }
#endif
  }

  return ret;
}

}
}
