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

#include "ob_direct_insert_sstable_ctx.h"
#include "share/ob_ddl_checksum.h"
#include "share/ob_ddl_error_message_table_operator.h"
#include "share/ob_ddl_common.h"
#include "share/ob_tablet_autoincrement_service.h"
#include "storage/ddl/ob_ddl_merge_task.h"
#include "storage/blocksstable/ob_index_block_builder.h"
#include "storage/compaction/ob_column_checksum_calculator.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/compaction/ob_tenant_freeze_info_mgr.h"
#include "sql/engine/pdml/static/ob_px_sstable_insert_op.h"
#include "storage/lob/ob_lob_util.h"
#include "storage/ddl/ob_tablet_ddl_kv_mgr.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::sql;


/***************              ObSSTableInsertTabletParam              *****************/
ObSSTableInsertTabletParam::ObSSTableInsertTabletParam()
  : context_id_(0), ls_id_(), tablet_id_(), table_id_(0), write_major_(false),
    task_cnt_(0), schema_version_(0), snapshot_version_(0), execution_id_(1), ddl_task_id_(0),
    data_format_version_(0)
{

}

ObSSTableInsertTabletParam::~ObSSTableInsertTabletParam()
{

}

bool ObSSTableInsertTabletParam::is_valid() const
{
  bool bret = context_id_ > 0
              && ls_id_.is_valid()
              && tablet_id_.is_valid()
              && table_id_ > 0
              && task_cnt_ >= 0
              && schema_version_ > 0
              && execution_id_ >= 0
              && ddl_task_id_ > 0
              && data_format_version_ > 0;
  return bret;
}

ObSSTableInsertRowIterator::ObSSTableInsertRowIterator(sql::ObExecContext &exec_ctx, sql::ObPxMultiPartSSTableInsertOp *op)
  : exec_ctx_(exec_ctx), op_(op), current_row_(), current_tablet_id_(), is_next_row_cached_(true)
{

}

ObSSTableInsertRowIterator::~ObSSTableInsertRowIterator()
{

}

void ObSSTableInsertRowIterator::reset()
{

}

int ObSSTableInsertRowIterator::get_next_row(common::ObNewRow *&row)
{
  UNUSEDx(row);
  return OB_NOT_SUPPORTED;
}

int ObSSTableInsertRowIterator::get_next_row_with_tablet_id(
    const uint64_t table_id,
    const int64_t rowkey_count,
    const int64_t snapshot_version,
    common::ObNewRow *&row,
    ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == op_ || 0 >= snapshot_version) ) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operator is null", K(ret), KP(op_), K(snapshot_version));
  } else {
    if (OB_UNLIKELY(is_next_row_cached_)) {
      is_next_row_cached_ = false;
    } else if (OB_FAIL(op_->get_next_row_with_cache())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next row from child failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      op_->clear_evaluated_flag();
      if (OB_FAIL(op_->get_tablet_id_from_row(op_->get_child()->get_spec().output_,
                                              op_->get_spec().row_desc_.get_part_id_index(),
                                              current_tablet_id_))) {
        LOG_WARN("get part id failed", K(ret));
      } else {
        const ObExprPtrIArray &exprs = op_->get_spec().ins_ctdef_.new_row_;
        ObEvalCtx &eval_ctx = op_->get_eval_ctx();
        int64_t extra_rowkey_cnt = storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
        int64_t request_cnt = exprs.count() + extra_rowkey_cnt;
        if (OB_UNLIKELY((rowkey_count > exprs.count()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected rowkey count", K(ret), K(rowkey_count), K(exprs.count()));
        } else if (current_row_.get_count() <= 0) {
          ObObj *cells = static_cast<ObObj *>(op_->get_exec_ctx().get_allocator().alloc(sizeof(ObObj) * request_cnt));
          if (OB_ISNULL(cells)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("allocate memory failed", K(ret));
          } else {
            new (cells) ObObj[request_cnt];
            current_row_.cells_ = cells;
            current_row_.count_ = request_cnt;
          }
        } else if (OB_UNLIKELY(current_row_.get_count() < request_cnt)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected row col count", K(ret), K(current_row_.get_count()), K(request_cnt));
        }

        if (OB_SUCC(ret)) {
          for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
            ObDatum *datum = NULL;
            const ObExpr *e = exprs.at(i);
            if (OB_ISNULL(e)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("expr is NULL", K(ret), K(i));
            } else if (OB_FAIL(e->eval(eval_ctx, datum))) {
              LOG_WARN("evaluate expression failed", K(ret), K(i), KPC(e));
            } else if (i < rowkey_count) {
              if (OB_FAIL(datum->to_obj(current_row_.cells_[i], e->obj_meta_, e->obj_datum_map_))) {
                LOG_WARN("convert datum to obj failed", K(ret), K(i), KPC(e));
              }
            } else if (OB_FAIL(datum->to_obj(current_row_.cells_[i + extra_rowkey_cnt], e->obj_meta_, e->obj_datum_map_))) {
              LOG_WARN("convert datum to obj failed", K(ret), K(i), KPC(e));
            }
          }
          // add extra rowkey
          current_row_.cells_[rowkey_count].set_int(-snapshot_version);
          current_row_.cells_[rowkey_count + 1].set_int(0);
        }
      }
    }
    if (OB_SUCC(ret)) {
      row = &current_row_;
      tablet_id = current_tablet_id_;
    }
  }
  return ret;
}

ObTabletID ObSSTableInsertRowIterator::get_current_tablet_id() const
{
  return current_tablet_id_;
}

int ObSSTableInsertRowIterator::get_sql_mode(ObSQLMode &sql_mode) const
{
  int ret = OB_SUCCESS;
  ObOperator *base_op = nullptr;
  const ObSQLSessionInfo *session_info = nullptr;
  if (OB_ISNULL(op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid op", K(ret));
  } else if (OB_ISNULL(base_op = static_cast<ObOperator *>(op_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid base operator", K(ret));
  } else if (OB_ISNULL(session_info = base_op->get_exec_ctx().get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid session info pointer", K(ret));
  } else {
    sql_mode = session_info->get_sql_mode();
  }
  return ret;
}

/***************              ObSSTableInsertSliceWriter               *****************/

ObSSTableInsertSliceParam::ObSSTableInsertSliceParam()
  : snapshot_version_(0),
    write_major_(false),
    sstable_index_builder_(nullptr),
    task_id_(0)
{
}

ObSSTableInsertSliceParam::~ObSSTableInsertSliceParam()
{
}

bool ObSSTableInsertSliceParam::is_valid() const
{
  return tablet_id_.is_valid() && ls_id_.is_valid() && table_key_.is_valid() &&
         start_seq_.is_valid() && start_scn_.is_valid() && frozen_scn_.is_valid() &&
         nullptr != sstable_index_builder_ && 0 != task_id_;
}

ObSSTableInsertSliceWriter::ObSSTableInsertSliceWriter()
  : rowkey_column_num_(0),
    is_index_table_(false),
    col_descs_(nullptr),
    snapshot_version_(0),
    allocator_(lib::ObLabel("PartInsSst")),
    lob_allocator_(lib::ObLabel("PartInsSstLob")),
    lob_cnt_(0),
    sql_mode_for_ddl_reshape_(0),
    reshape_ptr_(nullptr),
    is_inited_(false)
{
}

ObSSTableInsertSliceWriter::~ObSSTableInsertSliceWriter()
{
  if (nullptr != reshape_ptr_) {
    ObRowReshapeUtil::free_row_reshape(allocator_, reshape_ptr_, 1);
    reshape_ptr_ = nullptr;
  }
}

int ObSSTableInsertSliceWriter::init(const ObSSTableInsertSliceParam &slice_param,
                                     const ObTableSchema *table_schema,
                                     ObDDLKvMgrHandle &ddl_kv_mgr_handle)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSSTableInsertSliceWriter init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!slice_param.is_valid() || nullptr == table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(slice_param), KP(table_schema));
  } else {
    const ObSQLMode sql_mode_for_ddl_reshape = SMO_TRADITIONAL;
    if (OB_FAIL(sstable_redo_writer_.init(slice_param.ls_id_, slice_param.tablet_id_))) {
      LOG_WARN("fail to init sstable redo writer", KR(ret), K(slice_param.ls_id_),
               K(slice_param.tablet_id_));
    } else if (FALSE_IT(sstable_redo_writer_.set_start_scn(slice_param.start_scn_))) {
    } else if (OB_FAIL(redo_log_writer_callback_.init(DDL_MB_DATA_TYPE, slice_param.table_key_,
                                                      slice_param.task_id_, &sstable_redo_writer_, ddl_kv_mgr_handle))) {
      LOG_WARN("fail to init redo log writer callback", KR(ret));
    } else if (OB_FAIL(data_desc_.init(*table_schema,
                                       slice_param.ls_id_,
                                       slice_param.tablet_id_, // TODO(shuangcan): confirm this
                                       slice_param.write_major_ ? MAJOR_MERGE : MINOR_MERGE,
                                       slice_param.frozen_scn_.get_val_for_tx()))) {
      LOG_WARN("fail to init data desc", KR(ret));
    } else {
      data_desc_.sstable_index_builder_ = slice_param.sstable_index_builder_;
      data_desc_.is_ddl_ = true;
      if (OB_FAIL(macro_block_writer_.open(data_desc_, slice_param.start_seq_,
                                           &redo_log_writer_callback_))) {
        LOG_WARN("fail to open macro block writer", KR(ret), K_(data_desc),
                 K(slice_param.start_seq_));
      }
    }
    if (OB_SUCC(ret)) {
      const ObColDescIArray &col_descs = data_desc_.get_full_stored_col_descs();
      ObTableSchemaParam schema_param(allocator_);
      ObRelativeTable relative_table;
      // Hack to prevent row reshaping from converting empty string to null.
      //
      // Supposing we have a row of type varchar with some spaces and an index on this column,
      // and then we convert this column to char. In this case, the DDL routine will first rebuild
      // the data table and then rebuilding the index table. The row may be reshaped as follows.
      //
      // - without hack: '  '(varchar) => ''(char) => null(char)
      // - with hack: '  '(varchar) => ''(char) => ''(char)
      if (OB_FAIL(prepare_reshape(slice_param.tablet_id_, table_schema, schema_param, relative_table))) {
        LOG_WARN("failed to prepare params for reshape", K(ret));
      } else if (OB_FAIL(ObRowReshapeUtil::malloc_rows_reshape_if_need(
                   allocator_, col_descs, 1, relative_table, sql_mode_for_ddl_reshape,
                   reshape_ptr_))) {
        LOG_WARN("failed to malloc row reshape", KR(ret));
      } else if (OB_FAIL(datum_row_.init(allocator_, col_descs.count()))) {
        LOG_WARN("fail to init datum row", KR(ret), K(col_descs));
      }
    }
    if (OB_SUCC(ret)) {
      tablet_id_ = slice_param.tablet_id_;
      ls_id_ = slice_param.ls_id_;
      rowkey_column_num_ = table_schema->get_rowkey_column_num();
      is_index_table_ = table_schema->is_index_table();
      col_descs_ = &data_desc_.get_full_stored_col_descs();
      snapshot_version_ = slice_param.snapshot_version_;
      sql_mode_for_ddl_reshape_ = sql_mode_for_ddl_reshape;
      store_row_.flag_.set_flag(ObDmlFlag::DF_INSERT);
      is_inited_ = true;
    }
  }
  return ret;
}

int ObSSTableInsertSliceWriter::append_row(ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSTableInsertSliceWriter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!datum_row.is_valid() ||
                         datum_row.get_column_count() != col_descs_->count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(datum_row), K(col_descs_->count()));
  } else {
    if (OB_FAIL(datum_row.prepare_new_row(*col_descs_))) {
      LOG_WARN("fail to prepare new row", KR(ret));
    } else if (OB_FAIL(append_row(datum_row.get_new_row()))) {
      LOG_WARN("fail to append row", KR(ret), K(datum_row));
    }
  }
  return ret;
}

int ObSSTableInsertSliceWriter::append_row(const ObNewRow &row_val)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSTableInsertSliceWriter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!row_val.is_valid() || row_val.get_count() != col_descs_->count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(row_val), K(col_descs_->count()));
  } else {
    if (OB_FAIL(ObRowReshapeUtil::reshape_table_rows(&row_val, reshape_ptr_, col_descs_->count(),
                                                     &store_row_, 1, sql_mode_for_ddl_reshape_))) {
      LOG_WARN("fail to reshape table rows", KR(ret));
    } else if (OB_FAIL(check_null(store_row_.row_val_))) {
      LOG_WARN("fail to check null value in row", KR(ret), K(store_row_));
    } else if (OB_FAIL(datum_row_.from_store_row(store_row_))) {
      LOG_WARN("fail to transfer store row ", KR(ret), K(store_row_));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < col_descs_->count(); i++) {
      ObStorageDatum &datum = datum_row_.storage_datums_[i];
      if (col_descs_->at(i).col_type_.is_lob_storage() && !datum.is_nop() && !datum.is_null()) {
        lob_cnt_++;
        const int64_t timeout_ts =
          ObTimeUtility::current_time() + ObInsertLobColumnHelper::LOB_ACCESS_TX_TIMEOUT;
        bool has_lob_header = store_row_.row_val_.cells_[i].has_lob_header();
        if (OB_FAIL(ObInsertLobColumnHelper::insert_lob_column(
              lob_allocator_, ls_id_, tablet_id_, col_descs_->at(i), datum, timeout_ts, has_lob_header,
              MTL_ID()))) {
          LOG_WARN("fail to insert_lob_col", KR(ret), K(datum));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(macro_block_writer_.append_row(datum_row_))) {
        LOG_WARN("fail to appen row", KR(ret));
      }
    }
    if (lob_cnt_ % ObInsertLobColumnHelper::LOB_ALLOCATOR_RESET_CYCLE == 0) {
      lob_allocator_.reuse(); // reuse after append_row to macro block to save memory
    }
  }
  return ret;
}

int ObSSTableInsertSliceWriter::prepare_reshape(const ObTabletID &tablet_id,
                                                const ObTableSchema *table_schema,
                                                ObTableSchemaParam &schema_param,
                                                ObRelativeTable &relative_table) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(schema_param.convert(table_schema))) {
    LOG_WARN("failed to convert schema param", K(ret));
    if (OB_SCHEMA_ERROR == ret) {
      ret = OB_CANCELED;
    }
  } else if (OB_FAIL(relative_table.init(&schema_param, tablet_id))) {
    LOG_WARN("fail to init relative_table", K(ret), K(schema_param), K(tablet_id));
  }
  return ret;
}

int ObSSTableInsertSliceWriter::check_null(const ObNewRow &row_val) const
{
  int ret = OB_SUCCESS;
  if (is_index_table_) {
    // index table is index-organized but can have null values in index column
  } else if (OB_UNLIKELY(rowkey_column_num_ > row_val.count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid rowkey column number", KR(ret), K_(rowkey_column_num), K(row_val));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_column_num_; i++) {
      const ObObj &cell = row_val.cells_[i];
      if (cell.is_null()) {
        ret = OB_ER_INVALID_USE_OF_NULL;
        LOG_WARN("invalid null cell for row key column", KR(ret), K(cell));
      }
    }
  }
  return ret;
}

int ObSSTableInsertSliceWriter::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSTableInsertSliceWriter not init", KR(ret), KP(this));
  } else {
    if (OB_FAIL(macro_block_writer_.close())) {
      LOG_WARN("fail to close macro block writer", K(ret));
    }
  }
  return ret;
}

/***************              ObSSTableInsertTabletContext              *****************/

ObSSTableInsertTabletContext::ObSSTableInsertTabletContext()
  : mutex_(ObLatchIds::SSTABLE_INSERT_TABLET_CONTEXT_LOCK), allocator_(), data_sstable_redo_writer_(),
    sstable_created_(false), task_finish_count_(0), index_builder_(nullptr),
    task_id_(0)
{

}

ObSSTableInsertTabletContext::~ObSSTableInsertTabletContext()
{
  if (OB_NOT_NULL(index_builder_)) {
    index_builder_->~ObSSTableIndexBuilder();
    allocator_.free(index_builder_);
    index_builder_ = nullptr;
  }
  ddl_kv_mgr_handle_.reset();
  allocator_.reset();
}

int ObSSTableInsertTabletContext::init(const ObSSTableInsertTabletParam &build_param)
{
  int ret = OB_SUCCESS;
  const int64_t memory_limit = 1024L * 1024L * 1024L * 10L; // 10GB
  share::ObLocationService *location_service = GCTX.location_service_;
  lib::ObMutexGuard guard(mutex_);
  if (OB_UNLIKELY(build_param_.is_valid())) {
    ret = OB_INIT_TWICE;
    LOG_WARN("build param has been inited", K(ret), K(build_param_));
  } else if (OB_UNLIKELY(!build_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(build_param));
  } else if (OB_FAIL(data_sstable_redo_writer_.init(build_param.ls_id_, build_param.tablet_id_))) {
    LOG_WARN("fail to init sstable redo writer", K(ret), K(build_param));
  } else if (OB_FAIL(allocator_.init(OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                    lib::ObLabel("TabletInsCtx"),
                                    OB_SERVER_TENANT_ID,
                                    memory_limit))) {
    LOG_WARN("init alloctor failed", K(ret));
  } else {
    build_param_ = build_param;
  }
  return ret;
}

int ObSSTableInsertTabletContext::update(const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(snapshot_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(snapshot_version));
  } else {
    ObITable::TableKey table_key;
    lib::ObMutexGuard guard(mutex_);
    build_param_.snapshot_version_ = snapshot_version;
    if (OB_FAIL(get_table_key(table_key))) {
      LOG_WARN("get table key failed", K(ret), K(build_param_));
    } else if (OB_UNLIKELY(!table_key.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(table_key));
    } else if (data_sstable_redo_writer_.get_start_scn().is_valid_and_not_min()) {
      // ddl start log is already written, do nothing
    } else if (OB_FAIL(data_sstable_redo_writer_.start_ddl_redo(table_key,
      build_param_.execution_id_, build_param_.data_format_version_, ddl_kv_mgr_handle_))) {
      LOG_WARN("fail write start log", K(ret), K(table_key), K(build_param_));
    }
  }
  return ret;
}

int ObSSTableInsertTabletContext::build_sstable_slice(
    const ObSSTableInsertTabletParam &build_param,
    const blocksstable::ObMacroDataSeq &start_seq,
    common::ObNewRowIterator &iter,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  const int64_t tenant_id = MTL_ID();
  const ObTabletID &tablet_id = build_param.tablet_id_;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;
  ObArenaAllocator allocator(lib::ObLabel("PartInsSstTmp"));
  ObSSTableInsertSliceWriter *sstable_slice_writer = nullptr;
  bool ddl_committed = false;
  if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
      tenant_id, schema_guard, build_param.schema_version_))) {
    LOG_WARN("get tenant schema failed", K(ret), K(build_param));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id,
             build_param.table_id_, table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(build_param));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", K(ret), K(build_param));
  } else if (OB_FAIL(construct_sstable_slice_writer(build_param, start_seq, sstable_slice_writer, allocator))) {
    LOG_WARN("fail to construct sstable slice writer", KR(ret), K(build_param), K(start_seq));
  } else if (OB_ISNULL(sstable_slice_writer)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null sstable slce writer", KR(ret));
  } else {
    const int64_t rowkey_column_num = table_schema->get_rowkey_column_num();
    const int64_t snapshot_version = sstable_slice_writer->get_snapshot_version();
    ObISSTableInsertRowIterator *tablet_row_iter = reinterpret_cast<ObISSTableInsertRowIterator *>(&iter);
    ObNewRow *row_val = nullptr;
    ObTabletID row_tablet_id;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(THIS_WORKER.check_status())) {
        LOG_WARN("check status failed", K(ret));
      } else if (OB_FAIL(tablet_row_iter->get_next_row_with_tablet_id(
                   build_param.table_id_, rowkey_column_num, snapshot_version, row_val,
                   row_tablet_id))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next row failed", K(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (tablet_id != row_tablet_id) {
        ret = OB_SUCCESS;
        break;
      } else if (!ddl_committed && OB_FAIL(sstable_slice_writer->append_row(*row_val))) {
        int tmp_ret = OB_SUCCESS;
        int report_ret_code = OB_SUCCESS;
        if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret && table_schema->is_unique_index()) {
          LOG_USER_ERROR(OB_ERR_PRIMARY_KEY_DUPLICATE,
              "", static_cast<int>(sizeof("UNIQUE IDX") - 1), "UNIQUE IDX");
          char index_key_buffer[OB_TMP_BUF_SIZE_256];
          ObStoreRowkey index_key;
          int64_t task_id = 0;
          index_key.assign(row_val->cells_, rowkey_column_num);
          if (OB_TMP_FAIL(ObDDLErrorMessageTableOperator::extract_index_key(*table_schema, index_key, index_key_buffer, OB_TMP_BUF_SIZE_256))) {   // read the unique key that violates the unique constraint
            LOG_WARN("extract unique index key failed", K(tmp_ret), K(index_key), K(index_key_buffer));
            // TODO(shuangcan): check if we need to change part_id to tablet_id
          } else if (OB_TMP_FAIL(ObDDLErrorMessageTableOperator::get_index_task_id(*GCTX.sql_proxy_, *table_schema, task_id))) {
            LOG_WARN("get task id of index table failed", K(tmp_ret), K(task_id), KPC(table_schema));
          } else if (OB_TMP_FAIL(ObDDLErrorMessageTableOperator::generate_index_ddl_error_message(ret, *table_schema,
            task_id, row_tablet_id.id(), GCTX.self_addr(), *GCTX.sql_proxy_, index_key_buffer, report_ret_code))) {
            LOG_WARN("generate index ddl error message", K(tmp_ret), K(ret), K(report_ret_code));
          }
          if (OB_ERR_DUPLICATED_UNIQUE_KEY == report_ret_code) {
            //error message of OB_ERR_PRIMARY_KEY_DUPLICATE is not compatiable with oracle, so use a new error code
            ret = OB_ERR_DUPLICATED_UNIQUE_KEY;
          }
        } else if (OB_TRANS_COMMITED == ret) {
          ret = OB_SUCCESS;
          ddl_committed = true;
        } else {
          LOG_WARN("macro block writer append row failed", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        LOG_DEBUG("sstable insert op append row", KPC(row_val));
        ++affected_rows;
      }
    }
    if (OB_SUCC(ret)) {
      if (!ddl_committed && OB_FAIL(sstable_slice_writer->close())) {
        if (OB_TRANS_COMMITED == ret) {
          ret = OB_SUCCESS;
          ddl_committed = true;
        } else {
          LOG_WARN("close writer failed", K(ret));
        }
      }
    }
  }
  if (OB_NOT_NULL(sstable_slice_writer)) {
    sstable_slice_writer->~ObSSTableInsertSliceWriter();
    allocator.free(sstable_slice_writer);
    sstable_slice_writer = nullptr;
  }
  return ret;
}

int ObSSTableInsertTabletContext::construct_sstable_slice_writer(
    const ObSSTableInsertTabletParam &build_param,
    const ObMacroDataSeq &start_seq,
    ObSSTableInsertSliceWriter *&sstable_slice_writer,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  sstable_slice_writer = nullptr;
  const int64_t tenant_id = MTL_ID();
  ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  ObFreezeInfoProxy freeze_info_proxy(tenant_id);
  ObSimpleFrozenStatus frozen_status;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;
  ObITable::TableKey table_key;
  int64_t snapshot_version = 0;
  SCN snapshot_scn;
  {
    lib::ObMutexGuard guard(mutex_);
    snapshot_version = build_param_.snapshot_version_;
  }
  if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
      tenant_id, schema_guard, build_param.schema_version_))) {
    LOG_WARN("get tenant schema failed", K(ret), K(build_param));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id,
             build_param.table_id_, table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(build_param));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", K(ret), K(build_param));
  } else if (OB_FAIL(prepare_index_builder_if_need(*table_schema))) {
    LOG_WARN("prepare sstable index builder failed", K(ret), K(build_param));
  } else if (OB_FAIL(get_table_key(table_key))) {
    LOG_WARN("get table key failed", K(ret), K(build_param_));
  } else if (OB_FAIL(snapshot_scn.convert_for_tx(snapshot_version))) {
    LOG_WARN("fail to convert val to SCN", KR(ret), K(snapshot_version));
  } else if (OB_UNLIKELY(!table_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_key));
  } else if (OB_FAIL(freeze_info_proxy.get_frozen_info_less_than(
          *sql_proxy, snapshot_scn, frozen_status))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get freeze info failed", K(ret), K(build_param_));
    } else {
      frozen_status.frozen_scn_ = SCN::base_scn();
      ret = OB_SUCCESS;
    }
  }
  if (OB_SUCC(ret)) {
    ObSSTableInsertSliceParam slice_param;
    slice_param.tablet_id_ = build_param_.tablet_id_;
    slice_param.ls_id_ = build_param_.ls_id_;
    slice_param.table_key_ = table_key;
    slice_param.start_seq_ = start_seq;
    slice_param.start_scn_ = data_sstable_redo_writer_.get_start_scn();
    slice_param.snapshot_version_ = snapshot_version;
    slice_param.frozen_scn_ = frozen_status.frozen_scn_;
    slice_param.write_major_ = build_param.write_major_;
    slice_param.sstable_index_builder_ = index_builder_;
    slice_param.task_id_ = build_param_.ddl_task_id_;
    if (OB_ISNULL(sstable_slice_writer = OB_NEWx(ObSSTableInsertSliceWriter, (&allocator)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObSSTableInsertSliceWriter", KR(ret));
    } else if (OB_FAIL(sstable_slice_writer->init(slice_param, table_schema, ddl_kv_mgr_handle_))) {
      LOG_WARN("fail to init sstable slice writer", KR(ret), K(slice_param));
    } else {
      FLOG_INFO("init sstable slice writer finished", K(ret), K(slice_param));
    }
    if (OB_FAIL(ret)) {
      if (nullptr != sstable_slice_writer) {
        sstable_slice_writer->~ObSSTableInsertSliceWriter();
        allocator.free(sstable_slice_writer);
        sstable_slice_writer = nullptr;
      }
    }
  }
  return ret;
}

int ObSSTableInsertTabletContext::prepare_index_builder_if_need(const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  ObDataStoreDesc data_desc;
  lib::ObMutexGuard guard(mutex_);
  if (index_builder_ != nullptr) {
    LOG_INFO("index builder is already prepared");
  } else if (OB_FAIL(data_desc.init_as_index(table_schema,
                                    build_param_.ls_id_,
                                    build_param_.tablet_id_, // TODO(shuangcan): confirm this
                                    build_param_.write_major_ ? storage::MAJOR_MERGE : storage::MINOR_MERGE,
                                    1L /*snapshot_version*/,
                                    build_param_.data_format_version_))) {
    LOG_WARN("fail to init data desc", K(ret));
  } else {
    void *builder_buf = nullptr;
    data_desc.is_ddl_ = true;

    if (OB_ISNULL(builder_buf = allocator_.alloc(sizeof(ObSSTableIndexBuilder)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret));
    } else if (OB_ISNULL(index_builder_ = new (builder_buf) ObSSTableIndexBuilder())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to new ObSSTableIndexBuilder", K(ret));
    } else if (OB_FAIL(index_builder_->init(data_desc))) {
      LOG_WARN("failed to init index builder", K(ret), K(data_desc));
    }

    if (OB_FAIL(ret)) {
      if (nullptr != index_builder_) {
        index_builder_->~ObSSTableIndexBuilder();
        index_builder_ = nullptr;
      }
      if (nullptr != builder_buf) {
        allocator_.free(builder_buf);
        builder_buf = nullptr;
      }
    }
  }
  return ret;
}

int ObSSTableInsertTabletContext::get_tablet_cache_interval(ObTabletCacheInterval &interval)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  ObTabletAutoincrementService &autoinc_service = ObTabletAutoincrementService::get_instance();
  if (OB_FAIL(autoinc_service.get_tablet_cache_interval(MTL_ID(),
                                                        interval))) {
    LOG_WARN("failed to get tablet cache intervals", K(ret));
  } else {
    interval.task_id_ = task_id_;
    ++task_id_;
  }
  return ret;
}

int ObSSTableInsertTabletContext::inc_finish_count(bool &is_ready)
{
  int ret = OB_SUCCESS;
  is_ready = false;
  ATOMIC_INC(&task_finish_count_);
  if (task_finish_count_ >= build_param_.task_cnt_) {
    is_ready = true;
  }
  return ret;
}

int ObSSTableInsertTabletContext::create_sstable()
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  ObITable::TableKey table_key;
  if (!build_param_.write_major_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("create minor sstable is not support", K(ret));
  } else if (sstable_created_) {
    ret = OB_SUCCESS;
    LOG_INFO("sstable has been created", K(ret), K(build_param_), K(sstable_created_));
  } else if (OB_FAIL(get_table_key(table_key))) {
    LOG_WARN("get table key failed", K(ret), K(build_param_));
  } else if (OB_FAIL(create_sstable_with_clog(table_key, build_param_.table_id_))) {
    LOG_WARN("create sstable with clog failed", K(ret), K(build_param_), K(table_key));
  } else {
    sstable_created_ = true;
    if (OB_NOT_NULL(index_builder_)) {
      index_builder_->~ObSSTableIndexBuilder();
      allocator_.free(index_builder_);
      index_builder_ = nullptr;
    }
  }
  return ret;
}

struct SliceKey final
{
public:
  SliceKey() : idx_(-1), end_key_() {}
  ~SliceKey() = default;
  TO_STRING_KV(K(idx_), K(end_key_));
public:
  int64_t idx_;
  ObRowkey end_key_;
};

struct GetManageTabletIDs final
{
public:
  explicit GetManageTabletIDs() : ret_code_(OB_SUCCESS) {}
  ~GetManageTabletIDs() = default;
  int operator()(common::hash::HashMapPair<ObTabletID, ObSSTableInsertTabletContext *> &entry)
  {
    int ret = ret_code_; // for LOG_WARN
    if (OB_LIKELY(OB_SUCCESS == ret_code_) && OB_SUCCESS != (ret_code_ = tablet_ids_.push_back(entry.first))) {
      ret = ret_code_;
      LOG_WARN("push back tablet id failed", K(ret_code_), K(entry.first));
    }
    return ret_code_;
  }
  TO_STRING_KV(K(tablet_ids_), K(ret_code_));
public:
  ObArray<ObTabletID> tablet_ids_;
  int ret_code_;
};

int ObSSTableInsertTabletContext::create_sstable_with_clog(
    const ObITable::TableKey &table_key,
    const int64_t table_id)
{
  int ret = OB_SUCCESS;
  // write clog and create sstable
  const int64_t max_kept_major_version_number = 1;
  share::schema::ObMultiVersionSchemaService *schema_service = nullptr;
  const share::schema::ObTableSchema *table_schema = nullptr;
  const uint64_t tenant_id = MTL_ID();
  ObSchemaGetterGuard schema_guard;
  if (OB_ISNULL(schema_service = GCTX.schema_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("schema service is null", K(ret), KP(schema_service));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("get schema_guard failed", K(ret), K(table_key));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(table_key));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table schema is null", K(ret), K(table_key), KP(table_schema));
  } else {
    DEBUG_SYNC(AFTER_REMOTE_WRITE_DDL_PREPARE_LOG);
    if (OB_FAIL(data_sstable_redo_writer_.end_ddl_redo_and_create_ddl_sstable(
        build_param_.ls_id_, table_key, table_id, build_param_.execution_id_, build_param_.ddl_task_id_))) {
      LOG_WARN("fail create ddl sstable", K(ret), K(table_key));
    }
  }
  return ret;
}

int ObSSTableInsertTabletContext::get_table_key(ObITable::TableKey &table_key)
{
  int ret = OB_SUCCESS;
  table_key.reset();
  table_key.table_type_ = ObITable::TableType::MAJOR_SSTABLE;
  table_key.tablet_id_ = build_param_.tablet_id_;
  table_key.version_range_.snapshot_version_ = build_param_.snapshot_version_;
  return ret;
}

ObSSTableInsertTableParam::ObSSTableInsertTableParam()
  : exec_ctx_(nullptr), context_id_(0), dest_table_id_(OB_INVALID_ID), write_major_(false), schema_version_(0),
    snapshot_version_(0), task_cnt_(0), execution_id_(1), ddl_task_id_(1), data_format_version_(0), ls_tablet_ids_()
{
}

int ObSSTableInsertTableParam::assign(const ObSSTableInsertTableParam &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ls_tablet_ids_.assign(other.ls_tablet_ids_))) {
    LOG_WARN("assign tablet_ids failed", K(ret));
  } else {
    context_id_ = other.context_id_;
    dest_table_id_ = other.dest_table_id_;
    write_major_ = other.write_major_;
    schema_version_ = other.schema_version_;
    snapshot_version_ = other.snapshot_version_;
    task_cnt_ = other.task_cnt_;
    execution_id_ = other.execution_id_;
    ddl_task_id_ = other.ddl_task_id_;
    data_format_version_ = other.data_format_version_;
    exec_ctx_ = other.exec_ctx_;
  }
  return ret;
}

int ObSSTableInsertTableParam::fast_check_status()
{
  int ret = common::OB_SUCCESS;
  if (exec_ctx_ != nullptr) {
    ret = exec_ctx_->fast_check_status();
  }
  return ret;
}

ObSSTableInsertTableContext::ObSSTableInsertTableContext()
  : is_inited_(false), lock_(ObLatchIds::SSTABLE_INSERT_TABLE_CONTEXT_LOCK), param_(), allocator_(), tablet_ctx_map_(), finishing_idx_(0)
{
}

ObSSTableInsertTableContext::~ObSSTableInsertTableContext()
{
  remove_all_tablets_context(); // ignore error code.
  tablet_ctx_map_.destroy();
}

int ObSSTableInsertTableContext::init(
    const ObSSTableInsertTableParam &param)
{
  int ret = OB_SUCCESS;
  const int64_t memory_limit = 1024L * 1024L * 1024L * 10L; // 10GB
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableInsertSSTableContext has been inited twice", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(param));
  } else if (OB_FAIL(allocator_.init(OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                    lib::ObLabel("TablInsCtx"),
                                    OB_SERVER_TENANT_ID,
                                    memory_limit))) {
    LOG_WARN("init alloctor failed", K(ret));
  } else if (OB_FAIL(param_.assign(param))) {
    LOG_WARN("assign table insert param failed", K(ret));
  } else if (OB_FAIL(create_all_tablet_contexts(param.ls_tablet_ids_))) {
    LOG_WARN("create all tablet contexts failed", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObSSTableInsertTableContext::create_all_tablet_contexts(
  const common::ObIArray<LSTabletIDPair> &ls_tablet_ids)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ls_tablet_ids.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(ls_tablet_ids.count()));
  } else if (OB_FAIL(tablet_ctx_map_.create(ls_tablet_ids.count(), lib::ObLabel("TabInsCtx")))) {
    LOG_WARN("create tablet ctx map failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_tablet_ids.count(); ++i) {
      const ObTabletID &tablet_id = ls_tablet_ids.at(i).second;
      void *buf = nullptr;
      ObSSTableInsertTabletContext *tablet_ctx = nullptr;
      if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObSSTableInsertTabletContext)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc memory failed", K(ret));
      } else {
        tablet_ctx = new (buf) ObSSTableInsertTabletContext();
        ObSSTableInsertTabletParam param;
        param.context_id_ = param_.context_id_;
        param.ls_id_ = ls_tablet_ids.at(i).first;
        param.tablet_id_ = tablet_id;
        param.schema_version_ = param_.schema_version_;
        param.snapshot_version_ = param_.snapshot_version_;
        param.table_id_ = param_.dest_table_id_;
        param.write_major_ = param_.write_major_;
        param.task_cnt_ = param_.task_cnt_;
        param.execution_id_ = param_.execution_id_;
        param.ddl_task_id_ = param_.ddl_task_id_;
        param.data_format_version_ = param_.data_format_version_;
        if (OB_FAIL(tablet_ctx->init(param))) {
          LOG_WARN("init tablet insert sstable context", K(ret));
        } else if (OB_FAIL(tablet_ctx_map_.set_refactored(tablet_id, tablet_ctx))) {
          LOG_WARN("set tablet ctx map failed", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
        if (nullptr != tablet_ctx) {
          tablet_ctx->~ObSSTableInsertTabletContext();
          tablet_ctx = nullptr;
        }
        if (nullptr != buf) {
          allocator_.free(buf);
          buf = nullptr;
        }
      }
    }
  }
  return ret;
}

int ObSSTableInsertTableContext::update_context(const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableInsertSSTableContext has not been inited", K(ret));
  } else if (OB_UNLIKELY(snapshot_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(snapshot_version));
  } else {
    for (TABLET_CTX_MAP::iterator iter = tablet_ctx_map_.begin(); OB_SUCC(ret) && iter != tablet_ctx_map_.end(); ++iter) {
      ObSSTableInsertTabletContext *tablet_ctx = iter->second;
      if (OB_ISNULL(tablet_ctx)) {
        ret = OB_ERR_SYS;
        LOG_WARN("error sys, part ctx must not be nullptr", K(ret));
      } else if (OB_FAIL(tablet_ctx->update(snapshot_version))) {
        LOG_WARN("update tablet context failed", K(ret));
      }
    }
  }
  return ret;
}

int ObSSTableInsertTableContext::update_tablet_context(
    const ObTabletID &tablet_id,
    const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableInsertSSTableContext has not been inited", K(ret));
  } else if (OB_UNLIKELY(!tablet_id.is_valid() || snapshot_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tablet_id), K(snapshot_version));
  } else {
    ObSSTableInsertTabletContext *tablet_ctx = nullptr;
    if (OB_FAIL(get_tablet_context(tablet_id, tablet_ctx))) {
      LOG_WARN("get tablet context failed", K(ret), K(tablet_id));
    } else if (OB_ISNULL(tablet_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, tablet ctx must not be nullptr", K(ret));
    } else if (OB_FAIL(tablet_ctx->update(snapshot_version))) {
      LOG_WARN("update tablet context failed", K(ret));
    }
  }
  return ret;
}

int ObSSTableInsertTableContext::add_sstable_slice(
    const ObSSTableInsertTabletParam &build_param,
    const blocksstable::ObMacroDataSeq &start_seq,
    common::ObNewRowIterator &iter,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  ObSSTableInsertTabletContext *tablet_ctx = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableInsertSSTableContext has not been inited", K(ret));
  } else if (OB_FAIL(get_tablet_context(build_param.tablet_id_, tablet_ctx))) {
    LOG_WARN("get tablet context failed", K(ret), "tablet_id", build_param.tablet_id_);
  } else if (OB_ISNULL(tablet_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, tablet ctx must not be nullptr", K(ret));
  } else if (OB_FAIL(tablet_ctx->build_sstable_slice(build_param, start_seq, iter, affected_rows))) {
    LOG_WARN("build sstable slice failed", K(ret));
  }
  return ret;
}

int ObSSTableInsertTableContext::construct_sstable_slice_writer(
    const ObSSTableInsertTabletParam &build_param,
    const ObMacroDataSeq &start_seq,
    ObSSTableInsertSliceWriter *&sstable_slice_writer,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObSSTableInsertTabletContext *tablet_ctx = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableInsertSSTableContext has not been inited", K(ret));
  } else if (OB_FAIL(get_tablet_context(build_param.tablet_id_, tablet_ctx))) {
    LOG_WARN("get tablet context failed", K(ret), "tablet_id", build_param.tablet_id_);
  } else if (OB_ISNULL(tablet_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, tablet ctx must not be nullptr", K(ret));
  } else if (OB_FAIL(tablet_ctx->construct_sstable_slice_writer(build_param, start_seq, sstable_slice_writer, allocator))) {
    LOG_WARN("construct sstable slice writer failed", K(ret));
  }
  return ret;
}

int ObSSTableInsertTableContext::get_tablet_context(
    const ObTabletID &tablet_id,
    ObSSTableInsertTabletContext *&context)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableInsertSSTableContext has not been inited", K(ret));
  } else if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tablet_id));
  } else if (OB_FAIL(tablet_ctx_map_.get_refactored(tablet_id, context))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

int ObSSTableInsertTableContext::remove_all_tablets_context()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableInsertSSTableContext has not been inited", K(ret));
  } else {
    GetManageTabletIDs get_tablet_ids_fn;
    if (OB_FAIL(tablet_ctx_map_.foreach_refactored(get_tablet_ids_fn))) {
      LOG_WARN("get tablet ids failed", K(ret));
    } else if (OB_FAIL(get_tablet_ids_fn.ret_code_)) {
      LOG_WARN("get tablet ids failed", K(ret));
    }
    for (int64_t i = 0; i < get_tablet_ids_fn.tablet_ids_.count(); ++i) { // ignore error code.
      ObSSTableInsertTabletContext *tablet_context = nullptr;
      const ObTabletID &tablet_id = get_tablet_ids_fn.tablet_ids_.at(i);
      if (OB_FAIL(tablet_ctx_map_.erase_refactored(tablet_id, &tablet_context))) {
        LOG_WARN("erase failed", K(ret), K(tablet_id));
      } else {
        tablet_context->~ObSSTableInsertTabletContext();
        allocator_.free(tablet_context);
        tablet_context = nullptr;
      }
    }
  }
  return ret;
}

int ObSSTableInsertTableContext::finish(const bool need_commit)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableInsertSSTableContext has not been inited", K(ret));
  } else {
    GetManageTabletIDs get_tablet_ids_fn;
    if (OB_FAIL(tablet_ctx_map_.foreach_refactored(get_tablet_ids_fn))) {
      LOG_WARN("get tablet ids failed", K(ret));
    } else if (OB_FAIL(get_tablet_ids_fn.ret_code_)) {
      LOG_WARN("get tablet ids failed", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < get_tablet_ids_fn.tablet_ids_.count(); ++i) {
      const ObTabletID &tablet_id = get_tablet_ids_fn.tablet_ids_.at(i);
      ObSSTableInsertTabletContext *tablet_ctx = nullptr;
      if (OB_FAIL(get_tablet_context(tablet_id, tablet_ctx))) {
        LOG_WARN("get tablet context failed", K(ret));
      } else if (need_commit && OB_FAIL(tablet_ctx->create_sstable())) {
        LOG_WARN("create sstable failed", K(ret));
      }
    }
    remove_all_tablets_context(); // ignore error code.
  }
  return ret;
}

int ObSSTableInsertTableContext::get_tablet_ids(common::ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  tablet_ids.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableInsertSSTableContext has not been inited", K(ret));
  } else {
    GetManageTabletIDs get_tablet_ids_fn;
    if (OB_FAIL(tablet_ctx_map_.foreach_refactored(get_tablet_ids_fn))) {
      LOG_WARN("get tablet ids failed", K(ret));
    } else if (OB_FAIL(get_tablet_ids_fn.ret_code_)) {
      LOG_WARN("get tablet ids failed", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < get_tablet_ids_fn.tablet_ids_.count(); ++i) {
      const ObTabletID &tablet_id = get_tablet_ids_fn.tablet_ids_.at(i);
      if (OB_FAIL(tablet_ids.push_back(tablet_id))) {
        LOG_WARN("push back tablet id failed", K(ret), K(tablet_id));
      }
    }
  }
  return ret;
}

int ObSSTableInsertTableContext::notify_tablet_end(const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObSSTableInsertTabletContext *tablet_ctx = nullptr;
  bool is_ready = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableInsertSSTableContext has not been inited", K(ret));
  } else if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_id));
  } else if (OB_FAIL(get_tablet_context(tablet_id, tablet_ctx))) {
    LOG_WARN("get tablet context failed", K(ret), K(tablet_id));
  } else if (OB_ISNULL(tablet_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet context is null", K(ret), K(tablet_id),KP(tablet_ctx));
  } else if (OB_FAIL(tablet_ctx->inc_finish_count(is_ready))) {
    LOG_WARN("increase finish count failed", K(ret), K(tablet_id));
  } else if (is_ready) {
    ObSpinLockGuard guard(lock_);
    if (OB_FAIL(ready_tablets_.push_back(tablet_id))) {
      LOG_WARN("push back tablet id failed", K(ret), K(tablet_id));
    }
  }
  return ret;
}

int ObSSTableInsertTableContext::finish_ready_tablets(const int64_t target_count)
{
  int ret = OB_SUCCESS;
  int64_t cur_finishing_idx = 0;
  int64_t next_finishing_idx = 0;
  int64_t old_finishing_idx = 0;
  while (OB_SUCC(ret) && OB_SUCC(param_.fast_check_status()) && ready_tablets_.count() < target_count) {
    ob_usleep(1000);
    if (TC_REACH_TIME_INTERVAL(1000L * 1000L * 1L)) {
      LOG_INFO("wait ready tablets reach target count", K(ready_tablets_.count()), K(target_count));
    }
  }
  while (OB_SUCC(ret) && OB_SUCC(param_.fast_check_status())) {
    old_finishing_idx = cur_finishing_idx = ATOMIC_LOAD(&finishing_idx_);
    while ((next_finishing_idx = cur_finishing_idx + 1) <= target_count &&
           old_finishing_idx != (cur_finishing_idx = ATOMIC_CAS(&finishing_idx_, old_finishing_idx,
                                                                next_finishing_idx))) {
      old_finishing_idx = cur_finishing_idx;
      PAUSE();
    }
    if (next_finishing_idx > target_count) {
      break;
    }
    ObTabletID tablet_id = ready_tablets_.at(cur_finishing_idx);
    ObSSTableInsertTabletContext *tablet_ctx = nullptr;
    if (OB_FAIL(get_tablet_context(tablet_id, tablet_ctx))) {
      LOG_WARN("get tablet context failed", K(ret), K(tablet_id));
    } else if (OB_ISNULL(tablet_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet context is null", K(ret), K(tablet_id), KP(tablet_ctx));
    } else if (OB_FAIL(tablet_ctx->create_sstable())) {
      LOG_WARN("create sstable failed", K(ret), K(tablet_id));
    } else {
      LOG_INFO("finish ready tablet", K(ret), K(cur_finishing_idx), K(tablet_id), K(ready_tablets_.count()));
    }
  }
  return ret;
}

int ObSSTableInsertTableContext::get_tablet_cache_interval(const ObTabletID &tablet_id,
                                                           ObTabletCacheInterval &interval)
{
  int ret = OB_SUCCESS;
  ObSSTableInsertTabletContext *tablet_ctx = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableInsertSSTableContext has not been inited", K(ret));
  } else if (OB_FAIL(get_tablet_context(tablet_id, tablet_ctx))) {
    LOG_WARN("get tablet context failed", K(ret), "tablet_id", tablet_id);
  } else if (OB_ISNULL(tablet_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, tablet ctx must not be nullptr", K(ret));
  } else if (OB_FAIL(tablet_ctx->get_tablet_cache_interval(interval))) {
    LOG_WARN("add sstable slice failed", K(ret));
  }
  return ret;
}


/***************              ObSSTableInsertManager              *****************/
ObSSTableInsertManager::ObSSTableInsertManager()
  : is_inited_(false), mutex_(ObLatchIds::SSTABLE_INSERT_TABLE_MANAGER_LOCK), context_id_generator_(0)
{

}

ObSSTableInsertManager::~ObSSTableInsertManager()
{
  destroy();
}

ObSSTableInsertManager &ObSSTableInsertManager::get_instance()
{
  static ObSSTableInsertManager instance;
  return instance;
}

int ObSSTableInsertManager::init()
{
  int ret = OB_SUCCESS;
  const int64_t bucket_num = 1000L * 100L; // 10w
  const int64_t memory_limit = 1024L * 1024L * 1024L * 10L; // 10GB
  lib::ObMemAttr attr(OB_SERVER_TENANT_ID, "DInsSstMgr");
  SET_USE_500(attr);
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(allocator_.init(OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                     attr.label_,
                                     OB_SERVER_TENANT_ID,
                                     memory_limit))) {
    LOG_WARN("init alloctor failed", K(ret));
  } else if (OB_FAIL(bucket_lock_.init(bucket_num))) {
    LOG_WARN("init bucket lock failed", K(ret), K(bucket_num));
  } else if (OB_FAIL(table_ctx_map_.create(bucket_num, attr, attr))) {
    LOG_WARN("create context map failed", K(ret));
  } else {
    allocator_.set_attr(attr);
    context_id_generator_ = ObTimeUtility::current_time();
    is_inited_ = true;
  }
  return ret;
}

int64_t ObSSTableInsertManager::alloc_context_id()
{
  return ATOMIC_AAF(&context_id_generator_, 1);
}

int ObSSTableInsertManager::create_table_context(
    const ObSSTableInsertTableParam &param,
    int64_t &context_id)
{
  int ret = OB_SUCCESS;
  ObSSTableInsertTableContext *table_context = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(param));
  } else {
    void *buf = nullptr;
    if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObSSTableInsertTableContext)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory for insert sstable context failed", K(ret), K(param));
    } else {
      context_id = alloc_context_id();
      table_context = new (buf) ObSSTableInsertTableContext();
      const_cast<ObSSTableInsertTableParam &>(param).context_id_ = context_id;
      ObBucketHashWLockGuard guard(bucket_lock_, get_context_id_hash(context_id));
      if (OB_FAIL(table_context->init(param))) {
        LOG_WARN("set build param faild", K(ret), K(param));
      } else if (OB_FAIL(table_ctx_map_.set_refactored(context_id, table_context))) {
        LOG_WARN("set into hash map failed", K(ret), K(param), KP(table_context));
      }
    }
    if (OB_FAIL(ret) && nullptr != table_context) {
      table_context->~ObSSTableInsertTableContext();
      allocator_.free(table_context);
      table_context = nullptr;
    }
  }
  return ret;
}

int ObSSTableInsertManager::update_table_context(
    const int64_t context_id,
    const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectInsertSSTableManager has not been inited", K(ret));
  } else if (OB_UNLIKELY(context_id <= 0 || snapshot_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(context_id), K(snapshot_version));
  } else {
    ObSSTableInsertTableContext *table_context = nullptr;
    ObBucketHashRLockGuard guard(bucket_lock_, get_context_id_hash(context_id));
    if (OB_FAIL(get_context_no_lock(context_id, table_context))) {
      LOG_WARN("get context failed", K(ret));
    } else if (OB_FAIL(table_context->update_context(snapshot_version))) {
      LOG_WARN("update context failed", K(ret));
    }
  }
  return ret;
}

int ObSSTableInsertManager::update_table_tablet_context(
    const int64_t context_id,
    const ObTabletID &tablet_id,
    const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectInsertSSTableManager has not been inited", K(ret));
  } else if (OB_UNLIKELY(context_id <= 0 || !tablet_id.is_valid() || snapshot_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(context_id), K(tablet_id), K(snapshot_version));
  } else {
    ObSSTableInsertTableContext *table_context = nullptr;
    if (OB_FAIL(get_context(context_id, table_context))) {
      LOG_WARN("get context failed", K(ret));
    } else if (OB_FAIL(table_context->update_tablet_context(tablet_id, snapshot_version))) {
      LOG_WARN("update tablet context failed", K(ret));
    }
  }
  return ret;
}

uint64_t ObSSTableInsertManager::get_context_id_hash(const int64_t context_id)
{
  return common::murmurhash(&context_id, sizeof(context_id), 0L);
}

int ObSSTableInsertManager::finish_table_context(const int64_t context_id, const bool need_commit)
{
  int ret = OB_SUCCESS;
  ObSSTableInsertTableContext *table_context = nullptr;
  ObBucketHashWLockGuard guard(bucket_lock_, get_context_id_hash(context_id));
  if (OB_FAIL(get_context_no_lock(context_id, table_context))) {
    LOG_WARN("get context failed", K(ret));
  } else if (OB_FAIL(table_context->finish(need_commit))) {
    LOG_WARN("finish table context failed", K(ret));
  }
  if (nullptr != table_context) { // ignore ret
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(remove_context_no_lock(context_id))) {
      LOG_ERROR("erase factored failed", K(ret), K(tmp_ret), K(context_id));
    }
  }
  return ret;
}

int ObSSTableInsertManager::add_sstable_slice(
    const ObSSTableInsertTabletParam &param,
    const blocksstable::ObMacroDataSeq &start_seq,
    common::ObNewRowIterator &iter,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  ObSSTableInsertTableContext *table_ctx = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectInsertSSTableManager has not been inited", K(ret));
  } else if (OB_FAIL(get_context(param.context_id_, table_ctx))) {
    LOG_WARN("get context failed", K(ret));
  } else if (OB_FAIL(table_ctx->add_sstable_slice(param, start_seq, iter, affected_rows))) {
    LOG_WARN("add sstable slice failed", K(ret));
  }
  return ret;
}

int ObSSTableInsertManager::construct_sstable_slice_writer(
    const ObSSTableInsertTabletParam &param,
    const ObMacroDataSeq &start_seq,
    ObSSTableInsertSliceWriter *&sstable_slice_writer,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObSSTableInsertTableContext *table_ctx = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectInsertSSTableManager has not been inited", K(ret));
  } else if (OB_FAIL(get_context(param.context_id_, table_ctx))) {
    LOG_WARN("get context failed", K(ret));
  } else if (OB_FAIL(table_ctx->construct_sstable_slice_writer(param, start_seq, sstable_slice_writer, allocator))) {
    LOG_WARN("construct sstable slice writer failed", K(ret));
  }
  return ret;
}

int ObSSTableInsertManager::notify_tablet_end(const int64_t context_id, const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObSSTableInsertTableContext *table_ctx = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectInsertSSTableManager has not been inited", K(ret));
  } else if (OB_UNLIKELY(context_id < 0 || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(context_id), K(tablet_id));
  } else if (OB_FAIL(get_context(context_id, table_ctx))) {
    LOG_WARN("get context failed", K(ret));
  } else if (OB_ISNULL(table_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table context is null", K(ret), KP(table_ctx));
  } else if (OB_FAIL(table_ctx->notify_tablet_end(tablet_id))) {
    LOG_WARN("notify tablet failed", K(ret), K(tablet_id));
  }
  return ret;
}

int ObSSTableInsertManager::finish_ready_tablets(const int64_t context_id, const int64_t target_count)
{
  int ret = OB_SUCCESS;
  ObSSTableInsertTableContext *table_ctx = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectInsertSSTableManager has not been inited", K(ret));
  } else if (OB_UNLIKELY(context_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(context_id));
  } else if (OB_FAIL(get_context(context_id, table_ctx))) {
    LOG_WARN("get context failed", K(ret));
  } else if (OB_ISNULL(table_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table context is null", K(ret), KP(table_ctx));
  } else if (OB_FAIL(table_ctx->finish_ready_tablets(target_count))) {
    LOG_WARN("finsh ready tablets failed failed", K(ret), K(target_count));
  }
  return ret;
}

int ObSSTableInsertManager::get_tablet_ids(const int64_t context_id, common::ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  ObSSTableInsertTableContext *table_ctx = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectInsertSSTableManager has not been inited", K(ret));
  } else if (OB_FAIL(get_context(context_id, table_ctx))) {
    LOG_WARN("get context failed", K(ret));
  } else if (OB_ISNULL(table_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table context is null", K(ret), KP(table_ctx));
  } else if (OB_FAIL(table_ctx->get_tablet_ids(tablet_ids))) {
    LOG_WARN("get tablet ids failed", K(ret));
  }
  return ret;
}

int ObSSTableInsertManager::get_tablet_cache_interval(const int64_t context_id,
                                                            const ObTabletID &tablet_id,
                                                            ObTabletCacheInterval &interval)
{
  int ret = OB_SUCCESS;
  ObSSTableInsertTableContext *table_ctx = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectInsertSSTableManager has not been inited", K(ret));
  } else if (OB_FAIL(get_context(context_id, table_ctx))) {
    LOG_WARN("get context failed", K(ret));
  } else if (OB_ISNULL(table_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table context is null", K(ret), KP(table_ctx));
  } else if (OB_FAIL(table_ctx->get_tablet_cache_interval(tablet_id, interval))) {
    LOG_WARN("get tablet cache interval failed", K(ret));
  }
  return ret;
}

int ObSSTableInsertManager::get_context(
    const int64_t context_id,
    ObSSTableInsertTableContext *&ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(context_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(context_id));
  } else {
    ObBucketHashRLockGuard guard(bucket_lock_, get_context_id_hash(context_id));
    if (OB_FAIL(get_context_no_lock(context_id, ctx))) {
      LOG_WARN("get context without lock failed", K(ret), K(context_id));
    }
  }
  return ret;
}

int ObSSTableInsertManager::get_context_no_lock(
    const int64_t context_id,
    ObSSTableInsertTableContext *&ctx)
{
  int ret = OB_SUCCESS;
  ctx = nullptr;
  if (OB_FAIL(table_ctx_map_.get_refactored(context_id, ctx))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("get sstable insert context failed", K(ret), K(context_id));
    } else {
      ret = OB_ENTRY_NOT_EXIST;
    }
  } else if (OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is null", K(ret), K(context_id), KP(ctx));
  }
  return ret;
}

int ObSSTableInsertManager::remove_context_no_lock(const int64_t context_id)
{
  int ret = OB_SUCCESS;
  ObSSTableInsertTableContext *table_context = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(context_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(context_id));
  } else if (OB_FAIL(table_ctx_map_.erase_refactored(context_id, &table_context))) {
    LOG_WARN("erase table context failed", K(ret), K(context_id));
  } else {
    table_context->~ObSSTableInsertTableContext();
    allocator_.free(table_context);
    table_context = nullptr;
  }
  return ret;
}

void ObSSTableInsertManager::destroy()
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> context_id_arr;
  common::ObBucketWLockAllGuard lock_guard(bucket_lock_);
  for(TABLE_CTX_MAP::iterator iter = table_ctx_map_.begin(); iter != table_ctx_map_.end(); ++iter) { // ignore error code.
    if (OB_FAIL(context_id_arr.push_back(iter->first))) {
      LOG_ERROR("push back failed", K(ret));
    }
  }
  for (int64_t i = 0; i < context_id_arr.count(); i++) { // ignore error code.
    const int64_t context_id = context_id_arr.at(i);
    if (OB_FAIL(remove_context_no_lock(context_id))) {
      LOG_ERROR("remove context failed", K(ret), K(context_id));
    }
  }
  table_ctx_map_.destroy();
  allocator_.destroy();
}
