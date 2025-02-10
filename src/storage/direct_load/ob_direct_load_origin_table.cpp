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

#include "storage/direct_load/ob_direct_load_origin_table.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;
using namespace share;
using namespace share::schema;
using namespace transaction;

/**
 * ObDirectLoadOriginTableCreateParam
 */

ObDirectLoadOriginTableCreateParam::ObDirectLoadOriginTableCreateParam()
  : table_id_(OB_INVALID_ID)
{
}

ObDirectLoadOriginTableCreateParam::~ObDirectLoadOriginTableCreateParam()
{
}

bool ObDirectLoadOriginTableCreateParam::is_valid() const
{
  return OB_INVALID_ID != table_id_ && tablet_id_.is_valid() && ls_id_.is_valid();
}

/**
 * ObDirectLoadOriginTableMeta
 */

ObDirectLoadOriginTableMeta::ObDirectLoadOriginTableMeta()
  : table_id_(OB_INVALID_ID)
{
}

ObDirectLoadOriginTableMeta::~ObDirectLoadOriginTableMeta()
{
}

void ObDirectLoadOriginTableMeta::reset()
{
  table_id_ = OB_INVALID_ID;
  tablet_id_.reset();
  ls_id_.reset();
  tx_id_.reset();
  tx_seq_.reset();
}

/**
 * ObDirectLoadOriginTable
 */

ObDirectLoadOriginTable::ObDirectLoadOriginTable()
  : major_sstable_(nullptr), is_inited_(false)
{
  ddl_sstables_.set_tenant_id(MTL_ID());
}

ObDirectLoadOriginTable::~ObDirectLoadOriginTable()
{
}

void ObDirectLoadOriginTable::reset()
{
  is_inited_ = false;
  meta_.reset();
  tablet_handle_.reset();
  table_iter_.reset();
  major_sstable_ = nullptr;
  ddl_sstables_.reset();
}

int ObDirectLoadOriginTable::init(const ObDirectLoadOriginTableCreateParam &param)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadOriginTable init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(param));
  } else {
    const ObTabletID &tablet_id = param.tablet_id_;
    const ObLSID &ls_id = param.ls_id_;
    ObLSService *ls_svr = nullptr;
    ObLSHandle ls_handle;
    ObLS *ls = nullptr;
    if (OB_ISNULL(ls_svr = MTL(ObLSService *))) {
      ret = OB_ERR_SYS;
      LOG_WARN("MTL ObLSService is null", KR(ret), "tenant_id", MTL_ID());
    } else if (OB_FAIL(ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("fail to get ls", KR(ret), K(ls));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected ls is nullptr", KR(ret));
    } else if (OB_FAIL(ls->get_tablet(tablet_id, tablet_handle_))) {
      LOG_WARN("fail to get tablet", KR(ret), K(tablet_id));
    } else if (OB_FAIL(prepare_tables())) {
      LOG_WARN("fail to prepare tables", KR(ret));
    } else {
      meta_.ls_id_ = param.ls_id_;
      meta_.table_id_ = param.table_id_;
      meta_.tablet_id_ = param.tablet_id_;
      meta_.tx_id_ = param.tx_id_;
      meta_.tx_seq_ = param.tx_seq_;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadOriginTable::prepare_tables()
{
  int ret = OB_SUCCESS;
  ObITable *table = nullptr;
  table_iter_.reset();
  if (OB_FAIL(table_iter_.set_tablet_handle(tablet_handle_))) {
    LOG_WARN("Failed to set tablet handle to tablet table iter", K(ret));
  } else if (OB_FAIL(table_iter_.refresh_read_tables_from_tablet(INT64_MAX, false /*allow_not_ready*/, false /*major_sstable_only*/, false /*need_split_src_table*/, false/*need_split_dst_table*/))) {
    LOG_WARN("fail to get read tables", KR(ret), K(tablet_handle_));
  }
  // find major sstable or ddl sstables
  while (OB_SUCC(ret)) {
    if (OB_FAIL(table_iter_.table_iter()->get_next(table))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next table", KR(ret));
      } else {
        ret = OB_SUCCESS;
        break;
      }
    } else if (table->is_major_sstable()) {
      if (nullptr != major_sstable_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected multi major sstable", KR(ret), KPC(major_sstable_), KPC(table));
      } else if (OB_ISNULL(major_sstable_ = dynamic_cast<ObSSTable *>(table))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected not sstable", KR(ret), KPC(table));
      }
    } else if (table->is_ddl_sstable()) {
      ObSSTable *ddl_sstable = nullptr;
      if (OB_ISNULL(ddl_sstable = dynamic_cast<ObSSTable *>(table))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected not sstable", KR(ret), KPC(table));
      } else if (OB_FAIL(ddl_sstables_.push_back(ddl_sstable))) {
        LOG_WARN("fail to push back ddl sstable", KR(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(nullptr == major_sstable_ && ddl_sstables_.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected not found major sstable or ddl sstables", KR(ret), K(table_iter_));
    } else if (OB_UNLIKELY(nullptr != major_sstable_ && !ddl_sstables_.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected both major sstable and ddl sstables exists", KR(ret), K(table_iter_),
               KPC(major_sstable_), K(ddl_sstables_));
    }
  }
  return ret;
}

int ObDirectLoadOriginTable::scan(
    const ObDatumRange &key_range,
    ObIAllocator &allocator,
    ObDirectLoadOriginTableScanner *&row_iter,
    bool skip_read_lob)
{
  int ret = OB_SUCCESS;
  row_iter = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadOriginTable not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!key_range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", KR(ret), K(key_range));
  } else {
    ObDirectLoadOriginTableScanner *row_scanner = nullptr;
    if (OB_ISNULL(row_scanner = OB_NEWx(ObDirectLoadOriginTableScanner, (&allocator)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadOriginTableScanner", KR(ret));
    } else if (OB_FAIL(row_scanner->init(this, skip_read_lob))) {
      LOG_WARN("Fail to init row scanner", KR(ret), K(*this));
    } else if (OB_FAIL(row_scanner->open(key_range))) {
      LOG_WARN("Fail to open row scanner", KR(ret), K(key_range));
    } else {
      row_iter = row_scanner;
    }
    if (OB_FAIL(ret)) {
      if (nullptr != row_scanner) {
        row_scanner->~ObDirectLoadOriginTableScanner();
        allocator.free(row_scanner);
        row_scanner = nullptr;
      }
    }
  }
  return ret;
}

int ObDirectLoadOriginTable::get(const ObDatumRowkey &key,
                                 ObIAllocator &allocator,
                                 ObDirectLoadOriginTableGetter *&row_iter,
                                 bool skip_read_lob)
{
  int ret = OB_SUCCESS;
  row_iter = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadOriginTable not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", KR(ret), K(key));
  } else {
    ObDirectLoadOriginTableGetter *row_getter = nullptr;
    if (OB_ISNULL(row_getter = OB_NEWx(ObDirectLoadOriginTableGetter, (&allocator)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadOriginTableGetter", KR(ret));
    } else if (OB_FAIL(row_getter->init(this, skip_read_lob))) {
      LOG_WARN("Fail to init row scanner", KR(ret), K(*this));
    } else if (OB_FAIL(row_getter->open(key))) {
      LOG_WARN("Fail to open row scanner", KR(ret), K(key));
    } else {
      row_iter = row_getter;
    }
    if (OB_FAIL(ret)) {
      if (nullptr != row_getter) {
        row_getter->~ObDirectLoadOriginTableGetter();
        allocator.free(row_getter);
        row_getter = nullptr;
      }
    }
  }
  return ret;
}

/**
 * ObDirectLoadOriginTableAccessor
 */

ObDirectLoadOriginTableAccessor::ObDirectLoadOriginTableAccessor()
  : allocator_("TLD_OriAccess"),
    stmt_allocator_("TLD_OriAccess"),
    origin_table_(nullptr),
    schema_param_(stmt_allocator_),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
  stmt_allocator_.set_tenant_id(MTL_ID());
  col_ids_.set_tenant_id(MTL_ID());
}

ObDirectLoadOriginTableAccessor::~ObDirectLoadOriginTableAccessor()
{
}

int ObDirectLoadOriginTableAccessor::inner_init(ObDirectLoadOriginTable *origin_table,
                                                bool skip_read_lob)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == origin_table || !origin_table->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", KR(ret), KPC(origin_table));
  } else {
    origin_table_ = origin_table;
    if (OB_FAIL((init_table_access_param()))) {
      LOG_WARN("fail to init query range", KR(ret));
    } else if (OB_FAIL(init_table_access_ctx(skip_read_lob))) {
      LOG_WARN("fail to init table access param", KR(ret));
    } else if (OB_FAIL(init_get_table_param())) {
      LOG_WARN("fail to init get table param", KR(ret));
    } else {
      // set parent params
      column_count_ = col_ids_.count();
    }
  }
  return ret;
}

int ObDirectLoadOriginTableAccessor::init_table_access_param()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  const uint64_t table_id = origin_table_->get_meta().table_id_;
  const ObTabletID &tablet_id = origin_table_->get_meta().tablet_id_;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;
  ObRelativeTable relative_table;
  int64_t store_column_count = 0;
  if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id,
                                                                                  schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", KR(ret), K(tenant_id), K(table_id));
  } else if (OB_FAIL(schema_param_.convert(table_schema))) {
    LOG_WARN("fail to convert schema para", KR(ret));
  } else if (OB_FAIL(relative_table.init(&schema_param_, tablet_id))) {
    LOG_WARN("fail to init relative table", KR(ret));
  } else if (OB_FAIL(table_schema->get_store_column_count(store_column_count))) {
    LOG_WARN("fail to get store column count", KR(ret));
  }
  // schema_param_里面的列顺序是 get_column_ids(column_ids, false/*no_virtual*/), 与存储顺序是一致的, 只需要把虚拟生成列跳过
  for (int64_t i = 0; OB_SUCC(ret) && i < schema_param_.get_columns().count(); ++i) {
    if (schema_param_.get_columns().at(i)->is_virtual_gen_col()) {
      // skip
    } else if (OB_FAIL(col_ids_.push_back(i))) {
      LOG_WARN("fail to push back col id", KR(ret), K(i));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(col_ids_.count() != store_column_count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected col ids", KR(ret), K(schema_param_.get_columns()), K(store_column_count), K(col_ids_));
  }
  if (OB_SUCC(ret)) {
    //TODO(jianming.cjq): check init_dml_access_param
    if (OB_FAIL(table_access_param_.init_dml_access_param(relative_table,
                                                          origin_table_->get_tablet_handle().get_obj()->get_rowkey_read_info(),
                                                          schema_param_,
                                                          &col_ids_))) {
      LOG_WARN("fail to init merge param", KR(ret));
    } else if (GCTX.is_shared_storage_mode()) {
      table_access_param_.iter_param_.table_scan_opt_.io_read_batch_size_ = 1024L * 1024L * 2L; // 2M
      table_access_param_.iter_param_.table_scan_opt_.io_read_gap_size_ = 0;
    }
  }
  return ret;
}

int ObDirectLoadOriginTableAccessor::init_table_access_ctx(bool skip_read_lob)
{
  int ret = OB_SUCCESS;
  const uint64_t table_id = origin_table_->get_meta().table_id_;
  const ObTabletID &tablet_id = origin_table_->get_meta().tablet_id_;
  const ObTransID &tx_id = origin_table_->get_meta().tx_id_;
  const ObTxSEQ &tx_seq = origin_table_->get_meta().tx_seq_;
  const int64_t snapshot_version = ObTimeUtil::current_time_ns();
  ObQueryFlag query_flag(ObQueryFlag::Forward,
                         false /*daily_merge*/,
                         true /*optimize*/,
                         false /*whole_macro_scan*/,
                         false /*full_row*/,
                         false /*index_back*/,
                         false /*query_stat*/); //whole_macro_scan use false，otherwise query range is not overlap with sstable range will report error
  ObVersionRange trans_version_range;
  query_flag.multi_version_minor_merge_ = false;
  if (skip_read_lob) {
    query_flag.skip_read_lob_ = ObQueryFlag::OBSF_MASK_SKIP_READ_LOB;
  }
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = snapshot_version;
  share::SCN snapshot_scn;
  if (OB_FAIL(snapshot_scn.convert_for_tx(snapshot_version))) {
    LOG_WARN("fail to convert scn", KR(ret));
  } else if (OB_FAIL(store_ctx_.init_for_read(origin_table_->get_meta().ls_id_,
                                              tablet_id,
                                              INT64_MAX,
                                              -1,
                                              snapshot_scn))) {
    LOG_WARN("fail to init for read", KR(ret));
  } else if (OB_FAIL(table_access_ctx_.init(query_flag,
                                            store_ctx_,
                                            allocator_,
                                            stmt_allocator_,
                                            trans_version_range))) {
    LOG_WARN("fail to init table access context", KR(ret));
  } else {
    store_ctx_.mvcc_acc_ctx_.snapshot_.tx_id_ = tx_id;
    store_ctx_.mvcc_acc_ctx_.snapshot_.scn_ = tx_seq;
    table_access_ctx_.lob_locator_helper_->update_lob_locator_ctx(table_id, tablet_id.id(), 0);
  }
  return ret;
}

int ObDirectLoadOriginTableAccessor::init_get_table_param()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_table_param_.tablet_iter_.set_tablet_handle(origin_table_->get_tablet_handle()))) {
    LOG_WARN("Failed to set tablet handle to tablet table iter", K(ret));
  } else if (OB_FAIL(get_table_param_.tablet_iter_.refresh_read_tables_from_tablet(INT64_MAX,
                                                                                   false /*allow_not_ready*/,
                                                                                   false /*major_sstable_only*/,
                                                                                   false /*need_split_src_table*/,
                                                                                   false /*need_split_dst_table*/))) {
    LOG_WARN("fail to copy table iter", KR(ret));
  }
  return ret;
}

/**
 * ObDirectLoadOriginTableScanner
 */

int ObDirectLoadOriginTableScanner::init(ObDirectLoadOriginTable *origin_table, bool skip_read_lob)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadOriginTableScanner init twice", KR(ret), KP(this));
  } else if (OB_FAIL(inner_init(origin_table, skip_read_lob))) {
    LOG_WARN("fail to inner init", KR(ret));
  } else if (OB_FAIL(scan_merge_.init(table_access_param_, table_access_ctx_, get_table_param_))) {
    LOG_WARN("fail to init scan merge", KR(ret));
  } else {
    datum_row_.seq_no_ = 0;
    is_inited_ = true;
  }
  return ret;
}

int ObDirectLoadOriginTableScanner::open(const ObDatumRange &query_range)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadOriginTableScanner not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!query_range.is_valid() || !query_range.is_memtable_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", KR(ret), K(query_range));
  } else {
    scan_merge_.reuse();
    if (OB_FAIL(scan_merge_.open(query_range))) {
      LOG_WARN("fail to open scan merge", KR(ret), K(query_range));
    }
  }
  return ret;
}

int ObDirectLoadOriginTableScanner::get_next_row(const ObDirectLoadDatumRow *&result_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadOriginTableScanner not init", KR(ret), KP(this));
  } else {
    ObDatumRow *datum_row = nullptr;
    if (OB_FAIL(scan_merge_.get_next_row(datum_row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("get next row failed", KR(ret));
      }
    } else {
      datum_row_.storage_datums_ = datum_row->storage_datums_;
      datum_row_.count_ = datum_row->count_;
      result_row = &datum_row_;
    }
  }
  return ret;
}

/**
 * ObDirectLoadOriginTableGetter
 */

int ObDirectLoadOriginTableGetter::init(ObDirectLoadOriginTable *origin_table, bool skip_read_lob)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadOriginTableGetter init twice", KR(ret), KP(this));
  } else if (OB_FAIL(inner_init(origin_table, skip_read_lob))) {
    LOG_WARN("fail to inner init", KR(ret));
  } else if (OB_FAIL(
               single_merge_.init(table_access_param_, table_access_ctx_, get_table_param_))) {
    LOG_WARN("fail to init multi merge", KR(ret));
  } else {
    datum_row_.seq_no_ = 0;
    is_inited_ = true;
  }
  return ret;
}

int ObDirectLoadOriginTableGetter::open(const ObDatumRowkey &key)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadOriginTableGetter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", KR(ret), K(key));
  } else {
    single_merge_.reuse();
    if (OB_FAIL(single_merge_.open(key))) {
      LOG_WARN("fail to open multi merge", KR(ret), K(key));
    }
  }
  return ret;
}

int ObDirectLoadOriginTableGetter::get_next_row(const ObDirectLoadDatumRow *&result_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadOriginTableGetter not init", KR(ret), KP(this));
  } else {
    ObDatumRow *datum_row = nullptr;
    if (OB_FAIL(single_merge_.get_next_row(datum_row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("get next row failed", KR(ret));
      }
    } else if (datum_row->row_flag_.is_not_exist()) {
      ret = OB_ITER_END;
    } else {
      datum_row_.storage_datums_ = datum_row->storage_datums_;
      datum_row_.count_ = datum_row->count_;
      result_row = &datum_row_;
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
