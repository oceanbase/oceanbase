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
#include "storage/tablet/ob_tablet.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/ob_relative_table.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;
using namespace share;
using namespace share::schema;

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

/**
 * ObDirectLoadOriginTable
 */

ObDirectLoadOriginTable::ObDirectLoadOriginTable()
  : major_sstable_(nullptr), is_inited_(false)
{
}

ObDirectLoadOriginTable::~ObDirectLoadOriginTable()
{
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
  } else if (OB_FAIL(table_iter_.refresh_read_tables_from_tablet(INT64_MAX, false /*allow_not_ready*/))) {
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

int ObDirectLoadOriginTable::scan(const ObDatumRange &key_range,
                                  ObIAllocator &allocator, ObIStoreRowIterator *&row_iter)
{
  int ret = OB_SUCCESS;
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
    } else if (OB_FAIL(row_scanner->init(this, key_range))) {
      LOG_WARN("Fail to open row scanner", KR(ret), K(key_range), K(*this));
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

/**
 * ObDirectLoadOriginTableScanner
 */

ObDirectLoadOriginTableScanner::ObDirectLoadOriginTableScanner()
  : allocator_("TLD_OriSSTScan"), origin_table_(nullptr), schema_param_(allocator_), is_inited_(false)
{
}

ObDirectLoadOriginTableScanner::~ObDirectLoadOriginTableScanner()
{
}

int ObDirectLoadOriginTableScanner::init(ObDirectLoadOriginTable *origin_table,
                                         const ObDatumRange &query_range)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadOriginIterator init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == origin_table || !origin_table->is_valid() ||
                         !query_range.is_valid() || !query_range.is_memtable_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", KR(ret), KPC(origin_table), K(query_range));
  } else {
    origin_table_ = origin_table;
    allocator_.set_tenant_id(MTL_ID());
    if (OB_FAIL((init_table_access_param()))) {
      LOG_WARN("fail to init query range", KR(ret));
    } else if (OB_FAIL(init_table_access_ctx())) {
      LOG_WARN("fail to init table access param", KR(ret));
    } else if (OB_FAIL(init_get_table_param())) {
      LOG_WARN("fail to init get table param", KR(ret));
    } else if (OB_FAIL(
                 scan_merge_.init(table_access_param_, table_access_ctx_, get_table_param_))) {
      LOG_WARN("fail to init multi merge", KR(ret));
    } else if (OB_FAIL(scan_merge_.open(query_range))) {
      LOG_WARN("fail to open multi merge", KR(ret), K(query_range));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadOriginTableScanner::init_table_access_param()
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
  for (int64_t i = 0; OB_SUCC(ret) && i < store_column_count; ++i) {
    if (OB_FAIL(col_ids_.push_back(i))) {
      LOG_WARN("fail to push back col id", KR(ret), K(i));
    }
  }
  if (OB_SUCC(ret)) {
    //TODO(jianming.cjq): check init_dml_access_param
    if (OB_FAIL(table_access_param_.init_dml_access_param(relative_table,
                                                          origin_table_->get_tablet_handle().get_obj()->get_rowkey_read_info(),
                                                          schema_param_,
                                                          &col_ids_))) {
      LOG_WARN("fail to init merge param", KR(ret));
    }
  }
  return ret;
}

int ObDirectLoadOriginTableScanner::init_table_access_ctx()
{
  int ret = OB_SUCCESS;
  const uint64_t table_id = origin_table_->get_meta().table_id_;
  const ObTabletID &tablet_id = origin_table_->get_meta().tablet_id_;
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
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = snapshot_version;
  share::SCN snapshot_scn;
  if (OB_FAIL(snapshot_scn.convert_for_tx(snapshot_version))) {
    LOG_WARN("fail to convert scn", KR(ret));
  } else if (OB_FAIL(store_ctx_.init_for_read(origin_table_->get_meta().ls_id_, INT64_MAX, -1,
                                       snapshot_scn))) {
    LOG_WARN("fail to init for read", KR(ret));
  } else if (OB_FAIL(table_access_ctx_.init(query_flag, store_ctx_, allocator_, allocator_,
                                            trans_version_range))) {
    LOG_WARN("fail to init table access context", KR(ret));
  } else {
    table_access_ctx_.io_callback_ = &io_callback_;
    table_access_ctx_.lob_locator_helper_->update_lob_locator_ctx(table_id, tablet_id.id(), 0);
  }
  return ret;
}

int ObDirectLoadOriginTableScanner::init_get_table_param()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_table_param_.tablet_iter_.set_tablet_handle(origin_table_->get_tablet_handle()))) {
    LOG_WARN("Failed to set tablet handle to tablet table iter", K(ret));
  } else if (OB_FAIL(get_table_param_.tablet_iter_.refresh_read_tables_from_tablet(INT64_MAX, false /*allow_not_ready*/))) {
    LOG_WARN("fail to copy table iter", KR(ret));
  }
  return ret;
}

int ObDirectLoadOriginTableScanner::get_next_row(const ObDatumRow *&datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadOriginTableScanner not init", KR(ret), KP(this));
  } else {
    ObDatumRow *result_row = nullptr;
    if (OB_FAIL(scan_merge_.get_next_row(result_row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("get next row failed", KR(ret));
      }
    } else {
      datum_row = result_row;
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
