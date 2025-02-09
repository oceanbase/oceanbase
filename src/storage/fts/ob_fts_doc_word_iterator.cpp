/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE_FTS

#include "storage/fts/ob_fts_doc_word_iterator.h"
#include "storage/tx_storage/ob_access_service.h"
#include "storage/access/ob_table_scan_iterator.h"

namespace oceanbase
{
namespace storage
{

ObFTDocWordScanIterator::ObFTDocWordScanIterator()
  : allocator_(lib::ObMemAttr(MTL_ID(), "FTDWAlloc")),
    scan_allocator_(lib::ObMemAttr(MTL_ID(), "FTDWScanA")),
    table_param_(allocator_),
    scan_param_(),
    doc_word_iter_(nullptr),
    is_inited_(false)
{
}

ObFTDocWordScanIterator::~ObFTDocWordScanIterator()
{
  reset();
}

int ObFTDocWordScanIterator::init(
    const uint64_t table_id,
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const transaction::ObTxReadSnapshot *snapshot,
    const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init fulltext doc word scan iterator twice", K(ret), K(is_inited_));
  } else if (OB_FAIL(init_scan_param(table_id, ls_id, tablet_id, snapshot, schema_version))) {
    LOG_WARN("fail to init scan param", K(ret), K(table_id), K(ls_id), K(tablet_id), K(snapshot), K(schema_version));
  } else {
    is_inited_ = true;
  }
  if (OB_UNLIKELY(!is_inited_)) {
    reset();
  }
  return ret;
}

void ObFTDocWordScanIterator::reset()
{
  if (OB_NOT_NULL(doc_word_iter_)) {
    MTL(ObAccessService *)->revert_scan_iter(doc_word_iter_);
    doc_word_iter_ = nullptr;
  }
  table_param_.reset();
  allocator_.reset();
  scan_allocator_.reset();
  is_inited_ = false;
}

int ObFTDocWordScanIterator::reuse()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(doc_word_iter_)) {
    ret = OB_ERR_UNDEFINED;
    LOG_WARN("unexpected error, doc word iter is nullptr", K(ret));
  } else if (OB_FAIL(MTL(ObAccessService *)->reuse_scan_iter(false/*switch param*/, doc_word_iter_))) {
    LOG_WARN("fail to reuse storage scan iter", K(ret));
  } else {
    scan_param_.key_ranges_.reuse();
    scan_param_.ss_key_ranges_.reuse();
    scan_param_.mbr_filters_.reuse();
    scan_allocator_.reset_remain_one_page();
  }
  return ret;
}

int ObFTDocWordScanIterator::do_table_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K(is_inited_));
  } else if (OB_ISNULL(doc_word_iter_)) {
    ret = OB_ERR_UNDEFINED;
    LOG_WARN("unexpected error, doc word iter is nullptr", K(ret));
  } else if (OB_FAIL(MTL(ObAccessService *)->table_rescan(scan_param_, doc_word_iter_))) {
    LOG_WARN("fail to table rescan", K(ret));
  }
#ifdef OB_BUILD_PACKAGE
  LOG_TRACE("doc word rescan", K(ret), K(scan_param_));
#else
  LOG_INFO("doc word rescan", K(ret), K(scan_param_));
#endif
  return ret;
}

int ObFTDocWordScanIterator::do_scan(
    const uint64_t table_id,
    const common::ObDocId &doc_id)
{
  int ret = OB_SUCCESS;
  const bool need_rescan = nullptr != doc_word_iter_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(OB_INVALID_ID == table_id || !doc_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_id), K(doc_id));
  } else if (need_rescan) {
    reuse();
  }
  if (FAILEDx(build_key_range(table_id, doc_id, scan_param_.key_ranges_))) {
    LOG_WARN("fail to build key range", K(ret), K(table_id), K(doc_id));
  } else if (need_rescan && OB_FAIL(do_table_rescan())) {
    LOG_WARN("fail to do table rescan", K(ret));
  } else if (!need_rescan && OB_FAIL(do_table_scan())) {
    LOG_WARN("fail to do table scan", K(ret));
  }
  return ret;
}

int ObFTDocWordScanIterator::get_next_row(blocksstable::ObDatumRow *&datum_row)
{
  int ret = OB_SUCCESS;
  ObTableScanIterator *tsc_iter = nullptr;
  if (OB_ISNULL(doc_word_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("doc word iter is nullptr", K(ret), KP(doc_word_iter_));
  } else if (FALSE_IT(tsc_iter = static_cast<ObTableScanIterator *>(doc_word_iter_))) {
  } else if (OB_FAIL(tsc_iter->get_next_row(datum_row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next row", K(ret), KPC(tsc_iter));
    }
  }
  return ret;
}

int ObFTDocWordScanIterator::init_scan_param(
    const uint64_t table_id,
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const transaction::ObTxReadSnapshot *snapshot,
    const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  ObQueryFlag query_flag(ObQueryFlag::Forward, // scan_order
                         false, // daily_merge
                         false, // optimize
                         false, // sys scan
                         false, // full_row
                         false, // index_back
                         false, // query_stat
                         ObQueryFlag::MysqlMode, // sql_mode
                         false // read_latest
                        );
  if (OB_UNLIKELY(OB_INVALID_ID == table_id
              || !ls_id.is_valid()
              || !tablet_id.is_valid()
              || nullptr == snapshot
              || schema_version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(table_id), K(ls_id), K(tablet_id), KPC(snapshot), K(schema_version));
  } else if (OB_FAIL(build_table_param(table_id, table_param_, scan_param_.column_ids_))) {
    LOG_WARN("fail to build table param", K(ret), K(table_id));
  } else {
    scan_param_.ls_id_ = ls_id;
    scan_param_.tablet_id_ = tablet_id;
    scan_param_.schema_version_ = schema_version;
    scan_param_.is_get_ = false;
    scan_param_.scan_flag_.flag_ = query_flag.flag_;
    scan_param_.key_ranges_.set_attr(ObMemAttr(MTL_ID(), "ScanParamKR"));
    scan_param_.ss_key_ranges_.set_attr(ObMemAttr(MTL_ID(), "ScanParamSSKR"));
    scan_param_.index_id_ = 0;
    scan_param_.timeout_ = THIS_WORKER.get_timeout_ts();
    scan_param_.reserved_cell_count_ = scan_param_.column_ids_.count();
    scan_param_.limit_param_.limit_ = -1;
    scan_param_.limit_param_.offset_ = 0;
    scan_param_.sql_mode_ = SMO_DEFAULT;
    scan_param_.allocator_ = &allocator_;
    scan_param_.for_update_ = false;
    scan_param_.for_update_wait_timeout_ = scan_param_.timeout_;
    scan_param_.scan_allocator_ = &scan_allocator_;
    scan_param_.frozen_version_ = -1;
    scan_param_.force_refresh_lc_ = false;
    scan_param_.output_exprs_ = nullptr;
    scan_param_.aggregate_exprs_ = nullptr;
    scan_param_.op_ = nullptr;
    scan_param_.row2exprs_projector_ = nullptr;
    scan_param_.need_scn_ = false;
    scan_param_.pd_storage_flag_ = false;
    scan_param_.table_param_ = &table_param_;
    scan_param_.key_ranges_.reset();
    scan_param_.ss_key_ranges_.reset();
    if (OB_FAIL(scan_param_.snapshot_.assign(*snapshot))) {
      LOG_WARN("fail to assign snapshot", K(ret), KPC(snapshot));
    } else {
      scan_param_.tx_id_ = scan_param_.snapshot_.core_.tx_id_;
    }
  }
  return ret;
}

int ObFTDocWordScanIterator::build_table_param(
    const uint64_t table_id,
    share::schema::ObTableParam &table_param,
    common::ObIArray<uint64_t> &column_ids)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  share::schema::ObSchemaGetterGuard schema_guard;
  const share::schema::ObTableSchema *table_schema = nullptr;
  column_ids.reset();
  if (OB_UNLIKELY(OB_INVALID_ID == table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(table_id));
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("fail to get table scheam", K(ret), K(tenant_id), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, table scheam is nullptr", K(ret), K(tenant_id), K(table_id));
  } else if (OB_FAIL(table_schema->get_column_ids(column_ids))) {
    LOG_WARN("fail to get all column ids", K(ret), KPC(table_schema));
  } else if (OB_UNLIKELY(4 != column_ids.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, column count isn't 4 for fts doc word", K(ret), K(column_ids));
  } else if (OB_FAIL(table_param.convert(*table_schema, column_ids, sql::ObStoragePushdownFlag()))) {
    LOG_WARN("fail to convert table param", K(ret), K(column_ids), KPC(table_schema));
  }
  return ret;
}

int ObFTDocWordScanIterator::build_key_range(
    const uint64_t table_id,
    const common::ObDocId &doc_id,
    common::ObIArray<ObNewRange> &rowkey_range)
{
  int ret = OB_SUCCESS;
  static int64_t ROWKEY_COLUMN_COUNT = 2;
  common::ObNewRange range;
  void *buf = nullptr;
  if (OB_UNLIKELY(OB_INVALID_ID == table_id || !doc_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(doc_id));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(common::ObObj) * ROWKEY_COLUMN_COUNT * 2))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate buffer for rowkey", K(ret));
  } else {
    common::ObObj *objs= new common::ObObj[ROWKEY_COLUMN_COUNT * 2];
    objs[0].set_varbinary(doc_id.get_string());
    objs[1] = common::ObObj::make_min_obj();
    objs[2].set_varbinary(doc_id.get_string());
    objs[3] = common::ObObj::make_max_obj();
    range.table_id_ = table_id;
    range.start_key_ = ObRowkey(objs, ROWKEY_COLUMN_COUNT);
    range.end_key_ = ObRowkey(objs + ROWKEY_COLUMN_COUNT, ROWKEY_COLUMN_COUNT);
    range.border_flag_.set_inclusive_start();
    range.border_flag_.set_inclusive_end();
    if (OB_FAIL(rowkey_range.push_back(range))) {
      LOG_WARN("fail to push back key range", K(ret), K(range));
    }
  }
  return ret;
}

int ObFTDocWordScanIterator::do_table_scan()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K(is_inited_));
  } else if (OB_FAIL(MTL(ObAccessService *)->table_scan(scan_param_, doc_word_iter_))) {
    LOG_WARN("fail to do table scan", K(ret), K(scan_param_));
  }
#ifdef OB_BUILD_PACKAGE
  LOG_TRACE("doc word scan", K(ret), K(scan_param_));
#else
  LOG_INFO("doc word scan", K(ret), K(scan_param_));
#endif
  return ret;
}

} // end namespace storage
} // end namespace oceanbase
