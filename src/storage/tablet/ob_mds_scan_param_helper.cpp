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

#include "storage/tablet/ob_mds_scan_param_helper.h"

#include "lib/ob_errno.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/string/ob_string.h"
#include "common/ob_common_types.h"
#include "common/ob_range.h"
#include "common/ob_tablet_id.h"
#include "share/ob_ls_id.h"
#include "share/scn.h"
#include "share/rc/ob_tenant_base.h"
#include "sql/engine/basic/ob_pushdown_filter.h"
#include "storage/access/ob_dml_param.h"
#include "storage/tablet/ob_mds_schema_helper.h"
#include "storage/tablet/ob_tablet_common.h"
#include "storage/multi_data_source/mds_unit.h"
#include "storage/compaction/ob_medium_compaction_info.h"
#include "storage/tx/ob_trans_define_v4.h"

#define USING_LOG_PREFIX STORAGE

using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase
{
namespace storage
{
int ObMdsScanParamHelper::build_scan_param(
    common::ObIAllocator &allocator,
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const uint64_t table_id,
    const uint8_t mds_unit_id,
    const common::ObString &udf_key,
    const bool is_get,
    const int64_t timeout,
    const share::SCN &snapshot,
    ObTableScanParam &scan_param)
{
  int ret = OB_SUCCESS;
  const share::schema::ObTableSchema *table_schema = ObMdsSchemaHelper::get_instance().get_table_schema();

  scan_param.tenant_id_ = MTL_ID();
  scan_param.ls_id_ = ls_id;
  scan_param.tablet_id_ = tablet_id;
  scan_param.is_get_ = is_get;
  scan_param.is_for_foreign_check_ = false;
  scan_param.timeout_ = timeout;
  const bool is_mds_query = true;
  scan_param.scan_flag_.is_mds_query_ = is_mds_query & common::ObQueryFlag::OBSF_MASK_IS_MDS_QUERY;
  scan_param.allocator_ = &allocator;
  scan_param.scan_allocator_ = &allocator;
  scan_param.frozen_version_ = snapshot.get_val_for_tx();
  scan_param.need_scn_ = false;
  scan_param.for_update_ = false;
  scan_param.is_mds_query_ = true;
  scan_param.fb_snapshot_ = snapshot;
  scan_param.pd_storage_flag_ = false;
  scan_param.index_id_ = ObMdsSchemaHelper::MDS_TABLE_ID;
  scan_param.schema_version_ = ObMdsSchemaHelper::MDS_SCHEMA_VERSION;

  if (OB_FAIL(ret)) {
  } else {
    transaction::ObTxSnapshot tx_snapshot;
    tx_snapshot.version_ = snapshot;
    scan_param.snapshot_.init_ls_read(ls_id, tx_snapshot);
  }

  if (OB_FAIL(ret)) {
  } else {
    for (int64_t i = 0; i < ObMdsSchemaHelper::MDS_ROW_COLUMN_CNT; ++i) {
      if (OB_FAIL(scan_param.column_ids_.push_back(common::OB_APP_MIN_COLUMN_ID + i))) {
        LOG_WARN("fail to push back column id", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret)) {
      scan_param.reserved_cell_count_ = scan_param.column_ids_.count();
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret), KP(table_schema));
  } else {
    share::schema::ObTableParam *table_param = nullptr;
    if (OB_FAIL(build_table_param(
        *scan_param.allocator_,
        *table_schema,
        scan_param.column_ids_,
        scan_param.pd_storage_flag_,
        table_param))) {
      LOG_WARN("fail to build table param", K(ret));
    } else {
      scan_param.table_param_ = table_param;
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    common::ObNewRange key_range;
    if (is_get && OB_FAIL(build_key_range(
        allocator,
        table_id,
        mds_unit_id,
        udf_key,
        key_range))) {
      LOG_WARN("fail to build key range", K(ret));
    } else if (!is_get && OB_FAIL(build_key_range(
        allocator,
        table_id,
        mds_unit_id,
        key_range))) {
      LOG_WARN("fail to build key range", K(ret));
    } else if (OB_FAIL(scan_param.key_ranges_.push_back(key_range))) {
      LOG_WARN("fail to push back key range", K(ret));
    }
  }

  return ret;
}

int ObMdsScanParamHelper::build_medium_info_scan_param(
    common::ObIAllocator &allocator,
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    ObTableScanParam &scan_param)
{
  int ret = OB_SUCCESS;
  constexpr uint8_t mds_unit_id = mds::TupleTypeIdx<mds::NormalMdsTable, mds::MdsUnit<compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo>>::value;
  const int64_t abs_timeout = ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US + ObClockGenerator::getClock();

  if (OB_FAIL(build_scan_param(
      allocator,
      ls_id,
      tablet_id,
      ObMdsSchemaHelper::MDS_TABLE_ID,
      mds_unit_id,
      common::ObString()/*udf_key*/,
      false/*is_get*/,
      abs_timeout,
      share::SCN::max_scn()/*read_snapshot*/,
      scan_param))) {
    LOG_WARN("fail to build scan param", K(ret));
  }

  return ret;
}

int ObMdsScanParamHelper::build_key_range(
    common::ObIAllocator &allocator,
    const uint64_t table_id,
    const uint8_t mds_unit_id,
    const common::ObString &udf_key,
    common::ObNewRange &key_range)
{
  int ret = OB_SUCCESS;
  void *buf = allocator.alloc(sizeof(ObObj) * MDS_SSTABLE_ROWKEY_CNT);
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else {
    ObObj *obj = new (buf) ObObj[MDS_SSTABLE_ROWKEY_CNT]();
    obj[0].set_tinyint(mds_unit_id);
    obj[1].set_string(ObObjType::ObVarcharType, udf_key);

    const ObRowkey rowkey(obj, MDS_SSTABLE_ROWKEY_CNT);
    if (OB_FAIL(key_range.build_range(table_id, rowkey))) {
      LOG_WARN("fail to build key range", K(ret), K(table_id), K(rowkey));
    }

    if (OB_FAIL(ret)) {
      for (int64_t i = 0; i < MDS_SSTABLE_ROWKEY_CNT; ++i) {
        obj[i].~ObObj();
      }
      allocator.free(buf);
    }
  }

  return ret;
}

int ObMdsScanParamHelper::build_key_range(
    common::ObIAllocator &allocator,
    const uint64_t table_id,
    const uint8_t mds_unit_id,
    common::ObNewRange &key_range)
{
  int ret = OB_SUCCESS;
  char *buf = static_cast<char *>(allocator.alloc(sizeof(ObObj) * MDS_SSTABLE_ROWKEY_CNT  * 2));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else {
    ObObj *start_obj = new (buf) ObObj[MDS_SSTABLE_ROWKEY_CNT]();
    ObObj *end_obj = new (buf + sizeof(ObObj) * MDS_SSTABLE_ROWKEY_CNT) ObObj();
    start_obj[0].set_tinyint(mds_unit_id);
    start_obj[1].set_min_value();
    end_obj[0].set_tinyint(mds_unit_id);
    end_obj[1].set_max_value();

    key_range.table_id_ = table_id;
    key_range.start_key_.assign(start_obj, MDS_SSTABLE_ROWKEY_CNT);
    key_range.end_key_.assign(end_obj, MDS_SSTABLE_ROWKEY_CNT);
    key_range.border_flag_.unset_inclusive_start();
    key_range.border_flag_.unset_inclusive_end();
    key_range.flag_ = 0;
  }

  return ret;
}

int ObMdsScanParamHelper::build_table_param(
    common::ObIAllocator &allocator,
    const share::schema::ObTableSchema &table_schema,
    const common::ObIArray<uint64_t> &column_ids,
    const sql::ObStoragePushdownFlag &pd_pushdown_flag,
    share::schema::ObTableParam *&table_param)
{
  int ret = OB_SUCCESS;
  void *buf = allocator.alloc(sizeof(share::schema::ObTableParam));

  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret), "size", sizeof(share::schema::ObTableParam));
  } else {
    table_param = new (buf) ObTableParam(allocator);
    if (OB_FAIL(table_param->convert(table_schema, column_ids, pd_pushdown_flag))) {
      LOG_WARN("fail to convert", K(ret));
    }

    if (OB_FAIL(ret)) {
      table_param->~ObTableParam();
      allocator.free(buf);
      buf = nullptr;
    }
  }

  return ret;
}
} // namespace storage
} // namespace oceanbase