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

#include "storage/reorganization_info_table/ob_tablet_reorg_info_table_iterator.h"
#include "share/ob_dml_sql_splicer.h" // ObDMLSqlSplicer
#include "lib/mysqlclient/ob_mysql_proxy.h" // ObISqlClient, SMART_VAR
#include "lib/mysqlclient/ob_mysql_transaction.h" // ObMySQLTransaction
#include "share/location_cache/ob_location_struct.h" // ObTabletLSCache
#include "share/schema/ob_schema_utils.h" // ObSchemaUtils
#include "observer/ob_server_struct.h" // GCTX
#include "storage/reorganization_info_table/ob_tablet_reorg_info_table_schema_helper.h"
#include "storage/blocksstable/ob_datum_row_utils.h"
#include "observer/ob_inner_sql_connection.h"
#include "storage/access/ob_table_scan_iterator.h"
#include "storage/ob_inner_tablet_access_service.h"

#define USING_LOG_PREFIX STORAGE

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

namespace oceanbase
{
namespace storage
{

ObTabletReorgInfoTableIterator::ObTabletReorgInfoTableIterator()
  : is_inited_(false),
    allocator_(),
    tenant_id_(OB_INVALID_ID),
    ls_id_(),
    ctx_(nullptr),
    scan_iter_(nullptr)
{
}

ObTabletReorgInfoTableIterator::~ObTabletReorgInfoTableIterator()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(scan_iter_)) {
    free_scan_iter_(scan_iter_);
    scan_iter_ = nullptr;
  }

  if (OB_NOT_NULL(ctx_)) {
    ctx_->~ObInnerTableReadCtx();
    ctx_ = nullptr;
  }
  allocator_.reset();
}

int ObTabletReorgInfoTableIterator::init(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const common::ObNewRange &new_range,
    const bool is_get,
    const int64_t abs_timeout_ts)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet reorg info table iterator init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id || !ls_id.is_valid() || !new_range.is_valid() || abs_timeout_ts <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet reorg info table iterator init get invalid argument", K(ret), K(tenant_id), K(ls_id),
        K(new_range), K(abs_timeout_ts));
  } else if (OB_FAIL(prepare_scan_iter_(tenant_id, ls_id, new_range, is_get, abs_timeout_ts))) {
    LOG_WARN("failed to prepare scan iter", K(ret), K(tenant_id), K(ls_id), K(new_range));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObTabletReorgInfoTableIterator::prepare_scan_iter_(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const common::ObNewRange &new_range,
    const bool is_get,
    const int64_t abs_timeout_ts)
{
  int ret = OB_SUCCESS;
  share::SCN snapshot;
  const bool is_for_sslog = is_tenant_sslog_ls(tenant_id, ls_id);
  const uint64_t target_tenant_id = !is_for_sslog
                                    ? tenant_id : transaction::get_sslog_gts_tenant_id(tenant_id);
  if (OB_FAIL(get_local_read_scn_(target_tenant_id, abs_timeout_ts, snapshot))) {
    LOG_WARN("failed to get local read scn", K(ret), K(tenant_id), K(target_tenant_id), K(ls_id));
  } else if (OB_FAIL(build_read_ctx_(ls_id, abs_timeout_ts, is_get, new_range, snapshot))) {
    LOG_WARN("failed to build read ctx", K(ret), K(ls_id));
  } else if (OB_FAIL(build_scan_iter_())) {
    LOG_WARN("failed to build scan iter", K(ret), K(tenant_id), K(ls_id));
  }
  return ret;
}

int ObTabletReorgInfoTableIterator::get_local_read_scn_(
    const uint64_t tenant_id,
    const int64_t abs_timeout_ts,
    share::SCN &read_scn)
{
  int ret = OB_SUCCESS;
  ret = OB_EAGAIN;
  const transaction::MonotonicTs stc = transaction::MonotonicTs::current_time();
  transaction::MonotonicTs unused_ts(0);
  int64_t start_time = 0;
  while (OB_EAGAIN == ret) {
    start_time = ObTimeUtility::fast_current_time();
    if (start_time > abs_timeout_ts) {
      ret = OB_TIMEOUT;
      LOG_WARN("get gts timeout", KR(ret), K(start_time), K(abs_timeout_ts));
    } else if (OB_FAIL(OB_TS_MGR.get_gts(tenant_id, stc, NULL, read_scn, unused_ts))) {
      if (OB_EAGAIN != ret) {
        LOG_WARN("failed to get gts", KR(ret), K(tenant_id));
      } else {
        // waiting 10ms
        ob_usleep(10L * 1000L);
      }
    }
  }
  return ret;
}

int ObTabletReorgInfoTableIterator::build_read_ctx_(
    const share::ObLSID &ls_id,
    const int64_t abs_timeout_us,
    const bool is_get,
    const common::ObNewRange &new_range,
    const share::SCN &snapshot)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObInnerTableReadCtx)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to alloc mem", K(ret));
  } else {
    ctx_ = new(buf) ObInnerTableReadCtx();
    ctx_->reset();
    ctx_->abs_timeout_us_ = abs_timeout_us;
    ctx_->is_get_ = is_get;
    ctx_->key_range_ = &new_range;
    ctx_->ls_id_ = ls_id;
    ctx_->tablet_id_ = LS_REORG_INFO_TABLET;
    ctx_->snapshot_ = snapshot;
    if (!ctx_->is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("read ctx is invalid, unexpected", K(ret), KPC(ctx_));
    }
  }
  return ret;
}

int ObTabletReorgInfoTableIterator::build_scan_iter_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObAccessService *access_service = nullptr;
  ObInnerTabletAccessService *inner_tablet_as = nullptr;
  common::ObNewRowIterator *scan_iter = nullptr;

  if (!ctx_->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("read ctx is invalid, unexpected", K(ret), KPC(ctx_));
  } else if (OB_ISNULL(access_service = MTL(storage::ObAccessService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("access service should not be NULL", K(ret), KP(access_service));
  } else if (OB_ISNULL(inner_tablet_as = MTL(storage::ObInnerTabletAccessService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner tablet access service should not be NULL", K(ret), KP(inner_tablet_as));
  } else if (OB_FAIL(inner_tablet_as->read_rows(*ctx_, scan_iter))) {
    LOG_WARN("failed to read rows", K(ret), KPC(ctx_));
  } else if (ObNewRowIterator::ObTableScanIterator != scan_iter->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new row iterator type is unexpected", K(ret), KPC(scan_iter));
  } else {
    scan_iter_ = static_cast<ObTableScanIterator *>(scan_iter);
    scan_iter = nullptr;
  }

  if (OB_NOT_NULL(scan_iter)) {
    free_scan_iter_(scan_iter);
  }
  return ret;
}

int ObTabletReorgInfoTableIterator::inner_get_next_row_(
    ObTabletReorgInfoData &reorg_info_data,
    share::SCN &trans_scn,
    int64_t &sql_no)
{
  int ret = OB_SUCCESS;
  blocksstable::ObDatumRow *datum_row = nullptr;
  reorg_info_data.reset();
  trans_scn.reset();
  sql_no = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet reorg info table iterator do not init", K(ret));
  } else if (OB_FAIL(scan_iter_->get_next_row(datum_row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next row", K(ret));
    }
  } else if (OB_FAIL(reorg_info_data.row_2_data(datum_row, trans_scn, sql_no))) {
    LOG_WARN("failed to convert row to reorg info table data", K(ret), KPC(datum_row));
  }
  return ret;
}

int ObTabletReorgInfoTableIterator::get_next_row(
    ObTabletReorgInfoData &reorg_info_data,
    share::SCN &trans_scn)
{
  int ret = OB_SUCCESS;
  reorg_info_data.reset();
  int64_t sql_no = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet reorg info table iterator do not init", K(ret));
  } else if (OB_FAIL(inner_get_next_row_(reorg_info_data, trans_scn, sql_no))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next row", K(ret));
    }
  }
  return ret;
}

int ObTabletReorgInfoTableIterator::get_next_row(
    ObTabletReorgInfoData &reorg_info_data)
{
  int ret = OB_SUCCESS;
  reorg_info_data.reset();
  share::SCN unused_trans_scn;
  int64_t unused_sql_no = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet reorg info table iterator do not init", K(ret));
  } else if (OB_FAIL(inner_get_next_row_(reorg_info_data, unused_trans_scn, unused_sql_no))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next row", K(ret));
    }
  }
  return ret;
}

void ObTabletReorgInfoTableIterator::free_scan_iter_(ObNewRowIterator *iter)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(iter)) {
    ObAccessService *access_service = nullptr;
    if (OB_ISNULL(access_service = MTL(storage::ObAccessService *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("access service should not be NULL", K(ret), KP(access_service));
    } else if (OB_FAIL(access_service->revert_scan_iter(iter))) {
      LOG_WARN("failed to revert storage scan iter", K(ret));
    }
  }
}


}
}
