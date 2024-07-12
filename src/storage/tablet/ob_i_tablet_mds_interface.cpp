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

#include "storage/tablet/ob_i_tablet_mds_interface.h"

#include "lib/oblog/ob_log.h"
#include "storage/ls/ob_ls.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/tablet/ob_mds_row_iterator.h"
#include "storage/tablet/ob_mds_scan_param_helper.h"
#include "storage/tablet/ob_mds_schema_helper.h"
#include "storage/tablet/ob_tablet_medium_info_mds_node_filter.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
namespace storage
{
int ObITabletMdsInterface::get_tablet_handle_and_base_ptr(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    ObTabletHandle &tablet_handle,
    ObITabletMdsInterface *&base_ptr)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = MTL(ObLSService*);
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  ObTablet *tablet = nullptr;

  if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::TABLET_MOD))) {
    MDS_LOG(WARN, "fail to get ls", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG(WARN, "ls is null", K(ret), KP(ls), K(ls_id));
  } else if (OB_FAIL(ls->get_tablet(tablet_id, tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    MDS_LOG(WARN, "fail to get tablet", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG(WARN, "tablet is null", K(ret), K(ls_id), K(tablet_id), K(tablet_handle));
  } else {
    base_ptr = static_cast<ObITabletMdsInterface*>(tablet);
  }

  return ret;
}

int ObITabletMdsInterface::get_tablet_status(
    const share::SCN &snapshot,
    ObTabletCreateDeleteMdsUserData &data,
    const int64_t timeout) const
{
  #define PRINT_WRAPPER KR(ret), K(snapshot), K(data), K(timeout), K(*this)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!check_is_inited_())) {
    ret = OB_NOT_INIT;
    MDS_LOG_GET(WARN, "not inited");
  } else if (OB_UNLIKELY(!snapshot.is_max())) {
    ret = OB_NOT_SUPPORTED;
    MDS_LOG_GET(WARN, "only support read latest data currently");
  } else if (CLICK_FAIL((get_snapshot<mds::DummyKey, ObTabletCreateDeleteMdsUserData>(
      mds::DummyKey(),
      ReadTabletStatusOp(data),
      snapshot,
      timeout)))) {
    if (OB_EMPTY_RESULT != ret) {
      MDS_LOG(WARN, "fail to get snapshot", K(ret));
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ObITabletMdsInterface::get_latest_tablet_status(ObTabletCreateDeleteMdsUserData &data, bool &is_committed) const
{
  #define PRINT_WRAPPER KR(ret), K(data)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!check_is_inited_())) {
    ret = OB_NOT_INIT;
    MDS_LOG_GET(WARN, "not inited");
  } else if (CLICK_FAIL((get_latest<ObTabletCreateDeleteMdsUserData>(ReadTabletStatusOp(data), is_committed)))) {
    if (OB_EMPTY_RESULT != ret) {
      MDS_LOG(WARN, "fail to get latest", K(ret));
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ObITabletMdsInterface::get_latest_ddl_data(ObTabletBindingMdsUserData &data, bool &is_committed) const
{
  #define PRINT_WRAPPER KR(ret), K(data)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!check_is_inited_())) {
    ret = OB_NOT_INIT;
    MDS_LOG_GET(WARN, "not inited");
  } else {
    const ObTabletMeta &tablet_meta = get_tablet_meta_();
    const bool has_transfer_table = tablet_meta.has_transfer_table();
    ObITabletMdsInterface *src = nullptr;
    ObTabletHandle src_tablet_handle;
    if (has_transfer_table) {
      const share::ObLSID &src_ls_id = tablet_meta.transfer_info_.ls_id_;
      const common::ObTabletID &tablet_id = tablet_meta.tablet_id_;
      if (CLICK_FAIL(get_tablet_handle_and_base_ptr(src_ls_id, tablet_id, src_tablet_handle, src))) {
        MDS_LOG(WARN, "fail to get src tablet handle", K(ret), K(src_ls_id), K(tablet_id));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (CLICK_FAIL((cross_ls_get_latest<ObTabletBindingMdsUserData>(src, ReadBindingInfoOp(data), is_committed)))) {
      if (OB_EMPTY_RESULT != ret) {
        MDS_LOG_GET(WARN, "fail to cross ls get latest", K(lbt()));
      }
    } else if (!data.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      MDS_LOG_GET(WARN, "invalid user data", K(lbt()));
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ObITabletMdsInterface::get_ddl_data(
    const share::SCN &snapshot,
    ObTabletBindingMdsUserData &data,
    const int64_t timeout) const
{
  #define PRINT_WRAPPER KR(ret), K(data), K(snapshot), K(timeout)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!check_is_inited_())) {
    ret = OB_NOT_INIT;
    MDS_LOG_GET(WARN, "not inited");
  } else if (OB_UNLIKELY(!snapshot.is_max())) {
    ret = OB_NOT_SUPPORTED;
    MDS_LOG_GET(WARN, "only support read latest data currently");
  } else {
    const ObTabletMeta &tablet_meta = get_tablet_meta_();
    const bool has_transfer_table = tablet_meta.has_transfer_table();
    ObITabletMdsInterface *src = nullptr;
    ObTabletHandle src_tablet_handle;
    if (has_transfer_table) {
      const share::ObLSID &src_ls_id = tablet_meta.transfer_info_.ls_id_;
      const common::ObTabletID &tablet_id = tablet_meta.tablet_id_;
      if (CLICK_FAIL(get_tablet_handle_and_base_ptr(src_ls_id, tablet_id, src_tablet_handle, src))) {
        MDS_LOG(WARN, "fail to get src tablet handle", K(ret), K(src_ls_id), K(tablet_id));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (CLICK_FAIL((cross_ls_get_snapshot<mds::DummyKey, ObTabletBindingMdsUserData>(src, mds::DummyKey(),
        ReadBindingInfoOp(data), snapshot, timeout)))) {
      if (OB_EMPTY_RESULT != ret) {
        MDS_LOG_GET(WARN, "fail to cross ls get snapshot", K(lbt()));
      } else {
        data.set_default_value(); // use default value
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ObITabletMdsInterface::get_autoinc_seq(
    ObIAllocator &allocator,
    const share::SCN &snapshot,
    share::ObTabletAutoincSeq &data,
    const int64_t timeout) const
{
  #define PRINT_WRAPPER KR(ret), K(data), K(snapshot), K(timeout)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!check_is_inited_())) {
    ret = OB_NOT_INIT;
    MDS_LOG_GET(WARN, "not inited");
  } else if (OB_UNLIKELY(!snapshot.is_max())) {
    ret = OB_NOT_SUPPORTED;
    MDS_LOG_GET(WARN, "only support read latest data currently");
  } else {
    const ObTabletMeta &tablet_meta = get_tablet_meta_();
    const bool has_transfer_table = tablet_meta.has_transfer_table();
    ObITabletMdsInterface *src = nullptr;
    ObTabletHandle src_tablet_handle;
    if (has_transfer_table) {
      const share::ObLSID &src_ls_id = tablet_meta.transfer_info_.ls_id_;
      const common::ObTabletID &tablet_id = tablet_meta.tablet_id_;
      if (CLICK_FAIL(get_tablet_handle_and_base_ptr(src_ls_id, tablet_id, src_tablet_handle, src))) {
        MDS_LOG(WARN, "fail to get src tablet handle", K(ret), K(src_ls_id), K(tablet_id));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (CLICK_FAIL((cross_ls_get_snapshot<mds::DummyKey, share::ObTabletAutoincSeq>(src, mds::DummyKey(),
        ReadAutoIncSeqOp(allocator, data), snapshot, timeout)))) {
      if (OB_EMPTY_RESULT != ret) {
        MDS_LOG_GET(WARN, "fail to cross ls get snapshot", K(lbt()));
      } else {
        data.reset(); // use default value
        ret = OB_SUCCESS;
      }
    }
  }

  return ret;
  #undef PRINT_WRAPPER
}

int ObITabletMdsInterface::read_raw_data(
    common::ObIAllocator &allocator,
    const uint8_t mds_unit_id,
    const common::ObString &udf_key,
    const share::SCN &snapshot,
    const int64_t timeout_us,
    mds::MdsDumpKV &kv) const
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = get_tablet_meta_().ls_id_;
  const common::ObTabletID &tablet_id = get_tablet_meta_().tablet_id_;
  const int64_t abs_timeout = timeout_us + ObClockGenerator::getClock();

  SMART_VARS_3((ObTableScanParam, scan_param), (ObStoreCtx, store_ctx), (ObMdsRowIterator, iter)) {
    if (OB_FAIL(ObMdsScanParamHelper::build_scan_param(
        allocator,
        ls_id,
        tablet_id,
        ObMdsSchemaHelper::MDS_TABLE_ID,
        mds_unit_id,
        udf_key,
        true/*is_get*/,
        abs_timeout,
        snapshot,
        scan_param))) {
      MDS_LOG(WARN, "fail to build scan param", K(ret));
    } else if (OB_FAIL(mds_table_scan(scan_param, store_ctx, iter))) {
      MDS_LOG(WARN, "fail to do mds table scan", K(ret), K(snapshot), K(scan_param));
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_FAIL(iter.get_next_mds_kv(allocator, kv))) {
        if (OB_ITER_END != ret) {
          MDS_LOG(WARN, "fail to get next row", K(ret));
        }
      } else if (OB_UNLIKELY(OB_ITER_END != (tmp_ret = iter.get_next_row()))) {
        ret = OB_ERR_UNEXPECTED;
        MDS_LOG(WARN, "iter should reach the end", K(ret), K(tmp_ret), K(iter));
      }
    }

    if (OB_FAIL(ret)) {
      kv.reset();
    }
  }

  return ret;
}

int ObITabletMdsInterface::mds_table_scan(
    ObTableScanParam &scan_param,
    ObStoreCtx &store_ctx,
    ObMdsRowIterator &iter) const
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = get_tablet_meta_().ls_id_;
  const common::ObTabletID &tablet_id = get_tablet_meta_().tablet_id_;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  const ObTablet *tablet = static_cast<const ObTablet*>(this);
  ObTabletHandle tablet_handle;

  if (OB_FAIL(t3m->build_tablet_handle_for_mds_scan(const_cast<ObTablet*>(tablet), tablet_handle))) {
    MDS_LOG(WARN, "fail to build tablet handle", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(iter.init(scan_param, tablet_handle, store_ctx))) {
    MDS_LOG(WARN, "fail to init mds row iter", K(ret), K(ls_id), K(tablet_id), K(scan_param));
  }

  return ret;
}

template <>
int ObITabletMdsInterface::mds_range_query<compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo>(
    ObTableScanParam &scan_param,
    ObStoreCtx &store_ctx,
    ObMdsRangeQueryIterator<compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo> &iter) const
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = get_tablet_meta_().ls_id_;
  const common::ObTabletID &tablet_id = get_tablet_meta_().tablet_id_;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  const ObTablet *tablet = static_cast<const ObTablet*>(this);
  ObTabletHandle tablet_handle;

  if (OB_FAIL(t3m->build_tablet_handle_for_mds_scan(const_cast<ObTablet*>(tablet), tablet_handle))) {
    MDS_LOG(WARN, "fail to build tablet handle", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(iter.init(scan_param, tablet_handle, store_ctx, ObTabletMediumInfoMdsNodeFilter()))) {
    MDS_LOG(WARN, "fail to init range scan iter", K(ret), K(scan_param));
  }

  return ret;
}
} // namespace storage
} // namespace oceanbase
