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

#include "ob_all_virtual_ss_notify_tablets_stat.h"
#include "lib/container/ob_tuple.h"
#include "lib/function/ob_function.h"
#include "lib/list/ob_dlist.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_print_utils.h"
#include "share/ob_ls_id.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/ls/ob_ls.h"

namespace oceanbase
{
using namespace share;
using namespace storage;
using namespace storage::mds;
using namespace common;
using namespace omt;
using namespace detector;
namespace observer
{

struct SSNotifyTabletsStatApplyOnTabletOp {
  SSNotifyTabletsStatApplyOnTabletOp(ObAllVirtualSSNotifyTabletsStat *table, char *temp_buffer)
  : table_(table),
  temp_buffer_(temp_buffer) {}
  int operator()(ObTablet &tablet) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(table_->convert_tablet_info_to_row_(tablet, temp_buffer_, ObAllVirtualSSNotifyTabletsStat::BUFFER_SIZE, table_->cur_row_))) {
      SSLOG_LOG(WARN, "failed to convert_node_info_to_row_", K(ret), K(*table_));
    } else if (OB_FAIL(table_->scanner_.add_row(table_->cur_row_))) {
      SSLOG_LOG(WARN, "fail to add_row to scanner_", K(MTL_ID()), K(*table_));
    }
    return ret;
  }
  ObAllVirtualSSNotifyTabletsStat *table_;
  char *temp_buffer_;
};

struct SSNotifyTabletsStatApplyOnLSOp {
  SSNotifyTabletsStatApplyOnLSOp(ObAllVirtualSSNotifyTabletsStat *table, SSNotifyTabletsStatApplyOnTabletOp &apply_on_tablet_op)
  : table_(table),
  apply_on_tablet_op_(apply_on_tablet_op) {}
  int operator()(ObLS &ls) {
    int ret = OB_SUCCESS;
    if (table_->judege_in_ranges(ls.get_ls_id(), table_->ls_ranges_)) {
      (void) table_->get_tablet_info_(ls, apply_on_tablet_op_);
    } else {
      SSLOG_LOG(TRACE, "not in ranges", K(ret), K(*table_));
    }
    return OB_SUCCESS;
  }
  ObAllVirtualSSNotifyTabletsStat *table_;
  SSNotifyTabletsStatApplyOnTabletOp &apply_on_tablet_op_;
};

struct SSNotifyTabletsStatApplyOnTenantOp {
  SSNotifyTabletsStatApplyOnTenantOp(ObAllVirtualSSNotifyTabletsStat *table, SSNotifyTabletsStatApplyOnLSOp &op) : table_(table), op_(op) {}
  int operator()() {
    int ret = OB_SUCCESS;
    if (table_->judege_in_ranges(MTL_ID(), table_->tenant_ranges_)) {
      if (OB_FAIL(ObTenantMdsService::for_each_ls_in_tenant(op_))) {
        SSLOG_LOG(WARN, "failed to do for_each_ls_in_tenant", K(ret));
        ret = OB_SUCCESS;
      }
    } else {
      SSLOG_LOG(TRACE, "not in ranges", K(ret), K(*table_));
    }
    return ret;
  }
  ObAllVirtualSSNotifyTabletsStat *table_;
  SSNotifyTabletsStatApplyOnLSOp &op_;
};

int ObAllVirtualSSNotifyTabletsStat::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (false == start_to_read_) {
    if (OB_FAIL(get_primary_key_ranges_())) {
      SSLOG_LOG(WARN, "fail to get index scan ranges", KR(ret), K(MTL_ID()), K(*this));
    } else if (tablet_points_.empty()) {
      ret = OB_NOT_SUPPORTED;
      SSLOG_LOG(WARN, "tenant_id/ls_id/tablet_id must be specified", KR(ret), K(MTL_ID()), K(*this));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant_id/ls_id/tablet_id must be specified. range select is");
    } else {
      char *temp_buffer = nullptr;
      if (OB_ISNULL(temp_buffer = (char *)mtl_malloc(ObAllVirtualSSNotifyTabletsStat::BUFFER_SIZE, "VirSSTabletStat"))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SSLOG_LOG(WARN, "fail to alloc buffer", K(MTL_ID()), K(*this));
      } else {
        SSNotifyTabletsStatApplyOnTabletOp apply_on_table_op(this, temp_buffer);
        SSNotifyTabletsStatApplyOnLSOp apply_on_ls_op(this, apply_on_table_op);
        SSNotifyTabletsStatApplyOnTenantOp apply_on_tenant_op(this, apply_on_ls_op);
        if (OB_FAIL(omt_->operate_each_tenant_for_sys_or_self(apply_on_tenant_op))) {
          SSLOG_LOG(WARN, "ObMultiTenant operate_each_tenant_for_sys_or_self failed", K(ret), K(*this));
        } else {
          scanner_it_ = scanner_.begin();
          start_to_read_ = true;
        }
        mtl_free(temp_buffer);
      }
    }
  }
  if (OB_SUCC(ret) && true == start_to_read_) {
    if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
      if (OB_ITER_END != ret) {
        SSLOG_LOG(WARN, "failed to get_next_row", K(ret), K(*this));
      }
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

int ObAllVirtualSSNotifyTabletsStat::convert_tablet_info_to_row_(ObTablet &tablet,
                                                                 char *buffer,
                                                                 int64_t buffer_size,
                                                                 common::ObNewRow &row)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_SHARED_STORAGE
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  int64_t pos = 0;
  const int64_t count = output_column_ids_.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    uint64_t col_id = output_column_ids_.at(i);
    switch (col_id) {
      case OB_APP_MIN_COLUMN_ID: {// tenant_id
        cur_row_.cells_[i].set_int(MTL_ID());
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 1: {// ls_id
        cur_row_.cells_[i].set_int(tablet.get_ls_id().id());
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 2: {// tablet_id
        cur_row_.cells_[i].set_int(tablet.get_tablet_id().id());
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 3: {// reorganization_scn
        cur_row_.cells_[i].set_int(tablet.get_reorganization_scn().get_val_for_tx(true));
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 4: {// svr_ip
        if (false == (GCTX.self_addr().ip_to_string(ip_buffer_, IP_BUFFER_SIZE))) {
          ret = OB_ERR_UNEXPECTED;
          SSLOG_LOG(WARN, "ip_to_string failed", KR(ret));
        } else {
          cur_row_.cells_[i].set_varchar(ObString(ip_buffer_));
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        }
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 5: {// svr_port
        cur_row_.cells_[i].set_int(GCTX.self_addr().get_port());
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 6: {// ss_change_version
        ObTabletBasePointer *tablet_pointer = nullptr;
        if (OB_ISNULL(tablet_pointer = static_cast<ObTabletBasePointer *>(tablet.get_pointer_handle().get_resource_ptr()))) {
          ret = OB_BAD_NULL_ERROR;
          cur_row_.cells_[i].set_int(ret);
          SSLOG_LOG(WARN, "fail to get ss_change_version", KR(ret), K(tablet));
          ret = OB_SUCCESS;
        } else {
          cur_row_.cells_[i].set_int(tablet_pointer->get_ss_change_version().get_val_for_tx(true));
        }
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 7: {// notify_ss_change_version
        ObTabletBasePointer *tablet_pointer = nullptr;
        if (OB_ISNULL(tablet_pointer = static_cast<ObTabletBasePointer *>(tablet.get_pointer_handle().get_resource_ptr()))) {
          cur_row_.cells_[i].set_int(ret);
          SSLOG_LOG(WARN, "fail to get ss_change_version", KR(ret), K(tablet));
          ret = OB_SUCCESS;
        } else {
          cur_row_.cells_[i].set_int(tablet_pointer->get_notify_ss_change_version().get_val_for_tx(true));
        }
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 8: {// ss_min_change_version
        cur_row_.cells_[i].set_int(tablet.get_min_ss_tablet_version().get_val_for_tx(true));
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 9: {// oldest_ss_change_version
        share::SCN min_ss_ch_version;
        if (OB_FAIL(t3m->get_oldest_ss_change_version(tablet.get_ls_id(),
                                                      tablet.get_tablet_id(),
                                                      tablet.get_pointer_handle(),
                                                      min_ss_ch_version))) {
          SSLOG_LOG(WARN, "get oldest ss change version fail", K(ret));
          ret = OB_SUCCESS;
          cur_row_.cells_[i].set_int(ret);
        } else {
          cur_row_.cells_[i].set_int(min_ss_ch_version.get_val_for_tx(true));
        }
        break;
      }
      default:
        ob_abort();
        break;
    }
  }
#endif
  return ret;
}


int ObAllVirtualSSNotifyTabletsStat::get_primary_key_ranges_()
{
  int ret = OB_SUCCESS;
  if (key_ranges_.count() >= 1) {
    for (int64_t i = 0; OB_SUCC(ret) && i < key_ranges_.count(); i++) {
      ObNewRange &key_range = key_ranges_.at(i);
      if (OB_UNLIKELY(key_range.get_start_key().get_obj_cnt() != 3
                      || key_range.get_end_key().get_obj_cnt() != 3)) {
        ret = OB_ERR_UNEXPECTED;
        SSLOG_LOG(ERROR, "unexpected  # of rowkey columns",
                  K(ret),
                  "size of start key", key_range.get_start_key().get_obj_cnt(),
                  "size of end key", key_range.get_end_key().get_obj_cnt());
      } else {
        ObObj tenant_obj_low = (key_range.get_start_key().get_obj_ptr()[0]);
        ObObj tenant_obj_high = (key_range.get_end_key().get_obj_ptr()[0]);
        ObObj ls_obj_low = (key_range.get_start_key().get_obj_ptr()[1]);
        ObObj ls_obj_high = (key_range.get_end_key().get_obj_ptr()[1]);
        ObObj tablet_obj_low = (key_range.get_start_key().get_obj_ptr()[2]);
        ObObj tablet_obj_high = (key_range.get_end_key().get_obj_ptr()[2]);

        uint64_t tenant_low = tenant_obj_low.is_min_value() ? 0 : tenant_obj_low.get_uint64();
        uint64_t tenant_high = tenant_obj_high.is_max_value() ? UINT64_MAX : tenant_obj_high.get_uint64();
        ObLSID ls_low = ls_obj_low.is_min_value() ? ObLSID(0) : ObLSID(ls_obj_low.get_int());
        ObLSID ls_high = ls_obj_high.is_max_value() ? ObLSID(INT64_MAX) : ObLSID(ls_obj_high.get_int());
        ObTabletID tablet_low = tablet_obj_low.is_min_value() ? ObTabletID(0) : ObTabletID(tablet_obj_low.get_uint64());
        ObTabletID tablet_high = tablet_obj_high.is_max_value() ? ObTabletID(UINT64_MAX) : ObTabletID(tablet_obj_high.get_uint64());

        if (OB_FAIL(tenant_ranges_.push_back(ObTuple<uint64_t, uint64_t>(tenant_low, tenant_high)))) {
          SSLOG_LOG(WARN, "fail to push back", KR(ret), K(*this));
        } else if (OB_SUCCESS != (ret =
        (ls_ranges_.push_back(ObTuple<share::ObLSID, share::ObLSID>(ls_low, ls_high))))) {
          SSLOG_LOG(WARN, "fail to push back", KR(ret), K(*this));
        } else {
          if (tablet_low == tablet_high) {
            if (OB_FAIL(tablet_points_.push_back(tablet_low))) {
              SSLOG_LOG(WARN, "fail to push back", KR(ret), K(*this));
            }
          } else if (OB_SUCCESS != (ret =
            (tablet_ranges_.push_back(ObTuple<common::ObTabletID, common::ObTabletID>(tablet_low, tablet_high))))) {
            SSLOG_LOG(WARN, "fail to push back", KR(ret), K(*this));
          }
        }
      }
    }
  }
  SSLOG_LOG(INFO, "get_primary_key_ranges_", KR(ret), K(key_ranges_), K(*this));
  return ret;
}

bool ObAllVirtualSSNotifyTabletsStat::in_selected_points_(common::ObTabletID tablet_id)
{
  bool is_in_points = false;
  for (int64_t idx = 0; idx < tablet_points_.count(); ++idx) {
    if (tablet_id == tablet_points_[idx]) {
      is_in_points = true;
      break;
    }
  }
  return is_in_points;
}

int ObAllVirtualSSNotifyTabletsStat::get_tablet_info_(ObLS &ls, const ObFunction<int(ObTablet &)> &apply_on_tablet_op)
{
  int ret = OB_SUCCESS;
  if (!apply_on_tablet_op.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    SSLOG_LOG(ERROR, "invalid ob function", KR(ret), K(key_ranges_), K(*this));
  } else {
    for (int64_t idx = 0; idx < tablet_points_.count() && OB_SUCC(ret); ++idx) {
      ObTabletHandle tablet_handle;
      if (OB_FAIL(ls.get_tablet(tablet_points_[idx], tablet_handle, 0, storage::ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
        SSLOG_LOG(WARN, "fail to get tablet", KR(ret), K(key_ranges_), K(*this));
      } else if (OB_ISNULL(tablet_handle.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        SSLOG_LOG(ERROR, "get null tablet ptr", KR(ret), K(key_ranges_), K(*this));
      } else if (OB_FAIL(apply_on_tablet_op(*tablet_handle.get_obj()))) {
        SSLOG_LOG(WARN, "fail to apply op on tablet", KR(ret), K(key_ranges_), K(*this));
      }
    }
  }
  SSLOG_LOG(INFO, "get_tablet_info_", KR(ret), K(key_ranges_), K(*this));
  return ret;
}

}
}
