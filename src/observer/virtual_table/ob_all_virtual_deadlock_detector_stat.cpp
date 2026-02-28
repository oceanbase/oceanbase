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

#include "ob_all_virtual_deadlock_detector_stat.h"
#include "lib/container/ob_tuple.h"
#include "lib/function/ob_function.h"
#include "lib/list/ob_dlist.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_print_utils.h"
#include "share/ob_ls_id.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/multi_data_source/runtime_utility/common_define.h"
#include "storage/multi_data_source/runtime_utility/mds_tenant_service.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/ls/ob_ls.h"
#include "share/deadlock/ob_deadlock_detector_mgr.h"
#include "share/deadlock/ob_lcl_scheme/ob_lcl_node.h"

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

static constexpr int64_t BUFFER_SIZE = 32_MB;

int ObAllVirtualDeadLockDetectorStat::IterNodeOp::operator()(const UserBinaryKey &key, ObIDeadLockDetector *detector) {
  int ret = OB_SUCCESS;
  bool need_fill_virtual_info_flag = false;
  bool need_fill_conflict_actions_flag = false;
  if (this_->is_in_selected_id_points_(detector->get_resource_id())) {
    need_fill_virtual_info_flag = true;
    need_fill_conflict_actions_flag = true;
  } else if (this_->is_in_selected_id_ranges_(detector->get_resource_id())) {
    need_fill_virtual_info_flag = true;
  }
  if (need_fill_virtual_info_flag) {
    if (OB_FAIL(this_->convert_node_info_to_row_(need_fill_conflict_actions_flag, detector, temp_buffer_, BUFFER_SIZE, this_->cur_row_))) {
      DETECT_LOG(WARN, "fail to convert node info", K(MTL_ID()), K(key));
    } else if (OB_FAIL(this_->scanner_.add_row(this_->cur_row_))) {
      DETECT_LOG(WARN, "fail to add_row to scanner_", K(MTL_ID()), K(key));
    }
    ret = OB_SUCCESS;// skip this node if fill virtual info failed
  }
  return (ret == OB_SUCCESS);
}

int ObAllVirtualDeadLockDetectorStat::IterateTenantOp::operator()() {
  IterNodeOp for_each_op(this_, temp_buffer_);
  int ret = OB_SUCCESS;
  if (this_->is_in_selected_tenants_()) {
    ret = MTL(detector::ObDeadLockDetectorMgr*)->for_each_detector(for_each_op);
  }
  return ret;
}

int ObAllVirtualDeadLockDetectorStat::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (false == start_to_read_) {
    if (OB_FAIL(get_primary_key_ranges_())) {
      MDS_LOG(WARN, "fail to get index scan ranges", KR(ret), K(MTL_ID()), K(*this));
    } else {
      char *temp_buffer = nullptr;
      char *to_string_buffer = nullptr;
      constexpr int64_t BUFFER_SIZE = 32_MB;
      if (OB_ISNULL(temp_buffer = (char *)mtl_malloc(BUFFER_SIZE, "VirDetector"))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        DETECT_LOG(WARN, "fail to alloc buffer", K(MTL_ID()), K(*this));
      } else {
        IterateTenantOp func_iterate_tenant(this, temp_buffer);
        if (OB_FAIL(omt_->operate_each_tenant_for_sys_or_self(func_iterate_tenant))) {
          DETECT_LOG(WARN, "ObMultiTenant operate_each_tenant_for_sys_or_self failed", K(ret), K(*this));
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
        DETECT_LOG(WARN, "failed to get_next_row", K(ret), K(*this));
      }
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

int ObAllVirtualDeadLockDetectorStat::convert_node_info_to_row_(const bool need_fill_conflict_actions_flag,
                                                                ObIDeadLockDetector *detector,
                                                                char *buffer,
                                                                int64_t buffer_size,
                                                                common::ObNewRow &row)
{
  int ret = OB_SUCCESS;
  DetectorNodeInfoForVirtualTable info;
  int64_t pos = 0;
  if (OB_FAIL(detector->fill_virtual_info(need_fill_conflict_actions_flag, info, buffer, buffer_size, pos))) {
    DETECT_LOG(WARN, "fill virtual info failed");
  } else {
    const int64_t count = output_column_ids_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case OB_APP_MIN_COLUMN_ID: {// tenant_id
          cur_row_.cells_[i].set_int(MTL_ID());
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 1: {// detector_id
          cur_row_.cells_[i].set_int(info.detector_id_);
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 2: {// svr_ip
          if (false == (GCTX.self_addr().ip_to_string(ip_buffer_, IP_BUFFER_SIZE))) {
            ret = OB_ERR_UNEXPECTED;
            DETECT_LOG(WARN, "ip_to_string failed", KR(ret), K(*this));
          } else {
            cur_row_.cells_[i].set_varchar(ObString(ip_buffer_));
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 3: {// svr_port
          cur_row_.cells_[i].set_int(GCTX.self_addr().get_port());
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 4: {// module
          cur_row_.cells_[i].set_string(ObLongTextType, info.module_);
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 5: {// visitor
          cur_row_.cells_[i].set_string(ObLongTextType, info.visitor_);
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 6: {// action
          cur_row_.cells_[i].set_string(ObLongTextType, info.action_);
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 7: {// static_waiting_resource
          cur_row_.cells_[i].set_string(ObLongTextType, info.static_block_resource_);
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 8: {// static_block_list
          cur_row_.cells_[i].set_string(ObLongTextType, info.static_block_list_);
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 9: {// dynamic_waiting_resource
          cur_row_.cells_[i].set_string(ObLongTextType, info.dynamic_block_resource_);
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 10: {// dynamic_block_list
          cur_row_.cells_[i].set_string(ObLongTextType, info.dynamic_block_list_);
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 11: {// conflict_actions
          cur_row_.cells_[i].set_string(ObLongTextType, info.conflict_actions_);
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 12: {// waiter_create_time
          cur_row_.cells_[i].set_timestamp(info.waiter_create_time_);
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 13: {// create_time
          cur_row_.cells_[i].set_timestamp(info.create_time_);
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 14: {// timeout_ts
          cur_row_.cells_[i].set_timestamp(info.timeout_ts_);
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 15: {// allow_detect_time
          cur_row_.cells_[i].set_timestamp(info.allow_detect_time_);
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 16: {// lclv
          cur_row_.cells_[i].set_int(info.lclv_);
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 17: {// lcl_period
          cur_row_.cells_[i].set_int(info.lcl_period_);
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 18: {// private_label
          cur_row_.cells_[i].set_string(ObLongTextType, info.private_label_);
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 19: {// public_label
          cur_row_.cells_[i].set_string(ObLongTextType, info.public_label_);
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 20: {// count_down_allow_detect
          cur_row_.cells_[i].set_int(info.count_down_allow_detect_);
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 21: {// parent_list
          cur_row_.cells_[i].set_string(ObLongTextType, info.parent_list_);
          break;
        }
      }
    }
  }
  return ret;
}

int ObAllVirtualDeadLockDetectorStat::get_primary_key_ranges_()
{
  int ret = OB_SUCCESS;
  if (key_ranges_.count() >= 1) {
    for (int64_t i = 0; OB_SUCC(ret) && i < key_ranges_.count(); i++) {
      ObNewRange &key_range = key_ranges_.at(i);
      if (OB_UNLIKELY(key_range.get_start_key().get_obj_cnt() != 2
                      || key_range.get_end_key().get_obj_cnt() != 2)) {
        ret = OB_ERR_UNEXPECTED;
        MDS_LOG(ERROR, "unexpected  # of rowkey columns",
                  K(ret),
                  "size of start key", key_range.get_start_key().get_obj_cnt(),
                  "size of end key", key_range.get_end_key().get_obj_cnt());
      } else {
        ObObj tenant_obj_low = (key_range.get_start_key().get_obj_ptr()[0]);
        ObObj tenant_obj_high = (key_range.get_end_key().get_obj_ptr()[0]);
        ObObj detector_id_obj_low = (key_range.get_start_key().get_obj_ptr()[1]);
        ObObj detector_id_obj_high = (key_range.get_end_key().get_obj_ptr()[1]);

        uint64_t tenant_low = tenant_obj_low.is_min_value() ? 0 : tenant_obj_low.get_uint64();
        uint64_t tenant_high = tenant_obj_high.is_max_value() ? UINT64_MAX : tenant_obj_high.get_uint64();
        int64_t detector_id_low = detector_id_obj_low.is_min_value() ? 0 : detector_id_obj_low.get_int();
        int64_t detector_id_high = detector_id_obj_high.is_max_value() ? INT64_MAX : detector_id_obj_high.get_int();

        if (OB_FAIL(tenant_ranges_.push_back(ObTuple<uint64_t, uint64_t>(tenant_low, tenant_high)))) {
          MDS_LOG(WARN, "fail to push back", KR(ret), K(*this));
        } else if (detector_id_low == detector_id_high) {
          if (OB_FAIL(detector_id_points_.push_back(detector_id_low))) {
            MDS_LOG(WARN, "fail to push back", KR(ret), K(*this));
          }
        } else if (OB_FAIL(detector_id_ranges_.push_back(ObTuple<int64_t, int64_t>(detector_id_low, detector_id_high)))) {
          MDS_LOG(WARN, "fail to push back", KR(ret), K(*this));
        }
      }
    }
  }
  MDS_LOG(INFO, "get_primary_key_ranges_", KR(ret), K(key_ranges_), K(*this));
  return ret;
}

bool ObAllVirtualDeadLockDetectorStat::is_in_selected_tenants_()
{
  bool ret = false;
  uint64_t current_tenant_id = MTL_ID();
  for (int64_t idx = 0; idx < tenant_ranges_.count(); ++idx) {
    if (current_tenant_id >= tenant_ranges_[idx].element<0>() &&
        current_tenant_id <= tenant_ranges_[idx].element<1>()) {
      ret = true;
      break;
    }
  }
  return ret;
}

bool ObAllVirtualDeadLockDetectorStat::is_in_selected_id_points_(int64_t detector_id)
{
  bool ret = false;
  uint64_t current_tenant_id = MTL_ID();
  for (int64_t idx = 0; idx < detector_id_points_.count(); ++idx) {
    if (detector_id == detector_id_points_[idx]) {
      ret = true;
      break;
    }
  }
  return ret;
}

bool ObAllVirtualDeadLockDetectorStat::is_in_selected_id_ranges_(int64_t detector_id)
{
  bool ret = false;
  for (int64_t idx = 0; idx < detector_id_ranges_.count(); ++idx) {
    if (detector_id >= detector_id_ranges_[idx].element<0>() &&
        detector_id <= detector_id_ranges_[idx].element<1>()) {
      ret = true;
      break;
    }
  }
  return ret;
}

}
}