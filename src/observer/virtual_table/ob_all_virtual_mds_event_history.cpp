/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "ob_all_virtual_mds_event_history.h"
#include "lib/container/ob_tuple.h"
#include "lib/function/ob_function.h"
#include "lib/list/ob_dlist.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_print_utils.h"
#include "share/ob_ls_id.h"
#include "storage/multi_data_source/runtime_utility/common_define.h"
#include "storage/multi_data_source/runtime_utility/mds_tenant_service.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/ls/ob_ls.h"
#include "ob_mds_event_buffer.h"

namespace oceanbase
{
using namespace share;
using namespace storage;
using namespace storage::mds;
using namespace common;
using namespace omt;
namespace observer
{

bool ObAllVirtualMdsEventHistory::judge_key_in_ranges_(const MdsEventKey &key) const
{
  bool in_tenant_ranges = false;
  bool in_ls_ranges = false;
  bool in_tablet_ranges = false;
  for (int64_t idx = 0; idx < tenant_ranges_.count(); ++idx) {
    if (key.tenant_id_ >= tenant_ranges_[idx].element<0>() && key.tenant_id_ <= tenant_ranges_[idx].element<1>()) {
      in_tenant_ranges = true;
      break;
    }
  }
  if (!in_tenant_ranges) {
    for (int64_t idx = 0; idx < tenant_points_.count(); ++idx) {
      if (tenant_points_[idx] == key.tenant_id_) {
        in_tenant_ranges = true;
        break;
      }
    }
  }
  if (in_tenant_ranges) {
    for (int64_t idx = 0; idx < ls_ranges_.count(); ++idx) {
      if (key.ls_id_ >= ls_ranges_[idx].element<0>() && key.ls_id_ <= ls_ranges_[idx].element<1>()) {
        in_ls_ranges = true;
        break;
      }
    }
    if (!in_ls_ranges) {
      for (int64_t idx = 0; idx < ls_points_.count(); ++idx) {
        if (ls_points_[idx] == key.ls_id_) {
          in_ls_ranges = true;
          break;
        }
      }
    }
    if (in_ls_ranges) {
      for (int64_t idx = 0; idx < tablet_ranges_.count(); ++idx) {
        if (key.tablet_id_ >= tablet_ranges_[idx].element<0>() && key.tablet_id_ <= tablet_ranges_[idx].element<1>()) {
          in_tablet_ranges = true;
          break;
        }
      }
      if (!in_ls_ranges) {
        for (int64_t idx = 0; idx < tablet_points_.count(); ++idx) {
          if (tablet_points_[idx] == key.tablet_id_) {
            in_tablet_ranges = true;
            break;
          }
        }
      }
    }
  }
  return in_tenant_ranges && in_ls_ranges && in_tablet_ranges;
}

int ObAllVirtualMdsEventHistory::range_scan_(char *temp_buffer, int64_t buf_len)
{
  int ret = OB_SUCCESS;
  MDS_LOG(INFO, "start range read", K(*this));
  if (OB_FAIL(ObMdsEventBuffer::for_each([this, temp_buffer, buf_len](const MdsEventKey &key, const MdsEvent &event) -> int {
    int ret = OB_SUCCESS;
    int tmp_ret = OB_SUCCESS;
    if (judge_key_in_ranges_(key)) {
      if (MTL_ID() == OB_SYS_TENANT_ID ||// SYS租户可以看到所有租户的信息
          key.tenant_id_ == MTL_ID()) {// 非SYS租户只能看到本租户的信息
        MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
        if (MTL_ID() != key.tenant_id_) {
          tmp_ret = guard.switch_to(key.tenant_id_);
        }
        if (OB_SUCCESS == tmp_ret) {
          if (OB_FAIL(convert_event_info_to_row_(key, event, temp_buffer, buf_len, cur_row_))) {
            MDS_LOG(WARN, "failed to convert_node_info_to_row_", K(ret), K(*this));
          } else if (OB_FAIL(scanner_.add_row(cur_row_))) {
            MDS_LOG(WARN, "fail to add_row to scanner_", K(*this));
          } else {
            MDS_LOG(TRACE, "scan", K(key));
          }
        }
      }
    }
    return ret;
  }))) {
    MDS_LOG(WARN, "scan read failed", KR(ret), K(MTL_ID()), K(*this));
  }
  return ret;
}

int ObAllVirtualMdsEventHistory::point_read_(char *temp_buffer, int64_t buf_len)
{
  int ret = OB_SUCCESS;
  MDS_LOG(INFO, "start point read", K(*this));
  if ((tenant_points_.count() != ls_points_.count()) || (ls_points_.count() != tablet_points_.count())) {
    MDS_LOG(WARN, "points not match", K(MTL_ID()), K(*this));
  } else {
    for (int64_t idx = 0; idx < tenant_points_.count() && OB_SUCC(ret); ++idx) {
      MdsEventKey key(tenant_points_[idx], ls_points_[idx], tablet_points_[idx]);
      if (OB_FAIL(ObMdsEventBuffer::for_each(key, [&key, this, temp_buffer, buf_len](const MdsEvent &event) -> int {
        int ret = OB_SUCCESS;
        int tmp_ret = OB_SUCCESS;
        if (MTL_ID() == OB_SYS_TENANT_ID ||// SYS租户可以看到所有租户的信息
            key.tenant_id_ == MTL_ID()) {// 非SYS租户只能看到本租户的信息
          MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
          if (MTL_ID() != key.tenant_id_) {
            tmp_ret = guard.switch_to(key.tenant_id_);
          }
          if (OB_SUCCESS == tmp_ret) {
            if (OB_FAIL(convert_event_info_to_row_(key, event, temp_buffer, buf_len, cur_row_))) {
              MDS_LOG(WARN, "failed to convert_node_info_to_row_", K(ret), K(*this));
            } else if (OB_FAIL(scanner_.add_row(cur_row_))) {
              MDS_LOG(WARN, "fail to add_row to scanner_", K(MTL_ID()), K(*this));
            }
          }
        }
        return ret;
      }))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          MDS_LOG(WARN, "OB_ENTRY_NOT_EXIST", K(key));
        } else {
          MDS_LOG(WARN, "failed to do for_each", K(ret), K(*this));
        }
      } else {
        MDS_LOG(INFO, "read key", K(key), K(*this));
      }
    }
  }
  return ret;
}

int ObAllVirtualMdsEventHistory::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (false == start_to_read_) {
    if (OB_FAIL(get_primary_key_ranges_())) {
      MDS_LOG(WARN, "fail to get index scan ranges", KR(ret), K(MTL_ID()), K(*this));
    } else {
      char *temp_buffer = nullptr;
      constexpr int64_t BUFFER_SIZE = 32_MB;
      if (OB_ISNULL(temp_buffer = (char *)ob_malloc(BUFFER_SIZE, "VirMdsEvent"))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        MDS_LOG(WARN, "fail to alloc buffer", K(MTL_ID()), K(*this));
      } else {
        if (!tenant_ranges_.empty() || !ls_ranges_.empty() || !tablet_ranges_.empty()) {// scan read
          ret = range_scan_(temp_buffer, BUFFER_SIZE);
        } else {// point read
          ret = point_read_(temp_buffer, BUFFER_SIZE);
        }
        if (OB_SUCC(ret)) {
          scanner_it_ = scanner_.begin();
          start_to_read_ = true;
        }
        ob_free(temp_buffer);
      }
    }
  }
  if (OB_SUCC(ret) && true == start_to_read_) {
    if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
      if (OB_ITER_END != ret) {
        MDS_LOG(WARN, "failed to get_next_row", K(ret), K(*this));
      }
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

int ObAllVirtualMdsEventHistory::convert_event_info_to_row_(const MdsEventKey &key,
                                                            const MdsEvent &event,
                                                            char *buffer,
                                                            int64_t buffer_size,
                                                            common::ObNewRow &row)
{
  int ret = OB_SUCCESS;
  const int64_t count = output_column_ids_.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    uint64_t col_id = output_column_ids_.at(i);
    switch (col_id) {
      case OB_APP_MIN_COLUMN_ID: {// tenant_id
        cur_row_.cells_[i].set_int(MTL_ID());
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 1: {// ls_id
        cur_row_.cells_[i].set_int(key.ls_id_.id());
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 2: {// tablet_id
        cur_row_.cells_[i].set_int(key.tablet_id_.id());
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 3: {// svr_ip
        if (false == (GCTX.self_addr().ip_to_string(ip_buffer_, IP_BUFFER_SIZE))) {
          ret = OB_ERR_UNEXPECTED;
          MDS_LOG(WARN, "ip_to_string failed", KR(ret), K(*this));
        } else {
          cur_row_.cells_[i].set_varchar(ObString(ip_buffer_));
        }
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 4: {// svr_port
        cur_row_.cells_[i].set_int(GCTX.self_addr().get_port());
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 5: {// tid
        cur_row_.cells_[i].set_int(event.tid_);
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 6: {// tname
        int64_t pos = 0;
        databuff_printf(buffer, buffer_size, pos, "%s", event.tname_);
        buffer += pos;
        buffer_size -= pos;
        cur_row_.cells_[i].set_string(ObLongTextType, ObString(pos, buffer - pos));
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 7: {// trace
        int64_t pos = 0;
        databuff_printf(buffer, buffer_size, pos, "%s", to_cstring(event.trace_id_));
        buffer += pos;
        buffer_size -= pos;
        cur_row_.cells_[i].set_string(ObLongTextType, ObString(pos, buffer - pos));
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 8: {// timestamp
        cur_row_.cells_[i].set_timestamp(event.timestamp_);
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 9: {// event
        int64_t pos = 0;
        databuff_printf(buffer, buffer_size, pos, "%s", event.event_);
        buffer += pos;
        buffer_size -= pos;
        cur_row_.cells_[i].set_string(ObLongTextType, ObString(pos, buffer - pos));
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 10: {// info
        int64_t pos = 0;
        databuff_printf(buffer, buffer_size, pos, "%s", to_cstring(event.info_str_));
        buffer += pos;
        buffer_size -= pos;
        cur_row_.cells_[i].set_string(ObLongTextType, ObString(pos, buffer - pos));
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 11: {// unit_id
        cur_row_.cells_[i].set_int(event.unit_id_);
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 12: {// user_key
        int64_t pos = 0;
        databuff_printf(buffer, buffer_size, pos, "%s", to_cstring(event.key_str_));
        buffer += pos;
        buffer_size -= pos;
        cur_row_.cells_[i].set_string(ObLongTextType, ObString(pos, buffer - pos));
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 13: {// writer_type
        int64_t pos = 0;
        databuff_printf(buffer, buffer_size, pos, "%s", mds::obj_to_string(event.writer_type_));
        buffer += pos;
        buffer_size -= pos;
        cur_row_.cells_[i].set_string(ObLongTextType, ObString(pos, buffer - pos));
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 14: {// writer_id
        cur_row_.cells_[i].set_int(event.writer_id_);
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 15: {// seq_no
        cur_row_.cells_[i].set_int(event.seq_no_);
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 16: {// redo_scn
        cur_row_.cells_[i].set_uint64(event.redo_scn_.get_val_for_inner_table_field());
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 17: {// end_scn
        cur_row_.cells_[i].set_uint64(event.end_scn_.get_val_for_inner_table_field());
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 18: {// trans_version
        cur_row_.cells_[i].set_uint64(event.trans_version_.get_val_for_inner_table_field());
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 19: {// node_type
        int64_t pos = 0;
        databuff_printf(buffer, buffer_size, pos, "%s", mds::obj_to_string(event.node_type_));
        buffer += pos;
        buffer_size -= pos;
        cur_row_.cells_[i].set_string(ObLongTextType, ObString(pos, buffer - pos));
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 20: {// state
        int64_t pos = 0;
        databuff_printf(buffer, buffer_size, pos, "%s", mds::obj_to_string(event.state_));
        buffer += pos;
        buffer_size -= pos;
        cur_row_.cells_[i].set_string(ObLongTextType, ObString(pos, buffer - pos));
        break;
      }
    }
  }
  return ret;
}

int ObAllVirtualMdsEventHistory::get_primary_key_ranges_()
{
  int ret = OB_SUCCESS;
  if (key_ranges_.count() >= 1) {
    for (int64_t i = 0; OB_SUCC(ret) && i < key_ranges_.count(); i++) {
      ObNewRange &key_range = key_ranges_.at(i);
      if (OB_UNLIKELY(key_range.get_start_key().get_obj_cnt() != 3
                      || key_range.get_end_key().get_obj_cnt() != 3)) {
        ret = OB_ERR_UNEXPECTED;
        MDS_LOG(ERROR, "unexpected  # of rowkey columns",
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

        if (OB_SUCC(ret)) {
          if (tenant_low == tenant_high) {
            if (OB_FAIL(tenant_points_.push_back(tenant_low))) {
              MDS_LOG(WARN, "fail to push back", KR(ret), K(*this));
            }
          } else if (OB_SUCCESS != (ret =
            (tenant_ranges_.push_back(ObTuple<uint64_t, uint64_t>(tenant_low, tenant_high))))) {
            MDS_LOG(WARN, "fail to push back", KR(ret), K(*this));
          }
        }
        if (OB_SUCC(ret)) {
          if (ls_low == ls_high) {
            if (OB_FAIL(ls_points_.push_back(ls_low))) {
              MDS_LOG(WARN, "fail to push back", KR(ret), K(*this));
            }
          } else if (OB_SUCCESS != (ret =
            (ls_ranges_.push_back(ObTuple<share::ObLSID, share::ObLSID>(ls_low, ls_high))))) {
            MDS_LOG(WARN, "fail to push back", KR(ret), K(*this));
          }
        }
        if (OB_SUCC(ret)) {
          if (tablet_low == tablet_high) {
            if (OB_FAIL(tablet_points_.push_back(tablet_low))) {
              MDS_LOG(WARN, "fail to push back", KR(ret), K(*this));
            }
          } else if (OB_SUCCESS != (ret =
            (tablet_ranges_.push_back(ObTuple<common::ObTabletID, common::ObTabletID>(tablet_low, tablet_high))))) {
            MDS_LOG(WARN, "fail to push back", KR(ret), K(*this));
          }
        }
      }
    }
  }
  MDS_LOG(INFO, "get_primary_key_ranges_", KR(ret), K(key_ranges_), K(*this));
  return ret;
}

}
}