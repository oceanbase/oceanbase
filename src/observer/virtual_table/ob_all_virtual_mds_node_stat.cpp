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

#include "ob_all_virtual_mds_node_stat.h"
#include "storage/ls/ob_ls.h"

namespace oceanbase
{
using namespace share;
using namespace storage;
using namespace storage::mds;
using namespace common;
using namespace omt;
namespace observer
{

static constexpr int64_t BUFFER_SIZE = 32_MB;

struct ApplyOnTabletOp {
  ApplyOnTabletOp(ObAllVirtualMdsNodeStat *table, char *temp_buffer) : table_(table), temp_buffer_(temp_buffer) {}
  int operator()(ObTablet &tablet) {
    int ret = OB_SUCCESS;
    MdsNodeInfoForVirtualTable mds_info;
    mds::MdsTableHandle mds_table_handle;
    ObArray<MdsNodeInfoForVirtualTable> row_array;
    if (OB_FAIL(table_->get_mds_table_handle_(tablet, mds_table_handle, false))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        MDS_LOG(WARN, "failed to get_mds_table_handle_", K(ret), K(*table_));
      }
    } else if (OB_FAIL(mds_table_handle.fill_virtual_info(row_array))) {
      MDS_LOG(WARN, "failed to fill_virtual_info from mds_table", K(ret), K(*table_));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(tablet.fill_virtual_info(row_array))) {
        MDS_LOG(WARN, "failed to fill_virtual_info from tablet", K(ret), K(*table_));
      } else {
        for (int64_t idx = 0; idx < row_array.count() && OB_SUCC(ret); ++idx) {
          if (OB_FAIL(table_->convert_node_info_to_row_(row_array[idx], temp_buffer_, BUFFER_SIZE, table_->cur_row_))) {
            MDS_LOG(WARN, "failed to convert_node_info_to_row_", K(ret), K(*table_));
          } else if (OB_FAIL(table_->scanner_.add_row(table_->cur_row_))) {
            MDS_LOG(WARN, "fail to add_row to scanner_", K(MTL_ID()), K(*table_));
          }
        }
      }
    }
    return ret;
  }
  ObAllVirtualMdsNodeStat *table_;
  char *temp_buffer_;
};

struct ApplyOnLSOp {
  ApplyOnLSOp(ObAllVirtualMdsNodeStat *table, ApplyOnTabletOp &apply_on_tablet_op)
  : table_(table),
  apply_on_tablet_op_(apply_on_tablet_op) {}
  int operator()(ObLS &ls) {
    int ret = OB_SUCCESS;
    if (table_->judege_in_ranges(ls.get_ls_id(), table_->ls_ranges_)) {
      (void) table_->get_tablet_info_(ls, apply_on_tablet_op_);
    } else {
      MDS_LOG(TRACE, "not in ranges", K(ls.get_ls_id()), K(ret), K(*table_));
    }
    return OB_SUCCESS;
  }
  ObAllVirtualMdsNodeStat *table_;
  ApplyOnTabletOp &apply_on_tablet_op_;
};

struct ApplyOnTenantOp {
  ApplyOnTenantOp(ObAllVirtualMdsNodeStat *table, ApplyOnLSOp &op) : table_(table), op_(op) {}
  int operator()() {
    int ret = OB_SUCCESS;
    if (table_->judege_in_ranges(MTL_ID(), table_->tenant_ranges_)) {
      if (OB_FAIL(ObTenantMdsService::for_each_ls_in_tenant(op_))) {
        MDS_LOG(WARN, "failed to do for_each_ls_in_tenant", K(ret));
        ret = OB_SUCCESS;
      }
    } else {
      MDS_LOG(TRACE, "not in ranges", K(MTL_ID()), K(ret), K(*table_));
    }
    return ret;
  }
  ObAllVirtualMdsNodeStat *table_;
  ApplyOnLSOp &op_;
};

int ObAllVirtualMdsNodeStat::get_mds_table_handle_(ObTablet &tablet,
                                                   mds::MdsTableHandle &handle,
                                                   const bool create_if_not_exist)
{
  return tablet.get_mds_table_handle_(handle, create_if_not_exist);
}

int ObAllVirtualMdsNodeStat::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (false == start_to_read_) {
    if (OB_FAIL(get_primary_key_ranges_())) {
      MDS_LOG(WARN, "fail to get index scan ranges", KR(ret), K(MTL_ID()), K(*this));
    } else if (tablet_points_.empty()) {
      ret = OB_NOT_SUPPORTED;
      MDS_LOG(WARN, "tenant_id/ls_id/tablet_id must be specified", KR(ret), K(MTL_ID()), K(*this));
    } else {
      char *temp_buffer = nullptr;
      if (OB_ISNULL(temp_buffer = (char *)mtl_malloc(BUFFER_SIZE, "VirMdsStat"))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        MDS_LOG(WARN, "fail to alloc buffer", K(MTL_ID()), K(*this));
      } else {
        ApplyOnTabletOp apply_on_table_op(this, temp_buffer);
        ApplyOnLSOp apply_on_ls_op(this, apply_on_table_op);
        ApplyOnTenantOp apply_on_tenant_op(this, apply_on_ls_op);
        if (OB_FAIL(omt_->operate_each_tenant_for_sys_or_self(apply_on_tenant_op))) {
          MDS_LOG(WARN, "ObMultiTenant operate_each_tenant_for_sys_or_self failed", K(ret), K(*this));
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
        MDS_LOG(WARN, "failed to get_next_row", K(ret), K(*this));
      }
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

int ObAllVirtualMdsNodeStat::convert_node_info_to_row_(const storage::mds::MdsNodeInfoForVirtualTable &node_info,
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
        cur_row_.cells_[i].set_int(node_info.ls_id_.id());
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 2: {// tablet_id
        cur_row_.cells_[i].set_int(node_info.tablet_id_.id());
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
      case OB_APP_MIN_COLUMN_ID + 5: {// unit_id
        cur_row_.cells_[i].set_int(node_info.unit_id_);
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 6: {// user_key
        int64_t write_n = node_info.user_key_.to_string(buffer, buffer_size);
        buffer += write_n;
        buffer_size -= write_n;
        cur_row_.cells_[i].set_string(ObLongTextType, ObString(write_n, buffer - write_n));
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 7: {// version_idx
        cur_row_.cells_[i].set_int(node_info.version_idx_);
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 8: {// writer_type
        int64_t pos = 0;
        databuff_printf(buffer, buffer_size, pos, "%s", mds::obj_to_string(node_info.writer_.writer_type_));
        buffer += pos;
        buffer_size -= pos;
        cur_row_.cells_[i].set_string(ObLongTextType, ObString(pos, buffer - pos));
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 9: {// writer_id
        cur_row_.cells_[i].set_int(node_info.writer_.writer_id_);
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 10: {// seq_no
        cur_row_.cells_[i].set_int(node_info.seq_no_.cast_to_int());
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 11: {// redo_scn
        cur_row_.cells_[i].set_uint64(node_info.redo_scn_.get_val_for_inner_table_field());
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 12: {// end_scn
        cur_row_.cells_[i].set_uint64(node_info.end_scn_.get_val_for_inner_table_field());
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 13: {// trans_version
        cur_row_.cells_[i].set_uint64(node_info.trans_version_.get_val_for_inner_table_field());
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 14: {// node_type
        int64_t pos = 0;
        databuff_printf(buffer, buffer_size, pos, "%s", mds::obj_to_string(node_info.node_type_));
        buffer += pos;
        buffer_size -= pos;
        cur_row_.cells_[i].set_string(ObLongTextType, ObString(pos, buffer - pos));
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 15: {// state
        int64_t pos = 0;
        databuff_printf(buffer, buffer_size, pos, "%s", mds::obj_to_string(node_info.state_));
        buffer += pos;
        buffer_size -= pos;
        cur_row_.cells_[i].set_string(ObLongTextType, ObString(pos, buffer - pos));
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 16: {// position
        int64_t pos = 0;
        databuff_printf(buffer, buffer_size, pos, "%s", mds::obj_to_string(node_info.position_));
        buffer += pos;
        buffer_size -= pos;
        cur_row_.cells_[i].set_string(ObLongTextType, ObString(pos, buffer - pos));
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 17: {// user_data
        int64_t write_n = node_info.user_data_.to_string(buffer, buffer_size);
        buffer += write_n;
        buffer_size -= write_n;
        cur_row_.cells_[i].set_string(ObLongTextType, ObString(write_n, buffer - write_n));
        break;
      }
    }
  }
  return ret;
}

int ObAllVirtualMdsNodeStat::get_primary_key_ranges_()
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

        if (OB_FAIL(tenant_ranges_.push_back(ObTuple<uint64_t, uint64_t>(tenant_low, tenant_high)))) {
          MDS_LOG(WARN, "fail to push back", KR(ret), K(*this));
        } else if (OB_SUCCESS != (ret =
        (ls_ranges_.push_back(ObTuple<share::ObLSID, share::ObLSID>(ls_low, ls_high))))) {
          MDS_LOG(WARN, "fail to push back", KR(ret), K(*this));
        } else {
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

bool ObAllVirtualMdsNodeStat::in_selected_points_(common::ObTabletID tablet_id)
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

int ObAllVirtualMdsNodeStat::get_tablet_info_(ObLS &ls, const ObFunction<int(ObTablet &)> &apply_on_tablet_op)
{
  int ret = OB_SUCCESS;
  if (!apply_on_tablet_op.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    MDS_LOG(ERROR, "invalid ob function", KR(ret), K(key_ranges_), K(*this));
  } else {
    for (int64_t idx = 0; idx < tablet_points_.count() && OB_SUCC(ret); ++idx) {
      ObTabletHandle tablet_handle;
      if (OB_FAIL(ls.get_tablet(tablet_points_[idx], tablet_handle, 0, storage::ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
        MDS_LOG(WARN, "fail to get tablet", KR(ret), K(key_ranges_), K(*this));
      } else if (OB_ISNULL(tablet_handle.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        MDS_LOG(ERROR, "get null tablet ptr", KR(ret), K(key_ranges_), K(*this));
      } else if (OB_FAIL(apply_on_tablet_op(*tablet_handle.get_obj()))) {
        MDS_LOG(WARN, "fail to apply op on tablet", KR(ret), K(key_ranges_), K(*this));
      }
    }
  }
  MDS_LOG(INFO, "get_tablet_info_", KR(ret), K(key_ranges_), K(*this));
  return ret;
}

}
}
