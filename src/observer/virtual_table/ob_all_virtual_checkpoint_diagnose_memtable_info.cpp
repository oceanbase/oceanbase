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

#include "ob_all_virtual_checkpoint_diagnose_memtable_info.h"

namespace oceanbase
{
using namespace share;
using namespace storage;
using namespace common;
using namespace omt;
using namespace checkpoint;
namespace observer
{
int ObAllVirtualCheckpointDiagnoseMemtableInfo::get_primary_key_()
{
  int ret = OB_SUCCESS;
  if (key_ranges_.count() != 1) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "only support select a single trace, multiple range select is ");
    SERVER_LOG(WARN, "invalid key ranges", KR(ret));
  } else {
    ObNewRange &key_range = key_ranges_.at(0);
    if (OB_UNLIKELY(key_range.get_start_key().get_obj_cnt() != 2 || key_range.get_end_key().get_obj_cnt() != 2)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(ERROR,
                 "unexpected key_ranges_ of rowkey columns",
                 KR(ret),
                 "size of start key",
                 key_range.get_start_key().get_obj_cnt(),
                 "size of end key",
                 key_range.get_end_key().get_obj_cnt());
    } else {
      ObObj tenant_obj_low = (key_range.get_start_key().get_obj_ptr()[0]);
      ObObj tenant_obj_high = (key_range.get_end_key().get_obj_ptr()[0]);
      ObObj trace_obj_low = (key_range.get_start_key().get_obj_ptr()[1]);
      ObObj trace_obj_high = (key_range.get_end_key().get_obj_ptr()[1]);

      uint64_t tenant_low = tenant_obj_low.is_min_value() ? 0 : tenant_obj_low.get_uint64();
      uint64_t tenant_high = tenant_obj_high.is_max_value() ? UINT64_MAX : tenant_obj_high.get_uint64();
      uint64_t trace_id_low = trace_obj_low.is_min_value() ? 0 : trace_obj_low.get_int();
      uint64_t trace_id_high = trace_obj_high.is_max_value() ? 0 : trace_obj_high.get_int();
      if (tenant_low != tenant_high || trace_id_low != trace_id_high) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant id and trace_id must be specified. range select is ");
        SERVER_LOG(WARN,
                   "only support point select.",
                   KR(ret),
                   K(tenant_low),
                   K(tenant_high),
                   K(trace_id_low),
                   K(trace_id_high));
      } else {
        trace_id_ = trace_id_low;
        if (is_sys_tenant(effective_tenant_id_)
            || effective_tenant_id_ == tenant_low) {
          tenant_id_ = tenant_low;
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant id must not be specified to other tenant in user tenant. this is ");
          SERVER_LOG(WARN,
                     "tenant id must not be specified to other tenant in user tenant",
                     KR(ret),
                     K(effective_tenant_id_),
                     K(trace_id_low),
                     K(tenant_low));
        }
      }
    }
  }
  return ret;
}

int GenerateMemtableRow::operator()(const ObTraceInfo &trace_info,
    const ObCheckpointDiagnoseKey &key,
    const ObMemtableDiagnoseInfo &memtable_diagnose_info) const
{
  int ret = OB_SUCCESS;
  char ip_buf[common::OB_IP_STR_BUFF];
  char ptr_buf[16];
  int64_t pos = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < virtual_table_.output_column_ids_.count(); ++i) {
    uint64_t col_id = virtual_table_.output_column_ids_.at(i);
    switch (col_id) {
      // tenant_id
      case OB_APP_MIN_COLUMN_ID:
        virtual_table_.cur_row_.cells_[i].set_int(MTL_ID());
        break;
      // trace_id
      case OB_APP_MIN_COLUMN_ID + 1:
        virtual_table_.cur_row_.cells_[i].set_int(trace_info.trace_id_);
        break;
      // svr_ip
      case OB_APP_MIN_COLUMN_ID + 2:
        MEMSET(ip_buf, 0, common::OB_IP_STR_BUFF);
        if (virtual_table_.addr_.ip_to_string(ip_buf, sizeof(ip_buf))) {
          virtual_table_.cur_row_.cells_[i].set_varchar(ip_buf);
          virtual_table_.cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        } else {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "fail to execute ip_to_string", KR(ret));
        }
        break;
      // svr_port
      case OB_APP_MIN_COLUMN_ID + 3:
        virtual_table_.cur_row_.cells_[i].set_int(virtual_table_.addr_.get_port());
        break;
      // ls_id
      case OB_APP_MIN_COLUMN_ID + 4:
        virtual_table_.cur_row_.cells_[i].set_int(trace_info.ls_id_.id());
        break;
      // checkpoint_thread_name
      case OB_APP_MIN_COLUMN_ID + 5:
          virtual_table_.cur_row_.cells_[i].set_varchar(trace_info.thread_name_);
          virtual_table_.cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      // checkpoint_start_time
      case OB_APP_MIN_COLUMN_ID + 6:
        virtual_table_.cur_row_.cells_[i].set_timestamp(trace_info.checkpoint_start_time_);
        break;
      // tablet_id
      case OB_APP_MIN_COLUMN_ID + 7:
        virtual_table_.cur_row_.cells_[i].set_int(key.tablet_id_.id());
        break;
      // ptr
      case OB_APP_MIN_COLUMN_ID + 8:
        MEMSET(ptr_buf, 0, 16);
        databuff_print_obj(ptr_buf, 16, pos, key.checkpoint_unit_ptr_);
        virtual_table_.cur_row_.cells_[i].set_varchar(ptr_buf);
        virtual_table_.cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      // start_scn
      case OB_APP_MIN_COLUMN_ID + 9:
        virtual_table_.cur_row_.cells_[i].set_uint64(memtable_diagnose_info.start_scn_.get_val_for_inner_table_field());
        break;
      // end_scn
      case OB_APP_MIN_COLUMN_ID + 10:
        virtual_table_.cur_row_.cells_[i].set_uint64(memtable_diagnose_info.end_scn_.get_val_for_inner_table_field());
        break;
      // rec_scn
      case OB_APP_MIN_COLUMN_ID + 11:
        virtual_table_.cur_row_.cells_[i].set_uint64(memtable_diagnose_info.rec_scn_.get_val_for_inner_table_field());
        break;
      // create_flush_dag_time
      case OB_APP_MIN_COLUMN_ID + 12:
        virtual_table_.cur_row_.cells_[i].set_timestamp(memtable_diagnose_info.create_flush_dag_time_);
        break;
      // merge_finish_time
      case OB_APP_MIN_COLUMN_ID + 13:
        virtual_table_.cur_row_.cells_[i].set_timestamp(memtable_diagnose_info.merge_finish_time_);
        break;
      // start_gc_time
      case OB_APP_MIN_COLUMN_ID + 14:
        virtual_table_.cur_row_.cells_[i].set_timestamp(memtable_diagnose_info.start_gc_time_);
        break;
      // frozen_finish_time
      case OB_APP_MIN_COLUMN_ID + 15:
        virtual_table_.cur_row_.cells_[i].set_timestamp(memtable_diagnose_info.frozen_finish_time_);
        break;
      // merge_start_time
      case OB_APP_MIN_COLUMN_ID + 16:
        virtual_table_.cur_row_.cells_[i].set_timestamp(memtable_diagnose_info.merge_start_time_);
        break;
      // release_time
      case OB_APP_MIN_COLUMN_ID + 17:
        virtual_table_.cur_row_.cells_[i].set_timestamp(memtable_diagnose_info.release_time_);
        break;
      // memtable_occupy_size
      case OB_APP_MIN_COLUMN_ID + 18:
        virtual_table_.cur_row_.cells_[i].set_int(memtable_diagnose_info.memtable_occupy_size_);
        break;
      // occupy_size
      case OB_APP_MIN_COLUMN_ID + 19:
        virtual_table_.cur_row_.cells_[i].set_int(memtable_diagnose_info.occupy_size_);
        break;
      // concurrent_cnt
      case OB_APP_MIN_COLUMN_ID + 20:
        virtual_table_.cur_row_.cells_[i].set_int(memtable_diagnose_info.concurrent_cnt_);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "unexpected type", KR(ret), K(col_id));
        break;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(virtual_table_.scanner_.add_row(virtual_table_.cur_row_))) {
      SERVER_LOG(WARN, "failed to add row", K(ret), K(virtual_table_.cur_row_));
      if (OB_SIZE_OVERFLOW == ret) {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObAllVirtualCheckpointDiagnoseMemtableInfo::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (!start_to_read_) {
    SERVER_LOG(INFO, "__all_virtual_checkpoint_diagnose_memtable_info start");
    if (OB_FAIL(get_primary_key_())) {
      SERVER_LOG(WARN, "failed to get_primary_key", KR(ret));
    } else {
      MTL_SWITCH(tenant_id_)
      {
        if (OB_FAIL(MTL(ObCheckpointDiagnoseMgr*)->read_diagnose_info
              <ObMemtableDiagnoseInfo>(trace_id_, GenerateMemtableRow(*this)))) {
          SERVER_LOG(WARN, "failed to read trace info", K(ret), K(cur_row_));
        } else {
          scanner_it_ = scanner_.begin();
          start_to_read_ = true;
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "fail to get next row", K(ret));
    }
  } else {
    row = &cur_row_;
  }
  return ret;
}

int GenerateCheckpointUnitRow::operator()(const storage::checkpoint::ObTraceInfo &trace_info,
    const storage::checkpoint::ObCheckpointDiagnoseKey &key,
    const storage::checkpoint::ObCheckpointUnitDiagnoseInfo &checkpoint_unit_diagnose_info) const
{
  int ret = OB_SUCCESS;
  char ip_buf[common::OB_IP_STR_BUFF];
  char ptr_buf[16];
  int64_t pos = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < virtual_table_.output_column_ids_.count(); ++i) {
    uint64_t col_id = virtual_table_.output_column_ids_.at(i);
    switch (col_id) {
      // tenant_id
      case OB_APP_MIN_COLUMN_ID:
        virtual_table_.cur_row_.cells_[i].set_int(MTL_ID());
        break;
      // trace_id
      case OB_APP_MIN_COLUMN_ID + 1:
        virtual_table_.cur_row_.cells_[i].set_int(trace_info.trace_id_);
        break;
      // svr_ip
      case OB_APP_MIN_COLUMN_ID + 2:
        MEMSET(ip_buf, 0, common::OB_IP_STR_BUFF);
        if (virtual_table_.addr_.ip_to_string(ip_buf, sizeof(ip_buf))) {
          virtual_table_.cur_row_.cells_[i].set_varchar(ip_buf);
          virtual_table_.cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        } else {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "fail to execute ip_to_string", KR(ret));
        }
        break;
      // svr_port
      case OB_APP_MIN_COLUMN_ID + 3:
        virtual_table_.cur_row_.cells_[i].set_int(virtual_table_.addr_.get_port());
        break;
      // ls_id
      case OB_APP_MIN_COLUMN_ID + 4:
        virtual_table_.cur_row_.cells_[i].set_int(trace_info.ls_id_.id());
        break;
      // checkpoint_thread_name
      case OB_APP_MIN_COLUMN_ID + 5:
          virtual_table_.cur_row_.cells_[i].set_varchar(trace_info.thread_name_);
          virtual_table_.cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      // checkpoint_start_time
      case OB_APP_MIN_COLUMN_ID + 6:
        virtual_table_.cur_row_.cells_[i].set_timestamp(trace_info.checkpoint_start_time_);
        break;
      // tablet_id
      case OB_APP_MIN_COLUMN_ID + 7:
        virtual_table_.cur_row_.cells_[i].set_int(key.tablet_id_.id());
        break;
      // ptr
      case OB_APP_MIN_COLUMN_ID + 8:
        MEMSET(ptr_buf, 0, 16);
        databuff_print_obj(ptr_buf, 16, pos, key.checkpoint_unit_ptr_);
        virtual_table_.cur_row_.cells_[i].set_varchar(ptr_buf);
        virtual_table_.cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      // start_scn
      case OB_APP_MIN_COLUMN_ID + 9:
        virtual_table_.cur_row_.cells_[i].set_uint64(checkpoint_unit_diagnose_info.start_scn_.get_val_for_inner_table_field());
        break;
      // end_scn
      case OB_APP_MIN_COLUMN_ID + 10:
        virtual_table_.cur_row_.cells_[i].set_uint64(checkpoint_unit_diagnose_info.end_scn_.get_val_for_inner_table_field());
        break;
      // rec_scn
      case OB_APP_MIN_COLUMN_ID + 11:
        virtual_table_.cur_row_.cells_[i].set_uint64(checkpoint_unit_diagnose_info.rec_scn_.get_val_for_inner_table_field());
        break;
      // create_flush_dag_time
      case OB_APP_MIN_COLUMN_ID + 12:
        virtual_table_.cur_row_.cells_[i].set_timestamp(checkpoint_unit_diagnose_info.create_flush_dag_time_);
        break;
      // merge_finish_time
      case OB_APP_MIN_COLUMN_ID + 13:
        virtual_table_.cur_row_.cells_[i].set_timestamp(checkpoint_unit_diagnose_info.merge_finish_time_);
        break;
      // start_gc_time
      case OB_APP_MIN_COLUMN_ID + 14:
        virtual_table_.cur_row_.cells_[i].set_timestamp(checkpoint_unit_diagnose_info.start_gc_time_);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "unexpected type", KR(ret), K(col_id));
        break;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(virtual_table_.scanner_.add_row(virtual_table_.cur_row_))) {
      SERVER_LOG(WARN, "failed to add row", K(ret), K(virtual_table_.cur_row_));
      if (OB_SIZE_OVERFLOW == ret) {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}
int ObAllVirtualCheckpointDiagnoseCheckpointUnitInfo::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (!start_to_read_) {
    SERVER_LOG(INFO, "__all_virtual_checkpoint_diagnose_checkpoint_unit_info start");
    if (OB_FAIL(get_primary_key_())) {
      SERVER_LOG(WARN, "failed to get_primary_key", KR(ret));
    } else {
      MTL_SWITCH(tenant_id_)
      {
        if (OB_FAIL(MTL(ObCheckpointDiagnoseMgr*)->read_diagnose_info
              <ObCheckpointUnitDiagnoseInfo>(trace_id_, GenerateCheckpointUnitRow(*this)))) {
          SERVER_LOG(WARN, "failed to read trace info", K(ret), K(cur_row_));
        } else {
          scanner_it_ = scanner_.begin();
          start_to_read_ = true;
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "fail to get next row", K(ret));
    }
  } else {
    row = &cur_row_;
  }
  return ret;
}

}
}
