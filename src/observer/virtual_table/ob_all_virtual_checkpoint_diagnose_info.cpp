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

#include "ob_all_virtual_checkpoint_diagnose_info.h"
#include "storage/checkpoint/ob_checkpoint_diagnose.h"

namespace oceanbase
{
using namespace share;
using namespace storage;
using namespace common;
using namespace omt;
using namespace checkpoint;
namespace observer
{

bool ObAllVirtualCheckpointDiagnoseInfo::is_need_process(uint64_t tenant_id)
{
  if (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_) {
    return true;
  }
  return false;
}

int GenerateTraceRow::operator()(const ObTraceInfo &trace_info) const
{
  int ret = OB_SUCCESS;
  char ip_buf[common::OB_IP_STR_BUFF];
  for (int64_t i = 0; OB_SUCC(ret) && i < virtual_table_.output_column_ids_.count(); ++i) {
    uint64_t col_id = virtual_table_.output_column_ids_.at(i);
    switch (col_id) {
      // tenant_id
      case OB_APP_MIN_COLUMN_ID:
        virtual_table_.cur_row_.cells_[i].set_int(MTL_ID());
        break;
      // svr_ip
      case OB_APP_MIN_COLUMN_ID + 1:
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
      case OB_APP_MIN_COLUMN_ID + 2:
        virtual_table_.cur_row_.cells_[i].set_int(virtual_table_.addr_.get_port());
        break;
      // ls_id
      case OB_APP_MIN_COLUMN_ID + 3:
        virtual_table_.cur_row_.cells_[i].set_int(trace_info.ls_id_.id());
        break;
      // trace_id
      case OB_APP_MIN_COLUMN_ID + 4:
        virtual_table_.cur_row_.cells_[i].set_int(trace_info.trace_id_);
        break;
      // freeze_clock
      case OB_APP_MIN_COLUMN_ID + 5:
        virtual_table_.cur_row_.cells_[i].set_uint32(trace_info.freeze_clock_);
        break;
      // checkpoint_thread_name
      case OB_APP_MIN_COLUMN_ID + 6:
        virtual_table_.cur_row_.cells_[i].set_varchar(trace_info.thread_name_);
        virtual_table_.cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      // checkpoint_start_time
      case OB_APP_MIN_COLUMN_ID + 7:
        virtual_table_.cur_row_.cells_[i].set_timestamp(trace_info.checkpoint_start_time_);
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

int ObAllVirtualCheckpointDiagnoseInfo::process_curr_tenant(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (!start_to_read_) {
    SERVER_LOG(INFO, "__all_virtual_checkpoint_diagnose_info start");
    if(OB_FAIL(MTL(ObCheckpointDiagnoseMgr*)->read_trace_info(GenerateTraceRow(*this)))) {
      SERVER_LOG(WARN, "failed to read trace info", K(ret), K(cur_row_));

    } else {
      scanner_it_ = scanner_.begin();
      start_to_read_ = true;
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

void ObAllVirtualCheckpointDiagnoseInfo::release_last_tenant()
{
  scanner_.reuse();
  start_to_read_ = false;
}

}
}
