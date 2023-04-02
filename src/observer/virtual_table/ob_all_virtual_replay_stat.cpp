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

#include "ob_all_virtual_replay_stat.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "logservice/ob_log_service.h"

namespace oceanbase
{
namespace observer
{
int ObAllVirtualReplayStat::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (false == start_to_read_) {
    auto func_iter_ls = [&](const logservice::ObReplayStatus &replay_status) -> int
    {
      int ret = OB_SUCCESS;
      logservice::LSReplayStat replay_stat;
      if (OB_FAIL(replay_status.stat(replay_stat))) {
        SERVER_LOG(WARN, "replay_status stat failed", K(ret), K(replay_status));
      } else if (OB_FAIL(insert_stat_(replay_stat))) {
        SERVER_LOG(WARN, "insert stat failed", K(ret));
      } else if (OB_FAIL(scanner_.add_row(cur_row_))) {
        SERVER_LOG(WARN, "iter replay stat failed", KR(ret), K(replay_stat));
      } else {
        SERVER_LOG(INFO, "iter replay stat succ", K(replay_stat));
      }
      return ret;
    };
    auto func_iterate_tenant = [&func_iter_ls]() -> int
    {
      int ret = OB_SUCCESS;
      logservice::ObLogService *log_service = MTL(logservice::ObLogService*);
      if (NULL == log_service) {
        SERVER_LOG(INFO, "tenant has no ObLogService", K(MTL_ID()));
      } else if (OB_FAIL(log_service->iterate_replay(func_iter_ls))) {
        SERVER_LOG(WARN, "iter ls failed", K(ret));
      } else {
        SERVER_LOG(INFO, "iter ls succ", K(ret));
      }
      return ret;
    };
    if (OB_FAIL(omt_->operate_each_tenant_for_sys_or_self(func_iterate_tenant))) {
      SERVER_LOG(WARN, "iter tenant failed", K(ret));
    } else {
      scanner_it_ = scanner_.begin();
      start_to_read_ = true;
    }
  }
  if (OB_SUCC(ret) && start_to_read_) {
    if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "get next row failed", K(ret));
      }
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

int ObAllVirtualReplayStat::insert_stat_(logservice::LSReplayStat &replay_stat)
{
  int ret = OB_SUCCESS;
  const int64_t count = output_column_ids_.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
    uint64_t col_id = output_column_ids_.at(i);
    switch (col_id) {
      case OB_APP_MIN_COLUMN_ID:
        cur_row_.cells_[i].set_int(MTL_ID());
        break;
      case OB_APP_MIN_COLUMN_ID + 1:
        cur_row_.cells_[i].set_int(replay_stat.ls_id_);
        break;
      case OB_APP_MIN_COLUMN_ID + 2:
        if (false == GCTX.self_addr().ip_to_string(ip_, common::OB_IP_PORT_STR_BUFF)) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "ip_to_string failed", K(ret));
        } else {
          cur_row_.cells_[i].set_varchar(ObString::make_string(ip_));
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        }
        break;
      case OB_APP_MIN_COLUMN_ID + 3:
        cur_row_.cells_[i].set_int(GCTX.self_addr().get_port());
        break;
      case OB_APP_MIN_COLUMN_ID + 4:
        if (OB_FAIL(role_to_string(replay_stat.role_, role_str_, sizeof(role_str_)))) {
          SERVER_LOG(WARN, "role_to_string failed", K(ret), K(replay_stat));
        } else {
          cur_row_.cells_[i].set_varchar(ObString::make_string(role_str_));
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                                ObCharset::get_default_charset()));
        }
        break;
      case OB_APP_MIN_COLUMN_ID + 5:
        cur_row_.cells_[i].set_uint64(replay_stat.end_lsn_.val_);
        break;
      case OB_APP_MIN_COLUMN_ID + 6:
        cur_row_.cells_[i].set_bool(replay_stat.enabled_);
        break;
      case OB_APP_MIN_COLUMN_ID + 7:
        //TODO:SCN
        cur_row_.cells_[i].set_uint64(replay_stat.unsubmitted_lsn_.val_);
        break;
      case OB_APP_MIN_COLUMN_ID + 8: {
        cur_row_.cells_[i].set_uint64(replay_stat.unsubmitted_scn_.get_val_for_inner_table_field());
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 9:
        cur_row_.cells_[i].set_int(replay_stat.pending_cnt_);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "unkown column");
        break;
    }
  }
  return ret;
}
} // namespace observer
} // namespace oceanbase
