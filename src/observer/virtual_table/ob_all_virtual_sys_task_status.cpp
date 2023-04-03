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

#include "ob_all_virtual_sys_task_status.h"
#include "lib/profile/ob_trace_id.h"
#include "observer/ob_server.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
using namespace common;
using namespace share;

namespace observer
{

ObAllVirtualSysTaskStatus::ObAllVirtualSysTaskStatus()
  : iter_(),
    task_id_(),
    svr_ip_(),
    comment_()
{
}

ObAllVirtualSysTaskStatus::~ObAllVirtualSysTaskStatus()
{
}

int ObAllVirtualSysTaskStatus::init(ObSysTaskStatMgr &status_mgr)
{
  int ret = OB_SUCCESS;

  if (start_to_read_) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "cannot init twice", K(ret));
  } else if (OB_FAIL(status_mgr.get_iter(iter_))) {
    SERVER_LOG(WARN, "failed to get iter", K(ret));
  } else {
    start_to_read_ = true;
  }
  return ret;
}

int ObAllVirtualSysTaskStatus::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObSysTaskStat status;
  row = NULL;

  if (!start_to_read_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not inited", K(ret));
  } else if (!iter_.is_ready()) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "iter not ready", K(ret));
  } else if (OB_FAIL(iter_.get_next(status))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "failed to get next status", K(ret));
    }
  } else {
    const int64_t col_count = output_column_ids_.count();
    ObCollationType collcation_type = ObCharset::get_default_collation(ObCharset::get_default_charset());
     for (int64_t i = 0; OB_SUCC(ret) && i < col_count; i++) {
      uint64_t col_id = output_column_ids_.at(i);
      switch(col_id) {
        case OB_APP_MIN_COLUMN_ID: {
          // start_time
          cur_row_.cells_[i].set_timestamp(status.start_time_);
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 1: {
          // task_type
          const char *task_type = sys_task_type_to_str(status.task_type_);
          cur_row_.cells_[i].set_varchar(task_type);
          cur_row_.cells_[i].set_collation_type(collcation_type);
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 2: {
          // task_id
          int64_t n = status.task_id_.to_string(task_id_, sizeof(task_id_));
          if (n < 0 || n >= sizeof(task_id_)) {
            ret = OB_BUF_NOT_ENOUGH;
            SERVER_LOG(WARN, "buffer not enough", K(ret));
          } else {
            cur_row_.cells_[i].set_varchar(task_id_);
            cur_row_.cells_[i].set_collation_type(collcation_type);
          }
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 3: {
          // svr_ip
          if (!status.svr_ip_.ip_to_string(svr_ip_, sizeof(svr_ip_))) {
            ret = OB_BUF_NOT_ENOUGH;
            SERVER_LOG(WARN, "buffer not enough", K(ret));
          } else {
            cur_row_.cells_[i].set_varchar(svr_ip_);
            cur_row_.cells_[i].set_collation_type(collcation_type);
          }
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 4: {
          // svr_port
          cur_row_.cells_[i].set_int(status.svr_ip_.get_port());
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 5: {
          // tenant_id
          cur_row_.cells_[i].set_int(status.tenant_id_);
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 6: {
          // comment, ignore ret
          snprintf(comment_, sizeof(comment_), "%s", status.comment_);
          cur_row_.cells_[i].set_varchar(comment_);
          cur_row_.cells_[i].set_collation_type(collcation_type);
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 7: {
          // is_cancel
          cur_row_.cells_[i].set_int(status.is_cancel_);
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(ERROR, "invalid coloum_id", K(ret), K(col_id));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    row = &cur_row_;
  }
  return ret;
}

void ObAllVirtualSysTaskStatus::reset()
{
  iter_.reset();
  ObVirtualTableScannerIterator::reset();
}
}//observer
}//oceanbase
