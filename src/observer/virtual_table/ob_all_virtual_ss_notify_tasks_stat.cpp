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

#include "ob_all_virtual_ss_notify_tasks_stat.h"
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
#ifdef OB_BUILD_SHARED_STORAGE
#include "close_modules/shared_storage/storage/incremental/sslog/notify/ob_sslog_notify_service.h"
#endif

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

int ObAllVirtualSSNotifyTasksStat::IterNodeOp::operator()(sslog::ObSSLogNotifyTask &notify_task,
                                                          sslog::ObSSLogNotifyTaskQueue &queue)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_SHARED_STORAGE
  if (OB_FAIL(this_->convert_node_info_to_row_(state_,
                                               notify_task,
                                               temp_buffer_,
                                               ObAllVirtualSSNotifyTasksStat::BUFFER_SIZE,
                                               this_->cur_row_))) {
    SSLOG_LOG(WARN, "fail to convert node info", K(ret), K(MTL_ID()), K(notify_task));
  } else if (OB_FAIL(this_->scanner_.add_row(this_->cur_row_))) {
    if (OB_SIZE_OVERFLOW == ret) {
      SSLOG_LOG(INFO, "scanner is full", K(ret), K(MTL_ID()), K(notify_task));
    } else {
      SSLOG_LOG(WARN, "fail to add_row to scanner_", K(ret), K(MTL_ID()), K(notify_task));
    }
  }
#endif
  return ret;
}

int ObAllVirtualSSNotifyTasksStat::IterateTenantOp::operator()()
{
  IterNodeOp for_each_op(this_, temp_buffer_);
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_SHARED_STORAGE
  sslog::ObSSLogNotifyService *notify_srv = MTL(sslog::ObSSLogNotifyService *);
  if (OB_NOT_NULL(notify_srv)) {
    // waiting queue
    {
      ObByteLockGuard lg1(notify_srv->waiting_queue_.lock_);
      if (FALSE_IT(for_each_op.state_ = "WAIT")) {
      } else if (OB_FAIL(notify_srv->waiting_queue_.for_each_(for_each_op))) {
        SSLOG_LOG(WARN, "failed to do for_each", KR(ret));
      }
    }
    // ready queue
    {
      ObByteLockGuard lg2(notify_srv->ready_queue_.lock_);
      if (FALSE_IT(for_each_op.state_ = "READY")) {
      } else if (OB_FAIL(notify_srv->ready_queue_.for_each_(for_each_op))) {
        SSLOG_LOG(WARN, "failed to do for_each", KR(ret));
      }
    }
    // retire queue
    {
      ObByteLockGuard lg3(notify_srv->retire_queue_.lock_);
      if (FALSE_IT(for_each_op.state_ = "RETIRE")) {
      } else if (OB_FAIL(notify_srv->retire_queue_.for_each_(for_each_op))) {
        SSLOG_LOG(WARN, "failed to do for_each", KR(ret));
      }
    }
  }
#endif
  return ret;
}

int ObAllVirtualSSNotifyTasksStat::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (false == start_to_read_) {
    char *temp_buffer = nullptr;
    char *to_string_buffer = nullptr;
    constexpr int64_t BUFFER_SIZE = 32_MB;
    if (OB_ISNULL(temp_buffer = (char *)mtl_malloc(ObAllVirtualSSNotifyTasksStat::BUFFER_SIZE, "VirSSNotify"))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SSLOG_LOG(WARN, "fail to alloc buffer", K(MTL_ID()), K(*this));
    } else {
      IterateTenantOp func_iterate_tenant(this, temp_buffer);
      if (OB_FAIL(omt_->operate_each_tenant_for_sys_or_self(func_iterate_tenant))) {
        if (OB_SIZE_OVERFLOW == ret) {
          SSLOG_LOG(INFO, "scanner is full", K(ret), K(*this));
          // rewrite ret to success
          ret = OB_SUCCESS;
        } else {
          SSLOG_LOG(WARN, "ObMultiTenant operate_each_tenant_for_sys_or_self failed", K(ret), K(*this));
        }
      } else {
        // do nothing
      }
      if (OB_SUCCESS == ret) {
        scanner_it_ = scanner_.begin();
        start_to_read_ = true;
      }
      mtl_free(temp_buffer);
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

int ObAllVirtualSSNotifyTasksStat::convert_node_info_to_row_(const char *state,
                                                             const sslog::ObSSLogNotifyTask &notify_task,
                                                             char *buffer,
                                                             int64_t buffer_size,
                                                             common::ObNewRow &row)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_SHARED_STORAGE
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
        share::ObLSID ls_id;
        if (OB_FAIL(notify_task.meta_key_.get_ls_id(ls_id))) {
          cur_row_.cells_[i].set_int(ret);
          SSLOG_LOG(WARN, "get ls id failed", KR(ret), K(notify_task));
          ret = OB_SUCCESS;
        } else {
          cur_row_.cells_[i].set_int(ls_id.id());
        }
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 2: {// tablet_id
        sslog::ObSSLogNotifyTabletKey tablet_meta_key;
        if (notify_task.meta_key_.get_meta_type() == ObSSLogMetaType::SSLOG_TABLET_META) {
          if (OB_FAIL(notify_task.meta_key_.get_meta_key(tablet_meta_key))) {
            cur_row_.cells_[i].set_int(ret);
            SSLOG_LOG(WARN, "get tablet meta key failed", KR(ret), K(notify_task));
            ret = OB_SUCCESS;
          } else {
            cur_row_.cells_[i].set_int(tablet_meta_key.get_tablet_id().id());
          }
        } else {
          cur_row_.cells_[i].set_int(-1);
        }
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 3: {// idx
        cur_row_.cells_[i].set_int(notify_task.idx_);
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 4: {// svr_ip
        if (false == (GCTX.self_addr().ip_to_string(ip_buffer_, IP_BUFFER_SIZE))) {
          ret = OB_ERR_UNEXPECTED;
          SSLOG_LOG(WARN, "ip_to_string failed", KR(ret), K(notify_task));
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
      case OB_APP_MIN_COLUMN_ID + 6: {// state
        cur_row_.cells_[i].set_string(ObLongTextType, ObString(state));
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 7: {// type
        cur_row_.cells_[i].set_string(ObLongTextType, ObString(sslog::obj_to_string(notify_task.meta_key_.get_meta_type())));
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 8: {// reorganization_scn
        sslog::ObSSLogNotifyTabletKey tablet_meta_key;
        if (notify_task.meta_key_.get_meta_type() == ObSSLogMetaType::SSLOG_TABLET_META) {
          if (OB_FAIL(notify_task.meta_key_.get_meta_key(tablet_meta_key))) {
            cur_row_.cells_[i].set_int(ret);
            SSLOG_LOG(WARN, "get tablet meta key failed", KR(ret), K(notify_task));
            ret = OB_SUCCESS;
          } else {
            cur_row_.cells_[i].set_int(tablet_meta_key.get_transfer_scn().get_val_for_tx(true));
          }
        } else {
          cur_row_.cells_[i].set_int(-1);
        }
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 9: {// acquire_sslog_scn
        cur_row_.cells_[i].set_int(notify_task.acquire_sslog_ls_max_consequent_scn_.get_val_for_tx(true));
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 10: {// acquire_aim_ls_scn
        cur_row_.cells_[i].set_int(notify_task.acquire_aim_ls_max_consequent_scn_.get_val_for_tx(true));
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 11: {// sslog_kv_commit_version
        cur_row_.cells_[i].set_int(notify_task.sslog_kv_commit_version_.get_val_for_tx(true));
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 12: {// notify_path
        cur_row_.cells_[i].set_string(ObLongTextType, ObString(sslog::obj_to_string(notify_task.notify_path_)));
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 13: {// generate_ts
        cur_row_.cells_[i].set_timestamp(notify_task.debug_info_.generate_ts_);
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 14: {// enqueue_ts
        cur_row_.cells_[i].set_timestamp(notify_task.debug_info_.enqueue_ts_);
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 15: {// ready_ts
        cur_row_.cells_[i].set_timestamp(notify_task.debug_info_.ready_ts_);
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 16: {// consume_ts
        cur_row_.cells_[i].set_timestamp(notify_task.debug_info_.consume_ts_);
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 17: {// retire_reason
        cur_row_.cells_[i].set_string(ObLongTextType, ObString(sslog::obj_to_string(notify_task.debug_info_.retire_reason_)));
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 18: {// err_ret
        cur_row_.cells_[i].set_int(notify_task.debug_info_.err_ret_);
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 19: {// retry_cnt
        cur_row_.cells_[i].set_int(notify_task.debug_info_.retry_cnt_);
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

}
}
