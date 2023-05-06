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

#define USING_LOG_PREFIX OBLOG
#include "ob_log_ls_info.h"
#include "ob_log_config.h"            // TCONF

using namespace oceanbase::logfetcher;

namespace oceanbase
{
namespace libobcdc
{
void ObLogLSInfo::reset()
{
  ctx_.sv_.state_ = PART_STATE_INVALID;
  ctx_.sv_.trans_count_ = 0;
  tls_id_.reset();
  serve_info_.reset();
}

int ObLogLSInfo::init(const logservice::TenantLSID &tls_id,
    const bool start_serve_from_create,
    const int64_t start_tstamp,
    const bool is_served)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! is_invalid())) {
    LOG_ERROR("invalid state which is not INVALID", "state", print_state());
    ret = OB_STATE_NOT_MATCH;
  } else {
    tls_id_ = tls_id;
    serve_info_.reset(start_serve_from_create, start_tstamp);

    // set state to NORMAL
    ctx_.sv_.trans_count_ = 0;
    ctx_.sv_.state_ = is_served ? PART_STATE_NORMAL : PART_STATE_NOT_SERVED;;
  }
  return ret;
}

bool ObLogLSInfo::operator<(const ObLogLSInfo &other) const
{
  return tls_id_ < other.tls_id_;
}

bool ObLogLSInfo::offline(int64_t &end_trans_count)
{
  bool bool_ret = false;
  Ctx cur_ctx = ctx_;

  while (PART_STATE_OFFLINE != cur_ctx.sv_.state_) {
    Ctx old_ctx = cur_ctx;
    Ctx new_ctx = cur_ctx;
    new_ctx.sv_.state_ = PART_STATE_OFFLINE;  // No change in number of transactions, status changed to offline

    cur_ctx.iv_ = ATOMIC_CAS(&ctx_.iv_, old_ctx.iv_, new_ctx.iv_);

    if (old_ctx.iv_ == cur_ctx.iv_) {
      bool_ret = true;
      end_trans_count = cur_ctx.sv_.trans_count_;

      // CAS is successful, update current value
      cur_ctx.iv_ = new_ctx.iv_;
    }
  }

  return bool_ret;
}

void ObLogLSInfo::inc_trans_count_on_serving(bool &is_serving)
{
  Ctx cur_ctx = ctx_;
  bool done = false;

  is_serving = false;
  while (! done && is_serving_state_(cur_ctx.sv_.state_)) {
    Ctx old_ctx = cur_ctx;
    Ctx new_ctx = cur_ctx;
    new_ctx.sv_.trans_count_++;   // Status unchanged, number of transactions plus one

    cur_ctx.iv_ = ATOMIC_CAS(&ctx_.iv_, old_ctx.iv_, new_ctx.iv_);

    if (cur_ctx.iv_ == old_ctx.iv_) {
      done = true;
      is_serving = true;
    }
  }
}

int ObLogLSInfo::dec_trans_count(bool &need_remove)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_invalid())) {
    LOG_ERROR("invalid state", "state", print_state(), "state", ctx_.sv_.state_);
    ret = OB_STATE_NOT_MATCH;
  } else {
    Ctx cur_ctx = ctx_;
    bool done = false;

    while (OB_SUCCESS == ret && !done) {
      Ctx old_ctx = cur_ctx;
      Ctx new_ctx = cur_ctx;
      new_ctx.sv_.trans_count_--; // No change in status, number of transactions minus 1

      if (OB_UNLIKELY(new_ctx.sv_.trans_count_ < 0)) {
        LOG_ERROR("transaction count will become invalid, unexcepted",
            "state", print_state(), "trans_count", new_ctx.sv_.trans_count_);
        ret = OB_ERR_UNEXPECTED;
      } else {
        cur_ctx.iv_ = ATOMIC_CAS(&ctx_.iv_, old_ctx.iv_, new_ctx.iv_);

        if (old_ctx.iv_ == cur_ctx.iv_) {
          done = true;
          // If the transaction is offline and the transaction count is 0, then it needs to be deleted
          need_remove = ((PART_STATE_OFFLINE == new_ctx.sv_.state_) && (0 == new_ctx.sv_.trans_count_));
        }
      }
    }
  }
  return ret;
}

const char *ObLogLSInfo::print_state() const
{
  const char *ret = nullptr;

  switch (ctx_.sv_.state_) {
    case PART_STATE_INVALID: {
      ret = "INVALID";
      break;
    }
    case PART_STATE_NORMAL: {
      ret = "NORMAL";
      break;
    }
    case PART_STATE_OFFLINE: {
      ret = "OFFLINE";
      break;
    }
    case PART_STATE_NOT_SERVED: {
      ret = "NOT_SERVED";
      break;
    }
    default: {
      ret = "UNKNOWN";
      break;
    }
  }

  return ret;
}

bool LSInfoPrinter::operator()(
    const logservice::TenantLSID &tls_id,
    ObLogLSInfo *ls_info)
{
  if (tls_id.get_tenant_id() == tenant_id_) {
    if (OB_ISNULL(ls_info)) {
      LOG_ERROR_RET(OB_INVALID_ARGUMENT, "ls_info is invalid", K(tls_id), K(ls_info));
    } else if (ls_info->is_offline()) {
      offline_ls_count_++;
    } else if (ls_info->is_not_serving()) {
      not_served_ls_count_++;
    } else {
      serving_ls_count_++;
    }

    // TODO name
    if (TCONF.print_ls_serve_info) {
      // TODO
      LS_ISTAT(ls_info, "[SERVE_INFO]");
    }
  }
  return true;
}

bool LSInfoScannerByTenant::operator()(
    const logservice::TenantLSID &tls_id,
    ObLogLSInfo *ls_info)
{
  int ret = OB_SUCCESS;

  if (tls_id.get_tenant_id() == tenant_id_) {
    if (OB_FAIL(ls_array_.push_back(tls_id))) {
      LOG_ERROR("push LS key into array fail",
          KR(ret), K(tls_id), KPC(ls_info), K(ls_array_));
    }
  }

  return OB_SUCCESS == ret;
}

} // namespace XXX
} // namespace oceanbase
