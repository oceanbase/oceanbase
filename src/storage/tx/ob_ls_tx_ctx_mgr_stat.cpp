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

#include "ob_ls_tx_ctx_mgr_stat.h"
#include "ob_trans_ctx.h"

namespace oceanbase
{

using namespace common;
using namespace share;

namespace transaction
{

void ObLSTxCtxMgrStat::reset()
{
  addr_.reset();
  ls_id_.reset();
  is_master_ = false;
  is_stopped_ = false;
  state_ = -1;
  state_str_ = "INVALID";
  total_tx_ctx_count_ = 0;
  mgr_addr_ = 0;
}

//don't valid input arguments

int ObLSTxCtxMgrStat::init(const common::ObAddr &addr, const share::ObLSID &ls_id,
    const bool is_master, const bool is_stopped,
    const int64_t state, const char* state_str,
    const int64_t total_tx_ctx_count, const int64_t mgr_addr)
{
  int ret = OB_SUCCESS;

  addr_ = addr;
  ls_id_ = ls_id;
  is_master_ = is_master;
  is_stopped_ = is_stopped;
  state_ = state;
  state_str_ = state_str;
  total_tx_ctx_count_ = total_tx_ctx_count;
  mgr_addr_ = mgr_addr;

  return ret;
}

} // transaction
} // oceanbase
