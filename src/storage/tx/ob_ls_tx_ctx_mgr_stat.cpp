/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_ls_tx_ctx_mgr_stat.h"

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
  total_tx_ctx_count_ = 0;
  mgr_addr_ = 0;
}

//don't valid input arguments

int ObLSTxCtxMgrStat::init(const common::ObAddr &addr, const share::ObLSID &ls_id,
    const bool is_master, const bool is_stopped,
    const int64_t state,
    const int64_t total_tx_ctx_count, const int64_t mgr_addr)
{
  int ret = OB_SUCCESS;

  addr_ = addr;
  ls_id_ = ls_id;
  is_master_ = is_master;
  is_stopped_ = is_stopped;
  state_ = state;
  total_tx_ctx_count_ = total_tx_ctx_count;
  mgr_addr_ = mgr_addr;

  return ret;
}

} // transaction
} // oceanbase
