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

#ifndef OCEANBASE_TRANSACTION_OB_LS_TX_CTX_MGR_STAT_
#define OCEANBASE_TRANSACTION_OB_LS_TX_CTX_MGR_STAT_

#include "ob_trans_define.h"
#include "common/ob_range.h"

namespace oceanbase
{
namespace transaction
{
class ObLSTxCtxMgrStat
{
public:
  ObLSTxCtxMgrStat() { reset(); }
  virtual ~ObLSTxCtxMgrStat() { }
  void reset();
  int init(const common::ObAddr &addr, const share::ObLSID &ls_id,
      const bool is_master, const bool is_stopped,
      const int64_t state, const char* state_str,
      const int64_t total_tx_ctx_count, const int64_t mgr_addr);

  const common::ObAddr &get_addr() const { return addr_; }
  const share::ObLSID &get_ls_id() const { return ls_id_; }
  bool is_master() const { return is_master_; }
  bool is_stopped() const { return is_stopped_; }
  int64_t get_state() const { return state_; }
  const char* get_state_str() const { return state_str_; }
  int64_t get_total_tx_ctx_count() const { return total_tx_ctx_count_; }
  int64_t get_mgr_addr() const { return mgr_addr_; }

  TO_STRING_KV(K_(addr), K_(ls_id), K_(is_master), K_(is_stopped), K_(state),
      K_(total_tx_ctx_count), K_(mgr_addr));

private:
  common::ObAddr addr_;
  share::ObLSID ls_id_;
  bool is_master_;
  bool is_stopped_;
  int64_t state_;
  const char* state_str_;
  int64_t total_tx_ctx_count_;
  int64_t mgr_addr_;
};

} // transaction
} // oceanbase

#endif // OCEANBASE_TRANSACTION_OB_LS_TX_CTX_MGR_STAT_
