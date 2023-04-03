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

#ifndef OCEANBASE_TRANSACTION_OB_TRANS_END_TRANS_CALLBACK_
#define OCEANBASE_TRANSACTION_OB_TRANS_END_TRANS_CALLBACK_

#include "share/ob_define.h"
#include "lib/container/ob_array.h"
#include "ob_trans_define.h"

namespace oceanbase
{

namespace transaction
{
class ObTransService;
struct ObTxCommitCallback
{
public:
  ObTxCommitCallback() { reset(); }
  ~ObTxCommitCallback() { reset(); }
  int init(ObTransService* txs, const ObTransID tx_id, const int ret, const share::SCN commit_version)
  {
    txs_ = txs;
    tx_id_ = tx_id;
    ret_ = ret;
    commit_version_ = commit_version;
    inited_ = true;
    return OB_SUCCESS;
  }
  bool is_inited() { return inited_; }
  void disable() { enable_ = false; }
  void enable() { enable_ = true; }
  bool is_enabled() { return enable_; }
  void reset();
  void destroy() { reset(); }
  int callback();
  TO_STRING_KV(K_(inited), K_(enable), KP_(txs), K_(tx_id), K_(ret), K_(commit_version), K_(callback_count));
public:
  bool enable_;
  bool inited_;
  int64_t callback_count_;
  ObTransService* txs_;
  ObTransID tx_id_;
  int ret_;
  share::SCN commit_version_;
};

class ObTxCommitCallbackTask : public ObTransTask
{
public:
  ObTxCommitCallbackTask() { reset(); }
  ~ObTxCommitCallbackTask() { destroy(); }
  void reset();
  void destroy() { reset(); }
  int make(const int64_t task_type,
           const ObTxCommitCallback &cb,
           const MonotonicTs receive_gts_ts,
           const int64_t need_wait_interval_us);
  int callback(bool &has_cb);
  int64_t get_need_wait_us() const;
  static const int64_t MAX_NEED_WAIT_US = 500;
  TO_STRING_KV(K_(cb), K_(trans_need_wait_wrap));
private:
  ObTxCommitCallback cb_;
  ObTransNeedWaitWrap trans_need_wait_wrap_;
};

} // transaction
} // oceanbase

#endif // OCEANBASE_TRANSACTION_OB_TRANS_END_TRANS_CALLBACK_
