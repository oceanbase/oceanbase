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

#ifndef OCEANBASE_TX_ELR_HANDLER_
#define OCEANBASE_TX_ELR_HANDLER_

#include "ob_trans_define.h"

namespace oceanbase
{
namespace memtable
{
class ObMemtableCtx;
}

namespace transaction
{
class ObPartTransCtx;

enum TxELRState
{
  //初始化状态
  ELR_INIT = 0,
  //单分区和单机多分区事务，日志已经成功提交给clog，表示elr preparing
  ELR_PREPARING = 1,
  //gts的时间戳已经推过global trans version
  ELR_PREPARED = 2
};

class ObTxELRHandler
{
public:
  ObTxELRHandler() : elr_prepared_state_(ELR_INIT), mt_ctx_(NULL) {}
  void reset();

  int check_and_early_lock_release(bool row_updated, ObPartTransCtx *ctx);
  void set_memtable_ctx(memtable::ObMemtableCtx *mt_ctx) { mt_ctx_ = mt_ctx; }
  memtable::ObMemtableCtx *get_memtable_ctx() const { return mt_ctx_; }

  // elr state
  void set_elr_prepared() { ATOMIC_STORE(&elr_prepared_state_, TxELRState::ELR_PREPARED); }
  bool is_elr_prepared() const { return TxELRState::ELR_PREPARED == ATOMIC_LOAD(&elr_prepared_state_); }
  void reset_elr_state() { ATOMIC_STORE(&elr_prepared_state_, TxELRState::ELR_INIT); }
  TO_STRING_KV(K_(elr_prepared_state), KP_(mt_ctx));
private:
  // whether it is ready for elr
  TxELRState elr_prepared_state_;
  memtable::ObMemtableCtx *mt_ctx_;
};

} // transaction
} // oceanbase

#endif
