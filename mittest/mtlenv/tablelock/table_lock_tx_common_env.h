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

#ifndef OCEANBASE_STORAGE_TABLELOCK_OB_TABLE_LOCK_TX_COMMON_ENV_
#define OCEANBASE_STORAGE_TABLELOCK_OB_TABLE_LOCK_TX_COMMON_ENV_

#include "storage/tablelock/ob_table_lock_common.h"
#include "storage/tx/ob_trans_part_ctx.h"

namespace oceanbase
{
namespace transaction
{
namespace tablelock
{

class MockObTxCtx : public ObPartTransCtx
{
public:
  void init(const uint64_t tenant_id,
            const share::ObLSID &ls_id,
            const ObTransID &trans_id,
            ObTxData *tx_data,
            ObTableHandleV2 &lock_memtable_handle);
  void change_to_leader();
private:
  void init_memtable_ctx_(ObTableHandleV2 &lock_memtable_handle);
};

class MockTxEnv
{
public:
  MockTxEnv(const uint64_t tenant_id, const share::ObLSID ls_id)
    : tenant_id_(tenant_id),
      ls_id_(ls_id)
  {}
  virtual ~MockTxEnv() {}
public:
  struct MyTxCtx
  {
    ObTxDesc tx_desc_;
    MockObTxCtx tx_ctx_;
    ObTxData tx_data_;
  };
public:
  void start_tx(const ObTransID &tx_id,
                ObTableHandleV2 &lock_memtable_handle,
                MyTxCtx &my_ctx);
  void get_store_ctx(MyTxCtx &my_ctx,
                     ObTxTable *tx_table,
                     ObStoreCtx &store_ctx);
public:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
};

void MockObTxCtx::init(const uint64_t tenant_id,
                       const share::ObLSID &ls_id,
                       const ObTransID &trans_id,
                       ObTxData *tx_data,
                       ObTableHandleV2 &lock_memtable_handle)
{
  default_init_();

  ASSERT_EQ(OB_SUCCESS, lock_.init(this));
  ASSERT_EQ(OB_SUCCESS, init_log_cbs_(ls_id, trans_id));
  init_memtable_ctx_(lock_memtable_handle);
  ctx_tx_data_.test_init(*tx_data, nullptr);

  // mock dist trans ctx begin
  // we mock away the rpc_ and location_adapter
  rpc_ = NULL;
  // mock dist trans ctx end

  // mock trans ctx begin
  // we mock away the trans_service_ and trans_param_ and ctx_mgr_
  trans_service_ = NULL;
  trans_expired_time_ = UINT64_MAX;
  trans_id_ = trans_id;
  tenant_id_ = tenant_id;

  // mock trans ctx end
  cluster_version_ = CLUSTER_VERSION_4_0_0_0;
  timer_ = NULL;
  // trans part ctx
  ls_id_ = ls_id;

  mt_ctx_.trans_begin();
  mt_ctx_.set_trans_ctx(this);
  mt_ctx_.set_for_replay(false /*for_replay*/);
  pending_write_ = 0;
}

void MockObTxCtx::change_to_leader()
{
  role_state_ = TxCtxRoleState::LEADER;
}

void MockObTxCtx::init_memtable_ctx_(ObTableHandleV2 &lock_memtable_handle)
{
  mt_ctx_.is_inited_ = true;
  mt_ctx_.ctx_cb_allocator_.init(MTL_ID());
  ASSERT_EQ(OB_SUCCESS, mt_ctx_.enable_lock_table(lock_memtable_handle));
}

void MockTxEnv::start_tx(const ObTransID &tx_id,
                         ObTableHandleV2 &lock_memtable_handle,
                         MyTxCtx &my_ctx)
{
  my_ctx.tx_desc_.tx_id_ = tx_id;
  my_ctx.tx_desc_.state_ = ObTxDesc::State::ACTIVE;
  my_ctx.tx_ctx_.ctx_tx_data_.test_tx_data_reset();
  my_ctx.tx_ctx_.ctx_tx_data_.test_set_tx_id(tx_id);
  my_ctx.tx_ctx_.init(tenant_id_,
                      ls_id_,
                      tx_id,
                      &my_ctx.tx_data_,
                      lock_memtable_handle);
}

void MockTxEnv::get_store_ctx(MyTxCtx &my_ctx,
                              ObTxTable *tx_table,
                              ObStoreCtx &store_ctx)
{
  int64_t timeout = 1000000;
  ObTxSnapshot snapshot;
  snapshot.version_ = share::SCN::base_scn();
  int64_t tx_lock_timeout = 0;
  ObTxTableGuard tx_table_guard;
  concurrent_control::ObWriteFlag write_flag;
  tx_table_guard.tx_table_ = tx_table;
  store_ctx.ls_id_ = my_ctx.tx_ctx_.ls_id_;
  store_ctx.mvcc_acc_ctx_.init_write(my_ctx.tx_ctx_,
                                     my_ctx.tx_ctx_.mt_ctx_,
                                     my_ctx.tx_ctx_.trans_id_,
                                     ObTxSEQ(100000, 0),
                                     my_ctx.tx_desc_,
                                     tx_table_guard,
                                     snapshot,
                                     timeout,
                                     timeout,
                                     write_flag);
  store_ctx.replay_log_scn_ = share::SCN::base_scn();
}

} // tablelock
} // transaction
} // oceanbase

#endif
