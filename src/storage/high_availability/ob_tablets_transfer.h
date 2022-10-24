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

#ifndef OCEABASE_STORAGE_TABLETS_TRANSFER_
#define OCEABASE_STORAGE_TABLETS_TRANSFER_

#include "ob_storage_ha_struct.h"
#include "lib/thread/ob_work_queue.h"
#include "lib/thread/ob_dynamic_thread_pool.h"
#include "share/ob_common_rpc_proxy.h" // ObCommonRpcProxy
#include "share/ob_srv_rpc_proxy.h" // ObPartitionServiceRpcProxy
#include "share/scheduler/ob_sys_task_stat.h"
#include "observer/ob_rpc_processor_simple.h"
#include "share/scheduler/ob_dag_scheduler.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/ob_macro_block_meta_mgr.h"

namespace oceanbase
{
namespace storage
{

struct ObTabletsTransferCtx
{
public:
  ObTabletsTransferCtx();
  virtual ~ObTabletsTransferCtx();
  void reset();
  void reuse();
public:
  ObTabletsTransferArg arg_;
  int64_t create_ts_;
  int64_t finish_ts_;
  share::ObTaskId task_id_;
  VIRTUAL_TO_STRING_KV(
      K_(arg),
      K_(create_ts),
      K_(finish_ts),
      K_(task_id),
      K_(retry_count),
      K_(result));
private:
  common::SpinRWLock lock_;
  int32_t result_;
  int32_t retry_count_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletsTransferCtx);
};

struct ObTabletsTransferDagNetInitParam: public share::ObIDagInitParam
{
public:
  ObTabletsTransferDagNetInitParam()
   : arg_(),
     task_id_()
  {}
  virtual ~ObTabletsTransferDagNetInitParam() {}
  virtual bool is_valid() const override
  {
    return arg_.is_valid() && !task_id_.is_invalid();
  }
  VIRTUAL_TO_STRING_KV(K_(arg), K_(task_id));
  ObTabletsTransferArg arg_;
  share::ObTaskId task_id_;
};

class ObTabletsTransferDagNet: public share::ObIDagNet
{
public:
  ObTabletsTransferDagNet();
  virtual ~ObTabletsTransferDagNet();
  virtual int init_by_param(const share::ObIDagInitParam *param) override;

  virtual bool is_valid() const override
  {
    return param_.is_valid();
  }
  virtual int start_running() override;
  virtual bool operator == (const share::ObIDagNet &other) const override;
  virtual int64_t hash() const override;
  virtual int fill_comment(char *buf, const int64_t buf_len) const override;
  virtual int fill_dag_net_key(char *buf, const int64_t buf_len) const override;

  INHERIT_TO_STRING_KV("ObIDagNet", share::ObIDagNet, K_(param), K_(ctx));
private:
  int start_running_for_transfer();
private:
  bool is_inited_;
  ObTabletsTransferDagNetInitParam param_;
  ObTabletsTransferCtx *ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletsTransferDagNet);
};

}
}
#endif
