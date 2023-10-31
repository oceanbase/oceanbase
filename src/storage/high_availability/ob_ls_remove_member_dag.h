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

#ifndef OCEABASE_STORAGE_LS_REMOVE_MEMBER_DAG_
#define OCEABASE_STORAGE_LS_REMOVE_MEMBER_DAG_

#include "lib/thread/ob_work_queue.h"
#include "lib/thread/ob_dynamic_thread_pool.h"
#include "share/ob_common_rpc_proxy.h" // ObCommonRpcProxy
#include "share/ob_srv_rpc_proxy.h" // ObPartitionServiceRpcProxy
#include "share/scheduler/ob_sys_task_stat.h"
#include "observer/ob_rpc_processor_simple.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "ob_ls_remove_member_handler.h"
#include "storage/ob_storage_rpc.h"

namespace oceanbase
{
namespace storage
{

struct ObLSRemoveMemberCtx final
{
  ObLSRemoveMemberCtx();
  ~ObLSRemoveMemberCtx() = default;
  bool is_valid() const;
  void reset();

  TO_STRING_KV(
      K_(arg),
      K_(start_ts),
      K_(finish_ts),
      K_(result),
      KP_(storage_rpc));

  ObLSRemoveMemberArg arg_;
  int64_t start_ts_;
  int64_t finish_ts_;
  int32_t result_;
  ObStorageRpc *storage_rpc_;
};

struct ObLSRemoveMemberDagParam : public share::ObIDagInitParam
{
  ObLSRemoveMemberDagParam();
  virtual ~ObLSRemoveMemberDagParam() = default;
  virtual bool is_valid() const override;
  void reset();
  VIRTUAL_TO_STRING_KV(
      K_(arg),
      KP_(storage_rpc));
  ObLSRemoveMemberArg arg_;
  ObStorageRpc *storage_rpc_;
};

class ObLSRemoveMemberDag: public share::ObIDag
{
public:
  ObLSRemoveMemberDag();
  virtual ~ObLSRemoveMemberDag();
  ObLSRemoveMemberCtx *get_ctx() { return &ctx_; }
  virtual bool operator == (const ObIDag &other) const override;
  virtual int64_t hash() const override;
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int64_t to_string(char* buf, const int64_t buf_len) const override;
  virtual lib::Worker::CompatMode get_compat_mode() const override { return lib::Worker::CompatMode::MYSQL; }
  virtual uint64_t get_consumer_group_id() const override { return consumer_group_id_; }
  virtual int init_by_param(const share::ObIDagInitParam *param) override;
  virtual int create_first_task() override;
  virtual bool is_ha_dag() const override { return true; }
protected:
  bool is_inited_;
  ObLSRemoveMemberCtx ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObLSRemoveMemberDag);
};

class ObLSRemoveMemberTask : public share::ObITask
{
public:
  ObLSRemoveMemberTask();
  virtual ~ObLSRemoveMemberTask();
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObLSRemoveMemberTask"), KP(this), KPC(ctx_));
private:
  int do_change_member_();
  int remove_member_(ObLS *ls);
  int modify_member_number_(ObLS *ls);
  int transform_member_(ObLS *ls);
  int switch_learner_to_acceptor_(ObLS *ls);

  int report_to_rs_();
private:
  bool is_inited_;
  ObLSRemoveMemberCtx *ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObLSRemoveMemberTask);
};



}
}

#endif
