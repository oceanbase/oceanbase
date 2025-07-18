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

#ifndef OCEABASE_STORAGE_TRANSFER_PARALLEL_BUILD_TABLET_INFO_
#define OCEABASE_STORAGE_TRANSFER_PARALLEL_BUILD_TABLET_INFO_

#include "share/ob_define.h"
#include "lib/thread/ob_work_queue.h"
#include "lib/thread/ob_dynamic_thread_pool.h"
#include "share/ob_common_rpc_proxy.h" // ObCommonRpcProxy
#include "share/ob_srv_rpc_proxy.h" // ObPartitionServiceRpcProxy
#include "share/scheduler/ob_sys_task_stat.h"
#include "observer/ob_rpc_processor_simple.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "share/scn.h"
#include "ob_storage_ha_struct.h"
#include "storage/high_availability/ob_transfer_struct.h"


namespace oceanbase
{
namespace storage
{

class ObTransferParallelBuildTabletDag : public share::ObIDag
{
public:
  ObTransferParallelBuildTabletDag();
  virtual ~ObTransferParallelBuildTabletDag();
  virtual bool operator == (const share::ObIDag &other) const override;
  virtual uint64_t hash() const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;
  virtual lib::Worker::CompatMode get_compat_mode() const override
  { return lib::Worker::CompatMode::MYSQL; }
  virtual uint64_t get_consumer_group_id() const override
  { return consumer_group_id_; }
  virtual int64_t to_string(char* buf, const int64_t buf_len) const override;
  virtual bool is_ha_dag() const { return true; }

  int init(
      const share::ObLSID &ls_id,
      ObTransferBuildTabletInfoCtx *ctx);
  int get_ls(ObLS *&ls);

protected:
  bool is_inited_;
  share::ObLSID ls_id_;
  ObLSHandle ls_handle_;
  ObTransferBuildTabletInfoCtx *ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObTransferParallelBuildTabletDag);
};

class ObTransferParallelBuildTabletTask : public share::ObITask
{
public:
  ObTransferParallelBuildTabletTask();
  virtual ~ObTransferParallelBuildTabletTask();
  int init(
      const share::ObTransferTabletInfo &first_tablet_info,
      ObTransferBuildTabletInfoCtx *ctx);
  virtual int process() override;
  virtual int generate_next_task(share::ObITask *&next_task) override;
  VIRTUAL_TO_STRING_KV(K("ObTransferParallelBuildTabletTask"), KP(this), KPC(ctx_));
private:
  int do_build_tablet_infos_();
  int do_build_tablet_info_(const share::ObTransferTabletInfo &tablet_info);

private:
  bool is_inited_;
  share::ObTransferTabletInfo first_tablet_info_;
  ObTransferBuildTabletInfoCtx *ctx_;
  ObLS *ls_;
  DISALLOW_COPY_AND_ASSIGN(ObTransferParallelBuildTabletTask);
};

}
}
#endif
