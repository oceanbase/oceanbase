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

#ifndef OCEANBASE_STORAGE_OB_TENANT_SNAPSHOT_TASK_
#define OCEANBASE_STORAGE_OB_TENANT_SNAPSHOT_TASK_

#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "share/scheduler/ob_dag_scheduler_config.h"

namespace oceanbase
{
namespace storage
{
class ObTenantSnapshot;
class ObTenantSnapshotMgr;

struct ObTenantSnapshotCreateParam : public share::ObIDagInitParam {
  ObTenantSnapshotCreateParam(const share::ObTenantSnapshotID& tenant_snapshot_id,
                              const common::ObArray<share::ObLSID>& creating_ls_id_arr,
                              const common::ObCurTraceId::TraceId& trace_id,
                              ObTenantSnapshotMgr* tenant_snapshot_mgr):
                              tenant_snapshot_id_(tenant_snapshot_id),
                              creating_ls_id_arr_(creating_ls_id_arr),
                              trace_id_(trace_id),
                              tenant_snapshot_mgr_(tenant_snapshot_mgr) { }
  virtual ~ObTenantSnapshotCreateParam() {};
  virtual bool is_valid() const override;

  TO_STRING_KV(K(tenant_snapshot_id_), K(creating_ls_id_arr_), K(trace_id_), KP(tenant_snapshot_mgr_));

  ObTenantSnapshotCreateParam& operator=(const ObTenantSnapshotCreateParam& other) = delete;
  const share::ObTenantSnapshotID tenant_snapshot_id_;
  const common::ObArray<share::ObLSID> creating_ls_id_arr_;
  const common::ObCurTraceId::TraceId trace_id_;
  ObTenantSnapshotMgr* tenant_snapshot_mgr_;
};

class ObTenantSnapshotCreateDag : public share::ObIDag {
public:
  ObTenantSnapshotCreateDag(): ObIDag(share::ObDagType::DAG_TYPE_TENANT_SNAPSHOT_CREATE),
                               is_inited_(false),
                               tenant_snapshot_id_(),
                               creating_ls_id_arr_(),
                               tenant_snapshot_mgr_(nullptr) {}
  virtual ~ObTenantSnapshotCreateDag() {}
  virtual int init_by_param(const share::ObIDagInitParam *param) override;
  virtual int create_first_task() override;
  virtual bool operator==(const share::ObIDag &other) const override;
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param,
                              common::ObIAllocator &allocator) const override;
  virtual bool is_ha_dag() const override { return false; }
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int64_t hash() const override;
  virtual lib::Worker::CompatMode get_compat_mode() const override
  { return lib::Worker::CompatMode::MYSQL; }

  virtual uint64_t get_consumer_group_id() const override { return consumer_group_id_; }

  TO_STRING_KV(K(is_inited_),
      K(tenant_snapshot_id_), K(creating_ls_id_arr_), KP(tenant_snapshot_mgr_));
private:
  DISALLOW_COPY_AND_ASSIGN(ObTenantSnapshotCreateDag);

private:
  bool is_inited_;
  share::ObTenantSnapshotID tenant_snapshot_id_;
  common::ObArray<share::ObLSID> creating_ls_id_arr_;
  ObTenantSnapshotMgr* tenant_snapshot_mgr_;
};

class ObTenantSnapshotCreateTask: public share::ObITask
{
public:
  ObTenantSnapshotCreateTask(): ObITask(ObITask::TASK_TYPE_TENANT_SNAPSHOT_CREATE),
                                is_inited_(false),
                                tenant_snapshot_id_(),
                                creating_ls_id_arr_(nullptr),
                                tenant_snapshot_mgr_(nullptr) {};
  virtual ~ObTenantSnapshotCreateTask() {};
  int init(const share::ObTenantSnapshotID& tenant_snapshot_id,
           const common::ObArray<share::ObLSID>* creating_ls_id_arr,
           ObTenantSnapshotMgr* tenant_snapshot_mgr);
protected:
  virtual int process() override;

protected:
  bool is_inited_;
  share::ObTenantSnapshotID tenant_snapshot_id_;
  const common::ObArray<share::ObLSID>* creating_ls_id_arr_;
  ObTenantSnapshotMgr* tenant_snapshot_mgr_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTenantSnapshotCreateTask);
};

struct ObTenantSnapshotGCParam : public share::ObIDagInitParam {
  ObTenantSnapshotGCParam(const share::ObTenantSnapshotID tenant_snapshot_id,
                          const common::ObArray<share::ObLSID> &gc_ls_id_arr,
                          const bool gc_tenant_snapshot,
                          const common::ObCurTraceId::TraceId& trace_id,
                          ObTenantSnapshotMgr *tenant_snapshot_mgr)
                              : tenant_snapshot_id_(tenant_snapshot_id),
                                gc_ls_id_arr_(gc_ls_id_arr),
                                gc_tenant_snapshot_(gc_tenant_snapshot),
                                trace_id_(trace_id),
                                tenant_snapshot_mgr_(tenant_snapshot_mgr){}
  virtual ~ObTenantSnapshotGCParam(){}
  virtual bool is_valid() const override;

  TO_STRING_KV(K(tenant_snapshot_id_),
      K(gc_ls_id_arr_), K(gc_tenant_snapshot_), K(trace_id_), KP(tenant_snapshot_mgr_));

  const share::ObTenantSnapshotID tenant_snapshot_id_;
  const common::ObArray<share::ObLSID> gc_ls_id_arr_;
  const bool gc_tenant_snapshot_;  // gc tenant snapshot or gc ls snapshot
  const common::ObCurTraceId::TraceId trace_id_;
  ObTenantSnapshotMgr *tenant_snapshot_mgr_;
};

class ObTenantSnapshotGCDag : public share::ObIDag {
public:
  ObTenantSnapshotGCDag() : ObIDag(share::ObDagType::DAG_TYPE_TENANT_SNAPSHOT_GC),
                                is_inited_(false),
                                tenant_snapshot_id_(),
                                gc_ls_id_arr_(),
                                gc_tenant_snapshot_(false),
                                tenant_snapshot_mgr_() {}
  virtual ~ObTenantSnapshotGCDag() {}
  virtual int init_by_param(const share::ObIDagInitParam *param) override;
  virtual int create_first_task() override;
  virtual bool operator==(const ObIDag &other) const override;
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param,
                              ObIAllocator &allocator) const override;
  virtual bool is_ha_dag() const override { return false; }
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int64_t hash() const override;
  virtual lib::Worker::CompatMode get_compat_mode() const override
  { return lib::Worker::CompatMode::MYSQL; }

  virtual uint64_t get_consumer_group_id() const override { return consumer_group_id_; }

  TO_STRING_KV(K(is_inited_), K(tenant_snapshot_id_), K(gc_ls_id_arr_), KP(tenant_snapshot_mgr_));

private:
  DISALLOW_COPY_AND_ASSIGN(ObTenantSnapshotGCDag);

private:

  bool is_inited_;
  share::ObTenantSnapshotID tenant_snapshot_id_;
  common::ObArray<share::ObLSID> gc_ls_id_arr_;
  bool gc_tenant_snapshot_;
  ObTenantSnapshotMgr *tenant_snapshot_mgr_;
};

class ObTenantSnapshotGCTask : public share::ObITask {
public:
  ObTenantSnapshotGCTask() : ObITask(ObITask::TASK_TYPE_TENANT_SNAPSHOT_GC),
                                 is_inited_(false),
                                 tenant_snapshot_id_(),
                                 gc_ls_id_arr_(),
                                 gc_tenant_snapshot_(false),
                                 tenant_snapshot_mgr_(nullptr) {}
  virtual ~ObTenantSnapshotGCTask() {}
  int init(const share::ObTenantSnapshotID tenant_snapshot_id,
           const common::ObArray<share::ObLSID> *gc_ls_id_arr,
           bool gc_tenant_snapshot,
           ObTenantSnapshotMgr *tenant_snapshot_mgr);
protected:
  virtual int process() override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTenantSnapshotGCTask);
protected:
  bool is_inited_;
  share::ObTenantSnapshotID tenant_snapshot_id_;
  const common::ObArray<share::ObLSID> *gc_ls_id_arr_;
  bool gc_tenant_snapshot_;
  ObTenantSnapshotMgr *tenant_snapshot_mgr_;
};

}
}
#endif
