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

#ifndef OCEANBASE_STORAGE_BACKUP_FUSE_TABLET_DAG_H_
#define OCEANBASE_STORAGE_BACKUP_FUSE_TABLET_DAG_H_

#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/backup/ob_backup_fuse_tablet_ctx.h"
#include "storage/backup/ob_backup_tablet_meta_fuser.h"
#include "storage/high_availability/ob_storage_ha_dag.h"

//ObBackupTabletFuseDagNet is a tablet group restore dag net
//ObInitialBackupTabletGroupFuseDag is the dag net first dag. It creates two dags :
//  ObStartBackupTabletGroupFuseDag and ObFinishBackupTabletGroupFuseDag
//ObStartBackupTabletGroupFuseDag create some ObBackupTabletFuseDag which in restore tablet id list
//ObFinishBackupTabletGroupFuseDag is the end dag when dag net is running success or finally failed.

namespace oceanbase
{
namespace backup
{

class ObBackupTabletFuseDagNet: public share::ObIDagNet
{
public:
  ObBackupTabletFuseDagNet();
  virtual ~ObBackupTabletFuseDagNet();
  virtual int init_by_param(const share::ObIDagInitParam *param) override;
  virtual bool is_valid() const override;
  virtual int start_running() override;
  virtual bool operator == (const share::ObIDagNet &other) const override;
  virtual int64_t hash() const override;
  virtual int fill_comment(char *buf, const int64_t buf_len) const override;
  virtual int fill_dag_net_key(char *buf, const int64_t buf_len) const override;
  virtual int clear_dag_net_ctx() override;
  virtual int deal_with_cancel() override;
  lib::Worker::CompatMode get_compat_mode() const { return compat_mode_; }

  ObBackupTabletGroupFuseCtx *get_fuse_ctx() { return ctx_; }
  INHERIT_TO_STRING_KV("ObIDagNet", share::ObIDagNet, KPC_(ctx));
private:
  int alloc_fuse_ctx_();
  void free_fuse_ctx_();
private:
  bool is_inited_;
  ObBackupTabletGroupFuseCtx *ctx_; // ownership, init in initial dag
  lib::Worker::CompatMode compat_mode_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupTabletFuseDagNet);
};

class ObBackupTabletGroupFuseDag : public share::ObIDag 
{
public:
  explicit ObBackupTabletGroupFuseDag(const share::ObDagType::ObDagTypeEnum &dag_type);
  virtual ~ObBackupTabletGroupFuseDag();
  virtual bool operator == (const share::ObIDag &other) const override;
  virtual int64_t hash() const override;
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;
  virtual lib::Worker::CompatMode get_compat_mode() const { return compat_mode_; }
  virtual uint64_t get_consumer_group_id() const override { return consumer_group_id_; }
  ObBackupTabletGroupFuseCtx *get_ctx() const { return ctx_; }

protected:
  ObBackupTabletGroupFuseCtx *ctx_;
  lib::Worker::CompatMode compat_mode_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupTabletGroupFuseDag);
};

class ObInitialBackupTabletGroupFuseDag : public ObBackupTabletGroupFuseDag
{
public:
  ObInitialBackupTabletGroupFuseDag();
  virtual ~ObInitialBackupTabletGroupFuseDag();
  int init(share::ObIDagNet *dag_net);
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;
protected:
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObInitialBackupTabletGroupFuseDag);
};

class ObStartBackupTabletGroupFuseDag : public ObBackupTabletGroupFuseDag
{
public:
  ObStartBackupTabletGroupFuseDag();
  virtual ~ObStartBackupTabletGroupFuseDag();
  int init(share::ObIDagNet *dag_net, share::ObIDag *finish_dag);
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;
protected:
  bool is_inited_;
  share::ObIDag *finish_dag_;
  DISALLOW_COPY_AND_ASSIGN(ObStartBackupTabletGroupFuseDag);
};

class ObFinishBackupTabletGroupFuseDag : public ObBackupTabletGroupFuseDag
{
public:
  ObFinishBackupTabletGroupFuseDag();
  virtual ~ObFinishBackupTabletGroupFuseDag();
  int init(share::ObIDagNet *dag_net);
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;
protected:
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObFinishBackupTabletGroupFuseDag);
};

class ObBackupTabletFuseDag : public share::ObIDag
{
public:
  ObBackupTabletFuseDag();
  virtual ~ObBackupTabletFuseDag();
  int init(const ObInitBackupTabletFuseParam &param,
      const ObBackupTabletFuseItem &fuse_item,
      ObBackupTabletGroupFuseCtx &group_ctx);
  virtual bool operator == (const share::ObIDag &other) const override;
  virtual int64_t hash() const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;
  virtual int generate_next_dag(share::ObIDag *&dag);
  virtual lib::Worker::CompatMode get_compat_mode() const override { return compat_mode_; }
  virtual uint64_t get_consumer_group_id() const override { return consumer_group_id_; }

protected:
  bool is_inited_;
  ObBackupTabletFuseCtx fuse_ctx_;
  ObBackupTabletGroupFuseCtx *group_ctx_;
  lib::Worker::CompatMode compat_mode_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupTabletFuseDag);
};

}
}
#endif
