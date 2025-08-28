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

#ifndef OCEANBASE_STORAGE_BACKUP_FUSE_TABLET_TASK_H_
#define OCEANBASE_STORAGE_BACKUP_FUSE_TABLET_TASK_H_

#include "storage/backup/ob_backup_fuse_tablet_ctx.h"
#include "storage/backup/ob_backup_tablet_meta_fuser.h"

namespace oceanbase
{
namespace backup 
{

class ObInitialBackupTabletGroupFuseTask : public share::ObITask
{
public:
  ObInitialBackupTabletGroupFuseTask();
  virtual ~ObInitialBackupTabletGroupFuseTask();
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObInitialBackupTabletGroupFuseTask"), KP(this), KPC(ctx_));
private:
  int build_tablet_group_ctx_();
  int generate_tablet_fuse_dags_();
  int record_server_event_();

private:
  bool is_inited_;
  share::ObIDagNet *dag_net_;
  ObBackupTabletGroupFuseCtx *ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObInitialBackupTabletGroupFuseTask);
};

class ObStartBackupTabletGroupFuseTask : public share::ObITask
{
public:
  ObStartBackupTabletGroupFuseTask();
  virtual ~ObStartBackupTabletGroupFuseTask();
  int init(share::ObIDag *finish_dag);
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObStartBackupTabletGroupFuseTask"), KP(this), KPC(group_ctx_));
private:
  int check_need_fuse_tablet_(bool &need_fuse);
  int generate_tablet_fuse_dag_();
  int record_server_event_();

private:
  bool is_inited_;
  share::ObIDag *finish_dag_;
  ObBackupTabletGroupFuseCtx *group_ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObStartBackupTabletGroupFuseTask);
};

class ObFinishBackupTabletGroupFuseTask : public share::ObITask
{
public:
  ObFinishBackupTabletGroupFuseTask();
  virtual ~ObFinishBackupTabletGroupFuseTask();
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObFinishBackupTabletGroupFuseTask"), KP(this), KPC(group_ctx_));
private:
  int close_extern_writer_();
  int abort_extern_writer_();
  int generate_init_dag_();
  int record_server_event_();
  int report_task_result_();
private:
  bool is_inited_;
  share::ObIDagNet *dag_net_;
  ObBackupTabletGroupFuseCtx *group_ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObFinishBackupTabletGroupFuseTask);
};

class ObBackupTabletFuseTask : public share::ObITask
{
public:
  ObBackupTabletFuseTask();
  virtual ~ObBackupTabletFuseTask();
  int init(ObBackupTabletFuseCtx &fuse_ctx, ObBackupTabletGroupFuseCtx &group_ctx);
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObBackupTabletFuseTask"), KP(this), KPC_(fuse_ctx), KPC_(group_ctx));

private:
  int inner_process_(const ObBackupTabletFuseItem &fuse_item);
  int check_tablet_deleted_(
      const uint64_t tenant_id,
      const common::ObTabletID &tablet_id,
      bool &tablet_deleted);
  int check_tablet_reorganized_(
      const uint64_t tenant_id,
      const common::ObTabletID &tablet_id,
      bool &tablet_reoragnized);
  int fetch_tablet_meta_in_user_data_(
      const ObBackupMetaIndex &meta_index, 
      ObMigrationTabletParam &tablet_param);
  int fuse_tablet_item_(
      const ObBackupTabletFuseItem &fuse_item,
      ObMigrationTabletParam &output_tablet_param);
  int inner_fuse_tablet_item_(
      const ObMigrationTabletParam &param_v1,
      const ObMigrationTabletParam &param_v2,
      ObMigrationTabletParam &output_param);
  int write_new_tablet_info_(
      const ObMigrationTabletParam &output);
  int record_server_event_();

private:
  bool is_inited_;
  ObMySQLProxy *sql_proxy_;
  ObBackupTabletFuseCtx *fuse_ctx_;
  ObBackupTabletGroupFuseCtx *group_ctx_;
  ObBackupFuseTabletType fuse_type_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupTabletFuseTask);
};

}
}
#endif
