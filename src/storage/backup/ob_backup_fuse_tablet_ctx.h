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

#ifndef OCEANBASE_STORAGE_BACKUP_FUSE_TABLET_CTX_H_
#define OCEANBASE_STORAGE_BACKUP_FUSE_TABLET_CTX_H_

#include "lib/lock/ob_mutex.h"
#include "storage/backup/ob_backup_tablet_meta_fuser.h"
#include "storage/backup/ob_backup_task.h"
#include "storage/high_availability/ob_storage_ha_dag.h"

namespace oceanbase
{
namespace backup
{

enum class ObBackupFuseTabletType : uint64_t
{
  FUSE_TABLET_META_USE_V1 = 0,  // when transfer seq is not equal
  FUSE_TABLET_META_USE_V2 = 1,  // when transfer seq is equal
  FUSE_TABLET_META_DELETED = 2, // when tablet is deleted at backup data stage
  FUSE_TABLET_LS_INNER_TABLET = 3, // tablet is ls inner tablet
  FUSE_TABLET_META_REORGANIZED = 4, // when tablet is reorganized(split or merge) // FARM COMPAT WHITELIST
  MAX,
};

struct ObInitBackupTabletFuseParam final
{
  ObInitBackupTabletFuseParam();
  ~ObInitBackupTabletFuseParam();
  void reset();
  bool is_valid() const;
  int init(const ObLSBackupDagNetInitParam &init_param);
  int assign(const ObInitBackupTabletFuseParam &param);
  bool operator==(const ObInitBackupTabletFuseParam &other) const;

  TO_STRING_KV(
      K_(tenant_id),
      K_(job_desc),
      K_(backup_dest),
      K_(dest_id),
      K_(backup_set_desc),
      K_(ls_id),
      K_(turn_id),
      K_(retry_id));

  uint64_t tenant_id_;
  ObBackupJobDesc job_desc_;
  share::ObBackupDest backup_dest_;
  int64_t dest_id_;
  share::ObBackupSetDesc backup_set_desc_;
  share::ObLSID ls_id_;
  int64_t turn_id_;
  int64_t retry_id_;
  DISALLOW_COPY_AND_ASSIGN(ObInitBackupTabletFuseParam);
};

struct ObBackupTabletGroupFuseCtx final
{
public:
  ObBackupTabletGroupFuseCtx();
  ~ObBackupTabletGroupFuseCtx();
  bool is_valid() const;
  void reset();
  void reuse();
  int init();
  int do_fuse();
  int get_next_tablet_item(ObBackupTabletFuseItem &tablet_item);
  int write_tablet_meta(const ObMigrationTabletParam &param);
  int close_extern_writer();

public:
  int set_result(const int32_t result, const bool need_retry,
      const enum share::ObDagType::ObDagTypeEnum type = ObDagType::DAG_TYPE_MAX);
  bool is_failed() const;
  int get_result(int32_t &result);
  int check_allow_retry(bool &allow_retry);

public:
  bool is_inited_;
  int64_t start_ts_;
  int64_t finish_ts_;
  share::ObTaskId task_id_;
  ObInitBackupTabletFuseParam param_; // init in dag net
  lib::ObMutex mutex_;
  backup::ObBackupTabletMetaFuser fuser_;
  backup::ObExternTabletMetaWriter extern_tablet_meta_writer_;
  ObStorageHAResultMgr result_mgr_;
  ObBackupReportCtx report_ctx_;

  TO_STRING_KV(
      K_(start_ts),
      K_(finish_ts),
      K_(task_id),
      K_(param));
  DISALLOW_COPY_AND_ASSIGN(ObBackupTabletGroupFuseCtx);
};

struct ObBackupTabletFuseCtx final
{
public:
  ObBackupTabletFuseCtx();
  virtual ~ObBackupTabletFuseCtx();
  bool is_valid() const;
  void reset();
  VIRTUAL_TO_STRING_KV(KP_(group_ctx), K_(param), K_(fuse_item));

public:
  ObInitBackupTabletFuseParam param_;
  ObBackupTabletFuseItem fuse_item_;
  ObBackupTabletGroupFuseCtx *group_ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupTabletFuseCtx);
};

}
}

#endif
