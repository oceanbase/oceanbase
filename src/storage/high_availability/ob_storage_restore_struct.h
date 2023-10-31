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

#ifndef OCEABASE_STORAGE_HA_RESTORE_STRUCT_
#define OCEABASE_STORAGE_HA_RESTORE_STRUCT_

#include "share/ob_ls_id.h"
#include "common/ob_tablet_id.h"
#include "lib/container/ob_array.h"
#include "share/backup/ob_backup_path.h"
#include "common/ob_member.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "ob_storage_ha_struct.h"
#include "ob_tablet_ha_status.h"
#include "storage/backup/ob_backup_index_store.h"
#include "storage/backup/ob_backup_restore_util.h"
#include "storage/backup/ob_backup_data_struct.h"
#include "storage/blocksstable/ob_logic_macro_id.h"

namespace oceanbase
{
namespace storage
{

struct ObTenantRestoreCtx;
struct ObRestoreBaseInfo
{
  ObRestoreBaseInfo();
  virtual ~ObRestoreBaseInfo() = default;
  bool is_valid() const;
  void reset();
  int assign(const ObRestoreBaseInfo &restore_base_info);
  int copy_from(const ObTenantRestoreCtx &restore_arg);
  int get_restore_backup_set_dest(const int64_t backup_set_id, share::ObRestoreBackupSetBriefInfo &backup_set_dest) const;
  VIRTUAL_TO_STRING_KV(
      K_(restore_scn),
      K_(backup_cluster_version),
      K_(backup_data_version),
      K_(backup_dest),
      K_(backup_set_list));

  share::SCN restore_scn_;
  uint64_t backup_cluster_version_;
  uint64_t backup_data_version_;
  share::ObBackupDest backup_dest_;
  common::ObArray<share::ObRestoreBackupSetBriefInfo> backup_set_list_;
};

struct ObTabletRestoreAction
{
  enum ACTION
  {
    RESTORE_ALL = 0,
    RESTORE_TABLET_META = 1,
    RESTORE_MINOR = 2,
    RESTORE_MAJOR = 3,
    RESTORE_NONE = 4,
    MAX,
  };
  static const char *get_action_str(const ACTION &action);
  static bool is_valid(const ACTION &action);
  static bool is_restore_minor(const ACTION &action);
  static bool is_restore_major(const ACTION &action);
  static bool is_restore_none(const ACTION &action);
  static bool is_restore_all(const ACTION &action);
  static bool is_restore_tablet_meta(const ACTION &action);
  static int trans_restore_action_to_restore_status(
      const ACTION &action, ObTabletRestoreStatus::STATUS &status);
};

struct ObRestoreUtils
{
  static int  get_backup_data_type(
      const ObITable::TableKey &table_key,
      share::ObBackupDataType &data_type);
};

struct ObTabletGroupRestoreArg
{
  ObTabletGroupRestoreArg();
  virtual ~ObTabletGroupRestoreArg() = default;
  void reset();
  bool is_valid() const;
  int assign(const ObTabletGroupRestoreArg &arg);

  VIRTUAL_TO_STRING_KV(
      K_(tenant_id),
      K_(ls_id),
      K_(is_leader),
      K_(tablet_id_array),
      K_(src),
      K_(dst),
      K_(restore_base_info),
      K_(action));
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  bool is_leader_;
  ObArray<common::ObTabletID> tablet_id_array_;
  common::ObReplicaMember src_;
  common::ObReplicaMember dst_;
  ObRestoreBaseInfo restore_base_info_;
  ObTabletRestoreAction::ACTION action_;
};

struct ObLSRestoreArg
{
  ObLSRestoreArg();
  virtual ~ObLSRestoreArg() = default;
  void reset();
  bool is_valid() const;
  int assign(const ObLSRestoreArg &arg);

  VIRTUAL_TO_STRING_KV(
      K_(tenant_id),
      K_(ls_id),
      K_(is_leader),
      K_(src),
      K_(dst),
      K_(restore_base_info));
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  bool is_leader_;
  common::ObReplicaMember src_;
  common::ObReplicaMember dst_;
  ObRestoreBaseInfo restore_base_info_;
};

struct ObIRestoreDagNetCtx
{
public:
  ObIRestoreDagNetCtx();
  virtual ~ObIRestoreDagNetCtx();
  virtual int fill_comment(char *buf, const int64_t buf_len) const = 0;
  virtual int set_result(const int32_t result) = 0;
  virtual bool is_restore_failed() const = 0;
  virtual int check_need_retry(bool &need_retry) = 0;
  virtual int get_result(int32_t &result) = 0;
  DECLARE_PURE_VIRTUAL_TO_STRING;

public:
  static const int64_t MAX_RETRY_CNT = 3;
  share::ObTaskId task_id_;
  ObStorageHASrcInfo src_;
  int64_t start_ts_;
  int64_t finish_ts_;
  DISALLOW_COPY_AND_ASSIGN(ObIRestoreDagNetCtx);
};

struct ObRestoreMacroBlockId final
{
  ObRestoreMacroBlockId();
  ~ObRestoreMacroBlockId() = default;
  void reset();
  bool is_valid() const;

  TO_STRING_KV(
      K_(logic_block_id),
      K_(backup_physic_block_id));

  blocksstable::ObLogicMacroBlockId logic_block_id_;
  backup::ObBackupPhysicalID backup_physic_block_id_;
};

class ObRestoreMacroBlockIdMgr
{
public:
  ObRestoreMacroBlockIdMgr();
  virtual ~ObRestoreMacroBlockIdMgr();
  int init(
      const common::ObTabletID &tablet_id,
      const ObITable::TableKey &table_key,
      const ObRestoreBaseInfo &restore_base_info,
      backup::ObBackupMetaIndexStoreWrapper &meta_index_store,
      backup::ObBackupMetaIndexStoreWrapper &second_meta_index_store);
  int get_macro_block_id(
      const int64_t block_id_index,
      blocksstable::ObLogicMacroBlockId &logic_block_id,
      backup::ObBackupPhysicalID &physic_block_id);
  int get_block_id_index(
      const blocksstable::ObLogicMacroBlockId &logic_block_id,
      int64_t &block_id_index);
  int get_restore_macro_block_id_array(
      common::ObIArray<ObRestoreMacroBlockId> &block_id_array);

private:
  int inner_init_(
      const common::ObTabletID &tablet_id,
      const ObITable::TableKey &table_key,
      const ObRestoreBaseInfo &restore_base_info,
      backup::ObBackupMetaIndexStoreWrapper &meta_index_store,
      backup::ObBackupMetaIndexStoreWrapper &second_meta_index_store);
  int sort_block_id_array(common::ObIArray<blocksstable::ObLogicMacroBlockId> &logic_id_list);
private:
  bool is_inited_;
  ObITable::TableKey table_key_;
  ObArray<ObRestoreMacroBlockId> block_id_array_;
  DISALLOW_COPY_AND_ASSIGN(ObRestoreMacroBlockIdMgr);
};


}
}

#endif
