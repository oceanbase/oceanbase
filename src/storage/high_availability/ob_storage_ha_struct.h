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

#ifndef OCEABASE_STORAGE_HA_STRUCT_
#define OCEABASE_STORAGE_HA_STRUCT_

#include "lib/ob_define.h"
#include "share/ob_ls_id.h"
#include "common/ob_member.h"
#include "common/ob_tablet_id.h"
#include "lib/container/ob_array.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/ob_macro_block_meta_mgr.h"
#include "storage/blocksstable/ob_datum_rowkey.h"
#include "storage/blocksstable/ob_logic_macro_id.h"


namespace oceanbase
{
namespace storage
{

enum ObMigrationStatus
{
  OB_MIGRATION_STATUS_NONE = 0,
  OB_MIGRATION_STATUS_ADD = 1,
  OB_MIGRATION_STATUS_ADD_FAIL = 2,
  OB_MIGRATION_STATUS_MIGRATE = 3,
  OB_MIGRATION_STATUS_MIGRATE_FAIL = 4,
  OB_MIGRATION_STATUS_REBUILD = 5,
//  OB_MIGRATION_STATUS_REBUILD_FAIL = 6, not used yet
  OB_MIGRATION_STATUS_CHANGE = 7,
  OB_MIGRATION_STATUS_RESTORE_STANDBY = 8,
  OB_MIGRATION_STATUS_HOLD = 9,
  OB_MIGRATION_STATUS_MAX,
};

struct ObMigrationOpType
{
  enum TYPE
  {
    ADD_LS_OP = 0,
    MIGRATE_LS_OP = 1,
    REBUILD_LS_OP = 2,
    CHANGE_LS_OP = 3,
    REMOVE_LS_OP = 4,
    RESTORE_STANDBY_LS_OP = 5,
    MAX_LS_OP,
  };
  static const char *get_str(const TYPE &status);
  static TYPE get_type(const char *type_str);
  static OB_INLINE bool is_valid(const TYPE &type) { return type >= 0 && type < MAX_LS_OP; }
  static bool need_keep_old_tablet(const TYPE &type);
};

struct ObMigrationStatusHelper
{
public:
  static int trans_migration_op(const ObMigrationOpType::TYPE &op_type, ObMigrationStatus &migrate_status);
  static int trans_fail_status(const ObMigrationStatus &cur_status, ObMigrationStatus &fail_status);
  static int trans_reboot_status(const ObMigrationStatus &cur_status, ObMigrationStatus &reboot_status);
  static bool check_can_election(const ObMigrationStatus &cur_status);
  static bool check_can_restore(const ObMigrationStatus &cur_status);
  static bool check_allow_gc(const ObMigrationStatus &cur_status);
  static bool check_can_migrate_out(const ObMigrationStatus &cur_status);
  static int check_can_change_status(
      const ObMigrationStatus &cur_status,
      const ObMigrationStatus &change_status,
      bool &can_change);
  static bool is_valid(const ObMigrationStatus &status);
};

enum ObMigrationOpPriority
{
  PRIO_HIGH = 0,
  PRIO_LOW = 1,
  PRIO_MID = 2,
  PRIO_INVALID
};

struct ObMigrationOpArg
{
  ObMigrationOpArg();
  virtual ~ObMigrationOpArg() = default;
  bool is_valid() const;
  void reset();
  VIRTUAL_TO_STRING_KV(
      K_(ls_id),
      "type",
      ObMigrationOpType::get_str(type_),
      K_(cluster_id),
      K_(priority),
      K_(src),
      K_(dst),
      K_(data_src),
      K_(paxos_replica_number));
  share::ObLSID ls_id_;
  ObMigrationOpType::TYPE type_;
  int64_t cluster_id_;
  ObMigrationOpPriority priority_;
  common::ObReplicaMember src_;
  common::ObReplicaMember dst_;
  common::ObReplicaMember data_src_;
  int64_t paxos_replica_number_;
};

struct ObTabletsTransferArg
{
  ObTabletsTransferArg();
  virtual ~ObTabletsTransferArg() = default;
  bool is_valid() const;
  void reset();
  VIRTUAL_TO_STRING_KV(
      K_(tenant_id),
      K_(ls_id),
      K_(src),
      K_(tablet_id_array),
      K_(snapshot_log_ts));

  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObReplicaMember src_;
  common::ObArray<common::ObTabletID> tablet_id_array_;
  int64_t snapshot_log_ts_;
};

struct ObStorageHASrcInfo
{
  ObStorageHASrcInfo();
  virtual ~ObStorageHASrcInfo() = default;
  bool is_valid() const;
  void reset();
  uint64_t hash() const;
  bool operator ==(const ObStorageHASrcInfo &src_info) const;
  TO_STRING_KV(K_(src_addr), K_(cluster_id));

  common::ObAddr src_addr_;
  int64_t cluster_id_;
};

struct ObMacroBlockCopyInfo
{
  ObMacroBlockCopyInfo();
  virtual ~ObMacroBlockCopyInfo();
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(logic_macro_block_id), K_(need_copy));

  blocksstable::ObLogicMacroBlockId logic_macro_block_id_;
  bool need_copy_;
};

struct ObMacroBlockCopyArgInfo
{
  ObMacroBlockCopyArgInfo();
  virtual ~ObMacroBlockCopyArgInfo();
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(logic_macro_block_id));

  blocksstable::ObLogicMacroBlockId logic_macro_block_id_;
};

struct ObCopyTabletStatus
{
  enum STATUS
  {
    TABLET_EXIST = 0,
    TABLET_NOT_EXIST = 1,
    MAX_STATUS,
  };
  static OB_INLINE bool is_valid(const STATUS &status) { return status >= 0 && status < MAX_STATUS; }
};

struct ObCopyTabletSimpleInfo
{
  ObCopyTabletSimpleInfo();
  virtual ~ObCopyTabletSimpleInfo() {}
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(tablet_id), K_(status));
  common::ObTabletID tablet_id_;
  ObCopyTabletStatus::STATUS status_;
  int64_t data_size_;
};

struct ObMigrationFakeBlockID
{
  ObMigrationFakeBlockID();
  virtual ~ObMigrationFakeBlockID() = default;
  TO_STRING_KV(K_(migration_fake_block_id));
  static const int64_t FAKE_BLOCK_INDEX = INT64_MAX -1;
  blocksstable::MacroBlockId migration_fake_block_id_;
};

struct ObCopySSTableHelper
{
  static bool check_can_reuse(const ObSSTableStatus &status);
};

class ObIHAHandler
{
public:
  ObIHAHandler() {}
  virtual ~ObIHAHandler() {}
  virtual int process() = 0;
private:
  DISALLOW_COPY_AND_ASSIGN(ObIHAHandler);
};

struct ObMigrationUtils
{
  static bool is_need_retry_error(const int err);
  static int check_tablets_has_inner_table(
      const common::ObIArray<ObTabletID> &tablet_ids,
      bool &has_inner_table);
  static int get_ls_rebuild_seq(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      int64_t &rebuild_seq);
};

struct ObCopyTableKeyInfo final
{
  OB_UNIS_VERSION(1);
public:
  ObCopyTableKeyInfo();
  ~ObCopyTableKeyInfo() {}

  void reset();
  bool is_valid() const;
  uint64_t hash() const;
  bool operator == (const ObCopyTableKeyInfo &other) const;

  TO_STRING_KV(K_(src_table_key), K_(dest_table_key));
  ObITable::TableKey src_table_key_;
  ObITable::TableKey dest_table_key_;
};

struct ObCopyMacroRangeInfo final
{
  OB_UNIS_VERSION(1);
public:
  ObCopyMacroRangeInfo();
  ~ObCopyMacroRangeInfo();
  bool is_valid() const;
  void reset();
  void reuse();
  int assign(const ObCopyMacroRangeInfo &copy_macro_range_info);
  int deep_copy_start_end_key(const blocksstable::ObDatumRowkey & start_macro_block_end_key);

  TO_STRING_KV(K_(start_macro_block_id), K_(end_macro_block_id),
      K_(macro_block_count), K_(start_macro_block_end_key), K_(is_leader_restore));
public:
  blocksstable::ObLogicMacroBlockId start_macro_block_id_;
  blocksstable::ObLogicMacroBlockId end_macro_block_id_;
  int64_t macro_block_count_;
  bool is_leader_restore_;
  blocksstable::ObStorageDatum datums_[OB_INNER_MAX_ROWKEY_COLUMN_NUMBER];
  blocksstable::ObDatumRowkey start_macro_block_end_key_;
  ObArenaAllocator allocator_;

  DISALLOW_COPY_AND_ASSIGN(ObCopyMacroRangeInfo);
};

struct ObCopySSTableMacroRangeInfo final
{
public:
  ObCopySSTableMacroRangeInfo();
  ~ObCopySSTableMacroRangeInfo();
  bool is_valid() const;
  void reset();
  int assign(const ObCopySSTableMacroRangeInfo &sstable_macro_range_info);

  TO_STRING_KV(K_(copy_table_key), K_(copy_macro_range_array));

  ObITable::TableKey copy_table_key_;
  common::ObArray<ObCopyMacroRangeInfo> copy_macro_range_array_;
  DISALLOW_COPY_AND_ASSIGN(ObCopySSTableMacroRangeInfo);
};



}
}
#endif
