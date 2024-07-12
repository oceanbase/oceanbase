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
#include "share/ls/ob_ls_i_life_manager.h"
#include "share/scheduler/ob_dag_scheduler_config.h"


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
  OB_MIGRATION_STATUS_REBUILD_FAIL = 6,
  OB_MIGRATION_STATUS_CHANGE = 7,
  OB_MIGRATION_STATUS_RESTORE_STANDBY = 8,
  OB_MIGRATION_STATUS_HOLD = 9,
  OB_MIGRATION_STATUS_MIGRATE_WAIT = 10,
  OB_MIGRATION_STATUS_ADD_WAIT = 11,
  OB_MIGRATION_STATUS_REBUILD_WAIT = 12,
  OB_MIGRATION_STATUS_GC = 13,  // ls wait allow gc
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
  static int get_ls_wait_status(const TYPE &type, ObMigrationStatus &wait_status);
};

struct ObMigrationStatusHelper
{
public:
  static int trans_migration_op(const ObMigrationOpType::TYPE &op_type, ObMigrationStatus &migrate_status);
  static int trans_fail_status(const ObMigrationStatus &cur_status, ObMigrationStatus &fail_status);
  static int trans_reboot_status(const ObMigrationStatus &cur_status, ObMigrationStatus &reboot_status);
  static bool check_can_election(const ObMigrationStatus &cur_status);
  static bool check_can_restore(const ObMigrationStatus &cur_status);
  static int check_ls_allow_gc(
      const share::ObLSID &ls_id,
      const ObMigrationStatus &cur_status,
      bool &allow_gc);
  // Check the migration status. The LS in the XXX_FAIL state is considered to be an abandoned LS, which can be judged to be directly GC when restarting
  static bool need_online(const ObMigrationStatus &cur_status);
  static bool check_allow_gc_abandoned_ls(const ObMigrationStatus &cur_status);
  static bool check_can_migrate_out(const ObMigrationStatus &cur_status);
  static int check_can_change_status(
      const ObMigrationStatus &cur_status,
      const ObMigrationStatus &change_status,
      bool &can_change);
  static bool is_valid(const ObMigrationStatus &status);
  static int trans_rebuild_fail_status(
      const ObMigrationStatus &cur_status,
      const bool is_in_member_list,
      const bool is_ls_deleted,
      const bool is_tenant_dropped,
      ObMigrationStatus &fail_status);
  static int check_migration_in_final_state(
      const ObMigrationStatus &status,
      bool &in_final_state);
  static bool check_is_running_migration(const ObMigrationStatus &cur_status);
private:
  static int check_ls_transfer_tablet_(
      const share::ObLSID &ls_id,
      const ObMigrationStatus &migration_status,
      bool &allow_gc);
  static int check_transfer_dest_ls_status_for_ls_gc(
      const share::ObLSID &transfer_ls_id,
      const ObTabletID &tablet_id,
      const share::SCN &transfer_scn,
      const bool need_wait_dest_ls_replay,
      bool &allow_gc);
  static int check_transfer_dest_tablet_for_ls_gc(
      ObLS *ls,
      const ObTabletID &tablet_id,
      const share::SCN &transfer_scn,
      const bool need_wait_dest_ls_replay,
      bool &allow_gc);
  static bool check_migration_status_is_fail_(const ObMigrationStatus &cur_status);
  static int set_ls_migrate_gc_status_(
      ObLS &ls,
      bool &allow_gc);
  static int check_ls_with_transfer_task_(
      ObLS &ls,
      bool &need_check_allow_gc,
      bool &need_wait_dest_ls_replay);
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
  static int get_dag_priority(
      const ObMigrationOpType::TYPE &type,
      share::ObDagPrio::ObDagPrioEnum &prio);
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

class ObLSRebuildStatus final
{
  OB_UNIS_VERSION(1);
public:
  enum STATUS : uint8_t
  {
    NONE = 0,
    INIT = 1,
    DOING = 2,
    CLEANUP = 3,
    MAX
  };
public:
  ObLSRebuildStatus();
  ~ObLSRebuildStatus() = default;
  explicit ObLSRebuildStatus(const STATUS &status);
  ObLSRebuildStatus &operator=(const ObLSRebuildStatus &status);
  ObLSRebuildStatus &operator=(const STATUS &status);
  bool operator ==(const ObLSRebuildStatus &other) const { return status_ == other.status_; }
  bool operator !=(const ObLSRebuildStatus &other) const { return status_ != other.status_; }
  operator STATUS() const { return status_; }
  bool is_valid() const;
  STATUS get_status() const { return status_; }
  int set_status(int32_t status);
  void reset();
  TO_STRING_KV(K_(status));

private:
  STATUS status_;
};

class ObLSRebuildType final
{
  OB_UNIS_VERSION(1);
public:
  enum TYPE : uint8_t
  {
    NONE = 0,
    CLOG = 1,
    TRANSFER = 2,
    MAX
  };

public:
  ObLSRebuildType();
  ~ObLSRebuildType() = default;
  explicit ObLSRebuildType(const TYPE &type);
  ObLSRebuildType &operator=(const ObLSRebuildType &type);
  ObLSRebuildType &operator=(const TYPE &status);
  bool operator ==(const ObLSRebuildType &other) const { return type_ == other.type_; }
  bool operator !=(const ObLSRebuildType &other) const { return type_ != other.type_; }
  operator TYPE() const { return type_; }
  bool is_valid() const;
  TYPE get_type() const { return type_; }
  int set_type(int32_t type);
  void reset();

  TO_STRING_KV(K_(type));
private:
  TYPE type_;
};

struct ObLSRebuildInfo final
{
  OB_UNIS_VERSION(1);
public:
  ObLSRebuildInfo();
  ~ObLSRebuildInfo() = default;
  void reset();
  bool is_valid() const;
  bool is_in_rebuild() const;
  bool operator ==(const ObLSRebuildInfo &other) const;

  TO_STRING_KV(K_(status), K_(type));
  ObLSRebuildStatus status_;
  ObLSRebuildType type_;
};

struct ObTabletBackfillInfo final
{
  OB_UNIS_VERSION(1);
public:
  ObTabletBackfillInfo();
  virtual ~ObTabletBackfillInfo() = default;
  int init(const common::ObTabletID &tablet_id, bool is_committed);
  bool is_valid() const;
  void reset();
  bool operator == (const ObTabletBackfillInfo &other) const;
  TO_STRING_KV(
      K_(tablet_id),
      K_(is_committed));
  common::ObTabletID tablet_id_;
  bool is_committed_;
};

class ObBackfillTabletsTableMgr final
{
public:
  ObBackfillTabletsTableMgr();
  ~ObBackfillTabletsTableMgr();
  int init(const int64_t rebuild_seq, const share::SCN &transfer_start_scn);
  int init_tablet_table_mgr(const common::ObTabletID &tablet_id, const int64_t transfer_seq);
  int add_sstable(
      const common::ObTabletID &tablet_id,
      const int64_t rebuild_seq,
      const share::SCN &transfer_start_scn,
      const int64_t transfer_seq,
      ObTableHandleV2 &table_handle);
  int get_tablet_all_sstables(
      const common::ObTabletID &tablet_id, ObTablesHandleArray &table_handle_array);
  void reuse();
  int remove_tablet_table_mgr(const common::ObTabletID &tablet_id);
  int set_max_major_end_scn(
      const common::ObTabletID &tablet_id,
      const share::SCN &max_major_end_scn);
  int get_max_major_end_scn(
      const common::ObTabletID &tablet_id,
      share::SCN &max_major_end_scn);
  int get_local_rebuild_seq(int64_t &local_rebuild_seq);
private:
  class ObTabletTableMgr final
  {
  public:
    ObTabletTableMgr();
    ~ObTabletTableMgr();
    int init(
        const common::ObTabletID &tablet_id,
        const int64_t transfer_seq);
    int add_sstable(
        const int64_t transfer_seq,
        const share::SCN &transfer_start_scn,
        ObTableHandleV2 &table_handle);
    int get_all_sstables(ObTablesHandleArray &table_handle_array);
    int set_max_major_end_scn(const share::SCN &max_major_end_scn);
    int get_max_major_end_scn(share::SCN &max_major_end_scn);
  private:
    bool is_inited_;
    common::ObTabletID tablet_id_;
    int64_t transfer_seq_;
    share::SCN max_major_end_scn_;
    common::ObArenaAllocator allocator_;
    ObTablesHandleArray table_handle_array_;
    DISALLOW_COPY_AND_ASSIGN(ObTabletTableMgr);
  };
private:
  static const int64_t MAX_BUCKET_NUM = 128;
  typedef hash::ObHashMap<common::ObTabletID, ObTabletTableMgr *> TransferTableMap;
  bool is_inited_;
  common::SpinRWLock lock_;
  TransferTableMap map_;
  int64_t local_rebuild_seq_;
  share::SCN transfer_start_scn_;
  DISALLOW_COPY_AND_ASSIGN(ObBackfillTabletsTableMgr);
};

}
}
#endif
