/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEABASE_STORAGE_HA_STRUCT_
#define OCEABASE_STORAGE_HA_STRUCT_

#include "lib/ob_define.h"
#include "share/ob_ls_id.h"
#include "common/ob_member.h"
#include "common/ob_tablet_id.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_array_serialization.h"
#include "lib/queue/ob_link_queue.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/ob_macro_block_meta_mgr.h"
#include "storage/blocksstable/ob_datum_rowkey.h"
#include "storage/blocksstable/ob_logic_macro_id.h"
#include "share/ls/ob_ls_i_life_manager.h"
#include "share/ob_rpc_struct.h"
#include "share/scheduler/ob_dag_scheduler_config.h"
#include "share/rebuild_tablet/ob_rebuild_tablet_location.h"
#include "common/ob_learner_list.h"
#include "storage/high_availability/ob_tablet_ha_status.h"
namespace oceanbase
{
using namespace share;

namespace common
{
class ObInOutBandwidthThrottle;
class ObMySQLProxy;
} // namespace common
namespace obrpc
{
class ObStorageRpcProxy;
} // namespace obrpc

namespace storage
{
class ObStorageRpc;
struct ObLSTransferMetaInfo;

template <int64_t MAX_TABLET_COUNT>
class ObStorageHATabletIDArray
{
  static_assert(MAX_TABLET_COUNT > 0, "tablet id array capacity must be positive");
  OB_UNIS_VERSION(1);
public:
  ObStorageHATabletIDArray()
    : count_(0)
  {
  }
  ~ObStorageHATabletIDArray() = default;

  int assign(const common::ObIArray<common::ObTabletID> &tablet_id_array)
  {
    return assign_(tablet_id_array);
  }

  int assign(const ObStorageHATabletIDArray &tablet_id_array)
  {
    return this == &tablet_id_array ? OB_SUCCESS : assign_(tablet_id_array);
  }

  int push_back(const common::ObTabletID &tablet_id)
  {
    int ret = OB_SUCCESS;
    if (!tablet_id.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "tablet id is invalid", K(ret), K(tablet_id));
    } else if (count_ >= MAX_TABLET_COUNT) {
      ret = OB_SIZE_OVERFLOW;
      STORAGE_LOG(WARN, "tablet id array is size overflow", K(ret), K(count_));
    } else {
      id_array_[count_] = tablet_id;
      ++count_;
    }
    return ret;
  }

  int get_tablet_id_array(common::ObIArray<common::ObTabletID> &tablet_id_array)
  {
    int ret = OB_SUCCESS;
    tablet_id_array.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < count_; ++i) {
      if (OB_FAIL(tablet_id_array.push_back(id_array_[i]))) {
        STORAGE_LOG(WARN, "failed to push tablet id into array", K(ret), K(count_), K(i));
      }
    }
    return ret;
  }

  const common::ObTabletID &at(const int64_t idx) const
  {
    OB_ASSERT(idx >= 0 && idx < count_);
    return id_array_[idx];
  }

  common::ObTabletID &at(const int64_t idx)
  {
    OB_ASSERT(idx >= 0 && idx < count_);
    return id_array_[idx];
  }

  int64_t count() const { return count_; }
  bool empty() const { return 0 == count(); }
  void reset() { count_ = 0; }

  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    J_OBJ_START();
    J_NAME("id_array");
    J_COLON();
    (void)databuff_print_obj_array(buf, buf_len, pos, id_array_, count_);
    J_OBJ_END();
    return pos;
  }

private:
  template <typename TabletIDArray>
  int assign_(const TabletIDArray &tablet_id_array)
  {
    int ret = OB_SUCCESS;
    if (tablet_id_array.count() > MAX_TABLET_COUNT) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "cannot assign tablet id array", K(ret), K(tablet_id_array));
    } else {
      count_ = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < tablet_id_array.count(); ++i) {
        if (OB_FAIL(push_back(tablet_id_array.at(i)))) {
          STORAGE_LOG(WARN, "failed to push tablet id into array", K(ret), K(i));
        }
      }
    }
    return ret;
  }

private:
  int64_t count_;
  common::ObTabletID id_array_[MAX_TABLET_COUNT];
};

OB_DEF_SERIALIZE(ObStorageHATabletIDArray<MAX_TABLET_COUNT>, template <int64_t MAX_TABLET_COUNT>)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE_ARRAY(id_array_, count_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObStorageHATabletIDArray<MAX_TABLET_COUNT>, template <int64_t MAX_TABLET_COUNT>)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN_ARRAY(id_array_, count_);
  return len;
}

OB_DEF_DESERIALIZE(ObStorageHATabletIDArray<MAX_TABLET_COUNT>, template <int64_t MAX_TABLET_COUNT>)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  OB_UNIS_DECODE(count);
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(count < 0 || count > MAX_TABLET_COUNT)) {
    ret = OB_DESERIALIZE_ERROR;
    STORAGE_LOG(WARN, "invalid tablet id array count", K(ret), K(count));
  } else {
    count_ = count;
  }
  OB_UNIS_DECODE_ARRAY(id_array_, count_);
  return ret;
}

struct ObStorageHAServiceCtx
{
  ObStorageHAServiceCtx()
    : bandwidth_throttle_(nullptr),
      svr_rpc_proxy_(nullptr),
      storage_rpc_(nullptr),
      sql_proxy_(nullptr)
  {}
  ~ObStorageHAServiceCtx() = default;
  bool is_valid() const
  {
    return OB_NOT_NULL(bandwidth_throttle_)
        && OB_NOT_NULL(svr_rpc_proxy_)
        && OB_NOT_NULL(storage_rpc_)
        && OB_NOT_NULL(sql_proxy_);
  }
  void reset()
  {
    bandwidth_throttle_ = nullptr;
    svr_rpc_proxy_ = nullptr;
    storage_rpc_ = nullptr;
    sql_proxy_ = nullptr;
  }
  TO_STRING_KV(KP_(bandwidth_throttle), KP_(svr_rpc_proxy), KP_(storage_rpc), KP_(sql_proxy));

  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  obrpc::ObStorageRpcProxy *svr_rpc_proxy_;
  ObStorageRpc *storage_rpc_;
  common::ObMySQLProxy *sql_proxy_;
};

// Memory used by the HA(migration/rebuild/restore) data path must be charged to the tenant which
// the migrated LS/tablet belongs to. Otherwise it goes to tenant 500(OB_SERVER_TENANT_ID) by the
// default value of ObArenaAllocator/ModulePageAllocator, escapes the tenant memory limit and can
// only be observed as an anonymous 500 tenant memory bloat.
// The HA data path always runs with a valid tenant context, OB_SERVER_TENANT_ID is only kept as a
// defensive fallback.
uint64_t get_ha_mem_tenant_id();

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
  OB_MIGRATION_STATUS_REPLACE = 14,
  OB_MIGRATION_STATUS_REPLACE_WAIT = 15,
  OB_MIGRATION_STATUS_REPLACE_FAIL = 16,
  OB_MIGRATION_STATUS_REPLACE_HOLD = 17,
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
    REBUILD_TABLET_OP = 6,
    REPLACE_LS_OP = 7,
    RESTORE_LS_OP = 8,
    MAX_LS_OP,
  };
  static const char *get_str(const TYPE &status);
  static TYPE get_type(const char *type_str);
  static OB_INLINE bool is_valid(const TYPE &type) { return type >= 0 && type < MAX_LS_OP; }
  static int get_ls_wait_status(const TYPE &type, ObMigrationStatus &wait_status);
  static int get_ls_hold_status(const TYPE &type, ObMigrationStatus &hold_status);
  static int convert_to_dr_type(const TYPE &type, obrpc::ObDRTaskType &dr_type);
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
  static bool can_gc_ls_without_check_dependency(
      const ObMigrationStatus &cur_status);
  static bool can_gc_ls_without_member_verification(
      const ObMigrationStatus &cur_status);
  static bool check_can_report_readable_scn(
      const ObMigrationStatus &cur_status);
  static bool is_in_rebuild(
      const ObMigrationStatus &cur_status);
  static bool is_in_replace(const ObMigrationStatus &cur_status);
  static bool check_migration_status_is_fail(const ObMigrationStatus &cur_status);
private:
  static int check_ls_transfer_tablet_(
      const share::ObLSID &ls_id,
      bool &allow_gc);
  static int check_transfer_dest_tablet_for_ls_gc(
      ObLS *ls,
      const ObTabletID &tablet_id,
      bool &allow_gc);
  static int set_ls_migrate_gc_status_(
      ObLS &ls,
      bool &allow_gc);
  static int check_transfer_dest_ls_(
      const share::ObLSID &ls_id,
      bool &allow_gc);
  static int check_transfer_dest_tablets_(
      const ObLSTransferMetaInfo &transfer_meta_info,
      ObLS &dest_ls,
      bool &allow_gc);
  static int allow_transfer_src_ls_gc_(
      const ObMigrationStatus &migration_status,
      bool &allow_gc);

  //compatible ls gc function
  static int check_ls_transfer_tablet_v1_(
      const share::ObLSID &ls_id,
      const ObMigrationStatus &migration_status,
      const bool allow_gc_v2,
      bool &allow_gc);
  static int check_ls_with_transfer_task_v1_(
      ObLS &ls,
      const bool allow_gc_v2,
      bool &need_check_allow_gc,
      bool &need_wait_dest_ls_replay);
  static int check_transfer_dest_ls_status_for_ls_gc_v1_(
      const share::ObLSID &transfer_ls_id,
      const ObTabletID &tablet_id,
      const share::SCN &transfer_scn,
      const bool need_wait_dest_ls_replay,
      bool &allow_gc);
  static int check_transfer_dest_tablet_for_ls_gc_v1_(
      ObLS *ls,
      const ObTabletID &tablet_id,
      const share::SCN &transfer_scn,
      const bool need_wait_dest_ls_replay,
      bool &allow_gc);
  static int check_transfer_meta_info_compatible_(
      bool &for_compatible);
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

  int init(const obrpc::ObLSMigrateReplicaArg &arg);
  int init(const obrpc::ObLSAddReplicaArg &arg);
  int init(const obrpc::ObLSReplaceReplicaArg &arg);


  VIRTUAL_TO_STRING_KV(
      K_(ls_id),
      "type",
      ObMigrationOpType::get_str(type_),
      K_(cluster_id),
      K_(priority),
      K_(src),
      K_(dst),
      K_(data_src),
      K_(paxos_replica_number),
      K_(tablet_id_array),
      K_(member_list_config_version));
  share::ObLSID ls_id_;
  ObMigrationOpType::TYPE type_;
  int64_t cluster_id_;
  ObMigrationOpPriority priority_;
  common::ObReplicaMember src_;
  common::ObReplicaMember dst_;
  common::ObReplicaMember data_src_;
  int64_t paxos_replica_number_;
  bool prioritize_same_zone_src_;
  common::ObArray<ObTabletID> tablet_id_array_;

  // The member list config version in palf when rs sends the migration operation tasks.
  // Now only used for replace ls operation. Only if the config version is the same, then
  // the replica can forcibly change the member list to only include itself.
  palf::LogConfigVersion member_list_config_version_;

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

// ObCopyMacroRangeInfo describes a batch of continuous macro blocks which should be copied
// together, the batch is identified by the end rowkey of its first macro block.
//
// MEMORY NOTE: a tablet may contain millions of macro blocks, which means a single tablet can
// produce tens of thousands of ObCopyMacroRangeInfo. So this structure must stay small:
// 1) start_macro_block_end_key_ is deep copied into the arena owned by the object itself. The
//    arena uses a small page(ALLOCATOR_PAGE_SIZE) so that a ~100B rowkey does not occupy a whole
//    8KB page. The object is self contained on purpose: it is embedded in a RPC arg and is
//    deserialized by the RPC framework, so there is no chance to inject an allocator from outside.
// 2) the datum buffer needed by the producer / deserialize path is allocated on demand (see
//    prepare_datum_buffer()), so the range infos which are only filled through assign() or
//    deep_copy_start_end_key() do not pay for it at all.
struct ObCopyMacroRangeInfo final
{
  OB_UNIS_VERSION(1);
public:
  ObCopyMacroRangeInfo();
  ~ObCopyMacroRangeInfo();
  bool is_valid() const;
  // Go back to the state right after construction, all the arena pages are freed.
  void reset();
  // Prepare an empty range info for the next range. The memory hold by the previous content is
  // given back to the arena, but unlike reset() the arena keeps its normal pages, so filling range
  // infos in a loop does not malloc/free the page repeatedly.
  void reuse();
  // Allocate the datum buffer of start_macro_block_end_key_ on demand.
  // ATTENTION: it MUST be called before writing start_macro_block_end_key_.datums_ directly, e.g.
  // by ObStorageHAUtils::make_macro_id_to_datum(). The deserialize path does it by itself.
  int prepare_datum_buffer();
  int assign(const ObCopyMacroRangeInfo &copy_macro_range_info);
  int deep_copy_start_end_key(const blocksstable::ObDatumRowkey &start_macro_block_end_key);

  TO_STRING_KV(K_(start_macro_block_id), K_(end_macro_block_id),
      K_(macro_block_count), K_(start_macro_block_end_key), K_(is_leader_restore));
public:
  static const int64_t ALLOCATOR_PAGE_SIZE = 512;
private:
  // reset the members, the memory they reference is released by the caller
  void reset_fields_();
public:
  blocksstable::ObLogicMacroBlockId start_macro_block_id_;
  blocksstable::ObLogicMacroBlockId end_macro_block_id_;
  int64_t macro_block_count_;
  bool is_leader_restore_;
  blocksstable::ObDatumRowkey start_macro_block_end_key_;
private:
  blocksstable::ObStorageDatum *datum_buf_;
  common::ObArenaAllocator allocator_;

  DISALLOW_COPY_AND_ASSIGN(ObCopyMacroRangeInfo);
};

struct ObCopyMacroRangeIdInfo final
{
public:
  ObCopyMacroRangeIdInfo();
  ~ObCopyMacroRangeIdInfo();
  bool is_valid() const;
  void reset();
  void reuse();
  int assign(const ObCopyMacroRangeIdInfo &copy_macro_range_id_info);
  TO_STRING_KV(K_(range_info), K_(macro_block_ids));
public:
  ObCopyMacroRangeInfo range_info_;
  common::ObArray<blocksstable::ObLogicMacroBlockId> macro_block_ids_;
  DISALLOW_COPY_AND_ASSIGN(ObCopyMacroRangeIdInfo);
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
  // Every element owns the memory of its own rowkey, see the memory note of ObCopyMacroRangeInfo.
  common::ObArray<ObCopyMacroRangeIdInfo> copy_macro_range_array_;
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
    TABLET = 3,
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
  bool is_rebuild_ls_type() const { return ObLSRebuildType::CLOG == type_ || ObLSRebuildType::TRANSFER == type_; }
  bool is_rebuild_rebuild_type() const { return ObLSRebuildType::TABLET == type_; }
  TO_STRING_KV(K_(type));
private:
  TYPE type_;
};

struct ObRebuildTabletIDArray final : public ObStorageHATabletIDArray<64>
{
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
  int assign(const ObLSRebuildInfo &info);

  bool is_rebuild_ls() const { return type_.is_rebuild_ls_type(); }
  bool is_rebuild_tablet() const { return type_.is_rebuild_rebuild_type(); }

  TO_STRING_KV(K_(status), K_(type), K_(tablet_id_array), K_(src));
public:
  ObLSRebuildStatus status_;
  ObLSRebuildType type_;
  ObRebuildTabletIDArray tablet_id_array_;
  share::ObRebuildTabletLocation src_;
};

class ObTabletBackfillType final
{
public:
  enum TYPE
  {
    BACKFILL_TRANSFER_OUT = 0,
    BACKFILL_TRANSFER_IN = 1,
    BACKFILL_MAX
  };
public:
  ObTabletBackfillType() : type_(BACKFILL_MAX) {}
  ~ObTabletBackfillType() = default;
  explicit ObTabletBackfillType(const TYPE &type) : type_(type) {}
  ObTabletBackfillType &operator=(const TYPE &type) { type_ = type; return *this; }
  operator TYPE() const { return type_; }
  bool operator==(const TYPE &type) const { return type_ == type; }
  bool operator!=(const TYPE &type) const { return type_ != type; }
  bool operator==(const ObTabletBackfillType &other) const { return type_ == other.type_; }
  bool operator!=(const ObTabletBackfillType &other) const { return type_ != other.type_; }
  void reset() { type_ = BACKFILL_MAX; }
  bool is_valid() const { return type_ >= BACKFILL_TRANSFER_OUT && type_ < BACKFILL_MAX; }
  static const char *get_str(const ObTabletBackfillType &type);
  TO_STRING_KV("val", static_cast<uint8_t>(type_), "str", get_str(*this));
private:
  TYPE type_;
};

struct ObTabletBackfillInfo final
{
public:
  ObTabletBackfillInfo();
  ~ObTabletBackfillInfo() = default;
  bool is_valid() const;
  void reset();
  bool operator == (const ObTabletBackfillInfo &other) const;
  uint64_t hash() const;
  TO_STRING_KV(
      K_(tablet_id),
      K_(is_committed),
      K_(relative_ls_id),
      K_(reorganization_scn),
      K_(backfill_scn),
      K_(backfill_type),
      K_(tablet_status),
      K_(src_reorganization_scn),
      K_(transfer_seq));

  common::ObTabletID tablet_id_;
  bool is_committed_;
  share::ObLSID relative_ls_id_;
  share::SCN reorganization_scn_;
  share::SCN backfill_scn_;
  ObTabletBackfillType backfill_type_;
  ObTabletStatus tablet_status_;
  share::SCN src_reorganization_scn_;
  int64_t transfer_seq_;
};

class ObBackfillTabletsTableMgr final
{
public:
  ObBackfillTabletsTableMgr();
  ~ObBackfillTabletsTableMgr();
  int init(const int64_t rebuild_seq, const share::SCN &transfer_start_scn);
  int init_tablet_table_mgr(
      const common::ObTabletID &tablet_id,
      const int64_t transfer_seq,
      const ObTabletRestoreStatus::STATUS &restore_status);
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
  int get_restore_status(
      const common::ObTabletID &tablet_id,
      ObTabletRestoreStatus::STATUS &restore_status);
  int get_transfer_scn(share::SCN &transfer_scn);
private:
  class ObTabletTableMgr final
  {
  public:
    ObTabletTableMgr();
    ~ObTabletTableMgr();
    int init(
        const common::ObTabletID &tablet_id,
        const int64_t transfer_seq,
        const ObTabletRestoreStatus::STATUS &restore_status);
    int add_sstable(
        const int64_t transfer_seq,
        const share::SCN &transfer_start_scn,
        ObTableHandleV2 &table_handle);
    int get_all_sstables(ObTablesHandleArray &table_handle_array);
    int set_max_major_end_scn(const share::SCN &max_major_end_scn);
    int get_max_major_end_scn(share::SCN &max_major_end_scn);
    int get_restore_status(ObTabletRestoreStatus::STATUS &restore_status);
  private:
    bool is_inited_;
    common::ObTabletID tablet_id_;
    int64_t transfer_seq_;
    share::SCN max_major_end_scn_;
    common::ObArenaAllocator allocator_;
    ObTablesHandleArray table_handle_array_;
    ObTabletRestoreStatus::STATUS restore_status_;
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

class ObMacroBlockReuseMgr final
{
public:
  ObMacroBlockReuseMgr();
  ~ObMacroBlockReuseMgr();
  int init();
  int add_macro_block_reuse_info(
    const blocksstable::ObLogicMacroBlockId &logic_id,
    const blocksstable::MacroBlockId &macro_id,
    const int64_t &data_checksum);
  int get_macro_block_reuse_info(
    const blocksstable::ObLogicMacroBlockId &logic_id,
    blocksstable::MacroBlockId &macro_id,
    int64_t &data_checksum) const;
  bool is_inited() const { return is_inited_; }
  void reset();
  int64_t get_size() const;
private:
  struct MacroBlockReuseInfo final
  {
  public:
    MacroBlockReuseInfo(): id_(), data_checksum_(-1) {}
    MacroBlockReuseInfo(const blocksstable::MacroBlockId &id, const int64_t &data_checksum)
        : id_(id), data_checksum_(data_checksum) {}
    ~MacroBlockReuseInfo() = default;
    void reset();
  public:
    blocksstable::MacroBlockId id_;
    int64_t data_checksum_;

    TO_STRING_KV(K_(id), K_(data_checksum));
  };
  typedef ObLinearHashMap<blocksstable::ObLogicMacroBlockId, MacroBlockReuseInfo> ReuseMap;
  bool is_inited_;
  ReuseMap reuse_map_;
  DISALLOW_COPY_AND_ASSIGN(ObMacroBlockReuseMgr);
};

struct ObLogicTabletID final
{
public:
  ObLogicTabletID();
  ~ObLogicTabletID() = default;
  int init(const common::ObTabletID &tablet_id, const int64_t transfer_seq);
  bool is_valid() const;
  void reset();
  bool operator == (const ObLogicTabletID &other) const;
  bool operator != (const ObLogicTabletID &other) const;
  TO_STRING_KV(
      K_(tablet_id),
      K_(transfer_seq));
  common::ObTabletID tablet_id_;
  int64_t transfer_seq_;
};

struct ObLSMemberListInfo final
{
public:
  ObLSMemberListInfo();
  ~ObLSMemberListInfo() = default;
  void reset();
  bool is_valid() const;
  int assign(const ObLSMemberListInfo &info);

  TO_STRING_KV(K_(learner_list), K_(leader_addr), K_(member_list));
  common::GlobalLearnerList learner_list_;
  common::ObAddr leader_addr_;
  common::ObArray<common::ObMember> member_list_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObLSMemberListInfo);
};

struct ObLSMigrationCostStatic final
{
public:
  ObLSMigrationCostStatic();
  ~ObLSMigrationCostStatic() = default;
  void reset();
  int assign(const ObLSMigrationCostStatic &cost_static);
  int64_t to_string(char *buf, const int64_t buf_len) const;
public:
  share::SCN clog_checkpoint_scn_;
  int64_t tablet_count_;
  int64_t create_tablet_cost_;
  int64_t migration_dag_net_cost_;
  int64_t prewarm_cost_;
  int64_t wait_log_sync_cost_;
  int64_t wait_log_replay_cost_;
  int64_t complete_dag_net_cost_;
  int64_t start_ts_;
  int64_t finish_ts_;
  int64_t finished_tablet_count_;
  int64_t skipped_tablet_count_;
  int64_t dag_retry_count_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObLSMigrationCostStatic);
};

}
}
#endif
