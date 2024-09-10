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

#ifndef OB_STORAGE_SUPER_BLOCK_STRUCT_H_
#define OB_STORAGE_SUPER_BLOCK_STRUCT_H_

#include "common/log/ob_log_cursor.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "share/tenant_snapshot/ob_tenant_snapshot_id.h"
#include "share/ob_ls_id.h"
#include "common/ob_tablet_id.h"
#include "storage/meta_mem/ob_meta_obj_struct.h"
#include "storage/meta_store/ob_tenant_seq_generator.h"

namespace oceanbase
{
namespace storage
{

enum GCTabletType
{
  InvalidType = -1,
  DropTablet = 0,
  TransferOut = 1,
  CreateAbort = 2,
  DropLS = 3
};

struct ObServerSuperBlockHeader final
{
public:
  static const int32_t SERVER_SUPER_BLOCK_VERSION = 1;
  static const int64_t OB_MAX_SUPER_BLOCK_SIZE = 64 * 1024;

  ObServerSuperBlockHeader();
  ~ObServerSuperBlockHeader() = default;
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(version), K_(magic), K_(body_size), K_(body_crc));
  NEED_SERIALIZE_AND_DESERIALIZE;

  int32_t version_;
  int32_t magic_;
  int32_t body_size_;
  int32_t body_crc_;
};

enum class ObTenantCreateStatus
{
  CREATING = 0,
  CREATED, // 1
  CREATE_ABORT, // 2
  DELETING, // 3
  DELETED, // 4
  MAX
};

struct ObTenantItem
{
public:
  ObTenantItem() :
    tenant_id_(OB_INVALID_TENANT_ID),
    epoch_(0),
    status_(ObTenantCreateStatus::MAX) {}
  virtual ~ObTenantItem() {}
  bool is_valid() const
  {
    return OB_INVALID_TENANT_ID != tenant_id_ && epoch_ > 0 &&
        ObTenantCreateStatus::MAX != status_;
  }

  TO_STRING_KV(K_(tenant_id), K_(epoch), K_(status));
  OB_UNIS_VERSION_V(1);

public:
  uint64_t tenant_id_;
  int64_t epoch_;
  ObTenantCreateStatus status_;
};

struct ServerSuperBlockBody final
{
public:
  static const int64_t SUPER_BLOCK_BODY_VERSION = 1;
  static const int64_t MAX_TENANT_COUNT = 512;

  int64_t create_timestamp_;  // create timestamp
  int64_t modify_timestamp_;  // last modified timestamp
  int64_t macro_block_size_;

  // only meaningful for shared-nothing
  int64_t total_macro_block_count_;
  int64_t total_file_size_;
  common::ObLogCursor replay_start_point_;
  blocksstable::MacroBlockId tenant_meta_entry_;

  // only meaningful for shared-storage
  int64_t auto_inc_tenant_epoch_;
  int64_t tenant_cnt_;
  ObTenantItem tenant_item_arr_[MAX_TENANT_COUNT];
  ServerSuperBlockBody();
  bool is_valid() const;
  void reset();

  TO_STRING_KV("Type", "ObServerSuperBlockBody",
               K_(create_timestamp),
               K_(modify_timestamp),
               K_(macro_block_size),
               K_(total_macro_block_count),
               K_(total_file_size),
               K_(replay_start_point),
               K_(tenant_meta_entry),
               K_(auto_inc_tenant_epoch),
               K_(tenant_cnt));

  OB_UNIS_VERSION(SUPER_BLOCK_BODY_VERSION);
};

struct ObServerSuperBlock final
{
public:

  ObServerSuperBlock();
  ~ObServerSuperBlock() = default;

  // represents an entry to an empty linked list， distinguished with the invalid macro block id
  static const blocksstable::MacroBlockId EMPTY_LIST_ENTRY_BLOCK;

  bool is_valid() const;
  void reset();
  NEED_SERIALIZE_AND_DESERIALIZE;
  TO_STRING_KV(K_(header), K_(body));

  OB_INLINE int64_t get_macro_block_size() const
  {
    return body_.macro_block_size_;
  }
  OB_INLINE int64_t get_total_macro_block_count() const
  {
    return body_.total_macro_block_count_;
  }
  OB_INLINE int64_t get_super_block_size() const
  {
    return header_.get_serialize_size() + body_.get_serialize_size();
  }
  int construct_header();
  int format_startup_super_block(const int64_t macro_block_size, const int64_t data_file_size);

  ObServerSuperBlockHeader header_;
  ServerSuperBlockBody body_;
};

struct ObTenantSnapshotMeta final
{
public:
  ObTenantSnapshotMeta()
    : ls_meta_entry_(oceanbase::storage::ObServerSuperBlock::EMPTY_LIST_ENTRY_BLOCK), snapshot_id_()
  {
  }
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(ls_meta_entry), K_(snapshot_id));
  OB_UNIS_VERSION(1);
public:
  blocksstable::MacroBlockId ls_meta_entry_;
  share::ObTenantSnapshotID snapshot_id_;
};

enum class ObLSItemStatus : uint8_t
{
  CREATING = 0,
  CREATED, // 1
  CREATE_ABORT, // 2
  DELETED, // 3
  MAX
};

struct ObLSItem
{
public:
  ObLSItem() :
    ls_id_(),
    epoch_(0),
    status_(ObLSItemStatus::MAX) {}
  virtual ~ObLSItem() { reset(); }

  void reset()
  {
    ls_id_.reset();
    epoch_ = 0;
    status_ = ObLSItemStatus::MAX;
  }

  bool is_valid() const
  {
    return ls_id_.is_valid() && epoch_ >= 0 && ObLSItemStatus::MAX != status_;
  }

  TO_STRING_KV(K_(ls_id), K_(epoch), K_(status));
  OB_UNIS_VERSION_V(1);

public:
  share::ObLSID ls_id_;
  int64_t epoch_;
  ObLSItemStatus status_;
};

struct ObTenantSuperBlock final
{
public:
  static const int64_t MAX_SNAPSHOT_NUM = 32;
  static const int64_t MIN_SUPER_BLOCK_VERSION = 0;
  static const int64_t TENANT_SUPER_BLOCK_VERSION_V1 = 1;
  static const int64_t TENANT_SUPER_BLOCK_VERSION_V3 = 3;
  static const int64_t TENANT_SUPER_BLOCK_VERSION = 4;
  static const int64_t MAX_LS_COUNT = 128;
  ObTenantSuperBlock();
  ObTenantSuperBlock(const uint64_t tenant_id, const bool is_hidden = false);
  ~ObTenantSuperBlock() = default;
  ObTenantSuperBlock(const ObTenantSuperBlock &other);
  ObTenantSuperBlock &operator==(const ObTenantSuperBlock &other) = delete;
  ObTenantSuperBlock &operator!=(const ObTenantSuperBlock &other) = delete;
  void copy_snapshots_from(const ObTenantSuperBlock &other);
  void reset();
  bool is_valid() const;
  int get_snapshot(const share::ObTenantSnapshotID &snapshot_id, ObTenantSnapshotMeta &snapshot) const;
  bool is_old_version() const { return version_ < TENANT_SUPER_BLOCK_VERSION; }
  int add_snapshot(const ObTenantSnapshotMeta &snapshot);
  int delete_snapshot(const share::ObTenantSnapshotID &snapshot_id);
  int check_new_snapshot(const share::ObTenantSnapshotID &snapshot_id) const;
  bool is_trivial_version() const { return version_ == TENANT_SUPER_BLOCK_VERSION_V1; }

  TO_STRING_KV(K_(tenant_id),
               K_(replay_start_point),
               K_(ls_meta_entry),
               K_(tablet_meta_entry),
               K_(is_hidden),
               K_(version),
               K_(snapshot_cnt),
               K_(preallocated_seqs),
               K_(auto_inc_ls_epoch),
               K_(ls_cnt));

  OB_UNIS_VERSION(TENANT_SUPER_BLOCK_VERSION);
public:
  uint64_t tenant_id_;
  // only meaningful for shared-nothing
  common::ObLogCursor replay_start_point_;
  blocksstable::MacroBlockId ls_meta_entry_;
  blocksstable::MacroBlockId tablet_meta_entry_;

  bool is_hidden_;
  int64_t version_;
  int64_t snapshot_cnt_;
  ObTenantSnapshotMeta tenant_snapshots_[MAX_SNAPSHOT_NUM];
  // only meaningful for shared-storage
  ObTenantMonotonicIncSeqs preallocated_seqs_;
  int64_t auto_inc_ls_epoch_;
  int64_t ls_cnt_;
  ObLSItem ls_item_arr_[MAX_LS_COUNT];
};

#define IS_EMPTY_BLOCK_LIST(entry_block) (entry_block == oceanbase::storage::ObServerSuperBlock::EMPTY_LIST_ENTRY_BLOCK)
// Due to the design of slog, the log_id_'s initial value must be 1
#define SET_FIRST_VALID_SLOG_CURSOR(cursor) (set_cursor(cursor, 1/*file_id*/, 1/*log_id*/, 0/*offset*/))


struct ObActiveTabletItem
{
public:
  ObActiveTabletItem() : tablet_id_(), tablet_meta_version_(0) {}
  ObActiveTabletItem(const common::ObTabletID tablet_id, const int64_t tablet_meta_version)
    : tablet_id_(tablet_id), tablet_meta_version_(tablet_meta_version) {}

  bool is_valid() const { return tablet_id_.is_valid() && tablet_meta_version_ > 0; }

  TO_STRING_KV(K_(tablet_id), K_(tablet_meta_version));
  OB_UNIS_VERSION(1);

public:
  common::ObTabletID tablet_id_;
  int64_t tablet_meta_version_;
};

struct ObLSActiveTabletArray
{
public:
  ObLSActiveTabletArray()
    : items_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator("ActiveItems", MTL_ID())) {}

  ObLSActiveTabletArray(const ObLSActiveTabletArray &) = delete;
  ObLSActiveTabletArray &operator=(const ObLSActiveTabletArray &) = delete;

  bool is_valid() const { return items_.count() >= 0; }
  int assign(const ObLSActiveTabletArray &other);

  TO_STRING_KV(K_(items));

  OB_UNIS_VERSION(1);

public:
  common::ObSEArray<ObActiveTabletItem, 16> items_;
};

enum class ObPendingFreeTabletStatus : uint8_t
{
  WAIT_GC = 0,
  WAIT_VERIFY, // 1
  VERIFIED, // 2
  MAX
};

struct ObPendingFreeTabletItem
{
public:
  ObPendingFreeTabletItem()
    : tablet_id_(),
      tablet_meta_version_(0),
      status_(ObPendingFreeTabletStatus::MAX),
      free_time_(0),
      gc_type_(GCTabletType::DropTablet)
  {}
  ObPendingFreeTabletItem(
    const common::ObTabletID tablet_id,
    const int64_t tablet_meta_version,
    const ObPendingFreeTabletStatus status,
    int64_t free_time,
    GCTabletType gc_type)
    : tablet_id_(tablet_id), tablet_meta_version_(tablet_meta_version),
      status_(status), free_time_(free_time),
      gc_type_(gc_type)
  {}

  bool is_valid() const
  {
    return tablet_id_.is_valid() && tablet_meta_version_ > 0 &&
        ObPendingFreeTabletStatus::MAX != status_;
  }
  bool operator == (const ObPendingFreeTabletItem &other) const {
    return tablet_id_ == other.tablet_id_ &&
           tablet_meta_version_ == other.tablet_meta_version_ &&
           status_ == other.status_;
  }

  TO_STRING_KV(K_(tablet_id), K_(tablet_meta_version), K_(status));
  OB_UNIS_VERSION(1);

public:
  common::ObTabletID tablet_id_;
  int64_t tablet_meta_version_;
  ObPendingFreeTabletStatus status_;
  // pending_free_items in pending_free_tablet_arr are incremented according to free time
  int64_t free_time_;
  GCTabletType gc_type_;
};

struct ObLSPendingFreeTabletArray
{
public:
  ObLSPendingFreeTabletArray()
    : items_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator("PendFreeItems", MTL_ID())) {}

  ObLSPendingFreeTabletArray(const ObLSPendingFreeTabletArray &) = delete;
  ObLSPendingFreeTabletArray &operator=(const ObLSPendingFreeTabletArray &) = delete;

  bool is_valid() const { return items_.count() >= 0; }
  int assign(const ObLSPendingFreeTabletArray &other);

  TO_STRING_KV(K_(items));

  OB_UNIS_VERSION(1);

public:
  common::ObSEArray<ObPendingFreeTabletItem, 16> items_;
};

struct ObPrivateTabletCurrentVersion
{
public:
  ObPrivateTabletCurrentVersion() : tablet_addr_() {}

  bool is_valid() const { return tablet_addr_.is_valid(); }

  TO_STRING_KV(K_(tablet_addr));
  OB_UNIS_VERSION(1);

public:
  ObMetaDiskAddr tablet_addr_;
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OB_STORAGE_SUPER_BLOCK_STRUCT_H_
