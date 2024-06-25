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

namespace oceanbase
{
namespace storage
{
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

struct ServerSuperBlockBody final
{
public:
  static const int64_t SUPER_BLOCK_BODY_VERSION = 1;

  int64_t create_timestamp_;  // create timestamp
  int64_t modify_timestamp_;  // last modified timestamp
  int64_t macro_block_size_;
  int64_t total_macro_block_count_;
  int64_t total_file_size_;

  common::ObLogCursor replay_start_point_;
  blocksstable::MacroBlockId tenant_meta_entry_;

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
               K_(tenant_meta_entry));

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

struct ObTenantSuperBlock final
{
public:
  static const int64_t MAX_SNAPSHOT_NUM = 32;
  static const int64_t MIN_SUPER_BLOCK_VERSION = 0;
  static const int64_t TENANT_SUPER_BLOCK_VERSION_V1 = 1;
  static const int64_t TENANT_SUPER_BLOCK_VERSION_V3 = 3;
  static const int64_t TENANT_SUPER_BLOCK_VERSION = 4;
  ObTenantSuperBlock();
  ObTenantSuperBlock(const uint64_t tenant_id, const bool is_hidden = false);
  ~ObTenantSuperBlock() = default;
  ObTenantSuperBlock(const ObTenantSuperBlock &other);
  ObTenantSuperBlock &operator=(const ObTenantSuperBlock &other);
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
               K_(snapshot_cnt));
  OB_UNIS_VERSION(TENANT_SUPER_BLOCK_VERSION);
public:
  uint64_t tenant_id_;
  common::ObLogCursor replay_start_point_;
  blocksstable::MacroBlockId ls_meta_entry_;
  blocksstable::MacroBlockId tablet_meta_entry_;
  bool is_hidden_;
  int64_t version_;
  ObTenantSnapshotMeta tenant_snapshots_[MAX_SNAPSHOT_NUM];
  int64_t snapshot_cnt_;
};

#define IS_EMPTY_BLOCK_LIST(entry_block) (entry_block == oceanbase::storage::ObServerSuperBlock::EMPTY_LIST_ENTRY_BLOCK)

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OB_STORAGE_SUPER_BLOCK_STRUCT_H_
