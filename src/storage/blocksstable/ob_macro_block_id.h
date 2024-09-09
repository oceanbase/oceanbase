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

#ifndef SRC_STORAGE_BLOCKSSTABLE_OB_MACRO_BLOCK_ID_H_
#define SRC_STORAGE_BLOCKSSTABLE_OB_MACRO_BLOCK_ID_H_

#include "share/ob_define.h"
#include "common/storage/ob_io_device.h"

namespace oceanbase
{
namespace blocksstable
{
enum class ObMacroBlockIdMode : uint8_t
{
  ID_MODE_LOCAL = 0,
  ID_MODE_BACKUP = 1,
  ID_MODE_SHARE = 2,
  ID_MODE_MAX,
};

// When adding new type, we need to ensure if the new type have to add into MacroBlockId::is_shared_data_or_meta() or MacroBlockId::is_private_data_or_meta()
enum class ObStorageObjectType : uint8_t
{
  PRIVATE_DATA_MACRO                    = 0, // for share nothing and private macro of server in share storage
  PRIVATE_META_MACRO                    = 1,
  SHARED_MINI_DATA_MACRO                = 2,
  SHARED_MINI_META_MACRO                = 3,
  SHARED_MINOR_DATA_MACRO               = 4,
  SHARED_MINOR_META_MACRO               = 5,
  SHARED_MAJOR_DATA_MACRO               = 6,
  SHARED_MAJOR_META_MACRO               = 7,
  TMP_FILE                              = 8,
  SERVER_META                           = 9,
  TENANT_SUPER_BLOCK                    = 10,
  TENANT_UNIT_META                      = 11,
  LS_META                               = 12,
  LS_DUP_TABLE_META                     = 13,
  LS_ACTIVE_TABLET_ARRAY                = 14,
  LS_PENDING_FREE_TABLET_ARRAY          = 15,
  LS_TRANSFER_TABLET_ID_ARRAY           = 16,
  PRIVATE_TABLET_META                   = 17,
  PRIVATE_TABLET_CURRENT_VERSION        = 18,
  SHARED_MAJOR_TABLET_META              = 19,
  COMPACTION_SERVER                     = 20,
  LS_SVR_COMPACTION_STATUS              = 21,
  COMPACTION_REPORT                     = 22,
  SHARED_MAJOR_GC_INFO                  = 23,
  SHARED_MAJOR_META_LIST                = 24,
  LS_COMPACTION_STATUS                  = 25,
  TABLET_COMPACTION_STATUS              = 26,
  MAJOR_PREWARM_DATA                    = 27,
  MAJOR_PREWARM_DATA_INDEX              = 28,
  MAJOR_PREWARM_META                    = 29,
  MAJOR_PREWARM_META_INDEX              = 30,
  TENANT_DISK_SPACE_META                = 31,
  SHARED_TABLET_ID                      = 32,
  LS_COMPACTION_LIST                    = 33,
  IS_SHARED_TABLET_DELETED              = 34,
  IS_SHARED_TENANT_DELETED              = 35,
  CHECKSUM_ERROR_DUMP_MACRO             = 36,
  MAX   // Forbid insert object type in the middle! Only allow the tail!
        // Add new object type notice:
        // 1. if the object type only store in local cache, must add is_pin_storage_object_type, if the object type only store in remote object storage, must add is_read_through_storage_object_type
        // 2. need to modify ObBaseFileManager's is_macro_block_id_valid, macro_block_id_to_path and get_file_parent_dir function
        // 3. need to modify test_file_manager's test_path_convert case and test_get_file_parent_dir case
};

bool is_read_through_storage_object_type(const ObStorageObjectType type);
bool is_object_type_only_store_remote(const ObStorageObjectType type);
bool is_pin_storage_object_type(const ObStorageObjectType type);
bool is_ls_replica_prewarm_filter_object_type(const ObStorageObjectType type);

static const char *get_storage_objet_type_str(const ObStorageObjectType type)
{
  static const char *type_str_map_[static_cast<int32_t>(ObStorageObjectType::MAX) + 1] = {
    "PRIVATE_DATA_MACRO",
    "PRIVATE_META_MACRO",
    "SHARED_MINI_DATA_MACRO",
    "SHARED_MINI_META_MACRO",
    "SHARED_MINOR_DATA_MACRO",
    "SHARED_MINOR_META_MACRO",
    "SHARED_MAJOR_DATA_MACRO",
    "SHARED_MAJOR_META_MACRO",
    "TMP_FILE",
    "SERVER_META",
    "TENANT_SUPER_BLOCK",
    "TENANT_UNIT_META",
    "LS_META",
    "LS_DUP_TABLE_META",
    "LS_ACTIVE_TABLET_ARRAY",
    "LS_PENDING_FREE_TABLET_ARRAY",
    "LS_TRANSFER_TABLET_ID_ARRAY",
    "PRIVATE_TABLET_META",
    "PRIVATE_TABLET_CURRENT_VERSION",
    "SHARED_MAJOR_TABLET_META",
    "COMPACTION_SERVER",
    "LS_SVR_COMPACTION_STATUS",
    "COMPACTION_REPORT",
    "SHARED_MAJOR_GC_INFO",
    "SHARED_MAJOR_META_LIST",
    "LS_COMPACTION_STATUS",
    "TABLET_COMPACTION_STATUS",
    "MAJOR_PREWARM_DATA",
    "MAJOR_PREWARM_DATA_INDEX",
    "MAJOR_PREWARM_META",
    "MAJOR_PREWARM_META_INDEX",
    "TENANT_DISK_SPACE_META",
    "SHARED_TABLET_ID",
    "LS_COMPACTION_LIST",
    "IS_SHARED_TABLET_DELETED",
    "IS_SHARED_TENANT_DELETED",
    "CHECKSUM_ERROR_DUMP_MACRO",
    "MAX"
  };
  const char *type_str = type_str_map_[static_cast<int32_t>(ObStorageObjectType::MAX)];
  if (OB_LIKELY((type >= ObStorageObjectType::PRIVATE_DATA_MACRO)) ||
                (type <= ObStorageObjectType::MAX)) {
    type_str = type_str_map_[static_cast<int32_t>(type)];
  }
  return type_str;
}

class MacroBlockId final
{
public:
  MacroBlockId();
  // only for ID_MODE_LOCAL
  MacroBlockId(const uint64_t write_seq, const int64_t block_index, const int64_t third_id);
  MacroBlockId(const int64_t first_id, const int64_t second_id, const int64_t third_id, const int64_t fourth_id);
  MacroBlockId(const MacroBlockId &id);
  ~MacroBlockId() = default;

  OB_INLINE void reset()
  {
    first_id_ = 0;
    second_id_ = INT64_MAX;
    third_id_ = 0;
    fourth_id_ = 0;
    version_ = MACRO_BLOCK_ID_VERSION_V2;
  }
  bool is_valid() const;

  OB_INLINE bool is_local_id() const
  {
    return static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_LOCAL) == id_mode_;
  }
  OB_INLINE bool is_backup_id() const
  {
    return static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_BACKUP) == id_mode_;
  }
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  // reuse by ObMetaDiskAddr::to_string
  void first_id_to_string(char *buf, const int64_t buf_len, int64_t &pos) const;

  // General GETTER/SETTER
  int64_t first_id() const { return first_id_; }
  int64_t second_id() const { return second_id_; }
  int64_t third_id() const { return third_id_; }
  int64_t fourth_id() const { return fourth_id_; }

  void set_first_id(const int64_t first_id) { first_id_ = first_id; }
  void set_second_id(const int64_t second_id) { second_id_ = second_id; }
  void set_third_id(const int64_t third_id) { third_id_ = third_id; }
  void set_fourth_id(const int64_t fourth_id) { fourth_id_ = fourth_id; }

  void set_version_v2() { version_ = MACRO_BLOCK_ID_VERSION_V2; }
  uint64_t id_mode() const { return id_mode_; }
  bool is_id_mode_local() const; // sn deploy mode, but local macro id.
  bool is_id_mode_backup() const; // sn deploy mode, but backup macro id.
  bool is_id_mode_share() const; // ss deploy mode
  bool is_shared_data_or_meta() const; // shared tablet macro block in ss mode
  bool is_private_data_or_meta() const; // private tablet macro block in ss mode
  bool is_data() const; // shared data or private data
  bool is_meta() const; // shared meta or private meta
  void set_id_mode(const uint64_t id_mode) { id_mode_ = id_mode; }
  // Local mode
  int64_t block_index() const { return block_index_; }
  void set_block_index(const int64_t block_index) { block_index_ = block_index; }
  int64_t write_seq() const { return write_seq_; }
  void set_write_seq(const uint64_t write_seq) { write_seq_ = write_seq; }

  // Share mode
  void set_ss_version(const uint64_t ss_version) { ss_version_ = ss_version; }
  void set_ss_id_mode(const uint64_t ss_mode_id) { ss_id_mode_ = ss_mode_id; }
  ObStorageObjectType storage_object_type() const { return static_cast<ObStorageObjectType>(storage_object_type_); }
  void set_storage_object_type(const uint64_t storage_object_type) { storage_object_type_ = storage_object_type; }
  int64_t incarnation_id() const { return incarnation_id_; }
  void set_incarnation_id(const uint64_t incarnation_id) { incarnation_id_ = incarnation_id; }
  int64_t column_group_id() const { return column_group_id_; }
  void set_column_group_id(const uint64_t column_group_id) { column_group_id_ = column_group_id; }
  int64_t macro_transfer_seq() const { return 0; }  // TODO 这里应该return macro_transfer_seq_; 需要费渡修改 @gaishun.gs by xiaotao
  void set_macro_transfer_seq(const uint64_t macro_transfer_seq) { macro_transfer_seq_ = macro_transfer_seq; }
  int64_t tenant_seq() const { return fourth_id_; }  // TODO 这里应该return tenant_seq_; 需要费渡修改 @gaishun.gs by xiaotao
  void set_tenant_seq(const uint64_t tenant_seq) { tenant_seq_ = tenant_seq; }
  int64_t meta_transfer_seq() const { return 0; }  // TODO 这里应该return meta_transfer_seq_; 需要费渡修改 @gaishun.gs by xiaotao
  void set_meta_transfer_seq(const uint64_t meta_transfer_seq) { meta_transfer_seq_ = meta_transfer_seq; }
  int64_t meta_version_id() const { return fourth_id_; }  // TODO 这里应该return meta_version_id_; 需要费渡修改 @gaishun.gs by xiaotao
  void set_meta_version_id(const uint64_t meta_version_id) { meta_version_id_ = meta_version_id; }

  // Deivce mode
  void set_from_io_fd(const common::ObIOFd &block_id)
  {
    first_id_ = block_id.first_id_;
    second_id_ = block_id.second_id_;
    third_id_ = block_id.third_id_;
  }

  MacroBlockId& operator =(const MacroBlockId &other)
  {
    first_id_ = other.first_id_;
    second_id_ = other.second_id_;
    third_id_ = other.third_id_;
    fourth_id_ = other.fourth_id_;
    return *this;
  }
  bool operator ==(const MacroBlockId &other) const;
  bool operator !=(const MacroBlockId &other) const;
  bool operator <(const MacroBlockId &other) const;

  NEED_SERIALIZE_AND_DESERIALIZE;

  // just for compatibility, the macro block id of V1 is serialized directly by memcpy in some scenarios
  int memcpy_deserialize(const char* buf, const int64_t data_len, int64_t& pos);
  static MacroBlockId mock_valid_macro_id() { return MacroBlockId(0, AUTONOMIC_BLOCK_INDEX, 0); }

public:
  static const int64_t MACRO_BLOCK_ID_VERSION_V1 = 0;
  static const int64_t MACRO_BLOCK_ID_VERSION_V2 = 1; // addding fourth_id_ for V1

  static const int64_t EMPTY_ENTRY_BLOCK_INDEX = -1;
  static const int64_t AUTONOMIC_BLOCK_INDEX = -1;

  static const uint64_t SF_BIT_WRITE_SEQ = 52;
  static const uint64_t SF_BIT_STORAGE_OBJECT_TYPE = 8;
  static const uint64_t SF_BIT_INCARNATION_ID = 24;
  static const uint64_t SF_BIT_COLUMN_GROUP_ID = 16;
  static const uint64_t SF_BIT_RESERVED = 4;
  static const uint64_t SF_BIT_ID_MODE = 8;
  static const uint64_t SF_BIT_VERSION = 4;
  static const uint64_t SF_BIT_TRANSFER_SEQ = 20;
  static const uint64_t SF_BIT_TENANT_SEQ = 44;
  static const uint64_t SF_BIT_META_VERSION_ID = 44;

  static const uint64_t MAX_WRITE_SEQ = (0x1UL << MacroBlockId::SF_BIT_WRITE_SEQ) - 1;

private:
  static const uint64_t HASH_MAGIC_NUM = 2654435761;

private:
  union {
    int64_t first_id_;
    // for share nothing mode
    struct {
      uint64_t write_seq_ : SF_BIT_WRITE_SEQ;
      uint64_t id_mode_   : SF_BIT_ID_MODE;
      uint64_t version_   : SF_BIT_VERSION;
    };
    // for share storage mode
    struct {
      uint64_t storage_object_type_ : SF_BIT_STORAGE_OBJECT_TYPE;
      uint64_t incarnation_id_  : SF_BIT_INCARNATION_ID;
      uint64_t column_group_id_ : SF_BIT_COLUMN_GROUP_ID;
      uint64_t ss_reserved_ : SF_BIT_RESERVED;
      uint64_t ss_id_mode_   : SF_BIT_ID_MODE;
      uint64_t ss_version_   : SF_BIT_VERSION;
    };
  };
  union {
    int64_t second_id_;
    int64_t block_index_;    // the block index in the block_file when Local mode
  };
  union {
    int64_t third_id_;
    struct {
      uint64_t device_id_ : 8;
      uint64_t reserved_ : 56;
    };
  };
  union {
    int64_t fourth_id_;
    // for PRIVATE_DATA_MACRO and PRIVATE_META_MACRO
    struct {
      uint64_t macro_transfer_seq_  : SF_BIT_TRANSFER_SEQ;
      uint64_t tenant_seq_          : SF_BIT_TENANT_SEQ;
    };
    // for PRIVATE_TABLET_META and PRIVATE_TABLET_CURRENT_VERSION
    struct {
      uint64_t meta_transfer_seq_   : SF_BIT_TRANSFER_SEQ;
      uint64_t meta_version_id_     : SF_BIT_META_VERSION_ID;
    };
  };
};

OB_INLINE bool MacroBlockId::operator ==(const MacroBlockId &other) const
{
  return other.first_id_ == first_id_ && other.second_id_ == second_id_
      && other.third_id_ == third_id_ && other.fourth_id_ == fourth_id_;
}

OB_INLINE bool MacroBlockId::operator !=(const MacroBlockId &other) const
{
  return !(other == *this);
}

#define SERIALIZE_MEMBER_WITH_MEMCPY(member)                                 \
  if (OB_SUCC(ret)) {                                                        \
    if (OB_UNLIKELY(buf_len - pos < sizeof(member))) {                       \
      ret = OB_BUF_NOT_ENOUGH;                                               \
      LOG_WARN("buffer not enough", K(ret), KP(buf), K(buf_len), K(pos));    \
    } else {                                                                 \
      MEMCPY(buf + pos, &member, sizeof(member));                            \
      pos += sizeof(member);                                                 \
    }                                                                        \
  }

#define DESERIALIZE_MEMBER_WITH_MEMCPY(member)                               \
  if (OB_SUCC(ret)) {                                                        \
    if (OB_UNLIKELY(data_len - pos < sizeof(member))) {                      \
      ret = OB_DESERIALIZE_ERROR;                                            \
      LOG_WARN("buffer not enough", K(ret), KP(buf), K(data_len), K(pos));   \
    } else {                                                                 \
      MEMCPY(&member, buf + pos, sizeof(member));                            \
      pos += sizeof(member);                                                 \
    }                                                                        \
  }

} // namespace blocksstable
} // namespace oceanbase

#endif /* SRC_STORAGE_BLOCKSSTABLE_OB_MACRO_BLOCK_ID_H_ */
