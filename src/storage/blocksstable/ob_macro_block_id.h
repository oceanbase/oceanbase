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

namespace oceanbase {
namespace blocksstable {
// MacorBlockId format design document:
enum class ObMacroBlockIdMode : uint8_t {
  ID_MODE_LOCAL = 0,
  ID_MODE_APPEND,
  ID_MODE_BLOCK,
  ID_MODE_MAX,
};

class MacroBlockId final {
public:
  MacroBlockId();
  explicit MacroBlockId(const int64_t block_id);
  MacroBlockId(const MacroBlockId& id);
  MacroBlockId(
      const int32_t disk_no, const int32_t disk_install_count, const int32_t write_seq, const int32_t block_index);
  ~MacroBlockId() = default;

  void reset();
  bool is_valid() const;
  uint64_t hash() const;
  int64_t to_string(char* buf, const int64_t buf_len) const;

  // General GETTER/SETTER
  OB_INLINE int64_t first_id() const
  {
    return first_id_;
  }
  OB_INLINE int64_t second_id() const
  {
    return second_id_;
  }
  OB_INLINE int64_t third_id() const
  {
    return third_id_;
  }
  OB_INLINE int64_t fourth_id() const
  {
    return fourth_id_;
  }
  void set_first_id(int64_t first_id)
  {
    first_id_ = first_id;
  }
  void set_second_id(int64_t second_id)
  {
    second_id_ = second_id;
  }
  void set_third_id(int64_t third_id)
  {
    third_id_ = third_id;
  }
  void set_fourth_id(int64_t fourth_id)
  {
    fourth_id_ = fourth_id;
  }

  // Local mode GETTER
  OB_INLINE int32_t disk_no() const
  {
    return disk_no_;
  }
  OB_INLINE int32_t write_seq() const
  {
    return write_seq_;
  }
  OB_INLINE int64_t block_index() const
  {
    return block_index_;
  }

  // Append mode GETTER
  OB_INLINE int64_t file_offset() const
  {
    return file_offset_;
  }

  // SETTER
  void set_local_block_id(const int64_t block_index);
  void set_append_block_id(const int64_t file_id, const int64_t file_offset);

  bool operator==(const MacroBlockId& other) const;
  bool operator!=(const MacroBlockId& other) const;
  bool operator<(const MacroBlockId& other) const;

  NEED_SERIALIZE_AND_DESERIALIZE;

public:
  static ObMacroBlockIdMode DEFAULT_MODE;

private:
  // for unit test
  int serialize_old(char* buf, const int64_t buf_len, int64_t& pos) const;

private:
  // serialization
  static const uint64_t SF_BIT_RAW_ID = 56;
  static const uint64_t SF_BIT_ID_MODE = 4;
  static const uint64_t SF_BIT_VERSION = 4;
  static const uint64_t SF_MASK_RAW_ID = (0x1UL << SF_BIT_RAW_ID) - 1;
  static const uint64_t SF_MASK_ID_MODE = (0x1UL << SF_BIT_ID_MODE) - 1;
  static const uint64_t SF_MASK_VERSION = (0x1UL << SF_BIT_VERSION) - 1;
  static const int64_t MACRO_BLOCK_ID_VERSION = 1;
  static const uint64_t SF_MAX_BLOCK_INDEX_OLD = (0x1UL << 32) - 1;

  // local mode
  static const uint64_t SF_BIT_DISK_NO = 16;
  static const uint64_t SF_BIT_DISK_INSTALL_COUNT = 8;
  static const uint64_t SF_BIT_WRITE_SEQ = 20;
  static const uint64_t SF_BIT_OCCUPY_LOCAL = 20;
  static const uint64_t SF_MASK_DISK_NO = (0x1UL << SF_BIT_DISK_NO) - 1;
  static const uint64_t SF_MASK_DISK_INSTALL_COUNT = (0x1UL << SF_BIT_DISK_INSTALL_COUNT) - 1;
  static const uint64_t SF_MASK_WRITE_SEQ = (0x1UL << SF_BIT_WRITE_SEQ) - 1;
  static const uint64_t SF_FILTER_OCCUPY_LOCAL = ~((0x1UL << (64 - SF_BIT_OCCUPY_LOCAL)) - 1);
  static const uint64_t INVALID_BLOCK_INDEX_OLD_FORMAT = (0x1UL << 32) - 1;
  static const uint64_t HASH_MAGIC_NUM = 2654435761;

  // append mode
  static const uint64_t SF_BIT_FILE_ID = 32;
  static const uint64_t SF_BIT_OCCUPY_APPEND = 32;
  static const uint64_t SF_MASK_FILE_ID = (0x1UL << SF_BIT_FILE_ID) - 1;

  // block mode
  static const uint64_t SF_BIT_PEG_ID = 56;
  static const uint64_t SF_BIT_OCCUPY_BLOCK = 8;
  static const uint64_t SF_MASK_PEG_ID = (0x1UL << SF_BIT_PEG_ID) - 1;

  // Sequence id
  static const uint64_t SF_BIT_RESTART_SEQ = 32;
  static const uint64_t SF_BIT_REWRITE_SEQ = 32;
  static const uint64_t SF_MASK_RESTART_SEQ = (0x1UL << SF_BIT_RESTART_SEQ) - 1;
  static const uint64_t SF_MASK_REWRITE_SEQ = (0x1UL << SF_BIT_REWRITE_SEQ) - 1;

private:
  union {
    int64_t first_id_;
    struct  // serialization
    {
      uint64_t raw_id_ : SF_BIT_RAW_ID;    // The raw id of each mode
      uint64_t id_mode_ : SF_BIT_ID_MODE;  // mode: local/append/block
      uint64_t version_ : SF_BIT_VERSION;  // version
    };
    struct  // Local
    {
      uint64_t write_seq_ : SF_BIT_WRITE_SEQ;  // the rewrite sequence for block cache evict, doesn't serialization
      uint64_t disk_install_count_ : SF_BIT_DISK_INSTALL_COUNT;  // the disk install count, doesn't serialization
      uint64_t disk_no_ : SF_BIT_DISK_NO;                        // the disk number, doesn't serialization
      uint64_t reserved_local_ : SF_BIT_OCCUPY_LOCAL;
    };
    struct  // Append
    {
      int64_t file_id_ : SF_BIT_FILE_ID;  // the unique data file id
      uint64_t reserved_append_ : SF_BIT_OCCUPY_APPEND;
    };
    struct  // Block
    {
      int64_t peg_id_ : SF_BIT_PEG_ID;  // block_id.first_id_
      uint64_t reserved_block_ : SF_BIT_OCCUPY_BLOCK;
    };
  };
  union {
    int64_t second_id_;
    int64_t block_index_;  // the block index in the block_file when Local mode
    int64_t file_offset_;  // the file offset when Append mode
  };
  union {
    int64_t third_id_;
    struct  // Block mode Uniquely mark an block id
    {
      uint64_t rewrite_seq_ : SF_BIT_REWRITE_SEQ;
      uint64_t restart_seq_ : SF_BIT_RESTART_SEQ;
    };
  };
  union {
    int64_t fourth_id_;
    int64_t pool_id_;  // the storage pool id of OFS
  };
};

OB_INLINE bool MacroBlockId::operator==(const MacroBlockId& other) const
{
  return other.first_id_ == first_id_ && other.second_id_ == second_id_ && other.third_id_ == third_id_ &&
         other.fourth_id_ == fourth_id_;
}

OB_INLINE bool MacroBlockId::operator!=(const MacroBlockId& other) const
{
  return !(other == *this);
}
}  // namespace blocksstable
}  // namespace oceanbase

#endif /* SRC_STORAGE_BLOCKSSTABLE_OB_MACRO_BLOCK_ID_H_ */
