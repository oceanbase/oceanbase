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

namespace oceanbase
{
namespace blocksstable
{
enum class ObMacroBlockIdMode : uint8_t
{
  ID_MODE_LOCAL = 0,
  ID_MODE_MAX,
};

class MacroBlockId final
{
public:
  MacroBlockId();
  MacroBlockId(const int64_t first_id, const int64_t second_id, const int64_t third_id);
  MacroBlockId(const MacroBlockId &id);
  ~MacroBlockId() = default;

  OB_INLINE void reset()
  {
    first_id_ = 0;
    second_id_ = INT64_MAX;
    third_id_ = 0;
  }
  OB_INLINE bool is_valid() const
  {
    return MACRO_BLOCK_ID_VERSION == version_
        && id_mode_ < (uint64_t)ObMacroBlockIdMode::ID_MODE_MAX
        && second_id_ >= AUTONOMIC_BLOCK_INDEX && second_id_ < INT64_MAX
        && 0 == third_id_;
  }

  uint64_t hash() const;
  int hash(uint64_t &hash_val) const;
  int64_t to_string(char *buf, const int64_t buf_len) const;

  // General GETTER/SETTER
  int64_t first_id() const { return first_id_; }
  int64_t second_id() const { return second_id_; }
  int64_t third_id() const { return third_id_; }

  // Local mode
  int64_t block_index() const { return block_index_; }
  void set_block_index(const int64_t block_index) { block_index_ = block_index; }
  int64_t write_seq() const { return write_seq_; }
  void set_write_seq(const uint64_t write_seq) { write_seq_ = write_seq; }
  int64_t device_id() const { return device_id_; }
  void set_device_id(const uint64_t device_id) { device_id_ = device_id; }

  MacroBlockId& operator =(const MacroBlockId &other)
  {
    first_id_ = other.first_id_;
    second_id_ = other.second_id_;
    third_id_ = other.third_id_;
    return *this;
  }
  bool operator ==(const MacroBlockId &other) const;
  bool operator !=(const MacroBlockId &other) const;
  bool operator <(const MacroBlockId &other) const;

  NEED_SERIALIZE_AND_DESERIALIZE;

public:
  static const int64_t MACRO_BLOCK_ID_VERSION = 0;

  static const int64_t EMPTY_ENTRY_BLOCK_INDEX = -1;
  static const int64_t AUTONOMIC_BLOCK_INDEX = -1;

  static const uint64_t SF_BIT_WRITE_SEQ = 52;
  static const uint64_t SF_BIT_ID_MODE = 8;
  static const uint64_t SF_BIT_VERSION = 4;

  static const uint64_t MAX_WRITE_SEQ = (0x1UL << MacroBlockId::SF_BIT_WRITE_SEQ) - 1;

private:
  static const uint64_t HASH_MAGIC_NUM = 2654435761;

private:
  union {
    int64_t first_id_;
    struct {
      uint64_t write_seq_ : SF_BIT_WRITE_SEQ;
      uint64_t id_mode_   : SF_BIT_ID_MODE;
      uint64_t version_   : SF_BIT_VERSION;
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
};

OB_INLINE bool MacroBlockId::operator ==(const MacroBlockId &other) const
{
  return other.first_id_ == first_id_ && other.second_id_ == second_id_
      && other.third_id_ == third_id_;
}

OB_INLINE bool MacroBlockId::operator !=(const MacroBlockId &other) const
{
  return !(other == *this);
}
} // namespace blocksstable
} // namespace oceanbase

#endif /* SRC_STORAGE_BLOCKSSTABLE_OB_MACRO_BLOCK_ID_H_ */
