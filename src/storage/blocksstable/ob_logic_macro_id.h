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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_LOGIC_MACRO_BLOCK_ID_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_LOGIC_MACRO_BLOCK_ID_H_

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "common/ob_action_flag.h"
#include "common/object/ob_obj_type.h"
#include "common/ob_accuracy.h"
#include "lib/checksum/ob_crc64.h"

namespace oceanbase
{
namespace blocksstable
{

struct ObMacroDataSeq
{
  static const int64_t BIT_DATA_SEQ = 32;
  static const int64_t BIT_PARALLEL_IDX = 11;
  static const int64_t BIT_BLOCK_TYPE = 3;
  static const int64_t BIT_MERGE_TYPE = 2;
  static const int64_t BIT_SSTABLE_SEQ = 10;
  static const int64_t BIT_RESERVED = 5;
  static const int64_t BIT_SIGN = 1;
  static const int64_t MAX_PARALLEL_IDX = (0x1UL << BIT_PARALLEL_IDX) - 1;
  static const int64_t MAX_SSTABLE_SEQ = (0x1UL << BIT_SSTABLE_SEQ) - 1;
  enum BlockType {
    DATA_BLOCK = 0,
    INDEX_BLOCK = 1,
    META_BLOCK = 2,
  };
  enum MergeType {
    MAJOR_MERGE = 0,
    MINOR_MERGE = 1,
    REBUILD_MACRO_BLOCK_MERGE = 2,
  };

  ObMacroDataSeq() : macro_data_seq_(0) {}
  ObMacroDataSeq(const int64_t data_seq) : macro_data_seq_(data_seq) {}
  virtual ~ObMacroDataSeq() = default;
  ObMacroDataSeq &operator=(const ObMacroDataSeq &other)
  {
    if (this != &other) {
      macro_data_seq_ = other.macro_data_seq_;
    }
    return *this;
  }
  bool operator ==(const ObMacroDataSeq &other) const { return macro_data_seq_ == other.macro_data_seq_; }
  bool operator !=(const ObMacroDataSeq &other) const { return macro_data_seq_ != other.macro_data_seq_; }
  OB_INLINE void reset() { macro_data_seq_ = 0; }
  OB_INLINE int64_t get_data_seq() const { return macro_data_seq_; }
  OB_INLINE int64_t get_parallel_idx() const { return parallel_idx_; }
  OB_INLINE bool is_valid() const { return macro_data_seq_ >= 0; }
  OB_INLINE bool is_data_block() const { return block_type_ == DATA_BLOCK; }
  OB_INLINE bool is_index_block() const { return block_type_ == INDEX_BLOCK; }
  OB_INLINE bool is_meta_block() const { return block_type_ == META_BLOCK; }
  OB_INLINE bool is_major_merge() const { return merge_type_ == MAJOR_MERGE; }
  OB_INLINE void set_rebuild_merge_type()
  {
    merge_type_ = REBUILD_MACRO_BLOCK_MERGE;
  }
  OB_INLINE int set_sstable_seq(const int16_t sstable_logic_seq)
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(sstable_logic_seq >= MAX_SSTABLE_SEQ || sstable_logic_seq < 0)) {
      ret = common::OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "Invalid sstable seq", K(ret), K(sstable_logic_seq));
    } else {
      sstable_logic_seq_ = sstable_logic_seq;
    }
    return ret;
  }
  OB_INLINE int set_parallel_degree(const int64_t parallel_idx)
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(parallel_idx >= MAX_PARALLEL_IDX || parallel_idx < 0)) {
      ret = common::OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "Invalid parallel idx", K(parallel_idx));
    } else {
      parallel_idx_ = parallel_idx;
    }
    return ret;
  }
  OB_INLINE void set_data_block() { block_type_ = DATA_BLOCK; }
  OB_INLINE void set_index_block() { block_type_ = INDEX_BLOCK; }
  OB_INLINE void set_macro_meta_block() { block_type_ = META_BLOCK; }
  OB_INLINE void set_index_merge_block() { block_type_ = INDEX_BLOCK; parallel_idx_ = MAX_PARALLEL_IDX; }
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(const char *buf, const int64_t data_len, int64_t &pos);
  int64_t get_serialize_size() const;
  TO_STRING_KV(K_(data_seq), K_(parallel_idx), K_(block_type), K_(merge_type),
      K_(sstable_logic_seq), K_(reserved), K_(sign), K_(macro_data_seq));
  union
  {
    int64_t macro_data_seq_;
    struct
    {
      uint64_t data_seq_ : BIT_DATA_SEQ;
      uint64_t parallel_idx_ : BIT_PARALLEL_IDX;
      uint64_t block_type_ : BIT_BLOCK_TYPE;
      uint64_t merge_type_ : BIT_MERGE_TYPE;
      uint64_t sstable_logic_seq_ : BIT_SSTABLE_SEQ;
      uint64_t reserved_ : BIT_RESERVED;
      uint64_t sign_ : BIT_SIGN;
    };
  };
};


struct ObLogicMacroBlockId
{
private:
  static const int64_t LOGIC_BLOCK_ID_VERSION = 1;

public:
  ObLogicMacroBlockId()
    : data_seq_(), logic_version_(0), tablet_id_(0 /* ObTabletID::INVALID_TABLET_ID */), info_(0)
  {}
  ObLogicMacroBlockId(const int64_t data_seq, const uint64_t logic_version, const int64_t tablet_id)
    : data_seq_(data_seq), logic_version_(logic_version), tablet_id_(tablet_id), info_(0)
  {}

  int64_t hash() const;
  int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
  bool operator ==(const ObLogicMacroBlockId &other) const;
  bool operator !=(const ObLogicMacroBlockId &other) const;
  bool operator <(const ObLogicMacroBlockId &other) const;
  bool operator >(const ObLogicMacroBlockId &other) const;
  void reset();
  OB_INLINE bool is_valid() const
  {
    return data_seq_.is_valid() && logic_version_ > 0 && tablet_id_ > 0;
  }

  TO_STRING_KV(K_(data_seq), K_(logic_version), K_(tablet_id), K_(column_group_idx), K_(is_mds));

public:
  ObMacroDataSeq data_seq_;
  uint64_t logic_version_;
  int64_t tablet_id_;

  union {
    uint64_t info_;
    struct {
      uint64_t column_group_idx_ : 16;
      bool is_mds_               :  1;
      uint64_t reserved_         : 47;
    };
  };
  OB_UNIS_VERSION(LOGIC_BLOCK_ID_VERSION);
};

} // blocksstable
} // oceanbase

#endif
