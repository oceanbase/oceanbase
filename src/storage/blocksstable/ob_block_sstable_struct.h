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

#ifndef __BLOCK_SSTABLE_DATA_STRUCTURE_H__
#define __BLOCK_SSTABLE_DATA_STRUCTURE_H__

#include "lib/allocator/ob_mod_define.h"
#include "lib/container/ob_se_array.h"
#include "lib/io/ob_io_benchmark.h"
#include "lib/container/ob_iarray.h"
#include "lib/hash/ob_pointer_hashmap.h"
#include "common/log/ob_log_cursor.h"
#include "common/ob_store_format.h"
#include "share/schema/ob_table_schema.h"
#include "share/ob_encryption_util.h"
#include "storage/ob_i_store.h"
#include "ob_macro_block_common_header.h"
#include "ob_macro_block_id.h"

namespace oceanbase {
namespace storage {
class ObSSTable;
}
namespace blocksstable {

struct ObSuperBlockV2;
class ObStorageFile;

extern const char* BLOCK_SSTBALE_DIR_NAME;
extern const char* BLOCK_SSTBALE_FILE_NAME;

// block sstable header magic number;
const int64_t SUPER_BLOCK_MAGIC = 1000;
// const int64_t MACRO_BLOCK_COMMON_HEADER_MAGIC = 1001; // defined in ob_macro_block_common_header.h
const int64_t PARTITION_META_HEADER_MAGIC = 1002;
const int64_t SCHEMA_DATA_HEADER_MAGIC = 1003;
const int64_t COMPRESSOR_HEADER_MAGIC = 1004;
const int64_t MICRO_BLOCK_HEADER_MAGIC = 1005;
const int64_t MACRO_META_HEADER_MAGIC = 1006;
const int64_t SSTABLE_DATA_HEADER_MAGIC = 1007;
const int64_t MACRO_BLOCK_SECOND_INDEX_MAGIC = 1008;
const int64_t LOB_MACRO_BLOCK_HEADER_MAGIC = 1009;
const int64_t LOB_MICRO_BLOCK_HEADER_MAGIC = 1010;
const int64_t TABLE_MGR_META_HEADER_MAGIC = 1011;
const int64_t SUPER_BLOCK_MAGIC_V2 = 1012;
const int64_t TENANT_UNIT_META_HEADER_MAGIC = 1013;
const int64_t BF_MACRO_BLOCK_HEADER_MAGIC = 1014;
const int64_t BF_MICRO_BLOCK_HEADER_MAGIC = 1015;
const int64_t RAID_STRIP_HEADER_MAGIC = 1016;
const int64_t PG_ROOT_MAGIC = 1017;
const int64_t SERVER_SUPER_BLOCK_MAGIC = 1018;
const int64_t LINKED_MACRO_BLOCK_HEADER_MAGIC = 1019;

const int64_t MACRO_BLOCK_WITH_ENCODING_VERSION = 2;
const int64_t SSTABLE_MACRO_BLOCK_HEADER_VERSION_v1 = 1;
const int64_t SSTABLE_MACRO_BLOCK_HEADER_VERSION_v2 = 2;
const int64_t SSTABLE_MACRO_BLOCK_HEADER_VERSION_v3 = 3;  // add column order info to header
const int64_t MICRO_BLOCK_HEADER_VERSION = 1;
const int64_t MICRO_BLOCK_HEADERV2_VERSION = 1;
const int64_t LINKED_MACRO_BLOCK_HEADER_VERSION = 1;
const int64_t LOB_MACRO_BLOCK_HEADER_VERSION_V1 = 1;
const int64_t LOB_MACRO_BLOCK_HEADER_VERSION_V2 = 2;  // add column order info to header
const int64_t LOB_MICRO_BLOCK_HEADER_VERSION = 1;
const int64_t BF_MACRO_BLOCK_HEADER_VERSION = 1;
const int64_t BF_MICRO_BLOCK_HEADER_VERSION = 1;
const int64_t DISK_STRING_DEFAULT_LEN = 512;
const int64_t EXPIRE_VERSION_DELAY_TIME = 15L * 60L * 1000L * 1000L;  // 15 minutes
const int64_t PG_SUPER_BLOCK_HEADER_VERSION = 1;
const int64_t SERVER_SUPER_BLOCK_HEADER_VERSION = 1;

enum ObStoreFileSystemType { STORE_FILE_SYSTEM_LOCAL = 0, STORE_FILE_SYSTEM_RAID = 2, STORE_FILE_SYSTEM_MAX };

enum ObStoreFileType {
  STORE_FILE_SUPER_BLOCK = 0,
  STORE_FILE_META_BLOCK = 1,
  STORE_FILE_MACRO_BLOCK = 2,
  STORE_FILE_TYPE_MAX,
};

struct ObPosition {
  int32_t offset_;
  int32_t length_;
  ObPosition() : offset_(0), length_(0)
  {}
  void reset()
  {
    offset_ = 0;
    length_ = 0;
  }
  bool is_valid() const
  {
    return offset_ >= 0 && length_ >= 0;
  }
  TO_STRING_KV(K_(offset), K_(length));
};

struct ObMacroDataSeq {
  static const int64_t BIT_DATA_SEQ = 32;
  static const int64_t BIT_PARALLEL_IDX = 11;
  static const int64_t BIT_SPLIT_FLAG = 1;
  static const int64_t BIT_BLOCK_TYPE = 2;
  static const int64_t BIT_MERGE_TYPE = 2;
  static const int64_t BIT_RESERVED = 15;
  static const int64_t BIT_SIGN = 1;
  static const int64_t MAX_PARALLEL_IDX = (0x1UL << BIT_PARALLEL_IDX) - 1;
  enum BlockType {
    DATA_BLOCK = 0,
    LOB_BLOCK = 1,
  };
  enum MergeType {
    MAJOR_MERGE = 0,
    MINOR_MERGE = 1,
  };
  enum SplitStatus {
    NO_SPLIT = 0,
    IN_SPLIT = 1,
  };
  ObMacroDataSeq() : macro_data_seq_(0)
  {}
  ObMacroDataSeq(const int64_t data_seq) : macro_data_seq_(data_seq)
  {}
  virtual ~ObMacroDataSeq() = default;
  ObMacroDataSeq& operator=(const ObMacroDataSeq& other)
  {
    if (this != &other) {
      macro_data_seq_ = other.macro_data_seq_;
    }
    return *this;
  }
  bool operator==(const ObMacroDataSeq& other) const
  {
    return macro_data_seq_ == other.macro_data_seq_;
  }
  bool operator!=(const ObMacroDataSeq& other) const
  {
    return macro_data_seq_ != other.macro_data_seq_;
  }
  OB_INLINE void reset()
  {
    macro_data_seq_ = 0;
  }
  OB_INLINE int64_t get_data_seq() const
  {
    return macro_data_seq_;
  }
  OB_INLINE bool is_valid() const
  {
    return macro_data_seq_ >= 0;
  }
  OB_INLINE bool is_lob_block() const
  {
    return block_type_ == LOB_BLOCK;
  }
  OB_INLINE bool is_in_split() const
  {
    return split_flag_ == IN_SPLIT;
  }
  OB_INLINE bool is_major_merge() const
  {
    return merge_type_ == MAJOR_MERGE;
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
  OB_INLINE void set_lob_block()
  {
    block_type_ = LOB_BLOCK;
  }
  OB_INLINE void set_split_status()
  {
    split_flag_ = IN_SPLIT;
  }
  TO_STRING_KV(K_(data_seq), K_(parallel_idx), K_(split_flag), K_(block_type), K_(merge_type), K_(reserved), K_(sign),
      K_(macro_data_seq));
  union {
    int64_t macro_data_seq_;
    struct {
      uint64_t data_seq_ : BIT_DATA_SEQ;
      uint64_t parallel_idx_ : BIT_PARALLEL_IDX;
      uint64_t split_flag_ : BIT_SPLIT_FLAG;
      uint64_t block_type_ : BIT_BLOCK_TYPE;
      uint64_t merge_type_ : BIT_MERGE_TYPE;
      uint64_t reserved_ : BIT_RESERVED;
      uint64_t sign_ : BIT_SIGN;
    };
  };
};

struct ObCommitLogSpec {
  const char* log_dir_;
  int64_t max_log_size_;
  int64_t log_sync_type_;
  ObCommitLogSpec()
  {
    memset(this, 0, sizeof(*this));
  }
  bool is_valid() const
  {
    return NULL != log_dir_ && max_log_size_ > 0 && (log_sync_type_ == 0 || log_sync_type_ == 1);
  }
  TO_STRING_KV(K_(log_dir), K_(max_log_size), K_(log_sync_type));
};

struct ObStorageEnv {
  enum REDUNDANCY_LEVEL { EXTERNAL_REDUNDANCY = 0, NORMAL_REDUNDANCY = 1, HIGH_REDUNDANCY = 2, MAX_REDUNDANCY };
  // for disk manager
  const char* data_dir_;
  const char* sstable_dir_;
  int64_t default_block_size_;
  int64_t disk_avail_space_;
  int64_t datafile_disk_percentage_;
  REDUNDANCY_LEVEL redundancy_level_;

  // for sstable log writer
  ObCommitLogSpec log_spec_;

  // for clog writer
  const char* clog_dir_;
  const char* ilog_dir_;
  const char* clog_shm_path_;
  const char* ilog_shm_path_;

  // for cache
  int64_t index_cache_priority_;
  int64_t user_block_cache_priority_;
  int64_t user_row_cache_priority_;
  int64_t fuse_row_cache_priority_;
  int64_t bf_cache_priority_;
  int64_t clog_cache_priority_;
  int64_t index_clog_cache_priority_;
  int64_t bf_cache_miss_count_threshold_;

  int64_t ethernet_speed_;
  common::ObDiskType disk_type_;

  ObStorageEnv()
  {
    memset(this, 0, sizeof(*this));
  }
  bool is_valid() const;
  TO_STRING_KV(K_(data_dir), K_(default_block_size), K_(disk_avail_space), K_(datafile_disk_percentage),
      K_(redundancy_level), K_(log_spec), K_(clog_dir), K_(ilog_dir), K_(clog_shm_path), K_(ilog_shm_path),
      K_(index_cache_priority), K_(user_block_cache_priority), K_(user_row_cache_priority), K_(fuse_row_cache_priority),
      K_(bf_cache_priority), K_(clog_cache_priority), K_(index_clog_cache_priority), K_(bf_cache_miss_count_threshold),
      K_(ethernet_speed));
};

// all structures blow includes two kinds of data:
// 1. Header : dump memory data to data file directly.
// 2. Meta: serialize & deserialize to data file.
enum ObSuperBlockVersion {
  OB_SUPER_BLOCK_V1 = 1,
  OB_SUPER_BLOCK_V2 = 2,
  OB_SUPER_BLOCK_V3 = 3,
  OB_SUPER_BLOCK_VERSION_MAX
};

struct ObSuperBlockHeader {
  static const int64_t OB_MAX_SUPER_BLOCK_SIZE = 64 * 1024;
  int32_t super_block_size_;  // not used any more
  int32_t version_;
  int32_t magic_;  // magic number
  int32_t attr_;   // reserved, set 0

  ObSuperBlockHeader();
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(super_block_size), K_(version), K_(magic), K_(attr));
  NEED_SERIALIZE_AND_DESERIALIZE;
};

struct ObSuperBlockHeaderV2 final {
public:
  ObSuperBlockHeaderV2();
  ~ObSuperBlockHeaderV2() = default;
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(version), K_(magic));
  NEED_SERIALIZE_AND_DESERIALIZE;
  static const int32_t HEADER_VERSION = 3;

public:
  int32_t version_;
  int32_t magic_;  // magic number
};

struct ObSuperBlockV1 {
  ObSuperBlockV1();
  static const int64_t MAX_BACKUP_META_COUNT = 2;
  static const int64_t SUPER_BLOCK_RESERVED_COUNT = 7;
  static const int64_t MAX_SUPER_BLOCK_SIZE = 1 << 12;

  ObSuperBlockHeader header_;  // checksum_ is 0 for v1
  int64_t create_timestamp_;   // create timestamp
  int64_t modify_timestamp_;   // last modified timestamp
  int64_t macro_block_size_;
  int64_t total_macro_block_count_;
  int64_t reserved_block_count_;
  int64_t free_macro_block_count_;
  int64_t first_macro_block_;
  int64_t first_free_block_index_;
  int64_t total_file_size_;

  int64_t backup_meta_count_;
  int64_t backup_meta_blocks_[MAX_BACKUP_META_COUNT];

  // entry of macro block meta blocks,
  int64_t macro_block_meta_entry_block_index_;
  int64_t partition_meta_entry_block_index_;  // entry of partition meta blocks.
  common::ObLogCursor replay_start_point_;
  int64_t table_mgr_meta_entry_block_index_;  // entry of table mgr macro block meta blocks.
  int64_t partition_meta_log_seq_;            // log seq of partition meta in checkpoint
  int64_t table_mgr_meta_log_seq_;            // log seq of table mgr meta in checkpoint
  int64_t reserved_[SUPER_BLOCK_RESERVED_COUNT];

  bool is_valid() const;
  int read_super_block_buf(char* buf, const int64_t buf_size, int64_t& pos);
  int write_super_block_buf(char* buf, const int64_t buf_size, int64_t& pos) const;
  int set_super_block(const ObSuperBlockV2& other);
  TO_STRING_KV(K_(header), K_(create_timestamp), K_(modify_timestamp), K_(macro_block_size),
      K_(total_macro_block_count), K_(reserved_block_count), K_(free_macro_block_count), K_(first_macro_block),
      K_(first_free_block_index), K_(total_file_size), K_(backup_meta_count), "backup_meta_blocks_",
      common::ObArrayWrap<int64_t>(backup_meta_blocks_, backup_meta_count_), K_(macro_block_meta_entry_block_index),
      K_(partition_meta_entry_block_index), K_(table_mgr_meta_entry_block_index), K_(partition_meta_log_seq),
      K_(table_mgr_meta_log_seq), K_(replay_start_point));

  NEED_SERIALIZE_AND_DESERIALIZE;
};

// must guarantee operator == always success
struct ObSuperBlockV2 {
  struct MetaEntry {
    static const int64_t META_ENTRY_VERSION = 1;
    int64_t block_index_;  // first entry meta macro block id
    int64_t log_seq_;      // replay log seq
    int64_t file_id_;      // ofs file id
    int64_t file_size_;    // ofs file size
    MetaEntry();
    bool is_valid() const;
    void reset();
    TO_STRING_KV(K_(block_index), K_(log_seq), K_(file_id), K_(file_size));
    OB_UNIS_VERSION(META_ENTRY_VERSION);
  };

  struct SuperBlockContent {
    static const int64_t SUPER_BLOCK_CONTENT_VERSION = 2;

    int64_t create_timestamp_;  // create timestamp
    int64_t modify_timestamp_;  // last modified timestamp
    int64_t macro_block_size_;
    int64_t total_macro_block_count_;
    int64_t free_macro_block_count_;
    int64_t total_file_size_;

    // entry of macro block meta blocks,
    common::ObLogCursor replay_start_point_;
    MetaEntry macro_block_meta_;
    MetaEntry partition_meta_;
    MetaEntry table_mgr_meta_;
    MetaEntry tenant_config_meta_;

    SuperBlockContent();
    bool is_valid() const;
    void reset();
    TO_STRING_KV(K_(create_timestamp), K_(modify_timestamp), K_(macro_block_size), K_(total_macro_block_count),
        K_(free_macro_block_count), K_(total_file_size), K_(replay_start_point), K_(macro_block_meta),
        K_(partition_meta), K_(table_mgr_meta), K_(tenant_config_meta));
    OB_UNIS_VERSION(SUPER_BLOCK_CONTENT_VERSION);
  };

  ObSuperBlockV2();
  ObSuperBlockHeader header_;
  SuperBlockContent content_;

  bool is_valid() const;
  void super_block();
  int serialize(char* buf, const int64_t buf_size, int64_t& pos) const;
  int deserialize(const char* buf, const int64_t buf_size, int64_t& pos);
  int set_super_block(const ObSuperBlockV1& other);
  void reset();
  OB_INLINE int64_t get_macro_block_size() const
  {
    return content_.macro_block_size_;
  }
  OB_INLINE int64_t get_total_macro_block_count() const
  {
    return content_.total_macro_block_count_;
  }
  OB_INLINE int64_t get_free_macro_block_count() const
  {
    return content_.free_macro_block_count_;
  }
  OB_INLINE int64_t get_used_macro_block_count() const
  {
    return content_.total_macro_block_count_ - content_.free_macro_block_count_;
  }
  OB_INLINE int64_t get_serialize_size() const
  {
    return header_.get_serialize_size() + content_.get_serialize_size();
  }
  int fill_super_block_size();
  TO_STRING_KV(K_(header), K_(content));
};

struct ObMicroBlockHeader {
  int32_t header_size_;
  int32_t version_;
  int32_t magic_;
  int32_t attr_;
  int32_t column_count_;
  int32_t row_index_offset_;
  int32_t row_count_;
  ObMicroBlockHeader()
  {
    memset(this, 0, sizeof(*this));
  }
  bool is_valid() const;
  TO_STRING_KV(
      K_(header_size), K_(version), K_(magic), K_(attr), K_(column_count), K_(row_index_offset), K_(row_count));
};

struct ObMicroBlockHeaderV2 {
  int16_t header_size_;
  int16_t version_;
  int16_t row_count_;
  int16_t var_column_count_;
  int32_t row_data_offset_;
  union {
    struct {
      uint16_t row_index_byte_ : 3;
      uint16_t extend_value_bit_ : 3;
      uint16_t store_row_header_ : 1;
    };
    uint16_t opt_;
  };
  int16_t reserved_;

  ObMicroBlockHeaderV2();
  OB_INLINE bool is_valid() const;
  void reset()
  {
    new (this) ObMicroBlockHeaderV2();
  }

  TO_STRING_KV(K_(header_size), K_(version), K_(row_count), K_(var_column_count), K_(row_data_offset),
      K_(row_index_byte), K_(extend_value_bit), K_(store_row_header));
} __attribute__((packed));

OB_INLINE bool ObMicroBlockHeaderV2::is_valid() const
{
  return header_size_ >= sizeof(*this) && version_ >= MICRO_BLOCK_HEADERV2_VERSION && row_count_ > 0 &&
         row_index_byte_ >= 0 && extend_value_bit_ >= 0 && row_data_offset_ >= 0;
}

struct ObBloomFilterMicroBlockHeader {
  ObBloomFilterMicroBlockHeader()
  {
    reset();
  }
  void reset()
  {
    MEMSET(this, 0, sizeof(ObBloomFilterMicroBlockHeader));
  }
  OB_INLINE bool is_valid() const
  {
    return header_size_ == sizeof(ObBloomFilterMicroBlockHeader) && version_ >= BF_MICRO_BLOCK_HEADER_VERSION &&
           BF_MICRO_BLOCK_HEADER_MAGIC == magic_ && rowkey_column_count_ > 0 && row_count_ > 0;
  }
  TO_STRING_KV(K_(header_size), K_(version), K_(magic), K_(rowkey_column_count), K_(row_count));

  int16_t header_size_;
  int16_t version_;
  int16_t magic_;
  int16_t rowkey_column_count_;
  int32_t row_count_;
  int32_t reserved_;
};

struct ObLinkedMacroBlockHeader {
  int32_t header_size_;
  int32_t version_;
  int32_t magic_;
  int32_t attr_;
  int64_t meta_data_offset_;      // data offset base on current header
  int64_t meta_data_count_;       // current item count in current block
  int64_t previous_block_index_;  // last link block
  int64_t total_previous_count_;  // total item count in all previous blocks
  int64_t user_data1_;
  int64_t user_data2_;

  ObLinkedMacroBlockHeader()
  {
    memset(this, 0, sizeof(*this));
  }
  bool is_valid() const;
  TO_STRING_KV(K_(header_size), K_(version), K_(magic), K_(attr), K_(meta_data_offset), K_(meta_data_count),
      K_(previous_block_index), K_(total_previous_count), K_(user_data1), K_(user_data2));
};

struct ObLinkedMacroBlockHeaderV2 final {
public:
  ObLinkedMacroBlockHeaderV2()
  {
    reset();
  }
  ~ObLinkedMacroBlockHeaderV2() = default;
  bool is_valid() const;
  const MacroBlockId get_previous_block_id() const;
  void set_previous_block_id(const MacroBlockId& block_id);
  int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;
  int deserialize(const char* buf, const int64_t data_len, int64_t& pos);
  int64_t get_serialize_size() const;
  void reset()
  {
    header_size_ = sizeof(ObLinkedMacroBlockHeaderV2);
    version_ = (int32_t)LINKED_MACRO_BLOCK_HEADER_VERSION;
    magic_ = (int32_t)LINKED_MACRO_BLOCK_HEADER_MAGIC;
    attr_ = 0;
    item_count_ = 0;
    fragment_offset_ = 0;
    previous_block_first_id_ = 0;
    previous_block_second_id_ = 0;
    previous_block_third_id_ = 0;
    previous_block_fourth_id_ = 0;
  }
  TO_STRING_KV(K_(header_size), K_(version), K_(magic), K_(attr), K_(item_count), K_(fragment_offset),
      K_(previous_block_first_id), K_(previous_block_second_id), K_(previous_block_third_id),
      K_(previous_block_fourth_id));

public:
  int32_t header_size_;
  int32_t version_;
  int32_t magic_;
  int32_t attr_;
  int32_t item_count_;
  int32_t fragment_offset_;
  // record previous macro block's MacroBlockId
  int64_t previous_block_first_id_;
  int64_t previous_block_second_id_;
  int64_t previous_block_third_id_;
  int64_t previous_block_fourth_id_;
};

struct ObSSTableMacroBlockHeader {
  int32_t header_size_;
  int32_t version_;
  int32_t magic_;  // magic number;
  int32_t attr_;
  uint64_t table_id_;
  int64_t data_version_;
  int32_t column_count_;
  int32_t rowkey_column_count_;
  int32_t column_index_scale_;  // index store scale
  int32_t row_store_type_;      // 0 flat
  int32_t row_count_;
  int32_t occupy_size_;        // occupy size of the whole macro block, include common header
  int32_t micro_block_count_;  // block count
  int32_t micro_block_size_;
  int32_t micro_block_data_offset_;
  int32_t micro_block_data_size_;
  int32_t micro_block_index_offset_;
  int32_t micro_block_index_size_;
  int32_t micro_block_endkey_offset_;
  int32_t micro_block_endkey_size_;
  int64_t data_checksum_;  // ?? no need ??
  char compressor_name_[common::OB_MAX_HEADER_COMPRESSOR_NAME_LENGTH];

  int64_t encrypt_id_;
  int64_t master_key_id_;
  char encrypt_key_[share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH];
  // end encrypt
  int64_t data_seq_;
  int64_t partition_id_;  // added since 2.-
  ObSSTableMacroBlockHeader()
  {
    reset();
  }
  bool is_valid() const;
  void reset()
  {
    memset(this, 0, sizeof(ObSSTableMacroBlockHeader));
  }
  TO_STRING_KV(K_(header_size), K_(version), K_(magic), K_(attr), K_(table_id), K_(data_version), K_(column_count),
      K_(rowkey_column_count), K_(column_index_scale), K_(row_store_type), K_(row_count), K_(occupy_size),
      K_(micro_block_count), K_(micro_block_size), K_(micro_block_data_offset), K_(micro_block_data_size),
      K_(micro_block_index_offset), K_(micro_block_index_size), K_(micro_block_endkey_offset),
      K_(micro_block_endkey_size), K_(data_checksum), K_(compressor_name), K_(encrypt_id), K_(master_key_id),
      K_(encrypt_key), K_(data_seq), K_(partition_id));
};

struct ObBloomFilterMacroBlockHeader {
  ObBloomFilterMacroBlockHeader()
  {
    reset();
  }
  void reset()
  {
    MEMSET(this, 0, sizeof(ObBloomFilterMacroBlockHeader));
  }
  OB_INLINE bool is_valid() const
  {
    return header_size_ > 0 && version_ >= BF_MACRO_BLOCK_HEADER_VERSION && BF_MACRO_BLOCK_HEADER_MAGIC == magic_ &&
           common::OB_INVALID_ID != table_id_ && data_version_ >= 0 && rowkey_column_count_ > 0 && row_count_ > 0 &&
           occupy_size_ > 0 && micro_block_count_ > 0 && micro_block_data_offset_ > 0 && micro_block_data_size_ > 0 &&
           data_checksum_ >= 0 && partition_id_ >= -1 && ObMacroBlockCommonHeader::BloomFilterData == attr_;
  }
  TO_STRING_KV(K_(header_size), K_(version), K_(magic), K_(attr), K_(table_id), K_(partition_id), K_(data_version),
      K_(rowkey_column_count), K_(row_count), K_(occupy_size), K_(micro_block_count), K_(micro_block_data_offset),
      K_(micro_block_data_size), K_(data_checksum), K_(compressor_name));

  int32_t header_size_;
  int32_t version_;
  int32_t magic_;  // magic number;
  int32_t attr_;
  uint64_t table_id_;
  int64_t partition_id_;  // added since 2.-
  int64_t data_version_;
  int32_t rowkey_column_count_;
  int32_t row_count_;
  int32_t occupy_size_;        // occupy size of the whole macro block, include common header
  int32_t micro_block_count_;  // block count
  int32_t micro_block_data_offset_;
  int32_t micro_block_data_size_;
  int64_t data_checksum_;
  char compressor_name_[common::OB_MAX_COMPRESSOR_NAME_LENGTH];
};

class ObBufferReader;
class ObBufferWriter;

struct ObMacroBlockSchemaInfo final {
public:
  static const int64_t MACRO_BLOCK_SCHEMA_INFO_HEADER_VERSION = 1;
  ObMacroBlockSchemaInfo();
  ~ObMacroBlockSchemaInfo() = default;
  NEED_SERIALIZE_AND_DESERIALIZE;
  int deep_copy(ObMacroBlockSchemaInfo*& new_schema_info, common::ObIAllocator& allocator) const;
  int64_t get_deep_copy_size() const;
  int64_t to_string(char* buffer, const int64_t length) const;
  bool is_valid() const
  {
    return (0 == column_number_ || (nullptr != compressor_ && nullptr != column_id_array_ &&
                                       nullptr != column_type_array_ && nullptr != column_order_array_));
  }
  bool operator==(const ObMacroBlockSchemaInfo& other) const;
  int16_t column_number_;
  int16_t rowkey_column_number_;
  int64_t schema_version_;
  int16_t schema_rowkey_col_cnt_;
  char* compressor_;
  uint16_t* column_id_array_;
  common::ObObjMeta* column_type_array_;
  common::ObOrderType* column_order_array_;
};

struct ObMacroBlockMeta {
  // For compatibility, the variables in this struct MUST NOT be deleted or moved.
  // You should ONLY add variables at the end.
  // Note that if you use complex structure as variables, the complex structure should also keep compatibility.

  // The following variables need to be serialized
  int16_t attr_;
  union {
    uint64_t data_version_;
    int64_t previous_block_index_;  // nonsstable: previous_block_index_ link.
  };
  int16_t column_number_;              // column count of this table (size of column_checksum_)
  int16_t rowkey_column_number_;       // rowkey column count of this table
  int16_t column_index_scale_;         // store column index scale percent of column count;
  int16_t row_store_type_;             // {flat / sparse}
  int32_t row_count_;                  // row count of macro block;
  int32_t occupy_size_;                // data size of macro block;
  int64_t data_checksum_;              // data checksum of macro block
  int32_t micro_block_count_;          // micro block info in ObSSTableMacroBlockHeader
  int32_t micro_block_data_offset_;    // data offset base on macro block header.
  int32_t micro_block_index_offset_;   // data_size = index_offset - data_offset
  int32_t micro_block_endkey_offset_;  // index_size = endkey_offset - index_offset,
                                       // endkey_size = micro_block_mark_deletion_offset_ - endkey_offset
  char* compressor_;
  uint16_t* column_id_array_;
  common::ObObjMeta* column_type_array_;
  int64_t* column_checksum_;
  common::ObObj* endkey_;
  uint64_t table_id_;
  int64_t data_seq_;  // sequence in partition meta.
  int64_t schema_version_;
  int64_t snapshot_version_;

  int16_t schema_rowkey_col_cnt_;
  common::ObOrderType* column_order_array_;
  // only valid for multi-version minor sstable, row count delta relative to the base data
  // default 0
  int32_t row_count_delta_;

  int32_t micro_block_mark_deletion_offset_;  // micro_block_mark_deletion_size = delete_offset -
                                              // micro_block_mark_deletion_offset_
  bool macro_block_deletion_flag_;
  int32_t micro_block_delta_offset_;  // delta_size = occupy_size - micro_block_delta_offset;
  int64_t partition_id_;              // added since 2.0
  int16_t column_checksum_method_;    // 0 for unkown, 1 for ObObj::checksum(), 2 for ObObj::checksum_v2()
  int64_t progressive_merge_round_;
  // The following variables do not need to be serialized
  int32_t write_seq_;  // increment 1 every reuse pass.
  uint32_t bf_flag_;   // mark if rowkey_prefix bloomfilter in cache, bit=0: in, 0:not in
  int64_t create_timestamp_;
  int64_t retire_timestamp_;
  common::ObObj* collation_free_endkey_;
  int64_t encrypt_id_;
  int64_t master_key_id_;
  char encrypt_key_[share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH];
  ObMacroBlockMeta();
  bool operator==(const ObMacroBlockMeta& other) const;
  bool operator!=(const ObMacroBlockMeta& other) const;
  bool is_valid() const;
  int get_deep_copy_size(common::ObRowkey& collation_free_endkey, common::ObIAllocator& allocator, int64_t& size) const;
  int deep_copy(ObMacroBlockMeta*& meta_ptr, common::ObIAllocator& allocator) const;
  int check_collation_free_valid(bool& is_collation_free_valid) const;
  int extract_columns(common::ObIArray<share::schema::ObColDesc>& columns) const;
  int64_t to_string(char* buffer, const int64_t length) const;
  inline int32_t get_index_size() const
  {
    return micro_block_endkey_offset_ - micro_block_index_offset_;
  }
  inline int32_t get_block_index_size() const
  {
    return occupy_size_ - micro_block_index_offset_;
  }
  inline int32_t get_endkey_size() const
  {
    return 0 == micro_block_mark_deletion_offset_ ? occupy_size_ - micro_block_endkey_offset_
                                                  : micro_block_mark_deletion_offset_ - micro_block_endkey_offset_;
  }
  inline int32_t get_micro_block_mark_deletion_size() const
  {
    return 0 == micro_block_mark_deletion_offset_ ? 0 : micro_block_delta_offset_ - micro_block_mark_deletion_offset_;
  }
  inline int32_t get_micro_block_delta_size() const
  {
    return 0 == micro_block_delta_offset_ ? 0 : occupy_size_ - micro_block_delta_offset_;
  }
  NEED_SERIALIZE_AND_DESERIALIZE;
  OB_INLINE bool is_data_block() const
  {
    return is_sstable_data_block() || is_lob_data_block() || is_bloom_filter_data_block();
  }
  OB_INLINE bool is_normal_data_block() const
  {
    return is_sstable_data_block() || is_lob_data_block();
  }
  OB_INLINE bool is_sstable_data_block() const
  {
    return ObMacroBlockCommonHeader::SSTableData == attr_;
  }
  OB_INLINE bool is_lob_data_block() const
  {
    return ObMacroBlockCommonHeader::LobData == attr_ || ObMacroBlockCommonHeader::LobIndex == attr_;
  }
  OB_INLINE bool is_bloom_filter_data_block() const
  {
    return ObMacroBlockCommonHeader::BloomFilterData == attr_;
  }
  OB_INLINE bool is_sparse_format() const
  {
    return common::ObRowStoreType::SPARSE_ROW_STORE == row_store_type_;
  }
  OB_INLINE bool is_encrypted() const
  {
    return encrypt_id_ > static_cast<int64_t>(share::ObAesOpMode::ob_invalid_mode);
  }

private:
  int64_t get_meta_content_serialize_size() const;
};

enum ObStorageMetaStatus {
  NOT_INITED = 0,
  INITED,
  LOADED,
  WRITED,
};

struct ObSSTableColumnMeta {
  // For compatibility, the variables in this struct MUST NOT be deleted or moved.
  // You should ONLY add variables at the end.
  // Note that if you use complex structure as variables, the complex structure should also keep compatibility.
  static const int64_t SSTABLE_COLUMN_META_VERSION = 1;
  int64_t column_id_;
  int64_t column_default_checksum_;
  int64_t column_checksum_;
  ObSSTableColumnMeta();
  virtual ~ObSSTableColumnMeta();
  bool operator==(const ObSSTableColumnMeta& other) const;
  bool operator!=(const ObSSTableColumnMeta& other) const;
  void reset();
  bool is_valid() const
  {
    return column_id_ >= 0 && column_default_checksum_ >= 0 && column_checksum_ >= 0;
  }
  TO_STRING_KV(K_(column_id), K_(column_default_checksum), K_(column_checksum));
  OB_UNIS_VERSION_V(SSTABLE_COLUMN_META_VERSION);
};

struct ObStoreFileInfo {
  static const int64_t STORE_FILE_INFO_VERSION = 1;
  int64_t file_id_;
  int64_t file_size_;
  TO_STRING_KV(K_(file_id), K_(file_size));
  bool operator==(const ObStoreFileInfo& other) const;
  OB_UNIS_VERSION_V(STORE_FILE_INFO_VERSION);
};

struct ObStoreFileCtx final {
  static const int64_t STORE_FILE_CTX_VERSION = 1;
  static const int64_t DEFAULT_FILE_NUM = 16;
  static const int64_t MAX_VALID_BLOCK_COUNT_PER_FILE = INT32_MAX;

  ObStoreFileSystemType file_system_type_;
  ObStoreFileType file_type_;
  int64_t block_count_per_file_;  // only used for STORE_FILE_MACRO_BLOCK type
  common::ObFixedArray<ObStoreFileInfo, common::ObIAllocator> file_list_;

  TO_STRING_KV(K_(file_system_type), K_(file_type), K_(block_count_per_file), K_(file_list));
  explicit ObStoreFileCtx(common::ObIAllocator& allocator);
  virtual ~ObStoreFileCtx();
  int init(const ObStoreFileSystemType& file_system_type, const ObStoreFileType& file_type,
      const int64_t block_count_per_file);
  bool is_valid() const;
  void reset();
  bool need_file_id_list() const
  {
    return false;
  }
  int assign(const ObStoreFileCtx& src);
  bool equals(const ObStoreFileCtx& other) const;
  OB_UNIS_VERSION_V(STORE_FILE_CTX_VERSION);
  DISALLOW_COPY_AND_ASSIGN(ObStoreFileCtx);
};

struct ObSSTableMacroBlockId {
  blocksstable::MacroBlockId macro_block_id_;
  int64_t macro_block_id_in_files_;

  OB_INLINE bool is_valid() const;
  OB_INLINE void reset();
  TO_STRING_KV(K_(macro_block_id), K_(macro_block_id_in_files));
};

struct ObMacroBlockCtx final {
  ObMacroBlockCtx() : file_ctx_(NULL), sstable_block_id_(), sstable_(nullptr)
  {}
  const blocksstable::ObStoreFileCtx* file_ctx_;
  blocksstable::ObSSTableMacroBlockId sstable_block_id_;
  storage::ObSSTable* sstable_;

  OB_INLINE bool is_valid() const;
  OB_INLINE void reset();

  OB_INLINE const blocksstable::MacroBlockId& get_macro_block_id() const;
  TO_STRING_KV(K_(sstable_block_id), KP_(file_ctx));
};

enum ObColumnChecksumMethod { CCM_UNKOWN = 0, CCM_TYPE_AND_VALUE = 1, CCM_VALUE_ONLY = 2, CCM_IGNORE = 3, CCM_MAX };

// store basic info of sstable meta, for migrate partition.
struct ObSSTableBaseMeta {
  // For compatibility, the variables in this struct MUST NOT be deleted or moved.
  // You should ONLY add variables at the end.
  // Note that if you use complex structure as variables, the complex structure should also keep compatibility.
  static const int64_t SSTABLE_BASE_META_VERSION = 1;
  static const int64_t DEFAULT_COLUMN_IN_SSTABLE = 64;
  static const int64_t SSTABLE_FORMAT_VERSION_1 = 1;
  static const int64_t SSTABLE_FORMAT_VERSION_2 = 2;  // since 2.0
  static const int64_t SSTABLE_FORMAT_VERSION_3 = 3;  // since 2.1
  static const int64_t SSTABLE_FORMAT_VERSION_4 = 4;  // since 2.2
  static const int64_t SSTABLE_FORMAT_VERSION_5 = 5;  // since 2.2.3
  /*
   * Add SSTABLE_FORMAT_VERSION_6 for SqlSequence
   * Version<6 : (Rowkey | TransVersion) Cells
   * Version>=6 : (Rowkey | TransVersion | SqlSequence) Cells
   * */
  static const int64_t SSTABLE_FORMAT_VERSION_6 = 6;  // since 3.0

  int64_t index_id_;  // index_id_ == ObPartitionMeta::table_id_ means data table, or local index id.
  int64_t row_count_;
  int64_t occupy_size_;
  int64_t data_checksum_;
  int64_t row_checksum_;
  int64_t data_version_;
  int64_t rowkey_column_count_;
  int64_t table_type_;  // no usage
  int64_t index_type_;
  int64_t available_version_;  // no usage
  int64_t macro_block_count_;
  int64_t use_old_macro_block_count_;
  int64_t column_cnt_;
  common::ModulePageAllocator inner_alloc_;
  common::ObFixedArray<ObSSTableColumnMeta, common::ObIAllocator> column_metas_;
  int64_t macro_block_second_index_;
  int64_t total_sstable_count_;  // no usage
  // from 1470
  int64_t lob_macro_block_count_;
  int64_t lob_use_old_macro_block_count_;

  // added since 2.0
  int64_t sstable_format_version_;
  int64_t max_logic_block_index_;      // not used any more
  bool build_on_snapshot_;             // true if builds on snapshot version, false if builds on major version
  int64_t create_index_base_version_;  // if sstable is create index minor dump ssstable, the version is not null
  // for multi-version minor sstable
  int64_t schema_version_;
  int64_t progressive_merge_start_version_;
  int64_t progressive_merge_end_version_;
  int64_t create_snapshot_version_;
  int64_t checksum_method_;
  common::ObFixedArray<ObSSTableColumnMeta, common::ObIAllocator> new_column_metas_;
  int64_t progressive_merge_round_;
  int64_t progressive_merge_step_;
  share::schema::ObTableMode table_mode_;
  int64_t logical_data_version_;
  bool has_compact_row_;
  common::ObPGKey pg_key_;
  int8_t multi_version_rowkey_type_;
  bool contain_uncommitted_row_;
  int64_t upper_trans_version_;
  int64_t max_merged_trans_version_;
  uint64_t end_log_id_;

  ObSSTableBaseMeta();
  explicit ObSSTableBaseMeta(common::ObIAllocator& alloc);
  virtual ~ObSSTableBaseMeta()
  {}
  void set_allocator(common::ObIAllocator& alloc);
  bool operator==(const ObSSTableBaseMeta& other) const;
  bool operator!=(const ObSSTableBaseMeta& other) const;
  bool is_valid() const;
  virtual void reset();
  virtual int64_t get_total_macro_block_count() const
  {
    return macro_block_count_ + lob_macro_block_count_;
  }
  virtual int64_t get_data_macro_block_count() const
  {
    return macro_block_count_;
  }
  virtual int64_t get_lob_macro_block_count() const
  {
    return lob_macro_block_count_;
  }
  virtual int64_t get_total_use_old_macro_block_count() const
  {
    return use_old_macro_block_count_ + lob_use_old_macro_block_count_;
  }
  virtual int assign(const ObSSTableBaseMeta& meta);
  int set_upper_trans_version(const int64_t upper_trans_version);
  int64_t get_upper_trans_version() const
  {
    return ATOMIC_LOAD(&upper_trans_version_);
  }
  int64_t get_max_merged_trans_version() const
  {
    return ATOMIC_LOAD(&max_merged_trans_version_);
  }
  int check_data(const ObSSTableBaseMeta& other_meta);
  uint64_t get_end_log_id() const
  {
    return ATOMIC_LOAD(&end_log_id_);
  }
  VIRTUAL_TO_STRING_KV(K_(index_id), K_(row_count), K_(occupy_size), K_(data_checksum),
      //      K_(row_checksum),
      K_(data_version), K_(rowkey_column_count), K_(table_type), K_(index_type),
      //      K_(available_version),
      K_(macro_block_count), K_(use_old_macro_block_count), K_(column_cnt), K_(column_metas),
      //      K_(macro_block_second_index),
      //      K_(total_sstable_count),
      K_(lob_macro_block_count), K_(lob_use_old_macro_block_count),
      //      K_(max_logic_block_index),
      K_(build_on_snapshot), K_(create_index_base_version), K_(schema_version), K_(progressive_merge_start_version),
      K_(progressive_merge_end_version), K_(create_snapshot_version), K_(checksum_method), K_(progressive_merge_round),
      K_(progressive_merge_step), K_(table_mode), K_(pg_key),
      //      K_(new_column_metas),
      K_(logical_data_version), K_(multi_version_rowkey_type), K_(contain_uncommitted_row), K_(upper_trans_version),
      K_(max_merged_trans_version), K_(end_log_id), K_(sstable_format_version), K_(has_compact_row));
  OB_UNIS_VERSION_V(SSTABLE_BASE_META_VERSION);
  DISALLOW_COPY_AND_ASSIGN(ObSSTableBaseMeta);
};

struct ObSSTableMeta : public ObSSTableBaseMeta {
  // For compatibility, the variables in this struct MUST NOT be deleted or moved.
  // You should ONLY add variables at the end.
  // Note that if you use complex structure as variables, the complex structure should also keep compatibility.
  static const int64_t SSTABLE_META_VERSION = 1;
  static const int64_t DEFAULT_MACRO_BLOCK_NUM = 4;
  static const int64_t DEFAULT_MACRO_BLOCK_ARRAY_PAGE_SIZE =
      DEFAULT_MACRO_BLOCK_NUM * sizeof(blocksstable::MacroBlockId);
  static const int64_t DEFAULT_MACRO_BLOCK_ARRAY_IDX_PAGE_SIZE = DEFAULT_MACRO_BLOCK_NUM * sizeof(int64_t);
  typedef common::ObSEArray<MacroBlockId, DEFAULT_MACRO_BLOCK_NUM> MacroBlockArray;
  typedef common::ObSEArray<int64_t, DEFAULT_MACRO_BLOCK_NUM> MacroBlockIdxArray;
  MacroBlockArray macro_block_array_;
  // from 1470
  MacroBlockArray lob_macro_block_array_;

  MacroBlockIdxArray macro_block_idx_array_;
  MacroBlockIdxArray lob_macro_block_idx_array_;
  ObStoreFileCtx file_ctx_;
  ObStoreFileCtx lob_file_ctx_;
  // from 2.2
  MacroBlockId bloom_filter_block_id_;
  int64_t bloom_filter_block_id_in_files_;
  ObStoreFileCtx bloom_filter_file_ctx_;

  explicit ObSSTableMeta(common::ObIAllocator& alloc);
  virtual ~ObSSTableMeta()
  {}
  bool operator==(const ObSSTableMeta& other) const;
  bool operator!=(const ObSSTableMeta& other) const;
  bool is_valid() const;
  bool with_heap_table_flag() const;
  virtual void reset();
  INHERIT_TO_STRING_KV("ObSSTableBaseMeta", ObSSTableBaseMeta, K_(file_ctx), K_(lob_file_ctx),
      K_(macro_block_idx_array), K_(lob_macro_block_idx_array), K_(bloom_filter_block_id),
      K_(bloom_filter_block_id_in_files), K_(bloom_filter_file_ctx));
  OB_UNIS_VERSION_V(SSTABLE_META_VERSION);
};

struct ObPartitionMeta {
  // For compatibility, the variables in this struct MUST NOT be deleted or moved.
  // You should ONLY add variables at the end.
  // Note that if you use complex structure as variables, the complex structure should also keep compatibility.
  static const int64_t PARTITION_META_VERSION = 1;
  int64_t table_id_;
  int64_t partition_id_;
  int64_t partition_cnt_;
  int64_t data_version_;
  int16_t table_type_;         // sys, normal, index,
  int16_t migrate_status_;     // deprecated
  int16_t replica_status_;     // deprecated
  int64_t migrate_timestamp_;  // deprecated
  int64_t step_merge_start_version_;
  int64_t step_merge_end_version_;
  int64_t index_table_count_;  // local index table count, includes data table itself.
  common::ObString log_info_;
  storage::ObStoreType store_type_;
  int16_t is_restore_;
  int64_t partition_checksum_;
  int64_t create_timestamp_;

  ObPartitionMeta();
  virtual ~ObPartitionMeta();
  ObPartitionMeta& operator=(const ObPartitionMeta& meta);
  bool operator==(const ObPartitionMeta& meta) const;
  bool operator!=(const ObPartitionMeta& meta) const;
  bool is_valid() const;
  void reset();
  uint64_t get_tenant_id() const
  {
    return common::extract_tenant_id(table_id_);
  }
  TO_STRING_KV(K_(table_id), K_(partition_id), K_(partition_cnt), K_(data_version), K_(table_type), K_(migrate_status),
      K_(replica_status), K_(migrate_timestamp), K_(step_merge_start_version), K_(step_merge_end_version),
      K_(index_table_count), K(log_info_.length()), K_(store_type), K_(is_restore), K_(partition_checksum));
  // use allocator allocate memory for log_info_;
  int deserialize(const char* buf, const int64_t buf_len, int64_t& pos, common::ObIAllocator& allocator);
  OB_UNIS_VERSION_V(PARTITION_META_VERSION);
};

class ObRowHeader {
public:
  ObRowHeader()
  {
    memset(this, 0, sizeof(*this));
  }
  bool is_valid() const
  {
    return row_flag_ >= 0 && column_index_bytes_ >= 0;
  }
  int8_t get_row_flag() const
  {
    return row_flag_;
  }
  int8_t get_row_dml() const
  {
    return row_dml_;
  }
  int8_t get_version() const
  {
    return version_;
  }
  int8_t get_column_index_bytes() const
  {
    return column_index_bytes_;
  }
  int8_t get_row_type_flag() const
  {
    return row_type_flag_;
  }
  storage::ObMultiVersionRowFlag get_row_multi_version_flag() const
  {
    return storage::ObMultiVersionRowFlag(row_type_flag_);
  }
  int16_t get_column_count() const
  {
    return column_count_;
  }

  void set_row_flag(const int8_t row_flag)
  {
    row_flag_ = row_flag;
  }
  void set_column_index_bytes(const int8_t column_index_bytes)
  {
    column_index_bytes_ = column_index_bytes;
  }
  void set_row_dml(const int8_t row_dml)
  {
    row_dml_ = row_dml;
  }
  void set_version(const int8_t version)
  {
    version_ = version;
  }
  void set_reserved8(const int8_t reserved8)
  {
    reserved8_ = reserved8;
  }
  void set_column_count(const int16_t column_count)
  {
    column_count_ = column_count;
  }
  void set_row_type_flag(const int8_t row_type_flag)
  {
    row_type_flag_ = row_type_flag;
  }
  static int get_serialized_size()
  {
    return sizeof(ObRowHeader);
  }
  ObRowHeader& operator=(const ObRowHeader& src)
  {
    row_flag_ = src.row_flag_;
    column_index_bytes_ = src.column_index_bytes_;
    row_dml_ = src.row_dml_;
    version_ = src.version_;
    reserved8_ = src.reserved8_;
    column_count_ = src.column_count_;
    row_type_flag_ = src.row_type_flag_;
    return *this;
  }

  enum ObRowHeaderVersion {
    RHV_NO_TRANS_ID = 0,
    RHV_WITH_TRANS_ID = 1,
  };

  TO_STRING_KV(K_(row_flag), K_(column_index_bytes), K_(row_dml), K_(version), K_(row_type_flag), K_(reserved8),
      K_(column_count));

private:
  int8_t row_flag_;
  int8_t column_index_bytes_;
  int8_t row_dml_;
  int8_t version_;
  int8_t row_type_flag_;
  int8_t reserved8_;
  int16_t column_count_;
};

struct ObSSTableTriple {
  blocksstable::MacroBlockId block_id_;
  int64_t data_version_;
  int64_t data_seq_;
  ObSSTableTriple() : block_id_(), data_version_(0), data_seq_(0)
  {}
  ObSSTableTriple(const blocksstable::MacroBlockId id, const int64_t dv, const int64_t seq)
      : block_id_(id), data_version_(dv), data_seq_(seq)
  {}
  TO_STRING_KV(K_(block_id), K_(data_version), K_(data_seq));
};

struct ObSSTablePair {
  int64_t data_version_;
  int64_t data_seq_;
  ObSSTablePair() : data_version_(0), data_seq_(0)
  {}
  ObSSTablePair(const int64_t dv, const int64_t seq) : data_version_(dv), data_seq_(seq)
  {}

  bool operator==(const ObSSTablePair& other) const
  {
    return data_version_ == other.data_version_ && data_seq_ == other.data_seq_;
  }

  bool operator!=(const ObSSTablePair& other) const
  {
    return !(*this == other);
  }
  uint64_t hash() const
  {
    return common::combine_id(data_version_, data_seq_);
  }
  TO_STRING_KV(K_(data_version), K_(data_seq));
  OB_UNIS_VERSION(1);
};

struct ObLogicBlockIndex {
public:
  static const int64_t INVALID_LOGIC_BLOCK_INDEX = -1;

  ObLogicBlockIndex() : logic_block_index_(INVALID_LOGIC_BLOCK_INDEX)
  {}
  explicit ObLogicBlockIndex(const int64_t idx) : logic_block_index_(idx)
  {}
  ~ObLogicBlockIndex()
  {}
  ObLogicBlockIndex(const ObLogicBlockIndex& logic_idx) : logic_block_index_(logic_idx.logic_block_index_)
  {}

  bool operator==(const ObLogicBlockIndex& other) const
  {
    return logic_block_index_ == other.logic_block_index_;
  }
  bool operator!=(const ObLogicBlockIndex& other) const
  {
    return logic_block_index_ != other.logic_block_index_;
  }
  ObLogicBlockIndex& operator=(const ObLogicBlockIndex& other)
  {
    if (&other != this) {
      logic_block_index_ = other.logic_block_index_;
    }
    return *this;
  }

  bool is_valid() const
  {
    return logic_block_index_ >= INVALID_LOGIC_BLOCK_INDEX;
  }
  TO_STRING_KV(K_(logic_block_index));
  OB_UNIS_VERSION(1);

public:
  int64_t logic_block_index_;
};

struct ObMacroBlockItem {
public:
  ObMacroBlockItem() : macro_block_id_(), logic_block_index_()
  {}
  ~ObMacroBlockItem()
  {}
  ObMacroBlockItem(const MacroBlockId& id) : macro_block_id_(id), logic_block_index_()
  {}
  ObMacroBlockItem(const MacroBlockId& id, const int64_t idx) : macro_block_id_(id), logic_block_index_(idx)
  {}
  ObMacroBlockItem(const int64_t id, const int64_t idx) : macro_block_id_(id), logic_block_index_(idx)
  {}
  ObMacroBlockItem(const ObMacroBlockItem& item)
      : macro_block_id_(item.macro_block_id_), logic_block_index_(item.logic_block_index_)
  {}

  bool operator==(const ObMacroBlockItem& other) const
  {
    return macro_block_id_ == other.macro_block_id_ && logic_block_index_ == other.logic_block_index_;
  }

  bool operator!=(const ObMacroBlockItem& other) const
  {
    return !(*this == other);
  }

  ObMacroBlockItem& operator=(const ObMacroBlockItem& other)
  {
    if (&other != this) {
      macro_block_id_ = other.macro_block_id_;
      logic_block_index_ = other.logic_block_index_;
    }
    return *this;
  }
  TO_STRING_KV(K_(macro_block_id), K_(logic_block_index));

  // for compatibility, MacroBlockItem is regarded as valid when logic_block_index == INVALID_LOGIC_BLOCK_INDEX
  bool is_valid() const
  {
    return macro_block_id_.is_valid() && logic_block_index_.is_valid();
  }

public:
  MacroBlockId macro_block_id_;
  ObLogicBlockIndex logic_block_index_;
};

template <class K, class V>
struct ObGetBlockItemKey {
  void operator()(const K& k, const V& v)
  {
    UNUSED(k);
    UNUSED(v);
  }
};

template <>
struct ObGetBlockItemKey<MacroBlockId, ObMacroBlockItem*> {
  MacroBlockId operator()(const ObMacroBlockItem* macro_block_item) const
  {
    MacroBlockId block_id;
    if (OB_ISNULL(macro_block_item)) {
      STORAGE_LOG(ERROR, "invalid args", K(macro_block_item));
    } else {
      block_id = macro_block_item->macro_block_id_;
    }
    return block_id;
  }
};

template <>
struct ObGetBlockItemKey<ObLogicBlockIndex, ObMacroBlockItem*> {
  ObLogicBlockIndex operator()(const ObMacroBlockItem* macro_block_item) const
  {
    ObLogicBlockIndex logic_idx;
    if (OB_ISNULL(macro_block_item)) {
      STORAGE_LOG(ERROR, "invalid args", K(macro_block_item));
    } else {
      logic_idx = macro_block_item->logic_block_index_;
    }
    return logic_idx;
  }
};

typedef common::hash::ObPointerHashArray<MacroBlockId, ObMacroBlockItem*, ObGetBlockItemKey> BlockIdHashArray;
typedef common::hash::ObPointerHashArray<ObLogicBlockIndex, ObMacroBlockItem*, ObGetBlockItemKey> LogicIndexHashArray;

//=========================oceanbase::blocksstable==================================
int write_compact_rowkey(ObBufferWriter& writer, const common::ObObj* endkey, const int64_t count,
    const common::ObRowStoreType row_store_type);
int read_compact_rowkey(ObBufferReader& reader, const common::ObObjMeta* column_type_array, common::ObObj* endkey,
    const int64_t count, const common::ObRowStoreType row_store_type);

struct ObSimpleMacroBlockMetaInfo {
  int64_t block_index_;
  int16_t attr_;
  uint64_t table_id_;
  uint64_t data_version_;
  int64_t data_seq_;
  int32_t write_seq_;
  int64_t create_timestamp_;

  ObSimpleMacroBlockMetaInfo();
  void reuse();
  void reset();
  TO_STRING_KV(
      K_(block_index), K_(attr), K_(table_id), K_(data_version), K_(data_seq), K_(write_seq), K_(create_timestamp));
};

struct ObMacroBlockMarkerStatus {
  static const int64_t HOLD_ALERT_TIME = 12 * 1024 * 1024 * 3600LL;  // 12h
  int64_t total_block_count_;
  int64_t reserved_block_count_;
  int64_t macro_meta_block_count_;
  int64_t partition_meta_block_count_;
  int64_t data_block_count_;
  int64_t lob_data_block_count_;
  int64_t second_index_count_;
  int64_t lob_second_index_count_;
  int64_t bloomfilter_count_;
  int64_t hold_count_;
  int64_t pending_free_count_;
  int64_t free_count_;
  int64_t mark_cost_time_;
  int64_t sweep_cost_time_;
  int64_t hold_alert_time_;
  ObSimpleMacroBlockMetaInfo hold_info_;

  ObMacroBlockMarkerStatus();

  void reuse();
  void reset();
  inline void record_hold_meta(const int64_t now, const int64_t block_index, const ObMacroBlockMeta& meta);
  void fill_comment(char* buf, const int32_t buf_len) const;

  TO_STRING_KV(K_(total_block_count), K_(reserved_block_count), K_(macro_meta_block_count),
      K_(partition_meta_block_count), K_(data_block_count), K_(lob_data_block_count), K_(second_index_count),
      K_(lob_second_index_count), K_(bloomfilter_count), K_(hold_count), K_(pending_free_count), K_(free_count),
      K_(mark_cost_time), K_(sweep_cost_time), K_(hold_alert_time), K_(hold_info));
};

class ObMacroBlockMarkerBitMap {
public:
  ObMacroBlockMarkerBitMap();
  virtual ~ObMacroBlockMarkerBitMap();
  int init(const int64_t macro_block_count);
  void reuse();
  void reset();
  int set_block(const int64_t block_index);
  int test_block(const int64_t block_index, bool& bool_ret) const;

private:
  typedef uint64_t size_type;
  static const size_type BYTES_PER_BLOCK = sizeof(size_type);
  static const size_type BITS_PER_BLOCK = BYTES_PER_BLOCK * 8;

  static size_type mem_index(size_type pos)
  {
    return pos / BITS_PER_BLOCK;
  }
  static size_type bit_index(size_type pos)
  {
    return (pos % BITS_PER_BLOCK);
  }
  static size_type bit_mask(size_type pos)
  {
    return (1ULL << bit_index(pos));
  }

private:
  bool is_inited_;
  char* block_bitmap_;
  int64_t byte_size_;
  int64_t total_macro_block_count_;
};

class ObMacroBlockMarkerHelper {
public:
  ObMacroBlockMarkerHelper();
  virtual ~ObMacroBlockMarkerHelper();
  int init(const int64_t macro_block_count);
  void reuse();
  void reset();
  ObMacroBlockMarkerStatus& get_status()
  {
    return status_;
  }
  int set_block(const ObMacroBlockCommonHeader::MacroBlockType& type, const int64_t block_index);
  int set_block(const ObMacroBlockCommonHeader::MacroBlockType& type, const common::ObIArray<int64_t>& block_array);
  int set_block(
      const ObMacroBlockCommonHeader::MacroBlockType& type, const common::ObIArray<MacroBlockId>& block_array);
  int test_block(const int64_t block_index, bool& bool_ret) const;

private:
  bool is_inited_;
  ObMacroBlockMarkerStatus status_;
  ObMacroBlockMarkerBitMap bit_map_;
};

/****************************** following codes are inline functions ****************************/
inline void ObMacroBlockMarkerStatus::record_hold_meta(
    const int64_t now, const int64_t block_index, const ObMacroBlockMeta& meta)
{
  if (now > meta.create_timestamp_ + hold_alert_time_) {
    if (0 == hold_info_.create_timestamp_ || meta.create_timestamp_ < hold_info_.create_timestamp_) {
      hold_info_.block_index_ = block_index;
      hold_info_.attr_ = meta.attr_;
      hold_info_.table_id_ = meta.table_id_;
      hold_info_.data_version_ = meta.data_version_;
      hold_info_.data_seq_ = meta.data_seq_;
      hold_info_.write_seq_ = meta.write_seq_;
      hold_info_.create_timestamp_ = meta.create_timestamp_;
    }
  }
}

OB_INLINE bool ObSSTableMacroBlockId::is_valid() const
{
  return macro_block_id_.is_valid();  // TODO(): check macro_block_id_in_files_ later
}

OB_INLINE void ObSSTableMacroBlockId::reset()
{
  macro_block_id_.reset();
  macro_block_id_in_files_ = -1;
}

OB_INLINE bool ObMacroBlockCtx::is_valid() const
{
  return sstable_block_id_.is_valid() && nullptr != sstable_;  // file_ctx_ not checked
}

OB_INLINE void ObMacroBlockCtx::reset()
{
  file_ctx_ = NULL;
  sstable_block_id_.reset();
  sstable_ = nullptr;
}

OB_INLINE const blocksstable::MacroBlockId& ObMacroBlockCtx::get_macro_block_id() const
{
  return sstable_block_id_.macro_block_id_;
}

enum ObRecordHeaderVersion { RECORD_HEADER_VERSION_V2 = 2, RECORD_HEADER_VERSION_V3 = 3 };

struct ObRecordHeaderV3 {
public:
  ObRecordHeaderV3();
  ~ObRecordHeaderV3() = default;
  static int deserialize_and_check_record(
      const char* ptr, const int64_t size, const int16_t magic, const char*& payload_ptr, int64_t& payload_size);
  static int deserialize_and_check_record(const char* ptr, const int64_t size, const int16_t magic);
  int check_and_get_record(
      const char* ptr, const int64_t size, const int16_t magic, const char*& payload_ptr, int64_t& payload_size) const;
  int check_record(const char* ptr, const int64_t size, const int16_t magic) const;
  static int64_t get_serialize_size(const int64_t header_version, const int64_t column_cnt)
  {
    return RECORD_HEADER_VERSION_V2 == header_version
               ? sizeof(ObRecordCommonHeader)
               : sizeof(ObRecordCommonHeader) + sizeof(data_encoding_length_) + sizeof(row_count_) +
                     sizeof(column_cnt_) + column_cnt * sizeof(column_checksums_[0]);
  }
  void set_header_checksum();
  int check_header_checksum() const;
  inline bool is_compressed_data() const
  {
    return data_length_ != data_zlength_;
  }
  NEED_SERIALIZE_AND_DESERIALIZE;

private:
  int check_payload_checksum(const char* buf, const int64_t len) const;

public:
  struct ObRecordCommonHeader {
  public:
    ObRecordCommonHeader() = default;
    ~ObRecordCommonHeader() = default;
    inline bool is_compressed() const
    {
      return data_length_ != data_zlength_;
    }
    int16_t magic_;
    int8_t header_length_;
    int8_t version_;
    int16_t header_checksum_;
    int16_t reserved16_;
    int64_t data_length_;
    int64_t data_zlength_;
    int64_t data_checksum_;
  };
  int16_t magic_;
  int8_t header_length_;
  int8_t version_;
  int16_t header_checksum_;
  int16_t reserved16_;
  int64_t data_length_;
  int64_t data_zlength_;
  int64_t data_checksum_;
  // add since 2.2
  int32_t data_encoding_length_;
  int32_t row_count_;
  uint16_t column_cnt_;
  int64_t* column_checksums_;

  TO_STRING_KV(K_(magic), K_(header_length), K_(version), K_(header_checksum), K_(reserved16), K_(data_length),
      K_(data_zlength), K_(data_checksum), K_(data_encoding_length), K_(row_count), K_(column_cnt),
      KP(column_checksums_));
};

// ObStorageFile's superblock should inherit from this basic one
class ObISuperBlock {
public:
  ObISuperBlock() = default;
  virtual ~ObISuperBlock() = default;
  virtual void reset() = 0;

  PURE_VIRTUAL_NEED_SERIALIZE_AND_DESERIALIZE;
  DECLARE_PURE_VIRTUAL_TO_STRING;
  virtual int64_t get_timestamp() const = 0;
  virtual bool is_valid() const = 0;
};

struct ObSuperBlockMetaEntry {
public:
  static const int64_t META_ENTRY_VERSION = 1;
  ObSuperBlockMetaEntry() : macro_block_id_(-1)
  {}
  bool is_valid() const
  {
    return macro_block_id_.is_valid();
  }
  void reset()
  {
    macro_block_id_.reset();
    macro_block_id_.set_second_id(-1);
  }
  TO_STRING_KV(K_(macro_block_id));
  OB_UNIS_VERSION(META_ENTRY_VERSION);

public:
  blocksstable::MacroBlockId macro_block_id_;  // first entry meta macro block id
};

struct ObServerSuperBlock : public blocksstable::ObISuperBlock {
  struct ServerSuperBlockContent {
    static const int64_t SUPER_BLOCK_CONTENT_VERSION = 1;

    int64_t create_timestamp_;  // create timestamp
    int64_t modify_timestamp_;  // last modified timestamp
    int64_t macro_block_size_;
    int64_t total_macro_block_count_;
    int64_t total_file_size_;

    common::ObLogCursor replay_start_point_;
    ObSuperBlockMetaEntry super_block_meta_;
    ObSuperBlockMetaEntry tenant_config_meta_;

    ServerSuperBlockContent();
    bool is_valid() const;
    void reset();
    TO_STRING_KV("Type", "ObServerSuperBlock", K_(create_timestamp), K_(modify_timestamp), K_(macro_block_size),
        K_(total_macro_block_count), K_(total_file_size), K_(replay_start_point), K_(super_block_meta),
        K_(tenant_config_meta));
    OB_UNIS_VERSION(SUPER_BLOCK_CONTENT_VERSION);
  };

  ObServerSuperBlock();

  virtual int64_t get_timestamp() const override
  {
    return content_.modify_timestamp_;
  }
  virtual bool is_valid() const override;
  virtual void reset() override;
  VIRTUAL_NEED_SERIALIZE_AND_DESERIALIZE;
  VIRTUAL_TO_STRING_KV(K_(header), K_(content));

  OB_INLINE int64_t get_macro_block_size() const
  {
    return content_.macro_block_size_;
  }
  OB_INLINE int64_t get_total_macro_block_count() const
  {
    return content_.total_macro_block_count_;
  }
  OB_INLINE int64_t get_super_block_size() const
  {
    return header_.get_serialize_size() + content_.get_serialize_size();
  }
  int format_startup_super_block(const int64_t macro_block_size, const int64_t data_file_size);

  ObSuperBlockHeaderV2 header_;
  ServerSuperBlockContent content_;
};

struct ObMacroBlockMetaV2 final {
public:
  static const int64_t MACRO_BLOCK_META_VERSION_V1 = 1;
  static const int64_t MACRO_BLOCK_META_VERSION_V2 = 2;  // upgrade in version 3.1, new meta format
  ObMacroBlockMetaV2();
  ~ObMacroBlockMetaV2() = default;
  bool operator==(const ObMacroBlockMetaV2& other) const;
  bool operator!=(const ObMacroBlockMetaV2& other) const;
  bool is_valid() const;
  int get_deep_copy_size(common::ObRowkey& collation_free_endkey, common::ObIAllocator& allocator, int64_t& size) const;
  int deep_copy(ObMacroBlockMetaV2*& meta_ptr, common::ObIAllocator& allocator) const;
  int check_collation_free_valid(bool& is_collation_free_valid) const;
  int64_t to_string(char* buffer, const int64_t length) const;
  inline int32_t get_index_size() const
  {
    return micro_block_endkey_offset_ - micro_block_index_offset_;
  }
  inline int32_t get_block_index_size() const
  {
    return occupy_size_ - micro_block_index_offset_;
  }
  inline int32_t get_endkey_size() const
  {
    return 0 == micro_block_mark_deletion_offset_ ? occupy_size_ - micro_block_endkey_offset_
                                                  : micro_block_mark_deletion_offset_ - micro_block_endkey_offset_;
  }
  inline int32_t get_micro_block_mark_deletion_size() const
  {
    return 0 == micro_block_mark_deletion_offset_ ? 0 : micro_block_delta_offset_ - micro_block_mark_deletion_offset_;
  }
  inline int32_t get_micro_block_delta_size() const
  {
    return 0 == micro_block_delta_offset_ ? 0 : occupy_size_ - micro_block_delta_offset_;
  }
  NEED_SERIALIZE_AND_DESERIALIZE;
  OB_INLINE bool is_data_block() const
  {
    return is_sstable_data_block() || is_lob_data_block() || is_bloom_filter_data_block();
  }
  OB_INLINE bool is_normal_data_block() const
  {
    return is_sstable_data_block() || is_lob_data_block();
  }
  OB_INLINE bool is_sstable_data_block() const
  {
    return ObMacroBlockCommonHeader::SSTableData == attr_;
  }
  OB_INLINE bool is_lob_data_block() const
  {
    return ObMacroBlockCommonHeader::LobData == attr_ || ObMacroBlockCommonHeader::LobIndex == attr_;
  }
  OB_INLINE bool is_bloom_filter_data_block() const
  {
    return ObMacroBlockCommonHeader::BloomFilterData == attr_;
  }
  OB_INLINE bool is_sparse_format() const
  {
    return common::ObRowStoreType::SPARSE_ROW_STORE == row_store_type_;
  }
  OB_INLINE bool is_encrypted() const
  {
    return encrypt_id_ > static_cast<int64_t>(share::ObAesOpMode::ob_invalid_mode);
  }

private:
  int64_t get_meta_content_serialize_size() const;

public:
  // For compatibility, the variables in this struct MUST NOT be deleted or moved.
  // You should ONLY add variables at the end.
  // Note that if you use complex structure as variables, the complex structure should also keep compatibility.

  // The following variables need to be serialized
  int16_t attr_;
  union {
    uint64_t data_version_;
    int64_t previous_block_index_;  // nonsstable: previous_block_index_ link.
  };
  int16_t column_number_;              // column count of this table (size of column_checksum_)
  int16_t rowkey_column_number_;       // rowkey column count of this table
  int16_t column_index_scale_;         // store column index scale percent of column count;
  int16_t row_store_type_;             // reserve
  int32_t row_count_;                  // row count of macro block;
  int32_t occupy_size_;                // data size of macro block;
  int64_t data_checksum_;              // data checksum of macro block
  int32_t micro_block_count_;          // micro block info in ObSSTableMacroBlockHeader
  int32_t micro_block_data_offset_;    // data offset base on macro block header.
  int32_t micro_block_index_offset_;   // data_size = index_offset - data_offset
  int32_t micro_block_endkey_offset_;  // index_size = endkey_offset - index_offset,
                                       // endkey_size = micro_block_mark_deletion_offset_ - endkey_offset
  int64_t* column_checksum_;
  common::ObObj* endkey_;
  uint64_t table_id_;
  int64_t data_seq_;  // sequence in partition meta.
  int64_t schema_version_;
  int64_t snapshot_version_;

  int16_t schema_rowkey_col_cnt_;
  // only valid for multi-version minor sstable, row count delta relative to the base data
  // default 0
  int32_t row_count_delta_;

  int32_t micro_block_mark_deletion_offset_;  // micro_block_mark_deletion_size = delete_offset -
                                              // micro_block_mark_deletion_offset_
  bool macro_block_deletion_flag_;
  int32_t micro_block_delta_offset_;  // delta_size = occupy_size - micro_block_delta_offset;
  int64_t partition_id_;              // added since 2.0
  int16_t column_checksum_method_;    // 0 for unkown, 1 for ObObj::checksum(), 2 for ObObj::checksum_v2()
  int64_t progressive_merge_round_;
  // The following variables do not need to be serialized
  int32_t write_seq_;  // increment 1 every reuse pass.
  uint32_t bf_flag_;   // mark if rowkey_prefix bloomfilter in cache, bit=0: in, 0:not in
  int64_t create_timestamp_;
  int64_t retire_timestamp_;
  common::ObObj* collation_free_endkey_;
  int64_t encrypt_id_;
  int64_t master_key_id_;
  char encrypt_key_[share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH];
  bool contain_uncommitted_row_;
  int64_t max_merged_trans_version_;
};

struct ObFullMacroBlockMeta final {
public:
  ObFullMacroBlockMeta();
  ObFullMacroBlockMeta(const ObMacroBlockSchemaInfo* schema, const ObMacroBlockMetaV2* meta);
  ~ObFullMacroBlockMeta() = default;
  static int convert_macro_meta(const ObMacroBlockMeta& src_meta, ObMacroBlockMetaV2& dest_meta,
      ObMacroBlockSchemaInfo& dest_schema, common::ObIAllocator* allocator = nullptr);
  int assign(const ObFullMacroBlockMeta& other);
  int deep_copy(ObFullMacroBlockMeta& dst, common::ObIAllocator& allocator);
  int convert_from_old_macro_meta(const ObMacroBlockMeta& meta, common::ObIAllocator& allocator);
  bool is_valid() const
  {
    return nullptr != schema_ && nullptr != meta_;
  }
  void reset();
  TO_STRING_KV(K_(schema), K_(meta));
  const ObMacroBlockSchemaInfo* schema_;
  const ObMacroBlockMetaV2* meta_;
};

struct ObMacroBlockInfoPair final {
public:
  ObMacroBlockInfoPair() : block_id_(), meta_()
  {}
  ObMacroBlockInfoPair(const blocksstable::MacroBlockId& block_id, const ObFullMacroBlockMeta& meta)
      : block_id_(block_id), meta_(meta)
  {}
  ~ObMacroBlockInfoPair() = default;
  bool is_valid() const
  {
    return block_id_.is_valid() && meta_.is_valid();
  }
  void reset()
  {
    block_id_.reset();
    meta_.reset();
  }
  TO_STRING_KV(K_(block_id), K_(meta));
  blocksstable::MacroBlockId block_id_;
  ObFullMacroBlockMeta meta_;
};

struct ObFullMacroBlockMetaEntry final {
public:
  ObFullMacroBlockMetaEntry(ObMacroBlockMetaV2& meta_, ObMacroBlockSchemaInfo& schema);
  ~ObFullMacroBlockMetaEntry() = default;
  NEED_SERIALIZE_AND_DESERIALIZE;
  ObMacroBlockMetaV2& meta_;
  ObMacroBlockSchemaInfo& schema_;
};

class ObIMacroIdIterator {
public:
  ObIMacroIdIterator()
  {}
  virtual ~ObIMacroIdIterator()
  {}
  virtual int get_next_macro_id(MacroBlockId& block_id) = 0;
};

}  // end namespace blocksstable
}  // end namespace oceanbase
#endif
