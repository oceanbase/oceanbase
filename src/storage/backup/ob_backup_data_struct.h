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

#ifndef STORAGE_LOG_STREAM_BACKUP_DATA_STRUCT_H_
#define STORAGE_LOG_STREAM_BACKUP_DATA_STRUCT_H_

#include "storage/tablet/ob_tablet_meta.h"
#include "common/ob_tablet_id.h"
#include "common/object/ob_object.h"
#include "share/ob_ls_id.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/blocksstable/ob_sstable_meta.h"
#include "storage/tablet/ob_tablet_meta.h"
#include "share/backup/ob_backup_struct.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "share/ob_rs_mgr.h"
#include "storage/blocksstable/ob_logic_macro_id.h"

namespace oceanbase {

namespace share {
class ObLocationService;
}
namespace backup {

static const int64_t OB_DEFAULT_BACKUP_CONCURRENCY = 2;
static const int64_t OB_MAX_BACKUP_CONCURRENCY = 8;
static const int64_t OB_MAX_BACKUP_MEM_BUF_LEN = 8 * 1024 * 1024;  // 8MB
static const int64_t OB_MAX_BACKUP_INDEX_BUF_SIZE = 16 * 1024;     // 16KB;
static const int64_t OB_MAX_BACKUP_FILE_SIZE = 4 * 1024 * 1024;
static const int64_t OB_BACKUP_INDEX_BLOCK_SIZE = 16 * 1024;
static const int64_t OB_BACKUP_INDEX_BLOCK_NODE_CAPACITY = 256;
static const int64_t OB_BACKUP_MULTI_LEVEL_INDEX_BASE_LEVEL = 0;
static const int64_t OB_BACKUP_READ_BLOCK_SIZE = 2 << 20;  // 2 MB
static const int64_t OB_DEFAULT_BACKUP_BATCH_COUNT = 1024;
static const int64_t OB_INITIAL_BACKUP_MAX_FILE_ID = -1;


enum ObLSBackupStage {
  LOG_STREAM_BACKUP_SYS = 0,
  LOG_STREAM_BACKUP_MINOR = 1,
  LOG_STREAM_BACKUP_MAJOR = 2,
  LOG_STREAM_BACKUP_INDEX_REBUILD = 3,
  LOG_STREAM_BACKUP_COMPLEMENT_LOG = 4,
  LOG_STREAM_BACKUP_MAX,
};

enum ObBackupIndexLevel {
  BACKUP_INDEX_LEVEL_LOG_STREAM = 0,
  BACKUP_INDEX_LEVEL_TENANT = 1,
  MAX_BACKUP_INDEX_LEVEL,
};

struct ObBackupJobDesc {
  ObBackupJobDesc();
  bool is_valid() const;
  bool operator==(const ObBackupJobDesc &other) const;
  TO_STRING_KV(K_(job_id), K_(task_id), K_(trace_id));
  int64_t job_id_;
  int64_t task_id_;
  share::ObTaskId trace_id_;
};

enum ObBackupBlockType {
  BACKUP_BLOCK_MACRO_DATA = 0,
  BACKUP_BLOCK_META_DATA = 1,
  BACKUP_BLOCK_MACRO_INDEX = 2,
  BACKUP_BLOCK_META_INDEX = 3,
  BACKUP_BLOCK_MARCO_RANGE_INDEX = 4,
  BACKUP_BLOCK_MARCO_RANGE_INDEX_INDEX = 5,
  BACKUP_BLOCK_META_INDEX_INDEX = 6,
  BACKUP_BLOCK_MAX = 6,
};

enum ObBackupRestoreMode {
  BACKUP_MODE = 0,
  RESTORE_MODE = 1,
  MAX_MODE,
};

struct ObBackupIndexMergeParam;

struct ObLSBackupParam {
  ObLSBackupParam();
  virtual ~ObLSBackupParam();
  bool is_valid() const;
  int assign(const ObLSBackupParam &param);
  int convert_to(const ObBackupIndexLevel &index_level, const share::ObBackupDataType &backup_data_type,
      ObBackupIndexMergeParam &merge_param);
  int assign(const ObBackupIndexMergeParam &param);
  TO_STRING_KV(K_(job_id), K_(task_id), K_(backup_dest), K_(tenant_id), K_(dest_id), K_(backup_set_desc), K_(ls_id),
      K_(turn_id), K_(retry_id));
  int64_t job_id_;
  int64_t task_id_;
  share::ObBackupDest backup_dest_;
  uint64_t tenant_id_;
  int64_t dest_id_;
  share::ObBackupSetDesc backup_set_desc_;
  share::ObLSID ls_id_;
  int64_t turn_id_;
  int64_t retry_id_;
};

struct ObBackupIndexMergeParam {
  ObBackupIndexMergeParam();
  ~ObBackupIndexMergeParam();
  bool is_valid() const;
  int assign(const ObBackupIndexMergeParam &param);
  TO_STRING_KV(K_(task_id), K_(backup_dest), K_(tenant_id), K_(dest_id), K_(backup_set_desc), K_(backup_data_type), K_(index_level),
      K_(ls_id), K_(turn_id), K_(retry_id));

  int64_t task_id_;
  share::ObBackupDest backup_dest_;
  uint64_t tenant_id_;
  int64_t dest_id_;
  share::ObBackupSetDesc backup_set_desc_;
  share::ObBackupDataType backup_data_type_;
  ObBackupIndexLevel index_level_;
  share::ObLSID ls_id_;
  int64_t turn_id_;
  int64_t retry_id_;
};

enum ObBackupDataVersion {
  BACKUP_DATA_VERSION_V1 = 1,  // since 4.0.0
  BACKUP_DATA_VERSION_MAX,
};

enum ObBackupFileType {
  BACKUP_DATA_FILE = 0,
  BACKUP_MACRO_RANGE_INDEX_FILE = 1,
  BACKUP_META_INDEX_FILE = 2,
  BACKUP_SEC_META_INDEX_FILE = 3,
  BACKUP_FILE_TYPE_MAX,
};

enum ObBackupFileMagic {
  BACKUP_DATA_FILE_MAGIC = 0x0F0F,
  BACKUP_MACRO_RANGE_INDEX_FILE_MAGIC = 0x1F1F,
  BACKUP_META_INDEX_FILE_MAGIC = 0x2F2F,
  BACKUP_MAGIC_MAX = 0xFFFF,
};

int convert_backup_file_type_to_magic(const ObBackupFileType &file_type, ObBackupFileMagic &magic);

struct ObBackupFileHeader {
  ObBackupFileHeader();
  void reset();
  int check_valid() const;
  TO_STRING_KV(K_(magic), K_(version), K_(file_type), K_(reserved));
  int32_t file_type_;
  int32_t version_;
  int32_t magic_;
  int32_t reserved_;
};

struct ObBackupDataFileTrailer {
  ObBackupDataFileTrailer();
  void reset();
  void set_trailer_checksum();
  int16_t calc_trailer_checksum() const;
  int check_trailer_checksum() const;
  int check_valid() const;
  TO_STRING_KV(K_(data_type), K_(data_version), K_(macro_block_count), K_(meta_count), K_(macro_index_offset),
      K_(macro_index_length), K_(meta_index_offset), K_(meta_index_length), K_(offset), K_(length),
      K_(data_accumulate_checksum), K_(trailer_checksum));

  uint16_t data_type_;
  uint16_t data_version_;
  int64_t macro_block_count_;
  int64_t meta_count_;
  int64_t macro_index_offset_;
  int64_t macro_index_length_;
  int64_t meta_index_offset_;
  int64_t meta_index_length_;
  int64_t offset_;
  int64_t length_;
  int64_t data_accumulate_checksum_;
  int16_t trailer_checksum_;
};

struct ObBackupMacroBlockId {
  ObBackupMacroBlockId();
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(logic_id), K_(macro_block_id), K_(nested_offset), K_(nested_size));
  blocksstable::ObLogicMacroBlockId logic_id_;
  blocksstable::MacroBlockId macro_block_id_;
  int64_t nested_offset_;
  int64_t nested_size_;
};

struct ObBackupMacroBlockIndex;

struct ObBackupPhysicalID final {
  ObBackupPhysicalID();
  ObBackupPhysicalID(const ObBackupPhysicalID &id);
  ~ObBackupPhysicalID() = default;
  void reset();
  bool is_valid() const;
  int get_backup_macro_block_index(
      const blocksstable::ObLogicMacroBlockId &logic_id, ObBackupMacroBlockIndex &macro_index) const;
  ObBackupPhysicalID &operator=(const ObBackupPhysicalID &other);
  bool operator==(const ObBackupPhysicalID &other) const;
  bool operator!=(const ObBackupPhysicalID &other) const;
  static const ObBackupPhysicalID get_default();

  NEED_SERIALIZE_AND_DESERIALIZE;
  TO_STRING_KV(K_(backup_set_id), K_(ls_id), K_(turn_id), K_(retry_id), K_(file_id), K_(offset), K_(length));

public:
  static const int64_t BACKUP_TYPE_BIT = 12;
  static const int64_t BACKUP_LS_ID_BIT = 64;
  static const int64_t BACKUP_TURN_ID_BIT = 10;
  static const int64_t BACKUP_RETRY_ID_BIT = 10;
  static const int64_t BACKUP_FILE_ID_BIT = 22;
  static const int64_t BACKUP_RESERVED_ID_BIT = 10;
  static const int64_t BACKUP_SET_ID_BIT = 24;
  static const int64_t BACKUP_OFFSET_BIT = 26;
  static const int64_t BACKUP_LENGTH_BIT = 14;
  static constexpr const int64_t MAX_BACKUP_LS_ID = INT64_MAX;
  static constexpr const int64_t MAX_BACKUP_SET_ID = (1 << BACKUP_SET_ID_BIT) - 1;
  static constexpr const int64_t MAX_BACKUP_TURN_ID = (1 << BACKUP_TURN_ID_BIT) - 1;
  static constexpr const int64_t MAX_BACKUP_RETRY_ID = (1 << BACKUP_RETRY_ID_BIT) - 1;
  static constexpr const int64_t MAX_BACKUP_FILE_ID = (1 << BACKUP_FILE_ID_BIT) - 1;
  static constexpr const int64_t MAX_BACKUP_FILE_SIZE = (1 << BACKUP_OFFSET_BIT) - 1;
  static constexpr const int64_t MAX_BACKUP_BLOCK_SIZE = (1 << BACKUP_LENGTH_BIT) - 1;

public:
  union {
    int64_t first_id_;
    struct {
      int64_t ls_id_ : BACKUP_LS_ID_BIT;
    };
  };
  union {
    int64_t second_id_;
    struct {
      uint64_t type_ : BACKUP_TYPE_BIT;
      uint64_t turn_id_ : BACKUP_TURN_ID_BIT;
      uint64_t retry_id_ : BACKUP_RETRY_ID_BIT;
      uint64_t file_id_ : BACKUP_FILE_ID_BIT;
      uint64_t reserved_ : BACKUP_RESERVED_ID_BIT;
    };
  };
  union {
    int64_t third_id_;
    struct {
      uint64_t backup_set_id_ : BACKUP_SET_ID_BIT;
      uint64_t offset_ : BACKUP_OFFSET_BIT;
      uint64_t length_ : BACKUP_LENGTH_BIT;
    };
  };
};

struct ObBackupMacroBlockIndex {
  OB_UNIS_VERSION(1);

public:
  ObBackupMacroBlockIndex();
  void reset();
  bool is_valid() const;
  int get_backup_physical_id(ObBackupPhysicalID &physical_id) const;
  bool operator==(const ObBackupMacroBlockIndex &other) const;
  TO_STRING_KV(
      K_(logic_id), K_(backup_set_id), K_(ls_id), K_(turn_id), K_(retry_id), K_(file_id), K_(offset), K_(length));
  blocksstable::ObLogicMacroBlockId logic_id_;
  int64_t backup_set_id_;
  share::ObLSID ls_id_;
  int64_t turn_id_;
  int64_t retry_id_;
  int64_t file_id_;
  int64_t offset_;
  int64_t length_;
};

// the index is group by blocks, a index block is typically 16KB in size
// the index block is appended at the end of 4GB data file, there may be
// several index block in the 4GB data file
// @param offset_ : means the offset of index block in 4GB data file
// @param length_ : means the length of index block in 4GB data file
// @param first_index_ : means the first index number in the index block
// @param last_index_ : means the last index number in the index block
//                      if there exists many index blocks, the last index
//                      of each index block is always greater then the prev
//                      index block
struct ObBackupIndexBlockDesc {
  ObBackupIndexBlockDesc() : offset_(0), length_(0), first_index_(0), last_index_(0)
  {}
  TO_STRING_KV(K_(offset), K_(length), K_(first_index), K_(last_index));
  int64_t offset_;
  int64_t length_;
  int64_t first_index_;
  int64_t last_index_;
};

struct ObBackupMacroRangeIndex {
  OB_UNIS_VERSION(1);

public:
  ObBackupMacroRangeIndex();
  void reset();
  bool is_valid() const;
  bool operator==(const ObBackupMacroRangeIndex &other) const;
  TO_STRING_KV(K_(start_key), K_(end_key), K_(backup_set_id), K_(ls_id), K_(turn_id), K_(retry_id), K_(file_id),
      K_(offset), K_(length));
  blocksstable::ObLogicMacroBlockId start_key_;
  blocksstable::ObLogicMacroBlockId end_key_;
  int64_t backup_set_id_;
  share::ObLSID ls_id_;
  int64_t turn_id_;
  int64_t retry_id_;
  int64_t file_id_;
  int64_t offset_;
  int64_t length_;
};

// used when build multi level index
struct ObBackupMacroRangeIndexIndex {
  OB_UNIS_VERSION(1);

public:
  ObBackupMacroRangeIndexIndex();
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(end_key), K_(offset), K_(length));
  ObBackupMacroRangeIndex end_key_;
  int64_t offset_;
  int64_t length_;
};

class ObBackupMacroRangeIndex;

struct ObBackupMacroBlockIndexComparator {
  int operator()(const ObBackupMacroRangeIndex &lhs, const ObBackupMacroRangeIndex &rhs) const;
};

class ObBackupMetaIndex;

struct ObBackupMetaIndexComparator {
  int operator()(const ObBackupMetaIndex &lhs, const ObBackupMetaIndex &rhs) const;
};

struct ObBackupMacroRangeIndexCompareFunctor {
  bool operator()(const ObBackupMacroRangeIndex &lhs, const ObBackupMacroRangeIndex &rhs) const
  {
    return lhs.end_key_ < rhs.end_key_;
  }
};

struct ObCompareBackupMacroRangeIndexLogicId {
  bool operator()(const ObBackupMacroRangeIndex &item, const blocksstable::ObLogicMacroBlockId &logic_id) const
  {
    return item.end_key_ < logic_id;
  }
};

struct ObCompareBackupMacroRangeIndexIndexLogicId {
  bool operator()(const ObBackupMacroRangeIndexIndex &item, const blocksstable::ObLogicMacroBlockId &logic_id) const
  {
    return item.end_key_.end_key_ < logic_id;
  }
};

struct ObBackupMultiLevelIndexHeader {
  static const int16_t META_MULTI_INDEX_MAGIC = 0xFFFF;
  static const int16_t MACRO_MULTI_INDEX_MAGIC = 0xFFFE;
  OB_UNIS_VERSION(1);

public:
  ObBackupMultiLevelIndexHeader();
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(magic), K_(backup_type), K_(index_level));
  int16_t magic_;
  int16_t backup_type_;
  int32_t index_level_;
};

enum ObBackupMetaType {
  BACKUP_SSTABLE_META = 0,
  BACKUP_TABLET_META = 1,
  BACKUP_MACRO_BLOCK_ID_MAPPING_META = 2,
  BACKUP_META_MAX,
};

struct ObBackupMetaKey {
  OB_UNIS_VERSION(1);

public:
  ObBackupMetaKey();
  ~ObBackupMetaKey();
  bool operator<(const ObBackupMetaKey &other) const;
  bool operator>(const ObBackupMetaKey &other) const;
  bool operator==(const ObBackupMetaKey &other) const;
  bool operator!=(const ObBackupMetaKey &other) const;
  bool is_valid() const;
  void reset();
  int get_backup_index_file_type(ObBackupFileType &backup_file_type) const;
  TO_STRING_KV(K_(tablet_id), K_(meta_type));

  common::ObTabletID tablet_id_;
  ObBackupMetaType meta_type_;
};

struct ObBackupTabletMeta {
  OB_UNIS_VERSION(1);

public:
  ObBackupTabletMeta();
  virtual ~ObBackupTabletMeta() = default;
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(tablet_id), K_(tablet_meta));
  common::ObTabletID tablet_id_;
  storage::ObMigrationTabletParam tablet_meta_;
};

struct ObBackupSSTableMeta {
  OB_UNIS_VERSION(1);

public:
  ObBackupSSTableMeta();
  virtual ~ObBackupSSTableMeta() = default;
  bool is_valid() const;
  void reset();
  int assign(const ObBackupSSTableMeta &backup_sstable_meta);

  TO_STRING_KV(K_(tablet_id), K_(sstable_meta), K_(logic_id_list));
  common::ObTabletID tablet_id_;
  blocksstable::ObMigrationSSTableParam sstable_meta_;
  common::ObSArray<blocksstable::ObLogicMacroBlockId> logic_id_list_;
};

struct ObBackupMacroBlockIDPair final {
  static const uint64_t VERSION = 1;
  OB_UNIS_VERSION(VERSION);

public:
  ObBackupMacroBlockIDPair();
  ~ObBackupMacroBlockIDPair();
  bool operator<(const ObBackupMacroBlockIDPair &other) const;
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(logic_id), K_(physical_id));
  blocksstable::ObLogicMacroBlockId logic_id_;
  ObBackupPhysicalID physical_id_;
};

// TODO(yangyi.yyy): separate array and map to different data structure in 4.1
struct ObBackupMacroBlockIDMapping final {
  ObBackupMacroBlockIDMapping();
  ~ObBackupMacroBlockIDMapping();
  void reuse();
  int prepare_tablet_sstable(const uint64_t tenant_id,
      const storage::ObITable::TableKey &table_key, const common::ObIArray<blocksstable::ObLogicMacroBlockId> &list);
  TO_STRING_KV(K_(table_key), K_(id_pair_list));
  storage::ObITable::TableKey table_key_;
  common::ObArray<ObBackupMacroBlockIDPair> id_pair_list_;

private:
  friend class ObBackupTabletCtx;
  typedef common::hash::ObHashMap<blocksstable::ObLogicMacroBlockId, int64_t /*idx_in_array*/> LogicIdArrayIdxMap;
  LogicIdArrayIdxMap map_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupMacroBlockIDMapping);
};

struct ObBackupMacroBlockIDMappingsMeta final {
  enum Version {
    MAPPING_META_VERSION_V1 = 1,
    MAPPING_META_VERSION_MAX,
  };

public:
  ObBackupMacroBlockIDMappingsMeta();
  ~ObBackupMacroBlockIDMappingsMeta();
  void reuse();
  NEED_SERIALIZE_AND_DESERIALIZE;
  TO_STRING_KV(K_(sstable_count));

public:
  int64_t version_;
  int64_t sstable_count_;
  ObBackupMacroBlockIDMapping id_map_list_[MAX_SSTABLE_CNT_IN_STORAGE];
  DISALLOW_COPY_AND_ASSIGN(ObBackupMacroBlockIDMappingsMeta);
};

struct ObBackupMetaIndex {
  static const uint64_t VERSION = 1;
  OB_UNIS_VERSION(VERSION);

public:
  ObBackupMetaIndex();
  void reset();
  bool is_valid() const;
  bool operator==(const ObBackupMetaIndex &other) const;
  TO_STRING_KV(
      K_(meta_key), K_(backup_set_id), K_(ls_id), K_(turn_id), K_(retry_id), K_(file_id), K_(offset), K_(length));
  ObBackupMetaKey meta_key_;
  int64_t backup_set_id_;
  share::ObLSID ls_id_;
  int64_t turn_id_;
  int64_t retry_id_;
  int64_t file_id_;
  int64_t offset_;
  int64_t length_;
};

// used when build multi level index
struct ObBackupMetaIndexIndex {
  static const uint64_t VERSION = 1;
  OB_UNIS_VERSION(VERSION);

public:
  ObBackupMetaIndexIndex();
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(end_key), K_(offset), K_(length));
  ObBackupMetaIndex end_key_;
  int64_t offset_;
  int64_t length_;
};

struct ObBackupMetaIndexCompareFunctor {
  bool operator()(const ObBackupMetaIndex &lhs, const ObBackupMetaIndex &rhs) const
  {
    return lhs.meta_key_ < rhs.meta_key_;
  }
};

struct ObCompareBackupMetaIndexTabletId {
  bool operator()(const ObBackupMetaIndex &item, const ObBackupMetaKey &meta_key) const
  {
    return item.meta_key_ < meta_key;
  }
};

struct ObCompareBackupMetaIndexIndexTabletId {
  bool operator()(const ObBackupMetaIndexIndex &item, const ObBackupMetaKey &meta_key) const
  {
    return item.end_key_.meta_key_ < meta_key;
  }
};

struct ObBackupMultiLevelIndexTrailer {
  ObBackupMultiLevelIndexTrailer();
  void reset();
  void set_trailer_checksum();
  int16_t calc_trailer_checksum() const;
  int check_trailer_checksum() const;
  int check_valid() const;
  bool is_empty_index() const { return 0 == last_block_length_; /*0 means has no index at all*/ }
  TO_STRING_KV(K_(file_type), K_(tree_height), K_(last_block_offset), K_(last_block_length), K_(checksum));
  int32_t file_type_;
  int32_t tree_height_;
  int64_t last_block_offset_;
  int64_t last_block_length_;
  int16_t checksum_;
};

struct ObLSBackupStat {
  ObLSBackupStat();
  TO_STRING_KV(K_(ls_id), K_(backup_set_id), K_(file_id), K_(input_bytes), K_(output_bytes),
      K_(finish_macro_block_count), K_(finish_sstable_count), K_(finish_tablet_count));
  share::ObLSID ls_id_;
  int64_t backup_set_id_;
  int64_t file_id_;
  int64_t input_bytes_;
  int64_t output_bytes_;
  int64_t finish_macro_block_count_;
  int64_t finish_sstable_count_;
  int64_t finish_tablet_count_;
};

struct ObBackupRetryDesc {
  ObBackupRetryDesc();
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(ls_id), K_(turn_id), K_(retry_id), K_(last_file_id));
  share::ObLSID ls_id_;
  int64_t turn_id_;
  int64_t retry_id_;
  int64_t last_file_id_;
};

struct ObBackupMacroIndexMergeCtx {
  ObBackupMacroIndexMergeCtx();
  virtual ~ObBackupMacroIndexMergeCtx();
  TO_STRING_KV(K_(tenant_id), K_(backup_type), K_(ls_id), K_(retry_list));
  uint64_t tenant_id_;
  share::ObBackupDataType backup_type_;
  share::ObLSID ls_id_;
  ObArray<ObBackupRetryDesc> retry_list_;
};

struct ObLSBackupDagInitParam;

struct ObLSBackupDataParam {
  ObLSBackupDataParam();
  ~ObLSBackupDataParam();
  bool is_valid() const;
  int convert_to(const ObLSBackupStage &stage, ObLSBackupDagInitParam &dag_param);
  int convert_to(const ObBackupIndexLevel &index_level, ObBackupIndexMergeParam &merge_param);
  int assign(const ObLSBackupDataParam &other);
  TO_STRING_KV(K_(job_desc), K_(backup_stage), K_(backup_dest), K_(tenant_id), K_(dest_id), K_(backup_set_desc), K_(ls_id),
      K_(backup_data_type), K_(turn_id), K_(retry_id));
  ObBackupJobDesc job_desc_;
  ObLSBackupStage backup_stage_;
  share::ObBackupDest backup_dest_;
  uint64_t tenant_id_;
  int64_t dest_id_;
  share::ObBackupSetDesc backup_set_desc_;
  share::ObLSID ls_id_;
  share::ObBackupDataType backup_data_type_;
  int64_t turn_id_;
  int64_t retry_id_;
};

// TODO(yangyi.yyy): move to some place suitable later in 4.1
static bool is_aligned(uint64_t x)
{
  static const int64_t alignment = DIO_READ_ALIGN_SIZE;
  return 0 == (x & (alignment - 1));
}

int build_backup_file_header_buffer(const ObBackupFileHeader &file_header, const int64_t buf_len, char *buf,
    blocksstable::ObBufferReader &buffer_reader);
int build_common_header(const int64_t data_type, const int64_t data_length, const int64_t align_length,
    share::ObBackupCommonHeader *&common_header);
int build_multi_level_index_header(
    const int64_t index_type, const int64_t index_level, ObBackupMultiLevelIndexHeader &header);

struct ObBackupLSTaskInfo {
  ObBackupLSTaskInfo();
  ~ObBackupLSTaskInfo();
  bool is_valid() const;
  void reset();

  TO_STRING_KV(K_(task_id), K_(tenant_id), K_(ls_id), K_(turn_id), K_(retry_id), K_(backup_data_type),
      K_(backup_set_id), K_(input_bytes), K_(output_bytes), K_(tablet_count), K_(finish_tablet_count),
      K_(macro_block_count), K_(finish_macro_block_count), K_(max_file_id), K_(is_final));

  int64_t task_id_;
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  int64_t turn_id_;
  int64_t retry_id_;
  int64_t backup_data_type_;

  int64_t backup_set_id_;
  int64_t input_bytes_;
  int64_t output_bytes_;
  int64_t tablet_count_;
  int64_t finish_tablet_count_;
  int64_t macro_block_count_;
  int64_t finish_macro_block_count_;
  int64_t max_file_id_;
  bool is_final_;
};

struct ObBackupSkippedTablet {
  ObBackupSkippedTablet();
  ~ObBackupSkippedTablet();
  bool is_valid() const;
  TO_STRING_KV(K_(task_id), K_(tenant_id), K_(turn_id), K_(retry_id), K_(tablet_id),
      K_(ls_id), K_(backup_set_id), K_(skipped_type), K_(data_type));
  int64_t task_id_;
  uint64_t tenant_id_;
  int64_t turn_id_;
  int64_t retry_id_;
  common::ObTabletID tablet_id_;
  share::ObLSID ls_id_;
  int64_t backup_set_id_;
  share::ObBackupSkippedType skipped_type_;
  share::ObBackupDataType data_type_;
};

struct ObBackupReportCtx final {
  ObBackupReportCtx();
  ~ObBackupReportCtx();
  bool is_valid() const;
  TO_STRING_KV(KP_(location_service), KP_(sql_proxy), KP_(rpc_proxy));

  share::ObLocationService *location_service_;
  common::ObMySQLProxy *sql_proxy_;
  obrpc::ObSrvRpcProxy *rpc_proxy_;
};

}  // namespace backup
}  // namespace oceanbase

#endif
