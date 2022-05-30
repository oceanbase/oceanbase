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

#include <unistd.h>
#include "common/rowkey/ob_rowkey.h"
#include "storage/ob_i_store.h"
#include "storage/ob_partition_store.h"
#include "storage/blocksstable/ob_column_map.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/ob_macro_block_common_header.h"
#include "storage/blocksstable/slog/ob_base_storage_logger.h"
#include "ob_admin_dumpsst_print_helper.h"


#define ALLOC_OBJECT_1(allocator, obj, T, ...) \
  do \
  { \
    obj = NULL; \
    void *tmp = reinterpret_cast<void *>(allocator.alloc(sizeof(T))); \
    if (NULL == tmp) \
    { \
      _OB_LOG(WARN, "alloc mem for %s", #T); \
    } \
    else \
    { \
      obj = new(tmp) T( __VA_ARGS__ ); \
    } \
  } \
  while (0)

namespace oceanbase
{
namespace tools
{

int parse_string(const char* src, const char del, const char* dst[], int64_t& size);
int parse_table_key(const char *str, storage::ObITable::TableKey &table_key);
int parse_partition_key(const char *str, ObPartitionKey &pkey);
int parse_version_range(const char *str, ObVersionRange &version_range);
int parse_log_ts_range(const char *str, ObLogTsRange &log_ts_range);

struct DataBlock
{
  DataBlock() : data_(NULL), size_(0) {}
  const char *data_;
  int64_t size_;
  void reset()
  {
    data_ = NULL;
    size_ = 0;
  }
};

struct MacroBlock : public DataBlock
{
  MacroBlock() : DataBlock(), macro_id_(-1), common_header_() {}
  int setup(const char *data, const int64_t size, const int64_t macro_id);
  bool is_valid() const
  {
    return macro_id_ >= 0
        && data_ != NULL
        && size_ > 0
        && common_header_.is_valid();
  }
  void reset()
  {
    macro_id_ = -1;
    common_header_.reset();
    DataBlock::reset();
  }
  TO_STRING_KV(KP_(data),
               K_(size),
               K_(macro_id));
  int64_t macro_id_;
  blocksstable::ObMacroBlockCommonHeader common_header_;
};

struct MicroBlock : public DataBlock
{
  MicroBlock() : DataBlock(),
                 row_store_type_(-1),
                 micro_id_(-1),
                 column_map_(NULL),
                 column_id_list_(NULL),
                 column_cnt_(0)
  {}
  int setup(const char *data, const int64_t size, const int64_t micro_id,
      const blocksstable::ObColumnMap *column_map, const int64_t row_store_type,
      const uint16_t *column_id_list, const int64_t column_cnt);
  void reset()
  {
    row_store_type_ = -1;
    column_map_ = NULL;
    micro_id_ = -1;
    column_cnt_ = 0;
    column_id_list_ = NULL;
    DataBlock::reset();
  }
  bool is_valid() const
  {
    return data_ != NULL
        && size_ > 0
        && micro_id_ >= 0
        && column_map_ != NULL
        && column_id_list_ != NULL
        && column_cnt_ > 0;
  }
  TO_STRING_KV(KP_(data),
               K_(size),
               K_(micro_id),
               K_(row_store_type),
               K_(column_cnt));
  int64_t row_store_type_;
  int64_t micro_id_;
  const blocksstable::ObColumnMap *column_map_;
  const uint16_t *column_id_list_;
  int64_t column_cnt_;
};

template <typename T>
class DataBlockReader
{
public:
  virtual int64_t count() const = 0;
  virtual int set_index(const int64_t index) = 0;
  virtual int64_t get_index() const = 0;
  virtual int get_value(const T *&value) = 0;
  virtual int dump(const int64_t index) = 0;
public:
  static void dump_common_header(const blocksstable::ObMacroBlockCommonHeader *common_header);
  static void dump_sstable_header(const blocksstable::ObSSTableMacroBlockHeader *sstable_header);
  static void dump_linked_header(const blocksstable::ObLinkedMacroBlockHeader *linked_header);
  static void dump_row(const storage::ObStoreRow *row, const bool use_csv = false);
};

template <typename T>
void DataBlockReader<T>::dump_common_header(const blocksstable::ObMacroBlockCommonHeader *common_header)
{
  PrintHelper::print_dump_title("Common Header");
  fprintf(stdout, "%s\n", to_cstring(*common_header));
  PrintHelper::print_end_line();
}

template <typename T>
void DataBlockReader<T>::dump_sstable_header(const blocksstable::ObSSTableMacroBlockHeader *sstable_header)
{
  PrintHelper::print_dump_title("SSTable Header");
  PrintHelper::print_dump_line("header_size", sstable_header->header_size_);
  PrintHelper::print_dump_line("version", sstable_header->version_);
  PrintHelper::print_dump_line("magic", sstable_header->magic_);
  PrintHelper::print_dump_line("attr", sstable_header->attr_);
  PrintHelper::print_dump_line("table_id", sstable_header->table_id_);
  PrintHelper::print_dump_line("data_version", sstable_header->data_version_);
  PrintHelper::print_dump_line("column_count", sstable_header->column_count_);
  PrintHelper::print_dump_line("rowkey_column_count", sstable_header->rowkey_column_count_);
  PrintHelper::print_dump_line("row_store_type", sstable_header->row_store_type_);
  PrintHelper::print_dump_line("row_count", sstable_header->row_count_);
  PrintHelper::print_dump_line("occupy_size", sstable_header->occupy_size_);
  PrintHelper::print_dump_line("micro_block_count", sstable_header->micro_block_count_);
  PrintHelper::print_dump_line("micro_block_size", sstable_header->micro_block_size_);
  PrintHelper::print_dump_line("micro_block_data_offset", sstable_header->micro_block_data_offset_);
  PrintHelper::print_dump_line("micro_block_index_offset", sstable_header->micro_block_index_offset_);
  PrintHelper::print_dump_line("micro_block_index_size", sstable_header->micro_block_index_size_);
  PrintHelper::print_dump_line("micro_block_endkey_offset", sstable_header->micro_block_endkey_offset_);
  PrintHelper::print_dump_line("micro_block_endkey_size", sstable_header->micro_block_endkey_size_);
  PrintHelper::print_dump_line("data_checksum", sstable_header->data_checksum_);
  PrintHelper::print_dump_line("compressor_name", sstable_header->compressor_name_);
  PrintHelper::print_dump_line("data_seq", sstable_header->data_seq_);
  PrintHelper::print_dump_line("partition_id", sstable_header->partition_id_);
  PrintHelper::print_end_line();
}

template <typename T>
void DataBlockReader<T>::dump_linked_header(const blocksstable::ObLinkedMacroBlockHeader *linked_header)
{
  PrintHelper::print_dump_title("Lined Header");
  PrintHelper::print_dump_line("header_size", linked_header->header_size_);
  PrintHelper::print_dump_line("version", linked_header->version_);
  PrintHelper::print_dump_line("magic", linked_header->magic_);
  PrintHelper::print_dump_line("attr", linked_header->attr_);
  PrintHelper::print_dump_line("meta_data_offset", linked_header->meta_data_offset_);
  PrintHelper::print_dump_line("meta_data_count", linked_header->meta_data_count_);
  PrintHelper::print_dump_line("previous_block_index", linked_header->previous_block_index_);
  PrintHelper::print_dump_line("total_previous_count", linked_header->total_previous_count_);
  PrintHelper::print_dump_line("user_data1", linked_header->user_data1_);
  PrintHelper::print_dump_line("user_data1", linked_header->user_data2_);
  PrintHelper::print_end_line();
}

template <typename T>
void DataBlockReader<T>::dump_row(const storage::ObStoreRow *row, const bool use_csv)
{
  for (int64_t i = 0; i < row->row_val_.count_; ++i) {
    common::ObObj &cell = row->row_val_.cells_[i];
    PrintHelper::print_cell(cell, use_csv);
  }
  fprintf(stderr, "\n");
}

class MacroBlockReader : public DataBlockReader<MicroBlock>
{
public:
  MacroBlockReader() : sstable_header_(NULL),
                       macro_meta_(NULL),
                       column_id_list_(NULL),
                       column_type_list_(NULL),
                       column_checksum_(NULL),
                       curr_micro_block_id_(-1),
                       cur_mib_rh_(NULL),
                       is_inited_(false)
  {}
  virtual ~MacroBlockReader() {};
  int init(const char *data, const int64_t size, const int64_t macro_id, const blocksstable::ObMacroBlockMeta *macro_meta);
  virtual int64_t count() const override;
  virtual int set_index(const int64_t index) override;
  virtual int64_t get_index() const override;
  virtual int get_value(const MicroBlock *&value) override;
  virtual int dump(const int64_t micro_id) override;
  void reset();
private:
  int parse_micro_block_index();
  void dump_micro_index(const ObRowkey& endkey, const blocksstable::ObPosition& datapos,
      const blocksstable::ObPosition& keypos, const bool mark_deletion, int32_t num);
  int parse_micro_block_key(const char* buf, int64_t& pos, int64_t row_end_pos,
    const ObObjMeta* type_list, ObObj* objs, int32_t rowkey_count);
  void parse_one_micro_block_index(const char* index_ptr, const int64_t idx,
      blocksstable::ObPosition& datapos, blocksstable::ObPosition& keypos);
  int build_column_map();
  int set_current_micro_block();
  int dump_header();
  int get_micro_block_payload_buffer(const char* compressor_name,
                                     const char* buf,
                                     const int64_t size,
                                     const char*& out,
                                     int64_t& outsize,
                                     const blocksstable::ObRecordHeaderV3 *&rh);
private:
  static const int64_t ALL_MINOR_INDEX = -1;
  MacroBlock macro_block_;
  const blocksstable::ObSSTableMacroBlockHeader *sstable_header_;
  const blocksstable::ObMacroBlockMeta *macro_meta_;
  const uint16_t * column_id_list_;
  const common::ObObjMeta *column_type_list_;
  const int64_t *column_checksum_;
  int64_t curr_micro_block_id_;
  MicroBlock curr_micro_block_;
  common::ObSEArray<blocksstable::ObPosition, 100> micro_index_pos_;
  common::ObSEArray<blocksstable::ObPosition, 100> micro_index_keys_;
  common::ObSEArray<bool, 100> micro_mark_deletions_;
  blocksstable::ObColumnMap column_map_;
  const blocksstable::ObRecordHeaderV3 *cur_mib_rh_;
  common::ObArenaAllocator allocator_;
  bool is_inited_;
};

class MicroBlockIReader : public DataBlockReader<storage::ObStoreRow>
{
public:
  virtual int64_t count() const = 0;
  virtual int set_index(const int64_t index) = 0;
  virtual int64_t get_index() const = 0;
  virtual int get_value(const storage::ObStoreRow *&value) = 0;
  virtual int dump(const int64_t index) = 0;
  virtual int init(const MicroBlock &micro_block) = 0;
};

class FlatMicroBlockReader : public MicroBlockIReader
{
public:
  FlatMicroBlockReader() : micro_block_header_(NULL),
                           index_buffer_(NULL),
                           curr_row_index_(0),
                           is_inited_(false)
  {}
  virtual int init(const MicroBlock &micro_block) override;
  virtual int64_t count() const override;
  virtual int set_index(const int64_t index) override;
  virtual int64_t get_index() const override;
  virtual int get_value(const storage::ObStoreRow *&value) override;
  virtual int dump(const int64_t index) override;
  void reset();
private:
  static void dump_micro_header(const blocksstable::ObMicroBlockHeader* micro_block_header);
private:
  MicroBlock micro_block_;
  const blocksstable::ObMicroBlockHeader *micro_block_header_;
  const int32_t *index_buffer_;
  blocksstable::ObFlatRowReader reader_;
  int64_t curr_row_index_;
  common::ObObj columns_[common::OB_MAX_COLUMN_NUMBER];
  storage::ObStoreRow curr_row_;
  common::ObArenaAllocator allocator_;
  bool is_inited_;
};

class MicroBlockReader : public DataBlockReader<storage::ObStoreRow>
{
public:
  MicroBlockReader() : micro_reader_(NULL), is_inited_(false) {}
  int init(const MicroBlock &micro_block);
  virtual int64_t count() const override;
  virtual int set_index(const int64_t index) override;
  virtual int64_t get_index() const override;
  virtual int get_value(const storage::ObStoreRow *&value) override;
  virtual int dump(const int64_t index) override;
  void reset();
private:
  MicroBlock micro_block_;
  MicroBlockIReader *micro_reader_;
  FlatMicroBlockReader flat_micro_reader_;
  bool is_inited_;
};

class ObDumpsstPartition
{
public:
  int deserialize(const char *buf, const int64_t buf_len, int64_t &pos);
  int get_latest_store();
private:
  storage::ObPartitionStore store_;
};

class ObDumpsstPartitionImage : public blocksstable::ObIRedoModule
{
public:
  static ObDumpsstPartitionImage &get_instance();
  int init();
  virtual int replay(const blocksstable::ObRedoModuleReplayParam &param) override
  {
    UNUSEDx(param);
    return common::OB_SUCCESS;
  }

  virtual int parse(
      const int64_t subcmd,
      const char *buf,
      const int64_t len,
      FILE *stream) override
  {
    UNUSEDx(subcmd, buf, len, stream);
    return common::OB_SUCCESS;
  }
private:
  ObDumpsstPartitionImage() {}
  ~ObDumpsstPartitionImage() {}
};
}
}
