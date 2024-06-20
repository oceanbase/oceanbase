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
#pragma once

#include "ob_direct_load_tmp_file.h"
#include "storage/direct_load/ob_direct_load_external_row.h"
#include "storage/direct_load/ob_direct_load_i_table.h"
#include "storage/direct_load/ob_direct_load_sstable.h"
#include "storage/direct_load/ob_direct_load_table_data_desc.h"

namespace oceanbase
{
namespace storage
{
struct ObDirectLoadDataBlockHeader
{
  OB_UNIS_VERSION(1);
public:
  ObDirectLoadDataBlockHeader() : size_(0), occupy_size_(0), last_row_offset_(0) {}
  TO_STRING_KV(K_(size), K_(occupy_size), K(last_row_offset_));
public:
  int32_t size_; // 有效数据大小
  int32_t occupy_size_; // 实际占用大小
  int32_t last_row_offset_;
};

struct ObDirectLoadIndexBlockHeader
{
  OB_UNIS_VERSION(1);
public:
  ObDirectLoadIndexBlockHeader() : start_offset_(0), row_count_(0) {}
  TO_STRING_KV(K(start_offset_), K(row_count_));
public:
  uint64_t start_offset_;
  int64_t row_count_; //索引块的row_count
};

struct ObDirectLoadIndexBlockItem
{
  OB_UNIS_VERSION(1);
public:
  ObDirectLoadIndexBlockItem() : end_offset_(0) {}
  TO_STRING_KV(K(end_offset_));
public:
  uint64_t end_offset_; //索引项的offset
};

struct ObDirectLoadIndexInfo
{
public:
  ObDirectLoadIndexInfo() : offset_(0), size_(0) {}
  uint64_t offset_; //索引块对应数据块的offset
  int64_t size_; //数据块的大小
  TO_STRING_KV(K(offset_), K(size_));
};

class ObDirectLoadIndexBlock
{
public:
  static int64_t get_item_num_per_block(int64_t block_size);
};

struct ObDirectLoadSSTableBuildParam
{
public:
  ObDirectLoadSSTableBuildParam() : datum_utils_(nullptr), file_mgr_(nullptr) {}
  bool is_valid() const
  {
    return tablet_id_.is_valid() && table_data_desc_.is_valid() && nullptr != file_mgr_ &&
           nullptr != datum_utils_;
  }
  TO_STRING_KV(K_(tablet_id), K_(table_data_desc), KP_(file_mgr), KP_(datum_utils));
public:
  common::ObTabletID tablet_id_;
  ObDirectLoadTableDataDesc table_data_desc_;
  const blocksstable::ObStorageDatumUtils *datum_utils_;
  ObDirectLoadTmpFileManager *file_mgr_;
};

class ObDirectLoadIndexBlockWriter
{
public:
  ObDirectLoadIndexBlockWriter();
  ~ObDirectLoadIndexBlockWriter();
  int init(uint64_t tenant_id, int64_t buf_size, const ObDirectLoadTmpFileHandle &file_handle);
  int append_row(int64_t row_count, const ObDirectLoadIndexBlockItem &item);
  void reset();
  int close();
  int64_t get_total_index_size() const { return total_index_size_; }
  TO_STRING_KV(KP(buf_), K(buf_pos_), K(buf_size_), K(item_size_), K(total_index_size_), K(offset_),
               K(tenant_id_));
private:
  int flush_buffer();
  int write_item(const ObDirectLoadIndexBlockItem &item);
  void assign(const int64_t buf_pos, const int64_t buf_cap, char *buf);
private:
  uint64_t tenant_id_;
  int64_t header_length_;
  int64_t buf_pos_;
  int64_t buf_size_;
  int64_t item_size_; //每个索引块对应的索引项个数
  int64_t row_count_; //索引块的总行数
  int64_t total_index_size_; //索引项个数
  uint64_t offset_; //维护索引项的offset
  char *buf_;
  common::ObArenaAllocator allocator_;
  ObDirectLoadTmpFileIOHandle file_io_handle_;
  ObDirectLoadIndexBlockHeader header_;
  bool is_inited_;
  bool is_closed_;
  DISALLOW_COPY_AND_ASSIGN(ObDirectLoadIndexBlockWriter);
};

class ObDirectLoadDataBlockWriter2
{
public:
  ObDirectLoadDataBlockWriter2();
  ~ObDirectLoadDataBlockWriter2();
  int init(uint64_t tenant_id, int64_t buf_size, const ObDirectLoadTmpFileHandle &file_handle,
           ObDirectLoadIndexBlockWriter *index_block_writer);
  int append_row(const ObDirectLoadExternalRow &external_row);
  void reset();
  int close();
  ObDirectLoadDataBlockHeader *get_header() { return &header_; }
  int64_t get_total_row_count() const { return total_row_count_; }
  int64_t get_file_size() const { return file_size_; }
  TO_STRING_KV(KP(buf_), K(buf_pos_), K(buf_size_), K(file_size_), K(file_io_handle_),
               K(tenant_id_), K(total_row_count_), K(row_count_));
private:
  int write_item(const ObDirectLoadExternalRow &external_row);
  int write_large_item(const ObDirectLoadExternalRow &datum_row, int64_t total_size);
  void assign(const int64_t buf_pos, const int64_t buf_cap, char *buf);
  int flush_buffer(int64_t buf_size, char *buf);
private:
  uint64_t tenant_id_;
  int64_t header_length_;
  int64_t buf_pos_;
  int64_t buf_size_;
  int64_t total_row_count_;
  int64_t row_count_;
  uint64_t file_size_;
  ObDirectLoadExternalRow last_row_;
  char *buf_;
  ObDirectLoadTmpFileIOHandle file_io_handle_;
  common::ObArenaAllocator allocator_;
  ObDirectLoadDataBlockHeader header_;
  ObDirectLoadIndexBlockWriter *index_block_writer_;
  bool is_inited_;
  bool is_closed_;
  DISALLOW_COPY_AND_ASSIGN(ObDirectLoadDataBlockWriter2);
};

class ObDirectLoadSSTableBuilder : public ObIDirectLoadPartitionTableBuilder
{
public:
  ObDirectLoadSSTableBuilder()
    : allocator_("TLD_sstablebdr"),
      rowkey_allocator_("TLD_Rowkey"),
      is_closed_(false),
      is_inited_(false)
  {
    allocator_.set_tenant_id(MTL_ID());
    rowkey_allocator_.set_tenant_id(MTL_ID());
  }
  virtual ~ObDirectLoadSSTableBuilder() = default;
  int init(const ObDirectLoadSSTableBuildParam &param);
  int append_row(const common::ObTabletID &tablet_id,
                const table::ObTableLoadSequenceNo &seq_no,
                 const blocksstable::ObDatumRow &datum_row) override;
  int append_row(const ObDirectLoadExternalRow &external_row);
  int close() override;
  int64_t get_row_count() const override { return data_block_writer_.get_total_row_count(); }
  int get_tables(common::ObIArray<ObIDirectLoadPartitionTable *> &table_array,
                 common::ObIAllocator &allocator) override;
private:
  int check_rowkey_order(const blocksstable::ObDatumRowkey &rowkey);
private:
  ObDirectLoadSSTableBuildParam param_;
  ObDirectLoadDataBlockWriter2 data_block_writer_;
  ObDirectLoadIndexBlockWriter index_block_writer_;
  common::ObArenaAllocator allocator_;
  common::ObArenaAllocator rowkey_allocator_;
  blocksstable::ObDatumRowkey start_key_;
  blocksstable::ObDatumRowkey end_key_;
  ObDirectLoadTmpFileHandle data_file_handle_;
  ObDirectLoadTmpFileHandle index_file_handle_;
  ObDirectLoadTmpFileManager *file_mgr_;
  bool is_closed_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObDirectLoadSSTableBuilder);
};

class ObDirectLoadIndexBlockReader
{
public:
  ObDirectLoadIndexBlockReader();
  virtual ~ObDirectLoadIndexBlockReader() = default;
  int init(uint64_t tenant_id, int64_t buf_size, const ObDirectLoadTmpFileHandle &file_handle);
  int change_fragment(const ObDirectLoadTmpFileHandle &file_handle);
  int get_index_info(int64_t idx, ObDirectLoadIndexInfo &info);
  ObDirectLoadIndexBlockHeader *get_header() { return &header_; }
  TO_STRING_KV(KP(buf_), K(buf_size_), K(tenant_id_));
private:
  void assign(const int64_t buf_size, char *buf);
  int read_buffer(int64_t idx);
private:
  uint64_t tenant_id_;
  char *buf_;
  int64_t buf_size_;
  int64_t header_length_;
  int64_t item_size_;
  int64_t index_item_num_per_block_;
  int64_t io_timeout_ms_;
  ObDirectLoadIndexBlockHeader header_;
  common::ObArenaAllocator allocator_;
  ObDirectLoadTmpFileIOHandle file_io_handle_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObDirectLoadIndexBlockReader);
};

class ObDirectLoadDataBlockReader2
{
public:
  ObDirectLoadDataBlockReader2();
  virtual ~ObDirectLoadDataBlockReader2() = default;
  int init(int64_t buf_size, char *buf, int64_t cols_count);
  void assign(const int64_t buf_pos, const int64_t buf_size, char *buf);
  void change_fragment(const ObDirectLoadTmpFileHandle &file_handle);
  void reset();
  int get_next_item(const ObDirectLoadExternalRow *&item);
  ObDirectLoadDataBlockHeader *get_header() { return &header_; }
  TO_STRING_KV(KP(buf_), K(buf_pos_), K(buf_size_));
private:
  char *buf_;
  int64_t buf_pos_;
  int64_t buf_size_;
  ObDirectLoadDataBlockHeader header_;
  ObDirectLoadExternalRow curr_row_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObDirectLoadDataBlockReader2);
};

} // namespace storage
} // namespace oceanbase