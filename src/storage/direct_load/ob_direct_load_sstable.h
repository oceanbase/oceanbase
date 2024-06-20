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

#include "storage/access/ob_store_row_iterator.h"
#include "storage/access/ob_table_access_context.h"
#include "storage/access/ob_table_access_param.h"
#include "storage/direct_load/ob_direct_load_i_table.h"
#include "storage/ob_i_store.h"
#include "ob_direct_load_tmp_file.h"
#include "storage/direct_load/ob_direct_load_rowkey_iterator.h"
#include "storage/direct_load/ob_direct_load_external_row.h"
#include "storage/direct_load/ob_direct_load_table_data_desc.h"

namespace oceanbase
{
namespace storage
{
static const int64_t DIRECT_LOAD_DEFAULT_SSTABLE_DATA_BLOCK_SIZE = 16 * 1024LL; // data_buffer is 16K;
static const int64_t DIRECT_LOAD_DEFAULT_SSTABLE_INDEX_BLOCK_SIZE = 4 * 1024LL; // index_buffer is 4K;

class ObDirectLoadIndexBlockMetaIterator;
class ObDirectLoadSSTableScanner;

struct ObDirectLoadSSTableMeta
{
public:
  ObDirectLoadSSTableMeta()
    : rowkey_column_count_(0),
      column_count_(0),
      index_block_size_(0),
      data_block_size_(0),
      index_item_count_(0),
      index_block_count_(0),
      row_count_(0)
  {
  }
  void reset();
  TO_STRING_KV(K_(tablet_id), K_(rowkey_column_count), K_(column_count), K_(index_block_size),
               K_(data_block_size), K(index_item_count_), K(index_block_count_), K(row_count_));
public:
  common::ObTabletID tablet_id_;
  int64_t rowkey_column_count_;
  int64_t column_count_;
  int64_t index_block_size_; // 索引块大小
  int64_t data_block_size_; // 数据块大小
  int64_t index_item_count_; //索引项个数
  int64_t index_block_count_; //索引块个数
  int64_t row_count_; // row的个数
};

class ObDirectLoadSSTableFragmentMeta
{
public:
  ObDirectLoadSSTableFragmentMeta();
  ~ObDirectLoadSSTableFragmentMeta();
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(index_item_count), K_(index_block_count), K_(row_count), K_(occupy_size));
public:
  int64_t index_item_count_; //索引项个数
  int64_t index_block_count_; //索引块个数
  int64_t row_count_; //行的个数
  int64_t occupy_size_; //文件的大小
};

class ObDirectLoadSSTableFragment
{
public:
  ObDirectLoadSSTableFragment();
  ~ObDirectLoadSSTableFragment();
  void reset();
  bool is_valid() const;
  int assign(const ObDirectLoadSSTableFragment &other);
  TO_STRING_KV(K_(meta), K_(index_file_handle), K_(data_file_handle));
public:
  ObDirectLoadSSTableFragmentMeta meta_;
  ObDirectLoadTmpFileHandle index_file_handle_;
  ObDirectLoadTmpFileHandle data_file_handle_;
};

struct ObDirectLoadSSTableCreateParam
{
public:
  ObDirectLoadSSTableCreateParam()
    : rowkey_column_count_(0),
      column_count_(0),
      index_block_size_(0),
      data_block_size_(0),
      index_item_count_(0),
      index_block_count_(0),
      row_count_(0)
  {
    fragments_.set_tenant_id(MTL_ID());
  }
  bool is_valid() const
  {
    return tablet_id_.is_valid() &&
           rowkey_column_count_ > 0 &&
           column_count_ > 0 &&
           rowkey_column_count_ <= column_count_ &&
           index_block_size_ > 0 && index_block_size_ % DIO_ALIGN_SIZE == 0 &&
           data_block_size_ > 0 && data_block_size_ % DIO_ALIGN_SIZE == 0 &&
           index_item_count_ > 0 &&
           index_block_count_ > 0 &&
           row_count_ > 0 &&
           start_key_.is_valid() &&
           end_key_.is_valid() &&
           !fragments_.empty();
  }
  TO_STRING_KV(K_(tablet_id), K_(rowkey_column_count), K_(column_count), K_(index_block_size),
               K_(data_block_size), K_(index_item_count), K_(index_block_count), K_(row_count),
               K_(start_key), K_(end_key), K_(fragments));
public:
  common::ObTabletID tablet_id_;
  int64_t rowkey_column_count_;
  int64_t column_count_; // 写到sstable的列数目
  int64_t index_block_size_; // 索引块大小
  int64_t data_block_size_; // 数据块大小
  int64_t index_item_count_; // 索引项个数
  int64_t index_block_count_; //索引块个数
  int64_t row_count_; // row的行数
  blocksstable::ObDatumRowkey start_key_;
  blocksstable::ObDatumRowkey end_key_;
  common::ObArray<ObDirectLoadSSTableFragment> fragments_;
};

class ObDirectLoadSSTable : public ObIDirectLoadPartitionTable
{
public:
  ObDirectLoadSSTable();
  virtual ~ObDirectLoadSSTable();
  void reset();
  int init(ObDirectLoadSSTableCreateParam &param);
  int scan_index_block_meta(ObIAllocator &allocator,
                            ObDirectLoadIndexBlockMetaIterator *&meta_iter);
  // for sstable scan merge
  int scan(const ObDirectLoadTableDataDesc &table_data_desc,
           const blocksstable::ObDatumRange &key_range,
           const blocksstable::ObStorageDatumUtils *datum_utils,
           common::ObIAllocator &allocator,
           ObDirectLoadSSTableScanner *&scanner);
  int64_t get_row_count() const override { return meta_.row_count_; }
  const common::ObTabletID &get_tablet_id() const override { return meta_.tablet_id_; }
  bool is_empty() const { return 0 == meta_.row_count_; }
  bool is_valid() const override { return is_inited_; }
  void release_data() override;
  int copy(const ObDirectLoadSSTable &other);
  const common::ObArray<ObDirectLoadSSTableFragment> &get_fragment_array() const
  {
    return fragments_;
  }
  const ObDirectLoadSSTableMeta &get_meta() const { return meta_; }
  const blocksstable::ObDatumRowkey &get_start_key() const { return start_key_; }
  const blocksstable::ObDatumRowkey &get_end_key() const { return end_key_; }
  TO_STRING_KV(K_(meta), K_(start_key), K_(end_key), K(fragments_));
private:
  common::ObArenaAllocator allocator_;
  ObDirectLoadSSTableMeta meta_;
  blocksstable::ObDatumRowkey start_key_;
  blocksstable::ObDatumRowkey end_key_;
  common::ObArray<ObDirectLoadSSTableFragment> fragments_;
  bool is_inited_;
};

class ObDirectLoadSSTableFragmentOperator
{
public:
  ObDirectLoadSSTableFragmentOperator() : sstable_(nullptr), is_inited_(false) {}
  int init(ObDirectLoadSSTable *sstable);
  int get_fragment(const int64_t idx, ObDirectLoadSSTableFragment &fragment);
  int get_fragment_item_idx(int64_t idx, int64_t &locate_idx, int64_t &new_idx);
  int get_fragment_block_idx(int64_t idx, int64_t &locate_idx, int64_t &new_idx);
  int get_next_fragment(int64_t &curr_fragment_idx, ObDirectLoadSSTableFragment &fragment);
private:
  ObDirectLoadSSTable *sstable_;
  bool is_inited_;
};


} // namespace storage
} // namespace oceanbase