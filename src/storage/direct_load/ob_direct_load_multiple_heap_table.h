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

#include "storage/direct_load/ob_direct_load_i_table.h"
#include "storage/direct_load/ob_direct_load_multiple_heap_table_index_entry_iterator.h"
#include "storage/direct_load/ob_direct_load_tmp_file.h"

namespace oceanbase
{
namespace storage
{
class ObDirectLoadTableDataDesc;
class ObDirectLoadMultipleHeapTableTabletIndex;

struct ObDirectLoadMultipleHeapTableDataFragment
{
public:
  ObDirectLoadMultipleHeapTableDataFragment();
  ~ObDirectLoadMultipleHeapTableDataFragment();
  int assign(const ObDirectLoadMultipleHeapTableDataFragment &other);
  TO_STRING_KV(K_(block_count), K_(file_size), K_(row_count), K_(max_block_size), K_(file_handle));
public:
  int64_t block_count_;
  int64_t file_size_;
  int64_t row_count_;
  int64_t max_block_size_;
  ObDirectLoadTmpFileHandle file_handle_;
};

struct ObDirectLoadMultipleHeapTableCreateParam
{
public:
  ObDirectLoadMultipleHeapTableCreateParam();
  ~ObDirectLoadMultipleHeapTableCreateParam();
  bool is_valid() const;
  TO_STRING_KV(K_(column_count), K_(index_block_size), K_(data_block_size), K_(index_block_count),
               K_(data_block_count), K_(index_file_size), K_(data_file_size), K_(index_entry_count),
               K_(row_count), K_(max_data_block_size), K_(index_file_handle), K_(data_fragments));
public:
  int64_t column_count_;
  int64_t index_block_size_;
  int64_t data_block_size_;
  int64_t index_block_count_;
  int64_t data_block_count_;
  int64_t index_file_size_;
  int64_t data_file_size_;
  int64_t index_entry_count_;
  int64_t row_count_;
  int64_t max_data_block_size_;
  ObDirectLoadTmpFileHandle index_file_handle_;
  common::ObArray<ObDirectLoadMultipleHeapTableDataFragment> data_fragments_;
};

struct ObDirectLoadMultipleHeapTableMeta
{
public:
  ObDirectLoadMultipleHeapTableMeta();
  ~ObDirectLoadMultipleHeapTableMeta();
  void reset();
  TO_STRING_KV(K_(column_count), K_(index_block_size), K_(data_block_size), K_(index_block_count),
               K_(data_block_count), K_(index_file_size), K_(data_file_size), K_(index_entry_count),
               K_(row_count), K_(max_data_block_size));
public:
  int64_t column_count_;
  int64_t index_block_size_;
  int64_t data_block_size_;
  int64_t index_block_count_;
  int64_t data_block_count_;
  int64_t index_file_size_;
  int64_t data_file_size_;
  int64_t index_entry_count_;
  int64_t row_count_;
  int64_t max_data_block_size_;
};

class ObDirectLoadMultipleHeapTable : public ObIDirectLoadPartitionTable
{
public:
  typedef ObDirectLoadMultipleHeapTableIndexEntryIterator<ObDirectLoadMultipleHeapTable>
    IndexEntryIterator;
  ObDirectLoadMultipleHeapTable();
  virtual ~ObDirectLoadMultipleHeapTable();
  void reset();
  int init(const ObDirectLoadMultipleHeapTableCreateParam &param);
  const common::ObTabletID &get_tablet_id() const override { return tablet_id_; }
  int64_t get_row_count() const override { return meta_.row_count_; }
  bool is_valid() const override { return is_inited_; }
  int copy(const ObDirectLoadMultipleHeapTable &other);
  const ObDirectLoadMultipleHeapTableMeta &get_meta() const { return meta_; }
  const ObDirectLoadTmpFileHandle &get_index_file_handle() const { return index_file_handle_; }
  const common::ObIArray<ObDirectLoadMultipleHeapTableDataFragment> &get_data_fragments() const
  {
    return data_fragments_;
  }
  IndexEntryIterator index_entry_begin();
  IndexEntryIterator index_entry_end();
  int get_tablet_row_count(const common::ObTabletID &tablet_id,
                           const ObDirectLoadTableDataDesc &table_data_desc,
                           int64_t &row_count);
  TO_STRING_KV(K_(meta), K_(index_file_handle), K_(data_fragments));
private:
  common::ObTabletID tablet_id_; // invalid
  ObDirectLoadMultipleHeapTableMeta meta_;
  ObDirectLoadTmpFileHandle index_file_handle_;
  common::ObArray<ObDirectLoadMultipleHeapTableDataFragment> data_fragments_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
