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
#include "storage/direct_load/ob_direct_load_multiple_datum_rowkey.h"
#include "storage/direct_load/ob_direct_load_sstable_index_block_iterator.h"
#include "storage/direct_load/ob_direct_load_sstable_index_entry_iterator.h"
#include "storage/direct_load/ob_direct_load_tmp_file.h"

namespace oceanbase
{
namespace storage
{
class ObDirectLoadTableDataDesc;
class ObDirectLoadMultipleDatumRange;
class ObDirectLoadMultipleSSTableScanner;
class ObDirectLoadMultipleSSTableIndexBlockMetaIterator;

struct ObDirectLoadMultipleSSTableFragment
{
public:
  ObDirectLoadMultipleSSTableFragment();
  ~ObDirectLoadMultipleSSTableFragment();
  int assign(const ObDirectLoadMultipleSSTableFragment &other);
  TO_STRING_KV(K_(index_block_count), K_(data_block_count), K_(index_file_size), K_(data_file_size),
               K_(row_count), K_(max_data_block_size), K_(index_file_handle), K_(data_file_handle));
public:
  int64_t index_block_count_;
  int64_t data_block_count_;
  int64_t index_file_size_;
  int64_t data_file_size_;
  int64_t row_count_;
  int64_t max_data_block_size_;
  ObDirectLoadTmpFileHandle index_file_handle_;
  ObDirectLoadTmpFileHandle data_file_handle_;
};

struct ObDirectLoadMultipleSSTableCreateParam
{
public:
  ObDirectLoadMultipleSSTableCreateParam();
  ~ObDirectLoadMultipleSSTableCreateParam();
  bool is_valid() const;
  TO_STRING_KV(K_(rowkey_column_num), K_(column_count), K_(index_block_size), K_(data_block_size),
               K_(index_block_count), K_(data_block_count), K_(row_count), K_(max_data_block_size),
               K_(start_key), K_(end_key), K_(fragments));
public:
  int64_t rowkey_column_num_;
  int64_t column_count_;
  int64_t index_block_size_;
  int64_t data_block_size_;
  int64_t index_block_count_;
  int64_t data_block_count_;
  int64_t row_count_;
  int64_t max_data_block_size_;
  ObDirectLoadMultipleDatumRowkey start_key_;
  ObDirectLoadMultipleDatumRowkey end_key_;
  common::ObArray<ObDirectLoadMultipleSSTableFragment> fragments_;
};

struct ObDirectLoadMultipleSSTableMeta
{
public:
  ObDirectLoadMultipleSSTableMeta();
  ~ObDirectLoadMultipleSSTableMeta();
  void reset();
  TO_STRING_KV(K_(rowkey_column_num), K_(column_count), K_(index_block_size), K_(data_block_size),
               K_(index_block_count), K_(data_block_count), K_(row_count), K_(max_data_block_size));
public:
  int64_t rowkey_column_num_;
  int64_t column_count_;
  int64_t index_block_size_;
  int64_t data_block_size_;
  int64_t index_block_count_;
  int64_t data_block_count_; // same as index entry count
  int64_t row_count_;
  int64_t max_data_block_size_;
};

class ObDirectLoadMultipleSSTable : public ObIDirectLoadPartitionTable
{
public:
  typedef ObDirectLoadMultipleSSTableFragment Fragment;
  typedef ObDirectLoadSSTableIndexBlockIterator<ObDirectLoadMultipleSSTable> IndexBlockIterator;
  typedef ObDirectLoadSSTableIndexEntryIterator<ObDirectLoadMultipleSSTable> IndexEntryIterator;
  ObDirectLoadMultipleSSTable();
  virtual ~ObDirectLoadMultipleSSTable();
  void reset();
  int init(const ObDirectLoadMultipleSSTableCreateParam &param);
  const common::ObTabletID &get_tablet_id() const override { return tablet_id_; }
  int64_t get_row_count() const override { return meta_.row_count_; }
  bool is_valid() const override { return is_inited_; }
  int copy(const ObDirectLoadMultipleSSTable &other);
  bool is_empty() const { return 0 == meta_.row_count_; }
  const ObDirectLoadMultipleSSTableMeta &get_meta() const { return meta_; }
  const ObDirectLoadMultipleDatumRowkey &get_start_key() const { return start_key_; }
  const ObDirectLoadMultipleDatumRowkey &get_end_key() const { return end_key_; }
  const common::ObIArray<ObDirectLoadMultipleSSTableFragment> &get_fragments() const
  {
    return fragments_;
  }
  IndexBlockIterator index_block_begin();
  IndexBlockIterator index_block_end();
  IndexEntryIterator index_entry_begin();
  IndexEntryIterator index_entry_end();
  int scan(const ObDirectLoadTableDataDesc &table_data_desc,
           const ObDirectLoadMultipleDatumRange &range,
           const blocksstable::ObStorageDatumUtils *datum_utils, common::ObIAllocator &allocator,
           ObDirectLoadMultipleSSTableScanner *&scanner);
  int scan_whole_index_block_meta(const ObDirectLoadTableDataDesc &table_data_desc,
                                  common::ObIAllocator &allocator,
                                  ObDirectLoadMultipleSSTableIndexBlockMetaIterator *&meta_iter);
  int scan_tablet_whole_index_block_meta(
    const common::ObTabletID &tablet_id,
    const ObDirectLoadTableDataDesc &table_data_desc,
    const blocksstable::ObStorageDatumUtils *datum_utils,
    common::ObIAllocator &allocator,
    ObDirectLoadMultipleSSTableIndexBlockMetaIterator *&meta_iter);
  TO_STRING_KV(K_(meta), K_(start_key), K_(end_key), K_(fragments));
private:
  common::ObArenaAllocator allocator_;
  common::ObTabletID tablet_id_; // invalid
  ObDirectLoadMultipleSSTableMeta meta_;
  ObDirectLoadMultipleDatumRowkey start_key_;
  ObDirectLoadMultipleDatumRowkey end_key_;
  common::ObArray<ObDirectLoadMultipleSSTableFragment> fragments_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
