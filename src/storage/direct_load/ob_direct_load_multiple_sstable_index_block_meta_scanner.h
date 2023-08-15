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

#include "storage/direct_load/ob_direct_load_multiple_datum_row.h"
#include "storage/direct_load/ob_direct_load_multiple_datum_rowkey.h"
#include "storage/direct_load/ob_direct_load_multiple_sstable.h"
#include "storage/direct_load/ob_direct_load_rowkey_iterator.h"
#include "storage/direct_load/ob_direct_load_sstable_data_block_reader.h"
#include "storage/direct_load/ob_direct_load_sstable_index_block_reader.h"
#include "storage/direct_load/ob_direct_load_table_data_desc.h"

namespace oceanbase
{
namespace storage
{
class ObDirectLoadMultipleSSTable;
class ObDirectLoadMultipleDatumRange;

struct ObDirectLoadMultipleSSTableIndexBlockMeta
{
public:
  ObDirectLoadMultipleSSTableIndexBlockMeta();
  ~ObDirectLoadMultipleSSTableIndexBlockMeta();
  TO_STRING_KV(K_(count), K_(end_key));
public:
  int64_t count_; // count of entries
  ObDirectLoadMultipleDatumRowkey end_key_;
};

class ObDirectLoadMultipleSSTableIndexBlockMetaIterator
{
public:
  virtual ~ObDirectLoadMultipleSSTableIndexBlockMetaIterator() = default;
  virtual int get_next_meta(ObDirectLoadMultipleSSTableIndexBlockMeta &index_block_meta) = 0;
};

class ObDirectLoadMultipleSSTableIndexBlockMetaWholeScanner
  : public ObDirectLoadMultipleSSTableIndexBlockMetaIterator
{
  typedef ObDirectLoadMultipleDatumRow RowType;
  typedef ObDirectLoadSSTableIndexBlockReader IndexBlockReader;
  typedef ObDirectLoadSSTableDataBlockReader<RowType> DataBlockReader;
public:
  ObDirectLoadMultipleSSTableIndexBlockMetaWholeScanner();
  virtual ~ObDirectLoadMultipleSSTableIndexBlockMetaWholeScanner();
  int init(ObDirectLoadMultipleSSTable *sstable, const ObDirectLoadTableDataDesc &table_data_desc);
  int get_next_meta(ObDirectLoadMultipleSSTableIndexBlockMeta &index_block_meta) override;
private:
  ObDirectLoadMultipleSSTable *sstable_;
  IndexBlockReader index_block_reader_;
  DataBlockReader data_block_reader_;
  ObDirectLoadMultipleSSTable::IndexBlockIterator current_index_block_iter_;
  bool is_inited_;
};

class ObDirectLoadMultipleSSTableIndexBlockMetaTabletWholeScanner
  : public ObDirectLoadMultipleSSTableIndexBlockMetaIterator
{
  typedef ObDirectLoadMultipleDatumRow RowType;
  typedef ObDirectLoadSSTableIndexBlockReader IndexBlockReader;
  typedef ObDirectLoadSSTableDataBlockReader<RowType> DataBlockReader;

public:
  ObDirectLoadMultipleSSTableIndexBlockMetaTabletWholeScanner();
  virtual ~ObDirectLoadMultipleSSTableIndexBlockMetaTabletWholeScanner();
  int init(ObDirectLoadMultipleSSTable *sstable, const common::ObTabletID &tablet_id,
           const ObDirectLoadTableDataDesc &table_data_desc,
           const blocksstable::ObStorageDatumUtils *datum_utils);
  int get_next_meta(ObDirectLoadMultipleSSTableIndexBlockMeta &index_block_meta) override;
private:
  int locate_left_border(IndexBlockReader &index_block_reader, DataBlockReader &data_block_reader);
private:
  ObDirectLoadMultipleSSTable *sstable_;
  common::ObTabletID tablet_id_;
  const blocksstable::ObStorageDatumUtils *datum_utils_;
  IndexBlockReader index_block_reader_;
  DataBlockReader data_block_reader_;
  ObDirectLoadMultipleSSTable::IndexBlockIterator current_index_block_iter_;
  bool is_iter_end_;
  bool is_inited_;
};

class ObDirectLoadMultipleSSTableIndexBlockEndKeyIterator
  : public ObIDirectLoadMultipleDatumRowkeyIterator
{
public:
  ObDirectLoadMultipleSSTableIndexBlockEndKeyIterator();
  virtual ~ObDirectLoadMultipleSSTableIndexBlockEndKeyIterator();
  int init(ObDirectLoadMultipleSSTableIndexBlockMetaIterator *index_block_meta_iter);
  int get_next_rowkey(const ObDirectLoadMultipleDatumRowkey *&rowkey) override;
private:
  ObDirectLoadMultipleSSTableIndexBlockMetaIterator *index_block_meta_iter_;
  ObDirectLoadMultipleSSTableIndexBlockMeta index_block_meta_;
  bool is_inited_;
};

class ObDirectLoadMultipleSSTableIndexBlockTabletEndKeyIterator
  : public ObIDirectLoadDatumRowkeyIterator
{
public:
  ObDirectLoadMultipleSSTableIndexBlockTabletEndKeyIterator();
  virtual ~ObDirectLoadMultipleSSTableIndexBlockTabletEndKeyIterator();
  int init(ObDirectLoadMultipleSSTableIndexBlockMetaIterator *index_block_meta_iter);
  int get_next_rowkey(const blocksstable::ObDatumRowkey *&rowkey) override;
private:
  ObDirectLoadMultipleSSTableIndexBlockMetaIterator *index_block_meta_iter_;
  blocksstable::ObDatumRowkey rowkey_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
