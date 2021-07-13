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

#ifndef OCEANBASE_MEMTABLE_OB_MEMTABLE_SPARSE_ITERATOR_
#define OCEANBASE_MEMTABLE_OB_MEMTABLE_SPARSE_ITERATOR_

#include "storage/memtable/mvcc/ob_crtp_util.h"
#include "storage/memtable/ob_memtable_iterator.h"

namespace oceanbase {
namespace memtable {
class ObMemtableMultiVersionScanIterator;

class ObMemtableMultiVersionScanSparseIterator : public ObMemtableMultiVersionScanIterator {
public:
  ObMemtableMultiVersionScanSparseIterator();
  virtual ~ObMemtableMultiVersionScanSparseIterator();

protected:
  virtual void row_reset() override;
  virtual int init_next_value_iter() override;
  virtual int init_row_cells(ObIAllocator* allocator) override;

protected:
  // iterate row
  virtual int iterate_compacted_row(const common::ObStoreRowkey& key, storage::ObStoreRow& row) override;
  virtual int iterate_uncommitted_row(const ObStoreRowkey& key, storage::ObStoreRow& row) override;
  virtual int iterate_multi_version_row(const ObStoreRowkey& rowkey, storage::ObStoreRow& row) override;

private:
  // means SCANITER
  static const uint64_t VALID_MAGIC_NUM = 0x524554494e414353;
  DISALLOW_COPY_AND_ASSIGN(ObMemtableMultiVersionScanSparseIterator);
};

class ObReadSparseRow {
  DEFINE_ALLOCATOR_WRAPPER
public:
  static int iterate_sparse_row_key(const common::ObIArray<share::schema::ObColDesc>& columns,
      const common::ObStoreRowkey& rowkey, storage::ObStoreRow& row);
  static int prepare_sparse_rowkey_position(const int64_t rowkey_length, storage::ObStoreRow& row);
};

}  // namespace memtable
}  // namespace oceanbase

#endif
