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

#ifndef OB_SSTABLE_MULTI_VERSION_ROW_ITERATOR_H_
#define OB_SSTABLE_MULTI_VERSION_ROW_ITERATOR_H_

#include "storage/ob_sstable_row_getter.h"
#include "storage/ob_sstable_row_scanner.h"
#include "storage/ob_sstable_row_multi_getter.h"
#include "storage/ob_sstable_row_multi_scanner.h"

namespace oceanbase {
namespace storage {

class ObSSTableMultiVersionRowIterator : public ObISSTableRowIterator {
public:
  ObSSTableMultiVersionRowIterator();
  virtual ~ObSSTableMultiVersionRowIterator();
  virtual void reset();
  virtual void reuse() override;

protected:
  virtual int inner_open(const ObTableIterParam& iter_param, ObTableAccessContext& access_ctx, ObITable* table,
      const void* query_range) = 0;
  virtual int inner_get_next_row(const ObStoreRow*& row) = 0;
  template <typename T>
  int new_iterator(common::ObArenaAllocator& allocator);
  int get_not_exist_row(const common::ObStoreRowkey& rowkey, const ObStoreRow*& row);

protected:
  const ObTableIterParam* iter_param_;
  ObTableAccessContext* access_ctx_;
  ObSSTable* sstable_;
  union {
    const common::ObExtStoreRowkey* rowkey_;                     // for single get and exist
    const common::ObIArray<common::ObExtStoreRowkey>* rowkeys_;  // for multi get
    const common::ObExtStoreRange* range_;                       // for scan
    const common::ObIArray<common::ObExtStoreRange>* ranges_;    // for multi scan
    const void* query_range_;
  };
  ObSSTableRowIterator* iter_;
  ObStoreRow not_exist_row_;
  char obj_buf_[common::OB_ROW_MAX_COLUMNS_COUNT * sizeof(common::ObObj)];
  int64_t out_cols_cnt_;
  int64_t range_idx_;
  bool read_newest_;
};

class ObSSTableMultiVersionRowScannerBase : public ObSSTableMultiVersionRowIterator {
public:
  ObSSTableMultiVersionRowScannerBase() : skip_range_(), trans_version_range_(), gap_rowkey_()
  {}
  virtual ~ObSSTableMultiVersionRowScannerBase()
  {}
  virtual uint8_t get_iter_flag() override;
  virtual void reset();
  virtual void reuse() override;

protected:
  common::ObExtStoreRange skip_range_;
  common::ObVersionRange trans_version_range_;
  common::ObStoreRowkey gap_rowkey_;
};

class ObSSTableMultiVersionRowGetter : public ObSSTableMultiVersionRowIterator {
public:
  ObSSTableMultiVersionRowGetter()
  {}
  virtual ~ObSSTableMultiVersionRowGetter()
  {}
  virtual void reset() override;
  virtual void reuse() override;

protected:
  virtual int inner_open(const ObTableIterParam& iter_param, ObTableAccessContext& access_ctx, ObITable* table,
      const void* query_range) override;
  virtual int inner_get_next_row(const ObStoreRow*& row) override;

private:
  common::ObExtStoreRange multi_version_range_;
};

class ObSSTableMultiVersionRowScanner : public ObSSTableMultiVersionRowScannerBase {
public:
  ObSSTableMultiVersionRowScanner() : multi_version_range_()
  {}
  virtual ~ObSSTableMultiVersionRowScanner()
  {}
  virtual void reset() override;
  virtual void reuse() override;
  virtual int skip_range(int64_t range_idx, const common::ObStoreRowkey* gap_key, const bool include_gap_key) override;
  virtual int get_gap_end(int64_t& range_idx, const common::ObStoreRowkey*& gap_key, int64_t& gap_size) override;

protected:
  virtual int inner_open(const ObTableIterParam& iter_param, ObTableAccessContext& access_ctx, ObITable* table,
      const void* query_range) override;
  virtual int inner_get_next_row(const ObStoreRow*& row) override;

private:
  common::ObExtStoreRange multi_version_range_;
};

class ObSSTableMultiVersionRowMultiGetter : public ObSSTableMultiVersionRowIterator {
public:
  ObSSTableMultiVersionRowMultiGetter() : multi_version_ranges_(), pending_row_(NULL)
  {}
  virtual ~ObSSTableMultiVersionRowMultiGetter()
  {}
  virtual void reset() override;
  virtual void reuse() override;

protected:
  virtual int inner_open(const ObTableIterParam& iter_param, ObTableAccessContext& access_ctx, ObITable* table,
      const void* query_range) override;
  virtual int inner_get_next_row(const ObStoreRow*& row) override;

private:
  common::ObArray<common::ObExtStoreRange> multi_version_ranges_;
  const ObStoreRow* pending_row_;
};

class ObSSTableMultiVersionRowMultiScanner : public ObSSTableMultiVersionRowScannerBase {
public:
  ObSSTableMultiVersionRowMultiScanner()
      : multi_version_ranges_(),
        pending_row_(NULL),
        last_pending_range_idx_(0),
        reverse_scan_(false),
        orig_ranges_(NULL)
  {}
  virtual ~ObSSTableMultiVersionRowMultiScanner()
  {}
  virtual void reset();
  virtual void reuse() override;
  virtual int skip_range(int64_t range_idx, const ObStoreRowkey* gap_rowkey, const bool include_gap_key) override;
  virtual int get_gap_end(int64_t& range_idx, const common::ObStoreRowkey*& gap_key, int64_t& gap_size) override;

protected:
  virtual int inner_open(const ObTableIterParam& iter_param, ObTableAccessContext& access_ctx, ObITable* table,
      const void* query_range) override;
  virtual int inner_get_next_row(const ObStoreRow*& row) override;

private:
  common::ObArray<common::ObExtStoreRange> multi_version_ranges_;
  const ObStoreRow* pending_row_;
  int64_t last_pending_range_idx_;
  bool reverse_scan_;
  const common::ObIArray<common::ObExtStoreRange>* orig_ranges_;
};

}  // namespace storage
}  // namespace oceanbase

#endif
