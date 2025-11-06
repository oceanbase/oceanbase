/**
 * Copyright (c) 2025 OceanBase
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
#include "storage/blocksstable/ob_datum_rowkey.h"

namespace oceanbase
{
namespace storage
{
class ObSSTableEmptyRowGetter final : public ObStoreRowIterator
{
public:
  ObSSTableEmptyRowGetter() : range_idx_(0) {}
  virtual ~ObSSTableEmptyRowGetter() = default;
  void reuse() override
  {
    range_idx_ = 0;
    not_exist_row_.reset();
    ObStoreRowIterator::reuse();
  }
  void reset() override
  {
    range_idx_ = 0;
    not_exist_row_.reset();
    ObStoreRowIterator::reset();
  }
  int init(const ObTableIterParam &param,
           ObTableAccessContext &context,
           ObITable *table,
           const void *query_range)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(not_exist_row_.is_valid())) {
      ret = OB_INIT_TWICE;
      STORAGE_LOG(WARN, "init twice", K(ret), KP(this), K(not_exist_row_));
    } else if (OB_ISNULL(query_range)) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid args", K(ret), KP(query_range));
    } else {
      const blocksstable::ObDatumRowkey *rowkey =
        static_cast<const blocksstable::ObDatumRowkey *>(query_range);
      const int64_t column_count = param.get_out_col_cnt();
      if (OB_UNLIKELY(rowkey->get_datum_cnt() > column_count)) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "invalid rowkey cnt", K(ret), KPC(rowkey), K(column_count));
      } else if (OB_FAIL(not_exist_row_.init(*context.allocator_, column_count))) {
        STORAGE_LOG(WARN, "fail to init datum row", K(ret));
      } else {
        not_exist_row_.row_flag_.reset();
        not_exist_row_.row_flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);
        for (int64_t i = 0; i < not_exist_row_.get_column_count(); ++i) {
          if (i < rowkey->get_datum_cnt()) {
            not_exist_row_.storage_datums_[i] = rowkey->datums_[i];
          } else {
            not_exist_row_.storage_datums_[i].set_nop();
          }
        }
      }
    }
    return ret;
  }
  int get_next_row(const blocksstable::ObDatumRow *&row) override
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(!not_exist_row_.is_valid())) {
      ret = OB_NOT_INIT;
      STORAGE_LOG(WARN, "not init", K(ret), KP(this));
    } else if (range_idx_ == 0) {
      ++range_idx_;
      row = &not_exist_row_;
    } else {
      ret = OB_ITER_END;
    }
    return ret;
  }

private:
  int64_t range_idx_;
  blocksstable::ObDatumRow not_exist_row_;
};

class ObSSTableEmptyRowMultiGetter final : public ObStoreRowIterator
{
public:
  ObSSTableEmptyRowMultiGetter() : range_idx_(0), base_rowkeys_(nullptr) {}
  virtual ~ObSSTableEmptyRowMultiGetter() = default;
  void reuse() override
  {
    range_idx_ = 0;
    not_exist_row_.reset();
    base_rowkeys_ = nullptr;
    ObStoreRowIterator::reuse();
  }
  void reset() override
  {
    range_idx_ = 0;
    not_exist_row_.reset();
    base_rowkeys_ = nullptr;
    ObStoreRowIterator::reset();
  }
  int init(const ObTableIterParam &param,
           ObTableAccessContext &context,
           ObITable *table,
           const void *query_range) override
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(not_exist_row_.is_valid())) {
      ret = OB_INIT_TWICE;
      STORAGE_LOG(WARN, "init twice", K(ret), KP(this), K(not_exist_row_));
    } else if (OB_ISNULL(query_range)) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid args", K(ret), KP(query_range));
    } else {
      base_rowkeys_ = reinterpret_cast<const ObIArray<ObDatumRowkey> *>(query_range);
      const int64_t column_count = param.get_out_col_cnt();
      if (OB_FAIL(not_exist_row_.init(*context.allocator_, column_count))) {
        STORAGE_LOG(WARN, "fail to init datum row", K(ret));
      } else {
        not_exist_row_.row_flag_.reset();
        not_exist_row_.row_flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);
        for (int64_t i = 0; i < not_exist_row_.get_column_count(); ++i) {
          not_exist_row_.storage_datums_[i].set_nop();
        }
      }
    }
    return ret;
  }
  int get_next_row(const blocksstable::ObDatumRow *&row) override
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(!not_exist_row_.is_valid())) {
      ret = OB_NOT_INIT;
      STORAGE_LOG(WARN, "not init", K(ret), KP(this));
    } else if (range_idx_ < base_rowkeys_->count()) {
      const blocksstable::ObDatumRowkey &rowkey = base_rowkeys_->at(range_idx_);
      if (OB_UNLIKELY(rowkey.get_datum_cnt() > not_exist_row_.get_column_count())) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "invalid rowkey cnt", K(ret), K(rowkey),
                    K(not_exist_row_.get_column_count()));
      } else {
        for (int64_t i = 0; i < rowkey.get_datum_cnt(); ++i) {
          not_exist_row_.storage_datums_[i] = rowkey.datums_[i];
        }
        ++range_idx_;
        row = &not_exist_row_;
      }
    } else {
      ret = OB_ITER_END;
    }
    return ret;
  }

private:
  int64_t range_idx_;
  blocksstable::ObDatumRow not_exist_row_;
  const common::ObIArray<blocksstable::ObDatumRowkey> *base_rowkeys_;
};

class ObSSTableEmptyRowScanner final : public ObStoreRowIterator
{
public:
  ObSSTableEmptyRowScanner() = default;
  virtual ~ObSSTableEmptyRowScanner() = default;
  int init(const ObTableIterParam &param,
           ObTableAccessContext &context,
           ObITable *table,
           const void *query_range) override
  {
    return OB_SUCCESS;
  }
  int get_next_row(const blocksstable::ObDatumRow *&row) override { return OB_ITER_END; }
};

} // namespace storage
} // namespace oceanbase