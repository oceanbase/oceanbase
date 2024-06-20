/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_STORAGE_OB_TABLET_DDL_KV_MULTI_VERSION_ROW_ITERATOR_H_
#define OB_STORAGE_OB_TABLET_DDL_KV_MULTI_VERSION_ROW_ITERATOR_H_

#include "storage/access/ob_store_row_iterator.h"
#include "storage/ddl/ob_tablet_ddl_kv.h"

namespace oceanbase
{
namespace storage
{
class ObSSTableMultiVersionRowGetter;
class ObSSTableMultiVersionRowMultiGetter;

template <class T>
class ObDDLKVEmptyRowIterator : public ObStoreRowIterator
{
public:
  virtual int init(
      const ObTableIterParam &param,
      ObTableAccessContext &context,
      ObITable *table,
      const void *query_range)
  {
    return OB_SUCCESS;
  }
  virtual int get_next_row(const blocksstable::ObDatumRow *&row) { return OB_ITER_END; }
};

template <>
class ObDDLKVEmptyRowIterator<ObSSTableMultiVersionRowGetter> : public ObStoreRowIterator
{
public:
  ObDDLKVEmptyRowIterator() : range_idx_(0) {}
  virtual ~ObDDLKVEmptyRowIterator() {}
  virtual void reuse()
  {
    range_idx_ = 0;
    not_exist_row_.reset();
    ObStoreRowIterator::reuse();
  }
  virtual void reset()
  {
    range_idx_ = 0;
    not_exist_row_.reset();
    ObStoreRowIterator::reset();
  }
  virtual int init(
      const ObTableIterParam &param,
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
      const ObDatumRowkey *rowkey = static_cast<const ObDatumRowkey *>(query_range);
      const int64_t column_count = param.get_out_col_cnt();
      if (OB_UNLIKELY(rowkey->get_datum_cnt() > column_count)) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "invalid rowkey cnt", K(ret), KPC(rowkey), K(column_count));
      } else if (OB_FAIL(not_exist_row_.init(*context.get_range_allocator(), column_count))) {
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
  virtual int get_next_row(const blocksstable::ObDatumRow *&row)
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

template <>
class ObDDLKVEmptyRowIterator<ObSSTableMultiVersionRowMultiGetter> : public ObStoreRowIterator
{
public:
  ObDDLKVEmptyRowIterator() : range_idx_(0), base_rowkeys_(nullptr) {}
  virtual ~ObDDLKVEmptyRowIterator() {}
  virtual void reuse()
  {
    range_idx_ = 0;
    not_exist_row_.reset();
    base_rowkeys_ = nullptr;
    ObStoreRowIterator::reuse();
  }
  virtual void reset()
  {
    range_idx_ = 0;
    not_exist_row_.reset();
    base_rowkeys_ = nullptr;
    ObStoreRowIterator::reset();
  }
  virtual int init(
      const ObTableIterParam &param,
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
      base_rowkeys_ = reinterpret_cast<const ObIArray<ObDatumRowkey> *>(query_range);
      const int64_t column_count = param.get_out_col_cnt();
      if (OB_FAIL(not_exist_row_.init(*context.get_range_allocator(), column_count))) {
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
  virtual int get_next_row(const blocksstable::ObDatumRow *&row)
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

template <class T>
class ObDDLKVMultiVersionRowIterator : public ObStoreRowIterator
{
private:
  class IteratorInitializer
  {
  public:
    IteratorInitializer(T &iterator,
                        const ObTableIterParam &param,
                        ObTableAccessContext &context,
                        const void *query_range)
      : iterator_(iterator), param_(param), context_(context), query_range_(query_range)
    {
    }
    int operator()(ObDDLMemtable *ddl_memtable)
    {
      int ret = OB_SUCCESS;
      if (OB_ISNULL(ddl_memtable)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected ddl memtable is null", K(ret));
      } else if (OB_FAIL(iterator_.init(param_, context_, ddl_memtable, query_range_))) {
        STORAGE_LOG(WARN, "fail to init iterator", K(ret));
      }
      return ret;
    }
  private:
    T &iterator_;
    const ObTableIterParam &param_;
    ObTableAccessContext &context_;
    const void *query_range_;
  };

public:
  ObDDLKVMultiVersionRowIterator() : is_empty_(false) { type_ = iterator_.get_iter_type(); }
  virtual ~ObDDLKVMultiVersionRowIterator() {}
  virtual void reuse()
  {
    iterator_.reuse();
    empty_iterator_.reuse();
    is_empty_ = false;
    ObStoreRowIterator::reuse();
  }
  virtual void reset()
  {
    iterator_.reset();
    empty_iterator_.reset();
    is_empty_ = false;
    ObStoreRowIterator::reset();
  }
  virtual int init(
      const ObTableIterParam &param,
      ObTableAccessContext &context,
      ObITable *table,
      const void *query_range)
  {
    int ret = common::OB_SUCCESS;
    is_reclaimed_ = false;
    if (OB_ISNULL(query_range) || OB_ISNULL(table)) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid argument", K(ret), KP(query_range), KP(table));
    } else if (!table->is_direct_load_memtable()) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid argument", K(ret), KPC(table));
    } else {
      ObDDLKV *ddl_kv = static_cast<ObDDLKV *>(table);
      IteratorInitializer initializer(iterator_, param, context, query_range);
      if (OB_FAIL(ddl_kv->access_first_ddl_memtable(initializer))) {
        if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
          STORAGE_LOG(WARN, "fail to access first ddl memtable", K(ret));
        } else {
          ret = OB_SUCCESS;
          is_empty_ = true;
          if (OB_FAIL(empty_iterator_.init(param, context, nullptr, query_range))) {
            STORAGE_LOG(WARN, "fail to init empty ddl memtable", K(ret));
          }
        }
      }
    }
    return ret;
  }
  virtual int get_next_row(const blocksstable::ObDatumRow *&row)
  {
    int ret = OB_SUCCESS;
    if (is_empty_) {
      ret = empty_iterator_.get_next_row(row);
    } else {
      ret = iterator_.get_next_row(row);
    }
    return ret;
  }

private:
  T iterator_;
  ObDDLKVEmptyRowIterator<T> empty_iterator_;
  bool is_empty_;
};

} // namespace storage
} // namespace oceanbase

#endif // OB_STORAGE_OB_TABLET_DDL_KV_MULTI_VERSION_ROW_ITERATOR_H_
