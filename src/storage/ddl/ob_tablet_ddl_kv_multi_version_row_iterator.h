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
        STORAGE_LOG(WARN, "fail to init", K(ret));
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
    is_empty_ = false;
    ObStoreRowIterator::reuse();
  }
  virtual void reset()
  {
    iterator_.reset();
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
        }
      }
    }
    return ret;
  }
  virtual int get_next_row(const blocksstable::ObDatumRow *&row)
  {
    int ret = OB_SUCCESS;
    if (is_empty_) {
      ret = OB_ITER_END;
    } else {
      ret = iterator_.get_next_row(row);
    }
    return ret;
  }

private:
  T iterator_;
  bool is_empty_;
};

} // namespace storage
} // namespace oceanbase

#endif // OB_STORAGE_OB_TABLET_DDL_KV_MULTI_VERSION_ROW_ITERATOR_H_
