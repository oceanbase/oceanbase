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
public:
  ObDDLKVMultiVersionRowIterator() {}
  virtual ~ObDDLKVMultiVersionRowIterator() {}
  virtual void reuse()
  {
    iterator_.reuse();
    ObStoreRowIterator::reuse();
  }
  virtual void reset()
  {
    iterator_.reset();
    ObStoreRowIterator::reset();
  }
  // used for global query iterator pool, prepare for returning to pool
  virtual void reclaim()
  {
    iterator_.reclaim();
    ObStoreRowIterator::reclaim();
  }
  virtual int init(
      const ObTableIterParam &param,
      ObTableAccessContext &context,
      ObITable *table,
      const void *query_range)
  {
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(query_range) || OB_ISNULL(table)) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid argument", K(ret), KP(query_range), KP(table));
    } else if (!table->is_direct_load_memtable()) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid argument", K(ret), KPC(table));
    } else {
      ObDDLMemtable *ddl_memtable = nullptr;
      if (OB_FAIL((static_cast<ObDDLKV *>(table)->get_first_ddl_memtable(ddl_memtable)))) {
        STORAGE_LOG(WARN, "fail to get ddl memtable", K(ret));
      } else if (OB_FAIL(iterator_.init(param, context, ddl_memtable, query_range))) {
        STORAGE_LOG(WARN, "fail to init", K(ret));
      } else {
        type_ = iterator_.get_iter_type();
        is_sstable_iter_ = false;
        is_reclaimed_ = false;
      }
    }
    return ret;
  }
  virtual int set_ignore_shadow_row() { return iterator_.set_ignore_shadow_row(); }
  virtual bool can_blockscan() const { return iterator_.can_blockscan(); }
  virtual bool can_batch_scan() const { return iterator_.can_batch_scan(); }
  virtual int get_next_row(const blocksstable::ObDatumRow *&row)
  {
    return iterator_.get_next_row(row);
  }

private:
  T iterator_;
};

} // namespace storage
} // namespace oceanbase

#endif // OB_STORAGE_OB_TABLET_DDL_KV_MULTI_VERSION_ROW_ITERATOR_H_
