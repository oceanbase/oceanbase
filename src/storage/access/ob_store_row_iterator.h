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

#ifndef OB_STORAGE_OB_STORE_ROW_ITERATOR_H_
#define OB_STORAGE_OB_STORE_ROW_ITERATOR_H_

#include "storage/ob_i_store.h"
#include "storage/ob_table_store_stat_mgr.h"
#include "ob_block_row_store.h"

namespace oceanbase
{
namespace blocksstable
{
class ObSSTable;
}
namespace storage
{

class ObITable;
class ObBlockRowStore;
struct ObSSTableReadHandle;
struct ObTableIterParam;

class ObIStoreRowIterator
{
public:
  ObIStoreRowIterator() {}
  virtual ~ObIStoreRowIterator() {}
  virtual int get_next_row(const blocksstable::ObDatumRow *&row) = 0;
  //TODO removed by hanhui
  virtual int get_next_row(const ObStoreRow *&row)
  { return OB_NOT_SUPPORTED; }
};

class ObStoreRowIterator : public ObIStoreRowIterator
{
public:
  enum IteratorType {
    IteratorReserved = 0,
    IteratorSingleGet = 1,
    IteratorMultiGet = 2,
    IteratorScan = 3,
    IteratorMultiScan = 4,
    IteratorRowLockCheck = 5,
    IteratorInvalidType
  };
  ObStoreRowIterator() :
      type_(IteratorReserved),
      is_sstable_iter_(false),
      block_row_store_(nullptr)
  {};
  virtual ~ObStoreRowIterator();
  virtual void reuse();
  virtual void reset();
  virtual int init(
      const ObTableIterParam &param,
      ObTableAccessContext &context,
      ObITable *table,
      const void *query_range);
  virtual int get_next_row_ext(const blocksstable::ObDatumRow *&row, uint8_t& flag) {
    int ret = get_next_row(row);
    flag = get_iter_flag();
    return ret;
  }
  virtual uint8_t get_iter_flag() { return 0; }
  virtual bool is_sstable_iter() const { return is_sstable_iter_; }
  virtual int refresh_blockscan_checker(const blocksstable::ObDatumRowkey &rowkey)
  {
    UNUSED(rowkey);
    return OB_NOT_SUPPORTED;
  }
  bool can_blockscan() const
  {
    return (IteratorScan == type_ || IteratorMultiScan == type_) && is_sstable_iter_ &&
        nullptr != block_row_store_ && block_row_store_->can_blockscan();
  }
  bool filter_applied() const
  {
    return (IteratorScan == type_ || IteratorMultiScan == type_) && is_sstable_iter_ &&
        nullptr != block_row_store_ && block_row_store_->filter_applied();
  }
  virtual int get_next_row(const blocksstable::ObDatumRow *&row);
  virtual int get_next_rows()
  {
    return OB_SUCCESS;
  }
  virtual int prefetch_read_handle(ObSSTableReadHandle &read_handle)
  {
    UNUSED(read_handle);
    return OB_NOT_IMPLEMENT;
  }

  VIRTUAL_TO_STRING_KV(K_(type), K_(is_sstable_iter), KP_(block_row_store));

protected:
  virtual int inner_open(
      const ObTableIterParam &param,
      ObTableAccessContext &context,
      ObITable *table,
      const void *query_range)
  {
    UNUSEDx(param, context, table, query_range);
    return common::OB_NOT_IMPLEMENT;
  }
  virtual int fetch_row(ObSSTableReadHandle &read_handle, const blocksstable::ObDatumRow *&store_row)
  {
    UNUSEDx(read_handle, store_row);
    return OB_NOT_IMPLEMENT;
  }
  virtual int inner_get_next_row(const blocksstable::ObDatumRow *&store_row)
  {
    UNUSED(store_row);
    return OB_NOT_IMPLEMENT;
  }

protected:
  int type_;
  bool is_sstable_iter_;
  ObBlockRowStore *block_row_store_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObStoreRowIterator);
};

enum ObQRIterType
{
  T_INVALID_ITER_TYPE,
  T_SINGLE_GET,
  T_MULTI_GET,
  T_SINGLE_SCAN,
  T_MULTI_SCAN,
};

class ObQueryRowIterator : public ObIStoreRowIterator
{
public:
  ObQueryRowIterator() : type_(T_INVALID_ITER_TYPE) {}
  explicit ObQueryRowIterator(const ObQRIterType type) : type_(type) {}
  virtual ~ObQueryRowIterator() {}

  // get the next ObStoreRow and move the cursor
  // @param row [out], row can be modified outside
  // @return OB_ITER_END if end of iteration
  virtual int get_next_row(blocksstable::ObDatumRow *&row) = 0;
  virtual int get_next_row(const blocksstable::ObDatumRow *&row) override
  {
    return get_next_row(const_cast<blocksstable::ObDatumRow *&>(row));
  }
  virtual void reset() = 0;
  // Iterate row interface for vectorized engine.
  virtual int get_next_rows(int64_t &count, int64_t capacity)
  {
    // use get_next_row by default
    int ret = OB_SUCCESS;
    blocksstable::ObDatumRow *row = nullptr;
    count = 0;
    if (capacity <= 0) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "Invalid argument", K(ret), K(capacity));
    } else if (OB_FAIL(get_next_row(row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        STORAGE_LOG(WARN, "fail to get next row", K(ret));
      }
    } else {
      count = 1;
    }
    return ret;
  }
  //TODO @hanhui remove this
  virtual const common::ObIArray<share::schema::ObColDesc> *get_out_project_cells() { return nullptr; }
  VIRTUAL_TO_STRING_KV(K_(type));
public:
  ObQRIterType get_type() const { return type_; }
protected:
  ObQRIterType type_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObQueryRowIterator);
};

struct TableTypedIters
{
  TableTypedIters(const std::type_info& info, common::ObIAllocator &alloc);
  ~TableTypedIters() { reset(); }
  void reset();
  bool is_type(const std::type_info &type) const
  {
    return nullptr != type_info_ && *type_info_ == type;
  }
  const char* name() const
  {
    if (nullptr != type_info_) {
      return type_info_->name();
    } else {
      return "";
    }
  }
  TO_STRING_KV(K_(iters));
  const std::type_info* type_info_;
  common::ObIAllocator &allocator_;
  common::ObSEArray<ObStoreRowIterator *, 4, common::ObIAllocator&> iters_;
};

class ObStoreRowIterPool
{
public:
  ObStoreRowIterPool(common::ObIAllocator &alloc);
  ~ObStoreRowIterPool() { reset(); }
  void reset();
  int get_iter(const std::type_info &type, ObStoreRowIterator *&iter);
  void return_iter(ObStoreRowIterator *iter);
private:
  common::ObIAllocator &allocator_;
  common::ObSEArray<TableTypedIters *, 8, common::ObIAllocator&> table_iters_array_;
};

#define ALLOCATE_TABLE_STORE_ROW_IETRATOR(ctx, class, ptr)                                     \
do {                                                                                           \
  if (NULL != ctx.iter_pool_) {                                                                \
    if (OB_FAIL(ctx.iter_pool_->get_iter(typeid(class), ptr))) {                               \
      STORAGE_LOG(WARN, "Failed to get iter from pool", K(ret), K(ctx));                       \
    }                                                                                          \
  }                                                                                            \
  void *buf = NULL;                                                                            \
  if (OB_SUCC(ret) && NULL == ptr) {                                                           \
    if (NULL == (buf = ctx.stmt_allocator_->alloc(sizeof(class)))) {                           \
      ret = OB_ALLOCATE_MEMORY_FAILED;                                                         \
      STORAGE_LOG(WARN, "Fail to allocate memory", K(ret));                                    \
    } else {                                                                                   \
      ptr = new (buf) class();                                                                 \
    }                                                                                          \
  }                                                                                            \
} while(0)                                                                                     \

#define FREE_TABLE_STORE_ROW_IETRATOR(ctx, ptr)                                                \
do {                                                                                           \
  if (NULL != ptr) {                                                                           \
    ctx.stmt_allocator_->free(ptr);                                                            \
  }                                                                                            \
 } while(0)                                                                                    \

}
}
#endif //OB_STORAGE_OB_STORE_ROW_ITERATOR_H_
