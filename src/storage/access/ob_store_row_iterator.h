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
#include "storage/column_store/ob_i_cg_iterator.h"
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
    IteratorRowLockAndDuplicationCheck = 6,
    IteratorMultiRowLockCheck = 7,
    IteratorCOSingleGet = 8,
    IteratorCOMultiGet = 9,
    IteratorCOScan = 10,
    IteratorCOMultiScan = 11,
    IteratorInvalidType
  };
  ObStoreRowIterator() :
      type_(IteratorReserved),
      is_sstable_iter_(false),
      is_reclaimed_(false),
      block_row_store_(nullptr),
      long_life_allocator_(nullptr)
  {};
  virtual ~ObStoreRowIterator();
  virtual void reuse();
  virtual void reset();
  // used for global query iterator pool, prepare for returning to pool
  virtual void reclaim();
  virtual int init(
      const ObTableIterParam &param,
      ObTableAccessContext &context,
      ObITable *table,
      const void *query_range);
  virtual bool is_sstable_iter() const { return is_sstable_iter_; }
  virtual int refresh_blockscan_checker(const blocksstable::ObDatumRowkey &rowkey)
  {
    UNUSED(rowkey);
    return OB_NOT_SUPPORTED;
  }
  virtual int set_ignore_shadow_row() { return OB_NOT_SUPPORTED; }
  virtual bool can_blockscan() const
  {
    return false;
  }
  virtual bool can_batch_scan() const
  {
    return false;
  }
  virtual int get_next_row(const blocksstable::ObDatumRow *&row);
  virtual int get_next_rows()
  {
    return OB_SUCCESS;
  }
  int get_iter_type() const
  {
    return type_;
  }
  static bool is_get(int iter_type)
  {
    return IteratorSingleGet == iter_type ||
        IteratorCOSingleGet == iter_type;
  }
  static bool is_multi_get(int iter_type)
  {
    return IteratorMultiGet == iter_type ||
        IteratorCOMultiGet == iter_type;
  }
  static bool is_scan(int iter_type)
  {
    return IteratorScan == iter_type ||
        IteratorMultiScan == iter_type ||
        IteratorCOScan == iter_type ||
        IteratorCOMultiScan == iter_type;
  }
  OB_INLINE bool is_reclaimed() const { return is_reclaimed_; }

  VIRTUAL_TO_STRING_KV(K_(type), K_(is_sstable_iter), K_(is_reclaimed), KP_(block_row_store), KP_(long_life_allocator));

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
  bool is_reclaimed_;
  ObBlockRowStore *block_row_store_;
  ObIAllocator *long_life_allocator_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObStoreRowIterator);
};

// do not change the value of the enum
enum ObQRIterType
{
  T_INVALID_ITER_TYPE = -1,
  T_SINGLE_GET,
  T_MULTI_GET,
  T_SINGLE_SCAN,
  T_MULTI_SCAN,
  T_MAX_ITER_TYPE,
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
  // used for global query iterator pool, prepare for returning to pool
  virtual void reclaim()
  {
    OB_ASSERT_MSG(false, "ObQueryRowIterator dose not impl reclaim");
  }
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
  VIRTUAL_TO_STRING_KV(K_(type));
public:
  ObQRIterType get_type() const { return type_; }
  bool is_scan() const { return type_ == T_SINGLE_SCAN || type_ == T_MULTI_SCAN; }
protected:
  ObQRIterType type_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObQueryRowIterator);
};

template<typename T>
struct TableTypedIters
{
  TableTypedIters(const std::type_info& info, const uint32_t cg_idx, common::ObIAllocator &alloc);
  ~TableTypedIters();
  void reset();
  void reclaim();
  bool is_type(const std::type_info &type, const uint32_t cg_idx)
  {
    return nullptr != type_info_ && *type_info_ == type && cg_idx == cg_idx_;
  }
  const char* name() const
  {
    if (nullptr != type_info_) {
      return type_info_->name();
    } else {
      return "";
    }
  }
  TO_STRING_KV(K_(cg_idx), K_(iters));
  uint32_t cg_idx_;
  const std::type_info* type_info_;
  common::ObIAllocator &allocator_;
  common::ObSEArray<T*, 4, common::ObIAllocator&> iters_;
};

template<typename T>
class ObStoreRowIterPool
{
public:
  ObStoreRowIterPool(common::ObIAllocator &alloc);
  ~ObStoreRowIterPool();
  void reset();
  void reclaim();
  int get_iter(const std::type_info &type, T *&iter, const uint32_t cg_idx = OB_CS_INVALID_CG_IDX);
  void return_iter(T *iter);
  void return_cg_iter(T *iter, uint32_t cg_idx);
private:
  void inner_return_iter(T *iter, const uint32_t cg_idx);
  common::ObIAllocator &allocator_;
  common::ObSEArray<TableTypedIters<T>*, 8, common::ObIAllocator&> table_iters_array_;
};

template<typename T>
TableTypedIters<T>::TableTypedIters(const std::type_info& info, const uint32_t cg_idx, common::ObIAllocator &alloc)
  : cg_idx_(cg_idx),
    type_info_(&info),
    allocator_(alloc),
    iters_(OB_MALLOC_NORMAL_BLOCK_SIZE, allocator_)
{
}

template<typename T>
TableTypedIters<T>::~TableTypedIters()
{
  reset();
}

template<typename T>
void TableTypedIters<T>::reset()
{
  T *iter = nullptr;
  for (int64_t j = 0; j < iters_.count(); ++j) {
    if (OB_NOT_NULL(iter = iters_.at(j))) {
      iter->~T();
      allocator_.free(iter);
    }
  }
  iters_.reset();
  type_info_ = nullptr;
}

template<typename T>
void TableTypedIters<T>::reclaim()
{
  T *iter = nullptr;
  for (int64_t j = 0; j < iters_.count(); ++j) {
    if (OB_NOT_NULL(iter = iters_.at(j)) && !iter->is_reclaimed()) {
      iter->reclaim();
    }
  }
}

template<typename T>
ObStoreRowIterPool<T>::ObStoreRowIterPool(common::ObIAllocator &alloc)
  : allocator_(alloc),
    table_iters_array_(OB_MALLOC_NORMAL_BLOCK_SIZE, allocator_)
{
}

template<typename T>
ObStoreRowIterPool<T>::~ObStoreRowIterPool()
{
  reset();
}

template<typename T>
void ObStoreRowIterPool<T>::reset()
{
  T *iter = nullptr;
  for (int64_t i = 0; i < table_iters_array_.count(); ++i) {
    TableTypedIters<T> *typed_iters = table_iters_array_.at(i);
    if (OB_NOT_NULL(typed_iters)) {
      typed_iters->~TableTypedIters<T>();
      allocator_.free(typed_iters);
    }
  }
  table_iters_array_.reset();
}

template<typename T>
void ObStoreRowIterPool<T>::reclaim()
{
  for (int64_t i = 0; i < table_iters_array_.count(); ++i) {
    TableTypedIters<T> *typed_iters = table_iters_array_.at(i);
    if (OB_NOT_NULL(typed_iters)) {
      typed_iters->reclaim();
    }
  }
}

template<typename T>
void ObStoreRowIterPool<T>::return_iter(T *iter)
{
  inner_return_iter(iter, OB_CS_INVALID_CG_IDX);
}

template<typename T>
void ObStoreRowIterPool<T>::return_cg_iter(T *iter, uint32_t cg_idx)
{
  iter->reuse();
  inner_return_iter(iter, cg_idx);
}

template<typename T>
void ObStoreRowIterPool<T>::inner_return_iter(T *iter, const uint32_t cg_idx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(iter)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "iter is null", K(ret), K(iter));
  } else {
    TableTypedIters<T> *typed_iters = nullptr;
    for (int64_t i = 0; i < table_iters_array_.count(); ++i) {
      if (OB_NOT_NULL(table_iters_array_.at(i)) && table_iters_array_.at(i)->is_type(typeid(*iter), cg_idx)) {
        typed_iters = table_iters_array_.at(i);
        break;
      }
    }
    if (nullptr == typed_iters) {
      void *buf = nullptr;
      if (OB_ISNULL(buf = allocator_.alloc(sizeof(TableTypedIters<T>)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "Failed to alloc memory", K(ret));
      } else if (FALSE_IT(typed_iters = new(buf) TableTypedIters<T>(typeid(*iter), cg_idx, allocator_))) {
      } else if (OB_FAIL(table_iters_array_.push_back(typed_iters))) {
        STORAGE_LOG(WARN, "Failed to push back new typed_iters", K(ret), K(typeid(*iter).name()), K(*iter));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(typed_iters)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected null typed_iters", K(ret));
      } else if (OB_FAIL(typed_iters->iters_.push_back(iter))) {
        STORAGE_LOG(WARN, "Failed to push back iter", K(ret), K(typeid(*iter).name()), K(*iter));
      }
    }
    if (OB_FAIL(ret)) {
      STORAGE_LOG(ERROR, "Failed to return iter", K(ret), K(typeid(*iter).name()));
      iter->~T();
      allocator_.free(iter);
    }
  }
}

template<typename T>
int ObStoreRowIterPool<T>::get_iter(const std::type_info &type, T *&iter, const uint32_t cg_idx)
{
  int ret = OB_SUCCESS;
  iter = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_iters_array_.count(); ++i) {
    TableTypedIters<T> *typed_iters = table_iters_array_.at(i);
    if (OB_NOT_NULL(typed_iters) && typed_iters->is_type(type, cg_idx)) {
      if (typed_iters->iters_.count() > 0) {
        if (OB_FAIL(typed_iters->iters_.pop_back(iter))) {
          STORAGE_LOG(WARN, "Failed to pop back", K(ret), K(typed_iters->iters_));
        }
      }
      break;
    }
  }
  return ret;
}


#define ALLOCATE_TABLE_STORE_ROW_IETRATOR(ctx, class, ptr)                                     \
do {                                                                                           \
  if (NULL != ctx.get_stmt_iter_pool()) {                                                      \
    if (OB_FAIL(ctx.get_stmt_iter_pool()->get_iter(typeid(class), ptr))) {                     \
      STORAGE_LOG(WARN, "Failed to get iter from pool", K(ret), K(ctx));                       \
    }                                                                                          \
  }                                                                                            \
  void *buf = NULL;                                                                            \
  if (OB_SUCC(ret) && NULL == ptr) {                                                           \
    if (NULL == (buf = ctx.get_long_life_allocator()->alloc(sizeof(class)))) {                 \
      ret = OB_ALLOCATE_MEMORY_FAILED;                                                         \
      STORAGE_LOG(WARN, "Fail to allocate memory", K(ret));                                    \
    } else {                                                                                   \
      ptr = new (buf) class();                                                                 \
    }                                                                                          \
  }                                                                                            \
} while(0)

#define FREE_TABLE_STORE_ROW_IETRATOR(ctx, ptr)                                                \
do {                                                                                           \
  if (NULL != ptr) {                                                                           \
    ctx.get_long_life_allocator()->free(ptr);                                                  \
  }                                                                                            \
 } while(0)

#define ALLOCATE_TABLE_STORE_CG_IETRATOR(ctx, cg_idx, class, ptr)                              \
do {                                                                                           \
  if (NULL != ctx.cg_iter_pool_) {                                                             \
    if (OB_FAIL(ctx.cg_iter_pool_->get_iter(typeid(class), ptr, cg_idx))) {                    \
      STORAGE_LOG(WARN, "Failed to get iter from pool", K(ret), K(ctx));                       \
    }                                                                                          \
  }                                                                                            \
  void *buf = NULL;                                                                            \
  if (OB_SUCC(ret) && NULL == ptr) {                                                           \
    if (NULL == (buf = ctx.get_long_life_allocator()->alloc(sizeof(class)))) {                 \
      ret = OB_ALLOCATE_MEMORY_FAILED;                                                         \
      STORAGE_LOG(WARN, "Fail to allocate memory", K(ret));                                    \
    } else {                                                                                   \
      ptr = new (buf) class();                                                                 \
    }                                                                                          \
  }                                                                                            \
} while(0)

#define FREE_ITER_FROM_ALLOCATOR(alloc, ptr, T)                                               \
  do {                                                                                        \
    if (nullptr != ptr) {                                                                     \
      ptr->~T();                                                                              \
      if (OB_LIKELY(nullptr != alloc)) {                                                      \
        alloc->free(ptr);                                                                     \
      }                                                                                       \
      ptr = nullptr;                                                                          \
    }                                                                                         \
  } while (0)

#define FREE_TABLE_STORE_CG_IETRATOR(ctx, ptr) FREE_TABLE_STORE_ROW_IETRATOR(ctx, ptr)

}
}
#endif //OB_STORAGE_OB_STORE_ROW_ITERATOR_H_
