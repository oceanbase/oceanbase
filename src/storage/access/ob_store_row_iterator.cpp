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

#define USING_LOG_PREFIX STORAGE
#include "ob_store_row_iterator.h"
#include "ob_dml_param.h"
#include "storage/access/ob_table_access_context.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
namespace storage
{

ObStoreRowIterator::~ObStoreRowIterator()
{
}

void ObStoreRowIterator::reuse()
{
  is_sstable_iter_ = false;
}

void ObStoreRowIterator::reset()
{
  is_sstable_iter_ = false;
}

int ObStoreRowIterator::init(
    const ObTableIterParam &iter_param,
    storage::ObTableAccessContext &access_ctx,
    ObITable *table,
    const void *query_range)
{
  int ret = OB_SUCCESS;
  is_sstable_iter_ = table->is_sstable();
  if (is_sstable_iter_) {
    block_row_store_ = access_ctx.block_row_store_;
  }
  if (OB_FAIL(inner_open(iter_param, access_ctx, table, query_range))) {
    STORAGE_LOG(WARN, "Failed to inner open ObStoreRowIterator", K(ret));
  }
  return ret;
}

int ObStoreRowIterator::get_next_row(const ObDatumRow *&store_row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_get_next_row(store_row))) {
  }
  return ret;
}


TableTypedIters::TableTypedIters(const std::type_info& info, common::ObIAllocator &alloc)
  : type_info_(&info),
    allocator_(alloc),
    iters_(OB_MALLOC_NORMAL_BLOCK_SIZE, allocator_)
{
}

void TableTypedIters::reset()
{
  ObStoreRowIterator *iter = nullptr;
  for (int64_t j = 0; j < iters_.count(); ++j) {
    if (OB_NOT_NULL(iter = iters_.at(j))) {
      iter->~ObStoreRowIterator();
      allocator_.free(iter);
    }
  }
  iters_.reset();
  type_info_ = nullptr;
}

ObStoreRowIterPool::ObStoreRowIterPool(common::ObIAllocator &alloc)
  : allocator_(alloc),
    table_iters_array_(OB_MALLOC_NORMAL_BLOCK_SIZE, allocator_)
{
}

void ObStoreRowIterPool::reset()
{
  int ret = OB_SUCCESS;
  ObStoreRowIterator *iter = nullptr;
  for (int64_t i = 0; i < table_iters_array_.count(); ++i) {
    TableTypedIters *typed_iters = table_iters_array_.at(i);
    if (OB_NOT_NULL(typed_iters)) {
      typed_iters->reset();
      allocator_.free(typed_iters);
    }
  }
  table_iters_array_.reset();
}

void ObStoreRowIterPool::return_iter(ObStoreRowIterator *iter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(iter)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "iter is null", K(ret), K(iter));
  } else {
    TableTypedIters *typed_iters = nullptr;
    for (int64_t i = 0; i < table_iters_array_.count(); ++i) {
      if (OB_NOT_NULL(table_iters_array_.at(i)) && table_iters_array_.at(i)->is_type(typeid(*iter))) {
        typed_iters = table_iters_array_.at(i);
        break;
      }
    }
    if (nullptr == typed_iters) {
      void *buf = nullptr;
      if (OB_ISNULL(buf = allocator_.alloc(sizeof(TableTypedIters)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "Failed to alloc memory", K(ret));
      } else if (FALSE_IT(typed_iters = new(buf) TableTypedIters(typeid(*iter), allocator_))) {
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
      iter->~ObStoreRowIterator();
      allocator_.free(iter);
    }
  }
}


int ObStoreRowIterPool::get_iter(const std::type_info &type, ObStoreRowIterator *&iter)
{
  int ret = OB_SUCCESS;
  iter = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_iters_array_.count(); ++i) {
    TableTypedIters *typed_iters = table_iters_array_.at(i);
    if (OB_NOT_NULL(typed_iters) && typed_iters->is_type(type)) {
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

}
}
