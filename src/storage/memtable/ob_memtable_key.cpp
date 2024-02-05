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

#include "ob_memtable_key.h"
#include "lib/ob_errno.h"
#include "rowkey/ob_rowkey.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/ob_i_store.h"

namespace oceanbase
{
namespace memtable
{

constexpr int64_t ObMemtableKeyGenerator::STACK_BUFFER_SIZE;

int ObMemtableKeyGenerator::init(const storage::ObStoreRow *rows,
                                 const int64_t row_count,
                                 const int64_t schema_rowkey_count,
                                 const common::ObIArray<share::schema::ObColDesc> &columns)
{
  int ret = OB_SUCCESS;
  if (size_ != 0) {
    ret = OB_INIT_TWICE;
  } else {
    int64_t extra_size = row_count - STACK_BUFFER_SIZE;
    if (extra_size > 0) {
      if (FALSE_IT(p_extra_store_row_keys_ =
         (ObStoreRowkey *)share::mtl_malloc(extra_size * (sizeof(ObStoreRowkey)), "MemTableKey"))) {
      } else if (OB_ISNULL(p_extra_store_row_keys_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TRANS_LOG(WARN, "Failed to alloc ObStoreRowkey memory", K(ret), K(row_count), K(schema_rowkey_count), K(columns), KP(MTL_CTX()),
                  KP(lib::ObMallocAllocator::get_instance()), KP(p_extra_store_row_keys_));
      } else if (FALSE_IT(p_extra_memtable_keys_ =
                (ObMemtableKey *)share::mtl_malloc(extra_size * (sizeof(ObMemtableKey)), "MemTableKey"))) {
      } else if (OB_ISNULL(p_extra_memtable_keys_)) {
        share::mtl_free(p_extra_store_row_keys_);
        p_extra_store_row_keys_ = nullptr;
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TRANS_LOG(WARN, "Failed to alloc ObMemtableKey memory", K(ret), K(row_count), K(schema_rowkey_count), K(columns), KP(MTL_CTX()),
                  KP(lib::ObMallocAllocator::get_instance()), KP(p_extra_store_row_keys_));
      }
    }
    for (int i = 0; i < row_count && OB_SUCC(ret); ++i) {
      ObStoreRowkey *p_store_row_key = i < STACK_BUFFER_SIZE ? &store_row_key_buffer_[i] : &p_extra_store_row_keys_[i - STACK_BUFFER_SIZE];
      ObMemtableKey *p_memtable_key = i < STACK_BUFFER_SIZE ? &memtable_key_buffer_[i] : &p_extra_memtable_keys_[i - STACK_BUFFER_SIZE];
      new (p_store_row_key) ObStoreRowkey();
      new (p_memtable_key) ObMemtableKey();
      if (OB_FAIL(p_store_row_key->assign(rows[i].row_val_.cells_, schema_rowkey_count))) {
        p_store_row_key->~ObStoreRowkey();
        TRANS_LOG(WARN, "Failed to assign tmp rowkey", K(ret), K(rows[i]), K(row_count), K(schema_rowkey_count));
      } else if (OB_FAIL(p_memtable_key->encode(columns, p_store_row_key))) {
        p_memtable_key->~ObMemtableKey();
        p_store_row_key->~ObStoreRowkey();
        TRANS_LOG(WARN, "mtk encode fail", K(ret), K(rows[i]), K(row_count), K(schema_rowkey_count));
      } else {
        size_++;
      }
    }
    if (OB_FAIL(ret)) {
      reset();
    }
  }
  return ret;
}

ObMemtableKey &ObMemtableKeyGenerator::operator[](int64_t idx) {
  ObMemtableKey *element = nullptr;
  if (OB_UNLIKELY(idx < 0 || idx >= size_)) {
    ob_abort();
  }
  if (idx < STACK_BUFFER_SIZE) {
    element = &memtable_key_buffer_[idx];
  } else {
    element = &p_extra_memtable_keys_[idx - STACK_BUFFER_SIZE];
  }
  return *element;
}

const ObMemtableKey &ObMemtableKeyGenerator::operator[](int64_t idx) const
{
  return const_cast<ObMemtableKeyGenerator *>(this)->operator[](idx);
}

void ObMemtableKeyGenerator::reset()
{
  int64_t idx = size_ - 1;
  for(; idx >= STACK_BUFFER_SIZE; --idx) {
    p_extra_memtable_keys_[idx - STACK_BUFFER_SIZE].~ObMemtableKey();
    p_extra_store_row_keys_[idx - STACK_BUFFER_SIZE].~ObStoreRowkey();
  }
  for(; idx >= 0; --idx) {
    memtable_key_buffer_[idx].~ObMemtableKey();
    store_row_key_buffer_[idx].~ObStoreRowkey();
  }
  if (OB_UNLIKELY(nullptr != p_extra_memtable_keys_)) {
    share::mtl_free(p_extra_memtable_keys_);
  }
  if (OB_UNLIKELY(nullptr != p_extra_store_row_keys_)) {
    share::mtl_free(p_extra_store_row_keys_);
  }
  new (this) ObMemtableKeyGenerator();
}

ObMemtableKeyGenerator::~ObMemtableKeyGenerator()
{
  reset();
}

}
}