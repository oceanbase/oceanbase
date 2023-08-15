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

#include "storage/direct_load/ob_direct_load_multiple_heap_table_map.h"
#include "share/rc/ob_tenant_base.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_array_iterator.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace storage
{

ObDirectLoadMultipleHeapTableMap::ObDirectLoadMultipleHeapTableMap(int64_t mem_limit) :
  allocator_("TLD_HT_map"),
  mem_limit_(mem_limit)
{
}

int ObDirectLoadMultipleHeapTableMap::init()
{
  int ret = OB_SUCCESS;
  allocator_.set_tenant_id(MTL_ID());
  ret = tablet_map_.init();
  return ret;
}

int ObDirectLoadMultipleHeapTableMap::deep_copy_row(const RowType &row, RowType *&result_row)
{
  int ret = OB_SUCCESS;
  if (allocator_.used() >= mem_limit_) { //到达内存上限
    ret = OB_BUF_NOT_ENOUGH;
  }
  if (OB_SUCC(ret)) {
    result_row = OB_NEWx(RowType, (&allocator_));
    if (result_row == nullptr) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate row", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    int64_t size = row.get_deep_copy_size();
    char *buf = (char *)allocator_.alloc(size);
    int64_t pos = 0;
    if (buf == nullptr) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc buf", K(size), KR(ret));
    } else if (OB_FAIL(result_row->deep_copy(row, buf, size, pos))) {
      LOG_WARN("fail to copy row", KR(ret));
    }
  }
  if (OB_FAIL(ret)) {
    if (result_row != nullptr) {
      result_row->~RowType();
      result_row = nullptr;
    }
  }
  return ret;
}

int ObDirectLoadMultipleHeapTableMap::get_all_key_sorted(ObArray<KeyType> &key_array)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tablet_map_.get_all_key(key_array))) {
    LOG_WARN("fail to get all keys", KR(ret));
  }
  if (OB_SUCC(ret)) {
    std::sort(key_array.begin(), key_array.end());
  }
  return ret;
}


int ObDirectLoadMultipleHeapTableMap::add_row(const KeyType &key, const RowType &row)
{
  int ret = OB_SUCCESS;
  RowType *result_row = nullptr;
  if (OB_FAIL(deep_copy_row(row, result_row))) {
    if (OB_UNLIKELY(OB_BUF_NOT_ENOUGH != ret)) {
      LOG_WARN("fail to copy row", KR(ret));
    }
  } else if (OB_FAIL(tablet_map_.add(key, result_row))) {
    LOG_WARN("fail to add row", KR(ret));
  }
  return ret;
}

}
}
