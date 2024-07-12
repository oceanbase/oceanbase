/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "common_define.h"
#include "object/ob_object.h"
#include "share/datum/ob_datum.h"
#include "storage/tablet/ob_mds_schema_helper.h"
#include "share/allocator/ob_shared_memory_allocator_mgr.h"
namespace oceanbase
{
namespace storage
{
namespace mds
{

void *DefaultAllocator::alloc(const int64_t size)  { return ob_malloc(size, "MDS"); }
void *DefaultAllocator::alloc(const int64_t size, const ObMemAttr &attr) { return ob_malloc(size, attr); }
void DefaultAllocator::free(void *ptr) { ob_free(ptr); }
void DefaultAllocator::set_label(const lib::ObLabel &) {}
DefaultAllocator &DefaultAllocator::get_instance() { static DefaultAllocator alloc; return alloc; }
int64_t DefaultAllocator::get_alloc_times() { return ATOMIC_LOAD(&get_instance().alloc_times_); }
int64_t DefaultAllocator::get_free_times() { return ATOMIC_LOAD(&get_instance().free_times_); }

void *MdsAllocator::alloc(const int64_t size)
{
  void *ptr = MTL(share::ObSharedMemAllocMgr *)->mds_allocator().alloc(size);
  if (OB_NOT_NULL(ptr)) {
    ATOMIC_INC(&alloc_times_);
  }
  return ptr;
}

void *MdsAllocator::alloc(const int64_t size, const ObMemAttr &attr)
{
  return MTL(share::ObSharedMemAllocMgr *)->mds_allocator().alloc(size, attr);
}

void MdsAllocator::free(void *ptr) {
  if (OB_NOT_NULL(ptr)) {
    ATOMIC_INC(&free_times_);
    MTL(share::ObSharedMemAllocMgr *)->mds_allocator().free(ptr);
  }
}

void MdsAllocator::set_label(const lib::ObLabel &) {}

MdsAllocator &MdsAllocator::get_instance() { static MdsAllocator alloc; return alloc; }

int64_t MdsAllocator::get_alloc_times() { return ATOMIC_LOAD(&get_instance().alloc_times_); }
int64_t MdsAllocator::get_free_times() { return ATOMIC_LOAD(&get_instance().free_times_); }

int compare_mds_serialized_buffer(const char *lhs_buffer,
                                  const int64_t lhs_buffer_len,
                                  const char *rhs_buffer,
                                  const int64_t rhs_buffer_len,
                                  int &compare_result)
{
  int ret = OB_SUCCESS;
  ObObjMeta binary_meta;
  binary_meta.set_binary();
  ObDatum lhs_datum;
  ObDatum rhs_datum;
  lhs_datum.set_string((const char *)lhs_buffer, lhs_buffer_len);
  rhs_datum.set_string((const char *)rhs_buffer, rhs_buffer_len);
  bool is_null_last = ObMdsSchemaHelper::get_instance().get_rowkey_read_info()->is_oracle_mode();
  sql::ObExprBasicFuncs *basic_funcs = ObDatumFuncs::get_basic_func(binary_meta.get_type(),
                                                                    binary_meta.get_collation_type());
  common::ObDatumCmpFuncType cmp_func = is_null_last ? basic_funcs->null_last_cmp_ : basic_funcs->null_first_cmp_;
  if (OB_FAIL(cmp_func(lhs_datum, rhs_datum, compare_result))) {
    MDS_LOG(WARN, "Failed to compare datum", K(ret), K(lhs_datum), K(rhs_datum), K(binary_meta));
  } else {
    MDS_LOG(DEBUG, "comapre mds serialized buffer", K(ret), K(compare_result), K(lhs_datum), K(rhs_datum),
            K(binary_meta), KP(lhs_buffer), K(lhs_buffer_len), KP(rhs_buffer), K(rhs_buffer_len));
  }
  return ret;
}

}
}
}