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

#define USING_LOG_PREFIX SQL_ENG
#include "ob_hash_agg_variant.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

 using inline_ht = ObExtendHashTableVec<ObGroupRowBucketInline>;
 using outline_ht = ObExtendHashTableVec<ObGroupRowBucket>;
 int ObAggrHashTableWapper::prepare_hash_table(const int64_t item_size, const uint64_t tenant_id,
                                               common::ObIAllocator &alloc)
 {
   int ret = OB_SUCCESS;
   const int64_t inline_size = 0; // FIXME: change to 64
   int err_sim = OB_E(EventTable::EN_DISK_ERROR) 0;
   if (item_size <= inline_size && 0 == err_sim) {
     inline_ht *hash_table = nullptr;
     if (OB_ISNULL(hash_table = static_cast<inline_ht *>(alloc.alloc(sizeof(inline_ht))))) {
       ret = OB_ALLOCATE_MEMORY_FAILED;
       LOG_WARN("failed to alloc hash table", K(ret));
     } else {
       new (hash_table) inline_ht(tenant_id);
       hash_table_ptr_ = hash_table;
       inline_ptr_ = hash_table;
       real_ptr_ = (void *)hash_table;
       type_ = Type::INLINE;
       LOG_TRACE("print hash table type inline", K(item_size), K(inline_size));
     }
   } else {
     outline_ht *hash_table = nullptr;
     if (OB_ISNULL(hash_table = static_cast<outline_ht *>(alloc.alloc(sizeof(outline_ht))))) {
       ret = OB_ALLOCATE_MEMORY_FAILED;
       LOG_WARN("failed to alloc hash table", K(ret));
     } else {
       new (hash_table) outline_ht(tenant_id);
       hash_table_ptr_ = hash_table;
       outline_ptr_ = hash_table;
       real_ptr_ = (void *)hash_table;
       type_ = Type::OUTLINE;
       LOG_TRACE("print hash table type outline", K(item_size), K(inline_size));
     }
   }
   inited_ = true;
   return ret;
}

void ObAggrHashTableWapper::destroy()
{
  if (inited_ && nullptr != real_ptr_) {
    DestroyVisitor visitor;
    boost::apply_visitor(visitor, hash_table_ptr_);
    real_ptr_ = nullptr;
  }
}

}//ns sql
}//ns oceanbase