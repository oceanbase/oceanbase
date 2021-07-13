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

#ifndef OB_OCEANBASE_SCHEMA_SCHEMA_MEM_MGR_H_
#define OB_OCEANBASE_SCHEMA_SCHEMA_MEM_MGR_H_

#include <stdint.h>
#include "share/ob_define.h"
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_array.h"

namespace oceanbase {
namespace common {}
namespace share {
namespace schema {

class ObSchemaMemMgr {
public:
  ObSchemaMemMgr();
  virtual ~ObSchemaMemMgr();
  // TODO: Subsequent need to do 500 tenant memory split, schema_mgr memory usage needs to be split to tenants
  int init(const char* label, const uint64_t tenant_id);
  int alloc(const int size, void*& ptr, common::ObIAllocator** allocator = NULL);
  int free(void* ptr);
  int get_cur_alloc_cnt(int64_t& cnt) const;
  int check_can_switch_allocator(bool& can_switch) const;
  int in_current_allocator(const void* ptr, bool& in_curr_allocator);
  int is_same_allocator(const void* p1, const void* p2, bool& is_same_allocator);
  int switch_allocator();
  void dump() const;
  int check_can_release(bool& can_release) const;
  int try_reset_allocator();
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }

  int try_reset_another_allocator();
  int get_another_ptrs(common::ObArray<void*>& ptrs);

private:
  bool check_inner_stat() const;
  int find_ptr(const void* ptr, const int ptrs_pos, int& idx);

private:
  DISALLOW_COPY_AND_ASSIGN(ObSchemaMemMgr);

private:
  common::ObArenaAllocator allocator_[2];
  common::ObArray<void*> all_ptrs_[2];
  common::ObArray<void*> ptrs_[2];
  int pos_;
  bool is_inited_;
  uint64_t tenant_id_;
};

}  // end of namespace schema
}  // end of namespace share
}  // end of namespace oceanbase
#endif  // OB_OCEANBASE_SCHEMA_SCHEMA_MEM_MGR_H_
