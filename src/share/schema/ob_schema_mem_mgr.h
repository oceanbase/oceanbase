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
#include "share/schema/ob_schema_mgr.h"

namespace oceanbase
{
class ObSchemaMemory
{
public:
  ObSchemaMemory():pos_(OB_INVALID_INDEX), tenant_id_(OB_INVALID_TENANT_ID),
                  mem_used_(OB_INVALID_COUNT), mem_total_(OB_INVALID_COUNT),
                  used_schema_mgr_cnt_(OB_INVALID_COUNT),
                  free_schema_mgr_cnt_(OB_INVALID_COUNT),
                  allocator_idx_(OB_INVALID_INDEX) {}
  ~ObSchemaMemory() {}
  void reset();
  void init(const int64_t pos, const uint64_t &tenant_id,
            const int64_t &mem_used, const int64_t &mem_total,
            const int64_t &used_schema_mgr_cnt,
            const int64_t &free_schema_mgr_cnt,
            const int64_t &allocator_idx);
  int64_t get_pos() const { return pos_; }
  uint64_t get_tenant_id() const { return tenant_id_; }
  int64_t get_mem_used() const { return mem_used_; }
  int64_t get_mem_total() const { return mem_total_; }
  int64_t get_used_schema_mgr_cnt() const { return used_schema_mgr_cnt_; }
  int64_t get_free_schema_mgr_cnt() const { return free_schema_mgr_cnt_; }
  int64_t get_allocator_idx() const { return allocator_idx_; }
  TO_STRING_KV(K_(pos), K_(mem_used), K_(mem_total),
               K_(used_schema_mgr_cnt), K_(free_schema_mgr_cnt));
private:
  int64_t pos_;
  uint64_t tenant_id_;
  int64_t mem_used_;
  int64_t mem_total_;
  int64_t used_schema_mgr_cnt_;
  int64_t free_schema_mgr_cnt_;
  int64_t allocator_idx_;
};
namespace common
{
}
namespace share
{
namespace schema
{

class ObSchemaMemMgr
{
public:
  ObSchemaMemMgr();
  virtual ~ObSchemaMemMgr();
  // TODO: Subsequent need to do 500 tenant memory split, schema_mgr memory usage needs to be split to tenants
  //
  int init(const char *label,
           const uint64_t tenant_id);
  int get_cur_alloc_cnt(int64_t &cnt) const;
  int check_can_switch_allocator(const int64_t &switch_cnt, bool &can_switch) const;
  int in_current_allocator(const void *ptr, bool &in_curr_allocator);
  int is_same_allocator(const void *p1, const void * p2, bool &is_same_allocator);
  int switch_allocator();
  int switch_back_allocator();
  void dump() const;
  int check_can_release(bool &can_release) const;
  int try_reset_allocator();
  uint64_t get_tenant_id() const { return tenant_id_; }
  int try_reset_another_allocator();
  int get_another_ptrs(common::ObArray<void *> &ptrs);
  int get_current_ptrs(common::ObArray<void *> &ptrs);
  int get_all_ptrs(common::ObArray<void *> &ptrs);
  int get_all_alloc_info(common::ObIArray<ObSchemaMemory> &tenant_mem_infos);
  int alloc_schema_mgr(ObSchemaMgr *&schema_mgr, bool alloc_for_liboblog);
  int free_schema_mgr(ObSchemaMgr *&schema_mgr);
private:
  bool check_inner_stat() const;
  void dump_without_lock_() const;
  int find_ptr(const void *ptr, const int ptrs_pos, int &idx);
  int alloc_(const int size, void *&ptr,
             common::ObIAllocator **allocator = NULL);
  int free_(void *ptr);
private:
  DISALLOW_COPY_AND_ASSIGN(ObSchemaMemMgr);
private:
  // avoid construct in union section
  union {
    common::ObArenaAllocator allocator_[2];
  };
  int64_t all_ptrs_[2];
  common::ObArray<void *> ptrs_[2];
  int pos_;
  bool is_inited_;
  uint64_t tenant_id_;
  common::SpinRWLock schema_mem_rwlock_;
};

}//end of namespace schema
}//end of namespace share
}//end of namespace oceanbase
#endif //OB_OCEANBASE_SCHEMA_SCHEMA_MEM_MGR_H_
