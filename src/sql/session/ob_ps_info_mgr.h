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

#ifndef _OB_PS_SESSION_MGR_H
#define _OB_PS_SESSION_MGR_H 1
#include "lib/allocator/ob_pooled_allocator.h"
#include "lib/hash/ob_hashmap.h"
#include "sql/session/ob_ps_session_info.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/session/ob_prepared_sql_store.h"

namespace oceanbase
{
namespace sql
{
// manage all prepared statements of one session
class ObPsInfoMgr
{
public:
  typedef common::ObPooledAllocator<common::hash::HashMapTypes<uint32_t, ObPsSessionInfo *>::AllocType, common::ObWrapperAllocator>
  IdPsInfoMapAllocer;

  typedef common::ObPooledAllocator<common::hash::HashMapTypes<common::ObString, ObPsSessionInfo *>::AllocType, common::ObWrapperAllocator>
  NamePsInfoMapAllocer;

  typedef common::hash::ObHashMap<uint32_t,
                                  ObPsSessionInfo *,
                                  common::hash::NoPthreadDefendMode,
                                  common::hash::hash_func<uint32_t>,
                                  common::hash::equal_to<uint32_t>,
                                  IdPsInfoMapAllocer,
                                  common::hash::NormalPointer,
                                  common::ObWrapperAllocator
                                  > IdPsInfoMap;

  typedef common::hash::ObHashMap<common::ObString,
                                  ObPsSessionInfo *,
                                  common::hash::NoPthreadDefendMode,
                                  common::hash::hash_func<common::ObString>,
                                  common::hash::equal_to<common::ObString>,
                                  NamePsInfoMapAllocer,
                                  common::hash::NormalPointer,
                                  common::ObWrapperAllocator
                                  > NamePsInfoMap;
public:
  ObPsInfoMgr();
  virtual ~ObPsInfoMgr();
  int init();
  void reset();
  int add_ps_info(const uint64_t sql_id, const common::ObString &sql,
                  const ObPhysicalPlanCtx *pctx, const bool is_dml);
  int add_ps_info(const common::ObString &pname, const uint64_t sql_id, const common::ObString &sql,
                  const ObPhysicalPlanCtx *pctx, const bool is_dml);
  int remove_ps_info(const common::ObString &name);
  int remove_ps_info(const uint64_t stmt_id);
  ObPsSessionInfo *get_psinfo(const common::ObString &name);
  ObPsSessionInfo *get_psinfo(const uint64_t stmt_id);
  int close_all_stmt();
  int64_t get_ps_mem_size() { return block_allocator_.get_total_mem_size(); }

private:
  static const int64_t SMALL_BLOCK_SIZE = OB_SESSION_SMALL_BLOCK_SIZE - 8;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObPsInfoMgr);
  // function members
  uint64_t allocate_stmt_id() {return __sync_add_and_fetch(&last_stmt_id_, 1);}

private:
  // data members
  uint64_t last_stmt_id_;
  common::ObSmallBlockAllocator<> block_allocator_;
  common::ObWrapperAllocator bucket_allocator_wrapper_;
  IdPsInfoMapAllocer id_psinfo_map_allocer_;
  NamePsInfoMapAllocer name_psinfo_map_allocer_;
  IdPsInfoMap  id_psinfo_map_; // stmt-id -> ObPsSessionInfo
  NamePsInfoMap name_psinfo_map_;   // name -> ObPsSessionInfo for text prepare
  common::ObPooledAllocator<ObPsSessionInfo, common::ObWrapperAllocator> ps_session_info_pool_;
};

} // end namespace sql
} // end namespace oceanbase

#endif /* _OB_PS_SESSION_MGR_H */
