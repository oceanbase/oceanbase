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

#ifdef LIB_CACHE_OBJ_DEF
LIB_CACHE_OBJ_DEF(NS_CRSR, "CRSR", ObPlanCacheKey, ObPCVSet, ObPhysicalPlan, ObNewModIds::OB_SQL_PHY_PLAN)  // physical plan cache
LIB_CACHE_OBJ_DEF(NS_PRCR, "PRCR", pl::ObPLObjectKey, pl::ObPLObjectSet, pl::ObPLFunction, ObNewModIds::OB_SQL_PHY_PL_OBJ)  // procedure cache
LIB_CACHE_OBJ_DEF(NS_SFC, "SFC", pl::ObPLObjectKey, pl::ObPLObjectSet, pl::ObPLFunction, ObNewModIds::OB_SQL_PHY_PL_OBJ)   // function cache
LIB_CACHE_OBJ_DEF(NS_ANON, "ANON", pl::ObPLObjectKey, pl::ObPLObjectSet, pl::ObPLFunction, ObNewModIds::OB_SQL_PHY_PL_OBJ)  // anonymous cache
LIB_CACHE_OBJ_DEF(NS_TRGR, "TRGR", pl::ObPLObjectKey, pl::ObPLObjectSet, pl::ObPLPackage, ObNewModIds::OB_SQL_PHY_PL_OBJ)   // trigger cache
LIB_CACHE_OBJ_DEF(NS_PKG, "PKG", pl::ObPLObjectKey, pl::ObPLObjectSet, pl::ObPLPackage, ObNewModIds::OB_SQL_PHY_PL_OBJ)    // package cache
LIB_CACHE_OBJ_DEF(NS_TABLEAPI, "TABLEAPI", table::ObTableApiCacheKey, table::ObTableApiCacheNode, table::ObTableApiCacheObj, "OB_TABLEAPI_OBJ")    // tableapi cache
LIB_CACHE_OBJ_DEF(NS_CALLSTMT, "CALLSTMT", pl::ObPLObjectKey, pl::ObPLObjectSet, ObCallProcedureInfo, ObNewModIds::OB_SQL_PHY_PL_OBJ)  // call stmt cache
#ifdef OB_BUILD_SPM
LIB_CACHE_OBJ_DEF(NS_SPM, "SPM", ObBaselineKey, ObSpmSet, ObPlanBaselineItem, "OB_SQL_SPM_OBJ") // baseline cache
#endif /*OB_BUILD_SPM*/
#endif /*LIB_CACHE_OBJ_DEF*/

#ifndef OCEANBASE_SQL_PLAN_CACHE_OB_LIB_CACHE_REGISTER_
#define OCEANBASE_SQL_PLAN_CACHE_OB_LIB_CACHE_REGISTER_

#include "lib/allocator/ob_mod_define.h"
#include "lib/alloc/alloc_struct.h"
#include "sql/plan_cache/ob_pc_ref_handle.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}
namespace lib
{
class MemoryContext;
}

namespace sql
{
class ObILibCacheObject;
class ObILibCacheKey;
class ObILibCacheNode;
class ObPlanCache;


enum ObLibCacheNameSpace
{
NS_INVALID,
#define LIB_CACHE_OBJ_DEF(ns, ns_name, ck_class, cn_class, co_class, label) ns,
#include "sql/plan_cache/ob_lib_cache_register.h"
#undef LIB_CACHE_OBJ_DEF
NS_MAX
};

typedef int (*CNAllocFunc) (lib::MemoryContext &mem_ctx,
                            ObILibCacheNode  *&cache_node,
                            ObPlanCache *lib_cahe);
typedef int (*COAllocFunc) (lib::MemoryContext &mem_ctx,
                            ObILibCacheObject *&cache_obj,
                            CacheRefHandleID ref_handle,
                            uint64_t tenant_id);
typedef int (*CKAllocFunc) (ObIAllocator &allocator,
                            ObILibCacheKey *&cache_key);

class ObLibCacheRegister
{
public:
  static void register_cache_objs();
  static void register_lc_key();
  static void register_lc_obj();
  static void register_lc_node();
  static ObLibCacheNameSpace get_ns_type_by_name(const ObString &name);

public:
  static CKAllocFunc CK_ALLOC[NS_MAX];
  static COAllocFunc CO_ALLOC[NS_MAX];
  static CNAllocFunc CN_ALLOC[NS_MAX];
  static const char *NAME_TYPES[NS_MAX];
  static lib::ObLabel NS_TYPE_LABELS[NS_MAX];
};

#define LC_CO_ALLOC (ObLibCacheRegister::CO_ALLOC)
#define LC_CN_ALLOC (ObLibCacheRegister::CN_ALLOC)
#define LC_CK_ALLOC (ObLibCacheRegister::CK_ALLOC)
#define LC_NS_TYPE_LABELS (ObLibCacheRegister::NS_TYPE_LABELS)

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_SQL_PLAN_CACHE_OB_LIB_CACHE_REGISTER_
