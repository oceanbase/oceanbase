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

#include "sql/plan_cache/ob_plan_cache.h"
#include "sql/plan_cache/ob_lib_cache_node_factory.h"
#include "sql/plan_cache/ob_lib_cache_object_manager.h"
#include "sql/plan_cache/ob_lib_cache_register.h"
#include "sql/plan_cache/ob_pcv_set.h"
#include "pl/ob_pl.h"
#include "pl/ob_pl_package.h"
#include "observer/table/ob_table_cache.h"
#include "sql/resolver/cmd/ob_call_procedure_stmt.h"

#define USING_LOG_PREFIX SQL_PC

using namespace oceanbase::common;
namespace oceanbase
{
namespace common
{
class ObIAllocator;
}
namespace sql
{

COAllocFunc ObLibCacheRegister::CO_ALLOC[NS_MAX] = { };
CNAllocFunc ObLibCacheRegister::CN_ALLOC[NS_MAX] = { };
CKAllocFunc ObLibCacheRegister::CK_ALLOC[NS_MAX] = { };
lib::ObLabel ObLibCacheRegister::NS_TYPE_LABELS[NS_MAX] = { };
const char *ObLibCacheRegister::NAME_TYPES[NS_MAX] = { };

#define REG_LIB_CACHE_OBJ(NS, NS_NAME, CO_CLASS, LABEL)              \
do {                                                                 \
  [&]() {                                                            \
    if (OB_UNLIKELY(NS >= NS_MAX)) {                                 \
      LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "out of the max type");                              \
    } else {                                                         \
      NAME_TYPES[NS] = NS_NAME;                                      \
      LC_NS_TYPE_LABELS[NS] = LABEL;                                 \
      LC_CO_ALLOC[NS] = ObLCObjectManager::alloc<CO_CLASS>;          \
    }                                                                \
  }();                                                               \
} while(0)

#define REG_LIB_CACHE_NODE(NS, CN_CLASS)                             \
do {                                                                 \
  [&]() {                                                            \
    if (OB_UNLIKELY(NS >= NS_MAX)) {                                 \
      LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "out of the max type");                              \
    } else {                                                         \
      LC_CN_ALLOC[NS] = ObLCNodeFactory::create<CN_CLASS>;           \
    }                                                                \
  }();                                                               \
} while(0)

#define REG_LIB_CACHE_KEY(NS, CK_CLASS)                              \
do {                                                                 \
  [&]() {                                                            \
    if (OB_UNLIKELY(NS >= NS_MAX)) {                                 \
      LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "out of the max type");                              \
    } else {                                                         \
      LC_CK_ALLOC[NS] = OBLCKeyCreator::create<CK_CLASS>;            \
    }                                                                \
  }();                                                               \
} while(0)

void ObLibCacheRegister::register_cache_objs()
{
  register_lc_key();
  register_lc_obj();
  register_lc_node();
}

ObLibCacheNameSpace ObLibCacheRegister::get_ns_type_by_name(const ObString &name)
{
  ObLibCacheNameSpace ns_type = ObLibCacheNameSpace::NS_INVALID;
  for (uint32_t i = 1; i < ARRAYSIZEOF(NAME_TYPES); i++) {
    if (static_cast<int32_t>(strlen(NAME_TYPES[i])) == name.length()
        && strncasecmp(NAME_TYPES[i], name.ptr(), name.length()) == 0) {
      ns_type = static_cast<ObLibCacheNameSpace>(i);
      break;
    }
  }
  return ns_type;
}

void ObLibCacheRegister::register_lc_obj()
{
memset(NS_TYPE_LABELS, 0, sizeof(NS_TYPE_LABELS));
memset(CO_ALLOC, 0, sizeof(CO_ALLOC));
memset(NAME_TYPES, 0, sizeof(NAME_TYPES));
#define LIB_CACHE_OBJ_DEF(ns, ns_name, ck_class, cn_class, co_class, label) REG_LIB_CACHE_OBJ(ns, ns_name, co_class, label);
#include "sql/plan_cache/ob_lib_cache_register.h"
#undef LIB_CACHE_OBJ_DEF
}

void ObLibCacheRegister::register_lc_key()
{
memset(CK_ALLOC, 0, sizeof(CK_ALLOC));
#define LIB_CACHE_OBJ_DEF(ns, ns_name, ck_class, cn_class, co_class, label) REG_LIB_CACHE_KEY(ns, ck_class);
#include "sql/plan_cache/ob_lib_cache_register.h"
#undef LIB_CACHE_OBJ_DEF
}

void ObLibCacheRegister::register_lc_node()
{
memset(CN_ALLOC, 0, sizeof(CN_ALLOC));
#define LIB_CACHE_OBJ_DEF(ns, ns_name, ck_class, cn_class, co_class, label) REG_LIB_CACHE_NODE(ns, cn_class);
#include "sql/plan_cache/ob_lib_cache_register.h"
#undef LIB_CACHE_OBJ_DEF
}

} // namespace common
} // namespace oceanbase
