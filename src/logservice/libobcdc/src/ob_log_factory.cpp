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

#define USING_LOG_PREFIX OBLOG
#include "ob_log_factory.h"
#include "lib/objectpool/ob_resource_pool.h"

namespace oceanbase
{
namespace libobcdc
{
using namespace oceanbase::common;

#define RPALLOCATOR_ALLOC(object, LABEL) rp_alloc(object, LABEL)
#define RPALLOCATOR_FREE(object, LABEL) rp_free(object, LABEL)

#define LIBOBLOG_FACTORY_CLASS_IMPLEMENT(object_name, LABEL, allocator_type, arg...)  \
  int64_t object_name##Factory::alloc_count_ = 0; \
  int64_t object_name##Factory::release_count_ = 0; \
  const char *object_name##Factory::mod_type_ = #LABEL; \
  object_name *object_name##Factory::alloc(arg)  \
  {  \
    object_name *object = NULL;    \
    if (REACH_TIME_INTERVAL(LIBOBLOG_MEM_STAT_INTERVAL)) {  \
      LOG_INFO("libobcdc factory statistics",  \
                "object_name", #object_name,       \
                "label", #LABEL,                   \
                K_(alloc_count), K_(release_count), "used", alloc_count_ - release_count_);  \
    }  \
    if (NULL != (object = allocator_type##_ALLOC(object_name, LABEL))) { \
      (void)ATOMIC_FAA(&alloc_count_, 1);  \
    }  \
    return object;  \
  }                                             \
  void object_name##Factory::free(object_name *object)  \
  {\
    if (OB_ISNULL(object)) {\
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "object is null", KP(object));\
    } else {\
      object->destroy();  \
      allocator_type##_FREE(object, LABEL);        \
      object = NULL;\
      (void)ATOMIC_FAA(&release_count_, 1);\
    }\
  }\
  int64_t object_name##Factory::get_alloc_count()  \
  {  \
    return alloc_count_;  \
  }  \
  int64_t object_name##Factory::get_release_count()\
  {\
    return release_count_;\
  }\
  const char *object_name##Factory::get_mod_type()\
  {\
    return mod_type_;\
  }\


#define LIBOBLOG_FACTORY_CLASS_IMPLEMENT_USE_RP_ALLOC(object_name, LABEL, arg...) LIBOBLOG_FACTORY_CLASS_IMPLEMENT(object_name, LABEL, RPALLOCATOR, arg)

static constexpr const char STORE_TASK[] = "StoreTask";
static constexpr const char READ_LOG_BUF[] = "ReadLogBuf";
static constexpr const char BIG_BLOCK[] = "BigBlock";

LIBOBLOG_FACTORY_CLASS_IMPLEMENT_USE_RP_ALLOC(ObLogStoreTask, STORE_TASK)
LIBOBLOG_FACTORY_CLASS_IMPLEMENT_USE_RP_ALLOC(ReadLogBuf, READ_LOG_BUF)
LIBOBLOG_FACTORY_CLASS_IMPLEMENT_USE_RP_ALLOC(BigBlock, BIG_BLOCK)

}
}
