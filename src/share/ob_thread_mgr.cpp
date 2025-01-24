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

#include "ob_thread_mgr.h"
#include "lib/thread/thread_mgr.h"
#include "storage/tx/ob_trans_service.h"
#include "observer/ob_srv_deliver.h"
#include "observer/ob_startup_accel_task_handler.h"
#ifdef OB_BUILD_ARBITRATION
#include "logservice/arbserver/ob_arb_srv_deliver.h"
#endif
#include "logservice/rcservice/ob_role_change_service.h"
#include "observer/ob_startup_accel_task_handler.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/shared_storage/prewarm/ob_replica_prewarm_struct.h"
#include "close_modules/shared_storage/storage/shared_storage/ob_private_block_gc_task.h"
#include "close_modules/shared_storage/storage/shared_storage/ob_public_block_gc_service.h"
#endif

using namespace oceanbase::common;
using namespace oceanbase::lib;
namespace oceanbase
{
namespace share
{
void ob_init_create_func()
{
  #define TG_DEF(id, name, type, args...)                              \
    lib::create_funcs_[lib::TGDefIDs::id] = []() {                     \
      TG_##type *ret = OB_NEW(TG_##type, SET_USE_500("tg"), args);     \
      if (NULL != ret) {                                               \
        ret->attr_ = {#name, TGType::type};                            \
      }                                                                \
      return ret;                                                      \
    };
  #include "share/ob_thread_define.h"
  #undef TG_DEF
}
} // end of namespace share

namespace lib
{
void init_create_func()
{
  lib_init_create_func();
  share::ob_init_create_func();
}
}

} // end of namespace oceanbase
