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

#include "lib/thread/thread_mgr.h"
#include "lib/alloc/memory_dump.h"
#include "lib/io/ob_io_manager.h"
#include "lib/lock/ob_latch.h"

namespace oceanbase {
using namespace common;
namespace lib {
CreateFunc create_funcs_[] = {nullptr};
bool create_func_inited_ = false;

void __attribute__((weak)) init_create_func()
{
  lib_init_create_func();
}

void lib_init_create_func()
{
#define TG_DEF(id, name, desc, scope, type, args...)            \
  create_funcs_[TGDefIDs::id] = []() {                          \
    auto ret = OB_NEW(TGCLSMap<TGType::type>::CLS, "tg", args); \
    ret->attr_ = {#name, desc, TGScope::scope, TGType::type};   \
    return ret;                                                 \
  };
#include "lib/thread/thread_define.h"
#undef TG_DEF
}

TGMgr::TGMgr() : bs_(MAX_ID, bs_buf_)
{
  int ret = OB_SUCCESS;
  for (int i = 0; i < MAX_ID; i++) {
    bs_.set(i);
  }
  for (int i = 0; OB_SUCC(ret) && i < TGDefIDs::END; i++) {
    int tg_id = -1;
    ret = create_tg(i, tg_id);
    if (OB_FAIL(ret)) {
    } else if (tg_id < 0) {
      // do-nothing
    } else {
      default_tg_id_map_[i] = tg_id;
    }
  }
  abort_unless(OB_SUCCESS == ret);
}

TGMgr::~TGMgr()
{
  for (int i = 0; i < MAX_ID; i++) {
    destroy_tg(i);
  }
}

void TGMgr::destroy_tg(int tg_id)
{
  if (tg_id != -1) {
    ITG*& tg = tgs_[tg_id];
    if (tg != nullptr) {
      OB_LOG(INFO, "destroy tg", K(tg_id), KP(tg), K(tg->attr_));
      tg->stop();
      tg->wait();
      OB_DELETE(ITG, "", tg);
      tg = nullptr;
      free_tg_id(tg_id);
    }
  }
}

int TGMgr::alloc_tg_id()
{
  common::ObLatchMutexGuard guard(lock_, common::ObLatchIds::DEFAULT_MUTEX);
  int tg_id = bs_.find_first_significant(0);
  bs_.unset(tg_id);
  return tg_id;
}

void TGMgr::free_tg_id(int tg_id)
{
  common::ObLatchMutexGuard guard(lock_, common::ObLatchIds::DEFAULT_MUTEX);
  bs_.set(tg_id);
}

}  // namespace lib
}  // namespace oceanbase
