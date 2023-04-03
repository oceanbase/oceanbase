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

#define USING_LOG_PREFIX STORAGE

#include "storage/tx_storage/ob_ls_handle.h"

#include "share/rc/ob_tenant_base.h"
#include "storage/tx_storage/ob_ls_map.h"

namespace oceanbase
{
namespace storage
{
ObLSHandle::ObLSHandle()
  : ls_map_(nullptr),
    ls_(nullptr),
    mod_(ObLSGetMod::INVALID_MOD)
{
  INIT_OBJ_LEAK_DEBUG_NODE(node_, this, share::LEAK_CHECK_OBJ_LS_HANDLE, MTL_ID());
}

ObLSHandle::ObLSHandle(const ObLSHandle &other)
  : ls_map_(nullptr),
    ls_(nullptr),
    mod_(ObLSGetMod::INVALID_MOD)
{
  INIT_OBJ_LEAK_DEBUG_NODE(node_, this, share::LEAK_CHECK_OBJ_LS_HANDLE, MTL_ID());
  *this = other;
}

ObLSHandle::~ObLSHandle()
{
  reset();
}

bool ObLSHandle::is_valid() const
{
  return (nullptr != ls_ && nullptr != ls_map_);
}

ObLSHandle &ObLSHandle::operator=(const ObLSHandle &other)
{
  int ret = OB_SUCCESS;
  if (&other != this) {
    if (nullptr != ls_ && nullptr != ls_map_) {
      reset();
    }
    if (nullptr != other.ls_ && nullptr != other.ls_map_ && OB_SUCC(other.ls_->get_ref_mgr().inc(other.mod_))) {
      ls_ = other.ls_;
      ls_map_ = other.ls_map_;
      mod_ = other.mod_;
    } else {
      LOG_WARN("ls assign fail", K(ret), K(other), K(ls_), K(ls_map_));
    }
  }
  return *this;
}

int ObLSHandle::set_ls(const ObLSMap &ls_map, ObLS &ls, const ObLSGetMod &mod)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_SUCC(ls.get_ref_mgr().inc(mod))) {
    ls_map_ = &ls_map;
    ls_ = &ls;
    mod_ = mod;
  }
  return ret;
}

void ObLSHandle::reset()
{
  if (OB_NOT_NULL(ls_map_) && OB_NOT_NULL(ls_)) {
    ls_map_->revert_ls(ls_, mod_);
    ls_map_ = nullptr;
    ls_ = nullptr;
    mod_ = ObLSGetMod::INVALID_MOD;
  }
}
} // namespace storage
} // namespace oceanbase