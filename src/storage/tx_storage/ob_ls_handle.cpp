/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE


#include "ob_ls_handle.h"
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

int ObLSHandle::copy_from(const ObLSHandle &other)
{
  int ret = OB_SUCCESS;
  if (this == &other || is_valid() || !other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(this), K(other));
  } else if (OB_FAIL(other.ls_->get_ref_mgr().inc(other.mod_))) {
    // A deleted LS can reject new references even while other keeps it alive.
    LOG_WARN("ls inc ref fail", K(ret), KPC(this), K(other));
  } else {
    ls_map_ = other.ls_map_;
    ls_ = other.ls_;
    mod_ = other.mod_;
  }
  return ret;
}

void ObLSHandle::swap(ObLSHandle &other)
{
  const ObLSMap *ls_map = ls_map_;
  ObLS *ls = ls_;
  const ObLSGetMod mod = mod_;
  ls_map_ = other.ls_map_;
  ls_ = other.ls_;
  mod_ = other.mod_;
  other.ls_map_ = ls_map;
  other.ls_ = ls;
  other.mod_ = mod;
}

void ObLSHandle::reset()
{
  if (OB_NOT_NULL(ls_map_) && OB_NOT_NULL(ls_)) {
    ls_map_->revert_ls(ls_, mod_);
    ls_map_ = nullptr;
    ls_ = nullptr;
    mod_ = ObLSGetMod::INVALID_MOD;
    RESET_OBJ_LEAK_DEBUG_NODE(node_);
  }
}
} // namespace storage
} // namespace oceanbase
