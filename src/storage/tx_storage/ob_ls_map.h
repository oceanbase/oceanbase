/**
 * Copyright (c) 2021, 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_STORAGE_OB_LS_MAP_
#define OCEANBASE_STORAGE_OB_LS_MAP_

#include "lib/oblog/ob_log_module.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "lib/container/ob_iarray.h"
#include "lib/lock/ob_qsync_lock.h"
#include "storage/ls/ob_ls.h"
#include "share/leak_checker/obj_leak_checker.h"

namespace oceanbase
{
namespace storage
{
class ObLS;
class ObLSHandle;

class ObLSMap
{
public:
  friend class ObLSIterator;
  ObLSMap()
   : is_inited_(false),
    tenant_id_(OB_INVALID_ID),
    ls_allocator_(nullptr),
    ls_cnt_(0),
    ls_buckets_(nullptr),
    buckets_lock_(nullptr)
  {
    reset();
  }
  ~ObLSMap() { destroy(); }
  void reset();
  int init(const int64_t tenant_id, common::ObIAllocator *ls_allocator);
  void destroy();
  // allow_multi_true is used during replay
  int add_ls(ObLS &ls);
  int del_ls(const share::ObLSID &ls_id);
  int get_all_ls_id(ObIArray<ObLSID> &ls_id_array);
  int get_ls(const share::ObLSID &ls_id,
             ObLSHandle &handle,
             ObLSGetMod mod) const;
  OB_INLINE void revert_ls(ObLS *ls, ObLSGetMod mod) const;
  template <typename Function>
  int operate_ls(const share::ObLSID &ls_id, Function &fn);
  bool is_empty() const { return 0 == ATOMIC_LOAD(&ls_cnt_); }
  int64_t get_ls_count() const { return ATOMIC_LOAD(&ls_cnt_); }
  static TCRef &get_tcref()
  {
    static TCRef tcref(16);
    return tcref;
  }
private:
  OB_INLINE void free_ls(ObLS *ls) const;
  void del_ls_impl(ObLS *ls);
  int choose_preserve_ls(ObLS *left_ls, ObLS *right_ls, ObLS *&result_ls);
  int remove_duplicate_ls_in_linklist(ObLS *&head);
private:
  static const bool ENABLE_RECOVER_ALL_ZONE = false;
  bool is_inited_;
  int64_t tenant_id_;
  common::ObIAllocator *ls_allocator_;
  // total ls in current server of current tenant
  int64_t ls_cnt_;
  ObLS **ls_buckets_;
  const static int64_t BUCKETS_CNT = 1 << 8;
  common::ObQSyncLock *buckets_lock_;
};

//iterate all lss
class ObLSIterator
{
public:
  ObLSIterator();
  virtual ~ObLSIterator();
  virtual int get_next(ObLS *&ls);
  void reset();
  void set_ls_map(ObLSMap &ls_map, ObLSGetMod mod) {
    ls_map_ = &ls_map;
    mod_ = mod;
  }
  TO_STRING_KV("ls_count", lss_.count(), K_(bucket_pos), K_(array_idx));
private:
  common::ObArray<ObLS*> lss_;
  int64_t bucket_pos_;
  int64_t array_idx_;
  ObLSMap *ls_map_;
  ObLSGetMod mod_;
};

OB_INLINE void ObLSMap::free_ls(ObLS *ls) const
{
  int ret = OB_SUCCESS;
  ls->~ObLS();
  ls_allocator_->free(ls);
}

OB_INLINE void ObLSMap::revert_ls(ObLS *ls, ObLSGetMod mod) const
{
  if (OB_NOT_NULL(ls)) {
    if (ls->get_ref_mgr().dec(mod)) {
      STORAGE_LOG(INFO, "ObLSMap free ls", KP(ls), K(mod), K(ls->get_ls_id()));
      free_ls(ls);
    }
  }
}

template <typename Function>
int ObLSMap::operate_ls(const share::ObLSID &ls_id,
                               Function &fn)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObLSMap not init", K(ret), K(ls_id));
  } else {
    const int64_t pos = ls_id.hash() % BUCKETS_CNT;
    ObLS *ls = ls_buckets_[pos];
    common::ObQSyncLockReadGuard bucket_guard(buckets_lock_[pos]);
    while (OB_NOT_NULL(ls)) {
      if (ls->get_ls_id() == ls_id) {
        break;
      } else {
        ls = static_cast<ObLS *>(ls->next_);
      }
    }
    if (OB_ISNULL(ls)) {
      ret = OB_LS_NOT_EXIST;
    } else {
      ret = fn(ls_id, ls);
    }
  }
  return ret;
}

}
}
#endif // OCEANBASE_STORAGE_OB_LS_MAP_
