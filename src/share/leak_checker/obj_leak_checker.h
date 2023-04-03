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

#ifndef OCEANBASE_SHARE_OBJ_LEAK_CHECKER_H
#define OCEANBASE_SHARE_OBJ_LEAK_CHECKER_H

#include <unordered_map>
#include <vector>
#include <string>
#include "lib/ob_define.h"
#include "lib/lock/ob_spin_lock.h"

namespace oceanbase
{
namespace share
{

enum ObLeakCheckObjType
{
  LEAK_CHECK_OBJ_TABLET_HANDLE,
  LEAK_CHECK_OBJ_LS_HANDLE,
  LEAK_CHECK_OBJ_MAX_NUM
};

#define OBJ_LEAK_CHECKER ::oceanbase::share::ObObjLeakChecker::get_instance()

class ObObjLeakChecker final
{
public:
  // key_token -> lbt info 
  using ObLeakCheckObjMap = std::unordered_map<uint64_t, std::string>;
  using ObLeakCheckTenantObjVec = std::vector<ObLeakCheckObjMap>;
  // tenant_id -> ObLeakCheckTenantObjVec;
  using ObLeakCheckAllMap = std::unordered_map<uint64_t, ObLeakCheckTenantObjVec>;

  ObObjLeakChecker() : key_token_(0) {
  }
  ~ObObjLeakChecker() {}

  static ObObjLeakChecker &get_instance() {
    static ObObjLeakChecker instance_;
    return instance_;
  }

  template<typename T>
  void add(const uint64_t tenant_id, const ObLeakCheckObjType obj_type, const T *obj, uint64_t &key_token)
  {
    int ret = OB_SUCCESS;
    common::ObSpinLockGuard guard(lock_);
    key_token = key_token_++;

    int n = snprintf(lbt_buf_, 4096, "obj_type:%d obj:%p BT:%s", obj_type, obj, lbt());
    if (n <= 0) {
      abort();
    }
    auto it = map_.find(tenant_id);
    if (it == map_.end()) {
      ObLeakCheckTenantObjVec obj_vec;
      obj_vec.resize(LEAK_CHECK_OBJ_MAX_NUM);
      it = map_.insert({tenant_id, obj_vec}).first;
    }
    bool insert_ret = it->second[obj_type].insert({key_token, lbt_buf_}).second;
    ob_assert(insert_ret == true);
  }

  void del(const uint64_t tenant_id, const ObLeakCheckObjType obj_type, const uint64_t key_token)
  {
    common::ObSpinLockGuard guard(lock_);
    auto &obj_map = map_[tenant_id].at(obj_type);
    auto it = obj_map.find(key_token);
    ob_assert(it != obj_map.end());
    obj_map.erase(it);
  }

  void print_obj_leak(const uint64_t tenant_id, const ObLeakCheckObjType obj_type)
  {
    common::ObSpinLockGuard guard(lock_);
    auto it = map_.find(tenant_id);
    if (it != map_.end()) {
      if (LEAK_CHECK_OBJ_MAX_NUM == obj_type) {
        for (int64_t i = 0; i < LEAK_CHECK_OBJ_MAX_NUM; i++) {
          auto &obj_map = it->second.at(i);
          for (auto &pair : obj_map) {
            SHARE_LOG(INFO, "dump leak obj", K(pair.first), K(pair.second.c_str()));
          }
        }
      } else {
        auto &obj_map = it->second.at(obj_type);
        for (auto &pair : obj_map) {
          SHARE_LOG(INFO, "dump leak obj", K(pair.first), K(pair.second.c_str()));
        }
      }
    }
  }

private:
  ObSpinLock lock_;
  uint64_t key_token_;
  ObLeakCheckAllMap map_;
  char lbt_buf_[4096];
};



class ObObjLeakDebugNode final
{
public:
  ObObjLeakDebugNode() : is_inited_(false), key_token_(0) { }
  ~ObObjLeakDebugNode()
  {
    reset();
  }
  ObObjLeakDebugNode(const ObObjLeakDebugNode &) = delete;
  ObObjLeakDebugNode &operator=(const ObObjLeakDebugNode &) = delete;


  template<typename T>
  void init(const T *obj, const ObLeakCheckObjType obj_type, const uint64_t tenant_id)
  {
    if (is_inited_) {
      abort();
    } else if (nullptr == obj) {
      abort();
    } else {
      OBJ_LEAK_CHECKER.add(tenant_id, obj_type, obj, key_token_);
      tenant_id_ = tenant_id;
      obj_type_ = obj_type;
      is_inited_ = true;
    }
  }
  void reset()
  {
    if (is_inited_) {
      OBJ_LEAK_CHECKER.del(tenant_id_, obj_type_, key_token_);
      key_token_ = 0;
      is_inited_ = false;
    }
  }
  bool is_inited_;
  uint64_t tenant_id_;
  ObLeakCheckObjType obj_type_;

  uint64_t key_token_;
};

} // end share
} // end oceanbase

#ifdef ENABLE_OBJ_LEAK_CHECK
#define DEFINE_OBJ_LEAK_DEBUG_NODE(node) oceanbase::share::ObObjLeakDebugNode node
#define INIT_OBJ_LEAK_DEBUG_NODE(node, obj_ptr, obj_type, tenant_id)  node.init(obj_ptr, obj_type, tenant_id)

// if obj_type == LEAK_CHECK_OBJ_MAX_NUM, dump all type obj
#define PRINT_OBJ_LEAK(tenant_id, obj_type)                                       \
  {                                                                               \
    OBJ_LEAK_CHECKER.print_obj_leak(tenant_id, obj_type);                         \
  }

#else
#define DEFINE_OBJ_LEAK_DEBUG_NODE(node)
#define INIT_OBJ_LEAK_DEBUG_NODE(node, obj_ptr, desc, tenant_id)
#define PRINT_OBJ_LEAK(tenant_id, obj_type)
#endif



#endif // OCEANBASE_SHARE_OBJ_LEAK_CHECKER_H
