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

#ifndef OB_DI_CACHE_H_
#define OB_DI_CACHE_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <typeinfo>
#include "lib/ob_define.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/stat/ob_di_list.h"
#include "lib/container/ob_array.h"

namespace oceanbase
{
namespace common
{
class ObDISessionCollect : public ObDINode<ObDISessionCollect>
{
public:
  ObDISessionCollect();
  virtual ~ObDISessionCollect();
  void clean();
  TO_STRING_EMPTY();
  uint64_t session_id_;
  ObDiagnoseSessionInfo base_value_;
  DIRWLock lock_;
};

class ObDITenantCollect : public ObDINode<ObDITenantCollect>
{
public:
  ObDITenantCollect(ObIAllocator *allocator = NULL);
  virtual ~ObDITenantCollect();
  void clean();
  uint64_t tenant_id_;
  uint64_t last_access_time_;
  ObDiagnoseTenantInfo base_value_;
};

class ObDISessionCache
{
public:
  static ObDISessionCache &get_instance();
  void destroy();

  int get_all_diag_info(ObIArray<std::pair<uint64_t, ObDISessionCollect*> > &diag_infos);

  int get_the_diag_info(
      uint64_t session_id,
      ObDISessionCollect *&diag_infos);
  int get_node(uint64_t session_id, ObDISessionCollect *&session_collect);
private:
  static const int64_t MAX_SESSION_COLLECT_NUM = OB_MAX_SERVER_SESSION_CNT / 4;
  STATIC_ASSERT((MAX_SESSION_COLLECT_NUM > 4 * 1024 && MAX_SESSION_COLLECT_NUM < 10 * 1024),
      "unexpected MAX_SESSION_COLLECT_NUM");
private:
  struct ObSessionBucket
  {
    ObSessionBucket() : list_(), lock_()
    {
    }
    int get_the_node(const uint64_t hash_value, ObDISessionCollect *&value)
    {
      int ret = OB_ENTRY_NOT_EXIST;
      ObDISessionCollect *head = list_.get_header();
      ObDISessionCollect *node = list_.get_first();
      while (head != node && NULL != node) {
        if (hash_value == node->session_id_) {
          value = node;
          ret = OB_SUCCESS;
          break;
        } else {
          node = node->get_next();
        }
      }
      return ret;
    }
    ObDIList<ObDISessionCollect> list_;
    DIRWLock lock_;
  };
  ObDISessionCache();
  virtual ~ObDISessionCache();
  ObSessionBucket di_map_[MAX_SESSION_COLLECT_NUM];
  ObDISessionCollect collects_[MAX_SESSION_COLLECT_NUM];
};

struct ObTenantBucket
{
  ObTenantBucket() : list_()
  {
  }
  int get_the_node(const uint64_t hash_value, ObDITenantCollect *&value)
  {
    int ret = OB_ENTRY_NOT_EXIST;
    ObDITenantCollect *head = list_.get_header();
    ObDITenantCollect *node = list_.get_first();
    while (head != node && NULL != node) {
      if (hash_value == node->tenant_id_) {
        value = node;
        ret = OB_SUCCESS;
        break;
      } else {
        node = node->get_next();
      }
    }
    return ret;
  }
  ObDIList<ObDITenantCollect> list_;
};

struct AddWaitEvent
{
  AddWaitEvent() {}
  void operator()(ObDiagnoseTenantInfo &base_info, const ObDiagnoseTenantInfo &info)
  {
    base_info.add_wait_event(info);
  }
};

struct AddStatEvent
{
  AddStatEvent() {}
  void operator()(ObDiagnoseTenantInfo &base_info, const ObDiagnoseTenantInfo &info)
  {
    base_info.add_stat_event(info);
  }
};

struct AddLatchStat
{
  AddLatchStat() {}
  void operator()(ObDiagnoseTenantInfo &base_info, const ObDiagnoseTenantInfo &info)
  {
    base_info.add_latch_stat(info);
  }
};

struct ObDIBaseTenantCacheIterator
{
public:
  ObDIBaseTenantCacheIterator();
  ObDIBaseTenantCacheIterator(const ObDITenantCollect *collects, const int64_t count);
  ~ObDIBaseTenantCacheIterator() = default;
  bool is_valid() const { return nullptr != collects_ && count_ > 0; }
  int init(const ObDITenantCollect *collects, const int64_t count);
  int get_next(const ObDITenantCollect *&collect);
  void reset() { MEMSET(this, 0, sizeof(ObDIBaseTenantCacheIterator)); }
private:
  const ObDITenantCollect *collects_;
  int64_t count_;
  int64_t idx_;
};

template <uint64_t MAX_TENANT_NODE_NUM>
class ObDIBaseTenantCache
{
public:
  ObDIBaseTenantCache();
  virtual ~ObDIBaseTenantCache();
  void get_the_diag_info(
      uint64_t tenant_id,
      ObDiagnoseTenantInfo &diag_infos);
  ObDITenantCollect *get_node(uint64_t tenant_id, const bool replace = false);
  ObDITenantCollect *get_sys_tenant_node() {return &collects_[0];}
  int get_cache_iter(ObDIBaseTenantCacheIterator &cache_iter) const;
private:
  ObDITenantCollect *add_node(uint64_t tenant_id, const bool replace);
  int del_node(uint64_t tenant_id);
  ObTenantBucket di_map_[MAX_TENANT_NODE_NUM];
  ObDITenantCollect collects_[MAX_TENANT_NODE_NUM];
  uint64_t last_access_time_;
};


class ObDIThreadTenantCache: public ObDINode<ObDIThreadTenantCache>
{
public:
  static const int64_t DEFAULT_TENANT_NUM = 4;
  static const int64_t MAX_TENANT_NUM_PER_SERVER = 64;

  ObDIThreadTenantCache();
  virtual ~ObDIThreadTenantCache();
  void get_the_diag_info(
      uint64_t tenant_id,
      ObDiagnoseTenantInfo &diag_infos);
  int get_node(uint64_t tenant_id, ObDITenantCollect *&tenant_collect);
  ObDITenantCollect *get_sys_tenant_node() { return tenant_cache_.get_sys_tenant_node(); }
  int get_cache_iter(ObDIBaseTenantCacheIterator &cache_iter, const bool extend = false) const;
private:
  ObDIBaseTenantCache<DEFAULT_TENANT_NUM> tenant_cache_;
  ObDIBaseTenantCache<MAX_TENANT_NUM_PER_SERVER> *extend_tenant_cache_;
};

class ObDIGlobalTenantCache
{
public:
  static ObDIGlobalTenantCache &get_instance();

  void link(ObDIThreadTenantCache *node);
  void unlink(ObDIThreadTenantCache *node);
  int get_all_wait_event(
      ObIAllocator &allocator,
      ObIArray<std::pair<uint64_t, ObDiagnoseTenantInfo*> > &diag_infos);
  int get_all_stat_event(
      ObIAllocator &allocator,
      ObIArray<std::pair<uint64_t, ObDiagnoseTenantInfo*> > &diag_infos);
  int get_all_latch_stat(
      ObIAllocator &allocator,
      ObIArray<std::pair<uint64_t, ObDiagnoseTenantInfo*> > &diag_infos);
  int get_the_diag_info(
      uint64_t tenant_id,
      ObDiagnoseTenantInfo &diag_infos);
  int add_tenant_collect(const ObDITenantCollect &collect); // for switch tenant
private:
  ObDIGlobalTenantCache();
  virtual ~ObDIGlobalTenantCache();
  template<class _callback>
  int get_all_diag_info(ObIAllocator &allocator,
      ObIArray<std::pair<uint64_t, ObDiagnoseTenantInfo*> > &diag_infos, _callback &callback);
  template <class CALLBACK_FUNC>
  int get_tenant_stat(ObIAllocator &allocator,
      ObDIBaseTenantCacheIterator &cache_iter,
      CALLBACK_FUNC &callback,
      ObTenantBucket *di_map,
      const int64_t di_map_bucket_num);
  int add_tenant_stat_(ObDIBaseTenantCacheIterator &cache_iter);
  int add_tenant_collect_(const ObDITenantCollect &collect);
private:
  ObDIList<ObDIThreadTenantCache> list_;
  int64_t cnt_;
  DIRWLock lock_;
  ObDIBaseTenantCache<OB_MAX_SERVER_TENANT_CNT> unlinked_tenant_cache_;
};

//////////////////////////////////////////////////////////////////////////////////////////

template <uint64_t MAX_TENANT_NODE_NUM>
ObDIBaseTenantCache<MAX_TENANT_NODE_NUM>::ObDIBaseTenantCache()
  : di_map_(),
    collects_(),
    last_access_time_(0)
{
  collects_[0].tenant_id_ = OB_SYS_TENANT_ID;
  collects_[0].last_access_time_ = 1;
  ObTenantBucket &bucket = di_map_[OB_SYS_TENANT_ID % MAX_TENANT_NODE_NUM];
  bucket.list_.add_last(&collects_[0]);
}

template <uint64_t MAX_TENANT_NODE_NUM>
ObDIBaseTenantCache<MAX_TENANT_NODE_NUM>::~ObDIBaseTenantCache()
{
}

template <uint64_t MAX_TENANT_NODE_NUM>
ObDITenantCollect *ObDIBaseTenantCache<MAX_TENANT_NODE_NUM>::get_node(uint64_t tenant_id, const bool replace)
{
  int ret = OB_SUCCESS;
  ObDITenantCollect *tenant_collect = nullptr;
  ObTenantBucket &bucket = di_map_[tenant_id % MAX_TENANT_NODE_NUM];
  if (OB_SUCCESS == (ret = bucket.get_the_node(tenant_id, tenant_collect))) {
    last_access_time_++;
    tenant_collect->last_access_time_ = last_access_time_;
  } else {
    tenant_collect = add_node(tenant_id, replace);
  }
  return tenant_collect;
}
template <uint64_t MAX_TENANT_NODE_NUM>
int ObDIBaseTenantCache<MAX_TENANT_NODE_NUM>::get_cache_iter(ObDIBaseTenantCacheIterator &cache_iter) const
{
  cache_iter.reset();
  return cache_iter.init(collects_, MAX_TENANT_NODE_NUM);
}

template <uint64_t MAX_TENANT_NODE_NUM>
ObDITenantCollect *ObDIBaseTenantCache<MAX_TENANT_NODE_NUM>::add_node(uint64_t tenant_id, const bool replace)
{
  ObDITenantCollect *tenant_collect = nullptr;
  int64_t pos = -1;
  uint64_t last_access_time = last_access_time_;
  for (int64_t i = 1; i < MAX_TENANT_NODE_NUM; i++) {//0 is reserve for sys
    if (0 == collects_[i].last_access_time_) {
      pos = i;
      break;
    } else if (replace) {
      if (collects_[i].last_access_time_ < last_access_time) {
        pos = i;
        last_access_time = collects_[i].last_access_time_;
      }
    }
  }
  if (-1 != pos) {
    if (0 != collects_[pos].last_access_time_) {
      del_node(collects_[pos].tenant_id_);
    }
    last_access_time_++;
    collects_[pos].tenant_id_ = tenant_id;
    collects_[pos].last_access_time_ = last_access_time_;
    collects_[pos].base_value_.reset();
    ObTenantBucket &bucket = di_map_[tenant_id % MAX_TENANT_NODE_NUM];
    bucket.list_.add_last(&collects_[pos]);
    tenant_collect = &collects_[pos];
  }
  return tenant_collect;
}

template <uint64_t MAX_TENANT_NODE_NUM>
int ObDIBaseTenantCache<MAX_TENANT_NODE_NUM>::del_node(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObTenantBucket &bucket = di_map_[tenant_id % MAX_TENANT_NODE_NUM];
  ObDITenantCollect *collect = NULL;
  if (OB_SUCCESS == (ret = bucket.get_the_node(tenant_id, collect))) {
    bucket.list_.remove(collect);
    collect->last_access_time_ = 0;
  }
  return ret;
}

template <uint64_t MAX_TENANT_NODE_NUM>
void ObDIBaseTenantCache<MAX_TENANT_NODE_NUM>::get_the_diag_info(
  uint64_t tenant_id,
  ObDiagnoseTenantInfo &diag_infos)
{
  for (int64_t i = 0; i < MAX_TENANT_NODE_NUM; i++) {
    if (0 != collects_[i].last_access_time_ && tenant_id == collects_[i].tenant_id_) {
      diag_infos.add(collects_[i].base_value_);
      break;
    }
  }
}

}// end namespace common
}
#endif /* OB_DI_CACHE_H_ */
