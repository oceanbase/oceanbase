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

namespace oceanbase {
namespace common {
class ObDISessionCollect : public ObDINode<ObDISessionCollect> {
public:
  ObDISessionCollect();
  virtual ~ObDISessionCollect();
  void clean();
  TO_STRING_EMPTY();
  uint64_t session_id_;
  ObDiagnoseSessionInfo base_value_;
  DIRWLock lock_;
};

class ObDITenantCollect : public ObDINode<ObDITenantCollect> {
public:
  ObDITenantCollect();
  virtual ~ObDITenantCollect();
  void clean();
  uint64_t tenant_id_;
  uint64_t last_access_time_;
  ObDiagnoseTenantInfo base_value_;
};

class ObDISessionCache {
public:
  static ObDISessionCache& get_instance();
  void destroy();

  int get_all_diag_info(ObIArray<std::pair<uint64_t, ObDISessionCollect*> >& diag_infos);

  int get_the_diag_info(uint64_t session_id, ObDISessionCollect*& diag_infos);
  int get_node(uint64_t session_id, ObDISessionCollect*& session_collect);

private:
  struct ObSessionBucket {
    ObSessionBucket() : list_(), lock_()
    {}
    int get_the_node(const uint64_t hash_value, ObDISessionCollect*& value)
    {
      int ret = OB_ENTRY_NOT_EXIST;
      ObDISessionCollect* head = list_.get_header();
      ObDISessionCollect* node = list_.get_first();
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
  ObSessionBucket di_map_[OB_MAX_SERVER_SESSION_CNT];
  ObDISessionCollect collects_[OB_MAX_SERVER_SESSION_CNT];
};

struct ObTenantBucket {
  ObTenantBucket() : list_()
  {}
  int get_the_node(const uint64_t hash_value, ObDITenantCollect*& value)
  {
    int ret = OB_ENTRY_NOT_EXIST;
    ObDITenantCollect* head = list_.get_header();
    ObDITenantCollect* node = list_.get_first();
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

struct AddWaitEvent {
  AddWaitEvent()
  {}
  void operator()(ObDiagnoseTenantInfo& base_info, const ObDiagnoseTenantInfo& info)
  {
    base_info.add_wait_event(info);
  }
};

struct AddStatEvent {
  AddStatEvent()
  {}
  void operator()(ObDiagnoseTenantInfo& base_info, const ObDiagnoseTenantInfo& info)
  {
    base_info.add_stat_event(info);
  }
};

struct AddLatchStat {
  AddLatchStat()
  {}
  void operator()(ObDiagnoseTenantInfo& base_info, const ObDiagnoseTenantInfo& info)
  {
    base_info.add_latch_stat(info);
  }
};

template <uint64_t MAX_TENANT_NODE_NUM>
class ObDIBaseTenantCacheIterator;

template <uint64_t MAX_TENANT_NODE_NUM>
class ObDIBaseTenantCache {
public:
  ObDIBaseTenantCache();
  virtual ~ObDIBaseTenantCache();
  void get_the_diag_info(uint64_t tenant_id, ObDiagnoseTenantInfo& diag_infos);
  int get_node(uint64_t tenant_id, ObDITenantCollect*& tenant_collect);
  ObDITenantCollect* get_sys_tenant_node()
  {
    return &collects_[0];
  }

private:
  friend class ObDIBaseTenantCacheIterator<MAX_TENANT_NODE_NUM>;
  int add_node(uint64_t tenant_id, ObDITenantCollect*& tenant_collect);
  int del_node(uint64_t tenant_id);
  ObTenantBucket di_map_[MAX_TENANT_NODE_NUM];
  ObDITenantCollect collects_[MAX_TENANT_NODE_NUM];
  uint64_t last_access_time_;
};

template <uint64_t MAX_TENANT_NODE_NUM>
class ObDIBaseTenantCacheIterator {
public:
  ObDIBaseTenantCacheIterator(const ObDIBaseTenantCache<MAX_TENANT_NODE_NUM>& tenant_cache);
  virtual ~ObDIBaseTenantCacheIterator();
  int get_next(const ObDITenantCollect*& collect);

private:
  const ObDIBaseTenantCache<MAX_TENANT_NODE_NUM>& tenant_cache_;
  int64_t collect_idx_;
  DISALLOW_COPY_AND_ASSIGN(ObDIBaseTenantCacheIterator);
};

class ObDIThreadTenantCache : public ObDINode<ObDIThreadTenantCache> {
public:
  static const int64_t DEFAULT_TENANT_NODE_NUM = 32;

  ObDIThreadTenantCache();
  virtual ~ObDIThreadTenantCache();
  void get_the_diag_info(uint64_t tenant_id, ObDiagnoseTenantInfo& diag_infos);
  int get_node(uint64_t tenant_id, ObDITenantCollect*& tenant_collect);
  ObDITenantCollect* get_sys_tenant_node()
  {
    return tenant_cache_.get_sys_tenant_node();
  }
  const ObDIBaseTenantCache<DEFAULT_TENANT_NODE_NUM>& get_tenant_cache() const
  {
    return tenant_cache_;
  }

private:
  ObDIBaseTenantCache<DEFAULT_TENANT_NODE_NUM> tenant_cache_;
};

class ObDIGlobalTenantCache {
public:
  static ObDIGlobalTenantCache& get_instance();

  void link(ObDIThreadTenantCache* node);
  void unlink(ObDIThreadTenantCache* node);
  int get_all_wait_event(ObIAllocator& allocator, ObIArray<std::pair<uint64_t, ObDiagnoseTenantInfo*> >& diag_infos);
  int get_all_stat_event(ObIAllocator& allocator, ObIArray<std::pair<uint64_t, ObDiagnoseTenantInfo*> >& diag_infos);
  int get_all_latch_stat(ObIAllocator& allocator, ObIArray<std::pair<uint64_t, ObDiagnoseTenantInfo*> >& diag_infos);
  int get_the_diag_info(uint64_t tenant_id, ObDiagnoseTenantInfo& diag_infos);

private:
  ObDIGlobalTenantCache();
  virtual ~ObDIGlobalTenantCache();
  template <class _callback>
  int get_all_diag_info(
      ObIAllocator& allocator, ObIArray<std::pair<uint64_t, ObDiagnoseTenantInfo*> >& diag_infos, _callback& callback);
  template <uint64_t MAX_TENANT_NODE_NUM, class CALLBACK_FUNC>
  int get_tenant_stat(ObIAllocator& allocator, const ObDIBaseTenantCache<MAX_TENANT_NODE_NUM>& tenant_cache,
      CALLBACK_FUNC& callback, ObTenantBucket* di_map, const int64_t di_map_bucket_num);
  int add_tenant_stat(ObDIThreadTenantCache& node);

private:
  ObDIList<ObDIThreadTenantCache> list_;
  int64_t cnt_;
  DIRWLock lock_;
  ObDIBaseTenantCache<OB_MAX_SERVER_TENANT_CNT> unlinked_tenant_cache_;
};

//////////////////////////////////////////////////////////////////////////////////////////

template <uint64_t MAX_TENANT_NODE_NUM>
ObDIBaseTenantCache<MAX_TENANT_NODE_NUM>::ObDIBaseTenantCache() : di_map_(), collects_(), last_access_time_(0)
{
  collects_[0].tenant_id_ = OB_SYS_TENANT_ID;
  collects_[0].last_access_time_ = 1;
  ObTenantBucket& bucket = di_map_[OB_SYS_TENANT_ID % MAX_TENANT_NODE_NUM];
  bucket.list_.add_last(&collects_[0]);
}

template <uint64_t MAX_TENANT_NODE_NUM>
ObDIBaseTenantCache<MAX_TENANT_NODE_NUM>::~ObDIBaseTenantCache()
{}

template <uint64_t MAX_TENANT_NODE_NUM>
int ObDIBaseTenantCache<MAX_TENANT_NODE_NUM>::get_node(uint64_t tenant_id, ObDITenantCollect*& tenant_collect)
{
  int ret = OB_SUCCESS;
  ObTenantBucket& bucket = di_map_[tenant_id % MAX_TENANT_NODE_NUM];
  if (OB_SUCCESS == (ret = bucket.get_the_node(tenant_id, tenant_collect))) {
    last_access_time_++;
    tenant_collect->last_access_time_ = last_access_time_;
  } else {
    ret = add_node(tenant_id, tenant_collect);
  }
  return ret;
}

template <uint64_t MAX_TENANT_NODE_NUM>
int ObDIBaseTenantCache<MAX_TENANT_NODE_NUM>::add_node(uint64_t tenant_id, ObDITenantCollect*& tenant_collect)
{
  int ret = OB_SUCCESS;
  int64_t pos = -1;
  uint64_t last_access_time = last_access_time_;
  for (int64_t i = 1; i < MAX_TENANT_NODE_NUM; i++) {  // 0 is reserve for sys
    if (0 == collects_[i].last_access_time_) {
      pos = i;
      break;
    } else {
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
    ObTenantBucket& bucket = di_map_[tenant_id % MAX_TENANT_NODE_NUM];
    bucket.list_.add_last(&collects_[pos]);
    tenant_collect = &collects_[pos];
  }
  return ret;
}

template <uint64_t MAX_TENANT_NODE_NUM>
int ObDIBaseTenantCache<MAX_TENANT_NODE_NUM>::del_node(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObTenantBucket& bucket = di_map_[tenant_id % MAX_TENANT_NODE_NUM];
  ObDITenantCollect* collect = NULL;
  if (OB_SUCCESS == (ret = bucket.get_the_node(tenant_id, collect))) {
    bucket.list_.remove(collect);
    collect->last_access_time_ = 0;
  }
  return ret;
}

template <uint64_t MAX_TENANT_NODE_NUM>
void ObDIBaseTenantCache<MAX_TENANT_NODE_NUM>::get_the_diag_info(uint64_t tenant_id, ObDiagnoseTenantInfo& diag_infos)
{
  for (int64_t i = 0; i < MAX_TENANT_NODE_NUM; i++) {
    if (0 != collects_[i].last_access_time_ && tenant_id == collects_[i].tenant_id_) {
      diag_infos.add(collects_[i].base_value_);
      break;
    }
  }
}

template <uint64_t MAX_TENANT_NODE_NUM>
ObDIBaseTenantCacheIterator<MAX_TENANT_NODE_NUM>::ObDIBaseTenantCacheIterator(
    const ObDIBaseTenantCache<MAX_TENANT_NODE_NUM>& tenant_cache)
    : tenant_cache_(tenant_cache), collect_idx_(0)
{}

template <uint64_t MAX_TENANT_NODE_NUM>
ObDIBaseTenantCacheIterator<MAX_TENANT_NODE_NUM>::~ObDIBaseTenantCacheIterator()
{}

template <uint64_t MAX_TENANT_NODE_NUM>
int ObDIBaseTenantCacheIterator<MAX_TENANT_NODE_NUM>::get_next(const ObDITenantCollect*& collect)
{
  int ret = OB_SUCCESS;
  collect = NULL;

  while (NULL == collect && OB_SUCC(ret)) {
    if (collect_idx_ >= MAX_TENANT_NODE_NUM) {
      ret = OB_ITER_END;
    } else if (0 != tenant_cache_.collects_[collect_idx_].last_access_time_) {
      collect = &tenant_cache_.collects_[collect_idx_];
    }
    ++collect_idx_;
  }
  return ret;
}

}  // end namespace common
}  // namespace oceanbase
#endif /* OB_DI_CACHE_H_ */
