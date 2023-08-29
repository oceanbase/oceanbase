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

#ifndef OCEANBASE_COMMON_OB_TENANT_MGR_
#define OCEANBASE_COMMON_OB_TENANT_MGR_

#include "lib/allocator/page_arena.h"   // ObArenaAllocator
#include "lib/lock/ob_spin_lock.h"      // SpinRWLock
#include "lib/task/ob_timer.h"          // ObTimerTask
#include "lib/queue/ob_fixed_queue.h"   // ObFixedQueue
#include "share/ob_define.h"

namespace oceanbase
{
namespace common
{

class ObVirtualTenantManager
{
  static const int64_t BUCKET_NUM = 16;
public:
  static ObVirtualTenantManager &get_instance();
  int init();
  bool is_inited() const { return is_inited_; }
  void destroy();
  // get all the virtual tenant id.
  // @param[out] tenant_ids, return all the virtual tenant id.
  virtual int get_all_tenant_id(ObIArray<uint64_t> &tenant_ids) const;
  // add a tenant record.
  // @param[in] tenant_id, the tenant will be add.
  int add_tenant(const uint64_t tenant_id);
  // delete a tenant record.
  // @param[in] tenant_id, the tenant will be delete.
  int del_tenant(const uint64_t tenant_id);
  // check whether a tenant exist or not.
  // @param[in] tenant_id, the tenant will be check.
  virtual bool has_tenant(const uint64_t tenant_id) const;
  // set the tenant memory limit.
  // @param[in] tenant_id, which tenant's memory limit will be set.
  // @param[in] lower_limit, the min tenant memory limit will be set.
  // @param[out] upper_limit, the max tenant memory limit will be set.
  int set_tenant_mem_limit(const uint64_t tenant_id,
                           const int64_t lower_limit,
                           const int64_t upper_limit);
  // get a tenant memory limit.
  // @param[in] tenant_id, which tenant's limit will be get.
  // @param[out] lower_limit, the min tenant memory limit.
  // @param[out] upper_limit, the max tenant memory limit.
  virtual int get_tenant_mem_limit(const uint64_t tenant_id,
                                   int64_t &lower_limit,
                                   int64_t &upper_limit) const;
  int print_tenant_usage(char *print_buf,
                         int64_t buf_len,
                         int64_t &pos);
  // unused now.
  void reload_config();
private:
  class ObTenantInfo : public ObDLinkBase<ObTenantInfo>
  {
  public:
    ObTenantInfo();
    virtual ~ObTenantInfo() { reset(); }
    void reset();

    TO_STRING_KV(K_(tenant_id), K_(mem_lower_limit), K_(mem_upper_limit), K_(is_loaded));
  public:
    uint64_t tenant_id_;
    int64_t mem_lower_limit_;
    int64_t mem_upper_limit_;
    bool is_loaded_;
  };

  struct ObTenantBucket
  {
    ObDList<ObTenantInfo> info_list_;
    SpinRWLock lock_;
    ObTenantBucket() : info_list_(), lock_(ObLatchIds::DEFAULT_BUCKET_LOCK)
    {
    }
    int get_the_node(const uint64_t tenant_id, ObTenantInfo *&node)
    {
      int ret = OB_ENTRY_NOT_EXIST;
      ObTenantInfo *head = info_list_.get_header();
      node = info_list_.get_first();
      while (head != node && NULL != node) {
        if (tenant_id == node->tenant_id_) {
          ret = OB_SUCCESS;
          break;
        } else {
          node = node->get_next();
        }
      }
      return ret;
    }
  };
  int print_tenant_usage_(ObTenantInfo &node,
                          char *print_buf,
                          int64_t buf_len,
                          int64_t &pos);
  int get_kv_cache_mem_(const uint64_t tenant_id,
                        int64_t &kv_cache_mem);
  ObVirtualTenantManager();
  virtual ~ObVirtualTenantManager();
  int init_tenant_map_();
private:
  // a bucket map, every bucket content a ObTenantInfo list and a lock.
  ObTenantBucket *tenant_map_;
  // allocator and memattr for ObTenantInfo memory alloc
  ObArenaAllocator allocator_;
  ObMemAttr memattr_;

  bool is_inited_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObVirtualTenantManager);
};

class ObTenantCpuShare
{
public:
  /* Return value: The number of px threads assigned to tenant_id tenant */
  static int64_t calc_px_pool_share(uint64_t tenant_id, int64_t min_cpu);
};

}
}


#endif //OCEANBASE_COMMON_OB_TENANT_MGR_
