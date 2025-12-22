/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_DIAGNOSTIC_INFO_CONTAINER_H_
#define OB_DIAGNOSTIC_INFO_CONTAINER_H_

#include "lib/stat/ob_diagnostic_info_summary.h"
#include "lib/objectpool/ob_server_object_pool.h"

namespace oceanbase
{
namespace common
{

class ObRunningDiagnosticInfoContainer;
class ObDiagnosticInfoContainer;
class ObDISessionCollect;

typedef common::ObServerObjectPool<ObWaitEventStatArray> ObWaitEventPool;

class ObDiagnosticInfos
{
public:
  typedef ObLinkHashMap<SessionID, ObDiagnosticInfo,
      DiagnosticInfoValueAlloc<ObDiagnosticInfo, SessionID>>
      ObDiInfos;
  ObDiagnosticInfos(DiagnosticInfoValueAlloc<ObDiagnosticInfo, SessionID> &value_alloc);
  // no need to call di_infos_.reset() because every di element should call del() before this
  // destructor.
  ~ObDiagnosticInfos() = default;
  DISABLE_COPY_ASSIGN(ObDiagnosticInfos);
  int init(int64_t tenant_id);
  int allocate_diagnostic_info(int64_t tenant_id, int64_t group_id, int64_t session_id,
      ObWaitEventPool &pool, ObDiagnosticInfo *&di_info);
  int delete_diagnostic_info(const ObDiagnosticInfo *di_info);
  int inc_ref(const ObDiagnosticInfo *di_info);
  void dec_ref(ObDiagnosticInfo *di_info);
  int for_each(const std::function<bool(const SessionID &, ObDiagnosticInfo *)> &fn);
  int64_t size() const
  {
    return di_infos_.size();
  };
  int64_t get_alloc_conut() const
  {
    return di_infos_.get_alloc_handle().get_alloc_count();
  }
  int64_t get_rpc_size() const
  {
    return rpc_size_;
  }
  bool is_rpc_request(int64_t session_id) const
  {
    return session_id & (uint64_t)1 << 60;
  }
  void reset()
  {
    di_infos_.destroy();
  }
  TO_STRING_KV(K(size()));

private:
  friend class ObRunningDiagnosticInfoContainer;
  int get_session_diag_info(int64_t session_id, ObDISessionCollect &diag_info);
  ObDiInfos di_infos_;
  int64_t rpc_size_;
};

class ObRunningDiagnosticInfoContainer
{
public:
  explicit ObRunningDiagnosticInfoContainer(
      int64_t tenant_id, DiagnosticInfoValueAlloc<ObDiagnosticInfo, SessionID> value_alloc);
  ~ObRunningDiagnosticInfoContainer();
  DISABLE_COPY_ASSIGN(ObRunningDiagnosticInfoContainer);
  int init(int cpu_cnt);
  int allocate_diagnostic_info(int64_t tenant_id, int64_t group_id, int64_t session_id,
      ObWaitEventPool &pool, ObDiagnosticInfo *&di_info);
  int delete_diagnostic_info(const ObDiagnosticInfo *di_info);
  int inc_ref(const ObDiagnosticInfo *di_info);
  int dec_ref(ObDiagnosticInfo *di_info);
  int for_each(const std::function<bool(const SessionID &, ObDiagnosticInfo *)> &fn);
  int64_t size() const;
  int64_t get_rpc_size() const;
  TO_STRING_KV(K_(tenant_id), K_(slot_count), K_(slot_mask), K_(is_inited));
  int get_session_diag_info(int64_t session_id, ObDISessionCollect &diag_info);
  void reset();
  int64_t get_value_alloc_count() const;

private:
  int64_t tenant_id_;
  int64_t slot_mask_;
  int64_t slot_count_;
  bool is_inited_;
  DiagnosticInfoValueAlloc<ObDiagnosticInfo, SessionID> value_alloc_;
  ObDiagnosticInfos *buffer_;
};

class ObDiagnosticInfoContainer
{
public:
  friend class ObTenantDiagnosticInfoSummaryGuard;
  friend class ObBackGroundSessionGuard;
  friend class ObLocalDiagnosticInfo;
  explicit ObDiagnosticInfoContainer(int64_t tenant_id, int64_t di_upper_limit = MAX_DI_PER_TENANT);
  ~ObDiagnosticInfoContainer()
  {
    stop();
  };
  DISABLE_COPY_ASSIGN(ObDiagnosticInfoContainer);
  int init(int64_t cpu_cnt, bool is_global = false);
  // NOTICE: after acquire, need to revert and return di_info accordingly.
  int acquire_diagnostic_info(int64_t tenant_id, int64_t group_id, int64_t session_id,
      ObDiagnosticInfo *&di_info, bool using_cache = false);
  void revert_diagnostic_info(ObDiagnosticInfo *di);
  int for_each_running_di(std::function<bool(const SessionID &, ObDiagnosticInfo *)> &fn)
  {
    return runnings_.for_each(fn);
  }
  int for_each_and_delay_release_ref(std::function<bool(const SessionID &, ObDiagnosticInfo *)> &fn, ObArray<ObDiagnosticInfo *>&di_array);
  int release_diagnostic_info(ObArray<ObDiagnosticInfo *>&di_array);
  bool is_inited() const
  {
    return is_inited_;
  }
  bool is_global_container() const
  {
    return is_global_container_;
  }
  void set_global_container()
  {
    is_global_container_ = true;
  }
  static int mtl_new(ObDiagnosticInfoContainer *&container);
  static int mtl_init(ObDiagnosticInfoContainer *&container);
  static void mtl_wait(ObDiagnosticInfoContainer *&container);
  static void mtl_destroy(ObDiagnosticInfoContainer *&container);
  TO_STRING_KV(K_(is_inited), K_(tenant_id), K_(runnings), K_(summarys), K_(cache));

  // WARN: only use this on observer bootstrap phase.
  static ObDiagnosticInfoContainer *get_global_di_container()
  {
    static ObDiagnosticInfoContainer dic(OB_SYS_TENANT_ID, MAX_DI_PER_TENANT * 4);
    return &dic;
  }
  // used to reset global di container when observer elegant exit.
  static void clear_global_di_container()
  {
    if (get_global_di_container()->is_inited()) {
      get_global_di_container()->stop();
      get_global_di_container()->clear_di_cache();
      get_global_di_container()->summarys_.reset();
      get_global_di_container()->runnings_.reset();
      COMMON_LOG(INFO, "clear global di container");
    }
  }
  static ObDiExperimentalFeatureFlags &get_di_experimental_feature_flag()
  {
    static ObDiExperimentalFeatureFlags flags;
    return flags;
  }
  ObBaseDiagnosticInfoSummary &get_base_summary()
  {
    return summarys_;
  }
  int get_session_diag_info(int64_t session_id, ObDISessionCollect &diag_info);
  // only needed for global_di_container
  void purge_tenant_summary(int64_t tenant_id);
  int64_t get_running_size() const
  {
    return runnings_.size();
  };
  int64_t get_rpc_size() const
  {
    return runnings_.get_rpc_size();
  }
  void stop()
  {
    stop_ = true;
  };
  ObWaitEventPool &get_wait_event_pool()
  {
    return wait_event_pool_;
  };
  int return_di_to_cache(ObDiagnosticInfo *di_info);
  bool check_element_all_freed() const;
  void print_all_running_dis();

private:
  int aggregate_diagnostic_info_summary(ObDiagnosticInfo *di_info);
  int return_diagnostic_info(ObDiagnosticInfo *di_info);
  int push_element_to_cache();
  // inc ref in link hashmap
  int inc_ref(ObDiagnosticInfo *di)__attribute__((deprecated("pls use ObLocalDiagnosticInfo::inc_ref instead")));
  // di only take 1 ref for link_hash_map, when it deducted, erase it automatically.
  void dec_ref(ObDiagnosticInfo *di);
  void clear_di_cache();
  bool is_inited_;
  bool stop_;
  bool is_global_container_;
  int64_t tenant_id_;
  ObFixedClassAllocator<ObDiagnosticInfo> di_allocator_;
  ObFixedClassAllocator<ObDiagnosticInfoCollector> di_collector_allocator_;
  // NOTICE: do not alter order for below 4 elements(pool/summarys/runnings/cache).
  ObWaitEventPool wait_event_pool_;
  ObBaseDiagnosticInfoSummary summarys_;
  ObRunningDiagnosticInfoContainer runnings_;
  ObDiagnosticInfoCache<ObDiagnosticInfo> cache_;
};

} /* namespace common */
} /* namespace oceanbase */

#endif /* OB_DIAGNOSTIC_INFO_CONTAINER_H_ */