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

#ifndef OB_DIAGNOSTIC_SUMMARY_H_
#define OB_DIAGNOSTIC_SUMMARY_H_

#include "lib/stat/ob_diagnostic_info.h"
#include "lib/hash/ob_link_hashmap.h"
#include <functional>

namespace oceanbase
{
namespace common
{

class ObTenantDiagnosticInfoSummaryGuard;
class ObDiagnoseTenantInfo;

class ObDiagnosticInfoSlot
{
public:
  ObDiagnosticInfoSlot();
  ~ObDiagnosticInfoSlot() = default;
  DISABLE_COPY_ASSIGN(ObDiagnosticInfoSlot);
  void accumulate_diagnostic_info(ObDiagnosticInfo &di);
  const ObWaitEventStatArray &get_events() const
  {
    return events_;
  }
  ObStatEventAddStatArray &get_stats()
  {
    return stats_;
  }
  ObStatLatchArray &get_all_latch_stat()
  {
    return latch_stats_;
  };
  void atomic_add_stat(ObStatEventIds::ObStatEventIdEnum stat_no, int64_t value)
  {
    ObStatEventAddStat *stat = stats_.get(stat_no);
    if (OB_NOT_NULL(stat)) {
      stat->atomic_add(value);
    }
  }
  ObLatchStat *get_latch_stat(int64_t latch_id)
  {
    OB_ASSERT(latch_id >= 0 && latch_id < ObLatchIds::LATCH_END);
    return latch_stats_.get(latch_id);
  }
  TO_STRING_EMPTY();

private:
  void inner_accumuate_diagnostic_info(ObDiagnosticInfo &di);
  ObWaitEventStatArray events_;
  ObStatEventAddStatArray stats_;
  ObStatLatchArray latch_stats_;
  ObSpinLock lock_;  // only lock for write
};

class ObDiagnosticKey
{
public:
  explicit ObDiagnosticKey(int64_t tenant_id, int64_t group_id)
      : tenant_id_(tenant_id), group_id_(group_id){};
  ObDiagnosticKey() : tenant_id_(0), group_id_(0)
  {}
  ~ObDiagnosticKey() = default;
  void operator=(const ObDiagnosticKey &key)
  {
    this->tenant_id_ = key.tenant_id_;
    this->group_id_ = key.group_id_;
  }
  ObDiagnosticKey(const ObDiagnosticKey &key)
  {
    this->tenant_id_ = key.tenant_id_;
    this->group_id_ = key.group_id_;
  }

  bool operator==(const ObDiagnosticKey &other) const
  {
    return this->compare(other) == 0;
  }
  bool operator!=(const ObDiagnosticKey &other) const
  {
    return this->compare(other) != 0;
  }

  uint64_t hash() const
  {
    uint64_t hash_val = 0;
    hash_val = common::murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
    hash_val = common::murmurhash(&group_id_, sizeof(group_id_), hash_val);
    return hash_val;
  }

  int compare(const ObDiagnosticKey &other) const
  {
    if (other.tenant_id_ < tenant_id_ ||
        (other.tenant_id_ == tenant_id_ && other.group_id_ < group_id_)) {
      return 1;
    } else if (other.tenant_id_ == tenant_id_ && other.group_id_ == group_id_) {
      return 0;
    } else {
      return -1;
    }
  }
  int64_t get_tenant_id() const { return tenant_id_; }

  TO_STRING_KV(K_(tenant_id), K_(group_id));

private:
  int64_t tenant_id_;
  int64_t group_id_;
};

typedef common::LinkHashValue<ObDiagnosticKey> DICollectorHashValue;
class ObDiagnosticInfoCollector : public DICollectorHashValue
{
public:
  explicit ObDiagnosticInfoCollector();
  ~ObDiagnosticInfoCollector();
  DISABLE_COPY_ASSIGN(ObDiagnosticInfoCollector);
  int init(int cpu_cnt, int64_t tenant_id, int64_t group_id);
  int add_diagnostic_info(ObDiagnosticInfo &di);
  int64_t get_tenant_id() const
  {
    return tenant_id_;
  };
  int64_t get_group_id() const
  {
    return group_id_;
  };
  void get_all_events(ObWaitEventStatArray &arr) const;
  void get_all_add_stats(ObStatEventAddStatArray &arr) const;
  void get_all_latch_stat(ObLatchStatArray &arr) const;
  ObDiagnosticInfoSlot *get_slot(int64_t session_id);

  TO_STRING_KV(K_(tenant_id), K_(group_id), K_(slot_mask), K_(is_inited), K(get_uref()), K(get_href()));

private:
  int64_t tenant_id_;
  int64_t group_id_;
  int64_t slot_mask_;
  int64_t slot_count_;
  bool is_inited_;
  ObDiagnosticInfoSlot *di_info_bundle_;
};

class ObBaseDiagnosticInfoSummary
{
public:
  friend class ObTenantDiagnosticInfoSummaryGuard;
  friend class ObDiagnosticInfoContainer;
  explicit ObBaseDiagnosticInfoSummary(
      DiagnosticInfoValueAlloc<ObDiagnosticInfoCollector, ObDiagnosticKey> value_alloc)
      : collectors_(value_alloc), mutex_(ObLatchIds::DI_COLLECTOR_LOCK), cpu_cnt_(1), is_inited_(false)
  {}

  ~ObBaseDiagnosticInfoSummary()
  {
    reset();
  }
  int init(int64_t cpu_cnt);
  void reset()
  {
    collectors_.destroy();
  }
  int add_diagnostic_info(ObDiagnosticInfo &di);
  int get_tenant_event(int64_t tenant_id, ObWaitEventStatArray &arr);
  int get_tenant_add_stats(int64_t tenant_id, ObStatEventAddStatArray &arr);
  int get_tenant_latch_stat(int64_t tenant_id, ObLatchStatArray &arr);
  int for_each_group(int64_t tenant_id, std::function<void(int64_t, const ObDiagnoseTenantInfo &)> fn);

  int get_group_event(int64_t group_id, ObWaitEventStatArray &arr);
  int get_group_add_stats(int64_t group_id, ObStatEventAddStatArray &arr);
  int remove_if(std::function<bool(const ObDiagnosticKey&, ObDiagnosticInfoCollector*)> fn);
  int64_t get_value_alloc_count() const { return collectors_.get_alloc_handle().get_alloc_count(); }
  DISABLE_COPY_ASSIGN(ObBaseDiagnosticInfoSummary);
  TO_STRING_KV(K_(cpu_cnt));

private:
  int get_di_slot(
      int64_t tenant_id, int64_t group_id, int64_t session_id, ObDiagnosticInfoSlot *&slot);
  int get_di_collector(int64_t tenant_id, int64_t group_id, ObDiagnosticInfoCollector *&collector);
  typedef ObLinkHashMap<ObDiagnosticKey, ObDiagnosticInfoCollector,
      DiagnosticInfoValueAlloc<ObDiagnosticInfoCollector, ObDiagnosticKey>>
      SummaryMap;
  SummaryMap collectors_;
  lib::ObMutex mutex_;
  int64_t cpu_cnt_;
  bool is_inited_;
};

} /* namespace common */
} /* namespace oceanbase */

#endif /* OB_DIAGNOSTIC_SUMMARY_H_ */