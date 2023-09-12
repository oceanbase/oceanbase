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

#ifndef LOGSERVICE_COORDINATOR_ELECTION_PRIORITY_IMPL_H
#define LOGSERVICE_COORDINATOR_ELECTION_PRIORITY_IMPL_H
#include "lib/container/ob_array_serialization.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/ob_define.h"
#include "lib/oblog/ob_log.h"
#include "lib/container/ob_array.h"
#include "lib/guard/ob_unique_guard.h"
#include "lib/utility/ob_unify_serialize.h"
#include "logservice/palf/election/interface/election_priority.h"
#include "share/scn.h"
#include "share/ob_ls_id.h"
#include "share/ob_cluster_version.h"
#include <type_traits>
#include <assert.h>
#include "logservice/leader_coordinator/failure_event.h"
#include "logservice/leader_coordinator/ob_leader_coordinator.h"

namespace oceanbase
{
namespace unittest
{
class TestElectionPriority;
}
namespace logservice
{
namespace coordinator
{

struct AbstractPriority
{
  friend class unittest::TestElectionPriority;
  AbstractPriority() : is_valid_(false) {}
  // 判断该优先级从哪个版本开始生效
  virtual uint64_t get_started_version() const = 0;
  // 序列化相关
  virtual int serialize(char* buf, const int64_t buf_len, int64_t& pos) const = 0;
  virtual int deserialize(const char* buf, const int64_t data_len, int64_t& pos) = 0;
  virtual int64_t get_serialize_size(void) const = 0;
  // 相同优先级版本的比较策略
  virtual int compare(const AbstractPriority &rhs, int &result, ObStringHolder &reason) const = 0;
  // 刷新优先级的方法
  int refresh(const share::ObLSID &ls_id) {
    int ret = OB_SUCCESS;
    is_valid_ = false;
    if (OB_SUCC(refresh_(ls_id))) {
      is_valid_ = true;
    }
    return ret;
  }
  // 判断字段内容是否有效的方法，或者refresh()成功后也是true
  bool is_valid() const { return is_valid_; }
  // fatal failure将绕过RCS在选举层面直接切主
  bool has_fatal_failure() const { return is_valid_ && has_fatal_failure_(); }
  virtual TO_STRING_KV(K_(is_valid));
protected:
  virtual bool has_fatal_failure_() const = 0;
  virtual int refresh_(const share::ObLSID &ls_id) = 0;
protected:
  bool is_valid_;
};

// 适用于[2_2_0, 4_0_0)用于测试
struct PriorityV0 : public AbstractPriority
{
  friend class unittest::TestElectionPriority;
  OB_UNIS_VERSION(1);
public:
  PriorityV0() : port_number_(0) {}
  // 判断该优先级策略是否适用于特定的版本
  virtual uint64_t get_started_version() const override { return CLUSTER_VERSION_2000; }
  // 相同优先级版本的比较策略
  virtual int compare(const AbstractPriority &rhs, int &result, ObStringHolder &reason) const override;
  TO_STRING_KV(K_(is_valid), K_(port_number));
protected:
  virtual bool has_fatal_failure_() const override { return false; }
  // 刷新优先级的方法
  virtual int refresh_(const share::ObLSID &ls_id) override;
private:
  int64_t port_number_;
};
OB_SERIALIZE_MEMBER_TEMP(inline, PriorityV0, is_valid_, port_number_);

// 适用于[4_0_0, latest]
struct PriorityV1 : public AbstractPriority
{
/*********************************************************************************/
  static constexpr int64_t MAX_UNREPLAYED_LOG_TS_DIFF_THRESHOLD_US = 2 * 1000 * 1000L;
  friend class unittest::TestElectionPriority;
  OB_UNIS_VERSION(1);
public:
  PriorityV1() : is_observer_stopped_(false), is_server_stopped_(false), is_zone_stopped_(false),
                 is_primary_region_(false), is_in_blacklist_(false),  is_manual_leader_(false),
                 zone_priority_(INT64_MAX) {scn_.set_min();}
  // 判断该优先级策略是否适用于特定的版本
  virtual uint64_t get_started_version() const override { return CLUSTER_VERSION_4_0_0_0; }
  // 相同优先级版本的比较策略
  virtual int compare(const AbstractPriority &rhs, int &result, ObStringHolder &reason) const override;
  // int assign(const PriorityV1 &rhs);
  TO_STRING_KV(K_(is_valid), K_(is_observer_stopped), K_(is_server_stopped), K_(is_zone_stopped),
               K_(fatal_failures), K_(is_primary_region), K_(serious_failures), K_(is_in_blacklist),
               K_(in_blacklist_reason), K_(scn), K_(is_manual_leader), K_(zone_priority));
protected:
  // 刷新优先级的方法
  virtual int refresh_(const share::ObLSID &ls_id) override;
  virtual bool has_fatal_failure_() const override;
  int get_scn_(const share::ObLSID &ls_id, share::SCN &scn);
  int get_role_(const share::ObLSID &ls_id, common::ObRole &role) const;
private:
  int compare_observer_stopped_(int &ret, const PriorityV1&) const;
  int compare_server_stopped_flag_(int &ret, const PriorityV1&) const;
  int compare_zone_stopped_flag_(int &ret, const PriorityV1&) const;
  int compare_fatal_failures_(int &ret, const PriorityV1&) const;
  int compare_primary_region_(int &ret, const PriorityV1&) const;
  int compare_serious_failures_(int &ret, const PriorityV1&) const;
  int compare_scn_(int &ret, const PriorityV1&) const;
  int compare_in_blacklist_flag_(int &ret, const PriorityV1&, ObStringHolder &) const;
  int compare_manual_leader_flag_(int &ret, const PriorityV1&) const;
  int compare_zone_priority_(int &ret, const PriorityV1&) const;

  bool is_observer_stopped_;// kill -15
  bool is_server_stopped_;
  bool is_zone_stopped_;
  common::ObSEArray<FailureEvent, 3> fatal_failures_;// negative infos
  bool is_primary_region_;
  common::ObSEArray<FailureEvent, 3> serious_failures_;// negative infos
  share::SCN scn_;
  bool is_in_blacklist_;
  common::ObStringHolder in_blacklist_reason_;
  bool is_manual_leader_;
  int64_t zone_priority_;// smaller means higher priority
};
OB_SERIALIZE_MEMBER_TEMP(inline, PriorityV1, is_valid_, is_observer_stopped_, is_server_stopped_,
                         is_zone_stopped_, fatal_failures_, is_primary_region_, serious_failures_,
                         scn_, is_in_blacklist_, in_blacklist_reason_,
                         is_manual_leader_, zone_priority_);

class ElectionPriorityImpl : public palf::election::ElectionPriority
{
  friend class unittest::TestElectionPriority;
public:
  ElectionPriorityImpl() {}
  ElectionPriorityImpl(const share::ObLSID ls_id) : ls_id_(ls_id), lock_(common::ObLatchIds::ELECTION_LOCK) {}
  virtual ~ElectionPriorityImpl() {}
  void set_ls_id(const share::ObLSID ls_id);
  // 优先级需要序列化能力，以便通过消息传递给其他副本
  virtual int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;
  virtual int deserialize(const char* buf, const int64_t data_len, int64_t& pos);
  virtual int64_t get_serialize_size(void) const;
  // 主动刷新选举优先级的方法
  virtual int refresh();
  // 在priority间进行比较的方法
  virtual int compare_with(const palf::election::ElectionPriority &rhs,
                           const uint64_t compare_version,
                           const bool decentralized_voting,
                           int &result,
                           ObStringHolder &reason) const;
  virtual int get_size_of_impl_type() const;
  virtual void placement_new_impl(void *ptr) const;
  // fatal failure跳过RCS直接切主
  virtual bool has_fatal_failure() const;
  int64_t to_string(char *buf, const int64_t buf_len) const override;
private:
  share::ObLSID ls_id_;
  ObTuple<PriorityV0, PriorityV1> priority_tuple_;
  mutable ObSpinLock lock_;
};

}
}
}
#endif
