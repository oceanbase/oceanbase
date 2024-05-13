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

#ifndef OCEANBASE_SHARE_DEADLOCK_OB_DEADLOCK_DETECTOR_COMMON_DEFINE_H
#define OCEANBASE_SHARE_DEADLOCK_OB_DEADLOCK_DETECTOR_COMMON_DEFINE_H
#include "lib/container/ob_array.h"
#include "lib/hash/ob_link_hashmap.h"
#include "ob_deadlock_key_wrapper.h"
#include "lib/net/ob_addr.h"
#include "ob_deadlock_parameters.h"
#include "lib/function/ob_function.h"
#include "lib/guard/ob_unique_guard.h"
#include "lib/guard/ob_shared_guard.h"
#include "lib/utility/utility.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/container/ob_array_serialization.h"
#include "share/ob_occam_time_guard.h"
#include "storage/tx/ob_tx_seq.h"

#define DETECT_TIME_GUARD(threshold) TIMEGUARD_INIT(DETECT, threshold, 10_s)

namespace oceanbase
{
namespace share
{
namespace detector
{
// if msg in map count below LCL_MSG_CACHE_LIMIT/2, all pending msg is accepted
// if msg in map count greater than LCL_MSG_CACHE_LIMIT/2, but less than LCL_MSG_CACHE_LIMIT,
// random drop appending msg, drop probability depends on how many msg keeping in map,
// if msg count in map reach LCL_MSG_CACHE_LIMIT, drop probability is 100%, no more msg is accepted.
constexpr int64_t LCL_MSG_CACHE_LIMIT = 4096;

class ObLCLMessage;
class ObDependencyResource;
class ObDetectorPriority;
class ObDetectorUserReportInfo;
class ObDetectorInnerReportInfo;
class ObDeadLockCollectInfoMessage;
typedef common::ObFunction<int(const common::ObIArray<ObDetectorInnerReportInfo> &,
                               const int64_t)> DetectCallBack;
typedef common::ObFunction<int(ObIArray<ObDependencyResource>&,bool&)> BlockCallBack;
typedef common::ObFunction<int(ObDetectorUserReportInfo&)> CollectCallBack;

class ObIDeadLockDetector : public common::LinkHashValue<UserBinaryKey>
{
public:
  static int64_t total_constructed_count;
  static int64_t total_destructed_count;
public:
  virtual ~ObIDeadLockDetector() {};
public:
  virtual int add_parent(const ObDependencyResource &) = 0;
  virtual void set_timeout(const uint64_t timeout) = 0;
  virtual int register_timer_task() = 0;
  virtual void unregister_timer_task() = 0;
  virtual void dec_count_down_allow_detect() = 0;
  virtual int64_t to_string(char *buffer, const int64_t length) const = 0;// for debugging
  virtual const ObDetectorPriority &get_priority() const = 0;// return detector's priority
  // build a directed dependency relationship to other
  virtual int block(const ObDependencyResource &) = 0;
  virtual int block(const BlockCallBack &) = 0;
  virtual int get_block_list(common::ObIArray<ObDependencyResource> &cur_list) const = 0;
  // releace block list
  virtual int replace_block_list(const common::ObIArray<ObDependencyResource> &) = 0;
  // remove a directed dependency relationship to other
  virtual int activate(const ObDependencyResource &) = 0;
  virtual int activate_all() = 0;
  virtual uint64_t get_resource_id() const = 0;// get self resource id
  virtual int process_collect_info_message(const ObDeadLockCollectInfoMessage &) = 0;
  // handle message for scheme LCL
  virtual int process_lcl_message(const ObLCLMessage &) = 0;
};

class ObDetectorPriority
{
  OB_UNIS_VERSION(1);
public:
  ObDetectorPriority(uint64_t priority_value);
  ObDetectorPriority(const PRIORITY_RANGE &priority_range, uint64_t priority_value);
  ObDetectorPriority(const ObDetectorPriority &rhs);
  ObDetectorPriority &operator=(const ObDetectorPriority &rhs);
  bool is_valid() const;
  bool operator<(const ObDetectorPriority &rhs) const;
  bool operator>(const ObDetectorPriority &rhs) const;
  bool operator<=(const ObDetectorPriority &rhs) const;
  bool operator>=(const ObDetectorPriority &rhs) const;
  bool operator==(const ObDetectorPriority &rhs) const;
  bool operator!=(const ObDetectorPriority &rhs) const;
  const char *get_range_str() const;
  uint64_t get_value() const;
  TO_STRING_KV(K_(priority_range), K_(priority_value));
private:
  int64_t priority_range_;
  uint64_t priority_value_;
};

class ObDetectorUserReportInfo
{
  OB_UNIS_VERSION(1);
public:
  ObDetectorUserReportInfo();
  ObDetectorUserReportInfo &operator=(const ObDetectorUserReportInfo &) = delete;
  int assign(const ObDetectorUserReportInfo &rhs);
  bool is_valid() const;
  int set_module_name(const common::ObSharedGuard<char> &module_name);
  int set_visitor(const common::ObSharedGuard<char> &visitor);
  int set_resource(const common::ObSharedGuard<char> &resource);
  const common::ObString &get_module_name() const;
  const common::ObString &get_resource_visitor() const;
  const common::ObString &get_required_resource() const;
  const common::ObSArray<common::ObString> &get_extra_columns_names() const;
  const common::ObSArray<common::ObString> &get_extra_columns_values() const;
  uint8_t get_valid_extra_column_size() const;
  // use this interface like:
  // set_extra_info("1","2",ObString("3"),"4","5",ObString("6"),ObString("7"),ObString("8"));
  //
  // there are some compile restriction when using this interface, which are by designed:
  // 1. number of args must be even.
  // 2. number of args must less or equal than 2*EXTRA_INFO_COLUMNS.
  // 3. arbitrary arg must be either ObString or char*.
  // user should keep the rules above, or compile error.
  //
  // CAUTIONS:
  // 1. it's user's duty to care about all args' lifetime(all string pointer related).
  // user should keep the rules above, or runtime error.
  template <class ...Args>
  int set_extra_info(const Args &...rest);
  TO_STRING_KV(K_(module_name), K_(resource_visitor), K_(required_resource),
               K_(extra_columns_names), K_(extra_columns_values), K_(valid_extra_column_size));
private:
  enum class ValueType
  {
    COLUMN_NAME = 0,
    COLUMN_VALUE = 1
  };
  // recursion end
  template <int Floor>
  int set_extra_info_();
  // variadic template
  template <int Floor, class T1, class T2, class ...Args>
  int set_extra_info_(const T1 &column_name, const T2 &column_value, const Args &...rest);
  // for now, only ObString or char* type supported
  template <typename T>
  int set_columns_(const int64_t idx, const ValueType type, const T &column_info);
  int set_columns_(const int64_t idx, const ValueType type, const common::ObString &column_info);
  int set_columns_(const int64_t idx, const ValueType type, const char *column_info);
  int set_columns_(const int64_t idx, const ValueType type, const common::ObSharedGuard<char> &column_info);
  common::ObString module_name_;// like 'transaction' to transaction module
  common::ObString resource_visitor_;// like 'transaction id' to transaction module
  common::ObString required_resource_;// like 'row key' to transaction module
  transaction::ObTxSEQ blocked_seq_;// blocked tx holding row's lock by execute sql identified by this seq
  // ObSEArray is not allowed here,
  // cause different template parameter LOCAL_ARRAY_SIZE means different type
  // may influence rpc deserialization in compat scenario
  common::ObSArray<common::ObString> extra_columns_names_;// explain the meaning of extra columns
  // extra info that user could describe more things
  common::ObSArray<common::ObString> extra_columns_values_;
  uint8_t valid_extra_column_size_;// indicate whether extra info valid, and how many of them valid
  common::ObSharedGuard<char> module_name_guard_;
  common::ObSharedGuard<char> resource_visitor_guard_;
  common::ObSharedGuard<char> required_resource_guard_;
  common::ObArray<common::ObSharedGuard<char>> extra_columns_names_guard_;
  common::ObArray<common::ObSharedGuard<char>> extra_columns_values_guard_;
};

class ObDetectorInnerReportInfo
{
  OB_UNIS_VERSION(1);
public:
  ObDetectorInnerReportInfo();
  ObDetectorInnerReportInfo &operator=(const ObDetectorInnerReportInfo &) = delete;
  int assign(const ObDetectorInnerReportInfo &rhs);
  int set_args(const UserBinaryKey &binary_key,
               const common::ObAddr &addr, const uint64_t detector_id,
               const int64_t report_time, const int64_t created_time,
               const uint64_t event_id, const char *role,
               const uint64_t start_delay,
               const ObDetectorPriority &priority,
               const ObDetectorUserReportInfo &user_report_info);
  bool is_valid() const;
  const UserBinaryKey &get_user_key() const;
  uint64_t get_tenant_id() const;
  const common::ObAddr &get_addr() const;
  uint64_t get_detector_id() const;
  int64_t get_report_time() const;
  int64_t get_created_time() const;
  uint64_t get_event_id() const;
  int set_role(const char *role);
  const common::ObString &get_role() const;
  uint64_t get_start_delay() const;
  const ObDetectorPriority &get_priority() const;
  const ObDetectorUserReportInfo &get_user_report_info() const;
  TO_STRING_KV(K_(binary_key), K_(tenant_id), K_(addr), K_(detector_id),
               K_(report_time), K_(created_time),
               K_(event_id), K_(role), K_(start_delay), K_(priority), K_(user_report_info));
private:
  // binary key to describe user key info, to identify a dectector on a machine
  UserBinaryKey binary_key_;
  uint64_t tenant_id_;// the tenant who owns this detector
  // machine internet address,together witch detector_id_ and report_time_
  // identify a globally(through entire cluster) unique event
  common::ObAddr addr_;
  // detector id, together witch addr_ and report_time_
  // identify a globally(through entire cluster) unique event
  uint64_t detector_id_;
  // report info time during deadlock reconfirm process,
  // together witch detector_id_ and addr_
  // identify a globally(through entire cluster) unique event
  int64_t report_time_;
  int64_t created_time_;// the detector created time
  uint64_t event_id_;// hash result with awakened detector's addr_,detector_id_,report_time_ field,
                     // may not be globally(through entire cluster) unique, but mostly is
  common::ObString role_;// the role this detector plays
  // the delay time of detector start detect,
  // setted by detector user when detector created
  uint64_t start_delay_;
  // the detector priority, setted by detector user when detector created
  ObDetectorPriority priority_;
  ObDetectorUserReportInfo user_report_info_;// described by user
};

// ObDependencyResource describes the network resource on internet
// this structure could uniquely identified a detector on internet by address and user key
class ObDependencyResource
{
public:
  ObDependencyResource();
  ObDependencyResource(const ObDependencyResource &rhs);
  ObDependencyResource(const common::ObAddr &addr, const UserBinaryKey &user_key);
  ObDependencyResource &operator=(const ObDependencyResource &rhs);
  bool operator==(const ObDependencyResource &rhs) const;
  bool operator<(const ObDependencyResource &rhs) const;
  ~ObDependencyResource() = default;
  void reset();
  int set_args(const common::ObAddr &addr, const UserBinaryKey &user_key);
  const common::ObAddr &get_addr() const;
  const UserBinaryKey &get_user_key() const;
  bool is_valid() const;
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
  TO_STRING_KV(K_(addr), K_(user_key));
private:
  common::ObAddr addr_;
  UserBinaryKey user_key_;
};

template <class ...Args>
int ObDetectorUserReportInfo::set_extra_info(const Args &...rest)
{
  static_assert(sizeof...(rest) % 2 == 0, "number of args must be even.");
  static_assert(sizeof...(rest) <= EXTRA_INFO_COLUMNS * 2,
                "number of args reach extra columns size limit.");
  return set_extra_info_<0, Args...>(rest...);
}

template <int Floor, class T1, class T2, class ...Args>
int ObDetectorUserReportInfo::set_extra_info_(const T1 &column_name,
                                              const T2 &column_value,
                                              const Args &...rest)
{
  static_assert(Floor < EXTRA_INFO_COLUMNS, "number of parameters reach column size limit.");
  int ret = common::OB_SUCCESS;

  // reset invalid before the first assign action, set valid after the last assgin action
  if (Floor == 0) {
    valid_extra_column_size_ = 0;
    extra_columns_names_.reset();
    extra_columns_values_.reset();
  }

  int step = 0;
  if (++step && OB_FAIL(set_columns_(Floor, ValueType::COLUMN_NAME, column_name))) {
  } else if (++step && OB_FAIL(set_columns_(Floor, ValueType::COLUMN_VALUE, column_value))) {
  } else if (++step && (common::OB_SUCCESS != (ret = set_extra_info_<Floor+1, Args...>(rest...)))) {
  } else {}

  if (OB_FAIL(ret)) {
    DETECT_LOG(WARN, "set column operation failed", K(Floor), K(step));
  }

  return ret;
}

// recursion end
template <int Floor>
int ObDetectorUserReportInfo::set_extra_info_()
{
  valid_extra_column_size_ = Floor;
  return common::OB_SUCCESS;
}

}
}
}
#endif
