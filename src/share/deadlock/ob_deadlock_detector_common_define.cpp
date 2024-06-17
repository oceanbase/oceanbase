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

#include "ob_deadlock_detector_common_define.h"
#include "ob_deadlock_detector_mgr.h"

namespace oceanbase
{
namespace share
{
namespace detector
{

using namespace common;

int64_t ObIDeadLockDetector::total_constructed_count = 0;
int64_t ObIDeadLockDetector::total_destructed_count = 0;

OB_SERIALIZE_MEMBER(ObDetectorUserReportInfo, module_name_, resource_visitor_,
                    required_resource_, extra_columns_names_, extra_columns_values_,
                    valid_extra_column_size_, blocked_seq_);
OB_SERIALIZE_MEMBER(ObDetectorInnerReportInfo, binary_key_, tenant_id_, addr_,
                    detector_id_, report_time_,
                    created_time_, event_id_, role_, start_delay_, priority_, user_report_info_);
OB_SERIALIZE_MEMBER(ObDetectorPriority, priority_range_, priority_value_)

static int check_and_assign_ptr_(const char *others_ptr,
                                 const int64_t len,
                                 const char *print_info,
                                 ObString* self_string) {
  #define PRINT_WRAPPER KR(ret), K(len), K(calculated_len), "caller", print_info, K(*self_string)
  int ret = OB_SUCCESS;
  int64_t calculated_len = len;

  if (nullptr == others_ptr) {
    ret = OB_INVALID_ARGUMENT;
    DETECT_LOG(ERROR, "invalid argument", PRINT_WRAPPER);
  } else {
    if (calculated_len == 0) {
      calculated_len = strlen(others_ptr);
    }

    if (calculated_len <= 0 || calculated_len >= STR_LEN_LIMIT) {
      ret = OB_ERR_UNEXPECTED;
      DETECT_LOG(WARN, "string length is not satisfied length limit",
                        PRINT_WRAPPER, K(STR_LEN_LIMIT));
    } else {
      self_string->assign_ptr(others_ptr, static_cast<int32_t>(calculated_len));
    }
  }

  return ret;
  #undef PRINT_WRAPPER
}

/* * * * * * definition of ObDetectorPriority * * * * */

ObDetectorPriority::ObDetectorPriority(uint64_t priority_value) :
  priority_range_(static_cast<int64_t>(PRIORITY_RANGE::NORMAL)),
  priority_value_(priority_value) {
  // do nothing
}

ObDetectorPriority::ObDetectorPriority(const PRIORITY_RANGE &priority_range,
                                       uint64_t priority_value) :
  priority_range_(static_cast<int64_t>(priority_range)),
  priority_value_(priority_value) {
  // do nothing
}

ObDetectorPriority::ObDetectorPriority(const ObDetectorPriority &rhs)
{
  *this = rhs;
}

bool ObDetectorPriority::is_valid() const
{
  return INVALID_VALUE != priority_value_;
}

const char *ObDetectorPriority::get_range_str() const
{
  const char *ret = "unknown";
  if (priority_range_ == static_cast<int64_t>(PRIORITY_RANGE::EXTREMELY_LOW)) {
    ret = "extreme-low";
  } else if (priority_range_ == static_cast<int64_t>(PRIORITY_RANGE::LOW)) {
    ret = "low";
  } else if (priority_range_ == static_cast<int64_t>(PRIORITY_RANGE::NORMAL)) {
    ret = "normal";
  } else if (priority_range_ == static_cast<int64_t>(PRIORITY_RANGE::HIGH)) {
    ret = "high";
  } else if (priority_range_ == static_cast<int64_t>(PRIORITY_RANGE::EXTREMELY_HIGH)) {
    ret = "extreme-high";
  } else {
    // do nothing
  }
  return ret;
}

uint64_t ObDetectorPriority::get_value() const
{
  return priority_value_;
}

ObDetectorPriority &ObDetectorPriority::operator=(const ObDetectorPriority &rhs)
{
  this->priority_range_ = rhs.priority_range_;
  this->priority_value_ = rhs.priority_value_;
  return *this;
}

bool ObDetectorPriority::operator<(const ObDetectorPriority &rhs) const
{
  bool ret = false;
  if (priority_range_ < rhs.priority_range_) {
    ret = true;
  } else if (priority_range_ == rhs.priority_range_) {
    if (priority_value_ < rhs.priority_value_) {
      ret = true;
    }
  }
  return ret;
}

bool ObDetectorPriority::operator==(const ObDetectorPriority &rhs) const
{
  return priority_range_ == rhs.priority_range_ &&
         priority_value_ == rhs.priority_value_;
}

bool ObDetectorPriority::operator!=(const ObDetectorPriority &rhs) const
{
  return !((*this) == rhs);
}

bool ObDetectorPriority::operator>(const ObDetectorPriority &rhs) const
{
  return (!((*this) < rhs)) && ((*this) != rhs);
}

bool ObDetectorPriority::operator<=(const ObDetectorPriority &rhs) const
{
  return ((*this) < rhs) || ((*this) == rhs);
}

bool ObDetectorPriority::operator>=(const ObDetectorPriority &rhs) const
{
  return ((*this) > rhs) || ((*this) == rhs);
}

/* * * * * * definition of ObDependencyResource * * * * */

ObDependencyResource::ObDependencyResource()
{
  // do nothing
}

ObDependencyResource::ObDependencyResource(const ObDependencyResource &rhs)
{
  operator=(rhs);
}

ObDependencyResource::ObDependencyResource(const ObAddr &addr, const UserBinaryKey &user_key) :
  addr_(addr),
  user_key_(user_key) {}

void ObDependencyResource::reset()
{
  addr_.reset();
  user_key_.reset();
}

int ObDependencyResource::set_args(const common::ObAddr &addr, const UserBinaryKey &user_key)
{
  addr_ = addr;
  user_key_ = user_key;
  return OB_SUCCESS;
}

const ObAddr& ObDependencyResource::get_addr() const
{
  return addr_;
}

const UserBinaryKey &ObDependencyResource::get_user_key() const
{
  return user_key_;
}

bool ObDependencyResource::is_valid() const
{
  return user_key_.is_valid() && addr_.is_valid();
}

ObDependencyResource& ObDependencyResource::operator=(const ObDependencyResource &rhs)
{
  addr_ = rhs.addr_;
  user_key_ = rhs.user_key_;
  return *this;
}

uint64_t ObDependencyResource::hash() const
{
  uint64_t hash_val = 0;
  hash_val = addr_.hash();
  uint64_t key_hash = user_key_.hash();
  hash_val = murmurhash(&key_hash, sizeof(key_hash), hash_val);
  return hash_val;
}

bool ObDependencyResource::operator==(const ObDependencyResource &rhs) const
{
  return  addr_ == rhs.addr_ && user_key_ == rhs.user_key_;
}

bool ObDependencyResource::operator<(const ObDependencyResource &rhs) const
{
  if (addr_ < rhs.addr_) {
    return true;
  } else if (addr_ > rhs.addr_) {
    return false;
  } else {
    if (user_key_ < rhs.user_key_) {
      return true;
    } else {
      return false;
    }
  }
}

/* * * * * * definition of ObDetectorUserReportInfo * * * * */

ObDetectorUserReportInfo::ObDetectorUserReportInfo() :
  valid_extra_column_size_(0)
{
  // do nothing
}

int ObDetectorUserReportInfo::set_module_name(const common::ObSharedGuard<char> &module_name)
{
  (void) module_name_guard_.assign(module_name);
  return check_and_assign_ptr_(module_name_guard_.get_ptr(),
                               0,
                               "set_module_name(ObSharedGuard<char>)",
                               &module_name_);
}

int ObDetectorUserReportInfo::set_visitor(const common::ObSharedGuard<char> &visitor)
{
  (void) resource_visitor_guard_.assign(visitor);
  return check_and_assign_ptr_(resource_visitor_guard_.get_ptr(),
                               0,
                               "set_visitor(ObSharedGuard<char>)",
                               &resource_visitor_);
}

int ObDetectorUserReportInfo::set_resource(const common::ObSharedGuard<char> &resource)
{
  (void) required_resource_guard_.assign(resource);
  return check_and_assign_ptr_(required_resource_guard_.get_ptr(),
                               0,
                               "set_resource(ObSharedGuard<char>)",
                               &required_resource_);
}

bool ObDetectorUserReportInfo::is_valid() const
{
  return !(module_name_.empty() ||
           resource_visitor_.empty() ||
           required_resource_.empty() ||
           extra_columns_names_.error() ||
           extra_columns_values_.error());
}

const ObString &ObDetectorUserReportInfo::get_module_name() const
{
  return module_name_;
}

const ObString &ObDetectorUserReportInfo::get_resource_visitor() const
{
  return resource_visitor_;
}

const ObString &ObDetectorUserReportInfo::get_required_resource() const
{
  return required_resource_;
}

const ObSArray<ObString> &ObDetectorUserReportInfo::get_extra_columns_names() const
{
  return extra_columns_names_;
}

const ObSArray<ObString> &ObDetectorUserReportInfo::get_extra_columns_values() const
{
  return extra_columns_values_;
}

uint8_t ObDetectorUserReportInfo::get_valid_extra_column_size() const
{
  return valid_extra_column_size_;
}

int ObDetectorUserReportInfo::set_columns_(const int64_t idx,
                                           const ValueType type,
                                           const ObString &column_info)
{
  #define PRINT_WRAPPER KR(ret), K(idx), K(column_info)
  int ret = OB_SUCCESS;
  int64_t str_len = column_info.length();

  if (true == column_info.empty()) {
    ret = OB_INVALID_ARGUMENT;
    DETECT_LOG(WARN, "invalid argument", PRINT_WRAPPER);
  } else if (STR_LEN_LIMIT <= (str_len = column_info.length())) {
    ret = OB_INVALID_ARGUMENT;
    DETECT_LOG(WARN, "string length reach limit", PRINT_WRAPPER, K(STR_LEN_LIMIT));
  } else if (ValueType::COLUMN_NAME == type) {
    if (OB_FAIL(extra_columns_names_.push_back(ObString(str_len, column_info.ptr())))) {
      DETECT_LOG(WARN, "push name string failed", PRINT_WRAPPER);
    }
  } else if (ValueType::COLUMN_VALUE == type) {
    if (OB_FAIL(extra_columns_values_.push_back(ObString(str_len, column_info.ptr())))) {
      DETECT_LOG(WARN, "push value string failed", PRINT_WRAPPER);
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    int type_ = static_cast<int>(type);
    DETECT_LOG(ERROR, "code error! unknown type", PRINT_WRAPPER, K_(type));
  }

  return ret;
  #undef PRINT_WRAPPER
}

int ObDetectorUserReportInfo::set_columns_(const int64_t idx,
                                           const ValueType type,
                                           const char *column_info)
{
  #define PRINT_WRAPPER KR(ret), K(idx), KP(column_info), K(str_len)
  int ret = OB_SUCCESS;
  int64_t str_len = 0;

  if (nullptr == column_info) {
    ret = OB_INVALID_ARGUMENT;
    DETECT_LOG(WARN, "invalid argument", PRINT_WRAPPER);
  } else if (STR_LEN_LIMIT <= (str_len = strlen(column_info))) {
    ret = OB_INVALID_ARGUMENT;
    DETECT_LOG(WARN, "string length reach limit", PRINT_WRAPPER, K(STR_LEN_LIMIT));
  } else if (ValueType::COLUMN_NAME == type) {
    extra_columns_names_.push_back(ObString(str_len, column_info));
  } else if (ValueType::COLUMN_VALUE == type) {
    extra_columns_values_.push_back(ObString(str_len, column_info));
  } else {
    ret = OB_ERR_UNEXPECTED;
    int type_ = static_cast<int>(type);
    DETECT_LOG(ERROR, "code error! unknown type", PRINT_WRAPPER, K_(type));
  }

  return ret;
  #undef PRINT_WRAPPER
}

int ObDetectorUserReportInfo::set_columns_(const int64_t idx,
                                           const ValueType type,
                                           const common::ObSharedGuard<char> &column_info)
{
  #define PRINT_WRAPPER KR(ret), K(idx)
  int ret = OB_SUCCESS;

  if (ValueType::COLUMN_NAME == type) {
    extra_columns_names_guard_.push_back(column_info);
    extra_columns_names_.push_back(ObString(strlen(column_info.get_ptr()),
                                            column_info.get_ptr()));
  } else if (ValueType::COLUMN_VALUE == type) {
    extra_columns_values_guard_.push_back(column_info);
    extra_columns_values_.push_back(ObString(strlen(column_info.get_ptr()),
                                    column_info.get_ptr()));
  } else {
    ret = OB_ERR_UNEXPECTED;
    int type_ = static_cast<int>(type);
    DETECT_LOG(ERROR, "code error! unknown type", PRINT_WRAPPER, K_(type));
  }

  return ret;
  #undef PRINT_WRAPPER
}

int ObDetectorUserReportInfo::assign(const ObDetectorUserReportInfo &rhs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(extra_columns_names_.assign(rhs.extra_columns_names_))) {
    DETECT_LOG(WARN, "fail to copy array", K(rhs));
  } else if (OB_FAIL(extra_columns_values_.assign(rhs.extra_columns_values_))) {
    DETECT_LOG(WARN, "fail to copy array", K(rhs));
  } else if (OB_FAIL(extra_columns_names_guard_.assign(rhs.extra_columns_names_guard_))) {
    DETECT_LOG(WARN, "fail to copy array", K(rhs));
  } else if (OB_FAIL(extra_columns_values_guard_.assign(rhs.extra_columns_values_guard_))) {
    DETECT_LOG(WARN, "fail to copy array", K(rhs));
  } else {
    module_name_ = rhs.module_name_;
    resource_visitor_ = rhs.resource_visitor_;
    required_resource_ = rhs.required_resource_;
    valid_extra_column_size_ = rhs.valid_extra_column_size_;
    module_name_guard_ = rhs.module_name_guard_;
    resource_visitor_guard_ = rhs.resource_visitor_guard_;
    required_resource_guard_ = rhs.required_resource_guard_;
  }
  return ret;
}

ObDetectorInnerReportInfo::ObDetectorInnerReportInfo() :
  tenant_id_(INVALID_VALUE),
  detector_id_(INVALID_VALUE),
  report_time_(INVALID_VALUE),
  created_time_(INVALID_VALUE),
  event_id_(INVALID_VALUE),
  start_delay_(INVALID_VALUE),
  priority_(INVALID_VALUE)
{
  // do nothing
}

int ObDetectorInnerReportInfo::set_args(const UserBinaryKey &binary_key,
                                        const ObAddr &addr, const uint64_t detector_id,
                                        const int64_t report_time, const int64_t created_time,
                                        const uint64_t event_id,  const char *role,
                                        const uint64_t start_delay,
                                        const ObDetectorPriority &priority,
                                        const ObDetectorUserReportInfo &user_report_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_and_assign_ptr_(role, 0, "ObDetectorInnerReportInfo::set_args", &role_))) {
    DETECT_LOG(ERROR, "assign event field failed");
  } else if (OB_FAIL(user_report_info_.assign(user_report_info))) {
    DETECT_LOG(WARN, "assign user_report_info field failed");
  } else {
    start_delay_ = start_delay;
    priority_ = priority;
    binary_key_ = binary_key;
    tenant_id_ = MTL_ID();
    addr_ = addr;
    detector_id_ = detector_id;
    report_time_ = report_time;
    created_time_ = created_time;
    event_id_ = event_id;
  }

  return ret;
}

bool ObDetectorInnerReportInfo::is_valid() const
{
  return binary_key_.is_valid() && INVALID_VALUE != tenant_id_ &&
         addr_.is_valid() && INVALID_VALUE != detector_id_ &&
         INVALID_VALUE != report_time_ && INVALID_VALUE != created_time_ &&
         INVALID_VALUE != event_id_ && false == role_.empty() &&
         INVALID_VALUE != start_delay_ && user_report_info_.is_valid();
}

const UserBinaryKey &ObDetectorInnerReportInfo::get_user_key() const
{
  return binary_key_;
}

uint64_t ObDetectorInnerReportInfo::get_tenant_id() const
{
  return tenant_id_;
}

const ObAddr &ObDetectorInnerReportInfo::get_addr() const
{
  return addr_;
}

uint64_t ObDetectorInnerReportInfo::get_detector_id() const
{
  return detector_id_;
}

int ObDetectorInnerReportInfo::set_role(const char *role)
{
  return check_and_assign_ptr_(role, 0, "ObDetectorInnerReportInfo::set_role", &role_);
}

const ObString &ObDetectorInnerReportInfo::get_role() const
{
  return role_;
}

int64_t ObDetectorInnerReportInfo::get_report_time() const
{
  return report_time_;
}

int64_t ObDetectorInnerReportInfo::get_created_time() const
{
  return created_time_;
}

uint64_t ObDetectorInnerReportInfo::get_event_id() const
{
  return event_id_;
}

uint64_t ObDetectorInnerReportInfo::get_start_delay() const
{
  return start_delay_;
}

const ObDetectorPriority &ObDetectorInnerReportInfo::get_priority() const
{
  return priority_;
}

const ObDetectorUserReportInfo &ObDetectorInnerReportInfo::get_user_report_info() const
{
  return user_report_info_;
}

int ObDetectorInnerReportInfo::assign(const ObDetectorInnerReportInfo &rhs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(user_report_info_.assign(rhs.user_report_info_))) {
    DETECT_LOG(WARN, "fail to assign user report info", K(rhs));
  } else {
    binary_key_ = rhs.binary_key_;
    tenant_id_ = rhs.tenant_id_;
    addr_ = rhs.addr_;
    detector_id_ = rhs.detector_id_;
    report_time_ = rhs.report_time_;
    created_time_ = rhs.created_time_;
    event_id_ = rhs.event_id_;
    role_ = rhs.role_;
    start_delay_ = rhs.start_delay_;
    priority_ = rhs.priority_;
  }
  return ret;
}

}// namespace detector
}// namespace share
}// namespace oceanbase
