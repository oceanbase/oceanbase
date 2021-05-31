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

#include "share/config/ob_config.h"
#include <algorithm>
#include <cstring>
#include <ctype.h>
#include "common/ob_smart_var.h"

using namespace oceanbase::share;
namespace oceanbase {
namespace common {
const char* log_archive_config_keywords[] = {
    "MANDATORY",
    "OPTIONAL",
    "COMPRESSION",
};

const char* log_archive_compression_values[] = {
    "disable",
    "enable",
    "lz4_1.0",
    "zstd_1.3.8",
};

const char* log_archive_encryption_mode_values[] = {
    "None",
    "Transparent Encryption",
};

const char* log_archive_encryption_algorithm_values[] = {
    "None",
    "",
};

// ObConfigItem
ObConfigItem::ObConfigItem()
    : ck_(NULL),
      version_(0),
      inited_(false),
      initial_value_set_(false),
      value_updated_(false),
      value_valid_(false),
      lock_()
{
  MEMSET(value_str_, 0, sizeof(value_str_));
  MEMSET(value_reboot_str_, 0, sizeof(value_reboot_str_));
  MEMSET(name_str_, 0, sizeof(name_str_));
  MEMSET(info_str_, 0, sizeof(info_str_));
}

ObConfigItem::~ObConfigItem()
{
  if (NULL != ck_) {
    delete ck_;
  }
}

void ObConfigItem::init(
    Scope::ScopeInfo scope_info, const char* name, const char* def, const char* info, const ObParameterAttr attr)
{
  if (OB_ISNULL(name) || OB_ISNULL(def) || OB_ISNULL(info)) {
    OB_LOG(ERROR, "name or def or info is null", K(name), K(def), K(info));
  } else {
    set_name(name);
    if (!set_value(def)) {
      OB_LOG(ERROR, "Set config item value failed", K(name), K(def));
    } else {
      set_info(info);
      attr_ = attr;
      attr_.set_scope(scope_info);
    }
  }
  inited_ = true;
}

// ObConfigIntListItem
ObConfigIntListItem::ObConfigIntListItem(ObConfigContainer* container, Scope::ScopeInfo scope_info, const char* name,
    const char* def, const char* info, const ObParameterAttr attr)
    : value_()
{
  if (OB_LIKELY(NULL != container)) {
    container->set_refactored(ObConfigStringKey(name), this, 1);
  }
  init(scope_info, name, def, info, attr);
}

bool ObConfigIntListItem::set(const char* str)
{
  UNUSED(str);
  char* saveptr = NULL;
  char* s = NULL;
  char* endptr = NULL;
  value_.valid_ = true;

  while (value_.size_--) {
    value_.int_list_[value_.size_] = 0;
  }
  value_.size_ = 0;
  int ret = OB_SUCCESS;
  SMART_VAR(char[OB_MAX_CONFIG_VALUE_LEN], tmp_value_str)
  {
    MEMCPY(tmp_value_str, value_str_, sizeof(tmp_value_str));
    s = STRTOK_R(tmp_value_str, ";", &saveptr);
    if (OB_LIKELY(NULL != s)) {
      do {
        int64_t v = strtol(s, &endptr, 10);
        if (endptr != s + STRLEN(s)) {
          value_.valid_ = false;
          _OB_LOG(ERROR, "not a valid config, [%s]", s);
        }
        value_.int_list_[value_.size_++] = v;
      } while (OB_LIKELY(NULL != (s = STRTOK_R(NULL, ";", &saveptr))) && value_.valid_);
    }
  }
  return value_.valid_;
}

// ObConfigStrListItem
ObConfigStrListItem::ObConfigStrListItem() : value_()
{}

ObConfigStrListItem::ObConfigStrListItem(ObConfigContainer* container, Scope::ScopeInfo scope_info, const char* name,
    const char* def, const char* info, const ObParameterAttr attr)
    : value_()
{
  if (OB_LIKELY(NULL != container)) {
    container->set_refactored(ObConfigStringKey(name), this, 1);
  }
  init(scope_info, name, def, info, attr);
}

int ObConfigStrListItem::tryget(const int64_t idx, char* buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  const struct ObInnerConfigStrListItem* inner_value = &value_;
  ObLatch& latch = const_cast<ObLatch&>(inner_value->rwlock_);
  if (OB_ISNULL(buf) || OB_UNLIKELY(idx < 0) || OB_UNLIKELY(idx >= MAX_INDEX_SIZE) || OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "input argument is invalid", K(buf), K(idx), K(buf_len), K(ret));
  } else if (!inner_value->valid_) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "ValueStrList is not available, no need to get", K_(inner_value->valid), K(ret));
  } else if (OB_FAIL(latch.try_rdlock(ObLatchIds::CONFIG_LOCK))) {
    OB_LOG(WARN, "failed to tryrdlock rwlock_", K(ret));
  } else {  // tryrdlock succ
    int print_size = 0;
    int32_t min_len = 0;
    const char* segment_str = NULL;
    if (idx >= inner_value->size_) {
      ret = OB_ARRAY_OUT_OF_RANGE;
    } else {
      segment_str = inner_value->value_str_bk_ + inner_value->idx_list_[idx];
      min_len = std::min(static_cast<int32_t>(STRLEN(segment_str) + 1), static_cast<int32_t>(buf_len));
      print_size = snprintf(buf, static_cast<size_t>(buf_len), "%.*s", min_len, segment_str);
      if (print_size < 0 || print_size > min_len) {
        ret = OB_BUF_NOT_ENOUGH;
      }
    }
    latch.unlock();

    if (OB_FAIL(ret)) {
      OB_LOG(WARN,
          "failed to get value during lock",
          K(idx),
          K_(inner_value->size),
          K(buf_len),
          K(print_size),
          K(min_len),
          K(segment_str),
          K(ret));
    }
  }
  return ret;
}

int ObConfigStrListItem::get(const int64_t idx, char* buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  const struct ObInnerConfigStrListItem* inner_value = &value_;
  if (OB_ISNULL(buf) || idx < 0 || idx >= MAX_INDEX_SIZE || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "input argument is invalid", K(buf), K(idx), K(buf_len), K(ret));
  } else if (!inner_value->valid_) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "ValueStrList is not available, no need to get", K(inner_value->valid_), K(ret));
  } else {
    int print_size = 0;
    int32_t min_len = 0;
    const char* segment_str = NULL;
    ObLatch& latch = const_cast<ObLatch&>(inner_value->rwlock_);
    ObLatchRGuard rd_guard(latch, ObLatchIds::CONFIG_LOCK);
    if (idx >= inner_value->size_) {
      ret = OB_ARRAY_OUT_OF_RANGE;
    } else {
      segment_str = inner_value->value_str_bk_ + inner_value->idx_list_[idx];
      min_len = std::min(static_cast<int32_t>(STRLEN(segment_str) + 1), static_cast<int32_t>(buf_len));
      print_size = snprintf(buf, static_cast<size_t>(min_len), "%.*s", min_len, segment_str);
      if (print_size < 0 || print_size > min_len) {
        ret = OB_BUF_NOT_ENOUGH;
      }
    }

    if (OB_FAIL(ret)) {
      OB_LOG(WARN,
          "failed to get value during lock",
          K(idx),
          K(inner_value->size_),
          K(buf_len),
          K(print_size),
          K(min_len),
          K(segment_str),
          K(ret));
    }
  }
  return ret;
}

bool ObConfigStrListItem::set(const char* str)
{
  bool bret = true;
  UNUSED(str);
  int64_t length = static_cast<int64_t>(STRLEN(value_str_));
  if (0 != length) {
    int64_t idx_list[MAX_INDEX_SIZE];
    int64_t curr_idx = 0;
    idx_list[curr_idx++] = 0;
    for (int64_t i = 0; bret && i < length; ++i) {
      if (';' == value_str_[i]) {
        if (curr_idx < MAX_INDEX_SIZE) {
          idx_list[curr_idx++] = i + 1;  // record semicolon's site and set next idx site
        } else {                         // overflow
          bret = false;
        }
      } else {
        // do nothing
      }
    }

    if (bret) {  // value_str_ is available, memcpy to value_str_bk_
      int print_size = 0;
      ObLatchRGuard wr_guard(value_.rwlock_, ObLatchIds::CONFIG_LOCK);
      value_.valid_ = true;
      value_.size_ = curr_idx;
      MEMCPY(value_.idx_list_, idx_list, static_cast<size_t>(curr_idx) * sizeof(int64_t));
      int32_t min_len = std::min(static_cast<int32_t>(sizeof(value_.value_str_bk_)), static_cast<int32_t>(length) + 1);
      print_size = snprintf(value_.value_str_bk_, static_cast<size_t>(min_len), "%.*s", min_len, value_str_);
      if (print_size < 0 || print_size > min_len) {
        value_.valid_ = false;
        bret = false;
      } else {
        for (int64_t i = 1; i < value_.size_; ++i) {  // ';' --> '\0'
          value_.value_str_bk_[idx_list[i] - 1] = '\0';
        }
      }
    } else {
      OB_LOG(WARN, "input str is not available", K(str), K_(value_.valid), K_(value_.size), K(bret));
    }
  } else {
    ObLatchRGuard wr_guard(value_.rwlock_, ObLatchIds::CONFIG_LOCK);
    value_.size_ = 0;
    value_.valid_ = true;
  }
  return bret;
}

// ObConfigIntegralItem
void ObConfigIntegralItem::init(Scope::ScopeInfo scope_info, const char* name, const char* def, const char* range,
    const char* info, const ObParameterAttr attr)
{
  ObConfigItem::init(scope_info, name, def, info, attr);
  if (OB_ISNULL(range)) {
    OB_LOG(ERROR, "Range is NULL");
  } else if (!parse_range(range)) {
    OB_LOG(ERROR, "Parse check range fail", K(range));
  }
}

bool ObConfigIntegralItem::parse_range(const char* range)
{
  char buff[64] = {'\0'};
  const char* p_left = NULL;
  char* p_middle = NULL;
  char* p_right = NULL;
  bool bool_ret = true;
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_ISNULL(range)) {
    OB_LOG(ERROR, "Range is NULL!");
    bool_ret = false;
  } else if ('\0' == range[0]) {
    // do nothing
  } else if (OB_FAIL(databuff_printf(buff, sizeof(buff), pos, "%s", range))) {
    bool_ret = false;
    OB_LOG(WARN, "buf is not long enough", K(sizeof(buff)), K(pos), K(ret));
  } else {
    const int64_t buff_length = static_cast<int64_t>(STRLEN(buff));
    for (int64_t i = 0; i < buff_length; ++i) {
      if ('(' == buff[i] || '[' == buff[i]) {
        p_left = buff + i;
      } else if (buff[i] == ',') {
        p_middle = buff + i;
      } else if (')' == buff[i] || ']' == buff[i]) {
        p_right = buff + i;
      }
    }
    if (!p_left || !p_middle || !p_right || p_left >= p_middle || p_middle >= p_right) {
      bool_ret = false;
      // not validated
    } else {
      bool valid = true;
      char ch_right = *p_right;
      *p_right = '\0';
      *p_middle = '\0';

      if ('\0' != p_left[1]) {
        parse(p_left + 1, valid);
        if (valid) {
          if (*p_left == '(') {
            add_checker(new (std::nothrow) ObConfigGreaterThan(p_left + 1));
          } else if (*p_left == '[') {
            add_checker(new (std::nothrow) ObConfigGreaterEqual(p_left + 1));
          }
        }
      }

      if ('\0' != p_middle[1]) {
        parse(p_middle + 1, valid);
        if (valid) {
          if (')' == ch_right) {
            add_checker(new (std::nothrow) ObConfigLessThan(p_middle + 1));
          } else if (']' == ch_right) {
            add_checker(new (std::nothrow) ObConfigLessEqual(p_middle + 1));
          }
        }
      }

      bool_ret = true;
    }
  }
  return bool_ret;
}

// ObConfigDoubleItem
ObConfigDoubleItem::ObConfigDoubleItem(ObConfigContainer* container, Scope::ScopeInfo scope_info, const char* name,
    const char* def, const char* range, const char* info, const ObParameterAttr attr)
    : value_(0)
{
  if (OB_LIKELY(NULL != container)) {
    container->set_refactored(ObConfigStringKey(name), this, 1);
  }
  init(scope_info, name, def, range, info, attr);
}

ObConfigDoubleItem::ObConfigDoubleItem(ObConfigContainer* container, Scope::ScopeInfo scope_info, const char* name,
    const char* def, const char* info, const ObParameterAttr attr)
    : value_(0)
{
  if (OB_LIKELY(NULL != container)) {
    container->set_refactored(ObConfigStringKey(name), this, 1);
  }
  init(scope_info, name, def, "", info, attr);
}

void ObConfigDoubleItem::init(Scope::ScopeInfo scope_info, const char* name, const char* def, const char* range,
    const char* info, const ObParameterAttr attr)
{
  ObConfigItem::init(scope_info, name, def, info, attr);
  if (OB_ISNULL(range)) {
    OB_LOG(ERROR, "Range is NULL");
  } else if (!parse_range(range)) {
    OB_LOG(ERROR, "Parse check range fail", K(range));
  }
}

double ObConfigDoubleItem::parse(const char* str, bool& valid) const
{
  double v = 0.0;
  if (OB_ISNULL(str) || OB_UNLIKELY('\0' == str[0])) {
    valid = false;
  } else {
    char* endptr = NULL;
    v = strtod(str, &endptr);
    if (OB_ISNULL(endptr) || OB_UNLIKELY('\0' != *endptr)) {
      valid = false;
    } else {
      valid = true;
    }
  }
  return v;
}

bool ObConfigDoubleItem::parse_range(const char* range)
{
  char buff[64] = {'\0'};
  const char* p_left = NULL;
  char* p_middle = NULL;
  char* p_right = NULL;
  bool bool_ret = true;
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_ISNULL(range)) {
    OB_LOG(ERROR, "Range is NULL!");
    bool_ret = false;
  } else if ('\0' == range[0]) {
    // do nothing
  } else if (OB_FAIL(databuff_printf(buff, sizeof(buff), pos, "%s", range))) {
    bool_ret = false;
  } else {
    const int64_t buff_length = static_cast<int64_t>(STRLEN(buff));
    for (int64_t i = 0; i < buff_length; ++i) {
      if ('(' == buff[i] || '[' == buff[i]) {
        p_left = buff + i;
      } else if (',' == buff[i]) {
        p_middle = buff + i;
      } else if (')' == buff[i] || ']' == buff[i]) {
        p_right = buff + i;
      }
    }
    if (OB_ISNULL(p_left) || OB_ISNULL(p_middle) || OB_ISNULL(p_right)) {
      bool_ret = false;  // not validated
    } else if (OB_UNLIKELY(p_left >= p_middle) || OB_UNLIKELY(p_middle >= p_right)) {
      bool_ret = false;  // not validated
    } else {
      bool valid = true;
      char ch_right = *p_right;
      *p_right = '\0';
      *p_middle = '\0';

      parse(p_left + 1, valid);
      if (valid) {
        if (*p_left == '(') {
          add_checker(new (std::nothrow) ObConfigGreaterThan(p_left + 1));
        } else if (*p_left == '[') {
          add_checker(new (std::nothrow) ObConfigGreaterEqual(p_left + 1));
        }
      }

      parse(p_middle + 1, valid);
      if (valid) {
        if (')' == ch_right) {
          add_checker(new (std::nothrow) ObConfigLessThan(p_middle + 1));
        } else if (']' == ch_right) {
          add_checker(new (std::nothrow) ObConfigLessEqual(p_middle + 1));
        }
      }
      bool_ret = true;
    }
  }
  return bool_ret;
}

// ObConfigCapacityItem
ObConfigCapacityItem::ObConfigCapacityItem(ObConfigContainer* container, Scope::ScopeInfo scope_info, const char* name,
    const char* def, const char* range, const char* info, const ObParameterAttr attr)
{
  if (OB_LIKELY(NULL != container)) {
    container->set_refactored(ObConfigStringKey(name), this, 1);
  }
  init(scope_info, name, def, range, info, attr);
}

ObConfigCapacityItem::ObConfigCapacityItem(ObConfigContainer* container, Scope::ScopeInfo scope_info, const char* name,
    const char* def, const char* info, const ObParameterAttr attr)
{
  if (OB_LIKELY(NULL != container)) {
    container->set_refactored(ObConfigStringKey(name), this, 1);
  }
  init(scope_info, name, def, "", info, attr);
}

int64_t ObConfigCapacityItem::parse(const char* str, bool& valid) const
{
  int64_t ret = ObConfigCapacityParser::get(str, valid);
  if (!valid) {
    OB_LOG(ERROR, "set capacity error", "name", name(), K(str), K(valid));
  }
  return ret;
}

// ObConfigTimeItem
ObConfigTimeItem::ObConfigTimeItem(ObConfigContainer* container, Scope::ScopeInfo scope_info, const char* name,
    const char* def, const char* range, const char* info, const ObParameterAttr attr)
{
  if (OB_LIKELY(NULL != container)) {
    container->set_refactored(ObConfigStringKey(name), this, 1);
  }
  init(scope_info, name, def, range, info, attr);
}

ObConfigTimeItem::ObConfigTimeItem(ObConfigContainer* container, Scope::ScopeInfo scope_info, const char* name,
    const char* def, const char* info, const ObParameterAttr attr)
{
  if (OB_LIKELY(NULL != container)) {
    container->set_refactored(ObConfigStringKey(name), this, 1);
  }
  init(scope_info, name, def, "", info, attr);
}

int64_t ObConfigTimeItem::parse(const char* str, bool& valid) const
{
  int64_t value = ObConfigTimeParser::get(str, valid);
  if (!valid) {
    OB_LOG(ERROR, "set time error", "name", name(), K(str), K(valid));
  }
  return value;
}

// ObConfigIntItem
ObConfigIntItem::ObConfigIntItem(ObConfigContainer* container, Scope::ScopeInfo scope_info, const char* name,
    const char* def, const char* range, const char* info, const ObParameterAttr attr)
{
  if (OB_LIKELY(NULL != container)) {
    container->set_refactored(ObConfigStringKey(name), this, 1);
  }
  init(scope_info, name, def, range, info, attr);
}

ObConfigIntItem::ObConfigIntItem(ObConfigContainer* container, Scope::ScopeInfo scope_info, const char* name,
    const char* def, const char* info, const ObParameterAttr attr)
{
  if (OB_LIKELY(NULL != container)) {
    container->set_refactored(ObConfigStringKey(name), this, 1);
  }
  init(scope_info, name, def, "", info, attr);
}

int64_t ObConfigIntItem::parse(const char* str, bool& valid) const
{
  int64_t value = ObConfigIntParser::get(str, valid);
  if (!valid) {
    OB_LOG(ERROR, "set int error", "name", name(), K(str), K(valid));
  }
  return value;
}

// ObConfigMomentItem
ObConfigMomentItem::ObConfigMomentItem(ObConfigContainer* container, Scope::ScopeInfo scope_info, const char* name,
    const char* def, const char* info, const ObParameterAttr attr)
    : value_()
{
  if (OB_LIKELY(NULL != container)) {
    container->set_refactored(ObConfigStringKey(name), this, 1);
  }
  init(scope_info, name, def, info, attr);
}

bool ObConfigMomentItem::set(const char* str)
{
  int ret = true;
  struct tm tm_value;
  if (0 == STRCASECMP(str, "disable")) {
    value_.disable_ = true;
  } else if (OB_ISNULL(strptime(str, "%H:%M", &tm_value))) {
    value_.disable_ = true;
    ret = false;
    OB_LOG(ERROR, "Not well-formed moment item value", K(str));
  } else {
    value_.disable_ = false;
    value_.hour_ = tm_value.tm_hour;
    value_.minute_ = tm_value.tm_min;
  }
  return ret;
}

// ObConfigBoolItem
ObConfigBoolItem::ObConfigBoolItem(ObConfigContainer* container, Scope::ScopeInfo scope_info, const char* name,
    const char* def, const char* info, const ObParameterAttr attr)
    : value_(false)
{
  if (OB_LIKELY(NULL != container)) {
    container->set_refactored(ObConfigStringKey(name), this, 1);
  }
  init(scope_info, name, def, info, attr);
}

bool ObConfigBoolItem::set(const char* str)
{
  bool valid = false;
  const bool value = parse(str, valid);
  if (valid) {
    int64_t pos = 0;
    (void)databuff_printf(value_str_, sizeof(value_str_), pos, value ? "True" : "False");
    value_ = value;
  }
  return valid;
}

bool ObConfigBoolItem::parse(const char* str, bool& valid) const
{
  bool value = true;
  if (OB_ISNULL(str)) {
    valid = false;
    OB_LOG(ERROR, "Get bool config item fail, str is NULL!");
  } else if (0 == STRCASECMP(str, "false")) {
    valid = true;
    value = false;
  } else if (0 == STRCASECMP(str, "true")) {
    valid = true;
    value = true;
  } else if (0 == STRCASECMP(str, "off")) {
    valid = true;
    value = false;
  } else if (0 == STRCASECMP(str, "on")) {
    valid = true;
    value = true;
  } else if (0 == STRCASECMP(str, "no")) {
    valid = true;
    value = false;
  } else if (0 == STRCASECMP(str, "yes")) {
    valid = true;
    value = true;
  } else if (0 == STRCASECMP(str, "f")) {
    valid = true;
    value = false;
  } else if (0 == STRCASECMP(str, "t")) {
    valid = true;
    value = true;
  } else if (0 == STRCASECMP(str, "1")) {
    valid = true;
    value = true;
  } else if (0 == STRCASECMP(str, "0")) {
    valid = true;
    value = false;
  } else {
    OB_LOG(ERROR, "Get bool config item fail", K(str));
    valid = false;
  }
  return value;
}

// ObConfigStringItem
ObConfigStringItem::ObConfigStringItem(ObConfigContainer* container, Scope::ScopeInfo scope_info, const char* name,
    const char* def, const char* info, const ObParameterAttr attr)
{
  if (OB_LIKELY(NULL != container)) {
    container->set_refactored(ObConfigStringKey(name), this, 1);
  }
  init(scope_info, name, def, info, attr);
}

int ObConfigStringItem::copy(char* buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  const char* inner_value = value_str_;

  if (OB_FAIL(databuff_printf(buf, buf_len, "%s", inner_value))) {
    OB_LOG(WARN, "buffer not enough", K(ret), K(buf_len), K_(value_str));
  }
  return ret;
}

ObConfigLogArchiveOptionsItem::ObConfigLogArchiveOptionsItem(ObConfigContainer* container, Scope::ScopeInfo scope_info,
    const char* name, const char* def, const char* info, const ObParameterAttr attr)
    : value_()
{
  if (OB_LIKELY(NULL != container)) {
    container->set_refactored(ObConfigStringKey(name), this, 1);
  }
  init(scope_info, name, def, info, attr);
}

bool ObConfigLogArchiveOptionsItem::set(const char* str)
{
  UNUSED(str);
  value_.valid_ = true;
  int ret = OB_SUCCESS;
  SMART_VAR(char[OB_MAX_CONFIG_VALUE_LEN], tmp_str)
  {
    MEMSET(tmp_str, 0, OB_MAX_CONFIG_VALUE_LEN);
    const int32_t str_len = static_cast<int32_t>(STRLEN(value_str_));
    const int64_t FORMAT_BUF_LEN = str_len * 3;  // '=' will be replaced with ' = '
    char format_str_buf[FORMAT_BUF_LEN];
    MEMCPY(tmp_str, value_str_, str_len);
    tmp_str[str_len] = '\0';
    int ret = OB_SUCCESS;
    if (OB_FAIL(format_option_str(tmp_str, str_len, format_str_buf, FORMAT_BUF_LEN))) {
      value_.valid_ = false;
      OB_LOG(WARN, "failed to format_option_str", KR(ret), K(tmp_str));
    } else {
      char* saveptr = NULL;
      char* s = STRTOK_R(format_str_buf, " ", &saveptr);
      bool is_equal_sign_demanded = false;
      int64_t key_idx = -1;
      if (OB_LIKELY(NULL != s)) {
        do {
          if (is_equal_sign_demanded) {
            if (0 == ObString::make_string("=").case_compare(s)) {
              is_equal_sign_demanded = false;
            } else {
              OB_LOG(WARN, " '=' is expected", K(s));
              value_.valid_ = false;
            }
          } else if (key_idx < 0) {
            // single word, not kv
            int64_t idx = get_keywords_idx(s, is_equal_sign_demanded);
            if (idx < 0) {
              value_.valid_ = false;
              OB_LOG(WARN, " not expected isolate option", K(s));
            } else {
              if (is_equal_sign_demanded) {
                // key value
                key_idx = idx;
              } else {
                // isolate option
                key_idx = -1;
                process_isolated_option_(idx);
              }
            }
          } else {
            // demand value str
            process_kv_option_(key_idx, s);
            // reset key_idx
            key_idx = -1;
          }
        } while (value_.valid_ && OB_LIKELY(NULL != (s = STRTOK_R(NULL, " ", &saveptr))));
        if (key_idx >= 0) {
          value_.valid_ = false;
          OB_LOG(WARN, "kv option is not compelte", K(tmp_str));
        }
      } else {
        value_.valid_ = false;
        OB_LOG(WARN, "invalid config value", K(tmp_str));
      }
    }
    if (value_.valid_) {
      OB_LOG(INFO, "succ to set log_archive config value", K(value_), K(value_str_));
    } else {
      OB_LOG(WARN, "failed to set log_archive config value", K(value_), K(value_str_));
    }
  }
  return value_.valid_;
}

int64_t ObConfigLogArchiveOptionsItem::get_keywords_idx(const char* str, bool& is_key)
{
  int64_t idx = -1;
  is_key = false;
  for (int64_t i = 0; i < ARRAYSIZEOF(log_archive_config_keywords) && idx < 0; ++i) {
    if (0 == ObString::make_string(log_archive_config_keywords[i]).case_compare(str)) {
      idx = i;
      is_key = is_key_keyword(idx);
    }
  }
  return idx;
}

int64_t ObConfigLogArchiveOptionsItem::get_compression_option_idx(const char* str)
{
  int64_t idx = -1;
  for (int i = 0; i < ARRAYSIZEOF(log_archive_compression_values) && idx < 0; ++i) {
    if (0 == ObString::make_string(log_archive_compression_values[i]).case_compare(str)) {
      idx = i;
    }
  }
  return idx;
}

bool ObConfigLogArchiveOptionsItem::is_key_keyword(int64_t keyword_idx)
{
  return (LOG_ARCHIVE_COMPRESSION_IDX == keyword_idx || LOG_ARCHIVE_ENCRYPTION_MODE_IDX == keyword_idx ||
          LOG_ARCHIVE_ENCRYPTION_ALGORITHM_IDX == keyword_idx);
}

bool ObConfigLogArchiveOptionsItem::is_valid_isolate_option(const int64_t idx)
{
  bool bret = false;
  if (LOG_ARCHIVE_MANDATORY_IDX == idx) {
    bret = true;
  } else if (LOG_ARCHIVE_OPTIONAL_IDX == idx) {
    bret = true;
  } else {
    bret = false;
    OB_LOG(WARN, "invalid isolated option idx", K(idx));
  }
  return bret;
}

void ObConfigLogArchiveOptionsItem::process_isolated_option_(const int64_t idx)
{
  if (LOG_ARCHIVE_MANDATORY_IDX == idx) {
    value_.is_mandatory_ = true;
  } else if (LOG_ARCHIVE_OPTIONAL_IDX == idx) {
    value_.is_mandatory_ = false;
  } else {
    value_.valid_ = false;
    OB_LOG(WARN, "invalid isolated option idx", K(idx));
  }
}

void ObConfigLogArchiveOptionsItem::process_kv_option_(const int64_t key_idx, const char* value)
{
  static bool compression_state_map[] = {
      false,
      true,
      true,
      true,
  };
  static common::ObCompressorType compressor_type_map[] = {
      common::INVALID_COMPRESSOR,
      common::LZ4_COMPRESSOR,
      common::LZ4_COMPRESSOR,
      common::ZSTD_1_3_8_COMPRESSOR,
  };

  int ret = OB_SUCCESS;
  if (LOG_ARCHIVE_COMPRESSION_IDX == key_idx) {
    int64_t compression_option_idx = get_compression_option_idx(value);
    if (-1 == compression_option_idx || compression_option_idx >= ARRAYSIZEOF(compression_state_map) ||
        compression_option_idx >= ARRAYSIZEOF(compressor_type_map)) {
      OB_LOG(ERROR, "invalid compression_option_idx", K(compression_option_idx));
      value_.valid_ = false;
    } else {
      value_.is_compress_enabled_ = compression_state_map[compression_option_idx];
      value_.compressor_type_ = compressor_type_map[compression_option_idx];
    }
  } else if (LOG_ARCHIVE_ENCRYPTION_MODE_IDX == key_idx) {
    ObBackupEncryptionMode::EncryptionMode mode = ObBackupEncryptionMode::parse_str(value);
    if (ObBackupEncryptionMode::is_valid_for_log_archive(mode)) {
      value_.encryption_mode_ = mode;
      if (OB_FAIL(value_.set_default_encryption_algorithm())) {
        OB_LOG(WARN, "failed to set_default_encryption_algorithm", K(mode));
        value_.valid_ = false;
      }
    } else {
      OB_LOG(WARN, "invalid encrytion mode", K(mode));
      value_.valid_ = false;
    }
  } else if (LOG_ARCHIVE_ENCRYPTION_ALGORITHM_IDX == key_idx) {
    if (OB_FAIL(ObEncryptionUtil::parse_encryption_algorithm(value, value_.encryption_algorithm_))) {
      value_.valid_ = false;
      OB_LOG(WARN, "invalid encrytion algorithm", K(value_), K(value));
    } else if (!value_.is_encryption_meta_valid()) {
      value_.valid_ = false;
      OB_LOG(WARN, "invalid encrytion algorithm", K(value_));
    }
  } else {
    OB_LOG(WARN, "invalid key_idx", K(key_idx));
    value_.valid_ = false;
  }
}

int ObConfigLogArchiveOptionsItem::ObInnerConfigLogArchiveOptionsItem::set_default_encryption_algorithm()
{
  int ret = OB_SUCCESS;
  if (ObBackupEncryptionMode::NONE == encryption_mode_) {
    // do nothing
  } else if (ObBackupEncryptionMode::TRANSPARENT_ENCRYPTION == encryption_mode_) {
    encryption_algorithm_ = ObAesOpMode::ob_invalid_mode;
  } else {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid mode for log_archive", K(encryption_mode_));
  }
  return ret;
}

bool ObConfigLogArchiveOptionsItem::ObInnerConfigLogArchiveOptionsItem::is_encryption_meta_valid() const
{
  bool is_valid = false;
  return is_valid;
}

int ObConfigLogArchiveOptionsItem::format_option_str(const char* src, int64_t src_len, char* dest, int64_t dest_len)

{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src) || OB_UNLIKELY(src_len <= 0) || OB_ISNULL(dest) || dest_len < src_len) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", KR(ret), KP(src), KP(dest), K(src_len), K(dest_len));
  } else {
    const char* source_str = src;
    const char* locate_str = NULL;
    int64_t source_left_len = src_len;
    int32_t locate = -1;
    int64_t pos = 0;
    while (OB_SUCC(ret) && (source_left_len > 0) && (NULL != (locate_str = STRCHR(source_str, '=')))) {
      locate = locate_str - source_str;
      if (OB_FAIL(databuff_printf(dest, dest_len, pos, "%.*s = ", locate, source_str))) {
        OB_LOG(WARN, "failed to databuff_print", K(ret), K(dest), K(locate), K(source_str));
      } else {
        source_str = locate_str + 1;
        source_left_len -= (locate + 1);
      }
    }

    if (OB_SUCC(ret) && source_left_len > 0) {
      if (OB_FAIL(databuff_printf(dest, dest_len, pos, "%s", source_str))) {
        OB_LOG(WARN, "failed to databuff_print", KR(ret), K(dest), K(pos));
      }
    }
    if (OB_SUCC(ret)) {
      OB_LOG(INFO, "succ to format_option_str", K(src), K(dest));
    }
  }
  return ret;
}

}  // end of namespace common
}  // end of namespace oceanbase
