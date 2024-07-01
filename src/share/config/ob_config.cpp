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
#include "share/ob_cluster_version.h"
#include "share/ob_task_define.h"

using namespace oceanbase::share;
namespace oceanbase
{
namespace common
{
ObMemAttr g_config_mem_attr = SET_USE_UNEXPECTED_500("ConfigChecker");

const char *log_archive_config_keywords[] =
{
  "MANDATORY",
  "OPTIONAL",
  "COMPRESSION",
  //TODO(yaoying.yyy):暂时不开放归档加密
  //"ENCRYPTION_MODE",
//  "ENCRYPTION_ALGORITHM",
};

const char *log_archive_compression_values[] =
{
  "disable",
  "enable",
  "lz4_1.0",
  "zstd_1.3.8",
};

const char *log_archive_encryption_mode_values[] =
{
  "None",
  "Transparent Encryption",
};

const char *log_archive_encryption_algorithm_values[] =
{
  "None",
  "",
};

template<typename T>
static bool check_range(const ObConfigRangeOpts left_opt,
                        const ObConfigRangeOpts right_opt,
                        T &&value,
                        T &&min_value,
                        T &&max_value)
{
  bool left_ret = true;
  bool right_ret = true;
  if (ObConfigRangeOpts::OB_CONF_RANGE_NONE == left_opt) {
    // do nothing
  } else if (ObConfigRangeOpts::OB_CONF_RANGE_GREATER_THAN == left_opt) {
    left_ret = value > min_value;
  } else if (ObConfigRangeOpts::OB_CONF_RANGE_GREATER_EQUAL == left_opt) {
    left_ret = value >= min_value;
  } else {
    left_ret = false;
  }
  if (left_ret) {
    if (ObConfigRangeOpts::OB_CONF_RANGE_NONE == right_opt) {
      // do nothing
    } else if (ObConfigRangeOpts::OB_CONF_RANGE_LESS_THAN == right_opt) {
      right_ret = value < max_value;
    } else if (ObConfigRangeOpts::OB_CONF_RANGE_LESS_EQUAL == right_opt) {
      right_ret = value <= max_value;
    } else {
      right_ret = false;
    }
  }
  return left_ret && right_ret;
}

// ObConfigItem
ObConfigItem::ObConfigItem()
    : ck_(NULL), version_(0), dumped_version_(0), inited_(false), initial_value_set_(false),
      value_updated_(false), value_valid_(false), name_str_(nullptr), info_str_(nullptr),
      range_str_(nullptr), lock_()
{
}

ObConfigItem::~ObConfigItem()
{
  if (NULL != ck_) {
    ObConfigChecker *ck = const_cast<ObConfigChecker*>(ck_);
    OB_DELETE(ObConfigChecker, "unused", ck);
  }
}

void ObConfigItem::init(Scope::ScopeInfo scope_info,
                        const char *name,
                        const char *def,
                        const char *info,
                        const ObParameterAttr attr)
{
  if (OB_ISNULL(name) || OB_ISNULL(def) || OB_ISNULL(info)) {
    OB_LOG_RET(ERROR, common::OB_INVALID_ARGUMENT, "name or def or info is null", K(name), K(def), K(info));
  } else {
    set_name(name);
    if (!set_value(def)) {
      OB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "Set config item value failed", K(name), K(def));
    } else {
     set_info(info);
     attr_ = attr;
     attr_.set_scope(scope_info);
    }
  }
  inited_ = true;
}

const char *ObConfigItem::data_type() const
{
  const char *type_ptr = nullptr;
  switch(get_config_item_type()) {
    case ObConfigItemType::OB_CONF_ITEM_TYPE_BOOL: {
      type_ptr = DATA_TYPE_BOOL;
      break;
    }
    case ObConfigItemType::OB_CONF_ITEM_TYPE_INT: {
      type_ptr = DATA_TYPE_INT;
      break;
    }
    case ObConfigItemType::OB_CONF_ITEM_TYPE_DOUBLE: {
      type_ptr = DATA_TYPE_DOUBLE;
      break;
    }
    case ObConfigItemType::OB_CONF_ITEM_TYPE_STRING: {
      type_ptr = DATA_TYPE_STRING;
      break;
    }
    case ObConfigItemType::OB_CONF_ITEM_TYPE_INTEGRAL: {
      type_ptr = DATA_TYPE_INTEGRAL;
      break;
    }
    case ObConfigItemType::OB_CONF_ITEM_TYPE_STRLIST: {
      type_ptr = DATA_TYPE_STRLIST;
      break;
    }
    case ObConfigItemType::OB_CONF_ITEM_TYPE_INTLIST: {
      type_ptr = DATA_TYPE_INTLIST;
      break;
    }
    case ObConfigItemType::OB_CONF_ITEM_TYPE_TIME: {
      type_ptr = DATA_TYPE_TIME;
      break;
    }
    case ObConfigItemType::OB_CONF_ITEM_TYPE_MOMENT: {
      type_ptr = DATA_TYPE_MOMENT;
      break;
    }
    case ObConfigItemType::OB_CONF_ITEM_TYPE_CAPACITY: {
      type_ptr = DATA_TYPE_CAPACITY;
      break;
    }
    case ObConfigItemType::OB_CONF_ITEM_TYPE_LOGARCHIVEOPT: {
      type_ptr = DATA_TYPE_LOGARCHIVEOPT;
      break;
    }
    case ObConfigItemType::OB_CONF_ITEM_TYPE_VERSION: {
      type_ptr = DATA_TYPE_VERSION;
      break;
    }
    case ObConfigItemType::OB_CONF_ITEM_TYPE_MODE: {
      type_ptr = DATA_TYPE_MODE;
      break;
    }
    default: {
      // default: ObConfigItemType::OB_CONF_ITEM_TYPE_UNKNOWN and
      // other unexpected situations, return "UNKNOWN"
      type_ptr = DATA_TYPE_UNKNOWN;
      break;
    }
  }
  return type_ptr;
}
bool ObConfigItem::is_default(const char *value_str_,
                             const char *value_default_str_,
                             int64_t size) const
{
  return 0 == strncasecmp(value_str_, value_default_str_, size);
}

// ObConfigIntListItem
ObConfigIntListItem::ObConfigIntListItem(ObConfigContainer *container,
                                         Scope::ScopeInfo scope_info,
                                         const char *name,
                                         const char *def,
                                         const char *info,
                                         const ObParameterAttr attr)
    : value_()
{
  MEMSET(value_str_, 0, sizeof(value_str_));
  MEMSET(value_reboot_str_, 0, sizeof(value_reboot_str_));
  if (OB_LIKELY(NULL != container)) {
    container->set_refactored(ObConfigStringKey(name), this, 1);
  }
  init(scope_info, name, def, info, attr);
}

bool ObConfigIntListItem::set(const char *str)
{
  UNUSED(str);
  char *saveptr = NULL;
  char *s = NULL;
  char *endptr = NULL;
  value_.valid_ = true;

  while (value_.size_--) {
    value_.int_list_[value_.size_] = 0;
  }
  value_.size_ = 0;
  int ret = OB_SUCCESS;
  SMART_VAR(char[OB_MAX_CONFIG_VALUE_LEN], tmp_value_str) {
    MEMCPY(tmp_value_str, value_str_, sizeof (tmp_value_str));
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
ObConfigStrListItem::ObConfigStrListItem()
    : value_()
{
}

ObConfigStrListItem::ObConfigStrListItem(ObConfigContainer *container,
                                         Scope::ScopeInfo scope_info,
                                         const char *name,
                                         const char *def,
                                         const char *info,
                                         const ObParameterAttr attr)
    : value_()
{
  MEMSET(value_str_, 0, sizeof(value_str_));
  MEMSET(value_reboot_str_, 0, sizeof(value_reboot_str_));
  if (OB_LIKELY(NULL != container)) {
    container->set_refactored(ObConfigStringKey(name), this, 1);
  }
  init(scope_info, name, def, info, attr);
}

int ObConfigStrListItem::tryget(const int64_t idx, char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  const struct ObInnerConfigStrListItem *inner_value = &value_;
  ObLatch &latch = const_cast<ObLatch&>(inner_value->rwlock_);
  if (OB_ISNULL(buf)
      || OB_UNLIKELY(idx < 0)
      || OB_UNLIKELY(idx >= MAX_INDEX_SIZE)
      || OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "input argument is invalid", K(buf), K(idx), K(buf_len), K(ret));
  } else if (!inner_value->valid_) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "ValueStrList is not available, no need to get", K_(inner_value->valid), K(ret));
  } else if (OB_FAIL(latch.try_rdlock(ObLatchIds::CONFIG_LOCK))) {
    OB_LOG(WARN, "failed to tryrdlock rwlock_", K(ret));
  } else { //tryrdlock succ
    int print_size = 0;
    int32_t min_len = 0;
    const char *segment_str = NULL;
    if (idx >= inner_value->size_) {
      ret = OB_ARRAY_OUT_OF_RANGE;
    } else {
      segment_str = inner_value->value_str_bk_ + inner_value->idx_list_[idx];
      min_len = std::min(static_cast<int32_t>(STRLEN(segment_str) + 1),
                         static_cast<int32_t>(buf_len));
      print_size = snprintf(buf, static_cast<size_t>(buf_len), "%.*s", min_len, segment_str);
      if (print_size < 0 || print_size > min_len) {
        ret = OB_BUF_NOT_ENOUGH;
      }
    }
    latch.unlock();

    if (OB_FAIL(ret)) {
      OB_LOG(WARN, "failed to get value during lock",
             K(idx), K_(inner_value->size), K(buf_len), K(print_size), K(min_len), K(segment_str), K(ret));
    }
  }
  return ret;
}

int ObConfigStrListItem::get(const int64_t idx, char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  const struct ObInnerConfigStrListItem *inner_value = &value_;
  if (OB_ISNULL(buf) || idx < 0 || idx >= MAX_INDEX_SIZE || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "input argument is invalid", K(buf), K(idx), K(buf_len), K(ret));
  } else if (!inner_value->valid_) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "ValueStrList is not available, no need to get", K(inner_value->valid_), K(ret));
  } else {
    int print_size = 0;
    int32_t min_len = 0;
    const char *segment_str = NULL;
    ObLatch &latch = const_cast<ObLatch&>(inner_value->rwlock_);
    ObLatchRGuard rd_guard(latch, ObLatchIds::CONFIG_LOCK);
    if (idx >= inner_value->size_) {
      ret = OB_ARRAY_OUT_OF_RANGE;
    } else {
      segment_str = inner_value->value_str_bk_ + inner_value->idx_list_[idx];
      min_len = std::min(static_cast<int32_t>(STRLEN(segment_str) + 1),
                         static_cast<int32_t>(buf_len));
      print_size = snprintf(buf, static_cast<size_t>(min_len), "%.*s", min_len, segment_str);
      if (print_size < 0 || print_size > min_len) {
        ret = OB_BUF_NOT_ENOUGH;
      }
    }

    if (OB_FAIL(ret)) {
      OB_LOG(WARN, "failed to get value during lock",
             K(idx), K(inner_value->size_), K(buf_len), K(print_size),  K(min_len), K(segment_str), K(ret));
    }
  }
  return ret;
}

bool ObConfigStrListItem::set(const char *str)
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
          idx_list[curr_idx++] = i + 1; //record semicolon's site and set next idx site
        } else { //overflow
          bret = false;
        }
      } else {
        //do nothing
      }
    }

    if (bret) { // value_str_ is available, memcpy to value_str_bk_
      int print_size = 0;
      ObLatchRGuard wr_guard(value_.rwlock_, ObLatchIds::CONFIG_LOCK);
      value_.valid_ = true;
      value_.size_ = curr_idx;
      MEMCPY(value_.idx_list_, idx_list, static_cast<size_t>(curr_idx) * sizeof(int64_t));
      int32_t min_len = std::min(static_cast<int32_t>(sizeof(value_.value_str_bk_)),
                                 static_cast<int32_t>(length) + 1);
      print_size = snprintf(
          value_.value_str_bk_,static_cast<size_t>(min_len), "%.*s", min_len, value_str_);
      if (print_size < 0 || print_size > min_len) {
        value_.valid_ = false;
        bret = false;
      } else {
        for (int64_t i = 1; i < value_.size_; ++i) { // ';' --> '\0'
          value_.value_str_bk_[idx_list[i] - 1] = '\0';
        }
      }
    } else {
      OB_LOG_RET(WARN, common::OB_ERR_UNEXPECTED, "input str is not available", K(str), K_(value_.valid), K_(value_.size), K(bret));
    }
  } else {
    ObLatchRGuard wr_guard(value_.rwlock_, ObLatchIds::CONFIG_LOCK);
    value_.size_ = 0;
    value_.valid_ = true;
  }
  return bret;
}

// ObConfigIntegralItem
void ObConfigIntegralItem::init(Scope::ScopeInfo scope_info,
                                const char *name,
                                const char *def,
                                const char *range,
                                const char *info,
                                const ObParameterAttr attr)
{
  ObConfigItem::init(scope_info, name, def, info, attr);
  set_range(range);
  if (OB_ISNULL(range)) {
    OB_LOG_RET(ERROR, common::OB_INVALID_ARGUMENT, "Range is NULL");
  } else if (!parse_range(range)) {
    OB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "Parse check range fail", K(range));
  }
}

bool ObConfigIntegralItem::parse_range(const char *range)
{
  char buff[64] = {'\0'};
  const char *p_left = NULL;
  char *p_middle = NULL;
  char *p_right = NULL;
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
    if (!p_left || !p_middle || !p_right
        || p_left >= p_middle || p_middle >= p_right) {
      bool_ret = false;
      // not validated
    } else {
      bool valid = true;
      char ch_right = *p_right;
      *p_right = '\0';
      *p_middle = '\0';

      if ('\0' != p_left[1]) {
        int64_t val_left = parse(p_left + 1, valid);
        if (valid) {
          if (*p_left == '(') {
            min_value_ = val_left;
            left_interval_opt_ = ObConfigRangeOpts::OB_CONF_RANGE_GREATER_THAN;
          } else if (*p_left == '[') {
            min_value_ = val_left;
            left_interval_opt_ = ObConfigRangeOpts::OB_CONF_RANGE_GREATER_EQUAL;
          }
        }
      }

      if ('\0' != p_middle[1]) {
        int64_t val_right = parse(p_middle + 1, valid);
        if (valid) {
          if (')' == ch_right) {
            max_value_ = val_right;
            right_interval_opt_ = ObConfigRangeOpts::OB_CONF_RANGE_LESS_THAN;
          } else if (']' == ch_right) {
            max_value_ = val_right;
            right_interval_opt_ = ObConfigRangeOpts::OB_CONF_RANGE_LESS_EQUAL;
          }
        }
      }

      bool_ret = true;
    }
  }
  return bool_ret;
}

bool ObConfigIntegralItem::check() const
{
  // check order: value_valid_ --> range --> customized checker (for DEF_XXX_WITH_CHECKER)
  bool bool_ret = false;
  if (!value_valid_) {
  } else if (!check_range(left_interval_opt_, right_interval_opt_,
                          value_, min_value_, max_value_)) {
  } else if (ck_ && !ck_->check(*this)) {
  } else {
    bool_ret = true;
  }
  return bool_ret;
}

// ObConfigDoubleItem
ObConfigDoubleItem::ObConfigDoubleItem(ObConfigContainer *container,
                                       Scope::ScopeInfo scope_info,
                                       const char *name,
                                       const char *def,
                                       const char *range,
                                       const char *info,
                                       const ObParameterAttr attr)
    : value_(0), min_value_(0), max_value_(0),
      left_interval_opt_(ObConfigRangeOpts::OB_CONF_RANGE_NONE),
      right_interval_opt_(ObConfigRangeOpts::OB_CONF_RANGE_NONE)
{
  MEMSET(value_str_, 0, sizeof(value_str_));
  MEMSET(value_reboot_str_, 0, sizeof(value_reboot_str_));
  if (OB_LIKELY(NULL != container)) {
    container->set_refactored(ObConfigStringKey(name), this, 1);
  }
  init(scope_info, name, def, range, info, attr);
}

ObConfigDoubleItem::ObConfigDoubleItem(ObConfigContainer *container,
                                       Scope::ScopeInfo scope_info,
                                       const char *name,
                                       const char *def,
                                       const char *info,
                                       const ObParameterAttr attr)
    : value_(0), min_value_(0), max_value_(0),
      left_interval_opt_(ObConfigRangeOpts::OB_CONF_RANGE_NONE),
      right_interval_opt_(ObConfigRangeOpts::OB_CONF_RANGE_NONE)
{
  MEMSET(value_str_, 0, sizeof(value_str_));
  MEMSET(value_reboot_str_, 0, sizeof(value_reboot_str_));
  if (OB_LIKELY(NULL != container)) {
    container->set_refactored(ObConfigStringKey(name), this, 1);
  }
  init(scope_info, name, def, "", info, attr);
}

void ObConfigDoubleItem::init(Scope::ScopeInfo scope_info,
                              const char *name,
                              const char *def,
                              const char *range,
                              const char *info,
                              const ObParameterAttr attr)
{
  ObConfigItem::init(scope_info, name, def, info, attr);
  set_range(range);
  if (OB_ISNULL(range)) {
    OB_LOG_RET(ERROR, common::OB_INVALID_ARGUMENT, "Range is NULL");
  } else if (!parse_range(range)) {
    OB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "Parse check range fail", K(range));
  }
}

double ObConfigDoubleItem::parse(const char *str, bool &valid) const
{
  double v = 0.0;
  if (OB_ISNULL(str) || OB_UNLIKELY('\0' == str[0])) {
    valid = false;
  } else {
    char *endptr = NULL;
    v = strtod(str, &endptr);
    if (OB_ISNULL(endptr) || OB_UNLIKELY('\0' != *endptr)) {
      valid = false;
    } else {
      valid = true;
    }
  }
  return v;
}

bool ObConfigDoubleItem::parse_range(const char *range)
{
  char buff[64] = {'\0'};
  const char *p_left = NULL;
  char *p_middle = NULL;
  char *p_right = NULL;
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
      bool_ret = false; // not validated
    } else if (OB_UNLIKELY(p_left >= p_middle) || OB_UNLIKELY(p_middle >= p_right)) {
      bool_ret = false; // not validated
    } else {
      bool valid = true;
      char ch_right = *p_right;
      *p_right = '\0';
      *p_middle = '\0';

      double val_left = parse(p_left + 1, valid);
      if (valid) {
        if (*p_left == '(') {
          min_value_ = val_left;
          left_interval_opt_ = ObConfigRangeOpts::OB_CONF_RANGE_GREATER_THAN;
        } else if (*p_left == '[') {
          min_value_ = val_left;
          left_interval_opt_ = ObConfigRangeOpts::OB_CONF_RANGE_GREATER_EQUAL;
        }
      }

      double val_right = parse(p_middle + 1, valid);
      if (valid) {
        if (')' == ch_right) {
          max_value_ = val_right;
          right_interval_opt_ = ObConfigRangeOpts::OB_CONF_RANGE_LESS_THAN;
        } else if (']' == ch_right) {
          max_value_ = val_right;
          right_interval_opt_ = ObConfigRangeOpts::OB_CONF_RANGE_LESS_EQUAL;
        }
      }
      bool_ret = true;
    }
  }
  return bool_ret;
}

bool ObConfigDoubleItem::check() const
{
  // check order: value_valid_ --> range --> customized checker (for DEF_XXX_WITH_CHECKER)
  bool bool_ret = false;
  if (!value_valid_) {
  } else if (!check_range(left_interval_opt_, right_interval_opt_,
                          value_, min_value_, max_value_)) {
  } else if (ck_ && !ck_->check(*this)) {
  } else {
    bool_ret = true;
  }
  return bool_ret;
}

// ObConfigCapacityItem
ObConfigCapacityItem::ObConfigCapacityItem(ObConfigContainer *container,
                                           Scope::ScopeInfo scope_info,
                                           const char *name,
                                           const char *def,
                                           const char *range,
                                           const char *info,
                                           const ObParameterAttr attr)
{
  MEMSET(value_str_, 0, sizeof(value_str_));
  MEMSET(value_reboot_str_, 0, sizeof(value_reboot_str_));
  if (OB_LIKELY(NULL != container)) {
    container->set_refactored(ObConfigStringKey(name), this, 1);
  }
  init(scope_info, name, def, range, info, attr);
}

ObConfigCapacityItem::ObConfigCapacityItem(ObConfigContainer *container,
                                           Scope::ScopeInfo scope_info,
                                           const char *name,
                                           const char *def,
                                           const char *info,
                                           const ObParameterAttr attr)
{
  MEMSET(value_str_, 0, sizeof(value_str_));
  MEMSET(value_reboot_str_, 0, sizeof(value_reboot_str_));
  if (OB_LIKELY(NULL != container)) {
    container->set_refactored(ObConfigStringKey(name), this, 1);
  }
  init(scope_info, name, def, "", info, attr);
}

int64_t ObConfigCapacityItem::parse(const char *str, bool &valid) const
{
  int64_t value = ObConfigCapacityParser::get(str, valid, false);
  if (!valid) {
      OB_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "set capacity error", "name", name(), K(str), K(valid));
  }
  return value;
}

// ObConfigTimeItem
ObConfigTimeItem::ObConfigTimeItem(ObConfigContainer *container,
                                   Scope::ScopeInfo scope_info,
                                   const char *name,
                                   const char *def,
                                   const char *range,
                                   const char *info,
                                   const ObParameterAttr attr)
{
  MEMSET(value_str_, 0, sizeof(value_str_));
  MEMSET(value_reboot_str_, 0, sizeof(value_reboot_str_));
  if (OB_LIKELY(NULL != container)) {
    container->set_refactored(ObConfigStringKey(name), this, 1);
  }
  init(scope_info, name, def, range, info, attr);
}

ObConfigTimeItem::ObConfigTimeItem(ObConfigContainer *container,
                                   Scope::ScopeInfo scope_info,
                                   const char *name,
                                   const char *def,
                                   const char *info,
                                   const ObParameterAttr attr)
{
  MEMSET(value_str_, 0, sizeof(value_str_));
  MEMSET(value_reboot_str_, 0, sizeof(value_reboot_str_));
  if (OB_LIKELY(NULL != container)) {
    container->set_refactored(ObConfigStringKey(name), this, 1);
  }
  init(scope_info, name, def, "", info, attr);
}

int64_t ObConfigTimeItem::parse(const char *str, bool &valid) const
{
  int64_t value = ObConfigTimeParser::get(str, valid);
  if (!valid) {
      OB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "set time error", "name", name(), K(str), K(valid));
  }
  return value;
}

// ObConfigIntItem
ObConfigIntItem::ObConfigIntItem(ObConfigContainer *container,
                                 Scope::ScopeInfo scope_info,
                                 const char *name,
                                 const char *def,
                                 const char *range,
                                 const char *info,
                                 const ObParameterAttr attr)
{
  MEMSET(value_str_, 0, sizeof(value_str_));
  MEMSET(value_reboot_str_, 0, sizeof(value_reboot_str_));
  if (OB_LIKELY(NULL != container)) {
    container->set_refactored(ObConfigStringKey(name), this, 1);
  }
  init(scope_info, name, def, range, info, attr);
}

ObConfigIntItem::ObConfigIntItem(ObConfigContainer *container,
                                 Scope::ScopeInfo scope_info,
                                 const char *name,
                                 const char *def,
                                 const char *info,
                                 const ObParameterAttr attr)
{
  MEMSET(value_str_, 0, sizeof(value_str_));
  MEMSET(value_reboot_str_, 0, sizeof(value_reboot_str_));
  if (OB_LIKELY(NULL != container)) {
    container->set_refactored(ObConfigStringKey(name), this, 1);
  }
  init(scope_info, name, def, "", info, attr);
}

int64_t ObConfigIntItem::parse(const char *str, bool &valid) const
{
  int64_t value = ObConfigIntParser::get(str, valid);
  if (!valid) {
    OB_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "set int error", "name", name(), K(str), K(valid));
  }
  return value;
}

// ObConfigMomentItem
ObConfigMomentItem::ObConfigMomentItem(ObConfigContainer *container,
                                       Scope::ScopeInfo scope_info,
                                       const char *name,
                                       const char *def,
                                       const char *info,
                                       const ObParameterAttr attr)
    : value_()
{
  MEMSET(value_str_, 0, sizeof(value_str_));
  MEMSET(value_reboot_str_, 0, sizeof(value_reboot_str_));
  if (OB_LIKELY(NULL != container)) {
    container->set_refactored(ObConfigStringKey(name), this, 1);
  }
  init(scope_info, name, def, info, attr);
}

bool ObConfigMomentItem::set(const char *str)
{
  int ret = true;
  struct tm tm_value;
  memset(&tm_value, 0, sizeof(struct tm));
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
ObConfigBoolItem::ObConfigBoolItem(ObConfigContainer *container,
                                   Scope::ScopeInfo scope_info,
                                   const char *name,
                                   const char *def,
                                   const char *info,
                                   const ObParameterAttr attr)
    : value_(false)
{
  MEMSET(value_str_, 0, sizeof(value_str_));
  MEMSET(value_reboot_str_, 0, sizeof(value_reboot_str_));
  if (OB_LIKELY(NULL != container)) {
    container->set_refactored(ObConfigStringKey(name), this, 1);
  }
  init(scope_info, name, def, info, attr);
}

bool ObConfigBoolItem::set(const char *str)
{
  bool valid = false;
  const bool value = parse(str, valid);
  if (valid) {
    int64_t pos = 0;
    (void) databuff_printf(value_str_, sizeof (value_str_), pos, value ? "True" : "False");
    value_ = value;
  }
  return valid;
}

bool ObConfigBoolItem::parse(const char *str, bool &valid) const
{
  bool value = true;
  if (OB_ISNULL(str)) {
    valid = false;
    OB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "Get bool config item fail, str is NULL!");
  } else {
    value = ObConfigBoolParser::get(str, valid);
    if (!valid) {
      OB_LOG_RET(WARN, common::OB_ERR_UNEXPECTED, "Get bool config item fail", K(valid), K(str));
    }
  }
  return value;
}

// ObConfigStringItem
ObConfigStringItem::ObConfigStringItem(ObConfigContainer *container,
                                       Scope::ScopeInfo scope_info,
                                       const char *name,
                                       const char *def,
                                       const char *info,
                                       const ObParameterAttr attr)
{
  MEMSET(value_str_, 0, sizeof(value_str_));
  MEMSET(value_reboot_str_, 0, sizeof(value_reboot_str_));
  if (OB_LIKELY(NULL != container)) {
    container->set_refactored(ObConfigStringKey(name), this, 1);
  }
  init(scope_info, name, def, info, attr);
}

int ObConfigStringItem::copy(char *buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  const char *inner_value = value_str_;

  if (OB_FAIL(databuff_printf(buf, buf_len, "%s", inner_value))) {
    OB_LOG(WARN, "buffer not enough", K(ret), K(buf_len), K_(value_str));
  }
  return ret;
}

int ObConfigStringItem::deep_copy_value_string(ObIAllocator &allocator, ObString &dst)
{
  int ret = OB_SUCCESS;
  ObLatchRGuard rd_guard(const_cast<ObLatch&>(lock_), ObLatchIds::CONFIG_LOCK);
  ObString src = ObString::make_string(value_str_);
  if (OB_FAIL(ob_write_string(allocator, src, dst))) {
    OB_LOG(WARN, "fail to deep copy", KR(ret), K(src));
  }
  return ret;
}

ObConfigLogArchiveOptionsItem::ObConfigLogArchiveOptionsItem(ObConfigContainer *container,
                                                             Scope::ScopeInfo scope_info,
                                                             const char *name,
                                                             const char *def,
                                                             const char *info,
                                                             const ObParameterAttr attr)
: value_()
{
  if (OB_LIKELY(NULL != container)) {
    container->set_refactored(ObConfigStringKey(name), this, 1);
  }
  init(scope_info, name, def, info, attr);
}

bool ObConfigLogArchiveOptionsItem::set(const char *str)
{
  UNUSED(str);
  value_.valid_ = true;
  int ret = OB_SUCCESS;
  SMART_VAR(char[OB_MAX_CONFIG_VALUE_LEN], tmp_str) {
    MEMSET(tmp_str, 0, OB_MAX_CONFIG_VALUE_LEN);
    const int32_t str_len = static_cast<int32_t>(STRLEN(value_str_));
    const int64_t FORMAT_BUF_LEN = str_len * 3;// '=' will be replaced with ' = '
    char format_str_buf[FORMAT_BUF_LEN];
    MEMCPY(tmp_str, value_str_, str_len);
    tmp_str[str_len] = '\0';
    int ret = OB_SUCCESS;
    if (OB_FAIL(format_option_str(tmp_str, str_len, format_str_buf, FORMAT_BUF_LEN))) {
      value_.valid_ = false;
      OB_LOG(WARN, "failed to format_option_str", KR(ret), K(tmp_str));
    } else {
      char *saveptr = NULL;
      char *s = STRTOK_R(format_str_buf, " ", &saveptr);
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
            //single word, not kv
            int64_t idx = get_keywords_idx(s, is_equal_sign_demanded);
            if (idx < 0) {
              value_.valid_ = false;
              OB_LOG(WARN, " not expected isolate option", K(s));
            } else {
              if (is_equal_sign_demanded) {
                //key value
                key_idx = idx;
              } else {
                //isolate option
                key_idx = -1;
                process_isolated_option_(idx);
              }
            }
          } else {
            //demand value str
            process_kv_option_(key_idx, s);
            //reset key_idx
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

int64_t ObConfigLogArchiveOptionsItem::get_keywords_idx(const char *str, bool &is_key)
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

int64_t ObConfigLogArchiveOptionsItem::get_compression_option_idx(const char *str)
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
  return (LOG_ARCHIVE_COMPRESSION_IDX == keyword_idx
      || LOG_ARCHIVE_ENCRYPTION_MODE_IDX == keyword_idx
      || LOG_ARCHIVE_ENCRYPTION_ALGORITHM_IDX == keyword_idx);
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
    OB_LOG_RET(WARN, common::OB_INVALID_ARGUMENT, "invalid isolated option idx", K(idx));
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
    OB_LOG_RET(WARN, common::OB_INVALID_ARGUMENT, "invalid isolated option idx", K(idx));
  }
}

void ObConfigLogArchiveOptionsItem::process_kv_option_(const int64_t key_idx,
                                                       const char *value)
{
  static bool compression_state_map[] =
  {
    false,
    true,
    true,
    true,
  };
  static common::ObCompressorType compressor_type_map[] =
  {
    common::INVALID_COMPRESSOR,
    common::LZ4_COMPRESSOR,
    common::LZ4_COMPRESSOR,
    common::ZSTD_1_3_8_COMPRESSOR,
  };

  int ret = OB_SUCCESS;
  if (LOG_ARCHIVE_COMPRESSION_IDX == key_idx) {
    int64_t compression_option_idx = get_compression_option_idx(value);
    if (-1 == compression_option_idx
        || compression_option_idx >= ARRAYSIZEOF(compression_state_map)
        || compression_option_idx >= ARRAYSIZEOF(compressor_type_map)) {
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
    //do nothing
  } else if (ObBackupEncryptionMode::TRANSPARENT_ENCRYPTION == encryption_mode_) {
    encryption_algorithm_ = ObCipherOpMode::ob_aes_128_ecb;
  } else {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid mode for log_archive", K(encryption_mode_));
  }
  return ret;
}

bool ObConfigLogArchiveOptionsItem::ObInnerConfigLogArchiveOptionsItem::is_encryption_meta_valid() const
{
  bool is_valid = true;
  if (ObBackupEncryptionMode::NONE == encryption_mode_) {
    //do nothing
  } else if (ObBackupEncryptionMode::TRANSPARENT_ENCRYPTION == encryption_mode_) {
    switch (encryption_algorithm_) {
      case ObCipherOpMode::ob_aes_128_ecb:
      case ObCipherOpMode::ob_aes_192_ecb:
      case ObCipherOpMode::ob_aes_256_ecb:
      case ObCipherOpMode::ob_aes_128_gcm:
      case ObCipherOpMode::ob_aes_192_gcm:
      case ObCipherOpMode::ob_aes_256_gcm:
      case ObCipherOpMode::ob_sm4_cbc_mode:
      case ObCipherOpMode::ob_sm4_gcm:
        is_valid = true;
        break;
      case ObCipherOpMode::ob_aes_128_cbc:
      case ObCipherOpMode::ob_aes_192_cbc:
      case ObCipherOpMode::ob_aes_256_cbc:
      case ObCipherOpMode::ob_aes_128_cfb1:
      case ObCipherOpMode::ob_aes_192_cfb1:
      case ObCipherOpMode::ob_aes_256_cfb1:
      case ObCipherOpMode::ob_aes_128_cfb8:
      case ObCipherOpMode::ob_aes_192_cfb8:
      case ObCipherOpMode::ob_aes_256_cfb8:
      case ObCipherOpMode::ob_aes_128_cfb128:
      case ObCipherOpMode::ob_aes_192_cfb128:
      case ObCipherOpMode::ob_aes_256_cfb128:
      case ObCipherOpMode::ob_aes_128_ofb:
      case ObCipherOpMode::ob_aes_192_ofb:
      case ObCipherOpMode::ob_aes_256_ofb:
      case ObCipherOpMode::ob_sm4_mode:
      case ObCipherOpMode::ob_sm4_cbc:
      case ObCipherOpMode::ob_sm4_ecb:
      case ObCipherOpMode::ob_sm4_ofb:
      case ObCipherOpMode::ob_sm4_ctr:
      case ObCipherOpMode::ob_sm4_cfb128:
        is_valid = false;
        break;
      default:
        is_valid = false;
        break;
    }
  } else {
    is_valid = false;
  }
  return is_valid;
}

int ObConfigLogArchiveOptionsItem::format_option_str(const char *src, int64_t src_len,
                                                     char *dest, int64_t dest_len)


{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src) || OB_UNLIKELY(src_len <=0)
      || OB_ISNULL(dest) || dest_len < src_len) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", KR(ret), KP(src), KP(dest), K(src_len), K(dest_len));
  } else {
    const char *source_str = src;
    const char *locate_str = NULL;
    int64_t source_left_len = src_len;
    int32_t locate = -1;
    int64_t pos = 0;
    while (OB_SUCC(ret) && (source_left_len > 0)
           && (NULL != (locate_str = STRCHR(source_str, '=')))) {
      locate = static_cast<int32_t>(locate_str - source_str);
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
ObConfigModeItem::ObConfigModeItem(ObConfigContainer *container,
                                  Scope::ScopeInfo scope_info,
                                  const char *name,
                                  const char *def,
                                  ObConfigParser* parser,
                                  const char *info,
                                  const ObParameterAttr attr)
:parser_(parser)
{
  MEMSET(value_, 0, MAX_MODE_BYTES);
  if (OB_LIKELY(NULL != container)) {
    container->set_refactored(ObConfigStringKey(name), this, 1);
  }
  init(scope_info, name, def, info, attr);
}

ObConfigModeItem::~ObConfigModeItem()
{
  if (parser_ != NULL) {
    delete parser_;
  }
}

bool ObConfigModeItem::set(const char *str)
{
  bool valid = false;
  if (str == NULL || parser_ == NULL) {
    valid = false;
    OB_LOG_RET(WARN, OB_ERR_UNEXPECTED, "str or parser_ is NULL", K(str), K(parser_));
  } else {
    valid = parser_->parse(str, value_, MAX_MODE_BYTES);
    if (!valid) {
      OB_LOG_RET(WARN, OB_ERR_UNEXPECTED, "parse config item fail", K(str), K(parser_));
    }
  }
  return valid;
}

int ObConfigModeItem::init_mode(ObIConfigMode &mode)
{
  int ret = OB_SUCCESS;
  ObLatchRGuard r_guard(lock_, ObLatchIds::CONFIG_LOCK);
  if (OB_FAIL(mode.set_value(*this))) {
    OB_LOG(WARN, "set_value failed", KR(ret));
  };
  return ret;
}
ObConfigVersionItem::ObConfigVersionItem(ObConfigContainer *container,
                                         Scope::ScopeInfo scope_info,
                                         const char *name,
                                         const char *def,
                                         const char *range,
                                         const char *info,
                                         const ObParameterAttr attr)
    : dump_value_updated_(false)
{
  MEMSET(value_str_, 0, sizeof(value_str_));
  MEMSET(value_reboot_str_, 0, sizeof(value_reboot_str_));
  MEMSET(value_dump_str_, 0, sizeof(value_dump_str_));
  if (OB_LIKELY(NULL != container)) {
    container->set_refactored(ObConfigStringKey(name), this, 1);
  }
  init(scope_info, name, def, range, info, attr);
}

ObConfigVersionItem::ObConfigVersionItem(ObConfigContainer *container,
                                         Scope::ScopeInfo scope_info,
                                         const char *name,
                                         const char *def,
                                         const char *info,
                                         const ObParameterAttr attr)
    : dump_value_updated_(false)
{
  MEMSET(value_str_, 0, sizeof(value_str_));
  MEMSET(value_reboot_str_, 0, sizeof(value_reboot_str_));
  MEMSET(value_dump_str_, 0, sizeof(value_dump_str_));
  if (OB_LIKELY(NULL != container)) {
    container->set_refactored(ObConfigStringKey(name), this, 1);
  }
  init(scope_info, name, def, "", info, attr);
}

bool ObConfigVersionItem::set(const char *str)
{
  int64_t old_value = get_value();
  bool value_update = value_updated();
  bool valid = ObConfigIntegralItem::set(str);
  int64_t new_value = get_value();
  if (valid && value_update && old_value > new_value) {
    OB_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "Attention!!! data version is retrogressive", K(old_value), K(new_value));
  }
  if (value_update && old_value != new_value) {
    ObTaskController::get().allow_next_syslog();
    OB_LOG(INFO, "Config data version changed", K(old_value), K(new_value), K(value_update), K(valid));
  }
  return valid;
}

int64_t ObConfigVersionItem::parse(const char *str, bool &valid) const
{
  int ret = OB_SUCCESS;
  uint64_t version = 0;
  if (OB_FAIL(ObClusterVersion::get_version(str, version))) {
    OB_LOG(ERROR, "parse version failed", KR(ret), "name", name(), K(str));
  }
  valid = OB_SUCC(ret);
  return static_cast<int64_t>(version);
}

ObConfigVersionItem &ObConfigVersionItem::operator = (int64_t value)
{
  char buf[64] = {0};
  (void) snprintf(buf, sizeof(buf), "%ld", value);
  if (!set_value(buf)) {
    OB_LOG_RET(WARN, OB_ERR_UNEXPECTED, "obconfig version item set value failed");
  }
  return *this;
}

void ObConfigPairs::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  config_array_.reset();
  allocator_.clear();
}

int ObConfigPairs::assign(const ObConfigPairs &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    reset();
    int64_t array_cnt = other.config_array_.count();
    if (OB_FAIL(config_array_.reserve(array_cnt))) {
      OB_LOG(WARN, "fail to reserve array", KR(ret), K(array_cnt));
    } else {
      tenant_id_ = other.tenant_id_;
    }
    ObConfigPair pair;
    bool c_like_str = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < array_cnt; i++) {
      const ObConfigPair &other_pair = other.config_array_.at(i);
      if (OB_FAIL(ob_write_string(allocator_, other_pair.key_, pair.key_, c_like_str))) {
        OB_LOG(WARN, "fail to write string", KR(ret), K(other));
      } else if (OB_FAIL(ob_write_string(allocator_, other_pair.value_, pair.value_, c_like_str))) {
        OB_LOG(WARN, "fail to write string", KR(ret), K(other));
      } else if (OB_FAIL(config_array_.push_back(pair))) {
        OB_LOG(WARN, "fail to push back array", KR(ret), K(pair));
      }
    } // end for
  }
  return ret;
}

int ObConfigPairs::add_config(const ObString &key, const ObString &value)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < config_array_.count(); i++) {
    const ObConfigPair &pair = config_array_.at(i);
    if (0 == key.case_compare(pair.key_)) {
      ret = OB_ENTRY_EXIST;
      OB_LOG(WARN, "config already exist", KR(ret), K(key), K(value), K(pair));
    }
  } // end for

  ObConfigPair tmp_pair;
  bool c_like_str = true;
  if (FAILEDx(ob_write_string(allocator_, key, tmp_pair.key_, c_like_str))) {
    OB_LOG(WARN, "fail to write string", KR(ret), K(key));
  } else if (OB_FAIL(ob_write_string(allocator_, value, tmp_pair.value_, c_like_str))) {
    OB_LOG(WARN, "fail to write string", KR(ret), K(value));
  } else if (OB_FAIL(config_array_.push_back(tmp_pair))) {
    OB_LOG(WARN, "fail to push back array", KR(ret), K(tmp_pair));
  }
  return ret;
}

int ObConfigPairs::get_config_str(char *buf, const int64_t length) const
{
  int ret = OB_SUCCESS;
  int64_t array_cnt = config_array_.count();
  if (OB_UNLIKELY(
      array_cnt <= 0
      || OB_ISNULL(buf)
      || length <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arg", KR(ret), K(array_cnt), KP(buf), K(length));
  }
  int64_t pos = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < array_cnt; i++) {
    const ObConfigPair &pair = config_array_.at(i);
    if (OB_FAIL(databuff_printf(buf, length, pos, "%s%s=%s",
                (0 == pos ? "" : ","),
                pair.key_.ptr(), pair.value_.ptr()))) {
      OB_LOG(WARN, "fail to print pair", KR(ret), K(pair));
    }
  } // end for
  return ret;
}

int64_t ObConfigPairs::get_config_str_length() const
{
  int64_t length = config_array_.count() * 2; // reserved for some characters
  for (int64_t i = 0; i < config_array_.count(); i++) {
    length += config_array_.at(i).key_.length();
    length += config_array_.at(i).value_.length();
  } // end for
  return length;

}

} // end of namespace common
} // end of namespace oceanbase
