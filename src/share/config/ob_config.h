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

#ifndef OCEANBASE_SHARE_CONFIG_OB_CONFIG_H_
#define OCEANBASE_SHARE_CONFIG_OB_CONFIG_H_

#include <pthread.h>
#include "lib/compress/ob_compressor_pool.h"
#include "lib/container/ob_array_serialization.h"
#include "share/config/ob_config_helper.h"
#include "share/ob_encryption_util.h"
#include "share/parameter/ob_parameter_attr.h"

namespace oceanbase
{
namespace common
{

enum ObLogArchiveKeywordIdx {
  LOG_ARCHIVE_MANDATORY_IDX = 0,
  LOG_ARCHIVE_OPTIONAL_IDX = 1,
  LOG_ARCHIVE_COMPRESSION_IDX = 2,
  LOG_ARCHIVE_ENCRYPTION_MODE_IDX = 3,
  LOG_ARCHIVE_ENCRYPTION_ALGORITHM_IDX = 4,
};

enum ObConfigItemType{
  OB_CONF_ITEM_TYPE_UNKNOWN = -1,
  OB_CONF_ITEM_TYPE_BOOL = 0,
  OB_CONF_ITEM_TYPE_INT = 1,
  OB_CONF_ITEM_TYPE_DOUBLE = 2,
  OB_CONF_ITEM_TYPE_STRING = 3,
  OB_CONF_ITEM_TYPE_INTEGRAL = 4,
  OB_CONF_ITEM_TYPE_STRLIST = 5,
  OB_CONF_ITEM_TYPE_INTLIST = 6,
  OB_CONF_ITEM_TYPE_TIME = 7,
  OB_CONF_ITEM_TYPE_MOMENT = 8,
  OB_CONF_ITEM_TYPE_CAPACITY = 9,
  OB_CONF_ITEM_TYPE_LOGARCHIVEOPT = 10,
  OB_CONF_ITEM_TYPE_VERSION = 11,
  OB_CONF_ITEM_TYPE_MODE = 12,
};

static const char *const DATA_TYPE_UNKNOWN = "UNKNOWN";
static const char *const DATA_TYPE_BOOL = "BOOL";
static const char *const DATA_TYPE_INT = "INT";
static const char *const DATA_TYPE_DOUBLE = "DOUBLE";
static const char *const DATA_TYPE_STRING = "STRING";
static const char *const DATA_TYPE_INTEGRAL = "INTEGRAL";
static const char *const DATA_TYPE_STRLIST = "STR_LIST";
static const char *const DATA_TYPE_INTLIST = "INT_LIST";
static const char *const DATA_TYPE_TIME = "TIME";
static const char *const DATA_TYPE_MOMENT = "MOMENT";
static const char *const DATA_TYPE_CAPACITY = "CAPACITY";
static const char *const DATA_TYPE_LOGARCHIVEOPT = "LOGARCHIVEOPT";
static const char *const DATA_TYPE_VERSION = "VERSION";
static const char *const DATA_TYPE_MODE = "MODE";

enum class ObConfigRangeOpts {
  OB_CONF_RANGE_NONE,
  OB_CONF_RANGE_GREATER_THAN,
  OB_CONF_RANGE_GREATER_EQUAL,
  OB_CONF_RANGE_LESS_THAN,
  OB_CONF_RANGE_LESS_EQUAL,
};

extern ObMemAttr g_config_mem_attr;
class ObConfigItem
{
public:
  ObConfigItem();
  virtual ~ObConfigItem();

  void init(Scope::ScopeInfo scope_info,
            const char *name,
            const char *def,
            const char *info,
            const ObParameterAttr attr = ObParameterAttr());
  void add_checker(const ObConfigChecker *new_ck)
  {
    ck_ = OB_NEW(ObConfigConsChecker, g_config_mem_attr, ck_, new_ck);
  }
  virtual bool check() const
  {
    return NULL == ck_ ? value_valid_ : value_valid_ && ck_->check(*this);
  }
  virtual bool check_unit(const char *str) const
  {
    UNUSED(str);
    return true;
  }
  bool set_value(const common::ObString &string)
  {
    int64_t pos = 0;
    int ret = OB_SUCCESS;
    ObLatchWGuard wr_guard(lock_, ObLatchIds::CONFIG_LOCK);
    const char *ptr = value_ptr();
    if (nullptr == ptr) {
      value_valid_ = false;
    } else if (OB_FAIL(databuff_printf(const_cast<char *>(ptr), value_len(), pos,
                                       "%.*s", string.length(), string.ptr()))) {
      value_valid_ = false;
    } else {
      value_valid_ = set(ptr);
      if (inited_ && value_valid_) {
        value_updated_ = true;
      }
    }
    return value_valid_;
  }
  bool set_value(const char *str)
  {
    int64_t pos = 0;
    int ret = OB_SUCCESS;
    ObLatchWGuard wr_guard(lock_, ObLatchIds::CONFIG_LOCK);
    const char *ptr = value_ptr();
    if (nullptr == ptr) {
      value_valid_ = false;
    } else if (OB_FAIL(databuff_printf(const_cast<char *>(ptr), value_len(), pos, "%s", str))) {
      value_valid_ = false;
    } else {
      value_valid_ = set(str);
      if (inited_ && value_valid_) {
        value_updated_ = true;
      }
    }
    return value_valid_;
  }
  // 重启生效的配置项，需要保存并dump到spfile中
  bool set_reboot_value(const char *str)
  {
    int64_t pos = 0;
    int ret = OB_SUCCESS;
    const char *ptr = value_reboot_ptr();
    if (nullptr == ptr) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      ret = databuff_printf(const_cast<char *>(ptr), value_reboot_len(), pos, "%s", str);
    }
    return ret == OB_SUCCESS;
  }
  virtual bool set_dump_value(const char *str)
  {
    UNUSED(str);
    return false;
  }
  virtual void set_dump_value_updated()
  {
    // do nothing
  }
  virtual bool dump_value_updated() const
  {
    return false;
  }
  void set_value_updated()
  {
    value_updated_ = true;
  }
  bool value_updated()
  {
    return value_updated_;
  }
  void set_name(const char *name)
  {
    name_str_ = name;
  }
  void set_info(const char *info)
  {
    info_str_ = info;;
  }
  void set_range(const char* range)
  {
    range_str_ = range;
  }
  void set_version(int64_t version) { version_ = version; }
  void set_dumped_version(int64_t version) { dumped_version_ = version; }

  const char *str() const
  {
    ObLatchRGuard rd_guard(const_cast<ObLatch&>(lock_), ObLatchIds::CONFIG_LOCK);
    return value_ptr();
  }
  int case_compare(const char* str) const
  {
    ObLatchRGuard rd_guard(const_cast<ObLatch&>(lock_), ObLatchIds::CONFIG_LOCK);
    return ObString::make_string(value_ptr()).case_compare(str);
  }
  const char *default_str() const
  {
    ObLatchRGuard rd_guard(const_cast<ObLatch&>(lock_), ObLatchIds::CONFIG_LOCK);
    return value_default_ptr();
  }
  virtual const char *spfile_str() const
  {
    const char *ret = nullptr;
    ObLatchRGuard rd_guard(const_cast<ObLatch&>(lock_), ObLatchIds::CONFIG_LOCK);
    if (reboot_effective() && is_initial_value_set()) {
      ret = value_reboot_ptr();
    } else {
      ret = value_ptr();
    }
    return ret;
  }
  const char *name() const { return name_str_; }
  const char *info() const { return info_str_; }
  const char *range() const { return range_str_; }

  const char *section() const { return attr_.get_section(); }
  const char *scope() const { return attr_.get_scope(); }
  const char *source() const { return attr_.get_source(); }
  const char *edit_level() const { return attr_.get_edit_level(); }
  const char *data_type() const;
  /*obs启动首次读库设置该值*/
  void initial_value_set() { initial_value_set_ = true; }
  bool is_initial_value_set() const { return initial_value_set_; }
  int64_t version() const { return version_; }
  int64_t dumped_version() const { return dumped_version_; }

  bool invisible() const
  {
    return attr_.is_invisible();
  }

  bool is_not_editable() const
  {
    return attr_.is_readonly();
  }
  bool reboot_effective() const
  {
    return attr_.is_static();
  }
  virtual bool is_default(const char *value_str_,
                          const char *value_default_str_,
                          int64_t size) const;
  virtual bool operator >(const char *) const { return false; }
  virtual bool operator >=(const char *) const { return false; }
  virtual bool operator <(const char *) const { return false; }
  virtual bool operator <=(const char *) const { return false; }

  virtual ObConfigItemType get_config_item_type() const {
    return ObConfigItemType::OB_CONF_ITEM_TYPE_UNKNOWN;
  }
protected:
  //use current value to do input operation
  virtual bool set(const char *str) = 0;
  virtual const char *value_ptr() const = 0;
  virtual const char *value_reboot_ptr() const = 0;
  virtual const char *value_default_ptr() const = 0;
  virtual uint64_t value_len() const = 0;
  virtual uint64_t value_reboot_len() const = 0;
  const ObConfigChecker *ck_;
  int64_t version_;
  int64_t dumped_version_;
  bool inited_;
  bool initial_value_set_;
  bool value_updated_;
  bool value_valid_;
  const char* name_str_;
  const char* info_str_;
  const char* range_str_;
  common::ObLatch lock_;
private:
  ObParameterAttr attr_;
  DISALLOW_COPY_AND_ASSIGN(ObConfigItem);
};

class ObConfigIntListItem
  : public ObConfigItem
{
public:
  ObConfigIntListItem(ObConfigContainer *container,
                      Scope::ScopeInfo scope_info,
                      const char *name,
                      const char *def,
                      const char *info,
                      const ObParameterAttr attr = ObParameterAttr());
  virtual ~ObConfigIntListItem() {}

  //need reboot value need set it once startup, otherwise it will output current value
  const int64_t &operator[](int idx) const { return value_.int_list_[idx]; }
  int64_t &operator[](int idx) { return value_.int_list_[idx]; }
  ObConfigIntListItem &operator=(const char *str)
  {
    if (!set_value(str)) {
      OB_LOG_RET(WARN, common::OB_ERR_UNEXPECTED, "obconfig int list item set value failed");
    }
    return *this;
  }
  int size() const { return value_.size_; }
  bool valid() const { return value_.valid_; }
  virtual ObConfigItemType get_config_item_type() const {
    return ObConfigItemType::OB_CONF_ITEM_TYPE_INTLIST;
  }

protected:
  //use current value to do input operation
  bool set(const char *str);
  const char *value_ptr() const override
  {
    return value_str_;
  }
  const char *value_reboot_ptr() const override
  {
    return value_reboot_str_;
  }
  uint64_t value_len() const override
  {
    return sizeof(value_str_);
  }
  uint64_t value_reboot_len() const override
  {
    return sizeof(value_reboot_str_);
  }
  static const int64_t MAX_INDEX_SIZE = 64;
  struct ObInnerConfigIntListItem
  {
    ObInnerConfigIntListItem()
      : size_(0), valid_(false)
    {
      MEMSET(int_list_, 0, sizeof(int_list_));
    }
    ~ObInnerConfigIntListItem() {}

    int64_t int_list_[MAX_INDEX_SIZE];
    int size_;
    bool valid_;
  };

  struct ObInnerConfigIntListItem value_;
  static const uint64_t VALUE_BUF_SIZE = 32 * MAX_INDEX_SIZE;
  char value_str_[VALUE_BUF_SIZE];
  char value_reboot_str_[VALUE_BUF_SIZE];
private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigIntListItem);
};

class ObConfigStrListItem
  : public ObConfigItem
{
public:
  ObConfigStrListItem();
  ObConfigStrListItem(ObConfigContainer *container,
                      Scope::ScopeInfo scope_info,
                      const char *name,
                      const char *def,
                      const char *info,
                      const ObParameterAttr attr = ObParameterAttr());
  virtual ~ObConfigStrListItem() {}

  int tryget(const int64_t idx, char *buf, const int64_t buf_len) const;
  int get(const int64_t idx, char *buf, const int64_t buf_len) const;
  int get_str_item_length(const int64_t idx, int64_t &length) const
  {
    int ret = OB_SUCCESS;
    if (0 <= idx && idx < size() - 1) {
      if (idx < size() - 2) {
        length = value_.idx_list_[idx + 1] - value_.idx_list_[idx];
      } else {
        length = STRLEN(value_.value_str_bk_) - value_.idx_list_[idx];
      }
    } else {
      ret = OB_ARRAY_OUT_OF_RANGE;
    }
    return ret;
  }

  ObConfigStrListItem &operator=(const char *str)
  {
    if (!set_value(str)) {
      OB_LOG_RET(WARN, common::OB_ERR_UNEXPECTED, "obconfig str list item set value failed");
    }
    return *this;
  }

  //need reboot value need set it once startup, otherwise it will output current value
  int64_t size() const { return value_.size_; }
  bool valid() const { return value_.valid_; }
  virtual ObConfigItemType get_config_item_type() const {
    return ObConfigItemType::OB_CONF_ITEM_TYPE_STRLIST;
  }
public:
  static const int64_t MAX_INDEX_SIZE = 64;
  static const uint64_t VALUE_BUF_SIZE = 65536UL;
  struct ObInnerConfigStrListItem
  {
    ObInnerConfigStrListItem()
      : valid_(false), size_(0), rwlock_()
    {
      MEMSET(idx_list_, 0, sizeof(idx_list_));
      MEMSET(value_str_bk_, 0, sizeof(value_str_bk_));
    }
    ~ObInnerConfigStrListItem() {}

    ObInnerConfigStrListItem &operator = (const ObInnerConfigStrListItem &value)
    {
      if (this == &value) {
        //do nothing
      } else {
        ObLatchRGuard rd_guard(const_cast<ObLatch&>(value.rwlock_), ObLatchIds::CONFIG_LOCK);
        ObLatchWGuard wr_guard(rwlock_, ObLatchIds::CONFIG_LOCK);

        valid_ = value.valid_;
        size_ = value.size_;
        MEMCPY(idx_list_, value.idx_list_, sizeof(idx_list_));
        MEMCPY(value_str_bk_, value.value_str_bk_, sizeof(value_str_bk_));
      }
      return *this;
    }

    ObInnerConfigStrListItem(const ObInnerConfigStrListItem &value)
    {
      if (this == &value) {
        //do nothing
      } else {
        ObLatchRGuard rd_guard(const_cast<ObLatch&>(value.rwlock_), ObLatchIds::CONFIG_LOCK);
        ObLatchWGuard wr_guard(rwlock_, ObLatchIds::CONFIG_LOCK);

        valid_ = value.valid_;
        size_ = value.size_;
        MEMCPY(idx_list_, value.idx_list_, sizeof(idx_list_));
        MEMCPY(value_str_bk_, value.value_str_bk_, sizeof(value_str_bk_));
      }
    }

    bool valid_;
    int64_t size_;
    int64_t idx_list_[MAX_INDEX_SIZE];
    char value_str_bk_[VALUE_BUF_SIZE];
    ObLatch rwlock_;
  };

  struct ObInnerConfigStrListItem value_;

protected:
  //use current value to do input operation
  bool set(const char *str);
  const char *value_ptr() const override
  {
    return value_str_;
  }
  const char *value_reboot_ptr() const override
  {
    return value_reboot_str_;
  }
  uint64_t value_len() const override
  {
    return sizeof(value_str_);
  }
  uint64_t value_reboot_len() const override
  {
    return sizeof(value_reboot_str_);
  }

  char value_str_[VALUE_BUF_SIZE];
  char value_reboot_str_[VALUE_BUF_SIZE];
private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigStrListItem);
};

class ObConfigIntegralItem
  : public ObConfigItem
{
public:
  ObConfigIntegralItem()
    : value_(0), min_value_(0), max_value_(0),
      left_interval_opt_(ObConfigRangeOpts::OB_CONF_RANGE_NONE),
      right_interval_opt_(ObConfigRangeOpts::OB_CONF_RANGE_NONE)
  {
  }
  virtual ~ObConfigIntegralItem() {}

  bool operator >(const char *str) const
  { bool valid = true; return get_value() > parse(str, valid) && valid; }
  bool operator >=(const char *str) const
  { bool valid = true; return get_value() >= parse(str, valid) && valid; }
  bool operator <(const char *str) const
  { bool valid = true; return get_value() < parse(str, valid) && valid; }
  bool operator <=(const char *str) const
  { bool valid = true; return get_value() <= parse(str, valid) && valid; }

  // get_value() return the real-time value
  int64_t get_value() const { return value_; }
  // get() return the real-time value if it does not need reboot, otherwise it return initial_value
  int64_t get() const { return value_; }
  operator const int64_t &() const { return value_; }

  bool parse_range(const char *range);
  void init(Scope::ScopeInfo scope_info,
            const char *name,
            const char *def,
            const char *range,
            const char *info,
            const ObParameterAttr attr = ObParameterAttr());
  virtual ObConfigItemType get_config_item_type() const {
    return ObConfigItemType::OB_CONF_ITEM_TYPE_INTEGRAL;
  }
  virtual bool check() const override;
protected:
  //use current value to do input operation
  virtual bool set(const char *str);
  virtual int64_t parse(const char *str, bool &valid) const = 0;

private:
  int64_t value_;
  int64_t min_value_;
  int64_t max_value_;
  ObConfigRangeOpts left_interval_opt_;
  ObConfigRangeOpts right_interval_opt_;
  DISALLOW_COPY_AND_ASSIGN(ObConfigIntegralItem);
};
inline bool ObConfigIntegralItem::set(const char *str)
{
  bool valid = true;
  const int64_t value = parse(str, valid);
  if (valid) {
    value_ = value;
  }
  return valid;
}

class ObConfigDoubleItem
  : public ObConfigItem
{
public:
  ObConfigDoubleItem(ObConfigContainer *container,
                     Scope::ScopeInfo scope_info,
                     const char *name,
                     const char *def,
                     const char *range,
                     const char *info,
                     const ObParameterAttr attr = ObParameterAttr());
  ObConfigDoubleItem(ObConfigContainer *container,
                     Scope::ScopeInfo scope_info,
                     const char *name,
                     const char *def,
                     const char *info,
                     const ObParameterAttr attr = ObParameterAttr());
  virtual ~ObConfigDoubleItem() {}

  bool operator >(const char *str) const
  { bool valid = true; return get_value() > parse(str, valid) && valid; }
  bool operator >=(const char *str) const
  { bool valid = true; return get_value() >= parse(str, valid) && valid; }
  bool operator <(const char *str) const
  { bool valid = true; return get_value() < parse(str, valid) && valid; }
  bool operator <=(const char *str) const
  { bool valid = true; return get_value() <= parse(str, valid) && valid; }

  double get_value() const { return value_; }

  //need reboot value need set it once startup, otherwise it will output current value
  double get() const { return value_; }
  operator const double &() const { return value_; }

  ObConfigDoubleItem &operator = (double value);
  void init(Scope::ScopeInfo scope_info,
            const char *name,
            const char *def,
            const char *range,
            const char *info,
            const ObParameterAttr attr = ObParameterAttr());
  bool parse_range(const char *range);

  virtual ObConfigItemType get_config_item_type() const {
    return ObConfigItemType::OB_CONF_ITEM_TYPE_DOUBLE;
  }
  virtual bool check() const override;
protected:
  //use current value to do input operation
  bool set(const char *str);
  double parse(const char *str, bool &valid) const;
  const char *value_ptr() const override
  {
    return value_str_;
  }
  const char *value_reboot_ptr() const override
  {
    return value_reboot_str_;
  }
  uint64_t value_len() const override
  {
    return sizeof(value_str_);
  }
  uint64_t value_reboot_len() const override
  {
    return sizeof(value_reboot_str_);
  }

  static const uint64_t VALUE_BUF_SIZE = 64UL;
  char value_str_[VALUE_BUF_SIZE];
  char value_reboot_str_[VALUE_BUF_SIZE];

private:
  double value_;
  double min_value_;
  double max_value_;
  ObConfigRangeOpts left_interval_opt_;
  ObConfigRangeOpts right_interval_opt_;
  DISALLOW_COPY_AND_ASSIGN(ObConfigDoubleItem);
};
inline ObConfigDoubleItem &ObConfigDoubleItem::operator = (double value)
{
  char buf[2L<<10];
  (void) snprintf(buf, sizeof(buf), "%f", value);
  if (!set_value(buf)) {
    OB_LOG_RET(WARN, common::OB_ERR_UNEXPECTED, "obconfig double item set value failed");
  }
  return *this;
}
inline bool ObConfigDoubleItem::set(const char *str)
{
  bool valid = true;
  const double value = parse(str, valid);
  if (valid) {
    value_ = value;
  }
  return valid;
}


class ObConfigCapacityItem
  : public ObConfigIntegralItem
{
public:
  ObConfigCapacityItem(ObConfigContainer *container,
                       Scope::ScopeInfo scope_info,
                       const char *name,
                       const char *def,
                       const char *range,
                       const char *info,
                       const ObParameterAttr attr = ObParameterAttr());
  ObConfigCapacityItem(ObConfigContainer *container,
                       Scope::ScopeInfo scope_info,
                       const char *name,
                       const char *def,
                       const char *info,
                       const ObParameterAttr attr = ObParameterAttr());
  virtual ~ObConfigCapacityItem() {}

  ObConfigCapacityItem &operator = (int64_t value);
  virtual bool check_unit(const char *str) const
  {
    bool is_valid;
    IGNORE_RETURN ObConfigCapacityParser::get(str, is_valid);
    return is_valid;
  }

  virtual ObConfigItemType get_config_item_type() const {
    return ObConfigItemType::OB_CONF_ITEM_TYPE_CAPACITY;
  }
protected:
  int64_t parse(const char *str, bool &valid) const;
  const char *value_ptr() const override
  {
    return value_str_;
  }
  const char *value_reboot_ptr() const override
  {
    return value_reboot_str_;
  }
  uint64_t value_len() const override
  {
    return sizeof(value_str_);
  }
  uint64_t value_reboot_len() const override
  {
    return sizeof(value_reboot_str_);
  }

  static const uint64_t VALUE_BUF_SIZE = 32UL;
  char value_str_[VALUE_BUF_SIZE];
  char value_reboot_str_[VALUE_BUF_SIZE];

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigCapacityItem);
};
inline ObConfigCapacityItem &ObConfigCapacityItem::operator = (int64_t value)
{
  char buf[2L<<10];
  (void) snprintf(buf, sizeof(buf), "%ldB", value);
  if (!set_value(buf)) {
    OB_LOG_RET(WARN, common::OB_ERR_UNEXPECTED, "obconfig capacity item set value failed");
  }
  return *this;
}

class ObConfigTimeItem
  : public ObConfigIntegralItem
{
public:
  ObConfigTimeItem(ObConfigContainer *container,
                   Scope::ScopeInfo scope_info,
                   const char *name,
                   const char *def,
                   const char *range,
                   const char *info,
                   const ObParameterAttr attr = ObParameterAttr());
  ObConfigTimeItem(ObConfigContainer *container,
                   Scope::ScopeInfo scope_info,
                   const char *name,
                   const char *def,
                   const char *info,
                   const ObParameterAttr attr = ObParameterAttr());
  virtual ~ObConfigTimeItem() {}
  ObConfigTimeItem &operator = (int64_t value);
  virtual ObConfigItemType get_config_item_type() const {
    return ObConfigItemType::OB_CONF_ITEM_TYPE_TIME;
  }
protected:
  int64_t parse(const char *str, bool &valid) const;
  const char *value_ptr() const override
  {
    return value_str_;
  }
  const char *value_reboot_ptr() const override
  {
    return value_reboot_str_;
  }
  uint64_t value_len() const override
  {
    return sizeof(value_str_);
  }
  uint64_t value_reboot_len() const override
  {
    return sizeof(value_reboot_str_);
  }

  static const uint64_t VALUE_BUF_SIZE = 32UL;
  char value_str_[VALUE_BUF_SIZE];
  char value_reboot_str_[VALUE_BUF_SIZE];

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigTimeItem);
};
inline ObConfigTimeItem &ObConfigTimeItem::operator = (int64_t value){
  char buf[2L<<10];
  (void) snprintf(buf, sizeof(buf), "%ldus", value);
  if (!set_value(buf)) {
    OB_LOG_RET(WARN, common::OB_ERR_UNEXPECTED, "obconfig time item set value failed");
  }
  return *this;
}

class ObConfigIntItem
  : public ObConfigIntegralItem
{
public:
  ObConfigIntItem(ObConfigContainer *container,
                  Scope::ScopeInfo scope_info,
                  const char *name,
                  const char *def,
                  const char *range,
                  const char *info,
                  const ObParameterAttr attr = ObParameterAttr());
  ObConfigIntItem(ObConfigContainer *container,
                  Scope::ScopeInfo scope_info,
                  const char *name,
                  const char *def,
                  const char *info,
                  const ObParameterAttr attr = ObParameterAttr());
  virtual ~ObConfigIntItem() {}
  ObConfigIntItem &operator = (int64_t value);
  virtual ObConfigItemType get_config_item_type() const {
    return ObConfigItemType::OB_CONF_ITEM_TYPE_INT;
  }
protected:
  int64_t parse(const char *str, bool &valid) const;
  const char *value_ptr() const override
  {
    return value_str_;
  }
  const char *value_reboot_ptr() const override
  {
    return value_reboot_str_;
  }
  uint64_t value_len() const override
  {
    return sizeof(value_str_);
  }
  uint64_t value_reboot_len() const override
  {
    return sizeof(value_reboot_str_);
  }

  static const uint64_t VALUE_BUF_SIZE = 32UL;
  char value_str_[VALUE_BUF_SIZE];
  char value_reboot_str_[VALUE_BUF_SIZE];

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigIntItem);
};
inline ObConfigIntItem &ObConfigIntItem::operator = (int64_t value)
{
  char buf[64];
  (void) snprintf(buf, sizeof(buf), "%ld", value);
  if (!set_value(buf)) {
    OB_LOG_RET(WARN, common::OB_ERR_UNEXPECTED, "obconfig int item set value failed");
  }
  return *this;
}

class ObConfigMomentItem
  : public ObConfigItem
{
public:
  ObConfigMomentItem(ObConfigContainer *container,
                     Scope::ScopeInfo scope_info,
                     const char *name,
                     const char *def,
                     const char *info,
                     const ObParameterAttr attr = ObParameterAttr());
  virtual ~ObConfigMomentItem() {}
  //use current value to do input operation
  bool set(const char *str);

  //need reboot value need set it once startup, otherwise it will output current value
  bool disable() const { return value_.disable_; }
  int hour() const { return value_.hour_; }
  int minute() const { return value_.minute_; }
  virtual ObConfigItemType get_config_item_type() const {
    return ObConfigItemType::OB_CONF_ITEM_TYPE_MOMENT;
  }
  ObConfigMomentItem &operator=(const char *str)
  {
    if (!set_value(str)) {
      OB_LOG_RET(WARN, common::OB_ERR_UNEXPECTED, "obconfig moment item set value failed");
    }
    return *this;
  }
public:
  struct ObInnerConfigMomentItem
  {
    ObInnerConfigMomentItem() : disable_(true), hour_(-1), minute_(-1) {}
    ~ObInnerConfigMomentItem() {}

    bool disable_;
    int hour_;
    int minute_;
  };

protected:
  const char *value_ptr() const override
  {
    return value_str_;
  }
  const char *value_reboot_ptr() const override
  {
    return value_reboot_str_;
  }
  uint64_t value_len() const override
  {
    return sizeof(value_str_);
  }
  uint64_t value_reboot_len() const override
  {
    return sizeof(value_reboot_str_);
  }
  static const uint64_t VALUE_BUF_SIZE = 64UL;
  char value_str_[VALUE_BUF_SIZE];
  char value_reboot_str_[VALUE_BUF_SIZE];

private:
  struct ObInnerConfigMomentItem value_;
  DISALLOW_COPY_AND_ASSIGN(ObConfigMomentItem);
};

class ObConfigBoolItem
  : public ObConfigItem
{
public:
  ObConfigBoolItem(ObConfigContainer *container,
                   Scope::ScopeInfo scope_info,
                   const char *name,
                   const char *def,
                   const char *info,
                   const ObParameterAttr attr = ObParameterAttr());
  virtual ~ObConfigBoolItem() {}

  //need reboot value need set it once startup, otherwise it will output current value
  operator const bool &() const { return value_; }
  ObConfigBoolItem &operator = (const bool value) { set_value(value ? "True" : "False"); return *this; }
  virtual ObConfigItemType get_config_item_type() const {
    return ObConfigItemType::OB_CONF_ITEM_TYPE_BOOL;
  }
protected:
  //use current value to do input operation
  bool set(const char *str);
  bool parse(const char *str, bool &valid) const;
  const char *value_ptr() const override
  {
    return value_str_;
  }
  const char *value_reboot_ptr() const override
  {
    return value_reboot_str_;
  }
  uint64_t value_len() const override
  {
    return sizeof(value_str_);
  }
  uint64_t value_reboot_len() const override
  {
    return sizeof(value_reboot_str_);
  }

  static const uint64_t VALUE_BUF_SIZE = 8UL;
  char value_str_[VALUE_BUF_SIZE];
  char value_reboot_str_[VALUE_BUF_SIZE];
private:
  bool value_;
  DISALLOW_COPY_AND_ASSIGN(ObConfigBoolItem);
};

class ObConfigStringItem : public ObConfigItem
{
public:
  ObConfigStringItem(ObConfigContainer *container,
                     Scope::ScopeInfo scope_info,
                     const char *name,
                     const char *def,
                     const char *info,
                     const ObParameterAttr attr = ObParameterAttr());
  virtual ~ObConfigStringItem() {}

  //need reboot value need set it once startup, otherwise it will output current value
  operator const char *() const
  {
    ObLatchRGuard rd_guard(const_cast<ObLatch&>(lock_), ObLatchIds::CONFIG_LOCK);
    return value_str_;
  } // not safe, value maybe changed
  const char *get_value() const
  {
    ObLatchRGuard rd_guard(const_cast<ObLatch&>(lock_), ObLatchIds::CONFIG_LOCK);
    return value_str_;
  }
  ObString get_value_string() const
  {
    ObLatchRGuard rd_guard(const_cast<ObLatch&>(lock_), ObLatchIds::CONFIG_LOCK);
    return ObString::make_string(value_str_);
  }
  int case_compare(const char *str) const
  {
    ObLatchRGuard rd_guard(const_cast<ObLatch&>(lock_), ObLatchIds::CONFIG_LOCK);
    return ObString::make_string(value_str_).case_compare(str);
  }
  int copy(char *buf, const int64_t buf_len); // '\0' will be added
  int deep_copy_value_string(ObIAllocator &allocator, ObString &dst);
  virtual ObConfigItemType get_config_item_type() const {
    return ObConfigItemType::OB_CONF_ITEM_TYPE_STRING;
  }
  ObConfigStringItem &operator=(const char *str)
  {
    if (!set_value(str)) {
      OB_LOG_RET(WARN, common::OB_ERR_UNEXPECTED, "obconfig string item set value failed");
    }
    return *this;
  }
protected:
  //use current value to do input operation
  bool set(const char *str) { UNUSED(str); return true; }
  const char *value_ptr() const override
  {
    return value_str_;
  }
  const char *value_reboot_ptr() const override
  {
    return value_reboot_str_;
  }
  uint64_t value_len() const override
  {
    return sizeof(value_str_);
  }
  uint64_t value_reboot_len() const override
  {
    return sizeof(value_reboot_str_);
  }

  static const uint64_t VALUE_BUF_SIZE = 65536UL;
  char value_str_[VALUE_BUF_SIZE];
  char value_reboot_str_[VALUE_BUF_SIZE];

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigStringItem);
};

//config item like "MANDATORY COMPRESSION=lz4_1.0"
//or "MANDATORY COMPRESSION = lz4_1.0"

class ObConfigLogArchiveOptionsItem
  : public ObConfigItem
{
public:
  ObConfigLogArchiveOptionsItem() {}
  ObConfigLogArchiveOptionsItem(ObConfigContainer *container,
                      Scope::ScopeInfo scope_info,
                      const char *name,
                      const char *def,
                      const char *info,
                      const ObParameterAttr attr = ObParameterAttr());
  virtual ~ObConfigLogArchiveOptionsItem() {}

  ObConfigLogArchiveOptionsItem &operator=(const char *str)
  {
    if (!set_value(str)) {
      OB_LOG_RET(WARN, common::OB_ERR_UNEXPECTED, "obconfig log archive options item set value failed");
    }
    return *this;
  }

  //need reboot value need set it once startup, otherwise it will output current value
  bool valid() const { return value_.valid_; }
  bool is_mandatory() const {return value_.is_mandatory_;}
  bool need_compress() const {return value_.is_compress_enabled_;}
  common::ObCompressorType get_compressor_type() const
  { return value_.compressor_type_; }

  share::ObBackupEncryptionMode::EncryptionMode get_encryption_mode() const
  {return value_.encryption_mode_;}

  share::ObCipherOpMode get_encryption_algorithm() const
  {return value_.encryption_algorithm_;}
  virtual ObConfigItemType get_config_item_type() const {
    return ObConfigItemType::OB_CONF_ITEM_TYPE_LOGARCHIVEOPT;
  }
public:
  struct ObInnerConfigLogArchiveOptionsItem
  {
    ObInnerConfigLogArchiveOptionsItem()
      : valid_(false), is_mandatory_(false),
      is_compress_enabled_(false),
      compressor_type_(common::INVALID_COMPRESSOR),
      encryption_mode_(share::ObBackupEncryptionMode::NONE),
      encryption_algorithm_(share::ObCipherOpMode::ob_invalid_mode)
    {}
    ~ObInnerConfigLogArchiveOptionsItem() {}

    ObInnerConfigLogArchiveOptionsItem &operator = (const ObInnerConfigLogArchiveOptionsItem &value)
    {
      if (this == &value) {
        //do nothing
      } else {
        valid_ = value.valid_;
        is_mandatory_ = value.is_mandatory_;
        is_compress_enabled_ = value.is_compress_enabled_;
        compressor_type_ = value.compressor_type_;
        encryption_mode_ = value.encryption_mode_;
      }
      return *this;
    }

    ObInnerConfigLogArchiveOptionsItem(const ObInnerConfigLogArchiveOptionsItem &value)
    {
      if (this == &value) {
        //do nothing
      } else {
        valid_ = value.valid_;
        is_mandatory_ = value.is_mandatory_;
        is_compress_enabled_ = value.is_compress_enabled_;
        compressor_type_ = value.compressor_type_;
        encryption_mode_ = value.encryption_mode_;
        encryption_algorithm_ = value.encryption_algorithm_;
      }
    }
    TO_STRING_KV(K_(valid),
                 K_(is_mandatory),
                 K_(is_compress_enabled),
                 K_(compressor_type),
                 K_(encryption_mode),
                 K_(encryption_algorithm));
    int set_default_encryption_algorithm();
    bool is_encryption_meta_valid() const;
  public:
    bool valid_;
    bool is_mandatory_;
    bool is_compress_enabled_;
    common::ObCompressorType compressor_type_;
    share::ObBackupEncryptionMode::EncryptionMode encryption_mode_;
    share::ObCipherOpMode encryption_algorithm_;
  };

public:
  static int64_t get_keywords_idx(const char *str, bool &is_key);
  static int64_t get_compression_option_idx(const char *str);
  static bool is_key_keyword(int64_t idx);
  static int format_option_str(const char *src, int64_t src_len,
                               char *dest, int64_t dest_len);
  static bool is_valid_isolate_option(const int64_t idx);
public:
  struct ObInnerConfigLogArchiveOptionsItem value_;
protected:
  //use current value to do input operation
  bool set(const char *str);

  void process_isolated_option_(const int64_t idx);
  void process_kv_option_(const int64_t key_idx, const char *value);

  const char *value_ptr() const override
  {
    return value_str_;
  }
  const char *value_reboot_ptr() const override
  {
    return value_reboot_str_;
  }
  uint64_t value_len() const override
  {
    return sizeof(value_str_);
  }
  uint64_t value_reboot_len() const override
  {
    return sizeof(value_reboot_str_);
  }

  static const uint64_t VALUE_BUF_SIZE = 2048UL;
  char value_str_[VALUE_BUF_SIZE];
  char value_reboot_str_[VALUE_BUF_SIZE];

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigLogArchiveOptionsItem);
};

class ObConfigVersionItem
  : public ObConfigIntegralItem
{
public:
  ObConfigVersionItem(ObConfigContainer *container,
                       Scope::ScopeInfo scope_info,
                       const char *name,
                       const char *def,
                       const char *range,
                       const char *info,
                       const ObParameterAttr attr = ObParameterAttr());
  ObConfigVersionItem(ObConfigContainer *container,
                      Scope::ScopeInfo scope_info,
                      const char *name,
                      const char *def,
                      const char *info,
                      const ObParameterAttr attr = ObParameterAttr());
  virtual ~ObConfigVersionItem() {}

  virtual ObConfigItemType get_config_item_type() const {
    return ObConfigItemType::OB_CONF_ITEM_TYPE_VERSION;
  }
  bool set_dump_value(const char *str) override
  {
    int64_t pos = 0;
    int ret = OB_SUCCESS;
    ret = databuff_printf(value_dump_str_, sizeof(value_dump_str_), pos, "%s", str);
    return ret == OB_SUCCESS;
  }
  void set_dump_value_updated() override
  {
    dump_value_updated_ = true;
  }
  bool dump_value_updated() const override
  {
    return dump_value_updated_;
  }
  const char *spfile_str() const override
  {
    const char *ret = nullptr;
    ObLatchRGuard rd_guard(const_cast<ObLatch&>(lock_), ObLatchIds::CONFIG_LOCK);
    if (dump_value_updated()) {
      ret = value_dump_str_;
    } else if (reboot_effective() && is_initial_value_set()) {
      ret = value_reboot_str_;
    } else {
      ret = value_str_;
    }
    return ret;
  }
  ObConfigVersionItem &operator = (int64_t value);
protected:
  virtual bool set(const char *str) override;
  virtual int64_t parse(const char *str, bool &valid) const override;
  const char *value_ptr() const override
  {
    return value_str_;
  }
  const char *value_reboot_ptr() const override
  {
    return value_reboot_str_;
  }
  uint64_t value_len() const override
  {
    return sizeof(value_str_);
  }
  uint64_t value_reboot_len() const override
  {
    return sizeof(value_reboot_str_);
  }

  static const uint64_t VALUE_BUF_SIZE = 32UL; // 32 is enough for version like 4.2.0.0
  char value_str_[VALUE_BUF_SIZE];
  char value_reboot_str_[VALUE_BUF_SIZE];
  char value_dump_str_[VALUE_BUF_SIZE];
  bool dump_value_updated_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigVersionItem);
};


class ObConfigPairs
{
public:

struct ObConfigPair {
public:
  ObConfigPair()
    : key_(), value_()
  {}
  ~ObConfigPair() {}
  TO_STRING_KV(K_(key), K_(value));
public:
  ObString key_;
  ObString value_;
};

public:
  ObConfigPairs()
    : tenant_id_(common::OB_INVALID_TENANT_ID),
      allocator_(),
      config_array_()
  {}
  ~ObConfigPairs() {}
  void init(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  bool is_valid() const
  {
    return OB_INVALID_TENANT_ID != tenant_id_
           && config_array_.count() > 0;
  }
  void reset();
  int assign(const ObConfigPairs &other);

  uint64_t get_tenant_id() const { return tenant_id_; }
  const common::ObSArray<ObConfigPair> &get_configs() const { return config_array_; }
  int get_config_str(char *buf, const int64_t length) const;
  int64_t get_config_str_length() const;
  int add_config(const ObString &key, const ObString &value);

  TO_STRING_KV(K_(tenant_id), K_(config_array));
private:
  uint64_t tenant_id_;
  ObArenaAllocator allocator_;
  common::ObSArray<ObConfigPair> config_array_;
};

class ObIConfigMode;
class ObConfigModeItem: public ObConfigItem
{
public:
  ObConfigModeItem(ObConfigContainer *container,
          Scope::ScopeInfo scope_info,
          const char *name,
          const char *def,
          ObConfigParser* parser,
          const char *info,
          const ObParameterAttr attr = ObParameterAttr());
  virtual ~ObConfigModeItem();
  // get_value() return the real-time value
  const uint8_t* get_value() const { return value_; }
  // get() return the real-time value if it does not need reboot, otherwise it return initial_value
  const uint8_t* get() const { return value_; }
  operator const uint8_t* () const { return value_; }

  virtual ObConfigItemType get_config_item_type() const {
    return ObConfigItemType::OB_CONF_ITEM_TYPE_MODE;
  }
  int init_mode(ObIConfigMode &mode);
  static const int64_t MAX_MODE_BYTES = 32;
protected:
  //use current value to do input operation
  bool set(const char *str);
  const char *value_ptr() const override
  {
    return value_str_;
  }
  const char *value_reboot_ptr() const override
  {
    return value_reboot_str_;
  }
  uint64_t value_len() const override
  {
    return sizeof(value_str_);
  }
  uint64_t value_reboot_len() const override
  {
    return sizeof(value_reboot_str_);
  }

protected:
  static const uint64_t VALUE_BUF_SIZE = 65536UL;
  ObConfigParser *parser_;
  char value_str_[VALUE_BUF_SIZE];
  char value_reboot_str_[VALUE_BUF_SIZE];
  // max bits size: 8 * 32 = 256
  uint8_t value_[MAX_MODE_BYTES];
private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigModeItem);
};

class ObIConfigMode
{
public:
  ObIConfigMode() {}
  ~ObIConfigMode() {}
  virtual int set_value(const ObConfigModeItem &mode_item) = 0;
private:
  DISALLOW_COPY_AND_ASSIGN(ObIConfigMode);
};

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_SHARE_CONFIG_OB_CONFIG_H_
