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

#ifndef OCEANBASE_SHARE_CONFIG_OB_SYSTEM_CONFIG_VALUE_H_
#define OCEANBASE_SHARE_CONFIG_OB_SYSTEM_CONFIG_VALUE_H_

#include "share/ob_define.h"

// 去掉代码改动较大, 先保留

namespace oceanbase
{
namespace common
{
class ObSystemConfigValue
{
public:
  ObSystemConfigValue();
  virtual ~ObSystemConfigValue() {}

  void set_value(const char *value);
  void set_value(const ObString &value);
  const char *value() const { return value_; }
  void set_info(const char *info);
  void set_info(const ObString &info);
  const char *info() const { return info_; }
  void set_section(const char *section);
  void set_section(const ObString &section);
  const char *section() const { return section_; }
  void set_scope(const char *scope);
  void set_scope(const ObString &scope);
  const char *scope() const { return scope_; }
  void set_source(const char *source);
  void set_source(const ObString &source);
  const char *source() const { return source_; }
  void set_edit_level(const char *edit_level);
  void set_edit_level(const ObString &edit_level);
  const char *edit_level() const { return edit_level_; }
private:
  char value_[OB_MAX_CONFIG_VALUE_LEN];
  char info_[OB_MAX_CONFIG_INFO_LEN];
  char section_[OB_MAX_CONFIG_SECTION_LEN];
  char scope_[OB_MAX_CONFIG_SCOPE_LEN];
  char source_[OB_MAX_CONFIG_SOURCE_LEN];
  char edit_level_[OB_MAX_CONFIG_EDIT_LEVEL_LEN];
  // ObSystemConfig中ObHashMap使用了对象的拷贝构造函数,不能禁止
  //DISALLOW_COPY_AND_ASSIGN(ObSystemConfigValue);
};

inline ObSystemConfigValue::ObSystemConfigValue()
{
  MEMSET(value_, 0, OB_MAX_CONFIG_VALUE_LEN);
  MEMSET(info_, 0, OB_MAX_CONFIG_INFO_LEN);
  MEMSET(section_, 0, OB_MAX_CONFIG_SECTION_LEN);
  MEMSET(scope_, 0, OB_MAX_CONFIG_SCOPE_LEN);
  MEMSET(source_, 0, OB_MAX_CONFIG_SOURCE_LEN);
  MEMSET(edit_level_, 0, OB_MAX_CONFIG_EDIT_LEVEL_LEN);
}

inline void ObSystemConfigValue::set_value(const ObString &value)
{
  int64_t value_length = value.length();
  if (value_length >= OB_MAX_CONFIG_VALUE_LEN) {
    value_length = OB_MAX_CONFIG_VALUE_LEN;
  }
  snprintf(value_, OB_MAX_CONFIG_VALUE_LEN, "%.*s",
           static_cast<int>(value_length), value.ptr());
}

inline void ObSystemConfigValue::set_value(const char *value)
{
  set_value(ObString::make_string(value));
}

inline void ObSystemConfigValue::set_info(const ObString &info)
{
  int64_t info_length = info.length();
  if (info_length >= OB_MAX_CONFIG_INFO_LEN) {
    info_length = OB_MAX_CONFIG_INFO_LEN;
  }
  IGNORE_RETURN snprintf(info_, OB_MAX_CONFIG_INFO_LEN, "%.*s",
                         static_cast<int>(info_length), info.ptr());
}

inline void ObSystemConfigValue::set_info(const char *info)
{
  set_info(ObString::make_string(info));
}

inline void ObSystemConfigValue::set_section(const ObString &section)
{
  int64_t section_length = section.length();
  if (section_length >= OB_MAX_CONFIG_SECTION_LEN) {
    section_length = OB_MAX_CONFIG_SECTION_LEN - 1;
  }
  int64_t pos = 0;
  (void) databuff_printf(section_, OB_MAX_CONFIG_SECTION_LEN, pos, "%.*s",
                         static_cast<int>(section_length), section.ptr());
}

inline void ObSystemConfigValue::set_section(const char *section)
{
  set_section(ObString::make_string(section));
}

inline void ObSystemConfigValue::set_scope(const ObString &scope)
{
  int64_t scope_length = scope.length();
  if (scope_length >= OB_MAX_CONFIG_SCOPE_LEN) {
    scope_length = OB_MAX_CONFIG_SCOPE_LEN - 1;
  }
  int64_t pos = 0;
  (void) databuff_printf(scope_, OB_MAX_CONFIG_SCOPE_LEN, pos, "%.*s",
                         static_cast<int>(scope_length), scope.ptr());
}

inline void ObSystemConfigValue::set_scope(const char *scope)
{
  set_scope(ObString::make_string(scope));
}

inline void ObSystemConfigValue::set_source(const ObString &source)
{
  int64_t source_length = source.length();
  if (source_length >= OB_MAX_CONFIG_SOURCE_LEN) {
    source_length = OB_MAX_CONFIG_SOURCE_LEN - 1;
  }
  int64_t pos = 0;
  (void) databuff_printf(source_, OB_MAX_CONFIG_SOURCE_LEN, pos, "%.*s",
                         static_cast<int>(source_length), source.ptr());
}

inline void ObSystemConfigValue::set_source(const char *source)
{
  set_source(ObString::make_string(source));
}

inline void ObSystemConfigValue::set_edit_level(const ObString &edit_level)
{
  int64_t edit_level_length = edit_level.length();
  if (edit_level_length >= OB_MAX_CONFIG_EDIT_LEVEL_LEN) {
    edit_level_length = OB_MAX_CONFIG_EDIT_LEVEL_LEN - 1;
  }
  int64_t pos = 0;
  (void) databuff_printf(edit_level_, OB_MAX_CONFIG_EDIT_LEVEL_LEN, pos, "%.*s",
                         static_cast<int>(edit_level_length), edit_level.ptr());
}

inline void ObSystemConfigValue::set_edit_level(const char *edit_level)
{
  set_edit_level(ObString::make_string(edit_level));
}

} // end of namespace common
} // end of namespace oceanbase

#endif // OCEANBASE_SHARE_CONFIG_OB_SYSTEM_CONFIG_VALUE_H_
