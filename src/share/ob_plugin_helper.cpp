/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE

#include "share/ob_force_print_log.h"
#include "share/ob_plugin_helper.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace share
{

const char *OB_PLUGIN_PREFIX = "ob_builtin_";
const char *OB_PLUGIN_SUFFIX = "_plugin";
const char *OB_PLUGIN_VERSION_SUFFIX = "_plugin_version";
const char *OB_PLUGIN_SIZE_SUFFIX = "_sizeof_plugin";

#define OB_PLUGIN_GETTER(buf, buf_len, name, name_len, suffix, suffix_len)              \
do {                                                                                    \
  const int64_t prefix_len = STRLEN(OB_PLUGIN_PREFIX);                                  \
  if (OB_UNLIKELY(buf_len <= prefix_len + name_len + suffix_len)) {                     \
    ret = OB_INVALID_ARGUMENT;                                                          \
    LOG_WARN("This buffer is too small to accommodate all of name", K(ret), K(buf_len), \
        K(prefix_len), K(name_len), K(suffix_len));                                     \
  } else {                                                                              \
    MEMCPY(buf + prefix_len + name_len, suffix, suffix_len);                            \
    MEMCPY(buf + prefix_len, name, name_len);                                           \
    MEMCPY(buf, OB_PLUGIN_PREFIX, prefix_len);                                          \
    buf[prefix_len + name_len + suffix_len] = '\0';                                     \
  }                                                                                     \
} while (false)

int get_plugin_version_str(
    char *buf,
    const int64_t buf_len,
    const char *name,
    const int64_t name_len)
{
  int ret = common::OB_SUCCESS;
  OB_PLUGIN_GETTER(buf,
                   buf_len,
                   name,
                   name_len,
                   OB_PLUGIN_VERSION_SUFFIX,
                   STRLEN(OB_PLUGIN_VERSION_SUFFIX));
  return ret;
}

int get_plugin_size_str(
    char *buf,
    const int64_t buf_len,
    const char *name,
    const int64_t name_len)
{
  int ret = common::OB_SUCCESS;
  OB_PLUGIN_GETTER(buf,
                   buf_len,
                   name,
                   name_len,
                   OB_PLUGIN_SIZE_SUFFIX,
                   STRLEN(OB_PLUGIN_SIZE_SUFFIX));
  return ret;
}

int get_plugin_str(
    char *buf,
    const int64_t buf_len,
    const char *name,
    const int64_t name_len)
{
  int ret = common::OB_SUCCESS;
  OB_PLUGIN_GETTER(buf,
                   buf_len,
                   name,
                   name_len,
                   OB_PLUGIN_SUFFIX,
                   STRLEN(OB_PLUGIN_SUFFIX));
  return ret;
}

int ObPluginName::set_name(const char *name)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("The name is nullptr", K(ret), KP(name));
  } else if (OB_UNLIKELY(STRLEN(name) >= OB_PLUGIN_NAME_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("The name is too long", K(ret), KCSTRING(name));
  } else {
    int i = 0;
    while ('\0' != name[i]) {
      name_[i] = tolower(name[i]);
      ++i;
    }
    name_[i] = '\0';
  }
  return ret;
}

// Some input has no '\0',
int ObPluginName::set_name(const ObString &name)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(name.empty() || (name.length() >= OB_PLUGIN_NAME_LENGTH))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Parser name invalid", K(ret), K(name));
  } else {
    ObString::obstr_size_t i = 0;
    for (; (i < name.length()) && ('\0' != name[i]); ++i) {
      name_[i] = tolower(name[i]);
    }
    name_[i] = '\0';
  }
  return ret;
}

} // end namespace share
} // end namespace oceanbase
