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

#define USING_LOG_PREFIX SHARE

#include "plugin/sys/ob_plugin_load_param.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace plugin {

const char ITEM_TERMINATE_CHAR  = ',';
const char FIELD_TERMINATE_CHAR = ':';

const char *ObPluginLoadOption::value_string() const
{
  switch (value_) {
    case ObPluginLoadOption::INVALID: return "INVALID";
    case ObPluginLoadOption::ON:      return "ON";
    case ObPluginLoadOption::OFF:     return "OFF";
    case ObPluginLoadOption::MAX:     return "MAX";
    default:                          return "UNKNOWN_OPTION";
  }
}

DEF_TO_STRING(ObPluginLoadOption)
{
  return snprintf(buf, buf_len, "%d:%s", static_cast<int>(value_), value_string());
}

ObPluginLoadOption ObPluginLoadOption::from_string(const ObString &str)
{
  ObPluginLoadOption ret(ObPluginLoadOption::INVALID);
  if (0 == str.case_compare("on")) {
    ret.value_ = ObPluginLoadOption::ON;
  } else if (0 == str.case_compare("off")) {
    ret.value_ = ObPluginLoadOption::OFF;
  }
  return ret;
}

int ObPluginLoadParamParser::parse(const ObString &param, ObIArray<ObPluginLoadParam> &plugin_params)
{
  int ret = OB_SUCCESS;
  ObString param_str(param);
  param_str = param_str.trim();
  if (param_str.empty()) {
    // nothing to do
  } else {
    ObPluginLoadParam plugin_param;
    ObString last_str = param_str;
    while (OB_SUCC(ret) && !last_str.empty() && OB_NOT_NULL(last_str.find(ITEM_TERMINATE_CHAR))) {
      ObString item_str = last_str.split_on(ITEM_TERMINATE_CHAR);
      last_str = last_str.trim();
      item_str = item_str.trim();
      if (item_str.empty()) {
        // ignore
      } else if (OB_FAIL(parse_item(item_str, plugin_param))) {
        LOG_WARN("failed to parse plugin load param", K(ret));
      } else if (OB_FAIL(plugin_params.push_back(plugin_param))) {
        LOG_WARN("failed to push back plugin param", K(plugin_param), K(ret));
      }
    }

    ObString item_str = last_str;
    if (OB_FAIL(ret)) {
    } else if (item_str.empty()) {
    } else if (OB_FAIL(parse_item(item_str, plugin_param))) {
      LOG_WARN("failed to parse plugin load param", K(ret));
    } else if (OB_FAIL(plugin_params.push_back(plugin_param))) {
      LOG_WARN("failed to push back plugin param", K(plugin_param), K(ret));
    }
  }
  return ret;
}

int ObPluginLoadParamParser::parse_item(const ObString &item_str, ObPluginLoadParam &plugin_param)
{
  int ret = OB_SUCCESS;
  if (item_str.empty()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObString src_str = item_str;
    ObString library_name;
    ObPluginLoadOption load_option(ObPluginLoadOption::ON);
    const char *sep = item_str.find(FIELD_TERMINATE_CHAR);
    LOG_DEBUG("item str find field terminated char", KP(sep), K(item_str));
    if (OB_ISNULL(sep)) {
      library_name = item_str;
    } else {
      library_name = src_str.split_on(sep);
      ObString option_str = src_str.trim();
      load_option = ObPluginLoadOption::from_string(option_str);
      if (library_name.empty()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("library name is empty", K(ret), K(item_str));
      } else if (ObPluginLoadOption::INVALID == load_option.value()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid plugin load option", K(load_option), K(option_str));
      }
    }

    if (OB_SUCC(ret)) {
      plugin_param.library_name = library_name;
      plugin_param.load_option  = load_option;
    }
  }
  return ret;
}

} // namespace plugin
} // namespace oceanbase
