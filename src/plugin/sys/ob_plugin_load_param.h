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

#pragma once

#include "lib/string/ob_string.h"
#include "lib/container/ob_array.h"

namespace oceanbase {
namespace plugin {

class ObPluginLoadOption final
{
public:
  enum ObOptionEnum
  {
    INVALID,
    ON,        // only print warning log
    OFF,       // disabled
    MAX,
  };

public:
  ObPluginLoadOption() = default;
  explicit ObPluginLoadOption(ObOptionEnum value) : value_(value)
  {}

  ObOptionEnum value() const { return value_; }

  const char * value_string() const;
  DECLARE_TO_STRING;

  static ObPluginLoadOption from_string(const common::ObString &str);

private:
  ObOptionEnum value_ = ObPluginLoadOption::INVALID;
};

struct ObPluginLoadParam
{
  common::ObString   library_name;
  ObPluginLoadOption load_option;

  TO_STRING_KV(K(library_name), K(load_option));
};

class ObPluginLoadParamParser
{
public:
  static int parse(const common::ObString &param, common::ObIArray<ObPluginLoadParam> &plugins_load);

private:
  static int parse_item(const common::ObString &item, ObPluginLoadParam &plugin_load);
};

} // namespace plugin
} // namespace oceanbase
