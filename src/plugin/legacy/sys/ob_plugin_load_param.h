/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
