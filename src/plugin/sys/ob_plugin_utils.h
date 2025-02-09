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

#include "lib/alloc/alloc_struct.h"
#include "lib/string/ob_string.h"
#include "oceanbase/ob_plugin.h"

namespace oceanbase {
namespace plugin {

extern lib::ObLabel OB_PLUGIN_MEMORY_LABEL;

constexpr int OB_PLUGIN_NAME_MAX_LENGTH = 64;

/**
 * get type name of `ObPluginType`
 */
const char *ob_plugin_type_to_string(ObPluginType type);

/**
 * A hash function for case-insensitive string
 * @details plugin name is not case-sensitive
 */
struct ObPluginNameHash
{
  int operator() (const common::ObString &name, uint64_t &res) const;
};

/**
 * An equal-to function for case-insensitive string
 * @details plugin name is not case-sensitive
 */
struct ObPluginNameEqual
{
  bool operator()(const common::ObString &name1, const ObString &name2) const;
};

} // namespace plugin
} // namespace oceanbase
