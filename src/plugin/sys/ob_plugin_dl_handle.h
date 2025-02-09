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
#include "lib/string/ob_sql_string.h"

namespace oceanbase {
namespace plugin {

/**
 * Indicate a dynamic library, or shared object
 */
class ObPluginDlHandle final
{
public:
  ObPluginDlHandle() = default;
  ~ObPluginDlHandle();

  int init(const common::ObString &dl_path, const common::ObString &dl_name);
  void destroy();

  common::ObString name() const { return dl_name_.string(); }

  template <typename Type>
  int read_value(const char *symbol_name, Type &value)
  {
    return read_value(symbol_name, &value, static_cast<int64_t>(sizeof(value)));
  }

  int read_value(const char *symbol_name, void *ptr, int64_t size);
  int read_symbol(const char *symbol_name, void *&value);

  TO_STRING_KV(K_(dl_name), K_(dl_handle));

private:
  common::ObSqlString  dl_name_;
  void *       dl_handle_ = nullptr; /// the handle dlopen returned
};

} // namespace plugin
} // namespace oceanbase
