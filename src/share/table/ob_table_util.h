/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SHARE_TABLE_OB_TABLE_UTIL_
#define OCEANBASE_SHARE_TABLE_OB_TABLE_UTIL_

#include "lib/string/ob_string.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace table
{

class ObTableUtils
{
public:
  static const ObString &get_kv_normal_trace_info() { return KV_NORMAL_TRACE_INFO; }
  static const ObString &get_kv_ttl_trace_info() { return KV_TTL_TRACE_INFO; }
  static bool is_kv_trace_info(const ObString &trace_info);
private:
  static const ObString KV_NORMAL_TRACE_INFO;
  static const ObString KV_TTL_TRACE_INFO;
};

}  // namespace table
}  // namespace oceanbase

#endif /* OCEANBASE_SHARE_TABLE_OB_TABLE_UTIL_ */