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

#include "ob_table_util.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace table
{
  const ObString ObTableUtils::KV_NORMAL_TRACE_INFO = ObString::make_string("OBKV Operation");
  const ObString ObTableUtils::KV_TTL_TRACE_INFO = ObString::make_string("TTL Delete");

  bool ObTableUtils::is_kv_trace_info(const ObString &trace_info)
  {
    return (trace_info.compare(KV_NORMAL_TRACE_INFO) == 0 || trace_info.compare(KV_TTL_TRACE_INFO) == 0);
  }
}
}
