/**
 * Copyright (c) 2025 OceanBase
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

#include "share/ai_service/ob_ai_func_provider.h"
#include "lib/oblog/ob_log_module.h"

namespace oceanbase
{
namespace common
{

int ObAiBatchFileLine::to_json(common::ObIAllocator &allocator, common::ObString &json_str) const
{
  int ret = OB_SUCCESS;
  json_str.reset();

  if (OB_UNLIKELY(body_.empty() || OB_ISNULL(body_.ptr()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BATCH-FILE] body is empty or null, cannot produce valid JSONL",
             K(ret), K(body_.length()), KP(body_.ptr()),
             K(custom_id_), K(method_), K(url_));
  } else {
    const int64_t buf_size = 256 + custom_id_.length() + method_.length()
                             + url_.length() + body_.length();
    char *buf = static_cast<char*>(allocator.alloc(buf_size));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for json", K(ret));
    } else {
      int64_t pos = 0;
      pos = snprintf(buf, buf_size,
                     "{\"custom_id\":\"%.*s\",\"method\":\"%.*s\",\"url\":\"%.*s\",\"body\":%.*s}",
                     static_cast<int>(custom_id_.length()), custom_id_.ptr(),
                     static_cast<int>(method_.length()), method_.ptr(),
                     static_cast<int>(url_.length()), url_.ptr(),
                     static_cast<int>(body_.length()), body_.ptr());
      if (pos < 0 || pos >= buf_size) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("[BATCH-FILE] JSONL line truncated by snprintf", K(ret), K(pos), K(buf_size),
                 K(custom_id_.length()), K(body_.length()));
      } else {
        json_str.assign_ptr(buf, pos);
        int64_t dump_len = MIN(pos, 200);
        LOG_DEBUG("[BATCH-FILE] JSONL line generated",
                  K(pos), K(custom_id_),
                  "head", common::ObString(static_cast<int32_t>(dump_len), buf));
      }
    }
  }
  return ret;
}

} // namespace common
} // namespace oceanbase
