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

#ifndef OCEANBASE_LIB_STRING_OB_SENSITIVE_STRING
#define OCEANBASE_LIB_STRING_OB_SENSITIVE_STRING

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/allocator/page_arena.h"
#include "lib/oblog/ob_log.h"

namespace oceanbase
{
namespace common
{
// ObSensitiveString Wraps a string containing sensitive information
// ensuring the access_key is obscured during printing.
class ObSensitiveString
{
public:
  ObSensitiveString(const char *str) : str_(str) {}
  ObSensitiveString(const ObString &str) : str_(str) {}
  int64_t to_string(char *buf, const int64_t len) const 
  {
    ObArenaAllocator allocator(ObModIds::OB_LOG);
    int ret = OB_SUCCESS;
    int64_t pos = 0;
    char *ptr = nullptr;
    if (OB_LIKELY(nullptr != buf) && OB_LIKELY(len > 0)) {
      // Print "NULL" when str_ is nullptr, and "" when it's empty. 
      // Note that ob_dup_cstring will return nullptr even if an empty string is passed,
      // making it impossible to distinguish between an empty string and a null pointer. 
      if (str_.ptr() == nullptr) {
        (void)logdata_printf(buf, len, pos, "NULL");
      } else if (str_.length() == 0) {
        (void)logdata_printf(buf, len, pos, ""); 
      } else if (OB_FAIL(ob_dup_cstring(allocator, str_, ptr))) {
        LIB_LOG(ERROR, "ob_dup_cstring failed", K(ret));
      } else {
        if (OB_ISNULL(ptr)) {
          (void)logdata_printf(buf, len, pos, "NULL");
        } else {
          const char *start_pos = strcasestr(ptr, "access_key");
          if (OB_ISNULL(start_pos)) {
            (void)logdata_printf(buf, len, pos, "%s", ptr);
          } else {
            const int32_t prefix_length = start_pos - ptr;
            (void)logdata_printf(buf, len, pos, "%.*s", MIN(prefix_length, static_cast<int32_t>(len)), ptr);
            (void)logdata_printf(buf, len, pos, "access_key={hidden_access_key}");
            const char *end_pos = STRCHR(start_pos, '&');
            if (end_pos != nullptr) {
              (void)logdata_printf(buf, len, pos, "%s", end_pos);
            }
          }
        }
      }
    }
    return pos;
  }
private:
  const ObString str_;
};
} // end namespace common
} // end namespace oceanbase

#endif