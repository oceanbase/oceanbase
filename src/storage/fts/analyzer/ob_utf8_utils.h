/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_UTF8_UTILS_H_
#define OB_UTF8_UTILS_H_

#include <unicode/uchar.h>
#include <unicode/utf8.h>

#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase
{
namespace storage
{

// Utility class for UTF-8 string processing
class ObUTF8Utils final
{
public:
  // Checks whether a slice is a valid UTF-8 sequence.
  static int validate(const char *str, const int32_t len);

  // Counts Unicode codepoints in a slice.
  static int count(const char *str, const int32_t len, int32_t &codepoint_cnt);

  // Truncates a slice at a codepoint boundary by codepoint count.
  // If the slice does not have enough codepoints, the full length is returned.
  static int truncate(const char *str,
                      const int32_t len,
                      const int32_t max_codepoint_cnt,
                      int32_t &truncated_len,
                      int32_t &codepoint_cnt);

  // Returns whether a token represents an emoji character or sequence.
  static int is_emoji(const char *str, const int32_t len, bool &is_emoji);

  // Decodes the next Unicode codepoint and advances the byte offset.
  static int next_codepoint(const char *str,
                            const int32_t len,
                            int32_t &offset,
                            UChar32 &codepoint);

private:
  // These codepoints are treated as emoji only when followed by an emoji presentation selector
  // or keycap.
  static bool is_emoji_rk(const UChar32 codepoint);
};

} // namespace storage
} // namespace oceanbase

#endif // OB_UTF8_UTILS_H_
