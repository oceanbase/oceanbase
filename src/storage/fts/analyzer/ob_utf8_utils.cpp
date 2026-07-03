/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE_FTS

#include "ob_utf8_utils.h"
#include "lib/oblog/ob_log.h"

namespace oceanbase
{
namespace storage
{

int ObUTF8Utils::validate(const char *str, const int32_t len)
{
  int ret = OB_SUCCESS;
  int32_t offset = 0;
  UChar32 codepoint = U_SENTINEL;
  if (OB_ISNULL(str) || OB_UNLIKELY(len < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(str), K(len));
  }
  while (OB_SUCC(ret) && offset < len) {
    if (OB_FAIL(next_codepoint(str, len, offset, codepoint))) {
      LOG_WARN("failed to get next codepoint", K(ret), KP(str), K(len), K(offset));
    }
  }
  return ret;
}

int ObUTF8Utils::count(const char *str, const int32_t len, int32_t &codepoint_cnt)
{
  int ret = OB_SUCCESS;
  codepoint_cnt = 0;
  int32_t offset = 0;
  UChar32 codepoint = U_SENTINEL;
  if (OB_ISNULL(str) || OB_UNLIKELY(len < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(str), K(len));
  }
  while (OB_SUCC(ret) && offset < len) {
    if (OB_FAIL(next_codepoint(str, len, offset, codepoint))) {
      LOG_WARN("failed to get next codepoint", K(ret), KP(str), K(len), K(offset));
    } else {
      ++codepoint_cnt;
    }
  }
  return ret;
}

int ObUTF8Utils::truncate(const char *str,
                          const int32_t len,
                          const int32_t max_codepoint_cnt,
                          int32_t &truncated_len,
                          int32_t &codepoint_cnt)
{
  int ret = OB_SUCCESS;
  truncated_len = 0;
  codepoint_cnt = 0;
  int32_t offset = 0;
  UChar32 codepoint = U_SENTINEL;
  if (OB_ISNULL(str) || OB_UNLIKELY(len < 0 || max_codepoint_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(str), K(len), K(max_codepoint_cnt));
  }
  while (OB_SUCC(ret) && offset < len && codepoint_cnt < max_codepoint_cnt) {
    if (OB_FAIL(next_codepoint(str, len, offset, codepoint))) {
      LOG_WARN("failed to get next codepoint", K(ret), KP(str), K(len), K(offset));
    } else {
      truncated_len = offset;
      ++codepoint_cnt;
    }
  }
  return ret;
}

int ObUTF8Utils::is_emoji(const char *str, const int32_t len, bool &is_emoji)
{
  int ret = OB_SUCCESS;
  is_emoji = false;
  int32_t offset = 0;
  UChar32 codepoint = U_SENTINEL;
  if (OB_ISNULL(str) || OB_UNLIKELY(len < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(str), K(len));
  } else if (0 == len) {
    // skip the following
  } else if (OB_FAIL(next_codepoint(str, len, offset, codepoint))) {
    LOG_WARN("failed to get next codepoint", K(ret), KP(str), K(len), K(offset));
  } else if (u_hasBinaryProperty(codepoint, UCHAR_EMOJI)
      || u_hasBinaryProperty(codepoint, UCHAR_EXTENDED_PICTOGRAPHIC)) {
    if (is_emoji_rk(codepoint)) {
      UChar32 trailer = U_SENTINEL;
      if (offset < len) {
        if (OB_FAIL(next_codepoint(str, len, offset, trailer))) {
          LOG_WARN("failed to get next codepoint", K(ret), KP(str), K(len), K(offset));
        } else {
          is_emoji = (trailer == 0xFE0F || trailer == 0x20E3);
        }
      }
    } else {
      is_emoji = true;
    }
  }
  return ret;
}

int ObUTF8Utils::next_codepoint(const char *str,
                                const int32_t len,
                                int32_t &offset,
                                UChar32 &codepoint)
{
  int ret = OB_SUCCESS;
  const int32_t old_offset = offset;
  codepoint = U_SENTINEL;
  if (OB_ISNULL(str) || OB_UNLIKELY(old_offset < 0 || old_offset >= len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(str), K(len), K(offset));
  } else {
    U8_NEXT(str, offset, len, codepoint);
    if (OB_UNLIKELY(codepoint < 0 || offset <= old_offset || offset > len)) {
      ret = OB_ERR_INCORRECT_STRING_VALUE;
      LOG_WARN("invalid next codepoint", K(ret), KP(str), K(len), K(offset), K(codepoint));
    }
  }
  return ret;
}

bool ObUTF8Utils::is_emoji_rk(const UChar32 codepoint)
{
  return codepoint == 0x002A
      || codepoint == 0x0023
      || (codepoint >= 0x0030 && codepoint <= 0x0039)
      || codepoint == 0x00A9
      || codepoint == 0x00AE
      || codepoint == 0x2122
      || codepoint == 0x3030
      || codepoint == 0x303D;
}

} // namespace storage
} // namespace oceanbase
