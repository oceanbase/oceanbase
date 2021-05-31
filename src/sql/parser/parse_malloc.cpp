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

#define USING_LOG_PREFIX SQL_PARSER
#include "sql/parser/parse_malloc.h"
#include <string.h>
#include <lib/alloc/alloc_assist.h>
#include "lib/charset/ob_ctype.h"
#include "sql/parser/parse_define.h"
#include "sql/parser/parser_proxy_func.h"

// get memory from the thread obStringBuf, and not release untill thread quits
void* parse_malloc(const size_t nbyte, void* malloc_pool)
{
  void* ptr = NULL;
  size_t headlen = sizeof(int64_t);
  if (OB_ISNULL(malloc_pool)) {
  } else if (OB_UNLIKELY(nbyte <= 0)) {
  } else {
    if (OB_UNLIKELY(NULL == (ptr = parser_alloc_buffer(malloc_pool, headlen + nbyte)))) {
    } else {
      *(static_cast<int64_t*>(ptr)) = nbyte;
      ptr = static_cast<char*>(ptr) + headlen;
      MEMSET(ptr, 0, nbyte);
    }
  }
  return ptr;
}

/* ptr must point to a memory allocated by parse_malloc.cpp */
void* parse_realloc(void* ptr, size_t nbyte, void* malloc_pool)
{
  void* new_ptr = NULL;
  // need not to check nbyte
  if (OB_ISNULL(malloc_pool)) {
  } else {
    if (OB_UNLIKELY(NULL == ptr)) {
      new_ptr = parser_alloc_buffer(malloc_pool, nbyte);
    } else {
      size_t headlen = sizeof(int64_t);
      if (OB_UNLIKELY(NULL == (new_ptr = parser_alloc_buffer(malloc_pool, headlen + nbyte)))) {
      } else {
        int64_t obyte = *(reinterpret_cast<int64_t*>(static_cast<char*>(ptr) - headlen));
        *(static_cast<int64_t*>(new_ptr)) = nbyte;
        new_ptr = static_cast<char*>(new_ptr) + headlen;
        MEMMOVE(new_ptr, ptr, static_cast<int64_t>(nbyte) > obyte ? obyte : nbyte);
        parser_free_buffer(malloc_pool, static_cast<char*>(ptr) - headlen);
      }
    }
  }
  return new_ptr;
}

char* parse_strndup(const char* str, size_t nbyte, void* malloc_pool)
{
  char* new_str = NULL;
  // need not to check nbyte
  if (OB_ISNULL(str)) {
  } else {
    if (OB_LIKELY(NULL != (new_str = static_cast<char*>(parse_malloc(nbyte + 1, malloc_pool))))) {
      MEMMOVE(new_str, str, nbyte);
      new_str[nbyte] = '\0';
    } else {
    }
  }
  return new_str;
}

char* parse_strdup(const char* str, void* malloc_pool, int64_t* out_len)
{
  char* out_str = NULL;
  if (OB_ISNULL(str)) {
  } else if (OB_ISNULL(out_len)) {
  } else {
    size_t dup_len = STRLEN(str);
    out_str = parse_strndup(str, dup_len, malloc_pool);
    *out_len = dup_len;
  }
  return out_str;
}

char* parse_str_convert_utf8(
    const struct ObCharsetInfo* src_cs, const char* str, void* malloc_pool, int64_t* out_len, int* extra_errno)
{
  char* out_str = NULL;
  if (OB_ISNULL(str) || OB_ISNULL(out_len) || OB_ISNULL(extra_errno)) {
  } else {
    uint errors;
    size_t str_len = STRLEN(str);
    size_t dst_len = str_len * 4;
    if (OB_ISNULL(out_str = static_cast<char*>(parse_malloc(dst_len + 1, malloc_pool)))) {
    } else {
      *out_len = ob_convert(out_str, dst_len, &ob_charset_utf8mb4_general_ci, str, str_len, src_cs, &errors);
      out_str[*out_len] = '\0';
      if (0 != errors) {
        *extra_errno = OB_PARSER_ERR_ILLEGAL_NAME;
      }
    }
  }
  return out_str;
}

char* strndup_with_prefix(const char* prefix, const char* str, size_t nbyte, void* malloc_pool)
{
  char* new_str = NULL;
  size_t prefix_len = strlen(prefix);
  // need not to check nbyte
  if (OB_ISNULL(str)) {
  } else {
    if (OB_LIKELY(NULL != (new_str = static_cast<char*>(parse_malloc(nbyte + prefix_len, malloc_pool))))) {
      MEMMOVE(new_str, prefix, prefix_len);
      MEMMOVE(new_str + prefix_len, str, nbyte);
    } else {
      // do nothing
    }
  }
  return new_str;
}

char* strndup_with_prefix_and_postfix(
    const char* prefix, const char* postfix, const char* str, size_t nbyte, void* malloc_pool)
{
  char* new_str = NULL;
  size_t prefix_len = strlen(prefix);
  size_t postfix_len = strlen(postfix);
  if (OB_ISNULL(str)) {
  } else {
    if (OB_LIKELY(
            NULL != (new_str = static_cast<char*>(parse_malloc(nbyte + prefix_len + postfix_len, malloc_pool))))) {
      MEMMOVE(new_str, prefix, prefix_len);
      MEMMOVE(new_str + prefix_len, str, nbyte);
      MEMMOVE(new_str + prefix_len + nbyte, postfix, postfix_len);
    } else {
    }
  }
  return new_str;
}

void parse_free(void* ptr)
{
  UNUSED(ptr);
  /* do nothing, we don't really free the memory */
}

char* cp_str_value(const char* src, const size_t nbyte, void* malloc_pool)
{
  char* new_str = NULL;
  if (OB_ISNULL(src)) {
  } else {
    if (OB_LIKELY(NULL != (new_str = static_cast<char*>(parse_malloc(nbyte, malloc_pool))))) {
      MEMCPY(new_str, src, nbyte);
    } else {
    }
  }
  return new_str;
}
