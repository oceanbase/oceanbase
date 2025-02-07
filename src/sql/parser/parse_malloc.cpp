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


void *malloc_parentheses_info(const size_t nbyte, void *malloc_pool)
{
  void *ptr = NULL;
  if (OB_ISNULL(malloc_pool)) {
  } else if (OB_UNLIKELY(nbyte <= 0)) {
  } else {
    if (OB_UNLIKELY(NULL == (ptr = parser_alloc_buffer(malloc_pool, nbyte)))) {
    } else {
      MEMSET(ptr, 0, nbyte);
    }
  }
  return ptr;
}

// get memory from the thread obStringBuf, and not release until thread quits
void *parse_malloc(const size_t nbyte, void *malloc_pool)
{
  void *ptr = NULL;
  size_t headlen = sizeof(int64_t);
  if (OB_ISNULL(malloc_pool)) {
  } else if (OB_UNLIKELY(nbyte <= 0)) {
  } else {
    if (OB_UNLIKELY(NULL == (ptr = parser_alloc_buffer(malloc_pool, headlen + nbyte)))) {
    } else {
      *(static_cast<int64_t *>(ptr)) = nbyte;
      ptr = static_cast<char *>(ptr) + headlen;
      MEMSET(ptr, 0, nbyte);
    }
  }
  return ptr;
}

void *parser_alloc(void *malloc_pool, const int64_t alloc_size)
{
  void *ptr = NULL;
  if (OB_ISNULL(malloc_pool)) {
  } else {
    if (OB_UNLIKELY(NULL == (ptr = parser_alloc_buffer(malloc_pool, alloc_size)))) {
    } else {
      MEMSET(ptr, 0, alloc_size);
    }
  }
  return ptr;
}

/* ptr must point to a memory allocated by parse_malloc.cpp */
void *parse_realloc(void *ptr, size_t nbyte, void *malloc_pool)
{
  void *new_ptr = NULL;
  //need not to check nbyte
  if (OB_ISNULL(malloc_pool)) {
  } else {
    if (OB_UNLIKELY(NULL == ptr)) {
      new_ptr = parser_alloc_buffer(malloc_pool, nbyte);
    } else {
      size_t headlen = sizeof(int64_t);
      if (OB_UNLIKELY(NULL == (new_ptr = parser_alloc_buffer(malloc_pool, headlen + nbyte)))) {
      } else {
        int64_t obyte = *(reinterpret_cast<int64_t *>(static_cast<char *>(ptr) - headlen));
        *(static_cast<int64_t *>(new_ptr)) = nbyte;
        new_ptr = static_cast<char *>(new_ptr) + headlen;
        MEMMOVE(new_ptr, ptr, static_cast<int64_t>(nbyte) > obyte ? obyte : nbyte);
        parser_free_buffer(malloc_pool, static_cast<char *>(ptr) - headlen);
      }
    }
  }
  return new_ptr;
}

char *parse_strndup(const char *str, size_t nbyte, void *malloc_pool)
{
  char *new_str = NULL;
  //need not to check nbyte
  if (OB_ISNULL(str)) {
  } else {
    if (OB_LIKELY(NULL != (new_str = static_cast<char *>(parse_malloc(nbyte + 1, malloc_pool))))) {
      MEMMOVE(new_str, str, nbyte);
      new_str[nbyte] = '\0';
    } else {
    }
  }
  return new_str;
}

char *parse_strdup(const char *str, void *malloc_pool, int64_t *out_len)
{
  char *out_str = NULL;
  if (OB_ISNULL(str)) {
  } else if (OB_ISNULL(out_len)) {
  } else {
    size_t dup_len = STRLEN(str);
    out_str = parse_strndup(str, dup_len, malloc_pool);
    *out_len = dup_len;
  }
  return out_str;
}

char *replace_invalid_character(const struct ObCharsetInfo* src_cs, const struct ObCharsetInfo* oracle_db_cs,
                                const char *str, int64_t *out_len, void *malloc_pool, int *extra_errno)
{
  char *out_str = NULL;
  if (OB_ISNULL(str) || OB_ISNULL(extra_errno) || OB_ISNULL(out_len)) {
  } else if (NULL == src_cs || NULL == oracle_db_cs) {
    out_str = const_cast<char *>(str);
  } else {
    ob_wc_t replace_char = (!!(oracle_db_cs->state & OB_CS_UNICODE)) &&
                           (!!(src_cs->state & OB_CS_UNICODE))
                           ? 0xFFFD : '?';
    uint errors = 0;
    size_t str_len = STRLEN(str);
    char *temp_str = NULL;
    size_t temp_len = str_len * 4;
    if (OB_ISNULL(temp_str = static_cast<char *>(parse_malloc(temp_len + 1, malloc_pool)))) {
    } else {
      int64_t temp_res_len = static_cast<int64_t>(
        ob_convert(temp_str, temp_len, oracle_db_cs, str, str_len, src_cs, false, replace_char, &errors));
      size_t dst_len = temp_res_len * 4;
      if (OB_ISNULL(out_str = static_cast<char *>(parse_malloc(dst_len + 1, malloc_pool)))) {
      } else {
        *out_len = static_cast<int64_t>(
          ob_convert(out_str, dst_len, src_cs, temp_str, temp_res_len, oracle_db_cs, false, replace_char, &errors));
        out_str[*out_len] = '\0';
      }
    }
  }
  return out_str;
}

char *parse_str_convert_utf8(const struct ObCharsetInfo* src_cs, const char *str, void *malloc_pool, int64_t *out_len, int *extra_errno)
{
  char *out_str = NULL;
  if (OB_ISNULL(str)
      || OB_ISNULL(out_len)
      || OB_ISNULL(extra_errno)) {
  } else {
    uint errors = 0;
    size_t str_len = STRLEN(str);
    size_t dst_len = str_len * 4;
    if (OB_ISNULL(out_str = static_cast<char *>(parse_malloc(dst_len + 1, malloc_pool)))) {
    } else {
      *out_len = static_cast<int64_t>(
        ob_convert(out_str, dst_len, &ob_charset_utf8mb4_general_ci, str, str_len, src_cs, false, '?', &errors));
      out_str[*out_len] = '\0';
      if (0 != errors) {
        *extra_errno = OB_PARSER_ERR_ILLEGAL_NAME;
      }
    }
  }
  return out_str;
}

char *strndup_with_prefix(const char *prefix, const char *str, size_t nbyte, void *malloc_pool)
{
  char *new_str = NULL;
  size_t prefix_len = strlen(prefix);
  //need not to check nbyte
  if (OB_ISNULL(str)) {
  } else {
    if (OB_LIKELY(NULL != (new_str = static_cast<char *>(parse_malloc(nbyte + prefix_len, malloc_pool))))) {
      MEMMOVE(new_str, prefix, prefix_len);
      MEMMOVE(new_str + prefix_len, str, nbyte);
    } else {
      // do nothing
    }
  }
  return new_str;
}

char *strndup_with_prefix_and_postfix(const char *prefix,
        const char *postfix, const char* str, size_t nbyte, void *malloc_pool)
{
  char *new_str = NULL;
  size_t prefix_len = strlen(prefix);
  size_t postfix_len = strlen(postfix);
  if (OB_ISNULL(str)) {
  } else {
    if (OB_LIKELY(NULL != (new_str = static_cast<char *>(parse_malloc(
                              nbyte + prefix_len + postfix_len, malloc_pool))))) {
      MEMMOVE(new_str, prefix, prefix_len);
      MEMMOVE(new_str + prefix_len, str, nbyte);
      MEMMOVE(new_str + prefix_len + nbyte, postfix, postfix_len);
    } else {
    }
  }
  return new_str;
}

void parse_free(void *ptr)
{
  UNUSED(ptr);
  /* do nothing, we don't really free the memory */
}

char *cp_str_value(const char *src, const size_t nbyte, void *malloc_pool)
{
  char *new_str = NULL;
  if (OB_ISNULL(src)) {
  } else {
    if (OB_LIKELY(NULL != (new_str = static_cast<char *>(parse_malloc(nbyte, malloc_pool))))) {
      MEMCPY(new_str, src, nbyte);
    } else {
    }
  }
  return new_str;
}

char *parse_strdup_with_replace_multi_byte_char(const char *str, int *connection_collation_,
                                                void *malloc_pool, int64_t *out_len)
{
  char *out_str = NULL;
  if (OB_ISNULL(str) || OB_ISNULL(connection_collation_) || OB_ISNULL(out_len)) {
  } else if (OB_LIKELY(NULL != (out_str = static_cast<char *>(parse_malloc(strlen(str) + 1,
                                                                           malloc_pool))))) {
    int64_t len = 0;
    int64_t dup_len = strlen(str);
    for (int64_t i = 0; i < dup_len; ++i) {
      if (*connection_collation_ == 28/*CS_TYPE_GBK_CHINESE_CI*/
       || *connection_collation_ == 87/*CS_TYPE_GBK_BIN*/
       || *connection_collation_ == 248/*CS_TYPE_GB18030_CHINESE_CI*/
       || *connection_collation_ == 249/*CS_TYPE_GB18030_BIN*/
       || (*connection_collation_ >= 216/*CS_TYPE_GB18030_2022_BIN*/
           && *connection_collation_ <= 222/*CS_TYPE_GB18030_2022_STROKE_CS*/)) {
        if (i + 1 < dup_len) {
          if (str[i] == (char)0xa1 && str[i+1] == (char)0xa1) {//gbk multi byte space
            out_str[len++] = ' ';
            ++i;
          } else if (str[i] == (char)0xa3 && str[i+1] == (char)0xa8) {
            //gbk multi byte left parenthesis
            out_str[len++] = '(';
            ++i;
          } else if (str[i] == (char)0xa3 && str[i+1] == (char)0xa9) {
            //gbk multi byte right parenthesis
            out_str[len++] = ')';
            ++i;
          } else {
            out_str[len++] = str[i];
          }
        } else {
          out_str[len++] = str[i];
        }
      } else if (
        *connection_collation_ == 45/*CS_TYPE_UTF8MB4_GENERAL_CI*/
        || *connection_collation_ == 46/*CS_TYPE_UTF8MB4_BIN*/
        || *connection_collation_ == 63/*CS_TYPE_BINARY*/
        || *connection_collation_ == 255/*CS_TYPE_UTF8MB4_0900_AI_CI*/
        || (*connection_collation_ >= 224/*CS_TYPE_UTF8MB4_UNICODE_CI*/
        && *connection_collation_ <= 247/*CS_TYPE_UTF8MB4_VIETNAMESE_CI*/)) {
        if (i + 2 < dup_len) {
          if (str[i] == (char)0xe3 && str[i+1] == (char)0x80 && str[i+2] == (char)0x80) {
            //utf8 multi byte space
            out_str[len++] = ' ';
            i = i + 2;
          } else if (str[i] == (char)0xef && str[i+1] == (char)0xbc && str[i+2] == (char)0x88) {
          //utf8 multi byte left parenthesis
            out_str[len++] = '(';
            i = i + 2;
          } else if (str[i] == (char)0xef && str[i+1] == (char)0xbc && str[i+2] == (char)0x89) {
          //utf8 multi byte right parenthesis
            out_str[len++] = ')';
            i = i + 2;
          } else {
            out_str[len++] = str[i];
          }
        } else {
          out_str[len++] = str[i];
        }
      } else if (
        *connection_collation_ == 152
        || *connection_collation_ == 153) {
        if (i + 1 < dup_len) {
          if (str[i] == (char)0xa1 && str[i+1] == (char)0x40) {//hkscs multi byte space
            out_str[len++] = ' ';
            ++i;
          } else if (str[i] == (char)0xa1 && str[i+1] == (char)0x5d) {
            //hkscs multi byte left parenthesis
            out_str[len++] = '(';
            ++i;
          } else if (str[i] == (char)0xa1 && str[i+1] == (char)0x5e) {
            //hkscs multi byte right parenthesis
            out_str[len++] = ')';
            ++i;
          } else {
            out_str[len++] = str[i];
          }
        } else {
          out_str[len++] = str[i];
        }
      } else {
        out_str[len++] = str[i];
      }
    }
    if (len > 0) {
      out_str[len] = '\0';
      *out_len = len;
    }
  }
  return out_str;
}

bool check_real_escape(const ObCharsetInfo *cs, char *str, int64_t str_len,
                       int64_t last_escape_check_pos)
{
  bool is_real_escape = true;
  if (NULL != cs && cs->escape_with_backslash_is_dangerous) {
    char *cur_pos = str + str_len;
    char *last_check_pos = str + last_escape_check_pos;
    int error = 0;
    size_t expected_well_formed_len = cur_pos - last_check_pos;
    while (last_check_pos < cur_pos) {
      size_t real_well_formed_len = cs->cset->well_formed_len(
                  cs, last_check_pos, cur_pos, UINT64_MAX, &error);
      last_check_pos += (real_well_formed_len + ((error != 0) ? 1 : 0));
    }
    if (error != 0) { //the final well-formed result
      *cur_pos = '\\';
      if (cs->cset->ismbchar(cs, cur_pos - 1, cur_pos + 1)) {
        is_real_escape = false;
      }
    }
  }
  return is_real_escape;
}
