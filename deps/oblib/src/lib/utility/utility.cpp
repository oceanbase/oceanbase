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

#define USING_LOG_PREFIX LIB

#include "dirent.h"
#include <dlfcn.h>
#include "lib/utility/utility.h"
#include "lib/ob_define.h"
#include "util/easy_inet.h"
#include "common/rowkey/ob_rowkey.h"
#include "lib/time/ob_time_utility.h"
#include "lib/file/file_directory_utils.h"
#include "common/ob_range.h"
#include "lib/string/ob_sql_string.h"
#include "lib/stat/ob_diagnose_info.h"
#include "common/object/ob_object.h"
#include "lib/net/ob_addr.h"
#include "lib/json/ob_yson.h"

namespace oceanbase
{
using namespace lib;
namespace common
{
extern "C"
{
  void do_breakpad_init() __attribute__((weak));
  void do_breakpad_init()
  {}
}

void hex_dump(const void *data, const int32_t size,
              const bool char_type /*= true*/, const int32_t log_level /*= OB_LOG_LEVEL_DEBUG*/)
{
  int ret = OB_SUCCESS;
  if (OB_LOGGER.get_log_level() < log_level) { return; }
  /* dumps size bytes of *data to stdout. Looks like:
   * [0000] 75 6E 6B 6E 6F 77 6E 20
   * 30 FF 00 00 00 00 39 00 unknown 0.....9.
   * (in a single line of course)
   */
  unsigned const char *p = (unsigned char *)data;
  unsigned char c = 0;
  int n = 0;
  char bytestr[4] = {0};
  char addrstr[10] = {0};
  char hexstr[ 16 * 3 + 5] = {0};
  char charstr[16 * 1 + 5] = {0};

  for (n = 1; n <= size; n++) {
    if (n % 16 == 1) {
      /* store address for this line */
      IGNORE_RETURN snprintf(addrstr, sizeof(addrstr), "%.4x",
                             (int)((unsigned long)p - (unsigned long)data));
    }

    c = *p;
    if (isprint(c) == 0) {
      c = '.';
    }

    /* store hex str (for left side) */
    IGNORE_RETURN snprintf(bytestr, sizeof(bytestr), "%02X ", *p);
    strncat(hexstr, bytestr, sizeof(hexstr) - strlen(hexstr) - 1);

    /* store char str (for right side) */
    IGNORE_RETURN snprintf(bytestr, sizeof(bytestr), "%c", c);
    strncat(charstr, bytestr, sizeof(charstr) - strlen(charstr) - 1);

    if (n % 16 == 0) {
      /* line completed */
      if (char_type)
        _OB_NUM_LEVEL_LOG(log_level, OB_SUCCESS, "[%ld] [%4.4s]   %-50.50s  %s\n",
                          pthread_self(), addrstr, hexstr, charstr);
      else
        _OB_NUM_LEVEL_LOG(log_level, OB_SUCCESS, "[%ld] [%4.4s]   %-50.50s\n",
                          pthread_self(), addrstr, hexstr);
      hexstr[0] = 0;
      charstr[0] = 0;
    } else if (n % 8 == 0) {
      /* half line: add whitespaces */
      strncat(hexstr, "  ", sizeof(hexstr) - strlen(hexstr) - 1);
      strncat(charstr, " ", sizeof(charstr) - strlen(charstr) - 1);
    }
    p++; /* next byte */
  }

  if (strlen(hexstr) > 0) {
    /* print rest of buffer if not empty */
    if (char_type)
      _OB_NUM_LEVEL_LOG(log_level, OB_SUCCESS, "[%ld] [%4.4s]   %-50.50s  %s\n",
                        pthread_self(), addrstr, hexstr, charstr);
    else
      _OB_NUM_LEVEL_LOG(log_level, OB_SUCCESS, "[%ld] [%4.4s]   %-50.50s\n",
                        pthread_self(), addrstr, hexstr);
  }
}
int32_t parse_string_to_int_array(const char *line,
                                  const char del, int32_t *array, int32_t &size)
{
  int ret = 0;
  ObArenaAllocator allocator;
  const char *start = line;
  const char *p = NULL;
  char *buffer = NULL;

  if (NULL == line || NULL == array || size <= 0) {
    ret = OB_INVALID_ARGUMENT;
  }

  if (NULL == (buffer = static_cast<char*>(allocator.alloc(OB_MAX_ROW_KEY_LENGTH)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }

  int32_t idx = 0;
  if (OB_SUCC(ret)) {
    while (OB_SUCC(ret) && NULL != start) {
      p = strchr(start, del);
      if (NULL != p) {
        memset(buffer, 0, OB_MAX_ROW_KEY_LENGTH);
        strncpy(buffer, start, p - start);
        if (strlen(buffer) > 0) {
          if (idx >= size) {
            ret = OB_SIZE_OVERFLOW;
            break;
          } else {
            array[idx++] = static_cast<int32_t>(strtol(buffer, NULL, 10));
          }
        }
        start = p + 1;
      } else {
        if (strlen(start) > OB_MAX_ROW_KEY_LENGTH - 1) {
          ret = OB_SIZE_OVERFLOW;
          break;
        } else {
          memset(buffer, 0, OB_MAX_ROW_KEY_LENGTH);
          strcpy(buffer, start);
          if (strlen(buffer) > 0) {
            if (idx >= size) {
              ret = OB_SIZE_OVERFLOW;
              break;
            } else {
              array[idx++] = static_cast<int32_t>(strtol(buffer, NULL, 10));
            }
          }
          break;
        }
      }
    }

    if (OB_SUCC(ret)) { size = idx; }
  }

  if (NULL != buffer) {
    allocator.free(buffer);
  }
  return ret;
}

int escape_enter_symbol(char *buffer, const int64_t length, int64_t &pos, const char *src)
{
  int ret = OB_SUCCESS;
  int64_t copy_size = pos;
  int64_t src_len = strlen(src);
  if (pos + src_len >= length || NULL == buffer) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    char escape = 0;
    for (int i = 0; OB_SUCC(ret) && i < src_len; ++i) {
      escape = 0;
      switch (src[i]) {
        case '\n':
          escape = 'n';
          break;
        default:
          break;
      }
      if (escape != 0) {
        if (copy_size >= length - 2) {
          ret = OB_BUF_NOT_ENOUGH;
          break;
        }
        buffer[pos++] = '\\';
        buffer[pos++] = escape;
        copy_size += 2;
      } else {
        if (copy_size >= length - 1) {
          ret = OB_BUF_NOT_ENOUGH;
          break;
        }
        buffer[pos++] = src[i];
        copy_size += 1;
      }
    }
    if (copy_size < length) {
      buffer[pos] = '\0';
    } else {
      ret = OB_BUF_NOT_ENOUGH;
    }
  }
  return ret;
}
/**
 * only escape " and \, used to stringify range2str
 */
int escape_range_string(char *buffer, const int64_t length, int64_t &pos, const ObString &in)
{
  int ret = OB_SUCCESS;
  int64_t copy_size = 0;
  if (pos + in.length() >= length || NULL == buffer) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    char escape = 0;;
    for (int i = 0; OB_SUCC(ret) && i < in.length(); ++i) {
      escape = 0;
      switch (in.ptr()[i]) {
        case '"':
          escape = '"';
          break;
        case '\\':
          escape = '\\';
          break;
        default:
          break;
      }
      if (escape != 0) {
        if (copy_size >= length - 2) {
          ret = OB_BUF_NOT_ENOUGH;
          break;
        }
        buffer[pos++] = '\\';
        buffer[pos++] = escape;
        copy_size += 2;
      } else {
        if (copy_size >= length - 1) {
          ret = OB_BUF_NOT_ENOUGH;
          break;
        }
        buffer[pos++] = in.ptr()[i];
        copy_size += 1;
      }
    }
  }
  return ret;
}

int convert_comment_str(char *comment_str)
{
  int ret = OB_SUCCESS;
  if (comment_str == NULL) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS != (ret = replace_str(comment_str, strlen(comment_str), "\\n", "\n"))) {
    _OB_LOG(WARN, "replace \\n to enter failed, src_str=%s, ret=%d", comment_str, ret);
  }
  return ret;
}

bool is2n(int64_t input)
{
  return (((~input + 1) & input) == input);
};

bool all_zero(const char *buffer, const int64_t size)
{
  bool bret = true;
  const char *buffer_end = buffer + size;
  const char *start = (char *)upper_align((int64_t)buffer, sizeof(int64_t));
  start = std::min(start, buffer_end);
  const char *end = (char *)lower_align((int64_t)(buffer + size), sizeof(int64_t));
  end = std::max(end, buffer);

  bret = all_zero_small(buffer, start - buffer);
  if (bret) {
    bret = all_zero_small(end, buffer + size - end);
  }
  if (bret) {
    const char *iter = start;
    while (iter < end) {
      if (0 != *((int64_t *)iter)) {
        bret = false;
        break;
      }
      iter += sizeof(int64_t);
    }
  }
  return bret;
};

bool all_zero_small(const char *buffer, const int64_t size)
{
  bool bret = true;
  if (NULL != buffer) {
    for (int64_t i = 0; i < size; i++) {
      if (0 != *(buffer + i)) {
        bret = false;
        break;
      }
    }
  }
  return bret;
}

char *str_trim(char *str)
{
  char *p = str, *sa = str;
  while (*p) {
    if (*p != ' ') {
      *str++ = *p;
    }
    p++;
  }
  *str = 0;
  return sa;
}

char *ltrim(char *str)
{
  char *p = str;
  while (p != NULL && *p != '\0' && isspace(*p)) {
    ++p;
  }
  return p;
}

char *rtrim(char *str)
{
  if ((str != NULL) && *str != '\0') {
    char *p = str + strlen(str) - 1;
    while ((p > str) && isspace(*p)) {
      --p;
    }
    *(p + 1) = '\0';
  }
  return str;
}
const char *inet_ntoa_s(char *buffer, size_t n, const uint64_t ipport)
{
  buffer[0] = '\0';
  uint32_t ip = (uint32_t)(ipport & 0xffffffff);
  int port = (int)((ipport >> 32) & 0xffff);
  unsigned char *bytes = (unsigned char *) &ip;
  if (port > 0) {
    IGNORE_RETURN snprintf(buffer, n, "%d.%d.%d.%d:%d", bytes[0], bytes[1], bytes[2], bytes[3], port);
  } else {
    IGNORE_RETURN snprintf(buffer, n, "%d.%d.%d.%d:-1", bytes[0], bytes[1], bytes[2], bytes[3]);
  }
  return buffer;
}

const char *inet_ntoa_s(char *buffer, size_t n, const uint32_t ip)
{
  buffer[0] = '\0';
  unsigned char *bytes = (unsigned char *) &ip;
  IGNORE_RETURN snprintf(buffer, n, "%d.%d.%d.%d", bytes[0], bytes[1], bytes[2], bytes[3]);
  return buffer;
}

const char *time2str(const int64_t time_us, const char *format)
{
  // FIXME: To Be Removed
  static const int32_t BUFFER_SIZE = 256;
  thread_local char buffer[4 * BUFFER_SIZE];
  RLOCAL(uint64_t, i);
  uint64_t cur = i++ % 4;
  buffer[cur * BUFFER_SIZE] = '\0';
  struct tm time_struct;
  int64_t time_s = time_us / 1000000;
  int64_t cur_second_time_us = time_us % 1000000;
  if (NULL != localtime_r(&time_s, &time_struct)) {
    int64_t pos = strftime(&buffer[cur * BUFFER_SIZE], BUFFER_SIZE, format, &time_struct);
    if (pos < BUFFER_SIZE) {
      IGNORE_RETURN snprintf(&buffer[cur * BUFFER_SIZE + pos], BUFFER_SIZE - pos, ".%ld %ld", cur_second_time_us, time_us);
    }
  }
  return &buffer[cur * BUFFER_SIZE];
}

void print_rowkey(FILE *fd, ObString &rowkey)
{
  char *pchar = rowkey.ptr();
  for (int i = 0; i < rowkey.length(); ++i, pchar++) {
    if (isprint(*pchar) != 0) {
      fprintf(fd, "%c", *pchar);
    } else {
      fprintf(fd, "\\%hhu", *pchar);
    }
  }
}

int mem_chunk_serialize(char *buf, int64_t len, int64_t &pos, const char *data, int64_t data_len)
{
  int err = OB_SUCCESS;
  int64_t tmp_pos = pos;
  if (NULL == buf || len <= 0 || pos < 0 || pos > len || NULL == data || 0 > data_len) {
    err = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS != (err = serialization::encode_i64(buf, len, tmp_pos, data_len))) {
    _OB_LOG_RET(ERROR, err, "encode_i64(buf=%p, len=%ld, pos=%ld, i=%ld)=>%d", buf, len, tmp_pos, data_len,
            err);
  } else if (tmp_pos + data_len > len) {
    err = OB_SERIALIZE_ERROR;
  } else {
    MEMCPY(buf + tmp_pos, data, data_len);
    tmp_pos += data_len;
    pos = tmp_pos;
  }
  return err;
}

int mem_chunk_deserialize(const char *buf, int64_t len, int64_t &pos, char *data, int64_t data_len,
                          int64_t &real_len)
{
  int err = OB_SUCCESS;
  int64_t tmp_pos = pos;
  if (NULL == buf || len <= 0 || pos < 0 || pos > len || NULL == data || data_len < 0) {
    err = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS != (err = serialization::decode_i64(buf, len, tmp_pos, &real_len))) {
    _OB_LOG_RET(ERROR, err, "decode_i64(buf=%p, len=%ld, pos=%ld, i=%ld)=>%d", buf, len, tmp_pos, real_len,
            err);
  } else if (real_len > data_len || tmp_pos + real_len > len) {
    err = OB_DESERIALIZE_ERROR;
  } else {
    MEMCPY(data, buf + tmp_pos, real_len);
    tmp_pos += real_len;
    pos = tmp_pos;
  }
  return err;
}

int get_double_expand_size(int64_t &current_size, const int64_t limit_size)
{
  int ret = OB_SUCCESS;
  int64_t new_size = current_size << 1;
  if (current_size > limit_size) {
    ret = OB_SIZE_OVERFLOW;
  } else if (new_size > limit_size) {
    current_size = limit_size;
  } else {
    current_size = new_size;
  }
  return ret;
}

uint16_t bswap16(uint16_t a)
{
  return static_cast<uint16_t>(((a >> 8) & 0xFFU) | ((a << 8) & 0xFF00U));
}

bool str_isprint(const char *str, const int64_t length)
{
  bool bret = false;
  if (NULL != str
      && 0 != length) {
    bret = true;
    for (int64_t i = 0; i < length; i++) {
      if (!isprint(str[i])) {
        bret = false;
        break;
      }
    }
  }
  return bret;
}


int replace_str(char *src_str, const int64_t src_str_buf_size,
                const char *match_str, const char *replace_str)
{
  int ret                 = OB_SUCCESS;
  int64_t str_len         = 0;
  int64_t match_str_len   = 0;
  int64_t replace_str_len = 0;
  const char *find_pos    = NULL;
  char new_str[OB_MAX_EXPIRE_INFO_STRING_LENGTH];

  if (NULL == src_str || src_str_buf_size <= 0
      || NULL == match_str || NULL == replace_str) {
    _OB_LOG(WARN, "invalid param, src_str=%p, src_str_buf_size=%ld, "
            "match_str=%p, replace_str=%p",
            src_str, src_str_buf_size, match_str, replace_str);
    ret = OB_ERROR;
  } else if (NULL != (find_pos = strstr(src_str, match_str))) {
    match_str_len = strlen(match_str);
    replace_str_len = strlen(replace_str);
    while (OB_SUCC(ret) && NULL != find_pos) {
      str_len = find_pos - src_str + replace_str_len
          + strlen(find_pos + match_str_len);
      if (str_len >= OB_MAX_EXPIRE_INFO_STRING_LENGTH
          || str_len >= src_str_buf_size) {
        _OB_LOG(WARN, "str after replace is too large, new_size=%ld, "
                "new_buf_size=%ld, src_str_buf_size=%ld",
                str_len, OB_MAX_EXPIRE_INFO_STRING_LENGTH, src_str_buf_size);
        ret = OB_ERROR;
        break;
      } else {
        memset(new_str, 0, OB_MAX_EXPIRE_INFO_STRING_LENGTH);
        strncpy(new_str, src_str, find_pos - src_str);
        strcat(new_str, replace_str);
        strcat(new_str, find_pos + match_str_len);
        strcpy(src_str, new_str);
      }

      find_pos = strstr(src_str, match_str);
    }
  }

  return ret;
}

int get_ethernet_speed(const char *devname, int64_t &speed)
{
  int ret = OB_SUCCESS;
  if (NULL == devname) {
    ret = OB_INVALID_ARGUMENT;
    _OB_LOG(WARN, "invalid devname %p", devname);
  } else {
    ret = get_ethernet_speed(ObString::make_string(devname), speed);
  }
  return ret;
}

int get_ethernet_speed(const ObString &devname, int64_t &speed)
{
  int ret = OB_SUCCESS;
  bool exist = false;
  char path[OB_MAX_FILE_NAME_LENGTH];
  static int dev_file_exist = 1;
  if (0 == devname.length()) {
    ret = OB_INVALID_ARGUMENT;
    _OB_LOG(WARN, "empty devname");
  } else {
    IGNORE_RETURN snprintf(path, sizeof(path), "/sys/class/net/%.*s", devname.length(), devname.ptr());
    if (OB_SUCCESS != (ret = FileDirectoryUtils::is_exists(path, exist)) || !exist) {
      if (dev_file_exist) {
      _OB_LOG(WARN, "path %s not exist", path);
       dev_file_exist = 0;
      }
      ret = OB_FILE_NOT_EXIST;
    }
  }
  if (OB_SUCCESS != ret)
  {}
  else {
    CharArena alloc;
    ObString str;
    IGNORE_RETURN snprintf(path, sizeof(path), "/sys/class/net/%.*s/bonding/",
                           devname.length(), devname.ptr());
    if (OB_SUCCESS != (ret = FileDirectoryUtils::is_exists(path, exist))) {
      LIB_LOG(WARN, "check net file if exists failed.", K(ret));
    } else if (exist) {
      IGNORE_RETURN snprintf(path, sizeof(path), "/sys/class/net/%.*s/bonding/slaves",
                             devname.length(), devname.ptr());
      if (OB_SUCCESS != (ret = load_file_to_string(path, alloc, str))) {
        _OB_LOG(WARN, "load file %s failed, ret %d", path, ret);
      } else if (0 == str.length()) {
        _OB_LOG(WARN, "can't get slave ethernet");
        ret = OB_ERROR;
      } else {
        int len = 0;
        while (len < str.length() && !isspace(str.ptr()[len])) {
          len++;
        }
        IGNORE_RETURN snprintf(path, sizeof(path), "/sys/class/net/%.*s/speed", len, str.ptr());
      }
    } else {
      IGNORE_RETURN snprintf(path, sizeof(path), "/sys/class/net/%.*s/speed",
                             devname.length(), devname.ptr());
    }
    if (OB_SUCCESS == ret) {
      if (OB_SUCCESS != (ret = load_file_to_string(path, alloc, str))) {
        _OB_LOG(WARN, "load file %s failed, ret %d", path, ret);
      } else {
        speed = atoll(str.ptr());
        speed = speed * 1024 * 1024 / 8;
      }
    }
  }
  return ret;
}

int deep_copy_obj(ObIAllocator &allocator, const ObObj &src, ObObj &dst)
{
  int ret = OB_SUCCESS;
  if (!src.need_deep_copy()) {
    dst = src;
  } else {
    char *buf = NULL;
    int64_t size = src.get_deep_copy_size();
    int64_t pos = 0;
    if (size > 0) {
      if (NULL == (buf = static_cast<char *>(allocator.alloc(size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("Fail to allocate memory, ", K(size), K(ret));
      } else if (OB_FAIL(dst.deep_copy(src, buf, size, pos))){
        LOG_WARN("Fail to deep copy obj, ", K(ret));
      } else { }//do nothing
    } else {
      dst = src;
    }
  }
  return ret;
}

int deep_copy_objparam(ObIAllocator &allocator, const ObObjParam &src, ObObjParam &dst)
{
  int ret = OB_SUCCESS;
  if (!src.need_deep_copy()) {
    dst = src;
  } else if (OB_FAIL(deep_copy_obj(allocator, src, dst))) {
    LOG_WARN("failed to deep copy obj", K(ret));
  } else {
    dst.set_accuracy(src.get_accuracy());
    dst.unset_result_flag(dst.get_result_flag());
    dst.set_result_flag(src.get_result_flag());
    dst.set_param_flag(src.get_param_flag());
    dst.set_param_meta(src.get_param_meta());
  }
  return ret;
}

bool is_case_space_equal(const char *s1, int64_t s1_len, const char *s2, int64_t s2_len)
{
  int result = true;

  //Check input
  if (NULL == s1 || NULL == s2 || s1_len < 0 || s2_len < 0) {
    result = false;
    _OB_LOG_RET(ERROR, OB_INVALID_ARGUMENT,
                "Invalid argument, input arguments include NULL pointer or string length is less than zero.");
  } else if (s1 != s2) { //If s1 == s2,return 1
    while (1) {
      //Left trim
      while (s1_len != 0 && isspace(*s1)) {
        --s1_len;
        ++s1;
      }

      while (s2_len != 0 && isspace(*s2)) {
        --s2_len;
        ++s2;
      }

      //To stop while(1).
      if (0 == s1_len && 0 == s2_len) {
        result = true;
        break;
      } else if (0 == s1_len || 0 == s2_len) {
        result = false;
        break;
      }

      if (false == result) {
        break;
      }

      //Compare chars bettween s1 and s2
      while (s1_len != 0 && s2_len != 0) {
        if (tolower(*s1) != tolower(*s2)) {
          result = false;
          break;
        }

        int s1_is_space = 0;
        int s2_is_space = 0;

        if (--s1_len != 0) {
          s1_is_space = isspace(*(++s1));
        }

        if (--s2_len != 0) {
          s2_is_space = isspace(*(++s2));
        }

        if (s1_is_space != s2_is_space) {
          result = false;
          break;
        } else if (s1_is_space && s2_is_space) {
          break;
        }
      } // end of while (s1_len != 0 && s2_len != 0)
    } // end of while(1)
  } // end of else if (s1 != s2)

  return result;
}

bool is_n_case_space_equal(const char *s1, int64_t s1_len, const char *s2, int64_t s2_len,
                           int64_t cmp_len)
{
  bool result = true;

  //Check input
  if (NULL == s1 || NULL == s2 || s1_len < 0 || s2_len < 0) {
    result = false;
    _OB_LOG_RET(ERROR, OB_INVALID_ARGUMENT,
                "Invalid argument, input arguments include NULL pointer or string length is less than zero.");
  } else if (s1 != s2) { //If s1 == s2,return 1
    while (1) {
      //Left trim
      while (s1_len != 0 && isspace(*s1)) {
        --s1_len;
        ++s1;
      }

      while (s2_len != 0 && isspace(*s2)) {
        --s2_len;
        ++s2;
      }

      //To stop while(1).
      if (cmp_len <= 0 || (0 == s1_len && 0 == s2_len)) {
        result = true;
        break;
      } else if (0 == s1_len || 0 == s2_len) {
        result = false;
        break;
      }

      if (false == result) {
        break;
      }

      //Compare chars bettween s1 and s2
      while (cmp_len > 0 && s1_len != 0 && s2_len != 0) {
        if (tolower(*s1) != tolower(*s2)) {
          result = false;
          break;
        }

        --cmp_len;

        int s1_is_space = 0;
        int s2_is_space = 0;

        if (--s1_len != 0) {
          s1_is_space = isspace(*(++s1));
        }

        if (--s2_len != 0) {
          s2_is_space = isspace(*(++s2));
        }

        if (s1_is_space != s2_is_space) {
          result = false;
          break;
        } else if (s1_is_space && s2_is_space) {
          --cmp_len;
          break;
        }
      } // end of while (cmp_len > 0 && s1_len != 0 && s2_len != 0)
    } // end of while(1)
  } // end of if (s1 != s2)

  return result;
}

int wild_compare(const char *str, const char *wild_str, const bool str_is_pattern)
{
  const char wild_many = '%';
  const char wild_one = '_';
  const char wild_prefix = '\\';
  char cmp = 0;
  const int EQUAL = 0;
  const int UNEQUAL = 1;
  const int UNKOWN = -1;
  int ret = UNKOWN;
  if (NULL != str && NULL != wild_str) {
    while (*wild_str) {
      while (*wild_str && *wild_str != wild_many && *wild_str != wild_one) {
        if (*wild_str == wild_prefix && wild_str[1]) {
          wild_str++;
          if (str_is_pattern && *str++ != wild_prefix) {
            ret = UNEQUAL;
            break;
          }
        }
        if (*wild_str++ != *str++) {
          ret = UNEQUAL;
          break;
        }
      }
      if (UNKOWN != ret) {
        break;
      }
      if (! *wild_str ) {
        ret = (*str != 0) ? UNEQUAL : EQUAL;
        break;
      }
      if (*wild_str++ == wild_one) {
        if (! *str || (str_is_pattern && *str == wild_many)) {
          ret = UNEQUAL;                     // One char; skip
          break;
        }
        if (*str++ == wild_prefix && str_is_pattern && *str) {
          str++;
        }
      } else {						// Found wild_many
        while (str_is_pattern && *str == wild_many) {
          str++;
        }
        for (; *wild_str ==  wild_many || *wild_str == wild_one; wild_str++) {
          if (*wild_str == wild_many) {
            while (str_is_pattern && *str == wild_many)
              str++;
          }
          else {
            if (str_is_pattern && *str == wild_prefix && str[1]) {
              str+=2;
            } else if (! *str++) {
              ret = UNEQUAL;
              break;
            }
          }
        }
        if (!*wild_str) {
          ret = EQUAL;		// wild_many as last char: OK
          break;
        }
        if ((cmp= *wild_str) == wild_prefix && wild_str[1] && !str_is_pattern) {
          cmp=wild_str[1];
        }
        for (;;str++) {
          while (*str && *str != cmp) {
            str++;
          }
          if (!*str) {
            ret = UNEQUAL;
            break;
          }
          if (wild_compare(str,wild_str,str_is_pattern) == 0) {
            ret = EQUAL;
            break;
          }
        }
      }
    }
    if (UNKOWN == ret) {
      ret = (*str != 0) ? UNEQUAL : EQUAL;
    }
  }
  return ret;
}

int wild_compare(const ObString &str, const ObString &wild_str, const bool str_is_pattern)
{
  const char wild_many = '%';
  const char wild_one = '_';
  const char wild_prefix = '\\';
  char cmp = 0;
  const int EQUAL = 0;
  const int UNEQUAL = 1;
  const int UNKOWN = -1;
  int ret = UNKOWN;
  const char *str_ptr = str.ptr();
  const char *wild_ptr = wild_str.ptr();
  const ObString::obstr_size_t str_length = str.length();
  const ObString::obstr_size_t wild_length = wild_str.length();
  ObString::obstr_size_t str_pos = 0;
  ObString::obstr_size_t wild_pos = 0;
#define INC_STR()                               \
  str_ptr++;                                    \
  str_pos++;

#define INC_WILD()                              \
  wild_ptr++;                                   \
  wild_pos++;

  if (NULL != str_ptr && NULL != wild_ptr) {
    while (wild_pos < wild_length) {
      //check char without wild_many or wild_one.
      while (wild_pos < wild_length && *wild_ptr != wild_many && *wild_ptr != wild_one) {
        if (*wild_ptr == wild_prefix && (wild_pos+1 < wild_length)) {
          INC_WILD();
          if (str_is_pattern) {
            if (str_pos == str_length || (str_pos < str_length && *str_ptr != wild_prefix)) {
              ret = UNEQUAL;
              break;
            }
            INC_STR();
          }
        }
        if ((str_pos >= str_length
             || (str_pos < str_length && wild_pos < wild_length && *wild_ptr != *str_ptr))) {
          ret = UNEQUAL;
          break;
        }
        INC_STR();
        INC_WILD();
      }

      if (UNKOWN != ret) {
        break;
      }
      if (wild_pos >= wild_length) {
        ret = (str_pos < str_length) ? UNEQUAL : EQUAL;
        break;
      }
      if (wild_pos < wild_length && *wild_ptr == wild_one) {
        INC_WILD();
        if (str_pos >= str_length
            || (str_is_pattern && (str_pos < str_length && *str_ptr == wild_many))) {
          ret = UNEQUAL;       // One char; skip
          break;
        }
        if (str_pos + 1 < str_length && *str_ptr == wild_prefix && str_is_pattern) {
          INC_STR();
        }
        INC_STR();
      } else {
        INC_WILD();
        while (str_is_pattern && str_pos < str_length && *str_ptr == wild_many) {
          INC_STR();
        }
        for (; wild_pos < wild_length && (*wild_ptr == wild_many || *wild_ptr == wild_one);
             wild_ptr++, wild_pos++) {
          if (*wild_ptr == wild_many) {
            while (str_is_pattern && str_pos < str_length && *str_ptr == wild_many) {
              INC_STR();
            }
          } else {
            if (str_is_pattern && str_pos + 1 < str_length && *str_ptr == wild_prefix) {
              INC_STR();
            } else if (str_pos >= str_length) {
              ret = UNEQUAL;
              break;
            }
            INC_STR();
          }
        }
        if (wild_pos >= wild_length) {
          ret = EQUAL;      // wild_many as last char: OK
          break;
        }
        if ((cmp = *wild_ptr) == wild_prefix && wild_pos + 1 < wild_length && !str_is_pattern) {
          cmp = wild_ptr[1];
        }
        for (;; str_pos++, str_ptr++) {
          while (str_pos < str_length && *str_ptr != cmp) {
            INC_STR();
          }
          if (str_pos >= str_length) {
            ret = EQUAL;
            break;
          }
          if (wild_compare(ObString(str_length - str_pos, str_ptr),
                           ObString(wild_length - wild_pos, wild_ptr), str_is_pattern) == 0) {
            ret = EQUAL;
            break;
          }
        }
      }
    }
    if (UNKOWN == ret) {
      ret = (str_pos < str_length) ? UNEQUAL : EQUAL;
    }
  }
#undef INC_STR
#undef INC_WILD
  return ret;
}

uint64_t get_sort(uint count, ...)
{
  const char wild_many = '%';
  const char wild_one = '_';
  const char wild_prefix = '\\';
  va_list args;
  va_start(args,count);
  ulong sort=0;

  // Should not use this function with more than 8 arguments for compare.
  if (count <= 8) {
    while (count--) {
      char *start = NULL;
      char *str= va_arg(args,char*);
      uint chars= 0;
      uint wild_pos= 0;           // first wildcard position

      if ((start = str)) {
        for (; *str ; str++) {
          if (*str == wild_prefix && str[1]) {
            str++;
          } else if (*str == wild_many || *str == wild_one) {
            wild_pos= (uint) (str - start) + 1;
            break;
          }
          chars= 128;                             // Marker that chars existed
        }
      }
      sort= (sort << 8) + (wild_pos ? min(wild_pos, 127U) : chars);
    }
  }
  va_end(args);
  return sort;
}

uint64_t get_sort(const ObString &str)
{
  const char wild_many = '%';
  const char wild_one = '_';
  const char wild_prefix = '\\';
  uint64_t sort = 0;
  if (str.length() > 0) {
    const char *str_ptr = str.ptr();
    ObString::obstr_size_t str_pos = 0;
    const ObString::obstr_size_t str_length = str.length();
    uint32_t specific_chars = 128;
    uint32_t wild_pos = 0; // first wildcard position

    for (; str_pos < str_length; str_ptr++, str_pos++) {
      if (str_pos + 1 < str_length && *str_ptr == wild_prefix) {
        str_ptr++;
        str_pos++;
      } else if (str_pos < str_length && (*str_ptr == wild_many || *str_ptr == wild_one)) {
        wild_pos = str_pos + 1;
        break;
      }
    }
    sort = wild_pos ? min(wild_pos, 127U): specific_chars;
  }
  return sort;
}

bool prefix_match(const char *prefix, const char *str)
{
  bool b_ret = false;
  if (NULL == prefix || NULL == str) {

  } else {
    size_t prefix_len = strlen(prefix);
    size_t str_len = strlen(str);
    if (prefix_len > str_len) {

    } else if (0 == MEMCMP(prefix, str, prefix_len)) {
      b_ret = true;
    }
  }
  return b_ret;
}

int str_cmp(const void *v1, const void *v2)
{
  int ret = 0;
  if (NULL == v1) {
    ret = -1;
  } else if (NULL == v2) {
    ret = 1;
  } else {
    ret = strcmp((const char*)v1, (const char*)v2);
  }
  return ret;
}

const char* get_default_if()
{
  static char ifname[128] = {};
  int found = 0;
  FILE *file = NULL;
  file = fopen("/proc/net/route", "r");
  if (file) {
    char dest[16] = {};
    char gw[16] = {};
    char remain[1024+1] = {};
    if (1 == fscanf(file, "%1024[^\n]\n", remain)) {
      while (1) {
        int r = fscanf(file,
                       "%127s\t%15s\t%15s\t%1023[^\n]\n",
                       ifname, dest, gw, remain);
        if (r < 4) {
          break;
        }
        if (MEMCMP(gw, "00000000", 8) != 0) {
          found = 1;
          break;
        }
      }
    }
    fclose(file);
  }
  if (!found) {
    ifname[0] = '\0';
  }
  return ifname;
}

int sql_append_hex_escape_str(const ObString &str, ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  const int64_t need_len = sql.length() + str.length() * 2 + 3; // X''

  if (OB_FAIL(sql.reserve(need_len))) {
    LOG_WARN("reserve sql failed, ", K(ret));
  } else if (OB_FAIL(sql.append("X'"))) {
    LOG_WARN("append string failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < str.length(); ++i) {
      if (OB_FAIL(sql.append_fmt("%02X", static_cast<uint8_t>(str.ptr()[i])))) {
        LOG_WARN("append string failed", K(ret), K(i));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(sql.append("'"))) {
      LOG_WARN("append string failed", K(ret));
    }
  }
  return ret;
}

static int pidfile_test(const char *pidfile)
{
  int ret = OB_SUCCESS;
  int fd = open(pidfile, O_RDONLY);

  if (fd < 0) {
    LOG_INFO("fid file doesn't exist", KCSTRING(pidfile));
    ret = OB_FILE_NOT_EXIST;
  } else {
    if (lockf(fd, F_TEST, 0) != 0) {
      ret = OB_ERROR;
    }
    close(fd);
  }

  return ret;
}

static int read_pid(const char *pidfile, long &pid)
{
  int ret = OB_SUCCESS;
  char buf[32] = {};
  int fd = open(pidfile, O_RDONLY);

  if (fd < 0) {
    LOG_ERROR("can't open pid file", KCSTRING(pidfile), K(errno));
    ret = OB_FILE_NOT_EXIST;
  } else if (read(fd, buf, sizeof(buf) - 1) <= 0) {
    LOG_ERROR("fail to read pid from file", KCSTRING(pidfile), K(errno));
    ret = OB_IO_ERROR;
  } else {
    pid = strtol(buf, NULL, 10);
  }

  // POSIX file descriptors are non-negative integers.
  if (fd >= 0) {
    close(fd);
  }

  return ret;
}

static int use_daemon()
{
  int ret = OB_SUCCESS;
  const int nochdir = 1;
  const int noclose = 0;
  if (daemon(nochdir, noclose) < 0) {
    LOG_ERROR("create daemon process fail", K(errno));
    ret = OB_ERR_SYS;
  }
  reset_tid_cache();
  // bt("enable_preload_bt") = 1;
  return ret;
}

int start_daemon(const char *pidfile)
{
  int ret = OB_SUCCESS;

  ret = pidfile_test(pidfile);

  if (ret != OB_SUCCESS && ret != OB_FILE_NOT_EXIST) {
    LOG_ERROR("pid already exists");
  } else {
    ret = OB_SUCCESS;
  }

  // start daemon
  if (OB_SUCC(ret) && OB_FAIL(use_daemon())) {
    LOG_ERROR("create daemon process fail", K(ret));
  }

  if (OB_SUCC(ret)) {
    int fd = open(pidfile, O_RDWR|O_CREAT, 0600);

    if (fd < 0) {  // open pidfile fail
      LOG_ERROR("can't open pid file", KCSTRING(pidfile), K(fd), K(errno));
      ret = OB_IO_ERROR;
    } else if (lockf(fd, F_TLOCK, 0) < 0) {  // other process has locked it
      long pid = 0;
      if (OB_FAIL(read_pid(pidfile, pid))) {
        LOG_ERROR("read pid fail", KCSTRING(pidfile), K(ret));
      } else {
        LOG_ERROR("process is running", K(pid));
      }
      close(fd);
    } else {  // I hold the lock, won't close this fd.
      if (ftruncate(fd, 0) < 0) {
        LOG_ERROR("ftruncate pid file fail", KCSTRING(pidfile), K(errno));
        ret = OB_IO_ERROR;
      } else {
        char buf[32] = {};
        IGNORE_RETURN snprintf(buf, sizeof(buf), "%d", getpid());
        ssize_t len = strlen(buf);
        ssize_t nwrite = write(fd, buf, len);
        if (len != nwrite) {
          LOG_ERROR("write pid file fail", KCSTRING(pidfile), K(errno));
          ret = OB_IO_ERROR;
        }
      }
    }
  }

  return ret;
}

int ob_alloc_printf(ObString &result, ObIAllocator &alloc, const char* fmt, va_list ap)
{
  int ret = OB_SUCCESS;
  va_list ap2;
  va_copy(ap2, ap);
  int64_t n = vsnprintf(NULL, 0, fmt, ap);
  if (n < 0) {
    LOG_ERROR("vsnprintf failed", K(n), K(errno));
    ret = OB_ERR_SYS;
  } else {
    char* buf = static_cast<char*>(alloc.alloc(n+1));
    if (NULL == buf) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory");
    } else {
      int64_t n2 = vsnprintf(buf, n+1, fmt, ap2);
      if (n2 < 0) {
        LOG_ERROR("vsnprintf failed", K(n), K(errno));
        ret = OB_ERR_SYS;
      } else if (n != n2) {
        LOG_ERROR("vsnprintf failed", K(n), K(n2));
        ret = OB_ERR_SYS;
      } else {
        result.assign_ptr(buf, static_cast<int32_t>(n));
      }
    }
  }
  return ret;
}

int ob_alloc_printf(ObString &result, ObIAllocator &alloc, const char* fmt, ...)
{
  va_list ap;
  va_start(ap, fmt);
  int ret = ob_alloc_printf(result, alloc, fmt, ap);
  va_end(ap);
  return ret;
}

#define CLICK_STR(i) has_click_str ? click_str_[(i)] : ""
#define EQUAL_STR has_click_str ? "=" : ""
#define COMMA_STR has_click_str ? ", " : ","
#define COMMA_STR_LEN has_click_str ? 2 : 1

#define FSTR "%s%s%d%s"
#define VSTR(i) CLICK_STR(i), EQUAL_STR, click_[i], COMMA_STR

int64_t ObTimeGuard::to_string(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (!need_record_log_) {
    ret = databuff_printf(buf, buf_len, pos, "time guard have no click events for optimization");
  } else {
    int64_t i = 0;

    ret = databuff_printf(buf, buf_len, pos, "time guard '%s' cost too much time, used=%ld%s",
        owner_, common::ObTimeUtility::fast_current_time() - start_ts_,
        click_count_ > 0 ? ", time_dist: " : "");

    if (OB_SUCC(ret) && click_count_ > 0) {
      ret = databuff_printf(buf, buf_len, pos, "%s=%d", click_str_[0], click_[0]);
    }
    for (int i = 1; OB_SUCC(ret) && i < click_count_; i++) {
      ret = databuff_printf(buf, buf_len, pos, ", %s=%d", click_str_[i], click_[i]);
    }
  }

  if (OB_FAIL(ret)) pos = 0;

  return pos;
}

DEFINE_TO_YSON_KV(ObTimeGuard, OB_ID(click), common::ObArrayWrap<int32_t>(click_, click_count_));

////////////////////////////////////////////////////////////////////
// BandwidthThrottle
ObBandwidthThrottle::ObBandwidthThrottle()
    : lock_(ObLatchIds::BANDWIDTH_THROTTLE_LOCK),
      rate_(0),
      next_avaliable_timestamp_(0),
      unlimit_bytes_(0),
      total_bytes_(0),
      total_sleep_ms_(0),
      last_printed_bytes_(0),
      last_printed_sleep_ms_(0),
      last_printed_ts_(0),
      inited_(false)
{
}


ObBandwidthThrottle::~ObBandwidthThrottle()
{
}

int ObBandwidthThrottle::init(const int64_t rate, const char *comment)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "init throttle twice.", K(ret));
  } else if (rate < 0 || OB_ISNULL(comment)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments.", K(ret), K(rate), KP(comment));
  } else {
    ObSpinLockGuard guard(lock_);
    rate_ = rate;
    next_avaliable_timestamp_ = ObTimeUtility::current_time();
    unlimit_bytes_ = 0;
    last_printed_bytes_ = 0;
    last_printed_ts_ = ObTimeUtility::current_time();
    total_bytes_ = 0;
    databuff_printf(comment_, OB_MAX_TASK_COMMENT_LENGTH, "%s", comment);
    inited_ = true;

    COMMON_LOG(INFO, "init bandwidth", K(rate_), KCSTRING(comment_));
    if (0 == rate_) {
      LOG_ERROR("bandwidth throttle rate is zero, please make sure is the config right?");
    }
  }
  return ret;
}

int ObBandwidthThrottle::get_rate(int64_t &rate)
{
  int ret = OB_SUCCESS;
  rate = 0;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not inited", K(ret));
  } else {
    rate = rate_ / (1024 * 1024); // MB/s
  }
  return ret;
}

int ObBandwidthThrottle::set_rate(const int64_t rate)
{
  int ret = OB_SUCCESS;

  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not inited", K(ret));
  } else if (rate < 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid args", K(ret), K(rate));
  } else {
    ObSpinLockGuard guard(lock_);
    if (rate_ != rate) {
      COMMON_LOG(INFO, "set bandwidth", "old rate", rate_, "new rate", rate);
      rate_ = rate;
      next_avaliable_timestamp_ = ObTimeUtility::current_time();
      unlimit_bytes_ = 0;
      if (0 == rate_) {
        LOG_ERROR("bandwidth throttle rate is zero, please make sure is the config right?");
      }
    }
  }

  return ret;
}

void ObBandwidthThrottle::destroy()
{
  ObSpinLockGuard guard(lock_);
  COMMON_LOG(INFO, "destroy bandwidth throttle");
  inited_ = false;
}

int ObBandwidthThrottle::limit_and_sleep(const int64_t bytes, const int64_t last_active_time, const int64_t max_idle_time, int64_t &sleep_us)
{
  int ret = OB_SUCCESS;
  const int64_t PRINT_LOG_INTERVAL_MS = 1000;// 1s
  int64_t avaliable_timestamp = 0;
  sleep_us = 0;

  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "throttle is not initialized.", K(ret));
  } else if (OB_FAIL(cal_limit(bytes, avaliable_timestamp))) {
    LOG_WARN("failed to cal limit", K(ret));
  } else if (OB_FAIL(do_sleep(avaliable_timestamp, last_active_time, max_idle_time, sleep_us))) {
    LOG_WARN("failed to do sleep", K(ret));
  } else {
    ObSpinLockGuard guard(lock_);
    const int64_t cur_time = ObTimeUtility::current_time();
    const int64_t print_interval_ms = (cur_time - last_printed_ts_) / 1000;

    total_bytes_ += bytes;
    total_sleep_ms_ += sleep_us / 1000;
    if (print_interval_ms > PRINT_LOG_INTERVAL_MS) {
      const int64_t copy_KB = (total_bytes_ - last_printed_bytes_) / 1024;
      const int64_t speed_KB_per_s = copy_KB * 1000 / print_interval_ms;
      const int64_t sleep_ms_sum = total_sleep_ms_ - last_printed_sleep_ms_;
      const int64_t rate_KB = rate_ / 1024;
      COMMON_LOG(INFO, "print band limit", KCSTRING_(comment), K(copy_KB), K(sleep_ms_sum), K(speed_KB_per_s),
          K_(total_sleep_ms), K_(total_bytes), "rate_KB/s", rate_KB, K(print_interval_ms));
      last_printed_bytes_ = total_bytes_;
      last_printed_sleep_ms_ = total_sleep_ms_;
      last_printed_ts_ = cur_time;
    }
  }
  return ret;
}

int ObBandwidthThrottle::cal_limit(const int64_t bytes, int64_t &avaliable_timestamp)
{
  int ret = OB_SUCCESS;
  avaliable_timestamp = 0;

  ObSpinLockGuard guard(lock_);
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "throttle is not initialized.", K(ret));
  } else if (bytes < 0 || bytes > 1024 * 1024 * 1024LL/* 1G */) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid input bytes.", K(ret), K(bytes));
  } else if (0 == rate_) {
    avaliable_timestamp = INT64_MAX;
  } else {
    int64_t cur_time = ObTimeUtility::current_time();
    unlimit_bytes_ += bytes;

    if (unlimit_bytes_ > INT64_MAX / 1000 / 1000) {
      COMMON_LOG(ERROR, "unlimit_bytes_ is too large, cannot limit it...", K(bytes), K(rate_), K(unlimit_bytes_));
      next_avaliable_timestamp_ = cur_time;
      unlimit_bytes_ = 0;
    } else if (unlimit_bytes_ * 1000 * 1000 >= 200 * rate_) { // The minimum calculation unit is 200us, and the fragments smaller than the limit will be calculated next time
      next_avaliable_timestamp_ = next_avaliable_timestamp_ + unlimit_bytes_ * 1000 * 1000 / rate_ + 1;
      unlimit_bytes_ = 0;

      if (cur_time > next_avaliable_timestamp_ + 200) { // Update the time stamp of the record when it exceeds 200us
        next_avaliable_timestamp_ = cur_time - 200;
      }
    }

    if (cur_time > next_avaliable_timestamp_) {
      avaliable_timestamp = cur_time;
    } else {
      avaliable_timestamp = next_avaliable_timestamp_;
    }

    if (avaliable_timestamp - cur_time > UINT32_MAX) {
      COMMON_LOG(ERROR, "invalid limit time, maybe callers does not sleep properly after call this func",
          K(ret), K(bytes), K(avaliable_timestamp), K(next_avaliable_timestamp_), K(unlimit_bytes_));
    }
  }

  return ret;
}

int ObBandwidthThrottle::do_sleep(
    const int64_t next_avaliable_ts, const int64_t last_active_time, const int64_t max_idle_time,
    int64_t &sleep_us)
{
  int ret = OB_SUCCESS;

  int64_t cur_time = ObTimeUtility::current_time();
  int64_t max_wait_time = cur_time - last_active_time > 0 ?
      max_idle_time - (cur_time - last_active_time) : max_idle_time;
  int64_t sleep_time = next_avaliable_ts - cur_time;
  int64_t real_sleep_time = std::min(sleep_time, max_wait_time);
  sleep_us = 0;

  if (real_sleep_time > 0 && real_sleep_time <= UINT32_MAX) {
    COMMON_LOG(DEBUG, "do band limit sleep", K(max_wait_time), K(sleep_time), K(real_sleep_time), K(last_active_time));
    sleep_us = real_sleep_time;
    ob_usleep<common::ObWaitEventIds::BANDWIDTH_THROTTLE_SLEEP>(static_cast<uint32_t>(real_sleep_time));
  }

  return ret;
}

ObInOutBandwidthThrottle::ObInOutBandwidthThrottle()
  : in_throttle_(),
    out_throttle_()
{
}

ObInOutBandwidthThrottle::~ObInOutBandwidthThrottle()
{
}

int ObInOutBandwidthThrottle::init(const int64_t rate)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(in_throttle_.init(rate, "in"))) {
    COMMON_LOG(WARN, "failed to init in_throttle_", K(ret));
  } else if (OB_FAIL(out_throttle_.init(rate, "out"))) {
    COMMON_LOG(WARN, "failed to init out_throttle_", K(ret));
  }
  return ret;
}

int ObInOutBandwidthThrottle::set_rate(const int64_t rate)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(in_throttle_.set_rate(rate))) {
    COMMON_LOG(WARN, "failed to set in_throttle_ rate", K(ret));
  } else if (OB_FAIL(out_throttle_.set_rate(rate))) {
    COMMON_LOG(WARN, "failed to set out_throttle_", K(ret));
  }
  return ret;
}

int ObInOutBandwidthThrottle::get_rate(int64_t &rate)
{
  int ret = OB_SUCCESS;
  rate = 0;
  if (OB_FAIL(out_throttle_.get_rate(rate))) {
    COMMON_LOG(WARN, "failed to get rate", K(ret));
  }
  return ret;
}

int ObInOutBandwidthThrottle::limit_in_and_sleep(
    const int64_t bytes, const int64_t last_active_time, const int64_t max_idle_time)
{
  int ret = OB_SUCCESS;
  int64_t sleep_us = 0;

  if (OB_FAIL(in_throttle_.limit_and_sleep(bytes, last_active_time, max_idle_time, sleep_us))) {
    COMMON_LOG(WARN, "failed to limit in_throttle_", K(ret));
  }

  EVENT_ADD(BANDWIDTH_IN_THROTTLE, bytes);
  EVENT_ADD(BANDWIDTH_IN_SLEEP_US, sleep_us);
  return ret;
}

int ObInOutBandwidthThrottle::limit_out_and_sleep(
    const int64_t bytes, const int64_t last_active_time, const int64_t max_idle_time, int64_t *need_sleep_us)
{
  int ret = OB_SUCCESS;
  int64_t sleep_us = 0;

  if (OB_FAIL(out_throttle_.limit_and_sleep(bytes, last_active_time, max_idle_time, sleep_us))) {
    COMMON_LOG(WARN, "failed to limit out_throttle_", K(ret));
  }
  if (OB_SUCC(ret) && nullptr != need_sleep_us) {
    *need_sleep_us = sleep_us;
  }

  EVENT_ADD(BANDWIDTH_OUT_THROTTLE, bytes);
  EVENT_ADD(BANDWIDTH_OUT_SLEEP_US, sleep_us);
  return ret;
}

void ObInOutBandwidthThrottle::destroy()
{
  in_throttle_.destroy();
  out_throttle_.destroy();
}

char *ltoa10(int64_t val,char *dst, const bool is_signed)
{
  char buffer[65];
  uint64_t uval = (uint64_t) val;

  if (is_signed)
  {
    if (val < 0)
    {
      *dst++ = '-';
      /* Avoid integer overflow in (-val) for LONGLONG_MIN*/
      uval = (uint64_t)0 - uval;
    }
  }

  char *p = &buffer[sizeof(buffer)-1];
  *p = '\0';
  int64_t new_val= (int64_t) (uval / 10);
  *--p = (char)('0'+ (uval - (uint64_t) new_val * 10));
  val = new_val;

  while (val != 0)
  {
    new_val=val/10;
    *--p = (char)('0' + (val-new_val*10));
    val= new_val;
  }
  while ((*dst++ = *p++) != 0) ;
  return dst-1;
}

int long_to_str10(int64_t val,char *dst, const int64_t buf_len, const bool is_signed, int64_t &length)
{
  int ret = OB_SUCCESS;
  length = 0;
  if (OB_ISNULL(dst)
      || buf_len < 2) {//at least one char and '\0'
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Input invalid", KP(dst), K(buf_len), K(ret));
  } else {
    char buffer[65];
    uint64_t uval = (uint64_t) val;

    if (is_signed)
    {
      if (val < 0)
      {
        *dst = '-';
        ++length;
        /* Avoid integer overflow in (-val) for LONGLONG_MIN*/
        uval = (uint64_t)0 - uval;
      }
    }

    char *p = &buffer[sizeof(buffer)-1];
    *p = '\0';
    int64_t new_val= (int64_t) (uval / 10);
    *--p = (char)('0'+ (uval - (uint64_t) new_val * 10));
    val = new_val;

    while (val != 0)
    {
      new_val=val/10;
      *--p = (char)('0' + (val-new_val*10));
      val= new_val;
    }

    while (length < buf_len && (*(dst + length++) = *p++) != 0) ;
    --length;
    if (*(dst + length) != 0) {
      ret = OB_SIZE_OVERFLOW;
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////////////////


const char *replica_type_to_str(const ObReplicaType &type)
{
  const char *str = "";

  switch (type) {
  case REPLICA_TYPE_FULL:
    str = "REPLICA_TYPE_FULL";
    break;
  case REPLICA_TYPE_BACKUP:
    str = "REPLICA_TYPE_BACKUP";
    break;
  case REPLICA_TYPE_LOGONLY:
    str = "REPLICA_TYPE_LOGONLY";
    break;
  case REPLICA_TYPE_READONLY:
    str = "REPLICA_TYPE_READONLY";
    break;
  case REPLICA_TYPE_MEMONLY:
    str = "REPLICA_TYPE_MEMONLY";
    break;
  default:
    str = "REPLICA_TYPE_UNKNOWN";
  }
  return str;
}

bool ez2ob_addr(ObAddr &addr, easy_addr_t& ez)
{
  bool ret = false;
  addr.reset();
  if (AF_INET == ez.family && !(ret = addr.set_ipv4_addr(ntohl(ez.u.addr), ntohs(ez.port)))) {
    LIB_LOG(WARN, "fail to set ipv4 addr", K(addr));
  } else if (AF_INET6 == ez.family && !(ret = addr.set_ipv6_addr(ez.u.addr6, ntohs(ez.port)))) {
    LIB_LOG(WARN, "fail to set ipv6 addr", K(addr));
  } else if (AF_UNIX == ez.family && !(ret = addr.set_unix_addr(ez.u.unix_path))) {
    LIB_LOG(WARN, "fail to set unix addr", K(addr));
  }
  return ret;
}

void get_addr_by_proxy_sessid(const uint64_t session_id, ObAddr &addr)
{
  const int32_t ip = static_cast<int32_t>((session_id >> 32) & 0xFFFFFFFF);
  const int32_t port = static_cast<int32_t>((session_id >> 16) & 0xFFFF);
  IGNORE_RETURN addr.set_ipv4_addr(ip, port);
}

int ob_strtoll(const char *str, char *&endptr, int64_t &res)
{
  int ret = OB_SUCCESS;
  res = 0;
  if (OB_ISNULL(str)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    res = strtoll(str, &endptr, 10);
    if (INT64_MAX == res) {
      ret = OB_SIZE_OVERFLOW;
    }
  }
  return ret;
}

int ob_strtoull(const char *str, char *&endptr, uint64_t &res)
{
  int ret = OB_SUCCESS;
  res = 0;
  int64_t tmp_res = 0;
  if (OB_ISNULL(str)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    tmp_res = strtoull(str, &endptr, 10);
    if (UINT64_MAX == tmp_res) {
      ret = OB_SIZE_OVERFLOW;
    } else {
      res = tmp_res;
    }
  }
  return ret;
}

int ob_atoll(const char *str, int64_t &res)
{
  int ret = OB_SUCCESS;
  char *endptr = NULL;
  int64_t val = 0;
  if (OB_ISNULL(str)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ob_strtoll(str, endptr, val))) {
    LIB_LOG(WARN, "failed to strtoll", K(ret), KCSTRING(str));
  } else if (str == endptr || OB_ISNULL(endptr) || OB_UNLIKELY('\0' != *endptr)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    res = val;
  }
  return ret;
}

struct tm *ob_localtime(const time_t *unix_sec, struct tm *result)
{
  static const int HOURS_IN_DAY = 24;
  static const int MINUTES_IN_HOUR = 60;
  static const int DAYS_FROM_UNIX_TIME = 2472632;
  static const int DAYS_FROM_YEAR = 153;
  static const int MAGIC_UNKONWN_FIRST = 146097;
  static const int MAGIC_UNKONWN_SEC = 1461;
  //use __timezone from glibc/time/tzset.c, default value is -480 for china
  const int32_t tz_minutes = static_cast<int32_t>(__timezone / 60);

//only support time > 1970/1/1 8:0:0
  if (OB_LIKELY(NULL != result) && OB_LIKELY(NULL != unix_sec) && OB_LIKELY(*unix_sec > 0)) {
    result->tm_sec  = static_cast<int>((*unix_sec) % MINUTES_IN_HOUR);
    int tmp_i       = static_cast<int>((*unix_sec) / MINUTES_IN_HOUR) - tz_minutes;
    result->tm_min  = tmp_i % MINUTES_IN_HOUR;
    tmp_i          /= MINUTES_IN_HOUR;
    result->tm_hour = tmp_i % HOURS_IN_DAY;
    result->tm_mday = tmp_i / HOURS_IN_DAY;
    int tmp_a       = result->tm_mday + DAYS_FROM_UNIX_TIME;
    int tmp_b       = (tmp_a * 4 + 3) / MAGIC_UNKONWN_FIRST;
    int tmp_c       = tmp_a - (tmp_b * MAGIC_UNKONWN_FIRST) / 4;
    int tmp_d       = ((tmp_c * 4 + 3) / MAGIC_UNKONWN_SEC);
    int tmp_e       = tmp_c - (tmp_d * MAGIC_UNKONWN_SEC) / 4;
    int tmp_m       = (5 * tmp_e + 2) / DAYS_FROM_YEAR;
    result->tm_mday = tmp_e + 1 - (DAYS_FROM_YEAR * tmp_m + 2) / 5;
    result->tm_mon  = tmp_m + 2 - (tmp_m / 10) * 12;
    result->tm_year = tmp_b * 100 + tmp_d - 6700 + (tmp_m / 10);
  }
  return result;
}


void ob_fast_localtime(time_t &cached_unix_sec, struct tm &cached_localtime,
    const time_t &input_unix_sec, struct tm *output_localtime)
{
  if (OB_LIKELY(NULL != output_localtime)) {
    if (cached_unix_sec == input_unix_sec) {
      *output_localtime = cached_localtime;
    } else if (OB_UNLIKELY(0 == cached_unix_sec)) {//init
      cached_unix_sec = input_unix_sec;
      localtime_r(&input_unix_sec, output_localtime);
      cached_localtime = *output_localtime;;
    } else {
      cached_unix_sec = input_unix_sec;
      cached_localtime = *ob_localtime(&input_unix_sec, output_localtime);
    }
  }
}

int is_dir_empty(const char *dirname, bool &is_empty)
{
  int ret = OB_SUCCESS;
  if (NULL == dirname) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    is_empty = true;
    DIR *dir = opendir(dirname);
    if (NULL == dir) {
      if (ENOENT == errno) {
        is_empty = true;
      } else {
        ret = OB_IO_ERROR;
        LIB_LOG(ERROR, "opendir failed", K(ret), K(errno), KP(dirname));
      }
    } else {
      while (OB_SUCC(ret)) {
        struct dirent entry;
        struct dirent *pentry = &entry;
        memset(&entry, 0, sizeof(struct dirent));
        if (0 != readdir_r(dir, pentry, &pentry)) {
          ret = OB_IO_ERROR;
          LIB_LOG(WARN, "readdir_r failed", K(ret), K(errno), KP(dirname));
        } else if (NULL == pentry) {
          ret = OB_ITER_END;
        } else if (strcmp(".", pentry->d_name) == 0
            || strcmp("..", pentry->d_name) == 0) {
          continue;
        } else {
          is_empty = false;
          break;
        }
      }

      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }

      if (0 != closedir(dir)) {
        ret = OB_IO_ERROR;
        LIB_LOG(ERROR, "closedir failed", K(ret), K(errno), KP(dirname));
      } else {
        dir = NULL;
      }
    }
  }
  return ret;
}

static int64_t get_cpu_cache_size(int sysconf_name, const char *sysfs_path, int64_t default_value)
{
  int64_t cache_size = sysconf(sysconf_name);
  if (OB_UNLIKELY(cache_size <= 0)) {
    FILE *file = nullptr;
    file = fopen(sysfs_path, "r");
    if (file) {
      fscanf(file, "%ld", &cache_size);
      fclose(file);
      cache_size *= 1024;
    }
    if (OB_UNLIKELY(nullptr == file || cache_size <= 0)) {
      int ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "failed to read cpu cache size in file", KP(file), K(cache_size));
      cache_size = default_value;
    }
  }
  return cache_size;
}

int64_t get_level1_dcache_size()
{
  const char *path = "/sys/devices/system/cpu/cpu0/cache/index0/size";
  static int64_t l1_dcache_size = get_cpu_cache_size(_SC_LEVEL1_DCACHE_SIZE, path, 32768/*default L1 dcache size : 32K*/);
  return l1_dcache_size;
}

int64_t get_level1_icache_size()
{
  const char *path = "/sys/devices/system/cpu/cpu0/cache/index1/size";
  static int64_t l1_icache_size = get_cpu_cache_size(_SC_LEVEL1_ICACHE_SIZE, path, 32768/*default L1 icache size : 32K*/);
  return l1_icache_size;
}

int64_t get_level2_cache_size()
{
  const char *path = "/sys/devices/system/cpu/cpu0/cache/index2/size";
  static int64_t l2_cache_size = get_cpu_cache_size(_SC_LEVEL2_CACHE_SIZE, path, 524288/*default L2 cache size : 512K*/);
  return l2_cache_size;
}

int64_t get_level3_cache_size()
{
  const char *path = "/sys/devices/system/cpu/cpu0/cache/index3/size";
  static int64_t l3_cache_size = get_cpu_cache_size(_SC_LEVEL3_CACHE_SIZE, path, 8388608/*default L3 cache size : 8192K*/);
  return l3_cache_size;
}

int extract_cert_expired_time(const char* cert, const int64_t cert_len, int64_t &expired_time)
{
  int ret = OB_SUCCESS;
  STACK_OF(X509_INFO)  *chain = NULL;
  BIO *cbio = NULL;
  if (OB_ISNULL(cert)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("public cert from kms is null!", K(ret));
  } else if (OB_ISNULL(cbio = BIO_new_mem_buf((void*)cert, cert_len))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("BIO_new_mem_buf failed", K(ret));
  } else if (OB_ISNULL(chain = PEM_X509_INFO_read_bio(cbio, NULL, NULL, NULL))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("PEM_X509_INFO_read_bio failed", K(ret));
  } else {
    ASN1_TIME *notAfter = NULL;
    X509_INFO *x509_info = NULL;
    if (OB_ISNULL(x509_info = sk_X509_INFO_value(chain, 0))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("get app cert failed!", K(ret));
    } else if (OB_ISNULL((notAfter = X509_get_notAfter(x509_info->x509)))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("X509_get_notAfter failed",K(ret));
    } else {
      struct tm tm1;
      memset (&tm1, 0, sizeof (tm1));
      tm1.tm_year = (notAfter->data[ 0] - '0') * 10 + (notAfter->data[ 1] - '0') + 100;
      tm1.tm_mon  = (notAfter->data[ 2] - '0') * 10 + (notAfter->data[ 3] - '0') - 1;
      tm1.tm_mday = (notAfter->data[ 4] - '0') * 10 + (notAfter->data[ 5] - '0');
      tm1.tm_hour = (notAfter->data[ 6] - '0') * 10 + (notAfter->data[ 7] - '0');
      tm1.tm_min  = (notAfter->data[ 8] - '0') * 10 + (notAfter->data[ 9] - '0');
      tm1.tm_sec  = (notAfter->data[10] - '0') * 10 + (notAfter->data[11] - '0');
      time_t expired_time_t = mktime(&tm1);
      expired_time_t += (int)(mktime(localtime(&expired_time_t)) - mktime(gmtime(&expired_time_t)));
      expired_time = expired_time_t * 1000000;
    }
  }
  if (NULL != cbio) {
    BIO_free(cbio);
  }
  if (NULL != chain) {
    sk_X509_INFO_pop_free(chain, X509_INFO_free);
  }
  return ret;
}


} // end namespace common
} // end namespace oceanbase
