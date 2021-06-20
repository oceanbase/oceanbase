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

#ifndef OCEANBASE_CHARSET_H_
#define OCEANBASE_CHARSET_H_

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/charset/ob_ctype.h"

namespace oceanbase {
namespace common {

enum ObCharsetType {
  CHARSET_INVALID = 0,
  CHARSET_BINARY = 1,
  CHARSET_UTF8MB4 = 2,
  CHARSET_MAX,
};

enum ObCollationType {
  CS_TYPE_INVALID = 0,
  CS_TYPE_UTF8MB4_GENERAL_CI = 45,
  CS_TYPE_UTF8MB4_BIN = 46,
  CS_TYPE_BINARY = 63,
  CS_TYPE_COLLATION_FREE = 100,
  CS_TYPE_MAX,
};

// oracle CHARSET ID
// https://docs.oracle.com/cd/B19306_01/server.102/b14200/functions095.htm
// https://blog.csdn.net/guizicjj/article/details/4434088
enum ObNlsCharsetId {
  CHARSET_INVALID_ID = 0,
  CHARSET_AL32UTF8_ID = 873,
  CHARSET_MAX_ID,
};

/*
Coercibility Meaning Example
0 Explicit collation Value with COLLATE clause
1 No collation Concatenation of strings with different collations
2 Implicit collation Column value, stored routine parameter or local variable
3 System constant USER() return value
4 Coercible Literal string
5 Ignorable NULL or an expression derived from NULL
*/
enum ObCollationLevel {
  CS_LEVEL_EXPLICIT = 0,
  CS_LEVEL_NONE = 1,
  CS_LEVEL_IMPLICIT = 2,
  CS_LEVEL_SYSCONST = 3,
  CS_LEVEL_COERCIBLE = 4,
  CS_LEVEL_NUMERIC = 5,
  CS_LEVEL_IGNORABLE = 6,
  CS_LEVEL_INVALID,  // here we didn't define CS_LEVEL_INVALID as 0,
                     // since 0 is a valid value for CS_LEVEL_EXPLICIT in mysql 5.6.
                     // fortunately we didn't need to use it to define array like charset_arr,
                     // and we didn't persist it on storage.
};

struct ObCharsetWrapper {
  ObCharsetType charset_;
  const char* description_;
  ObCollationType default_collation_;
  int64_t maxlen_;
};

struct ObCollationWrapper {
  ObCollationType collation_;
  ObCharsetType charset_;
  int64_t id_;
  bool default_;
  bool compiled_;
  int64_t sortlen_;
};

const char* ob_collation_type_str(ObCollationType collation_type);

class ObCharset {
private:
  ObCharset(){};
  virtual ~ObCharset(){};

public:
  static const int64_t CHARSET_WRAPPER_COUNT = 2;
  static const int64_t COLLATION_WRAPPER_COUNT = 3;

  static double strntod(const char* str, size_t str_len, char** endptr, int* err);
  static int64_t strntoll(const char* str, size_t str_len, int base, char** end_ptr, int* err);
  static uint64_t strntoull(const char* str, size_t str_len, int base, char** end_ptr, int* err);
  static int64_t strntoll(const char* str, size_t str_len, int base, int* err);
  static uint64_t strntoull(const char* str, size_t str_len, int base, int* err);
  static uint64_t strntoullrnd(const char* str, size_t str_len, int unsigned_fl, char** endptr, int* err);
  static char* lltostr(int64_t val, char* dst, int radix, int upcase);
  static size_t scan_str(const char* str, const char* end, int sq);
  static uint32_t instr(
      ObCollationType collation_type, const char* str1, int64_t str1_len, const char* str2, int64_t str2_len);
  static uint32_t locate(ObCollationType collation_type, const char* str1, int64_t str1_len, const char* str2,
      int64_t str2_len, int64_t pos);
  static int well_formed_len(
      ObCollationType collation_type, const char* str, int64_t str_len, int64_t& well_formed_len);
  static int well_formed_len(ObCollationType collation_type, const char* str, int64_t str_len, int64_t& well_formed_len,
      int32_t& well_formed_error);
  static int strcmp(
      ObCollationType collation_type, const char* str1, int64_t str1_len, const char* str2, int64_t str2_len);

  static int strcmpsp(ObCollationType collation_type, const char* str1, int64_t str1_len, const char* str2,
      int64_t str2_len, bool cmp_endspace);

  static size_t casedn(const ObCollationType collation_type, char* src, size_t src_len, char* dest, size_t dest_len);
  static size_t caseup(const ObCollationType collation_type, char* src, size_t src_len, char* dest, size_t dest_len);
  static size_t sortkey(ObCollationType collation_type, const char* str, int64_t str_len, char* key, int64_t key_len,
      bool& is_valid_unicode);
  static uint64_t hash(ObCollationType collation_type, const char* str, int64_t str_len, uint64_t seed,
      const bool calc_end_space, hash_algo hash_algo);
  static uint64_t hash(
      ObCollationType collation_type, const char* str, int64_t str_len, uint64_t seed, hash_algo hash_algo);

  static int like_range(ObCollationType collation_type, const ObString& like_str, char escape, char* min_str,
      size_t* min_str_len, char* max_str, size_t* max_str_len);
  static size_t strlen_char(ObCollationType collation_type, const char* str, int64_t str_len);
  static size_t strlen_byte_no_sp(ObCollationType collation_type, const char* str, int64_t str_len);
  static size_t charpos(ObCollationType collation_type, const char* str, const int64_t str_len, const int64_t length);
  static size_t max_bytes_charpos(ObCollationType collation_type, const char* str, const int64_t str_len,
      const int64_t max_bytes, int64_t& char_len);
  // match like pattern
  static bool wildcmp(ObCollationType collation_type, const ObString& str, const ObString& wildstr, int32_t escape,
      int32_t w_one, int32_t w_many);
  static int mb_wc(ObCollationType collation_type, const ObString& mb, int32_t& wc);
  static int mb_wc(ObCollationType collation_type, const char* mb, const int64_t mb_size, int32_t& length, int32_t& wc);
  static int wc_mb(ObCollationType collation_type, int32_t wc, char* buff, int32_t buff_len, int32_t& length);
  static int display_len(ObCollationType collation_type, const ObString& mb, int64_t& width);
  static int max_display_width_charpos(ObCollationType collation_type, const char* mb, const int64_t mb_size,
      const int64_t max_width, int64_t& char_pos, int64_t* total_widta = NULL);
  static const char* charset_name(ObCharsetType charset_type);
  static const char* charset_name(ObCollationType coll_type);
  static const char* collation_name(ObCollationType collation_type);
  static int collation_name(ObCollationType coll_type, ObString& coll_name);
  static const char* collation_level(const ObCollationLevel cs_level);
  static ObCharsetType charset_type(const char* cs_name);
  static ObCollationType collation_type(const char* cs_name);
  static ObCharsetType charset_type(const ObString& cs_name);
  static ObCharsetType charset_type_by_name_oracle(const ObString& cs_name);
  static ObCollationType collation_type(const ObString& cs_name);
  static bool is_valid_collation(ObCharsetType charset_type, ObCollationType coll_type);
  static bool is_valid_collation(int64_t coll_type_int);
  static bool is_valid_charset(int64_t cs_type_int);
  static ObCharsetType charset_type_by_coll(ObCollationType coll_type);
  static int charset_name_by_coll(const ObString& coll_name, common::ObString& cs_name);
  static int charset_name_by_coll(ObCollationType coll_type, common::ObString& cs_name);
  static int calc_collation(const ObCollationLevel level1, const ObCollationType type1, const ObCollationLevel level2,
      const ObCollationType type2, ObCollationLevel& res_level, ObCollationType& res_type);
  static int result_collation(const ObCollationLevel level1, const ObCollationType type1, const ObCollationLevel level2,
      const ObCollationType type2, ObCollationLevel& res_level, ObCollationType& res_type);
  static int aggregate_collation(const ObCollationLevel level1, const ObCollationType type1,
      const ObCollationLevel level2, const ObCollationType type2, ObCollationLevel& res_level,
      ObCollationType& res_type);
  static bool is_bin_sort(ObCollationType collation_type);
  static ObCollationType get_bin_collation(const ObCharsetType charset_type);
  static int first_valid_char(
      const ObCollationType collation_type, const char* buf, const int64_t buf_size, int64_t& char_len);

  static int last_valid_char(
      const ObCollationType collation_type, const char* buf, const int64_t buf_size, int64_t& char_len);

  static ObCharsetType get_default_charset();
  static ObCollationType get_default_collation(ObCharsetType charset_type);
  static ObCollationType get_default_collation_oracle(ObCharsetType charset_type);
  static ObCollationType get_default_collation_by_mode(ObCharsetType charset_type, bool is_oracle_mode);
  static int get_default_collation(ObCharsetType charset_type, ObCollationType& coll_type);
  static int get_default_collation(const ObCollationType& in, ObCollationType& out);
  static ObCollationType get_system_collation();
  static bool is_default_collation(ObCollationType type);
  static bool is_default_collation(ObCharsetType charset_type, ObCollationType coll_type);
  static const char* get_default_charset_name()
  {
    return ObCharset::charset_name(ObCharset::get_default_charset());
  }
  static const char* get_default_collation_name()
  {
    return ObCharset::collation_name(ObCharset::get_default_collation(ObCharset::get_default_charset()));
  }
  static void get_charset_wrap_arr(const ObCharsetWrapper*& charset_wrap_arr, int64_t& charset_wrap_arr_len)
  {
    charset_wrap_arr = charset_wrap_arr_;
    charset_wrap_arr_len = CHARSET_WRAPPER_COUNT;
  }
  static void get_collation_wrap_arr(const ObCollationWrapper*& collation_wrap_arr, int64_t& collation_wrap_arr_len)
  {
    collation_wrap_arr = collation_wrap_arr_;
    collation_wrap_arr_len = COLLATION_WRAPPER_COUNT;
  }
  static int check_and_fill_info(ObCharsetType& charset_type, ObCollationType& collation_type);

  static int strcmp(const ObCollationType collation_type, const ObString& l_str, const ObString& r_str);
  // when invoke this, if ObString a = "134";  this func will core; so avoid passing src as a style
  static size_t casedn(const ObCollationType collation_type, ObString& src);
  static size_t caseup(const ObCollationType collation_type, ObString& src);
  static bool case_insensitive_equal(
      const ObString& one, const ObString& another, const ObCollationType& collation_type = CS_TYPE_UTF8MB4_GENERAL_CI);
  static bool case_sensitive_equal(const ObString& one, const ObString& another);
  static bool case_compat_mode_equal(const ObString& one, const ObString& another);
  static uint64_t hash(
      const ObCollationType collation_type, const ObString& str, uint64_t seed = 0, hash_algo hash_algo = NULL);
  static bool case_mode_equal(const ObNameCaseMode mode, const ObString& one, const ObString& another);
  static bool is_space(const ObCollationType collation_type, char c);
  static bool is_graph(const ObCollationType collation_type, char c);
  static bool usemb(const ObCollationType collation_type);
  static int is_mbchar(const ObCollationType collation_type, const char* str, const char* end);
  static const ObCharsetInfo* get_charset(const ObCollationType coll_type);
  static int get_mbmaxlen_by_coll(const ObCollationType collation_type, int64_t& mbmaxlen);
  static int get_mbminlen_by_coll(const ObCollationType collation_type, int64_t& mbminlen);

  static int fit_string(const ObCollationType collation_type, const char* str, const int64_t str_len,
      const int64_t len_limit_in_byte, int64_t& byte_num, int64_t& char_num);

  static int get_aggregate_len_unit(const ObCollationType collation_type, bool& len_in_byte);

  static int charset_convert(const ObCollationType from_type, const char* from_str, const uint32_t from_len,
      const ObCollationType to_type, char* to_str, uint32_t to_len, uint32_t& result_len);
  enum CONVERT_FLAG : int64_t {
    COPY_STRING_ON_SAME_CHARSET = 1 << 0,
    REPLACE_UNKNOWN_CHARACTER = 1 << 1,
  };
  static int charset_convert(ObIAllocator& alloc, const ObString& in, const ObCollationType src_cs_type,
      const ObCollationType dst_cs_type, ObString& out, int64_t convert_flag = 0);

  static int whitespace_padding(ObIAllocator& allocator, const ObCollationType coll_type, const ObString& input,
      const int64_t pad_whitespace_length, ObString& result);

  static bool is_cs_nonascii(ObCollationType collation_type);
  static bool is_cjk_charset(ObCollationType collation_type);
  static bool is_valid_connection_collation(ObCollationType collation_type);

public:
  static const int64_t VALID_COLLATION_TYPES = 3;

private:
  static bool is_argument_valid(const ObCharsetInfo* charset_info, const char* str, int64_t str_len);
  static bool is_argument_valid(
      const ObCollationType collation_type, const char* str1, int64_t str_len1, const char* str2, int64_t str_len2);

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObCharset);

private:
  static const ObCharsetWrapper charset_wrap_arr_[CHARSET_WRAPPER_COUNT];
  static const ObCollationWrapper collation_wrap_arr_[COLLATION_WRAPPER_COUNT];
  static void* charset_arr[CS_TYPE_MAX];  // CHARSET_INFO *
  static ObCharsetType default_charset_type_;
  static ObCollationType default_collation_type_;
};

class ObCharsetUtils {
public:
  static int init(ObIAllocator& allocator);
  static ObString get_const_str(ObCollationType coll_type, int ascii)
  {
    ObString result;
    if (ascii >= 0 && ascii <= INT8_MAX && ObCharset::is_valid_collation(coll_type)) {
      result = const_str_for_ascii_[ObCharset::charset_type_by_coll(coll_type)][ascii];
    }
    return result;
  }
  template <typename foreach_char_func>
  static int foreach_char(common::ObString& str, common::ObCollationType collation_type, foreach_char_func& func)
  {
    int ret = common::OB_SUCCESS;
    int32_t wchar = 0;
    int32_t length = 0;
    ObString encoding;

    for (common::ObString temp_str = str; OB_SUCC(ret) && !temp_str.empty(); temp_str += length) {
      if (OB_FAIL(ObCharset::mb_wc(collation_type, temp_str.ptr(), temp_str.length(), length, wchar))) {
        COMMON_LOG(WARN, "fail to call mb_wc", K(ret), KPHEX(temp_str.ptr(), temp_str.length()));
      } else {
        encoding.assign_ptr(temp_str.ptr(), length);
        if (OB_FAIL(func(encoding, wchar))) {
          COMMON_LOG(
              WARN, "fail to call func", K(ret), K(encoding), KPHEX(encoding.ptr(), encoding.length()), K(wchar));
        }
        COMMON_LOG(DEBUG, "foreach char", K(encoding), KPHEX(encoding.ptr(), encoding.length()), K(wchar));
      }
    }
    return ret;
  }

private:
  static ObString const_str_for_ascii_[CHARSET_MAX][INT8_MAX + 1];
};

class ObStringScanner {
public:
  ObStringScanner(const ObString& str, common::ObCollationType collation_type)
      : str_(str), collation_type_(collation_type)
  {}
  int next_character(ObString& encoding, int32_t& wchar);
  TO_STRING_KV(K_(str), K_(collation_type));

private:
  const ObString& str_;
  common::ObCollationType collation_type_;
};

class ObCharSetString {
public:
  ObCharSetString(ObString str, ObCollationType cs_type) : str_(str), cs_type_(cs_type)
  {}
  ObString& get_string()
  {
    return str_;
  }
  int to_cs_type(ObIAllocator& allocator, ObCollationType target_cs_type)
  {
    return ObCharset::charset_convert(allocator, str_, cs_type_, target_cs_type, str_);
  }

protected:
  ObString str_;
  ObCollationType cs_type_;
};

// to_string adapter
template <>
inline int databuff_print_obj(char* buf, const int64_t buf_len, int64_t& pos, const ObCollationType& t)
{
  return databuff_printf(buf, buf_len, pos, "\"%s\"", ObCharset::collation_name(t));
}
template <>
inline int databuff_print_key_obj(
    char* buf, const int64_t buf_len, int64_t& pos, const char* key, const bool with_comma, const ObCollationType& t)
{
  return databuff_printf(buf, buf_len, pos, WITH_COMMA("%s:\"%s\""), key, ObCharset::collation_name(t));
}
}  // namespace common
}  // namespace oceanbase

#endif /* OCEANBASE_CHARSET_H_ */
