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

#ifndef OCEANBASE_LIB_OBMYSQL_OB_CTYPE_
#define OCEANBASE_LIB_OBMYSQL_OB_CTYPE_

//#include "lib/charset/ob_mysql_global.h"
#include "lib/hash_func/wyhash.h"

#ifdef __cplusplus
extern "C" {
#endif

#define OB_CS_SUCC(value) (OB_LIKELY(OB_CS_SUCCESS == (value)))

#define OB_UTF8MB4 "utf8mb4"

#define OB_UTF8MB4_GENERAL_CI OB_UTF8MB4 "_general_ci"
#define OB_UTF8MB4_GENERAL_CS OB_UTF8MB4 "_general_cs"
#define OB_UTF8MB4_BIN OB_UTF8MB4 "_bin"
#define OB_UTF8MB4_UNICODE_CI OB_UTF8MB4 "_unicode_ci"

#define OB_UTF16 "utf16"

#define OB_UTF16_GENERAL_CI OB_UTF16 "_general_ci"
#define OB_UTF16_BIN OB_UTF16 "_bin"
#define OB_UTF16_UNICODE_CI OB_UTF16 "_unicode_ci"

/* My charsets_list flags */
#define OB_CS_COMPILED 1               /* compiled-in sets               */
#define OB_CS_CONFIG 2                 /* sets that have a *.conf file   */
#define OB_CS_INDEX 4                  /* sets listed in the Index file  */
#define OB_CS_LOADED 8                 /* sets that are currently loaded */
#define OB_CS_BINSORT 16               /* if binary sort order           */
#define OB_CS_PRIMARY 32               /* if primary collation           */
#define OB_CS_STRNXFRM 64              /* if strnxfrm is used for sort   */
#define OB_CS_UNICODE 128              /* is a charset is BMP Unicode    */
#define OB_CS_READY 256                /* if a charset is initialized    */
#define OB_CS_AVAILABLE 512            /* If either compiled-in or loaded*/
#define OB_CS_CSSORT 1024              /* if case sensitive sort order   */
#define OB_CS_HIDDEN 2048              /* don't display in SHOW          */
#define OB_CS_PUREASCII 4096           /* if a charset is pure ascii     */
#define OB_CS_NONASCII 8192            /* if not ASCII-compatible        */
#define OB_CS_UNICODE_SUPPLEMENT 16384 /* Non-BMP Unicode characters */
#define OB_CS_LOWER_SORT 32768         /* If use lower case as weight   */
#define OB_CHARSET_UNDEFINED 0

#define OB_SEQ_INTTAIL 1
#define OB_SEQ_SPACES 2

/* wm_wc and wc_mb return codes */
#define OB_CS_SUCCESS 0
#define OB_CS_ERR_ILLEGAL_SEQUENCE -1 /* Wrong by sequence: wb_wc                   */
#define OB_CS_ERR_ILLEGAL_UNICODE -2  /* Cannot encode Unicode to charset: wc_mb    */
#define OB_CS_ERR_NUM_OUT_OF_RANGE -3
#define OB_CS_ERR_TOOSMALL -101  /* Need at least one byte:    wc_mb and mb_wc */
#define OB_CS_ERR_TOOSMALL2 -102 /* Need at least two bytes:   wc_mb and mb_wc */
#define OB_CS_ERR_TOOSMALL3 -103 /* Need at least three bytes: wc_mb and mb_wc */
/* These following three are currently not really used */
#define OB_CS_ERR_TOOSMALL4 -104 /* Need at least 4 bytes: wc_mb and mb_wc */
#define OB_CS_ERR_TOOSMALL5 -105 /* Need at least 5 bytes: wc_mb and mb_wc */
#define OB_CS_ERR_TOOSMALL6 -106 /* Need at least 6 bytes: wc_mb and mb_wc */
/* A helper macros for "need at least n bytes" */
#define OB_CS_ERR_TOOSMALLN(n) (-100 - (n))

#define _MY_U 01    /* Upper case */
#define _MY_L 02    /* Lower case */
#define _MY_NMR 04  /* Numeral (digit) */
#define _MY_SPC 010 /* Spacing character */
#define _MY_PNT 020 /* Punctuation */
#define _MY_CTR 040 /* Control character */
#define _MY_B 0100  /* Blank */
#define _MY_X 0200  /* heXadecimal digit */

#define ob_isascii(c) (0 == ((c) & ~0177))
#define ob_toascii(c) ((c)&0177)
#define ob_tocntrl(c) ((c)&31)
#define ob_toprint(c) ((c) | 64)
#define ob_toupper(s, c) (char)((s)->to_upper[(uchar)(c)])
#define ob_tolower(s, c) (char)((s)->to_lower[(uchar)(c)])
#define ob_isalpha(s, c) ((s)->ctype != NULL ? ((s)->ctype + 1)[(uchar)(c)] & (_MY_U | _MY_L) : 0)
#define ob_isupper(s, c) ((s)->ctype != NULL ? ((s)->ctype + 1)[(uchar)(c)] & _MY_U : 0)
#define ob_islower(s, c) ((s)->ctype != NULL ? ((s)->ctype + 1)[(uchar)(c)] & _MY_L : 0)
#define ob_isdigit(s, c) ((s)->ctype != NULL ? ((s)->ctype + 1)[(uchar)(c)] & _MY_NMR : 0)
#define ob_isxdigit(s, c) ((s)->ctype != NULL ? ((s)->ctype + 1)[(uchar)(c)] & _MY_X : 0)
#define ob_isalnum(s, c) ((s)->ctype != NULL ? ((s)->ctype + 1)[(uchar)(c)] & (_MY_U | _MY_L | _MY_NMR) : 0)
#define ob_isspace(s, c) ((s)->ctype != NULL ? ((s)->ctype + 1)[(uchar)(c)] & _MY_SPC : 0)
#define ob_ispunct(s, c) ((s)->ctype != NULL ? ((s)->ctype + 1)[(uchar)(c)] & _MY_PNT : 0)
#define ob_isprint(s, c) \
  ((s)->ctype != NULL ? ((s)->ctype + 1)[(uchar)(c)] & (_MY_PNT | _MY_U | _MY_L | _MY_NMR | _MY_B) : 0)
#define ob_isgraph(s, c) ((s)->ctype != NULL ? ((s)->ctype + 1)[(uchar)(c)] & (_MY_PNT | _MY_U | _MY_L | _MY_NMR) : 0)
#define ob_iscntrl(s, c) ((s)->ctype != NULL ? ((s)->ctype + 1)[(uchar)(c)] & _MY_CTR : 0)

#define ob_charset_assert(condition)  \
  if (!(condition)) {                 \
    right_to_die_or_duty_to_live_c(); \
  }

struct ObCharsetInfo;

#define ob_wc_t unsigned long

#define OB_CS_REPLACEMENT_CHARACTER 0xFFFD

#define MY_ERRNO_EDOM 33
#define MY_ERRNO_ERANGE 34

typedef int (*ob_charset_conv_mb_wc)(const unsigned char* str, const unsigned char* end, ob_wc_t* wchar);
typedef int (*ob_charset_conv_wc_mb)(ob_wc_t wchar, unsigned char* str, unsigned char* end);
typedef size_t (*ob_charset_conv_case)(
    const struct ObCharsetInfo* cs, char* src, size_t src_len, char* dst, size_t dst_len);

typedef struct {
  unsigned int beg;
  unsigned int end;
  unsigned int mb_len;
} ob_match_info;

typedef struct ObUnicasePages {
  uint32_t** upper_pages;
  uint32_t** lower_pages;
  uint32_t** sort_pages;
} ObUnicasePages;

typedef struct ObCharsetHandler {
  // ismbchar()  - detects whether the given string is a multi-byte sequence
  uint32_t (*ismbchar)(const char* str, size_t len);
  // numchars()  - returns number of characters in the given string, e.g. in SQL function CHAR_LENGTH().
  size_t (*numchars)(const struct ObCharsetInfo* cs, const char* str, size_t len);
  // charpos()   - calculates the offset of the given position in the string.
  //               Used in SQL functions LEFT(), RIGHT(), SUBSTRING(),
  size_t (*charpos)(const struct ObCharsetInfo* cs, const char* str, size_t len, size_t pos);

  // max_bytes_charpos()   - calculates the offset of the given byte position in the string.
  size_t (*max_bytes_charpos)(
      const struct ObCharsetInfo* cs, const char* str, size_t str_len, size_t max_bytes, size_t* char_len);
  // well_formed_len()
  //             - returns length of a given multi-byte string in bytes
  //               Used in INSERTs to shorten the given string so it
  //               a) is "well formed" according to the given character set
  //               b) can fit into the given data type
  size_t (*well_formed_len)(const char* str, size_t len, size_t nchars, int* error);

  // lengthsp()  - returns the length of the given string without trailing spaces.
  size_t (*lengthsp)(const char* str, size_t len);

  // mb_wc       - converts the left multi-byte sequence into its Unicode code.
  ob_charset_conv_mb_wc mb_wc;
  // wc_mb       - converts the given Unicode code into multi-byte sequence.
  ob_charset_conv_wc_mb wc_mb;
  int (*ctype)(const struct ObCharsetInfo* cs, int* ctype, const unsigned char* s, const unsigned char* e);

  // caseup      - converts the given string to lowercase using length
  ob_charset_conv_case caseup;
  // casedn      - converts the given string to lowercase using length
  ob_charset_conv_case casedn;
  // fill()     - writes the given Unicode value into the given string
  //              with the given length. Used to pad the string, usually
  //              with space character, according to the given charset.
  void (*fill)(const struct ObCharsetInfo*, char* str, size_t len, int fill);
  // String-to-number conversion routines
  int64_t (*strntoll)(const char* str, size_t len, int base, char** endptr, int* err);
  uint64_t (*strntoull)(const char* str, size_t len, int base, char** endptr, int* err);
  double (*strntod)(char* str, size_t len, char** end, int* err);
  uint64_t (*strntoull10rnd)(const char* str, size_t len, int unsigned_flag, char** endptr, int* error);
  // scan()    - to skip leading spaces in the given string.
  //             Used when a string value is inserted into a numeric field.
  size_t (*scan)(const char* b, const char* e, int sq);
} ObCharsetHandler;

// OB_COLLATION_HANDLER
// ====================
// strnncoll()   - compares two strings according to the given collation
// strnncollsp() - like the above but ignores trailing spaces for PAD SPACE
//                 collations. For NO PAD collations, identical to strnncoll.
// strnxfrm()    - makes a sort key suitable for memcmp() corresponding
//                 to the given string
// like_range()  - creates a LIKE range, for optimizer
// wildcmp()     - wildcard comparison, for LIKE
// strcasecmp()  - 0-terminated string comparison
// instr()       - finds the first substring appearance in the string
// hash_sort()   - calculates hash value taking into account
//                 the collation rules, e.g. case-insensitivity,
//                 accent sensitivity, etc.

static const int HASH_BUFFER_LENGTH = 128;

typedef uint64_t (*hash_algo)(const void* input, uint64_t length, uint64_t seed);

typedef struct ObCollationHandler {
  // strnncoll()   - compares two strings according to the given collation
  int (*strnncoll)(const struct ObCharsetInfo* cs, const unsigned char* src, size_t src_len, const unsigned char* dst,
      size_t dst_len);
  // strnncollsp() - like the above but ignores trailing spaces for PAD SPACE
  //                 collations. For NO PAD collations, identical to strnncoll.
  int (*strnncollsp)(const struct ObCharsetInfo* cs, const unsigned char* src, size_t src_len, const unsigned char* dst,
      size_t dst_len);
  // strnxfrm()    - makes a sort key suitable for memcmp() corresponding
  //                 to the given string
  size_t (*strnxfrm)(const struct ObCharsetInfo*, unsigned char* dst, size_t dst_len, uint32_t nweights,
      const unsigned char* src, size_t srclen, int* is_valid_unicode);

  // like_range()  - creates a LIKE range, for optimizer
  int (*like_range)(const struct ObCharsetInfo* cs, const char* str, size_t str_len, int w_prefix, int w_one,
      int w_many, size_t res_length, char* min_str, char* max_str, size_t* min_len, size_t* max_len);
  // wildcmp()     - wildcard comparison, for LIKE
  int (*wildcmp)(const struct ObCharsetInfo* cs, const char* str, const char* strend, const char* wildstr,
      const char* wildend, int escape, int w_one, int w_many);

  // instr()       - finds the first substring appearance in the string
  uint32_t (*instr)(const struct ObCharsetInfo* cs, const char* base, size_t base_len, const char* str, size_t str_len,
      ob_match_info* match, uint32_t nmatch);

  // hash_sort()   - calculates hash value taking into account
  //                 the collation rules, e.g. case-insensitivity,
  //                 accent sensitivity, etc.
  void (*hash_sort)(const struct ObCharsetInfo* cs, const unsigned char* s, size_t slen, uint64_t* nr1, uint64_t* nr2,
      const int calc_end_space, hash_algo hash_algo);
} ObCollationHandler;

typedef struct ObCharsetInfo {
  uint32_t number;
  uint32_t primary_number;
  uint32_t binary_number;
  uint32_t state;
  const char* csname;
  const char* name;
  const char* comment;
  const char* tailoring;
  unsigned char* ctype;
  unsigned char* to_lower;
  unsigned char* to_upper;
  unsigned char* sort_order;
  ObUnicasePages* caseinfo;
  unsigned char* state_map;
  unsigned char* ident_map;
  uint32_t strxfrm_multiply;
  uint32_t caseup_multiply;
  uint32_t casedn_multiply;
  uint32_t mbminlen;
  uint32_t mbmaxlen;
  uint32_t min_sort_char;
  uint32_t max_sort_char; /* For LIKE optimization */
  unsigned char pad_char;
  int escape_with_backslash_is_dangerous;
  unsigned char levels_for_compare;
  unsigned char levels_for_order;

  ObCharsetHandler* cset;
  ObCollationHandler* coll;

} ObCharsetInfo;

typedef struct ob_uni_ctype {
  unsigned char pctype;
  unsigned char* ctype;
} ObUniCtype;

extern ObUniCtype ob_uni_ctype[256];

extern ObCharsetInfo ob_charset_bin;
extern ObCharsetInfo ob_charset_utf8mb4_bin;
extern ObCharsetInfo ob_charset_utf8mb4_general_ci;
extern ObCharsetHandler ob_charset_utf8mb4_handler;

//=============================================================================

void ob_fill_8bit(const ObCharsetInfo* cs, char* str, size_t len, int fill);
int64_t ob_strntoll_8bit(const char* str, size_t len, int base, char** e, int* err);
uint64_t ob_strntoull_8bit(const char* str, size_t len, int base, char** e, int* err);
double ob_strntod_8bit(char* s, size_t len, char** e, int* err);
uint64_t ob_strntoull10rnd_8bit(const char* str, size_t len, int unsigned_fl, char** endptr, int* error);
size_t ob_scan_8bit(const char* str, const char* end, int sq);

//======================================================================

/* For 8-bit character set */
int ob_like_range_simple(const ObCharsetInfo* cs, const char* str, size_t str_len, int escape, int w_one, int w_many,
    size_t res_length, char* min_str, char* max_str, size_t* min_length, size_t* max_length);

int64_t ob_strntoll(const char* str, size_t str_len, int base, char** end, int* err);
int64_t ob_strntoull(const char* str, size_t str_len, int base, char** end, int* err);

int ob_like_range_mb(const ObCharsetInfo* cs, const char* str, size_t str_len, int escape, int w_one, int w_many,
    size_t res_length, char* min_str, char* max_str, size_t* min_length, size_t* max_length);

int ob_wildcmp_mb(const ObCharsetInfo* cs, const char* str_ptr, const char* str_end_ptr, const char* wild_str_ptr,
    const char* wild_end_ptr, int escape_char, int w_one_char, int w_many_char);

int ob_wildcmp_mb_impl(const ObCharsetInfo* cs, const char* str_ptr, const char* str_end_ptr, const char* wild_str_ptr,
    const char* wild_end_ptr, int escape_char, int w_one_char, int w_many_char);

unsigned int ob_instr_mb(const ObCharsetInfo* cs, const char* base, size_t base_len, const char* str, size_t str_len,
    ob_match_info* match, uint32_t nmatch);

const unsigned char* skip_trailing_space(const unsigned char* str, size_t len);

size_t ob_numchars_mb(const ObCharsetInfo* cs, const char* str, size_t len);

size_t ob_charpos_mb(const ObCharsetInfo* cs, const char* str, size_t len, size_t pos);

size_t ob_max_bytes_charpos_mb(
    const ObCharsetInfo* cs, const char* str, size_t str_len, size_t max_bytes, size_t* char_len);
int ob_mb_ctype_mb(
    const ObCharsetInfo* cs __attribute__((unused)), int* ctype, const unsigned char* s, const unsigned char* e);

size_t ob_lengthsp_8bit(const char* str, size_t str_len);

int ob_strnncoll_mb_bin(const ObCharsetInfo* cs __attribute__((unused)), const unsigned char* str1, size_t str1_len,
    const unsigned char* str2, size_t str2_len);

int ob_strnncollsp_mb_bin(const ObCharsetInfo* cs __attribute__((unused)), const unsigned char* str1, size_t str1_len,
    const unsigned char* str2, size_t str2_len);

int ob_wildcmp_mb_bin(const ObCharsetInfo* cs, const char* str, const char* str_end, const char* wildstr,
    const char* wildend, int escape, int w_one, int w_many);

void ob_hash_sort_mb_bin(const ObCharsetInfo* cs __attribute__((unused)), const unsigned char* key, size_t len,
    uint64_t* nr1, uint64_t* nr2, const int calc_end_space, hash_algo hash_algo);

uint32_t ob_convert(char* to, uint32_t to_length, const ObCharsetInfo* to_cs, const char* from, uint32_t from_length,
    const ObCharsetInfo* from_cs, uint32_t* errors);

size_t ob_strnxfrm_unicode_full_bin(const ObCharsetInfo* cs, unsigned char* dst, size_t dstlen, uint32_t nweights,
    const unsigned char* src, size_t srclen, int* is_valid_unicode);

size_t ob_strnxfrm_unicode(const ObCharsetInfo* cs, unsigned char* dst, size_t dstlen, uint32_t nweights,
    const unsigned char* src, size_t src_len, int* is_valid_unicode);

int ob_wildcmp_unicode(const ObCharsetInfo* cs, const char* str, const char* str_end, const char* wildstr,
    const char* wildend, int escape, int w_one, int w_many, uint32_t** weights);

extern void right_to_die_or_duty_to_live_c();

#ifdef __cplusplus
}
#endif

#endif /* OCEANBASE_LIB_OBMYSQL_OB_CTYPE_ */
