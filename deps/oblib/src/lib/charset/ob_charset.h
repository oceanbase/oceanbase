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
#include "lib/charset/ob_template_helper.h"

namespace oceanbase
{
namespace common
{

// 我们目前的字符集实现参考MySQL，现在MySQL源码，进入strings目录，
// 根据文件名后缀可以大体找到对应编码实现

enum ObCharsetType
{
  CHARSET_SESSION_CACHE_NOT_LOADED_MARK = -2,
  CHARSET_ANY = -1,
  CHARSET_INVALID = 0,
  CHARSET_BINARY = 1,
  CHARSET_UTF8MB4 = 2,
  CHARSET_GBK = 3,
  CHARSET_UTF16 = 4,
  CHARSET_GB18030 = 5,
  CHARSET_LATIN1 = 6,
  CHARSET_GB18030_2022 = 7,
  CHARSET_ASCII = 8,
  CHARSET_TIS620 = 9,
  CHARSET_UTF16LE = 10,
  CHARSET_SJIS = 11,
  CHARSET_BIG5 = 12,
  CHARSET_HKSCS = 13,
  CHARSET_HKSCS31 = 14,
  CHARSET_DEC8 = 15,
  CHARSET_GB2312 = 16,
  CHARSET_UJIS = 17,
  CHARSET_EUCKR = 18,
  CHARSET_EUCJPMS = 19,
  CHARSET_CP932 = 20,
  CHARSET_CP850 = 21,
  CHARSET_HP8 = 22,
  CHARSET_MACROMAN = 23,
  CHARSET_SWE7 = 24,
  CHARSET_MAX,
};

/*
*AGGREGATE_2CHARSET[CHARSET_UTF8MB4][CHARSET_GBK]=1表示结果为第一个参数CHARSET_UTF8MB4
*AGGREGATE_2CHARSET[CHARSET_GBK][CHARSET_UTF8MB4]=2表示结果为第二个参数CHARSET_UTF8MB4
*矩阵中只对当前需要考虑的情况填值1&2,其余补0
*return value means idx of the resule type， 0 means OB_CANT_AGGREGATE_2COLLATIONS
*there is no possibly to reach AGGREGATE_2CHARSET[CHARSET_UTF8MB4][CHARSET_UTF8MB4] and so on
*/
static const int AGGREGATE_2CHARSET[CHARSET_MAX][CHARSET_MAX] = {
//CHARSET_INVALID,CHARSET_BINARY,CHARSET_UTF8MB4...
  {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0},//CHARSET_INVALID
  {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0},//CHARSET_BINARY
  {0,0,0,1,2,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},//CHARSET_UTF8MB4
  {0,0,2,0,2,0,1,0,1,0,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0},//CHARSET_GBK
  {0,0,1,1,0,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1},//CHARSET_UTF16
  {0,0,2,0,2,0,1,0,1,0,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0},//CHARSET_GB18030
  {0,0,2,2,2,2,0,2,1,0,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0},//CHARSET_LATIN1
  {0,0,2,0,2,0,1,0,1,0,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0},//CHARSET_GB18030_2022
  {0,0,2,2,2,2,2,2,0,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2},//CHARSET_ASCII
  {0,0,2,0,2,0,0,0,1,0,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0},//CHARSET_TIS620
  {0,0,2,1,0,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1}, // UTF16LE
  {0,0,2,0,2,0,0,0,1,0,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0}, // SJIS
  {0,0,2,0,2,0,0,0,1,0,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0}, // BIG5
  {0,0,2,0,2,0,0,0,1,0,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0}, // HKSCS
  {0,0,2,0,2,0,0,0,1,0,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0}, // HKSCS31
  {0,0,2,0,2,0,0,0,1,0,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0},// DEC8
  {0,0,2,0,2,0,0,0,1,0,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0},// GB2312
  {0,0,2,0,2,0,0,0,1,0,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0}, // CP932
  {0,0,2,0,2,0,0,0,1,0,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0}, // EUCKR
  {0,0,2,0,2,0,0,0,1,0,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0}, // UJIS
  {0,0,2,0,2,0,0,0,1,0,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0}, // EUCJPMS
  {0,0,2,0,2,0,0,0,1,0,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0}, // cp850
  {0,0,2,0,2,0,0,0,1,0,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0}, // hp8
  {0,0,2,0,2,0,0,0,1,0,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0}, // macroman
  {0,0,2,0,2,0,0,0,1,0,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0}, // swe7
};

enum ObCollationType
{
  CS_TYPE_INVALID = 0,
  CS_TYPE_BIG5_CHINESE_CI = 1,
  CS_TYPE_DEC8_SWEDISH_CI = 3,
  CS_TYPE_CP850_GENERAL_CI = 4,
  CS_TYPE_LATIN1_GERMAN1_CI = 5,
  CS_TYPE_HP8_ENGLISH_CI = 6,
  CS_TYPE_LATIN1_SWEDISH_CI = 8,
  CS_TYPE_SWE7_SWEDISH_CI = 10,
  CS_TYPE_ASCII_GENERAL_CI = 11,
  CS_TYPE_UJIS_JAPANESE_CI = 12,
  CS_TYPE_SJIS_JAPANESE_CI = 13,
  CS_TYPE_LATIN1_DANISH_CI = 15,

  CS_TYPE_TIS620_THAI_CI = 18,
  CS_TYPE_EUCKR_KOREAN_CI = 19,
  CS_TYPE_GB2312_CHINESE_CI = 24,
  CS_TYPE_GBK_CHINESE_CI = 28,
  CS_TYPE_LATIN1_GERMAN2_CI = 31,
  CS_TYPE_MACROMAN_GENERAL_CI = 39,
  CS_TYPE_UTF8MB4_GENERAL_CI = 45,
  CS_TYPE_UTF8MB4_BIN = 46,
  CS_TYPE_LATIN1_BIN = 47,
  CS_TYPE_LATIN1_GENERAL_CI = 48,
  CS_TYPE_LATIN1_GENERAL_CS = 49,
  CS_TYPE_MACROMAN_BIN = 53,
  CS_TYPE_UTF16_GENERAL_CI = 54,
  CS_TYPE_UTF16_BIN = 55,
  CS_TYPE_UTF16LE_GENERAL_CI = 56,
  CS_TYPE_UTF16LE_BIN = 62,
  CS_TYPE_BINARY = 63,
  CS_TYPE_ASCII_BIN = 65,
  CS_TYPE_DEC8_BIN = 69,
  CS_TYPE_HP8_BIN = 72,
  CS_TYPE_CP850_BIN = 80,
  CS_TYPE_SWE7_BIN = 82,
  CS_TYPE_BIG5_BIN = 84,
  CS_TYPE_EUCKR_BIN = 85,
  CS_TYPE_GB2312_BIN = 86,
  CS_TYPE_GBK_BIN = 87,
  CS_TYPE_SJIS_BIN = 88,

  CS_TYPE_TIS620_BIN = 89,
  CS_TYPE_UJIS_BIN = 91,
  CS_TYPE_LATIN1_SPANISH_CI = 94,
  CS_TYPE_CP932_JAPANESE_CI = 95,
  CS_TYPE_CP932_BIN = 96,
  CS_TYPE_EUCJPMS_JAPANESE_CI = 97,
  CS_TYPE_EUCJPMS_BIN = 98,
  CS_TYPE_COLLATION_FREE = 100, // mysql中间没有使用这个
  CS_TYPE_UTF16_UNICODE_CI = 101,
  CS_TYPE_UTF16_ICELANDIC_UCA_CI = 102,
  CS_TYPE_UTF16_LATVIAN_UCA_CI = 103,
  CS_TYPE_UTF16_ROMANIAN_UCA_CI = 104,
  CS_TYPE_UTF16_SLOVENIAN_UCA_CI = 105,
  CS_TYPE_UTF16_POLISH_UCA_CI = 106,
  CS_TYPE_UTF16_ESTONIAN_UCA_CI = 107,
  CS_TYPE_UTF16_SPANISH_UCA_CI = 108,
  CS_TYPE_UTF16_SWEDISH_UCA_CI = 109,
  CS_TYPE_UTF16_TURKISH_UCA_CI = 110,
  CS_TYPE_UTF16_CZECH_UCA_CI = 111,
  CS_TYPE_UTF16_DANISH_UCA_CI = 112,
  CS_TYPE_UTF16_LITHUANIAN_UCA_CI = 113,
  CS_TYPE_UTF16_SLOVAK_UCA_CI = 114,
  CS_TYPE_UTF16_SPANISH2_UCA_CI = 115,
  CS_TYPE_UTF16_ROMAN_UCA_CI = 116,
  CS_TYPE_UTF16_PERSIAN_UCA_CI = 117,
  CS_TYPE_UTF16_ESPERANTO_UCA_CI = 118,
  CS_TYPE_UTF16_HUNGARIAN_UCA_CI = 119,
  CS_TYPE_UTF16_SINHALA_UCA_CI = 120,
  CS_TYPE_UTF16_GERMAN2_UCA_CI = 121,
  CS_TYPE_UTF16_CROATIAN_UCA_CI = 122,
  CS_TYPE_UTF16_UNICODE_520_CI = 123,
  CS_TYPE_UTF16_VIETNAMESE_CI  = 124,
  CS_TYPE_ANY = 125, // unused in mysql
  CS_TYPE_HKSCS_BIN = 152,
  CS_TYPE_HKSCS31_BIN = 153,
  CS_TYPE_GB18030_2022_BIN = 216, // unused in mysql
  CS_TYPE_GB18030_2022_PINYIN_CI = 217, // unused in mysql
  CS_TYPE_GB18030_2022_PINYIN_CS = 218, // unused in mysql
  CS_TYPE_GB18030_2022_RADICAL_CI = 219, // unused in mysql
  CS_TYPE_GB18030_2022_RADICAL_CS = 220, // unused in mysql
  CS_TYPE_GB18030_2022_STROKE_CI = 221, // unused in mysql
  CS_TYPE_GB18030_2022_STROKE_CS = 222, // unused in mysql
  CS_TYPE_UTF8MB4_UNICODE_CI = 224,
  CS_TYPE_UTF8MB4_ICELANDIC_UCA_CI,
  CS_TYPE_UTF8MB4_LATVIAN_UCA_CI ,
  CS_TYPE_UTF8MB4_ROMANIAN_UCA_CI ,
  CS_TYPE_UTF8MB4_SLOVENIAN_UCA_CI,
  CS_TYPE_UTF8MB4_POLISH_UCA_CI  ,
  CS_TYPE_UTF8MB4_ESTONIAN_UCA_CI ,
  CS_TYPE_UTF8MB4_SPANISH_UCA_CI ,
  CS_TYPE_UTF8MB4_SWEDISH_UCA_CI ,
  CS_TYPE_UTF8MB4_TURKISH_UCA_CI ,
  CS_TYPE_UTF8MB4_CZECH_UCA_CI  ,
  CS_TYPE_UTF8MB4_DANISH_UCA_CI  ,
  CS_TYPE_UTF8MB4_LITHUANIAN_UCA_CI,
  CS_TYPE_UTF8MB4_SLOVAK_UCA_CI  ,
  CS_TYPE_UTF8MB4_SPANISH2_UCA_CI,
  CS_TYPE_UTF8MB4_ROMAN_UCA_CI,
  CS_TYPE_UTF8MB4_PERSIAN_UCA_CI ,
  CS_TYPE_UTF8MB4_ESPERANTO_UCA_CI,
  CS_TYPE_UTF8MB4_HUNGARIAN_UCA_CI,
  CS_TYPE_UTF8MB4_SINHALA_UCA_CI ,
  CS_TYPE_UTF8MB4_GERMAN2_UCA_CI ,
  CS_TYPE_UTF8MB4_CROATIAN_UCA_CI,
  CS_TYPE_UTF8MB4_UNICODE_520_CI ,
  CS_TYPE_UTF8MB4_VIETNAMESE_CI  ,
  CS_TYPE_GB18030_CHINESE_CI = 248,
  CS_TYPE_GB18030_BIN = 249,
  CS_TYPE_GB18030_CHINESE_CS = 251,
  CS_TYPE_UTF8MB4_0900_AI_CI = 255,
  CS_TYPE_UTF8MB4_DE_PB_0900_AI_CI ,
  CS_TYPE_UTF8MB4_IS_0900_AI_CI   ,
  CS_TYPE_UTF8MB4_LV_0900_AI_CI   ,
  CS_TYPE_UTF8MB4_RO_0900_AI_CI   ,
  CS_TYPE_UTF8MB4_SL_0900_AI_CI   ,
  CS_TYPE_UTF8MB4_PL_0900_AI_CI   ,
  CS_TYPE_UTF8MB4_ET_0900_AI_CI   ,
  CS_TYPE_UTF8MB4_ES_0900_AI_CI   ,
  CS_TYPE_UTF8MB4_SV_0900_AI_CI   ,
  CS_TYPE_UTF8MB4_TR_0900_AI_CI   ,
  CS_TYPE_UTF8MB4_CS_0900_AI_CI   ,
  CS_TYPE_UTF8MB4_DA_0900_AI_CI   ,
  CS_TYPE_UTF8MB4_LT_0900_AI_CI   ,
  CS_TYPE_UTF8MB4_SK_0900_AI_CI   ,
  CS_TYPE_UTF8MB4_ES_TRAD_0900_AI_CI,
  CS_TYPE_UTF8MB4_LA_0900_AI_CI   ,
  CS_TYPE_UTF8MB4_EO_0900_AI_CI  = 273 ,
  CS_TYPE_UTF8MB4_HU_0900_AI_CI   ,
  CS_TYPE_UTF8MB4_HR_0900_AI_CI   ,
  CS_TYPE_UTF8MB4_VI_0900_AI_CI  = 277 ,
  CS_TYPE_UTF8MB4_0900_AS_CS   ,
  CS_TYPE_UTF8MB4_DE_PB_0900_AS_CS ,
  CS_TYPE_UTF8MB4_IS_0900_AS_CS   ,
  CS_TYPE_UTF8MB4_LV_0900_AS_CS   ,
  CS_TYPE_UTF8MB4_RO_0900_AS_CS   ,
  CS_TYPE_UTF8MB4_SL_0900_AS_CS   ,
  CS_TYPE_UTF8MB4_PL_0900_AS_CS   ,
  CS_TYPE_UTF8MB4_ET_0900_AS_CS   ,
  CS_TYPE_UTF8MB4_ES_0900_AS_CS   ,
  CS_TYPE_UTF8MB4_SV_0900_AS_CS   ,
  CS_TYPE_UTF8MB4_TR_0900_AS_CS   ,
  CS_TYPE_UTF8MB4_CS_0900_AS_CS   ,
  CS_TYPE_UTF8MB4_DA_0900_AS_CS   ,
  CS_TYPE_UTF8MB4_LT_0900_AS_CS   ,
  CS_TYPE_UTF8MB4_SK_0900_AS_CS   ,
  CS_TYPE_UTF8MB4_ES_TRAD_0900_AS_CS,
  CS_TYPE_UTF8MB4_LA_0900_AS_CS  ,
  CS_TYPE_UTF8MB4_EO_0900_AS_CS  = 296,
  CS_TYPE_UTF8MB4_HU_0900_AS_CS   ,
  CS_TYPE_UTF8MB4_HR_0900_AS_CS   ,
  CS_TYPE_UTF8MB4_VI_0900_AS_CS = 300,
  CS_TYPE_UTF8MB4_JA_0900_AS_CS = 303,
  CS_TYPE_UTF8MB4_JA_0900_AS_CS_KS ,
  CS_TYPE_UTF8MB4_0900_AS_CI   ,
  CS_TYPE_UTF8MB4_RU_0900_AI_CI   ,
  CS_TYPE_UTF8MB4_RU_0900_AS_CS   ,
  CS_TYPE_UTF8MB4_ZH_0900_AS_CS = 308 ,
  CS_TYPE_UTF8MB4_0900_BIN,
  CS_TYPE_UTF8MB4_NB_0900_AI_CI   ,
  CS_TYPE_UTF8MB4_NB_0900_AS_CS   ,
  CS_TYPE_UTF8MB4_NN_0900_AI_CI   ,
  CS_TYPE_UTF8MB4_NN_0900_AS_CS   ,
  CS_TYPE_UTF8MB4_SR_LATN_0900_AI_CI,
  CS_TYPE_UTF8MB4_SR_LATN_0900_AS_CS,
  CS_TYPE_UTF8MB4_BS_0900_AI_CI   ,
  CS_TYPE_UTF8MB4_BS_0900_AS_CS   ,
  CS_TYPE_UTF8MB4_BG_0900_AI_CI   ,
  CS_TYPE_UTF8MB4_BG_0900_AS_CS   ,
  CS_TYPE_UTF8MB4_GL_0900_AI_CI   ,
  CS_TYPE_UTF8MB4_GL_0900_AS_CS   ,
  CS_TYPE_UTF8MB4_MN_CYRL_0900_AI_CI,
  CS_TYPE_UTF8MB4_MN_CYRL_0900_AS_CS,
  //pinyin order (occupied)
  CS_TYPE_PINYIN_BEGIN_MARK,
  CS_TYPE_UTF8MB4_ZH_0900_AS_CS_CPY, //308 in mysql 8.0
  CS_TYPE_GBK_ZH_0900_AS_CS,
  CS_TYPE_UTF16_ZH_0900_AS_CS,
  CS_TYPE_GB18030_ZH_0900_AS_CS,
  CS_TYPE_LATIN1_ZH_0900_AS_CS,
  CS_TYPE_GB18030_2022_ZH_0900_AS_CS,
  CS_TYPE_ASCII_ZH_0900_AS_CS,
  CS_TYPE_TIS620_ZH_0900_AS_CS,
  CS_TYPE_UTF16LE_ZH_0900_AS_CS,
  CS_TYPE_SJIS_ZH_0900_AS_CS,
  CS_TYPE_BIG5_ZH_0900_AS_CS,
  CS_TYPE_HKSCS_ZH_0900_AS_CS,
  CS_TYPE_HKSCS31_ZH_0900_AS_CS,
  CS_TYPE_DEC8_ZH_0900_AS_CS,
  CS_TYPE_GB2312_ZH_0900_AS_CS, // invalid
  CS_TYPE_UJIS_ZH_0900_AS_CS, // invalid
  CS_TYPE_EUCKR_ZH_0900_AS_CS, // invalid
  CS_TYPE_EUCJPMS_ZH_0900_AS_CS, // invalid
  CS_TYPE_CP932_ZH_0900_AS_CS, // invalid
  CS_TYPE_CP850_ZH_0900_AS_CS, // invalid
  CS_TYPE_HP8_ZH_0900_AS_CS, // invalid
  CS_TYPE_MACROMAN_ZH_0900_AS_CS, // invalid
  CS_TYPE_SWE7_ZH_0900_AS_CS, // invalid

  //radical-stroke order
  CS_TYPE_RADICAL_BEGIN_MARK,
  CS_TYPE_UTF8MB4_ZH2_0900_AS_CS,
  CS_TYPE_GBK_ZH2_0900_AS_CS,
  CS_TYPE_UTF16_ZH2_0900_AS_CS,
  CS_TYPE_GB18030_ZH2_0900_AS_CS,
  CS_TYPE_LATIN1_ZH2_0900_AS_CS,
  CS_TYPE_GB18030_2022_ZH2_0900_AS_CS,
  CS_TYPE_ASCII_ZH2_0900_AS_CS,
  CS_TYPE_TIS620_ZH2_0900_AS_CS,
  CS_TYPE_UTF16LE_ZH2_0900_AS_CS,
  CS_TYPE_SJIS_ZH2_0900_AS_CS,
  CS_TYPE_BIG5_ZH2_0900_AS_CS,
  CS_TYPE_HKSCS_ZH2_0900_AS_CS,
  CS_TYPE_HKSCS31_ZH2_0900_AS_CS,
  CS_TYPE_DEC8_ZH2_0900_AS_CS,
  CS_TYPE_GB2312_ZH2_0900_AS_CS, // invalid
  CS_TYPE_UJIS_ZH2_0900_AS_CS, //invalid
  CS_TYPE_EUCKR_ZH2_0900_AS_CS, // invalid
  CS_TYPE_EUCJPMS_ZH2_0900_AS_CS, // invalid
  CS_TYPE_CP932_ZH2_0900_AS_CS, // invalid
  CS_TYPE_CP850_ZH2_0900_AS_CS, // invalid
  CS_TYPE_HP8_ZH2_0900_AS_CS, // invalid
  CS_TYPE_MACROMAN_ZH2_0900_AS_CS, // invalid
  CS_TYPE_SWE7_ZH2_0900_AS_CS, // invalid
  //stroke order
  CS_TYPE_STROKE_BEGIN_MARK,
  CS_TYPE_UTF8MB4_ZH3_0900_AS_CS,
  CS_TYPE_GBK_ZH3_0900_AS_CS,
  CS_TYPE_UTF16_ZH3_0900_AS_CS,
  CS_TYPE_GB18030_ZH3_0900_AS_CS,
  CS_TYPE_LATIN1_ZH3_0900_AS_CS,
  CS_TYPE_GB18030_2022_ZH3_0900_AS_CS,
  CS_TYPE_ASCII_ZH3_0900_AS_CS,
  CS_TYPE_TIS620_ZH3_0900_AS_CS,
  CS_TYPE_UTF16LE_ZH3_0900_AS_CS,
  CS_TYPE_SJIS_ZH3_0900_AS_CS,
  CS_TYPE_BIG5_ZH3_0900_AS_CS,
  CS_TYPE_HKSCS_ZH3_0900_AS_CS,
  CS_TYPE_HKSCS31_ZH3_0900_AS_CS,
  CS_TYPE_DEC8_ZH3_0900_AS_CS,
  CS_TYPE_GB2312_ZH3_0900_AS_CS, // invalid
  CS_TYPE_UJIS_ZH3_0900_AS_CS, // invalid
  CS_TYPE_EUCKR_ZH3_0900_AS_CS, // invalid
  CS_TYPE_EUCJPMS_ZH3_0900_AS_CS, // invalid
  CS_TYPE_CP932_ZH3_0900_AS_CS, // invalid
  CS_TYPE_CP850_ZH3_0900_AS_CS, // invalid
  CS_TYPE_HP8_ZH3_0900_AS_CS, // invalid
  CS_TYPE_MACROMAN_ZH3_0900_AS_CS, // invalid
  CS_TYPE_SWE7_ZH3_0900_AS_CS, // invalid
  CS_TYPE_MAX
};

// oracle 模式下字符集名称对应的 ID 值
// https://docs.oracle.com/cd/B19306_01/server.102/b14200/functions095.htm
enum ObNlsCharsetId
{
  CHARSET_INVALID_ID = 0,
  CHARSET_US7ASCII_ID = 1,
  CHARSET_WE8MSWIN1252_ID=31,
  CHARSET_TH8TISASCII_ID = 41,
  CHARSET_ZHS16GBK_ID = 852,
  CHARSET_ZHS32GB18030_ID = 854,
  CHARSET_ZHS32GB18030_2022_ID = 859, // not used in oracle
  CHARSET_ZHT16HKSCS_ID = 868,
  CHARSET_UTF8_ID = 871,
  CHARSET_AL32UTF8_ID = 873,
  CHARSET_ZHT16HKSCS31_ID = 992,
  CHARSET_AL16UTF16_ID = 2000,
  CHARSET_AL16UTF16LE_ID = 2002,
  CHARSET_MAX_ID,
};

/*
Coercibility Meaning Example
0 Explicit collation Value with COLLATE clause
1 No collation Concatenation of strings with different collations
2 Implicit collation Column value, stored routine parameter or local variable
3 System constant such as USER() and VERSION() return value, or system variable
4 Coercible Literal string
5 Numeric or temporal value
6 Ignorable NULL or an expression derived from NULL

for reasons why, please refer to
https://dev.mysql.com/doc/refman/8.0/en/charset-collation-coercibility.html
*/
enum ObCollationLevel
{
  CS_LEVEL_EXPLICIT = 0,
  CS_LEVEL_NONE = 1,
  CS_LEVEL_IMPLICIT = 2,
  CS_LEVEL_SYSCONST = 3,
  CS_LEVEL_COERCIBLE = 4,
  CS_LEVEL_NUMERIC = 5,
  CS_LEVEL_IGNORABLE = 6,
  CS_LEVEL_INVALID,   // here we didn't define CS_LEVEL_INVALID as 0,
                      // since 0 is a valid value for CS_LEVEL_EXPLICIT in mysql 5.6.
                      // fortunately we didn't need to use it to define array like charset_arr,
                      // and we didn't persist it on storage.
};


struct ObCharsetWrapper
{
  ObCharsetType charset_;
  const char *description_;
  ObCollationType default_collation_;
  int64_t maxlen_;
};

struct ObCollationWrapper
{
  ObCollationType collation_;
  ObCharsetType charset_;
  int64_t id_;
  bool default_;
  bool compiled_;
  int64_t sortlen_;
};

enum ObNLSCollation
{
  NLS_COLLATION_INVALID = -1,
  NLS_COLLATION_BINARY = 0,
  NLS_COLLATION_SCHINESE_PINYIN_900,
  NLS_COLLATION_SCHINESE_RADICAL_900,
  NLS_COLLATION_SCHINESE_STROKE_900,
  NLS_COLLATION_SCHINESE_PINYIN_M,
  NLS_COLLATION_SCHINESE_PINYIN2_M,
  NLS_COLLATION_SCHINESE_RADICAL2_M,
  NLS_COLLATION_SCHINESE_STROKE2_M,
  NLS_COLLATION_MAX
};

const char *ob_collation_type_str(ObCollationType collation_type);

class ObCharset
{
private:
  ObCharset() {};
  virtual ~ObCharset() {};

public:
  // for all possiable character sets in general, a multi-byte character length
  // could be 5 bytes as maximum value and 1 bytes as minimum value
  static const int32_t MAX_MB_LEN = 5;
  static const int32_t MIN_MB_LEN = 1;

  static const int32_t MAX_CASE_MULTIPLY = 4;
  //比如latin1 1byte ,utf8mb4 4byte,转换因子为4，也可以理解为最多使用4字节存储一个字符
  static const int32_t CharConvertFactorNum = 4;
  static const int64_t VALID_CHARSET_TYPES = 24;
  static const int64_t VALID_COLLATION_TYPES = 167;
  static int init_charset();
  // strntodv2 is an enhanced version of strntod,
  // which handles nan/infinity values in oracle mode.
  // We still keep strntod to keep it compatible with mysql implementation
  static double strntodv2(const char *str,
                        size_t str_len,
                        char **endptr,
                        int *err);
  static double strntod(const char *str,
                        size_t str_len,
                        char **endptr,
                        int *err);
  static int64_t strntoll(const char *str,
                   size_t str_len,
                   int base,
                   char **end_ptr,
                   int *err);
  static uint64_t strntoull(const char *str,
                            size_t str_len,
                            int base,
                            char **end_ptr,
                            int *err);
  static int64_t strntoll(const char *str,
                   size_t str_len,
                   int base,
                   int *err);
  static uint64_t strntoull(const char *str,
                            size_t str_len,
                            int base,
                            int *err);
  static uint64_t strntoullrnd(const char *str,
                               size_t str_len,
                               int unsigned_fl,
                               char **endptr,
                               int *err);
  static char *lltostr(int64_t val,
                       char *dst,
                       int radix,
                       int upcase);
  static size_t scan_str(const char *str,
                         const char *end,
                         int sq);
  // return position in characters
  static uint32_t instr(ObCollationType collation_type,
                        const char *str1,
                        int64_t str1_len,
                        const char *str2,
                        int64_t str2_len);

  // return position in bytes
  static int64_t instrb(ObCollationType collation_type,
                        const char *str1,
                        int64_t str1_len,
                        const char *str2,
                        int64_t str2_len);
  static uint32_t locate(ObCollationType collation_type,
                         const char *str1,
                         int64_t str1_len,
                         const char *str2,
                         int64_t str2_len,
                         int64_t pos);
  static int well_formed_len(ObCollationType collation_type,
                             const char *str,
                             int64_t str_len,
                             int64_t &well_formed_len);
  static int well_formed_len(ObCollationType collation_type,
                             const char *str,
                             int64_t str_len,
                             int64_t &well_formed_len,
                             int32_t &well_formed_error);
  static int strcmp(ObCollationType collation_type,
                    const char *str1,
                    int64_t str1_len,
                    const char *str2,
                    int64_t str2_len);

  static int strcmpsp(ObCollationType collation_type,
                      const char *str1,
                      int64_t str1_len,
                      const char *str2,
                      int64_t str2_len,
                      bool cmp_endspace);

  static size_t casedn(const ObCollationType collation_type,
                       char *src, size_t src_len,
                       char *dest, size_t dest_len);
  static size_t caseup(const ObCollationType collation_type,
                       char *src, size_t src_len,
                       char *dest, size_t dest_len);
  static int caseup(const ObCollationType collation_type,
                      const ObString &src,
                      ObString &dst,
                      ObIAllocator &allocator);
  static int casedn(const ObCollationType collation_type,
                      const ObString &src,
                      ObString &dst,
                      ObIAllocator &allocator);
  static size_t sortkey(ObCollationType collation_type,
                        const char *str,
                        int64_t str_len,
                        char *key,
                        int64_t key_len,
                        bool &is_valid_unicode);
  static size_t sortkey_var_len(ObCollationType collation_type,
                          const char *str,
                          int64_t str_len,
                          char *key,
                          int64_t key_len,
                          bool is_space_cmp,
                          bool &is_valid_unicode);
  static uint64_t hash(ObCollationType collation_type,
                       const char *str,
                       int64_t str_len,
                       uint64_t seed,
                       const bool calc_end_space,
                       hash_algo hash_algo);
  static uint64_t hash(ObCollationType collation_type,
                           const char *str,
                           int64_t str_len,
                           uint64_t seed,
                           hash_algo hash_algo);

  static int like_range(ObCollationType collation_type,
                        const ObString &like_str,
                        char escape,
                        char *min_str,
                        size_t *min_str_len,
                        char *max_str,
                        size_t *max_str_len,
                        size_t *prefix_len = NULL);
  static size_t strlen_char(ObCollationType collation_type,
                            const char *str,
                            int64_t str_len);
  static size_t strlen_byte_no_sp(ObCollationType collation_type,
                                  const char *str,
                                  int64_t str_len);
  static size_t charpos(ObCollationType collation_type,
                        const char *str,
                        const int64_t str_len,
                        const int64_t length,
                        int *ret = NULL);
  static size_t max_bytes_charpos(ObCollationType collation_type,
                        const char *str,
                        const int64_t str_len,
                        const int64_t max_bytes,
                        int64_t &char_len);
  // match like pattern
  static bool wildcmp(ObCollationType collation_type,
                      const ObString &str,
                      const ObString &wildstr,
                      int32_t escape, int32_t w_one, int32_t w_many);
  static int mb_wc(ObCollationType collation_type, const ObString &mb, int32_t &wc);
  static int mb_wc(ObCollationType collation_type,
                   const char *mb, const int64_t mb_size, int32_t &length, int32_t &wc);
  static int wc_mb(ObCollationType collation_type, int32_t wc, char *buff, int32_t buff_len, int32_t &length);
  static int display_len(ObCollationType collation_type, const ObString &mb, int64_t &width);
  static int max_display_width_charpos(ObCollationType collation_type, const char *mb, const int64_t mb_size,
                                       const int64_t max_width, int64_t &char_pos,
                                       int64_t *total_widta = NULL);
  static const char *charset_name(ObCharsetType charset_type);
  static const char *charset_name(ObCollationType coll_type);
  static const char *collation_name(ObCollationType collation_type);
  static int collation_name(ObCollationType coll_type, ObString &coll_name);
  static const char* collation_level(const ObCollationLevel cs_level);
  static ObCharsetType charset_type(const char *cs_name);
  static ObCollationType collation_type(const char *cs_name);
  static ObCharsetType charset_type(const ObString &cs_name);
  static ObCharsetType charset_type_by_name_oracle(const ObString &cs_name);
  static ObCollationType collation_type(const ObString &cs_name);
  static bool is_valid_collation(ObCharsetType charset_type, ObCollationType coll_type);
  static bool is_valid_collation(int64_t coll_type_int);
  static bool is_valid_charset(int64_t cs_type_int);
  static bool is_gb18030_2022(int64_t coll_type_int) {
    ObCollationType coll_type = static_cast<ObCollationType>(coll_type_int);
    return CS_TYPE_GB18030_2022_BIN <= coll_type && coll_type <= CS_TYPE_GB18030_2022_STROKE_CS;
  }
  static bool is_gb_charset(int64_t cs_type_int)
  {
    ObCharsetType charset_type = static_cast<ObCharsetType>(cs_type_int);
    return CHARSET_GBK == charset_type
      || CHARSET_GB18030 == charset_type
      || CHARSET_GB18030_2022 == charset_type;
  }
  static ObCharsetType charset_type_by_coll(ObCollationType coll_type);
  static int check_valid_implicit_convert(ObCollationType src_type, ObCollationType dst_type);
  static int charset_name_by_coll(const ObString &coll_name, common::ObString &cs_name);
  static int charset_name_by_coll(ObCollationType coll_type, common::ObString &cs_name);
  static int calc_collation(const ObCollationLevel level1,
                            const ObCollationType type1,
                            const ObCollationLevel level2,
                            const ObCollationType type2,
                            ObCollationLevel &res_level,
                            ObCollationType &res_type);
  static int result_collation(const ObCollationLevel level1,
                              const ObCollationType type1,
                              const ObCollationLevel level2,
                              const ObCollationType type2,
                              ObCollationLevel &res_level,
                              ObCollationType &res_type);
  static int aggregate_collation_old(const ObCollationLevel level1,
                                 const ObCollationType type1,
                                 const ObCollationLevel level2,
                                 const ObCollationType type2,
                                 ObCollationLevel &res_level,
                                 ObCollationType &res_type);
  static int aggregate_collation_new(
                                const ObCollationLevel collation_level1,
                                const ObCollationType collation_type1,
                                const ObCollationLevel collation_level2,
                                const ObCollationType collation_type2,
                                ObCollationLevel &res_level,
                                ObCollationType &res_type,
                                uint32_t flags);
  static bool left_is_superset(const ObCollationLevel collation_level1,
                               const ObCollationType collation_type1,
                               const ObCollationLevel collation_level2,
                               const ObCollationType collation_type2);
  static bool is_bin_sort(ObCollationType collation_type);

  static bool is_ci_collate(ObCollationType collation_type);

  static ObCollationType get_bin_collation(const ObCharsetType charset_type);
  static int first_valid_char(const ObCollationType collation_type,
                              const char *buf,
                              const int64_t buf_size,
                              int64_t &char_len);

  static int last_valid_char(const ObCollationType collation_type,
                             const char *buf,
                             const int64_t buf_size,
                             int64_t &char_len);

  static ObCharsetType get_default_charset();
  static ObCollationType get_default_collation(ObCharsetType charset_type);
  static ObCollationType get_default_collation_oracle(ObCharsetType charset_type);
  static ObCollationType get_default_collation_by_mode(ObCharsetType charset_type, bool is_oracle_mode);
  static int get_default_collation(ObCharsetType charset_type, ObCollationType &coll_type);
  static int get_default_collation(const ObCollationType &in, ObCollationType &out);
  static ObCollationType get_system_collation();
  static bool is_default_collation(ObCollationType type);
  static bool is_default_collation(ObCharsetType charset_type, ObCollationType coll_type);
  static const char* get_default_charset_name()
  { return ObCharset::charset_name(ObCharset::get_default_charset()); }
  static const char* get_default_collation_name()
  { return ObCharset::collation_name(ObCharset::get_default_collation(ObCharset::get_default_charset())); }
  static void get_charset_wrap_arr(const ObCharsetWrapper *&charset_wrap_arr, int64_t &charset_wrap_arr_len)
  { charset_wrap_arr = charset_wrap_arr_; charset_wrap_arr_len = VALID_CHARSET_TYPES; }
  static void get_collation_wrap_arr(const ObCollationWrapper *&collation_wrap_arr, int64_t &collation_wrap_arr_len)
  { collation_wrap_arr = collation_wrap_arr_; collation_wrap_arr_len = VALID_COLLATION_TYPES; }
  static int check_and_fill_info(ObCharsetType &charset_type, ObCollationType &collation_type);

  static int strcmp(const ObCollationType collation_type,
                    const ObString &l_str,
                    const ObString &r_str);
  //these interface is not safe:
  //when invoke this, if ObString a = "134";  this func will core; so avoid passing src as a style
  //if collation type is gb18030, this func will die
  //**Please** use toupper and tolower instead of casedn and caseup
  static size_t casedn(const ObCollationType collation_type, ObString &src);
  static size_t caseup(const ObCollationType collation_type, ObString &src);

  static int toupper(const ObCollationType collation_type,
                     const ObString &src, ObString &dst,
                     ObIAllocator &allocator);
  static int tolower(const ObCollationType collation_type,
                     const ObString &src, ObString &dst,
                     ObIAllocator &allocator);
  static int tolower(const ObCharsetInfo *cs,
                     const ObString &src, ObString &dst,
                     ObIAllocator &allocator);

  static bool case_insensitive_equal(const ObString &one,
                                     const ObString &another,
                                     const ObCollationType &collation_type = CS_TYPE_UTF8MB4_GENERAL_CI);
  static bool case_sensitive_equal(const ObString &one, const ObString &another);
  static bool case_compat_mode_equal(const ObString &one, const ObString &another);
  static uint64_t hash(const ObCollationType collation_type, const ObString &str,
                       uint64_t seed = 0, hash_algo hash_algo = NULL);
  static uint64_t hash(
      const ObCollationType collation_type, const ObString &str,
      uint64_t seed, const bool calc_end_space, hash_algo hash_algo);
  static bool case_mode_equal(const ObNameCaseMode mode,
                              const ObString &one,
                              const ObString &another);
  static bool is_space(const ObCollationType collation_type, char c);
  static bool is_graph(const ObCollationType collation_type, char c);
  static bool usemb(const ObCollationType collation_type);
  static int is_mbchar(const ObCollationType collation_type, const char *str, const char *end);
  static const ObCharsetInfo *get_charset(const ObCollationType coll_type);
  static int get_mbmaxlen_by_coll(const ObCollationType collation_type, int64_t &mbmaxlen);
  static int get_mbminlen_by_coll(const ObCollationType collation_type, int64_t &mbminlen);

  static int fit_string(const ObCollationType collation_type,
                        const char *str,
                        const int64_t str_len,
                        const int64_t len_limit_in_byte,
                        int64_t &byte_num,
                        int64_t &char_num);

  static int get_aggregate_len_unit(const ObCollationType collation_type, bool &len_in_byte);

  // 实现不同字符集之间的转换
  static int charset_convert(const ObCollationType from_type,
                             const char *from_str,
                             const uint32_t from_len,
                             const ObCollationType to_type,
                             char *to_str,
                             int64_t to_len,
                             uint32_t &result_len,
                             bool trim_incomplete_tail = true,
                             bool report_error = true,
                             const ob_wc_t replaced_char = '?');
  enum CONVERT_FLAG : int64_t {
    COPY_STRING_ON_SAME_CHARSET = 1<<0,
    REPLACE_UNKNOWN_CHARACTER = 1<<1,
    REPLACE_UNKNOWN_CHARACTER_ON_SAME_CHARSET = 1<<2,
  };
  static int charset_convert(ObIAllocator &alloc,
                             const ObString &in,
                             const ObCollationType src_cs_type,
                             const ObCollationType dst_cs_type,
                             ObString &out,
                             int64_t convert_flag = 0,
                             int64_t *action_flag = NULL);

  static int whitespace_padding(ObIAllocator &allocator,
                                const ObCollationType coll_type,
                                const ObString &input,
                                const int64_t pad_whitespace_length,
                                ObString &result);

  static bool is_cs_nonascii(ObCollationType collation_type);
  static bool is_cs_unicode(ObCollationType collation_type);
  static bool is_cs_uca(ObCollationType collation_type);
  static int get_replace_character(ObCollationType collation_type, int32_t &replaced_char_unicode);
  static bool is_cjk_charset(ObCollationType collation_type);
  static bool is_valid_connection_collation(ObCollationType collation_type);
  static const char* get_oracle_charset_name_by_charset_type(ObCharsetType charset_type);
  static int get_nls_charset_id_by_charset_type(ObCharsetType charset_type);
  static ObNlsCharsetId charset_type_to_ora_charset_id(ObCharsetType cs_type);
  static ObCharsetType ora_charset_type_to_charset_type(ObNlsCharsetId charset_id);
  static int trim_end_of_str(const char *buf, int length, char *&trim_end, ObCharsetType ctype);
  static bool is_valid_nls_collation(ObNLSCollation nls_collation);
  static bool is_valid_ora_charset_id(ObNlsCharsetId charset_id);
  static ObCollationType ora_charset_type_to_coll_type(ObNlsCharsetId charset_id);
  static ObCollationType get_coll_type_by_nlssort_param(ObCharsetType charset_type,
                                                        const ObString &nlssort_param);
private:
  static int init_charset_and_arr();
  static int init_charset_info_coll_info(ObCharsetInfo *cs, ObCharsetLoader& loader);
  static bool is_argument_valid(const ObCharsetInfo *charset_info, const char *str, int64_t str_len);
  static bool is_argument_valid(const ObCollationType &collation_type, const char *str1, int64_t str_len1, const char *str2, int64_t str_len2);
  static int copy_zh_cs(ObCharsetInfo *from_cs, ObCollationType to_coll_type, ObCharsetInfo *&to_cs);
  static int copy_zh_cs(ObCharsetInfo *from_cs, ObCharsetType charset_type, ObCharsetInfo *&to_cs);

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObCharset);
private:
  static const ObCharsetWrapper charset_wrap_arr_[VALID_CHARSET_TYPES];
  static const ObCollationWrapper collation_wrap_arr_[VALID_COLLATION_TYPES];
  static ObCharsetInfo *charset_arr[CS_TYPE_MAX];   // CHARSET_INFO *
  static ObCharsetType collation_charset_map[CS_TYPE_MAX];
  static ObCharsetType default_charset_type_;
  static ObCollationType default_collation_type_;
};

class ObCharsetUtils
{
public:
  static int init(ObIAllocator &allocator);
  static ObString get_const_str(ObCollationType coll_type, int ascii)
  {
    ObString result;
    if (ascii >= 0  && ascii <= INT8_MAX
        && ObCharset::is_valid_collation(coll_type)) {
      result = const_str_for_ascii_[ObCharset::charset_type_by_coll(coll_type)][ascii];
    }
    return result;
  }
  template<typename foreach_char_func>
  static int foreach_char(const common::ObString &str,
                   common::ObCollationType collation_type,
                   foreach_char_func &func,
                   bool ignore_invalid_character = false)
  {
    int ret = common::OB_SUCCESS;
    int32_t wchar = 0;
    int32_t length = 0;
    ObString encoding;

    for (common::ObString temp_str = str; OB_SUCC(ret) && !temp_str.empty(); temp_str+=length) {
      if (OB_FAIL(ObCharset::mb_wc(collation_type, temp_str.ptr(), temp_str.length(), length, wchar))) {
        COMMON_LOG(WARN, "fail to call mb_wc", K(ret), KPHEX(temp_str.ptr(), temp_str.length()), K(ignore_invalid_character));
        if (OB_ERR_INCORRECT_STRING_VALUE == ret && ignore_invalid_character) {
          ret = common::OB_SUCCESS;
          wchar = INT32_MAX;
          int64_t min_len = 0;
          ObCharset::get_mbminlen_by_coll(collation_type, min_len);
          length = static_cast<int32_t>(min_len);
        }
      }
      if (OB_SUCC(ret)) {
        encoding.assign_ptr(temp_str.ptr(), length);
        if (OB_FAIL(func(encoding, wchar))) {
          COMMON_LOG(WARN, "fail to call func", K(ret), K(encoding),
                     KPHEX(encoding.ptr(), encoding.length()), K(wchar));
        }
        COMMON_LOG(DEBUG, "foreach char", K(encoding),
                   KPHEX(encoding.ptr(), encoding.length()), K(wchar));
      }
    }
    return ret;
  }

  static int remove_char_endspace(ObString &str,
                                  const ObCharsetInfo *charsetInfo);
private:
  static ObString const_str_for_ascii_[CHARSET_MAX][INT8_MAX + 1];
};

class ObStringScanner
{
public:
  enum {
    IGNORE_INVALID_CHARACTER = 1<<0,
  };
  ObStringScanner(const ObString &str, common::ObCollationType collation_type, uint64_t flags = 0)
    : origin_str_(str), str_(str), collation_type_(collation_type), flags_(flags)
  {}
  int next_character(ObString &encoding_value, int32_t &unicode_value);
  bool next_character(ObString &encoding_value, int32_t &unicode_value, int &ret);
  ObString get_remain_str() { return str_; }
  void forward_bytes(int64_t n) { str_ += n; }
  TO_STRING_KV(K_(str), K_(collation_type));
private:
  const ObString &origin_str_;
  ObString str_;
  common::ObCollationType collation_type_;
  uint64_t flags_;
};

class ObCharSetString
{
public:
  ObCharSetString(ObString str, ObCollationType cs_type)
    : str_(str), cs_type_(cs_type)
  {}
  ObString &get_string() { return str_; }
  int to_cs_type(ObIAllocator&allocator, ObCollationType target_cs_type) {
    return ObCharset::charset_convert(allocator, str_, cs_type_, target_cs_type, str_);
  }
protected:
  ObString str_;
  ObCollationType cs_type_;
};

// to_string adapter
template<>
inline int databuff_print_obj(char *buf, const int64_t buf_len,
                              int64_t &pos, const ObCollationType &t)
{
  return databuff_printf(buf, buf_len, pos, "\"%s\"", ObCharset::collation_name(t));
}
template<>
inline int databuff_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                                  const bool with_comma, const ObCollationType &t)
{
  return databuff_printf(buf, buf_len, pos, WITH_COMMA("%s:\"%s\""), key, ObCharset::collation_name(t));
}
} // namespace common
} // namespace oceanbase

#endif /* OCEANBASE_CHARSET_H_ */
