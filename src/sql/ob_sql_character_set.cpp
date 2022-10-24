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

#include "sql/ob_sql_character_set.h"
#include <strings.h>
#include <stdio.h>
#include "share/ob_define.h"
namespace oceanbase
{
namespace sql
{

typedef struct CharacterSet
{
  const char *name_;
  int32_t number_;
} ObCharacterSet;

typedef struct CharacterSetR
{
  int32_t number_;
  const char *name_;
} ObCharacterSetR;

static const ObCharacterSet charset[] =
{
  {"armscii8", 32},
  {"ascii", 11},
  {"big5", 1},
  {"binary", 63},
  {"cp1250", 26},
  {"cp1251", 14},
  {"cp1256", 57},
  {"cp1257", 29},
  {"cp850", 4},
  {"cp852", 40},
  {"cp866", 36},
  {"cp932", 95},
  {"dec8", 3},
  {"eucjpms", 97},
  {"euckr", 19},
  {"gb2312", 24},
  {"gbk", 28},
  {"geostd8", 92},
  {"greek", 25},
  {"hebrew", 16},
  {"hp8", 6},
  {"keybcs2", 37},
  {"koi8r", 7},
  {"koi8u", 22},
  {"latin1", 5},
  {"latin2", 2},
  {"latin5", 30},
  {"latin7", 20},
  {"macce", 38},
  {"macroman", 39},
  {"sjis", 13},
  {"swe7", 10},
  {"tis620", 18},
  {"ucs2", 35},
  {"ujis", 12},
  {"utf8", 33}
};

static const ObCharacterSetR charsetR[] =
{
  {1, "big5"},
  {2, "latin2"},
  {3, "dec8"},
  {4, "cp850"},
  {5, "latin1"},
  {6, "hp8"},
  {7, "koi8r"},
  {10, "swe7"},
  {11, "ascii"},
  {12, "ujis"},
  {13, "sjis"},
  {14, "cp1251"},
  {16, "hebrew"},
  {18, "tis620"},
  {19, "euckr"},
  {20, "latin7"},
  {22, "koi8u"},
  {24, "gb2312"},
  {25, "greek"},
  {26, "cp1250"},
  {28, "gbk"},
  {29, "cp1257"},
  {30, "latin5"},
  {32, "armscii8"},
  {33, "utf8"},
  {35, "ucs2"},
  {36, "cp866"},
  {37, "keybcs2"},
  {38, "macce"},
  {39, "macroman"},
  {40, "cp852"},
  {57, "cp1256"},
  {63, "binary"},
  {92, "geostd8"},
  {95, "cp932"},
  {97, "eucjpms"},
};

int32_t get_char_number_from_name(const common::ObString &name)
{
  int32_t ret = 0;
  int32_t low = 0;
  int32_t high = ARRAYSIZEOF(charset) - 1;
  int cret = 0;
  while (low <= high) {
    ret = low + (high - low) / 2;
    cret = name.case_compare(charset[ret].name_);
    if (0 == cret) {
      break;
    } else if (0 > cret) {
      high = ret - 1;
    } else {
      low = ret + 1;
    }
  }

  if (low > high) {
    ret = -1;
  } else {
    ret = charset[ret].number_;
  }
  return ret;
}

const char *get_char_name_from_number(int32_t number)
{
  const char *name = "gbk";
  int32_t ret = 0;
  int32_t low = 0;
  int32_t high = ARRAYSIZEOF(charsetR) - 1;
  while (low <= high) {
    ret = low + (high - low) / 2;
    if (charsetR[ret].number_ == number) {
      break;
    } else if (charsetR[ret].number_ > number) {
      high = ret - 1;
    } else {
      low = ret + 1;
    }
  }

  if (low > high) {
    ret = -1;
  } else {
    name = charsetR[ret].name_;
  }
  return name;
}
}
}
