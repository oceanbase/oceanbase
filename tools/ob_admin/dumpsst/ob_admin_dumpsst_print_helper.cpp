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

#include "ob_admin_dumpsst_print_helper.h"

namespace oceanbase
{
using namespace storage;
using namespace common;
using namespace blocksstable;
namespace tools
{

const std::string PrintHelper::term(",");
const std::string PrintHelper::trans_term("\\,");

#define FPRINTF(args...) fprintf(stderr, ##args)
#define P_BAR() FPRINTF("|")
#define P_DASH() FPRINTF("------------------------------")
#define P_END_DASH() FPRINTF("--------------------------------------------------------------------------------")
#define P_NAME(key) FPRINTF("%s", (key))
#define P_NAME_FMT(key) FPRINTF("%30s", (key))
#define P_VALUE_STR_B(key) FPRINTF("[%s]", (key))
#define P_VALUE_INT_B(key) FPRINTF("[%d]", (key))
#define P_VALUE_BINT_B(key) FPRINTF("[%ld]", (key))
#define P_VALUE_STR(key) FPRINTF("%s", (key))
#define P_VALUE_INT(key) FPRINTF("%d", (key))
#define P_VALUE_BINT(key) FPRINTF("%ld", (key))
#define P_NAME_BRACE(key) FPRINTF("{%s}", (key))
#define P_COLOR(color) FPRINTF(color)
#define P_LB() FPRINTF("[")
#define P_RB() FPRINTF("]")
#define P_LBRACE() FPRINTF("{")
#define P_RBRACE() FPRINTF("}")
#define P_COLON() FPRINTF(":")
#define P_COMMA() FPRINTF(",")
#define P_TAB() FPRINTF("\t")
#define P_LINE_NAME(key) FPRINTF("%30s", (key))
#define P_LINE_VALUE(key, type) P_LINE_VALUE_##type((key))
#define P_LINE_VALUE_INT(key) FPRINTF("%-30d", (key))
#define P_LINE_VALUE_BINT(key) FPRINTF("%-30ld", (key))
#define P_LINE_VALUE_STR(key) FPRINTF("%-30s", (key))
#define P_END() FPRINTF("\n")

#define P_TAB_LEVEL(level) \
  do { \
    for (int64_t i = 1; i < (level); ++i) { \
      P_TAB(); \
      if (i != level - 1) P_BAR(); \
    } \
  } while (0)

#define P_DUMP_LINE(type) \
  do { \
    P_TAB_LEVEL(level); \
    P_BAR(); \
    P_LINE_NAME(name); \
    P_BAR(); \
    P_LINE_VALUE(value, type); \
    P_END(); \
  } while (0)

#define P_DUMP_LINE_COLOR(type) \
  do { \
    P_COLOR(LIGHT_GREEN); \
    P_TAB_LEVEL(level); \
    P_BAR(); \
    P_COLOR(CYAN); \
    P_LINE_NAME(name); \
    P_BAR(); \
    P_COLOR(NONE_COLOR); \
    P_LINE_VALUE(value, type); \
    P_END(); \
  } while (0)

void PrintHelper::print_dump_title(const char *title, const int64_t level)
{
  if (isatty(fileno(stderr))) {
    P_COLOR(LIGHT_GREEN);
  }
  P_TAB_LEVEL(level);
  P_DASH();
  P_NAME_BRACE(title);
  P_DASH();
  P_END();
}

void PrintHelper::print_end_line(const int64_t level)
{
  if (isatty(fileno(stderr))) {
    P_COLOR(LIGHT_GREEN);
  }
  P_TAB_LEVEL(level);
  P_END_DASH();
  P_END();
  if (isatty(fileno(stderr))) {
    P_COLOR(NONE_COLOR);
  }
}

void PrintHelper::print_dump_title(const char *title, const int64_t &value, const int64_t level)
{
  if (isatty(fileno(stderr))) {
    P_COLOR(LIGHT_GREEN);
  }
  P_TAB_LEVEL(level);
  P_DASH();
  P_LBRACE();
  P_NAME(title);
  P_VALUE_BINT_B(value);
  P_RBRACE();
  P_DASH();
  P_END();
  if (isatty(fileno(stderr))) {
    P_COLOR(NONE_COLOR);
  }
}

void PrintHelper::print_dump_line(const char *name, const uint32_t &value, const int64_t level)
{
  if (isatty(fileno(stderr))) {
    P_DUMP_LINE_COLOR(INT);
  } else {
    P_DUMP_LINE(INT);
  }
}

void PrintHelper::print_dump_line(const char *name, const uint64_t &value, const int64_t level)
{
  if (isatty(fileno(stderr))) {
    P_DUMP_LINE_COLOR(BINT);
  } else {
    P_DUMP_LINE(BINT);
  }
}

void PrintHelper::print_dump_line(const char *name, const int32_t &value, const int64_t level)
{
  if (isatty(fileno(stderr))) {
    P_DUMP_LINE_COLOR(INT);
  } else {
    P_DUMP_LINE(INT);
  }
}

void PrintHelper::print_dump_line(const char *name, const int64_t &value, const int64_t level)
{
  if (isatty(fileno(stderr))) {
    P_DUMP_LINE_COLOR(BINT);
  } else {
    P_DUMP_LINE(BINT);
  }
}

void PrintHelper::print_dump_line(const char *name, const char *value, const int64_t level)
{
  if (isatty(fileno(stderr))) {
    P_DUMP_LINE_COLOR(STR);
  } else {
    P_DUMP_LINE(STR);
  }
}

void PrintHelper::print_dump_list_start(const char *name, const int64_t level)
{
  if (isatty(fileno(stderr))) {
    P_COLOR(LIGHT_GREEN);
    P_TAB_LEVEL(level);
    P_BAR();
    P_COLOR(CYAN);
    P_NAME_FMT(name);
    P_BAR();
    P_COLOR(NONE_COLOR);
    P_LB();
  } else {
    P_TAB_LEVEL(level);
    P_BAR();
    P_NAME_FMT(name);
    P_BAR();
    P_LB();
  }
}

void PrintHelper::print_dump_list_value(const int64_t &value, const bool is_last)
{
  P_VALUE_BINT(value);
  if (!is_last) {
    P_COMMA();
  }
}

void PrintHelper::print_dump_list_value(const int32_t &value, const bool is_last)
{
  P_VALUE_INT(value);
  if (!is_last) {
    P_COMMA();
  }
}

void PrintHelper::print_dump_list_value(const char *value, const bool is_last)
{
  P_VALUE_STR(value);
  if (!is_last) {
    P_COMMA();
  }
}

void PrintHelper::print_dump_list_end()
{
  if (isatty(fileno(stderr))) {
    P_RB();
    P_END();
    P_COLOR(NONE_COLOR);
  } else {
    P_RB();
    P_END();
  }
}

void PrintHelper::print_dump_cols_info_start(const char *n1, const char *n2, const char *n3, const char *n4)
{
  if (isatty(fileno(stderr))) {
    P_COLOR(LIGHT_GREEN);
  }
  FPRINTF("--------{%-15s %15s %15s %15s}----------\n", n1, n2, n3, n4);
}

void PrintHelper::print_dump_cols_info_line(const int32_t &v1, const char *v2, const int64_t &v3, const int64_t &v4)
{
  if (isatty(fileno(stderr))) {
    P_COLOR(LIGHT_GREEN);
    P_BAR();
    P_COLOR(NONE_COLOR);
    FPRINTF("\t[%-15d %15s %15ld %15ld]\n", v1, v2, v3, v4);
  } else {
    P_BAR();
    FPRINTF("\t[%-15d %15s %15ld %15ld]\n", v1, v2, v3, v4);
  }
}

void PrintHelper::print_row_title(const bool use_csv, const ObStoreRow *row, const int64_t row_index)
{
  // print title
  if (!use_csv) {
    if (isatty(fileno(stderr))) {
      P_COLOR(LIGHT_GREEN);
      P_BAR();
      P_COLOR(CYAN);
      P_NAME("ROW");
      P_VALUE_BINT_B(row_index);
      P_COLON();
      P_NAME("trans_id=");
      P_VALUE_BINT_B(row->trans_id_.get_id());
      P_COMMA();
      P_NAME("flag=");
      P_VALUE_INT_B(row->flag_.get_serialize_flag());
      P_COMMA();
      P_NAME("multi_version_row_flag=");
      P_VALUE_INT_B(row->row_type_flag_.flag_);
      P_BAR();
      P_COLOR(NONE_COLOR);
    } else {
      P_BAR();
      P_NAME("ROW");
      P_VALUE_BINT_B(row_index);
      P_COLON();
      P_NAME("trans_id=");
      P_VALUE_BINT_B(row->trans_id_.get_id());
      P_COMMA();
      P_NAME("flag=");
      P_VALUE_INT_B(row->flag_.get_serialize_flag());
      P_COMMA();
      P_NAME("multi_version_row_flag=");
      P_VALUE_INT_B(row->row_type_flag_.flag_);
      P_BAR();
    }
  }
}

void PrintHelper::print_cell(const ObObj &cell, const bool use_csv)
{
  if (!use_csv) {
    P_VALUE_STR_B(to_cstring(cell));
  } else {
    int64_t pos = 0;
    char buf[MAX_BUF_SIZE];
    cell.print_sql_literal(buf, MAX_BUF_SIZE, pos);
    P_NAME(buf);
    P_COMMA();
  }
}

void PrintHelper::replace_all(std::string &str, const std::string &from, const std::string &to)
{
  if(from.empty())
    return;
  size_t start_pos = 0;
  while((start_pos = str.find(from, start_pos)) != std::string::npos) {
    str.replace(start_pos, from.length(), to);
    start_pos += to.length();
  }
}

}
}
