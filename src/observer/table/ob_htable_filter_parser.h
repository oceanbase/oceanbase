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

#ifndef _OB_HTABLE_FILTER_PARSER_H
#define _OB_HTABLE_FILTER_PARSER_H 1
#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_se_array.h"
namespace oceanbase
{
namespace common
{
class ObArenaAllocator;
} // end namespace common
namespace table
{
namespace hfilter
{
class Filter;
class Comparable;
}
class ObHTableFilterParser
{
public:
  struct SimpleString
  {
    char *str_;
    int64_t len_;
  };
public:
  static const int64_t PARSER_ERROR_MSG_SIZE = 512;
  void *scanner_;  // the reentrant lex scanner
  int error_code_;
  // for yyerror()
  int first_column_;
  int last_column_;
  int first_line_;
  int last_line_;
  char error_msg_[PARSER_ERROR_MSG_SIZE];
public:
  ObHTableFilterParser();
  virtual ~ObHTableFilterParser();
  int init(common::ObIAllocator* allocator);
  void destroy();
  // parse the filter string
  int parse_filter(const common::ObString &filter_string, hfilter::Filter *&filter);

  int64_t get_input_len() const { return filter_string_->length(); }
  const char *get_input_str() const { return filter_string_->ptr(); }
  void set_result_filter(hfilter::Filter *filter) { result_filter_ = filter; }

  void *alloc(int64_t size);
  void *realloc(void *ptr, int64_t size);
  void free(void *ptr);
  common::ObIAllocator *allocator() { return allocator_; }
  virtual int create_comparator(const SimpleString &bytes, hfilter::Comparable *&comparator);
  int create_prefix_comparator(const SimpleString &bytes, hfilter::Comparable *&comparator);
  int store_filter(hfilter::Filter *&filter);
protected:
  common::ObIAllocator* allocator_;
  common::ObSEArray<hfilter::Comparable*, 8> comparators_;
  common::ObSEArray<hfilter::Filter*, 8> filters_;
  // the input filter string
  const common::ObString *filter_string_;
  // parse result
  hfilter::Filter *result_filter_;
private:
  static const common::ObString BINARY_TYPE;
  static const common::ObString BINARY_PREFIX_TYPE;
  static const common::ObString REGEX_STRING_TYPE;
  static const common::ObString SUB_STRING_TYPE;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObHTableFilterParser);
};

} // end namespace table
} // end namespace oceanbase

// for yacc/lex error report
class YYLTYPE;
extern void ob_hfilter_error(YYLTYPE *loc, oceanbase::table::ObHTableFilterParser *p, const char *s, ...);
// for flex memory management
extern void *ob_hfilter_alloc(size_t bytes, void* yyscanner);
extern void *ob_hfilter_realloc(void *ptr, size_t bytes, void* yyscanner);
extern void ob_hfilter_free(void *ptr, void* yyscanner);
#endif /* _OB_HTABLE_FILTER_PARSER_H */
