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

#define USING_LOG_PREFIX SERVER
#include "ob_htable_filter_parser.h"
#include "htable_filter_lex.hxx"
using namespace oceanbase::table;
using namespace oceanbase::common;

ObHTableFilterParser::ObHTableFilterParser()
    :scanner_(nullptr),
     error_code_(OB_SUCCESS),
     first_column_(0),
     last_column_(0),
     first_line_(0),
     last_line_(0),
     allocator_(nullptr),
     filter_string_(nullptr),
     result_filter_(nullptr)
{
  error_msg_[0] = '\0';
}

ObHTableFilterParser::~ObHTableFilterParser()
{
  destroy();
}

int ObHTableFilterParser::init(common::ObIAllocator* allocator)
{
  int ret = OB_SUCCESS;
  if (nullptr != scanner_ || nullptr != allocator_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("scanner already inited", K(ret));
  } else {
    allocator_ = allocator;
    if (OB_FAIL(ob_hfilter_lex_init_extra(this, &scanner_))) {
      LOG_WARN("failed to init lex", K(ret));
    }
  }
  return ret;
}
using hfilter::Comparable;
using hfilter::Filter;

void ObHTableFilterParser::destroy()
{
  if (nullptr != scanner_) {
    ob_hfilter_lex_destroy(scanner_);
    scanner_ = nullptr;
  }
  int64_t N = comparators_.count();
  for (int64_t i = 0; i < N; ++i) {
    comparators_.at(i)->~Comparable();
  }
  comparators_.reset();
  N = filters_.count();
  for (int64_t i = 0; i < N; ++i) {
    filters_.at(i)->~Filter();
  }
  filters_.reset();
}

// @see htable_filter_tab.cxx
extern int ob_hfilter_parse(oceanbase::table::ObHTableFilterParser *parse_ctx);

int ObHTableFilterParser::parse_filter(const ObString &filter_string, hfilter::Filter *&filter)
{
  int ret = OB_SUCCESS;
  filter = NULL;
  filter_string_ = &filter_string;
  const char * buf = filter_string.ptr();
  int32_t len = filter_string.length();
  YY_BUFFER_STATE bp = ob_hfilter__scan_bytes(buf, len, scanner_);
  ob_hfilter__switch_to_buffer(bp, scanner_);
  ret = ob_hfilter_parse(this);  // the bison parser
  LOG_DEBUG("parse filter", K(ret), K(filter_string));
  if (OB_SUCC(ret)) {
    filter = result_filter_;
  } else {
    if (1 == ret) {  // syntax error
      if (OB_SUCCESS != error_code_) {
        ret = error_code_;
      } else {
        ret = OB_ERR_PARSER_SYNTAX;
      }
    }
    LOG_WARN("failed to parse filter", K(ret), K_(error_msg), K(filter_string));
  }
  ob_hfilter__delete_buffer(bp, scanner_);
  return ret;
}

const ObString ObHTableFilterParser::BINARY_TYPE = ObString::make_string("binary");
const ObString ObHTableFilterParser::BINARY_PREFIX_TYPE = ObString::make_string("binaryprefix");
const ObString ObHTableFilterParser::REGEX_STRING_TYPE = ObString::make_string("regexstring");
const ObString ObHTableFilterParser::SUB_STRING_TYPE = ObString::make_string("substring");
int ObHTableFilterParser::create_comparator(const SimpleString &bytes, hfilter::Comparable *&comparator)
{
  int ret = OB_SUCCESS;
  char *p = static_cast<char*>(memchr(bytes.str_, ':', bytes.len_));
  if (NULL == p) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("no : found in the comprator", K(ret));
  } else {
    comparator = NULL;
    int64_t len1 = p-bytes.str_;
    ObString comparator_type(len1, bytes.str_);
    ObString comparator_value(bytes.len_-len1-1, p+1);
    if (comparator_type == BINARY_TYPE) {
      comparator = OB_NEWx(hfilter::BinaryComparator, allocator_, comparator_value);
    } else if (comparator_type == BINARY_PREFIX_TYPE) {
      comparator = OB_NEWx(hfilter::BinaryPrefixComparator, allocator_, comparator_value);
    } else if (comparator_type == REGEX_STRING_TYPE) {
      //comparator = OB_NEWx(hfilter::RegexStringComparator, allocator_, comparator_value);
      LOG_WARN("regexstring comparator not supported yet", K(ret));
      ret = OB_NOT_SUPPORTED;
    } else if (comparator_type == SUB_STRING_TYPE) {
      comparator = OB_NEWx(hfilter::SubStringComparator, allocator_, comparator_value);
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid comprator type", K(ret), K(comparator_type));
    }
    if (OB_SUCC(ret) && NULL == comparator) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("no memory", K(ret));
    }
    if (NULL != comparator) {
      if (OB_FAIL(comparators_.push_back(comparator))) {
        LOG_WARN("failed to add comparator", K(ret));
        comparator->~Comparable();
        comparator = NULL;
      }
    }
  }
  return ret;
}

int ObHTableFilterParser::create_prefix_comparator(const SimpleString &bytes, hfilter::Comparable *&comparator)
{
  int ret = OB_SUCCESS;
  ObString comparator_value(bytes.len_, bytes.str_);
  comparator = OB_NEWx(hfilter::BinaryPrefixComparator, allocator_, comparator_value);
  if (NULL == comparator) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("no memory", K(ret));
  }
  if (NULL != comparator) {
    if (OB_FAIL(comparators_.push_back(comparator))) {
      LOG_WARN("failed to add comparator", K(ret));
      comparator->~Comparable();
      comparator = NULL;
    }
  }
  return ret;
}

int ObHTableFilterParser::store_filter(hfilter::Filter *&filter)
{
  int ret = OB_SUCCESS;
  if (NULL != filter) {
    if (OB_FAIL(filters_.push_back(filter))) {
      LOG_WARN("failed to add filter", K(ret));
      filter->~Filter();
      filter = NULL;
    }
  }
  return ret;
}

#include "sql/parser/parse_malloc.h"
void *ObHTableFilterParser::alloc(int64_t size)
{
  return parse_malloc(size, allocator_);
}

void *ObHTableFilterParser::realloc(void *ptr, int64_t size)
{
  return parse_realloc(ptr, size, allocator_);
}

void ObHTableFilterParser::free(void *ptr)
{
  // we don't need to free the memory from arena allocator
  UNUSED(ptr);
}

void ob_hfilter_error(YYLTYPE *loc, oceanbase::table::ObHTableFilterParser *p, const char *format_msg, ...)
{
  if (OB_LIKELY(NULL != p)) {
    va_list ap;
    va_start(ap, format_msg);
    vsnprintf(p->error_msg_, p->PARSER_ERROR_MSG_SIZE, format_msg, ap);
    if (OB_LIKELY(NULL != loc)) {
        p->first_column_ = loc->first_column;
        p->last_column_ = loc->last_column;
        p->first_line_ = loc->first_line;
        p->last_line_ = loc->last_line;
    }
    va_end(ap);
  }
}

void *ob_hfilter_alloc(size_t bytes, void* yyscanner)
{
  ObHTableFilterParser *p = ob_hfilter_get_extra(yyscanner);
  return p->alloc(bytes);
}

void *ob_hfilter_realloc(void *ptr, size_t bytes, void* yyscanner)
{
  ObHTableFilterParser *p = ob_hfilter_get_extra(yyscanner);
  return p->realloc(ptr, bytes);
}

void ob_hfilter_free(void *ptr, void* yyscanner)
{
  ObHTableFilterParser *p = ob_hfilter_get_extra(yyscanner);
  p->free(ptr);
}
