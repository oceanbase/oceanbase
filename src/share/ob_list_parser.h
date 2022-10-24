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

#ifndef __OCEANBASE_SHARE_LIST_PARSER_H__
#define __OCEANBASE_SHARE_LIST_PARSER_H__

#include "lib/ob_define.h"

namespace oceanbase
{
namespace share
{

class ObListMatchCb
{
public:
  virtual int match(const char *value) = 0;
  virtual bool finish() { return true; }
};

class ObListParser
{
public:
  ObListParser() : SYM_LIST_SEP(','), allow_space_(false), token_(0), cur_(NULL), cb_(NULL) {}
  ObListParser(char list_sep)
      : SYM_LIST_SEP(list_sep), token_(0), cur_(NULL), cb_(NULL) {}
  ~ObListParser() = default;
  void set_match_callback(ObListMatchCb &cb) { cb_ = &cb; }
  // Whether to ignore the spaces before and after the value
  void set_allow_space(bool allow) { allow_space_ = allow; }
  int parse(const char *data);
private:
  int match(int sym);
  int get_token(); // Pre-reading symbol
private:
  static const int MAX_TOKEN_SIZE = 1024;
  // Symbol table
  static const int SYM_END = 0;
  static const int SYM_KEY = 1;
  static const int SYM_VALUE = 2;
  // Delimiter
  // Can be customized according to different scenarios, such as
  // e.g.1 value1,value2,value3...
  const char SYM_LIST_SEP; // The separator in the middle of value','
  // Temporary variables
  bool allow_space_; // Whether to allow spaces
  int token_;
  char value_buf_[MAX_TOKEN_SIZE];
  const char *cur_;
  // Call back every time a List is parsed
  ObListMatchCb *cb_;

  DISALLOW_COPY_AND_ASSIGN(ObListParser);
};

}
}
#endif /* __OCEANBASE_SHARE_LIST_PARSER_H__ */
//// end of header file
