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

#ifndef __OCEANBASE_SHARE_KV_PARSER_H__
#define __OCEANBASE_SHARE_KV_PARSER_H__

#include "share/ob_define.h"
#include "lib/allocator/page_arena.h"

namespace oceanbase
{
namespace share
{

class ObKVMatchCb
{
public:
  virtual int match(const char *key, const char *value) = 0;
  virtual bool check() const { return true; }
};

class ObKVParser
{
public:
  ObKVParser() : SYM_KV_SEP(':'), SYM_PAIR_SEP(','),
    allow_space_(true), token_(0), cur_(NULL), data_(NULL), data_length_(0), cb_(NULL) {}
  ObKVParser(char kv_sep, char pair_sep)
      : SYM_KV_SEP(kv_sep), SYM_PAIR_SEP(pair_sep),
      allow_space_(true), token_(0), cur_(NULL), data_(NULL), data_length_(0), cb_(NULL) {}
  ~ObKVParser() = default;
  void set_match_callback(ObKVMatchCb &cb) { cb_ = &cb; }
  void set_allow_space(bool allow) { allow_space_ = allow; }
  int parse(const char *data);
  int parse(const char *data, int64_t data_length);
private:
  int match(int sym);
  int emit(int sym);
  int get_token(); // Pre-reading symbol
  int kv_pair();
private:
  static const int MAX_TOKEN_SIZE = 1024;
  // Symbol table
  static const int SYM_END = 0;
  static const int SYM_KEY = 1;
  static const int SYM_VALUE = 2;
  // Delimiter
  // Can be customized according to different scenarios, such as
  // e.g.1 cpu:12,memory:33
  // e.g.2 user=1024&pass=****
  const char SYM_KV_SEP; // The separator':' in the middle of key:value
  const char SYM_PAIR_SEP; // kv, the separator in the middle of kv','
  // Temporary variables
  bool allow_space_; // Whether to allow spaces
  int token_;
  char key_buf_[MAX_TOKEN_SIZE];
  char value_buf_[MAX_TOKEN_SIZE];
  const char *cur_;
  char *data_;
  int64_t data_length_;
  // Call back every time a KV pair is parsed
  ObKVMatchCb *cb_;
  common::ObArenaAllocator allocator_;

  DISALLOW_COPY_AND_ASSIGN(ObKVParser);
};

}
}
#endif /* __OCEANBASE_SHARE_KV_PARSER_H__ */
//// end of header file
