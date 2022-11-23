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

#ifndef __OB_TOOLS_CMD_ARGS_PARSER_H__
#define __OB_TOOLS_CMD_ARGS_PARSER_H__
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <cassert>
class CmdArgsParser
{
  const static int64_t MAX_N_ARGS = 1<<10;
  struct arg_t {
    arg_t(): name_(NULL), value_(null_), default_value_(NULL) {}
    ~arg_t() {}
    const char* name_;
    const char* value_;
    const char* default_value_;
  };
  public:
    CmdArgsParser(): parse_seq_(0), n_args_(0) {
      default_arg_.name_ = "*default*";
      default_arg_.value_ = null_;
      default_arg_.default_value_ = null_;
    }
    ~CmdArgsParser() {}
    bool reset() {
      memset(args_, 0, sizeof(args_));
      n_args_ = 0;
      parse_seq_ |= 1;
      return true;
    }
    bool check(int argc, char** argv, ...) {
      bool args_is_valid = true;
      char* p = NULL;
      parse_seq_ = (parse_seq_&~1) + 2;

      for(int64_t i = 0; i < n_args_/2; i++) {
        arg_t arg = args_[i];
        args_[i] = args_[n_args_-1-i];
        args_[n_args_-1-i] = arg;
      }
      for(int64_t i = 0; i < argc; i++) {
        if (argv[i][0] == ':' || NULL == (p = strchr(argv[i], '=')))continue;
        *p++ = 0;
        arg_t* arg = get_arg(argv[i]);
        if (arg && &default_arg_ != arg) arg->value_ = p;
        *--p = '=';
      }
      for(int64_t i = 0; i < argc; i++) {
        if (argv[i][0] != ':' && (p = strchr(argv[i], '=')))continue;
        p = argv[i][0] == ':'? argv[i]+1: argv[i];
        arg_t* arg = get_next_unset_arg();
        if (arg && arg->name_) arg->value_ = p;
      }
      for(int64_t i = 0; i < n_args_; i++) {
        if (null_ == args_[i].value_ && args_[i].default_value_)
          args_[i].value_ = args_[i].default_value_;
        if (null_ == args_[i].value_)args_is_valid = false;
      }
      if (0 == strcmp("true", getenv("dump_args")?:"false"))
      {
        dump(argc, argv);
      }
      return args_is_valid;
    }

    void dump(int argc, char** argv) {
      printf("cmd_args_parser.dump:\n");
      for(int64_t i = 0; i < argc; i++) {
        printf("argv[%ld]=%s\n", i, argv[i]);
      }
      for(int64_t i = 0; i < n_args_; i++) {
        printf("args[%ld]={name=%s, value=%s, default=%s}\n",
               i, args_[i].name_, args_[i].value_, args_[i].default_value_);
      }
    }

    arg_t* get_next_unset_arg() {
      for(int64_t i = 0; i < n_args_; i++) {
        if (null_ == args_[i].value_)
          return args_ + i;
      }
      return NULL;
    }

    arg_t* get_arg(const char* name, const char* default_value = NULL) {
      assert(n_args_ < MAX_N_ARGS && name);
      if (parse_seq_&1) {
        args_[n_args_].name_ = name;
        args_[n_args_].default_value_ = default_value;
        args_[n_args_].value_ = null_;
        return args_ + (n_args_++);
      }
      for(int64_t i = 0; i < n_args_; i++) {
        if (0 == strcmp(args_[i].name_, name))
          return args_ + i;
      }
      return &default_arg_;
    }
  private:
    static const char* null_;
    int64_t parse_seq_;
    int64_t n_args_;
    arg_t default_arg_;
    arg_t args_[MAX_N_ARGS];
};
inline bool argv1_match_func(const char* argv1, const char* func)
{
  const char* last_part = strrchr(func, '.');
  if (NULL != last_part)
  {
    last_part++;
  }
  else
  {
    last_part = func;
  }
  return 0 == strcmp(last_part, argv1);
}

const char* CmdArgsParser::null_  __attribute__ ((weak)) = "*null*" ;
CmdArgsParser __cmd_args_parser __attribute__ ((weak));
#define _Arg(name, ...) __cmd_args_parser.get_arg(#name,  ##__VA_ARGS__)
#define BoolArg(name, ...) (0 == atoll(__cmd_args_parser.get_arg(#name,  ##__VA_ARGS__)->value_)) ? true : false
#define IntArg(name, ...) atoll(__cmd_args_parser.get_arg(#name,  ##__VA_ARGS__)->value_)
#define StrArg(name, ...) __cmd_args_parser.get_arg(#name,  ##__VA_ARGS__)->value_
#define CmdCall(argc, argv, func, ...) \
  (argc >= 2 && argv1_match_func(argv[1], #func) && __cmd_args_parser.reset() && __cmd_args_parser.check(argc-2, argv+2, ##__VA_ARGS__))? \
  func(__VA_ARGS__)

#define CmdCallSimple(argc, argv, func) \
  (argc >= 3 && argv1_match_func(argv[1], #func))? \
  func(argc - 2, argv + 2)
#endif /* __OB_TOOLS_CMD_ARGS_PARSER_H__ */
