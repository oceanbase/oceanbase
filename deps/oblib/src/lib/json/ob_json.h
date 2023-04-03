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

#ifndef LIB_JSON_OB_JSON_
#define LIB_JSON_OB_JSON_
#include <sys/types.h>
#include <regex.h>
#include "lib/list/ob_dlist.h"
#include "lib/string/ob_string.h"
#include "lib/allocator/page_arena.h"
#include "common/data_buffer.h"
#include "lib/list/ob_list.h"
class ObJsonTest_token_test_Test;
class ObJsonTest_basic_test_Test;

namespace oceanbase
{
namespace json
{
////////////////////////////////////////////////////////////////
// Json types
enum Type
{
  JT_NULL,
  JT_TRUE,
  JT_FALSE,
  JT_STRING,
  JT_NUMBER/*Only support integer now*/,
  JT_ARRAY,
  JT_OBJECT
};
struct Pair;
struct Value;
typedef ::oceanbase::common::ObDList<Pair> Object; // list of Pairs
typedef ::oceanbase::common::ObDList<Value> Array;  // list of Values
typedef ::oceanbase::common::ObString String;
typedef int64_t Number;
struct Value;
struct Pair: public ::oceanbase::common::ObDLinkBase<Pair>
{
  enum
  {
    FT_HIDE = 0,
    FT_PASS = 1,
  };

  String name_;
  Value *value_;
  int filt_flag_;

  Pair(): name_(), value_(NULL), filt_flag_(FT_HIDE) {}
  virtual ~Pair() {}
};
struct Value: public common::ObDLinkBase<Value>
{
public:
  Value() : type_(JT_NULL), int_(0), str_(), obj_(), arr_() {}
  virtual ~Value() {}

  Type get_type() const { return type_; }
  Number get_number() const { return int_; }
  const String &get_string() const { return str_; }
  const Object &get_object() const { return obj_; }
  const Array &get_array() const { return arr_; }
  Object &get_object() { return obj_; }
  Array &get_array() { return arr_; }

  void set_type(Type t) { type_ = t; }
  void set_int(int64_t v) { int_ = v; }
  void set_string(char *buf, int32_t buf_len) { str_.assign_ptr(buf, buf_len); }
  void object_add(Pair *pair) { obj_.add_last(pair); }
  void array_add(Value *v) { arr_.add_last(v); }
private:
  Type type_;
  int64_t int_;
  String str_;
  Object obj_;
  Array arr_;
};
////////////////////////////////////////////////////////////////
// callback utility of Parser
class JsonProcessor
{
public:
  virtual ~JsonProcessor() {}
  virtual void process_value(String &key, Value *value) = 0;
};
class ToStringJsonProcessor : public json::JsonProcessor
{
public:
  virtual void process_value(json::String &key, json::Value *value)
  {
    if ((key == N_TID) && (value->get_type() == JT_NUMBER) &&
        (value->get_number() <= 0)) {
      value->set_type(JT_NULL);
    }
  }
};

class Parser
{
public:
  Parser(): cur_token_(), allocator_(NULL), json_processor_(NULL) {}
  virtual ~Parser() {}

  void reset()
  {
    cur_token_.reset();
    allocator_ = NULL;
    json_processor_ = NULL;
  }

  int init(common::ObArenaAllocator *allocator, JsonProcessor *json_processor = NULL);

  int parse(const char *buf, const int64_t buf_len, Value *&root);
private:
  // types and constants
  enum TokenType
  {
    TK_INVALID = -1,
    TK_NUMBER = 258,
    TK_STRING = 259,
    TK_TRUE = 260,
    TK_FALSE = 261,
    TK_NULL = 262,
  };
  struct Token
  {
    void reset()
    {
      type_ = 0;
      value_.int_ = 0;
      value_.str_.begin_ = NULL;
      value_.str_.end_ = NULL;
    }

    int type_;
    union
    {
      int64_t int_;
      struct
      {
        const char *begin_;
        const char *end_;
      } str_;
    } value_;
  };
private:
  friend class ::ObJsonTest_token_test_Test;
  friend class ::ObJsonTest_basic_test_Test;
  // function members
  int token(const char *&begin, const char *end, Token &token);

  int parse_value(const char *&begin, const char *end, Value *&value);
  int parse_array(const char *&begin, const char *end, Value *&arr);
  int parse_pair(const char *&begin, const char *end, Pair *&pair);
  int parse_object(const char *&begin, const char *end, Value *&obj);

  int alloc_value(Type t, Value *&value);
  int alloc_pair(Pair *&pair);
private:
  // data members
  Token cur_token_;
  common::ObArenaAllocator *allocator_;
  JsonProcessor *json_processor_;
private:
  DISALLOW_COPY_AND_ASSIGN(Parser);
};
////////////////////////////////////////////////////////////////
class Walker
{
public:
  explicit Walker(const Value *root) : root_(root), step_in_(true) {}
  virtual ~Walker() {}
  void reset()
  {
    root_ = NULL;
    step_in_ = true;
  }

  int go();
protected:
  virtual int on_null(int level, Type parent) = 0;
  virtual int on_true(int level, Type parent) = 0;
  virtual int on_false(int level, Type parent) = 0;
  virtual int on_string(int level, Type parent, const String &str) = 0;
  virtual int on_number(int level, Type parent, const Number &num) = 0;
  virtual int on_array_start(int level, Type parent, const Array &arr) = 0;
  virtual int on_array_end(int level, Type parent, const Array &arr) = 0;
  virtual int on_array_item_start(int level, Type parent, const Value *val) = 0;
  virtual int on_array_item_end(int level, Type parent, const Value *val, bool is_last) = 0;
  virtual int on_object_start(int level, Type parent, const Object &obj) = 0;
  virtual int on_object_end(int level, Type parent, const Object &obj) = 0;
  virtual int on_object_member_start(int level, Type parent, const Pair *kv) = 0;
  virtual int on_object_member_end(int level, Type parent, const Pair *kv, bool is_last) = 0;
  virtual int on_walk_start() = 0;
  virtual int on_walk_end() = 0;
private:
  // function members
  int step(int level, const Value *node, Type parent);
protected:
  // data members
  const Value *root_;
  bool step_in_;
private:
  DISALLOW_COPY_AND_ASSIGN(Walker);
};

struct Tidy: public Walker
{
  explicit Tidy(const Value *root): Walker(root), print_buffer_() {}
  virtual ~Tidy() {}

  void reset()
  {
    print_buffer_.reset();
    Walker::reset();
  }

  int64_t to_string(char *buf, const int64_t buf_len) const;
  static const int64_t MAX_PRINTABLE_SIZE = 2 * 1024 * 1024;
protected:
  virtual int on_null(int level, Type parent);
  virtual int on_true(int level, Type parent);
  virtual int on_false(int level, Type parent);
  virtual int on_string(int level, Type parent, const String &str);
  virtual int on_number(int level, Type parent, const Number &num);
  virtual int on_array_start(int level, Type parent, const Array &arr);
  virtual int on_array_end(int level, Type parent, const Array &arr);
  virtual int on_array_item_start(int level, Type parent, const Value *val);
  virtual int on_array_item_end(int level, Type parent, const Value *val, bool is_last);
  virtual int on_object_start(int level, Type parent, const Object &obj);
  virtual int on_object_end(int level, Type parent, const Object &obj);
  virtual int on_object_member_start(int level, Type parent, const Pair *kv);
  virtual int on_object_member_end(int level, Type parent, const Pair *kv, bool is_last);
  virtual int on_walk_start() { return common::OB_SUCCESS; }
  virtual int on_walk_end() { return common::OB_SUCCESS; }

  int print_indent(int level);
protected:
  mutable common::ObDataBuffer print_buffer_;
};

class RegexFilter : public Tidy
{
  typedef common::ObList<regex_t, common::ObArenaAllocator> RegexList;
public:
  RegexFilter();
  virtual ~RegexFilter();
  void reset();
public:
  virtual int on_obj_name(common::ObDataBuffer &path_buffer, bool &is_match) const;
public:
  void register_regex(const char *pattern);
  void set_root(const Value *root) { root_ = root; }
protected:
  virtual int on_walk_start();
  virtual int on_object_member_start(int level, Type parent, const Pair *kv);
  virtual int on_object_member_end(int level, Type parent, const Pair *kv, bool is_last);
private:
  int mark_need_print(Value *node,
                      common::ObDataBuffer path_buffer,
                      bool &is_need_print) const;
private:
  common::ObArenaAllocator allocator_;
  RegexList regex_list_;
};

class JsonFilter
{
public:
  JsonFilter(const char **regexs,
             const int64_t regexn)
    : allocator_(common::ObModIds::OB_SQL_PHY_PLAN_STRING),
      parser_(),
      filter_(),
      root_(NULL)
  {
    int ret = common::OB_SUCCESS;
    root_ = NULL;
    parser_.init(&allocator_, NULL);
    for (int64_t i = 0; OB_SUCC(ret) && i < regexn; ++i) {
      filter_.register_regex(regexs[i]);
    }
  }
  virtual ~JsonFilter() {}
public:
  void reset()
  {
    allocator_.reuse(); //FIXME ?
    parser_.reset();
    filter_.reset();
    root_ = NULL;
  };
  void set_buf(const char *buf, const int64_t buf_len)
  {
    parser_.parse(buf, buf_len, root_);
    filter_.set_root(root_);
  };
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    return filter_.to_string(buf, buf_len);
  };
private:
  common::ObArenaAllocator allocator_;
  json::Parser parser_;
  json::RegexFilter filter_;
  json::Value *root_;
};

class Path
{
public:
  explicit Path(Value *root): root_(root) {}
  virtual ~Path() {}
  int for_all_path();
  virtual int on_path(String &path, Pair *kv) = 0;
private:
  // function members
  int iterate(Value *node,
              common::ObDataBuffer path_buffer);
private:
  // data members
  Value *root_;
private:
  DISALLOW_COPY_AND_ASSIGN(Path);
};
/*
typedef int (*QueryCallback)(Pair* kv);
class Query: private Path
{
  public:
    Query(Value *root):Path(root){};
    virtual ~Query() {};
    int register_callback(const char* pattern, QueryCallback cb);
    int go();
  private:
    // types and constants
    typedef common::ObArray<std::pair<regex_t, QueryCallback> > Callbacks;
  private:
    // disallow copy
    Query(const Query &other);
    Query& operator=(const Query &other);
    // function members
    virtual int on_path(String &path, Pair* kv);
  private:
    // data members
    Callbacks callbacks_;
};
*/
} // end namespace json
} // end namespace oceanbase

#endif /* LIB_JSON_OB_JSON_ */
