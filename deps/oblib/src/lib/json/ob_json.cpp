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

#define USING_LOG_PREFIX LIB
#include "lib/utility/ob_hang_fatal_error.h"
#include "common/ob_smart_call.h"
#include "lib/json/ob_json.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/utility.h"
using namespace oceanbase::common;

namespace oceanbase
{
namespace json
{
int Parser::token(const char *&begin, const char *end, Token &token)
{
  int ret = OB_ITER_END;
  const char *c = begin;
  if (OB_ISNULL(c)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("begin ptr is NULL", K(ret));
  } else {
    // skip whitespace
    while (c != end) {
      if (isspace(*c)) {
        c++;
      } else {
        break;
      }
    }
    if (c != end) {
      switch (*c) {
        case '{':
        case '}':
        case '[':
        case ']':
        case ',':
        case ':':
          ret = OB_SUCCESS;
          token.type_ = *c;
          c++;
          break;
        case 't':
        case 'T':
          // true ?
          if (end - c >= 4
              && 0 == strncasecmp("true", c, 4)) {
            ret = OB_SUCCESS;
            token.type_ = TK_TRUE;
            c += 4;
          } else {
            ret = OB_ERR_PARSER_SYNTAX;
          }
          break;
        case 'f':
        case 'F':
          // false ?
          if (end - c >= 5
              && 0 == strncasecmp("false", c, 5)) {
            ret = OB_SUCCESS;
            token.type_ = TK_FALSE;
            c += 5;
          } else {
            ret = OB_ERR_PARSER_SYNTAX;
          }
          break;
        case 'n':
        case 'N':
          // null ?
          if (end - c >= 4
              && 0 == strncasecmp("null", c, 4)) {
            ret = OB_SUCCESS;
            token.type_ = TK_NULL;
            c += 4;
          } else {
            ret = OB_ERR_PARSER_SYNTAX;
          }
          break;
        case '-':
        case '0':
        case '1':
        case '2':
        case '3':
        case '4':
        case '5':
        case '6':
        case '7':
        case '8':
        case '9': {
          // number
          ret = OB_SUCCESS;
          token.type_ = TK_NUMBER;
          bool is_neg = false;
          if (*c == '-') {
            ++c;
            if (c != end
                && (*c >= '0' && *c <= '9')) {
              token.value_.int_ = *c - '0';
              c++;
              is_neg = true;
            } else {
              ret = OB_ERR_PARSER_SYNTAX;
            }
          } else {
            token.value_.int_ = *c - '0';
            c++;
          }
          if (OB_SUCC(ret)) {
            while (c != end) {
              if (*c >= '0' && *c <= '9') {
                token.value_.int_ = token.value_.int_ * 10 + (*c - '0');
              } else {
                break;
              }
              c++;
            }
          }
          if (OB_SUCC(ret) && is_neg) {
            token.value_.int_ = - token.value_.int_;
          }
          break;
        }
        case '"': {
          // string
          token.type_ = TK_STRING;
          token.value_.str_.begin_ = c + 1;
          ++c;
          while (c != end && *c != '"') {
            c++;
          }
          if (c != end) {
            token.value_.str_.end_ = c++;
            ret = OB_SUCCESS;
          } else {
            ret = OB_ERR_PARSER_SYNTAX;
          }
          break;
        }
        default:
          ret = OB_ERR_PARSER_SYNTAX;
          break;
      }
    } // end if
    if (OB_SUCC(ret)) {
      begin = c;
    } else {
      token.type_ = TK_INVALID;
    }
  }
  return ret;
}

int Parser::init(common::ObArenaAllocator *allocator, JsonProcessor *json_processor)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator is NULL", K(ret));
  } else {
    allocator_ = allocator;
    json_processor_ = json_processor;
  }
  return ret;
}

int Parser::alloc_value(Type t, Value *&value)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator not set", K(ret));
  } else if (OB_ISNULL(value = (Value *)allocator_->alloc(sizeof(Value)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("no memory", K(ret));
  } else {
    value = new(value) Value();
    value->set_type(t);
  }
  return ret;
}

int Parser::alloc_pair(Pair *&pair)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator not set", K(ret));
  } else if (OB_ISNULL(pair = (Pair *)allocator_->alloc(sizeof(Pair)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("no memory", K(ret));
  } else {
    pair = new(pair) Pair();
  }
  return ret;
}

int Parser::parse_value(const char *&begin, const char *end, Value *&value)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(token(begin, end, cur_token_))) {
    LOG_WARN("fail to get token", K(ret));
  } else {
    switch (cur_token_.type_) {
      case TK_STRING:
        if (OB_FAIL(alloc_value(JT_STRING, value))) {
          LOG_WARN("fail to alloc value", K(ret));
        } else if (OB_ISNULL(value)) {
          ret = OB_ERR_UNEXPECTED;;
          LOG_WARN("succ to alloc value, but value is NULL", K(ret));
        } else {
          value->set_string(const_cast<char *>(cur_token_.value_.str_.begin_),
                            static_cast<int32_t>(cur_token_.value_.str_.end_ -
                                                 cur_token_.value_.str_.begin_));
        }
        break;
      case TK_NUMBER:
        if (OB_FAIL(alloc_value(JT_NUMBER, value))) {
          LOG_WARN("fail to alloc value", K(ret));
        } else if (OB_ISNULL(value)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("succ to alloc value, but value is NULL", K(ret));
        } else {
          value->set_int(cur_token_.value_.int_);
        }
        break;
      case TK_TRUE:
        if (OB_FAIL(alloc_value(JT_TRUE, value))) {
          LOG_WARN("fail to alloc value", K(ret));
        } else if (OB_ISNULL(value)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("succ to alloc value, but value is NULL", K(ret));
        } else {}
        break;
      case TK_FALSE:
        if (OB_FAIL(alloc_value(JT_FALSE, value))) {
          LOG_WARN("fail to alloc value", K(ret));
        } else if (OB_ISNULL(value)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("succ to alloc value, but value is NULL", K(ret));
        } else {}
        break;
      case TK_NULL:
        if (OB_FAIL(alloc_value(JT_NULL, value))) {
          LOG_WARN("fail to alloc value", K(ret));
        } else if (OB_ISNULL(value)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("succ to alloc value, but value is NULL", K(ret));
        } else {}
        break;
      case '{':
        if (OB_FAIL(SMART_CALL(parse_object(begin, end, value)))) {
          LOG_WARN("fail to parse object", K(ret));
        } else {}
        break;
      case '[':
        if (OB_FAIL(SMART_CALL(parse_array(begin, end, value)))) {
          LOG_WARN("fail to parse array", K(ret));
        } else {}
        break;
      default:
        ret = OB_ERR_PARSER_SYNTAX;
        // no need to print warn log, the caller will descide wether it is valid
        LOG_INFO("invalid token type, maybe it is valid empty json type", K_(cur_token_.type), K(ret));
        break;
    }
  }
  return ret;
}

int Parser::parse_array(const char *&begin, const char *end, Value *&arr)
{
  int ret = OB_SUCCESS;
  Value *value = NULL;
  if (OB_FAIL(alloc_value(JT_ARRAY, arr))) {
    LOG_WARN("fail to alloc value", K(ret));
  } else if (OB_ISNULL(arr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("succ to alloc value, but value is NULL", K(ret));
  } else if (OB_FAIL(SMART_CALL(parse_value(begin, end, value)))) {
    if (cur_token_.type_ == ']') {
      ret = OB_SUCCESS;
      // empty array
    } else {
      LOG_WARN("invalid array", K_(cur_token_.type), K(ret));
    }
  } else {
    arr->array_add(value);
    bool is_finish = false;
    while (OB_SUCC(ret)
           && !is_finish
           && OB_SUCC(token(begin, end, cur_token_))) {
      switch (cur_token_.type_) {
        case ',':
          if (OB_SUCC(SMART_CALL(parse_value(begin, end, value)))) {
            arr->array_add(value);
          }
          break;
        case ']':
          is_finish = true;
          break;
        default:
          ret = OB_ERR_PARSER_SYNTAX;
          break;
      }
    }
  }
  return ret;
}

int Parser::parse_pair(const char *&begin, const char *end, Pair *&pair)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(token(begin, end, cur_token_))) {
    LOG_WARN("fail to get token", K(ret));
  } else if (cur_token_.type_ != TK_STRING) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("type of current token is not TK_STRING", K(ret), K(cur_token_.type_));
  } else {
    if (OB_FAIL(alloc_pair(pair))) {
      LOG_WARN("fail to alloc pair", K(ret));
    } else if (OB_ISNULL(pair)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("succ to alloc pair, but pair is NULL", K(ret));
    } else {
      pair->name_.assign_ptr(const_cast<char *>(cur_token_.value_.str_.begin_),
                             static_cast<int32_t>(cur_token_.value_.str_.end_ -
                                                  cur_token_.value_.str_.begin_));
      if (OB_FAIL(token(begin, end, cur_token_))) {
        LOG_WARN("fail to get token", K(ret));
      } else if (':' != cur_token_.type_) {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("invalid pair", K(cur_token_.type_));
      } else if (OB_FAIL(SMART_CALL(parse_value(begin, end, pair->value_)))) {
        if (cur_token_.type_ != '}' && begin != end) {
          LOG_WARN("lack of member value", K(ret), K(pair->name_), KCSTRING(begin));
        } else {}
      } else if (NULL != json_processor_) {
        json_processor_->process_value(pair->name_, pair->value_);
      } else {}
    }
  }
  return ret;
}

int Parser::parse_object(const char *&begin, const char *end, Value *&obj)
{
  int ret = OB_SUCCESS;
  Pair *pair = NULL;
  if (OB_FAIL(alloc_value(JT_OBJECT, obj))) {
    LOG_WARN("fail to alloc value", K(ret));
  } else if (OB_ISNULL(obj)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("succ to alloc value, but value is NULL", K(ret));
  } else if (OB_FAIL(SMART_CALL(parse_pair(begin, end, pair)))) {
    if ('}' == cur_token_.type_) {
      // empty object
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("invalid object", K_(cur_token_.type), K(ret));
    }
  } else {
    obj->object_add(pair);
    bool is_finish = false;
    while (OB_SUCC(ret)
           && !is_finish
           && OB_SUCC(token(begin, end, cur_token_))) {
      switch (cur_token_.type_) {
        case ',':
          if (OB_SUCC(SMART_CALL(parse_pair(begin, end, pair)))) {
            obj->object_add(pair);
          } else {}
          break;
        case '}':
          is_finish = true;
          break;
        default:
          break;
      }
    }
  }
  return ret;
};

int Parser::parse(const char *buf, const int64_t buf_len, Value *&root)
{
  int ret = OB_SUCCESS;
  const char *begin = buf;
  const char *end = buf + buf_len;
  cur_token_.type_ = TK_INVALID;
  Token tok;
  if (OB_SUCC(token(begin, end, tok))) {
    switch (tok.type_) {
      case '{':
        if (OB_FAIL(SMART_CALL(parse_object(begin, end, root)))) {
          LOG_WARN("fail to parse object", K(ret));
        } else {}
        break;
      case '[':
        if (OB_FAIL(SMART_CALL(parse_array(begin, end, root)))) {
          LOG_WARN("fail to parse array", K(ret));
        } else {}
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid token type", K(ret), K(tok.type_));
    }
  }
  return ret;
}
////////////////////////////////////////////////////////////////
int Walker::step(int level, const Value *node, Type parent)
{
  int ret = OB_SUCCESS;
  if (NULL != node) {
    switch (node->get_type()) {
      case JT_NULL:
        if (OB_FAIL(on_null(level, parent))) {
          LOG_WARN("fail to run on_null", K(ret), K(level));
        } else {}
        break;
      case JT_TRUE:
        if (OB_FAIL(on_true(level, parent))) {
          LOG_WARN("fail to run on_true", K(ret), K(level));
        } else {}
        break;
      case JT_FALSE:
        if (OB_FAIL(on_false(level, parent))) {
          LOG_WARN("fail to run on_false", K(ret), K(level));
        } else {}
        break;
      case JT_STRING:
        if (OB_FAIL(on_string(level, parent, node->get_string()))) {
          LOG_WARN("fail to run on_string", K(ret), K(level));
        } else {}
        break;
      case JT_NUMBER:
        if (OB_FAIL(on_number(level, parent, node->get_number()))) {
          LOG_WARN("fail to run on_number", K(ret), K(level));
        } else {}
        break;
      case JT_ARRAY: {
        const Array &arr = node->get_array();
        if (OB_FAIL(on_array_start(level, parent, arr))) {
          LOG_WARN("fail to run on_array_start", K(ret), K(level));
        } else {}
        DLIST_FOREACH(subnode, arr) {
          if (OB_FAIL(on_array_item_start(level, parent, subnode))) {
            LOG_WARN("fail to run on_array_item_start", K(ret), K(level));
          } else if (OB_FAIL(step(level + 1, subnode, JT_ARRAY))) {
            LOG_WARN("fail to run step", K(ret), K(level));
          } else if (OB_FAIL(on_array_item_end(
                      level, parent, subnode, subnode == arr.get_last()))) {
            LOG_WARN("fail to run on_array_item_end", K(ret), K(level));
          } else {}
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(on_array_end(level, parent, arr))) {
          LOG_WARN("fail to run on_array_end", K(ret), K(level));
        } else {}
        break;
      }
      case JT_OBJECT: {
        const Object &obj = node->get_object();
        if (OB_FAIL(on_object_start(level, parent, obj))) {
          LOG_WARN("fail to run on_object_start", K(ret), K(level));
        } else {}
        DLIST_FOREACH(kv, obj) {
          if (OB_ISNULL(kv)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("kv is NULL", K(ret));
          } else if (OB_FAIL(on_object_member_start(level, parent, kv))) {
            LOG_WARN("fail to run on_object_member_start", K(ret), K(level));
          } else {
            if (step_in_) {
              if (OB_FAIL(step(level + 1, kv->value_, JT_OBJECT))) {
                LOG_WARN("fail to run step", K(ret), K(level));
              } else {}
            }
            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(on_object_member_end(
                        level, parent, kv, kv == obj.get_last()))) {
              LOG_WARN("fail to run on_object_member_end", K(ret), K(level));
            }
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(on_object_end(level, parent, obj))) {
          LOG_WARN("fail to run on_object_end", K(ret), K(level));
        } else {}
        break;
      }
      default:
        break;
    }
  }
  return ret;
}

int Walker::go()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(on_walk_start())) {
    LOG_WARN("fail to run on_walk_start", K(ret));
  } else {
    if (step_in_) {
      if (OB_FAIL(step(0, root_, JT_NULL))) {
        LOG_WARN("fail to run step", K(ret));
      } else {}
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(on_walk_end())) {
    LOG_WARN("fail to run on_walk_end", K(ret));
  } else {}
  return ret;
}

////////////////////////////////////////////////////////////////
int64_t Tidy::to_string(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  (void)print_buffer_.set_data(buf, buf_len);
  if (OB_FAIL(const_cast<Tidy *>(this)->go())) {
    databuff_printf(buf, buf_len, pos, "WARN failed to go, ret=%d", ret);
  } else {}
  return print_buffer_.get_position();
}

int Tidy::print_indent(int level)
{
  int ret = OB_SUCCESS;
  char *buf = print_buffer_.get_data();
  const int64_t buf_len = print_buffer_.get_capacity();
  int64_t &pos = print_buffer_.get_position();

  for (int i = 0; OB_SUCC(ret) && i < level; ++i) {
    if (OB_FAIL(BUF_PRINTF("  "))) {
      LOG_WARN("fail to printf", K(ret));
    } else {}
  } // end for
  return ret;
}

int Tidy::on_array_start(int level, Type parent, const Array &arr)
{
  char *buf = print_buffer_.get_data();
  const int64_t buf_len = print_buffer_.get_capacity();
  int64_t &pos = print_buffer_.get_position();
  UNUSED(arr);
  UNUSED(parent);
  UNUSED(level);
  //  if (JT_OBJECT == parent) {
  //    BUF_PRINTF("\n");
  //    print_indent(level);
  //  }
  return BUF_PRINTF(" [\n");
}

int Tidy::on_array_end(int level, Type parent, const Array &arr)
{
  UNUSED(parent);
  UNUSED(arr);
  int ret = OB_SUCCESS;
  char *buf = print_buffer_.get_data();
  const int64_t buf_len = print_buffer_.get_capacity();
  int64_t &pos = print_buffer_.get_position();
  if (OB_FAIL(print_indent(level))) {
    LOG_WARN("fail to print indent", K(ret));
  } else if (OB_FAIL(BUF_PRINTF("]"))) {
    LOG_WARN("fail to buf printf", K(ret));
  } else {}
  return ret;
}

int Tidy::on_object_start(int level, Type parent, const Object &obj)
{
  UNUSED(obj);
  UNUSED(level);
  UNUSED(parent);
  int ret = OB_SUCCESS;
  char *buf = print_buffer_.get_data();
  const int64_t buf_len = print_buffer_.get_capacity();
  int64_t &pos = print_buffer_.get_position();
  //  if (JT_OBJECT == parent) {
  //    BUF_PRINTF("\n");
  //    print_indent(level);
  //  }
  if (level > 0) {
    if (OB_FAIL(BUF_PRINTF(" "))) {
      LOG_WARN("fail to buf printf", K(ret));
    } else {}
  }
  if (0 == obj.get_size()) {
    if (OB_FAIL(BUF_PRINTF("{"))) {
      LOG_WARN("fail to buf printf", K(ret));
    } else {}
  } else {
    if (OB_FAIL(BUF_PRINTF("{\n"))) {
      LOG_WARN("fail to buf printf", K(ret));
    } else {}
  }
  return ret;
}

int Tidy::on_object_end(int level, Type parent, const Object &obj)
{
  UNUSED(parent);
  UNUSED(obj);
  int ret = OB_SUCCESS;
  char *buf = print_buffer_.get_data();
  const int64_t buf_len = print_buffer_.get_capacity();
  int64_t &pos = print_buffer_.get_position();
  if (0 == obj.get_size()) {
    if (OB_FAIL(BUF_PRINTF(" }"))) {
      LOG_WARN("fail to buf printf", K(ret));
    } else {}
  } else {
    if (OB_FAIL(print_indent(level))) {
      LOG_WARN("fail to print indent", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("}"))) {
      LOG_WARN("fail to buf printf", K(ret));
    } else {}
  }
  return OB_SUCCESS;
}

int Tidy::on_array_item_start(int level, Type parent, const Value *val)
{
  UNUSED(parent);
  UNUSED(val);
  return print_indent(level + 1);
}

int Tidy::on_array_item_end(int level, Type parent, const Value *val, bool is_last)
{
  UNUSED(level);
  UNUSED(parent);
  UNUSED(val);
  int ret = OB_SUCCESS;
  char *buf = print_buffer_.get_data();
  const int64_t buf_len = print_buffer_.get_capacity();
  int64_t &pos = print_buffer_.get_position();
  if (is_last) {
    if (OB_FAIL(BUF_PRINTF("\n"))) {
      LOG_WARN("fail to buf printf", K(ret));
    } else {}
  } else {
    if (OB_FAIL(BUF_PRINTF(",\n"))) {
      LOG_WARN("fail to buf printf", K(ret));
    } else {}
  }
  return ret;
}

int Tidy::on_object_member_end(int level, Type parent, const Pair *kv, bool is_last)
{
  UNUSED(level);
  UNUSED(parent);
  UNUSED(kv);
  int ret = OB_SUCCESS;
  char *buf = print_buffer_.get_data();
  const int64_t buf_len = print_buffer_.get_capacity();
  int64_t &pos = print_buffer_.get_position();
  if (is_last) {
    if (OB_FAIL(BUF_PRINTF("\n"))) {
      LOG_WARN("fail to buf printf", K(ret));
    } else {}
  } else {
    if (OB_FAIL(BUF_PRINTF(",\n"))) {
      LOG_WARN("fail to buf printf", K(ret));
    } else {}
  }
  return ret;
}

int Tidy::on_null(int level, Type parent)
{
  UNUSED(level);
  UNUSED(parent);
  char *buf = print_buffer_.get_data();
  const int64_t buf_len = print_buffer_.get_capacity();
  int64_t &pos = print_buffer_.get_position();
  return BUF_PRINTF("null");
}

int Tidy::on_true(int level, Type parent)
{
  UNUSED(level);
  UNUSED(parent);
  char *buf = print_buffer_.get_data();
  const int64_t buf_len = print_buffer_.get_capacity();
  int64_t &pos = print_buffer_.get_position();
  return BUF_PRINTF("true");
}

int Tidy::on_false(int level, Type parent)
{
  UNUSED(level);
  UNUSED(parent);
  char *buf = print_buffer_.get_data();
  const int64_t buf_len = print_buffer_.get_capacity();
  int64_t &pos = print_buffer_.get_position();
  return BUF_PRINTF("false");
}

int Tidy::on_string(int level, Type parent, const String &str)
{
  UNUSED(level);
  UNUSED(parent);
  char *buf = print_buffer_.get_data();
  const int64_t buf_len = print_buffer_.get_capacity();
  int64_t &pos = print_buffer_.get_position();
  return BUF_PRINTF("\"%.*s\"", str.length(), str.ptr());
}

int Tidy::on_number(int level, Type parent, const Number &num)
{
  UNUSED(level);
  UNUSED(parent);
  char *buf = print_buffer_.get_data();
  const int64_t buf_len = print_buffer_.get_capacity();
  int64_t &pos = print_buffer_.get_position();
  return BUF_PRINTF("%ld", num);
}

int Tidy::on_object_member_start(int level, Type parent, const Pair *kv)
{
  UNUSED(parent);
  int ret = OB_SUCCESS;
  char *buf = print_buffer_.get_data();
  const int64_t buf_len = print_buffer_.get_capacity();
  int64_t &pos = print_buffer_.get_position();
  if (OB_ISNULL(kv)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("kv is NULL", K(ret));
  } else if (OB_FAIL(print_indent(level + 1))) {
    LOG_WARN("fail to print indent", K(ret));
  } else if (OB_FAIL(BUF_PRINTF("\"%.*s\":",
                                kv->name_.length(),
                                kv->name_.ptr()))) {
    LOG_WARN("fail to buf printf", K(ret));
  } else {}
  return ret;
}

////////////////////////////////////////////////////////////////
RegexFilter::RegexFilter(): Tidy(NULL),
                            allocator_(ObModIds::OB_REGEX),
                            regex_list_(allocator_)
{
}

RegexFilter::~RegexFilter() {}

void RegexFilter::reset()
{
  regex_list_.reset();
  Tidy::reset();
}

int RegexFilter::on_obj_name(common::ObDataBuffer &path_buffer, bool &is_match) const
{
  int ret = OB_SUCCESS;
  is_match = false;
  RegexList::const_iterator iter;
  for (iter = regex_list_.begin(); OB_SUCC(ret) && false == is_match &&
       iter != regex_list_.end(); iter++) {
    int regexec_ret = regexec(&(*iter), path_buffer.get_data(), 0, NULL, 0);
    if (REG_NOMATCH == regexec_ret) {
      // not match, continue
    } else if (0 == regexec_ret) {
      is_match = true;
    } else {
      ret = OB_ERR_REGEXP_ERROR;
      const static int64_t REG_ERR_MSG_BUF_LEN = 512;
      char reg_err_msg[REG_ERR_MSG_BUF_LEN];
      size_t err_msg_len = regerror(regexec_ret, &(*iter), reg_err_msg, REG_ERR_MSG_BUF_LEN);
      LOG_WARN("fail to run match func: regexec", K(ret),
               K(regexec_ret), K(err_msg_len), KCSTRING(reg_err_msg));
    }
  }
  return ret;
}

void RegexFilter::register_regex(const char *pattern)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(NULL != pattern)) {
    regex_t regex;
    int regcomp_ret = 0;
    if (OB_UNLIKELY(0 != (regcomp_ret = regcomp(&regex, pattern, REG_EXTENDED | REG_NOSUB)))) {
      ret = OB_ERR_REGEXP_ERROR;
      LOG_ERROR("regcomp fail", K(ret), K(regcomp_ret), KCSTRING(pattern));
    } else if (OB_FAIL(regex_list_.push_back(regex))) {
      LOG_ERROR("push back to regex list fail", K(ret));
      regfree(&regex);
    } else {
      LOG_INFO("register regex pattern succ", KCSTRING(pattern));
    }
  } else {}
}

int RegexFilter::on_object_member_start(int level, Type parent, const Pair *kv)
{
  UNUSED(parent);
  int ret = OB_SUCCESS;
  char *buf = print_buffer_.get_data();
  const int64_t buf_len = print_buffer_.get_capacity();
  int64_t &pos = print_buffer_.get_position();
  if (OB_ISNULL(kv)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("kv is NULL", K(ret));
  } else if (kv->filt_flag_) {
    if (OB_FAIL(print_indent(level + 1))) {
      LOG_WARN("fail to print indent", K(ret), K(level));
    } else if (OB_FAIL(BUF_PRINTF("\"%.*s\": ", kv->name_.length(), kv->name_.ptr()))) {
      LOG_WARN("fail to buf printf", K(ret), K(kv->name_));
    } else {}
  } else {
    step_in_ = false;
  }
  return ret;
}

int RegexFilter::on_object_member_end(int level, Type parent, const Pair *kv, bool is_last)
{
  UNUSED(level);
  UNUSED(parent);
  UNUSED(kv);
  int ret = OB_SUCCESS;
  char *buf = print_buffer_.get_data();
  const int64_t buf_len = print_buffer_.get_capacity();
  int64_t &pos = print_buffer_.get_position();
  if (OB_ISNULL(kv)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("kv is NULL", K(ret));
  } else if (kv->filt_flag_) {
    if (is_last) {
      if (OB_FAIL(BUF_PRINTF("\n"))) {
        LOG_WARN("fail to buf printf", K(ret));
      } else {}
    } else {
      if (OB_FAIL(BUF_PRINTF(",\n"))) {
        LOG_WARN("fail to buf printf", K(ret));
      } else {}
    }
  } else {
    step_in_ = true;
  }
  return ret;
}

int RegexFilter::on_walk_start()
{
  bool is_need_print = false;
  char data_buffer[4096];
  ObDataBuffer path_buffer;
  (void)path_buffer.set_data(data_buffer, sizeof(data_buffer) - 1);
  return mark_need_print(const_cast<Value *>(root_), path_buffer, is_need_print);
}

int RegexFilter::mark_need_print(Value *node,
                                 common::ObDataBuffer path_buffer,
                                 bool &is_need_print) const
{
  int ret = OB_SUCCESS;
  is_need_print = false;
  if (NULL != node) {
    switch (node->get_type()) {
      case JT_OBJECT:
        DLIST_FOREACH(kv, node->get_object()) {
          ObDataBuffer tmp_buffer = path_buffer;
          if (OB_ISNULL(kv)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("kv is NULL", K(ret));
          } else if (OB_FAIL(databuff_printf(tmp_buffer.get_data(),
                                             tmp_buffer.get_capacity(),
                                             tmp_buffer.get_position(),
                                             "/%.*s",
                                             kv->name_.length(),
                                             kv->name_.ptr()))) {
            LOG_WARN("fail to databuff printf", K(ret));
          } else {
            bool path_is_match = false;
            if (OB_FAIL(on_obj_name(tmp_buffer, path_is_match))) {
              LOG_WARN("fail to run on_obj_name", K(ret));
            } else {
              bool child_node_need_print = false;
              if (true == path_is_match) {
                kv->filt_flag_ = Pair::FT_PASS;
                is_need_print = true;
              } else {}
              if (OB_FAIL(mark_need_print(kv->value_, tmp_buffer, child_node_need_print))) {
                LOG_WARN("fail to mark if child node need print", K(ret));
              } else if (true == child_node_need_print) {
                kv->filt_flag_ = Pair::FT_PASS;
                is_need_print = true;
              } else {}
            }
          }
        }
        break;
      case JT_ARRAY:
        DLIST_FOREACH(subnode, node->get_array()) {
          ObDataBuffer tmp_buffer = path_buffer;
          bool arr_node_need_print = false;
          if (OB_FAIL(mark_need_print(subnode, tmp_buffer, arr_node_need_print))) {
            LOG_WARN("fail to mark if arrary node need print", K(ret));
          } else if (true == arr_node_need_print) {
            is_need_print = true;
          } else {}
        }
        break;
      default:
        is_need_print = false;
        break;
    }
  }
  return ret;
}
////////////////////////////////////////////////////////////////
int Path::for_all_path()
{
  int ret = OB_SUCCESS;
  char data_buffer[4096];
  ObDataBuffer path_buffer;
  (void)path_buffer.set_data(data_buffer, sizeof(data_buffer) - 1);
  if (OB_FAIL(iterate(root_, path_buffer))) {
    LOG_WARN("fail to iterate", K(ret));
  } else {}
  return ret;
}

int Path::iterate(Value *node, ObDataBuffer path_buff)
{
  int ret = OB_SUCCESS;
  if (NULL != node) {
    String path;
    switch (node->get_type()) {
      case JT_OBJECT:
        DLIST_FOREACH(kv, node->get_object()) {
          if (OB_ISNULL(kv)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("kv is NULL", K(ret));
          } else if (OB_FAIL(databuff_printf(path_buff.get_data(),
                                             path_buff.get_capacity(),
                                             path_buff.get_position(),
                                             "/%.*s",
                                             kv->name_.length(),
                                             kv->name_.ptr()))) {
            LOG_WARN("fail to databuff printf", K(ret), K(kv->name_));
          } else if (FALSE_IT(path.assign_ptr(path_buff.get_data(), static_cast<int32_t>(
                          path_buff.get_position())))) {
          } else if (OB_FAIL(on_path(path, kv))) {
            LOG_WARN("fail to run on_path", K(ret));
          } else if (OB_FAIL(iterate(kv->value_, path_buff))) {
            LOG_WARN("fail to iterate", K(ret));
          } else {}
        }
        break;
      case JT_ARRAY:
        DLIST_FOREACH(subnode, node->get_array()) {
          if (OB_FAIL(iterate(subnode, path_buff))) {
            LOG_WARN("fail to iterate", K(ret));
          } else {}
        }
        break;
      default:
        break;
    } // end switch
  }
  return ret;
}

}
}
