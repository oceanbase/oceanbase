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

#define USING_LOG_PREFIX SHARE

#include "share/ob_resource_limit.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/oblog/ob_log.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/json/ob_json.h"
#include "share/config/ob_config_helper.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase
{
namespace share
{

template<typename T1, typename T2>
void assign(T2 &dst, const T1 &src)
{
  dst = src;
}

void assign(TStr &dst, const TStr &src)
{
  STRNCPY(dst, src, sizeof(dst));
  dst[sizeof(dst) - 1] = '\0';
}

void assign(int64_t &dst, const RLInt &src)
{
  dst = src.v_;
}

void assign(TStr &dst, const RLStr &src)
{
  STRNCPY(dst, src.v_, sizeof(dst));
  dst[sizeof(dst) - 1] = '\0';
}

void assign(int64_t &dst, const RLCap &src)
{
  dst = src.v_;
}

int parse(const ObString &str, RLInt &value)
{
  int ret = OB_SUCCESS;
  char c_str[64];
  if (str.length() > sizeof(c_str) - 1) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("buffer not enough", K(str.length()));
  } else {
    MEMCPY(c_str, str.ptr(), str.length());
    c_str[str.length()]= '\0';
    bool valid = false;
    value.v_ = ObConfigIntParser::get(c_str, valid);
    if (!valid) {
      ret = OB_CONVERT_ERROR;
      LOG_WARN("convert failed");
    }
  }
  return ret;
}

int parse(const ObString &str, RLStr &value)
{
  int ret = OB_SUCCESS;
  if (str.length() > sizeof(value.v_) - 1) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("buffer not enough", K(str.length()));
  } else {
    MEMCPY(&value.v_[0], str.ptr(), str.length());
    value.v_[str.length()] = '\0';
  }
  return ret;
}

int parse(const ObString &str, RLCap &value)
{
  int ret = OB_SUCCESS;
  char c_str[64];
  if (str.length() > sizeof(c_str) - 1) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("buffer not enough", K(str.length()));
  } else {
    MEMCPY(c_str, str.ptr(), str.length());
    c_str[str.length()]= '\0';
    bool valid = false;
    value.v_ = ObConfigCapacityParser::get(c_str, valid);
    if (!valid) {
      ret = OB_CONVERT_ERROR;
      LOG_WARN("convert failed");
    }
  }
  return ret;
}

int ObResourceLimit::update(const ObString &key,
                            const ObString &value)
{
  int ret = OB_SUCCESS;
  bool matched = false;

#define RL_DEF(name, type, ...)                                                  \
  if (OB_SUCC(ret) && !matched && 0 == key.case_compare(#name)) {                \
    type tmp_##name;                                                             \
    if (OB_FAIL(parse(value, tmp_##name))) {                                     \
      SHARE_LOG(WARN, "parse " #name " failed", K(ret), K(value));               \
    } else {                                                                     \
      ::assign(name, tmp_##name);                                                \
      matched = true;                                                            \
    }                                                                            \
  }
#include "share/ob_resource_limit_def.h"
#undef RL_DEF

  if (OB_SUCC(ret) && !matched) {
    int ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("not founded", K(ret), K(key));
  }
  return ret;
}

bool ObResourceLimit::IS_ENABLED = false;

ObResourceLimit::ObResourceLimit()
{
  load_default();
}

int64_t ObResourceLimit::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  oceanbase::common::databuff_print_kv(buf, buf_len, pos
#define RL_DEF(name, ...) , K(name)
#include "share/ob_resource_limit_def.h"
#undef RL_DEF
  );
  J_OBJ_END();
  return pos;
}

void ObResourceLimit::assign(const ObResourceLimit &other)
{
#define RL_DEF(name, ...) ::assign(name, other.name);
#include "share/ob_resource_limit_def.h"
#undef RL_DEF
}

void ObResourceLimit::load_default()
{
#define RL_DEF(name, type, default_value) \
  abort_unless(OB_SUCCESS == update(#name, default_value));
#include "share/ob_resource_limit_def.h"
#undef RL_DEF
}

int ObResourceLimit::load_json(const char *c_str)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("RLJsonParser");
  json::Value *data = NULL;
  json::Parser parser;
  ObString str(c_str);
  if (OB_FAIL(parser.init(&allocator))) {
    LOG_WARN("parser init failed", K(ret));
  } else if (OB_FAIL(parser.parse(str.ptr(), str.length(), data))) {
    LOG_WARN("parse json failed", K(ret), K(str));
  } else if (NULL == data) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no root value", K(ret));
  } else if (json::JT_OBJECT != data->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error json format", K(ret));
  } else {
#define RL_DEF(name, type, default_value)                                  \
  if (OB_SUCC(ret)) {                                                      \
    bool matched = false;                                                  \
    DLIST_FOREACH_X(it, data->get_object(), OB_SUCC(ret)) {                \
      if (0 == it->name_.case_compare(#name)) {                            \
        if (json::JT_STRING != it->value_->get_type()) {                   \
          ret = OB_INVALID_CONFIG;                                         \
          SHARE_LOG(WARN, "invalid type", K(ret));                         \
        } else {                                                           \
          matched = true;                                                  \
          type tmp_##name;                                                 \
          if (OB_FAIL(parse(it->value_->get_string(), tmp_##name))) {      \
            SHARE_LOG(WARN, "parse " #name " failed", K(ret),              \
                      K(it->value_->get_string()));                        \
          } else {                                                         \
            ::assign(name, tmp_##name);                                    \
          }                                                                \
        }                                                                  \
      }                                                                    \
    }                                                                      \
    if (OB_SUCC(ret) && !matched) {                                        \
      SHARE_LOG(WARN, #name " not founded, use default value: " default_value); \
      ret = update(#name, default_value);                                  \
    }                                                                      \
  }
#include "share/ob_resource_limit_def.h"
#undef RL_DEF
  }

  LOG_INFO("json loaded", K(ret), K(*this));
  return ret;
}

int ObResourceLimit::load_config(const char *c_str)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(c_str) || strlen(c_str) <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), KP(c_str));
  } else if (0 == ObString(c_str).case_compare("auto")) {
    load_default();
  } else {
    ret = load_json(c_str);
  }
  return ret;
}

} //end share
} //end oceanbase
