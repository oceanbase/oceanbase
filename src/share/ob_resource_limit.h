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

#ifndef OCEANBASE_SHARE_OB_RESOURCE_LIMIT_H
#define OCEANBASE_SHARE_OB_RESOURCE_LIMIT_H

#include <stdint.h>
#include <type_traits>
#include "lib/ob_abort.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/ob_errno.h"

namespace oceanbase
{
namespace common
{
class ObString;
}

namespace share
{

using TStr = char[32];

struct RLInt { int64_t v_; };
struct RLStr { TStr v_; };
struct RLCap { int64_t v_; };

class ObResourceLimit
{
public:
  static bool IS_ENABLED;
  ObResourceLimit();
  int update(const common::ObString &key, const common::ObString &value);
  void load_default();
  int load_json(const char *str);
  int load_config(const char *str);
  void assign(const ObResourceLimit &other);
  int64_t to_string(char *buf, const int64_t buf_len) const;
#define RL_DEF(name, rltype, ...)                                                             \
  using name##_return_type =                                                                  \
    std::conditional<!std::is_same<rltype, RLStr>::value, decltype(rltype::v_), char*>::type; \
  name##_return_type get_##name() const { return (name##_return_type)name; }
#include "share/ob_resource_limit_def.h"
#undef RL_DEF
private:
#define RL_DEF(name, type, default_value) decltype(type::v_) name;
#include "share/ob_resource_limit_def.h"
#undef RL_DEF
  DISALLOW_COPY_AND_ASSIGN(ObResourceLimit);
};

inline ObResourceLimit &get_rl_config()
{
  static ObResourceLimit one;
  return one;
}

#define RL_CONF (::oceanbase::share::get_rl_config())
#define RL_IS_ENABLED (::oceanbase::share::ObResourceLimit::IS_ENABLED)

} //end share
} //end oceanbase

#endif
