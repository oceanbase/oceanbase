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

#include "lib/ob_name_id_def.h"
#include <stdlib.h>
#include <string.h>
namespace oceanbase
{
namespace name
{
static const char* ID_NAMES[NAME_COUNT+1];
static const char* ID_DESCRIPTIONS[NAME_COUNT+1];
struct RuntimeIdNameMapInit
{
  RuntimeIdNameMapInit()
  {
#define DEF_NAME(name_sym, description) ID_NAMES[name_sym] = #name_sym;
#define DEF_NAME_PAIR(name_sym, description) \
  DEF_NAME(name_sym ## _begin, description " begin")    \
  DEF_NAME(name_sym ## _end, description " end")

#include "ob_name_id_def.h"
#undef DEF_NAME
#undef DEF_NAME_PAIR
#define DEF_NAME(name_sym, description) ID_DESCRIPTIONS[name_sym] = description;
#define DEF_NAME_PAIR(name_sym, description) \
  DEF_NAME(name_sym ## _begin, description " begin")    \
  DEF_NAME(name_sym ## _end, description " end")
#include "ob_name_id_def.h"
#undef DEF_NAME
#undef DEF_NAME_PAIR
  }
};
static RuntimeIdNameMapInit INIT;

const char* get_name(int32_t id)
{
  const char* ret = NULL;
  if (id < NAME_COUNT && id >= 0) {
    ret = oceanbase::name::ID_NAMES[id];
  }
  return ret;
}

const char* get_description(int32_t id)
{
  const char* ret = NULL;
  if (id < NAME_COUNT && id >= 0) {
    ret = oceanbase::name::ID_DESCRIPTIONS[id];
  }
  return ret;
}

} // end namespace name_id_map
} // end namespace oceanbase
