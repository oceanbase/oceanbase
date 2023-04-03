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

#define USING_LOG_PREFIX RS
#include "ob_resource_weight_parser.h"
#include "lib/utility/utility.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
namespace oceanbase
{
namespace rootserver
{

int ObResourceWeightParser::MyCb::match(const char *key, const char *value)
{
  int ret = OB_SUCCESS;
  static const int key_len[]    = { 5,        7,          4,       5};
  static const char *keys[]     = {"iops",   "memory",   "cpu",   "disk"};
  static WeightSetter setters[] = {set_iops, set_memory, set_cpu, set_disk};
  bool found = false;

  for (int i = 0; i < 4; ++i) {
    if (0 == STRNCASECMP(keys[i], key, key_len[i])) {
      int64_t w = 0;
      if (OB_FAIL(ob_atoll(value, w))) {
        LOG_WARN("fail parse value to int64", K(value), K(ret));
      } else {
        setters[i](weight_, static_cast<double>(w) / 100);
        found = true;
      }
      break;
    }
  }
  if (!found) {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("unknown kv pair", K(key), K(value), K(ret));
  }
  return ret;
}

int ObResourceWeightParser::parse(const char *str, ObResourceWeight &weight)
{
  int ret = OB_SUCCESS;
  weight.reset();
  MyCb cb(weight);
  ObKVParser parser;
  parser.set_match_callback(cb);
  if (OB_FAIL(parser.parse(str))) {
    LOG_WARN("fail parse weight", K(str), K(ret));
  } else if (!weight.is_valid()) {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("invalid weight", K(str), K(weight), "empty", weight.is_empty(), K(ret));
  }
  return ret;
}


}/* ns rootserver*/
}/* ns oceanbase */


