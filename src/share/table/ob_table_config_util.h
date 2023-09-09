/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_OB_TABLE_CONFIG_UTIL_H_
#define OCEANBASE_OB_TABLE_CONFIG_UTIL_H_
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "share/config/ob_server_config.h"
namespace oceanbase
{
namespace common
{
enum ObKVFeatureType {
  INVALIDTYPE,
  TTL,
  REROUTING,
  HOTKEY,
  MAXTYPE
};

class ObKVFeatureMode final
{
public:
  ObKVFeatureMode(): is_valid_(false), value_(0) {}
  ObKVFeatureMode(const uint8_t *values);
  bool is_valid() { return is_valid_; }
  bool check_mode_valid(uint8_t mode) { return mode > 2 ? false : true; }
  bool is_ttl_enable();
  bool is_rerouting_enable();
  bool is_hotkey_enable();
  void set_ttl_mode(uint8_t mode);
  void set_rerouting_mode(uint8_t mode);
  void set_hotkey_mode(uint8_t mode);
  void set_value(uint8_t value);
  int8_t get_value() const { return value_; }
private:
  bool is_valid_;
	union {
    uint8_t value_;
    struct {
        uint8_t ttl_mode_ : 2;
        uint8_t rerouting_mode_ : 2;
        uint8_t hotkey_mode_ : 2;
        uint8_t reserver_mode_ :2;
      };
    };
private:
  DISALLOW_COPY_AND_ASSIGN(ObKVFeatureMode);
};

class ObKVFeatureModeUitl
{
public:
  static bool is_obkv_feature_enable(ObKVFeatureType feat_type);
  static bool is_ttl_enable();
  static bool is_rerouting_enable();
  static bool is_hotkey_enable();
};

}
}
#endif