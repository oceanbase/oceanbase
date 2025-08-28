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
#include "observer/omt/ob_tenant_config_mgr.h"
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
  bool check_mode_valid(uint16_t mode) { return mode > 2 ? false : true; }
  bool is_ttl_enable();
  bool is_rerouting_enable();
  bool is_hotkey_enable();
  void set_ttl_mode(uint16_t mode);
  void set_rerouting_mode(uint16_t mode);
  void set_hotkey_mode(uint16_t mode);
  void set_value(uint16_t value);
  uint16_t get_value() const { return value_; }
private:
  bool is_valid_;
	union {
    uint16_t value_; //FARM COMPAT WHITELIST
    struct {
        uint16_t ttl_mode_ : 2;
        uint16_t rerouting_mode_ : 2;
        uint16_t hotkey_mode_ : 2;
        uint16_t reserver_mode_ : 10;
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

class ObPrivControlMode final
{
public:
  ObPrivControlMode(): is_valid_(false), value_(0) {}
  ObPrivControlMode(const uint8_t *values);
  bool is_valid() { return is_valid_; }
  bool check_mode_valid(uint8_t mode) { return mode > 2 ? false : true; }
  void set_value(uint64_t value);
  u_int64_t get_value() const { return value_; }
private:
  bool is_valid_;
	union {
    uint64_t value_;
    struct {
        uint8_t reserved_check0_ : 2;
        uint8_t reserved_check1_ : 2;
        uint8_t reserved_check2_ : 2;
        uint8_t reserved_check3_ : 2;
        uint8_t reserved_check4_ : 2;
        uint8_t reserved_check5_ : 2;
        uint8_t reserved_check6_ : 2;
        uint8_t reserved_check7_ : 2;
        uint8_t reserved_check8_ : 2;
        uint8_t reserved_check9_ : 2;
        uint8_t reserved_check10_ : 2;
        uint8_t reserved_check11_ : 2;
        uint8_t reserved_check12_ : 2;
        uint8_t reserved_check13_ : 2;
        uint8_t reserved_check14_ : 2;
        uint8_t reserved_check15_ : 2;
        uint8_t reserved_check16_ : 2;
        uint8_t reserved_check17_ : 2;
        uint8_t reserved_check18_ : 2;
        uint8_t reserved_check19_ : 2;
        uint8_t reserved_check20_ : 2;
        uint8_t reserved_check21_ : 2;
        uint8_t reserved_check22_ : 2;
        uint8_t reserved_check23_ : 2;
        uint8_t reserved_check24_ : 2;
        uint8_t reserved_check25_ : 2;
        uint8_t reserved_check26_ : 2;
        uint8_t reserved_check27_ : 2;
        uint8_t reserved_check28_ : 2;
        uint8_t reserved_check29_ : 2;
        uint8_t reserved_check30_ : 2;
        uint8_t reserved_check31_ : 2;
      };
    };
private:
  DISALLOW_COPY_AND_ASSIGN(ObPrivControlMode);
};

class ObKVConfigUtil
{
public:
  static int get_compress_type(const int64_t tenant_id, 
                               int64_t result_size,
                               ObCompressorType &compressor_type);
};

}
}
#endif