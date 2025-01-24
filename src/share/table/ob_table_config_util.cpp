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

#define USING_LOG_PREFIX SHARE
#include "ob_table_config_util.h"
#include "share/config/ob_config_mode_name_def.h"
namespace oceanbase
{
namespace common
{
ObKVFeatureMode::ObKVFeatureMode(const uint8_t *values)
{
  if (OB_UNLIKELY(values == NULL)) {
    value_ = 0;
    is_valid_ = false;
  } else {
    value_ = values[0];
    is_valid_ = true;
  }
}

void ObKVFeatureMode::set_ttl_mode(uint8_t mode)
{
  is_valid_ = check_mode_valid(mode);
  if (is_valid_) {
    ttl_mode_ = mode;
  }
}

void ObKVFeatureMode::set_rerouting_mode(uint8_t mode)
{
  is_valid_ = check_mode_valid(mode);
  if (is_valid_) {
    rerouting_mode_ = mode;
  }
}

void ObKVFeatureMode::set_hotkey_mode(uint8_t mode)
{
  is_valid_ = check_mode_valid(mode);
  if (is_valid_) {
    hotkey_mode_ = mode;
  }
}

void ObKVFeatureMode::set_value(uint8_t value)
{
  if ((value & 0b11) == 0b11 || ((value >> 2) & 0b11) == 0b11 || ((value >> 4) & 0b11) == 0b11) {
    is_valid_ = false;
  } else {
    is_valid_ = true;
    value_ = value;
  }
}


bool ObKVFeatureMode::is_ttl_enable() {
  bool mode = MODE_DEFAULT_VAL_TTL;
  if (ttl_mode_ == ObKvFeatureModeParser::MODE_ON) {
    mode = true;
  } else if (ttl_mode_ == ObKvFeatureModeParser::MODE_OFF) {
    mode = false;
  }
  return mode;
}

bool ObKVFeatureMode::is_rerouting_enable() {
  bool mode = MODE_DEFAULT_VAL_REROUTING;
  if (rerouting_mode_ == ObKvFeatureModeParser::MODE_ON) {
    mode = true;
  } else if (rerouting_mode_ == ObKvFeatureModeParser::MODE_ON) {
    mode = false;
  }
  return mode;
}

bool ObKVFeatureMode::is_hotkey_enable() {
  bool mode = MODE_DEFAULT_VAL_HOTKEY;
  if (hotkey_mode_ == ObKvFeatureModeParser::MODE_ON) {
    mode = true;
  } else if (hotkey_mode_ == ObKvFeatureModeParser::MODE_ON) {
    mode = false;
  }
  return mode;
}

bool ObKVFeatureModeUitl::is_obkv_feature_enable(ObKVFeatureType feat_type)
{
  bool bret = false;
  ObKVFeatureMode cfg(GCONF._obkv_feature_mode);
  if (!cfg.is_valid()) {
    bret = false;
    OB_LOG_RET(WARN, OB_INVALID_ARGUMENT, "unexpected, cfg is invalid");
  } else {
    switch (feat_type) {
      case ObKVFeatureType::TTL:
        bret = cfg.is_ttl_enable();
        break;
      case ObKVFeatureType::REROUTING:
        bret = cfg.is_rerouting_enable();
        break;
      case ObKVFeatureType::HOTKEY:
        bret = cfg.is_hotkey_enable();
        break;
      default:
        OB_LOG_RET(WARN, OB_INVALID_ARGUMENT, "unexpected feature type", K(feat_type));
        break;
    }
  }
  return bret;
}

bool ObKVFeatureModeUitl::is_ttl_enable()
{
  return is_obkv_feature_enable(ObKVFeatureType::TTL);
}

bool ObKVFeatureModeUitl::is_rerouting_enable()
{
 return is_obkv_feature_enable(ObKVFeatureType::REROUTING);
}

bool ObKVFeatureModeUitl::is_hotkey_enable()
{
  return is_obkv_feature_enable(ObKVFeatureType::HOTKEY);
}

ObPrivControlMode::ObPrivControlMode(const uint8_t *values)
{
  if (OB_UNLIKELY(values == NULL)) {
    value_ = 0;
    is_valid_ = false;
  } else {
    value_ = (int64_t)values[0] | ((int64_t)values[1] << 8) | ((int64_t)values[2] << 16) | ((int64_t)values[3] << 24)
            | ((int64_t)values[4] << 32) | ((int64_t)values[5] << 40) | ((int64_t)values[6] << 48)
            | ((int64_t)values[7] << 56);
    is_valid_ = true;
  }
}

void ObPrivControlMode::set_value(uint64_t value)
{
  if ((value & 0b11) == 0b11 || ((value >> 2) & 0b11) == 0b11 || ((value >> 4) & 0b11) == 0b11
     || ((value >> 6) & 0b11) == 0b11 || ((value >> 8) & 0b11) == 0b11 || ((value >> 10) & 0b11) == 0b11
     || ((value >> 12) & 0b11) == 0b11 || ((value >> 14) & 0b11) == 0b11 || ((value >> 16) & 0b11) == 0b11
     || ((value >> 18) & 0b11) == 0b11 || ((value >> 20) & 0b11) == 0b11 || ((value >> 22) & 0b11) == 0b11
     || ((value >> 24) & 0b11) == 0b11 || ((value >> 26) & 0b11) == 0b11 || ((value >> 28) & 0b11) == 0b11
     || ((value >> 30) & 0b11) == 0b11 || ((value >> 32) & 0b11) == 0b11 || ((value >> 34) & 0b11) == 0b11
     || ((value >> 36) & 0b11) == 0b11 || ((value >> 38) & 0b11) == 0b11 || ((value >> 40) & 0b11) == 0b11
     || ((value >> 42) & 0b11) == 0b11 || ((value >> 44) & 0b11) == 0b11 || ((value >> 46) & 0b11) == 0b11
     || ((value >> 48) & 0b11) == 0b11 || ((value >> 50) & 0b11) == 0b11 || ((value >> 52) & 0b11) == 0b11
     || ((value >> 54) & 0b11) == 0b11 || ((value >> 56) & 0b11) == 0b11 || ((value >> 58) & 0b11) == 0b11
     || ((value >> 60) & 0b11) == 0b11 || ((value >> 62) & 0b11) == 0b11) {
    is_valid_ = false;
  } else {
    is_valid_ = true;
    value_ = value;
  }
}

int ObKVConfigUtil::get_compress_type(const int64_t tenant_id,
                                      int64_t result_size,
                                      ObCompressorType &compressor_type)
{
  int ret = OB_SUCCESS;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (!tenant_config.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to get rpc compress config", K(tenant_id));
  } else {
    int64_t compress_threshold = tenant_config->kv_transport_compress_threshold;
    ObString compress_type_str(tenant_config->kv_transport_compress_func.get_value());
    if (OB_FAIL(ObCompressorPool::get_instance().get_compressor_type(compress_type_str, compressor_type))) {
      LOG_WARN("fail to get compress type", K(ret), K(compress_type_str));
    } else if (compressor_type == NONE_COMPRESSOR || result_size < compress_threshold) {
      compressor_type = INVALID_COMPRESSOR;
    }
    LOG_DEBUG("[TABLEAPI] the rpc compress type", K(ret), K(compressor_type), K(result_size));
  }
  return ret;
}

}
}