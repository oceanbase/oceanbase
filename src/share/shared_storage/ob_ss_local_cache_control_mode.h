//Copyright (c) 2024 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OCEANBASE_SHARE_SHARED_STORAGE_OB_SS_LOCAL_CACHE_CONTROL_MODE_H_
#define OCEANBASE_SHARE_SHARED_STORAGE_OB_SS_LOCAL_CACHE_CONTROL_MODE_H_

#include "share/config/ob_config.h"

namespace oceanbase
{
namespace common
{

struct ObSSLocalCacheControlMode : public ObIConfigMode
{
public:
  ObSSLocalCacheControlMode(): value_(0) {}
  ObSSLocalCacheControlMode(const uint8_t *values);
  void assign(const ObSSLocalCacheControlMode &other);
  virtual int set_value(const ObConfigModeItem &mode_item) override;
  bool check_mode_valid(uint16_t mode) const { return mode > 2 ? false : true; }
  bool is_micro_cache_enable() const { return ((micro_cache_mode_ == MODE_OFF) ? false : true); }
  bool is_macro_read_cache_enable() const { return ((macro_read_cache_mode_ == MODE_OFF) ? false : true); }
  bool is_macro_write_cache_enable() const { return ((macro_write_cache_mode_ == MODE_OFF) ? false : true); }
  void set_micro_cache_mode(uint16_t mode);
  void set_macro_read_cache_mode(uint16_t mode);
  void set_macro_write_cache_mode(uint16_t mode);
  uint16_t get_value() const { return value_; }

public:
  static const int16_t MODE_DEFAULT = 0b00;
  static const int16_t MODE_ON = 0b01;
  static const int16_t MODE_OFF = 0b10;
private:
  union {
    uint16_t value_;
    struct {
      uint16_t micro_cache_mode_ : 2;
      uint16_t macro_read_cache_mode_ : 2;
      uint16_t macro_write_cache_mode_ : 2;
      uint16_t reserved_ : 10;
    };
  };
private:
  DISALLOW_COPY_AND_ASSIGN(ObSSLocalCacheControlMode);
};

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_SHARE_SHARED_STORAGE_OB_SS_LOCAL_CACHE_CONTROL_MODE_H_
