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

#ifndef OCEANBASE_SHARE_CONFIG_OB_SYSTEM_CONFIG_KEY_H_
#define OCEANBASE_SHARE_CONFIG_OB_SYSTEM_CONFIG_KEY_H_

#include "share/ob_define.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/hash_func/murmur_hash.h"
#include "lib/string/ob_string.h"
#include "lib/utility/utility.h"
#include "common/ob_zone.h"

namespace oceanbase
{
namespace common
{
class ObSystemConfigKey
{
public:
  ObSystemConfigKey();
  virtual ~ObSystemConfigKey() {}
  // with exactly equal comparision
  bool operator == (const ObSystemConfigKey &other) const;
  uint64_t hash() const;
  inline int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
  // check if key's information can fit this. i.e. can use the `key' to get config accordingly
  bool match(const ObSystemConfigKey &key) const;
  bool match_ip_port(const ObSystemConfigKey &key) const;
  bool match_server_type(const ObSystemConfigKey &key) const;
  bool match_zone(const ObSystemConfigKey &key) const;
  int set_int(const ObString &key, int64_t intval);
  int set_varchar(const ObString &key, const char *strval);
  // set config name
  void set_name(const ObString &name);
  void set_name(const char *name);
  void set_server_type(const ObString &server_type);
  void set_server_ip(const ObString &server_ip);
  int set_zone(const ObString &zone);
  const char *name() const { return name_; }
  void set_version(const int64_t version);
  int64_t get_version() const;
  int64_t to_string(char *buf, const int64_t len) const;

private:
  static const char *DEFAULT_VALUE;
  ObZone zone_;
  int64_t server_port_;
  int64_t version_;
  char name_[OB_MAX_CONFIG_NAME_LEN];
  char server_type_[OB_SERVER_TYPE_LENGTH];
  char server_ip_[OB_MAX_SERVER_ADDR_SIZE];
  // ObSystemConfig中ObHashMap使用了对象的拷贝构造函数,不能禁止
  //DISALLOW_COPY_AND_ASSIGN(ObSystemConfigKey);
};

inline ObSystemConfigKey::ObSystemConfigKey()
    :zone_(), server_port_(0), version_(0)
{
  MEMSET(name_, 0, OB_MAX_CONFIG_NAME_LEN);
  strncpy(server_type_, DEFAULT_VALUE, sizeof(server_type_));
  server_type_[sizeof(server_type_) - 1] = '\0';
  strncpy(server_ip_, DEFAULT_VALUE, sizeof(server_ip_));
  server_ip_[sizeof(server_ip_) - 1] = '\0';
}

inline bool ObSystemConfigKey::operator == (const ObSystemConfigKey &other) const
{
  return (this == &other
          || (0 == STRCMP(name_, other.name_)
              && zone_ == other.zone_
              && (0 == STRCMP(server_type_, other.server_type_))
              && (0 == STRCMP(server_ip_, other.server_ip_))
              && server_port_ == other.server_port_));
}

inline bool ObSystemConfigKey::match(const ObSystemConfigKey &other) const
{
  bool ret = false;
  ret = (this == &other
         || (0 == STRCMP(name_, other.name_)
             && (zone_ == other.zone_ || zone_.is_empty())
             && ((0 == STRCMP(server_type_, other.server_type_))
                 || (0 == STRCMP(server_type_, DEFAULT_VALUE)))
             && ((0 == STRCMP(server_ip_, other.server_ip_))
                 || (0 == STRCMP(server_ip_, DEFAULT_VALUE)))
             && (server_port_ == other.server_port_ || 0 == server_port_)));
  return ret;
}

inline bool ObSystemConfigKey::match_ip_port(const ObSystemConfigKey &other) const
{
  return match(other) && 0 != server_port_ && (0 != STRCMP(server_ip_, DEFAULT_VALUE));
}

inline bool ObSystemConfigKey::match_server_type(const ObSystemConfigKey &other) const
{
  return match(other) && (0 != STRCMP(server_type_, DEFAULT_VALUE));
}

inline bool ObSystemConfigKey::match_zone(const ObSystemConfigKey &other) const
{
  return match(other) && (zone_ != "");
}

inline void ObSystemConfigKey::set_name(const ObString &name)
{
  int64_t name_length = name.length();
  if (name_length >= OB_MAX_CONFIG_NAME_LEN) {
    name_length = OB_MAX_CONFIG_NAME_LEN;
  }
  int64_t pos = 0;
  (void) databuff_printf(name_, OB_MAX_CONFIG_NAME_LEN, pos, "%.*s",
                         static_cast<int>(name_length), name.ptr());
}

inline void ObSystemConfigKey::set_name(const char *name)
{
  set_name(ObString::make_string(name));
}

inline void ObSystemConfigKey::set_server_type(const ObString &server_type)
{
  int64_t server_type_length = server_type.length();
  if (server_type_length >= OB_SERVER_TYPE_LENGTH) {
    server_type_length = OB_SERVER_TYPE_LENGTH;
  }
  snprintf(server_type_, OB_SERVER_TYPE_LENGTH, "%.*s",
           static_cast<int>(server_type_length), server_type.ptr());
}

inline void ObSystemConfigKey::set_server_ip(const ObString &server_ip)
{
  int64_t server_ip_length = server_ip.length();
  if (server_ip_length >= OB_MAX_SERVER_ADDR_SIZE) {
    server_ip_length = OB_MAX_SERVER_ADDR_SIZE;
  }
  snprintf(server_ip_, OB_MAX_SERVER_ADDR_SIZE, "%.*s",
           static_cast<int>(server_ip_length), server_ip.ptr());
}

inline int ObSystemConfigKey::set_zone(const ObString &zone)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(zone.ptr())) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid zone", K(zone), K(ret));
  } else {
    if (OB_FAIL(zone_.assign(zone))) {
      SHARE_LOG(WARN, "set string failed", K(zone), K(ret));
    }
  }
  return ret;
}

inline uint64_t ObSystemConfigKey::hash() const
{
  uint64_t hash = murmurhash(this, (int32_t) sizeof(*this), 0);
  return hash;
}
} // end of namespace common
} // end of namespace oceanbase

#endif // OCEANBASE_SHARE_CONFIG_OB_SYSTEM_CONFIG_KEY_H_
