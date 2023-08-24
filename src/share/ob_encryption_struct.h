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

#include "lib/ob_define.h"
#include "lib/container/ob_se_array.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/utility/ob_print_utils.h"
#include "share/ob_encryption_util.h"

#ifndef OCEANBASE_SHARE_OB_ENCRYPT_STRUCT_
#define OCEANBASE_SHARE_OB_ENCRYPT_STRUCT_

namespace oceanbase
{
using common::OB_ERR_UNEXPECTED;
using common::ObString;
namespace share
{

template<int64_t BUFSIZE>
class ObEncryptKey
{
public:
  ObEncryptKey() : str_(BUFSIZE, 0, buf_) {}
  ~ObEncryptKey() {}
  ObEncryptKey(const ObEncryptKey &rhs);
  ObEncryptKey& operator=(const ObEncryptKey &rhs);
  int assign(const ObEncryptKey &rhs);
  void reset() { str_.set_length(0); }
  int64_t size() const { return str_.length(); }
  int set_content(const ObString &key);
  int set_content(const char *ptr, const int64_t size);
  ObString& get_content() { return str_; }
  const ObString& get_content() const { return str_; }
  char* ptr() { return str_.ptr(); }
  const char* ptr() const { return str_.ptr(); }
  TO_STRING_KV(K_(str));
  OB_UNIS_VERSION(1);
private:
  char buf_[BUFSIZE];
  ObString str_;
};

OB_SERIALIZE_MEMBER_TEMP(template<int64_t BUFSIZE>, ObEncryptKey<BUFSIZE>, str_);

template<int64_t BUFSIZE>
ObEncryptKey<BUFSIZE>::ObEncryptKey(const ObEncryptKey &rhs)
{
  str_.assign_buffer(buf_, BUFSIZE);
  str_.write(rhs.get_content().ptr(), rhs.get_content().length());
}

template<int64_t BUFSIZE>
ObEncryptKey<BUFSIZE>& ObEncryptKey<BUFSIZE>::operator=(const ObEncryptKey &rhs)
{
  str_.set_length(0);
  str_.write(rhs.get_content().ptr(), rhs.get_content().length());
  return *this;
}

template<int64_t BUFSIZE>
int ObEncryptKey<BUFSIZE>::assign(const ObEncryptKey &rhs)
{
  int ret = common::OB_SUCCESS;
  str_.set_length(0);
  const int32_t rhs_len = rhs.get_content().length();
  if (OB_UNLIKELY(rhs_len != str_.write(rhs.get_content().ptr(), rhs_len))) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(WARN, "key content is wrong", K(ret), K(rhs), K(rhs_len));
  }
  return ret;
}

template<int64_t BUFSIZE>
int ObEncryptKey<BUFSIZE>::set_content(const ObString &key)
{
  int ret = common::OB_SUCCESS;
  const int64_t len = key.length();
  if (len < 0 || len > BUFSIZE) {
    SHARE_LOG(WARN, "key content is wrong", K(key));
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    str_.set_length(0);
    str_.write(key.ptr(), key.length());
  }
  return ret;
}

template<int64_t BUFSIZE>
int ObEncryptKey<BUFSIZE>::set_content(const char *ptr, const int64_t len)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(ptr) || len < 0 || len > BUFSIZE) {
    SHARE_LOG(WARN, "invalid argument when set_content", KP(ptr), K(len));
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    str_.set_length(0);
    str_.write(ptr, static_cast<int32_t>(len));
  }
  return ret;
}

typedef ObEncryptKey<OB_CLOG_ENCRYPT_MASTER_KEY_LEN> ObEncryptMasterKeyString;
typedef ObEncryptKey<OB_ENCRYPTED_TABLE_KEY_LEN> ObEncryptTableKeyString;

struct ObEncryptMeta
{
  //used for encrypting data, not table_key
  ObEncryptMasterKeyString master_key_;
  //The actual length of table_key is 15, and OB_ENCRYPTED_TABLE_KEY_LEN is used here, in order to avoid defining one more String
  ObEncryptTableKeyString table_key_;
  ObEncryptTableKeyString encrypted_table_key_;
  uint64_t tenant_id_;  // no need to serialize
  int64_t master_key_version_;
  int64_t encrypt_algorithm_;
  ObEncryptMeta() : tenant_id_(OB_INVALID_ID), master_key_version_(-1), encrypt_algorithm_(-1) {}
  TO_STRING_KV(K_(tenant_id), K_(master_key_version), K_(encrypt_algorithm), K_(master_key),
               K_(table_key));
  int assign(const ObEncryptMeta &other);
  void reset();
  int replace_tenant_id(const uint64_t real_tenant_id);
  bool is_valid_before_decrypt_table_key() const
  {
    return encrypt_algorithm_ > 0
           && master_key_version_ > 0
           && encrypted_table_key_.size() > 0;
  }
  bool is_valid() const
  {
    return encrypt_algorithm_ > 0
           && master_key_version_ > 0
           && master_key_.size() > 0
           && table_key_.size() > 0
           && encrypted_table_key_.size() > 0;
  }
  OB_UNIS_VERSION(1);
};

struct ObZoneEncryptMeta
{
public:
  ObZoneEncryptMeta() : version_(VERSION), master_key_version_(-1), encrypt_algorithm_(-1) {}
  ~ObZoneEncryptMeta() {reset();}
  void reset();
  bool is_valid_except_master_key_version() const;
  bool is_valid() const;
  int assign(const ObZoneEncryptMeta &other);
  void set_master_key_version(int64_t master_key_version) {master_key_version_ = master_key_version;}
  TO_STRING_KV(K_(master_key_version), K_(encrypt_algorithm));
  OB_UNIS_VERSION(1);
public:
  static const int64_t VERSION = 1;
  int16_t version_;
  int64_t master_key_version_;
  int64_t encrypt_algorithm_;
};

enum CLogEncryptStatBitIndex
{
  CLOG_CONTAIN_ENCRYPTED_ROW = 0,//Contains encrypted logs
  CLOG_CONTAIN_NON_ENCRYPTED_ROW = 1,//Contains unencrypted logs
};

//Use scenarios to ensure no concurrency issues
struct ObCLogEncryptStatMap
{
public:
  ObCLogEncryptStatMap() : val_(0) {}
  ~ObCLogEncryptStatMap() {reset_map();}
  void reset_map();
  void set_map(const int64_t idx);
  bool test_map(const int64_t idx) const;
public:
  uint16_t val_;
};

};//share
};//oceanbase
#endif
