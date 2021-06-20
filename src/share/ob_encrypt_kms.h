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
#include "lib/json/ob_json.h"
#include "lib/allocator/page_arena.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/container/ob_se_array.h"
#ifndef OCEANBASE_SHARE_OB_ENCRYPT_KMS_H
#define OCEANBASE_SHARE_OB_ENCRYPT_KMS_H

namespace oceanbase {
namespace share {

struct ObKmsResult {
  ObKmsResult() : key_version_(0), key_expired_time_(0), content_()
  {}
  ~ObKmsResult()
  {}
  TO_STRING_KV(K_(key_version), K_(key_expired_time));

  int64_t key_version_;
  int64_t key_expired_time_;
  common::ObString content_;
};

class ObKmsClient {
public:
  ObKmsClient()
  {}
  virtual ~ObKmsClient()
  {}

public:
  virtual int init(const char* kms_info, int64_t kms_len);
  virtual int check_param_valid();
  common::ObString get_root_ca() const
  {
    return common::ObString();
  }
  bool is_sm_scene();
};

class ObSSLClient : public ObKmsClient {
public:
  ObSSLClient() : ObKmsClient(), private_key_(), public_cert_()
  {}
  virtual ~ObSSLClient(){};

public:
  bool is_bkmi_mode() const
  {
    return false;
  }

public:
  ObKmsResult private_key_;  // private
  ObKmsResult public_cert_;
};

}  // namespace share
}  // namespace oceanbase

#endif
