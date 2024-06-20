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

#ifndef _OB_SHARE_OB_GAIS_MSG_H_
#define _OB_SHARE_OB_GAIS_MSG_H_

#include "lib/utility/ob_unify_serialize.h"
#include "lib/net/ob_addr.h"
#include "share/ob_autoincrement_param.h"
#include "share/ob_define.h"

namespace oceanbase
{
namespace obrpc
{
struct ObGAISNextValRpcResult;
struct ObGAISCurrValRpcResult;
}
namespace share
{

/* Request for get next auto increment value */
struct ObGAISNextAutoIncValReq
{
  OB_UNIS_VERSION(1);

public:
  ObGAISNextAutoIncValReq() : autoinc_key_(), offset_(0), increment_(0), base_value_(0),
                              max_value_(0), desired_cnt_(0), cache_size_(0), sender_(), autoinc_version_(OB_INVALID_VERSION) {}
  int init(const AutoincKey &autoinc_key,
           const uint64_t offset,
           const uint64_t increment,
           const uint64_t base_value,
           const uint64_t max_value,
           const uint64_t desired_cnt,
           const uint64_t cache_size,
           const common::ObAddr &sender,
           const int64_t &autoinc_version);
  bool is_valid() const
  {
    return is_valid_tenant_id(autoinc_key_.tenant_id_) && offset_ > 0 && increment_ > 0 &&
             max_value_ > 0 && desired_cnt_ > 0 && cache_size_ > 0 && sender_.is_valid()
             && autoinc_version_ >= OB_INVALID_VERSION;
  }
  TO_STRING_KV(K_(autoinc_key), K_(offset), K_(increment), K_(base_value), K_(max_value),
                                K_(desired_cnt), K_(cache_size), K_(sender), K_(autoinc_version));

  AutoincKey autoinc_key_;
  uint64_t offset_;
  uint64_t increment_;
  uint64_t base_value_;
  uint64_t max_value_;
  uint64_t desired_cnt_;
  uint64_t cache_size_;
  common::ObAddr sender_;
  int64_t autoinc_version_;
};

/* GAIS autoinc key rpc argument */
struct ObGAISAutoIncKeyArg
{
  OB_UNIS_VERSION(1);

public:
  ObGAISAutoIncKeyArg() : autoinc_key_(), sender_(), autoinc_version_(OB_INVALID_VERSION) {}
  int init(const AutoincKey &autoinc_key, const common::ObAddr &sender, const int64_t autoinc_version);
  bool is_valid() const
  {
    return is_valid_tenant_id(autoinc_key_.tenant_id_) && sender_.is_valid() && autoinc_version_ >= OB_INVALID_VERSION;
  }
  TO_STRING_KV(K_(autoinc_key), K_(sender), K_(autoinc_version));

  AutoincKey autoinc_key_;
  common::ObAddr sender_;
  int64_t autoinc_version_;
};

/* Request for push local sync value to global */
struct ObGAISPushAutoIncValReq
{
  OB_UNIS_VERSION(1);

public:
  ObGAISPushAutoIncValReq() : autoinc_key_(), base_value_(0), max_value_(0), sender_(),
                              autoinc_version_(OB_INVALID_VERSION), cache_size_(0) {}
  int init(const AutoincKey &autoinc_key,
           const uint64_t base_value,
           const uint64_t max_value,
           const common::ObAddr &sender,
           const int64_t &autoinc_version,
           const int64_t cache_size);
  bool is_valid() const
  {
    return is_valid_tenant_id(autoinc_key_.tenant_id_) && max_value_ > 0 && base_value_ <= max_value_
            && sender_.is_valid() && autoinc_version_ >= OB_INVALID_VERSION && cache_size_ >= 0;
  }
  TO_STRING_KV(K_(autoinc_key), K_(base_value), K_(max_value), K_(sender), K_(autoinc_version),
               K_(cache_size));

  AutoincKey autoinc_key_;
  uint64_t base_value_;
  uint64_t max_value_;
  common::ObAddr sender_;
  int64_t autoinc_version_;
  int64_t cache_size_;
};

struct ObGAISBroadcastAutoIncCacheReq
{
  OB_UNIS_VERSION(1);

public:
  ObGAISBroadcastAutoIncCacheReq() : tenant_id_(0), buf_(NULL), buf_size_(0) {}

  int init(const uint64_t tenant_id, const char *buf, const int64_t size)
  {
    tenant_id_ = tenant_id;
    buf_ = buf;
    buf_size_ = size;
    return common::OB_SUCCESS;
  }

  bool is_valid() const
  {
    return is_valid_tenant_id(tenant_id_) && NULL != buf_ && buf_size_ > 0;
  }

  TO_STRING_KV(K_(tenant_id), KP_(buf), K_(buf_size));
  uint64_t tenant_id_;
  const char *buf_;
  int64_t buf_size_;
};

/* Request for get next sequence value */
struct ObGAISNextSequenceValReq
{
  OB_UNIS_VERSION(1);

public:
  ObGAISNextSequenceValReq() : schema_(), sender_()
  {}
  int init(const schema::ObSequenceSchema &schema, const common::ObAddr &sender);
  int assign(const ObGAISNextSequenceValReq &src_req);
  bool is_valid() const
  {
    return is_valid_tenant_id(schema_.get_tenant_id()) && schema_.get_sequence_id() != OB_INVALID_ID
           && schema_.get_cache_size() > static_cast<int64_t>(0) && sender_.is_valid();
  }
  TO_STRING_KV(K_(schema), K_(sender));

  schema::ObSequenceSchema schema_;
  common::ObAddr sender_;
};

} // share
} // oceanbase

#endif // _OB_SHARE_OB_GAIS_MSG_H_
