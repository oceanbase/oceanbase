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

#define USING_LOG_PREFIX SHARE

#include "share/ob_define.h"
#include "share/ob_gais_msg.h"
#include "share/ob_gais_rpc.h"

namespace oceanbase
{
using namespace oceanbase::common;
using namespace oceanbase::obrpc;
using namespace oceanbase::share;

namespace share
{
// ObGAISNextAutoIncValReq
OB_SERIALIZE_MEMBER(ObGAISNextAutoIncValReq,
                    autoinc_key_, offset_, increment_, base_value_, max_value_, desired_cnt_,
                    cache_size_, sender_, autoinc_version_);

// ObGAISAutoIncKeyArg
OB_SERIALIZE_MEMBER(ObGAISAutoIncKeyArg, autoinc_key_, sender_, autoinc_version_);

// ObGAISPushAutoIncValReq
OB_SERIALIZE_MEMBER(ObGAISPushAutoIncValReq, autoinc_key_, base_value_, max_value_, sender_,
                    autoinc_version_, cache_size_);

OB_DEF_SERIALIZE(ObGAISBroadcastAutoIncCacheReq)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(tenant_id_);
  OB_UNIS_ENCODE(buf_size_);
  if (OB_SUCC(ret)) {
    if (pos + buf_size_ > buf_len) {
      ret = OB_BUF_NOT_ENOUGH;
    } else {
      MEMCPY(buf + pos, buf_, buf_size_);
      pos += buf_size_;
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObGAISBroadcastAutoIncCacheReq)
{
  int ret = OB_SUCCESS;
  OB_UNIS_DECODE(tenant_id_);
  OB_UNIS_DECODE(buf_size_);
  if (OB_SUCC(ret)) {
    buf_ = buf + pos;
    pos += buf_size_;
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObGAISBroadcastAutoIncCacheReq)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(tenant_id_);
  OB_UNIS_ADD_LEN(buf_size_);
  len += buf_size_;
  return len;
}
OB_SERIALIZE_MEMBER(ObGAISNextSequenceValReq, schema_, sender_);

int ObGAISNextAutoIncValReq::init(const AutoincKey &autoinc_key,
                                  const uint64_t offset,
                                  const uint64_t increment,
                                  const uint64_t base_value,
                                  const uint64_t max_value,
                                  const uint64_t desired_cnt,
                                  const uint64_t cache_size,
                                  const common::ObAddr &sender,
                                  const int64_t &autoinc_version)
{
  int ret = OB_SUCCESS;
  if (!is_valid_tenant_id(autoinc_key.tenant_id_) || max_value <= 0 ||
        cache_size <= 0 || offset < 1 || increment < 1 || base_value > max_value ||
        desired_cnt <= 0 || !sender.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(autoinc_key), K(offset), K(increment), K(base_value),
                                 K(max_value), K(desired_cnt), K(cache_size), K(sender));
  } else {
    autoinc_key_ = autoinc_key;
    offset_ = offset;
    increment_ = increment;
    base_value_ = base_value;
    max_value_ = max_value;
    desired_cnt_ = desired_cnt;
    cache_size_ = cache_size;
    sender_ = sender;
    autoinc_version_ = get_modify_autoinc_version(autoinc_version);
  }
  return ret;
}

int ObGAISAutoIncKeyArg::init(const AutoincKey &autoinc_key, const common::ObAddr &sender, const int64_t autoinc_version)
{
  int ret = OB_SUCCESS;
  if (!is_valid_tenant_id(autoinc_key.tenant_id_) || !sender.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(autoinc_key), K(sender));
  } else {
    autoinc_key_ = autoinc_key;
    sender_ = sender;
    autoinc_version_ = get_modify_autoinc_version(autoinc_version);
  }
  return ret;
}

int ObGAISPushAutoIncValReq::init(const AutoincKey &autoinc_key,
                                  const uint64_t base_value,
                                  const uint64_t max_value,
                                  const common::ObAddr &sender,
                                  const int64_t &autoinc_version,
                                  const int64_t cache_size)
{
  int ret = OB_SUCCESS;
  if (!is_valid_tenant_id(autoinc_key.tenant_id_) ||
       max_value <= 0 || base_value > max_value || !sender.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(autoinc_key), K(base_value), K(max_value), K(sender));
  } else {
    autoinc_key_ = autoinc_key;
    base_value_ = base_value;
    max_value_ = max_value;
    sender_ = sender;
    autoinc_version_ = get_modify_autoinc_version(autoinc_version);
    cache_size_ = cache_size;
  }
  return ret;
}

int ObGAISNextSequenceValReq::init(const schema::ObSequenceSchema &schema,
                                   const common::ObAddr &sender)
{
  int ret = OB_SUCCESS;
  if (!is_valid_tenant_id(schema.get_tenant_id()) || !sender.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(schema), K(sender));
  } else if (OB_FAIL(schema_.assign(schema))){
    LOG_WARN("fail to init schemar_", K(ret));
  } else {
    sender_ = sender;
  }
  return ret;
}

int ObGAISNextSequenceValReq::assign(const ObGAISNextSequenceValReq &src_req)
{
  int ret = OB_SUCCESS;
  if (this != &src_req) {
    if (OB_FAIL(schema_.assign(src_req.schema_))) {
      LOG_WARN("fail assign schema_", K(src_req));
    } else {
      sender_ = src_req.sender_;
    }
  }
  return ret;
}

} // share
} // oceanbase
