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
OB_SERIALIZE_MEMBER(ObGAISPushAutoIncValReq, autoinc_key_, base_value_, max_value_, sender_, autoinc_version_);

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
                                  const int64_t &autoinc_version)
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
  }
  return ret;
}

} // share
} // oceanbase
