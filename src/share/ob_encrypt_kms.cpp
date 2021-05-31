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

#include "share/ob_encrypt_kms.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;

int ObKmsClient::init(const char* kms_info, int64_t kms_len)
{
  int ret = OB_SUCCESS;
  UNUSED(kms_info);
  UNUSED(kms_len);
  return ret;
}

int ObKmsClient::check_param_valid()
{
  return OB_NOT_SUPPORTED;
}

bool ObKmsClient::is_sm_scene()
{
  bool ret = false;
  return ret;
}
