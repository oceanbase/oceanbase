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
#ifndef OB_HMAC_SIGNATURE_H_
#define OB_HMAC_SIGNATURE_H_
#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <openssl/hmac.h>
#include <openssl/sha.h>
#include <openssl/evp.h>
#include <openssl/bio.h>
#include "lib/container/ob_array.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/string/ob_string_buffer.h"
#include "share/rc/ob_tenant_base.h"
#include "lib/restore/ob_storage_info.h"
namespace oceanbase
{
namespace common
{
int generate_signature_nonce(char *nonce, const int64_t buf_size);

int generate_request_id(char *request_id, const int64_t buf_size);

int base64_encoded(const char *input, const int64_t input_len, char *encoded_result,
    const int64_t encoded_result_buf_len);

int percent_encode(const char *content, char *buf, const int64_t buf_len);

int hmac_sha1(const char *key, const char *data, char *encoded_result, const int64_t encode_buf_len,
    uint32_t &result_len);

int sign_request(ObArray<std::pair<const char *, const char *>> params, const char *method,
    char *signature, const int64_t signature_buf_len);

}  // namespace common
}  // namespace oceanbase
#endif  // OB_HMAC_SIGNATURE_H_