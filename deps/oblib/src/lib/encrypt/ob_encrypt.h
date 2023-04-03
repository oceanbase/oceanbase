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

#ifndef _OCEANBASE_COMMON_DES_CRYPT_H_
#define _OCEANBASE_COMMON_DES_CRYPT_H_
#include "lib/string/ob_string.h"
#include "lib/ob_define.h"
#include "crypt.h"
namespace oceanbase
{
namespace common
{
class DES_crypt
{
public:
  DES_crypt();
  ~DES_crypt();

  ///@fn stringTobyte translate input string to numerical value of 0 or 1
  ///@param[out] dest the output array of numerical value, which size is src_len * 8
  ///@param[in] src input string
  ///@param[in] src_len len of input string
  static int stringTobyte(char *dest, const int64_t dest_len, const char *src, int64_t src_len);

  ///@fn byteTostring translate input numerical array to string
  ///@param[out] dest the output string, which size if src_len / 8
  ///@param[in] src input numeriacal array
  ///@param[in] src_len len of input array
  static int byteTostring(char *dest, const int64_t dest_len, const char *src, const int64_t src_len);

  ///@fn pad extend the dest's lenght ,to be multiple of 8
  ///@param [in and out] dest malloc new_length in the function
  ///@param[in and out] slen in the original length,out the new_length
  int pad(char *dest, const int64_t dest_len,  int64_t &slen);

  ///@fn unpad lessen the src length to the original length
  int unpad(char *src, int64_t &slen);

  int des_encrypt(const ObString &user_name, const int64_t timestamp, const int64_t skey,
                  char *buffer, int64_t &buffer_length);
  int des_decrypt(ObString &user_name, int64_t &timestamp, const int64_t skey, char *buffer,
                  int64_t buffer_length);
private:
  const static int64_t OB_CRYPT_UNIT = 64;
  const static int64_t OB_BYTE_NUM = 8;
  const static int32_t OB_OP_MASK = 0x0080;
  const static int64_t OB_ENRYPT_KEY_BYTE = sizeof(int64_t) * 8;
  const static int32_t OB_CRYPT_MAGIC_NUM  = 1136;
  //the length include (name_length_length + name_length + timestamp) * 8
  const static int64_t OB_ENCRYPT_TXT_BYTE = 640;

  //const static int64_t OB_MAX_TOKEN_BUFFER_LENGTH = 80;

  const static int ENCODE_FLAG = 0;
  const static int DECODE_FLAG = 1;
private:
  char keybyte_[OB_ENRYPT_KEY_BYTE];
  char txtbyte_[OB_ENCRYPT_TXT_BYTE];
  char txtptr_[OB_MAX_TOKEN_BUFFER_LENGTH];
  int32_t original_txt_length_;
  struct crypt_data data_;
};
}
}
#endif

