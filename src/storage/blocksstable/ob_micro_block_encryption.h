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

#ifndef OB_MICRO_BLOCK_ENCRYPTION_H_
#define OB_MICRO_BLOCK_ENCRYPTION_H_
#include "ob_data_buffer.h"
#include "share/ob_encryption_util.h"

namespace oceanbase {

namespace blocksstable {

class ObMicroBlockEncryption
{
public:
#ifndef OB_BUILD_TDE_SECURITY
  ObMicroBlockEncryption() {}
  virtual ~ObMicroBlockEncryption() {}
#else
  ObMicroBlockEncryption();
  virtual ~ObMicroBlockEncryption();
  void reset();
  int init(const int64_t encrypt_id, const int64_t tenant_id,
           const int64_t master_key_id = 0, const char *encrypt_key = NULL, const int64_t encrypt_key_len = 0);
  int encrypt(const char *in, const int64_t in_size, const char *&out, int64_t &out_size);
  int decrypt(const char *in, const int64_t in_size, const char *&out, int64_t &out_size);
  int generate_iv(const int64_t macro_seq, const int64_t block_offset);
private:
  //此密钥用于mysql模式下表加解密
  int generate_key();
  //此密钥用于oracle模式下加解密
  int generate_key(char *master_key, const int64_t master_key_len, char *encrypt_key, const int64_t encrypt_key_len);
  bool need_encrypt();
  bool need_refresh(const int64_t encrypt_id, const int64_t tenant_id, const int64_t master_key_id,
      const char *encrypt_key, const int64_t encrypt_key_len);
private:
  bool is_inited_;
  uint64_t tenant_id_;
  ObSelfBufferWriter encrypt_buf_;
  ObSelfBufferWriter decrypt_buf_;
  int64_t encrypt_id_;
  int64_t raw_key_len_;
  char raw_key_[common::OB_MAX_ENCRYPTION_KEY_NAME_LENGTH];      // 真正用于数据加解密的密钥
  char encrypt_key_[common::OB_MAX_ENCRYPTION_KEY_NAME_LENGTH];  // 密钥的密文
  char iv_[share::ObBlockCipher::OB_DEFAULT_IV_LENGTH];
  int64_t encrypt_key_len_;                                      // 密钥的密文的长度
  int64_t master_key_id_;                                        // 主密钥版本
  int64_t iv_len_;
#endif
DISALLOW_COPY_AND_ASSIGN(ObMicroBlockEncryption);
};

}
}
#endif // OB_MICRO_BLOCK_ENCRYPTION_H_
