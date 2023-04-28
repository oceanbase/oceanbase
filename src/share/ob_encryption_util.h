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
#include <openssl/evp.h>
#include "lib/container/ob_se_array.h"

#ifndef OCEANBASE_SHARE_OB_ENCRYPTION_UTIIL_H
#define OCEANBASE_SHARE_OB_ENCRYPTION_UTIIL_H

namespace oceanbase
{
namespace common
{
class ObString;
}
namespace share
{
struct ObEncryptMeta;
struct ObZoneEncryptMeta;

class ObKeyGenerator
{
public:
  static int generate_encrypt_key(char *buf, int64_t len);
};

enum ObAesOpMode {
  ob_invalid_mode = 0,
  ob_aes_128_ecb = 1,
  ob_aes_192_ecb = 2,
  ob_aes_256_ecb = 3,
  ob_aes_128_cbc = 4,
  ob_aes_192_cbc = 5,
  ob_aes_256_cbc = 6,
  ob_aes_128_cfb1 = 7,
  ob_aes_192_cfb1 = 8,
  ob_aes_256_cfb1 = 9,
  ob_aes_128_cfb8 = 10,
  ob_aes_192_cfb8 = 11,
  ob_aes_256_cfb8 = 12,
  ob_aes_128_cfb128 = 13,
  ob_aes_192_cfb128 = 14,
  ob_aes_256_cfb128 = 15,
  ob_aes_128_ofb = 16,
  ob_aes_192_ofb = 17,
  ob_aes_256_ofb = 18,
  ob_sm4_mode = 19,
  ob_sm4_cbc_mode = 20,
  //attention:remember to modify compare_aes_mod_safety when add new mode
  ob_max_mode
};

class ObAesEncryption
{
public:
//高级对称加密算法AES
  static int aes_encrypt(const char *key, const int64_t &key_len, const char *data,
                         const int64_t data_len, const int64_t &buf_len, const char *iv,
                         const int64_t iv_len, enum ObAesOpMode mode, char *buf, int64_t &out_len);
  static int aes_decrypt(const char *key, const int64_t &key_len, const char *data,
                         const int64_t data_len, const int64_t &buf_len, const char *iv,
                         const int64_t iv_len, enum ObAesOpMode mode, char *buf, int64_t &out_len);
  static int aes_needs_iv(ObAesOpMode opmode, bool &need_iv);
  static int64_t aes_encrypted_length(const int64_t data_len)
  {
    return (data_len / OB_AES_BLOCK_SIZE + 1) * OB_AES_BLOCK_SIZE;
  }
  static int64_t aes_max_decrypted_length(const int64_t encrypt_len)
  {
    /*encrypt_len is supposed >= OB_AES_BLOCK_SIZE*/
    return encrypt_len - 1 - (encrypt_len % OB_AES_BLOCK_SIZE);
  }
  static int64_t aes_safe_buffer_length(const int64_t data_len)
  {
    return data_len + OB_AES_BLOCK_SIZE;
  }
  //return true if right is more safe then left
  static bool compare_aes_mod_safety(ObAesOpMode left, ObAesOpMode right);
private:
  static void aes_create_key(const unsigned char *key, int key_length, char *rkey, enum ObAesOpMode opmode);
public:
  static const int OB_MAX_AES_KEY_LENGTH = 256;
  static const int OB_AES_IV_SIZE = 16;
  static const int OB_AES_BLOCK_SIZE = 16;
};

const int64_t OB_ROOT_KEY_LEN = 16;
const int64_t OB_ORIGINAL_TABLE_KEY_LEN = 15;
const int64_t OB_ENCRYPTED_TABLE_KEY_LEN = 16;
const int64_t OB_MAX_MASTER_KEY_LENGTH = 16;
const int64_t OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH = 16;
const int64_t OB_ENCRYPT_KEY_SALT_LEN = 8;
const int64_t OB_SALTED_MASTER_KEY_LEN = OB_MAX_MASTER_KEY_LENGTH + OB_ENCRYPT_KEY_SALT_LEN;
const int64_t OB_MAX_ENCRYPTED_KEY_LENGTH = 80;
const int64_t OB_INTERNAL_ENCRYPTED_KEY_LENGTH = 32;

const int64_t OB_CLOG_ENCRYPT_RANDOM_LEN = 16;
const int64_t OB_CLOG_ENCRYPT_MASTER_KEY_LEN = 32;
const int64_t OB_CLOG_ENCRYPT_TABLE_KEY_LEN = 32;

class ObEncryptionUtil
{
public:
  static const ObAesOpMode DEFAULT_TABLE_KEY_ENCRYPT_ALGORITHM = share::ObAesOpMode::ob_aes_128_ecb;
  static const ObAesOpMode MASTER_KEY_ENCRYPT_ALGORITHM = share::ObAesOpMode::ob_aes_128_cbc;

  static int init_ssl_malloc();
  static int parse_encryption_algorithm(const common::ObString &str, ObAesOpMode &encryption_algorithm);
  static int parse_encryption_algorithm(const char *str, ObAesOpMode &encryption_algorithm);
  static int parse_encryption_id(const char *str, int64_t &encrypt_id);
  static int parse_encryption_id(const common::ObString &str, int64_t &encrypt_id);

  // return the max length after encryption
  static int64_t encrypted_length(const int64_t data_len);
  // return an unencrypted data length whose length after encryption is always less than data_len
  static int64_t decrypted_length(const int64_t data_len);
  // return the max length after decryption
  static int64_t safe_buffer_length(const int64_t data_len);
};

struct ObBackupEncryptionMode final
{
  enum EncryptionMode
  {
    NONE = 0, // 不加密
    PASSWORD = 1, // 密码校验
    PASSWORD_ENCRYPTION = 2,//密码校验+加密
    TRANSPARENT_ENCRYPTION = 3,//透明加密
    DUAL_MODE_ENCRYPTION = 4,//透明加密+密码校验
    MAX_MODE
  };
  static bool is_valid(const EncryptionMode &mode);
  static bool is_valid_for_log_archive(const EncryptionMode &mode);
  static const char *to_str(const EncryptionMode &mode);
  static EncryptionMode parse_str(const char *str);
  static EncryptionMode parse_str(const common::ObString &str);
};


enum ObHashAlgorithm {
  OB_HASH_INVALID = 0,
  OB_HASH_MD4,
  OB_HASH_MD5,
  OB_HASH_SH1,
  OB_HASH_SH224,
  OB_HASH_SH256,
  OB_HASH_SH384,
  OB_HASH_SH512,
  OB_HASH_MAX
};

class ObHashUtil
{
public:
  static int hash(const enum ObHashAlgorithm algo, const common::ObString data,
                  common::ObIAllocator &allocator, common::ObString &output);
  static int hash(const enum ObHashAlgorithm algo, const char *data, const int64_t data_len,
                  char *buf, const int64_t &buf_len, int64_t &out_len);
  static int get_hash_output_len(const ObHashAlgorithm algo, int64_t &output_len);
  static int get_sha_hash_algorightm(const int64_t bit_length, ObHashAlgorithm &algo);
private:
  static const EVP_MD* get_hash_evp_md(const ObHashAlgorithm algo);
};

class ObTdeEncryptEngineLoader {
public:
  enum ObEncryptEngineType {
    OB_NONE_ENGINE = 0,
    OB_INVALID_ENGINE = 1,
    OB_AES_ENGINE = 2,
    OB_SM4_ENGINE = 3,
    OB_MAX_ENGINE
  };
  static ObTdeEncryptEngineLoader &get_instance();
  ObTdeEncryptEngineLoader() {
    ssl_init();
    MEMSET(tde_engine_, 0, sizeof(ENGINE*)*OB_MAX_ENGINE);
  }
  ~ObTdeEncryptEngineLoader() { destroy(); }
  void ssl_init();
  void destroy();
  int  load(const common::ObString& engine);
  ObEncryptEngineType get_engine_type(const common::ObString& engine);
  ENGINE* get_tde_engine(ObAesOpMode &mode) const;
  int reload_config();
private:
  bool is_inited_;
  ENGINE* tde_engine_[OB_MAX_ENGINE];
};

}//end share
} //end oceanbase
#endif
