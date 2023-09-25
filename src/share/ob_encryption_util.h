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
  static int generate_encrypt_key_char(char *buf, int64_t len);
};

enum ObCipherOpMode {
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
  ob_sm4_mode = 19,     // old sm4_ctr using NULL as IV which is wrong, should not be used
  ob_sm4_cbc_mode = 20, // sm4_cbc, use NULL as IV
  ob_aes_128_gcm = 21,
  ob_aes_192_gcm = 22,
  ob_aes_256_gcm = 23,
  ob_sm4_cbc = 24,     // sm4_cbc, use random iv
  ob_sm4_ecb = 25,
  ob_sm4_ofb = 26,
  ob_sm4_cfb128 = 27,
  ob_sm4_ctr = 28, // sm4_ctr using unique iv
  ob_sm4_gcm = 29,
  /* attention:
    1.remember to modify compare_aes_mod_safety when add new mode
    2.considering compatibility of parse_encryption_id, only add modes sequentially
  */
  ob_max_mode
};

class ObBlockCipher
{
public:
  static int encrypt(const char *key, const int64_t key_len,
                     const char *data, const int64_t data_len, const int64_t buf_len,
                     const char *iv, const int64_t iv_len, const char *aad, const int64_t aad_len,
                     const int64_t tag_len, const ObCipherOpMode mode, char *buf, int64_t &out_len,
                     char *tag);
  static int decrypt(const char *key, const int64_t key_len,
                     const char *data, const int64_t data_len, const int64_t buf_len,
                     const char *iv, const int64_t iv_len, const char *aad, const int64_t aad_len,
                     const char *tag, const int64_t tag_len, const ObCipherOpMode mode, char *buf,
                     int64_t &out_len);
  static bool is_valid_cipher_opmode(const ObCipherOpMode opmode);
  static bool is_need_iv(const ObCipherOpMode opmode);
  static bool is_need_aead(const ObCipherOpMode opmode);
  static int get_key_length(const ObCipherOpMode opmode);
  static int64_t get_iv_length(const ObCipherOpMode opmode);
  static int64_t get_aead_tag_length(const ObCipherOpMode opmode);
  static int64_t get_ciphertext_length(const ObCipherOpMode opmode, const int64_t plaintext_len);
  static int64_t get_max_plaintext_length(const ObCipherOpMode opmode,
                                          const int64_t ciphertext_len);
  //return true if right is more safe then left
  static bool compare_aes_mod_safety(ObCipherOpMode left, ObCipherOpMode right);
private:
  static bool is_need_padding(const ObCipherOpMode opmode);
  static void create_key(const unsigned char *key, int key_length, char *rkey,
                         enum ObCipherOpMode opmode);
public:
  static const int OB_MAX_CIPHER_KEY_LENGTH = 256; // max aes key bit length
  static const int OB_DEFAULT_IV_LENGTH = 16;
  static const int OB_CIPHER_BLOCK_LENGTH = 16;
  static const int OB_DEFAULT_AEAD_AAD_LENGTH = 16;
  static const int OB_DEFAULT_AEAD_TAG_LENGTH = 16;
};

const int64_t OB_ROOT_KEY_LEN = 16;
const int64_t OB_ORIGINAL_TABLE_KEY_LEN = 15;
const int64_t OB_ENCRYPTED_TABLE_KEY_LEN = 16;
const int64_t OB_INTERNAL_MASTER_KEY_LENGTH = 16;
const int64_t OB_MAX_MASTER_KEY_LENGTH = 32;
const int64_t OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH = 16;
const int64_t OB_ENCRYPT_KEY_SALT_LEN = 8;
const int64_t OB_SALTED_MASTER_KEY_LEN = OB_INTERNAL_MASTER_KEY_LENGTH + OB_ENCRYPT_KEY_SALT_LEN;
const int64_t OB_MAX_ENCRYPTED_KEY_LENGTH = 256;
const int64_t OB_INTERNAL_ENCRYPTED_KEY_LENGTH = 32;
const int64_t OB_CLOG_ENCRYPT_MASTER_KEY_LEN = 32;
const int64_t OB_CLOG_ENCRYPT_TABLE_KEY_LEN = 32;

class ObEncryptionUtil
{
public:

  static int init_ssl_malloc();
#ifdef OB_BUILD_TDE_SECURITY
  static bool need_encrypt(const ObCipherOpMode opmode);
  static int get_tde_method(int64_t tenant_id, common::ObString &tde_method);
  static int get_tde_kms_info(int64_t tenant_id, common::ObString &kms_info);
#endif
  static int parse_encryption_algorithm(const common::ObString &str,
                                        ObCipherOpMode &encryption_algorithm);
  static int parse_encryption_algorithm(const char *str,
                                        ObCipherOpMode &encryption_algorithm);
  static int parse_encryption_id(const char *str, int64_t &encrypt_id);
  static int parse_encryption_id(const common::ObString &str, int64_t &encrypt_id);
  static bool is_aes_encryption(const ObCipherOpMode opmode);
  static bool is_sm4_encryption(const ObCipherOpMode opmode);

#ifdef OB_BUILD_TDE_SECURITY
  static const ObCipherOpMode DEFAULT_TABLE_KEY_AES_ENCRYPT_ALGORITHM =
                                                              share::ObCipherOpMode::ob_aes_128_ecb;
  static const ObCipherOpMode DEFAULT_TABLE_KEY_SM4_ENCRYPT_ALGORITHM =
                                                             share::ObCipherOpMode::ob_sm4_cbc_mode;
  static const ObCipherOpMode DEFAULT_TABLE_KEY_AES256_ENCRYPT_ALGORITHM =
                                                             share::ObCipherOpMode::ob_aes_256_ecb;
  static const ObCipherOpMode SYS_DATA_ENCRYPT_ALGORITHM = share::ObCipherOpMode::ob_aes_128_cbc;
  static int64_t sys_encrypted_length(int64_t data_len);
  // encrypt data with create a random iv and store iv in buf if needed.
  static int encrypt_data(const char *key, const int64_t key_len, enum ObCipherOpMode mode,
                          const char *data, const int64_t data_len,
                          char *buf, const int64_t buf_len, int64_t &out_len);
  // using to decrypt data which encrypted by encrypt_data
  static int decrypt_data(const char *key, const int64_t key_len, enum ObCipherOpMode mode,
                          const char *data, const int64_t data_len,
                          char *buf, const int64_t buf_len, int64_t &out_len);
  static int encrypt_data(const char *key, const int64_t key_len, enum ObCipherOpMode mode,
                          const char *data, const int64_t data_len, const char *iv,
                          const int64_t iv_len, char *buf, const int64_t buf_len, int64_t &out_len);
  static int encrypt_data(const share::ObEncryptMeta &meta,
                          const char *data, const int64_t data_len,
                          char *buf, int64_t const buf_len, int64_t &out_len);
  static int decrypt_data(const share::ObEncryptMeta &meta,
                          const char *data, const int64_t data_len,
                          char *buf, int64_t const buf_len, int64_t &out_len);
  static int create_encrypted_table_key(const uint64_t tenant_id, uint64_t &master_key_id,
                                        char *out_buf, const int64_t out_buf_len, int64_t &out_len,
                                        int retry_times = 3);
  static int decrypt_table_key(const uint64_t tenant_id,
                               const char *master_key, const int64_t master_key_len,
                               const char *encrypt_key, const int64_t encrypt_key_len,
                               char *out_buf, const int64_t out_buf_len, int64_t &out_len);
  static int decrypt_table_key(share::ObEncryptMeta &meta);
  static int encrypt_master_key(const uint64_t tenant_id, const char *data, const int64_t data_len,
                                char *buf, const int64_t buf_len, int64_t &out_len);
  static int decrypt_master_key(const uint64_t tenant_id, const char *data, const int64_t data_len,
                                char *buf, const int64_t buf_len, int64_t &out_len);
  static int encrypt_sys_data(const uint64_t tenant_id, const char *data, const int64_t data_len,
                              char *buf, const int64_t buf_len, int64_t &out_len);
  static int decrypt_sys_data(const uint64_t tenant_id, const char *data, const int64_t data_len,
                              char *buf, const int64_t buf_len, int64_t &out_len);
  static int encrypt_zone_data(share::ObZoneEncryptMeta &meta,
                               const char *data, const int64_t data_len,
                               char *buf, const int64_t buf_len, int64_t &out_len);
  static int decrypt_zone_data(const share::ObZoneEncryptMeta &meta,
                               const char *data, const int64_t data_len,
                               char *buf, const int64_t buf_len, int64_t &out_len);
#endif

#ifdef OB_BUILD_TDE_SECURITY
  // return the max buf length after encryption
  static int64_t encrypted_length(const ObCipherOpMode mode, const int64_t data_len);
  // return an unencrypted data length whose length after encryption is always less than data_len
  static int64_t decrypted_length(const ObCipherOpMode mode, const int64_t data_len);
  // return the max length after decryption
  static int64_t safe_buffer_length(const int64_t data_len);
private:
  static int encrypt_sys_data_default(const char *data, const int64_t data_len,
                                      char *buf, const int64_t buf_len, int64_t &out_len);
  static int decrypt_sys_data_default(const char *data, const int64_t data_len,
                                      char *buf, const int64_t buf_len, int64_t &out_len);
private:
  static const char* system_encrypt_key_;
  static const char* system_encrypt_iv_;
#endif
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

#ifdef OB_BUILD_TDE_SECURITY
class ObTdeMethodUtil
{
public:
  static bool is_valid(const common::ObString &tde_method);
  static bool is_internal(const common::ObString &tde_method);
  static bool is_kms(const common::ObString &tde_method);
  static bool is_sm_algorithm(const common::ObString &tde_method);
  static bool is_aes256_algorithm(const common::ObString &tde_method);
  static bool use_external_key_id(const common::ObString &tde_method);
  static ObString extract_kms_name(const common::ObString &tde_method);
  static ObString extract_mode_name(const common::ObString &tde_method);
private:
  static bool inner_is_internal(const common::ObString &kms_name);
  static bool inner_is_kms(const common::ObString &kms_name);
  static bool is_valid_kms(const common::ObString &kms_name);
  static bool is_valid_mode(const common::ObString &mode_name);
};
#endif

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
  ENGINE* get_tde_engine(ObCipherOpMode &mode) const;
  int reload_config();
private:
  bool is_inited_;
  ENGINE* tde_engine_[OB_MAX_ENGINE];
};

}//end share
} //end oceanbase
#endif
