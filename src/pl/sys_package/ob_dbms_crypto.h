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

#ifndef OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_CRYPTO_H_
#define OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_CRYPTO_H_
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
namespace pl
{

struct ObCryptoCipherCtx
{
  enum EncryptAlgorithm {
    ENCRYPT_INVALID = 0,
    ENCRYPT_DES,
    ENCRYPT_3DES_2KEY,
    ENCRYPT_3DES,
    ENCRYPT_AES128 = 6,
    ENCRYPT_AES192,
    ENCRYPT_AES256,
    ENCRYPT_MAX
  };
  enum BlockChainMode {
    CHAIN_INVALID = 0,
    CHAIN_CBC,
    CHAIN_CFB,
    CHAIN_ECB,
    CHAIN_OFB,
    CHAIN_MAX
  };
  enum PaddingMode {
    PAD_INVALID = 0,
    PAD_PKCS5,
    PAD_NONE,
    PAD_MAX
  };
  static const int32_t CHAIN_MODE_SHIFT = 8;

  ObCryptoCipherCtx()
    : algo_(ENCRYPT_INVALID), chain_mode_(CHAIN_INVALID), pad_mode_(PAD_INVALID)
  {}
  ~ObCryptoCipherCtx() = default;
  int parse_block_cipher_type(const int16_t type);
  int get_cipher_type(const EVP_CIPHER *&cipher);
  int get_cipher_block_size(int64_t &block_size);
  int get_encrypted_length(const int64_t input_len, int64_t &buf_len);
  int check_key_length(const common::ObString &key_str);
  int check_input_length(const ObString &input_str, const bool in_decrypt);
  int get_real_iv(const common::ObString &orig_iv,
                         common::ObIAllocator &allocator,
                         common::ObString &real_iv);
  bool is_stream_cipher_mode();
  bool need_padding();
  bool need_manual_padding();
  int pkcs_padding(const common::ObString &in_str,
                   common::ObIAllocator &allocator,
                   common::ObString &pad_str);
  int pkcs_unpadding(const common::ObString &in_str, int64_t &out_len);

  TO_STRING_KV(K_(algo), K_(chain_mode), K_(pad_mode));

  EncryptAlgorithm algo_;
  BlockChainMode chain_mode_;
  PaddingMode pad_mode_;
};

class ObDbmsCrypto
{
public:
  enum HashAlgorithm {
    HASH_INVALID = 0,
    HASH_MD4,
    HASH_MD5,
    HASH_SH1,
    HASH_SH256,
    HASH_SH384,
    HASH_SH512,
    HASH_MAX
  };

  static int encrypt(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int decrypt(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int hash(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int parse_hash_algorithm(const int32_t type, HashAlgorithm &algo);
  static int get_hash_output_len(const HashAlgorithm algo, int64_t &output_len);
  static const EVP_MD* get_hash_evp_md(const HashAlgorithm algo);
};

} // end of pl
} // end of oceanbase
#endif /* OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_CRYPTO_H_ */
