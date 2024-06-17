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

#ifndef OB_BUILD_TDE_SECURITY

#define USING_LOG_PREFIX SHARE
#include "share/ob_encryption_util.h"
#include <openssl/md4.h>
#include <openssl/md5.h>
#include <openssl/sha.h>
#include "share/ob_encryption_struct.h"
#include "lib/alloc/alloc_assist.h"
#include "share/ob_errno.h"
#include "lib/atomic/atomic128.h"
#include "lib/string/ob_string.h"
#include "lib/random/ob_random.h"
#include "lib/utility/ob_macro_utils.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_tenant_config_mgr.h"

namespace oceanbase
{
using namespace common;
namespace share
{

int ObKeyGenerator::generate_encrypt_key(char *buf, int64_t len)
{
  int ret = OB_SUCCESS;
  if (len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the buf of len is invalid", K(ret));
  } else if (!RAND_bytes((unsigned char*)buf, (int)len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("failed to get a rand bytes");
  }
  return ret;
}

int ObKeyGenerator::generate_encrypt_key_char(char *buf, int64_t len)
{
  int ret = OB_SUCCESS;
  if (len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the buf of len is invalid", K(ret));
  } else {
    int i;
    for (i = 0; i < len; ++i) {
      switch (common::ObRandom::rand(0, 2)) {
        case 1:
        buf[i] = 'A' + common::ObRandom::rand(0, 25);
        break;
        case 2:
        buf[i] = 'a' + common::ObRandom::rand(0, 25);
        break;
        default:
        buf[i] = '0' + common::ObRandom::rand(0, 9);
        break;
      }
    }
  }
  return ret;
}

static const EVP_CIPHER *get_evp_cipher(const ObCipherOpMode mode)
{
  switch (mode)
  {
    case ob_aes_128_ecb:    return EVP_aes_128_ecb();
    case ob_aes_128_cbc:    return EVP_aes_128_cbc();
    case ob_aes_128_cfb1:   return EVP_aes_128_cfb1();
    case ob_aes_128_cfb8:   return EVP_aes_128_cfb8();
    case ob_aes_128_cfb128: return EVP_aes_128_cfb128();
    case ob_aes_128_ofb:    return EVP_aes_128_ofb();
    case ob_aes_192_ecb:    return EVP_aes_192_ecb();
    case ob_aes_192_cbc:    return EVP_aes_192_cbc();
    case ob_aes_192_cfb1:   return EVP_aes_192_cfb1();
    case ob_aes_192_cfb8:   return EVP_aes_192_cfb8();
    case ob_aes_192_cfb128: return EVP_aes_192_cfb128();
    case ob_aes_192_ofb:    return EVP_aes_192_ofb();
    case ob_aes_256_ecb:    return EVP_aes_256_ecb();
    case ob_aes_256_cbc:    return EVP_aes_256_cbc();
    case ob_aes_256_cfb1:   return EVP_aes_256_cfb1();
    case ob_aes_256_cfb8:   return EVP_aes_256_cfb8();
    case ob_aes_256_cfb128: return EVP_aes_256_cfb128();
    case ob_aes_256_ofb:    return EVP_aes_256_ofb();
    case ob_aes_128_gcm:    return EVP_aes_128_gcm();
    case ob_aes_192_gcm:    return EVP_aes_192_gcm();
    case ob_aes_256_gcm:    return EVP_aes_256_gcm();
#ifdef OB_USE_BABASSL
    case ob_sm4_mode:       return EVP_sm4_ctr();
    case ob_sm4_cbc_mode:   return EVP_sm4_cbc();
    case ob_sm4_cbc:        return EVP_sm4_cbc();
    case ob_sm4_ecb:        return EVP_sm4_ecb();
    case ob_sm4_ofb:        return EVP_sm4_ofb();
    case ob_sm4_cfb128:     return EVP_sm4_cfb128();
    case ob_sm4_ctr:        return EVP_sm4_ctr();
    case ob_sm4_gcm:        return EVP_sm4_gcm();
#endif
    default: return NULL;
  }
}

int ObBlockCipher::get_key_length(const ObCipherOpMode opmode)
{
  int length = 0;
  const EVP_CIPHER *cipher = get_evp_cipher(opmode);
  if (NULL != cipher) {
    length = EVP_CIPHER_key_length(cipher);
  }
  return length;
}

bool ObBlockCipher::is_need_iv(const ObCipherOpMode opmode)
{
  bool need_iv = false;
  const EVP_CIPHER *cipher = get_evp_cipher(opmode);
  if (NULL != cipher) {
    int default_iv_length = EVP_CIPHER_iv_length(cipher);
    need_iv = default_iv_length == 0 ? false : true;
  }
  return need_iv;
}

bool ObBlockCipher::is_need_aead(const ObCipherOpMode opmode)
{
  bool need_aead = false;
  const EVP_CIPHER *cipher = get_evp_cipher(opmode);
  if (NULL != cipher) {
    need_aead = EVP_CIPHER_mode(cipher) == EVP_CIPH_GCM_MODE;
  }
  return need_aead;
}

bool ObBlockCipher::is_need_padding(const ObCipherOpMode opmode)
{
  bool need_padding = false;
  const EVP_CIPHER *cipher = get_evp_cipher(opmode);
  if (NULL != cipher) {
    need_padding = EVP_CIPH_ECB_MODE == EVP_CIPHER_mode(cipher) ||
                   EVP_CIPH_CBC_MODE == EVP_CIPHER_mode(cipher);
  }
  return need_padding;
}

int64_t ObBlockCipher::get_iv_length(const ObCipherOpMode opmode)
{
  int64_t iv_length = 0;
  const EVP_CIPHER *cipher = get_evp_cipher(opmode);
  if (NULL != cipher) {
    iv_length = EVP_CIPHER_iv_length(cipher);
  }
  return iv_length;
}

int64_t ObBlockCipher::get_aead_tag_length(const ObCipherOpMode opmode)
{
  return is_need_aead(opmode) ? OB_DEFAULT_AEAD_TAG_LENGTH : 0;
}

int64_t ObBlockCipher::get_ciphertext_length(const ObCipherOpMode opmode, const int64_t data_len)
{
  return is_need_padding(opmode) ?
         (data_len / OB_CIPHER_BLOCK_LENGTH + 1) * OB_CIPHER_BLOCK_LENGTH : data_len;
}

int64_t ObBlockCipher::get_max_plaintext_length(const ObCipherOpMode opmode,
                                                const int64_t encrypt_len)
{
  return is_need_padding(opmode) ?
         encrypt_len - 1 - (encrypt_len % OB_CIPHER_BLOCK_LENGTH) : encrypt_len;
}

void ObBlockCipher::create_key(const unsigned char *key, int key_length, char *rkey,
                               enum ObCipherOpMode opmode)
{
  const int key_size= get_key_length(opmode);
  char *rkey_end = NULL;
  char *ptr = NULL;                                   /* 真正的key的起始位置 */
  char *sptr = NULL;
  char *key_end= ((char *)key) + key_length;
  rkey_end= rkey + key_size;
  memset(rkey, 0, key_size);          /* 初始化key */
  for (ptr= rkey, sptr= (char *)key; sptr < key_end; ptr++, sptr++)
  {
    if (ptr == rkey_end)
      ptr= rkey;
    *ptr^= *sptr;
  }
}

bool ObBlockCipher::is_valid_cipher_opmode(const ObCipherOpMode opmode)
{
  return opmode > ObCipherOpMode::ob_invalid_mode && opmode < ObCipherOpMode::ob_max_mode &&
         opmode != ObCipherOpMode::ob_sm4_mode;
}

int ObBlockCipher::encrypt(const char *key, const int64_t key_len,
                           const char *data, const int64_t data_len, const int64_t buf_len,
                           const char *iv, const int64_t iv_len,
                           const char *aad, const int64_t aad_len, const int64_t tag_len,
                           enum ObCipherOpMode mode, char *buf, int64_t &out_len, char *tag)
{
  int ret = OB_SUCCESS;
  bool need_iv = is_need_iv(mode);
  bool need_aead = is_need_aead(mode);
  bool need_padding = is_need_padding(mode);
  if (!is_valid_cipher_opmode(mode) ||
      (OB_ISNULL(key) && key_len != 0) || key_len < 0 ||  // allow NULL key
      (OB_ISNULL(data) && data_len != 0) ||   data_len < 0 || // allow NULL data
      OB_ISNULL(buf) || buf_len < get_ciphertext_length(mode, data_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(data_len), K(key_len), K(buf_len), KP(buf), KP(data),
             KP(key), K(mode));
  } else if (need_iv && ((iv_len != 0 && OB_ISNULL(iv)) ||
                         (iv_len != 0 && iv_len != get_iv_length(mode)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid initial vector argument", K(ret), K(iv_len), KP(iv), K(mode));
  } else if (need_aead && ((aad_len != 0 && OB_ISNULL(aad)) ||
                           (aad_len != 0 && aad_len != OB_DEFAULT_AEAD_AAD_LENGTH) ||
                           tag_len != OB_DEFAULT_AEAD_TAG_LENGTH || OB_ISNULL(tag))){
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid aead argument", K(ret), K(aad_len), K(tag_len), KP(aad), K(mode));
  } else {
    int u_len=0, f_len=0;
    unsigned char rkey[OB_MAX_CIPHER_KEY_LENGTH / 8];
    unsigned char *iv_encrypt = (!need_iv || iv_len == 0) ? NULL : (unsigned char *)iv;
    unsigned char *add_encrypt = (!need_aead || aad_len == 0) ? NULL : (unsigned char *)aad;
    ENGINE *engine = ObTdeEncryptEngineLoader::get_instance().get_tde_engine(mode);
    EVP_CIPHER_CTX *ctx = EVP_CIPHER_CTX_new();
    const EVP_CIPHER *cipher = get_evp_cipher(mode);
    if (NULL != engine) {
      if (EXECUTE_COUNT_PER_SEC(10)) {
        LOG_INFO("tde use engine to encrypt data", K(mode));
      }
    }
    if (OB_ISNULL(ctx) ||OB_ISNULL(cipher)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("get EVP_ctx or EVP_cipher is NULL", K(ret), KP(ctx), KP(cipher));
    } else if (FALSE_IT(create_key((const unsigned char *)key, (int)key_len, (char *)rkey, mode))) {
    } else if (FALSE_IT(EVP_CIPHER_CTX_init(ctx))) {
    } else if (!EVP_EncryptInit_ex(ctx, cipher, engine, rkey, (unsigned char*)iv_encrypt)) {
      ret = OB_ERR_AES_ENCRYPT;
      LOG_WARN("fail to init evp encrytion cipher and engine in encrypt", K(ret));
    } else if (need_padding && !EVP_CIPHER_CTX_set_padding(ctx, true)) {
      ret = OB_ERR_AES_ENCRYPT;
      LOG_WARN("fail to set padding in encrypt", K(ret));
    } else if (need_aead && NULL != add_encrypt && !EVP_EncryptUpdate(ctx, NULL, &u_len, (unsigned char *)add_encrypt, (int)aad_len)) {
      ret = OB_ERR_AES_ENCRYPT;
      LOG_WARN("fail to set AAD in encrypt", K(ret));
    } else if (!EVP_EncryptUpdate(ctx, (unsigned char *)buf, &u_len, (unsigned char *)data, (int)data_len)) {
      ret = OB_ERR_AES_ENCRYPT;
      LOG_WARN("fail to encrypt data in encrypt", K(ret));
    } else if (!EVP_EncryptFinal(ctx, (unsigned char*)buf + u_len, &f_len)) {
      ret = OB_ERR_AES_ENCRYPT;
      LOG_WARN("fail to encrypt final frame data in encrypt", K(ret));
    } else if (need_aead && !EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_GET_TAG, (int)tag_len, (unsigned char*)tag)) {
      ret = OB_ERR_AES_ENCRYPT;
      LOG_WARN("fail to encrypt final frame data in encrypt", K(ret));
    } else {
      out_len = u_len + f_len;
    }
    if (OB_UNLIKELY(OB_ERR_AES_ENCRYPT == ret)) {
      common::ObString err_reason = common::ObString::make_string(ERR_reason_error_string(ERR_get_error()));
      LOG_WARN("encrypt failed", K(err_reason));
    }
    if (OB_NOT_NULL(ctx)) {
      EVP_CIPHER_CTX_free(ctx);
    }
  }
  return ret;
}

int ObBlockCipher::decrypt(const char *key, const int64_t key_len,
                           const char *data, const int64_t data_len, const int64_t buf_len,
                           const char *iv, const int64_t iv_len, const char *aad,
                           const int64_t aad_len, const char *tag, const int64_t tag_len,
                           enum ObCipherOpMode mode, char *buf, int64_t &out_len)
{
  int ret = OB_SUCCESS;
  bool need_iv = is_need_iv(mode);
  bool need_aead = is_need_aead(mode);
  bool need_padding = is_need_padding(mode);
  if (!is_valid_cipher_opmode(mode) ||
      (OB_ISNULL(key) && key_len != 0) || key_len < 0 ||
      (OB_ISNULL(data) && data_len != 0) || data_len < 0 ||
      OB_ISNULL(buf) || buf_len < data_len) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(data_len), K(key_len), K(buf_len), KP(buf), KP(data),
             KP(key), K(mode));
  } else if (need_iv && ((iv_len != 0 && OB_ISNULL(iv)) ||
                         (iv_len != 0 && iv_len != get_iv_length(mode)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid initial vector argument", K(ret), K(iv_len), KP(iv), K(mode));
  } else if (need_aead && ((aad_len != 0 && OB_ISNULL(aad)) ||
                           (aad_len != 0 && aad_len != OB_DEFAULT_AEAD_AAD_LENGTH) ||
                           tag_len != OB_DEFAULT_AEAD_TAG_LENGTH || OB_ISNULL(tag))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid aead argument", K(ret), K(aad_len), K(tag_len), KP(aad), KP(tag), K(mode));
  } else {
    int u_len=0, f_len=0;
    unsigned char rkey[OB_MAX_CIPHER_KEY_LENGTH / 8];
    unsigned char *iv_decrypt = (!need_iv || iv_len == 0) ? NULL : (unsigned char *)iv;
    unsigned char *add_decrypt = (!need_aead || aad_len == 0) ? NULL : (unsigned char *)aad;
    ENGINE *engine = ObTdeEncryptEngineLoader::get_instance().get_tde_engine(mode);
    EVP_CIPHER_CTX *ctx = EVP_CIPHER_CTX_new();
    const EVP_CIPHER *cipher = get_evp_cipher(mode);
    if (NULL != engine) {
      if (EXECUTE_COUNT_PER_SEC(10)) {
        LOG_INFO("use engine to decrypt data", K(mode));
      }
    }
    if (OB_ISNULL(ctx) || OB_ISNULL(cipher)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("fail to to init cipher ctx in decrypt", K(ret), KP(ctx), KP(cipher));
    } else if (FALSE_IT(create_key((const unsigned char *)key, (int)key_len, (char *)rkey, mode))) {
    } else if (FALSE_IT(EVP_CIPHER_CTX_init(ctx))) {
    } else if (!EVP_DecryptInit_ex(ctx, cipher, engine, rkey, (unsigned char*)iv_decrypt)) {
      ret = OB_ERR_AES_DECRYPT;
      LOG_WARN("fail to init evp ctx in decrypt", K(ret));
    } else if (need_padding && !EVP_CIPHER_CTX_set_padding(ctx, true)) {
      ret = OB_ERR_AES_DECRYPT;
      LOG_WARN("fail to set padding in decrypt", K(ret));
    } else if (need_aead && NULL != add_decrypt && !EVP_DecryptUpdate(ctx, NULL, &u_len, (const unsigned char *)add_decrypt, (int)aad_len)) {
      ret = OB_ERR_AES_DECRYPT;
      LOG_WARN("fail to set AAD in decrypt", K(ret));
    } else if (!EVP_DecryptUpdate(ctx, (unsigned char *)buf, &u_len, (const unsigned char *)data, (int)data_len)) {
      ret = OB_ERR_AES_DECRYPT;
      LOG_WARN("fail to decrypt data in decrypt", K(ret));
    } else if (need_aead && !EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_TAG, (int)tag_len, (unsigned char *)tag)) {
      ret = OB_ERR_AES_DECRYPT;
    } else if (!EVP_DecryptFinal(ctx, (unsigned char*)buf + u_len, &f_len)) {
      ret = OB_ERR_AES_DECRYPT;
      LOG_WARN("fail to decrypt final frame data in decrypt", K(ret), K(u_len), K(f_len));
    } else {
      out_len = u_len + f_len;
    }
    if (OB_UNLIKELY(OB_ERR_AES_DECRYPT == ret)) {
      common::ObString err_reason = common::ObString::make_string(ERR_reason_error_string(ERR_get_error()));
      LOG_WARN("decrypt failed", K(err_reason));
    }
    if (OB_NOT_NULL(ctx)) {
      EVP_CIPHER_CTX_free(ctx);
    }
  }
  return ret;
}

static void* ob_malloc_openssl(size_t nbyte, const char *, int)
{
  ObMemAttr attr;
  attr.ctx_id_ = ObCtxIds::GLIBC;
  attr.label_ = ObModIds::OB_BUFFER;
  SET_IGNORE_MEM_VERSION(attr);
  lib::ObMallocHookAttrGuard guard(attr);
  return malloc(nbyte);
}

static void* ob_realloc_openssl(void *ptr, size_t nbyte, const char *, int)
{
  ObMemAttr attr;
  attr.ctx_id_ = ObCtxIds::GLIBC;
  attr.label_ = ObModIds::OB_BUFFER;
  SET_IGNORE_MEM_VERSION(attr);
  lib::ObMallocHookAttrGuard guard(attr);
  return realloc(ptr, nbyte);
}

static void ob_free_openssl(void *ptr, const char *, int)
{
  free(ptr);
}

int ObEncryptionUtil::init_ssl_malloc()
{
  int ret = OB_SUCCESS;
  int tmp_ret = CRYPTO_set_mem_functions(ob_malloc_openssl, ob_realloc_openssl, ob_free_openssl);
  if (OB_UNLIKELY(tmp_ret != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to set crypto mem functions", K(tmp_ret), K(ret));
  }
  return ret;
}

int ObEncryptionUtil::parse_encryption_algorithm(const char *str,
                                                 ObCipherOpMode &encryption_algorithm)
{
  int ret = OB_SUCCESS;
  encryption_algorithm = ObCipherOpMode::ob_invalid_mode;
  return ret;
}

int ObEncryptionUtil::parse_encryption_id(const ObString &str, int64_t &encrypt_id)
{
  int ret = OB_SUCCESS;
  ObCipherOpMode encryption_algorithm = ObCipherOpMode::ob_invalid_mode;
  return ret;
}

bool ObEncryptionUtil::is_aes_encryption(const ObCipherOpMode opmode) {
  return (opmode >= ObCipherOpMode::ob_aes_128_ecb && opmode <= ObCipherOpMode::ob_aes_256_ofb) ||
         (opmode >= ObCipherOpMode::ob_aes_128_gcm && opmode <= ObCipherOpMode::ob_aes_256_gcm);
}

bool ObEncryptionUtil::is_sm4_encryption(const ObCipherOpMode opmode) {
  return (opmode >= ObCipherOpMode::ob_sm4_mode && opmode <= ObCipherOpMode::ob_sm4_cbc_mode) ||
         (opmode >= ObCipherOpMode::ob_sm4_cbc && opmode <= ObCipherOpMode::ob_sm4_gcm);
}

bool ObBackupEncryptionMode::is_valid(const EncryptionMode &mode)
{
  return mode >= NONE && mode < MAX_MODE;
}

//TODO(yaoying.yyy):暂时只支持tde，后续需要更新
bool ObBackupEncryptionMode::is_valid_for_log_archive(const EncryptionMode &mode)
{
  return (NONE == mode || TRANSPARENT_ENCRYPTION == mode);

}

const char *backup_encryption_strs[] =
{
  "NONE",
  "PASSWORD",
  "PASSWORD_ENCRYPTION",
  "TRANSPARENT_ENCRYPTION",
  "DUAL_MODE_ENCRYPTION",
};

const char *ObBackupEncryptionMode::to_str(const EncryptionMode &mode)
{
  const char *str = "UNKNOWN";

  if (is_valid(mode)) {
    str = backup_encryption_strs[mode];
  }
  return str;
}

ObBackupEncryptionMode::EncryptionMode ObBackupEncryptionMode::parse_str(const char *str)
{
  ObString obstr(str);
  return parse_str(obstr);
}

ObBackupEncryptionMode::EncryptionMode ObBackupEncryptionMode::parse_str(const common::ObString &str)
{
  EncryptionMode mode = MAX_MODE;
  const int64_t count = ARRAYSIZEOF(backup_encryption_strs);
  STATIC_ASSERT(static_cast<int64_t>(ObBackupEncryptionMode::MAX_MODE) == count,
      "encryption mode count mismatch");

  if (str.empty()) {
    mode = NONE;
  } else {
    for (int64_t i = 0; i < count; ++i) {
      if (0 == str.case_compare(backup_encryption_strs[i])) {
        mode = static_cast<EncryptionMode>(i);
        break;
      }
    }
  }
  return mode;
}

int ObHashUtil::hash(const enum ObHashAlgorithm algo, const ObString data,
                     ObIAllocator &allocator, ObString &output)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  const char *data_ptr = NULL != data.ptr() ? data.ptr() : "";
  int64_t buf_len = 0;
  int64_t out_len = 0;
  if (OB_FAIL(get_hash_output_len(algo, buf_len))) {
    LOG_WARN("fail to get hash output len", K(algo), K(ret));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret));
  } else if (OB_FAIL(hash(algo, data_ptr, data.length(), buf, buf_len, out_len))) {
    LOG_WARN("fail to calc hash output", K(ret));
  } else {
    output.assign_ptr(buf, static_cast<int32_t>(out_len));
  }
  return ret;
}

int ObHashUtil::hash(const enum ObHashAlgorithm algo, const char *data, const int64_t data_len,
                       char *buf, const int64_t &buf_len, int64_t &out_len)
{
  int ret = OB_SUCCESS;
  int64_t expect_out_len = 0;
  if (ObHashAlgorithm::OB_HASH_INVALID == algo || OB_ISNULL(data) || data_len < 0 ||
      OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(algo), K(data_len), K(buf_len), K(ret));
  } else if (OB_FAIL(get_hash_output_len(algo, expect_out_len))) {
    LOG_WARN("fail to get hash output len", K(algo), K(ret));
  } else if (OB_UNLIKELY(buf_len < expect_out_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(algo), K(buf_len), K(expect_out_len), K(ret));
  } else {
    unsigned int res_len = 0;
    const EVP_MD *md = get_hash_evp_md(algo);
    EVP_MD_CTX *mdctx = EVP_MD_CTX_create();
    if (OB_ISNULL(mdctx) || OB_ISNULL(md)) {
      ret = OB_ERR_AES_ENCRYPT;
      LOG_WARN("fail to init hash ctx", K(ret));
    } else if (!EVP_DigestInit_ex(mdctx, md, NULL)) {
      ret = OB_ERR_AES_ENCRYPT;
      LOG_WARN("fail to init hash ctx", K(ret));
    } else if (!EVP_DigestUpdate(mdctx, (const unsigned char *)data, data_len)) {
      ret = OB_ERR_AES_ENCRYPT;
      LOG_WARN("fail to update hash result", K(ret));
    } else if (!EVP_DigestFinal_ex(mdctx, (unsigned char *)buf, &res_len)) {
      ret = OB_ERR_AES_ENCRYPT;
      LOG_WARN("fail to retrieve hash result", K(ret));
    } else if (OB_UNLIKELY(expect_out_len != res_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid hash result length", K(expect_out_len), K(res_len), K(ret));
    } else {
      out_len = res_len;
    }
    if (OB_NOT_NULL(mdctx)) {
      EVP_MD_CTX_destroy(mdctx);
    }
  }
  return ret;
}

int ObHashUtil::get_hash_output_len(const ObHashAlgorithm algo, int64_t &output_len)
{
  int ret = OB_SUCCESS;
  switch(algo) {
    case OB_HASH_MD4:
      output_len = MD4_DIGEST_LENGTH;
      break;
    case OB_HASH_MD5:
      output_len = MD5_DIGEST_LENGTH;
      break;
    case OB_HASH_SH1:
      output_len = SHA_DIGEST_LENGTH;
      break;
    case OB_HASH_SH224:
      output_len = SHA224_DIGEST_LENGTH;
      break;
    case OB_HASH_SH256:
      output_len = SHA256_DIGEST_LENGTH;
      break;
    case OB_HASH_SH384:
      output_len = SHA384_DIGEST_LENGTH;
      break;
    case OB_HASH_SH512:
      output_len = SHA512_DIGEST_LENGTH;
      break;
    default:
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "cipher type passed");
  }
  return ret;
}

int ObHashUtil::get_sha_hash_algorightm(const int64_t bit_length, ObHashAlgorithm &algo)
{
  int ret = OB_SUCCESS;
  const int64_t BIT_PER_BYTE = 8;
  switch(bit_length) {
    case SHA_DIGEST_LENGTH * BIT_PER_BYTE:
      algo = OB_HASH_SH1;
      break;
    case SHA224_DIGEST_LENGTH * BIT_PER_BYTE:
      algo = OB_HASH_SH224;
      break;
    case SHA256_DIGEST_LENGTH * BIT_PER_BYTE:
      algo = OB_HASH_SH256;
      break;
    case SHA384_DIGEST_LENGTH * BIT_PER_BYTE:
      algo = OB_HASH_SH384;
      break;
    case SHA512_DIGEST_LENGTH * BIT_PER_BYTE:
      algo = OB_HASH_SH512;
      break;
    default:
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "sha hash output length");
  }
  return ret;
}

const EVP_MD* ObHashUtil::get_hash_evp_md(const ObHashAlgorithm algo)
{
  switch(algo) {
    case OB_HASH_MD4: return EVP_md4();
    case OB_HASH_MD5: return EVP_md5();
    case OB_HASH_SH1: return EVP_sha1();
    case OB_HASH_SH224: return EVP_sha224();
    case OB_HASH_SH256: return EVP_sha256();
    case OB_HASH_SH384: return EVP_sha384();
    case OB_HASH_SH512: return EVP_sha512();
    default: return NULL;
  }
}

void ObTdeEncryptEngineLoader::ssl_init()
{
    OpenSSL_add_all_digests();
    OpenSSL_add_all_ciphers();
    OPENSSL_load_builtin_modules();
    ENGINE_load_builtin_engines();
    ERR_load_ERR_strings();
}

int ObTdeEncryptEngineLoader::load(const common::ObString& engine)
{
  int ret = OB_SUCCESS;
  common::ObString err_reason;
  ObEncryptEngineType type = get_engine_type(engine);
  if (OB_NONE_ENGINE == type) {
  } else if (OB_INVALID_ENGINE == type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unsupport engine", K(engine));
  } else {
    if (NULL == tde_engine_[type]) {
      ENGINE *e = ENGINE_by_id(engine.ptr());
      if (NULL == e) {
        ret = OB_INIT_FAIL;
        err_reason = common::ObString::make_string(ERR_reason_error_string(ERR_get_error()));
        LOG_WARN("load engine failed", K(engine), K(err_reason));
      } else if (!ENGINE_init(e)) {
        ret = OB_INIT_FAIL;
        err_reason = common::ObString::make_string(ERR_reason_error_string(ERR_get_error()));
        LOG_WARN("Failed initialisation engine!", K(engine), K(err_reason));
        ENGINE_free(e);
      } else {
        tde_engine_[type] = e;
        LOG_INFO("tde install engine success", K(engine));
      }
    }
  }
  return ret;
}

void ObTdeEncryptEngineLoader::destroy()
{
  for (int i = 0; i < OB_MAX_ENGINE; i++) {
    if (NULL != tde_engine_[i]) {
      ENGINE_finish(tde_engine_[i]);
      ENGINE_free(tde_engine_[i]);
    }
  }
}

ENGINE* ObTdeEncryptEngineLoader::get_tde_engine(ObCipherOpMode &mode) const
{
  ObEncryptEngineType type = OB_INVALID_ENGINE;
  if (ObEncryptionUtil::is_aes_encryption(mode)) {
    type = OB_AES_ENGINE;
  } else if (ObEncryptionUtil::is_sm4_encryption(mode)) {
    type = OB_SM4_ENGINE;
  } else {
    type = OB_INVALID_ENGINE;
  }
  return tde_engine_[type];
}

ObTdeEncryptEngineLoader &ObTdeEncryptEngineLoader::get_instance()
{
  static ObTdeEncryptEngineLoader instance;
  return instance;
}

ObTdeEncryptEngineLoader::ObEncryptEngineType ObTdeEncryptEngineLoader::get_engine_type(const common::ObString& engine)
{
  ObEncryptEngineType type = OB_INVALID_ENGINE;
  if (OB_NOT_NULL(strcasestr(engine.ptr(), "sm4"))) {
    type = OB_SM4_ENGINE;
  } else if (OB_NOT_NULL(strcasestr(engine.ptr(), "hy")) || OB_NOT_NULL(strcasestr(engine.ptr(), "hct"))) {
    type = OB_SM4_ENGINE;
  } else if (OB_NOT_NULL(strcasestr(engine.ptr(), "aes"))) {
    type = OB_AES_ENGINE;
  } else if (0 == engine.case_compare("none")) {
    type = OB_NONE_ENGINE;
  }
  return type;
}

int ObTdeEncryptEngineLoader::reload_config()
{
  int ret = OB_SUCCESS;
  common::ObString engine = GCONF._load_tde_encrypt_engine.get_value_string();
  return load(engine);
}

}//end share
} //end oceanbase

#endif
