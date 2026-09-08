/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FITNESS FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX LIB

#include "ob_rsa_getter.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "lib/file/file_directory_utils.h"
#include "lib/allocator/ob_malloc.h"
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <openssl/bio.h>
#include <openssl/evp.h>
#include <openssl/err.h>
#include <openssl/rsa.h>

using namespace oceanbase::common;

namespace oceanbase {
namespace common {

const char *ObRsaGetter::DEFAULT_WALLET_PATH = "wallet/rsa";
const char *ObRsaGetter::RSA_PRIVATE_KEY_FILE = "rsa_private.pem";
const char *ObRsaGetter::RSA_PUBLIC_KEY_FILE = "rsa_public.pem";

ObRsaGetter &ObRsaGetter::instance()
{
  static ObRsaGetter instance_;
  return instance_;
}

ObRsaGetter::ObRsaGetter()
    : is_inited_(false),
      rsa_key_()
{
}

ObRsaGetter::~ObRsaGetter()
{
  destroy();
}

int ObRsaGetter::init()
{
  int ret = OB_SUCCESS;
  int load_ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LIB_LOG(WARN, "ObRsaGetter init twice", K(ret));
  } else if (OB_FAIL(create_wallet_directory(DEFAULT_WALLET_PATH))) {
    LIB_LOG(WARN, "failed to create wallet directory", K(ret), K(DEFAULT_WALLET_PATH));
  } else {
    load_ret = load_key_from_wallet(DEFAULT_WALLET_PATH);
    if (OB_SUCCESS == load_ret) {
      LIB_LOG(INFO, "RSA key loaded from wallet", K(DEFAULT_WALLET_PATH));
    } else if (OB_FILE_NOT_EXIST == load_ret) {
      LIB_LOG(INFO, "RSA key file not exist, will generate new key pair", K(DEFAULT_WALLET_PATH));
      if (OB_FAIL(generate_rsa_key_pair())) {
        LIB_LOG(WARN, "failed to generate RSA key pair", K(ret));
      } else if (OB_FAIL(save_key_to_wallet(DEFAULT_WALLET_PATH))) {
        LIB_LOG(WARN, "failed to save RSA key to wallet", K(ret));
      } else {
        LIB_LOG(INFO, "RSA key pair generated and saved", K(DEFAULT_WALLET_PATH));
      }
    } else {
      ret = load_ret;
      LIB_LOG(WARN, "failed to load RSA key from wallet", K(ret), K(DEFAULT_WALLET_PATH));
    }
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
    LIB_LOG(INFO, "ObRsaGetter init finished", K(DEFAULT_WALLET_PATH));
  } else if (OB_INIT_TWICE != ret) {
    LIB_LOG(WARN, "failed to init RSA getter, RSA-based full authentication over insecure channels is unavailable",
            K(ret), K(DEFAULT_WALLET_PATH));
    destroy();
    ret = OB_SUCCESS;
  }

  return ret;
}

void ObRsaGetter::destroy()
{
  rsa_key_.reset();
  is_inited_ = false;
}

int ObRsaGetter::generate_rsa_key_pair(int key_bits)
{
  int ret = OB_SUCCESS;
  RSA *rsa = nullptr;
  BIGNUM *bn = nullptr;
  BIO *bio_private = nullptr;
  BIO *bio_public = nullptr;
  char *private_key_data = nullptr;
  char *public_key_data = nullptr;
  long private_key_len = 0;
  long public_key_len = 0;

  if (key_bits < 1024 || key_bits > 4096) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid key bits", K(ret), K(key_bits));
  } else {
    ERR_clear_error();
    // Create BIGNUM for public exponent
    bn = BN_new();
    if (bn == nullptr) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LIB_LOG(WARN, "failed to create BIGNUM", K(ret));
      log_openssl_error_stack("BN_new");
    } else if (BN_set_word(bn, OB_RSA_PUBLIC_EXPONENT) != 1) {
      ret = OB_ERR_UNEXPECTED;
      LIB_LOG(WARN, "failed to set BIGNUM", K(ret));
      log_openssl_error_stack("BN_set_word");
    } else {
      // Generate RSA key pair
      rsa = RSA_new();
      if (rsa == nullptr) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LIB_LOG(WARN, "failed to create RSA", K(ret));
        log_openssl_error_stack("RSA_new");
      } else if (RSA_generate_key_ex(rsa, key_bits, bn, nullptr) != 1) {
        ret = OB_ERR_UNEXPECTED;
        LIB_LOG(WARN, "failed to generate RSA key", K(ret));
        log_openssl_error_stack("RSA_generate_key_ex");
      } else if (OB_ISNULL(bio_private = BIO_new(BIO_s_mem()))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LIB_LOG(WARN, "failed to create BIO for private key", K(ret));
        log_openssl_error_stack("BIO_new private key");
      } else if (PEM_write_bio_RSAPrivateKey(
                     bio_private, rsa, nullptr, nullptr, 0, nullptr, nullptr) != 1) {
        ret = OB_ERR_UNEXPECTED;
        LIB_LOG(WARN, "failed to write private key to BIO", K(ret));
        log_openssl_error_stack("PEM_write_bio_RSAPrivateKey");
      } else if (OB_ISNULL(bio_public = BIO_new(BIO_s_mem()))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LIB_LOG(WARN, "failed to create BIO for public key", K(ret));
        log_openssl_error_stack("BIO_new public key");
      } else if (PEM_write_bio_RSA_PUBKEY(bio_public, rsa) != 1) {
        ret = OB_ERR_UNEXPECTED;
        LIB_LOG(WARN, "failed to write public key to BIO", K(ret));
        log_openssl_error_stack("PEM_write_bio_RSA_PUBKEY");
      } else if (OB_UNLIKELY(0 >= (private_key_len = BIO_get_mem_data(bio_private, &private_key_data)))) {
        ret = OB_ERR_UNEXPECTED;
        LIB_LOG(WARN, "failed to get private key data from BIO", K(ret), K(private_key_len));
        log_openssl_error_stack("BIO_get_mem_data private key");
      } else if (OB_UNLIKELY(OB_RSA_MAX_KEY_LENGTH <= private_key_len)) {
        ret = OB_SIZE_OVERFLOW;
        LIB_LOG(WARN, "private key size overflow", K(ret), K(private_key_len));
      } else if (OB_UNLIKELY(0 >= (public_key_len = BIO_get_mem_data(bio_public, &public_key_data)))) {
        ret = OB_ERR_UNEXPECTED;
        LIB_LOG(WARN, "failed to get public key data from BIO", K(ret), K(public_key_len));
        log_openssl_error_stack("BIO_get_mem_data public key");
      } else if (OB_UNLIKELY(OB_RSA_MAX_KEY_LENGTH <= public_key_len)) {
        ret = OB_SIZE_OVERFLOW;
        LIB_LOG(WARN, "public key size overflow", K(ret), K(public_key_len));
      } else {
        // Allocate memory for private key
        rsa_key_.private_key_ = static_cast<char *>(ob_malloc(OB_RSA_MAX_KEY_LENGTH, "RsaPrivKey"));
        // Allocate memory for public key
        rsa_key_.public_key_ = static_cast<char *>(ob_malloc(OB_RSA_MAX_KEY_LENGTH, "RsaPubKey"));
        if (rsa_key_.private_key_ == nullptr) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LIB_LOG(WARN, "failed to allocate memory for private key", K(ret));
        } else if (rsa_key_.public_key_ == nullptr) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LIB_LOG(WARN, "failed to allocate memory for public key", K(ret));
        } else {
          MEMSET(rsa_key_.private_key_, 0, OB_RSA_MAX_KEY_LENGTH);
          MEMCPY(rsa_key_.private_key_, private_key_data, private_key_len);
          rsa_key_.private_key_[private_key_len] = '\0';
          rsa_key_.private_key_len_ = private_key_len;

          MEMSET(rsa_key_.public_key_, 0, OB_RSA_MAX_KEY_LENGTH);
          MEMCPY(rsa_key_.public_key_, public_key_data, public_key_len);
          rsa_key_.public_key_[public_key_len] = '\0';
          rsa_key_.public_key_len_ = public_key_len;

          LIB_LOG(INFO, "RSA key pair generated successfully",
                  K(key_bits), K(private_key_len), K(public_key_len));
        }
      }
    }

    // Clean up resources
    if (bio_public != nullptr) {
      BIO_free_all(bio_public);
    }
    if (bio_private != nullptr) {
      BIO_free_all(bio_private);
    }
    if (rsa != nullptr) {
      RSA_free(rsa);
    }
    if (bn != nullptr) {
      BN_free(bn);
    }
  }

  return ret;
}

int ObRsaGetter::get_public_key(ObString &public_key, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LIB_LOG(WARN, "ObRsaGetter not inited", K(ret));
  } else if (!rsa_key_.is_valid()) {
    // Key data doesn't exist, report error
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(ERROR, "RSA key not loaded, should be initialized in observer init phase", K(ret));
  } else {
    char *buf = static_cast<char *>(allocator.alloc(rsa_key_.public_key_len_));
    if (buf == nullptr) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LIB_LOG(WARN, "failed to allocate memory for public key", K(ret));
    } else {
      MEMCPY(buf, rsa_key_.public_key_, rsa_key_.public_key_len_);
      public_key.assign_ptr(buf, static_cast<int32_t>(rsa_key_.public_key_len_));
    }
  }

  return ret;
}

int ObRsaGetter::decrypt_with_padding(EVP_PKEY *pkey,
                                      const unsigned char *ciphertext,
                                      const int64_t ciphertext_len,
                                      unsigned char *plaintext,
                                      int64_t &plaintext_len,
                                      const int64_t max_plaintext_len,
                                      const int padding_mode,
                                      const char *padding_name,
                                      const bool need_oaep_sha1)
{
  int ret = OB_SUCCESS;
  EVP_PKEY_CTX *key_ctx = NULL;
  size_t out_len = static_cast<size_t>(max_plaintext_len);

  ERR_clear_error();
  if (NULL == pkey || NULL == ciphertext || ciphertext_len <= 0 || NULL == plaintext || max_plaintext_len <= 0
      || NULL == padding_name) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid argument", K(ret));
  } else if (NULL == (key_ctx = EVP_PKEY_CTX_new(pkey, NULL))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LIB_LOG(WARN, "failed to create EVP_PKEY_CTX", K(ret), K(padding_name));
  } else if (EVP_PKEY_decrypt_init(key_ctx) <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(WARN, "failed to initialize EVP_PKEY_CTX for decryption", K(ret), K(padding_name));
  } else if (EVP_PKEY_CTX_set_rsa_padding(key_ctx, padding_mode) <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(WARN, "failed to set RSA padding mode", K(ret), K(padding_name));
  } else if (need_oaep_sha1 && EVP_PKEY_CTX_set_rsa_oaep_md(key_ctx, EVP_sha1()) <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(WARN, "failed to set RSA OAEP hash algorithm to SHA-1", K(ret), K(padding_name));
  } else if (need_oaep_sha1 && EVP_PKEY_CTX_set_rsa_mgf1_md(key_ctx, EVP_sha1()) <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(WARN, "failed to set RSA MGF1 hash algorithm to SHA-1", K(ret), K(padding_name));
  } else if (EVP_PKEY_decrypt(key_ctx, plaintext, &out_len, ciphertext, static_cast<size_t>(ciphertext_len)) <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(TRACE, "failed to decrypt RSA", K(ret), K(padding_name));
  } else {
    plaintext_len = static_cast<int64_t>(out_len);
  }

  if (NULL != key_ctx) {
    EVP_PKEY_CTX_free(key_ctx);
  }
  return ret;
}

int ObRsaGetter::decrypt_with_private_key(const unsigned char *ciphertext,
                                          const int64_t ciphertext_len,
                                          unsigned char *plaintext,
                                          int64_t &plaintext_len,
                                          const int64_t max_plaintext_len)
{
  int ret = OB_SUCCESS;
  int oaep_ret = OB_SUCCESS;
  int pkcs1_ret = OB_SUCCESS;
  EVP_PKEY *pkey = NULL;
  BIO *bio = NULL;

  plaintext_len = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LIB_LOG(WARN, "ObRsaGetter not inited", K(ret));
  } else if (NULL == ciphertext || ciphertext_len <= 0 || NULL == plaintext || max_plaintext_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid argument", K(ret), KP(ciphertext), K(ciphertext_len), KP(plaintext), K(max_plaintext_len));
  } else if (!rsa_key_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(ERROR, "RSA key not loaded, should be initialized in observer init phase", K(ret));
  } else {
    bio = BIO_new_mem_buf(rsa_key_.private_key_, static_cast<int>(rsa_key_.private_key_len_));
    if (NULL == bio) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LIB_LOG(WARN, "failed to create BIO", K(ret));
    } else {
      pkey = PEM_read_bio_PrivateKey(bio, NULL, NULL, NULL);
      if (NULL == pkey) {
        ret = OB_ERR_UNEXPECTED;
        LIB_LOG(WARN, "failed to read RSA private key", K(ret));
      } else {
        oaep_ret = decrypt_with_padding(pkey,
                                        ciphertext,
                                        ciphertext_len,
                                        plaintext,
                                        plaintext_len,
                                        max_plaintext_len,
                                        RSA_PKCS1_OAEP_PADDING,
                                        "RSA_PKCS1_OAEP_PADDING",
                                        true);
        if (OB_SUCCESS == oaep_ret) {
          ret = OB_SUCCESS;
          LIB_LOG(TRACE,
                  "RSA decryption successful",
                  "padding",
                  "RSA_PKCS1_OAEP_PADDING");
        } else {
          pkcs1_ret = decrypt_with_padding(pkey,
                                           ciphertext,
                                           ciphertext_len,
                                           plaintext,
                                           plaintext_len,
                                           max_plaintext_len,
                                           RSA_PKCS1_PADDING,
                                           "RSA_PKCS1_PADDING",
                                           false);
          if (OB_SUCCESS == pkcs1_ret) {
            ret = OB_SUCCESS;
            LIB_LOG(TRACE,
                    "RSA decryption fallback successful",
                    "padding",
                    "RSA_PKCS1_PADDING");
          } else {
            ret = oaep_ret;
            LIB_LOG(WARN, "both OAEP and PKCS1 padding decryption failed", K(ret));
          }
        }
      }
    }
    if (NULL != pkey) {
      EVP_PKEY_free(pkey);
    }
    if (NULL != bio) {
      BIO_free(bio);
    }
  }
  return ret;
}

int ObRsaGetter::save_key_to_wallet(const char *wallet_path)
{
  int ret = OB_SUCCESS;
  char *private_key_path = nullptr;
  char *public_key_path = nullptr;
  FILE *fp = nullptr;

  // This function is only called during init phase, no concurrency at this time
  if (!rsa_key_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(WARN, "RSA key not loaded", K(ret));
  } else {
    // Allocate path buffers
    private_key_path = static_cast<char *>(ob_malloc(OB_MAX_FILE_NAME_LENGTH, "RsaPrivPath"));
    public_key_path = static_cast<char *>(ob_malloc(OB_MAX_FILE_NAME_LENGTH, "RsaPubPath"));
    if (private_key_path == nullptr || public_key_path == nullptr) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LIB_LOG(WARN, "failed to allocate memory for path buffers", K(ret));
    } else {
      // Create wallet directory
      if (OB_FAIL(create_wallet_directory(wallet_path))) {
        LIB_LOG(WARN, "failed to create wallet directory", K(ret), K(wallet_path));
      } else {
        // Construct private key file path
        int n = snprintf(private_key_path, OB_MAX_FILE_NAME_LENGTH,
                        "%s/%s", wallet_path, RSA_PRIVATE_KEY_FILE);
        if (n < 0 || n >= OB_MAX_FILE_NAME_LENGTH) {
          ret = OB_SIZE_OVERFLOW;
          LIB_LOG(WARN, "private key path too long", K(ret), K(wallet_path));
        } else {
          // Save private key
          fp = fopen(private_key_path, "w");
          if (fp == nullptr) {
            ret = OB_IO_ERROR;
            LIB_LOG(WARN, "failed to open private key file", K(ret), K(private_key_path));
          } else {
            size_t written = fwrite(rsa_key_.private_key_, 1, rsa_key_.private_key_len_, fp);
            if (written != static_cast<size_t>(rsa_key_.private_key_len_)) {
              ret = OB_IO_ERROR;
              LIB_LOG(WARN, "failed to write private key", K(ret), K(written), K_(rsa_key_.private_key_len));
            } else {
              // Set file permissions to 600 (only owner can read and write)
              chmod(private_key_path, S_IRUSR | S_IWUSR);
              LIB_LOG(INFO, "private key saved successfully", K(private_key_path));
            }
            fclose(fp);
            fp = nullptr;
          }
        }

        if (OB_SUCC(ret)) {
          // Construct public key file path
          n = snprintf(public_key_path, OB_MAX_FILE_NAME_LENGTH,
                      "%s/%s", wallet_path, RSA_PUBLIC_KEY_FILE);
          if (n < 0 || n >= OB_MAX_FILE_NAME_LENGTH) {
            ret = OB_SIZE_OVERFLOW;
            LIB_LOG(WARN, "public key path too long", K(ret), K(wallet_path));
          } else {
            // Save public key
            fp = fopen(public_key_path, "w");
            if (fp == nullptr) {
              ret = OB_IO_ERROR;
              LIB_LOG(WARN, "failed to open public key file", K(ret), K(public_key_path));
            } else {
              size_t written = fwrite(rsa_key_.public_key_, 1, rsa_key_.public_key_len_, fp);
              if (written != static_cast<size_t>(rsa_key_.public_key_len_)) {
                ret = OB_IO_ERROR;
                LIB_LOG(WARN, "failed to write public key", K(ret), K(written), K_(rsa_key_.public_key_len));
              } else {
                // Set file permissions to 644 (owner can read and write, others read-only)
                chmod(public_key_path, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
                LIB_LOG(INFO, "public key saved successfully", K(public_key_path));
              }
              fclose(fp);
              fp = nullptr;
            }
          }
        }
      }
    }
  }

  // Free path buffers
  if (private_key_path != nullptr) {
    ob_free(private_key_path);
  }
  if (public_key_path != nullptr) {
    ob_free(public_key_path);
  }

  return ret;
}

int ObRsaGetter::load_key_from_wallet(const char *wallet_path)
{
  int ret = OB_SUCCESS;
  char *private_key_path = nullptr;
  char *public_key_path = nullptr;
  FILE *fp = nullptr;
  size_t private_key_len = 0;
  size_t public_key_len = 0;

  // This function is only called during init phase, no concurrency at this time
  // Allocate path buffers
  private_key_path = static_cast<char *>(ob_malloc(OB_MAX_FILE_NAME_LENGTH, "RsaPrivPath"));
  public_key_path = static_cast<char *>(ob_malloc(OB_MAX_FILE_NAME_LENGTH, "RsaPubPath"));
  if (private_key_path == nullptr || public_key_path == nullptr) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LIB_LOG(WARN, "failed to allocate memory for path buffers", K(ret));
  } else {
    // Construct private key file path
    int n = snprintf(private_key_path, OB_MAX_FILE_NAME_LENGTH,
                    "%s/%s", wallet_path, RSA_PRIVATE_KEY_FILE);
    if (n < 0 || n >= OB_MAX_FILE_NAME_LENGTH) {
      ret = OB_SIZE_OVERFLOW;
      LIB_LOG(WARN, "private key path too long", K(ret), K(wallet_path));
    } else {
    // Load private key
    fp = fopen(private_key_path, "r");
    if (fp == nullptr) {
      if (errno == ENOENT) {
        ret = OB_FILE_NOT_EXIST;
        LIB_LOG(INFO, "private key file not exist", K(ret), K(private_key_path));
      } else {
        ret = OB_IO_ERROR;
        LIB_LOG(WARN, "failed to open private key file", K(ret), K(private_key_path));
      }
    } else {
      // Temporary buffer for reading file
      char *temp_private_key = static_cast<char *>(ob_malloc(OB_RSA_MAX_KEY_LENGTH, "RsaTempPriv"));
      if (temp_private_key == nullptr) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LIB_LOG(WARN, "failed to allocate memory for temp private key", K(ret));
        fclose(fp);
        fp = nullptr;
      } else {
        MEMSET(temp_private_key, 0, OB_RSA_MAX_KEY_LENGTH);
        private_key_len = fread(temp_private_key, 1, OB_RSA_MAX_KEY_LENGTH - 1, fp);

        if (0 == private_key_len || ferror(fp)) {
          ret = OB_IO_ERROR;
          LIB_LOG(WARN, "failed to read private key", K(ret), K(private_key_len));
        } else {
          temp_private_key[private_key_len] = '\0';
          LIB_LOG(INFO, "private key loaded", K(private_key_len));
        }

        fclose(fp);
        fp = nullptr;

        if (OB_SUCC(ret)) {
          // Construct public key file path
          n = snprintf(public_key_path, OB_MAX_FILE_NAME_LENGTH,
                      "%s/%s", wallet_path, RSA_PUBLIC_KEY_FILE);
          if (n < 0 || n >= OB_MAX_FILE_NAME_LENGTH) {
            ret = OB_SIZE_OVERFLOW;
            LIB_LOG(WARN, "public key path too long", K(ret), K(wallet_path));
          } else {
            // Load public key
            fp = fopen(public_key_path, "r");
            if (fp == nullptr) {
              ret = OB_IO_ERROR;
              LIB_LOG(WARN, "failed to open public key file", K(ret), K(public_key_path));
            } else {
              char *temp_public_key = static_cast<char *>(ob_malloc(OB_RSA_MAX_KEY_LENGTH, "RsaTempPub"));
              if (temp_public_key == nullptr) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                LIB_LOG(WARN, "failed to allocate memory for temp public key", K(ret));
                fclose(fp);
                fp = nullptr;
              } else {
                MEMSET(temp_public_key, 0, OB_RSA_MAX_KEY_LENGTH);
                public_key_len = fread(temp_public_key, 1, OB_RSA_MAX_KEY_LENGTH - 1, fp);

                if (0 == public_key_len || ferror(fp)) {
                  ret = OB_IO_ERROR;
                  LIB_LOG(WARN, "failed to read public key", K(ret), K(public_key_len));
                } else {
                  temp_public_key[public_key_len] = '\0';
                  LIB_LOG(INFO, "public key loaded", K(public_key_len));
                }

                fclose(fp);
                fp = nullptr;

                if (OB_SUCC(ret)) {
                  if (OB_FAIL(validate_rsa_key_pair(temp_private_key,
                                                    private_key_len,
                                                    temp_public_key,
                                                    public_key_len))) {
                    LIB_LOG(WARN, "failed to validate RSA key pair", K(ret), K(wallet_path));
                  }
                }

                if (OB_SUCC(ret)) {
                  // Save to member variables (no lock needed, no concurrency during init phase)
                  // First release old memory (if any)
                  rsa_key_.reset();

                  // Transfer temporary buffers to rsa_key_
                  rsa_key_.private_key_ = temp_private_key;
                  rsa_key_.private_key_len_ = private_key_len;
                  rsa_key_.public_key_ = temp_public_key;
                  rsa_key_.public_key_len_ = public_key_len;

                  // Set temporary pointers to null to prevent deallocation
                  temp_private_key = nullptr;
                  temp_public_key = nullptr;

                  LIB_LOG(INFO, "RSA key loaded from wallet successfully", K(wallet_path));
                }

                // If failed, free temporary memory
                if (temp_public_key != nullptr) {
                  ob_free(temp_public_key);
                }
              }
            }
          }
        }

        // If failed, free temporary memory
        if (temp_private_key != nullptr) {
          MEMSET(temp_private_key, 0, OB_RSA_MAX_KEY_LENGTH);
          ob_free(temp_private_key);
        }
      }
    }
    }
  }

  // Free path buffers
  if (private_key_path != nullptr) {
    ob_free(private_key_path);
  }
  if (public_key_path != nullptr) {
    ob_free(public_key_path);
  }

  return ret;
}

int ObRsaGetter::create_wallet_directory(const char *wallet_path)
{
  int ret = OB_SUCCESS;

  if (wallet_path == nullptr || wallet_path[0] == '\0') {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid wallet path", K(ret), KP(wallet_path));
  } else {
    // Check if directory exists
    struct stat st;
    if (stat(wallet_path, &st) == 0) {
      if (S_ISDIR(st.st_mode)) {
        // Directory already exists
        LIB_LOG(DEBUG, "wallet directory already exists", K(wallet_path));
      } else {
        ret = OB_ERR_UNEXPECTED;
        LIB_LOG(WARN, "wallet path exists but is not a directory", K(ret), K(wallet_path));
      }
    } else {
      // Directory doesn't exist, create directory
      // First create parent directory
      char *parent_path = static_cast<char *>(ob_malloc(OB_MAX_FILE_NAME_LENGTH, "RsaParentPath"));
      if (parent_path == nullptr) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LIB_LOG(WARN, "failed to allocate memory for parent path", K(ret));
      } else {
        MEMCPY(parent_path, wallet_path, strlen(wallet_path));
        parent_path[strlen(wallet_path)] = '\0';

        // Find the last slash
        char *last_slash = strrchr(parent_path, '/');
        if (last_slash != nullptr && last_slash != parent_path) {
          *last_slash = '\0';

          // Recursively create parent directory
          struct stat parent_st;
          if (stat(parent_path, &parent_st) != 0) {
            if (OB_FAIL(create_wallet_directory(parent_path))) {
              LIB_LOG(WARN, "failed to create parent directory", K(ret), K(parent_path));
            }
          }
        }

        if (OB_SUCC(ret)) {
          // Create target directory
          if (mkdir(wallet_path, S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH) != 0) {
            if (errno != EEXIST) {
              ret = OB_IO_ERROR;
              LIB_LOG(WARN, "failed to create wallet directory", K(ret), K(wallet_path), K(errno));
            }
          } else {
            LIB_LOG(INFO, "wallet directory created", K(wallet_path));
          }
        }

        // Free memory
        ob_free(parent_path);
      }
    }
  }

  return ret;
}

void ObRsaGetter::log_openssl_error_stack(const char *operation)
{
  unsigned long error_code = 0;
  char error_buf[256];
  while (0 != (error_code = ERR_get_error())) {
    ERR_error_string_n(error_code, error_buf, sizeof(error_buf));
    LIB_LOG_RET(WARN, OB_ERR_UNEXPECTED, "OpenSSL error",
                KCSTRING(operation), K(error_code), KCSTRING(error_buf));
  }
}

int ObRsaGetter::validate_rsa_key_pair(const char *private_key,
                                       const int64_t private_key_len,
                                       const char *public_key,
                                       const int64_t public_key_len)
{
  int ret = OB_SUCCESS;
  BIO *private_bio = NULL;
  BIO *public_bio = NULL;
  EVP_PKEY *private_pkey = NULL;
  EVP_PKEY *public_pkey = NULL;

  if (OB_ISNULL(private_key) || OB_ISNULL(public_key)
      || OB_UNLIKELY(0 >= private_key_len || OB_RSA_MAX_KEY_LENGTH <= private_key_len
                     || 0 >= public_key_len || OB_RSA_MAX_KEY_LENGTH <= public_key_len)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid RSA key pair", K(ret), K(private_key_len), K(public_key_len));
  } else {
    ERR_clear_error();
    if (OB_ISNULL(private_bio = BIO_new_mem_buf(private_key, static_cast<int>(private_key_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LIB_LOG(WARN, "failed to create BIO for RSA private key", K(ret), K(private_key_len));
      log_openssl_error_stack("BIO_new_mem_buf private key");
    } else if (OB_ISNULL(private_pkey = PEM_read_bio_PrivateKey(private_bio, NULL, NULL, NULL))) {
      ret = OB_ERR_UNEXPECTED;
      LIB_LOG(WARN, "failed to read RSA private key", K(ret), K(private_key_len));
      log_openssl_error_stack("PEM_read_bio_PrivateKey");
    } else if (EVP_PKEY_RSA != EVP_PKEY_base_id(private_pkey)) {
      ret = OB_ERR_UNEXPECTED;
      LIB_LOG(WARN, "wallet private key is not RSA", K(ret));
    } else if (OB_ISNULL(public_bio = BIO_new_mem_buf(public_key, static_cast<int>(public_key_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LIB_LOG(WARN, "failed to create BIO for RSA public key", K(ret), K(public_key_len));
      log_openssl_error_stack("BIO_new_mem_buf public key");
    } else if (OB_ISNULL(public_pkey = PEM_read_bio_PUBKEY(public_bio, NULL, NULL, NULL))) {
      ret = OB_ERR_UNEXPECTED;
      LIB_LOG(WARN, "failed to read RSA public key", K(ret), K(public_key_len));
      log_openssl_error_stack("PEM_read_bio_PUBKEY");
    } else if (EVP_PKEY_RSA != EVP_PKEY_base_id(public_pkey)) {
      ret = OB_ERR_UNEXPECTED;
      LIB_LOG(WARN, "wallet public key is not RSA", K(ret));
    } else if (1 != EVP_PKEY_cmp(private_pkey, public_pkey)) {
      ret = OB_ERR_UNEXPECTED;
      LIB_LOG(WARN, "wallet RSA public key does not match private key", K(ret));
      log_openssl_error_stack("EVP_PKEY_cmp");
    }
  }

  if (NULL != public_pkey) {
    EVP_PKEY_free(public_pkey);
  }
  if (NULL != private_pkey) {
    EVP_PKEY_free(private_pkey);
  }
  if (NULL != public_bio) {
    BIO_free(public_bio);
  }
  if (NULL != private_bio) {
    BIO_free(private_bio);
  }
  return ret;
}

} // namespace common
} // namespace oceanbase
