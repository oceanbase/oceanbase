/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include <openssl/evp.h>
#include <openssl/x509.h>
#include <openssl/pem.h>
#include <openssl/err.h>
#include <openssl/engine.h>
#include "ob_ssl_config.h"
#include "lib/ob_define.h"
#include "lib/oblog/ob_log.h"
#include "lib/lock/ob_spin_rwlock.h"

namespace oceanbase {
namespace common {
static const int FD_MAX = 100 * 10000; //set the fd limit to be 100w
struct ssl_st {
  SSL* ssl;
  int  hand_shake_done;
};

static SSL_CTX* gs_ssl_ctx_array[OB_SSL_CTX_ID_MAX][OB_SSL_ROLE_MAX];
static SpinRWLock gs_ssl_array_lock;
static struct ssl_st gs_ssl_array[FD_MAX];

const char tls_ciphers_list[]= "!aNULL:!eNULL:!EXPORT:!LOW:!MD5:!DES:!RC2:!RC4:!PSK:"
                               "!DHE-DSS-DES-CBC3-SHA:!DHE-RSA-DES-CBC3-SHA:"
                               "!ECDH-RSA-DES-CBC3-SHA:!ECDH-ECDSA-DES-CBC3-SHA:"
                               "!ECDHE-RSA-DES-CBC3-SHA:!ECDHE-ECDSA-DES-CBC3-SHA:"
                               "ECDHE-ECDSA-AES128-GCM-SHA256:"
                               "ECDHE-ECDSA-AES256-GCM-SHA384:"
                               "ECDHE-RSA-AES128-GCM-SHA256:"
                               "ECDHE-RSA-AES256-GCM-SHA384:"
                               "ECDHE-ECDSA-AES128-SHA256:"
                               "ECDHE-RSA-AES128-SHA256:"
                               "ECDHE-ECDSA-AES256-SHA384:"
                               "ECDHE-RSA-AES256-SHA384:"
                               "DHE-RSA-AES128-GCM-SHA256:"
                               "DHE-DSS-AES128-GCM-SHA256:"
                               "DHE-RSA-AES128-SHA256:"
                               "DHE-DSS-AES128-SHA256:"
                               "DHE-DSS-AES256-GCM-SHA384:"
                               "DHE-RSA-AES256-SHA256:"
                               "DHE-DSS-AES256-SHA256:"
                               "ECDHE-RSA-AES128-SHA:ECDHE-ECDSA-AES128-SHA:"
                               "ECDHE-RSA-AES256-SHA:ECDHE-ECDSA-AES256-SHA:"
                               "DHE-DSS-AES128-SHA:DHE-RSA-AES128-SHA:"
                               "TLS_DHE_DSS_WITH_AES_256_CBC_SHA:DHE-RSA-AES256-SHA:"
                               "AES128-GCM-SHA256:DH-DSS-AES128-GCM-SHA256:"
                               "ECDH-ECDSA-AES128-GCM-SHA256:AES256-GCM-SHA384:"
                               "DH-DSS-AES256-GCM-SHA384:ECDH-ECDSA-AES256-GCM-SHA384:"
                               "AES128-SHA256:DH-DSS-AES128-SHA256:ECDH-ECDSA-AES128-SHA256:AES256-SHA256:"
                               "DH-DSS-AES256-SHA256:ECDH-ECDSA-AES256-SHA384:AES128-SHA:"
                               "DH-DSS-AES128-SHA:ECDH-ECDSA-AES128-SHA:AES256-SHA:"
                               "DH-DSS-AES256-SHA:ECDH-ECDSA-AES256-SHA:DHE-RSA-AES256-GCM-SHA384:"
                               "DH-RSA-AES128-GCM-SHA256:ECDH-RSA-AES128-GCM-SHA256:DH-RSA-AES256-GCM-SHA384:"
                               "ECDH-RSA-AES256-GCM-SHA384:DH-RSA-AES128-SHA256:"
                               "ECDH-RSA-AES128-SHA256:DH-RSA-AES256-SHA256:"
                               "ECDH-RSA-AES256-SHA384:ECDHE-RSA-AES128-SHA:"
                               "ECDHE-ECDSA-AES128-SHA:ECDHE-RSA-AES256-SHA:"
                               "ECDHE-ECDSA-AES256-SHA:DHE-DSS-AES128-SHA:DHE-RSA-AES128-SHA:"
                               "TLS_DHE_DSS_WITH_AES_256_CBC_SHA:DHE-RSA-AES256-SHA:"
                               "AES128-SHA:DH-DSS-AES128-SHA:ECDH-ECDSA-AES128-SHA:AES256-SHA:"
                               "DH-DSS-AES256-SHA:ECDH-ECDSA-AES256-SHA:DH-RSA-AES128-SHA:"
                               "ECDH-RSA-AES128-SHA:DH-RSA-AES256-SHA:ECDH-RSA-AES256-SHA:DES-CBC3-SHA";

const char baba_tls_ciphers_list[]= "!aNULL:!eNULL:!EXPORT:!LOW:!MD5:!DES:!RC2:!RC4:!PSK:"
                               "!DHE-DSS-DES-CBC3-SHA:!DHE-RSA-DES-CBC3-SHA:"
                               "!ECDH-RSA-DES-CBC3-SHA:!ECDH-ECDSA-DES-CBC3-SHA:"
                               "!ECDHE-RSA-DES-CBC3-SHA:!ECDHE-ECDSA-DES-CBC3-SHA:"
                               "ECC-SM2-WITH-SM4-SM3:ECDHE-SM2-WITH-SM4-SM3:"
                               "ECDHE-ECDSA-AES128-GCM-SHA256:"
                               "ECDHE-ECDSA-AES256-GCM-SHA384:"
                               "ECDHE-RSA-AES128-GCM-SHA256:"
                               "ECDHE-RSA-AES256-GCM-SHA384:"
                               "ECDHE-ECDSA-AES128-SHA256:"
                               "ECDHE-RSA-AES128-SHA256:"
                               "ECDHE-ECDSA-AES256-SHA384:"
                               "ECDHE-RSA-AES256-SHA384:"
                               "DHE-RSA-AES128-GCM-SHA256:"
                               "DHE-DSS-AES128-GCM-SHA256:"
                               "DHE-RSA-AES128-SHA256:"
                               "DHE-DSS-AES128-SHA256:"
                               "DHE-DSS-AES256-GCM-SHA384:"
                               "DHE-RSA-AES256-SHA256:"
                               "DHE-DSS-AES256-SHA256:"
                               "ECDHE-RSA-AES128-SHA:ECDHE-ECDSA-AES128-SHA:"
                               "ECDHE-RSA-AES256-SHA:ECDHE-ECDSA-AES256-SHA:"
                               "DHE-DSS-AES128-SHA:DHE-RSA-AES128-SHA:"
                               "TLS_DHE_DSS_WITH_AES_256_CBC_SHA:DHE-RSA-AES256-SHA:"
                               "AES128-GCM-SHA256:DH-DSS-AES128-GCM-SHA256:"
                               "ECDH-ECDSA-AES128-GCM-SHA256:AES256-GCM-SHA384:"
                               "DH-DSS-AES256-GCM-SHA384:ECDH-ECDSA-AES256-GCM-SHA384:"
                               "AES128-SHA256:DH-DSS-AES128-SHA256:ECDH-ECDSA-AES128-SHA256:AES256-SHA256:"
                               "DH-DSS-AES256-SHA256:ECDH-ECDSA-AES256-SHA384:AES128-SHA:"
                               "DH-DSS-AES128-SHA:ECDH-ECDSA-AES128-SHA:AES256-SHA:"
                               "DH-DSS-AES256-SHA:ECDH-ECDSA-AES256-SHA:DHE-RSA-AES256-GCM-SHA384:"
                               "DH-RSA-AES128-GCM-SHA256:ECDH-RSA-AES128-GCM-SHA256:DH-RSA-AES256-GCM-SHA384:"
                               "ECDH-RSA-AES256-GCM-SHA384:DH-RSA-AES128-SHA256:"
                               "ECDH-RSA-AES128-SHA256:DH-RSA-AES256-SHA256:"
                               "ECDH-RSA-AES256-SHA384:ECDHE-RSA-AES128-SHA:"
                               "ECDHE-ECDSA-AES128-SHA:ECDHE-RSA-AES256-SHA:"
                               "ECDHE-ECDSA-AES256-SHA:DHE-DSS-AES128-SHA:DHE-RSA-AES128-SHA:"
                               "TLS_DHE_DSS_WITH_AES_256_CBC_SHA:DHE-RSA-AES256-SHA:"
                               "AES128-SHA:DH-DSS-AES128-SHA:ECDH-ECDSA-AES128-SHA:AES256-SHA:"
                               "DH-DSS-AES256-SHA:ECDH-ECDSA-AES256-SHA:DH-RSA-AES128-SHA:"
                               "ECDH-RSA-AES128-SHA:DH-RSA-AES256-SHA:ECDH-RSA-AES256-SHA:DES-CBC3-SHA";


static X509* ob_ssl_get_sm_cert_memory(const char *cert)
{
  BIO *bio = NULL;
  X509 *x509 = NULL;
  if (NULL == (bio = BIO_new_mem_buf((void *)cert, -1))) {
    COMMON_LOG_RET(ERROR, OB_ALLOCATE_MEMORY_FAILED, "BIO_new_mem_buf failed", K(cert));
  } else if (NULL == (x509 = PEM_read_bio_X509(bio, NULL, NULL, NULL))) {
    COMMON_LOG_RET(WARN, OB_ERR_SYS, "PEM_read_bio_X509 failed", K(cert));
  }
  if (NULL != bio) {
    BIO_free(bio);
  }
  return x509;
}

static EVP_PKEY* ob_ssl_get_sm_pkey_memory(const char *key)
{
  BIO *bio = NULL;
  EVP_PKEY *pkey = NULL;
  if (NULL == (bio = BIO_new_mem_buf((void *)key, strlen(key)))) {
    COMMON_LOG_RET(ERROR, OB_ALLOCATE_MEMORY_FAILED, "BIO_new_mem_buf failed", K(key));
  } else if (NULL == (pkey = PEM_read_bio_PrivateKey(bio, NULL, NULL, NULL))) {
    COMMON_LOG_RET(WARN, OB_ERR_SYS, "PEM_read_bio_PrivateKey failed", K(key));
  }
  if (NULL != bio) {
    BIO_free(bio);
  }
  return pkey;
}

static int ob_ssl_config_check(const ObSSLConfig& ssl_config)
{
	int ret = OB_SUCCESS;
   if (!ssl_config.ca_cert_ || 0 == strlen(ssl_config.ca_cert_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid ca_cert", K(ssl_config.ca_cert_), K(ret));
  } else if (!ssl_config.sign_cert_ || 0 == strlen(ssl_config.sign_cert_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid sign_cert", K(ssl_config.sign_cert_), K(ret));
  } else if (!ssl_config.sign_private_key_ || 0 == strlen(ssl_config.sign_private_key_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid sign_private_key", K(ssl_config.sign_private_key_), K(ret));
  } else if (ssl_config.is_sm_) {
    if (!ssl_config.enc_cert_ || 0 == strlen(ssl_config.enc_cert_)) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "sm mode, invalid enc_cert", K(ssl_config.enc_cert_), K(ret));
    } else if (!ssl_config.enc_private_key_ || 0 == strlen(ssl_config.enc_private_key_)) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "sm mode, invalid  enc_private_key", K(ssl_config.enc_private_key_), K(ret));
    }
  }
  return ret;
}

static int ob_ssl_set_verify_mode_and_load_CA(SSL_CTX* ctx, const ObSSLConfig& ssl_config)
{
  int ret = OB_SUCCESS;
  SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER | SSL_VERIFY_CLIENT_ONCE, NULL);
  if (ssl_config.is_from_file_) {
    STACK_OF(X509_NAME) *list = NULL;
    if (0 == SSL_CTX_load_verify_locations(ctx, ssl_config.ca_cert_, NULL)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "SSL_CTX_load_verify_locations failed", K(ret), K(ssl_config.ca_cert_), K(ERR_error_string(ERR_get_error(), NULL)));
    } else if (NULL == (list = SSL_load_client_CA_file(ssl_config.ca_cert_))) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "SSL_load_client_CA_file failed", K(ssl_config.ca_cert_), K(ret), K(ERR_error_string(ERR_get_error(), NULL)));
    } else {
      ERR_clear_error();
      SSL_CTX_set_client_CA_list(ctx, list);
    }
  } else {
    BIO *bio = NULL;
    X509 *cert_x509 = NULL;
    X509_STORE *x509_store = NULL;
    if (NULL == (bio = BIO_new_mem_buf((void*)ssl_config.ca_cert_, -1))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(ERROR, "BIO_new_mem_buf failed ", K(ssl_config.ca_cert_), K(ret));
    } else if (NULL == (cert_x509 = PEM_read_bio_X509(bio, NULL, 0, NULL))) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "PEM_read_bio_X509 failed", K(ssl_config.ca_cert_), K(ret));
    } else if (NULL == (x509_store = X509_STORE_new())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(ERROR, "X509_STORE_new failed", K(ssl_config.ca_cert_), K(ret));
    } else if (0 == X509_STORE_add_cert(x509_store, cert_x509)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "X509_STORE_add_cert failed", K(ERR_error_string(ERR_get_error(), NULL)), K(ret));
    } else {
      SSL_CTX_set_cert_store(ctx, x509_store);
    }
    if (NULL != bio) {
      BIO_free(bio);
    }
    if (NULL != cert_x509) {
      X509_free(cert_x509);
    }
    if (OB_FAIL(ret)) {
      if (NULL != x509_store) {
        X509_STORE_free(x509_store);
      }
    }
  }
  return ret;
}

#ifdef OB_USE_BABASSL
static int ob_ssl_load_cert_and_pkey_for_sm_memory(SSL_CTX* ctx, const ObSSLConfig& ssl_config)
{
  int ret = OB_SUCCESS;
  EVP_PKEY *pkey = NULL;
  X509 *x509 = NULL;
  if (NULL == (pkey = ob_ssl_get_sm_pkey_memory(ssl_config.sign_private_key_))) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "ob_ssl_get_sm_pkey_memory failed", K(ssl_config.sign_private_key_), K(ret));
  } else if (!SSL_CTX_use_sign_PrivateKey(ctx, pkey)) {
    ret = OB_ERR_UNEXPECTED;
    EVP_PKEY_free(pkey);
    COMMON_LOG(WARN, "SSL_CTX_use_sign_PrivateKey failed", K(ssl_config.sign_private_key_), K(ret), K(ERR_error_string(ERR_get_error(), NULL)));
  } else if (NULL == (x509 = ob_ssl_get_sm_cert_memory(ssl_config.sign_cert_))){
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "ob_ssl_get_sm_cert_memory failed", K(ssl_config.sign_cert_), K(ret));
  } else if (!SSL_CTX_use_sign_certificate(ctx, x509)) {
    ret = OB_ERR_UNEXPECTED;
    X509_free(x509);
    COMMON_LOG(WARN, "SSL_CTX_use_sign_certificate failed", K(ssl_config.sign_cert_), K(ret),  K(ERR_error_string(ERR_get_error(), NULL)));
  } else if (NULL == (pkey = ob_ssl_get_sm_pkey_memory(ssl_config.enc_private_key_))) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "ob_ssl_get_sm_pkey_memory failed", K(ssl_config.enc_private_key_), K(ret));
  } else if (!SSL_CTX_use_enc_PrivateKey(ctx, pkey)) {
    ret = OB_ERR_UNEXPECTED;
    EVP_PKEY_free(pkey);
    COMMON_LOG(WARN, "SSL_CTX_use_enc_PrivateKey failed", K(ssl_config.enc_private_key_), K(ret), K(ERR_error_string(ERR_get_error(), NULL)));
  } else if (NULL == (x509 = ob_ssl_get_sm_cert_memory(ssl_config.enc_cert_))) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "ob_ssl_get_sm_cert_memory failed", K(ssl_config.enc_cert_), K(ret));
  } else if (!SSL_CTX_use_enc_certificate(ctx, x509)) {
    ret = OB_ERR_UNEXPECTED;
    X509_free(x509);
    COMMON_LOG(WARN, "SSL_CTX_use_enc_certificate  failed", K(ssl_config.enc_cert_), K(ret), K(ERR_error_string(ERR_get_error(), NULL)));
  }
  return ret;
}
#endif

static int ob_ssl_load_cert_and_pkey_for_intl_memory(SSL_CTX* ctx, const ObSSLConfig& ssl_config)
{
  int ret = OB_SUCCESS;
  int is_first = 1;
  BIO *bio = NULL;
  STACK_OF(X509_INFO) *inf = NULL;

  if (NULL == (bio = BIO_new_mem_buf((void*)ssl_config.sign_cert_, -1))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(ERROR, "BIO_new_mem_buf failed", K(ssl_config.sign_cert_), K(ret));
  } else if (NULL == (inf = PEM_X509_INFO_read_bio(bio, NULL, NULL, NULL))) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(ERROR, "PEM_X509_INFO_read_bio failed", K(ret), K(ssl_config.sign_cert_));
  } else {
    for (int i = 0; i < sk_X509_INFO_num(inf) && OB_SUCC(ret); i++) {
      X509_INFO *itmp = sk_X509_INFO_value(inf, i);
      if (itmp->x509) {
        if (is_first) {
          is_first = 0;
          if (SSL_CTX_use_certificate(ctx, itmp->x509) <= 0) {
            ret = OB_ERR_UNEXPECTED;
            COMMON_LOG(WARN, "SSL_CTX_use_certificate failed", K(ssl_config.sign_cert_), K(ret));
          }
        } else {
          if (SSL_CTX_add_extra_chain_cert(ctx, itmp->x509) <= 0) {
            ret = OB_ERR_UNEXPECTED;
            COMMON_LOG(WARN, "SSL_CTX_add_extra_chain_cert failed", K(ssl_config.sign_cert_), K(ret));
          } else {
            itmp->x509 = NULL;
          }
        }
      }
    }
    //free bio allocate before
    if (NULL != bio) {
      BIO_free(bio);
    }
    bio = NULL;
    if (OB_SUCC(ret)) {
      RSA *rsa = NULL;
      if (NULL == (bio = BIO_new_mem_buf((void*)ssl_config.sign_private_key_, -1))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        COMMON_LOG(ERROR, "BIO_new_mem_buf for sign_private_key failed", K(ssl_config.sign_private_key_), K(ret));
      } else if (NULL == (rsa = PEM_read_bio_RSAPrivateKey(bio, NULL, 0, NULL))) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "PEM_read_bio_RSAPrivateKey failed", K(ssl_config.sign_private_key_), K(ret));
      } else if (SSL_CTX_use_RSAPrivateKey(ctx, rsa) <= 0) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "SSL_CTX_use_RSAPrivateKey failed", K(ssl_config.sign_private_key_), K(ret));
      }
      if (OB_FAIL(ret)) {
        if (NULL != rsa) {
          RSA_free(rsa);
        }
      }
    }
  }
  if (NULL != inf) {
    sk_X509_INFO_pop_free(inf, X509_INFO_free);
  }
  if (NULL != bio) {
    BIO_free(bio);
  }
  return ret;
}

static int ob_ssl_load_cert_and_pkey(SSL_CTX* ctx, const ObSSLConfig& ssl_config)
{
  int ret = OB_SUCCESS;
  if (ssl_config.is_from_file_) {
    if (ssl_config.is_sm_) {
#ifdef OB_USE_BABASSL
      if (!SSL_CTX_use_sign_PrivateKey_file(ctx, ssl_config.sign_private_key_, SSL_FILETYPE_PEM)) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "SSL_CTX_use_sign_PrivateKey_file failed", K(ssl_config.sign_private_key_), K(ret));
      } else if (!SSL_CTX_use_sign_certificate_file(ctx, ssl_config.sign_cert_, SSL_FILETYPE_PEM)) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "SSL_CTX_use_sign_certificate_file failed", K(ssl_config.sign_cert_), K(ret));
      } else if (!SSL_CTX_use_enc_PrivateKey_file(ctx, ssl_config.enc_private_key_, SSL_FILETYPE_PEM)) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "SSL_CTX_use_enc_PrivateKey_file failed", K(ssl_config.enc_private_key_), K(ret));
      }  else if (!SSL_CTX_use_enc_certificate_file(ctx, ssl_config.enc_cert_, SSL_FILETYPE_PEM)) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "SSL_CTX_use_enc_certificate_file failed", K(ssl_config.enc_cert_), K(ret));
      }
#endif
    } else {
      if (SSL_CTX_use_certificate_chain_file(ctx, ssl_config.sign_cert_) <= 0) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "SSL_CTX_use_certificate_chain_file failed", K(ssl_config.sign_cert_), K(ret));
      } else if (SSL_CTX_use_PrivateKey_file(ctx, ssl_config.sign_private_key_, SSL_FILETYPE_PEM) <= 0) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "SSL_CTX_use_PrivateKey_file failed", K(ssl_config.sign_private_key_), K(ret));
      } else if (SSL_CTX_check_private_key(ctx) <= 0) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "SSL_CTX_check_private_key failed", K(ERR_error_string(ERR_get_error(), NULL)), K(ret));
      }
    }
  } else {
    if (ssl_config.is_sm_) {
#ifdef OB_USE_BABASSL
      if (OB_FAIL(ob_ssl_load_cert_and_pkey_for_sm_memory(ctx, ssl_config))) {
        COMMON_LOG(WARN, "ob_ssl_load_cert_and_pkey_for_sm_memory failed", K(ret));
      }
#endif
    } else {
      if (OB_FAIL(ob_ssl_load_cert_and_pkey_for_intl_memory(ctx, ssl_config))) {
        COMMON_LOG(WARN, "ob_ssl_load_cert_and_pkey_for_intl_memory failed", K(ret));
      } else if (SSL_CTX_check_private_key(ctx) <= 0) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "SSL_CTX_check_private_key failed", K(ret), K(ERR_error_string(ERR_get_error(), NULL)));
      }
    }
  }
  return ret;
}

static SSL_CTX* ob_ssl_create_ssl_ctx(const ObSSLConfig& ssl_config)
{
  int ret = OB_SUCCESS;
  SSL_CTX *ctx = NULL;

  if (ssl_config.is_sm_) {
#ifdef OB_USE_BABASSL
    ctx = SSL_CTX_new(NTLS_method());
    if (NULL != ctx) {
      SSL_CTX_enable_ntls(ctx);
    }
#endif
  } else {
    ctx = SSL_CTX_new(SSLv23_method());
  }

  if (NULL == ctx) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(ERROR, "SSL_CTX_new failed", K(ret));
  } else if (SSL_CTX_set_cipher_list(ctx, (ssl_config.is_sm_ ? baba_tls_ciphers_list : tls_ciphers_list)) <= 0) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "SSL_CTX_set_cipher_list failed", K(ret), K(ssl_config.is_sm_), K(ERR_error_string(ERR_get_error(), NULL)));
  } else if (OB_FAIL(ob_ssl_set_verify_mode_and_load_CA(ctx, ssl_config))) {
    COMMON_LOG(WARN, "ob_ssl_set_verify_mode_and_load_CA failed", K(ret));
  } else if (OB_FAIL(ob_ssl_load_cert_and_pkey(ctx, ssl_config))) {
    COMMON_LOG(WARN, "ob_ssl_load_cert_and_pkey for client failed", K(ret));
  } else {
    /* client side options */
    SSL_CTX_set_options(ctx, SSL_OP_MICROSOFT_SESS_ID_BUG);
    SSL_CTX_set_options(ctx, SSL_OP_NETSCAPE_CHALLENGE_BUG);
    SSL_CTX_set_options(ctx, SSL_OP_NETSCAPE_REUSE_CIPHER_CHANGE_BUG);

    /* server side options */
    SSL_CTX_set_options(ctx, SSL_OP_SSLREF2_REUSE_CERT_TYPE_BUG);
    SSL_CTX_set_options(ctx, SSL_OP_MICROSOFT_BIG_SSLV3_BUFFER);

    /* this option allow a potential SSL 2.0 rollback (CAN-2005-2969) */
    SSL_CTX_set_options(ctx, SSL_OP_MSIE_SSLV2_RSA_PADDING);

    SSL_CTX_set_options(ctx, SSL_OP_SSLEAY_080_CLIENT_DH_BUG);
    SSL_CTX_set_options(ctx, SSL_OP_TLS_D5_BUG);
    SSL_CTX_set_options(ctx, SSL_OP_TLS_BLOCK_PADDING_BUG);
    SSL_CTX_set_options(ctx, SSL_OP_DONT_INSERT_EMPTY_FRAGMENTS);
    SSL_CTX_set_options(ctx, SSL_OP_SINGLE_DH_USE);
    /*set_read_ahead may cause the first application data that been sent after
    * SSL handshake being unprocessed, forbid it.
    */
    SSL_CTX_set_read_ahead(ctx, 0);
  }
  return ctx;
}

static int ob_ssl_ctx_reconfigure(int ctx_id, int ctx_role, SSL_CTX* ssl_ctx)
{
  int ret = OB_SUCCESS;
  SSL_CTX* ssl_ctx_temp = NULL;
  if (NULL == (ssl_ctx_temp = gs_ssl_ctx_array[ctx_id][ctx_role])) {
    gs_ssl_ctx_array[ctx_id][ctx_role] = ssl_ctx;
  } else {
    SSL_CTX_free(ssl_ctx_temp);
    gs_ssl_ctx_array[ctx_id][ctx_role] = ssl_ctx;
  }
  return ret;
}

void ob_fd_disable_ssl(int fd)
{
  SSL* ssl = gs_ssl_array[fd].ssl;
  gs_ssl_array[fd].ssl = NULL;
  gs_ssl_array[fd].hand_shake_done = 0;
  if (OB_LIKELY(NULL == ssl)) {
  } else {
    int n = 0;
    int mode = 0;
    mode = SSL_RECEIVED_SHUTDOWN | SSL_SENT_SHUTDOWN;
    SSL_set_shutdown(ssl, mode);
    ERR_clear_error();
    SSL_shutdown(ssl);
    SSL_free(ssl);
  }
}

int ob_ssl_load_config(int ctx_id, const ObSSLConfig& ssl_config)
{
  int ret = OB_SUCCESS;
  static int is_inited = 0;
  SSL_CTX *client_ssl_ctx = NULL;
  SSL_CTX *server_ssl_ctx = NULL;
  if (ctx_id >= OB_SSL_CTX_ID_MAX) {
    ret = OB_SIZE_OVERFLOW;
    COMMON_LOG(ERROR, "ctx_id is beyond limit", K(ret), K(ctx_id), K(OB_SSL_CTX_ID_MAX));
  } else {
    if (!is_inited) {
      memset(gs_ssl_ctx_array, 0, sizeof(gs_ssl_ctx_array));
      memset(&gs_ssl_array, 0, sizeof(gs_ssl_array));
      SSL_library_init();
      SSL_load_error_strings();
      ENGINE_load_builtin_engines();
      OpenSSL_add_all_algorithms();
      is_inited = 1;
    }
    if (OB_FAIL(ob_ssl_config_check(ssl_config))) {
      COMMON_LOG(WARN, "ob_ssl_config_check failed", K(ctx_id), K(ret));
    } else if (OB_ISNULL(client_ssl_ctx = ob_ssl_create_ssl_ctx(ssl_config))) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "ob_ssl_create_client_ctx failed", K(ctx_id), K(ret));
    } else if (OB_ISNULL(server_ssl_ctx = ob_ssl_create_ssl_ctx(ssl_config))) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "ob_ssl_create_server_ctx failed", K(ctx_id), K(ret));
    } else {
      SpinWLockGuard guard(gs_ssl_array_lock);
      if (OB_FAIL(ob_ssl_ctx_reconfigure(ctx_id, OB_SSL_ROLE_SERVER, server_ssl_ctx))) {
        COMMON_LOG(WARN, "ob_ssl_ctx_reconfigure for server failed", K(ctx_id), K(ret));
      } else if (OB_FAIL(ob_ssl_ctx_reconfigure(ctx_id, OB_SSL_ROLE_CLIENT, client_ssl_ctx))) {
        COMMON_LOG(WARN, "ob_ssl_ctx_reconfigure for client failed", K(ctx_id), K(ret));
      }
    }
  }
  return ret;
}

int ob_fd_enable_ssl_for_server(int fd, int ctx_id, uint64_t tls_option)
{
  int ret = OB_SUCCESS;
  SSL_CTX *ctx = NULL;
  SpinRLockGuard guard(gs_ssl_array_lock);
  if (ctx_id >= OB_SSL_CTX_ID_MAX || fd >= FD_MAX) {
    ret = OB_SIZE_OVERFLOW;
    COMMON_LOG(ERROR, "ctx_id or fd is beyond limit", K(ret), K(fd), K(FD_MAX), K(ctx_id));
  } else if (OB_ISNULL(ctx = gs_ssl_ctx_array[ctx_id][OB_SSL_ROLE_SERVER])) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "SSL_CTX is null", K(ret), K(fd), K(ctx_id));
  } else {
    SSL* ssl = NULL;
    if (OB_ISNULL(ssl = SSL_new(ctx))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "SSL_new failed", K(ret), K(fd), K(ctx_id));
    } else if (0 == SSL_set_fd(ssl, fd)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "SSL_set_fd failed", K(ret), K(fd), K(ctx_id));
    } else {
      SSL_set_options(ssl, tls_option);
      SSL_set_accept_state(ssl);
      ATOMIC_STORE(&(gs_ssl_array[fd].ssl), ssl);
    }
    if (OB_FAIL(ret)) {
      if (NULL != ssl) {
        SSL_free(ssl);
      }
    }
  }
  return ret;
}

int ob_fd_enable_ssl_for_client(int fd, int ctx_id) {
  int ret = OB_SUCCESS;
  SSL_CTX *ctx = NULL;
  SpinRLockGuard guard(gs_ssl_array_lock);
  if (ctx_id >= OB_SSL_CTX_ID_MAX || fd >= FD_MAX) {
    ret = OB_SIZE_OVERFLOW;
    COMMON_LOG(ERROR, "ctx_id or fd is beyond limit", K(ret), K(fd), K(FD_MAX), K(ctx_id));
  } else if (OB_ISNULL(ctx = gs_ssl_ctx_array[ctx_id][OB_SSL_ROLE_CLIENT])) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "SSL_CTX is null", K(ret), K(fd), K(ctx_id));
  } else {
    SSL* ssl = NULL;
    if (OB_ISNULL(ssl = SSL_new(ctx))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "SSL_new failed", K(ret), K(fd), K(ctx_id));
    } else if (0 == SSL_set_fd(ssl, fd)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "SSL_set_fd failed", K(ret), K(fd), K(ctx_id));
    } else {
      SSL_set_connect_state(ssl);
      ATOMIC_STORE(&(gs_ssl_array[fd].ssl), ssl);
    }
    if (OB_FAIL(ret)) {
      if (NULL != ssl) {
        SSL_free(ssl);
      }
    }
  }
  return ret;
}

ssize_t ob_read_regard_ssl(int fd, void *buf, size_t nbytes)
{
  ssize_t rbytes = 0;
  SSL* ssl = NULL;
  if (OB_UNLIKELY(fd < 0 || fd >= FD_MAX)) {
    COMMON_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "fd is beyond limit", K(fd));
    rbytes = -1;
    errno = EINVAL;
  } else if (OB_LIKELY(NULL == (ssl = gs_ssl_array[fd].ssl))) {
    rbytes = read(fd, buf, nbytes);
  } else {
    if (OB_UNLIKELY(0 == gs_ssl_array[fd].hand_shake_done)) {
        ERR_clear_error();
        int ssl_ret = SSL_do_handshake(ssl);
        if (ssl_ret > 0) {
          rbytes = -1;
          errno = EINTR;
          gs_ssl_array[fd].hand_shake_done = 1;
          COMMON_LOG(INFO, "SSL_do_handshake succ", K(fd));
        } else {
          int err = SSL_get_error(ssl, ssl_ret);
          if (SSL_ERROR_WANT_READ == err) {
            rbytes = -1;
            errno = EAGAIN;
          } else if (SSL_ERROR_WANT_WRITE == err) {
            rbytes = -1;
            errno = EIO;
            COMMON_LOG_RET(ERROR, OB_ERR_SYS, "SSL_do_handshake want write in reading process", K(fd));
          } else {
            rbytes = -1;
            errno = EIO;
            COMMON_LOG_RET(WARN, OB_ERR_SYS, "SSL_do_handshake failed", K(fd), K(ERR_error_string(ERR_get_error(), NULL)));
          }
        }
    } else {
      ERR_clear_error();
      rbytes = SSL_read(ssl, buf, nbytes);
      if (rbytes <= 0) {
        int ssl_error = 0;
        ssl_error = SSL_get_error(ssl, rbytes);
        if (SSL_ERROR_WANT_READ == ssl_error) {
          rbytes = -1;
          errno = EAGAIN;
        } else if (SSL_ERROR_WANT_WRITE == ssl_error) {
          rbytes = -1;
          errno = EIO;
          COMMON_LOG_RET(ERROR, OB_ERR_SYS, "SSL_read want write, maybe peer started SSL renegotiation", K(fd));
        } else if (SSL_ERROR_ZERO_RETURN == ssl_error || 0 == ERR_peek_error()) {
          /* connection shutdown by peer*/
          rbytes = 0;
          COMMON_LOG_RET(WARN, OB_ERR_SYS, "SSL_read, peer shutdown cleanly", K(fd), K(ssl_error));
        } else {
          rbytes = -1;
		  int sys_errno = errno;
          errno = EIO;
          COMMON_LOG_RET(WARN, OB_ERR_SYS, "SSL_read failed", K(fd), K(sys_errno), K(ssl_error), K(ERR_error_string(ERR_get_error(), NULL)));
        }
      }
    }
  }
  return rbytes;
}

ssize_t ob_write_regard_ssl(int fd, const void *buf, size_t nbytes)
{
  ssize_t wbytes = 0;
  SSL* ssl = NULL;
  if (OB_UNLIKELY(fd < 0 || fd >= FD_MAX)) {
    COMMON_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "fd is beyond limit", K(fd));
    wbytes = -1;
    errno = EINVAL;
  } else if (OB_LIKELY(NULL == (ssl = gs_ssl_array[fd].ssl))) {
    wbytes = write(fd, buf, nbytes);
  } else {
     if (OB_UNLIKELY(0 == gs_ssl_array[fd].hand_shake_done)) {
        ERR_clear_error();
        int ssl_ret = SSL_do_handshake(ssl);
        if (ssl_ret > 0) {
          wbytes = -1;
          errno = EINTR;
          gs_ssl_array[fd].hand_shake_done = 1;
          COMMON_LOG(INFO, "SSL_do_handshake succ", K(fd));
        } else {
          int err = SSL_get_error(ssl, ssl_ret);
          if (SSL_ERROR_WANT_WRITE == err || SSL_ERROR_WANT_READ == err) {
            wbytes = -1;
            errno = EAGAIN;
          } else {
            wbytes = -1;
            errno = EIO;
            COMMON_LOG_RET(ERROR, OB_ERR_SYS, "SSL_do_handshake failed", K(fd), K(ERR_error_string(ERR_get_error(), NULL)));
          }
        }
    } else {
      ERR_clear_error();
      if (0 < (wbytes = SSL_write(ssl, buf, nbytes))) {
      } else {
        wbytes = -1;
        int ssl_error = 0;
        ssl_error = SSL_get_error(ssl, wbytes);
        if (SSL_ERROR_WANT_WRITE == ssl_error) {
          errno = EAGAIN;
        } else if (SSL_ERROR_WANT_READ == ssl_error) {
          errno = EIO;
          COMMON_LOG_RET(ERROR, OB_ERR_SYS, "SSL_write want read", K(fd));
        } else {
          errno = EIO;
          COMMON_LOG_RET(ERROR, OB_ERR_SYS, "ssl write faild", K(fd), K(ERR_error_string(ERR_get_error(), NULL)));
        }
      }
    }
  }
  return wbytes;
}

SSL* ob_fd_get_ssl_st(int fd)
{
  return ATOMIC_LOAD(&(gs_ssl_array[fd].ssl));
}

}
}
