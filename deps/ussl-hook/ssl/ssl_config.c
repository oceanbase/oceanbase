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

#include <openssl/evp.h>
#include <openssl/x509.h>
#include <openssl/pem.h>
#include <openssl/err.h>
#include <openssl/engine.h>
#include <openssl/ssl.h>
#include <string.h>
#include <pthread.h>
#include <dlfcn.h>
#include <bits/pthreadtypes.h>
#include "ssl_config.h"

#define SSL_CTX_ID_MAX 10
static pthread_rwlock_t g_ssl_ctx_rwlock;
static __thread int hook_flag = 1;
#define FD_MAX (100 * 10000) // set the fd limit to be 100w

static const char tls_ciphers_list[] =
    "!aNULL:!eNULL:!EXPORT:!LOW:!MD5:!DES:!RC2:!RC4:!PSK:"
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

static const char baba_tls_ciphers_list[] =
    "!aNULL:!eNULL:!EXPORT:!LOW:!MD5:!DES:!RC2:!RC4:!PSK:"
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


struct fd_ssl_st
{
  SSL *ssl;
  int hand_shake_done;
  int type;
  //USSL_AUTH_SSL_HANDSHAKE: only use ssl to do authentication
  //USSL_AUTH_SSL_IO ：use ssl to do authentication and I/O
};

static SSL_CTX *gs_ssl_ctx_array[SSL_CTX_ID_MAX][SSL_ROLE_MAX];
static struct fd_ssl_st gs_fd_ssl_array[FD_MAX];

static void __attribute__((constructor(103))) setup()
{
  memset(gs_ssl_ctx_array, 0, sizeof(gs_ssl_ctx_array));
  memset(gs_fd_ssl_array, 0, sizeof(gs_fd_ssl_array));
  pthread_rwlock_init(&g_ssl_ctx_rwlock, NULL);
}

#ifdef OB_USE_BABASSL
static X509 *ob_ssl_get_sm_cert_memory(const char *cert)
{
  BIO *bio = NULL;
  X509 *x509 = NULL;
  if (NULL == (bio = BIO_new_mem_buf(cert, -1))) {
    ussl_log_error("BIO_new_mem_buf failed, errno:%d", errno);
  } else if (NULL == (x509 = PEM_read_bio_X509(bio, NULL, NULL, NULL))) {
    ussl_log_error("PEM_read_bio_X509 failed, errno:%d", errno);
  }
  if (NULL != bio) {
    BIO_free(bio);
  }
  return x509;
}
#endif

#ifdef OB_USE_BABASSL
static EVP_PKEY *ob_ssl_get_sm_pkey_memory(const char *key)
{
  BIO *bio = NULL;
  EVP_PKEY *pkey = NULL;
  if (NULL == (bio = BIO_new_mem_buf(key, strlen(key)))) {
    ussl_log_error("BIO_new_mem_buf failed, errno:%d", errno);
  } else if (NULL == (pkey = PEM_read_bio_PrivateKey(bio, NULL, NULL, NULL))) {
    ussl_log_error("PEM_read_bio_PrivateKey failed, errno:%d", errno);
  }
  if (NULL != bio) {
    BIO_free(bio);
  }
  return pkey;
}
#endif

static int ob_ssl_config_check(const ssl_config_item_t *ssl_config)
{
  int ret = 0;
  if (NULL == ssl_config->ca_cert || 0 == strlen(ssl_config->ca_cert)) {
    ret = EINVAL;
    ussl_log_warn("invalid ca_cert, ret:%d", ret);
  } else if (NULL == ssl_config->sign_cert || 0 == strlen(ssl_config->sign_cert)) {
    ret = EINVAL;
    ussl_log_warn("invalid sign_cert, ret:%d", ret);
  } else if (NULL == ssl_config->sign_private_key || 0 == strlen(ssl_config->sign_private_key)) {
    ret = EINVAL;
    ussl_log_warn("invalid sign_private_key, ret:%d", ret);
  } else if (ssl_config->is_sm) {
    if (NULL == ssl_config->enc_cert || 0 == strlen(ssl_config->enc_cert)) {
      ret = EINVAL;
      ussl_log_warn("SM mode, invalid enc_cert, ret:%d", ret);
    } else if (NULL == ssl_config->enc_private_key || 0 == strlen(ssl_config->enc_private_key)) {
      ret = EINVAL;
      ussl_log_warn("SM mode, invalid  enc_private_key, ret:%d", ret);
    }
  }
  return ret;
}

static int ob_ssl_set_verify_mode_and_load_CA(SSL_CTX *ctx, const ssl_config_item_t *ssl_config, int verify_flag)
{
  int ret = 0;
  SSL_CTX_set_verify(ctx, verify_flag, NULL);
  if (ssl_config->is_from_file) {
    STACK_OF(X509_NAME) *list = NULL;
    if (0 == SSL_CTX_load_verify_locations(ctx, ssl_config->ca_cert, NULL)) {
      ret = EINVAL;
      ussl_log_warn("SSL_CTX_load_verify_locations failed ret:%d, err:%s", ret,
                    ERR_error_string(ERR_get_error(), NULL));
    } else if (NULL == (list = SSL_load_client_CA_file(ssl_config->ca_cert))) {
      ret = EINVAL;
      ussl_log_warn("SSL_load_client_CA_file failed, ret:%d, err:%s", ret,
                    ERR_error_string(ERR_get_error(), NULL));
    } else {
      ERR_clear_error();
      SSL_CTX_set_client_CA_list(ctx, list);
    }
  } else {
    BIO *bio = NULL;
    X509 *cert_x509 = NULL;
    X509_STORE *x509_store = NULL;
    if (NULL == (bio = BIO_new_mem_buf((void *)ssl_config->ca_cert, -1))) {
      ret = ENOMEM;
      ussl_log_error("BIO_new_mem_buf failed, ret:%d", ret);
    } else if (NULL == (cert_x509 = PEM_read_bio_X509(bio, NULL, 0, NULL))) {
      ret = ENOMEM;
      ussl_log_error("PEM_read_bio_X509 failed, ret:%d", ret);
    } else if (NULL == (x509_store = X509_STORE_new())) {
      ret = ENOMEM;
      ussl_log_error("X509_STORE_new failed, ret:%d", ret);
    } else if (0 == X509_STORE_add_cert(x509_store, cert_x509)) {
      ret = EINVAL;
      ussl_log_warn("X509_STORE_add_cert failed, ret:%d, err:%s", ret,
                    ERR_error_string(ERR_get_error(), NULL));
    } else {
      SSL_CTX_set_cert_store(ctx, x509_store);
    }

    if (NULL != bio) {
      BIO_free(bio);
    }
    if (NULL != cert_x509) {
      X509_free(cert_x509);
    }
    if (0 != ret) {
      if (NULL != x509_store) {
        X509_STORE_free(x509_store);
      }
    }
  }
  return ret;
}

#ifdef OB_USE_BABASSL
static int ob_ssl_load_cert_and_pkey_for_sm_memory(SSL_CTX *ctx,
                                                   const ssl_config_item_t *ssl_config)
{
  int ret = 0;
  EVP_PKEY *sign_pkey = NULL;
  EVP_PKEY *enc_pkey = NULL;
  X509 *sign_x509 = NULL;
  X509 *enc_x509 = NULL;
  if (NULL == (sign_pkey = ob_ssl_get_sm_pkey_memory(ssl_config->sign_private_key))) {
    ret = EINVAL;
    ussl_log_warn("ob_ssl_get_sm_pkey_memory failed, ret:%d", ret);
  } else if (!SSL_CTX_use_sign_PrivateKey(ctx, sign_pkey)) {
    ret = EINVAL;
    ussl_log_warn("SSL_CTX_use_sign_PrivateKey failed, ret:%d, err:%s", ret,
                  ERR_error_string(ERR_get_error(), NULL));
  } else if (NULL == (sign_x509 = ob_ssl_get_sm_cert_memory(ssl_config->sign_cert))) {
    ret = EINVAL;
    ussl_log_warn("ob_ssl_get_sm_cert_memory failed, ret:%d", ret);
  } else if (!SSL_CTX_use_sign_certificate(ctx, sign_x509)) {
    ret = EINVAL;
    ussl_log_warn("SSL_CTX_use_sign_certificate failed, ret:%d, err:%s", ret,
                  ERR_error_string(ERR_get_error(), NULL));
  } else if (NULL == (enc_pkey = ob_ssl_get_sm_pkey_memory(ssl_config->enc_private_key))) {
    ret = EINVAL;
    ussl_log_warn("ob_ssl_get_sm_pkey_memory failed, ret:%d", ret);
  } else if (!SSL_CTX_use_enc_PrivateKey(ctx, enc_pkey)) {
    ret = EINVAL;
    ussl_log_warn("SSL_CTX_use_enc_PrivateKey failed, ret:%d, err:%s", ret,
                  ERR_error_string(ERR_get_error(), NULL));
  } else if (NULL == (enc_x509 = ob_ssl_get_sm_cert_memory(ssl_config->enc_cert))) {
    ret = EINVAL;
    ussl_log_warn("ob_ssl_get_sm_cert_memory failed, ret:%d", ret);
  } else if (!SSL_CTX_use_enc_certificate(ctx, enc_x509)) {
    ret = EINVAL;
    ussl_log_warn("SSL_CTX_use_enc_certificate failed,ret:%d, err:%s", ret,
                  ERR_error_string(ERR_get_error(), NULL));
  }
  if (NULL != sign_pkey) {
    EVP_PKEY_free(sign_pkey);
  }
  if (NULL != enc_pkey) {
    EVP_PKEY_free(enc_pkey);
  }
  if (NULL != sign_x509) {
    X509_free(sign_x509);
  }
  if (NULL != enc_x509) {
    X509_free(enc_x509);
  }
  return ret;
}
#endif

static int ob_ssl_load_cert_and_pkey_for_intl_memory(SSL_CTX *ctx,
                                                     const ssl_config_item_t *ssl_config)
{
  int ret = 0;
  int is_first = 1;
  BIO *bio = NULL;
  STACK_OF(X509_INFO) *inf = NULL;

  if (NULL == (bio = BIO_new_mem_buf((void *)ssl_config->sign_cert, -1))) {
    ret = ENOMEM;
    ussl_log_error("BIO_new_mem_buf failed, ret:%d", ret);
  } else if (NULL == (inf = PEM_X509_INFO_read_bio(bio, NULL, NULL, NULL))) {
    ret = ENOMEM;
    ussl_log_error("PEM_X509_INFO_read_bio failed, ret:%d", ret);
  } else {
    for (int i = 0; i < sk_X509_INFO_num(inf) && (0 == ret); i++) {
      X509_INFO *itmp = sk_X509_INFO_value(inf, i);
      if (itmp->x509) {
        if (is_first) {
          is_first = 0;
          if (SSL_CTX_use_certificate(ctx, itmp->x509) <= 0) {
            ret = EINVAL;
            ussl_log_warn("SSL_CTX_use_certificate failed, ret:%d", ret);
          }
        } else {
          if (SSL_CTX_add_extra_chain_cert(ctx, itmp->x509) <= 0) {
            ret = EINVAL;
            ussl_log_warn("SSL_CTX_add_extra_chain_cert failed, ret:%d", ret);
          } else {
            itmp->x509 = NULL;
          }
        }
      }
    }
    // free bio allocate before
    if (NULL != bio) {
      BIO_free(bio);
    }
    bio = NULL;
    if (0 == ret) {
      RSA *rsa = NULL;
      if (NULL == (bio = BIO_new_mem_buf((void *)ssl_config->sign_private_key, -1))) {
        ret = ENOMEM;
        ussl_log_error("BIO_new_mem_buf for sign_private_key failed, ret:%d", ret);
      } else if (NULL == (rsa = PEM_read_bio_RSAPrivateKey(bio, NULL, 0, NULL))) {
        ret = ENOMEM;
        ussl_log_warn("PEM_read_bio_RSAPrivateKey failed, ret:%d", ret);
      } else if (SSL_CTX_use_RSAPrivateKey(ctx, rsa) <= 0) {
        ret = EINVAL;
        ussl_log_warn("SSL_CTX_use_RSAPrivateKey failed, ret:%d", ret);
      }
      if (0 != ret) {
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

static int ob_ssl_load_cert_and_pkey(SSL_CTX *ctx, const ssl_config_item_t *ssl_config)
{
  int ret = 0;
  if (ssl_config->is_from_file) {
    if (ssl_config->is_sm) {
#ifdef OB_USE_BABASSL
      if (!SSL_CTX_use_sign_PrivateKey_file(ctx, ssl_config->sign_private_key, SSL_FILETYPE_PEM)) {
        ret = EINVAL;
        ussl_log_warn("SSL_CTX_use_sign_PrivateKey_file failed,ret:%d", ret);
      } else if (!SSL_CTX_use_sign_certificate_file(ctx, ssl_config->sign_cert, SSL_FILETYPE_PEM)) {
        ret = EINVAL;
        ussl_log_warn("SSL_CTX_use_sign_certificate_file failed, ret:%d", ret);
      } else if (!SSL_CTX_use_enc_PrivateKey_file(ctx, ssl_config->enc_private_key,
                                                  SSL_FILETYPE_PEM)) {
        ret = EINVAL;
        ussl_log_warn("SSL_CTX_use_enc_PrivateKey_file failed, ret:%d", ret);
      } else if (!SSL_CTX_use_enc_certificate_file(ctx, ssl_config->enc_cert, SSL_FILETYPE_PEM)) {
        ret = EINVAL;
        ussl_log_warn("SSL_CTX_use_enc_certificate_file failed, ret:%d", ret);
      }
#endif
    } else {
      if (SSL_CTX_use_certificate_chain_file(ctx, ssl_config->sign_cert) <= 0) {
        ret = EINVAL;
        ussl_log_warn("SSL_CTX_use_certificate_chain_file failed, ret:%d", ret);
      } else if (SSL_CTX_use_PrivateKey_file(ctx, ssl_config->sign_private_key, SSL_FILETYPE_PEM) <=
                 0) {
        ret = EINVAL;
        ussl_log_warn("SSL_CTX_use_PrivateKey_file failed, ret:%d", ret);
      } else if (SSL_CTX_check_private_key(ctx) <= 0) {
        ret = EINVAL;
        ussl_log_warn("SSL_CTX_check_private_key failed, ret:%d", ret);
      }
    }
  } else {
    if (ssl_config->is_sm) {
#ifdef OB_USE_BABASSL
      if (0 != (ret = ob_ssl_load_cert_and_pkey_for_sm_memory(ctx, ssl_config))) {
        ussl_log_warn("ob_ssl_load_cert_and_pkey_for_sm_memory failed, ret:%d", ret);
      }
#endif
    } else {
      if (0 != (ret = ob_ssl_load_cert_and_pkey_for_intl_memory(ctx, ssl_config))) {
        ussl_log_warn("ob_ssl_load_cert_and_pkey_for_intl_memory failed, ret:%d", ret);
      } else if (SSL_CTX_check_private_key(ctx) <= 0) {
        ret = EINVAL;
        ussl_log_warn("SSL_CTX_check_private_key failed, ret:%d, err:%s", ret,
                      ERR_error_string(ERR_get_error(), NULL));
      }
    }
  }
  return ret;
}

static const int CLIENT = 0;
static const int SERVER = 1;

static SSL_CTX *ob_ssl_create_ssl_ctx(const ssl_config_item_t *ssl_config, int type)
{
  int ret = 0;
  SSL_CTX *ctx = NULL;
  int verify_flag = 0;
  if (CLIENT == type) {
    verify_flag = SSL_VERIFY_NONE;
  } else {
    verify_flag = SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT;
  }

  if (ssl_config->is_sm) {
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
    ret = ENOMEM;
    ussl_log_error("SSL_CTX_new failed, ret:%d", ret);
  } else if (SSL_CTX_set_cipher_list(
                 ctx, (ssl_config->is_sm ? baba_tls_ciphers_list : tls_ciphers_list)) <= 0) {
    ret = EINVAL;
    ussl_log_warn("SSL_CTX_set_cipher_list failed, ret:%d, err:%s", ret,
                  ERR_error_string(ERR_get_error(), NULL));
  } else if (0 != (ret = ob_ssl_set_verify_mode_and_load_CA(ctx, ssl_config, verify_flag))) {
    ussl_log_warn("ob_ssl_set_verify_mode_and_load_CA failed, ret:%d", ret);
  } else if (0 != (ret = ob_ssl_load_cert_and_pkey(ctx, ssl_config))) {
    ussl_log_warn("ob_ssl_load_cert_and_pkey for client failed, ret:%d", ret);
  } else {
    /* client side options */
    SSL_CTX_set_options(ctx, SSL_OP_MICROSOFT_SESS_ID_BUG);
    SSL_CTX_set_options(ctx, SSL_OP_NETSCAPE_CHALLENGE_BUG);
    SSL_CTX_set_options(ctx, SSL_OP_NETSCAPE_REUSE_CIPHER_CHANGE_BUG);

    /* server side options */
    SSL_CTX_set_options(ctx, SSL_OP_SSLREF2_REUSE_CERT_TYPE_BUG);
    SSL_CTX_set_options(ctx, SSL_OP_MICROSOFT_BIG_SSLV3_BUFFER);
  #if OPENSSL_VERSION_NUMBER >= 0x10101000L
    SSL_CTX_set_options(ctx, SSL_OP_NO_TLSv1_3);
  #endif
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
  if (0 != ret) {
    SSL_CTX_free(ctx);
    ctx = NULL;
  }
  return ctx;
}

static int ob_ssl_ctx_reconfigure(int ctx_id, int ctx_role, SSL_CTX *ssl_ctx)
{
  int ret = 0;
  SSL_CTX *ssl_ctx_temp = NULL;
  if (NULL == (ssl_ctx_temp = gs_ssl_ctx_array[ctx_id][ctx_role])) {
    gs_ssl_ctx_array[ctx_id][ctx_role] = ssl_ctx;
  } else {
    SSL_CTX_free(ssl_ctx_temp);
    gs_ssl_ctx_array[ctx_id][ctx_role] = ssl_ctx;
  }
  return ret;
}

void fd_disable_ssl(int fd)
{
  if (fd >= 0) {
    SSL *ssl = gs_fd_ssl_array[fd].ssl;
    gs_fd_ssl_array[fd].ssl = NULL;
    gs_fd_ssl_array[fd].hand_shake_done = 0;
    gs_fd_ssl_array[fd].type = 0;
    if (NULL == ssl) {
    } else {
      int mode = 0;
      mode = SSL_SENT_SHUTDOWN | SSL_RECEIVED_SHUTDOWN;
      SSL_set_shutdown(ssl, mode);
      ERR_clear_error();
      SSL_shutdown(ssl);
      SSL_free(ssl);
    }
  }
}

int ssl_load_config(int ctx_id, const ssl_config_item_t *ssl_config)
{
  int ret = 0;
  SSL_CTX *client_ssl_ctx = NULL;
  SSL_CTX *server_ssl_ctx = NULL;
  if (ctx_id >= SSL_CTX_ID_MAX || ctx_id < 0) {
    ret = EINVAL;
    ussl_log_error("ctx_id is invalid, ret:%d, ctx_id:%d, max_ctx_id:%d", ret, ctx_id,
                   SSL_CTX_ID_MAX);
  } else {
    if (0 != (ret = ob_ssl_config_check(ssl_config))) {
      ussl_log_warn("ob_ssl_config_check failed, ret:%d, ctx_id:%d", ret, ctx_id);
    } else if (NULL == (client_ssl_ctx = ob_ssl_create_ssl_ctx(ssl_config, CLIENT))) {
      ret = EINVAL;
      ussl_log_warn("ob_ssl_create_client_ctx failed, ctx_id:%d, ret:%d", ctx_id, ret);
    } else if (NULL == (server_ssl_ctx = ob_ssl_create_ssl_ctx(ssl_config, SERVER))) {
      ret = EINVAL;
      ussl_log_warn("ob_ssl_create_server_ctx failed, ctx_id:%d, ret:%d", ctx_id, ret);
    } else {
      pthread_rwlock_wrlock(&g_ssl_ctx_rwlock);
      if (0 != (ret = ob_ssl_ctx_reconfigure(ctx_id, SSL_ROLE_SERVER, server_ssl_ctx))) {
        ussl_log_warn("ob_ssl_ctx_reconfigure for server failed, ctx_id:%d, ret:%d", ctx_id, ret);
      } else if (0 != (ret = ob_ssl_ctx_reconfigure(ctx_id, SSL_ROLE_CLIENT, client_ssl_ctx))) {
        ussl_log_warn("ob_ssl_ctx_reconfigure for client failed, ctx_id:%d, ret:%d", ctx_id, ret);
      } else {
        ssl_config_ctx_id = ctx_id;
        ussl_log_info("load ssl config success! ctx_id:%d", ctx_id);
      }
      pthread_rwlock_unlock(&g_ssl_ctx_rwlock);
    }
  }
  return ret;
}

int fd_enable_ssl_for_server(int fd, int ctx_id, int type, int has_method_none)
{
  int ret = 0;
  SSL_CTX *ctx = NULL;
  pthread_rwlock_rdlock(&g_ssl_ctx_rwlock);
  if (ctx_id >= SSL_CTX_ID_MAX || ctx_id < 0 || fd >= FD_MAX) {
    ret = EINVAL;
    ussl_log_error("ctx_id or fd is beyond limit, ret:%d, fd:%d, ctx_id:%d", ret, fd, ctx_id);
  } else if (NULL == (ctx = gs_ssl_ctx_array[ctx_id][SSL_ROLE_SERVER])) {
    ret = EINVAL;
    ussl_log_warn("SSL_CTX is null, maybe not configured, ret:%d, fd:%d, ctx_id:%d", ret, fd,
                  ctx_id);
  } else {
    SSL *ssl = NULL;
    if (NULL == (ssl = SSL_new(ctx))) {
      ret = ENOMEM;
      ussl_log_error("SSL_new failed, ret:%d, fd:%d, ctx_id:%d", ret, fd, ctx_id);
    } else if (0 == SSL_set_fd(ssl, fd)) {
      ret = EINVAL;
      ussl_log_warn("SSL_set_fd failed, ret:%d, fd:%d, ctx_id:%d", ret, fd, ctx_id);
    } else {
      //if server has auth method none, server does not verify client identity
      if (has_method_none) {
        SSL_set_verify(ssl, SSL_VERIFY_NONE, NULL);
      }
      SSL_set_accept_state(ssl);
      ATOMIC_STORE(&(gs_fd_ssl_array[fd].ssl), ssl);
      ATOMIC_STORE(&(gs_fd_ssl_array[fd].type), type);
    }
    if (0 != ret) {
      if (NULL != ssl) {
        SSL_free(ssl);
      }
    }
  }
  pthread_rwlock_unlock(&g_ssl_ctx_rwlock);
  return ret;
}

int fd_enable_ssl_for_client(int fd, int ctx_id, int type)
{
  int ret = 0;
  SSL_CTX *ctx = NULL;
  pthread_rwlock_rdlock(&g_ssl_ctx_rwlock);
  if (ctx_id >= SSL_CTX_ID_MAX || ctx_id < 0 || fd >= FD_MAX) {
    ret = EINVAL;
    ussl_log_error("ctx_id or fd is beyond limit, ret:%d, fd:%d, ctx_id:%d", ret, fd, ctx_id);
  } else if (NULL == (ctx = gs_ssl_ctx_array[ctx_id][SSL_ROLE_CLIENT])) {
    ret = EINVAL;
    ussl_log_warn("SSL_CTX is null, maybe not configured, ret:%d, fd:%d, ctx_id:%d", ret, fd,
                  ctx_id);
  } else {
    SSL *ssl = NULL;
    if (NULL == (ssl = SSL_new(ctx))) {
      ret = ENOMEM;
      ussl_log_error("SSL_new failed, ret:%d, fd:%d, ctx_id:%d", ret, fd, ctx_id);
    } else if (0 == SSL_set_fd(ssl, fd)) {
      ret = EINVAL;
      ussl_log_warn("SSL_set_fd failed, ret:%d, fd:%d, ctx_id:%d", ret, fd, ctx_id);
    } else {
      SSL_set_connect_state(ssl);
      ATOMIC_STORE(&(gs_fd_ssl_array[fd].ssl), ssl);
      ATOMIC_STORE(&(gs_fd_ssl_array[fd].type), type);
    }
    if (0 != ret) {
      if (NULL != ssl) {
        SSL_free(ssl);
      }
    }
  }
  pthread_rwlock_unlock(&g_ssl_ctx_rwlock);
  return ret;
}

int ssl_do_handshake(int fd)
{
  int ret = 0;
  SSL *ssl = NULL;
  if (NULL == (ssl = gs_fd_ssl_array[fd].ssl)) {
    ret = EINVAL;
    ussl_log_error("ssl is NULL, fd:%d, ret:%d", fd, ret);
  } else {
    ERR_clear_error();
    int ssl_ret = SSL_do_handshake(ssl);
    if (ssl_ret > 0) {
      gs_fd_ssl_array[fd].hand_shake_done = 1;
      if (USSL_AUTH_SSL_HANDSHAKE == gs_fd_ssl_array[fd].type) {
        fd_disable_ssl(fd);
      }
    } else {
      int err = SSL_get_error(ssl, ssl_ret);
      if (SSL_ERROR_WANT_READ == err) {
        ret = EAGAIN;
      } else if (SSL_ERROR_WANT_WRITE == err) {
        fd_disable_ssl(fd);
        ret = EIO;
        ussl_log_error("SSL_do_handshake want write, fd:%d", fd);
      } else {
        fd_disable_ssl(fd);
        ret = EIO;
        ussl_log_warn("SSL_do_handshake failed, err:%s", ERR_error_string(ERR_get_error(), NULL));
      }
    }
  }
  return ret;
}

/*
static ssize_t do_ssl_read(int fildes, void *buf, size_t nbyte)
{
  ssize_t rbytes = -1;
  SSL *ssl = gs_fd_ssl_array[fildes].ssl;
  rbytes = SSL_read(ssl, buf, nbyte);
  if (rbytes <= 0) {
    int ssl_error = 0;
    ssl_error = SSL_get_error(ssl, rbytes);
    if (SSL_ERROR_WANT_READ == ssl_error) {
      rbytes = -1;
      errno = EAGAIN;
    } else if (SSL_ERROR_WANT_WRITE == ssl_error) {
      rbytes = -1;
      errno = EIO;
      ussl_log_error("SSL_read want write, maybe peer started SSL renegotiation, fd:%d", fildes);
    } else if (SSL_ERROR_ZERO_RETURN == ssl_error) {
      //connection shutdown by peer
      rbytes = 0;
      ussl_log_info("SSL_read return SSL_ERROR_ZERO_RETURN, peer shutdown, fd:%d", fildes);
    } else {
      rbytes = -1;
      errno = EIO;
      ussl_log_error("SSL_read failed, fd:%d, ssl_error:%d, errno:%d, reason:%s", fildes, ssl_error,
                     errno, ERR_error_string(ERR_get_error(), NULL));
    }
  }
  return rbytes;
}
*/

static ssize_t do_ssl_write(int fildes, const void *buf, size_t nbyte)
{
  ssize_t wbytes = -1;
  SSL *ssl = gs_fd_ssl_array[fildes].ssl;
  wbytes = SSL_write(ssl, buf, nbyte);
  if (wbytes <= 0) {
    wbytes = -1;
    int ssl_error = SSL_get_error(ssl, wbytes);
    if (SSL_ERROR_WANT_WRITE == ssl_error) {
      errno = EAGAIN;
    } else if (SSL_ERROR_WANT_READ == ssl_error) {
      errno = EIO;
      ussl_log_error("SSL_write want read, fd:%d", fildes);
    } else {
      errno = EIO;
      ussl_log_error("SSL_write failed, fd:%d, reason:%s", fildes,
                     ERR_error_string(ERR_get_error(), NULL));
    }
  }
  return wbytes;
}

ssize_t write_regard_ssl(int fildes, const void *buf, size_t nbyte)
{
  ssize_t wbytes = -1;
  if (fildes < 0 || fildes >= FD_MAX) {
    errno = EINVAL;
  } else {
    if (NULL == gs_fd_ssl_array[fildes].ssl) {
      wbytes = libc_write(fildes, buf, nbyte);
    } else {
      ERR_clear_error();
      wbytes = do_ssl_write(fildes, buf, nbyte);
    }
  }

  return wbytes;
}

static int ssl_handle_recv(SSL *ssl, ssize_t n)
{
  int sslerr = 0;
  int err = 0;
  int ret = 0;
  if (n > 0) {
    ret = 0;
  } else {
    sslerr = SSL_get_error(ssl, n);
    err = (SSL_ERROR_SYSCALL == sslerr) ? errno: 0;
    if (SSL_ERROR_WANT_READ == sslerr) {
      ret = EAGAIN;
      errno = EAGAIN;
    } else if (SSL_ERROR_ZERO_RETURN == sslerr || ERR_peek_error() == 0) {
      errno = ECONNRESET;
      ret = ECONNRESET;
    } else {
      errno = EIO;
      ret = EIO;
    }
  }

  return ret;
}

ssize_t read_regard_ssl(int fildes, char *buf, size_t nbytes)
{
  ssize_t rbytes = -1;
  if (fildes < 0 || fildes >= FD_MAX) {
    errno = EINVAL;
  } else {
    if (NULL == gs_fd_ssl_array[fildes].ssl) {
      rbytes = libc_read(fildes, buf, nbytes);
    } else {
      rbytes = 0;
      ssize_t n = 0;
      SSL *ssl = gs_fd_ssl_array[fildes].ssl;
      ERR_clear_error();
      int tmp_ret = 0;
      while (0 == tmp_ret) {
        n = SSL_read(ssl, buf, nbytes);
        if (n > 0) {
          rbytes += n;
        }
        int ret = ssl_handle_recv(ssl, n);
        if (0 == ret) {
          nbytes -= n;
          if (0 == nbytes) {
            tmp_ret = ENODATA;
            continue;
          }
          buf += n;
          continue;
        } else {
          if (rbytes) {
            tmp_ret = ENODATA;
            continue;
          }
          if (ECONNRESET == ret) {
            rbytes = 0;
            tmp_ret = ENODATA;
            continue;
          } else {
            rbytes = -1;
            tmp_ret = ENODATA;
            continue;
          }
        }
      }
    }
  }
  return rbytes;
}

ssize_t writev_regard_ssl(int fildes, const struct iovec *iov, int iovcnt)
{
  ssize_t wbytes = -1;
  if (fildes < 0 || fildes >= FD_MAX) {
    errno = EINVAL;
  } else {
    if (NULL == gs_fd_ssl_array[fildes].ssl) {
      wbytes = libc_writev(fildes, iov, iovcnt);
    } else {
      int i = 0;
      int ret = 0;
      ssize_t n = 0;
      SSL *ssl = gs_fd_ssl_array[fildes].ssl;
      wbytes = 0;
      for (i = 0; (i < iovcnt) && (0 == ret); i++) {
        ERR_clear_error();
        n = SSL_write(ssl, iov[i].iov_base, iov[i].iov_len);
        if (n > 0) {
          wbytes += n;
        } else {
          int ssl_error = SSL_get_error(ssl, n);
          if (SSL_ERROR_WANT_WRITE == ssl_error) {
            errno = EAGAIN;
          } else if (SSL_ERROR_WANT_READ == ssl_error) {
            errno = EIO;
            ussl_log_error("SSL_write want read, fd:%d", fildes);
          } else {
            errno = EIO;
            ussl_log_error("SSL_write failed, fd:%d, reason:%s", fildes,
                          ERR_error_string(ERR_get_error(), NULL));
          }
          // errno: EIO, need destroy connection
          // errno: EAGAIN:
          // (1) wbytes larger than 0 (means already send some data successfully), just return wbytes
          // (2) wbytes equal to zero (means send the first iov), wbytes equals to n (-1 means socket buffer
          // temporarily unwritable, 0 means need destroy connection)
          if (EIO == errno) {
            wbytes = -1;
          } else if (EAGAIN == errno) {
            if (wbytes > 0) {
            } else {
              wbytes += n;
            }
          }
          ret = ENODATA;
        }
      }
    }
  }
  return wbytes;
}

SSL_CTX* ussl_get_server_ctx(int ctx_id)
{
  SSL_CTX *ctx = NULL;
  pthread_rwlock_rdlock(&g_ssl_ctx_rwlock);
  ctx = gs_ssl_ctx_array[ctx_id][SSL_ROLE_SERVER];
  pthread_rwlock_unlock(&g_ssl_ctx_rwlock);
  return ctx;
}