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

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LIB_LOG(WARN, "ObRsaGetter init twice", K(ret));
  } else {
    // Try to create wallet directory
    if (OB_FAIL(create_wallet_directory(DEFAULT_WALLET_PATH))) {
      LIB_LOG(WARN, "failed to create wallet directory", K(ret), K(DEFAULT_WALLET_PATH));
    } else {
      // Try to load existing keys
      int load_ret = load_key_from_wallet(DEFAULT_WALLET_PATH);
      if (OB_SUCCESS == load_ret) {
        LIB_LOG(INFO, "RSA key loaded from wallet", K(DEFAULT_WALLET_PATH));
      } else if (OB_FILE_NOT_EXIST == load_ret) {
        // If file doesn't exist, generate new key pair
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

      if (OB_SUCC(ret)) {
        is_inited_ = true;
      }
    }
  }

  if (!is_inited_) {
    destroy();
  }

  LIB_LOG(INFO, "ObRsaGetter init finished", K(ret), K(DEFAULT_WALLET_PATH));
  return ret;
}

void ObRsaGetter::destroy()
{
  if (is_inited_) {
    rsa_key_.reset();
    is_inited_ = false;
  }
}

int ObRsaGetter::generate_rsa_key_pair(int key_bits)
{
  int ret = OB_SUCCESS;
  RSA *rsa = nullptr;
  BIGNUM *bn = nullptr;
  BIO *bio_private = nullptr;
  BIO *bio_public = nullptr;

  if (key_bits < 1024 || key_bits > 4096) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid key bits", K(ret), K(key_bits));
  } else {
    // Create BIGNUM for public exponent
    bn = BN_new();
    if (bn == nullptr) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LIB_LOG(WARN, "failed to create BIGNUM", K(ret));
    } else if (BN_set_word(bn, OB_RSA_PUBLIC_EXPONENT) != 1) {
      ret = OB_ERR_UNEXPECTED;
      LIB_LOG(WARN, "failed to set BIGNUM", K(ret));
    } else {
      // Generate RSA key pair
      rsa = RSA_new();
      if (rsa == nullptr) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LIB_LOG(WARN, "failed to create RSA", K(ret));
      } else if (RSA_generate_key_ex(rsa, key_bits, bn, nullptr) != 1) {
        ret = OB_ERR_UNEXPECTED;
        LIB_LOG(WARN, "failed to generate RSA key", K(ret));
      } else {
        bio_private = BIO_new(BIO_s_mem());
        bio_public = BIO_new(BIO_s_mem());
        if (bio_private == nullptr) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LIB_LOG(WARN, "failed to create BIO for private key", K(ret));
        } else if (PEM_write_bio_RSAPrivateKey(bio_private, rsa, nullptr, nullptr, 0, nullptr, nullptr) != 1) {
          ret = OB_ERR_UNEXPECTED;
          LIB_LOG(WARN, "failed to write private key to BIO", K(ret));
        } else if (bio_public == nullptr) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LIB_LOG(WARN, "failed to create BIO for public key", K(ret));
        } else if (PEM_write_bio_RSA_PUBKEY(bio_public, rsa) != 1) {
          ret = OB_ERR_UNEXPECTED;
          LIB_LOG(WARN, "failed to write public key to BIO", K(ret));
        } else {
          char *private_key_data = nullptr;
          long private_key_len = BIO_get_mem_data(bio_private, &private_key_data);
          char *public_key_data = nullptr;
          long public_key_len = BIO_get_mem_data(bio_public, &public_key_data);

          if (private_key_len <= 0 || private_key_len >= OB_RSA_MAX_KEY_LENGTH) {
            ret = OB_SIZE_OVERFLOW;
            LIB_LOG(WARN, "private key size overflow", K(ret), K(private_key_len));
          } else if (public_key_len <= 0 || public_key_len >= OB_RSA_MAX_KEY_LENGTH) {
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

int ObRsaGetter::decrypt_with_private_key(
    const unsigned char *ciphertext,
    const int64_t ciphertext_len,
    unsigned char *plaintext,
    int64_t &plaintext_len,
    const int64_t max_plaintext_len)
{
  int ret = OB_SUCCESS;
  RSA *rsa = nullptr;
  BIO *bio = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LIB_LOG(WARN, "ObRsaGetter not inited", K(ret));
  } else if (ciphertext == nullptr || ciphertext_len <= 0 ||
             plaintext == nullptr || max_plaintext_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid argument", K(ret), KP(ciphertext), K(ciphertext_len),
            KP(plaintext), K(max_plaintext_len));
  } else if (!rsa_key_.is_valid()) {
    // Key data doesn't exist, report error
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(ERROR, "RSA key not loaded, should be initialized in observer init phase", K(ret));
  } else {
    // Load private key from PEM format
    bio = BIO_new_mem_buf(rsa_key_.private_key_, static_cast<int>(rsa_key_.private_key_len_));
    if (bio == nullptr) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LIB_LOG(WARN, "failed to create BIO", K(ret));
    } else {
      rsa = PEM_read_bio_RSAPrivateKey(bio, nullptr, nullptr, nullptr);
      if (rsa == nullptr) {
        ret = OB_ERR_UNEXPECTED;
        LIB_LOG(WARN, "failed to read RSA private key", K(ret));
      } else {
        int decrypted_len = RSA_private_decrypt(
            static_cast<int>(ciphertext_len),
            ciphertext,
            plaintext,
            rsa,
            RSA_PKCS1_OAEP_PADDING);

        if (decrypted_len < 0) {
          ret = OB_ERR_UNEXPECTED;
          LIB_LOG(WARN, "failed to decrypt with RSA", K(ret));
        } else if (decrypted_len > max_plaintext_len) {
          ret = OB_SIZE_OVERFLOW;
          LIB_LOG(WARN, "plaintext buffer too small", K(ret),
                  K(decrypted_len), K(max_plaintext_len));
        } else {
          plaintext_len = decrypted_len;
          LIB_LOG(DEBUG, "RSA decryption successful", K(ciphertext_len), K(plaintext_len));
        }
      }
    }

    // Clean up resources
    if (rsa != nullptr) {
      RSA_free(rsa);
    }
    if (bio != nullptr) {
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
        size_t read_len = fread(temp_private_key, 1, OB_RSA_MAX_KEY_LENGTH - 1, fp);

        if (read_len <= 0 || ferror(fp)) {
          ret = OB_IO_ERROR;
          LIB_LOG(WARN, "failed to read private key", K(ret), K(read_len));
        } else {
          temp_private_key[read_len] = '\0';
          LIB_LOG(INFO, "private key loaded", K(read_len));
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
                read_len = fread(temp_public_key, 1, OB_RSA_MAX_KEY_LENGTH - 1, fp);

                if (read_len <= 0 || ferror(fp)) {
                  ret = OB_IO_ERROR;
                  LIB_LOG(WARN, "failed to read public key", K(ret), K(read_len));
                } else {
                  temp_public_key[read_len] = '\0';
                  LIB_LOG(INFO, "public key loaded", K(read_len));
                }

                fclose(fp);
                fp = nullptr;

                if (OB_SUCC(ret)) {
                  // Save to member variables (no lock needed, no concurrency during init phase)
                  // First release old memory (if any)
                  rsa_key_.reset();

                  // Transfer temporary buffers to rsa_key_
                  rsa_key_.private_key_ = temp_private_key;
                  rsa_key_.private_key_len_ = strlen(temp_private_key);
                  rsa_key_.public_key_ = temp_public_key;
                  rsa_key_.public_key_len_ = read_len;

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

} // namespace common
} // namespace oceanbase
