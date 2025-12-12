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

#ifndef OB_RSA_GETTER_H_
#define OB_RSA_GETTER_H_

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/lock/ob_latch.h"
#include <openssl/rsa.h>
#include <openssl/pem.h>
#include <openssl/err.h>

namespace oceanbase {
namespace common {

// RSA key related constant definitions
#define OB_RSA_KEY_BITS 2048                     // RSA key bits
#define OB_RSA_MAX_KEY_LENGTH 4096               // RSA key maximum length (PEM format)
#define OB_RSA_PUBLIC_EXPONENT 65537             // RSA public exponent
#define OB_RSA_MAX_ENCRYPT_SIZE 256              // RSA maximum encryption data size
#define OB_RSA_MAX_DECRYPT_SIZE 256              // RSA maximum decryption data size

// RSA key storage structure
struct ObRsaKey
{
public:
  char *private_key_;                           // Private key (PEM format, heap allocated)
  char *public_key_;                            // Public key (PEM format, heap allocated)
  int64_t private_key_len_;                     // Private key length
  int64_t public_key_len_;                      // Public key length

  ObRsaKey() : private_key_(nullptr), public_key_(nullptr),
               private_key_len_(0), public_key_len_(0)
  {
  }

  ~ObRsaKey()
  {
    reset();
  }

  void reset()
  {
    if (private_key_ != nullptr) {
      MEMSET(private_key_, 0, OB_RSA_MAX_KEY_LENGTH);
      ob_free(private_key_);
      private_key_ = nullptr;
    }
    if (public_key_ != nullptr) {
      MEMSET(public_key_, 0, OB_RSA_MAX_KEY_LENGTH);
      ob_free(public_key_);
      public_key_ = nullptr;
    }
    private_key_len_ = 0;
    public_key_len_ = 0;
  }

  bool is_valid() const
  {
    return private_key_ != nullptr && public_key_ != nullptr &&
           private_key_len_ > 0 && public_key_len_ > 0;
  }

  int64_t to_string(char* buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    if (nullptr != buf && buf_len > 0) {
      pos = snprintf(buf, buf_len, "private_key_len=%ld, public_key_len=%ld",
                    private_key_len_, public_key_len_);
    }
    return pos;
  }
};

// RSA key getter class
class ObRsaGetter
{
public:
  static ObRsaGetter &instance();

  /**
   * Initialize RSA getter (must be called during observer init phase, no concurrency)
   * Will try to load keys from wallet, if not exist, generate new keys and save
   * Uses default wallet path
   * @return Error code
   */
  int init();

  /**
   * Destroy RSA getter
   */
  void destroy();

  /**
   * Get public key (read-only operation, data must be prepared in init phase)
   * @param public_key Output public key string
   * @param allocator Memory allocator
   * @return Error code, returns error if not initialized or key doesn't exist
   */
  int get_public_key(ObString &public_key, ObIAllocator &allocator);

  /**
   * Decrypt data using private key (read-only operation)
   * @param ciphertext Ciphertext data
   * @param ciphertext_len Ciphertext data length
   * @param plaintext Plaintext output buffer
   * @param plaintext_len Plaintext length (output)
   * @param max_plaintext_len Maximum plaintext buffer length
   * @return Error code, returns error if key doesn't exist
   */
  int decrypt_with_private_key(
      const unsigned char *ciphertext,
      const int64_t ciphertext_len,
      unsigned char *plaintext,
      int64_t &plaintext_len,
      const int64_t max_plaintext_len);

  /**
   * Check if key is loaded
   * @return true if key is loaded
   */
  bool is_key_loaded() const { return rsa_key_.is_valid(); }

private:
  ObRsaGetter();
  ~ObRsaGetter();

  /**
   * Generate RSA key pair (only called during init phase)
   * @param key_bits RSA key bits, default 2048
   * @return Error code
   */
  int generate_rsa_key_pair(int key_bits = OB_RSA_KEY_BITS);

  /**
   * Load RSA key from wallet file (only called during init phase)
   * @param wallet_path Wallet directory path, if empty use default path
   * @return Error code
   */
  int load_key_from_wallet(const char *wallet_path);

  /**
   * Save RSA key to wallet file (only called during init phase)
   * @param wallet_path Wallet directory path, if empty use default path
   * @return Error code
   */
  int save_key_to_wallet(const char *wallet_path);

  /**
   * Create wallet directory (if not exists) (only called during init phase)
   * @param wallet_path Wallet directory path
   * @return Error code
   */
  int create_wallet_directory(const char *wallet_path);

private:
  static const char *DEFAULT_WALLET_PATH;        // Default wallet path
  static const char *RSA_PRIVATE_KEY_FILE;       // Private key file name
  static const char *RSA_PUBLIC_KEY_FILE;        // Public key file name

  bool is_inited_;                                // Whether initialized
  ObRsaKey rsa_key_;                             // RSA key pair (read-only after initialization)

  DISALLOW_COPY_AND_ASSIGN(ObRsaGetter);
};

} // namespace common
} // namespace oceanbase

#endif // OB_RSA_GETTER_H_
