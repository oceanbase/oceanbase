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

#ifndef OCEANBASE_LIB_ENCRYPT_OB_ENCRYPTED_HELPER_
#define OCEANBASE_LIB_ENCRYPT_OB_ENCRYPTED_HELPER_

#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include "lib/string/ob_string.h"

#define SCRAMBLE_LENGTH 20
#define SHA1_HASH_SIZE 20 /* Hash size in bytes */
#define ENC_STRING_BUF_LEN SCRAMBLE_LENGTH * 2 + 1 //strlen(hash str) + '*'
#define ENC_BUF_LEN SCRAMBLE_LENGTH * 2 + 2 //strlen(hash str) + '*' + '\0'
#define SHA1CircularShift(bits,word) (((word) << (bits)) | ((word) >> (32-(bits))))

namespace oceanbase
{
namespace common
{
class ObEncryptedHelper
{
public:
  static int encrypt_passwd_to_stage1(const ObString &password, ObString &stage1);
  static int encrypt_passwd_to_stage2(const ObString &password, ObString &encrypted_pass);
  static int encrypt_stage1_to_stage2_hex(const ObString &stage1, char *stage2_hex_buf,
                                          const int64_t buf_len, int64_t &copy_len);
  static int check_login(const ObString &login_reply,
                         const ObString &scramble_str,
                         const ObString &stored_stage2,
                         bool &pass);
  static int displayable_to_hex(const ObString &displayable, ObString &hex);

  // used by mysql client
  static int encrypt_password(const ObString &raw_pwd, const ObString &scramble,
                              char *pwd_buf, const int64_t buf_len, int64_t &copy_len);

  // used by proxy client
  static int encrypt_stage1_hex(const ObString &stage1_hex_str,
                                const ObString &scramble, char *pwd_buf,
                                const int64_t buf_len, int64_t &copy_len);

  static int my_xor(const unsigned char *s1,
                    const unsigned char *s2,
                    uint32_t len,
                    unsigned char *to);

private:
  /*
      This structure will hold context information for the SHA-1
      hashing operation
   */
  typedef struct SHA1_CONTEXT
  {
    unsigned long  Length;    /* Message length in bits      */
    uint32_t Intermediate_Hash[SHA1_HASH_SIZE / 4]; /* Message Digest  */
    int Computed;     /* Is the digest computed?     */
    int Corrupted;    /* Is the message digest corrupted? */
    int16_t Message_Block_Index;  /* Index into message block array   */
    uint8_t Message_Block[64];  /* 512-bit message blocks      */
  } SHA1_CONTEXT;

  enum sha_result_codes
  {
    SHA_SUCCESS = 0,
    SHA_NULL,   /* Null pointer parameter */
    SHA_INPUT_TOO_LONG, /* input data too long */
    SHA_STATE_ERROR /* called Input after Result */
  };

  /*
      Process the next 512 bits of the message stored in the Message_Block array.

      SYNOPSIS
        SHA1ProcessMessageBlock()

       DESCRIPTION
         Many of the variable names in this code, especially the single
         character names, were used because those were the names used in
         the publication.
  */
  static void SHA1ProcessMessageBlock(SHA1_CONTEXT *context);
  /*
    Pad message

    SYNOPSIS
      SHA1PadMessage()
      context: [in/out]   The context to pad

    DESCRIPTION
      According to the standard, the message must be padded to an even
      512 bits.  The first padding bit must be a '1'. The last 64 bits
      represent the length of the original message.  All bits in between
      should be 0.  This function will pad the message according to
      those rules by filling the Message_Block array accordingly.  It
      will also call the ProcessMessageBlock function provided
      appropriately. When it returns, it can be assumed that the message
      digest has been computed.

  */
  static void SHA1PadMessage(SHA1_CONTEXT *context);
  /*
    Initialize SHA1Context

    SYNOPSIS
      mysql_sha1_reset()
      context [in/out]    The context to reset.

   DESCRIPTION
     This function will initialize the SHA1Context in preparation
     for computing a new SHA1 message digest.

   RETURN
     SHA_SUCCESS    ok
     != SHA_SUCCESS sha Error Code.
  */
  static int mysql_sha1_reset(SHA1_CONTEXT *context);
  //add check in the wrapper, leaving inner mysql code untouched.
  static int mysql_sha1_reset_wrap(SHA1_CONTEXT *context);

  /*
     Return the 160-bit message digest into the array provided by the caller

    SYNOPSIS
      mysql_sha1_result()
      context [in/out]    The context to use to calculate the SHA-1 hash.
      Message_Digest: [out] Where the digest is returned.

    DESCRIPTION
      NOTE: The first octet of hash is stored in the 0th element,
      the last octet of hash in the 19th element.

   RETURN
     SHA_SUCCESS    ok
     != SHA_SUCCESS sha Error Code.
  */

  static int mysql_sha1_result(SHA1_CONTEXT *context,
                               unsigned char Message_Digest[SHA1_HASH_SIZE]);
  //add check in the wrapper, leaving inner mysql code untouched.
  static int mysql_sha1_result_wrap(SHA1_CONTEXT *context, unsigned char *message_digest);

  /*
    Accepts an array of octets as the next portion of the message.

    SYNOPSIS
     mysql_sha1_input()
     context [in/out] The SHA context to update
     message_array  An array of characters representing the next portion
        of the message.
    length    The length of the message in message_array

   RETURN
     SHA_SUCCESS    ok
     != SHA_SUCCESS sha Error Code.
  */
  static int mysql_sha1_input(SHA1_CONTEXT *context,
                              const unsigned char *message_array,
                              unsigned length);
  //add check in the wrapper, leaving inner mysql code untouched.
  static int mysql_sha1_input_wrap(SHA1_CONTEXT *context,
                                   const unsigned char *message_array,
                                   const ObString::obstr_size_t length);

  static int char_to_hex(char literal, int64_t &out);

  const static uint32_t sha_const_key[5];
  /* Constants defined in SHA-1 */
  static const uint32_t K[4];

  DISALLOW_COPY_AND_ASSIGN(ObEncryptedHelper);
};
}// namespace common
}//namespace oceanbase
#endif
