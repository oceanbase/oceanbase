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

#define USING_LOG_PREFIX SHARE_SCHEMA

#include "lib/encrypt/ob_encrypted_helper.h"

#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"

using namespace oceanbase;
using namespace oceanbase::common;

const uint32_t ObEncryptedHelper::sha_const_key[5] =
{
  0x67452301,
  0xEFCDAB89,
  0x98BADCFE,
  0x10325476,
  0xC3D2E1F0
};
const uint32_t ObEncryptedHelper::K[4] =
{
  0x5A827999,
  0x6ED9EBA1,
  0x8F1BBCDC,
  0xCA62C1D6
};

/*
 *  When login, server sends random scramble str.
 *  Client gets the scramble, use this logic to generate login rsp:
 *    stage1 = sha1(passwd)
 *
 *    stage1's size is 41B, contain '*' + octal num(40B)
 *    it is readable for user
 * */
int ObEncryptedHelper::encrypt_passwd_to_stage1(const ObString &password, ObString &stage1)
{
  int ret = OB_SUCCESS;
  SHA1_CONTEXT sha1_context;
  memset(static_cast<void *>(&sha1_context), 0, sizeof(SHA1_CONTEXT));
  unsigned char hash_stage1[SHA1_HASH_SIZE] = {0};

  if (OB_ISNULL(stage1.ptr())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stage1 not null", KP(stage1.ptr()));
  } else if (OB_UNLIKELY(stage1.length() < ENC_STRING_BUF_LEN)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stage1 length too short", KP(stage1.ptr()), K(stage1.length()), K(ret));
  } else {
    // sha1(passwd) => stage1
    if (OB_FAIL(mysql_sha1_reset_wrap(&sha1_context))) {
      LOG_WARN("failed to reset sha context", K(ret));
    } else if (OB_FAIL(mysql_sha1_input_wrap(&sha1_context,
        reinterpret_cast<const unsigned char *>(password.ptr()), password.length()))) {
      LOG_WARN("failed to feed stage1 input", K(ret));
    } else if (OB_FAIL(mysql_sha1_result_wrap(&sha1_context, hash_stage1))) {
      LOG_WARN("failed to get hash_stage1", K(ret));
    } else {
      int64_t cnt = 0;
      char *out_buf = stage1.ptr();
      //in mysql, 40-digit hash always begins with a '*' in order to distinguish from pre 4.1 hash
      out_buf[0] = '*';
      for (int64_t i = 0; OB_SUCC(ret) && i < SHA1_HASH_SIZE; ++i) {
        cnt = snprintf(out_buf + 1 + i * 2, 3, "%02hhx",
            reinterpret_cast<char *>(&hash_stage1)[i]);
        if (2 != cnt) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to snprintf, converted cnt != 2", K(cnt), K(ret));
        }
      }
      LOG_DEBUG("succ to encrypt_passwd_to_stage1", K(stage1));
    }
  }
  return ret;
}

/*
 *  When login, server sends random scramble str.
 *  Client gets the scramble, use this logic to generate login rsp:
 *    stage1 = sha1(passwd)
 *    stage2 = sha1(stage1)
 *
 *    stage2's size is 41B, contain '*' + octal num(40B)
 *    it is readable for user
 * */
int ObEncryptedHelper::encrypt_passwd_to_stage2(const ObString &password, ObString &mysql_stage2)
{
  int ret = OB_SUCCESS;
  SHA1_CONTEXT sha1_context;
  memset(static_cast<void *>(&sha1_context), 0, sizeof(SHA1_CONTEXT));
  unsigned char hash_stage1[SHA1_HASH_SIZE] = {0};
  unsigned char hash_stage2[SHA1_HASH_SIZE] = {0};

  if (OB_ISNULL(mysql_stage2.ptr())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("mysql_stage2 not null", KP(mysql_stage2.ptr()));
  } else if (OB_UNLIKELY(mysql_stage2.length() < ENC_STRING_BUF_LEN)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("mysql_stage2 length too short",
        KP(mysql_stage2.ptr()), K(mysql_stage2.length()), K(ret));
  } else {
    /* sha1(passwd) => stage1
     * sha1(stage1) => stage2
     * concat('*', stage2) => mysql_stage2
     */
    if (OB_FAIL(mysql_sha1_reset_wrap(&sha1_context))) {
      LOG_WARN("failed to reset sha context", K(ret));
    } else if (OB_FAIL(mysql_sha1_input_wrap(&sha1_context,
        reinterpret_cast<const unsigned char *>(password.ptr()), password.length()))) {
      LOG_WARN("failed to feed stage1 input", K(ret));
    } else if (OB_FAIL(mysql_sha1_result_wrap(&sha1_context, hash_stage1))) {
      LOG_WARN("failed to get hash_stage1", K(ret));
    } else if (OB_FAIL(mysql_sha1_reset_wrap(&sha1_context))) {
      LOG_WARN("failed to reset sha context", K(ret));
    } else if (OB_FAIL(mysql_sha1_input_wrap(&sha1_context, hash_stage1, SHA1_HASH_SIZE))) {
      LOG_WARN("failed to feed stage2 input", K(ret));
    } else if (OB_FAIL(mysql_sha1_result_wrap(&sha1_context, hash_stage2))) {
      LOG_WARN("failed to get hash_stage2", K(ret));
    } else {
      int64_t cnt = 0;
      char *out_buf = mysql_stage2.ptr();
      //in mysql, 40-digit hash always begins with a '*' in order to distinguish from pre 4.1 hash
      out_buf[0] = '*';
      for (int64_t i = 0; OB_SUCC(ret) && i < SHA1_HASH_SIZE; ++i) {
        cnt = snprintf(out_buf + 1 + i * 2, 3, "%02hhx",
            reinterpret_cast<char *>(&hash_stage2)[i]);
        if (2 != cnt) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to snprintf, converted cnt != 2", K(cnt), K(ret));
        }
      }
    }
  }
  return ret;
}

/*
 *    stage1's size is 41B, contain '*' + octal num(40B)
 *    it is readable for user
 *
 *    stage2_hex's size is 20B, contain hex num(20B)
 *    it is unreadable for user
 * */
int ObEncryptedHelper::encrypt_stage1_to_stage2_hex(const ObString &stage1,
    char *stage2_hex_buf, const int64_t buf_len, int64_t &copy_len)
{
  int ret = OB_SUCCESS;
  SHA1_CONTEXT sha1_context;
  memset(static_cast<void *>(&sha1_context), 0, sizeof(SHA1_CONTEXT));

  if (OB_ISNULL(stage1.ptr()) || OB_UNLIKELY(stage1.length() != SCRAMBLE_LENGTH * 2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stage1 is invalid", KP(stage1.ptr()), KP(stage1.length()));
  } else if (OB_ISNULL(stage2_hex_buf) || OB_UNLIKELY(buf_len != SCRAMBLE_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stage2 is invalid", KP(stage2_hex_buf), K(buf_len), K(ret));
  } else {
    //stage1 ==> stage1_hex
    char stage1_hex[SCRAMBLE_LENGTH] = {0};
    ObString stage1_hex_str(SCRAMBLE_LENGTH, stage1_hex);
    if (OB_FAIL(displayable_to_hex(stage1, stage1_hex_str))) {
      LOG_WARN("fail to displayable_to_hex", K(stage1), K(ret));
    }

    // sha1(stage1_hex) => stage2
    if (FAILEDx(mysql_sha1_reset_wrap(&sha1_context))) {
      LOG_WARN("failed to reset sha context", K(ret));
    } else if (OB_FAIL(mysql_sha1_input_wrap(&sha1_context,
        reinterpret_cast<const unsigned char *>(stage1_hex_str.ptr()), stage1_hex_str.length()))) {
      LOG_WARN("failed to feed stage1 input", K(ret));
    } else if (OB_FAIL(mysql_sha1_result_wrap(&sha1_context, reinterpret_cast<unsigned char *>(stage2_hex_buf)))) {
      LOG_WARN("failed to get hash_stage1", K(ret));
    } else {
      copy_len = SCRAMBLE_LENGTH;
    }
  }
  return ret;
}

/*
 * MySQL validation logic:
 *  Server stores the 'stage2' hash, which is cleartext password calc-ed sha1 twice.
 *  When login, server sends random scramble str.
 *  Client gets the scramble, use this logic to generate login rsp:
 *    stage1 = sha1(passwd)
 *    stage2 = sha1(stage1)
 *    scrambled_stage2 = sha1(scramble_got_from_server, stage2)
 *    login_rsp = xor(stage1, scrambled_stage2)
 *
 **/
int ObEncryptedHelper::encrypt_password(const ObString &raw_pwd, const ObString &scramble_str,
                                        char *pwd_buf, const int64_t buf_len, int64_t &copy_len)
{
  int ret = OB_SUCCESS;
  copy_len = 0;
  if (raw_pwd.empty()) {
    LOG_INFO("empty password");
  } else if (OB_UNLIKELY(scramble_str.empty())
             || OB_UNLIKELY(SCRAMBLE_LENGTH != scramble_str.length())
             || OB_ISNULL(pwd_buf) || (buf_len <= SHA1_HASH_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(raw_pwd), K(scramble_str), KP(pwd_buf), K(buf_len), K(ret));
  } else {
    SHA1_CONTEXT sha1_context;
    MEMSET(static_cast<void *>(&sha1_context), 0, sizeof(SHA1_CONTEXT));
    unsigned char stage1_client[SHA1_HASH_SIZE] = {0};

    // 1. stage1 = sha1(passwd)
    if (OB_FAIL(mysql_sha1_reset_wrap(&sha1_context))) {
      LOG_WARN("failed to reset sha context", K(ret));
    } else if (OB_FAIL(mysql_sha1_input_wrap(&sha1_context,
            reinterpret_cast<const unsigned char *>(raw_pwd.ptr()), raw_pwd.length()))) {
      LOG_WARN("failed to feed raw_pwd input", K(ret));
    } else if (OB_FAIL(mysql_sha1_result_wrap(&sha1_context, stage1_client))) {
      LOG_WARN("failed to get stage1_client", K(ret));
    }

    // 2. stage2 = sha1(stage1)
    if (OB_SUCC(ret)) {
      const ObString stage1_str(SHA1_HASH_SIZE, reinterpret_cast<const char *>(stage1_client));
      if (OB_FAIL(encrypt_stage1_hex(stage1_str, scramble_str, pwd_buf, buf_len, copy_len))) {
        LOG_WARN("failed to encrypt_stage1", K(ret));
      }
    }
  }
  return ret;
}

/*
 * MySQL validation logic:
 *  Server stores the 'stage2' hash, which is cleartext password calc-ed sha1 twice.
 *  When login, server sends random scramble str.
 *  Client gets the scramble, use this logic to generate login rsp:
 *    stage1 = sha1(passwd)
 *    stage2 = sha1(stage1_hex)
 *    scrambled_stage2 = sha1(scramble_got_from_server, stage2_hex)
 *    login_rsp = xor(stage1_hex, scrambled_stage2)
 *
 *    stage1_hex's size is 20B, contain hex num(20B)
 *    it is unreadable for user
 **/
int ObEncryptedHelper::encrypt_stage1_hex(const ObString &stage1_hex_str,
    const ObString &scramble_str, char *pwd_buf, const int64_t buf_len, int64_t &copy_len)
{
  int ret = OB_SUCCESS;
  copy_len = 0;
  if (OB_UNLIKELY(scramble_str.empty())
      || OB_UNLIKELY(SCRAMBLE_LENGTH != scramble_str.length())
      || OB_ISNULL(pwd_buf)
      || OB_UNLIKELY(buf_len <= SHA1_HASH_SIZE)
      || OB_ISNULL(stage1_hex_str.ptr())
      || OB_UNLIKELY(stage1_hex_str.length() != SHA1_HASH_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", KP(stage1_hex_str.ptr()), K(stage1_hex_str.length()),
             K(scramble_str), KP(pwd_buf), K(buf_len), K(ret));
  } else {
    SHA1_CONTEXT sha1_context;
    MEMSET(static_cast<void *>(&sha1_context), 0, sizeof(SHA1_CONTEXT));
    unsigned char stage2_client[SHA1_HASH_SIZE] = {0};
    unsigned char stage2_scambled[SHA1_HASH_SIZE] = {0};


    // 1. stage2 = sha1(stage1)
    if (OB_SUCC(ret)) {
      if (OB_FAIL(mysql_sha1_reset_wrap(&sha1_context))) {
        LOG_WARN("failed to reset sha context", K(ret));
      } else if (OB_FAIL(mysql_sha1_input_wrap(&sha1_context,
          reinterpret_cast<const unsigned char *>(stage1_hex_str.ptr()), SHA1_HASH_SIZE))) {
        LOG_WARN("failed to feed stage1_client input", K(ret));
      } else if (OB_FAIL(mysql_sha1_result_wrap(&sha1_context, stage2_client))) {
        LOG_WARN("failed to get stage2_client", K(ret));
      }
    }

    // 2. scrambled_stage2 = sha1(scramble_got_from_server, stage2)
    if (OB_SUCC(ret)) {
      if (OB_FAIL(mysql_sha1_reset_wrap(&sha1_context))) {
        LOG_WARN("failed to reset sha context", K(ret));
      } else if (OB_FAIL(mysql_sha1_input_wrap(&sha1_context,
          reinterpret_cast<const unsigned char *>(scramble_str.ptr()), SCRAMBLE_LENGTH))) {
        LOG_WARN("failed to feed scramble_str input", K(ret));
      } else if (OB_FAIL(mysql_sha1_input_wrap(&sha1_context, stage2_client, SHA1_HASH_SIZE))) {
        LOG_WARN("failed to feed stage2_client input", K(ret));
      } else if (OB_FAIL(mysql_sha1_result_wrap(&sha1_context, stage2_scambled))) {
        LOG_WARN("failed to get stage2_scambled", K(ret));
      }
    }

    // 3. login_rsp = xor(stage1, scrambled_stage2)
    if (OB_SUCC(ret)) {
      if (OB_FAIL(my_xor(reinterpret_cast<const unsigned char *>(stage1_hex_str.ptr()),
          stage2_scambled, SHA1_HASH_SIZE, reinterpret_cast<unsigned char *>(pwd_buf)))) {
        LOG_WARN("failed to calc xor", K(ret));
      } else {
        copy_len = SHA1_HASH_SIZE;
      }
    }
  }
  return ret;
}

int ObEncryptedHelper::check_login(const ObString &login_reply,
                                   const ObString &scramble_str,
                                   const ObString &stored_stage2,
                                   bool &pass)
{
  int ret = OB_SUCCESS;
  SHA1_CONTEXT sha1_context;
  memset(static_cast<void *>(&sha1_context), 0, sizeof(SHA1_CONTEXT));
  unsigned char stage2_scambled[SHA1_HASH_SIZE] = {0};
  unsigned char stage1_client[SHA1_HASH_SIZE] = {0};
  unsigned char stage2_client[SHA1_HASH_SIZE] = {0};

  /*
   * MySQL validation logic:
   *  Server stores the 'stage2' hash, which is cleartext password calc-ed sha1 twice.
   *  When login, server sends random scramble str.
   *  Client gets the scramble, use this logic to generate login rsp:
   *    stage1 = sha1(passwd)
   *    stage2 = sha1(stage1)
   *    scrambled_stage2 = sha1(scramble_got_from_server, stage2)
   *    login_rsp = xor(stage1, scrambled_stage2)
   *  When server gets rsp, use this logic to check login:
   *    scrambled_stage2_server = sha1(scramble_got_from_server, stage2_stored)
   *    stage1_client_rebuilt = xor(login_rsp, scrambled_stage2_server)
   *    stage2_client_rebuilt = sha1(stage1_client_rebuilt)
   *    if (stage2_client_rebuilt == stage2_stored) {
   *      pass
   *    } else {
   *      die
   *    }
   * */
  if (NULL == scramble_str.ptr() || scramble_str.length() < SCRAMBLE_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid scramble_str", KP(scramble_str.ptr()), K(scramble_str.length()));
  } else if (NULL == stored_stage2.ptr() || stored_stage2.length() != SHA1_HASH_SIZE) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid stored_stage2", KP(stored_stage2.ptr()), K(stored_stage2.length()));
  } else if (NULL == login_reply.ptr() || login_reply.length() != SHA1_HASH_SIZE) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid login_reply", KP(login_reply.ptr()), K(login_reply.length()));
  } else {
    //scample the stored stage2 hash with scramble_str
    if (OB_FAIL(mysql_sha1_reset_wrap(&sha1_context))) {
      LOG_WARN("failed to reset sha context", K(ret));
    } else if (OB_FAIL(mysql_sha1_input_wrap(&sha1_context,
        reinterpret_cast<const unsigned char *>(scramble_str.ptr()), SCRAMBLE_LENGTH))) {
      LOG_WARN("failed to feed scramble_str input", K(ret));
    } else if (OB_FAIL(mysql_sha1_input_wrap(&sha1_context,
        reinterpret_cast<const unsigned char *>(stored_stage2.ptr()), SHA1_HASH_SIZE))) {
      LOG_WARN("failed to feed stored_stage2 input", K(ret));
    } else if (OB_FAIL(mysql_sha1_result_wrap(&sha1_context, stage2_scambled))) {
      LOG_WARN("failed to get stage2_scambled", K(ret));
    } else if (OB_FAIL(my_xor(reinterpret_cast<const unsigned char *>(login_reply.ptr()),
        stage2_scambled, SHA1_HASH_SIZE, stage1_client))) {
      LOG_WARN("failed to calc xor", K(ret));
    }
    //reproduce stage2 of client, compare it to stored_stage2
    if (OB_SUCC(ret)) {
      if (OB_FAIL(mysql_sha1_reset_wrap(&sha1_context))) {
        LOG_WARN("failed to reset sha context", K(ret));
      } else if (OB_FAIL(mysql_sha1_input_wrap(&sha1_context, stage1_client, SCRAMBLE_LENGTH))) {
        LOG_WARN("failed to feed stage1_client input", K(ret));
      } else if (OB_FAIL(mysql_sha1_result_wrap(&sha1_context, stage2_client))) {
        LOG_WARN("failed to get stage2_client", K(ret));
      } else {
        pass = (0 == MEMCMP(stage2_client, stored_stage2.ptr(), SHA1_HASH_SIZE));
      }
    }
  }
  return ret;
}

int ObEncryptedHelper::displayable_to_hex(const ObString &displayable, ObString &hex)
{
  int ret = OB_SUCCESS;
  const char * in_buf = displayable.ptr();
  char * out_buf = hex.ptr();
  if (NULL == in_buf || displayable.length() < SHA1_HASH_SIZE * 2) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid displayable string", KP(in_buf), K(displayable.length()), K(ret));
  } else if (NULL == out_buf || hex.length() < SHA1_HASH_SIZE) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid hex buf", KP(in_buf), K(displayable.length()), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < SHA1_HASH_SIZE; ++i) {
      int64_t high = 0;
      int64_t low = 0;
      if (OB_FAIL(char_to_hex(in_buf[i * 2], high))) {
        LOG_WARN("Failed convert high char to hex", K(ret));
      } else if (OB_FAIL(char_to_hex(in_buf[i * 2 + 1], low))) {
        LOG_WARN("Failed convert low char to hex", K(ret));
      } else {
        out_buf[i] = static_cast<char>(high << 4 | low);
      }
    }
  }
  return ret;
}


/*
  Process the next 512 bits of the message stored in the Message_Block array.

  SYNOPSIS
    SHA1ProcessMessageBlock()

   DESCRIPTION
     Many of the variable names in this code, especially the single
     character names, were used because those were the names used in
     the publication.
*/
void ObEncryptedHelper::SHA1ProcessMessageBlock(SHA1_CONTEXT *context)
{
  int   t = 0;       /* Loop counter      */
  uint32_t  temp = 0;      /* Temporary word value    */
  uint32_t  W[80];       /* Word sequence     */
  uint32_t  A = 0, B = 0, C = 0, D = 0, E = 0;     /* Word buffers      */
  int idx = 0;

  /*
    Initialize the first 16 words in the array W
  */

  for (t = 0; t < 16; t++) {
    idx = t * 4;
    W[t] = context->Message_Block[idx] << 24;
    W[t] |= context->Message_Block[idx + 1] << 16;
    W[t] |= context->Message_Block[idx + 2] << 8;
    W[t] |= context->Message_Block[idx + 3];
  }


  for (t = 16; t < 80; t++) {
    W[t] = SHA1CircularShift(1, W[t - 3] ^ W[t - 8] ^ W[t - 14] ^ W[t - 16]);
  }

  A = context->Intermediate_Hash[0];
  B = context->Intermediate_Hash[1];
  C = context->Intermediate_Hash[2];
  D = context->Intermediate_Hash[3];
  E = context->Intermediate_Hash[4];

  for (t = 0; t < 20; t++) {
    temp = SHA1CircularShift(5, A) + ((B & C) | ((~B) & D)) + E + W[t] + K[0];
    E = D;
    D = C;
    C = SHA1CircularShift(30, B);
    B = A;
    A = temp;
  }

  for (t = 20; t < 40; t++) {
    temp = SHA1CircularShift(5, A) + (B ^ C ^ D) + E + W[t] + K[1];
    E = D;
    D = C;
    C = SHA1CircularShift(30, B);
    B = A;
    A = temp;
  }

  for (t = 40; t < 60; t++) {
    temp = (SHA1CircularShift(5, A) + ((B & C) | (B & D) | (C & D)) + E + W[t] +
            K[2]);
    E = D;
    D = C;
    C = SHA1CircularShift(30, B);
    B = A;
    A = temp;
  }

  for (t = 60; t < 80; t++) {
    temp = SHA1CircularShift(5, A) + (B ^ C ^ D) + E + W[t] + K[3];
    E = D;
    D = C;
    C = SHA1CircularShift(30, B);
    B = A;
    A = temp;
  }

  context->Intermediate_Hash[0] += A;
  context->Intermediate_Hash[1] += B;
  context->Intermediate_Hash[2] += C;
  context->Intermediate_Hash[3] += D;
  context->Intermediate_Hash[4] += E;

  context->Message_Block_Index = 0;
}

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
void ObEncryptedHelper::SHA1PadMessage(SHA1_CONTEXT *context)
{
  /*
    Check to see if the current message block is too small to hold
    the initial padding bits and length.  If so, we will pad the
    block, process it, and then continue padding into a second
    block.
  */

  int i = context->Message_Block_Index;

  if (i > 55) {
    context->Message_Block[i++] = 0x80;
    memset((char *) &context->Message_Block[i], 0, sizeof(context->Message_Block[0]) * (64 - i));
    context->Message_Block_Index = 64;

    /* This function sets context->Message_Block_Index to zero  */
    SHA1ProcessMessageBlock(context);

    memset((char *) &context->Message_Block[0], 0, sizeof(context->Message_Block[0]) * 56);
    context->Message_Block_Index = 56;
  } else {
    context->Message_Block[i++] = 0x80;
    memset((char *) &context->Message_Block[i], 0, sizeof(context->Message_Block[0]) * (56 - i));
    context->Message_Block_Index = 56;
  }

  /*
    Store the message length as the last 8 octets
  */

  context->Message_Block[56] = (int8_t)(context->Length >> 56);
  context->Message_Block[57] = (int8_t)(context->Length >> 48);
  context->Message_Block[58] = (int8_t)(context->Length >> 40);
  context->Message_Block[59] = (int8_t)(context->Length >> 32);
  context->Message_Block[60] = (int8_t)(context->Length >> 24);
  context->Message_Block[61] = (int8_t)(context->Length >> 16);
  context->Message_Block[62] = (int8_t)(context->Length >> 8);
  context->Message_Block[63] = (int8_t)(context->Length);

  SHA1ProcessMessageBlock(context);
}
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
int ObEncryptedHelper::mysql_sha1_reset(SHA1_CONTEXT *context)
{
  context->Length     = 0;
  context->Message_Block_Index    = 0;

  context->Intermediate_Hash[0]   = sha_const_key[0];
  context->Intermediate_Hash[1]   = sha_const_key[1];
  context->Intermediate_Hash[2]   = sha_const_key[2];
  context->Intermediate_Hash[3]   = sha_const_key[3];
  context->Intermediate_Hash[4]   = sha_const_key[4];

  context->Computed   = 0;
  context->Corrupted  = 0;

  return SHA_SUCCESS;
}

int ObEncryptedHelper::mysql_sha1_reset_wrap(SHA1_CONTEXT *context)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(context)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("context is null", KP(context), K(ret));
  } else {
    int sha_ret = SHA_SUCCESS;
    if (SHA_SUCCESS != (sha_ret = mysql_sha1_reset(context))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to reset sha", K(sha_ret), K(ret));
    }
  }
  return ret;
}

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

int ObEncryptedHelper::mysql_sha1_result(SHA1_CONTEXT *context,
                                         unsigned char Message_Digest[SHA1_HASH_SIZE])
{
  int i = 0;
  if (!context->Computed) {
    SHA1PadMessage(context);
    /* message may be sensitive, clear it out */
    memset((char *) context->Message_Block, 0, 64);
    context->Length   = 0;    /* and clear length  */
    context->Computed = 1;
  }

  for (i = 0; i < SHA1_HASH_SIZE; i++)
    Message_Digest[i] = (int8_t)((context->Intermediate_Hash[i >> 2] >> 8
                                  * (3 - (i & 0x03))));
  return SHA_SUCCESS;
}

int ObEncryptedHelper::mysql_sha1_result_wrap(SHA1_CONTEXT *context,
                                              unsigned char *message_digest)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(context)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("context is null", KP(context), K(ret));
  } else if (OB_ISNULL(message_digest)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("message_digest is null", KP(message_digest), K(ret));
  } else {
    int sha_ret = SHA_SUCCESS;
    if (SHA_SUCCESS != (sha_ret = mysql_sha1_result(context, message_digest))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get sha result", K(sha_ret), K(ret));
    }
  }
  return ret;
}


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

int ObEncryptedHelper::mysql_sha1_input(SHA1_CONTEXT *context,
                                        const unsigned char *message_array,
                                        unsigned length)
{
  int sha_ret = SHA_SUCCESS;
  if (!length) {
    sha_ret = SHA_SUCCESS;
  } else if (!context || !message_array) {
    /* We assume client knows what it is doing in non-debug mode */
    sha_ret = SHA_NULL;
  } else if (!!context->Computed) {
    sha_ret = (context->Corrupted = SHA_STATE_ERROR);
  } else if (!!context->Corrupted) {
    sha_ret = context->Corrupted;
  } else {
    while (SHA_SUCCESS == sha_ret && length--) {
      context->Message_Block[context->Message_Block_Index++] =
          (*message_array & 0xFF);
      context->Length  += 8;  /* Length is in bits */
      /*
        Then we're not debugging we assume we never will get message longer
        2^64 bits.
      */
      if (context->Length == 0) {
        sha_ret = (context->Corrupted = 1);  /* Message is too long */
      } else {
        if (context->Message_Block_Index == 64) {
          SHA1ProcessMessageBlock(context);
        }
        message_array++;
      }
    }
  }
  return sha_ret;
}


int ObEncryptedHelper::mysql_sha1_input_wrap(SHA1_CONTEXT *context,
                                             const unsigned char *message_array,
                                             const ObString::obstr_size_t length)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(context)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("context is null", KP(context), K(ret));
  } else if (OB_ISNULL(message_array)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("message_array is null", KP(message_array), K(ret));
  } else {
    int sha_ret = SHA_SUCCESS;
    if (SHA_SUCCESS != (sha_ret = mysql_sha1_input(context, message_array, static_cast<uint32_t>(length)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to feed sha input", K(sha_ret), K(ret));
    }
  }
  return ret;
}


int ObEncryptedHelper::my_xor(const unsigned char *s1,
                              const unsigned char *s2,
                              uint32_t len,
                              unsigned char *to)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(s1) || OB_ISNULL(s2) || OB_ISNULL(to)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("input is null", KP(s1), KP(s2));
  } else {
    const unsigned char *s1_end = s1 + len;
    while (s1 < s1_end) {
      *to++ = *s1++ ^ *s2++;
    }
  }
  return ret;
}


int ObEncryptedHelper::char_to_hex(char input, int64_t &out)
{
  int ret = OB_SUCCESS;
  if (!((input >= '0' && input <= '9')
      || (input >= 'a' && input <= 'f')
      || (input >= 'A' && input <= 'F'))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("input not in range", K(input));
  } else {
    input = static_cast<char>(tolower(input));
    if (input >= '0' && input <= '9') {
      out = static_cast<int64_t>(input - '0');
    } else if (input >= 'a' && input <= 'f') {
      out = static_cast<int64_t>(input - 'a' + 10);
    }
  }
  return ret;
}


