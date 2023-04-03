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

#include "lib/encrypt/ob_encrypt.h"
#include "lib/ob_define.h"
#include "lib/oblog/ob_log.h"
#include <crypt.h>
#include <unistd.h>
#include <stdlib.h>
namespace oceanbase
{
namespace common
{
DES_crypt::DES_crypt()
  : original_txt_length_(0)
{
  data_.current_saltbits = 0;
  data_.direction = 0;
  data_.initialized = 0;
}

DES_crypt::~DES_crypt()
{
}

int DES_crypt::stringTobyte(char *dest, const int64_t dest_len, const char *src,
                            const int64_t src_len)
{
  int err = OB_SUCCESS;
  if (dest == NULL || src == NULL || dest_len < src_len * 8) {
    err = OB_INVALID_ARGUMENT;
    _OB_LOG(WARN, "invalid param. dest = %p, src = %p, dest_len = %ld, src_len = %ld", dest, src,
              dest_len, src_len);
  }
  int64_t tmp = 0;
  if (OB_SUCCESS == err) {
    for (int64_t i = 0; i < src_len; i++) {
      int32_t op = OB_OP_MASK;
      for (int64_t j = 0; j < OB_BYTE_NUM; j++) {
        tmp = src[i] & op;
        if (tmp == 0) {
          dest[OB_BYTE_NUM * i + j] = 0;
        } else {
          dest[OB_BYTE_NUM * i + j] = 1;
        }
        op >>= 1;
      }
    }
  }
  return err;
}

int DES_crypt::byteTostring(char *dest, const int64_t dest_len, const char *src,
                            const int64_t src_len)
{
  int err = OB_SUCCESS;
  if (dest == NULL || src == NULL || src_len % OB_BYTE_NUM != 0 || dest_len < src_len / 8) {
    err = OB_INVALID_ARGUMENT;
    _OB_LOG(WARN, "invalid argument. dest=%p, dest_len=%ld, src=%p, src_len=%ld", dest, dest_len, src,
              src_len);
  }
  int32_t op = 0x0000;
  if (OB_SUCCESS == err) {
    for (int64_t i = 0; i < src_len; i++) {
      if (src[i] != 0 && src[i] != 1) {
        err = OB_INVALID_ARGUMENT;
        _OB_LOG(WARN, "invalid argument, char is [%c], but should be 0 or 1", src[i]);
        break;
      } else {
        op |= src[i];
        if (i % OB_BYTE_NUM == 7) {
          dest[(i + 1) / 8 - 1] = static_cast<char>(op);
          op = 0x0000;
        } else {
          op <<= 1;
        }
      }
    }
  }
  return err;
}

int DES_crypt::pad(char *dest, const int64_t dest_len, int64_t &slen)
{
  int err = OB_SUCCESS;
  if (dest == NULL || dest_len < OB_MAX_TOKEN_BUFFER_LENGTH || slen > OB_MAX_TOKEN_BUFFER_LENGTH) {
    err = OB_INVALID_ARGUMENT;
    _OB_LOG(WARN,
              "invalid argument. dest =%p, dest_len = %ld, slen = %ld, OB_MAX_TOKEN_BUFFER_LENGTH =%ld", dest,
              dest_len, slen, OB_MAX_TOKEN_BUFFER_LENGTH);
  }
  if (OB_SUCCESS == err) {
    original_txt_length_ = static_cast<int32_t>(slen);
    for (int64_t i = slen; i < OB_MAX_TOKEN_BUFFER_LENGTH; i++) {
      dest[i] = '$';
    }
    slen = OB_MAX_TOKEN_BUFFER_LENGTH;
  }
  return err;
}

int DES_crypt::unpad(char *src, int64_t &slen)
{
  int err = OB_SUCCESS;
  if (src == NULL || slen != OB_MAX_TOKEN_BUFFER_LENGTH) {
    err = OB_INVALID_ARGUMENT;
    _OB_LOG(WARN, "invalid argument, src =%p, slen = %ld, OB_MAX_TOKEN_BUFFER_LENGTH= %ld", src, slen,
              OB_MAX_TOKEN_BUFFER_LENGTH);
  } else {
    for (int i = original_txt_length_; i < OB_MAX_TOKEN_BUFFER_LENGTH; i++) {
      if (src[i] == '$') {
        src[i] = '\0';
      } else {
        err = OB_ERROR;
        _OB_LOG(WARN, "fail in unpad txt. the char is '%c', but should be '$'", src[i]);
      }
    }
  }
  if (OB_SUCCESS == err) {
    slen = original_txt_length_;
  }
  return err;
}

int DES_crypt::des_encrypt(const ObString &user_name, const int64_t timestamp, const int64_t skey,
                           char *buffer, int64_t &buffer_length)
{
  int err = OB_SUCCESS;
  if (buffer_length <= (int64_t)(sizeof(int64_t) + 2 * sizeof(int32_t) + user_name.length()) ||
      buffer_length % 8 != 0) {
    err = OB_INVALID_ARGUMENT;
    _OB_LOG(WARN, "invalid argument. buffer_length = %ld, should be 80", buffer_length);
  }
  if (OB_SUCCESS == err) {
    if (user_name.length() == 0) {
      err = OB_INVALID_ARGUMENT;
      _OB_LOG(WARN, "user_name should not be null. user_name.length=%d", user_name.length());
    }
  }
  if (OB_SUCCESS == err) {
    data_.initialized = 0;
    err = stringTobyte(keybyte_, OB_ENRYPT_KEY_BYTE, reinterpret_cast<const char *>(&skey),
                       sizeof(int64_t));
    if (err != OB_SUCCESS) {
      _OB_LOG(WARN, "fail to stringTobyte. err=%d", err);
    } else {
      setkey_r(keybyte_, &data_);
    }
  }
  int64_t length = 0;
  if (OB_SUCCESS == err) {
    length = 2 * sizeof(int32_t)  + sizeof(int64_t) + user_name.length();
    original_txt_length_ = static_cast<int32_t>(length);
    err = pad(txtptr_, OB_MAX_TOKEN_BUFFER_LENGTH, length);
    if (OB_SUCCESS != err) {
      _OB_LOG(WARN, "fail to pad array. err=%d", err);
    }
  }
  int64_t pos = 0;
  if (OB_SUCCESS == err) {
    *(reinterpret_cast<int32_t *>(txtptr_ + pos)) = OB_CRYPT_MAGIC_NUM;
    pos += sizeof(int32_t);
    *(reinterpret_cast<int32_t *>(txtptr_ + pos)) = user_name.length();
    pos += sizeof(int32_t);
    MEMCPY(txtptr_ + pos, user_name.ptr(), user_name.length());
    pos += user_name.length();
    *(reinterpret_cast<int64_t *>(txtptr_ + pos)) = timestamp;
    pos += sizeof(int64_t);
  }
  if (OB_SUCCESS == err) {
    err = stringTobyte(txtbyte_, OB_ENCRYPT_TXT_BYTE, txtptr_, length);
    if (err != OB_SUCCESS) {
      _OB_LOG(WARN, "fail to translate string to byte.err=%d", err);
    }
  }
  if (OB_SUCCESS == err) {
    length = length * OB_BYTE_NUM;
    char *ptr = txtbyte_;
    for (int64_t i = 0; i < length; i += OB_CRYPT_UNIT) {
      ptr = &txtbyte_[i];
      encrypt_r(ptr, ENCODE_FLAG, &data_);
    }
  }
  if (OB_SUCCESS == err) {
    err = byteTostring(buffer, buffer_length, txtbyte_, length);
    if (err != OB_SUCCESS) {
      _OB_LOG(WARN, "fail to translate to string.err=%d", err);
    } else {
      buffer_length = length / OB_BYTE_NUM;
    }
  }
  return err;
}

int DES_crypt::des_decrypt(ObString &user_name, int64_t &timestamp, const int64_t skey,
                           char *buffer, int64_t buffer_length)
{
  int err = OB_SUCCESS;
  if (buffer == NULL || buffer_length != OB_MAX_TOKEN_BUFFER_LENGTH) {
    err = OB_INVALID_ARGUMENT;
    _OB_LOG(WARN, "invalid argument.err=%d, buffer=%p,buffer_length =%ld", err, buffer,
              buffer_length);
  }
  if (OB_SUCCESS == err) {
    data_.initialized = 0;
    err = stringTobyte(keybyte_, OB_ENRYPT_KEY_BYTE, reinterpret_cast<const char *>(&skey),
                       sizeof(int64_t));
    if (err != OB_SUCCESS) {
      _OB_LOG(WARN, "fail to stringTobyte. err=%d", err);
    } else {
      setkey_r(keybyte_, &data_);
    }
  }
  if (OB_SUCCESS == err) {
    err = stringTobyte(txtbyte_, OB_ENCRYPT_TXT_BYTE, buffer, buffer_length);
    if (err != OB_SUCCESS) {
      _OB_LOG(WARN, "fail to tranlate to byte. err=%d", err);
    }
  }
  char *ptr = txtbyte_;
  if (OB_SUCCESS == err) {
    for (int64_t i = 0; i < buffer_length * OB_BYTE_NUM; i += OB_CRYPT_UNIT) {
      ptr = &txtbyte_[i];
      encrypt_r(ptr, DECODE_FLAG, &data_);
    }
  }
  if (OB_SUCCESS == err) {
    err = byteTostring(buffer, buffer_length, txtbyte_, buffer_length * 8);
    if (err != OB_SUCCESS) {
      _OB_LOG(WARN, "fail to tranlate to string. err=%d", err);
    }
  }
  if (OB_SUCCESS == err) {
    int64_t pos = 0;
    int32_t magic_number = *(reinterpret_cast<const int32_t *>(buffer + pos));
    if (magic_number != OB_CRYPT_MAGIC_NUM) {
      err = OB_DECRYPT_FAILED;
      _OB_LOG(WARN, "magic number not legal.maybe the wrong skey,or the wrong data, err = %d", err);
    } else {
      pos += sizeof(int32_t);
      int32_t user_name_length = *(reinterpret_cast<const int32_t *>(buffer + pos));
      pos += sizeof(int32_t);
      user_name.assign_ptr(buffer + pos, user_name_length);
      pos += user_name_length;
      timestamp = *(reinterpret_cast<const int64_t *>(buffer + pos));
    }
  }
  return err;
}
}
}

