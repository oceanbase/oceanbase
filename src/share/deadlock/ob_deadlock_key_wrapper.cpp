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

#include "ob_deadlock_key_wrapper.h"
#include "ob_deadlock_parameters.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/utility/serialization.h"
#define NEED_DEFINE
#include "ob_deadlock_key_register.h"
#undef NEED_DEFINE

namespace oceanbase
{
namespace share
{
namespace detector
{

extern const char * MEMORY_LABEL;

using namespace common;

uint64_t UserBinaryKey::BufferFactory::malloc_times = 0;
uint64_t UserBinaryKey::BufferFactory::free_times = 0;

int UserBinaryKey::BufferFactory::get_buffer(uint64_t buffer_length, char *&p_buffer)
{
  int ret = OB_SUCCESS;

  if (nullptr != p_buffer) {
    ret = OB_INVALID_ARGUMENT;
    DETECT_LOG(WARN, "p_buffer is not null", KP(p_buffer));
  } else if (nullptr == (p_buffer = (char*)common::ob_malloc(buffer_length, MEMORY_LABEL))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    DETECT_LOG(WARN, "malloc memory failed", KP(p_buffer));
  } else {
    ATOMIC_AAF(&malloc_times, 1);
  }

  return ret;
}

void UserBinaryKey::BufferFactory::revert_buffer(char *&p_buffer)
{
  ob_free(p_buffer);
  p_buffer = nullptr;
  ATOMIC_AAF(&free_times, 1);
}

void UserBinaryKey::reset()
{
  // if (nullptr != key_binary_code_buffer_) {
  //   BufferFactory::revert_buffer(key_binary_code_buffer_);
  //   key_binary_code_buffer_ = nullptr;
  // }
  key_type_id_ = INVALID_VALUE;
  key_binary_code_buffer_length_ = 0;
}

UserBinaryKey::UserBinaryKey() :
  key_type_id_(INVALID_VALUE),
  key_binary_code_buffer_length_(0)
{
  // do nothing
}

UserBinaryKey::UserBinaryKey(const UserBinaryKey &other) :
  key_type_id_(INVALID_VALUE),
  key_binary_code_buffer_length_(0)
{
  this->operator=(other);
}

UserBinaryKey::~UserBinaryKey()
{
  #define PRINT_WRAPPER K(*this)
  reset();
  #undef PRINT_WRAPPER
}

bool UserBinaryKey::is_valid() const
{
  return INVALID_VALUE != key_type_id_ &&
         BUFFER_LIMIT_SIZE >= key_binary_code_buffer_length_ &&
         0 != key_binary_code_buffer_length_;
}

int64_t UserBinaryKey::to_string(char *buffer, const int64_t length) const
{
  int64_t used_length = 0;
  int64_t pos = 0;

  switch (key_type_id_) {
    #define USER_REGISTER(T, ID) \
    case ID:\
    {\
      GET_TYPE(ID) key;\
      if (OB_SUCCESS != key.deserialize(key_binary_code_buffer_,\
                                        key_binary_code_buffer_length_, pos)) {\
        DETECT_LOG_RET(WARN, common::OB_ERR_UNEXPECTED, "key deserilalize failed", KP_(key_type_id));\
      } else {\
        used_length = key.to_string(buffer, length);\
      }\
    }\
    break;
    #define NEED_REGISTER
    #include "ob_deadlock_key_register.h"
    #undef NEED_REGISTER
    #undef USER_REGISTER
  default:
    static const char *err_str = "invalid key";
    const int64_t err_str_len = strlen(err_str);
    if (length >= err_str_len) {
      memcpy(buffer, err_str, err_str_len);
      used_length = err_str_len;
    }
    break;
  }

  return used_length;
}

int UserBinaryKey::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  #define PRINT_WRAPPER KR(ret), K(start_pos), K(pos), K(*this)
  int ret = common::OB_SUCCESS;
  int64_t start_pos = pos;

  if (OB_FAIL(common::serialization::encode(buf, buf_len, pos, key_type_id_))) {
    // do nothing
  } else if (OB_FAIL(common::serialization::encode(buf,
                                                   buf_len,
                                                   pos,
                                                   key_binary_code_buffer_length_))) {
    // do nothing
  } else if (OB_SUCCESS != (ret = common::serialization::encode<char, BUFFER_LIMIT_SIZE>
                           (buf, buf_len, pos, key_binary_code_buffer_))) {
    // do nothing
  } else {
    // do nothing
  }

  // roll back path
  if (OB_FAIL(ret)) {
    DETECT_LOG(WARN, "serialization failed", PRINT_WRAPPER);
    pos = start_pos;
  } else {
    // do nothing
  }

  return ret;
  #undef PRINT_WRAPPER
}

int UserBinaryKey::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  #define PRINT_WRAPPER KR(ret), K(pos), K(*this)
  int ret = common::OB_SUCCESS;

  if (OB_FAIL(common::serialization::decode(buf, data_len, pos, key_type_id_))) {
    // do nothing
  } else if (OB_FAIL(common::serialization::decode(buf,
                                                   data_len,
                                                   pos,
                                                   key_binary_code_buffer_length_))) {
    // do nothing
  } else if (OB_SUCCESS != (ret = common::serialization::decode<char, BUFFER_LIMIT_SIZE>
                           (buf, data_len, pos, key_binary_code_buffer_))) {
    // do nothing
  } else {
    // do nothing
  }

  // roll back path
  if (OB_FAIL(ret)) {
    DETECT_LOG(WARN, "serialization failed", PRINT_WRAPPER);
  } else {
    // do nothing
  }

  return ret;
  #undef PRINT_WRAPPER
}

int64_t UserBinaryKey::get_serialize_size(void) const
{
  return common::serialization::encoded_length(key_type_id_) +
         common::serialization::encoded_length(key_binary_code_buffer_length_) +
         common::serialization::encoded_length<char, BUFFER_LIMIT_SIZE>(key_binary_code_buffer_);
}

UserBinaryKey& UserBinaryKey::operator=(const UserBinaryKey &other)
{
  #define PRINT_WRAPPER KR(ret), K(*this), K(other)

  if (this != &other) {
    memcpy(key_binary_code_buffer_,
           other.key_binary_code_buffer_,
           other.key_binary_code_buffer_length_);
    key_binary_code_buffer_length_ = other.key_binary_code_buffer_length_;
    key_type_id_ = other.key_type_id_;
  }

  return *this;
  #undef PRINT_WRAPPER
}

int UserBinaryKey::compare(const UserBinaryKey &other) const
{
  int ret = 0;

  if (this != &other) {
    if (key_type_id_ > other.key_type_id_) {
      ret = 1;
    } else if (key_type_id_ < other.key_type_id_) {
      ret = -1;
    } else {
      if (key_binary_code_buffer_length_ > other.key_binary_code_buffer_length_) {
        ret = 1;
      } else if (key_binary_code_buffer_length_ < other.key_binary_code_buffer_length_) {
        ret = -1;
      } else {
        if (key_binary_code_buffer_length_ > BUFFER_LIMIT_SIZE) {
          DETECT_LOG(ERROR, "key_binary_code_buffer_length_ over buffer length limit",
                     K_(key_binary_code_buffer_length), K(BUFFER_LIMIT_SIZE));
        }
        for (uint64_t i = 0; i < key_binary_code_buffer_length_; ++i) {
          if (key_binary_code_buffer_[i] > other.key_binary_code_buffer_[i]) {
            ret = 1;
            break;
          } else if (key_binary_code_buffer_[i] < other.key_binary_code_buffer_[i]) {
            ret = -1;
            break;
          } else {
            continue;
          }
        }
      }
    }
  }

  return ret;
}

bool UserBinaryKey::operator==(const UserBinaryKey &other) const
{
  return 0 == compare(other);
}

bool UserBinaryKey::operator!=(const UserBinaryKey &other) const
{
  return 0 != compare(other);
}

bool UserBinaryKey::operator<(const UserBinaryKey &other) const
{
  return compare(other) < 0 ? true : false;
}

uint64_t UserBinaryKey::hash() const
{
  uint64_t hash_val = 0;

  if (is_valid()) {
    hash_val = common::murmurhash(&key_type_id_, sizeof(key_type_id_), hash_val);
    hash_val = common::murmurhash(&key_binary_code_buffer_length_,
                                  sizeof(key_binary_code_buffer_length_),
                                  hash_val);
    hash_val = common::murmurhash(&key_binary_code_buffer_,
                                  static_cast<int32_t>(key_binary_code_buffer_length_),
                                  hash_val);
  }

  return hash_val;
}

}// namespace detector
}// namespace share
}// namespace oceanbase
