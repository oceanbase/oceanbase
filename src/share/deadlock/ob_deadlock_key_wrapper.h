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

#ifndef OCEANBASE_SHARE_DEADLOCK_OB_DEADLOCK_KEY_WRAPPER_
#define OCEANBASE_SHARE_DEADLOCK_OB_DEADLOCK_KEY_WRAPPER_
#include "ob_deadlock_parameters.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/utility/serialization.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/utility.h"
#include "lib/oblog/ob_log_module.h"
#include "share/ob_errno.h"
#include "lib/allocator/ob_malloc.h"
#define NEED_DECLARATION
#include "ob_deadlock_key_register.h"
#undef NEED_DECLARATION

namespace oceanbase
{
namespace share
{
namespace detector
{

template <typename T>
class __TypeMapper {};

template <int ID>
class __IDMapper {};

#define REGISTE_TYPE_ID(T, ID) \
template <>\
class __TypeMapper<T> {\
public:\
  static const std::uint64_t id = ID;\
};\
template <>\
class __IDMapper<ID> {\
public:\
  typedef T type;\
};

#define GET_TYPE(ID) __IDMapper<ID>::type
#define GET_ID(T) __TypeMapper<T>::id

#define USER_REGISTER(T, ID) REGISTE_TYPE_ID(T, ID)
#define NEED_REGISTER
#include "ob_deadlock_key_register.h"
#undef NEED_REGISTER
#undef USER_REGISTER
#undef REGISTE_TYPE_ID

class ObDetectorLabelRequest;

class UserBinaryKey
{
  // for serialization
  OB_UNIS_VERSION(1);
public:
  UserBinaryKey();
  UserBinaryKey(const UserBinaryKey &other);
  UserBinaryKey& operator=(const UserBinaryKey &other);
  ~UserBinaryKey();
  void reset();
  template <typename T>
  int set_user_key(const T& user_key);
  bool is_valid() const;
  // for hash
  int compare(const UserBinaryKey &other) const;
  bool operator==(const UserBinaryKey &other) const;
  bool operator<(const UserBinaryKey &other) const;
  bool operator!=(const UserBinaryKey &other) const;
  uint64_t hash() const;
  // for log print
  int64_t to_string(char *buffer, const int64_t length) const;
  struct BufferFactory {
    static int get_buffer(uint64_t buffer_length, char *&p_buffer);
    static void revert_buffer(char* &p_buffer);
    static uint64_t malloc_times;
    static uint64_t free_times;
  };
private:
  uint64_t key_type_id_;
  uint64_t key_binary_code_buffer_length_;
  char key_binary_code_buffer_[BUFFER_LIMIT_SIZE];
};

template<typename T>
int UserBinaryKey::set_user_key(const T &user_key)
{
  #define PRINT_WRAPPER KR(ret), K(user_key), K(length), K(*this)
  int ret = common::OB_SUCCESS;
  int64_t length = 0;

  if (BUFFER_LIMIT_SIZE < user_key.get_serialize_size()) {
    ret = common::OB_BUF_NOT_ENOUGH;
  } else if (OB_FAIL(user_key.serialize(key_binary_code_buffer_,
                                        user_key.get_serialize_size(),
                                        length))) {
    DETECT_LOG(WARN, "user key serialization failed", PRINT_WRAPPER);
  } else {
    key_type_id_ = GET_ID(T);
    key_binary_code_buffer_length_ = user_key.get_serialize_size();
  }

  if (OB_FAIL(ret)) {
    reset();
  }

  return ret;
  #undef PRINT_WRAPPER
}

}// namespace detector
}// namespace share
}// namespace oceanbase

#endif