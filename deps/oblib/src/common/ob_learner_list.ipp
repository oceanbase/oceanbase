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

#include "ob_learner_list.h"

namespace oceanbase
{

namespace common
{
template <int64_t MAX_SIZE, typename T>
void BaseLearnerList<MAX_SIZE, T>::reset()
{
  learner_array_.reset();
}

template <int64_t MAX_SIZE, typename T>
bool BaseLearnerList<MAX_SIZE, T>::is_valid() const
{
  return learner_array_.count() > 0;
}

template <int64_t MAX_SIZE, typename T>
bool BaseLearnerList<MAX_SIZE, T>::is_full() const
{
  return learner_array_.count() >= MAX_SIZE;
}

// caller should ensure learner is valid
template <int64_t MAX_SIZE, typename T>
int64_t BaseLearnerList<MAX_SIZE, T>::get_index_by_learner(const T &learner) const
{
  int64_t idx = -1;
  int64_t length = learner_array_.count();
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < length; ++i) {
    T tmp;
    if (OB_FAIL(learner_array_.at(i, tmp))) {
      COMMON_LOG(ERROR, "SEArray.at() failed", K(ret), K(learner_array_), K(i));
      break;
    } else if (tmp == learner) {
      idx = i;
      break;
    }
  }
  return idx;
}

template <int64_t MAX_SIZE, typename T>
int64_t BaseLearnerList<MAX_SIZE, T>::get_index_by_addr(const common::ObAddr &server) const
{
  int64_t idx = -1;
  int64_t length = learner_array_.count();
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < length; ++i) {
    T tmp;
    if (OB_FAIL(learner_array_.at(i, tmp))) {
      COMMON_LOG(ERROR, "SEArray.at() failed", K(ret), K(learner_array_), K(i));
      break;
    } else if (tmp.get_server() == server) {
      idx = i;
      break;
    }
  }
  return idx;
}

template <int64_t MAX_SIZE, typename T>
int BaseLearnerList<MAX_SIZE, T>::get_learner_by_addr(const common::ObAddr server, T &learner) const
{
  int ret = OB_SUCCESS;
  const int64_t idx = get_index_by_addr(server);
  if (-1 == idx) {
    ret = OB_ENTRY_NOT_EXIST;
  } else if (OB_FAIL(get_learner(idx, learner))) {
  }
  return ret;
}

template <int64_t MAX_SIZE, typename T>
T &BaseLearnerList<MAX_SIZE, T>::get_learner(const int64_t idx)
{
  if (idx < 0) {
    COMMON_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "get_index_by_addr failed", K(idx));
  }
  return learner_array_[idx];
}

template <int64_t MAX_SIZE, typename T>
bool BaseLearnerList<MAX_SIZE, T>::contains(const T &learner) const
{
  int bool_ret = false;
  if (OB_UNLIKELY(!learner.is_valid())) {
  } else if (-1 == get_index_by_learner(learner)) {
  } else {
    bool_ret = true;
  }
  return bool_ret;
}

template <int64_t MAX_SIZE, typename T>
bool BaseLearnerList<MAX_SIZE, T>::contains(const common::ObAddr &server) const
{
  int bool_ret = false;
  if (OB_UNLIKELY(!server.is_valid())) {
  } else if (-1 == get_index_by_addr(server)) {
  } else {
    bool_ret = true;
  }
  return bool_ret;
}

template <int64_t MAX_SIZE, typename T>
int BaseLearnerList<MAX_SIZE, T>::add_server(const common::ObAddr &server)
{
  int ret = OB_SUCCESS;
  const bool is_member = std::is_same<common::ObMember, T>::value;
  if (false == is_member) {
    ret = OB_NOT_SUPPORTED;
  } else {
    ret = add_learner(common::ObMember(server, OB_INVALID_TIMESTAMP));
  }
  return ret;
}

template <int64_t MAX_SIZE, typename T>
int BaseLearnerList<MAX_SIZE, T>::add_learner(const T &learner)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!learner.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (learner_array_.count() >= MAX_SIZE) {
    ret = OB_SIZE_OVERFLOW;
  } else if (OB_UNLIKELY(contains(learner))) {
    ret = OB_ENTRY_EXIST;
  } else if (OB_FAIL(learner_array_.push_back(learner))) {
    COMMON_LOG(ERROR, "learner_array_ push back failed", K(ret), K(learner));
  } else {
    std::sort(learner_array_.begin(), learner_array_.end(), [](const T &a, const T &b){ return a < b;});
  }
  return ret;
}

template <int64_t MAX_SIZE, typename T>
int BaseLearnerList<MAX_SIZE, T>::remove_learner(const T &learner)
{
  int ret = OB_SUCCESS;
  int64_t idx = -1;
  if (OB_UNLIKELY(!learner.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (-1 == (idx = get_index_by_learner(learner))) {
    ret = OB_ENTRY_NOT_EXIST;
  } else if (OB_FAIL(learner_array_.remove(idx))) {
    COMMON_LOG(ERROR, "learner_array_ remove failed", K(ret), K(idx));
  }
  return ret;
}

template <int64_t MAX_SIZE, typename T>
int BaseLearnerList<MAX_SIZE, T>::remove_learner(const common::ObAddr &server)
{
  int ret = OB_SUCCESS;
  int64_t idx = -1;
  if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (-1 == (idx = get_index_by_addr(server))) {
    ret = OB_ENTRY_NOT_EXIST;
  } else if (OB_FAIL(learner_array_.remove(idx))) {
    COMMON_LOG(ERROR, "learner_array_ remove failed", K(ret), K(idx));
  }
  return ret;
}

template <int64_t MAX_SIZE, typename T>
int64_t BaseLearnerList<MAX_SIZE, T>::get_member_number() const
{
  return learner_array_.count();
}

template <int64_t MAX_SIZE, typename T>
int BaseLearnerList<MAX_SIZE, T>::get_learner(const int64_t idx, T &learner) const
{
  int ret = OB_SUCCESS;
  if (idx < 0 || idx >= learner_array_.count()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(learner_array_.at(idx, learner))) {
    COMMON_LOG(ERROR, "SEArray.at() failed", K_(learner_array), K(idx));
  }
  return ret;
}

template <int64_t MAX_SIZE, typename T>
bool BaseLearnerList<MAX_SIZE, T>::learner_addr_equal(const BaseLearnerList<MAX_SIZE, T> &learner_list) const
{
  bool bool_ret = true;
  const int64_t learner_number = get_member_number();
  if (learner_number != learner_list.get_member_number()) {
    bool_ret = false;
  } else {
    common::ObAddr addr;
    for (int64_t i = 0; true == bool_ret && i < learner_number; ++i) {
      if (!learner_list.contains(learner_array_[i].get_server())) {
        bool_ret = false;
      } else {}
    }
  }
  return bool_ret;
}

template <int64_t MAX_SIZE, typename T>
int BaseLearnerList<MAX_SIZE, T>::get_server_by_index(const int64_t idx, common::ObAddr &addr) const
{
  int ret = OB_SUCCESS;
  T learner;
  if (OB_FAIL(get_learner(idx, learner))) {
  } else {
    addr = learner.get_server();
  }
  return ret;
}

template <int64_t MAX_SIZE, typename T>
int BaseLearnerList<MAX_SIZE, T>::get_member_by_index(const int64_t idx, common::ObMember &member) const
{
  int ret = OB_SUCCESS;
  T learner;
  if (OB_FAIL(get_learner(idx, learner))) {
  } else {
    member = learner;
  }
  return ret;
}

template <int64_t MAX_SIZE, typename T>
BaseLearnerList<MAX_SIZE, T> &BaseLearnerList<MAX_SIZE, T>::operator=(const BaseLearnerList<MAX_SIZE, T> &learner_list)
{
  if (this != &learner_list) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = deep_copy(learner_list))) {
      COMMON_LOG_RET(ERROR, tmp_ret, "deep_copy failed", K(tmp_ret));
    }
  }
  return *this;
}

template <int64_t MAX_SIZE, typename T>
int BaseLearnerList<MAX_SIZE, T>::deep_copy(const BaseLearnerList<MAX_SIZE, T> &learner_list)
{
  reset();
  return append(learner_list);
}

template <int64_t MAX_SIZE, typename T>
int BaseLearnerList<MAX_SIZE, T>::append(const BaseLearnerList<MAX_SIZE, T> &learner_list)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < learner_list.get_member_number(); ++i) {
    T learner;
    if (OB_FAIL(learner_list.get_learner(i, learner))) {
      COMMON_LOG(WARN, "get_learner failed", K(ret), K(i));
    } else if (OB_FAIL(this->add_learner(learner))) {
      COMMON_LOG(WARN, "add_learner failed", K(ret));
    }
  }
  return ret;
}

template <int64_t MAX_SIZE, typename T>
template <int64_t ARG_MAX_SIZE>
int BaseLearnerList<MAX_SIZE, T>::deep_copy_to(BaseLearnerList<ARG_MAX_SIZE, common::ObMember> &learner_list) const
{
  int ret = OB_SUCCESS;
  const int64_t number = get_member_number();
  learner_list.reset();
  for (int64_t idx = 0; idx < number && OB_SUCC(ret); ++idx) {
    common::ObAddr server;
    if (OB_FAIL(get_server_by_index(idx, server))) {
      COMMON_LOG(WARN, "get_server_by_index failed", K(ret), K(idx));
    } else if (OB_FAIL(learner_list.add_learner(ObMember(server, 1)))) {
      COMMON_LOG(WARN, "add_learner failed", K(ret), K(server));
    }
  }
  if (OB_FAIL(ret)) {
    COMMON_LOG(WARN, "BaseLearnerList deep_copy_to failed", K(ret));
  }
  return ret;
}

template <int64_t MAX_SIZE, typename T>
int BaseLearnerList<MAX_SIZE, T>::serialize(SERIAL_PARAMS) const
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_ISNULL(buf) || pos < 0 || pos > buf_len) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(learner_array_.serialize(buf, buf_len, new_pos))) {
        COMMON_LOG(WARN, "serialize learner failed", K(ret));
  } else {
    pos = new_pos;
  }
  return ret;
}

template <int64_t MAX_SIZE, typename T>
int BaseLearnerList<MAX_SIZE, T>::deserialize(DESERIAL_PARAMS)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  int64_t learner_number = 0;
  if (OB_ISNULL(buf) || pos < 0 || pos > data_len) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(learner_array_.deserialize(buf, data_len, new_pos))) {
    COMMON_LOG(WARN, "learner_array_ deserialize failed", K(ret));
  } else {
    pos = new_pos;
  }
  return ret;
}

template <int64_t MAX_SIZE, typename T>
int64_t BaseLearnerList<MAX_SIZE, T>::get_serialize_size() const
{
  int64_t size = 0;
  size += learner_array_.get_serialize_size();
  return size;
}

template <int64_t MAX_SIZE, typename T>
int BaseLearnerList<MAX_SIZE, T>::transform_to_string(
        common::ObSqlString &output_string) const
{
  int ret = OB_SUCCESS;
  output_string.reset();
  if (0 > get_member_number()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), "learner count", get_member_number());
  } else if (0 == get_member_number()) {
    output_string.reset();
  } else {
    bool need_comma = false;
    common::ObMember learner;
    char ip_port[MAX_IP_PORT_LENGTH] = "";
    for (int i = 0; OB_SUCC(ret) && i < get_member_number(); i++) {
      if (OB_FAIL(get_member_by_index(i, learner))) {
        COMMON_LOG(WARN, "failed to get learner from learner list", K(ret), K(i));
      } else if (OB_FAIL(learner.get_server().ip_port_to_string(ip_port, sizeof(ip_port)))) {
        COMMON_LOG(WARN, "convert server to string failed", K(ret), K(learner));
      } else if (need_comma && OB_FAIL(output_string.append(","))) {
        COMMON_LOG(WARN, "failed to append comma to string", K(ret));
      } else if (OB_FAIL(output_string.append_fmt("%.*s:%ld:%ld", static_cast<int>(sizeof(ip_port)), ip_port, learner.get_timestamp(), learner.get_flag()))) {
        COMMON_LOG(WARN, "failed to append ip_port to string", K(ret), K(learner));
      } else {
        need_comma = true;
      }
    }
  }
  return ret;
}
} // namespace common end
} // namespace oceanbase end
