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

#include "share/ob_occam_time_guard.h"
#include "election_priority_impl.h"
#include "lib/list/ob_dlist.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/ob_errno.h"
#include "lib/utility/serialization.h"
#include "logservice/leader_coordinator/common_define.h"
#include "share/ob_cluster_version.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace logservice
{
using namespace palf;
using namespace palf::election;
namespace coordinator
{

constexpr int64_t serialization_end_flag = -1;

struct SerializeFunctor
{
public:
  SerializeFunctor(char *buf, const int64_t buf_len, int64_t &pos) : buf_(buf), buf_len_(buf_len), pos_(pos) {}
  template <typename T>
  int operator()(const T &element)
  {
    LC_TIME_GUARD(1_s);
    int ret = OB_SUCCESS;
    int64_t size = element.get_serialize_size();
    if (CLICK_FAIL(serialization::encode(buf_, buf_len_, pos_, element.get_started_version()))) {
      COORDINATOR_LOG(ERROR, "encode type id failed", KR(ret), K(MTL_ID()), K(element));
    } else if (CLICK_FAIL(serialization::encode(buf_, buf_len_, pos_, size))) {
      COORDINATOR_LOG(ERROR, "encode size failed", KR(ret), K(MTL_ID()), K(element));
    } else if (CLICK_FAIL(serialization::encode(buf_, buf_len_, pos_, element))) {
      COORDINATOR_LOG(ERROR, "encode element failed", KR(ret), K(MTL_ID()), K(element));
    } else {
      COORDINATOR_LOG(DEBUG, "encode element", KR(ret), K(MTL_ID()), K(element), K(size), K(pos_));
    }
    return ret;
  }
private:
  char *buf_;
  const int64_t buf_len_;
  int64_t &pos_;
};

struct DeSerializeFunctor
{
public:
  DeSerializeFunctor(int64_t decode_version, int64_t decode_size, const char *buf, const int64_t buf_len, int64_t &pos) :
  decode_version_(decode_version), decode_size_(decode_size), buf_(buf),
  buf_len_(buf_len), start_pos_(pos), pos_(pos), decoded_success_(false) {}
  template <typename T>
  int operator()(T &element)
  {
    LC_TIME_GUARD(1_s);
    int ret = OB_SUCCESS;
    if (element.get_started_version() == decode_version_) {
      if (decoded_success_ == true) {
        COORDINATOR_LOG(ERROR, "this buffer has been decodede", KR(ret), K(MTL_ID()), K(element));
      } else if (CLICK_FAIL(serialization::decode(buf_, buf_len_, pos_, element))) {
        COORDINATOR_LOG(ERROR, "decode element failed", KR(ret), K(MTL_ID()), K(element));
      } else if (pos_ - start_pos_ != decode_size_) {
        ret = OB_ERR_UNEXPECTED;
        COORDINATOR_LOG(ERROR, "decode execution unexpected", KR(ret), K(MTL_ID()), K(element));
      } else {
        decoded_success_ = true;
        COORDINATOR_LOG(DEBUG, "decode element", KR(ret), K(MTL_ID()), K(element), K(decode_size_));
      }
    }
    return ret;
  }
  bool is_decoded_success() const { return decoded_success_; }
  int64_t get_started_pos() const { return start_pos_; }
private:
  int64_t decode_version_;
  int64_t decode_size_;
  const char *buf_;
  const int64_t buf_len_;
  int64_t start_pos_;
  int64_t &pos_;
  bool decoded_success_;
};

int ElectionPriorityImpl::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  LC_TIME_GUARD(1_s);
  int ret = OB_SUCCESS;
  if (CLICK_FAIL(priority_tuple_.for_each(SerializeFunctor(buf, buf_len, pos)))) {
    COORDINATOR_LOG(ERROR, "serialize tuple failed", KR(ret), K(MTL_ID()));
  } else if (CLICK_FAIL(serialization::encode(buf, buf_len, pos, serialization_end_flag))) {
    COORDINATOR_LOG(ERROR, "serialize end flag failed", KR(ret), K(MTL_ID()));
  }
  return ret;
}

int ElectionPriorityImpl::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
{
  LC_TIME_GUARD(1_s);
  #define PRINT_WRAPPER KR(ret), K(MTL_ID()), K(type), K(data_size)
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(buf)) {
    while (pos < data_len && OB_SUCC(ret)) {
      int64_t type = -1;
      int64_t data_size = 0;
      if (CLICK_FAIL(serialization::decode(buf, data_len, pos, type))) {
        COORDINATOR_LOG_(ERROR, "decode type id failed");
      }
      if (type == serialization_end_flag) {
        break;
      } else if (CLICK_FAIL(serialization::decode(buf, data_len, pos, data_size))) {
        COORDINATOR_LOG_(ERROR, "decode data size failed");
      } else {
        DeSerializeFunctor functor(type, data_size, buf, data_len, pos);
        if (CLICK_FAIL(priority_tuple_.for_each(functor))) {
          COORDINATOR_LOG_(ERROR, "decode element failed");
        } else if (OB_UNLIKELY(!functor.is_decoded_success())) {
          COORDINATOR_LOG_(INFO, "this type not found in tuple, maybe is a new priority type, just skip the buffer");
          pos = functor.get_started_pos();
          pos += data_size;// 跳过这个对象
        } else {
          COORDINATOR_LOG_(DEBUG, "decode element success");
        }
      }
    }
  }
  if (CLICK_FAIL(ret)) {
    COORDINATOR_LOG(ERROR, "decode priority failed", KR(ret), K(MTL_ID()));
  }
  return ret;
  #undef PRINT_WRAPPER
}

struct GetAllElementSerializeSizeWithExtraInfo
{
  GetAllElementSerializeSizeWithExtraInfo() : total_size_(0) {}
  template <typename T>
  int operator()(const T &element)
  {
    LC_TIME_GUARD(1_s);
    total_size_ += serialization::encoded_length(element.get_started_version());
    total_size_ += serialization::encoded_length(element.get_serialize_size());
    total_size_ += element.get_serialize_size();
    return OB_SUCCESS;
  }
  int64_t get_total_size() const { return total_size_; }
private:
  int64_t total_size_;
};

int64_t ElectionPriorityImpl::get_serialize_size() const
{
  LC_TIME_GUARD(1_s);
  GetAllElementSerializeSizeWithExtraInfo functor;
  (void) priority_tuple_.for_each(functor);
  return functor.get_total_size() + serialization::encoded_length(serialization_end_flag);;
}

struct GetClosestVersionPriority
{
  GetClosestVersionPriority(const uint64_t min_cluster_version) : min_cluster_version_(min_cluster_version), closest_version_(0), closest_priority_(nullptr) {}
  template <typename T>
  int operator()(const T &element)
  {
    LC_TIME_GUARD(1_s);
    if (element.get_started_version() <= min_cluster_version_ && element.get_started_version() > closest_version_) {
      closest_version_ = element.get_started_version();
      closest_priority_ = &element;
    }
    return OB_SUCCESS;
  }
  const AbstractPriority *get_closest_priority() { return closest_priority_; }
private:
  const uint64_t min_cluster_version_;
  uint64_t closest_version_;
  const AbstractPriority *closest_priority_;
};

int ElectionPriorityImpl::compare_with(const ElectionPriority &rhs,
                                       const uint64_t compare_version,
                                       const bool decentralized_voting,
                                       int &result,
                                       ObStringHolder &reason) const
{
  LC_TIME_GUARD(1_s);
  int ret = OB_SUCCESS;
  // 这里如果转型失败直接抛异常，但设计上转型不会失败
  const ElectionPriorityImpl &rhs_impl = dynamic_cast<const ElectionPriorityImpl &>(rhs);
  GetClosestVersionPriority functor1(compare_version);
  GetClosestVersionPriority functor2(compare_version);
  (void) priority_tuple_.for_each(functor1);
  (void) rhs_impl.priority_tuple_.for_each(functor2);
  if (functor1.get_closest_priority() == nullptr || functor2.get_closest_priority() == nullptr) {
    ret = OB_ERR_UNEXPECTED;
    COORDINATOR_LOG(ERROR, "can't get closest priority from tuple", KR(ret), K(MTL_ID()), K(*this), K(rhs), K(compare_version));
  } else if (!functor1.get_closest_priority()->is_valid() && !functor2.get_closest_priority()->is_valid()) {
    result = 0;
    COORDINATOR_LOG(WARN, "compare between invalid priority");
  } else if (functor1.get_closest_priority()->is_valid() && !functor2.get_closest_priority()->is_valid()) {
    result = decentralized_voting ? 1 : 0;
    (void) reason.assign("compare with invalid rhs priority");
    COORDINATOR_LOG(WARN, "rhs priority is invalid", KR(ret), K(MTL_ID()), K(*this), K(rhs), K(compare_version), K(result), K(reason));
  } else if (!functor1.get_closest_priority()->is_valid() && functor2.get_closest_priority()->is_valid()) {
    result = decentralized_voting ? -1 : 0;
    (void) reason.assign("compare with invalid lhs priority");
    COORDINATOR_LOG(WARN, "lhs priority is invalid", KR(ret), K(MTL_ID()), K(*this), K(rhs), K(compare_version), K(result), K(reason));
  } else if (CLICK_FAIL(functor1.get_closest_priority()->compare(*functor2.get_closest_priority(), result, reason))) {
    COORDINATOR_LOG(ERROR, "compare priority failed", KR(ret), K(MTL_ID()), K(*this), K(rhs), K(compare_version));
  } else {
    COORDINATOR_LOG(TRACE, "compare priority success", KR(ret), K(MTL_ID()), K(*this), K(rhs), K(compare_version), K(result), K(reason));
  }
  return ret;
}

int ElectionPriorityImpl::get_size_of_impl_type() const
{
  return sizeof(ElectionPriorityImpl);
}

void ElectionPriorityImpl::placement_new_impl(void *ptr) const
{
  new(ptr) ElectionPriorityImpl();
}

struct RefeshPriority
{
  RefeshPriority(int &ret, const share::ObLSID &ls_id) : ret_(ret), ls_id_(ls_id) {}
  template <typename T>
  int operator()(T &element)
  {
    LC_TIME_GUARD(1_s);
    int ret = OB_SUCCESS;
    if (CLICK_FAIL(element.refresh(ls_id_))) {
      if (OB_NO_NEED_UPDATE == ret) {
        ret = OB_SUCCESS;
      } else {
        ret_ = ret;
        COORDINATOR_LOG(WARN, "refresh priority failed", KR(ret), K(MTL_ID()), K(ls_id_), K(element));
      }
    }
    return ret;
  }
private:
  int &ret_;
  const share::ObLSID ls_id_;
};

int ElectionPriorityImpl::refresh()
{
  LC_TIME_GUARD(1_s);
  int ret = OB_SUCCESS;
  RefeshPriority functor(ret, ls_id_);
  if (CLICK_FAIL(priority_tuple_.for_each(functor))) {
    COORDINATOR_LOG(WARN, "refresh priority failed", KR(ret), K(MTL_ID()), K_(ls_id), K(*this));
  }
  return ret;
}

void ElectionPriorityImpl::set_ls_id(const share::ObLSID ls_id)
{
  ls_id_ = ls_id;
}

int64_t ElectionPriorityImpl::to_string(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  GetClosestVersionPriority functor(GET_MIN_CLUSTER_VERSION());
  (void) priority_tuple_.for_each(functor);
  if (functor.get_closest_priority() == nullptr) {
    ret = OB_ERR_UNEXPECTED;
    COORDINATOR_LOG(ERROR, "can't get closest priority from tuple", KR(ret), K(MTL_ID()), K(*this), K(GET_MIN_CLUSTER_VERSION()));
  } else {
    databuff_printf(buf, buf_len, pos, "{priority:%s}", common::to_cstring(*functor.get_closest_priority()));
  }
  return pos;
}

bool ElectionPriorityImpl::has_fatal_failure() const
{
  bool ret = false;
  GetClosestVersionPriority functor(GET_MIN_CLUSTER_VERSION());
  (void) priority_tuple_.for_each(functor);
  if (functor.get_closest_priority() == nullptr) {
    ret = OB_ERR_UNEXPECTED;
    COORDINATOR_LOG(ERROR, "can't get closest priority from tuple", K(ret), K(MTL_ID()), K(*this));
  } else {
    ret = functor.get_closest_priority()->has_fatal_failure();
  }
  return ret;
}

}
}
}