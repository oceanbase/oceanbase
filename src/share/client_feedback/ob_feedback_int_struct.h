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

#ifndef OCEANBASE_SHARE_OB_INT_STRUCT_H_
#define OCEANBASE_SHARE_OB_INT_STRUCT_H_

#include "share/client_feedback/ob_client_feedback_basic.h"

namespace oceanbase
{
namespace share
{

class ObFeedbackIntStruct : public ObAbstractFeedbackObject<ObFeedbackIntStruct>
{
public:
  ObFeedbackIntStruct(ObFeedbackElementType type)
    : ObAbstractFeedbackObject<ObFeedbackIntStruct>(type), int_value_(0) {}
  virtual ~ObFeedbackIntStruct() {}

  void set_value(const int64_t value) { int_value_ = value; }
  int64_t get_value() const { return int_value_; }

  bool operator==(const ObFeedbackIntStruct &other) const
  {
    return ((type_ == other.type_) && (int_value_ == other.int_value_));
  }

  bool operator!=(const ObFeedbackIntStruct &other) const
  {
    return !(*this == other);
  }

  void reset() { int_value_ = 0; }

  FB_OBJ_DEFINE_METHOD;

  TO_STRING_KV("type", get_feedback_element_type_str(type_), K_(int_value));

protected:
  int64_t int_value_;
};

inline bool ObFeedbackIntStruct::is_valid_obj() const
{
  return true;
}

#define INT_FB_STRUCT(name, type) \
class name : public ObFeedbackIntStruct \
{ \
public: \
  name() : ObFeedbackIntStruct(type) {} \
  virtual ~name() {} \
};

enum ObFollowerFirstFeedbackType
{
  FFF_HIT_MIN = 0,
  FFF_HIT_LEADER = 1,   // all related partitions are leaders
  // add others if needed
  FF_HIT_MAX,
};
INT_FB_STRUCT(ObFollowerFirstFeedback, FOLLOWER_FIRST_FB_ELE);

} // end namespace share
} // end namespace oceanbase

#endif // OCEANBASE_SHARE_OB_INT_STRUCT_H_
