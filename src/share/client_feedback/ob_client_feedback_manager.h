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

#ifndef OCEANBASE_SHARE_OB_CLIENT_FEEDBACK_MANAGER_H_
#define OCEANBASE_SHARE_OB_CLIENT_FEEDBACK_MANAGER_H_

#include "lib/container/ob_se_array.h"
#include "share/client_feedback/ob_client_feedback_basic.h"
#include "share/client_feedback/ob_feedback_int_struct.h"
#include "share/client_feedback/ob_feedback_partition_struct.h"
#include "share/partition_table/ob_partition_location.h"

namespace oceanbase
{
namespace share
{
struct ObReplicaLocation;

#define SERI_FEEDBACK_OBJ(obj) \
do { \
  if (OB_SUCC(ret) && (NULL != obj)) { \
    ret = obj->serialize(buf, len, pos); \
  } \
} while(0)

struct ObFBPartitionParam
{
public:
  int64_t schema_version_;
  ObPartitionLocation pl_;
  int64_t original_partition_id_;

  ObFBPartitionParam()
    : schema_version_(0), pl_(), original_partition_id_(common::OB_INVALID_INDEX)
  {}

  ~ObFBPartitionParam() {}

  bool is_valid() const {
    return (pl_.is_valid() && (OB_INVALID_INDEX != original_partition_id_));
  }

  void reset() {
    schema_version_ = 0;
    pl_.reset();
    original_partition_id_ = common::OB_INVALID_INDEX;
  }

  TO_STRING_KV(K_(schema_version), K_(original_partition_id), K_(pl));
};

// C/S feedback protocol:
//
class ObFeedbackManager
{
public:
  ObFeedbackManager()
    : follower_first_feedback_(NULL), pl_feedback_(NULL),
    arena_(common::ObModIds::SHARE_CS_FEEDBACKS) {}
  virtual ~ObFeedbackManager() { reset(); }

  void reset()
  {
    free_feedback_element(pl_feedback_);
    pl_feedback_ = NULL;
    free_feedback_element(follower_first_feedback_);
    follower_first_feedback_ = NULL;
    arena_.reset();
  }

  int serialize(char *buf, const int64_t len, int64_t &pos) const;
  int deserialize(char *buf, const int64_t len, int64_t &pos);

  int add_partition_fb_info(const ObFBPartitionParam &param);
  int add_follower_first_fb_info(const ObFollowerFirstFeedbackType type);

  const ObFollowerFirstFeedback *get_follower_first_feedback() { return follower_first_feedback_; }
  const ObFeedbackPartitionLocation *get_pl_feedback() { return pl_feedback_; }

  bool is_empty() const { return (NULL == follower_first_feedback_) && (NULL == pl_feedback_); }

  template<typename T>
  int alloc_feedback_element(T *&obj) {
    INIT_SUCC(ret);
    char *tmp_buf = NULL;
    if (OB_ISNULL(tmp_buf = reinterpret_cast<char *>(arena_.alloc(sizeof(T))))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      SHARE_LOG(ERROR, "fail to alloc memory", "size", sizeof(T), K(ret));
    } else {
      obj = new (tmp_buf) T();
    }
    return ret;
  }

  template<typename T>
  void free_feedback_element(T *fb_obj) {
    if (NULL != fb_obj) {
      fb_obj->~T();
      arena_.free((void *)fb_obj);
      fb_obj = NULL;
    }
  }

  template<typename T>
  int deserialize_feedback_obj(char *buf, const int64_t len, int64_t &pos, T *&obj) {
    INIT_SUCC(ret);
    if (NULL != obj) {
      obj->reset(); // override the previous one
    } else if (OB_FAIL(alloc_feedback_element(obj))) {
      SHARE_LOG(ERROR, "fail to alloc fd ele", K(ret));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(obj->deserialize(buf, len, pos))) {
        SHARE_LOG(ERROR, "fail to deserizlize", KP(buf), K(len), K(pos), K(ret));
      }
    }

    if (OB_FAIL(ret) && (NULL != obj)) {
      free_feedback_element(obj);
      obj = NULL;
    }
    return ret;
  }

private:
  ObFollowerFirstFeedback *follower_first_feedback_;
  ObFeedbackPartitionLocation *pl_feedback_;
  common::ObArenaAllocator arena_; // use to alloc feedback element

  DISALLOW_COPY_AND_ASSIGN(ObFeedbackManager);
};

} // end namespace share
} // end namespace oceanbase

#endif // OCEANBASE_SHARE_OB_CLIENT_FEEDBACK_MANAGER_H_
