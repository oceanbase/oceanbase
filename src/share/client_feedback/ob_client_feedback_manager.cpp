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

#define USING_LOG_PREFIX SHARE

#include "share/client_feedback/ob_client_feedback_manager.h"
#include "share/partition_table/ob_partition_location.h"

namespace oceanbase
{
namespace share
{
using namespace common;

int ObFeedbackManager::serialize(char *buf, const int64_t len, int64_t &pos) const
{
  OB_FB_SER_START;
  SERI_FEEDBACK_OBJ(follower_first_feedback_);
  SERI_FEEDBACK_OBJ(pl_feedback_);
  OB_FB_SER_END;
}

int ObFeedbackManager::deserialize(char *buf, const int64_t len, int64_t &pos)
{
  OB_FB_DESER_START;
  reset();
  while ((pos < len) && OB_SUCC(ret)) {
    int64_t type = 0;
    int64_t tmp_pos = pos; // pos before read type
    // read type
    OB_FB_DECODE_INT(type, int64_t);

    if (OB_SUCC(ret)) {
      if (!is_valid_fb_element_type(type)) {
        int64_t struct_len = 0;
        OB_FB_DECODE_INT(struct_len, int64_t);
        LOG_INFO("unrecoginse type", K(type), K(struct_len));
        if (OB_SUCC(ret)) {
          pos += struct_len;
        }
      } else {
        pos = tmp_pos; // recover the pos before type
        switch (type) {
          case PARTITION_LOCATION_FB_ELE: {
            if (OB_FAIL(deserialize_feedback_obj(buf, len, pos, pl_feedback_))) {
              LOG_ERROR("fail to deserialize pl", K(ret));
            }
            break;
          }
          case FOLLOWER_FIRST_FB_ELE: {
            if (OB_FAIL(deserialize_feedback_obj(buf, len, pos, follower_first_feedback_))) {
              LOG_ERROR("fail to deserialize follower first feedback", K(ret));
            }
            break;
          }
          default: {
             ret = OB_ERR_UNEXPECTED;
             LOG_ERROR("invalid type", K(type), K(ret));
          }
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    reset();
  }
  OB_FB_DESER_END;
}

int ObFeedbackManager::add_partition_fb_info(const ObFBPartitionParam &param)
{
  INIT_SUCC(ret);
  ObFeedbackPartitionLocation *fb_pl = NULL;

  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(param), K(ret));
  } else {
    if (NULL != pl_feedback_) {
      pl_feedback_->reset();
      fb_pl = pl_feedback_;
    } else {
      if (OB_FAIL(alloc_feedback_element(fb_pl))) {
        LOG_WARN("fail to alloc feedback partition location", K(ret));
      } else if (OB_ISNULL(fb_pl)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid fb_pl", K(fb_pl), K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    fb_pl->set_table_id(param.pl_.get_table_id());
    fb_pl->set_partition_id(param.original_partition_id_);
    fb_pl->set_schema_version(param.schema_version_);
    const ObIArray<ObReplicaLocation> &rl_array = param.pl_.get_replica_locations();
    ObFeedbackReplicaLocation fb_rl;
    for (int64_t i = 0; OB_SUCC(ret) && (i < rl_array.count()); ++i) {
      fb_rl.reset();
      const ObReplicaLocation &rl = rl_array.at(i);
      fb_rl.server_ = rl.server_;
      fb_rl.role_ = rl.role_;
      fb_rl.replica_type_ = rl.replica_type_;
      if (!fb_rl.is_valid_obj()) {
        LOG_WARN("invalid feedback replica", K(fb_rl));
        continue;
      } else {
        // only care FULL or READONLY replica
        if ((REPLICA_TYPE_FULL == rl.replica_type_) || (REPLICA_TYPE_READONLY == rl.replica_type_)) {
          if (OB_FAIL(fb_pl->add_replica(fb_rl))) {
            LOG_WARN("fail to add replica", K(fb_rl), K(ret));
          }
        } else {
          continue;
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(!fb_pl->is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid partition location", KPC(fb_pl), K(ret));
      } else if (OB_UNLIKELY(fb_pl->get_replica_count() <= 0)) {
        // no available replica, don't add, and return succ
        free_feedback_element(fb_pl);
        fb_pl = NULL;
      }
    }
  }

  if (OB_FAIL(ret) && (NULL != fb_pl)) {
    free_feedback_element(fb_pl);
    fb_pl = NULL;
  }

  pl_feedback_ = fb_pl; // no matter succ or not
  return ret;
}

int ObFeedbackManager::add_follower_first_fb_info(const ObFollowerFirstFeedbackType type)
{
  INIT_SUCC(ret);
  if (type <= FFF_HIT_MIN || type >= FF_HIT_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid type", K(type), K(ret));
  } else {
    ObFollowerFirstFeedback *fff = NULL;
    if (NULL != follower_first_feedback_) {
      follower_first_feedback_->reset();
      fff = follower_first_feedback_;
    } else {
      if (OB_FAIL(alloc_feedback_element(fff))) {
        LOG_WARN("fail to alloc follower first feedback", K(ret));
      } else if (OB_ISNULL(fff)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid fff", K(fff), K(ret));
      }
    }

    if ((NULL != fff) && OB_SUCC(ret)) {
      fff->set_value(static_cast<int64_t>(type));
    }

    if (OB_FAIL(ret) && (NULL != fff)) {
      free_feedback_element(fff);
      fff = NULL;
    }

    follower_first_feedback_ = fff; // no matter succ or not
  }
  return ret;
}

} // end namespace share
} // end namespace oceanbase
