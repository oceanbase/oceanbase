/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "share/detect/ob_detect_rpc_proxy.h"

namespace oceanbase {
namespace obrpc {

OB_SERIALIZE_MEMBER(TaskInfo, task_id_, task_state_);
OB_SERIALIZE_MEMBER(ObTaskStateDetectResp, task_infos_);

int ObTaskStateDetectReq::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  int64_t peer_id_size = peer_ids_.size();

  if (NULL == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "[DM] invalid arguments.", KP(buf), K(buf_len), K(ret));
  } else if (OB_FAIL(serialization::encode_vi64(
      buf, buf_len, pos, peer_id_size))) {
      COMMON_LOG(WARN, "[DM] serialize peer_id_size failed.",
                KP(buf), K(buf_len), K(pos), K(peer_id_size), K(ret));
  } else {
    FOREACH_X(iter, peer_ids_, OB_SUCC(ret)) {
      const ObDetectableId &peer_id = iter->first;
      if (OB_FAIL(peer_id.serialize(buf, buf_len, pos))) {
        COMMON_LOG(WARN, "[DM] serialize peer_id failed.",
                KP(buf), K(buf_len), K(pos), K(peer_id), K(ret));
      }
    }
  }
  return ret;
}

int ObTaskStateDetectReq::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t peer_id_size = 0;
  if (NULL == buf || data_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "[DM] invalid arguments.", KP(buf), K(data_len), K(ret));
  } else if (serialization::decode_vi64(
      buf, data_len, pos, reinterpret_cast<int64_t *>(&peer_id_size))) {
    COMMON_LOG(WARN, "[DM] deserialize peer_id_size failed.",
               KP(buf), K(data_len), K(pos), K(peer_id_size), K(ret));
  } else {
    for (int64_t i = 0; i < peer_id_size && OB_SUCC(ret); ++i) {
      ObDetectableId peer_id;
      if (OB_FAIL(peer_id.deserialize(buf, data_len, pos))) {
        COMMON_LOG(WARN, "[DM] deserialize peer_id failed.",
                  KP(buf), K(data_len), K(pos), K(peer_id), K(ret));
      } else {
        if (OB_FAIL(uniq_ids_.push_back(peer_id))) {
          COMMON_LOG(WARN, "[DM] failed to push back to uniq_ids_", K(ret));
        }
      }
    }
  }
  return ret;
}

int64_t ObTaskStateDetectReq::get_serialize_size(void) const
{
  int64_t total_size = 0;
  int64_t peer_id_size = peer_ids_.size();
  total_size += serialization::encoded_length_vi64(peer_id_size);
  FOREACH(iter, peer_ids_) {
    const ObDetectableId &peer_id = iter->first;
    total_size += peer_id.get_serialize_size();
  }
  int ret = OB_SUCCESS;
  return total_size;
}

int ObTaskStateDetectAsyncCB::process()
{
  ObThreadCondGuard guard(cond_);
  int ret = OB_SUCCESS;
  if (OB_FAIL(rpc_status_->response_.assign(result_))) {
    COMMON_LOG(WARN, "[DM] failed to assign", K(ret), K_(dst));
  }
  rpc_status_->set_processed(true);
  ret = cond_.broadcast();
  return ret;
}

void ObTaskStateDetectAsyncCB::on_timeout()
{
  ObThreadCondGuard guard(cond_);
  int ret = OB_SUCCESS;
  COMMON_LOG(WARN, "[DM] detect async rpc timeout", K_(dst));
  rpc_status_->set_is_timeout(true);
  ret = cond_.broadcast();
}

} // end namespace obrpc
} // end namespace oceanbase
