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

#define USING_LOG_PREFIX RPC_OBRPC
#include "rpc/obrpc/ob_rpc_packet.h"
#include "lib/utility/utility.h"
#include "lib/ob_define.h"
#include "lib/coro/co_var.h"
#include "common/storage/ob_sequence.h"
#include "common/ob_tenant_data_version_mgr.h"
#include "rpc/obrpc/ob_rpc_net_handler.h"
#include "share/ob_cluster_version.h"

using namespace oceanbase::common::serialization;
using namespace oceanbase::common;

namespace oceanbase
{

namespace obrpc
{

ObRpcPacketSet ObRpcPacketSet::instance_;

int ObRpcPacketHeader::serialize(char* buf, const int64_t buf_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (buf_len - pos >= get_encoded_size()) {
    seq_no_ = ObSequence::get_max_seq_no();
    data_version_ = 0;
    if (ObRpcNetHandler::is_self_cluster(dst_cluster_id_) &&
        ODV_MGR.is_enable_compatible_monotonic() && tenant_id_ > 0) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(ODV_MGR.get(tenant_id_, data_version_))) {
        data_version_ = LAST_BARRIER_DATA_VERSION;
        LOG_WARN("fail to get data_version", K(tmp_ret), K(tenant_id_));
      }
    }
    LOG_TRACE("rpc send info", K_(tenant_id), K_(data_version), K_(seq_no), K_(dst_cluster_id),
              K_(pcode), K(ObRpcNetHandler::CLUSTER_ID));
    if (OB_FAIL(encode_i32(buf, buf_len, pos, pcode_))) {
      LOG_WARN("Encode error", K(ret), KP(buf), K(buf_len), K(pos));
    } else if (OB_FAIL(encode_i8(buf, buf_len, pos, static_cast<int8_t> (get_encoded_size())))) {
      LOG_WARN("Encode error", K(ret), KP(buf), K(buf_len), K(pos));
    } else if (OB_FAIL(encode_i8(buf, buf_len, pos, priority_))) {
      LOG_WARN("Encode error", K(ret), KP(buf), K(buf_len), K(pos));
    } else if (OB_FAIL(encode_i16(buf, buf_len, pos, flags_))) {
      LOG_WARN("Encode error", K(ret), KP(buf), K(buf_len), K(pos));
    } else if (OB_FAIL(encode_i64(buf, buf_len, pos, checksum_))) {
      LOG_WARN("Encode error", K(ret), KP(buf), K(buf_len), K(pos));
    } else if (OB_FAIL(encode_i64(buf, buf_len, pos, tenant_id_))) {
      LOG_WARN("Encode error", K(ret), KP(buf), K(buf_len), K(pos));
    } else if (OB_FAIL(encode_i64(buf, buf_len, pos, priv_tenant_id_))) {
      LOG_WARN("Encode error", K(ret), KP(buf), K(buf_len), K(pos));
    } else if (OB_FAIL(encode_i64(buf, buf_len, pos, session_id_))) {
      LOG_WARN("Encode error", K(ret), KP(buf), K(buf_len), K(pos));
    } else if (OB_FAIL(encode_i64(buf, buf_len, pos, trace_id_[0]))) {
      LOG_WARN("Encode error", K(ret), KP(buf), K(buf_len), K(pos));
    } else if (OB_FAIL(encode_i64(buf, buf_len, pos, trace_id_[1]))) {
      LOG_WARN("Encode error", K(ret), KP(buf), K(buf_len), K(pos));
    } else if (OB_FAIL(encode_i64(buf, buf_len, pos, timeout_))) {
      LOG_WARN("Encode error", K(ret), KP(buf), K(buf_len), K(pos));
    } else if (OB_FAIL(encode_i64(buf, buf_len, pos, timestamp_))) {
      LOG_WARN("Encode error", K(ret), KP(buf), K(buf_len), K(pos));
    } else if (OB_FAIL(cost_time_.serialize(buf, buf_len, pos))) {
      LOG_WARN("Encode error", K(ret), KP(buf), K(buf_len), K(pos));
    } else if (OB_FAIL(encode_i64(buf, buf_len, pos, dst_cluster_id_))) {
      LOG_WARN("Encode error", K(ret), KP(buf), K(buf_len), K(pos));
    } else if (OB_FAIL(encode_i32(buf, buf_len, pos, compressor_type_))) {
      LOG_WARN("Encode error", K(ret), KP(buf), K(buf_len), K(pos));
    } else if (OB_FAIL(encode_i32(buf, buf_len, pos, original_len_))) {
      LOG_WARN("Encode error", K(ret), KP(buf), K(buf_len), K(pos));
    } else if (OB_FAIL(encode_i64(buf, buf_len, pos, src_cluster_id_))) {
      LOG_WARN("Encode error", K(ret), KP(buf), K(buf_len), K(pos));
    } else if (OB_FAIL(encode_i64(buf, buf_len, pos, unis_version_))) {
      LOG_WARN("Encode error", K(ret), KP(buf), K(buf_len), K(pos));
    } else if (OB_FAIL(encode_i32(buf, buf_len, pos, request_level_))) {
      LOG_WARN("Encode error", K(ret), KP(buf), K(buf_len), K(pos));
    } else if (OB_FAIL(encode_i64(buf, buf_len, pos, seq_no_))) {
      LOG_WARN("Encode error", K(ret), KP(buf), K(buf_len), K(pos));
    } else if (OB_FAIL(encode_i32(buf, buf_len, pos, group_id_))) {
      LOG_WARN("Encode error", K(ret), KP(buf), K(buf_len), K(pos));
    } else if (OB_FAIL(encode_i64(buf, buf_len, pos, trace_id_[2]))) {
      LOG_WARN("Encode error", K(ret), KP(buf), K(buf_len), K(pos));
    } else if (OB_FAIL(encode_i64(buf, buf_len, pos, trace_id_[3]))) {
      LOG_WARN("Encode error", K(ret), KP(buf), K(buf_len), K(pos));
    } else if (OB_FAIL(encode_i64(buf, buf_len, pos, cluster_name_hash_))) {
      LOG_WARN("Encode error", K(ret), KP(buf), K(buf_len), K(pos));
    } else if (OB_FAIL(encode_i64(buf, buf_len, pos, data_version_))) {
      LOG_WARN("Encode error", K(ret), KP(buf), K(buf_len), K(pos));
    } else {
      //do nothing
#ifdef ERRSIM
      if (OB_FAIL(encode_i64(buf, buf_len, pos, static_cast<int64_t>(module_type_.type_)))) {
        LOG_WARN("Encode error", K(ret), KP(buf), K(buf_len), K(pos));
      }
#endif
    }
  } else {
    ret = OB_BUF_NOT_ENOUGH;
  }

  return ret;
}

int ObRpcPacketHeader::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (data_len - pos >= HEADER_SIZE) {
    if (OB_FAIL(decode_i32(buf, data_len, pos, reinterpret_cast<int32_t*>(&pcode_)))) {
      LOG_WARN("Decode error", K(ret), KP(buf), K(data_len), K(pos));
    } else if (OB_FAIL(decode_i8(buf, data_len, pos, reinterpret_cast<int8_t*>(&hlen_)))) {
      LOG_WARN("Decode error", K(ret), KP(buf), K(data_len), K(pos));
    } else if (OB_FAIL(decode_i8(buf, data_len, pos, reinterpret_cast<int8_t*>(&priority_)))) {
      LOG_WARN("Decode error", K(ret), KP(buf), K(data_len), K(pos));
    } else if (OB_FAIL(decode_i16(buf, data_len, pos, reinterpret_cast<int16_t*>(&flags_)))) {
      LOG_WARN("Decode error", K(ret), KP(buf), K(data_len), K(pos));
    } else if (OB_FAIL(decode_i64(buf, data_len, pos, reinterpret_cast<int64_t*>(&checksum_)))) {
      LOG_WARN("Decode error", K(ret), KP(buf), K(data_len), K(pos));
    } else if (OB_FAIL(decode_i64(buf, data_len, pos, reinterpret_cast<int64_t*>(&tenant_id_)))) {
      LOG_WARN("Decode error", K(ret), KP(buf), K(data_len), K(pos));
    } else if (OB_FAIL(decode_i64(buf, data_len, pos, reinterpret_cast<int64_t*>(&priv_tenant_id_)))) {
      LOG_WARN("Decode error", K(ret), KP(buf), K(data_len), K(pos));
    } else if (OB_FAIL(decode_i64(buf, data_len, pos, reinterpret_cast<int64_t*>(&session_id_)))) {
      LOG_WARN("Decode error", K(ret), KP(buf), K(data_len), K(pos));
    } else if (OB_FAIL(decode_i64(buf, data_len, pos, reinterpret_cast<int64_t*>(&trace_id_[0])))) {
      LOG_WARN("Decode error", K(ret), KP(buf), K(data_len), K(pos));
    } else if (OB_FAIL(decode_i64(buf, data_len, pos, reinterpret_cast<int64_t*>(&trace_id_[1])))) {
      LOG_WARN("Decode error", K(ret), KP(buf), K(data_len), K(pos));
    } else if (OB_FAIL(decode_i64(buf, data_len, pos, reinterpret_cast<int64_t*>(&timeout_)))) {
      LOG_WARN("Decode error", K(ret), KP(buf), K(data_len), K(pos));
    } else if (OB_FAIL(decode_i64(buf, data_len, pos, reinterpret_cast<int64_t*>(&timestamp_)))) {
      LOG_WARN("Decode error", K(ret), KP(buf), K(data_len), K(pos));
    }
  } else {
    ret = OB_INVALID_DATA;
  }

  if (OB_SUCC(ret)) {
    dst_cluster_id_ = common::INVALID_CLUSTER_ID;
    compressor_type_ = INVALID_COMPRESSOR;
    original_len_ = 0;
    src_cluster_id_ = common::INVALID_CLUSTER_ID;
    unis_version_ = 0;
    request_level_ = 0;
    seq_no_ = 0;
    group_id_ = 0;

    if (hlen_ > pos && OB_FAIL(cost_time_.deserialize(buf, hlen_, pos))) {
      LOG_WARN("Decode error", K(ret), KP(buf), K(hlen_), K(pos));
    } else if (hlen_ > pos && OB_FAIL(decode_i64(buf, hlen_, pos, &dst_cluster_id_))) {
      LOG_WARN("Decode error", K(ret), KP(buf), K(hlen_), K(pos));
    } else if (hlen_ > pos &&
               OB_FAIL(decode_i32(buf, hlen_, pos,
                                  reinterpret_cast<int32_t*>(&compressor_type_)))) {
      LOG_WARN("Decode error", K(ret), KP(buf), K(hlen_), K(pos));
    } else if (hlen_ > pos && OB_FAIL(decode_i32(buf, hlen_, pos, &original_len_))) {
      LOG_WARN("Decode error", K(ret), KP(buf), K(hlen_), K(pos));
    } else if (hlen_ > pos && OB_FAIL(decode_i64(buf, hlen_, pos, &src_cluster_id_))) {
      LOG_WARN("Decode error", K(ret), KP(buf), K(hlen_), K(pos));
    } else if (hlen_ > pos &&
               OB_FAIL(decode_i64(buf, hlen_, pos,
                                  reinterpret_cast<int64_t*>(&unis_version_))) ){
      LOG_WARN("Decode error", K(ret), KP(buf), K(hlen_), K(pos));
    } else if (hlen_ > pos && OB_FAIL(decode_i32(buf, hlen_, pos, &request_level_))) {
      LOG_WARN("Decode error", K(ret), KP(buf), K(hlen_), K(pos));
    } else if (hlen_ > pos && OB_FAIL(decode_i64(buf, hlen_, pos, &seq_no_))) {
      LOG_WARN("Decode error", K(ret), KP(buf), K(hlen_), K(pos));
    } else if (hlen_ > pos && OB_FAIL(decode_i32(buf, hlen_, pos, &group_id_))) {
      LOG_WARN("Decode error", K(ret), KP(buf), K(hlen_), K(pos));
    } else if (hlen_ > pos && OB_FAIL(decode_i64(buf, hlen_, pos, reinterpret_cast<int64_t*>(&trace_id_[2])))) {
      LOG_WARN("Decode error", K(ret), KP(buf), K(hlen_), K(pos));
    } else if (hlen_ > pos && OB_FAIL(decode_i64(buf, hlen_, pos, reinterpret_cast<int64_t*>(&trace_id_[3])))) {
      LOG_WARN("Decode error", K(ret), KP(buf), K(hlen_), K(pos));
    } else if (hlen_ > pos && OB_FAIL(decode_i64(buf, hlen_, pos, reinterpret_cast<int64_t*>(&cluster_name_hash_)))) {
      LOG_WARN("Decode error", K(ret), KP(buf), K(hlen_), K(pos));
    } else if (hlen_ > pos && OB_FAIL(decode_i64(buf, data_len, pos, reinterpret_cast<int64_t*>(&data_version_)))) {
      LOG_WARN("Decode error", K(ret), KP(buf), K(data_len), K(pos));
    } else {
#ifdef ERRSIM
      int64_t type = 0;
      if (hlen_ > pos && OB_FAIL(decode_i64(buf, hlen_, pos, &type))) {
        LOG_WARN("Decode error", K(ret), KP(buf), K(hlen_), K(pos));
      } else {
        module_type_.type_ = static_cast<ObErrsimModuleType::TYPE>(type);
      }
#endif

    }
    ObSequence::update_max_seq_no(seq_no_);
    LOG_DEBUG("rpc receive seq_no ", K_(seq_no), K(ObSequence::get_max_seq_no()));
    // for RPC response, if the src_cluster_id is the same as current cluster id, we set the
    // data_version here. for RPC request, the failure of setting data_version may cause
    // disconnection, to avoid it, we delay setting to the RPC process phase.
    if (OB_SUCC(ret) && flags_ & ObRpcPacketHeader::RESP_FLAG &&
        ObRpcNetHandler::is_self_cluster(src_cluster_id_) && data_version_ > 0 && tenant_id_ > 0) {
      if (OB_FAIL(ODV_MGR.set(tenant_id_, data_version_))) {
        LOG_WARN("fail to update data_version", K(ret), KP(tenant_id_), KDV(data_version_));
      }
      LOG_TRACE("rpc receive data version", K_(tenant_id), K_(data_version), K_(pcode),
                K_(src_cluster_id), K(ObRpcNetHandler::CLUSTER_ID));
    }
    if (OB_ARB_GC_NOTIFY == pcode_ && REACH_TIME_INTERVAL(5000000)) {
      LOG_TRACE("receive arb rpc", K_(src_cluster_id), K_(pcode));
    }
  }

  return ret;
}

const uint8_t ObRpcPacket::MAGIC_HEADER_FLAG[4] =
{ ObRpcPacket::API_VERSION, 0xDB, 0xDB, 0xCE };
const uint8_t ObRpcPacket::MAGIC_COMPRESS_HEADER_FLAG[4] =
{ ObRpcPacket::API_VERSION, 0xDB, 0xDB, 0xCC };

uint32_t ObRpcPacket::global_chid = 0;
uint64_t ObRpcPacket::INVALID_CLUSTER_NAME_HASH = 0;

ObRpcPacket::ObRpcPacket()
    : cdata_(NULL), clen_(0), chid_(0), receive_ts_(0L),
      assemble_(false), msg_count_(0), payload_(0)
{
  easy_list_init(&list_);
  memset(&hdr_, 0, sizeof (hdr_));
  set_dst_cluster_id(common::INVALID_CLUSTER_ID);
  set_compressor_type(INVALID_COMPRESSOR);
  set_original_len(0);
  set_src_cluster_id(common::INVALID_CLUSTER_ID);
}

ObRpcPacket::~ObRpcPacket()
{
}

int ObRpcPacket::encode_ez_header(char *buf, int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  uint32_t ez_payload_size = static_cast<uint32_t>(get_encoded_size());

  MEMCPY(buf, ObRpcPacket::MAGIC_HEADER_FLAG, 4);
  pos += 4;

  if (OB_FAIL(encode_i32(buf, len, pos, ez_payload_size))) { // next 4 for packet content length
    LOG_WARN("failed to encode ez_payload_size", K(ret));
  } else if (OB_FAIL(encode_i32(buf, len, pos, chid_))) {
    // next 4 for channel id
    LOG_WARN("failed to encode chid", K(ret));
  } else if (OB_FAIL(encode_i32(buf, len, pos, 0))) {//skip 4 bytes for reserved
    LOG_WARN("failed to encode reserved", K(ret));
  }

  return ret;
}

uint64_t ObRpcPacket::get_self_cluster_name_hash()
{
  return oceanbase::obrpc::ObRpcNetHandler::CLUSTER_NAME_HASH;
}


_RLOCAL(int, g_pcode);

ObRpcCheckSumCheckLevel g_rpc_checksum_check_level = ObRpcCheckSumCheckLevel::FORCE;

} // ends of namespace obrpc
} // ends of namespace oceanbase
