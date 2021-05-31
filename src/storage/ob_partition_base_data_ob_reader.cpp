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

#define USING_LOG_PREFIX STORAGE
#include "ob_partition_base_data_ob_reader.h"
#include "lib/utility/ob_tracepoint.h"
#include "storage/ob_table_mgr.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/ob_partition_migrator_table_key_mgr.h"
#include "storage/ob_pg_storage.h"
#include "storage/ob_file_system_util.h"

namespace oceanbase {
using namespace common;
using namespace obrpc;
using namespace blocksstable;
using namespace memtable;
using namespace share::schema;
namespace storage {
template <ObRpcPacketCode RPC_CODE>
int ObStreamRpcReader<RPC_CODE>::init(ObInOutBandwidthThrottle& bandwidth_throttle)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("can not init twice", K(ret));
  } else {
    bandwidth_throttle_ = &bandwidth_throttle;
  }

  if (OB_SUCC(ret)) {
    if (NULL == (buf = reinterpret_cast<char*>(allocator_.alloc(OB_MALLOC_BIG_BLOCK_SIZE)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc buf", K(ret));
    } else if (!rpc_buffer_.set_data(buf, OB_MALLOC_BIG_BLOCK_SIZE)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to set rpc buffer", K(ret));
    } else {
      is_inited_ = true;
    }
  }

  return ret;
}

template <ObRpcPacketCode RPC_CODE>
int ObStreamRpcReader<RPC_CODE>::fetch_next_buffer_if_need()
{
  int ret = OB_SUCCESS;
  bool need_fetch = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(check_need_fetch_next_buffer(need_fetch))) {
    LOG_WARN("check need fetch next buffer failed", K(ret), K(need_fetch));
  } else if (need_fetch && OB_FAIL(fetch_next_buffer())) {
    if (OB_ITER_END != ret) {
      OB_LOG(WARN, "fail to fetch next buffer", K(ret), K(need_fetch), K(rpc_buffer_), K(rpc_buffer_parse_pos_));
    }
  }
  return ret;
}

template <ObRpcPacketCode RPC_CODE>
template <typename Data>
int ObStreamRpcReader<RPC_CODE>::fetch_and_decode(Data& data)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(fetch_next_buffer_if_need())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to fetch buffer", K(ret));
    }
  } else if (OB_FAIL(serialization::decode(
                 rpc_buffer_.get_data(), rpc_buffer_.get_position(), rpc_buffer_parse_pos_, data))) {
    LOG_WARN("failed to decode", K_(rpc_buffer), K_(rpc_buffer_parse_pos), K(ret));
  } else {
    LOG_INFO("decode data", K(rpc_buffer_), K(rpc_buffer_parse_pos_), K(data));
  }
  return ret;
}

template <ObRpcPacketCode RPC_CODE>
template <typename Data>
int ObStreamRpcReader<RPC_CODE>::fetch_and_decode(ObIAllocator& allocator, Data& data)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(fetch_next_buffer_if_need())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to fetch buffer", K(ret));
    }
  } else if (OB_FAIL(data.deserialize(
                 allocator, rpc_buffer_.get_data(), rpc_buffer_.get_position(), rpc_buffer_parse_pos_))) {
    LOG_WARN("failed to decode", K_(rpc_buffer), K_(rpc_buffer_parse_pos), K(ret));
  }
  return ret;
}

template <ObRpcPacketCode RPC_CODE>
template <typename Data>
int ObStreamRpcReader<RPC_CODE>::fetch_and_decode_list(ObIAllocator& allocator, ObIArray<Data>& data_list)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    Data tmp_data;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(fetch_next_buffer_if_need())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to fetch buffer", K(ret));
        }
      } else if (OB_FAIL(tmp_data.deserialize(
                     allocator, rpc_buffer_.get_data(), rpc_buffer_.get_position(), rpc_buffer_parse_pos_))) {
        LOG_WARN("failed to decode", K(rpc_buffer_), K(ret));
      } else if (OB_FAIL(data_list.push_back(tmp_data))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

template <ObRpcPacketCode RPC_CODE>
template <typename Data>
int ObStreamRpcReader<RPC_CODE>::fetch_and_decode_list(ObIArray<Data>& data_list)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    Data tmp_data;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(fetch_next_buffer_if_need())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to fetch buffer", K(ret));
        }
      } else if (OB_FAIL(
                     tmp_data.deserialize(rpc_buffer_.get_data(), rpc_buffer_.get_position(), rpc_buffer_parse_pos_))) {
        LOG_WARN("failed to decode", K(rpc_buffer_), K(ret));
      } else if (OB_FAIL(data_list.push_back(tmp_data))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

template <ObRpcPacketCode RPC_CODE>
int ObStreamRpcReader<RPC_CODE>::check_need_fetch_next_buffer(bool& need_fetch)
{
  int ret = OB_SUCCESS;
  need_fetch = false;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (rpc_buffer_parse_pos_ < 0 || last_send_time_ < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(rpc_buffer_parse_pos_), K(last_send_time_));
  } else if (rpc_buffer_.get_position() - rpc_buffer_parse_pos_ > 0) {
    // do nothing
    need_fetch = false;
    LOG_DEBUG("has left data, no need to get more", K(rpc_buffer_), K(rpc_buffer_parse_pos_), K(need_fetch));
  } else {
    need_fetch = true;
  }
  return ret;
}

template <ObRpcPacketCode RPC_CODE>
int ObStreamRpcReader<RPC_CODE>::fetch_next_buffer()
{
  int ret = OB_SUCCESS;
  const int64_t max_idle_time = OB_DEFAULT_STREAM_WAIT_TIMEOUT - OB_DEFAULT_STREAM_RESERVE_TIME;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    int tmp_ret = bandwidth_throttle_->limit_in_and_sleep(rpc_buffer_.get_position(), last_send_time_, max_idle_time);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("failed to sleep_for_bandlimit", K(tmp_ret));
    }

    rpc_buffer_.get_position() = 0;
    rpc_buffer_parse_pos_ = 0;
    if (handle_.has_more()) {
      if (OB_SUCCESS != (ret = handle_.get_more(rpc_buffer_))) {
        STORAGE_LOG(WARN, "get_more(send request) failed", K(ret));
        ret = OB_DATA_SOURCE_TIMEOUT;
      } else if (rpc_buffer_.get_position() <= 0) {
        ret = OB_ERR_SYS;
        LOG_ERROR("rpc buffer has no data", K(ret), K(rpc_buffer_));
      } else {
        LOG_DEBUG("get more data", K(rpc_buffer_), K(rpc_buffer_parse_pos_));
        data_size_ += rpc_buffer_.get_position();
      }
      last_send_time_ = ObTimeUtility::current_time();
    } else {
      ret = OB_ITER_END;
      LOG_INFO("no more data", K(rpc_buffer_), K(rpc_buffer_parse_pos_));
    }
  }
  return ret;
}

template <ObRpcPacketCode RPC_CODE>
int do_fetch_next_buffer_if_need(common::ObInOutBandwidthThrottle& bandwidth_throttle, common::ObDataBuffer& rpc_buffer,
    int64_t& rpc_buffer_parse_pos, obrpc::ObPartitionServiceRpcProxy::SSHandle<RPC_CODE>& handle,
    int64_t& last_send_time, int64_t& total_data_size)
{
  int ret = OB_SUCCESS;
  const int64_t max_idle_time = OB_DEFAULT_STREAM_WAIT_TIMEOUT - OB_DEFAULT_STREAM_RESERVE_TIME;

  if (rpc_buffer_parse_pos < 0 || last_send_time < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(rpc_buffer_parse_pos), K(last_send_time));
  } else if (rpc_buffer.get_position() - rpc_buffer_parse_pos > 0) {
    // do nothing
    LOG_DEBUG("has left data, no need to get more", K(rpc_buffer), K(rpc_buffer_parse_pos));
  } else {
    int tmp_ret = bandwidth_throttle.limit_in_and_sleep(rpc_buffer.get_position(), last_send_time, max_idle_time);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("failed to sleep_for_bandlimit", K(tmp_ret));
    }

    rpc_buffer.get_position() = 0;
    rpc_buffer_parse_pos = 0;
    if (handle.has_more()) {
      if (OB_SUCCESS != (ret = handle.get_more(rpc_buffer))) {
        STORAGE_LOG(WARN, "get_more(send request) failed", K(ret));
        ret = OB_DATA_SOURCE_TIMEOUT;
      } else if (rpc_buffer.get_position() < 0) {
        ret = OB_ERR_SYS;
        LOG_ERROR("rpc buffer has no data", K(ret), K(rpc_buffer));
      } else if (0 == rpc_buffer.get_position()) {
        if (!handle.has_more()) {
          ret = OB_ITER_END;
          LOG_DEBUG("empty rpc buffer, no more data", K(rpc_buffer), K(rpc_buffer_parse_pos));
        } else {
          ret = OB_ERR_SYS;
          LOG_ERROR("rpc buffer has no data", K(ret), K(rpc_buffer));
        }
      } else {
        LOG_DEBUG("get more data", K(rpc_buffer), K(rpc_buffer_parse_pos));
        total_data_size += rpc_buffer.get_position();
      }
      last_send_time = ObTimeUtility::current_time();
    } else {
      ret = OB_ITER_END;
      LOG_DEBUG("no more data", K(rpc_buffer), K(rpc_buffer_parse_pos));
    }
  }
  return ret;
}

template <obrpc::ObRpcPacketCode RPC_CODE>
ObLogicStreamRpcReader<RPC_CODE>::ObLogicStreamRpcReader()
    : is_inited_(false),
      bandwidth_throttle_(NULL),
      rpc_buffer_(),
      rpc_buffer_parse_pos_(0),
      allocator_(ObNewModIds::OB_PARTITION_MIGRATE),
      last_send_time_(0),
      data_size_(0),
      object_count_(0),
      rpc_header_()
{}

template <ObRpcPacketCode RPC_CODE>
int ObLogicStreamRpcReader<RPC_CODE>::init(ObInOutBandwidthThrottle& bandwidth_throttle, const bool has_lob)
{

  int ret = OB_SUCCESS;
  char* buf = NULL;
  const int64_t buf_size = has_lob ? OB_MAX_PACKET_BUFFER_LENGTH : OB_MALLOC_BIG_BLOCK_SIZE;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("can not init twice", K(ret));
  } else {
    bandwidth_throttle_ = &bandwidth_throttle;
  }

  if (OB_SUCC(ret)) {
    if (NULL == (buf = reinterpret_cast<char*>(allocator_.alloc(buf_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc buf", K(ret));
    } else if (!rpc_buffer_.set_data(buf, buf_size)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to set rpc buffer", K(ret));
    } else {
      is_inited_ = true;
    }
  }

  return ret;
}

template <ObRpcPacketCode RPC_CODE>
int ObLogicStreamRpcReader<RPC_CODE>::fetch_next_buffer_if_need()
{
  int ret = OB_SUCCESS;
  bool need_fetch = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(check_need_fetch_next_buffer(need_fetch))) {
    LOG_WARN("check need fetch next buffer failed", K(ret), K(need_fetch));
  } else if (need_fetch) {
    object_count_ = 0;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(fetch_next_buffer())) {
        if (OB_ITER_END != ret) {
          OB_LOG(WARN, "fail to fetch next buffer", K(ret), K(need_fetch), K(rpc_buffer_), K(rpc_buffer_parse_pos_));
        }
      } else if (OB_FAIL(decode_rpc_header())) {
        OB_LOG(WARN, "fail to fetch and decode rpc header", K(ret), K(rpc_header_));
      } else if (0 == rpc_header_.object_count_ && rpc_header_.has_next_rpc()) {
        // The source sends an empty packet, continues to fetch the next buffer and parses the header
      } else {
        break;
      }
    }
  }
  return ret;
}

template <ObRpcPacketCode RPC_CODE>
int ObLogicStreamRpcReader<RPC_CODE>::check_need_fetch_next_buffer(bool& need_fetch)
{
  int ret = OB_SUCCESS;
  need_fetch = false;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (rpc_buffer_parse_pos_ < 0 || last_send_time_ < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(rpc_buffer_parse_pos_), K(last_send_time_));
  } else if ((rpc_buffer_.get_position() - rpc_buffer_parse_pos_ > 0) || rpc_header_.need_reconnect()) {
    // do nothing
    need_fetch = false;
    LOG_DEBUG("has left data, no need to get more", K(rpc_buffer_), K(rpc_buffer_parse_pos_), K(need_fetch));
  } else {
    need_fetch = true;
  }
  return ret;
}

template <ObRpcPacketCode RPC_CODE>
template <typename Data>
int ObLogicStreamRpcReader<RPC_CODE>::fetch_and_decode(Data& data)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(fetch_next_buffer_if_need())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to fetch buffer", K(ret));
    }
  } else if (object_count_ < rpc_header_.object_count_) {
    if (OB_FAIL(
            serialization::decode(rpc_buffer_.get_data(), rpc_buffer_.get_position(), rpc_buffer_parse_pos_, data))) {
      LOG_WARN("failed to decode", K_(rpc_buffer), K_(rpc_buffer_parse_pos), K(ret));
    } else {
      ++object_count_;
      LOG_DEBUG("decode data", K(rpc_buffer_), K(rpc_buffer_parse_pos_), K(data), K(object_count_));
    }
  } else if (rpc_header_.need_reconnect()) {
    ret = OB_RPC_NEED_RECONNECT;
    LOG_INFO("rpc need re-connect", K(ret), K(rpc_header_));
  } else if (rpc_header_.end_connect()) {
    ret = OB_ITER_END;
  }
  return ret;
}

template <ObRpcPacketCode RPC_CODE>
int ObLogicStreamRpcReader<RPC_CODE>::decode_rpc_header()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (rpc_buffer_parse_pos_ < 0 || last_send_time_ < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(rpc_buffer_parse_pos_), K(last_send_time_));
  } else if (OB_FAIL(serialization::decode(
                 rpc_buffer_.get_data(), rpc_buffer_.get_position(), rpc_buffer_parse_pos_, rpc_header_))) {
    LOG_WARN("failed to decode", K_(rpc_buffer), K_(rpc_buffer_parse_pos), K(ret));
  } else if (!rpc_header_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get rpc header", K(ret), K(rpc_header_));
  } else {
    object_count_ = 0;
    LOG_INFO("decode rpc header", K(rpc_buffer_), K(rpc_buffer_parse_pos_), K(rpc_header_));
  }
  return ret;
}

template <ObRpcPacketCode RPC_CODE>
int ObLogicStreamRpcReader<RPC_CODE>::fetch_next_buffer()
{
  int ret = OB_SUCCESS;
  const int64_t max_idle_time = OB_DEFAULT_STREAM_WAIT_TIMEOUT - OB_DEFAULT_STREAM_RESERVE_TIME;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    int tmp_ret = bandwidth_throttle_->limit_in_and_sleep(rpc_buffer_.get_position(), last_send_time_, max_idle_time);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("failed to sleep_for_bandlimit", K(tmp_ret));
    }

    rpc_buffer_.get_position() = 0;
    rpc_buffer_parse_pos_ = 0;
    if (handle_.has_more()) {
      if (OB_SUCCESS != (ret = handle_.get_more(rpc_buffer_))) {
        STORAGE_LOG(WARN, "get_more(send request) failed", K(ret));
        ret = OB_DATA_SOURCE_TIMEOUT;
      } else if (rpc_buffer_.get_position() <= 0) {
        ret = OB_ERR_SYS;
        LOG_ERROR("rpc buffer has no data", K(ret), K(rpc_buffer_));
      } else {
        LOG_DEBUG("get more data", K(rpc_buffer_), K(rpc_buffer_parse_pos_));
        data_size_ += rpc_buffer_.get_position();
      }
      last_send_time_ = ObTimeUtility::current_time();
    } else {
      ret = OB_ITER_END;
      LOG_INFO("no more data", K(rpc_buffer_), K(rpc_buffer_parse_pos_));
    }
  }
  return ret;
}

ObPartitionBaseDataMetaObReader::ObPartitionBaseDataMetaObReader()
    : is_inited_(false),
      saved_storage_info_(),
      handle_(),
      bandwidth_throttle_(NULL),
      rpc_buffer_(),
      rpc_buffer_parse_pos_(0),
      allocator_(ObNewModIds::OB_PARTITION_MIGRATE),
      total_sstable_count_(0),
      fetched_sstable_count_(0),
      last_send_time_(0),
      data_size_(0),
      store_type_(INVALID_STORE_TYPE),
      table_key_(),
      macro_block_list_()
{}

ObPartitionBaseDataMetaObReader::~ObPartitionBaseDataMetaObReader()
{}

int ObPartitionBaseDataMetaObReader::init(obrpc::ObPartitionServiceRpcProxy& srv_rpc_proxy,
    common::ObInOutBandwidthThrottle& bandwidth_throttle, const ObAddr& addr, const common::ObPartitionKey& pkey,
    const ObVersion& version, const ObStoreType& store_type)
{
  int ret = OB_SUCCESS;
  ObFetchBaseDataMetaArg rpc_arg;
  char* buf = NULL;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("can not init twice", K(ret));
  } else if (OB_UNLIKELY(!addr.is_valid()) || OB_UNLIKELY(!version.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(addr), K(version), K(ret));
  } else if (NULL == (buf = reinterpret_cast<char*>(allocator_.alloc(OB_MALLOC_BIG_BLOCK_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret));
  } else if (!rpc_buffer_.set_data(buf, OB_MALLOC_BIG_BLOCK_SIZE)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to set rpc buffer", K(ret));
  } else {
    rpc_arg.pkey_ = pkey;
    rpc_arg.version_ = version;
    rpc_arg.store_type_ = store_type;
    LOG_INFO("init fetch_base_data_meta", K(addr), K(rpc_arg), K(store_type));
    if (OB_FAIL(srv_rpc_proxy.to(addr).by(OB_DATA_TENANT_ID).fetch_base_data_meta(rpc_arg, rpc_buffer_, handle_))) {
      if (OB_TENANT_NOT_IN_SERVER != ret) {
        LOG_WARN("failed to send fetch base data meta rpc use OB_DATA_TENANT_ID", K(ret), K(addr), K(rpc_arg));
      } else if (OB_FAIL(srv_rpc_proxy.to(addr).fetch_base_data_meta(rpc_arg, rpc_buffer_, handle_))) {
        LOG_WARN("failed to send fetch base data meta rpc", K(ret), K(addr), K(rpc_arg));
      }
    }

    if (OB_SUCC(ret)) {
      bandwidth_throttle_ = &bandwidth_throttle;
      rpc_buffer_parse_pos_ = 0;
      total_sstable_count_ = 0;
      fetched_sstable_count_ = 0;
      last_send_time_ = ObTimeUtility::current_time();
      data_size_ = rpc_buffer_.get_position();
      store_type_ = store_type;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObPartitionBaseDataMetaObReader::init(obrpc::ObPartitionServiceRpcProxy& srv_rpc_proxy,
    common::ObInOutBandwidthThrottle& bandwidth_throttle, const ObAddr& addr, const common::ObPartitionKey& pkey,
    const obrpc::ObFetchPhysicalBaseMetaArg& arg)
{
  int ret = OB_SUCCESS;
  ObStoreType store_type = ObStoreType::INVALID_STORE_TYPE;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("partition base data meta ob reader init twice", K(ret));
  } else if (!addr.is_valid() || !pkey.is_valid() || !arg.table_key_.is_valid() ||
             !arg.table_key_.version_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN(
        "partition base dat meta ob reader init get invalid argument", K(ret), K(addr), K(pkey), K(arg.table_key_));
  } else if (ObITable::TableType::MAJOR_SSTABLE == arg.table_key_.table_type_) {
    store_type = ObStoreType::MAJOR_SSSTORE;
  } else if (ObITable::TableType::MINOR_SSTABLE == arg.table_key_.table_type_) {
    store_type = ObStoreType::MINOR_SSSTORE;
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("table table do not support when upgrade", K(ret), K(arg.table_key_));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(init(srv_rpc_proxy, bandwidth_throttle, addr, pkey, arg.table_key_.version_, store_type))) {
      LOG_WARN("fail to init partition base data meta ob reader", K(ret));
    } else {
      table_key_ = arg.table_key_;
    }
  }
  return ret;
}

int ObPartitionBaseDataMetaObReader::fetch_next_buffer_if_need()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(do_fetch_next_buffer_if_need(
                 *bandwidth_throttle_, rpc_buffer_, rpc_buffer_parse_pos_, handle_, last_send_time_, data_size_))) {
    LOG_WARN("failed to fetch next buffer if need", K(ret));
  }
  return ret;
}

int ObPartitionBaseDataMetaObReader::fetch_partition_meta(
    blocksstable::ObPartitionMeta& partition_meta, int64_t& sstable_count)
{
  int ret = OB_SUCCESS;
  partition_meta.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(fetch_next_buffer_if_need())) {
    LOG_WARN("failed to fetch next buffer", K(ret));
  } else if (OB_FAIL(partition_meta.deserialize(
                 rpc_buffer_.get_data(), rpc_buffer_.get_position(), rpc_buffer_parse_pos_))) {
    LOG_WARN("failed to decode partition meta", K(ret));
  } else if (OB_FAIL(serialization::decode_i64(
                 rpc_buffer_.get_data(), rpc_buffer_.get_position(), rpc_buffer_parse_pos_, &total_sstable_count_))) {
    LOG_WARN("failed to decode total_sstable_count_", K(ret));
  } else if (partition_meta.store_type_ != store_type_) {
    ret = OB_VERSION_NOT_MATCH;
    LOG_WARN("ssstore type note match", K(partition_meta), K_(store_type), K(ret));
  } else {
    sstable_count = total_sstable_count_;
    LOG_INFO("succeed to fetch partition meta",
        K_(total_sstable_count),
        K(rpc_buffer_),
        K(rpc_buffer_parse_pos_),
        K(partition_meta));
  }
  return ret;
}

int ObPartitionBaseDataMetaObReader::fetch_next_sstable_meta(
    blocksstable::ObSSTableBaseMeta& sstable_meta, common::ObIArray<blocksstable::ObSSTablePair>& macro_block_list)
{
  int ret = OB_SUCCESS;
  blocksstable::ObSSTablePair tmp_sstable_pair;
  const int64_t max_idle_time = OB_DEFAULT_STREAM_WAIT_TIMEOUT - OB_DEFAULT_STREAM_RESERVE_TIME;

  sstable_meta.reset();
  macro_block_list.reset();
  LOG_INFO("fetch next sstable meta",
      K(total_sstable_count_),
      K(fetched_sstable_count_),
      K(rpc_buffer_),
      K(rpc_buffer_parse_pos_));
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (total_sstable_count_ == fetched_sstable_count_) {
    if (rpc_buffer_.get_position() != rpc_buffer_parse_pos_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("all sstable meta is fetched but the there is still unused data in rpc buffer",
          K(ret),
          K(rpc_buffer_parse_pos_),
          K(rpc_buffer_));
    } else {
      int tmp_ret = bandwidth_throttle_->limit_in_and_sleep(rpc_buffer_.get_position(), last_send_time_, max_idle_time);
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("failed to sleep_for_bandlimit", K(tmp_ret));
      }

      ret = OB_ITER_END;
      LOG_INFO("has already fetch all sstable meta", K(total_sstable_count_), K(fetched_sstable_count_));
    }
  } else if (OB_FAIL(fetch_next_buffer_if_need())) {
    LOG_WARN("failed to fetch next buffer", K(ret));
  } else if (OB_FAIL(
                 sstable_meta.deserialize(rpc_buffer_.get_data(), rpc_buffer_.get_position(), rpc_buffer_parse_pos_))) {
    LOG_WARN("failed to decode partition meta", K(ret), K(rpc_buffer_), K(rpc_buffer_parse_pos_));
  } else if (OB_FAIL(macro_block_list.reserve(sstable_meta.get_total_macro_block_count()))) {
    LOG_WARN("failed to reserve macro block list",
        K(ret),
        "total_macro_block_count",
        sstable_meta.get_total_macro_block_count());
  } else {
    ++fetched_sstable_count_;
    for (int64_t macro_block_count = 0; OB_SUCC(ret) && macro_block_count < sstable_meta.get_total_macro_block_count();
         ++macro_block_count) {
      if (OB_FAIL(fetch_next_buffer_if_need())) {
        LOG_WARN("failed to fetch next buffer", K(ret));
      } else if (OB_FAIL(tmp_sstable_pair.deserialize(
                     rpc_buffer_.get_data(), rpc_buffer_.get_position(), rpc_buffer_parse_pos_))) {
        LOG_WARN("failed to decode tmp_sstable_pair", K(ret));
      } else if (OB_FAIL(macro_block_list.push_back(tmp_sstable_pair))) {
        LOG_WARN("failed to add sstable pair", K(ret));
      }
    }
  }
  return ret;
}

int ObPartitionBaseDataMetaObReader::fetch_sstable_meta(ObSSTableBaseMeta& sstable_meta)
{
  int ret = OB_SUCCESS;
  ObPartitionMeta partition_meta;
  int64_t sstable_count = 0;
  ObSSTableBaseMeta tmp_meta;
  ObArray<blocksstable::ObSSTablePair> macro_block_list;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition base data meta ob reader do not init", K(ret));
  } else if (!table_key_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table key is not invalid, do not fetch sstable meta", K(ret), K(table_key_));
  } else if (OB_FAIL(fetch_partition_meta(partition_meta, sstable_count))) {
    LOG_WARN("fail to fetch partition meta", K(ret), K(sstable_count));
  } else {
    while (OB_SUCC(ret)) {
      macro_block_list.reuse();
      tmp_meta.reset();
      if (OB_FAIL(fetch_next_sstable_meta(tmp_meta, macro_block_list))) {
        LOG_WARN("fail to fetch next sstable meta", K(ret), K(tmp_meta));
      } else if (tmp_meta.index_id_ == table_key_.table_id_) {
        if (OB_FAIL(sstable_meta.assign(tmp_meta))) {
          LOG_WARN("fail to  assign sstable meta", K(ret));
        } else if (OB_FAIL(macro_block_list_.assign(macro_block_list))) {
          LOG_WARN("fail to assign macro block list", K(ret), K(macro_block_list.count()));
        } else {
          break;
        }
      }
    }
  }
  return ret;
}

int ObPartitionBaseDataMetaObReader::fetch_macro_block_list(ObIArray<blocksstable::ObSSTablePair>& macro_block_list)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition base data meta ob reader do not init", K(ret));
  } else if (!table_key_.is_valid()) {
    LOG_WARN("table key is not invalid can not fetch macro block list", K(ret), K(table_key_));
  } else if (OB_FAIL(macro_block_list.assign(macro_block_list_))) {
    LOG_WARN("fail to assgin macro block list", K(ret));
  }
  return ret;
}

ObPartitionMacroBlockObReader::ObPartitionMacroBlockObReader()
    : is_inited_(false),
      handle_(),
      old_handle_(),
      bandwidth_throttle_(NULL),
      data_buffer_(),
      rpc_buffer_(),
      rpc_buffer_parse_pos_(0),
      allocator_(ObNewModIds::OB_PARTITION_MIGRATE),
      meta_allocator_(ObNewModIds::OB_PARTITION_MIGRATE),
      macro_block_mem_context_(),
      last_send_time_(0),
      data_size_(0),
      use_old_rpc_(false)
{}

ObPartitionMacroBlockObReader::~ObPartitionMacroBlockObReader()
{
  void* ptr = reinterpret_cast<void*>(data_buffer_.data());
  if (nullptr != ptr && macro_block_mem_context_.get_allocator().contains(ptr)) {
    macro_block_mem_context_.free(ptr);
  }
}

int ObPartitionMacroBlockObReader::init(obrpc::ObPartitionServiceRpcProxy& srv_rpc_proxy,
    common::ObInOutBandwidthThrottle& bandwidth_throttle, const common::ObAddr& src_server,
    const storage::ObITable::TableKey table_key, const common::ObIArray<ObMigrateArgMacroBlockInfo>& list,
    const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  ObFetchMacroBlockListArg rpc_arg;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else if (!src_server.is_valid() || list.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(src_server), K(list.count()));
  } else if (OB_FAIL(macro_block_mem_context_.init())) {
    LOG_WARN("failed to init macro block memory context", K(ret));
  } else if (OB_FAIL(alloc_buffers())) {
    LOG_WARN("failed to alloc buffers", K(ret));
  } else if (OB_FAIL(rpc_arg.arg_list_.reserve(list.count()))) {
    LOG_WARN("failed to reserve rpc arg", K(ret), K(list.count()));
  } else {
    rpc_arg.table_key_ = table_key;
    for (int64_t i = 0; OB_SUCC(ret) && i < list.count(); ++i) {
      if (OB_FAIL(rpc_arg.arg_list_.push_back(list.at(i).fetch_arg_))) {
        LOG_WARN("failed to add fetch arg", K(ret), K(i));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (rpc_arg.get_serialize_size() > OB_MALLOC_BIG_BLOCK_SIZE) {
      ret = OB_ERR_SYS;
      LOG_ERROR("rpc arg must not larger than packet size", K(ret), K(rpc_arg.get_serialize_size()));
    } else if (OB_FAIL(srv_rpc_proxy.to(src_server)
                           .by(OB_DATA_TENANT_ID)
                           .dst_cluster_id(cluster_id)
                           .fetch_macro_block(rpc_arg, rpc_buffer_, handle_))) {
      LOG_WARN("failed to send fetch macro block rpc", K(src_server), K(ret));
    } else {
      bandwidth_throttle_ = &bandwidth_throttle;
      rpc_buffer_parse_pos_ = 0;
      last_send_time_ = ObTimeUtility::current_time();
      data_size_ = rpc_buffer_.get_position();
      is_inited_ = true;
      LOG_INFO("get first package fetch macro block", K(rpc_arg), K(rpc_buffer_));
    }
  }

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = E(EventTable::EN_MIGRATE_FETCH_MACRO_BLOCK) OB_SUCCESS;
  }
#endif
  return ret;
}
// 1.4x old rpc fetch macro block
int ObPartitionMacroBlockObReader::init_with_old_rpc(obrpc::ObPartitionServiceRpcProxy& srv_rpc_proxy,
    common::ObInOutBandwidthThrottle& bandwidth_throttle, const common::ObAddr& src_server,
    const storage::ObITable::TableKey table_key, const common::ObIArray<ObMigrateArgMacroBlockInfo>& list)
{
  int ret = OB_SUCCESS;
  ObFetchMacroBlockListOldArg rpc_arg;
  ObFetchMacroBlockOldArg old_arg;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else if (!src_server.is_valid() || list.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(src_server), K(list.count()));
  } else if (OB_FAIL(macro_block_mem_context_.init())) {
    LOG_WARN("failed to init macro block memory context", K(ret));
  } else if (OB_FAIL(alloc_buffers())) {
    LOG_WARN("failed to alloc buffers", K(ret));
  } else if (OB_FAIL(rpc_arg.arg_list_.reserve(list.count()))) {
    LOG_WARN("failed to reserve rpc arg", K(ret), K(list.count()));
  } else {
    rpc_arg.pkey_ = table_key.pkey_;
    rpc_arg.data_version_ = table_key.version_;
    if (table_key.table_type_ == ObITable::TableType::MAJOR_SSTABLE) {
      rpc_arg.store_type_ = ObStoreType::MAJOR_SSSTORE;
    } else if (table_key.table_type_ == ObITable::TableType::MINOR_SSTABLE) {
      rpc_arg.store_type_ = ObStoreType::MINOR_SSSTORE;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table type do not support to call rpc from 1.4x", K(ret), K(table_key));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < list.count(); ++i) {
      old_arg.reset();
      const ObFetchMacroBlockArg& fetch_arg = list.at(i).fetch_arg_;
      old_arg.data_seq_ = fetch_arg.data_seq_;
      old_arg.data_version_ = fetch_arg.data_version_;
      old_arg.index_id_ = table_key.table_id_;
      old_arg.macro_block_index_ = fetch_arg.macro_block_index_;
      if (OB_FAIL(rpc_arg.arg_list_.push_back(old_arg))) {
        LOG_WARN("fail to push old arg into array", K(ret), K(i), K(old_arg));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (rpc_arg.get_serialize_size() > OB_MALLOC_BIG_BLOCK_SIZE) {
      ret = OB_ERR_SYS;
      LOG_ERROR("rpc arg must not larger than packet size", K(ret), K(rpc_arg.get_serialize_size()));
    } else if (OB_FAIL(srv_rpc_proxy.to(src_server)
                           .by(OB_DATA_TENANT_ID)
                           .fetch_macro_block_old(rpc_arg, rpc_buffer_, old_handle_))) {
      LOG_WARN("failed to send fetch macro block rpc", K(src_server), K(ret));
    } else {
      bandwidth_throttle_ = &bandwidth_throttle;
      rpc_buffer_parse_pos_ = 0;
      last_send_time_ = ObTimeUtility::current_time();
      data_size_ = rpc_buffer_.get_position();
      use_old_rpc_ = true;
      is_inited_ = true;
      LOG_INFO("get first package fetch macro block", K(rpc_arg), K(rpc_buffer_));
    }
  }

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = E(EventTable::EN_MIGRATE_FETCH_MACRO_BLOCK) OB_SUCCESS;
  }
#endif
  return ret;
}

int ObPartitionMacroBlockObReader::alloc_buffers()
{
  int ret = OB_SUCCESS;
  char* buf = NULL;

  // used in init() func, should not check is_inited_
  if (NULL == (buf = reinterpret_cast<char*>(allocator_.alloc(OB_MALLOC_BIG_BLOCK_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret));
  } else if (!rpc_buffer_.set_data(buf, OB_MALLOC_BIG_BLOCK_SIZE)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to set rpc buffer", K(ret));
  } else if (OB_FAIL(alloc_from_memctx_first(buf))) {
    LOG_WARN("failed to alloc buf", K(ret));
  } else {
    data_buffer_.assign(buf, OB_DEFAULT_MACRO_BLOCK_SIZE);
  }
  return ret;
}

int ObPartitionMacroBlockObReader::alloc_from_memctx_first(char*& buf)
{
  int ret = OB_SUCCESS;
  buf = NULL;
  if (OB_ISNULL(buf = reinterpret_cast<char*>(macro_block_mem_context_.alloc()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc buf from mem ctx", K(ret));
  }

  if (OB_ISNULL(buf)) {
    ret = OB_SUCCESS;
    if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator_.alloc(OB_DEFAULT_MACRO_BLOCK_SIZE)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc buf", K(ret));
    }
  }
  return ret;
}

int ObPartitionMacroBlockObReader::fetch_next_buffer_if_need()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (use_old_rpc_ && OB_FAIL(fetch_next_buffer_with_old_rpc())) {  // use old rpc
    LOG_WARN("fail to fetch next buffer with old rpc", K(ret));
  } else if (OB_FAIL(fetch_next_buffer())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to fetch next buffer", K(ret));
    }
  }
  return ret;
}

int ObPartitionMacroBlockObReader::fetch_next_buffer()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(do_fetch_next_buffer_if_need(
                 *bandwidth_throttle_, rpc_buffer_, rpc_buffer_parse_pos_, handle_, last_send_time_, data_size_))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to fetch next buffer if need", K(ret));
    }
  }
  return ret;
}

int ObPartitionMacroBlockObReader::fetch_next_buffer_with_old_rpc()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(do_fetch_next_buffer_if_need(
                 *bandwidth_throttle_, rpc_buffer_, rpc_buffer_parse_pos_, old_handle_, last_send_time_, data_size_))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to fetch next buffer if need", K(ret));
    }
  }
  return ret;
}

int ObPartitionMacroBlockObReader::get_next_macro_block(blocksstable::ObFullMacroBlockMeta& meta,
    blocksstable::ObBufferReader& data, blocksstable::MacroBlockId& src_macro_id)
{
  int ret = OB_SUCCESS;
  src_macro_id.reset();
  meta_allocator_.reuse();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(fetch_next_buffer_if_need())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to fetch next buffer if need", K(ret));
    }
  } else {
    if (OB_FAIL(deserialize_macro_meta(
            rpc_buffer_.get_data(), rpc_buffer_.get_position(), rpc_buffer_parse_pos_, meta_allocator_, meta))) {
      LOG_WARN("failed to decode meta", K(ret), K(rpc_buffer_));
    }
  }

  if (OB_SUCC(ret)) {
    data_buffer_.set_pos(0);
    while (OB_SUCC(ret)) {
      if (data_buffer_.length() > meta.meta_->occupy_size_) {
        ret = OB_ERR_SYS;
        LOG_WARN("data buffer must not larger than occupy size", K(ret), K(data_buffer_), K(meta));
      } else if (data_buffer_.length() == meta.meta_->occupy_size_) {
        data.assign(data_buffer_.data(), data_buffer_.capacity(), data_buffer_.length());
        LOG_DEBUG("get_next_macro_block", K(rpc_buffer_), K(rpc_buffer_parse_pos_), K(meta));
        break;
      } else if (OB_FAIL(fetch_next_buffer_if_need())) {
        LOG_WARN("failed to fetch next buffer if need", K(ret));
      } else {
        int64_t need_size = meta.meta_->occupy_size_ - data_buffer_.length();
        int64_t rpc_remain_size = rpc_buffer_.get_position() - rpc_buffer_parse_pos_;
        int64_t copy_size = std::min(need_size, rpc_remain_size);
        if (copy_size > data_buffer_.remain()) {
          ret = OB_BUF_NOT_ENOUGH;
          LOG_ERROR("data buffer is not enough, macro block data must not larger than data buffer",
              K(ret),
              K(copy_size),
              K(data_buffer_),
              K(meta));
        } else {
          LOG_DEBUG("copy rpc to data buffer",
              K(need_size),
              K(rpc_remain_size),
              K(copy_size),
              "occupy_size",
              meta.meta_->occupy_size_,
              K(rpc_buffer_parse_pos_));
          MEMCPY(data_buffer_.current(), rpc_buffer_.get_data() + rpc_buffer_parse_pos_, copy_size);
          if (OB_FAIL(data_buffer_.advance(copy_size))) {
            STORAGE_LOG(
                ERROR, "BUG here! data_buffer_ advance failed.", K(ret), K(data_buffer_.remain()), K(copy_size));
          } else {
            rpc_buffer_parse_pos_ += copy_size;
          }
        }
      }
    }
  }
  return ret;
}

ObPartitionMacroBlockObProducer::ObPartitionMacroBlockObProducer()
    : is_inited_(false),
      macro_list_(),
      macro_idx_(0),
      handle_idx_(0),
      prefetch_meta_time_(0),
      prefetch_meta_(),
      store_handle_(),
      sstable_(NULL),
      file_handle_()

{}

ObPartitionMacroBlockObProducer::~ObPartitionMacroBlockObProducer()
{
  for (int64_t i = 0; i < MAX_PREFETCH_MACRO_BLOCK_NUM; ++i) {
    read_handle_[i].reset();
  }
}

int ObPartitionMacroBlockObProducer::init(
    const ObITable::TableKey& table_key, const common::ObIArray<obrpc::ObFetchMacroBlockArg>& arg_list)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObITable* table = NULL;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else if (!table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(table_key), K(arg_list));
  } else if (OB_FAIL(macro_list_.assign(arg_list))) {
    LOG_WARN("failed to copy macro list", K(ret));
  } else if (OB_FAIL(ObPartitionService::get_instance().acquire_sstable(table_key, store_handle_))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_MAJOR_SSTABLE_NOT_EXIST;
      LOG_WARN("sstable may has been merged", K(ret));
    } else {
      LOG_WARN("failed to get table", K(table_key), K(ret));
    }
  } else if (OB_ISNULL(table = store_handle_.get_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable should not be null here", K(ret));
  } else if (!table->is_sstable()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table should be sstable", K(table), K(ret));
  } else {
    macro_idx_ = -1;
    handle_idx_ = 0;
    prefetch_meta_.reset();
    sstable_ = static_cast<ObSSTable*>(table);
    if (OB_FAIL(file_handle_.assign(sstable_->get_storage_file_handle()))) {
      LOG_WARN("fail to get file handle", K(ret), K(sstable_->get_storage_file_handle()));
    } else {
      is_inited_ = true;
      LOG_INFO("succeed to init macro block producer", K(table_key), K(arg_list.count()));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(prefetch())) {
      LOG_WARN("failed to prefetch", K(ret));
    }
  }
  return ret;
}

int ObPartitionMacroBlockObProducer::get_next_macro_block(
    blocksstable::ObFullMacroBlockMeta& meta, blocksstable::ObBufferReader& data)
{
  int ret = OB_SUCCESS;
  const int64_t io_timeout_ms = GCONF._data_storage_io_timeout / 1000L;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (macro_idx_ < 0 || macro_idx_ > macro_list_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid macro_idx_", K(ret), K(macro_idx_), K(prefetch_meta_), K(macro_list_));
  } else if (macro_list_.count() == macro_idx_) {
    ret = OB_ITER_END;
    LOG_INFO("get next macro block end");
  } else if (!read_handle_[handle_idx_].is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("read handle is not valid, cannot wait", K(ret), K(handle_idx_));
  } else if (OB_FAIL(read_handle_[handle_idx_].wait(io_timeout_ms))) {
    LOG_WARN("failed to wait read handle", K(ret));
  } else if (!prefetch_meta_.is_valid()) {
    ret = OB_ERR_SYS;
    LOG_ERROR("prefetch_meta_ must not null", K(ret), K(prefetch_meta_), K(macro_idx_), K(macro_list_.count()));
  } else if (prefetch_meta_.meta_->data_seq_ != macro_list_.at(macro_idx_).data_seq_ ||
             prefetch_meta_.meta_->data_version_ != macro_list_.at(macro_idx_).data_version_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN(
        "prefetch meta and macro arg list is not match", K(ret), K(prefetch_meta_), "arg", macro_list_.at(macro_idx_));
  } else {
    meta = prefetch_meta_;
    data.assign(read_handle_[handle_idx_].get_buffer(), meta.meta_->occupy_size_);
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(prefetch())) {
      LOG_WARN("failed to do prefetch", K(ret));
    }
  }
  return ret;
}

int ObPartitionMacroBlockObProducer::prefetch()
{
  int ret = OB_SUCCESS;
  blocksstable::MacroBlockId macro_block_id;
  blocksstable::ObMacroBlockReadInfo read_info;
  blocksstable::ObMacroBlockCtx macro_block_ctx;
  ObStorageFile* file = NULL;
  prefetch_meta_time_ = ObTimeUtility::current_time();
  ++macro_idx_;
  handle_idx_ = (handle_idx_ + 1) % MAX_PREFETCH_MACRO_BLOCK_NUM;
  read_handle_[handle_idx_].reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (macro_idx_ < 0 || macro_idx_ > macro_list_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid macro_idx_", K(ret), K(macro_idx_));
  } else if (macro_idx_ == macro_list_.count()) {
    // no need to
    LOG_INFO("has finish, no need do prefetch", K(macro_idx_), K(macro_list_.count()));
  } else if (OB_FAIL(get_macro_read_info(macro_list_.at(macro_idx_), macro_block_ctx, read_info))) {
    LOG_WARN("failed to get macro block meta", K(ret), "arg", macro_list_.at(macro_idx_));
  } else if (!prefetch_meta_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("prefetch_meta_ must no NULL");
  } else if (OB_ISNULL(file = file_handle_.get_storage_file())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to get pg file", K(ret), K(file_handle_));
  } else if (FALSE_IT(read_handle_[handle_idx_].set_file(file))) {
  } else if (OB_FAIL(file->async_read_block(read_info, read_handle_[handle_idx_]))) {
    STORAGE_LOG(WARN, "Fail to async read block, ", K(ret));
  } else {
    LOG_INFO("do prefetch", K(macro_idx_), K(macro_list_.count()), K(prefetch_meta_));
  }

  return ret;
}

int ObPartitionMacroBlockObProducer::get_macro_read_info(const obrpc::ObFetchMacroBlockArg& arg,
    blocksstable::ObMacroBlockCtx& macro_block_ctx, blocksstable::ObMacroBlockReadInfo& read_info)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (arg.macro_block_index_ < 0 || arg.macro_block_index_ >= sstable_->get_total_macro_blocks().count()) {
    ret = OB_ERR_SYS;
    LOG_WARN("macro block index is out of range",
        K(ret),
        K(arg),
        "sstable macro_block_count",
        sstable_->get_total_macro_blocks().count());
  } else if (OB_FAIL(sstable_->get_combine_macro_block_ctx(arg.macro_block_index_, macro_block_ctx))) {
    LOG_WARN("Failed to get combined_macro_block_ctx", K(ret), K(arg));
  } else if (OB_FAIL(sstable_->get_meta(macro_block_ctx.get_macro_block_id(), prefetch_meta_))) {
    LOG_WARN("fail to get meta", K(ret), K(macro_block_ctx));
  } else if (!prefetch_meta_.is_valid()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("failed to get macro block meta image", K(ret), K(macro_block_ctx));
  } else if (prefetch_meta_.meta_->data_seq_ != arg.data_seq_ ||
             prefetch_meta_.meta_->data_version_ != arg.data_version_) {
    ret = OB_ERR_SYS;
    LOG_ERROR("meta data not match arg", K(ret), K(prefetch_meta_), K(arg));
  } else {
    read_info.macro_block_ctx_ = &macro_block_ctx;
    read_info.offset_ = 0;
    read_info.size_ = prefetch_meta_.meta_->occupy_size_;
    read_info.io_desc_.category_ = SYS_IO;
    read_info.io_desc_.wait_event_no_ = ObWaitEventIds::DB_FILE_MIGRATE_READ;
  }
  return ret;
}

int ObLogicBaseMetaProducer::init(ObPartitionService* partition_service, ObFetchLogicBaseMetaArg* arg)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(partition_service) || OB_ISNULL(arg)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(partition_service), K(arg));
  } else {
    partition_service_ = partition_service;
    arg_ = arg;
    is_inited_ = true;
  }
  return ret;
}

int ObLogicBaseMetaProducer::get_logic_endkey_list(common::ObIArray<common::ObStoreRowkey>& end_key_list)
{
  int ret = OB_SUCCESS;
  ObTableHandle tmp_handle;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  }

  if (OB_SUCC(ret)) {
    // TODO: should split to more range
    end_key_list.reuse();
    ObStoreRowkey tmp_endkey;
    tmp_endkey.set_max();
    if (OB_FAIL(end_key_list.push_back(tmp_endkey))) {
      LOG_WARN("failed to push back", K(ret));
    } else {
      LOG_INFO("succ to get logic table meta", K(arg_), K(ret));
    }
  }

  return ret;
}

int ObLogicBaseMetaReader::init(ObPartitionServiceRpcProxy& srv_rpc_proxy, ObInOutBandwidthThrottle& bandwidth_throttle,
    const ObAddr& addr, const ObFetchLogicBaseMetaArg& rpc_arg, const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("can not init twice", K(ret));
  } else if (OB_FAIL(rpc_reader_.init(bandwidth_throttle))) {
    LOG_WARN("failed to init rpc_reader", K(ret));
  } else {
    LOG_INFO("init fetch logic base meta reader", K(addr), K(rpc_arg));
    if (OB_FAIL(srv_rpc_proxy.to(addr)
                    .by(OB_DATA_TENANT_ID)
                    .dst_cluster_id(cluster_id)
                    .fetch_logic_base_meta(rpc_arg, rpc_reader_.get_rpc_buffer(), rpc_reader_.get_handle()))) {
      LOG_WARN("failed to send fetch base data meta rpc", K(ret), K(addr), K(rpc_arg), K(cluster_id));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObLogicBaseMetaReader::fetch_end_key_list(common::ObIArray<common::ObStoreRowkey>& end_key_list)
{
  int ret = OB_SUCCESS;
  int64_t end_key_count = 0;

  if (OB_FAIL(rpc_reader_.fetch_and_decode(end_key_count))) {
    LOG_WARN("failed to end key count");
  } else if (end_key_count <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("end_key_count should not be null", K(ret));
  } else {
    end_key_list.reserve(end_key_count);
    if (OB_FAIL(rpc_reader_.fetch_and_decode_list(allocator_, end_key_list))) {
      LOG_WARN("failed to decode end_key_list", K(ret));
    } else if (end_key_count != end_key_list.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("end key count not matched", K(end_key_count), K(end_key_list.count()), K(ret));
    }
  }
  return ret;
}

ObLogicRowProducer::ObLogicRowProducer()
    : is_inited_(false),
      data_checksum_calc_(),
      tables_handle_(),
      start_key_buf_(OB_MALLOC_NORMAL_BLOCK_SIZE),
      range_(),
      guard_(),
      allocator_(ObNewModIds::OB_PARTITION_MIGRATE),
      storage_(NULL),
      table_schema_(NULL),
      memtable_(NULL),
      arg_(),
      ms_row_iterator_(),
      need_check_memtable_(false),
      can_reset_row_iter_(false),
      mem_ctx_factory_(NULL),
      start_time_(0),
      end_time_(0),
      memtable_pkey_()
{}

int ObLogicRowProducer::init(ObPartitionService* partition_service, ObFetchLogicRowArg* arg)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("logic row produce init twice", K(ret));
  } else if (OB_ISNULL(partition_service) || OB_ISNULL(arg) || OB_ISNULL(partition_service->get_mem_ctx_factory())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(partition_service), K(arg));
  } else if (OB_FAIL(arg->deep_copy(allocator_, arg_))) {
    LOG_WARN("fail to deep copy arg", K(ret));
  } else {
    int64_t save_schema_version = -1;
    range_.reset();
    range_.get_range() = arg_.key_range_;
    mem_ctx_factory_ = partition_service->get_mem_ctx_factory();
    if (OB_FAIL(GCTX.schema_service_->retry_get_schema_guard(
            arg->schema_version_, arg->table_key_.table_id_, schema_guard_, save_schema_version))) {
      LOG_WARN("fail to get schema guard", K(ret), K(arg->schema_version_));
    } else if (arg->schema_version_ != save_schema_version) {
      ret = OB_SCHEMA_ERROR;
      LOG_ERROR("scheam version not match", K(ret), K(arg_), K(save_schema_version));
    } else if (OB_FAIL(schema_guard_.get_table_schema(arg->table_key_.table_id_, table_schema_))) {
      LOG_WARN("fail to get table schema", K(ret), KP(table_schema_), K(arg->schema_version_));
    } else if (OB_ISNULL(table_schema_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema should not be NULL", K(ret), KP(table_schema_));
    } else if (OB_FAIL(partition_service->get_partition(arg->table_key_.pkey_, pg_guard_)) ||
               OB_ISNULL(pg_guard_.get_partition_group())) {
      LOG_WARN("fail to get partition group", K(ret), "pkey", arg->table_key_.pkey_);
    } else if (OB_FAIL(pg_guard_.get_partition_group()->get_pg_partition(arg->table_key_.pkey_, guard_)) ||
               OB_ISNULL(guard_.get_pg_partition())) {
      LOG_WARN("fail to get pg partition", K(ret));
    } else if (OB_ISNULL(storage_ = reinterpret_cast<ObPartitionStorage*>(guard_.get_pg_partition()->get_storage()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition storage should not be NULL", K(ret));
    } else if (OB_FAIL(start_key_buf_.ensure_space(OB_MALLOC_NORMAL_BLOCK_SIZE, ObModIds::OB_PARTITION_MIGRATOR))) {
      LOG_WARN("fetch logical row fail to allocator memory for last key buf.", K(ret));
    } else if (OB_FAIL(range_.to_collation_free_range_on_demand_and_cutoff_range(allocator_))) {
      STORAGE_LOG(WARN, "Failed to transform range to collation free and range cutoff", K_(range), K(ret));
    } else {
      memtable_pkey_ = pg_guard_.get_partition_group()->get_partition_key();
      if (OB_FAIL(init_logical_row_iter(arg->table_key_.trans_version_range_, range_))) {
        LOG_WARN("fail to init logical row iterator", K(ret), K(arg->table_key_.trans_version_range_), K_(range));
      } else if (OB_FAIL(data_checksum_calc_.init(table_schema_->get_rowkey_column_num(), arg->data_checksum_))) {
        LOG_WARN("fail to init data checksum calculate", K(ret));
      } else {
        start_time_ = ObTimeUtility::current_time();
        is_inited_ = true;
      }
    }
  }
  return ret;
}

int ObLogicRowProducer::init_logical_row_iter(const ObVersionRange& version_range, ObExtStoreRange& key_range)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(version_range);
  UNUSED(key_range);
  return ret;
}

int ObLogicRowProducer::set_new_key_range(ObStoreRange& new_key_range)
{
  int ret = OB_SUCCESS;
  // Set start_key_ to last_rowkey, and open left,
  // The opening and closing of the right range of new_key_range_ is the same as the incoming one
  new_key_range.set_border_flag(arg_.key_range_.get_border_flag());
  new_key_range.set_table_id(arg_.key_range_.get_table_id());
  new_key_range.set_left_open();
  new_key_range.set_end_key(arg_.key_range_.get_end_key());
  const ObStoreRowkey& last_rowkey = data_checksum_calc_.get_last_rowkey();
  ObMemBufAllocatorWrapper allocator(start_key_buf_, ObModIds::OB_PARTITION_MIGRATOR);
  if (OB_FAIL(last_rowkey.deep_copy(new_key_range.get_start_key(), allocator))) {
    LOG_WARN("failed to deep copy last rowkey", K(ret), K(last_rowkey));
  }
  return ret;
}

int ObLogicRowProducer::reset_logical_row_iter()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("logic row producer do not init", K(ret));
  } else {
    ms_row_iterator_.reuse();
    tables_handle_.reset();
    range_.reset();
    if (OB_FAIL(set_new_key_range(range_.get_range()))) {
      LOG_WARN("failed to set new key range", K(ret));
    } else if (OB_FAIL(range_.to_collation_free_range_on_demand_and_cutoff_range(allocator_))) {
      STORAGE_LOG(WARN, "Failed to transform range to collation free and range cutoff", K_(range), K(ret));
    } else {
      if (OB_FAIL(init_logical_row_iter(arg_.table_key_.trans_version_range_, range_))) {
        LOG_WARN("fail to init logical row iter", K(ret), K(range_));
      } else {
        can_reset_row_iter_ = false;
      }
    }

    if (OB_SUCC(ret)) {
      LOG_INFO("reset logic row iter", K(arg_), K_(range));
    }
  }

  return ret;
}

bool ObLogicRowProducer::can_reset_logical_row_iter()
{
  bool ret = false;
  if (ObMemtableState::MINOR_FROZEN == memtable_->get_memtable_state()) {
    if (memtable_->get_frozen_trans_version() <=
        pg_guard_.get_partition_group()->get_pg_storage().get_publish_version()) {
      // It shows that the frozen memtable has formed a dump file
      ret = true;
    }
  }
  return ret;
}

int ObLogicRowProducer::inner_get_next_row(const ObStoreRow*& store_row)
{
  int ret = OB_SUCCESS;
  ObStoreRowkey tmp_key;
  if (OB_FAIL(ms_row_iterator_.get_next_row(store_row))) {
    if (OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "failed to get next row", K(ret));
    } else {
      LOG_INFO("first compacted row count is", K(data_checksum_calc_.get_first_compacted_row_count()));
    }
  } else if (NULL == store_row) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "store row must not be NULL", K(ret), KP(store_row));
  } else if (OB_FAIL(data_checksum_calc_.append_row(store_row))) {
    LOG_WARN("fail to append row to data checksum calc", K(ret));
  } else {
    end_time_ = ObTimeUtility::current_time();
    int64_t total_time = end_time_ - start_time_;

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (need_check_memtable_ && total_time >= (10 * 1000 * 1000) &&
               !can_reset_row_iter_) {  // Need to check the status of memtable and the time interval of 10s is reached
      can_reset_row_iter_ = can_reset_logical_row_iter();
      start_time_ = end_time_;
    }
  }
  return ret;
}

int ObLogicRowProducer::get_next_row(const ObStoreRow*& store_row)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("logic row procuder do not init", K(ret));
  } else if (OB_FAIL(inner_get_next_row(store_row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to run inner get next row", K(ret));
    }
  }
  return ret;
}

int ObLogicRowProducer::get_schema_rowkey_count(int64_t& schema_rowkey_count)
{
  int ret = OB_SUCCESS;
  schema_rowkey_count = 0;
  if (!is_inited_ || OB_ISNULL(table_schema_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("logic row procuder do not init", K(ret));
  } else {
    schema_rowkey_count = table_schema_->get_rowkey_column_num();
  }
  return ret;
}

int ObLogicRowProducer::check_split_source_table(const ObTablesHandle& tables_handle_, bool& is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < tables_handle_.get_count(); i++) {
    ObITable* table = tables_handle_.get_table(i);
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (table->is_memtable()) {
      if (tables_handle_.get_table(i)->get_partition_key() != memtable_pkey_) {
        is_exist = true;
      }
    } else if (table->is_sstable()) {
      if (tables_handle_.get_table(i)->get_partition_key() != arg_.table_key_.pkey_) {
        is_exist = true;
      }
    }
  }
  return ret;
}

// TODO() not use anymore, need delete it
ObLogicDataChecksumCalculate::ObLogicDataChecksumCalculate()
    : is_inited_(false),
      data_buffer_(0, ObModIds::OB_PARTITION_MIGRATOR, false),  // Do not do byte alignment
      row_writer_(),
      last_rowkey_buf_(OB_MALLOC_NORMAL_BLOCK_SIZE),
      last_rowkey_(),
      data_checksum_(0),
      schema_rowkey_count_(0),
      first_compacted_row_count_(0)
{}

int ObLogicDataChecksumCalculate::init(const int64_t schema_rowkey_count, const int64_t data_checksum)
{
  int ret = OB_SUCCESS;
  const int64_t macro_block_size = OB_FILE_SYSTEM.get_macro_block_size();

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ob logic data checksum calculate init twice", K(ret));
  } else if (schema_rowkey_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ob logic data checksum calculate init get invalid argument", K(ret));
  } else if (OB_FAIL(data_buffer_.ensure_space(macro_block_size))) {
    LOG_WARN("data buffer fail to ensure space.", K(ret), K(macro_block_size));
  } else if (OB_FAIL(last_rowkey_buf_.ensure_space(OB_MALLOC_NORMAL_BLOCK_SIZE, ObModIds::OB_PARTITION_MIGRATOR))) {
    LOG_WARN("fetch logical row fail to allocator memory for last key buf.", K(ret));
  } else {
    schema_rowkey_count_ = schema_rowkey_count;
    data_checksum_ = data_checksum;
    is_inited_ = true;
  }
  return ret;
}

int ObLogicDataChecksumCalculate::append_row(const ObStoreRow* store_row)
{
  int ret = OB_SUCCESS;
  ObStoreRowkey tmp_key;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ob logic data check sum calculate do not init", K(ret));
  } else if (OB_ISNULL(store_row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("store row should not be null", K(ret), KP(store_row));
  } else {
    tmp_key.assign(store_row->row_val_.cells_, schema_rowkey_count_);
    // Calculate the checksum of the first compact row
    if (!last_rowkey_.is_valid()) {
      if (!store_row->row_type_flag_.is_compacted_multi_version_row() &&
          !store_row->row_type_flag_.is_uncommitted_row()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("last rowkey is invalid and store row is not compacted", K(ret), K(*store_row));
      } else if (OB_FAIL(inner_append_row(*store_row))) {
        LOG_WARN("fail to append row", K(ret));
      }
    } else if (tmp_key != last_rowkey_ && (store_row->row_type_flag_.is_compacted_multi_version_row() ||
                                              store_row->row_type_flag_.is_uncommitted_row())) {
      if (OB_FAIL(inner_append_row(*store_row))) {
        LOG_WARN("fail to append row", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      ObMemBufAllocatorWrapper allocator(last_rowkey_buf_, ObModIds::OB_PARTITION_MIGRATOR);
      if (OB_FAIL(tmp_key.deep_copy(last_rowkey_, allocator))) {
        LOG_WARN("fetch row fail to deep copy rowkey", K(ret), K(tmp_key), K(last_rowkey_));
      }
    }
  }
  return ret;
}

int ObLogicDataChecksumCalculate::inner_append_row(const ObStoreRow& store_row)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("logic row producer do not init", K(ret));
  } else {
    while (OB_SUCC(ret) && OB_FAIL(row_writer_.write(store_row.row_val_,
                               data_buffer_.current(),
                               data_buffer_.remain(),
                               store_row.is_sparse_row_ ? SPARSE_ROW_STORE : FLAT_ROW_STORE,
                               pos))) {
      if (OB_BUF_NOT_ENOUGH == ret && data_buffer_.capacity() < OB_MAX_PACKET_BUFFER_LENGTH) {
        if (OB_FAIL(data_buffer_.ensure_space(min(OB_MAX_PACKET_BUFFER_LENGTH, data_buffer_.capacity() * 4)))) {
          LOG_WARN("Failed to expand buffer size", K(ret));
        } else {
          pos = 0;
        }
      } else {
        LOG_WARN("fail to write row to data buffer", K(ret), K(store_row));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(data_buffer_.advance(pos))) {
        LOG_WARN("data buffer fail to advance.", K(ret), K(pos));
      } else {
        calc_data_checksum();
        ++first_compacted_row_count_;
      }
    }
  }

  return ret;
}

void ObLogicDataChecksumCalculate::calc_data_checksum()
{
  if (data_buffer_.length() > 0) {
    data_checksum_ = ob_crc64(data_checksum_, data_buffer_.data(), data_buffer_.length());
    data_buffer_.reuse();
  }
}

int ObPhysicalBaseMetaProducer::init(ObPartitionService* partition_service, ObFetchPhysicalBaseMetaArg* arg)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(partition_service) || OB_ISNULL(arg)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(partition_service), K(arg));
  } else {
    partition_service_ = partition_service;
    arg_ = arg;
    is_inited_ = true;
  }
  return ret;
}

int ObPhysicalBaseMetaProducer::get_sstable_meta(ObSSTableBaseMeta& sstable_meta, ObIArray<ObSSTablePair>& pair_list)
{
  int ret = OB_SUCCESS;
  ObTableHandle tmp_handle;
  ObSSTable* sstable = NULL;
  blocksstable::ObSSTablePair pair;
  blocksstable::ObMacroBlockMetaHandle meta_handle;
  ObFullMacroBlockMeta meta;
  pair_list.reuse();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(ObPartitionService::get_instance().acquire_sstable(arg_->table_key_, tmp_handle))) {
    LOG_WARN("failed to get table", K(arg_->table_key_), K(ret));
  } else if (OB_FAIL(tmp_handle.get_sstable(sstable))) {
    LOG_WARN("failed to get table", K(arg_->table_key_), K(ret));
  } else if (OB_ISNULL(sstable)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable should not be null here", K(ret));
  } else {
    auto& macro_list = sstable->get_total_macro_blocks();

    // use value copy instead reference, because we do not serialize macro_block_array_
    if (OB_FAIL(sstable_meta.assign(sstable->get_meta()))) {
      LOG_WARN("fail to assign sstable meta", K(ret));
    } else if (!sstable_meta.is_valid()) {
      ret = OB_ERR_SYS;
      LOG_ERROR("sstable meta is not valid", K(ret), K(sstable_meta));
    } else if (OB_FAIL(pair_list.reserve(macro_list.count()))) {
      LOG_WARN("failed to reserve list", K(ret), K(macro_list.count()));
    } else {
      LOG_INFO("get_sstable_meta", K(*arg_), K(sstable_meta));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < macro_list.count(); ++i) {
      if (OB_FAIL(sstable->get_meta(macro_list.at(i), meta))) {
        LOG_WARN("fail to get meta", K(ret), K(macro_list.at(i)));
      } else if (!meta.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("meta must not null", K(ret), "macro_block_id", macro_list.at(i));
      } else {
        pair.data_seq_ = meta.meta_->data_seq_;
        pair.data_version_ = meta.meta_->data_version_;
        if (OB_FAIL(pair_list.push_back(pair))) {
          LOG_WARN("failed to add pair list", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObPhysicalBaseMetaReader::init(ObPartitionServiceRpcProxy& srv_rpc_proxy,
    ObInOutBandwidthThrottle& bandwidth_throttle, const ObAddr& addr, const ObFetchPhysicalBaseMetaArg& rpc_arg,
    const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("can not init twice", K(ret));
  } else if (OB_FAIL(rpc_reader_.init(bandwidth_throttle))) {
    LOG_WARN("failed to init rpc_reader", K(ret));
  } else {
    LOG_INFO("init physical base meta reader", K(addr), K(rpc_arg));
    if (OB_FAIL(srv_rpc_proxy.to(addr)
                    .by(OB_DATA_TENANT_ID)
                    .dst_cluster_id(cluster_id)
                    .fetch_physical_base_meta(rpc_arg, rpc_reader_.get_rpc_buffer(), rpc_reader_.get_handle()))) {
      LOG_WARN("failed to send fetch base data meta rpc", K(ret), K(addr), K(rpc_arg), K(cluster_id));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObPhysicalBaseMetaReader::fetch_sstable_meta(ObSSTableBaseMeta& sstable_meta)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(rpc_reader_.fetch_and_decode(sstable_meta))) {
    LOG_WARN("failed to fetch and decode sstable meta", K(ret));
  } else if (!sstable_meta.is_valid()) {
    ret = OB_ERR_SYS;
    LOG_ERROR("invalid sstable_meta", K(ret), K(sstable_meta));
  }

  return ret;
}

int ObPhysicalBaseMetaReader::fetch_macro_block_list(common::ObIArray<ObSSTablePair>& macro_block_list)
{
  int ret = OB_SUCCESS;
  int64_t macro_block_count = 0;

  if (OB_FAIL(rpc_reader_.fetch_and_decode(macro_block_count))) {
    LOG_WARN("failed to end key count");
  } else if (macro_block_count < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid macro_block_count", K(ret), K(macro_block_count));
  } else {
    macro_block_list.reserve(macro_block_count);
    if (OB_FAIL(rpc_reader_.fetch_and_decode_list(macro_block_list))) {
      LOG_WARN("failed to decode macro_block_list", K(ret));
    } else if (macro_block_count != macro_block_list.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("macro block count not matched", K(macro_block_count), K(macro_block_list.count()), K(ret));
    }
  }
  return ret;
}

ObLogicRowSliceReader::ObLogicRowSliceReader()
    : is_inited_(false),
      srv_rpc_proxy_(NULL),
      src_server_(),
      rpc_reader_(),
      arg_(NULL),
      store_row_(),
      last_key_buf_(OB_MALLOC_NORMAL_BLOCK_SIZE),
      last_key_(),
      schema_rowkey_cnt_(0),
      cluster_id_(OB_INVALID_CLUSTER_ID)
{}

int ObLogicRowSliceReader::init(ObPartitionServiceRpcProxy& srv_rpc_proxy, ObInOutBandwidthThrottle& bandwidth_throttle,
    const ObAddr& src_server, const ObFetchLogicRowArg& rpc_arg, const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  int64_t save_schema_version = 0;
  share::schema::ObSchemaGetterGuard schema_guard;
  const share::schema::ObTableSchema* table_schema = NULL;
  bool has_lob = false;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("can not init twice", K(ret));
  } else if (!src_server.is_valid() || !rpc_arg.is_valid() || cluster_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ob logic row reader do not init", K(ret), K(src_server), K(cluster_id));
  } else if (OB_FAIL(GCTX.schema_service_->retry_get_schema_guard(
                 rpc_arg.schema_version_, rpc_arg.table_key_.table_id_, schema_guard, save_schema_version))) {
    LOG_WARN("fail to get schema guard", K(ret), K(rpc_arg.schema_version_));
  } else if (rpc_arg.schema_version_ != save_schema_version) {
    ret = OB_SCHEMA_ERROR;
    LOG_ERROR("scheam version not match", K(ret), K(arg_), K(save_schema_version));
  } else if (OB_FAIL(schema_guard.get_table_schema(rpc_arg.table_key_.table_id_, table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), KP(table_schema), K(rpc_arg.schema_version_));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema should not be NULL", K(ret), KP(table_schema));
  } else if (OB_FAIL(table_schema->has_lob_column(has_lob, true))) {
    LOG_WARN("Failed to check lob column in table schema");
  } else if (OB_FAIL(rpc_reader_.init(bandwidth_throttle, has_lob))) {
    LOG_WARN("failed to init rpc_reader", K(ret));
  } else {
    LOG_INFO("init fetch logic row reader", K(src_server), K(rpc_arg));
    srv_rpc_proxy_ = &srv_rpc_proxy;
    src_server_ = src_server;
    cluster_id_ = cluster_id;
    if (OB_FAIL(fetch_logic_row(rpc_arg))) {
      LOG_WARN("fail to fetch logic row", K(ret), K(rpc_arg));
    } else {
      arg_ = &rpc_arg;
      schema_rowkey_cnt_ = table_schema->get_rowkey_column_num();
      store_row_.reset();
      store_row_.row_val_.assign(buff_obj_, OB_ROW_MAX_COLUMNS_COUNT);
      store_row_.column_ids_ = column_ids_;
      store_row_.capacity_ = OB_ROW_MAX_COLUMNS_COUNT;
      is_inited_ = true;
    }
  }

  LOG_INFO("finish init logic row reader", K(ret), K(rpc_arg));
  return ret;
}

int ObLogicRowSliceReader::inner_get_next_row(const ObStoreRow*& store_row)
{
  int ret = OB_SUCCESS;
  store_row_.row_val_.count_ = store_row_.capacity_;  // reset count for sparse row
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ob logic row reader do not init", K(ret));
  } else if (OB_FAIL(rpc_reader_.fetch_and_decode(store_row_))) {
    if (OB_ITER_END != ret && OB_RPC_NEED_RECONNECT != ret) {
      LOG_WARN("failed to get store row", "dst", rpc_reader_.get_dst_addr(), K(ret));
    }
  } else {
    store_row = &store_row_;
  }
  return ret;
}

int ObLogicRowSliceReader::fetch_logic_row(const ObFetchLogicRowArg& rpc_arg)
{
  int ret = OB_SUCCESS;
  rpc_reader_.reuse();

  if (OB_ISNULL(srv_rpc_proxy_) || !src_server_.is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ob logic row reader do not init", K(ret));
  } else if (!rpc_arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fetch logic row get invaid argument", K(ret), K(rpc_arg));
  } else if (OB_FAIL(srv_rpc_proxy_->to(src_server_)
                         .by(OB_DATA_TENANT_ID)
                         .timeout(MAX_LOGIC_MIGRATE_TIME_OUT)
                         .dst_cluster_id(cluster_id_)
                         .fetch_logic_row_slice(rpc_arg, rpc_reader_.get_rpc_buffer(), rpc_reader_.get_handle()))) {
    LOG_WARN("fail to fetch logic row", K(ret), K(rpc_arg), K(cluster_id_));
  } else if (OB_FAIL(rpc_reader_.decode_rpc_header())) {
    LOG_WARN("fail to decode rpc header", K(ret), K(rpc_arg));
  }
  return ret;
}

int ObLogicRowSliceReader::get_next_row(const ObStoreRow*& store_row)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ob logic row reader do not init", K(ret));
  } else if (OB_FAIL(inner_get_next_row(store_row))) {
    if (OB_ITER_END == ret) {
      // do nothing
    } else if (OB_RPC_NEED_RECONNECT == ret) {
      if (OB_FAIL(copy_rowkey())) {
        LOG_WARN("fail to copy rowkey", K(ret), K(store_row_));
      } else if (OB_FAIL(rescan(store_row))) {
        LOG_WARN("fail to rescan", K(ret));
      }
    } else {
      LOG_WARN("fail to inner get next row", K(ret));
    }
  }

  return ret;
}

int ObLogicRowSliceReader::rescan(const ObStoreRow*& store_row)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ob logic row reader do not init", K(ret));
  } else {
    ObFetchLogicRowArg new_arg;
    new_arg.schema_version_ = arg_->schema_version_;
    new_arg.data_checksum_ = arg_->data_checksum_;
    new_arg.table_key_ = arg_->table_key_;
    // Set a new range, keep it the same as the right boundary of the incoming range (same closed and open)
    // Set the left boundary of the new range to ensure that the left side is uninclusive
    new_arg.key_range_.set_border_flag(arg_->key_range_.get_border_flag());
    new_arg.key_range_.set_start_key(last_key_);
    new_arg.key_range_.set_left_open();
    new_arg.key_range_.set_end_key(arg_->key_range_.get_end_key());
    if (OB_FAIL(fetch_logic_row(new_arg))) {
      LOG_INFO("fail to fecth logic row", K(ret), K(new_arg));
    } else if (OB_FAIL(inner_get_next_row(store_row))) {
      LOG_WARN("fail to get inner get next row", K(ret));
    }
  }
  return ret;
}

int ObLogicRowSliceReader::copy_rowkey()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ob logic row reader do not init", K(ret));
  } else if (!store_row_.row_type_flag_.is_last_multi_version_row()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("store row is not last multi version row", K(ret), K(store_row_));
  } else {
    ObStoreRowkey rowkey(store_row_.row_val_.cells_, schema_rowkey_cnt_);
    ObMemBufAllocatorWrapper allocator(last_key_buf_, ObNewModIds::OB_PARTITION_MIGRATE);
    if (OB_FAIL(rowkey.deep_copy(last_key_, allocator))) {
      LOG_WARN("fail to copy rowkey", K(ret), K(rowkey));
    }
  }
  return ret;
}

int ObLogicRowReader::init(ObPartitionServiceRpcProxy& srv_rpc_proxy, ObInOutBandwidthThrottle& bandwidth_throttle,
    const ObAddr& addr, const ObFetchLogicRowArg& rpc_arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("can not init twice", K(ret));
  } else if (OB_FAIL(rpc_reader_.init(bandwidth_throttle))) {
    LOG_WARN("failed to init rpc_reader", K(ret));
  } else {
    LOG_INFO("init fetch logic row reader", K(addr), K(rpc_arg));
    if (OB_FAIL(srv_rpc_proxy.to(addr)
                    .by(OB_DATA_TENANT_ID)
                    .timeout(MAX_LOGIC_MIGRATE_TIME_OUT)
                    .fetch_logic_row(rpc_arg, rpc_reader_.get_rpc_buffer(), rpc_reader_.get_handle()))) {
      LOG_WARN("failed to send fetch logic row rpc", K(ret), K(addr), K(rpc_arg));
    } else {
      arg_ = &rpc_arg;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObLogicRowReader::get_next_row(const ObStoreRow*& store_row)
{
  int ret = OB_SUCCESS;
  store_row_.reset();
  store_row_.row_val_.assign(buff_obj_, OB_ROW_MAX_COLUMNS_COUNT);

  if (OB_FAIL(rpc_reader_.fetch_and_decode(store_row_))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get store row", "dst", rpc_reader_.get_dst_addr(), K(ret));
    }
  } else {
    store_row = &store_row_;
  }
  return ret;
}

ObLogicRowFetcher::ObLogicRowFetcher()
    : is_inited_(false), logic_row_reader_(), slice_row_reader_(), row_reader_(NULL), arg_(NULL)
{}

int ObLogicRowFetcher::init(ObPartitionServiceRpcProxy& srv_rpc_proxy, ObInOutBandwidthThrottle& bandwidth_throttle,
    const ObAddr& src_server, const ObFetchLogicRowArg& rpc_arg, const int64_t cluster_id)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(slice_row_reader_.init(srv_rpc_proxy, bandwidth_throttle, src_server, rpc_arg, cluster_id))) {
    LOG_WARN("fail to init slice row reader", K(ret), K(rpc_arg));
  } else {
    row_reader_ = &slice_row_reader_;
  }

  if (OB_NOT_SUPPORTED == ret) {
    if (OB_FAIL(logic_row_reader_.init(srv_rpc_proxy, bandwidth_throttle, src_server, rpc_arg))) {
      LOG_WARN("fail to init logic row reader", K(ret), K(rpc_arg));
    } else {
      row_reader_ = &logic_row_reader_;
    }
  }

  if (OB_SUCC(ret)) {
    arg_ = &rpc_arg;
    is_inited_ = true;
  }
  return ret;
}

int ObLogicRowFetcher::get_next_row(const ObStoreRow*& store_row)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    LOG_WARN("logic row fetcher do not init", K(ret));
  } else if (OB_FAIL(row_reader_->get_next_row(store_row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next row", K(ret));
    }
  }
  return ret;
}

ObLogicDataChecksumReader::ObLogicDataChecksumReader()
    : is_inited_(false),
      last_key_buf_(OB_MALLOC_NORMAL_BLOCK_SIZE),
      last_key_(),
      src_server_(),
      srv_rpc_proxy_(NULL),
      bandwidth_throttle_(NULL),
      arg_(NULL),
      cluster_id_(OB_INVALID_CLUSTER_ID)
{}

int ObLogicDataChecksumReader::init(ObPartitionServiceRpcProxy& srv_rpc_proxy,
    ObInOutBandwidthThrottle& bandwidth_throttle, const ObAddr& addr, const int64_t cluster_id,
    const ObFetchLogicRowArg& rpc_arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("can not init twice", K(ret));
  } else if (OB_FAIL(last_key_buf_.ensure_space(OB_MALLOC_NORMAL_BLOCK_SIZE, ObNewModIds::OB_PARTITION_MIGRATE))) {
    LOG_WARN("fail to malloc last key buf", K(ret));
  } else {
    LOG_INFO("init fetch logic data checksum reader", K(addr), K(rpc_arg));
    src_server_ = addr;
    srv_rpc_proxy_ = &srv_rpc_proxy;
    bandwidth_throttle_ = &bandwidth_throttle;
    arg_ = &rpc_arg;
    cluster_id_ = cluster_id;
    is_inited_ = true;
    LOG_INFO("finish init fetch logic data checksum reader", K(addr), K(rpc_arg));
  }
  return ret;
}

int ObLogicDataChecksumReader::get_data_checksum(int64_t& data_checksum)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("logic data checksum reader do not init", K(ret));
  } else if (OB_FAIL(get_data_checksum_with_slice(data_checksum))) {
    LOG_WARN("fail to get data checksum with slice", K(ret));
  }

  if (OB_NOT_SUPPORTED == ret) {
    // try old rpc witout slice
    if (OB_FAIL(get_data_checksum_without_slice(data_checksum))) {
      LOG_WARN("fail to get data checksum without slice", K(ret));
    }
  }
  return ret;
}

int ObLogicDataChecksumReader::get_data_checksum_without_slice(int64_t& data_checksum)
{
  int ret = OB_SUCCESS;
  bool is_finish = false;
  data_checksum = 0;
  int64_t tmp_data_checksum = 0;
  ObStreamRpcReader<obrpc::OB_FETCH_LOGIC_DATA_CHECKSUM> rpc_reader;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("logic data checksum reader do not init", K(ret));
  } else if (OB_FAIL(rpc_reader.init(*bandwidth_throttle_))) {
    LOG_WARN("fail to init rpc reader without slice", K(ret));
  } else if (OB_FAIL(srv_rpc_proxy_->to(src_server_)
                         .by(OB_DATA_TENANT_ID)
                         .timeout(MAX_LOGIC_MIGRATE_TIME_OUT)
                         .fetch_logic_data_checksum(*arg_, rpc_reader.get_rpc_buffer(), rpc_reader.get_handle()))) {
    LOG_WARN("fail to fetch logic row", K(ret), K(*arg_));
  } else {
    while (OB_SUCC(ret) && !is_finish) {
      if (OB_FAIL(rpc_reader.fetch_and_decode(is_finish))) {
        LOG_WARN("failed to get store row", "dst", rpc_reader.get_dst_addr(), K(ret));
      } else if (OB_FAIL(rpc_reader.fetch_and_decode(tmp_data_checksum))) {
        LOG_WARN("failed to get store row", "dst", rpc_reader.get_dst_addr(), K(ret));
      } else {
        data_checksum = tmp_data_checksum;
      }
    }
  }
  return ret;
}

int ObLogicDataChecksumReader::get_data_checksum_with_slice(int64_t& data_checksum)
{
  int ret = OB_SUCCESS;
  data_checksum = 0;
  int64_t slice_data_checksum = 0;
  ObLogicStreamRpcReader<obrpc::OB_FETCH_LOGIC_DATA_CHECKSUM_SLICE> rpc_reader;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ob logic data checksum reader do not init", K(ret));
  } else if (OB_FAIL(rpc_reader.init(*bandwidth_throttle_))) {
    LOG_WARN("fail to init rpc reader with slice", K(ret));
  } else if (OB_FAIL(fetch_logic_data_checksum(*arg_, rpc_reader))) {
    LOG_WARN("fail to fetch logic data checksum", K(ret), K(*arg_));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(inner_get_data_checksum(slice_data_checksum, rpc_reader))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to run inner get data checksum", K(ret));
        }
      }

      if (OB_RPC_NEED_RECONNECT == ret) {
        // reconnection
        if (OB_FAIL(rescan(slice_data_checksum, rpc_reader))) {
          LOG_WARN("fail to rescan data checksum", K(ret));
        }
      }
    }

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }

    if (OB_SUCC(ret)) {
      data_checksum = slice_data_checksum;
    }
  }
  return ret;
}

int ObLogicDataChecksumReader::inner_get_data_checksum(
    int64_t& data_checksum, ObLogicStreamRpcReader<obrpc::OB_FETCH_LOGIC_DATA_CHECKSUM_SLICE>& rpc_reader)
{
  int ret = OB_SUCCESS;
  bool is_rowkey_valid = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("logic data checksum reader do not init", K(ret));
  } else {
    ObLogicDataChecksumProtocol checksum_protocol;
    if (OB_FAIL(rpc_reader.fetch_and_decode(checksum_protocol))) {
      if (OB_ITER_END != ret && OB_RPC_NEED_RECONNECT != ret) {
        LOG_WARN("fail to get checksum protol", K(ret), "dst", rpc_reader.get_dst_addr(), K(checksum_protocol));
      }
    } else {
      data_checksum = checksum_protocol.data_checksum_;
      is_rowkey_valid = checksum_protocol.is_rowkey_valid_;
      if (is_rowkey_valid) {
        // Need to retransmit at a breakpoint
        LOG_INFO("get data checksum need re-connection with src",
            K(checksum_protocol.rowkey_),
            K(checksum_protocol.data_checksum_));
        last_key_.reset();
        ObMemBufAllocatorWrapper allocator(last_key_buf_, ObNewModIds::OB_PARTITION_MIGRATE);
        if (OB_SUCCESS != (ret = checksum_protocol.rowkey_.deep_copy(last_key_, allocator))) {
          LOG_WARN("fail to copy last key", K(ret), K(checksum_protocol.rowkey_));
        }
      }
    }
  }
  return ret;
}

int ObLogicDataChecksumReader::fetch_logic_data_checksum(
    const ObFetchLogicRowArg& rpc_arg, ObLogicStreamRpcReader<obrpc::OB_FETCH_LOGIC_DATA_CHECKSUM_SLICE>& rpc_reader)
{
  int ret = OB_SUCCESS;
  rpc_reader.reuse();
  if (OB_ISNULL(srv_rpc_proxy_) || !src_server_.is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ob logic data check sum reader do not init", K(ret));
  } else if (!rpc_arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fetch logic data checksum get invalid argument", K(ret), K(rpc_arg));
  } else if (OB_FAIL(
                 srv_rpc_proxy_->to(src_server_)
                     .by(OB_DATA_TENANT_ID)
                     .timeout(MAX_LOGIC_MIGRATE_TIME_OUT)
                     .dst_cluster_id(cluster_id_)
                     .fetch_logic_data_checksum_slice(rpc_arg, rpc_reader.get_rpc_buffer(), rpc_reader.get_handle()))) {
    LOG_WARN("fail to fetch logic row", K(ret), K(rpc_arg), K(cluster_id_));
  } else if (OB_FAIL(rpc_reader.decode_rpc_header())) {
    LOG_WARN("fail to decode rpc header", K(ret), K(rpc_arg));
  }
  return ret;
}

int ObLogicDataChecksumReader::rescan(
    int64_t& data_checksum, ObLogicStreamRpcReader<obrpc::OB_FETCH_LOGIC_DATA_CHECKSUM_SLICE>& rpc_reader)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ob logic row reader do not init", K(ret));
  } else {
    ObFetchLogicRowArg new_arg;
    new_arg.schema_version_ = arg_->schema_version_;
    new_arg.data_checksum_ = data_checksum;
    new_arg.table_key_ = arg_->table_key_;
    // Set a new range, keep it the same as the right boundary of the incoming range (same closed and open)
    // Set the left boundary of the new range to ensure that the left is uninclusiv
    new_arg.key_range_.set_border_flag(arg_->key_range_.get_border_flag());
    new_arg.key_range_.set_start_key(last_key_);
    new_arg.key_range_.set_left_open();
    new_arg.key_range_.set_end_key(arg_->key_range_.get_end_key());
    if (OB_FAIL(fetch_logic_data_checksum(new_arg, rpc_reader))) {
      LOG_INFO("fail to fecth logic row", K(ret), K(new_arg));
    } else if (OB_FAIL(inner_get_data_checksum(data_checksum, rpc_reader))) {
      LOG_WARN("fail to get inner get next row", K(ret));
    }
  }
  return ret;
}

ObPGPartitionBaseDataMetaObProducer::ObPGPartitionBaseDataMetaObProducer()
    : is_inited_(false), pg_partition_meta_info_array_(), meta_info_idx_(0)
{}

ObPGPartitionBaseDataMetaObProducer::~ObPGPartitionBaseDataMetaObProducer()
{}

int ObPGPartitionBaseDataMetaObProducer::init(const ObPGKey& pg_key, const int64_t snapshot_version,
    const bool is_only_major_sstable, const int64_t log_ts, storage::ObPartitionService* partition_service)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObPGPartitionGuard pg_partition_guard;
  ObPGStorage* pg_storage = NULL;
  ObArray<ObPartitionKey> pkey_array;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("can not inited twice", K(ret));
  } else if (!pg_key.is_valid() || OB_ISNULL(partition_service) || snapshot_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(pg_key), KP(partition_service), K(snapshot_version));
  } else if (OB_FAIL(partition_service->get_partition(pg_key, guard))) {
    LOG_WARN("fail to get partition, ", K(ret), K(pg_key));
  } else if (OB_ISNULL(guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get partition group", K(ret), K(pg_key));
  } else if (NULL == (pg_storage = &(guard.get_partition_group()->get_pg_storage()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get pg partition storage", K(ret), KP(pg_storage));
  } else if (OB_FAIL(set_all_partition_meta_info(snapshot_version, is_only_major_sstable, log_ts, pg_storage))) {
    LOG_WARN("fail to get all partition meta info", K(ret), K(pg_key), K(snapshot_version));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObPGPartitionBaseDataMetaObProducer::set_all_partition_meta_info(
    const int64_t snapshot_version, const bool is_only_major_sstable, const int64_t log_ts, ObPGStorage* pg_storage)
{
  int ret = OB_SUCCESS;
  ObPartitionArray pkeys;
  ObPartitionKey trans_table_pkey;
  int64_t trans_table_end_log_ts = 0;
  int64_t trans_table_timestamp = 0;

  if (OB_ISNULL(pg_storage) || snapshot_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get all partition meta info get invalid argument", K(ret), KP(pg_storage), K(snapshot_version));
  } else if (OB_FAIL(pg_storage->get_all_pg_partition_keys(pkeys, true /*include_trans_table*/))) {
    LOG_WARN("fail to get all pg partition keys", K(ret));
  } else if (OB_FAIL(
                 pg_storage->get_trans_table_end_log_ts_and_timestamp(trans_table_end_log_ts, trans_table_timestamp))) {
    LOG_WARN("failed to get trans table end log id and timestamp", K(ret));
  } else {
    ObPGPartitionMetaInfo meta_info;
    for (int64_t i = 0; OB_SUCC(ret) && i < pkeys.count(); ++i) {
      meta_info.reset();
      const ObPartitionKey& pkey = pkeys.at(i);
      if (pkey.is_trans_table()) {
        trans_table_pkey = pkey;
      } else if (OB_FAIL(set_partition_meta_info(
                     pkey, snapshot_version, is_only_major_sstable, log_ts, pg_storage, meta_info))) {
        LOG_WARN("fail to get partition meta info", K(ret), K(pkey));
      } else if (OB_FAIL(pg_partition_meta_info_array_.push_back(meta_info))) {
        LOG_WARN("fail to push meta info into array", K(ret), K(meta_info));
      } else {
        LOG_INFO("pg partition meta info_array_count",
            K(pg_partition_meta_info_array_.count()),
            K(pg_partition_meta_info_array_.at(i)));
      }
    }
    if (OB_SUCC(ret) && trans_table_pkey.is_valid()) {
      if (OB_FAIL(set_partition_meta_info(trans_table_pkey,
              snapshot_version,
              is_only_major_sstable,
              trans_table_end_log_ts,
              pg_storage,
              meta_info))) {
        LOG_WARN("failed to set_partition_meta_info", K(ret), K(trans_table_pkey));
      } else if (OB_FAIL(pg_partition_meta_info_array_.push_back(meta_info))) {
        LOG_WARN("fail to push meta info into array", K(ret), K(meta_info));
      } else {
        LOG_INFO("pg partition meta info_array_count", K(pg_partition_meta_info_array_.count()), K(meta_info));
      }
    }
  }
  return ret;
}

int ObPGPartitionBaseDataMetaObProducer::set_partition_meta_info(const ObPartitionKey& pkey,
    const int64_t snapshot_version, bool is_only_major_sstable, const int64_t log_ts, ObPGStorage* pg_storage,
    ObPGPartitionMetaInfo& meta_info)
{
  int ret = OB_SUCCESS;

  if (!pkey.is_valid() || OB_ISNULL(pg_storage) || snapshot_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get partition meta info get invalid argument", K(ret), K(pkey), KP(pg_storage), K(snapshot_version));
  } else if (OB_FAIL(pg_storage->get_pg_partition_store_meta(pkey, meta_info.meta_))) {
    LOG_WARN("fail to get partition store meta", K(ret), K(pkey));
  } else if (OB_FAIL(pg_storage->get_migrate_table_ids(pkey, meta_info.table_id_list_))) {
    LOG_WARN("fail to get migrate table ids", K(ret), K(pkey));
  } else {
    ObFetchTableInfoResult table_info;
    const int64_t multi_version_start = meta_info.meta_.multi_version_start_;
    for (int64_t i = 0; OB_SUCC(ret) && i < meta_info.table_id_list_.count(); ++i) {
      table_info.reset();
      const uint64_t table_id = meta_info.table_id_list_.at(i);
      if (OB_FAIL(set_table_info(
              pkey, table_id, multi_version_start, is_only_major_sstable, log_ts, pg_storage, table_info))) {
        LOG_WARN("fail to get table info", K(ret), K(pkey), K(table_id));
      } else if (OB_FAIL(meta_info.table_info_.push_back(table_info))) {
        LOG_WARN("fail to push table info into array", K(ret), K(table_info));
      }
    }
  }
  return ret;
}

int ObPGPartitionBaseDataMetaObProducer::set_table_info(const ObPartitionKey& pkey, const uint64_t table_id,
    const int64_t multi_version_start, const bool is_only_major_sstable, const int64_t log_ts, ObPGStorage* pg_storage,
    ObFetchTableInfoResult& table_info)
{
  int ret = OB_SUCCESS;
  ObTablesHandle handle;
  ObTablesHandle gc_handle;

  if (!pkey.is_valid() || OB_ISNULL(pg_storage) || OB_INVALID_ID == table_id || multi_version_start < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get partition meta info get invalid argument",
        K(ret),
        K(pkey),
        KP(pg_storage),
        K(table_id),
        K(multi_version_start));
  } else if (OB_FAIL(pg_storage->get_partition_migrate_tables(pkey, table_id, handle, table_info.is_ready_for_read_))) {
    LOG_WARN("failed to get migrate tables", K(ret), K(pkey), K(table_id));
  } else if (OB_FAIL(ObTableKeyMgrUtil::convert_src_table_keys(
                 log_ts, table_id, is_only_major_sstable, handle, table_info.table_keys_))) {
    LOG_WARN("failed to convert src table keys", K(ret), K(table_id), K(is_only_major_sstable), K(handle), K(log_ts));
  } else if (!pkey.is_trans_table()) {
    if (OB_FAIL(pg_storage->get_partition_gc_tables(pkey, table_id, gc_handle))) {
      LOG_WARN("fail to get partition gc tables", K(ret), K(pkey), K(table_id));
    } else if (OB_FAIL(ObTableKeyMgrUtil::convert_src_table_keys(
                   log_ts, table_id, is_only_major_sstable, gc_handle, table_info.gc_table_keys_))) {
      LOG_WARN("failed to convert src gc table keys",
          K(ret),
          K(table_id),
          K(is_only_major_sstable),
          K(gc_handle),
          K(log_ts));
    }
  }

  if (OB_SUCC(ret)) {
    table_info.multi_version_start_ = multi_version_start;
    STORAGE_LOG(INFO, "succeed to fetch table info", K(pkey), K(table_info), K(handle), K(gc_handle));
  }
  return ret;
}

int ObPGPartitionBaseDataMetaObProducer::get_next_partition_meta_info(
    const ObPGPartitionMetaInfo*& pg_partition_meta_info)
{
  int ret = OB_SUCCESS;
  pg_partition_meta_info = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (pg_partition_meta_info_array_.count() < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pg partition meta info array should not smaller than 0",
        K(ret),
        "count",
        pg_partition_meta_info_array_.count());
  } else if (meta_info_idx_ > pg_partition_meta_info_array_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta info idx should smaller than pg partition meta info array count",
        K(ret),
        K(meta_info_idx_),
        "count",
        pg_partition_meta_info_array_.count());
  } else if (pg_partition_meta_info_array_.count() == meta_info_idx_) {
    ret = OB_ITER_END;
  } else {
    pg_partition_meta_info = &pg_partition_meta_info_array_.at(meta_info_idx_);
    ++meta_info_idx_;
  }
  return ret;
}

ObPGPartitionBaseDataMetaObReader::ObPGPartitionBaseDataMetaObReader() : is_inited_(false), rpc_reader_()
{}

ObPGPartitionBaseDataMetaObReader::~ObPGPartitionBaseDataMetaObReader()
{}

int ObPGPartitionBaseDataMetaObReader::init(obrpc::ObPartitionServiceRpcProxy& srv_rpc_proxy,
    common::ObInOutBandwidthThrottle& bandwidth_throttle, const ObAddr& addr,
    const obrpc::ObFetchPGPartitionInfoArg& rpc_arg, const int64_t cluster_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("can not init twice", K(ret));
  } else if (OB_UNLIKELY(!addr.is_valid()) || OB_UNLIKELY(!rpc_arg.is_valid()) || cluster_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(addr), K(rpc_arg), K(cluster_id));
  } else if (OB_FAIL(rpc_reader_.init(bandwidth_throttle))) {
    LOG_WARN("fail to init pg partition info rpc reader", K(ret));
  } else if (OB_FAIL(srv_rpc_proxy.to(addr)
                         .by(OB_DATA_TENANT_ID)
                         .timeout(FETCH_BASE_META_TIMEOUT)
                         .dst_cluster_id(cluster_id)
                         .fetch_pg_partition_info(rpc_arg, rpc_reader_.get_rpc_buffer(), rpc_reader_.get_handle()))) {
    LOG_WARN("failed to send fetch pg partition info rpc", K(ret), K(addr), K(rpc_arg));
  } else {
    is_inited_ = true;
    LOG_INFO("succeed to init fetch_pg partition_meta_info", K(addr), K(rpc_arg));
  }

  return ret;
}

int ObPGPartitionBaseDataMetaObReader::fetch_pg_partition_meta_info(ObPGPartitionMetaInfo& partition_meta_info)
{
  int ret = OB_SUCCESS;
  partition_meta_info.reset();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg base data meta ob reader do not init", K(ret));
  } else if (OB_FAIL(rpc_reader_.fetch_and_decode(partition_meta_info))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to fetch and decode partition meta info", K(ret));
    }
  } else if (!partition_meta_info.is_valid()) {
    ret = OB_ERR_SYS;
    LOG_ERROR("invalid partition meta info", K(ret), K(partition_meta_info));
  }
  return ret;
}

ObTailoredRowIterator::ObTailoredRowIterator()
    : ObILogicRowIterator(),
      is_inited_(false),
      row_iter_(),
      allocator_(),
      schema_guard_(),
      table_schema_(NULL),
      snapshot_version_(0)
{}

int ObTailoredRowIterator::init(const uint64_t index_id, const ObPartitionKey& pg_key, const int64_t schema_version,
    const ObITable::TableKey& table_key, ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tailored row iterator init twice", K(ret));
  } else if (OB_INVALID_ID == index_id || handle.empty() || !pg_key.is_valid() || !table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN(
        "tailored row iter init get invalid argument", K(ret), K(index_id), K(pg_key), K(table_key), K(schema_version));
  } else {
    ObExtStoreRange key_range;
    key_range.get_range().set_whole_range();
    ObVersionRange version_range;
    version_range.base_version_ = 0;
    version_range.multi_version_start_ = 0;
    version_range.snapshot_version_ = table_key.trans_version_range_.snapshot_version_;
    snapshot_version_ = table_key.trans_version_range_.snapshot_version_;
    int64_t save_schema_version = -1;
    memtable::ObIMemtableCtxFactory* mem_ctx_factory = ObPartitionService::get_instance().get_mem_ctx_factory();

    if (OB_FAIL(key_range.to_collation_free_range_on_demand_and_cutoff_range(allocator_))) {
      STORAGE_LOG(WARN, "Failed to transform range to collation free and range cutoff", K(key_range), K(ret));
    } else if (OB_FAIL(GCTX.schema_service_->retry_get_schema_guard(
                   schema_version, index_id, schema_guard_, save_schema_version))) {
      LOG_WARN("fail to get schema guard", K(ret), K(schema_version), K(index_id));
    } else if (save_schema_version < schema_version) {
      ret = OB_SCHEMA_ERROR;
      LOG_ERROR("scheam version not match", K(ret), K(index_id), K(save_schema_version), K(schema_version));
    } else if (OB_FAIL(schema_guard_.get_table_schema(index_id, table_schema_))) {
      LOG_WARN("fail to get table schema", K(ret), K(index_id), KP(table_schema_), K(schema_version));
    } else if (OB_ISNULL(table_schema_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema should not be NULL", K(ret), KP(table_schema_));
    } else if (OB_FAIL(row_iter_.init(handle, *table_schema_, key_range, version_range, mem_ctx_factory, pg_key))) {
      LOG_WARN("fail to init ms row iterator", K(ret), K(key_range));
    } else if (OB_FAIL(arg_.key_range_.deep_copy(allocator_, key_range.get_range()))) {
      LOG_WARN("failed to deep copy key range", K(ret), K(key_range));
    } else {
      arg_.schema_version_ = schema_version;
      arg_.table_key_ = table_key;
      is_inited_ = true;
      LOG_INFO("succeed init tailored row iter", K(index_id), K(table_key), K(schema_version), K(handle));
    }
  }
  return ret;
}

int ObTailoredRowIterator::get_next_row(const ObStoreRow*& store_row)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tailored row iter do not init", K(ret));
  } else if (OB_FAIL(row_iter_.get_next_row(store_row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next row", K(ret));
    }
  } else {
    if (arg_.table_key_.pkey_.is_trans_table()) {
      transaction::ObTransSSTableDurableCtxInfo ctx_info;
      int64_t old_pos = 0;
      // cells_[0] stores trans_id. cells_[1] stores transaction information
      if (OB_FAIL(ctx_info.deserialize(
              store_row->row_val_.cells_[1].v_.string_, store_row->row_val_.cells_[1].val_len_, old_pos))) {
        STORAGE_LOG(WARN, "failed to deserialize ctx_info", K(ret), K(ctx_info));
      } else if (ctx_info.trans_table_info_.is_commit()) {
        if (ctx_info.trans_table_info_.get_trans_version() > snapshot_version_) {
          int64_t old_serialize_size = ctx_info.get_serialize_size();
          ctx_info.trans_table_info_.set_status(transaction::ObTransTableStatusType::ABORT);
          int64_t new_pos = 0;
          int64_t new_serialize_size = ctx_info.get_serialize_size();
          const char* buf = store_row->row_val_.cells_[1].v_.string_;
          if (old_serialize_size != new_serialize_size) {
            ret = OB_ERR_UNEXPECTED;
            TRANS_LOG(ERROR,
                "unexpected error, serialize_size should be same",
                K(ret),
                K(old_serialize_size),
                K(new_serialize_size));
          } else if (OB_FAIL(ctx_info.serialize(const_cast<char*>(buf), new_serialize_size, new_pos))) {
            TRANS_LOG(WARN, "failed to serialize status info", K(ret), K(ctx_info), K(new_pos));
          } else if (old_pos != new_pos) {
            ret = OB_ERR_UNEXPECTED;
            TRANS_LOG(ERROR, "unexpected error, pos should be same", K(ret), K(old_pos), K(new_pos));
          }
        }
      }
    }
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
