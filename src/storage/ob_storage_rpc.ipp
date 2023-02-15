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

#ifndef OCEANBASE_STORAGE_RPC_IPP_
#define OCEANBASE_STORAGE_RPC_IPP_

namespace oceanbase
{
using namespace common;
using namespace obrpc;
using namespace blocksstable;
namespace storage
{
template <ObRpcPacketCode RPC_CODE>
ObStorageStreamRpcReader<RPC_CODE>::ObStorageStreamRpcReader() : is_inited_(false)
                                                 , bandwidth_throttle_(NULL)
                                                 , rpc_buffer_()
                                                 , rpc_buffer_parse_pos_(0)
                                                 , allocator_("PartitionMigrat")
                                                 , last_send_time_(0)
                                                 , data_size_(0)
{
}

template <ObRpcPacketCode RPC_CODE>
int ObStorageStreamRpcReader<RPC_CODE>::init(ObInOutBandwidthThrottle &bandwidth_throttle)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "can not init twice", K(ret));
  } else {
    bandwidth_throttle_ = &bandwidth_throttle;
  }

  if (OB_SUCC(ret)) {
    if (NULL == (buf = reinterpret_cast<char*>(allocator_.alloc(OB_MALLOC_BIG_BLOCK_SIZE)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to alloc buf", K(ret));
    } else if (!rpc_buffer_.set_data(buf, OB_MALLOC_BIG_BLOCK_SIZE)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to set rpc buffer", K(ret));
    } else {
      is_inited_ = true;
    }
  }

  return ret;
}

template <ObRpcPacketCode RPC_CODE>
int ObStorageStreamRpcReader<RPC_CODE>::fetch_next_buffer_if_need()
{
  int ret = OB_SUCCESS;
  bool need_fetch = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_FAIL(check_need_fetch_next_buffer(need_fetch))) {
    STORAGE_LOG(WARN, "check need fetch next buffer failed", K(ret), K(need_fetch));
  } else if (need_fetch && OB_FAIL(fetch_next_buffer())) {
    if (OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "fail to fetch next buffer", K(ret), K(need_fetch), K(rpc_buffer_), K(rpc_buffer_parse_pos_));
    }
  }
  return ret;
}

template <ObRpcPacketCode RPC_CODE>
template <typename Data>
int ObStorageStreamRpcReader<RPC_CODE>::fetch_and_decode(Data &data)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_FAIL(fetch_next_buffer_if_need())) {
    if (OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "failed to fetch buffer", K(ret));
    }
  } else if (OB_FAIL(serialization::decode(rpc_buffer_.get_data(),
                                           rpc_buffer_.get_position(),
                                           rpc_buffer_parse_pos_,
                                           data))) {
    STORAGE_LOG(WARN, "failed to decode", K_(rpc_buffer), K_(rpc_buffer_parse_pos), K(ret));
  } else {
    STORAGE_LOG(INFO, "decode data",K(rpc_buffer_), K(rpc_buffer_parse_pos_), K(data));
  }
  return ret;
}

template <ObRpcPacketCode RPC_CODE>
template <typename Data>
int ObStorageStreamRpcReader<RPC_CODE>::fetch_and_decode(ObIAllocator& allocator, Data &data)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_FAIL(fetch_next_buffer_if_need())) {
    if (OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "failed to fetch buffer", K(ret));
    }
  } else if (OB_FAIL(data.deserialize(allocator,
                                      rpc_buffer_.get_data(),
                                      rpc_buffer_.get_position(),
                                      rpc_buffer_parse_pos_))) {
    STORAGE_LOG(WARN, "failed to decode", K_(rpc_buffer), K_(rpc_buffer_parse_pos), K(ret));
  }
  return ret;
}

template <ObRpcPacketCode RPC_CODE>
template <typename Data>
int ObStorageStreamRpcReader<RPC_CODE>::fetch_and_decode_list(ObIAllocator& allocator,
                                                       ObIArray<Data> &data_list)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else {
    Data tmp_data;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(fetch_next_buffer_if_need())) {
        if (OB_ITER_END != ret) {
          STORAGE_LOG(WARN, "failed to fetch buffer", K(ret));
        }
      } else if (OB_FAIL(tmp_data.deserialize(allocator,
                                              rpc_buffer_.get_data(),
                                              rpc_buffer_.get_position(),
                                              rpc_buffer_parse_pos_))) {
        STORAGE_LOG(WARN, "failed to decode", K(rpc_buffer_), K(ret));
      } else if (OB_FAIL(data_list.push_back(tmp_data))) {
        STORAGE_LOG(WARN, "failed to push back", K(ret));
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
int ObStorageStreamRpcReader<RPC_CODE>::fetch_and_decode_list(const int64_t data_list_count, ObIArray<Data> &data_list)
{
  int ret = OB_SUCCESS;
  int64_t index = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (data_list_count < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "fetch and decode list get invalid argument", K(ret), K(data_list_count));
  } else {
    Data tmp_data;
    while (OB_SUCC(ret) && index < data_list_count) {
      if (OB_FAIL(fetch_next_buffer_if_need())) {
        if (OB_ITER_END != ret) {
          STORAGE_LOG(WARN, "failed to fetch buffer", K(ret));
        }
      } else if (OB_FAIL(tmp_data.deserialize(rpc_buffer_.get_data(),
                                              rpc_buffer_.get_position(),
                                              rpc_buffer_parse_pos_))) {
        STORAGE_LOG(WARN, "failed to decode", K(rpc_buffer_), K(ret), K(data_list));
      } else if (OB_FAIL(data_list.push_back(tmp_data))) {
        STORAGE_LOG(WARN, "failed to push back", K(ret));
      } else {
        index++;
      }
    }
  }

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  
  if (OB_SUCC(ret)) {
    if (data_list.count() != data_list_count) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "data list count is unexpected", K(ret), K(data_list), K(data_list_count));
    }
  }
  return ret;
}

template <ObRpcPacketCode RPC_CODE>
int ObStorageStreamRpcReader<RPC_CODE>::check_need_fetch_next_buffer(bool &need_fetch)
{
  int ret = OB_SUCCESS;
  need_fetch = false;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (rpc_buffer_parse_pos_ < 0 || last_send_time_ < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(rpc_buffer_parse_pos_), K(last_send_time_));
  } else if (rpc_buffer_.get_position() - rpc_buffer_parse_pos_ > 0) {
    // do nothing
    need_fetch = false;
    STORAGE_LOG(DEBUG, "has left data, no need to get more", K(rpc_buffer_), K(rpc_buffer_parse_pos_), K(need_fetch));
  } else {
    need_fetch = true;
  }
  return ret;
}

template <ObRpcPacketCode RPC_CODE>
int ObStorageStreamRpcReader<RPC_CODE>::fetch_next_buffer()
{
  int ret = OB_SUCCESS;
  const int64_t max_idle_time = OB_DEFAULT_STREAM_WAIT_TIMEOUT - OB_DEFAULT_STREAM_RESERVE_TIME;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else {
    int tmp_ret = bandwidth_throttle_->limit_in_and_sleep(rpc_buffer_.get_position(),
                                                          last_send_time_, max_idle_time);
    if (OB_SUCCESS != tmp_ret) {
      STORAGE_LOG(WARN, "failed to sleep_for_bandlimit", K(tmp_ret));
    }

    rpc_buffer_.get_position() = 0;
    rpc_buffer_parse_pos_ = 0;
    if (handle_.has_more()) {
      if (OB_FAIL(handle_.get_more(rpc_buffer_))) {
        STORAGE_LOG(WARN, "get_more(send request) failed", K(ret));
      } else if (rpc_buffer_.get_position() <= 0) {
        ret = OB_ERR_SYS;
        STORAGE_LOG(ERROR, "rpc buffer has no data", K(ret), K(rpc_buffer_));
      } else {
        STORAGE_LOG(DEBUG, "get more data", K(rpc_buffer_), K(rpc_buffer_parse_pos_));
        data_size_ += rpc_buffer_.get_position();
      }
      last_send_time_ = ObTimeUtility::current_time();
    } else {
      ret = OB_ITER_END;
      STORAGE_LOG(INFO, "no more data", K(rpc_buffer_), K(rpc_buffer_parse_pos_));
    }
  }
  return ret;
}

} // End of namespace storage
} // End of namespace oceanbase

#endif // OCEANBASE_STORAGE_RPC_IPP_
