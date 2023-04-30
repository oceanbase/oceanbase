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

#define USING_LOG_PREFIX SQL_DTL
#include "ob_dtl_channel.h"
#include "lib/oblog/ob_log.h"
#include "lib/lock/ob_thread_cond.h"
#include "share/config/ob_server_config.h"
#include "common/row/ob_row.h"
#include "sql/dtl/ob_dtl_flow_control.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace sql {
namespace dtl {

ObDtlChannel::ObDtlChannel(uint64_t id, const common::ObAddr &peer)
    : cond_(),
      pins_(0),
      id_(id),
      done_(false),
      send_buffer_size_(GCONF.dtl_buffer_size),
      msg_watcher_(),
      peer_(peer),
      channel_loop_(nullptr),
      dfc_(nullptr),
      first_recv_msg_(true),
      channel_is_eof_(false),
      alloc_buffer_cnt_(0),
      free_buffer_cnt_(0),
      state_(DTL_CHAN_RUN),
      use_interm_result_(false),
      batch_id_(0),
      is_px_channel_(false),
      ignore_error_(false),
      register_dm_info_(),
      loop_idx_(OB_INVALID_INDEX_INT64),
      compressor_type_(common::ObCompressorType::NONE_COMPRESSOR),
      owner_mod_(DTLChannelOwner::INVALID_OWNER),
      thread_id_(0),
      enable_channel_sync_(false),
      prev_link_(nullptr),
      next_link_(nullptr)
{
  cond_.init(common::ObLatchIds::DTL_CHANNEL_WAIT);
}

int64_t ObDtlChannel::get_op_id()
{
  return nullptr != dfc_ ? dfc_->get_op_id() : -1;
}

}  // dtl
}  // sql
}  // oceanbase
