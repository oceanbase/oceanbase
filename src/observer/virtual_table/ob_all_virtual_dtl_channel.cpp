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

#include "observer/virtual_table/ob_all_virtual_dtl_channel.h"
#include "sql/dtl/ob_dtl.h"
#include "sql/dtl/ob_dtl_basic_channel.h"
#include "sql/dtl/ob_dtl_tenant_mem_manager.h"
#include "sql/dtl/ob_op_metric.h"
#include "lib/allocator/ob_mod_define.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::sql::dtl;
using namespace oceanbase::observer;

void ObVirtualChannelInfo::get_info(ObDtlChannel* dtl_ch)
{
  ObDtlBasicChannel *ch = reinterpret_cast<ObDtlBasicChannel*>(dtl_ch);
  is_local_ = ObDtlChannel::DtlChannelType::LOCAL_CHANNEL == ch->get_channel_type();
  is_data_ = ch->is_data_channel();
  is_transmit_ = ch->belong_to_transmit_data();
  channel_id_ = ch->get_id();
  peer_id_ = ch->get_peer_id();;
  tenant_id_ = ch->get_tenant_id();
  alloc_buffer_cnt_ = ch->get_alloc_buffer_cnt();
  free_buffer_cnt_ = ch->get_free_buffer_cnt();
  send_buffer_cnt_ = ch->get_send_buffer_cnt();
  recv_buffer_cnt_ = ch->get_recv_buffer_cnt();
  processed_buffer_cnt_ = ch->get_processed_buffer_cnt();
  send_buffer_size_ = ch->get_send_buffer_size();
  hash_val_ = ch->get_hash_val();
  buffer_pool_id_ = ObDtlTenantMemManager::hash(hash_val_,
    common::ObServerConfig::get_instance()._px_chunklist_count_ratio);
  pins_ = ch->get_pins();
  ObOpMetric &metric = ch->get_op_metric();
  first_in_ts_ = metric.get_first_in_ts();
  first_out_ts_ = metric.get_first_out_ts();
  last_in_ts_ = metric.get_last_in_ts();
  last_out_ts_ = metric.get_last_out_ts();
  state_ = ch->get_state();
  op_id_ = ch->get_op_id();
  thread_id_ = ch->get_thread_id();
  owner_mod_ = ch->get_owner_mod();
  peer_ = ch->get_peer();
  eof_ = metric.get_eof();
}

int ObVirtualDtlChannelOp::operator()(ObDtlChannel *ch)
{
  int ret = OB_SUCCESS;
  ObVirtualChannelInfo chan_info;
  chan_info.get_info(ch);
  if (channels_->count() < MAX_CHANNEL_CNT_PER_TENANT) {
    if (OB_FAIL(channels_->push_back(chan_info))) {
      LOG_WARN("failed to push back channel info", K(ret));
    }
  }
  return ret;
}

ObVirtualDtlChannelIterator::ObVirtualDtlChannelIterator(ObArenaAllocator *allocator) :
    iter_allocator_(allocator),
    channels_(),
    cur_nth_channel_(0)
{}

int ObVirtualDtlChannelIterator::init()
{
  int ret = OB_SUCCESS;
  channels_.set_block_allocator(ObWrapperAllocator(iter_allocator_));
  if (OB_FAIL(get_all_channels())) {
    LOG_WARN("failed to get all channels", K(ret));
  }
  return ret;
}

int ObVirtualDtlChannelIterator::get_all_channels()
{
  int ret = OB_SUCCESS;
  ObVirtualDtlChannelOp op(&channels_);
  if (OB_FAIL(DTL.foreach_refactored(op))) {
    LOG_WARN("failed to get all channels", K(ret));
  }
  return ret;
}

int ObVirtualDtlChannelIterator::get_next_channel(ObVirtualChannelInfo &chan_info)
{
  int ret = OB_SUCCESS;
  if (cur_nth_channel_ < channels_.count()) {
    chan_info = channels_.at(cur_nth_channel_);
    ++cur_nth_channel_;
  } else {
    ret = OB_ITER_END;
  }
  return ret;
}

ObAllVirtualDtlChannel::ObAllVirtualDtlChannel() :
  ipstr_(),
  port_(0),
  arena_allocator_(ObModIds::OB_SQL_DTL),
  iter_(&arena_allocator_)
{}

void ObAllVirtualDtlChannel::destroy()
{
  iter_.reset();
  arena_allocator_.reuse();
  arena_allocator_.reset();
}

void ObAllVirtualDtlChannel::reset()
{
  port_ = 0;
  ipstr_.reset();
  iter_.reset();
  arena_allocator_.reuse();
  start_to_read_ = false;
}

int ObAllVirtualDtlChannel::inner_open()
{
  int ret = OB_SUCCESS;
  if (!start_to_read_) {
    if (OB_FAIL(iter_.init())) {
      LOG_WARN("failed to init iterator", K(ret));
    } else {
      start_to_read_ = true;
      char ipbuf[common::OB_IP_STR_BUFF];
      const common::ObAddr &addr = GCTX.self_addr();
      if (!addr.ip_to_string(ipbuf, sizeof(ipbuf))) {
        SERVER_LOG(ERROR, "ip to string failed");
        ret = OB_ERR_UNEXPECTED;
      } else {
        ipstr_ = ObString::make_string(ipbuf);
        if (OB_FAIL(ob_write_string(*allocator_, ipstr_, ipstr_))) {
          SERVER_LOG(WARN, "failed to write string", K(ret));
        }
        port_ = addr.get_port();
      }
    }
  }
  return ret;
}

int ObAllVirtualDtlChannel::get_row(ObVirtualChannelInfo &chan_info, ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObObj *cells = cur_row_.cells_;
  for (int64_t cell_idx = 0; OB_SUCC(ret) && cell_idx < output_column_ids_.count(); ++cell_idx) {
    uint64_t col_id = output_column_ids_.at(cell_idx);
    switch(col_id) {
      case SVR_IP: {
        cells[cell_idx].set_varchar(ipstr_);
        cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      }
      case SVR_PORT: {
        cells[cell_idx].set_int(port_);
        break;
      }
      case CHANNEL_ID: {
        cells[cell_idx].set_int(chan_info.channel_id_);
        break;
      }
      case OP_ID: {
        if (-1 != chan_info.op_id_) {
          cells[cell_idx].set_int(chan_info.op_id_);
        } else {
          cells[cell_idx].set_null();
        }
        break;
      }
      case PEER_ID: {
        cells[cell_idx].set_int(chan_info.peer_id_);
        break;
      }
      case TENANT_ID: {// OB_APP_MIN_COLUMN_ID + 5
        cells[cell_idx].set_int(chan_info.tenant_id_);
        break;
      }
      case IS_LOCAL: {
        cells[cell_idx].set_bool(chan_info.is_local_);
        break;
      }
      case IS_DATA: {
        cells[cell_idx].set_bool(chan_info.is_data_);
        break;
      }
      case IS_TRANSMIT: {
        cells[cell_idx].set_bool(chan_info.is_transmit_);
        break;
      }
      case ALLOC_BUFFER_CNT: {
        cells[cell_idx].set_int(chan_info.alloc_buffer_cnt_);
        break;
      }
      case FREE_BUFFER_CNT: {// OB_APP_MIN_COLUMN_ID + 10
        cells[cell_idx].set_int(chan_info.free_buffer_cnt_);
        break;
      }
      case SEND_BUFFER_CNT: {
        cells[cell_idx].set_int(chan_info.send_buffer_cnt_);
        break;
      }
      case RECV_BUFFER_CNT: {
        cells[cell_idx].set_int(chan_info.recv_buffer_cnt_);
        break;
      }
      case PROCESSED_BUFFER_CNT: {
        cells[cell_idx].set_int(chan_info.processed_buffer_cnt_);
        break;
      }
      case SEND_BUFFER_SIZE: {
        cells[cell_idx].set_int(chan_info.send_buffer_size_);
        break;
      }
      case HASH_VAL: {// OB_APP_MIN_COLUMN_ID + 15
        cells[cell_idx].set_int(chan_info.hash_val_);
        break;
      }
      case BUFFER_POOL_ID:{
        cells[cell_idx].set_int(chan_info.buffer_pool_id_);
        break;
      }
      case PINS: {
        cells[cell_idx].set_int(chan_info.pins_);
        break;
      }
      case FIRST_IN_TS: {
        if (0 != chan_info.first_in_ts_) {
          cells[cell_idx].set_timestamp(chan_info.first_in_ts_);
        } else {
          cells[cell_idx].set_null();
        }
        break;
      }
      case FIRST_OUT_TS: {
        if (0 != chan_info.first_out_ts_) {
          cells[cell_idx].set_timestamp(chan_info.first_out_ts_);
        } else {
          cells[cell_idx].set_null();
        }
        break;
      }
      case LAST_IN_TS: {// OB_APP_MIN_COLUMN_ID + 20
        if (0 != chan_info.last_in_ts_) {
          cells[cell_idx].set_timestamp(chan_info.last_in_ts_);
        } else {
          cells[cell_idx].set_null();
        }
        break;
      }
      case LAST_OUT_TS: {
        if (0 != chan_info.last_out_ts_) {
          cells[cell_idx].set_timestamp(chan_info.last_out_ts_);
        } else {
          cells[cell_idx].set_null();
        }
        break;
      }
      case STATE: {
        cells[cell_idx].set_int(chan_info.state_);
        break;
      }
      case THREAD_ID: {
        cells[cell_idx].set_int(chan_info.thread_id_);
        break;
      }
      case OWNER_MOD: {
        cells[cell_idx].set_int(chan_info.owner_mod_);
        break;
      }
      case PEER_IP: {// OB_APP_MIN_COLUMN_ID + 25
        const common::ObAddr &addr = chan_info.peer_;
        if (!addr.ip_to_string(peer_ip_buf_, sizeof(peer_ip_buf_))) {
          SERVER_LOG(ERROR, "ip to string failed");
          ret = OB_ERR_UNEXPECTED;
        } else {
          ObString ipstr = ObString::make_string(peer_ip_buf_);
          cells[cell_idx].set_varchar(ipstr);
        }
        break;
      }
      case PEER_PORT: {// OB_APP_MIN_COLUMN_ID + 26
        cells[cell_idx].set_int(chan_info.peer_.get_port());
        break;
      }
      case DTL_EOF: {
        cells[cell_idx].set_bool(chan_info.eof_);
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected column id", K(col_id));
      }
    }
  }
  if (OB_SUCC(ret)) {
    row = &cur_row_;
  }
  return ret;
}

int ObAllVirtualDtlChannel::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObVirtualChannelInfo ch_info;
  if (OB_FAIL(iter_.get_next_channel(ch_info))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next channel", K(ret));
    } else {
      arena_allocator_.reuse();
    }
  } else if (OB_FAIL(get_row(ch_info, row))) {
    LOG_WARN("failed to get row from channel info", K(ret));
  }
  return ret;
}
