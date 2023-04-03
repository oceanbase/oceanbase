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

#ifndef OB_ALL_VIRTUAL_DTL_CHANNEL_H
#define OB_ALL_VIRTUAL_DTL_CHANNEL_H

#include "sql/dtl/ob_dtl_channel.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "common/row/ob_row.h"

namespace oceanbase
{
namespace observer
{

class ObVirtualChannelInfo
{
public:
  ObVirtualChannelInfo() :
    is_local_(false), is_data_(false), is_transmit_(false), channel_id_(0), op_id_(-1), peer_id_(0), tenant_id_(0), alloc_buffer_cnt_(0),
    free_buffer_cnt_(0), send_buffer_cnt_(0), recv_buffer_cnt_(0), processed_buffer_cnt_(0), send_buffer_size_(0),
    hash_val_(0), buffer_pool_id_(0), pins_(0), first_in_ts_(0), first_out_ts_(0), last_in_ts_(0), last_out_ts_(0),
    state_(0), thread_id_(0), owner_mod_(0), peer_(), eof_(false)
  {}

  void get_info(sql::dtl::ObDtlChannel* ch);

  TO_STRING_KV(K(channel_id_), K(op_id_), K(peer_id_), K(tenant_id_));
public:
  bool is_local_;                 // 1
  bool is_data_;
  bool is_transmit_;
  uint64_t channel_id_;
  int64_t op_id_;                // 5
  int64_t peer_id_;
  uint64_t tenant_id_;
  int64_t alloc_buffer_cnt_;
  int64_t free_buffer_cnt_;
  int64_t send_buffer_cnt_;       // 10
  int64_t recv_buffer_cnt_;
  int64_t processed_buffer_cnt_;
  int64_t send_buffer_size_;
  int64_t hash_val_;
  int64_t buffer_pool_id_;        // 15
  int64_t pins_;
  int64_t first_in_ts_;
  int64_t first_out_ts_;
  int64_t last_in_ts_;
  int64_t last_out_ts_;           // 20
  int64_t state_;
  int64_t thread_id_;
  int64_t owner_mod_;
  ObAddr peer_;
  bool eof_;
};

class ObVirtualDtlChannelOp
{
public:
  explicit ObVirtualDtlChannelOp(common::ObArray<ObVirtualChannelInfo, common::ObWrapperAllocator> *channels):
    channels_(channels)
  {}
  ~ObVirtualDtlChannelOp() { channels_ = nullptr; }
  int operator()(sql::dtl::ObDtlChannel *entry);

private:
  // the maxinum of get channels
  static const int64_t MAX_CHANNEL_CNT_PER_TENANT = 1000000;
  common::ObArray<ObVirtualChannelInfo, common::ObWrapperAllocator> *channels_;
};

class ObVirtualDtlChannelIterator
{
public:
  ObVirtualDtlChannelIterator(common::ObArenaAllocator *allocator);
  ~ObVirtualDtlChannelIterator() { destroy(); }

  void destroy()
  {
    channels_.reset();
    iter_allocator_ = nullptr;
    cur_nth_channel_ = 0;
  }

  void reset()
  {
    channels_.reset();
    iter_allocator_->reuse();
    cur_nth_channel_ = 0;
  }
  int init();
  int get_all_channels();

  int get_next_channel(ObVirtualChannelInfo &chan_info);
private:
  common::ObArenaAllocator *iter_allocator_;
  common::ObArray<ObVirtualChannelInfo, common::ObWrapperAllocator> channels_;
  int64_t cur_nth_channel_;
};

class ObAllVirtualDtlChannel : public common::ObVirtualTableScannerIterator
{
public:
  ObAllVirtualDtlChannel();
  ~ObAllVirtualDtlChannel() { destroy(); }

  void destroy();
  void reset();
  int inner_open();
  int inner_get_next_row(common::ObNewRow *&row);

private:
  enum STORAGE_COLUMN
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    CHANNEL_ID,
    OP_ID,
    PEER_ID,
    TENANT_ID, // OB_APP_MIN_COLUMN_ID + 5
    IS_LOCAL,
    IS_DATA,
    IS_TRANSMIT,
    ALLOC_BUFFER_CNT,
    FREE_BUFFER_CNT,      // OB_APP_MIN_COLUMN_ID + 10
    SEND_BUFFER_CNT,
    RECV_BUFFER_CNT,
    PROCESSED_BUFFER_CNT,
    SEND_BUFFER_SIZE,
    HASH_VAL,       // OB_APP_MIN_COLUMN_ID + 15
    BUFFER_POOL_ID,
    PINS,
    FIRST_IN_TS,
    FIRST_OUT_TS,
    LAST_IN_TS,         // OB_APP_MIN_COLUMN_ID + 20
    LAST_OUT_TS,
    STATE,
    THREAD_ID,
    OWNER_MOD,
    PEER_IP,              // OB_APP_MIN_COLUMN_ID + 25
    PEER_PORT,            // OB_APP_MIN_COLUMN_ID + 26
    DTL_EOF,
  };
  int get_row(ObVirtualChannelInfo &chan_info, common::ObNewRow *&row);

private:
  common::ObString ipstr_;
  int32_t port_;
  char peer_ip_buf_[common::OB_IP_STR_BUFF];
  common::ObArenaAllocator arena_allocator_;
  ObVirtualDtlChannelIterator iter_;
};

} /* namespace observer */
} /* namespace oceanbase */

#endif /* OB_ALL_VIRTUAL_DTL_CHANNEL_H */
