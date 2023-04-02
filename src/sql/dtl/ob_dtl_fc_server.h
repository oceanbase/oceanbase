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

#ifndef OB_DTL_FC_SERVER_H
#define OB_DTL_FC_SERVER_H

#include "lib/utility/ob_unify_serialize.h"
#include "lib/atomic/ob_atomic.h"
#include "sql/dtl/ob_dtl_flow_control.h"
#include "sql/dtl/ob_dtl_linked_buffer.h"
#include "sql/dtl/ob_dtl_tenant_mem_manager.h"
#include "sql/dtl/ob_dtl_local_first_buffer_manager.h"
#include "lib/list/ob_dlist.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_mutex.h"
#include "lib/ob_define.h"
#include "lib/utility/ob_tracepoint.h"
#include "share/config/ob_server_config.h"

namespace oceanbase {
namespace sql {
namespace dtl {

class ObTenantDfc
{
public:
  ObTenantDfc(uint64_t tenant_id);
  virtual ~ObTenantDfc();
public:
  static int mtl_init(ObTenantDfc *&tenant_dfc);
  static void mtl_destroy(ObTenantDfc *&tenant_dfc);

  OB_INLINE virtual int64_t get_max_size_per_channel();

  OB_INLINE virtual bool need_block(ObDtlFlowControl *dfc);
  OB_INLINE virtual bool can_unblock(ObDtlFlowControl *dfc);

  OB_INLINE virtual void increase(int64_t size);
  OB_INLINE virtual void decrease(int64_t size);

  OB_INLINE virtual void increase_blocked_channel_cnt();
  OB_INLINE virtual void decrease_blocked_channel_cnt(int64_t unblock_cnt);

  int register_dfc_channel(ObDtlFlowControl &dfc, ObDtlChannel* ch);
  int unregister_dfc_channel(ObDtlFlowControl &dfc, ObDtlChannel* ch);

  int deregister_dfc(ObDtlFlowControl &dfc);

public:
  // for cache first msg and release first msg
  int64_t get_hash_value(int64_t chid);
  int cache_buffer(int64_t chid, ObDtlLinkedBuffer *&data_buffer, bool attach = false);
  void check_dtl(uint64_t tenant_id);
  // for block and unblock
  int enforce_block(ObDtlFlowControl *dfc, int64_t ch_idx);
  int try_process_first_buffer(ObDtlFlowControl *dfc, int64_t ch_idx);

  int block_tenant_dfc(ObDtlFlowControl *dfc, int64_t ch_idx, int64_t size);
  int unblock_tenant_dfc(ObDtlFlowControl *dfc, int64_t ch_idx, int64_t size);
  int try_unblock_tenant_dfc(ObDtlFlowControl *dfc, int64_t ch_idx);
  int unblock_channel(ObDtlFlowControl *dfc, int64_t ch_idx);
  int unblock_channels(ObDtlFlowControl *dfc);

  OB_INLINE virtual void increase_channel_cnt(int64_t n_ch);
  OB_INLINE virtual void decrease_channel_cnt(int64_t n_ch);

  virtual void calc_max_buffer(int64_t max_parallel_cnt);
  uint64_t get_tenant_id() { return tenant_id_; }

  int64_t get_current_buffer_used() { return tenant_dfc_.get_used(); }
  int64_t get_current_blocked_cnt() { return tenant_dfc_.get_blocked_cnt(); }
  int64_t get_current_total_blocked_cnt() { return (ATOMIC_LOAD(&blocked_dfc_cnt_)); }
  int64_t get_current_buffer_cnt() { return tenant_dfc_.get_total_buffer_cnt(); }
  int64_t get_max_parallel() { return max_parallel_cnt_; }
  int64_t get_max_blocked_buffer_size() { return max_blocked_buffer_size_; }
  int64_t get_max_buffer_size() { return max_buffer_size_; }
  int64_t get_accumulated_blocked_cnt() { return tenant_dfc_.get_accumulated_blocked_cnt(); }
  int64_t get_channel_cnt() { return (ATOMIC_LOAD(&channel_total_cnt_)); }

  int get_buffer_cache(ObDtlDfoKey &key, ObDtlLocalFirstBufferCache *&buf_cache);
  int try_process_first_buffer_by_qc(ObDtlFlowControl *dfc, ObDtlChannel *ch, int64_t ch_idx, bool &got);
  int register_first_buffer_cache(ObDtlLocalFirstBufferCache *buf_cache);
  int unregister_first_buffer_cache(ObDtlDfoKey &key, ObDtlLocalFirstBufferCache *org_buf_cache);

  ObDtlLocalFirstBufferCacheManager *get_new_first_buffer_manager() { return &first_buffer_mgr_; }
  OB_INLINE ObDtlTenantMemManager *get_tenant_mem_manager() { return &tenant_mem_mgr_; }
private:
  static int init_channel_mem_manager();
  static int init_first_buffer_manager();
  int clean_on_timeout();
  void check_dtl_buffer_size();

private:
  // global data flow control
  ObDtlFlowControl tenant_dfc_;
  uint64_t tenant_id_;
  int64_t blocked_dfc_cnt_;
  int64_t channel_total_cnt_;

  int64_t max_parallel_cnt_;
  // 超过该值，则block收到的msg
  int64_t max_blocked_buffer_size_;
  // 超过该值，则不buffer数据
  int64_t max_buffer_size_;
  static const int64_t THRESHOLD_SIZE = 2097152;
  static const int64_t MAX_BUFFER_CNT = 3;
  static const int64_t MAX_BUFFER_FACTOR = 2;
  static const int64_t DFC_CPU_RATIO = 10;
  // static const int64_t THRESHOLD_MAX_BUFFER_SIZE = 2097152;
  // // Suppose Max buffer size = MAX_BUFFER_FACTOR * max_blocked_buffer_size_
  // static const int64_t MAX_BUFFER_FACTOR = 2;
  const double OVERSOLD_RATIO = 0.8;

  ObDtlTenantMemManager tenant_mem_mgr_;
  ObDtlLocalFirstBufferCacheManager first_buffer_mgr_;
public:
  TO_STRING_KV(K_(tenant_id), K_(blocked_dfc_cnt), K_(channel_total_cnt));
};

class ObDfcServer
{
public:
  ObDfcServer()
  {}
  ~ObDfcServer() { destroy(); }

  int init();
  void destroy();

  int block_on_increase_size(ObDtlFlowControl *dfc, int64_t ch_idx, int64_t size);
  int unblock_on_decrease_size(ObDtlFlowControl *dfc, int64_t ch_idx, int64_t size);
  int unblock_channels(ObDtlFlowControl *dfc);
  int unblock_channel(ObDtlFlowControl *dfc, int64_t ch_idx);

  int register_dfc_channel(ObDtlFlowControl &dfc, ObDtlChannel* ch);
  int unregister_dfc_channel(ObDtlFlowControl &dfc, ObDtlChannel* ch);

  int register_dfc(ObDtlFlowControl &dfc);
  int deregister_dfc(ObDtlFlowControl &dfc);

  int cache(uint64_t tenant_id, int64_t chid, ObDtlLinkedBuffer *&data_buffer, bool attach = false);
  int cache(int64_t chid, ObDtlLinkedBuffer *&data_buffer, bool attach = false);
  int try_process_first_buffer(ObDtlFlowControl *dfc, int64_t ch_idx);
  int get_buffer_cache(uint64_t tenant_id, ObDtlDfoKey &key, ObDtlLocalFirstBufferCache *&buf_cache);

  int register_first_buffer_cache(uint64_t tenant_id, ObDtlLocalFirstBufferCache *buf_cache);
  int unregister_first_buffer_cache(uint64_t tenant_id, ObDtlDfoKey &key, ObDtlLocalFirstBufferCache *org_buf_cache);

  ObDtlTenantMemManager *get_tenant_mem_manager(int64_t tenant_id);
private:
  int get_current_tenant_dfc(uint64_t tenant_id, ObTenantDfc *&tenant_dfc);
};

OB_INLINE int64_t ObTenantDfc::get_max_size_per_channel()
{
  //这里不原子拿，因为仅仅估算，感觉没有必要保证原子性
  int64_t tmp_total_cnt = channel_total_cnt_;
  int64_t tmp_blocked_dfc_cnt = blocked_dfc_cnt_;
  //并发不一致可能导致小于0
  tmp_blocked_dfc_cnt = tmp_total_cnt <= tmp_blocked_dfc_cnt ? tmp_total_cnt : tmp_blocked_dfc_cnt;
  int64_t tmp_unblock_dfc_cnt = tmp_total_cnt - tmp_blocked_dfc_cnt;
  tmp_total_cnt = 0 == tmp_total_cnt ? 1 : tmp_total_cnt;
  int64_t max_parallel_cnt = (0 == max_parallel_cnt_) ? 1 : max_parallel_cnt_;
  // 这里是为了限制一个channel使用了太多buffer
  int64_t tmp_max_size_per_channel = max_blocked_buffer_size_ / max_parallel_cnt * 2;
  int64_t max_size_per_channel = max_blocked_buffer_size_ / tmp_total_cnt * 8 / 10;
  if (0 != tmp_total_cnt && 0 != tmp_blocked_dfc_cnt) {
    max_size_per_channel = max_blocked_buffer_size_ * (1 + tmp_unblock_dfc_cnt * 8 / 10 / tmp_blocked_dfc_cnt) / tmp_total_cnt;
  }
  if (max_size_per_channel > tmp_max_size_per_channel) {
    max_size_per_channel = tmp_max_size_per_channel;
  }
  return max_size_per_channel;
}

// 这里是假设在整个workder都达到平衡状态下，由于worker数限制了dfc数量，所以每个op的dfc认为是和max_dop相关的
// 即同时receive处理时，只有max_dop的dfc需要buffer，其他时候的buffer都是多余的，而不是从channel角度看，否则channel需要的个数，就和channel平方相关，这里不假设是这个
OB_INLINE bool ObTenantDfc::need_block(ObDtlFlowControl *dfc)
{
  // first judge whether current dfc need block, reduce concurrent effects
  // then judge whether global server need block
  bool need_block = false;
  if (nullptr != dfc && dfc->need_block()) {
    int64_t max_size_per_dfc = get_max_size_per_channel() * dfc->get_channel_count();
    need_block = dfc->get_used() >= max_size_per_dfc || tenant_dfc_.get_used() >= max_buffer_size_;
  #ifdef ERRSIM
    int ret = common::OB_SUCCESS;
    ret = OB_E(EventTable::EN_FORCE_DFC_BLOCK) ret;
    need_block = (common::OB_SUCCESS != ret) ? true : need_block;
    SQL_DTL_LOG(TRACE, "trace block", K(need_block), K(ret));
    ret = common::OB_SUCCESS;
  #endif
  }
  return need_block;
}

OB_INLINE bool ObTenantDfc::can_unblock(ObDtlFlowControl *dfc)
{
  bool can_unblock = false;
  // 如果有一个channel，一直接收比较大的msg，如block等，会不会导致全部block住，只有等待该sql运行完，其他session才会继续
  if (nullptr != dfc && dfc->is_block()) {
    // 这里有几种策略进行unblocking操作
    // 1 直接根据当前dfc的状态发送unblocking msg
    // 2 如果超卖，则根据全局使用情况发送unblock msg
    // 需要注意的是，如果既需要看当前dfc满足条件，又需要满足全局使用情况，则dfc_server当满足时，对所有dfc发送unblocking状况
    // 否则会导致某个channel由于之前达到了自身可以unblock条件，由于没有发送unblocking msg，后面可能没有该channel的任何msg处理，这样就永远再发送unblocking，deadlock
    // 所以必须dfc server在达到unblocking条件时，对当前所有的dfc通知unblocking，感觉这样有点重
    // 现仅仅满足其中之一就通知unblocking
    int64_t max_size_per_dfc = get_max_size_per_channel() * dfc->get_channel_count();
    can_unblock = dfc->can_unblock() ||
      (dfc->get_used() <= max_size_per_dfc / 2 && tenant_dfc_.get_used() < max_buffer_size_);
  }
  return can_unblock;
}

OB_INLINE void ObDtlCacheBufferInfo::set_buffer(ObDtlLinkedBuffer *buffer)
{
  buffer_ = buffer;
  if (nullptr != buffer) {
    ts_ = buffer->timeout_ts();
  }
}

OB_INLINE void ObTenantDfc::increase(int64_t size)
{
  tenant_dfc_.increase(size);
}

OB_INLINE void ObTenantDfc::decrease(int64_t size)
{
  tenant_dfc_.decrease(size);
}

OB_INLINE void ObTenantDfc::increase_channel_cnt(int64_t n_ch)
{
  ATOMIC_AAF(&channel_total_cnt_, n_ch);
}

OB_INLINE void ObTenantDfc::decrease_channel_cnt(int64_t n_ch)
{
  ATOMIC_SAF(&channel_total_cnt_, n_ch);
}

OB_INLINE void ObTenantDfc::increase_blocked_channel_cnt()
{
  ATOMIC_INC(&blocked_dfc_cnt_);
  tenant_dfc_.increase_blocked_cnt(1);
}

OB_INLINE void ObTenantDfc::decrease_blocked_channel_cnt(int64_t unblock_cnt)
{
  ATOMIC_SAF(&blocked_dfc_cnt_, unblock_cnt);
  tenant_dfc_.decrease_blocked_cnt(unblock_cnt);
}

} // dtl
} // sql
} // oceanbase

#endif /* OB_DTL_FC_SERVER_H */
