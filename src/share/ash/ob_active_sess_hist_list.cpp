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

#include "share/ash/ob_active_sess_hist_list.h"
#include "lib/allocator/ob_malloc.h"
#include "share/config/ob_server_config.h"
#include "lib/guard/ob_shared_guard.h"          // ObShareGuard
#include "lib/ob_running_mode.h"
#include "lib/time/ob_time_utility.h"
#include "share/ash/ob_ash_refresh_task.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/tx/ob_trans_define_v4.h"
#include "storage/tx/ob_lock_diag_stmt_ring.h"

constexpr int64_t SET_COMPRESS_FLAG_THRESHOLD = 3;
constexpr int64_t RESET_COMPRESS_FLAG_THRESHOLD = 3;
constexpr double EXPECT_WRITE_SPEED_TIMES = 1.0;

namespace oceanbase
{
namespace common
{
share::ObActiveSessHistList* __attribute__((used)) lib_get_ash_list_instance() {
  return &share::ObActiveSessHistList::get_instance();
}
}
}
using namespace oceanbase::common;
using namespace oceanbase::share;

ObActiveSessHistList::ObActiveSessHistList()
    : ash_size_(0),
    mutex_(common::ObLatchIds::ASH_LOCK),
    ash_buffer_(),
    prev_write_nums_(),
    over_thread_seconds_count_(0),
    below_thread_seconds_count_(0),
    prev_write_array_index_(0),
    last_compress_num_(0),
    is_compress_(false)
{
  if (GCONF.is_valid()) {
    ash_size_ = GCONF._ob_ash_size;
  }
  if (ash_size_ == 0) {
    if (lib::is_mini_mode()) {
      ash_size_ = 10 * 1024 * 1024;  // 10M
    } else {
      ash_size_ = 30 * 1024 * 1024;  // 30M
    }
  }
}

ObActiveSessHistList& ObActiveSessHistList::get_instance()
{
  static ObActiveSessHistList the_one;
  return the_one;
}


int ObActiveSessHistList::init()
{
  int ret = OB_SUCCESS;
  if (ash_buffer_.is_valid()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ash buffer exist", KR(ret));
  } else if (OB_FAIL(mutex_.trylock())) {
    LOG_WARN("previous ash resize task is executing", KR(ret));
  } else {
    common::ObSharedGuard<ObAshBuffer> tmp;
    if (OB_FAIL(allocate_ash_buffer(ash_size_, tmp))) {
      LOG_WARN("failed to allocate ash buffer", KR(ret));
    } else {
      ash_buffer_ = tmp;
      LOG_INFO("ash buffer init OK", K_(ash_buffer));
      int64_t buffer_size = GCONF._ob_lock_diagnose_detail_buffer_num;
      if (buffer_size == 0) {
        buffer_size = lib::is_mini_mode() ? 4096 : 8192;
      }
      if (OB_FAIL(lock_wait_detail_buffer_.init(buffer_size))) {
        LOG_WARN("lock wait detail buffer init failed, lock diagnose degraded", KR(ret));
        ret = OB_SUCCESS;
      }
    }
    mutex_.unlock();
  }
  return ret;
}

int ObActiveSessHistList::resize_ash_size()
{
  int ret = OB_SUCCESS;
  int64_t ash_size = GCONF._ob_ash_size;
  if (ash_size == 0) {
    if (lib::is_mini_mode()) {
      ash_size = 10 * 1024 * 1024;  // 10M
    } else {
      ash_size = 30 * 1024 * 1024;  // 30M
    }
  }
  if (ash_size != ash_size_) {
    LockGuard lock(mutex_);
    // allocator new
    common::ObSharedGuard<ObAshBuffer> tmp;
    if (OB_FAIL(allocate_ash_buffer(ash_size, tmp))) {
      LOG_WARN("failed to allocate ash buffer", KR(ret));
    } else {
      // copy old to new
      ForwardIterator iter = create_forward_iterator_no_lock();
      while (iter.has_next()) {
        const ObActiveSessionStatItem &stat = iter.next();
        if (iter.distance() <= tmp->size()) {
          tmp->copy_from_ash_buffer(stat);
        }
      }
      // swap old with new (with mutex protection)
      LOG_INFO("successfully resize ash buffer", K(ash_size), "prev_ash_buffer", ash_buffer_.get_ptr(), "prev_size", ash_size_);
      ash_buffer_ = tmp;
      ash_size_ = ash_size;
    }
  }
  return ret;
}

int ObActiveSessHistList::allocate_ash_buffer(int64_t ash_size, common::ObSharedGuard<ObAshBuffer> &ash_buffer)
{
  int ret = OB_SUCCESS;
  ObMemAttr attr;
  attr.label_ = "ash";
  attr.ctx_id_ = ObCtxIds::DEFAULT_CTX_ID;
  attr.tenant_id_ = OB_SYS_TENANT_ID;
  if (OB_FAIL(ob_make_shared<ObAshBuffer>(ash_buffer))) {
    LOG_WARN("failed to make ash buffer", KR(ret));
  } else {
    ash_buffer->set_label("ASHListBuffer");
    ash_buffer->set_tenant_id(OB_SYS_TENANT_ID);
    if (OB_FAIL(ash_buffer->prepare_allocate(ash_size / sizeof(ObActiveSessionStatItem)))) {
      LOG_WARN("fail init ASH circular buffer", K(ret));
    } else {
      LOG_INFO("init ASH circular buffer OK", "size", ash_buffer->size());
    }
  }
  return ret;
}

double ObActiveSessHistList::cal_expect_write_speed(double times)
{
  double expect_write_speed = 0;
  double remaining_seconds = (ASH_REFRESH_INTERVAL - (ObTimeUtility::current_time() - pre_check_snapshot_time_)) / 1000000;
  if (remaining_seconds - 1e-6 > 0) {
    expect_write_speed = free_slots_num() * times / remaining_seconds;
  } else {
    expect_write_speed = free_slots_num() * times / ASH_REFRESH_INTERVAL * 1000000;
  }
  return expect_write_speed;
}

double ObActiveSessHistList::cal_avg_pre_write_num()
{
  double avg_pre_write_num = 0;
  int64_t total_write_num = 0;
  for (int64_t i = 0; i < PREV_WRITE_NUM_ARRAY_SIZE; i++) {
    total_write_num += prev_write_nums_[i];
  }
  avg_pre_write_num = total_write_num / (prev_write_array_index_ > PREV_WRITE_NUM_ARRAY_SIZE ? PREV_WRITE_NUM_ARRAY_SIZE
                                                                                             : prev_write_array_index_);
  return avg_pre_write_num;
}

void ObActiveSessHistList::check_if_need_compress()
{
  int ret = OB_SUCCESS;
  if (is_compress_ || prev_write_array_index_ == 0) {
    //do nothing
  } else {
    double expect_write_speed = cal_expect_write_speed(EXPECT_WRITE_SPEED_TIMES);
    double avg_pre_write_speed = cal_avg_pre_write_num();
    LOG_INFO("check if need compress", K(expect_write_speed), K(avg_pre_write_speed), K(is_compress_), K(over_thread_seconds_count_), K(last_compress_num_), K(SET_COMPRESS_FLAG_THRESHOLD), K(prev_write_nums_[0]), K(prev_write_nums_[1]), K(prev_write_nums_[2]), K(free_slots_num()));
    if (avg_pre_write_speed < expect_write_speed) {
      over_thread_seconds_count_ = 0;
    } else if (expect_write_speed > 0) {
      over_thread_seconds_count_ += avg_pre_write_speed / expect_write_speed;
    }
  }
  if (over_thread_seconds_count_ >= SET_COMPRESS_FLAG_THRESHOLD) {
    is_compress_ = true;
    below_thread_seconds_count_ = 0;
  }
}

void ObActiveSessHistList::check_if_can_reset_compress_flag()
{
  int ret = OB_SUCCESS;
  if (is_compress_) {
    double expect_write_speed = cal_expect_write_speed(EXPECT_WRITE_SPEED_TIMES);
    double avg_pre_write_speed = cal_avg_pre_write_num();
    LOG_INFO("check if need reset compress", K(expect_write_speed), K(avg_pre_write_speed), K(is_compress_), K(over_thread_seconds_count_), K(below_thread_seconds_count_), K(RESET_COMPRESS_FLAG_THRESHOLD), K(last_compress_num_), K(SET_COMPRESS_FLAG_THRESHOLD), K(prev_write_nums_[0]), K(prev_write_nums_[1]), K(prev_write_nums_[2]), K(free_slots_num()));
    if (avg_pre_write_speed + last_compress_num_ < expect_write_speed) {
      below_thread_seconds_count_++;
    } else {
      below_thread_seconds_count_ = 0;
    }
    if (below_thread_seconds_count_ >= RESET_COMPRESS_FLAG_THRESHOLD) {
      is_compress_ = false;
      over_thread_seconds_count_ = 0;
    }
  }
}

namespace oceanbase
{
namespace share
{

static ObLockDiagSampleStat G_LOCK_DIAG_SAMPLE_STAT;

ObLockDiagSampleStat &get_lock_diag_sample_stat()
{
  return G_LOCK_DIAG_SAMPLE_STAT;
}

ObLockWaitDetailBuffer::ObLockWaitDetailBuffer()
  : is_inited_(false), buffer_size_(0), entries_(nullptr), next_alloc_seq_(0)
{
}

ObLockWaitDetailBuffer::~ObLockWaitDetailBuffer()
{
  destroy();
}

int ObLockWaitDetailBuffer::init(const int64_t buffer_size)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (buffer_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    buffer_size_ = buffer_size;
    const int64_t alloc_bytes = sizeof(ObLockWaitDetail) * buffer_size_;
    entries_ = static_cast<ObLockWaitDetail *>(ob_malloc(alloc_bytes, "LkWaitDetBuf"));
    if (OB_ISNULL(entries_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MEMSET(entries_, 0, alloc_bytes);
      next_alloc_seq_ = 0;
      is_inited_ = true;
    }
  }
  return ret;
}

void ObLockWaitDetailBuffer::destroy()
{
  if (OB_NOT_NULL(entries_)) {
    ob_free(entries_);
    entries_ = nullptr;
  }
  is_inited_ = false;
  buffer_size_ = 0;
  next_alloc_seq_ = 0;
}

uint64_t ObLockWaitDetailBuffer::alloc_detail_seq()
{
  return ATOMIC_AAF(&next_alloc_seq_, 1);
}

ObLockWaitDetail *ObLockWaitDetailBuffer::get_entry(const uint64_t seq)
{
  return OB_NOT_NULL(entries_) && buffer_size_ > 0
         ? &entries_[seq % buffer_size_] : nullptr;
}

const ObLockWaitDetail *ObLockWaitDetailBuffer::get_entry(const uint64_t seq) const
{
  return OB_NOT_NULL(entries_) && buffer_size_ > 0
         ? &entries_[seq % buffer_size_] : nullptr;
}

bool ObLockWaitDetailBuffer::read_detail(const uint64_t target_seq,
                                         ObLockWaitDetail &out,
                                         bool &holder_valid) const
{
  bool rowkey_hit = false;
  holder_valid = false;
  for (int64_t retry = 0; retry < 2; ++retry) {
    const ObLockWaitDetail *det = get_entry(target_seq);
    if (OB_ISNULL(det)) {
      break;
    }
    const uint64_t alloc_seq = ATOMIC_LOAD(&det->alloc_seq_);
    if (alloc_seq != target_seq || alloc_seq == 0) {
      continue;
    }
    const uint64_t holder_seq_before = ATOMIC_LOAD_ACQ(&det->holder_filled_seq_);
    MEMCPY(&out, det, sizeof(ObLockWaitDetail));
    MEM_BARRIER();
    const uint64_t alloc_seq_after = ATOMIC_LOAD_RLX(&det->alloc_seq_);
    const uint64_t holder_seq_after = ATOMIC_LOAD_RLX(&det->holder_filled_seq_);
    if (alloc_seq == alloc_seq_after && alloc_seq == target_seq) {
      rowkey_hit = true;
      holder_valid = (holder_seq_before == target_seq
                      && holder_seq_after == target_seq
                      && out.holder_filled_seq_ == target_seq);
      break;
    }
  }
  return rowkey_hit;
}

bool ObLockWaitDetailBuffer::is_holder_filled(const uint64_t det_seq,
                                              const int64_t holder_seq) const
{
  const ObLockWaitDetail *det = get_entry(det_seq);
  return OB_ISNULL(det)
         ? false
         : (ATOMIC_LOAD_ACQ(&det->alloc_seq_) == det_seq
            && ATOMIC_LOAD_ACQ(&det->holder_filled_seq_) == det_seq
            && det->last_filled_holder_seq_ == holder_seq);
}

int ObLockWaitDetailBuffer::lookup_and_fill_detail(const transaction::ObTransID &holder_tx_id,
                                                   const int64_t holder_seq,
                                                   const uint64_t det_seq)
{
  int ret = OB_SUCCESS;
  ObLockWaitDetail *det = get_entry(det_seq);
  ObLockDiagSampleStat &stat = get_lock_diag_sample_stat();
  if (OB_ISNULL(det)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (ATOMIC_LOAD(&det->alloc_seq_) != det_seq) {
    ++stat.detail_mismatch_;
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    transaction::ObTxDesc *holder_desc = nullptr;
    transaction::ObTransService *txs = MTL(transaction::ObTransService *);
    const int64_t lookup_begin = ObTimeUtility::current_time();
    ++stat.tx_desc_mgr_lookup_attempt_;
    if (OB_ISNULL(txs)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(txs->get_tx_desc_mgr().get(holder_tx_id, holder_desc))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ++stat.tx_desc_mgr_lookup_notfound_;
        LOG_TRACE("[LOCK_DIAG] holder tx desc not found",
                 K(ret), K(holder_tx_id), K(holder_seq), K(det_seq));
        ret = OB_SUCCESS;
      } else {
        ++stat.tx_desc_mgr_lookup_fail_;
        LOG_WARN("[LOCK_DIAG] failed to lookup holder tx desc",
                 K(ret), K(holder_tx_id), K(holder_seq), K(det_seq));
      }
    } else {
      ++stat.tx_desc_mgr_lookup_ok_;
      if (ATOMIC_LOAD(&det->alloc_seq_) != det_seq) {
        ++stat.detail_mismatch_;
        ret = OB_ENTRY_NOT_EXIST;
      } else {
        transaction::ObLockDiagStmtSlot stmt_info;
        transaction::ObLockDiagQuerySqlSlot query_sql_info;
        bool has_query_sql = false;
        transaction::ObTxStmtRing &stmt_ring = holder_desc->get_stmt_ring();
        if (OB_FAIL(stmt_ring.lookup_stmt_info(holder_seq, stmt_info, query_sql_info, has_query_sql))) {
          if (OB_EAGAIN == ret) {
            ++stat.ring_retry_;
          }
        } else if (ATOMIC_LOAD(&det->alloc_seq_) != det_seq) {
          ++stat.detail_mismatch_;
          ret = OB_ENTRY_NOT_EXIST;
        } else {
          ATOMIC_STORE_REL(&det->holder_filled_seq_, 0);
          MEMCPY(det->holder_sql_id_, stmt_info.sql_id_, common::OB_MAX_SQL_ID_LENGTH);
          det->holder_sql_id_[common::OB_MAX_SQL_ID_LENGTH] = '\0';
          det->holder_query_sql_[0] = '\0';
          if (has_query_sql) {
            strncpy(det->holder_query_sql_, query_sql_info.query_sql_, LOCK_DIAG_HOLDER_QUERY_SQL_LEN);
            det->holder_query_sql_[LOCK_DIAG_HOLDER_QUERY_SQL_LEN] = '\0';
          }
          if (ATOMIC_LOAD(&det->alloc_seq_) != det_seq) {
            ++stat.detail_mismatch_;
            ret = OB_ENTRY_NOT_EXIST;
          } else {
            det->last_filled_holder_seq_ = holder_seq;
            ATOMIC_STORE_REL(&det->holder_filled_seq_, det_seq);
          }
        }
      }
      txs->get_tx_desc_mgr().revert(*holder_desc);
    }
    stat.tx_desc_mgr_lookup_cost_us_ += ObTimeUtility::current_time() - lookup_begin;
  }
  return ret;
}

} // namespace share
} // namespace oceanbase
