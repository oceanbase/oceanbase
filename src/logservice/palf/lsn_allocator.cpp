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

#include "lsn_allocator.h"
#include "log_group_entry_header.h"

namespace oceanbase
{
using namespace share;
namespace palf
{
const int64_t LSNAllocator::LOG_ID_DELTA_UPPER_BOUND;
const int64_t LSNAllocator::LOG_TS_DELTA_UPPER_BOUND;

LSNAllocator::LSNAllocator()
  : lock_(ObLatchIds::LOG_OFFSET_ALLOC_LOCK),
    is_inited_(false)
{
  reset();
}

LSNAllocator::~LSNAllocator()
{
  reset();
}

void LSNAllocator::reset()
{
  is_inited_ = false;
  lsn_ts_meta_.v128_.lo = 0;
  lsn_ts_meta_.v128_.hi = 0;
  lsn_ts_meta_.is_need_cut_ = 1;
  log_id_base_ = OB_INVALID_LOG_ID;
  scn_base_ = 0;
}

int LSNAllocator::init(const int64_t log_id,
                       const SCN &scn,
                       const LSN &start_lsn)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_INVALID_LOG_ID == log_id || !scn.is_valid() || !start_lsn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid arguments", K(ret), K(log_id), K(scn), K(start_lsn));
  } else {
    log_id_base_ = log_id;
    scn_base_ = scn.get_val_for_logservice();
    lsn_ts_meta_.v128_.lo = 0;
    lsn_ts_meta_.lsn_val_ = start_lsn.val_;
    lsn_ts_meta_.is_need_cut_ = 1;
    is_inited_ = true;
    PALF_LOG(INFO, "LSNAllocator init success", K_(log_id_base), K_(scn_base), K(start_lsn),
        "lsn_ts_meta_.is_need_cut_", lsn_ts_meta_.is_need_cut_,
        "lsn_ts_meta_.log_id_delta_", lsn_ts_meta_.log_id_delta_,
        "lsn_ts_meta_.scn_delta_", lsn_ts_meta_.scn_delta_,
        "lsn_ts_meta_.lsn_val_", lsn_ts_meta_.lsn_val_);
  }
  return ret;
}

int LSNAllocator::truncate(const LSN &lsn, const int64_t log_id, const SCN &scn)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!lsn.is_valid() || OB_INVALID_LOG_ID == log_id || !scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid arguments", K(ret), K(lsn), K(log_id), K(scn));
  } else {
    LSNTsMeta last;
    LSNTsMeta next;
    while (true) {
      WLockGuard guard(lock_);
      LOAD128(last, &lsn_ts_meta_);
      next.log_id_delta_ = 0;
      next.scn_delta_ = 0;
      next.lsn_val_ = lsn.val_;
      next.is_need_cut_ = 1;
      if (CAS128(&lsn_ts_meta_, last, next)) {
        log_id_base_ = log_id;
        scn_base_ = scn.get_val_for_logservice();
        PALF_LOG(INFO, "truncate success", K(lsn), K(log_id), K(scn));
        break;
      } else {
        PAUSE();
      }
    }
  }
  return ret;
}

int LSNAllocator::inc_update_last_log_info(const LSN &lsn, const int64_t log_id, const SCN &scn)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!lsn.is_valid() || !scn.is_valid() || OB_INVALID_LOG_ID == log_id)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid arguments", K(ret), K(lsn), K(scn), K(log_id));
  } else {
    LSNTsMeta last;
    LSNTsMeta next;
    bool need_update_base = false;
    do {
      if (need_update_base) {
        WLockGuard guard(lock_);
        LOAD128(last, &lsn_ts_meta_);
        const int64_t cur_log_id = log_id_base_ + last.log_id_delta_;
        next.log_id_delta_ = 0;
        next.scn_delta_ = 0;
        next.lsn_val_ = lsn.val_;
        next.is_need_cut_ = 1;

        if (log_id < cur_log_id || lsn.val_ < last.lsn_val_) {
          // no need update
        } else if (CAS128(&lsn_ts_meta_, last, next)) {
          log_id_base_ = log_id;
          scn_base_ = scn.get_val_for_logservice();
          PALF_LOG(TRACE, "inc_update_last_log_info success", K(lsn), K(scn), K(log_id));
        } else {
          ret = OB_ERR_UNEXPECTED;
          PALF_LOG(ERROR, "CAS128 failed, unexpected", K(ret));
        }
        break;
      }
      need_update_base = false;
      RLockGuard guard(lock_);
      while (OB_SUCC(ret)) {
        LOAD128(last, &lsn_ts_meta_);
        const int64_t cur_log_id = log_id_base_ + last.log_id_delta_;
        if (log_id < cur_log_id || lsn.val_ < last.lsn_val_) {
          // no need update
          break;
        } else if (log_id - log_id_base_ >= LOG_ID_DELTA_UPPER_BOUND) {
          // log_id reaches the upper bound
          need_update_base = true;
          break;
        } else if (scn.get_val_for_logservice() - scn_base_ >= LOG_TS_DELTA_UPPER_BOUND) {
          // scn reaches the upper bound
          need_update_base = true;
          break;
        } else {
          next.log_id_delta_ = log_id - log_id_base_;
          next.scn_delta_ = scn.get_val_for_logservice() - scn_base_;
          next.lsn_val_ = lsn.val_;
          next.is_need_cut_ = 1;
          if (CAS128(&lsn_ts_meta_, last, next)) {
            break;
          } else {
            PAUSE();
          }
        }
      }
    } while (need_update_base);
  }
  return ret;
}

int LSNAllocator::inc_update_scn_base(const SCN &ref_scn)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!ref_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid arguments", K(ret), K(ref_scn));
  } else {
    LSNTsMeta last;
    LSNTsMeta next;
    const uint64_t scn = ref_scn.get_val_for_logservice();
    while (true) {
      WLockGuard guard(lock_);
      LOAD128(last, &lsn_ts_meta_);
      next = last;
      next.scn_delta_ = 0;
      next.is_need_cut_ = 1;
      if (scn_base_ + last.scn_delta_ > scn) {
        // no need update
        PALF_LOG(INFO, "inc_update_scn_base success", K_(scn_base),
            "scn_delta", last.scn_delta_, K(scn));
        break;
      } else if (CAS128(&lsn_ts_meta_, last, next)) {
        scn_base_ = scn;
        PALF_LOG(INFO, "inc_update_scn_base success", K_(scn_base), K(scn));
        break;
      } else {
        PAUSE();
      }
    }
  }
  return ret;
}

int64_t LSNAllocator::get_max_log_id() const
{
  int64_t max_log_id = OB_INVALID_LOG_ID;
  if (IS_NOT_INIT) {
  } else {
    RLockGuard guard(lock_);
    LSNTsMeta last;
    LOAD128(last, &lsn_ts_meta_);
    max_log_id = log_id_base_ + last.log_id_delta_;
  }
  return max_log_id;
}

SCN LSNAllocator::get_max_scn() const
{
  SCN result;
  uint64_t max_scn = 0;
  if (IS_NOT_INIT) {
  } else {
    RLockGuard guard(lock_);
    LSNTsMeta last;
    LOAD128(last, &lsn_ts_meta_);
    max_scn = scn_base_ + last.scn_delta_;
    int ret = OB_SUCCESS;
    if (OB_FAIL(result.convert_for_logservice(max_scn))) {
      PALF_LOG(ERROR, "failed to convert_for_logservice", K(max_scn),
               K(scn_base_), K(last.scn_delta_));
    }
  }
  return result;
}

int LSNAllocator::get_curr_end_lsn(LSN &curr_end_lsn) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    LSNTsMeta last;
    LOAD128(last, &lsn_ts_meta_);
    curr_end_lsn.val_ = last.lsn_val_;
  }
  return ret;
}

int LSNAllocator::try_freeze_by_time(LSN &last_lsn, int64_t &last_log_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    LSNTsMeta last;
    LSNTsMeta next;
    while (true) {
      RLockGuard guard(lock_);
      LOAD128(last, &lsn_ts_meta_);
      if (1 == last.is_need_cut_) {
        // last log has been cut
        ret = OB_STATE_NOT_MATCH;
        last_lsn.val_ = last.lsn_val_;
        last_log_id = log_id_base_ + last.log_id_delta_;
        break;
      } else {
        next.lsn_val_ = last.lsn_val_;
        next.is_need_cut_ = 1;
        next.log_id_delta_ = last.log_id_delta_;
        next.scn_delta_ = last.scn_delta_;
        if (CAS128(&lsn_ts_meta_, last, next)) {
          last_lsn.val_ = next.lsn_val_;
          last_log_id = log_id_base_ + last.log_id_delta_;
          break;
        } else {
          PAUSE();
        }
      }
    }
    PALF_LOG(INFO, "try_freeze_by_time", K(ret), K(last_lsn), K(last_log_id));
  }
  return ret;
}

int LSNAllocator::try_freeze(LSN &last_lsn, int64_t &last_log_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    LSNTsMeta last;
    LSNTsMeta next;
    while (true) {
      RLockGuard guard(lock_);
      LOAD128(last, &lsn_ts_meta_);
      if (1 == last.is_need_cut_) {
        // last log has been cut
        last_lsn.val_ = last.lsn_val_;
        last_log_id = log_id_base_ + last.log_id_delta_;
        break;
      } else {
        next.lsn_val_ = last.lsn_val_;
        next.is_need_cut_ = 1;
        next.log_id_delta_ = last.log_id_delta_;
        next.scn_delta_ = last.scn_delta_;
        if (CAS128(&lsn_ts_meta_, last, next)) {
          last_lsn.val_ = next.lsn_val_;
          last_log_id = log_id_base_ + last.log_id_delta_;
          break;
        } else {
          PAUSE();
        }
      }
    }
  }
  return ret;
}

int LSNAllocator::alloc_lsn_scn(const SCN &base_scn,
                                const int64_t size, // 已包含LogHeader size
                                const int64_t log_id_upper_bound,
                                const LSN &lsn_upper_bound,
                                LSN &lsn,
                                int64_t &log_id,
                                SCN &scn,
                                bool &is_new_group_log,
                                bool &need_gen_padding_entry,
                                int64_t &padding_len)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (size <= 0 || !base_scn.is_valid() || log_id_upper_bound <= 0 || !lsn_upper_bound.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid arguments", K(ret), K(base_scn), K(size), K(log_id_upper_bound), K(lsn_upper_bound));
  } else {
    // 生成新日志时需加上log_group_entry_header的size
    const int64_t new_group_log_size = size + LogGroupEntryHeader::HEADER_SER_SIZE;
    bool need_update_base = false;
    do {
      LSNTsMeta last;
      LSNTsMeta next;
      if (need_update_base) {
        // update base value with wrlock
        WLockGuard guard(lock_);
        LOAD128(last, &lsn_ts_meta_);
        const int64_t last_log_id = log_id_base_ + last.log_id_delta_;
        const uint64_t last_scn = scn_base_ + last.scn_delta_;
        const uint64_t new_scn = std::max(base_scn.get_val_for_logservice(), last_scn);

        log_id_base_ = last_log_id;
        scn_base_ = new_scn;
        next.is_need_cut_ = last.is_need_cut_;

        next.scn_delta_ = 0;
        next.log_id_delta_ = 0;
        next.lsn_val_ = last.lsn_val_;
        if (CAS128(&lsn_ts_meta_, last, next)) {
          // PALF_LOG(INFO, "update base value and lsn_ts_meta_ successfully", K_(log_id_base), K_(scn_base));
        } else {
          ret = OB_ERR_UNEXPECTED;
          PALF_LOG(ERROR, "CAS128 failed, unexpected", K(ret));
        }
      }
      // alloc lsn/log_id/scn with rdlock
      need_update_base = false;
      RLockGuard guard(lock_);
      while (OB_SUCC(ret)) {
        is_new_group_log = false;
        need_gen_padding_entry = false;
        padding_len = 0;
        LOAD128(last, &lsn_ts_meta_);
        const int64_t last_log_id = log_id_base_ + last.log_id_delta_;
        const uint64_t last_scn = scn_base_ + last.scn_delta_;
        const uint64_t tmp_next_scn = std::max(base_scn.get_val_for_logservice(), last_scn + 1);

        if ((tmp_next_scn + 1) - scn_base_ >= LOG_TS_DELTA_UPPER_BOUND) {
          // 对于可能生成的padding log, 也会占用一个scn
          need_update_base = true;
        } else if ((last.log_id_delta_ + 2) >= LOG_ID_DELTA_UPPER_BOUND) {
          // 对于可能生成的padding log, 也会占用一个log_id
          need_update_base = true;
        } else {
          // do nothing
        }

        if (need_update_base) {
          break;
        }

        uint64_t tmp_next_block_id = lsn_2_block(LSN(last.lsn_val_), PALF_BLOCK_SIZE);
        uint64_t tmp_next_log_id_delta = last.log_id_delta_;
        uint64_t tmp_next_scn_delta = tmp_next_scn - scn_base_;
        // 下一条日志是否需要cut
        bool is_next_need_cut = false;
        const uint64_t last_block_offset = lsn_2_offset(LSN(last.lsn_val_), PALF_BLOCK_SIZE);
        uint64_t tmp_next_block_offset = 0;
        if (last.is_need_cut_) {
          // 上一条日志不再聚合，需生成新日志
          is_new_group_log = true;
          tmp_next_block_offset = last_block_offset + new_group_log_size;
          // 判断新日志是否会达到/跨过2M边界，是则下一条日志要触发freeze
          if ((last_block_offset & LOG_CUT_TRIGGER_MASK) + new_group_log_size >= LOG_CUT_TRIGGER) {
            is_next_need_cut = true;
          }
        } else if (last_block_offset > 0
                   && (last_block_offset & LOG_CUT_TRIGGER_MASK) == 0) {
          // 上一条日志末尾恰好已到2M边界,预期不会出现，因为这种情况last.is_need_cut_一定为true
          ret = OB_ERR_UNEXPECTED;
          PALF_LOG(WARN, "last_block_offset is reach 2M boundary", K(ret), K(last_block_offset));
        } else if (last_block_offset > 0
                   && ((last_block_offset & LOG_CUT_TRIGGER_MASK) + size) > LOG_CUT_TRIGGER) {
          // 上一条日志聚合本条日志后会跨2M边界，本条日志不再聚合
          is_new_group_log = true;
          is_next_need_cut = false;
          tmp_next_block_offset = last_block_offset + new_group_log_size;
          // 判断新日志是否会达到/跨过2M边界，是则下一条日志要触发freeze
          if ((last_block_offset & LOG_CUT_TRIGGER_MASK) + new_group_log_size >= LOG_CUT_TRIGGER) {
            is_next_need_cut = true;
          }
        } else {
          // 聚合到上一条日志尾部
          is_new_group_log = false;
          is_next_need_cut = false;
          tmp_next_block_offset = last_block_offset + size;
          // 判断新日志是否会达到/跨过2M边界，是则下一条日志要触发freeze
          if ((last_block_offset & LOG_CUT_TRIGGER_MASK) + size >= LOG_CUT_TRIGGER) {
            is_next_need_cut = true;
          }
        }
        if (tmp_next_block_offset < PALF_BLOCK_SIZE) {
          // 未超过文件size，需判断文件末尾空间是否小于4K
          // 是则以padding形式聚合到日志末尾
          // 否则不处理
          if (PALF_BLOCK_SIZE - tmp_next_block_offset < CLOG_FILE_TAIL_PADDING_TRIGGER) {
            // 文件尾小于4K, 需生成padding entry补齐，将新日志存到下一个文件中
            is_new_group_log = true;
            need_gen_padding_entry = true;
            // padding_len包含padding_log的log_group_entry_header_size
            padding_len = PALF_BLOCK_SIZE - last_block_offset;
            tmp_next_block_id++;  // block_id++
            tmp_next_block_offset = new_group_log_size;
            is_next_need_cut = false;
            // 判断新日志是否会达到/跨过2M边界，是则下一条日志要触发freeze
            if (new_group_log_size >= LOG_CUT_TRIGGER) {
              is_next_need_cut = true;
            }
          }
        } else if (tmp_next_block_offset == PALF_BLOCK_SIZE) {
          // 恰好到达文件尾
          tmp_next_block_id++;  // block_id++
          tmp_next_block_offset = 0;
          is_next_need_cut = true;
        } else {
          // 当前文件无法容纳该日志, 需要切文件
          // 首先在本文件尾生成一个padding_entry,它的scn与后一条日志相同
          // 然后将新日志写到下一个文件开头
          is_new_group_log = true;
          need_gen_padding_entry = true;
          // padding_len包含padding_log的log_group_entry_header_size
          padding_len = PALF_BLOCK_SIZE - last_block_offset;
          tmp_next_block_id++;  // block_id++
          tmp_next_block_offset = new_group_log_size;
          is_next_need_cut = false;
          // 判断新日志是否会达到/跨过2M边界，是则下一条日志要触发freeze
          if (new_group_log_size >= LOG_CUT_TRIGGER) {
            is_next_need_cut = true;
          }
        }
        if (is_new_group_log) {
          tmp_next_log_id_delta++;
        }
        const int64_t output_next_scn_delta = tmp_next_scn_delta;
        if (need_gen_padding_entry) {
          tmp_next_log_id_delta++;
          tmp_next_scn_delta++;
        }
        next.lsn_val_ = (tmp_next_block_id  * PALF_BLOCK_SIZE) + tmp_next_block_offset;
        next.is_need_cut_ = is_next_need_cut ? 1 : 0;
        next.log_id_delta_ = tmp_next_log_id_delta;
        next.scn_delta_ = tmp_next_scn_delta;

        int64_t new_log_id = is_new_group_log ? (last_log_id + 1) : last_log_id;
        if (need_gen_padding_entry) {
          // Padding entry also consumes one log_id.
          new_log_id++;
        }
        LSN new_max_lsn;
        new_max_lsn.val_ = next.lsn_val_;
        if (new_log_id > log_id_upper_bound || new_max_lsn > lsn_upper_bound) {
          ret = OB_EAGAIN;
          if (REACH_TIME_INTERVAL(100 * 1000)) {
            PALF_LOG(INFO, "log_id or lsn will exceed upper bound, need retry", K(ret), K(size),
                K(new_log_id), K(new_max_lsn), K(is_new_group_log), K(need_gen_padding_entry),
                K(log_id_upper_bound), K(lsn_upper_bound));
          }
          break;
        } else if (CAS128(&lsn_ts_meta_, last, next)) {
          lsn.val_ = last.lsn_val_;
          if (is_new_group_log) {
            log_id = last_log_id + 1;
          } else {
            log_id = last_log_id;
          }

          uint64_t scn_val = scn_base_ + output_next_scn_delta;
          if (OB_FAIL(scn.convert_for_logservice(scn_val))) {
            PALF_LOG(ERROR, "failed to convert scn", K(ret), K(base_scn), K(scn));
          }

          PALF_LOG(TRACE, "alloc_lsn_ts succ", K(ret), K(base_scn), K(size), K(lsn), K(last.lsn_val_),
               K(next.lsn_val_), "next.is_need_cut", next.is_need_cut_, K(log_id), K(scn));
          break;
        } else {
          PAUSE();
        }
      }
    } while(need_update_base);
  }
  return ret;
}
}  // namespace palf
}  // namespace oceanbase
