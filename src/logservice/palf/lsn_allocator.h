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

#ifndef OCEANBASE_LOGSERVICE_LOG_OFFSET_ALLOCATOR_
#define OCEANBASE_LOGSERVICE_LOG_OFFSET_ALLOCATOR_

#include "lib/atomic/atomic128.h"
#include "lib/lock/ob_tc_rwlock.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/scn.h"
#include "lsn.h"

namespace oceanbase
{
namespace palf
{
class LSNAllocator
{
public:
  LSNAllocator();
  ~LSNAllocator();
public:
  int init(const int64_t log_id, const share::SCN &scn, const LSN &start_lsn);
  void reset();
  int get_log_block_size(const uint64_t block_id, int64_t &block_size) const
  {
    // TODO: by haofan
    // 为了支持log文件大小可配置,需要为每个文件获取对应的size
    UNUSED(block_id);
    block_size = PALF_BLOCK_SIZE;
    return OB_SUCCESS;
  }
  int64_t get_max_log_id() const;
  share::SCN get_max_scn() const;
  int get_curr_end_lsn(LSN &curr_end_lsn) const;
  int try_freeze_by_time(LSN &last_lsn, int64_t &last_log_id);
  int try_freeze(LSN &last_lsn, int64_t &last_log_id);
  // 功能: 为一条日志分配lsn, scn.
  //
  // @param [in] base_scn: scn下界值
  // @param [in] size: 日志体长度,如果是聚合日志需包含LogHeader长度
  //
  // @param [out] lsn: 分配的lsn
  // @param [out] scn: 分配的scn
  // @param [out] is_new_log: 是否需要生成新日志
  // @param [out] need_gen_padding_entry: 是否需要在本条日志之前生成padding_entry
  // @param [out] padding_len: padding部分的总长度
  //
  int alloc_lsn_scn(const share::SCN &base_scn,
                    const int64_t size,
                    const int64_t log_id_upper_bound,
                    const LSN &lsn_upper_bound,
                    LSN &lsn,
                    int64_t &log_id,
                    share::SCN &scn,
                    bool &is_new_log,
                    bool &need_gen_padding_entry,
                    int64_t &padding_len);
  // 更新last_lsn和log_timestamp
  // receive_log/append_disk_log 时调用
  int inc_update_last_log_info(const LSN &lsn, const int64_t log_id, const share::SCN &scn);
  // inc update scn base, called by change access mode and to leader active
  int inc_update_scn_base(const share::SCN &scn);
  int truncate(const LSN &lsn, const int64_t log_id, const share::SCN &scn);
  // 获取last_lsn和log_timestamp
  TO_STRING_KV("max_log_id", get_max_log_id(), "max_lsn", lsn_ts_meta_.lsn_val_,
      "max_scn", get_max_scn());
private:
  static const int32_t LOG_ID_DELTA_BIT_CNT = 28;  // log_id_delta部分的位数，可生成25万个log_id
  static const int32_t LOG_TS_DELTA_BIT_CNT = 35;  // scn_delta部分的位数, ns级别，约可用32秒
  static const int64_t LOG_ID_DELTA_UPPER_BOUND = (1ul << LOG_ID_DELTA_BIT_CNT) - 1000;
  static const int64_t LOG_TS_DELTA_UPPER_BOUND = (1ul << LOG_TS_DELTA_BIT_CNT) - 1000;
  static const uint64_t LOG_CUT_TRIGGER = 1 << 21;          // 聚合日志跨2M边界时切分
  static const uint64_t LOG_CUT_TRIGGER_MASK = (1 << 21) - 1;
  static const uint64_t MAX_SUPPORTED_BLOCK_ID = 0xfffffffff - 1000;  // block_id告警阈值
  static const uint64_t MAX_SUPPORTED_BLOCK_OFFSET = 0xfffffff;        // block_offset的最大支持256MB
private:
  union LSNTsMeta
  {
    struct types::uint128_t v128_;
    struct
    {
      uint8_t is_need_cut_ : 1;  // whether next log need cut
      uint64_t log_id_delta_ : LOG_ID_DELTA_BIT_CNT;
      uint64_t scn_delta_ : LOG_TS_DELTA_BIT_CNT;
      uint64_t lsn_val_ : 64;
    };
  };
private:
  typedef common::RWLock RWLock;
  typedef RWLock::RLockGuard RLockGuard;
  typedef RWLock::WLockGuard WLockGuard;
private:
  LSNTsMeta lsn_ts_meta_;
  int64_t log_id_base_;
  uint64_t scn_base_;
  mutable RWLock lock_;
  bool is_inited_;
private:
  DISALLOW_COPY_AND_ASSIGN(LSNAllocator);
};
}  // namespace palf
}  // namespace oceanbase
#endif // OCEANBASE_LOGSERVICE_LOG_OFFSET_ALLOCATOR_
