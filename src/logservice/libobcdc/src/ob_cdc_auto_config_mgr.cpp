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

#define USING_LOG_PREFIX OBLOG

#include "ob_cdc_auto_config_mgr.h"
#include "ob_log_config.h"                  // TCONF
#include "ob_log_utils.h"

#define REFRESH_NUM_FIELD_DIRECT(FIELD_NAME, FIELD_VALUE) \
  do { \
    set_##FIELD_NAME(FIELD_VALUE); \
    LOG_INFO("[AUTO_CONFIG]", K_(FIELD_NAME)); \
  } while (0)

#define REFRESH_NUM_FIELD_WITH_CONFIG(FIELD_NAME, FIELD_VALUE, CONFIG_VALUE) \
  do { \
    if (CONFIG_VALUE > 0) { \
      set_##FIELD_NAME(CONFIG_VALUE); \
      LOG_INFO("[AUTO_CONFIG][USER_CONFIG]", K_(FIELD_NAME)); \
    } else { \
      set_##FIELD_NAME(FIELD_VALUE); \
      LOG_INFO("[AUTO_CONFIG][AUTO]", K_(FIELD_NAME)); \
    } \
  } while (0)


namespace oceanbase
{
using namespace oceanbase::common;
namespace libobcdc
{

// max queue length is 10W
const int64_t ObCDCAutoConfigMgr::MAX_QUEUE_LENGTH = 100000;

ObCDCAutoConfigMgr &ObCDCAutoConfigMgr::get_instance()
{
  static ObCDCAutoConfigMgr instance;
  return instance;
}

void ObCDCAutoConfigMgr::reset()
{
}

void ObCDCAutoConfigMgr::init(const ObLogConfig &config)
{
  init_queue_length_(config);
  init_initial_config_(config);
  refresh_dynamic_config_(config);
  LOG_INFO("init ObCDCAutoConfigMgr succ");
}

void ObCDCAutoConfigMgr::configure(const ObLogConfig &config)
{
  refresh_dynamic_config_(config);
}

void ObCDCAutoConfigMgr::refresh_factor_(const ObLogConfig &config)
{
  REFRESH_NUM_FIELD_DIRECT(memory_limit, config.memory_limit.get());
  factor_ = get_log2_(memory_limit_) - 20;
  if (factor_ < 11) factor_ = 11;
  _LOG_INFO("[AUTO_CONFIG][MEMORY_LIMIT: %s(%ld)][FACTOR: %ld]",
      SIZE_TO_STR(memory_limit_), memory_limit_, factor_);
}

void ObCDCAutoConfigMgr::init_queue_length_(const ObLogConfig &config)
{
  refresh_factor_(config);
  const static int64_t DEFAULT_STORAGE_QUEUE_LENGTH = 1024;
  int64_t auto_queue_length = 1 << (factor_ - 3);
  if (auto_queue_length > MAX_QUEUE_LENGTH) auto_queue_length = MAX_QUEUE_LENGTH;
  const int64_t br_queue_length = std::min((auto_queue_length * 32), MAX_QUEUE_LENGTH);
  REFRESH_NUM_FIELD_DIRECT(auto_queue_length, auto_queue_length);
  REFRESH_NUM_FIELD_WITH_CONFIG(br_queue_length, br_queue_length, config.br_queue_length.get());
  const int64_t resource_collector_queue_length = br_queue_length_;
  REFRESH_NUM_FIELD_DIRECT(resource_collector_queue_length, resource_collector_queue_length);
  REFRESH_NUM_FIELD_DIRECT(formatter_queue_length, br_queue_length_);
  REFRESH_NUM_FIELD_DIRECT(dml_parser_queue_length, br_queue_length_);
  REFRESH_NUM_FIELD_DIRECT(lob_data_merger_queue_length, br_queue_length_);

  const int64_t msg_sorter_queue_length = br_queue_length;
  REFRESH_NUM_FIELD_WITH_CONFIG(msg_sorter_task_count_upper_limit, msg_sorter_queue_length, config.msg_sorter_task_count_upper_limit.get());
  REFRESH_NUM_FIELD_WITH_CONFIG(sequencer_queue_length, MAX_QUEUE_LENGTH, config.sequencer_queue_length.get());
  REFRESH_NUM_FIELD_WITH_CONFIG(storager_queue_length, DEFAULT_STORAGE_QUEUE_LENGTH, config.storager_queue_length.get());
  REFRESH_NUM_FIELD_WITH_CONFIG(reader_queue_length, DEFAULT_STORAGE_QUEUE_LENGTH, config.reader_queue_length.get());
}

void ObCDCAutoConfigMgr::init_initial_config_(const ObLogConfig &config)
{
  const int64_t factor = factor_;
  const int64_t part_trans_task_prealloc_count = (1 << (factor_ - 11)) * 20000;
  REFRESH_NUM_FIELD_WITH_CONFIG(part_trans_task_prealloc_count, part_trans_task_prealloc_count, config.part_trans_task_prealloc_count.get());
}

void ObCDCAutoConfigMgr::refresh_dynamic_config_(const ObLogConfig &config)
{
  refresh_factor_(config);
  const static int64_t DEFAULT_STORAGER_MEM_PERCENT = 1;
  const static int64_t DEFAULT_STORAGER_TASK_UPPER_BOUND = 100;
  const int64_t redo_dispatcher_limit = (1 << (factor_ - 11)) * 32 * _M_;
  const int64_t auto_part_trans_task_upper_bound = (1 << (factor_ - 11)) * 20000;
  const int64_t active_part_trans_task_upper_bound = auto_part_trans_task_upper_bound;
  const int64_t reusable_part_trans_task_upper_bound = auto_part_trans_task_upper_bound;
  const int64_t ready_to_seq_task_upper_bound = auto_part_trans_task_upper_bound;
  const int64_t extra_redo_dispatch_memory_size = 1 * _K_ + (1 << (factor_ - 9)) * (factor_ - 11)  * _M_;
  const int64_t redo_dispatch_exceed_ratio = factor_ <= 12 ? 1 : (1 << (factor_ - 13));

  REFRESH_NUM_FIELD_WITH_CONFIG(redo_dispatcher_memory_limit, redo_dispatcher_limit, config.redo_dispatcher_memory_limit.get());
  REFRESH_NUM_FIELD_WITH_CONFIG(extra_redo_dispatch_memory_size, extra_redo_dispatch_memory_size, config.extra_redo_dispatch_memory_size.get());
  REFRESH_NUM_FIELD_WITH_CONFIG(redo_dispatched_memory_limit_exceed_ratio, redo_dispatch_exceed_ratio, config.redo_dispatched_memory_limit_exceed_ratio.get());
  _LOG_INFO("[AUTO_CONFIG][REDO_DISPATCH_MEMORY_LIMIT: %s(%ld)}][EXTRA_REDO_FOR_SKEW_PART: %s(%ld)]",
      SIZE_TO_STR(redo_dispatcher_memory_limit_), redo_dispatcher_memory_limit_,
      SIZE_TO_STR(extra_redo_dispatch_memory_size_), extra_redo_dispatch_memory_size_);
  REFRESH_NUM_FIELD_WITH_CONFIG(part_trans_task_active_count_upper_bound, active_part_trans_task_upper_bound, config.part_trans_task_active_count_upper_bound.get());
  REFRESH_NUM_FIELD_WITH_CONFIG(part_trans_task_reusable_count_upper_bound, reusable_part_trans_task_upper_bound, config.part_trans_task_reusable_count_upper_bound.get());
  REFRESH_NUM_FIELD_WITH_CONFIG(ready_to_seq_task_upper_bound, ready_to_seq_task_upper_bound, config.ready_to_seq_task_upper_bound.get());
  REFRESH_NUM_FIELD_WITH_CONFIG(storager_task_count_upper_bound, DEFAULT_STORAGER_TASK_UPPER_BOUND, config.storager_task_count_upper_bound.get());
  REFRESH_NUM_FIELD_WITH_CONFIG(storager_mem_percentage, DEFAULT_STORAGER_MEM_PERCENT, config.storager_mem_percentage.get());

}

int64_t ObCDCAutoConfigMgr::get_log2_(int64_t value)
{
  int64_t res = 0;

  if (value > 0) {
    // will modify value, but should not affect invoker
    while (value >>= 1) ++res;
  }

  return res;
}

} // namespace libobcdc
} // namespace oceanbase
