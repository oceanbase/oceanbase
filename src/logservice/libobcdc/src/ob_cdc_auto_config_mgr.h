/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef  OCEANBASE_LIBOBCDC_AUTO_CONFIG_MGR_H_
#define  OCEANBASE_LIBOBCDC_AUTO_CONFIG_MGR_H_

#include "ob_cdc_macro_utils.h"
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase
{
namespace libobcdc
{
class ObLogConfig;
class ObCDCAutoConfigMgr
{
public:
  static ObCDCAutoConfigMgr &get_instance();
  ~ObCDCAutoConfigMgr() { reset(); }
public:
  void init(const ObLogConfig &config);
  void reset();
  void configure(const ObLogConfig &config);
public:

private:
  ObCDCAutoConfigMgr() { reset(); }
private:
  void refresh_factor_(const ObLogConfig &config);
  void init_queue_length_(const ObLogConfig &config);
  // should invoke after init_queue_length_ to ensure factor_ is valid
  void init_initial_config_(const ObLogConfig &config);
  void refresh_dynamic_config_(const ObLogConfig &config);
  int64_t get_log2_(int64_t value);
private:
  static const int64_t MAX_QUEUE_LENGTH;


// FIELD DEFINE BEGIN //
private:
// The automatic adjustment values of some parameters in adaptive mode correspond to memory_limit
//
// | memory_limit                      | 2G    | 4G    | 8G    | 16G   | 32G   | 128G  |
// | --------------------------------- | ----- | ----- | ----- | ----- | ----- | ----- |
// | factor                            | 11    | 12    | 13    | 14    | 15    | 17    |
// | auto_queue_length                 | 256   | 512   | 1024  | 2048  | 4096  | 16384 |
// | br_queue_length                   | 8192  | 16384 | 32768 | 65536 | 10W   | 10W   |
// | part_trans_task_prealloc_count    | 2W    | 4W    | 8W    | 16W   | 32W   | 128W  |
// | auto_part_trans_task_upper_bound  | 2W    | 4W    | 8W    | 16W   | 32W   | 128W  |
// | redo_dispatcher_memory_limit      | 32M   | 64M   | 128M  | 256M  | 512M  | 2G    |
// | extra_redo_dispatch_memory_size   | 1K    | 8M    | 32M   | 96M   | 256M  | 1.5G  |
// | redo_dispatch_exceed_ratio        | 1     | 1     | 1     | 2     | 4     | 16    |
  int64_t factor_;
DEFINE_FIELD_WITH_GETTER(int64_t, br_queue_length);

// thread queue length
DEFINE_FIELD_WITH_GETTER(int64_t, auto_queue_length);
DEFINE_FIELD_WITH_GETTER(int64_t, sequencer_queue_length);
DEFINE_FIELD_WITH_GETTER(int64_t, storager_queue_length);
DEFINE_FIELD_WITH_GETTER(int64_t, reader_queue_length);
DEFINE_FIELD_WITH_GETTER(int64_t, lob_data_merger_queue_length);
DEFINE_FIELD_WITH_GETTER(int64_t, msg_sorter_task_count_upper_limit);
DEFINE_FIELD_WITH_GETTER(int64_t, resource_collector_queue_length);
DEFINE_FIELD_WITH_GETTER(int64_t, formatter_queue_length);
DEFINE_FIELD_WITH_GETTER(int64_t, dml_parser_queue_length);

// initial-value can't change after init
DEFINE_FIELD_WITH_GETTER(int64_t, part_trans_task_prealloc_count);

// flow controll
DEFINE_FIELD_WITH_GETTER(int64_t, memory_limit);
DEFINE_FIELD_WITH_GETTER(int64_t, redo_dispatcher_memory_limit);
DEFINE_FIELD_WITH_GETTER(int64_t, extra_redo_dispatch_memory_size);
DEFINE_FIELD_WITH_GETTER(int64_t, redo_dispatched_memory_limit_exceed_ratio);
DEFINE_FIELD_WITH_GETTER(int64_t, part_trans_task_active_count_upper_bound);
DEFINE_FIELD_WITH_GETTER(int64_t, part_trans_task_reusable_count_upper_bound);
DEFINE_FIELD_WITH_GETTER(int64_t, ready_to_seq_task_upper_bound);
DEFINE_FIELD_WITH_GETTER(int64_t, storager_task_count_upper_bound);
DEFINE_FIELD_WITH_GETTER(int64_t, storager_mem_percentage);
// FIELD DEFINE END //

DISABLE_COPY_ASSIGN(ObCDCAutoConfigMgr);

};

#define CDC_CFG_MGR (ObCDCAutoConfigMgr::get_instance())

} // namespace libobcdc
} // namespace oceanbase

#endif // OCEANBASE_LIBOBCDC_AUTO_CONFIG_MGR_H_
