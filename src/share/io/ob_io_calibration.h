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

#ifndef OCEANBASE_SHARE_IO_OB_IO_CALIBRATION_H
#define OCEANBASE_SHARE_IO_OB_IO_CALIBRATION_H

#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "lib/thread/thread_mgr_interface.h"
#include "lib/queue/ob_fixed_queue.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/container/ob_array_wrap.h"
#include "lib/lock/ob_drw_lock.h"
#include "share/io/ob_io_define.h"
#include "storage/blocksstable/ob_block_manager.h"

namespace oceanbase
{
namespace common
{

struct ObIOBenchLoad final
{
public:
  ObIOBenchLoad();
  ~ObIOBenchLoad();
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K(mode_), K(size_));
public:
  ObIOMode mode_;
  int64_t size_;
};

struct ObIOBenchResult final
{
  OB_UNIS_VERSION(1);
public:
  ObIOBenchResult();
  ~ObIOBenchResult();
  void reset();
  bool is_valid() const;
  bool operator==(const ObIOBenchResult &other) const;
  DEFINE_TO_STRING(BUF_PRINTF("mode:%d, size:%ld, iops:%.2f, rt_us:%.2f", (int)mode_, size_, iops_, rt_us_));
public:
  ObIOMode mode_;
  int64_t size_;
  double iops_;
  double rt_us_;
};

struct ObIOAbility final
{
public:
  typedef ObArray<ObIOBenchResult> MeasureItemArray;
  ObIOAbility();
  ~ObIOAbility();
  void reset();
  bool is_valid() const;
  int assign(const ObIOAbility &other);
  bool operator==(const ObIOAbility &other) const;
  int add_measure_item(const ObIOBenchResult &item);
  const MeasureItemArray &get_measure_items(const ObIOMode mode) const;
  int get_iops(const ObIOMode mode, const int64_t size, double&iops) const;
  int get_rt(const ObIOMode mode, const int64_t size, double&rt_us) const;
  TO_STRING_KV("measure_items", ObArrayWrap<MeasureItemArray>(measure_items_, (int)ObIOMode::MAX_MODE));
private:
  int find_item(const ObIOMode mode, const int64_t size, int64_t &item_idx) const;
private:
  MeasureItemArray measure_items_[static_cast<int>(ObIOMode::MAX_MODE)];
};

class ObIOBenchRunner : public lib::TGRunnable
{
public:
  ObIOBenchRunner();
  ~ObIOBenchRunner();
  int init(const int64_t block_count);
  int do_benchmark(const ObIOBenchLoad &load, const int64_t thread_count, ObIOBenchResult &result);
  void destroy();
  virtual void run1() override;

private:
  bool is_inited_;
  ObArray<blocksstable::ObMacroBlockHandle> block_handles_;
  ObIOBenchLoad load_;
  int tg_id_;
  int64_t io_count_;
  int64_t rt_us_;
  char *write_buf_;
};

class ObIOBenchController : public lib::TGRunnable
{
public:
  ObIOBenchController();
  virtual ~ObIOBenchController();
  int start_io_bench();
  void run1();
  int64_t get_start_timestamp();
  int64_t get_finish_timestamp();
  int get_ret_code();
private:
  int tg_id_;
  lib::ObMutex running_mutex_;
  int64_t start_ts_;
  int64_t finish_ts_;
  int ret_code_;
};

/**
 * load benchmark result file from the config directory
 */
class ObIOCalibration final
{
public:
  static ObIOCalibration &get_instance();
  static int parse_calibration_table(ObIOAbility &io_ability);
  static int parse_calibration_string(const ObString &calibration_string, ObIOBenchResult &item);
  int init();
  void destroy();
  int update_io_ability(const ObIOAbility &io_ability);
  int reset_io_ability();
  int get_io_ability(ObIOAbility &io_ability);
  int get_iops_scale(const ObIOMode mode, const int64_t size, double &iops_scale, bool &is_io_ability_valid);
  int read_from_table();
  int write_into_table(ObMySQLTransaction &trans, const ObAddr &addr, const ObIOAbility &io_ability);
  int refresh(const bool only_refresh, const ObIArray<ObIOBenchResult> &items);
  int execute_benchmark();
  int get_benchmark_status(int64_t &start_ts, int64_t &finish_ts, int &ret_code);
private:
  ObIOCalibration();
  ~ObIOCalibration();
  DISALLOW_COPY_AND_ASSIGN(ObIOCalibration);
private:
  static const ObIOMode BASELINE_IO_MODE = ObIOMode::READ;
  static const int64_t BASELINE_IO_SIZE = 16L * 1024L;
  bool is_inited_;
  double baseline_iops_;
  ObIOAbility io_ability_;
  DRWLock lock_;
  ObIOBenchController benchmark_controller_;
};

}// end namespace oceanbase
}// end namespace oceanbase

#endif//OCEANBASE_SHARE_IO_OB_IO_CALIBRATION_H
