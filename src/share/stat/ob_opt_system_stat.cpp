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

#define USING_LOG_PREFIX SQL_OPT
#include "share/stat/ob_opt_system_stat.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/utility/ob_macro_utils.h"
#include "src/storage/blocksstable/ob_block_manager.h"
#include "src/share/io/ob_io_manager.h"

namespace oceanbase {
namespace common {
using namespace sql;

OB_DEF_SERIALIZE(ObOptSystemStat) {
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              last_analyzed_,
              cpu_speed_,
              disk_seq_read_speed_,
              disk_rnd_read_speed_,
              network_speed_
              );
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObOptSystemStat) {
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              last_analyzed_,
              cpu_speed_,
              disk_seq_read_speed_,
              disk_rnd_read_speed_,
              network_speed_
              );
  return len;
}

OB_DEF_DESERIALIZE(ObOptSystemStat) {
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
              last_analyzed_,
              cpu_speed_,
              disk_seq_read_speed_,
              disk_rnd_read_speed_,
              network_speed_
              );
  return ret;
}

void OptSystemIoBenchmark::reset()
{
  disk_seq_read_speed_ = 0;
  disk_rnd_read_speed_ = 0;
  init_ = false;
}

OptSystemIoBenchmark& OptSystemIoBenchmark::get_instance()
{
  static OptSystemIoBenchmark benchmark;
  return benchmark;
}

int OptSystemIoBenchmark::run_benchmark(ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  int64_t load_size = 16 * 1024; //16k
  int64_t io_count = 0;
  int64_t rt_us = 0;
  int64_t data_size = 0;
  char *read_buf = NULL;
  ObIOInfo io_info;
  io_info.tenant_id_ = OB_SERVER_TENANT_ID;
  io_info.size_ = load_size;
  io_info.buf_ = nullptr;
  io_info.flag_.set_mode(ObIOMode::READ);
  io_info.flag_.set_resource_group_id(THIS_WORKER.get_group_id());
  io_info.flag_.set_sys_module_id(ObIOModule::CALIBRATION_IO);
  io_info.flag_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
  io_info.flag_.set_unlimited(true);
  io_info.timeout_us_ = MAX_IO_WAIT_TIME_MS;
  ObIOHandle io_handle;
  // prepare io bench
  ObSEArray<blocksstable::ObMacroBlockHandle, 16> block_handles;
  const double MIN_FREE_SPACE_PERCENTAGE = 0.1; //
  const int64_t MIN_CALIBRATION_BLOCK_COUNT = 1024L * 1024L * 1024L / OB_DEFAULT_MACRO_BLOCK_SIZE;
  const int64_t MAX_CALIBRATION_BLOCK_COUNT = 20L * 1024L * 1024L * 1024L / OB_DEFAULT_MACRO_BLOCK_SIZE;
  const int64_t free_block_count = OB_SERVER_BLOCK_MGR.get_free_macro_block_count();
  const int64_t total_block_count = OB_SERVER_BLOCK_MGR.get_total_macro_block_count();
  int64_t benchmark_block_count = free_block_count * 0.2;
  if (free_block_count <= MIN_CALIBRATION_BLOCK_COUNT ||
      1.0 * free_block_count / total_block_count < MIN_FREE_SPACE_PERCENTAGE) {
    ret = OB_SERVER_OUTOF_DISK_SPACE;
    LOG_WARN("out of space", K(ret), K(free_block_count), K(total_block_count));
  } else if (OB_ISNULL(read_buf = static_cast<char *>(allocator.alloc(OB_DEFAULT_MACRO_BLOCK_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate read memory failed", K(ret));
  } else {
    benchmark_block_count = min(benchmark_block_count, MAX_CALIBRATION_BLOCK_COUNT);
    benchmark_block_count = max(benchmark_block_count, MIN_CALIBRATION_BLOCK_COUNT);
    io_info.user_data_buf_ = read_buf;
  }
  // prepare macro blocks
  for (int64_t i = 0; OB_SUCC(ret) && i < benchmark_block_count; ++i) {
    blocksstable::ObMacroBlockHandle block_handle;
    if (OB_FAIL(OB_SERVER_BLOCK_MGR.alloc_block(block_handle))) {
      LOG_WARN("alloc macro block failed", K(ret), K(i));
    } else if (OB_FAIL(block_handles.push_back(block_handle))) {
      LOG_WARN("push back block handle failed", K(ret), K(block_handle));
    }
  }
  //test rnd io
  while (OB_SUCC(ret) && io_count < 100) {
    int64_t block_idx = ObRandom::rand(block_handles.count()/2, block_handles.count() - 1);
    io_info.fd_.first_id_ = block_handles[block_idx].get_macro_id().first_id();
    io_info.fd_.second_id_ = block_handles[block_idx].get_macro_id().second_id();
    io_info.offset_ = ObRandom::rand(0, OB_DEFAULT_MACRO_BLOCK_SIZE - load_size);
    io_info.size_ = load_size;
    const int64_t begin_ts = ObTimeUtility::fast_current_time();
    if (OB_FAIL(OB_IO_MANAGER.read(io_info, io_handle))) {
      LOG_WARN("io benchmark read failed", K(ret), K(io_info));
    } else {
      ++io_count;
      rt_us += ObTimeUtility::fast_current_time() - begin_ts;
      data_size += io_handle.get_data_size();
    }
  }
  if (OB_SUCC(ret)) {
    rt_us = rt_us >= 1 ? rt_us : 1;
    disk_rnd_read_speed_ = data_size / rt_us;
    init_ = true;
  }
  io_count = 0;
  rt_us = 0;
  data_size = 0;
  //test seq io
  while (OB_SUCC(ret) && io_count < 100 && io_count < block_handles.count()/2) {
    int64_t block_idx = io_count;
    io_info.fd_.first_id_ = block_handles[block_idx].get_macro_id().first_id();
    io_info.fd_.second_id_ = block_handles[block_idx].get_macro_id().second_id();
    io_info.offset_ = 0;
    io_info.size_ = OB_DEFAULT_MACRO_BLOCK_SIZE;
    const int64_t begin_ts = ObTimeUtility::fast_current_time();
    if (OB_FAIL(OB_IO_MANAGER.read(io_info, io_handle))) {
      LOG_WARN("io benchmark read failed", K(ret), K(io_info));
    } else {
      ++io_count;
      rt_us += ObTimeUtility::fast_current_time() - begin_ts;
      data_size += io_handle.get_data_size();
    }
  }
  if (OB_SUCC(ret)) {
    rt_us = rt_us >= 1 ? rt_us : 1;
    disk_seq_read_speed_ = data_size / rt_us;
    allocator.free(read_buf);
  }
  return ret;
}

//TODO: collect system stat with workload

}
}
