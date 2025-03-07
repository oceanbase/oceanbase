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
#include "share/ob_io_device_helper.h"
#include "src/share/io/ob_io_manager.h"
#include "observer/ob_server_struct.h"

#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/shared_storage/ob_disk_space_manager.h"
#endif

namespace oceanbase {
namespace common {
using namespace sql;
using namespace storage;

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

int OptSystemIoBenchmark::run_benchmark(ObIAllocator &allocator, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t load_size = 16 * 1024; //16k
  int64_t io_count = 0;
  int64_t rt_us = 0;
  int64_t data_size = 0;
  char *read_buf = NULL;
  ObIOInfo io_info;
  io_info.tenant_id_ = tenant_id;
  io_info.size_ = load_size;
  io_info.buf_ = nullptr;
  io_info.flag_.set_mode(ObIOMode::READ);
  io_info.flag_.set_sys_module_id(ObIOModule::CALIBRATION_IO);
  io_info.flag_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
  io_info.flag_.set_unlimited(true);
  io_info.timeout_us_ = MAX_IO_WAIT_TIME_MS;
  ObIOHandle io_handle;
  // prepare io bench
  ObSEArray<blocksstable::ObStorageObjectHandle, 16> block_handles;

  const double MIN_FREE_SPACE_PERCENTAGE = 0.1; //
  const int64_t MIN_CALIBRATION_BLOCK_COUNT = 1024L * 1024L * 1024L / OB_DEFAULT_MACRO_BLOCK_SIZE;
  const int64_t MAX_CALIBRATION_BLOCK_COUNT = 20L * 1024L * 1024L * 1024L / OB_DEFAULT_MACRO_BLOCK_SIZE;
  int64_t free_block_count = OB_STORAGE_OBJECT_MGR.get_free_macro_block_count();
  int64_t total_block_count = OB_STORAGE_OBJECT_MGR.get_total_macro_block_count();
  int64_t benchmark_block_count = free_block_count * 0.2;
  int64_t max_rnd_read_offset = OB_DEFAULT_MACRO_BLOCK_SIZE - load_size;
  int64_t ss_first_id = ObIOFd::NORMAL_FILE_ID;  // first_id is not used in shared storage mode;
  int64_t ss_second_id = OB_INVALID_FD;

  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || is_virtual_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant id invalid", KR(ret), K(tenant_id));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (GCTX.is_shared_storage_mode()) {
    MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
    if (OB_FAIL(guard.switch_to(tenant_id, false/*need_check_allow*/))) {
      LOG_WARN("fail to switch tenant", KR(ret), K(tenant_id));
    } else {
      ObTenantFileManager *tnt_file_manager = MTL(ObTenantFileManager*);
      ObTenantDiskSpaceManager *tnt_disk_space_manager = MTL(ObTenantDiskSpaceManager*);
      ss_second_id = tnt_file_manager->get_micro_cache_file_fd();
      if (OB_UNLIKELY(OB_INVALID_FD == ss_second_id)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("micro cache file is not exist, cannot do benchmark",
            KR(ret), K(tenant_id), K(ss_second_id));
      } else {
        const int64_t free_disk_size = tnt_disk_space_manager->get_micro_cache_file_size();
        free_block_count = free_disk_size / OB_DEFAULT_MACRO_BLOCK_SIZE;
        total_block_count = free_block_count;
        max_rnd_read_offset = free_disk_size - load_size;
        ss_second_id = tnt_file_manager->get_micro_cache_file_fd();
      }
    }
#endif
  }

  if (OB_FAIL(ret)) {
  } else if (free_block_count <= MIN_CALIBRATION_BLOCK_COUNT ||
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
  if (OB_SUCC(ret) && !GCTX.is_shared_storage_mode()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < benchmark_block_count; ++i) {
      blocksstable::ObStorageObjectOpt obj_opt;
      obj_opt.set_private_object_opt();
      blocksstable::ObStorageObjectHandle block_handle;
      if (OB_FAIL(OB_STORAGE_OBJECT_MGR.alloc_object(obj_opt, block_handle))) {
        LOG_WARN("alloc macro block failed", K(ret), K(i));
      } else if (OB_FAIL(block_handles.push_back(block_handle))) {
        LOG_WARN("push back block handle failed", K(ret), K(block_handle));
      }
    }
  }
  //test rnd io
  while (OB_SUCC(ret) && io_count < 100) {
    io_handle.reset();
    int64_t block_idx = ObRandom::rand(benchmark_block_count / 2, benchmark_block_count - 1);
    if (!GCTX.is_shared_storage_mode()) {
      io_info.fd_.first_id_ = block_handles[block_idx].get_macro_id().first_id();
      io_info.fd_.second_id_ = block_handles[block_idx].get_macro_id().second_id();
    } else {
      io_info.fd_.first_id_ = ss_first_id;
      io_info.fd_.second_id_ = ss_second_id;
    }
    io_info.fd_.device_handle_ = &LOCAL_DEVICE_INSTANCE;
    io_info.offset_ = ObRandom::rand(0, max_rnd_read_offset);
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
  io_info.offset_ = 0;
  while (OB_SUCC(ret) && io_count < 100 && io_count < benchmark_block_count / 2) {
    io_handle.reset();
    int64_t block_idx = io_count;
    if (!GCTX.is_shared_storage_mode()) {
      io_info.fd_.first_id_ = block_handles[block_idx].get_macro_id().first_id();
      io_info.fd_.second_id_ = block_handles[block_idx].get_macro_id().second_id();
      io_info.offset_ = 0;
    } else {
      io_info.fd_.first_id_ = ss_first_id;
      io_info.fd_.second_id_ = ss_second_id;
      io_info.offset_ += OB_DEFAULT_MACRO_BLOCK_SIZE;
    }
    io_info.fd_.device_handle_ = &LOCAL_DEVICE_INSTANCE;
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
