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
#ifdef ENABLE_QPL_COMPRESSION

#include <string.h>
#include <memory>
#include "codec_deflate_qpl.h"

namespace oceanbase
{
namespace common
{
namespace ZLIB_LITE
{

static constexpr int32_t RET_ERROR = -1;

CodecDeflateQpl::CodecDeflateQpl()
    : qpl_excute_path_(NULL),
      jobs_buffer_(NULL),
      job_ptr_pool_(NULL),
      job_pool_ready_(false),
      random_engine_(NULL)
{}

CodecDeflateQpl::~CodecDeflateQpl()
{}

int CodecDeflateQpl::init(qpl_path_t path, QplAllocator &allocator, int32_t job_num)
{
  using namespace std;
  int ret = 0;
  if (job_num < 0) {
    return -1;
  }
  if (job_pool_ready_) {
    return -1;
  }

  qpl_path_ = path;
  job_num_ = job_num;
  qpl_excute_path_ = (path == qpl_path_hardware) ? "Hardware" : "Software";
  allocator_ = allocator;

  if (0 == job_num) {
    job_pool_ready_ = true;
    return 0;
  }

  distribution_.~uniform_int_distribution<int>();
  new (&distribution_) std::uniform_int_distribution<int>(0, job_num - 1);
  void *random_engine_ptr = allocator.allocate(sizeof(std::mt19937));
  if (nullptr == random_engine_ptr) {
  } else {
    random_engine_ = new (random_engine_ptr)std::mt19937();

    //const char * qpl_version = qpl_get_library_version();

    /// Get size required for saving a single qpl job object
    uint32_t job_size = 0;
    qpl_get_job_size(path, &job_size);

    /// Allocate entire buffer for storing all job objects
    jobs_buffer_ = static_cast<char *>(allocator_.allocate(job_size * job_num));
    job_ptr_pool_ = static_cast<qpl_job **>(allocator.allocate(sizeof(qpl_job *) * job_num));
    job_ptr_locks_ = static_cast<std::atomic_bool *>(allocator.allocate(sizeof(std::atomic_bool) * job_num));
    if (nullptr == jobs_buffer_ || nullptr == job_ptr_pool_ || nullptr == job_ptr_locks_) {
      allocator.deallocate(jobs_buffer_);
      allocator.deallocate(job_ptr_pool_);
      allocator.deallocate(job_ptr_locks_);
      jobs_buffer_ = nullptr;
      job_ptr_pool_ = nullptr;
      job_ptr_locks_ = nullptr;
      return -1;
    }

    /// Initialize pool for storing all job object pointers
    /// Reallocate buffer by shifting address offset for each job object.
    for (uint32_t index = 0; index < job_num_; ++index) {
      new (job_ptr_locks_ + index) std::atomic_bool();

      qpl_job * qpl_job_ptr = (qpl_job *)(jobs_buffer_ + index * job_size);
      int status = qpl_init_job(path, qpl_job_ptr);
      if (status != QPL_STS_OK) {
        job_pool_ready_ = false;

        for (uint32_t i = 0; i < index; i++) {
          qpl_fini_job(job_ptr_pool_[i]);
        }

        allocator_.deallocate(jobs_buffer_);
        jobs_buffer_ = nullptr;
        allocator_.deallocate(job_ptr_pool_);
        job_ptr_pool_ = nullptr;
        allocator_.deallocate(job_ptr_locks_);
        job_ptr_locks_ = nullptr;
        return status;
      }
      job_ptr_pool_[index] = qpl_job_ptr;

      // Even if we use the hardware_path as the parameter, the cloud-qpl may init with software
      // if there is no hardware or something others unexpected.
      // If cloud-qpl change the path, we can get the info from the qpl_job_ptr->data_ptr.path.
      if (qpl_job_ptr->data_ptr.path != path) {
        qpl_excute_path_ = (qpl_job_ptr->data_ptr.path == qpl_path_hardware) ? "Hardware" : "Software";
      }
      unlock_job(index);
    }
  }

  job_pool_ready_ = true;
  return 0;
}

void CodecDeflateQpl::deinit()
{
  if (job_pool_ready_) {
    for (uint32_t i = 0; i < job_num_; ++i) {
      if (job_ptr_pool_[i] != nullptr) {
        while (!try_lock_job(i));
        qpl_fini_job(job_ptr_pool_[i]);
        unlock_job(i);
        job_ptr_pool_[i] = nullptr;
      }
    }

    allocator_.deallocate(jobs_buffer_);
    jobs_buffer_ = nullptr;
    allocator_.deallocate(job_ptr_pool_);
    job_ptr_pool_ = nullptr;
    allocator_.deallocate(job_ptr_locks_);
    job_pool_ready_ = false;
  }

  if (random_engine_ != nullptr) {
    allocator_.deallocate(random_engine_);
  }
}

CodecDeflateQpl & CodecDeflateQpl::get_hardware_instance()
{
  static CodecDeflateQpl hw_codec;
  return hw_codec;
}

CodecDeflateQpl & CodecDeflateQpl::get_software_instance()
{
  static CodecDeflateQpl sw_codec;
  return sw_codec;
}

void CodecDeflateQpl::release_job(uint32_t job_id, qpl_job *job_ptr)
{
  if (job_id < 0) {
    if (job_ptr != nullptr) {
      qpl_fini_job(job_ptr);
      allocator_.deallocate(job_ptr);
    }
  } else if (is_job_pool_ready()) {
    unlock_job(job_num_ - job_id);
  }
}

bool CodecDeflateQpl::try_lock_job(uint32_t index)
{
  bool bret = false;
  bool expected = false;
  if (index < job_num_) {
    bret = job_ptr_locks_[index].compare_exchange_strong(expected, true);
  }
  return bret;
}

void CodecDeflateQpl::unlock_job(uint32_t index)
{
  if (index < job_num_) {
    job_ptr_locks_[index].store(false);
  }
}

qpl_job * CodecDeflateQpl::acquire_job(uint32_t & job_id)
{
  job_id = -1;
  qpl_job *job = nullptr;
  if (job_num_ <= 0) {
  } else if (is_job_pool_ready() && random_engine_ != nullptr) {
    uint32_t retry = 0;
    int index = distribution_(*random_engine_);
    while (!try_lock_job(index)) {
      index = distribution_(*random_engine_);
      retry++;
      if (retry > job_num_) {
        break;
      }
    }

    if (retry <= job_num_) {
      job_id = job_num_ - index;
      if (index < job_num_) {
        job = job_ptr_pool_[index];
      }
    }
  }

  if (nullptr == job) {
    /// Get size required for saving a single qpl job object
    uint32_t job_size = 0;
    qpl_get_job_size(qpl_path_, &job_size);

    /// Allocate buffer for storing job
    job = static_cast<qpl_job *>(allocator_.allocate(job_size));
    if (job != nullptr) {
      int status = qpl_init_job(qpl_path_, job);
      if (QPL_STS_OK != status) {
        allocator_.deallocate(job);
        job = nullptr;
      }
    }
  }
  return job;
}

int64_t CodecDeflateQpl::do_compress_data(const char * source, uint32_t source_size, char * dest, uint32_t dest_size)
{
  uint32_t job_id = -1;
  qpl_job * job_ptr = nullptr;
  uint32_t compressed_size = 0;
  int64_t  ret = RET_ERROR;

  job_ptr = acquire_job(job_id);

  if (job_ptr != nullptr) {

    job_ptr->op = qpl_op_compress;
    job_ptr->next_in_ptr = reinterpret_cast<uint8_t *>(const_cast<char *>(source));
    job_ptr->next_out_ptr = reinterpret_cast<uint8_t *>(dest);
    job_ptr->available_in = source_size;
    job_ptr->level = qpl_default_level;
    job_ptr->available_out = dest_size;
    job_ptr->flags = QPL_FLAG_FIRST | QPL_FLAG_DYNAMIC_HUFFMAN | QPL_FLAG_LAST;

    qpl_status status = qpl_execute_job(job_ptr);

    if (status == QPL_STS_OK) {
      compressed_size = job_ptr->total_out;
      ret = static_cast<int64_t>(compressed_size);
    } else {
      ret = RET_ERROR;
    }

    release_job(job_id, job_ptr);
  }
  return ret;
}

int64_t CodecDeflateQpl::do_decompress_data(const char * source, uint32_t source_size, char * dest, uint32_t uncompressed_size)
{
  uint32_t  job_id = -1;
  qpl_job * job_ptr = nullptr;
  uint32_t  decompressed_size = 0;
  int64_t   ret = RET_ERROR;

  job_ptr = acquire_job(job_id);

  if (job_ptr != nullptr) {
    job_ptr->op = qpl_op_decompress;
    job_ptr->next_in_ptr = reinterpret_cast<uint8_t *>(const_cast<char *>(source));
    job_ptr->next_out_ptr = reinterpret_cast<uint8_t *>(dest);
    job_ptr->available_in = source_size;
    job_ptr->available_out = uncompressed_size;
    job_ptr->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST;

    qpl_status status = qpl_execute_job(job_ptr);

    if (status == QPL_STS_OK) {
      decompressed_size = job_ptr->total_out;
      ret = static_cast<int64_t>(decompressed_size);
    } else {
      ret = RET_ERROR;
    }

    release_job(job_id, job_ptr);
  }

  return ret;
}

int qpl_init(QplAllocator &allocator, int32_t job_num)
{
  int ret = 0;
  CodecDeflateQpl &hardware_qpl = CodecDeflateQpl::get_hardware_instance();
  ret = hardware_qpl.init(qpl_path_hardware, allocator, job_num);
  if (0 == ret) {
    // If there is no hardware, qpl will use software instead
  } else if (QPL_STS_NO_MEM_ERR == ret) {
    // If you do not install the usdm_drv, the cloud-qpl return this error code.
    // But we can use software instead
    CodecDeflateQpl &software_qpl = CodecDeflateQpl::get_software_instance();
    ret = software_qpl.init(qpl_path_software, allocator, job_num);
  }

  return ret;
}

void qpl_deinit()
{
  CodecDeflateQpl &hardware_qpl = CodecDeflateQpl::get_hardware_instance();
  hardware_qpl.deinit();

  CodecDeflateQpl &software_qpl = CodecDeflateQpl::get_software_instance();
  software_qpl.deinit();
}

int64_t qpl_compress(const char* source, char* dest, int input_size, int max_output_size)
{
  int64_t res = RET_ERROR;
  if (CodecDeflateQpl::get_hardware_instance().is_job_pool_ready()) {
    res = CodecDeflateQpl::get_hardware_instance().do_compress_data(
        source, input_size, dest, max_output_size);
  }
  if (res == RET_ERROR && CodecDeflateQpl::get_software_instance().is_job_pool_ready()) {
    res = CodecDeflateQpl::get_software_instance().do_compress_data(
        source, input_size, dest, max_output_size);
  }
  return res;
}

int64_t qpl_decompress(const char* source, char* dest, int input_size, int max_output_size)
{
  int64_t res = RET_ERROR;
  if (CodecDeflateQpl::get_hardware_instance().is_job_pool_ready()) {
    res = CodecDeflateQpl::get_hardware_instance().do_decompress_data(
        source, input_size, dest, max_output_size);
  }
  if (res == RET_ERROR && CodecDeflateQpl::get_software_instance().is_job_pool_ready()) {
    res = CodecDeflateQpl::get_software_instance().do_decompress_data(
        source, input_size, dest, max_output_size);
  }
  return res;
}

bool qpl_hardware_enabled()
{
  CodecDeflateQpl &hardware_qpl = CodecDeflateQpl::get_hardware_instance();
  return hardware_qpl.is_job_pool_ready() && 0 == strcasecmp("Hardware", hardware_qpl.qpl_execute_path());
}

}//namespace ZLIB_LITE
}//namespace common
}//namespace oceanbase

#endif // ENABLE_QPL_COMPRESSION
