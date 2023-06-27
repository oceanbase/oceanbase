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

#include <memory>
#include "codec_deflate_qpl.h"

namespace oceanbase
{
namespace common
{
namespace ZLIB_LITE
{

static constexpr int32_t RET_ERROR = -1;

CodecDeflateQpl::CodecDeflateQpl(qpl_path_t path)
    : random_engine_(std::random_device()())
    , distribution_(0, MAX_JOB_NUMBER - 1)
{
  uint32_t job_size = 0;
  const char * qpl_version = qpl_get_library_version();
  qpl_excute_path_ = (path == qpl_path_hardware) ? "Hardware" : "Software";
  /// Get size required for saving a single qpl job object
  qpl_get_job_size(path, &job_size);
  /// Allocate entire buffer for storing all job objects
  jobs_buffer_ = new char[job_size * MAX_JOB_NUMBER];
  /// Initialize pool for storing all job object pointers
  /// Reallocate buffer by shifting address offset for each job object.
  for (uint32_t index = 0; index < MAX_JOB_NUMBER; ++index) {
    qpl_job * qpl_job_ptr = (qpl_job *)(jobs_buffer_ + index * job_size);
    auto status = qpl_init_job(path, qpl_job_ptr); 
    if (status != QPL_STS_OK) {
      job_pool_ready_ = false;
      delete [] jobs_buffer_;
      jobs_buffer_ = nullptr;
      if (path == qpl_path_hardware) {
        LIB_LOG(WARN, "Initialization of IAA hardware failed, will attempt to use software DeflateQpl codec instead of hardware DeflateQpl codec.",
                K(status), KCSTRING(qpl_version));
      } else {
        LIB_LOG(WARN, "Initialization of software DeflateQpl codec failed, QPL compression/decompression cannot be enabled.", K(status));
      }
 
      return;
    }
    job_ptr_pool_[index] = qpl_job_ptr;
    unlock_job(index);
  }

  LIB_LOG(INFO, "assisted DeflateQpl codec is ready!", KCSTRING(qpl_excute_path_), KCSTRING(qpl_version));
    
  job_pool_ready_ = true;
}

CodecDeflateQpl::~CodecDeflateQpl()
{
  if (is_job_pool_ready()) {
    for (uint32_t i = 0; i < MAX_JOB_NUMBER; ++i) {
      if (job_ptr_pool_[i] != nullptr) {
        while (!try_lock_job(i));
        qpl_fini_job(job_ptr_pool_[i]);
        unlock_job(i);
        job_ptr_pool_[i] = nullptr;
      }
    }

    delete[] jobs_buffer_;
    jobs_buffer_ = nullptr;
    job_pool_ready_ = false;
  }
}

CodecDeflateQpl & CodecDeflateQpl::get_hardware_instance()
{
  static CodecDeflateQpl hw_codec(qpl_path_hardware);
  return hw_codec;
}

CodecDeflateQpl & CodecDeflateQpl::get_software_instance()
{
  static CodecDeflateQpl sw_codec(qpl_path_software);
  return sw_codec;
}

void CodecDeflateQpl::release_job(uint32_t job_id)
{
  if (is_job_pool_ready()) {
    unlock_job(MAX_JOB_NUMBER - job_id);
  }
}

bool CodecDeflateQpl::try_lock_job(uint32_t index)
{
  bool bret = false;
  bool expected = false;
  if (index < MAX_JOB_NUMBER) {
    bret = job_ptr_locks_[index].compare_exchange_strong(expected, true);
  }
  return bret;
}

void CodecDeflateQpl::unlock_job(uint32_t index)
{
  if (index < MAX_JOB_NUMBER) {
    job_ptr_locks_[index].store(false);
  }
}

qpl_job * CodecDeflateQpl::acquire_job(uint32_t & job_id)
{
  qpl_job *job = nullptr;
  if (is_job_pool_ready()) {
    uint32_t retry = 0;
    int index = distribution_(random_engine_);
    while (!try_lock_job(index)) {
      index = distribution_(random_engine_);
      retry++;
      if (retry > MAX_JOB_NUMBER) {
        return nullptr;
      }
    }
    job_id = MAX_JOB_NUMBER - index;
    if (index < MAX_JOB_NUMBER) {
      job = job_ptr_pool_[index];
    }
  }
  return job;
}

int32_t CodecDeflateQpl::do_compress_data(const char * source, uint32_t source_size, char * dest, uint32_t dest_size) 
{
  uint32_t job_id = 0;
  qpl_job * job_ptr = nullptr;
  uint32_t compressed_size = 0;
  if (!(job_ptr = acquire_job(job_id))) {
    LIB_LOG(WARN, "DeflateQpl codec failed, doCompressData->acquireJob fail.", KCSTRING(qpl_excute_path_));
    return RET_ERROR;
  }

  job_ptr->op = qpl_op_compress;
  job_ptr->next_in_ptr = reinterpret_cast<uint8_t *>(const_cast<char *>(source));
  job_ptr->next_out_ptr = reinterpret_cast<uint8_t *>(dest);
  job_ptr->available_in = source_size;
  job_ptr->level = qpl_default_level;
  job_ptr->available_out = dest_size;
  job_ptr->flags = QPL_FLAG_FIRST | QPL_FLAG_DYNAMIC_HUFFMAN | QPL_FLAG_LAST | QPL_FLAG_OMIT_VERIFY;

  qpl_status status = qpl_execute_job(job_ptr);

  if (status == QPL_STS_OK) {
    compressed_size = job_ptr->total_out;
    release_job(job_id);
    return compressed_size;
  } else {
    LIB_LOG(WARN, "DeflateQpl codec failed, doCompressData->qpl_execute_job.", KCSTRING(qpl_excute_path_), K(status));
    release_job(job_id);
    return RET_ERROR;
  }
}

int32_t CodecDeflateQpl::do_decompress_data(const char * source, uint32_t source_size, char * dest, uint32_t uncompressed_size)
{
  uint32_t job_id = 0;
  qpl_job * job_ptr = nullptr;
  uint32_t decompressed_size = 0;
  if (!(job_ptr = acquire_job(job_id))) {
    LIB_LOG(WARN, "DeflateQpl codec failed, doDecompressData->acquireJob fail: acquire qpl job failed.", KCSTRING(qpl_excute_path_));
    return RET_ERROR;
  }

  job_ptr->op = qpl_op_decompress;
  job_ptr->next_in_ptr = reinterpret_cast<uint8_t *>(const_cast<char *>(source));
  job_ptr->next_out_ptr = reinterpret_cast<uint8_t *>(dest);
  job_ptr->available_in = source_size;
  job_ptr->available_out = uncompressed_size;
  job_ptr->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST;

  qpl_status status = qpl_execute_job(job_ptr);

  if (status == QPL_STS_OK) {
    decompressed_size = job_ptr->total_out;
    release_job(job_id);
    return decompressed_size;
  } else {
    LIB_LOG(WARN, "DeflateQpl codec failed, doDeCompressData->qpl_execute_job", KCSTRING(qpl_excute_path_), K(status));
    release_job(job_id); 
    return RET_ERROR;
  }

  return decompressed_size;
}

int32_t qpl_compress(const char* source, char* dest, int input_size, int max_output_size)
{
  int32_t res = RET_ERROR;
  if (CodecDeflateQpl::get_hardware_instance().is_job_pool_ready()) {
    res = CodecDeflateQpl::get_hardware_instance().do_compress_data(
        source, input_size, dest, max_output_size);
  }
  if (res == RET_ERROR) {
    res = CodecDeflateQpl::get_software_instance().do_compress_data(
        source, input_size, dest, max_output_size);
  }
  return res;
}

int32_t qpl_decompress(const char* source, char* dest, int input_size, int max_output_size)
{
  int32_t res = RET_ERROR;
  if (CodecDeflateQpl::get_hardware_instance().is_job_pool_ready()) {
    res = CodecDeflateQpl::get_hardware_instance().do_decompress_data(
        source, input_size, dest, max_output_size);
  }
  if (res == RET_ERROR) {
    res = CodecDeflateQpl::get_software_instance().do_decompress_data(
        source, input_size, dest, max_output_size);
  }
  return res;
}


}//namespace ZLIB_LITE
}//namespace common
}//namespace oceanbase

#endif // ENABLE_QPL_COMPRESSION
