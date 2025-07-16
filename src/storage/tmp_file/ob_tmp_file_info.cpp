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

#define USING_LOG_PREFIX STORAGE

#include "storage/tmp_file/ob_tmp_file_info.h"
#include "share/ob_errno.h"

namespace oceanbase
{
namespace tmp_file
{
void ObTmpFileWriteInfo::record_write_stat(bool is_unaligned_write,
                                           int64_t write_persisted_tail_page_cnt,
                                           bool lack_page_cnt)
{
  write_req_cnt_++;
  if (is_unaligned_write) {
    unaligned_write_req_cnt_++;
  }
  write_persisted_tail_page_cnt_ += write_persisted_tail_page_cnt;
  lack_page_cnt_ += lack_page_cnt;
  last_modify_ts_ = ObTimeUtility::current_time();
}

void ObTmpFileWriteInfo::reset()
{
  write_req_cnt_ = 0;
  unaligned_write_req_cnt_ = 0;
  write_persisted_tail_page_cnt_ = 0;
  lack_page_cnt_ = 0;
  last_modify_ts_ = -1;
}

void ObTmpFileReadInfo::record_read_stat(bool is_unaligned_read, int64_t read_size,
                                         int64_t total_truncated_page_read_cnt, int64_t total_kv_cache_page_read_cnt,
                                         int64_t total_uncached_page_read_cnt, int64_t total_wbp_page_read_cnt,
                                         int64_t truncated_page_read_hits, int64_t kv_cache_page_read_hits,
                                         int64_t uncached_page_read_hits, int64_t aggregate_read_io_cnt,
                                         int64_t wbp_page_read_hits)
{
  read_req_cnt_++;
  if (is_unaligned_read) {
    unaligned_read_req_cnt_++;
  }
  total_read_size_ += read_size;
  last_access_ts_ = ObTimeUtility::current_time();
  total_truncated_page_read_cnt_ += total_truncated_page_read_cnt;
  total_kv_cache_page_read_cnt_ += total_kv_cache_page_read_cnt;
  total_uncached_page_read_cnt_ += total_uncached_page_read_cnt;
  total_wbp_page_read_cnt_ += total_wbp_page_read_cnt;
  truncated_page_read_hits_ += truncated_page_read_hits;
  kv_cache_page_read_hits_ += kv_cache_page_read_hits;
  uncached_page_read_hits_ += uncached_page_read_hits;
  aggregate_read_io_cnt_ += aggregate_read_io_cnt;
  wbp_page_read_hits_ += wbp_page_read_hits;
}

void ObTmpFileReadInfo::reset()
{
  read_req_cnt_ = 0;
  unaligned_read_req_cnt_ = 0;
  total_truncated_page_read_cnt_ = 0;
  total_kv_cache_page_read_cnt_ = 0;
  total_uncached_page_read_cnt_ = 0;
  total_wbp_page_read_cnt_ = 0;
  truncated_page_read_hits_ = 0;
  kv_cache_page_read_hits_ = 0;
  uncached_page_read_hits_ = 0;
  aggregate_read_io_cnt_ = 0;
  wbp_page_read_hits_ = 0;
  total_read_size_ = 0;
  last_access_ts_ = -1;
}

void ObTmpFileMetaTreeInfo::reset()
{
  meta_tree_epoch_ = 0;
  meta_tree_level_cnt_ = 0;
  meta_size_ = 0;
  cached_meta_page_num_ = 0;
}

int ObTmpFileBaseInfo::init(
    const ObCurTraceId::TraceId &trace_id,
    const uint64_t tenant_id,
    const int64_t dir_id,
    const int64_t fd,
    const int64_t file_size,
    const int64_t truncated_offset,
    const bool is_deleting,
    const int64_t ref_cnt,
    const int64_t birth_ts,
    const void* const tmp_file_ptr,
    const char* const label,
    const OB_TMP_FILE_TYPE file_type,
    const int64_t compressible_fd)
{
  int ret = OB_SUCCESS;
  trace_id_ = trace_id;
  tenant_id_ = tenant_id;
  dir_id_ = dir_id;
  fd_ = fd;
  file_size_ = file_size;
  truncated_offset_ = truncated_offset;
  is_deleting_ = is_deleting;
  ref_cnt_ = ref_cnt;
  birth_ts_ = birth_ts;
  tmp_file_ptr_ = tmp_file_ptr;
  if (NULL != label) {
    label_.assign_strive(label);
  }
  file_type_ = file_type;
  compressible_fd_ = compressible_fd;
  return ret;
}

void ObTmpFileBaseInfo::reset()
{
  trace_id_.reset();
  tenant_id_ = OB_INVALID_TENANT_ID;
  dir_id_ = ObTmpFileGlobal::INVALID_TMP_FILE_DIR_ID;
  fd_ = ObTmpFileGlobal::INVALID_TMP_FILE_FD;
  file_size_ = 0;
  truncated_offset_ = 0;
  is_deleting_ = false;
  ref_cnt_ = 0;
  birth_ts_ = -1;
  tmp_file_ptr_ = nullptr;
  label_.reset();
  file_type_ = OB_TMP_FILE_TYPE::NORMAL;
  compressible_fd_ = ObTmpFileGlobal::INVALID_TMP_FILE_FD;
}

int ObSNTmpFileInfo::init(const ObCurTraceId::TraceId &trace_id,
                          const uint64_t tenant_id,
                          const int64_t dir_id,
                          const int64_t fd,
                          const int64_t file_size,
                          const int64_t truncated_offset,
                          const bool is_deleting,
                          const int64_t ref_cnt,
                          const int64_t birth_ts,
                          const void* const tmp_file_ptr,
                          const char* const label,
                          const OB_TMP_FILE_TYPE file_type,
                          const int64_t compressible_fd,
                          const ObTmpFileMetaTreeInfo &tree_info,
                          const ObTmpFileWriteInfo &write_info,
                          const ObTmpFileReadInfo &read_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTmpFileBaseInfo::init(trace_id, tenant_id, dir_id, fd, file_size,
                                  truncated_offset, is_deleting, ref_cnt, birth_ts,
                                  tmp_file_ptr, label, file_type, compressible_fd))) {
    LOG_WARN("fail to init ObTmpFileBaseInfo", KR(ret), K(trace_id), K(tenant_id), K(dir_id), K(fd),
             K(file_size), K(truncated_offset), K(is_deleting), K(ref_cnt), K(birth_ts),
             K(tmp_file_ptr), K(label), K(file_type), K(compressible_fd));
  } else {
    tree_info_ = tree_info;
    write_info_ = write_info;
    read_info_ = read_info;
  }
  return ret;
}

void ObSNTmpFileInfo::reset()
{
  tree_info_.reset();
  write_info_.reset();
  read_info_.reset();
  ObTmpFileBaseInfo::reset();
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObSSTmpFileInfo::init(
    const ObCurTraceId::TraceId &trace_id,
    const uint64_t tenant_id,
    const int64_t dir_id,
    const int64_t fd,
    const int64_t file_size,
    const int64_t truncated_offset,
    const bool is_deleting,
    const int64_t cached_page_num,
    const int64_t write_back_data_page_num,
    const int64_t flushed_data_page_num,
    const int64_t ref_cnt,
    const int64_t birth_ts,
    const void* const tmp_file_ptr,
    const char* const label,
    const OB_TMP_FILE_TYPE file_type,
    const int64_t compressible_fd,
    const ObTmpFileWriteInfo &write_info,
    const ObTmpFileReadInfo &read_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTmpFileBaseInfo::init(trace_id, tenant_id, dir_id, fd, file_size,
                                      truncated_offset, is_deleting, ref_cnt, birth_ts,
                                      tmp_file_ptr, label, file_type, compressible_fd))) {
    LOG_WARN("fail to init ObTmpFileBaseInfo", KR(ret), K(trace_id), K(tenant_id), K(dir_id), K(fd),
             K(file_size), K(truncated_offset), K(is_deleting), K(ref_cnt), K(birth_ts),
             K(tmp_file_ptr), K(label), K(file_type), K(compressible_fd));
  } else {
    cached_data_page_num_ = cached_page_num;
    write_back_data_page_num_ = write_back_data_page_num;
    flushed_data_page_num_ = flushed_data_page_num;
    write_info_ = write_info;
    read_info_ = read_info;
  }
  return ret;
}

void ObSSTmpFileInfo::reset()
{
  write_info_.reset();
  read_info_.reset();
  ObTmpFileBaseInfo::reset();
}
#endif

}  // end namespace tmp_file
}  // end namespace oceanbase
