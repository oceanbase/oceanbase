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

#ifndef OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_INFO_H_
#define OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_INFO_H_

#include "storage/tmp_file/ob_tmp_file_global.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
namespace tmp_file
{
class ObTmpFileIOReadCtx;
class ObTmpFileIOWriteCtx;

struct ObTmpFileWriteInfo
{
public:
  ObTmpFileWriteInfo() :
    write_req_cnt_(0),
    unaligned_write_req_cnt_(0),
    write_persisted_tail_page_cnt_(0),
    lack_page_cnt_(0),
    last_modify_ts_(-1) {}
  void record_write_stat(bool is_unaligned_write, int64_t write_persisted_tail_page_cnt, bool lack_page_cnt);
  void reset();
public:
  int64_t write_req_cnt_;
  int64_t unaligned_write_req_cnt_;
  int64_t write_persisted_tail_page_cnt_;
  int64_t lack_page_cnt_;
  int64_t last_modify_ts_;
  TO_STRING_KV(K(write_req_cnt_), K(unaligned_write_req_cnt_),
               K(write_persisted_tail_page_cnt_), K(lack_page_cnt_), K(last_modify_ts_));
};

struct ObTmpFileReadInfo
{
public:
  ObTmpFileReadInfo() :
    read_req_cnt_(0),
    unaligned_read_req_cnt_(0),
    total_truncated_page_read_cnt_(0),
    total_kv_cache_page_read_cnt_(0),
    total_uncached_page_read_cnt_(0),
    total_wbp_page_read_cnt_(0),
    truncated_page_read_hits_(0),
    kv_cache_page_read_hits_(0),
    uncached_page_read_hits_(0),
    aggregate_read_io_cnt_(0),
    wbp_page_read_hits_(0),
    total_read_size_(0),
    last_access_ts_(-1) {}
  void record_read_stat(bool is_unaligned_read, int64_t read_size,
                        int64_t total_truncated_page_read_cnt, int64_t total_kv_cache_page_read_cnt,
                        int64_t total_uncached_page_read_cnt, int64_t total_wbp_page_read_cnt,
                        int64_t truncated_page_read_hits, int64_t kv_cache_page_read_hits,
                        int64_t uncached_page_read_hits, int64_t aggregate_read_io_cnt,
                        int64_t wbp_page_read_hits);
  virtual void reset();
public:
  int64_t read_req_cnt_;
  int64_t unaligned_read_req_cnt_;
  int64_t total_truncated_page_read_cnt_;          // the total read count of truncated pages
  int64_t total_kv_cache_page_read_cnt_;           // the total read count of pages in kv_cache
  int64_t total_uncached_page_read_cnt_;           // the total read count of pages with io
  int64_t total_wbp_page_read_cnt_;                // the total read count of pages in wbp
  int64_t truncated_page_read_hits_;               // the hit count of truncated pages when read
  int64_t kv_cache_page_read_hits_;                // the hit count of pages in kv_cache when read
  int64_t uncached_page_read_hits_;                // the hit count of persisted pages when read
  int64_t aggregate_read_io_cnt_;                  // the count of aggregating read (prefetch read)
  int64_t wbp_page_read_hits_;                     // the hit count of pages in wbp when read
  int64_t total_read_size_;
  int64_t last_access_ts_;
  TO_STRING_KV(K(read_req_cnt_), K(unaligned_read_req_cnt_), K(total_truncated_page_read_cnt_),
               K(total_kv_cache_page_read_cnt_), K(total_uncached_page_read_cnt_),
               K(total_wbp_page_read_cnt_), K(truncated_page_read_hits_), K(kv_cache_page_read_hits_),
               K(uncached_page_read_hits_), K(aggregate_read_io_cnt_), K(wbp_page_read_hits_),
               K(total_read_size_), K(last_access_ts_));
};

struct ObTmpFileMetaTreeInfo
{
public:
  ObTmpFileMetaTreeInfo() :
    meta_tree_epoch_(0),
    meta_tree_level_cnt_(0),
    meta_size_(0),
    cached_meta_page_num_(0) {}
  void reset();
public:
  int64_t meta_tree_epoch_;
  int64_t meta_tree_level_cnt_;
  int64_t meta_size_;
  int64_t cached_meta_page_num_;
  TO_STRING_KV(K(meta_tree_epoch_), K(meta_tree_level_cnt_), K(meta_size_), K(cached_meta_page_num_));
};

//for virtual table show
class ObTmpFileBaseInfo
{
public:
  ObTmpFileBaseInfo() :
    trace_id_(),
    tenant_id_(OB_INVALID_TENANT_ID),
    dir_id_(ObTmpFileGlobal::INVALID_TMP_FILE_DIR_ID),
    fd_(ObTmpFileGlobal::INVALID_TMP_FILE_FD),
    file_size_(0),
    truncated_offset_(0),
    is_deleting_(false),
    ref_cnt_(0),
    birth_ts_(-1),
    tmp_file_ptr_(nullptr),
    label_(),
    file_type_(OB_TMP_FILE_TYPE::NORMAL),
    compressible_fd_(ObTmpFileGlobal::INVALID_TMP_FILE_FD)
    {}
  virtual ~ObTmpFileBaseInfo() { reset(); }
  int init(const ObCurTraceId::TraceId &trace_id,
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
           const int64_t compressible_fd);
  virtual ObTmpFileWriteInfo &get_write_info() = 0;
  virtual ObTmpFileReadInfo &get_read_info() = 0;
  virtual void reset();
public:
  common::ObCurTraceId::TraceId trace_id_;
  uint64_t tenant_id_;
  int64_t dir_id_;
  int64_t fd_;
  int64_t file_size_;
  int64_t truncated_offset_;
  bool is_deleting_;
  int64_t ref_cnt_;
  int64_t birth_ts_;
  const void *tmp_file_ptr_;
  ObFixedLengthString<ObTmpFileGlobal::TMP_FILE_MAX_LABEL_SIZE + 1> label_;
  OB_TMP_FILE_TYPE file_type_;
  int64_t compressible_fd_;

  TO_STRING_KV(K(trace_id_), K(tenant_id_), K(dir_id_), K(fd_), K(file_size_),
               K(truncated_offset_), K(is_deleting_),
               K(ref_cnt_), K(birth_ts_), KP(tmp_file_ptr_), K(label_),
               K(file_type_), K(compressible_fd_));
};

//for virtual table show
class ObSNTmpFileInfo final : public ObTmpFileBaseInfo
{
public:
  ObSNTmpFileInfo() :
    ObTmpFileBaseInfo(),
    tree_info_(),
    write_info_(),
    read_info_() {}

  int init(const ObCurTraceId::TraceId &trace_id,
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
           const ObTmpFileReadInfo &read_info);
  virtual ~ObSNTmpFileInfo() { reset(); }
  virtual void reset() override;
  virtual ObTmpFileWriteInfo &get_write_info() override { return write_info_; }
  virtual ObTmpFileReadInfo &get_read_info() override { return read_info_; }

public:
  ObTmpFileMetaTreeInfo tree_info_;
  ObTmpFileWriteInfo write_info_;
  ObTmpFileReadInfo read_info_;
  INHERIT_TO_STRING_KV("ObTmpFileBaseInfo", ObTmpFileBaseInfo,
                       K(tree_info_), K(write_info_), K(read_info_));
};

#ifdef OB_BUILD_SHARED_STORAGE
class ObSSTmpFileInfo final : public ObTmpFileBaseInfo
{
public:
  ObSSTmpFileInfo() :
    ObTmpFileBaseInfo(),
    cached_data_page_num_(0),
    write_back_data_page_num_(0),
    flushed_data_page_num_(0) {}
  int init(const ObCurTraceId::TraceId &trace_id,
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
           const ObTmpFileReadInfo &read_info);
  virtual ~ObSSTmpFileInfo() { reset(); }
  virtual void reset() override;
  virtual ObTmpFileWriteInfo &get_write_info() override { return write_info_; }
  virtual ObTmpFileReadInfo &get_read_info() override { return read_info_; }
public:
  int64_t cached_data_page_num_;
  int64_t write_back_data_page_num_;
  int64_t flushed_data_page_num_;
  ObTmpFileWriteInfo write_info_;
  ObTmpFileReadInfo read_info_;
  INHERIT_TO_STRING_KV("ObTmpFileBaseInfo", ObTmpFileBaseInfo,
                       K(cached_data_page_num_), K(write_back_data_page_num_),
                       K(flushed_data_page_num_), K(write_info_), K(read_info_));
};
#endif

}  // end namespace tmp_file
}  // end namespace oceanbase

#endif // OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_INFO_H_
