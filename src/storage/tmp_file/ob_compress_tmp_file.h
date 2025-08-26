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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_COMPRESS_TMP_FILE_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_COMPRESS_TMP_FILE_H_

#include "storage/tmp_file/ob_tmp_file_io_info.h"
#include "storage/tmp_file/ob_compress_tmp_file_io_handle.h"

namespace oceanbase
{
namespace tmp_file
{
class ObCompressTmpFile final
{
public:
  struct CompressedUnitHeader
  {
    static const uint32_t MAGIC_NUM = 0xcea0ba3e;

    CompressedUnitHeader()
      : is_compressed_(false),
        magic_num_(MAGIC_NUM),
        data_len_(0),
        original_data_offset_(0),
        original_data_len_(0) {}
    OB_INLINE void reset()
    {
      is_compressed_ = false;
      magic_num_ = MAGIC_NUM;
      data_len_ = 0;
      original_data_offset_ = 0;
      original_data_len_ = 0;
    }
    OB_INLINE bool is_valid(const int64_t compress_unit_size)
    {
      return data_len_ > 0
             && MAGIC_NUM == magic_num_
             && original_data_len_ > 0
             && data_len_ <= original_data_len_
             && original_data_len_ <= compress_unit_size;
    }
    TO_STRING_KV(K(is_compressed_), K(magic_num_),
        K(data_len_), K(original_data_offset_), K(original_data_len_));
    bool is_compressed_; //是否压缩
    uint32_t magic_num_;
    int32_t data_len_;//压缩后的长度
    uint64_t original_data_offset_;
    int32_t original_data_len_;//压缩前的长度
  };
  enum TaskStep : int16_t
  {
    INVALID_STEP = -1,
    FIRST_STEP = 0, //inited, nothing has done, ready to alloc buf
    SECOND_STEP = 1, //has alloced buffer, ready to held data segment
    THIRD_STEP = 2, //has hold data segment, ready to compress and write
    FINISHED = 3,
  };
  struct WaitTask : public common::ObDLinkBase<WaitTask>
  {
    WaitTask() : start_offset_(-1),
                 write_cond_() {}
    OB_INLINE bool is_valid()
    {
      return 0 <= start_offset_;
    }
    int64_t start_offset_; //start read offset from buffer tmp file
    common::ObCond write_cond_; //write to compressed tmp file if cond is met
    TO_STRING_KV(K(start_offset_));
  };
  //write to compressed file
  struct CompressTask
  {
    explicit CompressTask()
      : task_step_(INVALID_STEP),
        read_buf_(nullptr),
        read_size_(0),
        compress_buf_(nullptr),
        compress_buf_size_(0),
        wait_task_() {}
    OB_INLINE bool is_valid_to_compress(const int64_t COMPRESS_UNIT_SIZE)
    {
      return THIRD_STEP == task_step_
             && NULL != read_buf_
             && 0 < read_size_
             && COMPRESS_UNIT_SIZE >= read_size_
             && NULL != compress_buf_
             && 0 < compress_buf_size_
             && wait_task_.is_valid();
    }

    TaskStep task_step_;
    char *read_buf_;
    int64_t read_size_;
    char *compress_buf_;
    int64_t compress_buf_size_;
    WaitTask wait_task_;
    TO_STRING_KV(K(task_step_), KP(read_buf_), K(read_size_),
                 KP(compress_buf_), K(compress_buf_size_), K(wait_task_));
  };
public:
  ObCompressTmpFile();
  ~ObCompressTmpFile();
  void reset();
  int init(const uint64_t tenant_id,
           int64_t &fd,
           const int64_t dir_id,
           const ObCompressorType comptype,
           const int64_t comp_unit_size,
           const char* const label,
           ObFIFOAllocator* buf_allocator,
           const void* file_bunch);
  int read(const uint64_t tenant_id, const ObTmpFileIOInfo &io_info, ObCompTmpFileIOHandle &io_handle);
  int write(const uint64_t tenant_id, const ObTmpFileIOInfo &io_info, ObCompTmpFileIOHandle &io_handle);
  int seal(const uint64_t tenant_id);
  int remove_files(const uint64_t tenant_id);
  int remove_files_if_needed(const uint64_t tenant_id);
  int set_deleting();
  OB_INLINE void inc_ref_cnt() { ATOMIC_INC(&ref_cnt_); }
  OB_INLINE void dec_ref_cnt() { ATOMIC_AAF(&ref_cnt_, -1); }
  OB_INLINE int64_t get_ref_cnt() const { return ATOMIC_LOAD(&ref_cnt_); }
  bool can_remove();
  void get_file_size(int64_t &file_size);
  TO_STRING_KV(K(COMPRESS_UNIT_SIZE),
               K(ref_cnt_), K(fd_), K(compressed_fd_),
               K(end_offset_for_compress_), K(compressed_read_offset_), K(read_offset_), K(is_sealed_),
               K(is_deleting_), K(err_marked_), K(next_unit_header_inited_), K(next_unit_header_),
               KP(buf_allocator_), KP(compressor_), K(file_size_),
               K(moved_data_size_),
               KP(decompress_buf_), K(decompress_buf_start_offset_), K(decompress_buf_valid_len_),
               KP(read_buf_), K(read_buf_start_offset_), K(read_buf_valid_len_),
               K(PREFETCH_SIZE), K(READ_BUF_SIZE));
private:
  int decompress_data_and_update_var_(const int64_t remain_size, char *comp_read_buf, const int64_t comp_read_size, char *user_buf, int64_t &copy_size);
  int compress_read_(const uint64_t tenant_id, const ObTmpFileIOInfo &io_info, ObTmpFileIOHandle &io_handle);
  int move_data_to_compressed_file_(const uint64_t tenant_id, const ObTmpFileIOInfo &io_info, const int64_t end_offset, CompressTask &wait_task, ObCompTmpFileIOHandle &io_handle);
  int move_data_to_compressed_file_(const uint64_t tenant_id, CompressTask &wait_task, ObCompTmpFileIOHandle &io_handle);
  int read_and_then_write_(const uint64_t tenant_id, const ObTmpFileIOInfo &io_info, CompressTask &compress_task, ObCompTmpFileIOHandle &comp_io_handle);
  void generate_read_io_info_(char *read_buf, const int64_t read_size, const ObTmpFileIOInfo &user_io_info, ObTmpFileIOInfo &read_io_info);
  void generate_write_io_info_(char *buf, const int64_t write_size, const ObTmpFileIOInfo &user_io_info, ObTmpFileIOInfo &write_io_info);
  void print_wait_list_(int return_ret);
  int signal_wait_tasks_();

  int next_comp_read_buf_(const ObTmpFileIOInfo &user_io_info,
                          const uint64_t tenant_id,
                          ObTmpFileIOHandle &io_handle,
                          char*& comp_read_buf,
                          int64_t& comp_read_size);
  int mark_err_();
private:
  int64_t COMPRESS_UNIT_SIZE;

  int64_t ref_cnt_;
  int64_t fd_; //Buffer Tmp File
  int64_t compressed_fd_; //Compressed Tmp File

  ObITmpFileHandle buffer_tmp_file_handle_;
  ObITmpFileHandle compressed_tmp_file_handle_;

  int64_t end_offset_for_compress_;
  int64_t compressed_read_offset_;
  int32_t read_offset_;
  bool is_sealed_;
  bool is_deleting_;
  bool err_marked_;
  bool next_unit_header_inited_;
  CompressedUnitHeader next_unit_header_;
  //TODO:为什么要用这个buf
  common::ObFIFOAllocator *buf_allocator_;
  common::ObCompressor *compressor_;
  int64_t file_size_;
  int64_t moved_data_size_;
  ObSpinLock lock_;
  ObSpinLock move_data_lock_;
  ObDList<WaitTask> wait_list_;

  char* decompress_buf_;
  int64_t decompress_buf_start_offset_;
  int64_t decompress_buf_valid_len_;

  char* read_buf_;
  int64_t read_buf_start_offset_;
  int64_t read_buf_valid_len_;

  int64_t PREFETCH_SIZE;
  int64_t READ_BUF_SIZE;

  const void* file_bunch_;
};

class ObCompressTmpFileHandle final
{
public:
  ObCompressTmpFileHandle() : ptr_(nullptr) {}
  ObCompressTmpFileHandle(ObCompressTmpFile *tmp_file);
  ObCompressTmpFileHandle(const ObCompressTmpFileHandle &handle);
  ObCompressTmpFileHandle & operator=(const ObCompressTmpFileHandle &other);
  ~ObCompressTmpFileHandle() { reset(); }
  OB_INLINE ObCompressTmpFile * get() const { return ptr_; }
  bool is_inited() { return nullptr != ptr_; }
  void reset();
  int init(ObCompressTmpFile *tmp_file);
  TO_STRING_KV(KP(ptr_));
private:
  ObCompressTmpFile *ptr_;
};

class ObCompressTmpFileBunch : public common::ObDLinkBase<ObCompressTmpFileBunch>
{
public:
  typedef common::ObLinearHashMap<ObTmpFileKey, ObCompressTmpFileHandle> CompTmpFileMap;
public:
  ObCompressTmpFileBunch();
  ~ObCompressTmpFileBunch();
  void reset();
public:
  int init(const uint64_t tenant_id, int64_t &fd, const int64_t dir_id,
           const char* const label, const ObCompressorType comptype, const int64_t comp_unit_size,
           const int64_t compressible_tmp_file_cnt,
           ObConcurrentFIFOAllocator *comp_tmp_file_allocator, ObFIFOAllocator *compress_buf_allocator,
           CompTmpFileMap *compressible_files);
  OB_INLINE int64_t get_fd() { return fd_; }
  int read(const uint64_t tenant_id, const ObTmpFileIOInfo &io_info, ObCompTmpFileIOHandle &io_handle);
  int write(const uint64_t tenant_id, const ObTmpFileIOInfo &io_info);
  int seal(const uint64_t tenant_id, int64_t &sealed_comp_file_num);
  int get_total_file_size(int64_t &file_size);
  int seal_partial_comp_tmp_files(const uint64_t tenant_id, const int64_t max_unsealed_file_num_each_bunch, int64_t &seal_file_num);
  OB_INLINE void inc_ref_cnt() { ATOMIC_INC(&ref_cnt_); }
  OB_INLINE void dec_ref_cnt() { ATOMIC_AAF(&ref_cnt_, -1); }
  OB_INLINE int64_t get_ref_cnt() const { return ATOMIC_LOAD(&ref_cnt_); }
  OB_INLINE ObSEArray<int64_t, 128> &get_fds() { return fds_; }
  bool can_remove();
  void set_deleting(int64_t &unsealed_file_cnt);
  void get_unsealed_file_cnt(bool &is_normal_bunch, int64_t &unsealed_file_cnt);
  TO_STRING_KV(K(is_inited_), K(err_marked_), K(is_sealed_), K(is_deleting_),
               K(fd_), K(ref_cnt_), K(compressible_tmp_file_cnt_), K(write_req_seq_),
               K(next_read_fd_index_), K(next_read_file_offset_), K(sealed_tmp_file_cnt_),
               KP(compressible_files_), K(fds_));
private:
  bool is_inited_;
  bool err_marked_;
  bool is_sealed_;
  bool is_deleting_;
  int64_t fd_;
  int64_t ref_cnt_;
  int64_t compressible_tmp_file_cnt_;
  int64_t write_req_seq_;
  int32_t next_read_fd_index_;
  int64_t next_read_file_offset_;
  int64_t sealed_tmp_file_cnt_;
  CompTmpFileMap *compressible_files_;
  ObSEArray<int64_t, 128> fds_;
  ObSEArray<ObCompressTmpFileHandle, 128> comp_file_handles_;
  ObSpinLock lock_;
};

class ObCompTmpFileBunchHandle
{
public:
  ObCompTmpFileBunchHandle() : ptr_(nullptr) {}
  ObCompTmpFileBunchHandle(ObCompressTmpFileBunch *tmp_file_bunch);
  ObCompTmpFileBunchHandle(const ObCompTmpFileBunchHandle &handle);
  ObCompTmpFileBunchHandle & operator=(const ObCompTmpFileBunchHandle &other);
  ~ObCompTmpFileBunchHandle() { reset(); }
  OB_INLINE ObCompressTmpFileBunch * get() const { return ptr_; }
  bool is_inited() { return nullptr != ptr_; }
  void reset();
  int init(ObCompressTmpFileBunch *tmp_file_bunch);
  TO_STRING_KV(KP(ptr_));
private:
  ObCompressTmpFileBunch *ptr_;
};

}
}

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_OB_COMPRESS_TMP_FILE_H_
