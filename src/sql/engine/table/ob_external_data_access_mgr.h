/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */


#ifndef OB_EXTERNAL_DATA_ACCESS_MGR_H_
#define OB_EXTERNAL_DATA_ACCESS_MGR_H_


#include "lib/file/ob_file.h"
#include "common/storage/ob_io_device.h"
#include "share/backup/ob_backup_struct.h"
#include "sql/ob_sql_context.h"
#include "ob_external_file_access.h"
#include "ob_external_data_page_cache.h"

namespace oceanbase
{

namespace sql {

class ObExternalAccessFileInfo final
{
  /// only initialized in @c ObExternalDataAccessMgr
  friend class ObExternalDataAccessMgr;
public:
  ObExternalAccessFileInfo() :
    url_(), content_digest_(), modify_time_(-1), page_size_(0), file_size_(0),
    access_info_(nullptr), device_handle_(nullptr), allocator_(nullptr)
  {}

  ~ObExternalAccessFileInfo();
  bool is_valid() const;
  int assign(const ObExternalAccessFileInfo &other);
  const ObString &get_url() const { return url_; }
  const ObString &get_file_content_digest() const { return content_digest_; }
  const ObObjectStorageInfo *get_access_info() const { return access_info_; }
  ObObjectStorageInfo *get_access_info() { return access_info_; }

  TO_STRING_KV(K_(url), K_(content_digest), K_(modify_time), KP_(access_info), KP_(device_handle));

  // delete reason: copy_constructor and assignment_operator should be delete in the future
  ObExternalAccessFileInfo(const ObExternalAccessFileInfo& other) = delete;
  const ObExternalAccessFileInfo& operator= (const ObExternalAccessFileInfo&) = delete;

  int64_t get_modify_time() const { return modify_time_; }
  int64_t get_page_size() const { return page_size_; }
  int64_t get_file_size() const { return file_size_; }

  const ObIODevice *get_device_handle() const { return device_handle_; }

private:
  static int copy_url(ObString &dest, const ObString &src, common::ObIAllocator *allocator);
  ObIODevice *&get_device_handle_() { return device_handle_; }
  int set_access_info(const ObObjectStorageInfo *access_info, common::ObIAllocator *allocator);
  int set_basic_file_info(const ObString &url, const ObString &content_digest,
                          const int64_t modify_time, const int64_t page_size,
                          const int64_t file_size, common::ObIAllocator &allocator);
  void reset_();
  bool is_copyable_() const { return nullptr != allocator_; }

private:
  ObString url_;
  ObString content_digest_;
  int64_t modify_time_;
  int64_t page_size_;
  int64_t file_size_;
  ObObjectStorageInfo *access_info_;
  ObIODevice *device_handle_;
  /// if @c allocator_ is not null, means @c this owns @c url_
  common::ObIAllocator *allocator_;
};

class ObExternalDataAccessMgr final
{
public: // for MTL
  ObExternalDataAccessMgr();
  ~ObExternalDataAccessMgr();
  static int mtl_init(ObExternalDataAccessMgr* &ExDAM);
  int init();
  int start();
  int stop();
  int wait();
  void destroy();
public: // for user
  int open_and_reg_file(
      const ObString &url,
      const ObString &content_digest,
      const ObObjectStorageInfo *info,
      const int64_t modify_time,
      const int64_t file_size,
      ObIOFd &fd);
  int close_file(ObIOFd &fd);
  int async_read(
      const ObIOFd &fd,
      const ObExternalReadInfo &info,
      const bool enable_page_cache,
      ObExternalFileReadHandle &handle);
private: // inner struct
struct FileMapKey {
    FileMapKey();
    ~FileMapKey();
    FileMapKey(common::ObIAllocator *allocator);
    uint64_t hash() const;
    int hash(uint64_t &hash_val) const;
    TO_STRING_KV(K_(page_size), K_(url), K_(content_digest), K_(modify_time));
    bool operator == (const FileMapKey &other) const;
    int init(const ObString &url, const ObString &content_digest,
             const int64_t modify_time, const int64_t page_size);
    int assign(const FileMapKey &other);
    void reset();

    // disable copy
    FileMapKey(const FileMapKey &) = delete;
    void operator=(const FileMapKey &) = delete;

    static uint64_t hash(const ObString &url, const ObString &content_digest,
                         const int64_t modify_time, const int64_t page_size);

  private:
    bool is_copyable_() const;

  private:
    int64_t page_size_;
    ObString url_;
    ObString content_digest_;
    int64_t modify_time_;
    /// if @c allocator_ is not null, means @c this owns @c url_
    common::ObIAllocator *allocator_;
  };
  struct InnerAccessFileInfo {
    InnerAccessFileInfo():
      info_(), ref_cnt_(0)
      {};
    ~InnerAccessFileInfo() = default;
    ObExternalAccessFileInfo info_;
    inline void inc_ref() { ATOMIC_AAF(&ref_cnt_, 1); }
    inline int64_t dec_ref() { return ATOMIC_SAF(&ref_cnt_, 1); }
    int64_t get_ref() const { return ATOMIC_LOAD(&ref_cnt_); }
    bool is_valid() const { return info_.is_valid() &&  get_ref() >= 0; }
    TO_STRING_KV(K_(info), K_(ref_cnt));
    private:
      int64_t ref_cnt_;
  };
private:
  static const int BUCKET_NUM = 1283;
private: // function
  int force_delete_from_fd_map_(
      const FileMapKey &key);
  int force_delete_from_file_map_(
      const ObIOFd &fd);
  int get_file_info_by_file_key_(
      const FileMapKey &key, // input
      ObIOFd &fd, // output
      InnerAccessFileInfo *&inner_file_info); // output
  int get_modify_time_by_fd_(
      const ObIOFd &fd,
      int64_t &modify_time);
  int fill_cache_hit_buf_and_get_cache_miss_segments_(
      const ObIOFd &fd,
      const ObString &url,
      const ObString &content_digest,
      const int64_t modify_time,
      const int64_t page_size,
      const int64_t rd_offset,
      const int64_t rd_len,
      const bool enable_page_cache,
      char* buffer,
      ObExternalFileReadHandle &exReadhandle,
      ObIArray<ObExtCacheMissSegment> &seg_ar);
  int record_one_and_reset_seg_(
      ObExtCacheMissSegment &seg,
      ObIArray<ObExtCacheMissSegment> &seg_ar) const;
  int get_rd_info_arr_by_cache_miss_seg_arr_(
      const ObIOFd &fd,
      const ObString &url,
      const ObString &content_digest,
      const int64_t modify_time,
      const int64_t page_size,
      const int64_t file_size,
      const ObIArray<ObExtCacheMissSegment> &seg_arr,
      const ObExternalReadInfo &src_rd_info,
      const bool enable_page_cache,
      ObIArray<ObExternalReadInfo> &rd_info_arr);
  int inner_cache_hit_process_(
    const int64_t cur_pos,
    const int64_t cur_rd_offset,
    const int64_t cur_buf_size,
    const int64_t cache_page_size,
    const ObExternalDataPageCacheValueHandle &v_hdl,
    char* buffer,
    ObExternalFileReadHandle &exReadhandle,
    ObExtCacheMissSegment &cur_seg,
    ObIArray<ObExtCacheMissSegment> &seg_arr);
  int inner_cache_miss_process_(
    const int64_t cur_pos,
    const int64_t cur_rd_offset,
    const int64_t cur_buf_size,
    char* buffer,
    ObExtCacheMissSegment &cur_seg,
    ObIArray<ObExtCacheMissSegment> &seg_arr);
  int inner_async_read_tmp_(
    ObIOFd &fd,
    InnerAccessFileInfo &inner_file_info,
    const ObExternalReadInfo &external_read_info,
    blocksstable::ObStorageObjectHandle &io_handle);

private: // inner feild
  // TODO: one map is enough
  common::hash::ObHashMap<FileMapKey, ObIOFd> fd_map_;
  common::hash::ObHashMap<ObIOFd, InnerAccessFileInfo*> file_map_;
  // lock both fd_map_ and file_map_
  common::ObBucketLock bucket_lock_;
  ObFIFOAllocator inner_file_info_alloc_;
  ObFIFOAllocator callback_alloc_;
  ObExternalDataPageCache &kv_cache_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObExternalDataAccessMgr);
};



} // namespace sql
} // namespace oceanbase
#endif // OB_EXTERNAL_DATA_ACCESS_MGR_H_
