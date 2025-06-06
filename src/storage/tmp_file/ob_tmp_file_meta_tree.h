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

#ifndef OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_META_TREE_H_
#define OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_META_TREE_H_

#include "deps/oblib/src/lib/container/ob_se_array.h"
#include "storage/tmp_file/ob_tmp_file_write_cache.h"
#include "storage/tmp_file/ob_tmp_file_global.h"
#include "storage/tmp_file/ob_tmp_file_block_manager.h"
#include "storage/tmp_file/ob_tmp_file_block_allocating_priority_manager.h"

namespace oceanbase
{
namespace tmp_file
{

class ObTmpPageValueHandle;
class ObTmpFileMetaTreeInfo;
class ObTmpFileBlockManager;

struct ObSharedNothingTmpFileMetaItem
{
public:
  ObSharedNothingTmpFileMetaItem() :
    page_level_(-1),
    physical_page_id_(-1),
    block_index_(ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX),
    virtual_page_id_(-1) {}
  bool is_valid() const
  {
    return  page_level_ >= 0
            && virtual_page_id_ >= 0;
  }
  void reset()
  {
    page_level_ = -1;
    physical_page_id_ = -1;
    block_index_ = ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX;
    virtual_page_id_ = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
  }
  int16_t page_level_;         // child page level
  int16_t physical_page_id_;   // child page 在宏块内的page id
  int64_t block_index_;        // child page 对应的宏块block index
  int64_t virtual_page_id_;    // child page 对应的文件逻辑页号最小值（即索引）
  TO_STRING_KV(K(page_level_), K(physical_page_id_),
               K(block_index_), K(virtual_page_id_));
};  // 24B

struct ObSharedNothingTmpFileDataItem
{
  ObSharedNothingTmpFileDataItem() :
    block_index_(ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX),
    physical_page_id_(-1),
    physical_page_num_(0),
    virtual_page_id_(-1) {}
  ObSharedNothingTmpFileDataItem(const int64_t block_index,
                                 const int16_t physical_page_id,
                                 const int16_t physical_page_num,
                                 const int64_t virtual_page_id) :
    block_index_(block_index), physical_page_id_(physical_page_id),
    physical_page_num_(physical_page_num), virtual_page_id_(virtual_page_id) {}
  bool operator!=(const ObSharedNothingTmpFileDataItem &other) const
  {
    return other.block_index_ != block_index_
           || other.physical_page_id_ != physical_page_id_
           || other.physical_page_num_ != physical_page_num_
           || other.virtual_page_id_ != virtual_page_id_;
  }
  bool operator==(const ObSharedNothingTmpFileDataItem &other) const
  {
    return other.block_index_ == block_index_
           && other.physical_page_id_ == physical_page_id_
           && other.physical_page_num_ == physical_page_num_
           && other.virtual_page_id_ == virtual_page_id_;
  }
  bool is_valid() const
  {
    return ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX != block_index_
           && physical_page_id_ >= 0
           && physical_page_num_ > 0 //data item must be cleaned up if physical_page_num_ = 0
           && virtual_page_id_ >= 0;
  }
  void reset()
  {
    block_index_ = ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX;
    physical_page_id_ = -1;
    physical_page_num_ = 0;
    virtual_page_id_ = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
  }
  int64_t block_index_;        // 刷盘data extent所在的宏块block index
  int16_t physical_page_id_;   // 刷盘data extent在宏块内的起始page id
  int16_t physical_page_num_;  // 刷盘data extent在宏块内的连续page数量
  int64_t virtual_page_id_;    // 刷盘data extent在整个文件中逻辑页号（偏移位置)
  TO_STRING_KV(K(block_index_), K(physical_page_id_),
               K(physical_page_num_), K(virtual_page_id_));
};  // 24B

class ObTmpFileDataItemPageIterator
{
public:
  ObTmpFileDataItemPageIterator() : array_(nullptr), arr_index_(-1), page_index_(-1) {}
  ~ObTmpFileDataItemPageIterator() { reset(); }
  int init(const ObIArray<ObSharedNothingTmpFileDataItem> *array, const int64_t start_page_virtual_id);
  void reset();
  bool has_next() const;
  int next(int64_t &block_index, int32_t &page_index);
private:
  const ObIArray<ObSharedNothingTmpFileDataItem> *array_;
  int64_t arr_index_;
  int64_t page_index_;
};

struct ObSharedNothingTmpFileTreePageHeader
{
  ObSharedNothingTmpFileTreePageHeader() :
    page_level_(-1),
    item_num_(0),
    magic_number_(0),
    checksum_(0) {}
  int16_t page_level_;
  int16_t item_num_;
  uint32_t magic_number_;
  uint64_t checksum_;
  TO_STRING_KV(K(page_level_), K(item_num_), K(magic_number_), K(checksum_));
};

class ObSharedNothingTmpFileMetaTree
{
public:
  struct BacktraceNode
  {
    BacktraceNode() : page_meta_info_(),
                      level_page_index_(-1),
                      prev_item_index_(-1) {}
    BacktraceNode(const ObSharedNothingTmpFileMetaItem &item,
                  const int32_t level_page_index,
                  const int16_t prev_item_index)
                  : page_meta_info_(item),
                    level_page_index_(level_page_index),
                    prev_item_index_(prev_item_index) {}
    bool is_valid() const
    {
      return page_meta_info_.is_valid()
             && 0 <= level_page_index_;
    }
    //corresponding to a page (represent meta info of the page)
    ObSharedNothingTmpFileMetaItem page_meta_info_;
    //the page index in its level
    int32_t level_page_index_;
    //prev processed item index on the page
    int16_t prev_item_index_;
    TO_STRING_KV(K(page_meta_info_), K(level_page_index_), K(prev_item_index_));
  };

  struct LastTruncateLeafInfo
  {
    LastTruncateLeafInfo() : page_index_in_leaf_level_(-1),
                             item_index_in_page_(-1),
                             release_to_end_in_page_(false) {}
    bool is_valid() const
    {
      return 0 <= page_index_in_leaf_level_
             && 0 <= item_index_in_page_;
    }
    void reset()
    {
      page_index_in_leaf_level_ = -1;
      item_index_in_page_ = -1;
      release_to_end_in_page_ = false;
    }
    int32_t page_index_in_leaf_level_;
    int16_t item_index_in_page_;
    bool release_to_end_in_page_;
    TO_STRING_KV(K(page_index_in_leaf_level_), K(item_index_in_page_), K(release_to_end_in_page_));
  };

  struct DiskOccupiedInfo
  {
    DiskOccupiedInfo() : page_entry_arr_(),
                        next_entry_arr_index_(-1),
                        next_page_index_in_entry_(-1),
                        next_alloced_page_num_(1) {}
    bool is_valid() const
    {
      return  0 <= next_entry_arr_index_
              && 0 <= next_page_index_in_entry_
              && !page_entry_arr_.empty()
              && next_entry_arr_index_ < page_entry_arr_.count()
              && 0 < next_alloced_page_num_;
    }
    void set_empty()
    {
      next_entry_arr_index_ = -1;
      next_page_index_in_entry_ = -1;
    }
    bool is_empty() const
    {
      return 0 > next_entry_arr_index_
             && 0 > next_page_index_in_entry_;
    }
    bool is_equal(const DiskOccupiedInfo &other)
    {
      return other.page_entry_arr_.count() == page_entry_arr_.count()
            && other.next_entry_arr_index_ == next_entry_arr_index_
            && other.next_page_index_in_entry_ == next_page_index_in_entry_
            && other.next_alloced_page_num_ == next_alloced_page_num_;
    }
    void reset()
    {
      page_entry_arr_.reset();
      next_entry_arr_index_ = -1;
      next_page_index_in_entry_ = -1;
      next_alloced_page_num_ = 1;
    }
    common::ObSEArray<ObTmpFileBlockRange, 4> page_entry_arr_;
    int16_t next_entry_arr_index_;
    int16_t next_page_index_in_entry_;
    int16_t next_alloced_page_num_;
    TO_STRING_KV(K(page_entry_arr_), K(next_entry_arr_index_), K(next_page_index_in_entry_), K(next_alloced_page_num_));
  };

  struct StatInfo
  {
    StatInfo() : all_type_page_occupy_disk_cnt_(0),
                 all_type_page_released_cnt_(0),
                 tree_epoch_(0),
                 this_epoch_meta_page_released_cnt_(0) {}
    void reset()
    {
      all_type_page_occupy_disk_cnt_ = 0;
      all_type_page_released_cnt_ = 0;
      tree_epoch_ = 0;
      this_epoch_meta_page_released_cnt_ = 0;
    }
    int64_t all_type_page_occupy_disk_cnt_;
    int64_t all_type_page_released_cnt_;
    int64_t tree_epoch_;
    int64_t this_epoch_meta_page_released_cnt_;
    TO_STRING_KV(K(all_type_page_occupy_disk_cnt_), K(all_type_page_released_cnt_),
                 K(tree_epoch_), K(this_epoch_meta_page_released_cnt_));
  };

public:
  ObSharedNothingTmpFileMetaTree();
  ~ObSharedNothingTmpFileMetaTree();
  void reset();
public:
  int init(const int64_t fd, ObTmpFileWriteCache *wbp, ObIAllocator *callback_allocator, ObTmpFileBlockManager *block_manager);

  //append write: We always write the rightmost page of the leaf layer
  //It happens after a tmp file write request:
  //  data pages of a write request is persisted on disk
  int insert_items(const common::ObIArray<ObSharedNothingTmpFileDataItem> &data_items,
                   const int64_t timeout_ms);

  //NOTE: Don't call this function now
  int insert_item(const ObSharedNothingTmpFileDataItem &data_item);

  //It happens when there is a tmp file read request
  int search_data_items(const int64_t offset,
                        const int64_t read_size,
                        const int64_t timeout_ms,
                        common::ObIArray<ObSharedNothingTmpFileDataItem> &data_items);

  //It happens when there is a tmp file write request:
  //  a tmp file page that is not full need to be continued writing.
  //last_data_item is the page info of the unfilled data page.
  int get_last_data_item_write_tail(const int64_t timeout_ms,
                                    ObSharedNothingTmpFileDataItem &last_data_item);

  //it happens when there is a tmp file clearing
  int clear(const int64_t last_truncate_offset, const int64_t total_file_size);

  //it happens when there is a need to truncate a tmp file
  int truncate(const int64_t last_truncate_offset, const int64_t end_truncate_offset);

  //NOTE: the following function is called externally.
  //      It needs to be called internally without holding lock_ (avoid deadlock).
  void print_meta_tree_overview_info();
  //NOTE: need control print frequency.
  void print_meta_tree_total_info();
  //for virtual table to show
  int copy_info(ObTmpFileMetaTreeInfo &tree_info);

private:
  int try_to_insert_items_to_array_(const ObIArray<ObSharedNothingTmpFileDataItem> &data_items,
                                    bool &need_build_tree);
  int build_tree_(const ObIArray<ObSharedNothingTmpFileDataItem> &data_items,
                  const int64_t timeout_ms,
                  ObIArray<ObSEArray<ObTmpFilePageHandle, 2>> &level_new_pages,
                  ObTmpFilePageHandle &new_page_handle);
  int truncate_array_(const int64_t end_offset);
  int search_data_items_from_array_(const int64_t end_offset,
                                    int64_t &cur_offset,
                                    common::ObIArray<ObSharedNothingTmpFileDataItem> &data_items);
  int get_items_of_internal_page_(const ObSharedNothingTmpFileMetaItem &page_info,
                                  const int32_t level_page_index,
                                  const int16_t cur_item_index,
                                  const int64_t end_offset,
                                  bool &is_last_item,
                                  ObIArray<ObSharedNothingTmpFileMetaItem> &meta_items,
                                  //TODO: represented by a constant
                                  const int64_t timeout_ms = 10 * 1000 /*10s*/);
  int backtrace_truncate_tree_(const int64_t end_offset,
                               ObIArray<BacktraceNode> &search_path,
                               ObIArray<ObSharedNothingTmpFileMetaItem> &reserved_meta_items,
                               ObSEArray<ObTmpFilePageHandle, 2> &write_cache_p_handles,
                               ObSEArray<ObTmpPageValueHandle, 2> &read_cache_p_handles);
  int backtrace_search_data_items_(const int64_t end_offset,
                                   const int64_t timeout_ms,
                                   int64_t &offset,
                                   ObIArray<BacktraceNode> &search_path,
                                   ObIArray<ObSharedNothingTmpFileDataItem> &data_items);
  int rollback_insert_(ObSEArray<ObTmpFilePageHandle, 2> &right_page_handles,
                       const ObSEArray<ObTmpFileWriteCacheKey, 2> &level_new_pages,
                       const int16_t modify_origin_page_level,
                       const int16_t origin_arr_index,
                       const int16_t origin_index_in_entry);
  int insert_item_to_array_or_new_tree_(const ObSharedNothingTmpFileDataItem &data_item);
  int release_items_of_leaf_page_(const ObSharedNothingTmpFileMetaItem &page_info,
                                  const int32_t level_page_index,
                                  const int64_t end_offset,
                                  const int16_t begin_release_index,
                                  bool &release_last_item);
  int get_items_of_leaf_page_(const ObSharedNothingTmpFileMetaItem &page_info,
                              const int32_t level_page_index,
                              const int64_t end_offset,
                              const bool need_find_index,
                              int64_t &cur_offset,
                              common::ObIArray<ObSharedNothingTmpFileDataItem> &data_items,
                              const int64_t timeout_ms = 10 * 1000 /*10s*/);
  int release_meta_page_(const ObSharedNothingTmpFileMetaItem &page_info,
                         const int32_t page_index_in_level);
  virtual int release_tmp_file_page_(const int64_t block_index,
                                     const int64_t begin_page_id, const int64_t page_num);
  int get_rightmost_pages_for_write_(const int64_t timeout_ms,
                                     ObSEArray<ObTmpFilePageHandle, 2> &right_page_handles);
  int get_rightmost_leaf_page_for_write_(const int64_t timeout_ms,
                                         ObSharedNothingTmpFileMetaItem &page_info,
                                         ObTmpFilePageHandle &leaf_right_page_handle);
  int fast_get_rightmost_leaf_page_for_write_(ObTmpFilePageHandle &leaf_right_page_handle,
                                              int16_t &page_remain_cnt);
  int get_last_item_of_internal_page_(const char* page_buf,
                                      int16_t &last_item_index,
                                      ObSharedNothingTmpFileMetaItem &last_meta_item);
  int try_to_fill_rightmost_internal_page_(ObTmpFilePageHandle &page_handle,
                                           const ObIArray<ObSharedNothingTmpFileMetaItem> &meta_items,
                                           const int16_t page_level,
                                           int64_t &write_count,
                                           ObIArray<int16_t> &level_origin_page_write_counts);
  int try_to_fill_rightmost_leaf_page_(ObTmpFilePageHandle &page_handle,
                                       const ObIArray<ObSharedNothingTmpFileDataItem> &data_items,
                                       const ObIArray<ObSEArray<ObTmpFilePageHandle, 2>> &level_new_pages,
                                       int64_t &write_count,
                                       ObIArray<int16_t> &level_origin_page_write_counts);
  int add_new_page_and_fill_items_at_leaf_(const ObIArray<ObSharedNothingTmpFileDataItem> &data_items,
                                           const int64_t timeout_ms,
                                           int64_t &write_count,
                                           ObSharedNothingTmpFileMetaItem &meta_item,
                                           ObIArray<ObSEArray<ObTmpFilePageHandle, 2>> &level_new_pages);
  int add_new_page_and_fill_items_at_internal_(const ObIArray<ObSharedNothingTmpFileMetaItem> &meta_items,
                                               const int16_t page_level,
                                               const int64_t timeout_ms,
                                               int64_t &write_count,
                                               ObSharedNothingTmpFileMetaItem &meta_item,
                                               ObIArray<ObSEArray<ObTmpFilePageHandle, 2>> &level_new_pages);
  int cascade_modification_at_internal_(ObSEArray<ObTmpFilePageHandle, 2> &right_page_handles,
                                        const ObIArray<ObSharedNothingTmpFileMetaItem> &new_leaf_page_infos,
                                        const int64_t timeout_ms,
                                        ObIArray<int16_t> &level_origin_page_write_counts,
                                        ObIArray<ObSEArray<ObTmpFilePageHandle, 2>> &level_new_pages);
  int finish_insert_(const int return_ret,
                     ObSEArray<ObTmpFilePageHandle, 2> &right_page_handles,
                     ObIArray<ObSEArray<ObTmpFilePageHandle, 2>> &level_new_pages,
                     const ObIArray<int16_t> &level_origin_page_write_counts,
                     const int16_t origin_arr_index,
                     const int16_t origin_index_in_entry);
  int try_to_fill_rightmost_internal_page_(char *internal_page_buff,
                                           const int16_t page_level,
                                           const ObSharedNothingTmpFileMetaItem &child_page_info,
                                           ObSEArray<ObTmpFileWriteCacheKey, 2> &level_new_pages,
                                           ObSharedNothingTmpFileMetaItem &meta_item,
                                           int16_t &modify_origin_page_level);
  int try_to_fill_rightmost_leaf_page_(ObTmpFilePageHandle &page_handle,
                                       const ObSharedNothingTmpFileDataItem &data_item,
                                       ObSEArray<ObTmpFileWriteCacheKey, 2> &level_new_pages,
                                       ObSharedNothingTmpFileMetaItem &meta_item,
                                       int16_t &modify_origin_page_level);
  int cascade_modification_at_internal_(ObSEArray<ObTmpFilePageHandle, 2> &right_page_handles,
                                        const ObSharedNothingTmpFileMetaItem &new_leaf_page_info,
                                        ObSEArray<ObTmpFileWriteCacheKey, 2> &level_new_pages,
                                        int16_t &modify_origin_page_level);
  int calc_and_set_page_checksum_(char* page_buff);
  int get_page_(const ObSharedNothingTmpFileMetaItem &page_info,
                const int32_t level_page_index,
                char *&page_buff,
                ObTmpFilePageHandle &write_cache_page_handle,
                ObTmpPageValueHandle &read_cache_page_handle,
                const int64_t timeout_ms = 10 * 1000 /*10s*/);
  int check_page_(const char* const page_buff);
  virtual int cache_page_for_write_(const ObTmpFileWriteCacheKey &page_key,
                                    const ObSharedNothingTmpFileMetaItem &page_info,
                                    const int64_t timeout_ms,
                                    ObTmpFilePageHandle &page_handle);
  int alloc_new_meta_page_(const ObTmpFileWriteCacheKey page_key,
                           const int64_t timeout_ms,
                           ObTmpFilePageHandle &page_handle,
                           ObSharedNothingTmpFileMetaItem &meta_item);
  int update_prealloc_info_();
  int rollback_prealloc_info_(const int16_t origin_arr_index,
                              const int16_t origin_index_in_entry);
  int release_prealloc_data_();
  int init_page_header_(char* page_buff,
                        const int16_t page_level);
  int read_page_header_(const char* page_buff,
                        ObSharedNothingTmpFileTreePageHeader &page_header);
  int remove_page_item_from_tail_(char* page_buff,
                                  const int16_t remove_item_num);
  int truncate_(const int64_t end_offset);
  int calculate_truncate_index_path_(ObIArray<std::pair<int32_t, int16_t>> &item_index_arr);
  template<typename ItemType>
  int read_item_(const char* page_buff,
                 const int64_t target_virtual_page_id,
                 int16_t &item_index,
                 ItemType &item);
  template<typename ItemType>
  int read_item_(const char* page_buff,
                 const int16_t item_index,
                 ItemType &item);
  template<typename ItemType>
  int rewrite_item_(char* page_buff,
                    const int16_t item_index,
                    const ItemType &item) __attribute__((noinline));
  template<typename ItemType>
  int write_items_(char* page_buff,
                   const ObIArray<ItemType> &items,
                   const int64_t begin_index,
                   int16_t &write_count);
  template<typename ItemType>
  int write_item_(char* page_buff,
                  ItemType &item);
  int check_tree_is_empty_();
  template<typename ItemType>
  void read_page_content_(const char *page_buff,
                          ObSharedNothingTmpFileTreePageHeader &page_header,
                          ObIArray<ItemType> &data_items);
  template<typename ItemType>
  void read_page_simple_content_(const char *page_buff,
                                 ObSharedNothingTmpFileTreePageHeader &page_header,
                                 ItemType &first_item,
                                 ItemType &last_item);

private:
  //only for unittest
  static void set_max_array_item_cnt(const int16_t cnt)
  {
    MAX_DATA_ITEM_ARRAY_COUNT = cnt;
  }
  static void set_max_page_item_cnt(const int16_t cnt)
  {
    MAX_PAGE_ITEM_COUNT = cnt;
  }
private:
  static const uint16_t PAGE_MAGIC_NUM;
  static const int16_t PAGE_HEADER_SIZE;
  static int16_t MAX_DATA_ITEM_ARRAY_COUNT;
  //only for unittest
  static int16_t MAX_PAGE_ITEM_COUNT;

private:
  bool is_inited_;
  int64_t fd_;
  ObTmpFileWriteCache *wbp_;
  ObIAllocator *callback_allocator_;
  ObTmpFileBlockManager *block_manager_;
  ObSharedNothingTmpFileMetaItem root_item_;
  //When the tmp file writes less data, we can use an array instead of a tree to store metadata
  common::ObSEArray<ObSharedNothingTmpFileDataItem, 16> data_item_array_;
  common::ObSEArray<int32_t, 2> level_page_num_array_;
  common::SpinRWLock lock_;
  LastTruncateLeafInfo last_truncate_leaf_info_;
  int64_t released_offset_;
  DiskOccupiedInfo prealloc_info_;
public:
  StatInfo stat_info_;
  //NOTE: without protection of lock_, we do not recommend using K(meta_tree_) externally.
  TO_STRING_KV(K(fd_), K(root_item_),
               K(data_item_array_), K(last_truncate_leaf_info_),
               K(released_offset_),
               K(prealloc_info_), K(stat_info_));
};


}  // end namespace tmp_file
}  // end namespace oceanbase
#endif // OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_META_TREE_H_