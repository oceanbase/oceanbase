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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_TMP_FILE_META_TREE_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_TMP_FILE_META_TREE_H_

#include "deps/oblib/src/lib/container/ob_se_array.h"
#include "storage/tmp_file/ob_tmp_file_write_buffer_pool.h"
#include "storage/tmp_file/ob_tmp_file_global.h"

namespace oceanbase
{
namespace tmp_file
{

class ObTmpPageValueHandle;
class ObSNTmpFileInfo;

struct ObSharedNothingTmpFileMetaItem
{
public:
  ObSharedNothingTmpFileMetaItem() :
    page_level_(-1),
    physical_page_id_(-1),
    buffer_page_id_(ObTmpFileGlobal::INVALID_PAGE_ID),
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
    buffer_page_id_ = ObTmpFileGlobal::INVALID_PAGE_ID;
    block_index_ = ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX;
    virtual_page_id_ = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
  }
  int16_t page_level_;         // child page level
  int16_t physical_page_id_;   // child page 在宏块内的page id
  uint32_t buffer_page_id_;    // child page 在写缓存中的page id
  int64_t block_index_;        // child page 对应的宏块block index
  int64_t virtual_page_id_;    // child page 对应的文件逻辑页号最小值（即索引）
  TO_STRING_KV(K(page_level_), K(buffer_page_id_), K(physical_page_id_),
               K(block_index_), K(virtual_page_id_));
};  // 24B

struct ObSharedNothingTmpFileDataItem
{
  ObSharedNothingTmpFileDataItem() :
    block_index_(ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX),
    physical_page_id_(-1),
    physical_page_num_(0),
    virtual_page_id_(-1) {}
  bool operator!=(const ObSharedNothingTmpFileDataItem &other) const
  {
    return other.block_index_ != block_index_
           || other.physical_page_id_ != physical_page_id_
           || other.physical_page_num_ != physical_page_num_
           || other.virtual_page_id_ != virtual_page_id_;
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

struct ObTmpFileTreeIOInfo
{
  ObTmpFileTreeIOInfo() :tree_epoch_(-1), block_index_(ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX),
                         flush_start_page_id_(ObTmpFileGlobal::INVALID_PAGE_ID),
                         flush_start_level_page_index_(-1),
                         flush_end_page_id_(ObTmpFileGlobal::INVALID_PAGE_ID),
                         physical_start_page_id_(-1),
                         flush_nums_(0),
                         page_level_(-1) {}
  void reset()
  {
    tree_epoch_ = -1;
    block_index_ = ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX;
    flush_start_page_id_ = ObTmpFileGlobal::INVALID_PAGE_ID;
    flush_start_level_page_index_ = -1;
    flush_end_page_id_ = ObTmpFileGlobal::INVALID_PAGE_ID;
    physical_start_page_id_ = -1;
    flush_nums_ = 0;
    page_level_ = -1;
  }
  bool is_valid() const
  {
    return 0 <= tree_epoch_
           && ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX != block_index_
           && ObTmpFileGlobal::INVALID_PAGE_ID != flush_start_page_id_
           && 0 <= flush_start_level_page_index_
           && ObTmpFileGlobal::INVALID_PAGE_ID != flush_end_page_id_
           && physical_start_page_id_ >= 0
           && flush_nums_ > 0
           && page_level_ >= 0;
  }
  int64_t tree_epoch_;
  int64_t block_index_;            //刷脏页所在的block index
  uint32_t flush_start_page_id_;   //刷脏起始页在写缓存中的page_id
  int32_t flush_start_level_page_index_; //刷脏起始页在其所在层的 page_index
  uint32_t flush_end_page_id_;     //刷脏最后一个页在写缓存中的page_id
  int16_t physical_start_page_id_; //这批页在block中的start page id（物理偏移）
  int16_t flush_nums_;             //刷脏的页数
  int16_t page_level_;             //刷脏的页所属的元数据树层次
  TO_STRING_KV(K(tree_epoch_), K(block_index_), K(flush_start_page_id_),
               K(flush_start_level_page_index_), K(flush_end_page_id_),
               K(physical_start_page_id_), K(flush_nums_), K(page_level_));
};

struct ObTmpFileTreeFlushContext
{
  ObTmpFileTreeFlushContext() : tree_epoch_(-1),
                                is_meta_reach_end_(false),
                                last_flush_level_(-1),
                                last_flush_page_id_(ObTmpFileGlobal::INVALID_PAGE_ID),
                                last_flush_page_index_in_level_(-1) {}
  void reset()
  {
    tree_epoch_ = -1;
    is_meta_reach_end_ = false;
    last_flush_level_ = -1;
    last_flush_page_id_ = ObTmpFileGlobal::INVALID_PAGE_ID;
    last_flush_page_index_in_level_ = -1;
  }
  bool is_valid() const
  {
    return tree_epoch_ >= 0
           && last_flush_level_ >= 0
           && ObTmpFileGlobal::INVALID_PAGE_ID != last_flush_page_id_
           && last_flush_page_index_in_level_ >= 0;
  }
  int64_t tree_epoch_;
  bool is_meta_reach_end_;
  int16_t last_flush_level_;
  uint32_t last_flush_page_id_;
  int32_t last_flush_page_index_in_level_;
  TO_STRING_KV(K(tree_epoch_), K(is_meta_reach_end_), K(last_flush_level_), K(last_flush_page_id_),
      K(last_flush_page_index_in_level_));
};

enum ObTmpFileTreeEvictType : int16_t
{
  INVALID = -1,
  FULL = 0,  //flush all pages
  MAJOR,     //flush all pages except rightmost page of each level
};

class ObSharedNothingTmpFileMetaTree
{
public:
  struct LevelPageRangeInfo
  {
    LevelPageRangeInfo() : start_page_id_(ObTmpFileGlobal::INVALID_PAGE_ID),
                           flushed_end_page_id_(ObTmpFileGlobal::INVALID_PAGE_ID),
                           end_page_id_(ObTmpFileGlobal::INVALID_PAGE_ID),
                           cached_page_num_(0),
                           flushed_page_num_(0),
                           evicted_page_num_(0) {}
    LevelPageRangeInfo(const uint32_t start_page_id,
                       const uint32_t flushed_page_id,
                       const uint32_t end_page_id,
                       const int32_t cached_page_num,
                       const int32_t flushed_page_num,
                       const int32_t evicted_page_num) :
                       start_page_id_(start_page_id),
                       flushed_end_page_id_(flushed_page_id),
                       end_page_id_(end_page_id),
                       cached_page_num_(cached_page_num),
                       flushed_page_num_(flushed_page_num),
                       evicted_page_num_(evicted_page_num) {}
    uint32_t start_page_id_;
    uint32_t flushed_end_page_id_;
    uint32_t end_page_id_;
    //Rules:
    //  cached_page_num_ + evicted_page_num_ = level total page num
    //  flushed_page_num_ >= evicted_page_num_
    //  flushed_pages must contail evicted_pages, but maybe some cached_pages.
    //  cached_pages -> [start_page_id_, end_page_id_]
    //  flushed_pages -> [start_page_id_, flushed_end_page_id_] or none(depnds on whether flushed_end_page_id_ is valid or not)
    //  They do not contain duplicate pages, even if the page is cached/flushed/evicted multiple times.
    int32_t cached_page_num_;
    int32_t flushed_page_num_;
    int32_t evicted_page_num_;
    TO_STRING_KV(K(start_page_id_), K(flushed_end_page_id_), K(end_page_id_),
                 K(cached_page_num_), K(flushed_page_num_), K(evicted_page_num_));
  };

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

  struct StatInfo
  {
    StatInfo() : meta_page_flushing_cnt_(0),
                 all_type_page_flush_cnt_(0),
                 all_type_flush_page_released_cnt_(0),
                 meta_page_alloc_cnt_(0),
                 meta_page_free_cnt_(0) {}
    void reset()
    {
      meta_page_flushing_cnt_ = 0;
      all_type_page_flush_cnt_ = 0;
      all_type_flush_page_released_cnt_ = 0;
      meta_page_alloc_cnt_ = 0;
      meta_page_free_cnt_ = 0;
    }
    int64_t meta_page_flushing_cnt_;
    //can contain the same page if page is flushed again
    // contain total pages(meta and data)
    int64_t all_type_page_flush_cnt_;
    int64_t all_type_flush_page_released_cnt_;
    //alloc and free in write cache
    int64_t meta_page_alloc_cnt_;
    int64_t meta_page_free_cnt_;
    TO_STRING_KV(K(meta_page_flushing_cnt_), K(all_type_page_flush_cnt_), K(all_type_flush_page_released_cnt_),
                 K(meta_page_alloc_cnt_), K(meta_page_free_cnt_));
  };

public:
  ObSharedNothingTmpFileMetaTree();
  ~ObSharedNothingTmpFileMetaTree();
  void reset();
public:
  int init(const int64_t fd, ObTmpWriteBufferPool *wbp, ObIAllocator *callback_allocator);

  //append write: We always write the rightmost page of the leaf layer
  //It happens after a tmp file write request:
  //  data pages of a write request is persisted on disk
  int prepare_for_insert_items();
  int insert_items(const common::ObIArray<ObSharedNothingTmpFileDataItem> &data_items);

  //It happens when there is a tmp file read request
  int search_data_items(const int64_t offset,
                        const int64_t read_size,
                        common::ObIArray<ObSharedNothingTmpFileDataItem> &data_items);

  //It happens when there is a meta tree pages flush request:
  //  tmp file data and meta pages are aggregated to flush
  int flush_meta_pages_for_block(const int64_t block_index,
                                 const ObTmpFileTreeEvictType flush_type,
                                 char *block_buff,
                                 int64_t &write_offset,
                                 ObTmpFileTreeFlushContext &flush_context,
                                 common::ObIArray<ObTmpFileTreeIOInfo> &meta_io_array);

  //It happens after meta tree pages flush:
  //  meta pages is persisted on disk successfully
  int update_after_flush(const common::ObIArray<ObTmpFileTreeIOInfo> &meta_io_array);

  //It happens when there is a tmp file write request:
  //  a tmp file page that is not full need to be continued writing.
  //last_data_item is the page info of the unfilled data page.
  int prepare_for_write_tail(ObSharedNothingTmpFileDataItem &last_data_item);
  // After the tail is written (the tail page exists in disk)
  int finish_write_tail(const ObSharedNothingTmpFileDataItem &last_data_item,
                        const bool release_tail_in_disk);

  //It happens when there is a tmp file eviction
  int evict_meta_pages(const int64_t expected_page_num,
                       const ObTmpFileTreeEvictType flush_type,
                       int64_t &actual_evict_page_num);

  //it happens when there is a tmp file clearing
  int clear(const int64_t last_truncate_offset, const int64_t total_file_size);

  //it happens when there is a need to truncate a tmp file
  int truncate(const int64_t last_truncate_offset, const int64_t end_truncate_offset);

  //get the num of tree pages that need to be flushed
  int get_need_flush_page_num(int64_t &total_need_flush_page_num,
                              int64_t &total_need_flush_rightmost_page_num) const;
  //get the num of tree pages that need to be evicted
  int get_need_evict_page_num(int64_t &total_need_evict_page_num,
                              int64_t &total_need_evict_rightmost_page_num) const;
  //NOTE: the following function is called externally.
  //      It needs to be called internally without holding lock_ (avoid deadlock).
  void print_meta_tree_overview_info();
  //NOTE: need control print frequency.
  void print_meta_tree_total_info();
  //for virtual table to show
  int copy_info(ObSNTmpFileInfo &tmp_file_info);

private:
  int modify_meta_items_at_parent_level_(const ObTmpFileTreeIOInfo &meta_io,
                                         const int32_t start_page_index_in_level);
  int truncate_array_(const int64_t end_offset);
  int search_data_items_from_array_(const int64_t end_offset,
                                    int64_t &cur_offset,
                                    common::ObIArray<ObSharedNothingTmpFileDataItem> &data_items);
  int get_items_of_internal_page_(const ObSharedNothingTmpFileMetaItem &page_info,
                                  const int32_t level_page_index,
                                  const int16_t cur_item_index,
                                  const int64_t end_offset,
                                  bool &is_last_item,
                                  ObIArray<ObSharedNothingTmpFileMetaItem> &meta_items);
  int backtrace_truncate_tree_(const int64_t end_offset,
                               ObIArray<BacktraceNode> &search_path,
                               ObIArray<ObSharedNothingTmpFileMetaItem> &reserved_meta_items,
                               ObIArray<ObTmpPageValueHandle> &p_handles);
  int backtrace_search_data_items_(const int64_t end_offset,
                                   int64_t &offset,
                                   ObIArray<BacktraceNode> &search_path,
                                   ObIArray<ObSharedNothingTmpFileDataItem> &data_items);
  int finish_insert_(const int return_ret,
                     ObIArray<int16_t> &level_origin_page_write_counts,
                     ObIArray<ObSEArray<uint32_t, 2>> &level_new_pages);
  int try_to_insert_items_to_array_(const ObIArray<ObSharedNothingTmpFileDataItem> &data_items,
                                    ObIArray<ObSEArray<uint32_t, 2>> &level_new_pages);
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
                              common::ObIArray<ObSharedNothingTmpFileDataItem> &data_items);
  virtual int release_meta_page_(const ObSharedNothingTmpFileMetaItem &page_info,
                                 const int32_t page_index_in_level);
  virtual int release_tmp_file_page_(const int64_t block_index,
                                     const int64_t begin_page_id, const int64_t page_num);
  int get_rightmost_leaf_page_for_write_(ObSharedNothingTmpFileMetaItem &page_info);
  int get_last_item_of_internal_page_(const uint32_t parent_page_id,
                                      ObSharedNothingTmpFileMetaItem &page_info,
                                      ObSharedNothingTmpFileMetaItem &last_meta_item);
  int try_to_fill_rightmost_internal_page_(const ObIArray<ObSharedNothingTmpFileMetaItem> &meta_items,
                                           const int16_t page_level,
                                           int64_t &write_count,
                                           ObIArray<int16_t> &level_origin_page_write_counts);
  int try_to_fill_rightmost_leaf_page_(const ObIArray<ObSharedNothingTmpFileDataItem> &data_items,
                                       const ObIArray<ObSEArray<uint32_t, 2>> &level_new_pages,
                                       int64_t &write_count,
                                       ObIArray<int16_t> &level_origin_page_write_counts);
  int add_new_page_and_fill_items_at_leaf_(const ObIArray<ObSharedNothingTmpFileDataItem> &data_items,
                                           int64_t &write_count,
                                           ObSharedNothingTmpFileMetaItem &meta_item,
                                           ObIArray<ObSEArray<uint32_t, 2>> &level_new_pages);
  int add_new_page_and_fill_items_at_internal_(const ObIArray<ObSharedNothingTmpFileMetaItem> &meta_items,
                                               const int16_t page_level,
                                               int64_t &write_count,
                                               ObSharedNothingTmpFileMetaItem &meta_item,
                                               ObIArray<ObSEArray<uint32_t, 2>> &level_new_pages);
  int cascade_modification_at_internal_(const ObIArray<ObSharedNothingTmpFileMetaItem> &new_leaf_page_infos,
                                        ObIArray<int16_t> &level_origin_page_write_counts,
                                        ObIArray<ObSEArray<uint32_t, 2>> &level_new_pages);
  int modify_child_pages_location(char *page_buff);
  int modify_meta_items_during_evict_(const ObIArray<uint32_t> &evict_pages,
                                      const int16_t level,
                                      const int32_t start_page_index_in_level);
  int flush_leaf_pages_(const uint32_t flush_start_page_id,
                        const int32_t start_page_index_in_level,
                        const ObTmpFileTreeEvictType flush_type,
                        char *block_buff,
                        int64_t &write_offset,
                        ObTmpFileTreeIOInfo &meta_io_info);
  int calc_and_set_page_checksum_(char* page_buff);
  int flush_internal_pages_(const uint32_t flush_start_page_id,
                            const int16_t level,
                            const int32_t start_page_index_in_level,
                            const ObTmpFileTreeEvictType flush_type,
                            char *block_buff,
                            int64_t &write_offset,
                            ObTmpFileTreeIOInfo &meta_io_info);
  int get_page_(const ObSharedNothingTmpFileMetaItem &page_info,
                const int32_t level_page_index,
                char *&page_buff,
                ObTmpPageValueHandle &p_handle);
  int check_page_(const char* const page_buff);
  virtual int cache_page_for_write_(const uint32_t parent_page_id,
                            ObSharedNothingTmpFileMetaItem &page_info);
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
  inline bool is_page_in_write_cache(const ObSharedNothingTmpFileMetaItem &meta_item)
  {
    return ObTmpFileGlobal::INVALID_PAGE_ID != meta_item.buffer_page_id_;
  }
  inline bool is_page_flushed(const ObSharedNothingTmpFileMetaItem &meta_item)
  {
    return ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX != meta_item.block_index_
           && 0 <= meta_item.physical_page_id_;
  }

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
  ObTmpWriteBufferPool *wbp_;
  ObIAllocator *callback_allocator_;
  int64_t tree_epoch_;
  ObSharedNothingTmpFileMetaItem root_item_;
  //When the tmp file writes less data, we can use an array instead of a tree to store metadata
  common::ObSEArray<ObSharedNothingTmpFileDataItem, 4> data_item_array_;
  //level page ranges in write cache
  common::ObSEArray<LevelPageRangeInfo, 2> level_page_range_array_;
  //Only writing the rightmost pages
  //Used to prevent the rightmost pages from being evicted
  bool is_writing_;
  common::SpinRWLock lock_;
  LastTruncateLeafInfo last_truncate_leaf_info_;
  int64_t released_offset_;
public:
  StatInfo stat_info_;
  //NOTE: without protection of lock_, we do not recommend using K(meta_tree_) externally.
  TO_STRING_KV(K(fd_), K(tree_epoch_), K(root_item_),
               K(data_item_array_), K(level_page_range_array_), K(is_writing_),
               K(last_truncate_leaf_info_), K(released_offset_), K(stat_info_));
};


}  // end namespace tmp_file
}  // end namespace oceanbase
#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_OB_TMP_FILE_META_TREE_H_