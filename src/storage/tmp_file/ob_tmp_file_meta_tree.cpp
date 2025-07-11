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

#include "ob_tmp_file_meta_tree.h"
#include "storage/tmp_file/ob_tmp_file_manager.h"

namespace oceanbase
{
using namespace storage;
using namespace share;

namespace tmp_file
{

const uint16_t ObSharedNothingTmpFileMetaTree::PAGE_MAGIC_NUM = 0xa12f;
const int16_t ObSharedNothingTmpFileMetaTree::PAGE_HEADER_SIZE = 16; //16B
int16_t ObSharedNothingTmpFileMetaTree::MAX_DATA_ITEM_ARRAY_COUNT = 16;

//only for unittest
int16_t ObSharedNothingTmpFileMetaTree::MAX_PAGE_ITEM_COUNT = INT16_MAX;

//-----------------------ObSharedNothingTmpFileMetaTree-----------------------

int ObTmpFileDataItemPageIterator::init(const ObIArray<ObSharedNothingTmpFileDataItem> *array, const int64_t start_page_virtual_id)
{
  int ret = OB_SUCCESS;
  int64_t offset = 0;
  if (OB_NOT_NULL(array_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", KR(ret), K(array_));
  } else if (OB_ISNULL(array) ||
             OB_UNLIKELY(array->empty() ||
                         start_page_virtual_id == ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", KR(ret), KPC(array));
  } else if (FALSE_IT((offset = start_page_virtual_id - array->at(0).virtual_page_id_))) {
  } else if (OB_UNLIKELY(offset < 0 || offset >= array->at(0).physical_page_num_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected error", KR(ret), K(offset), K(array->at(0)));
  } else {
    array_ = array;
    arr_index_ = 0;
    page_index_ = array->at(0).physical_page_id_ + offset;
  }
  return ret;
}

void ObTmpFileDataItemPageIterator::reset()
{
  array_ = nullptr;
  arr_index_ = -1;
  page_index_ = -1;
}

bool ObTmpFileDataItemPageIterator::has_next() const
{
  return OB_NOT_NULL(array_) && arr_index_ < array_->count();
}

int ObTmpFileDataItemPageIterator::next(int64_t &block_index, int32_t &page_index)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(array_) || arr_index_ >= array_->count()) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(!array_->at(arr_index_).is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected error", KR(ret), KPC(array_), K(arr_index_));
  } else {
    const ObSharedNothingTmpFileDataItem &range = array_->at(arr_index_);
    block_index = range.block_index_;
    page_index = page_index_;
    page_index_++;
    if (page_index_ == range.physical_page_id_ + range.physical_page_num_) {
      arr_index_++;
      if (arr_index_ < array_->count()) {
        page_index_ = array_->at(arr_index_).physical_page_id_;
      } else {
        page_index_ = -1;
      }
    }
  }
  return ret;
}
//-----------------------ObSharedNothingTmpFileMetaTree-----------------------

ObSharedNothingTmpFileMetaTree::ObSharedNothingTmpFileMetaTree() :
    is_inited_(false),
    fd_(-1),
    wbp_(nullptr),
    callback_allocator_(nullptr),
    block_manager_(nullptr),
    root_item_(),
    data_item_array_(),
    level_page_num_array_(),
    lock_(common::ObLatchIds::TMP_FILE_LOCK),
    last_truncate_leaf_info_(),
    released_offset_(0),
    prealloc_info_(),
    stat_info_()
{
  STATIC_ASSERT(sizeof(struct ObSharedNothingTmpFileMetaItem) == 24, "size of tree meta item is mismatch");
  STATIC_ASSERT(sizeof(struct ObSharedNothingTmpFileDataItem) == 24, "size of tree data item is mismatch");
  STATIC_ASSERT(sizeof(struct ObSharedNothingTmpFileTreePageHeader) == 16, "size of tree page header is mismatch");
}

ObSharedNothingTmpFileMetaTree::~ObSharedNothingTmpFileMetaTree()
{
  reset();
}

void ObSharedNothingTmpFileMetaTree::reset()
{
  is_inited_ = false;
  fd_ = -1;
  wbp_ = nullptr;
  callback_allocator_ = nullptr;
  block_manager_ = nullptr;
  root_item_.reset();
  data_item_array_.reset();
  level_page_num_array_.reset();
  last_truncate_leaf_info_.reset();
  released_offset_ = 0;
  prealloc_info_.reset();
  stat_info_.reset();
}

int ObSharedNothingTmpFileMetaTree::init(const int64_t fd,
                                         ObTmpFileWriteCache *wbp,
                                         ObIAllocator *callback_allocator,
                                         ObTmpFileBlockManager *block_manager)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == wbp
                  || NULL == callback_allocator
                  || NULL == block_manager)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", KR(ret), K(fd_), KP(wbp), KP(callback_allocator), KP(block_manager));
  } else {
    data_item_array_.set_attr(ObMemAttr(MTL_ID(), "TFDataItemArr"));
    level_page_num_array_.set_attr(ObMemAttr(MTL_ID(), "TFTreeLevelArr"));
    fd_ = fd;
    wbp_ = wbp;
    callback_allocator_ = callback_allocator;
    block_manager_ = block_manager;
    is_inited_ = true;
  }
  return ret;
}

int ObSharedNothingTmpFileMetaTree::get_rightmost_pages_for_write_(
    const int64_t timeout_ms,
    ObSEArray<ObTmpFilePageHandle, 2> &right_page_handles)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!root_item_.is_valid()
                  || root_item_.page_level_ >= level_page_num_array_.count())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "unexpected root_item_", KR(ret), K(fd_), K(root_item_), K(level_page_num_array_));
  } else {
    const int16_t FULL_PAGE_META_ITEM_NUM =
              MIN(MAX_PAGE_ITEM_COUNT, (ObTmpFileGlobal::PAGE_SIZE - PAGE_HEADER_SIZE) / sizeof(ObSharedNothingTmpFileMetaItem));
    int16_t level = 0;
    while (OB_SUCC(ret)
          && level <= root_item_.page_level_) {
      if (OB_FAIL(right_page_handles.push_back(ObTmpFilePageHandle()))) {
        STORAGE_LOG(WARN, "fail to push back", KR(ret), K(fd_));
      } else {
        level++;
      }
    }
    if (OB_SUCC(ret)) {
      ObTmpFileWriteCacheKey cache_key(fd_, root_item_.page_level_, 0);
      ObSharedNothingTmpFileMetaItem meta_item = root_item_;
      ObSharedNothingTmpFileMetaItem next_level_meta_item;
      int16_t last_item_index = -1;
      level = root_item_.page_level_;
      while (OB_SUCC(ret)
            && 0 <= level) {
        next_level_meta_item.reset();
        last_item_index = -1;
        if (OB_UNLIKELY(cache_key.level_page_index_ + 1 != level_page_num_array_.at(level))) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(ERROR, "unexpected cache_key", KR(ret), K(fd_), K(cache_key), K(level_page_num_array_));
        } else if (OB_FAIL(wbp_->get_page(cache_key, right_page_handles.at(level)))) {
          if (OB_ENTRY_NOT_EXIST != ret) {
            STORAGE_LOG(ERROR, "fail to get page", KR(ret), K(fd_), K(cache_key));
          } else {
            ret = OB_SUCCESS;
            if (OB_FAIL(cache_page_for_write_(cache_key, meta_item, timeout_ms, right_page_handles.at(level)))) {
              STORAGE_LOG(WARN, "fail to cache page for write", KR(ret), K(fd_), K(cache_key), K(meta_item), K(timeout_ms));
            }
          }
        }
        if (OB_SUCC(ret) && 0 < level) {
          char *page_buff = right_page_handles.at(level).get_page()->get_buffer();
          if (OB_FAIL(get_last_item_of_internal_page_(page_buff, last_item_index, next_level_meta_item))) {
            STORAGE_LOG(WARN, "fail to get last item of internal page", KR(ret), K(fd_), KP(page_buff), K(meta_item));
          } else {
            cache_key.tree_level_--;
            cache_key.level_page_index_ = cache_key.level_page_index_ * FULL_PAGE_META_ITEM_NUM + last_item_index;
            meta_item = next_level_meta_item;
          }
        }
        level--;
      }
    }
  }
  return ret;
}

int ObSharedNothingTmpFileMetaTree::get_rightmost_leaf_page_for_write_(
    const int64_t timeout_ms,
    ObSharedNothingTmpFileMetaItem &page_info,
    ObTmpFilePageHandle &leaf_right_page_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!root_item_.is_valid()
                  || level_page_num_array_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "unexpected root_item_ or level_page_num_array_", KR(ret), K(fd_), K(root_item_), K(level_page_num_array_));
  } else {
    const int16_t FULL_PAGE_META_ITEM_NUM =
              MIN(MAX_PAGE_ITEM_COUNT, (ObTmpFileGlobal::PAGE_SIZE - PAGE_HEADER_SIZE) / sizeof(ObSharedNothingTmpFileMetaItem));
    ObSharedNothingTmpFileMetaItem meta_item = root_item_;
    ObSharedNothingTmpFileMetaItem next_level_meta_item;
    int16_t last_item_index = -1;
    ObTmpFileWriteCacheKey cache_key(fd_, root_item_.page_level_, 0);
    if (OB_SUCC(ret)) {
      int16_t level = root_item_.page_level_;
      while (OB_SUCC(ret)
            && 0 <= level) {
        leaf_right_page_handle.reset();
        next_level_meta_item.reset();
        last_item_index = -1;
        if (OB_FAIL(wbp_->get_page(cache_key, leaf_right_page_handle))) {
          if (OB_ENTRY_NOT_EXIST != ret) {
            STORAGE_LOG(WARN, "fail to get page", KR(ret), K(fd_), K(cache_key));
          } else {
            ret = OB_SUCCESS;
            if (OB_FAIL(cache_page_for_write_(cache_key, meta_item, timeout_ms, leaf_right_page_handle))) {
              STORAGE_LOG(WARN, "fail to cache page for write", KR(ret), K(fd_), K(cache_key), K(meta_item), K(timeout_ms));
            }
          }
        }
        if (OB_SUCC(ret) && 0 < level) {
          if (OB_FAIL(get_last_item_of_internal_page_(leaf_right_page_handle.get_page()->get_buffer(), last_item_index, next_level_meta_item))) {
            STORAGE_LOG(WARN, "fail to get last item of internal page", KR(ret), K(fd_), KP(leaf_right_page_handle.get_page()->get_buffer()), K(meta_item));
          } else {
            cache_key.tree_level_--;
            cache_key.level_page_index_ = cache_key.level_page_index_ * FULL_PAGE_META_ITEM_NUM + last_item_index;
            meta_item = next_level_meta_item;
          }
        }
        level--;
      }
    }
    if (OB_SUCC(ret)) {
      page_info = meta_item;
    }
  }
  return ret;
}

int ObSharedNothingTmpFileMetaTree::fast_get_rightmost_leaf_page_for_write_(
    ObTmpFilePageHandle &leaf_right_page_handle,
    int16_t &page_remain_cnt)
{
  int ret = OB_SUCCESS;
  page_remain_cnt = 0;
  const int16_t FULL_PAGE_DATA_ITEM_NUM =
              MIN(MAX_PAGE_ITEM_COUNT, (ObTmpFileGlobal::PAGE_SIZE - PAGE_HEADER_SIZE) / sizeof(ObSharedNothingTmpFileDataItem));
  int32_t leaf_page_num = level_page_num_array_.at(0);
  ObTmpFileWriteCacheKey cache_key(fd_, 0, leaf_page_num - 1);
  if (OB_FAIL(wbp_->get_page(cache_key, leaf_right_page_handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      STORAGE_LOG(ERROR, "fail to get page", KR(ret), K(fd_), K(cache_key));
    } else {
      STORAGE_LOG(DEBUG, "page not exist in write cache", KR(ret), K(fd_), K(cache_key));
      ret = OB_SUCCESS;
      leaf_right_page_handle.reset();
    }
  } else if (OB_UNLIKELY(!leaf_right_page_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "unexpected leaf_right_page_handle", KR(ret), K(fd_), K(leaf_right_page_handle));
  } else {
    ObSharedNothingTmpFileTreePageHeader page_header;
    char *page_buff = leaf_right_page_handle.get_page()->get_buffer();
    if (OB_FAIL(read_page_header_(page_buff, page_header))) {
      STORAGE_LOG(WARN, "fail to read page header", KR(ret), K(fd_), KP(page_buff));
    } else {
      page_remain_cnt = FULL_PAGE_DATA_ITEM_NUM - page_header.item_num_;
    }
  }
  return ret;
}

int ObSharedNothingTmpFileMetaTree::get_last_item_of_internal_page_(
    const char* page_buff,
    int16_t &last_item_index,
    ObSharedNothingTmpFileMetaItem &last_meta_item)
{
  int ret = OB_SUCCESS;
  last_item_index = -1;
  last_meta_item.reset();
  ObSharedNothingTmpFileTreePageHeader page_header;
  if (OB_FAIL(read_page_header_(page_buff, page_header))) {
    STORAGE_LOG(WARN, "fail to read page header", KR(ret), K(fd_), KP(page_buff));
  } else if (OB_UNLIKELY(0 >= page_header.item_num_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "unexpected item_num", KR(ret), K(fd_), K(page_header));
  } else if (OB_FAIL(read_item_(page_buff, page_header.item_num_ - 1, last_meta_item))) {
    STORAGE_LOG(WARN, "fail to read item", KR(ret), K(fd_), KP(page_buff), K(page_header));
  } else {
    last_item_index = page_header.item_num_ - 1;
  }
  return ret;
}

int ObSharedNothingTmpFileMetaTree::insert_items(
    const ObIArray<ObSharedNothingTmpFileDataItem> &data_items,
    const int64_t timeout_ms)
{
  int ret = OB_SUCCESS;
  bool need_build_tree = false;
  SpinWLockGuard guard(lock_);
  if (OB_UNLIKELY(data_items.empty())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", KR(ret), K(data_items));
  } else if (OB_UNLIKELY(!data_items.at(0).is_valid()
                        || released_offset_ > data_items.at(0).virtual_page_id_ * ObTmpFileGlobal::PAGE_SIZE
                        || (!prealloc_info_.is_empty() && 0 != prealloc_info_.next_entry_arr_index_)
                        || (prealloc_info_.is_empty() && !prealloc_info_.page_entry_arr_.empty()))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid argument", KR(ret), K(data_items.at(0)), KPC(this));
  } else if (!root_item_.is_valid() && OB_FAIL(try_to_insert_items_to_array_(data_items, need_build_tree))) {
    STORAGE_LOG(WARN, "fail to try to insert items to array", KR(ret), K(data_items), KPC(this));
  } else if (need_build_tree || root_item_.is_valid()) {
    int64_t write_count = 0;
    const int64_t data_item_count = data_items.count();
    const int16_t origin_arr_index = prealloc_info_.next_entry_arr_index_;
    const int16_t origin_index_in_entry = prealloc_info_.next_page_index_in_entry_;
    ObSEArray<int16_t, 2> level_origin_page_write_counts;
    ObSEArray<ObSEArray<ObTmpFilePageHandle, 2>, 2> level_new_pages;
    ObSEArray<ObTmpFilePageHandle, 2> right_page_handles;
    if (OB_FAIL(right_page_handles.push_back(ObTmpFilePageHandle()))) {
      STORAGE_LOG(WARN, "fail to push back", KR(ret), KPC(this));
    } else if (OB_FAIL(wbp_->shared_lock(fd_))) {
      STORAGE_LOG(WARN, "fail to shared lock", KR(ret), K(fd_));
    } else {
      bool fast_get_succ = false;
      int16_t page_remain_cnt = 0;
      if (need_build_tree && OB_FAIL(build_tree_(data_items, timeout_ms, level_new_pages, right_page_handles.at(0)))) {
        STORAGE_LOG(WARN, "fail to build tree", KR(ret), K(data_items), K(timeout_ms), KPC(this));
      } else if (!right_page_handles.at(0).is_valid()) {
        if (OB_UNLIKELY(!level_new_pages.empty())) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(ERROR, "unexpected level_new_pages", KR(ret), K(level_new_pages), KPC(this));
        } else if (OB_FAIL(fast_get_rightmost_leaf_page_for_write_(right_page_handles.at(0), page_remain_cnt))) {
          STORAGE_LOG(WARN, "fail to fast get rightmost leaf page for write", KR(ret), KPC(this));
        } else if (right_page_handles.at(0).is_valid() && page_remain_cnt >= data_item_count) {
          fast_get_succ = true;
        } else {
          right_page_handles.reset();
          if (OB_FAIL(get_rightmost_pages_for_write_(timeout_ms, right_page_handles))) {
            STORAGE_LOG(WARN, "fail to get rightmost pages for write", KR(ret), K(timeout_ms), KPC(this));
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(try_to_fill_rightmost_leaf_page_(right_page_handles.at(0), data_items, level_new_pages,
                                                          write_count, level_origin_page_write_counts))) {
          STORAGE_LOG(WARN, "fail to try to fill rightmost leaf page", KR(ret),
                              K(right_page_handles.at(0)), K(data_items), K(level_new_pages), KPC(this));
        } else if (write_count < data_item_count) {
          ObSEArray<ObSharedNothingTmpFileMetaItem, 1> meta_items;
          ObSharedNothingTmpFileMetaItem meta_item;
          if (OB_UNLIKELY(fast_get_succ)) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "unexpected fast_get", KR(ret), K(fast_get_succ), KPC(this));
          } else if (level_page_num_array_.count() <= 1) {
            //there is only a root page
            if (OB_FAIL(meta_items.push_back(root_item_))) {
              STORAGE_LOG(WARN, "fail to push back", KR(ret), K(fd_), K(root_item_));
            }
          }
          //alloc new leaf pages
          while (OB_SUCC(ret)
                && write_count < data_item_count) {
            meta_item.reset();
            if (OB_FAIL(add_new_page_and_fill_items_at_leaf_(data_items, timeout_ms, write_count, meta_item, level_new_pages))) {
              STORAGE_LOG(WARN, "fail to add new page and fill items at leaf", KR(ret),
                              K(data_items), K(timeout_ms), K(write_count), K(level_new_pages), KPC(this));
            } else if (OB_FAIL(meta_items.push_back(meta_item))) {
              STORAGE_LOG(WARN, "fail to push back", KR(ret), K(fd_), K(meta_item));
            }
          }
          //cascade to modify the internal pages
          if (FAILEDx(cascade_modification_at_internal_(right_page_handles, meta_items,
                      timeout_ms, level_origin_page_write_counts, level_new_pages))) {
            STORAGE_LOG(WARN, "fail to cascade modification at internal", KR(ret), K(right_page_handles), K(meta_items),
                                  K(timeout_ms), K(level_origin_page_write_counts), K(level_new_pages), KPC(this));
          }
        }
      }
      if (FAILEDx(level_page_num_array_.reserve(level_new_pages.count()))) {
        STORAGE_LOG(WARN, "fail to reserve for level_page_num_array_", KR(ret), K(fd_), K(level_new_pages.count()));
      }
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(wbp_->unlock(fd_))) {
        STORAGE_LOG(WARN, "fail to unlock", KR(tmp_ret), K(fd_));
      } else if (OB_TMP_FAIL(finish_insert_(ret, right_page_handles, level_new_pages, level_origin_page_write_counts,
                                                        origin_arr_index, origin_index_in_entry))) {
        STORAGE_LOG(WARN, "fail to finish insert", KR(tmp_ret), KR(ret), K(right_page_handles), K(level_origin_page_write_counts),
                                              K(level_new_pages), K(origin_arr_index), K(origin_index_in_entry), KPC(this));
      }
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }
  }
  if (OB_SUCC(ret)) {
    ARRAY_FOREACH_N(data_items, i, cnt) {
      stat_info_.all_type_page_occupy_disk_cnt_ += data_items.at(i).physical_page_num_;
    }
  }
  return ret;
}

int ObSharedNothingTmpFileMetaTree::try_to_insert_items_to_array_(
    const ObIArray<ObSharedNothingTmpFileDataItem> &data_items,
    bool &need_build_tree)
{
  int ret = OB_SUCCESS;
  need_build_tree = false;
  if (OB_UNLIKELY(data_items.empty())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid argument", KR(ret), K(fd_), K(data_items));
  } else if (!data_item_array_.empty()) {
    const ObSharedNothingTmpFileDataItem &last_item = data_item_array_.at(data_item_array_.count() - 1);
    if (OB_UNLIKELY(data_items.at(0).virtual_page_id_ != last_item.virtual_page_id_ + last_item.physical_page_num_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected data_items or data_item_array_", KR(ret), K(fd_), K(data_items), K(last_item));
    }
  }
  if (OB_SUCC(ret)) {
    const int64_t total_count = data_item_array_.count() + data_items.count();
    if (total_count <= MAX_DATA_ITEM_ARRAY_COUNT) {
      const int16_t array_capacity = data_item_array_.get_capacity();
      if (array_capacity < total_count) {
        if (OB_FAIL(data_item_array_.reserve(MIN(MAX(total_count, 2 * array_capacity), MAX_DATA_ITEM_ARRAY_COUNT)))) {
          STORAGE_LOG(WARN, "fail to reserve for data_item_array_", KR(ret), K(fd_), K(total_count), K(array_capacity));
          if (OB_ALLOCATE_MEMORY_FAILED == ret) {
            need_build_tree = true;
            ret = OB_SUCCESS;
          }
        }
      }
      ARRAY_FOREACH_N(data_items, i, cnt) {
        if (OB_UNLIKELY(!data_items.at(i).is_valid())) {
          ret = OB_INVALID_ARGUMENT;
          STORAGE_LOG(ERROR, "invalid argument", KR(ret), K(fd_), K(data_items.at(i)));
        } else if (OB_FAIL(data_item_array_.push_back(data_items.at(i)))) {
          STORAGE_LOG(ERROR, "fail to push back", KR(ret), K(fd_), K(data_items.at(i)));
        }
      }
    } else {
      need_build_tree = true;
    }
  }
  return ret;
}

int ObSharedNothingTmpFileMetaTree::build_tree_(const ObIArray<ObSharedNothingTmpFileDataItem> &data_items,
                                                const int64_t timeout_ms,
                                                ObIArray<ObSEArray<ObTmpFilePageHandle, 2>> &level_new_pages,
                                                ObTmpFilePageHandle &new_page_handle)
{
  int ret = OB_SUCCESS;
  const int16_t MAX_PAGE_DATA_ITEM_NUM =
      MIN(MAX_PAGE_ITEM_COUNT, (ObTmpFileGlobal::PAGE_SIZE - PAGE_HEADER_SIZE) / sizeof(data_items.at(0)));
  ObTmpFileWriteCacheKey leaf_page_offset(fd_, 0/*tree level*/, 0/*level page index*/);
  ObSEArray<ObTmpFilePageHandle, 2> new_pages;
  if (OB_UNLIKELY(!level_new_pages.empty()
                  || !level_page_num_array_.empty()
                  || MAX_DATA_ITEM_ARRAY_COUNT > MAX_PAGE_ITEM_COUNT)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid argument", KR(ret), K(fd_), K(level_page_num_array_),
                            K(level_new_pages), K(MAX_DATA_ITEM_ARRAY_COUNT), K(MAX_PAGE_ITEM_COUNT));
  } else if (OB_FAIL(new_pages.push_back(ObTmpFilePageHandle()))) {
    STORAGE_LOG(WARN, "fail to push back", KR(ret), K(fd_));
  } else if (OB_FAIL(level_new_pages.push_back(new_pages))) {
    STORAGE_LOG(WARN, "fail to push back", KR(ret), K(fd_), K(new_pages));
  } else {
    char *new_page_buff = NULL;
    int16_t count = 0;
    ObSharedNothingTmpFileMetaItem meta_item;
    if (OB_FAIL(alloc_new_meta_page_(leaf_page_offset, timeout_ms, new_page_handle, meta_item))) {
      STORAGE_LOG(WARN, "fail to alloc new meta page", KR(ret), K(fd_), K(leaf_page_offset), K(timeout_ms));
    } else if (FALSE_IT(new_page_buff = new_page_handle.get_page()->get_buffer())) {
    } else if (OB_FAIL(init_page_header_(new_page_buff, 0 /*level*/))) {
      STORAGE_LOG(WARN, "fail to init page header", KR(ret), K(fd_), KP(new_page_buff));
    } else if (!data_item_array_.empty() &&
                OB_FAIL(write_items_(new_page_buff, data_item_array_, 0/*begin_index*/, count))) {
      STORAGE_LOG(WARN, "fail to write items", KR(ret), K(fd_), K(data_item_array_), KP(new_page_buff));
    } else if (OB_FAIL(wbp_->put_page(new_page_handle))) {
      STORAGE_LOG(WARN, "fail to put page", KR(ret), K(fd_), K(new_page_handle));
    } else {
      level_new_pages.at(0).at(0) = new_page_handle;
      root_item_ = meta_item;
      root_item_.virtual_page_id_ = data_item_array_.empty() ?
                  data_items.at(0).virtual_page_id_ : data_item_array_.at(0).virtual_page_id_;
      if (MAX_PAGE_DATA_ITEM_NUM == count) {
        new_page_handle.get_page()->set_is_full(true);
      }
      //print page content
      ObSharedNothingTmpFileTreePageHeader tmp_page_header;
      ObSharedNothingTmpFileDataItem first_item;
      ObSharedNothingTmpFileDataItem last_item;
      read_page_simple_content_(new_page_buff, tmp_page_header, first_item, last_item);
      STORAGE_LOG(INFO, "dump tree page", KR(ret), K(fd_), K(leaf_page_offset),
                                KP(new_page_buff), K(tmp_page_header), K(first_item), K(last_item));
    }
  }
  return ret;
}

int ObSharedNothingTmpFileMetaTree::try_to_fill_rightmost_leaf_page_(
    ObTmpFilePageHandle &page_handle,
    const ObIArray<ObSharedNothingTmpFileDataItem> &data_items,
    const ObIArray<ObSEArray<ObTmpFilePageHandle, 2>> &level_new_pages,
    int64_t &write_count,
    ObIArray<int16_t> &level_origin_page_write_counts)
{
  int ret = OB_SUCCESS;
  write_count = 0;
  if (OB_UNLIKELY(data_items.empty() || !data_items.at(0).is_valid()
                  || (level_new_pages.empty() && level_page_num_array_.empty())
                  || !level_origin_page_write_counts.empty()
                  || NULL == page_handle.get_page())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid argument", KR(ret), K(fd_), K(data_items), K(level_new_pages),
                        K(level_page_num_array_), K(level_origin_page_write_counts), K(page_handle));
  } else {
    const int16_t MAX_PAGE_DATA_ITEM_NUM =
        MIN(MAX_PAGE_ITEM_COUNT, (ObTmpFileGlobal::PAGE_SIZE - PAGE_HEADER_SIZE) / sizeof(data_items.at(0)));
    int32_t level_page_index = level_new_pages.empty() ? level_page_num_array_.at(0) - 1 : 0;
    int16_t count = 0;
    ObSharedNothingTmpFileTreePageHeader page_header;
    char *leaf_page_buff = page_handle.get_page()->get_buffer();
    ObTmpFileWriteCacheKey leaf_page_offset(fd_, 0, level_page_index);
    if (OB_FAIL(read_page_header_(leaf_page_buff, page_header))) {
      STORAGE_LOG(WARN, "fail to read header", KR(ret), K(fd_), KP(leaf_page_buff));
    } else if (0 == page_header.item_num_) {
      if (OB_UNLIKELY(level_new_pages.empty())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "leaf page item_num is 0", KR(ret), K(fd_), KP(leaf_page_buff), K(page_header));
      }
    } else {
      //check last data item
      ObSharedNothingTmpFileDataItem origin_last_item;
      if (OB_FAIL(read_item_(leaf_page_buff, page_header.item_num_ - 1, origin_last_item))) {
        STORAGE_LOG(WARN, "fail to read item", KR(ret), K(fd_), KP(leaf_page_buff), K(page_header));
      } else if (OB_UNLIKELY(data_items.at(0).virtual_page_id_ != origin_last_item.virtual_page_id_ + origin_last_item.physical_page_num_)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected data_items or origin_last_item", KR(ret), K(fd_), K(data_items), K(origin_last_item));
      }
    }
    if (OB_SUCC(ret)) {
      if (!level_page_num_array_.empty() && OB_FAIL(level_origin_page_write_counts.push_back(count))) {
        STORAGE_LOG(WARN, "fail to push back", KR(ret), K(fd_), K(count));
      } else if (page_header.item_num_ < MAX_PAGE_DATA_ITEM_NUM) {
        if (OB_FAIL(write_items_(leaf_page_buff, data_items, 0, count))) {
          STORAGE_LOG(WARN, "fail to write items", KR(ret), K(fd_), KP(leaf_page_buff), K(data_items));
        } else {
          write_count += count;
          if (!level_page_num_array_.empty()) {
            level_origin_page_write_counts.at(0) = count;
          }
          if (page_header.item_num_ + count == MAX_PAGE_DATA_ITEM_NUM) {
            page_handle.get_page()->set_is_full(true);
          }
        }
        if (OB_SUCC(ret)) {
          //print page content
          ObSharedNothingTmpFileTreePageHeader tmp_page_header;
          ObSharedNothingTmpFileDataItem first_item;
          ObSharedNothingTmpFileDataItem last_item;
          read_page_simple_content_(leaf_page_buff, tmp_page_header, first_item, last_item);
          STORAGE_LOG(DEBUG, "dump tree page", KR(ret), K(fd_), K(leaf_page_offset),
                            KP(leaf_page_buff), K(tmp_page_header), K(first_item), K(last_item));
        }
      }
    }
  }
  return ret;
}

int ObSharedNothingTmpFileMetaTree::add_new_page_and_fill_items_at_leaf_(
    const ObIArray<ObSharedNothingTmpFileDataItem> &data_items,
    const int64_t timeout_ms,
    int64_t &write_count,
    ObSharedNothingTmpFileMetaItem &meta_item,
    ObIArray<ObSEArray<ObTmpFilePageHandle, 2>> &level_new_pages)
{
  int ret = OB_SUCCESS;
  meta_item.reset();
  const int16_t MAX_PAGE_DATA_ITEM_NUM =
      MIN(MAX_PAGE_ITEM_COUNT, (ObTmpFileGlobal::PAGE_SIZE - PAGE_HEADER_SIZE) / sizeof(data_items.at(0)));
  char *new_page_buff = nullptr;
  int16_t count = 0;
  int32_t level_page_index = 0;
  if (!level_page_num_array_.empty()) {
    level_page_index += level_page_num_array_.at(0);
  }
  if (!level_new_pages.empty()) {
    level_page_index += level_new_pages.at(0).count();
  }
  ObTmpFilePageHandle new_page_handle;
  ObTmpFileWriteCacheKey leaf_page_offset(fd_, 0, level_page_index);
  if (OB_UNLIKELY(!level_new_pages.empty() && 1 != level_new_pages.count())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid argument", KR(ret), K(fd_), K(level_new_pages));
  } else if (OB_UNLIKELY(data_items.count() <= write_count || !data_items.at(write_count).is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid argument", KR(ret), K(fd_), K(data_items), K(write_count));
  } else if (level_new_pages.empty()) {
    ObSEArray<ObTmpFilePageHandle, 2> new_pages;
    if (OB_FAIL(level_new_pages.push_back(new_pages))) {
      STORAGE_LOG(WARN, "fail to push back", KR(ret), K(fd_), K(new_pages));
    }
  }
  if (FAILEDx(level_new_pages.at(0).push_back(ObTmpFilePageHandle()))) {
    STORAGE_LOG(WARN, "fail to push back", KR(ret), K(fd_));
  } else if (OB_FAIL(alloc_new_meta_page_(leaf_page_offset, timeout_ms, new_page_handle, meta_item))) {
    STORAGE_LOG(WARN, "fail to alloc page from write cache", KR(ret), K(fd_), K(leaf_page_offset), K(timeout_ms));
  } else if (FALSE_IT(new_page_buff = new_page_handle.get_page()->get_buffer())) {
  } else if (OB_FAIL(init_page_header_(new_page_buff, 0 /*level*/))) {
    STORAGE_LOG(WARN, "fail to init page header", KR(ret), K(fd_), KP(new_page_buff));
  } else if (OB_FAIL(write_items_(new_page_buff, data_items, write_count/*begin_index*/, count))) {
    STORAGE_LOG(WARN, "fail to write items", KR(ret), K(fd_), KP(new_page_buff), K(data_items), K(write_count));
  } else if (OB_FAIL(wbp_->put_page(new_page_handle))) {
    STORAGE_LOG(WARN, "fail to put page", KR(ret), K(fd_), K(new_page_handle));
  } else {
    level_new_pages.at(0).at(level_new_pages.at(0).count() - 1) = new_page_handle;
    meta_item.virtual_page_id_ = data_items.at(write_count).virtual_page_id_;
    write_count += count;
    if (MAX_PAGE_DATA_ITEM_NUM == count) {
      new_page_handle.get_page()->set_is_full(true);
    }
  }
  if (OB_SUCC(ret)) {
    //print page content
    ObSharedNothingTmpFileTreePageHeader tmp_page_header;
    ObSharedNothingTmpFileDataItem first_item;
    ObSharedNothingTmpFileDataItem last_item;
    read_page_simple_content_(new_page_buff, tmp_page_header, first_item, last_item);
    STORAGE_LOG(INFO, "dump tree page", KR(ret), K(fd_), K(leaf_page_offset),
                              KP(new_page_buff), K(tmp_page_header), K(first_item), K(last_item));
  }
  return ret;
}

int ObSharedNothingTmpFileMetaTree::cascade_modification_at_internal_(
    ObSEArray<ObTmpFilePageHandle, 2> &right_page_handles,
    const ObIArray<ObSharedNothingTmpFileMetaItem> &new_leaf_page_infos,
    const int64_t timeout_ms,
    ObIArray<int16_t> &level_origin_page_write_counts,
    ObIArray<ObSEArray<ObTmpFilePageHandle, 2>> &level_new_pages)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(new_leaf_page_infos.empty()
                  || level_new_pages.empty()
                  || right_page_handles.count() < level_page_num_array_.count())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid argument", KR(ret), K(fd_), K(new_leaf_page_infos), K(level_new_pages), K(right_page_handles),
                                                              K(level_page_num_array_));
  } else {
    ObSEArray<ObSharedNothingTmpFileMetaItem, 1> prev_meta_items;
    ObSEArray<ObSharedNothingTmpFileMetaItem, 1> meta_items;
    ObSharedNothingTmpFileMetaItem meta_item;
    const int16_t origin_level_count = level_page_num_array_.count();
    int16_t cur_level = 1;
    while (OB_SUCC(ret)) {
      int64_t write_count = 0;
      prev_meta_items.reset();
      if (cur_level > origin_level_count && meta_items.count() == 1) {
        //we need to change root_item_
        break;
      } else if (cur_level == 1 && OB_FAIL(prev_meta_items.assign(new_leaf_page_infos))) {
        STORAGE_LOG(WARN, "fail to assign", KR(ret), K(fd_), K(new_leaf_page_infos));
      } else if (cur_level > 1 && OB_FAIL(prev_meta_items.assign(meta_items))) {
        STORAGE_LOG(WARN, "fail to assign", KR(ret), K(fd_), K(meta_items));
      } else if (cur_level < origin_level_count &&
                OB_FAIL(try_to_fill_rightmost_internal_page_(right_page_handles.at(cur_level), prev_meta_items,
                                            cur_level, write_count, level_origin_page_write_counts))) {
        STORAGE_LOG(WARN, "fail to try to fill rightmost internal page", KR(ret), K(fd_), K(right_page_handles.at(cur_level)),
                                            K(prev_meta_items), K(cur_level), K(level_origin_page_write_counts));
      } else {
        int64_t meta_item_count = prev_meta_items.count();
        if (write_count < meta_item_count) {
          meta_items.reset();
          if (origin_level_count - 1 == cur_level) {
            if (OB_FAIL(meta_items.push_back(root_item_))) {
              STORAGE_LOG(WARN, "fail to push back", KR(ret), K(fd_), K(root_item_));
            }
          }
          while (OB_SUCC(ret) && write_count < meta_item_count) {
            meta_item.reset();
            if (OB_FAIL(add_new_page_and_fill_items_at_internal_(prev_meta_items, cur_level, timeout_ms,
                                                                    write_count, meta_item, level_new_pages))) {
              STORAGE_LOG(WARN, "fail to add new page and fill items at internal", KR(ret), K(fd_),
                                            K(prev_meta_items), K(cur_level), K(timeout_ms), K(write_count));
            } else if (OB_FAIL(meta_items.push_back(meta_item))) {
              STORAGE_LOG(WARN, "fail to push_back", KR(ret), K(fd_), K(meta_item));
            }
          }
        } else {
          //cur level does not alloc new pages
          break;
        }
      }
      cur_level++;
    }
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(origin_level_count == level_new_pages.count())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "unexpected level_new_pages", KR(ret), K(fd_), K(level_new_pages), K(origin_level_count));
      } else if (origin_level_count < level_new_pages.count()) {
        if (OB_UNLIKELY(1 != meta_items.count()
                        || 1 != level_new_pages.at(level_new_pages.count() - 1).count())) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(ERROR, "unexpected meta_items or level_new_pages", KR(ret), K(fd_), K(meta_items),
                                              K(level_new_pages), K(origin_level_count));
        } else {
          root_item_ = meta_items.at(0);
        }
      }
    }
  }
  return ret;
}

int ObSharedNothingTmpFileMetaTree::try_to_fill_rightmost_internal_page_(
    ObTmpFilePageHandle &page_handle,
    const ObIArray<ObSharedNothingTmpFileMetaItem> &meta_items,
    const int16_t page_level,
    int64_t &write_count,
    ObIArray<int16_t> &level_origin_page_write_counts)
{
  int ret = OB_SUCCESS;
  write_count = 0;
  //only a writing thread, array count will not change
  if (OB_UNLIKELY(meta_items.empty()
                  || page_level < 1
                  || page_level >= level_page_num_array_.count()
                  || page_level != level_origin_page_write_counts.count()
                  || NULL == page_handle.get_page())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid argument", KR(ret), K(fd_), K(meta_items), K(page_level), K(level_page_num_array_),
                                              K(level_origin_page_write_counts), K(page_handle));
  } else {
    const int16_t MAX_PAGE_META_ITEM_NUM =
            MIN(MAX_PAGE_ITEM_COUNT, (ObTmpFileGlobal::PAGE_SIZE - PAGE_HEADER_SIZE) / sizeof(meta_items.at(0)));
    char *internal_page_buff = page_handle.get_page()->get_buffer();
    ObSharedNothingTmpFileTreePageHeader page_header;
    int16_t count = 0;
    int32_t level_page_index = level_page_num_array_.at(page_level) - 1;
    ObTmpFileWriteCacheKey internal_page_offset(fd_, page_level, level_page_index);
    //only a writing thread.
    if (OB_FAIL(read_page_header_(internal_page_buff, page_header))) {
      STORAGE_LOG(WARN, "fail to read page header", KR(ret), K(fd_), KP(internal_page_buff));
    } else if (OB_FAIL(level_origin_page_write_counts.push_back(count))) {
      STORAGE_LOG(WARN, "fail to push back", KR(ret), K(fd_), K(count));
    } else if (page_header.item_num_ < MAX_PAGE_META_ITEM_NUM) {
      if (page_header.item_num_ <= 0) {
        //the rightmost page in internal level must has items
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "unexpected item_num", KR(ret), K(fd_), K(page_header));
      } else if (OB_FAIL(write_items_(internal_page_buff, meta_items, 0, count))) {
        STORAGE_LOG(WARN, "fail to write items", KR(ret), K(fd_), KP(internal_page_buff), K(meta_items));
      } else {
        write_count += count;
        level_origin_page_write_counts.at(page_level) = count;
        if (page_header.item_num_ + count == MAX_PAGE_META_ITEM_NUM) {
          page_handle.get_page()->set_is_full(true);
        }
        //print page content
        ObSharedNothingTmpFileTreePageHeader tmp_page_header;
        ObSharedNothingTmpFileMetaItem first_item;
        ObSharedNothingTmpFileMetaItem last_item;
        read_page_simple_content_(internal_page_buff, tmp_page_header, first_item, last_item);
        STORAGE_LOG(INFO, "dump tree page", KR(ret), K(fd_), K(internal_page_offset),
                                  KP(internal_page_buff), K(tmp_page_header), K(first_item), K(last_item));
      }
    }
  }
  return ret;
}

int ObSharedNothingTmpFileMetaTree::add_new_page_and_fill_items_at_internal_(
    const ObIArray<ObSharedNothingTmpFileMetaItem> &meta_items,
    const int16_t page_level,
    const int64_t timeout_ms,
    int64_t &write_count,
    ObSharedNothingTmpFileMetaItem &meta_item,
    ObIArray<ObSEArray<ObTmpFilePageHandle, 2>> &level_new_pages)
{
  int ret = OB_SUCCESS;
  const int16_t MAX_PAGE_META_ITEM_NUM =
      MIN(MAX_PAGE_ITEM_COUNT, (ObTmpFileGlobal::PAGE_SIZE - PAGE_HEADER_SIZE) / sizeof(meta_items.at(0)));
  char *new_page_buff = NULL;
  int16_t count = 0;
  int32_t level_page_index = 0;
  if (page_level < level_page_num_array_.count()) {
    level_page_index += level_page_num_array_.at(page_level);
  }
  if (page_level < level_new_pages.count()) {
    level_page_index += level_new_pages.at(page_level).count();
  }
  ObTmpFilePageHandle new_page_handle;
  ObTmpFileWriteCacheKey internal_page_offset(fd_, page_level, level_page_index);
  if (OB_UNLIKELY(meta_items.count() <= write_count || !meta_items.at(write_count).is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid argument", KR(ret), K(fd_), K(meta_items), K(write_count));
  } else if (OB_UNLIKELY(level_new_pages.count() != page_level
                          && level_new_pages.count() != page_level + 1)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "unexpected level_new_pages", KR(ret), K(fd_), K(page_level), K(level_new_pages));
  } else if (level_new_pages.count() == page_level) {
    ObSEArray<ObTmpFilePageHandle, 2> new_pages;
    if (OB_FAIL(level_new_pages.push_back(new_pages))) {
      STORAGE_LOG(WARN, "fail to push back", KR(ret), K(fd_), K(new_pages));
    }
  }
  if (FAILEDx(level_new_pages.at(page_level).push_back(ObTmpFilePageHandle()))) {
    STORAGE_LOG(WARN, "fail to push back", KR(ret), K(fd_), K(page_level));
  } else if (OB_FAIL(alloc_new_meta_page_(internal_page_offset, timeout_ms, new_page_handle, meta_item))) {
    STORAGE_LOG(WARN, "fail to alloc new meta page", KR(ret), K(fd_), K(internal_page_offset), K(timeout_ms));
  } else if (FALSE_IT(new_page_buff = new_page_handle.get_page()->get_buffer())) {
  } else if (OB_FAIL(init_page_header_(new_page_buff, page_level))) {
    STORAGE_LOG(WARN, "fail to init page header", KR(ret), K(fd_), KP(new_page_buff));
  } else if (OB_FAIL(write_items_(new_page_buff, meta_items, write_count/*begin_index*/, count))) {
    STORAGE_LOG(WARN, "fail to write items", KR(ret), K(fd_), KP(new_page_buff), K(meta_items), K(write_count));
  } else if (OB_FAIL(wbp_->put_page(new_page_handle))) {
        STORAGE_LOG(WARN, "fail to put page", KR(ret), K(fd_), K(new_page_handle));
  } else {
    level_new_pages.at(page_level).at(level_new_pages.at(page_level).count() - 1) = new_page_handle;
    meta_item.virtual_page_id_ = meta_items.at(write_count).virtual_page_id_;
    write_count += count;
    if (MAX_PAGE_META_ITEM_NUM == count) {
      new_page_handle.get_page()->set_is_full(true);
    }
  }
  if (OB_SUCC(ret)) {
    //print page content
    ObSharedNothingTmpFileTreePageHeader tmp_page_header;
    ObSharedNothingTmpFileMetaItem first_item;
    ObSharedNothingTmpFileMetaItem last_item;
    read_page_simple_content_(new_page_buff, tmp_page_header, first_item, last_item);
    STORAGE_LOG(INFO, "dump tree page", KR(ret), K(fd_), K(internal_page_offset),
                              KP(new_page_buff), K(tmp_page_header), K(first_item), K(last_item));
  }
  return ret;
}

int ObSharedNothingTmpFileMetaTree::finish_insert_(
    const int return_ret,
    ObSEArray<ObTmpFilePageHandle, 2> &right_page_handles,
    ObIArray<ObSEArray<ObTmpFilePageHandle, 2>> &level_new_pages,
    const ObIArray<int16_t> &level_origin_page_write_counts,
    const int16_t origin_arr_index,
    const int16_t origin_index_in_entry)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(level_origin_page_write_counts.count() > level_page_num_array_.count()
                  || level_origin_page_write_counts.count() > right_page_handles.count())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "unexpected new record level page infos", KR(ret), K(fd_),
                  K(level_origin_page_write_counts), K(level_page_num_array_), K(right_page_handles));
  } else if (OB_SUCCESS == return_ret) {
    ObTmpFileBlockHandle block_handle;
    ObTmpFileBlock *block = nullptr;
    ObTmpFilePage *page = nullptr;
    ARRAY_FOREACH_N(level_new_pages, level, level_cnt) {
      int32_t level_new_page_num = level_new_pages.at(level).count();
      if (level_page_num_array_.count() <= level) {
        if (OB_FAIL(level_page_num_array_.push_back(level_new_page_num))) {
          STORAGE_LOG(ERROR, "fail to push back", KR(ret), K(fd_), K(level_new_page_num));
        }
      } else {
        level_page_num_array_.at(level) += level_new_page_num;
      }
      ARRAY_FOREACH_N(level_new_pages.at(level), i, new_page_cnt) {
        block_handle.reset();
        block = nullptr;
        page = level_new_pages.at(level).at(i).get_page();
        if (OB_ISNULL(page)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(ERROR, "unexpected null page", KR(ret), K(fd_), KP(page));
        } else if (OB_FAIL(block_manager_->get_tmp_file_block_handle(page->get_page_id().block_index_, block_handle))) {
          STORAGE_LOG(ERROR, "fail to get tmp file block handle", KR(ret), K(fd_), K(page->get_page_id()));
        } else if (OB_ISNULL(block = block_handle.get())) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(ERROR, "unexpected null block", KR(ret), K(fd_), KP(block));
        } else if (OB_FAIL(block->insert_page_into_flushing_list(level_new_pages.at(level).at(i)))) {
          STORAGE_LOG(ERROR, "fail to insert page into flushing list", KR(ret), K(fd_), K(level_new_pages.at(level).at(i)));
        }
      }
    }
    if (FAILEDx(update_prealloc_info_())) {
      STORAGE_LOG(WARN, "fail to update prealloc_info_", KR(ret), K(fd_));
    } else if (!data_item_array_.empty()) {
      data_item_array_.reset();
    }
  } else { //fail
    //rollback
    ARRAY_FOREACH_N(level_origin_page_write_counts, i, cnt) {
      const int16_t remove_cnt = level_origin_page_write_counts.at(i);
      if (0 != remove_cnt) {
        char *page_buff = right_page_handles.at(i).get_page()->get_buffer();
        int32_t level_page_index = level_page_num_array_[i] - 1;
        ObTmpFileWriteCacheKey page_key(fd_, i, level_page_index);
        if (OB_FAIL(remove_page_item_from_tail_(page_buff, remove_cnt))) {
          STORAGE_LOG(WARN, "fail to remove page item from tail", KR(ret), K(fd_), K(page_key), KP(page_buff), K(remove_cnt));
        }
      }
    }
    ARRAY_FOREACH_N(level_new_pages, level, level_cnt) {
      ARRAY_FOREACH_N(level_new_pages.at(level), i, new_page_cnt) {
        if (level_new_pages.at(level).at(i).is_valid()) {
          ObTmpFilePage *page = level_new_pages.at(level).at(i).get_page();
          if (OB_ISNULL(page)) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(ERROR, "unexpected null page", KR(ret), K(fd_), KP(page));
          } else if (OB_FAIL(wbp_->free_page(page->get_page_key()))) {
            STORAGE_LOG(ERROR, "fail to free meta page in write cache", KR(ret), K(fd_), K(page->get_page_key()));
          }
        }
      }
    }
    if (FAILEDx(rollback_prealloc_info_(origin_arr_index, origin_index_in_entry))) {
      STORAGE_LOG(WARN, "fail to rollbak prealloc_info_", KR(ret), K(fd_));
    } else {
      if (level_page_num_array_.empty()) {
        root_item_.reset();
      } else {
        if (OB_UNLIKELY(1 != level_page_num_array_.at(level_page_num_array_.count() - 1)
                        || root_item_.page_level_ + 1 != level_page_num_array_.count())) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(ERROR, "unexpected level_page_num_array_", KR(ret), K(fd_), K(level_page_num_array_), K(root_item_));
        }
      }
    }
    STORAGE_LOG(INFO, "fail to insert, finish rollback to before", KR(ret), K(return_ret), KPC(this));
  }
  return ret;
}

//NOTE: Don't call this function now
// int ObSharedNothingTmpFileMetaTree::insert_item(
//     const ObSharedNothingTmpFileDataItem &data_item)
// {
//   int ret = OB_SUCCESS;
//   SpinWLockGuard guard(lock_);
//   if (OB_UNLIKELY(!data_item.is_valid()
//                   || released_offset_ > data_item.virtual_page_id_ * ObTmpFileGlobal::PAGE_SIZE
//                   || (!prealloc_info_.is_empty() && 0 != prealloc_info_.next_entry_arr_index_))) {
//     ret = OB_INVALID_ARGUMENT;
//     STORAGE_LOG(ERROR, "invalid argument", KR(ret), K(data_item), KPC(this));
//   } else if (!root_item_.is_valid()) {
//     if (OB_FAIL(insert_item_to_array_or_new_tree_(data_item))) {
//       STORAGE_LOG(WARN, "fail to try to insert item to array or new tree", KR(ret), K(data_item), KPC(this));
//     }
//   } else {
//     const int16_t origin_arr_index = prealloc_info_.next_entry_arr_index_;
//     const int16_t origin_index_in_entry = prealloc_info_.next_page_index_in_entry_;
//     int16_t modify_origin_page_level = -1;
//     ObSharedNothingTmpFileMetaItem meta_item;
//     ObSEArray<ObTmpFilePageHandle, 2> right_page_handles;
//     ObSEArray<ObTmpFileWriteCacheKey, 2> level_new_pages;
//     int16_t page_remain_cnt = 0;
//     bool fast_get_succ = false;
//     if (OB_FAIL(right_page_handles.push_back(ObTmpFilePageHandle()))) {
//       STORAGE_LOG(WARN, "fail to push back", KR(ret), KPC(this));
//     } else if (OB_FAIL(fast_get_rightmost_leaf_page_for_write_(right_page_handles.at(0), page_remain_cnt))) {
//       STORAGE_LOG(WARN, "fail to fast get rightmost leaf page for write", KR(ret), KPC(this));
//     } else if (right_page_handles.at(0).is_valid() && 0 < page_remain_cnt) {
//       fast_get_succ = true;
//     } else {
//       right_page_handles.reset();
//       if (OB_FAIL(get_rightmost_pages_for_write_(right_page_handles))) {
//         STORAGE_LOG(WARN, "fail to get rightmost pages for write", KR(ret), KPC(this));
//       }
//     }
//     if (OB_SUCC(ret)) {
//       if (OB_UNLIKELY(right_page_handles.empty())) {
//         STORAGE_LOG(ERROR, "right_page_handles is empty", KR(ret), KPC(this));
//       } else if (OB_FAIL(try_to_fill_rightmost_leaf_page_(right_page_handles.at(0), data_item, level_new_pages, meta_item, modify_origin_page_level))) {
//         STORAGE_LOG(WARN, "fail to try to fill rightmost leaf page", KR(ret), K(data_item), K(level_new_pages), KPC(this));
//       } else if (meta_item.is_valid()) {
//         //cascade to modify the internal pages
//         if (OB_UNLIKELY(fast_get_succ)) {
//           ret = OB_ERR_UNEXPECTED;
//           STORAGE_LOG(WARN, "unexpected fast_get", KR(ret), K(fast_get_succ), KPC(this));
//         } else if (OB_FAIL(cascade_modification_at_internal_(right_page_handles,
//                                   meta_item, level_new_pages, modify_origin_page_level))) {
//           STORAGE_LOG(WARN, "fail to cascade modification at internal", KR(ret), K(right_page_handles),
//                                         K(meta_item), K(level_new_pages), K(modify_origin_page_level), KPC(this));
//         }
//       }
//     }
//     if (OB_FAIL(ret)) {
//       int tmp_ret = OB_SUCCESS;
//       if (OB_TMP_FAIL(rollback_insert_(right_page_handles, level_new_pages, modify_origin_page_level, origin_arr_index, origin_index_in_entry))) {
//         STORAGE_LOG(WARN, "fail to rollback insert", KR(tmp_ret), KR(ret),
//               K(right_page_handles), K(level_new_pages), K(modify_origin_page_level), K(origin_arr_index), K(origin_index_in_entry), KPC(this));
//       }
//     }
//     ARRAY_FOREACH_N(right_page_handles, i, count) {
//       right_page_handles.at(i).reset();
//     }
//   }
//   if (OB_SUCC(ret)) {
//     stat_info_.all_type_page_occupy_disk_cnt_ += data_item.physical_page_num_;
//   }
//   return ret;
// }

// int ObSharedNothingTmpFileMetaTree::insert_item_to_array_or_new_tree_(
//     const ObSharedNothingTmpFileDataItem &data_item)
// {
//   int ret = OB_SUCCESS;
//   if (OB_UNLIKELY(!level_page_num_array_.empty()
//                   || MAX_DATA_ITEM_ARRAY_COUNT >= MAX_PAGE_ITEM_COUNT
//                   || (prealloc_info_.is_empty() && !prealloc_info_.page_entry_arr_.empty())
//                   || (!prealloc_info_.is_empty() && 0 != prealloc_info_.next_entry_arr_index_))) {
//     ret = OB_INVALID_ARGUMENT;
//     STORAGE_LOG(ERROR, "invalid argument", KR(ret), K(fd_), K(level_page_num_array_),
//                             K(MAX_DATA_ITEM_ARRAY_COUNT), K(MAX_PAGE_ITEM_COUNT), K(prealloc_info_));
//   } else if (!data_item_array_.empty()) {
//     const ObSharedNothingTmpFileDataItem &last_item = data_item_array_.at(data_item_array_.count() - 1);
//     if (OB_UNLIKELY(data_item.virtual_page_id_ != last_item.virtual_page_id_ + last_item.physical_page_num_)) {
//       ret = OB_ERR_UNEXPECTED;
//       STORAGE_LOG(ERROR, "unexpected data_item or data_item_array_", KR(ret), K(fd_), K(data_item), K(last_item));
//     }
//   }
//   if (OB_SUCC(ret)) {
//     bool need_build_tree = false;
//     const int64_t total_count = data_item_array_.count() + 1;
//     if (total_count <= MAX_DATA_ITEM_ARRAY_COUNT) {
//       const int16_t array_capacity = data_item_array_.get_capacity();
//       if (array_capacity < total_count) {
//         if (OB_FAIL(data_item_array_.reserve(MIN(MAX(total_count, 2 * array_capacity), MAX_DATA_ITEM_ARRAY_COUNT)))) {
//           STORAGE_LOG(WARN, "fail to reserve for data_item_array_", KR(ret), K(fd_), K(total_count), K(array_capacity));
//           if (OB_ALLOCATE_MEMORY_FAILED == ret) {
//             need_build_tree = true;
//           }
//         }
//       }
//       if (FAILEDx(data_item_array_.push_back(data_item))) {
//         STORAGE_LOG(ERROR, "fail to push back", KR(ret), K(fd_), K(data_item));
//       }
//     } else {
//       need_build_tree = true;
//     }
//     if (true == need_build_tree) {
//       ret = OB_SUCCESS;
//       const int16_t origin_arr_index = prealloc_info_.next_entry_arr_index_;
//       const int16_t origin_index_in_entry = prealloc_info_.next_page_index_in_entry_;
//       ObTmpFileWriteCacheKey leaf_page_offset(fd_, 0, 0);
//       ObTmpFilePageHandle page_handle;
//       ObSharedNothingTmpFileMetaItem meta_item;
//       char *new_page_buff = nullptr;
//       int16_t count = 0;
//       if (OB_FAIL(level_page_num_array_.push_back(1))) {
//         STORAGE_LOG(WARN, "fail to push back", KR(ret), K(fd_));
//       } else if (OB_FAIL(alloc_new_meta_page_(leaf_page_offset, page_handle, meta_item))) {
//         STORAGE_LOG(WARN, "fail to alloc cache meta page", KR(ret), K(fd_), K(leaf_page_offset));
//       } else if (FALSE_IT(new_page_buff = page_handle.get_page()->get_buffer())) {
//       } else if (OB_FAIL(init_page_header_(new_page_buff, 0 /*level*/))) {
//         STORAGE_LOG(WARN, "fail to init page header", KR(ret), K(fd_), KP(new_page_buff));
//       } else if (!data_item_array_.empty() &&
//                   OB_FAIL(write_items_(new_page_buff, data_item_array_, 0/*begin_index*/, count))) {
//         STORAGE_LOG(WARN, "fail to write items", KR(ret), K(fd_), K(data_item_array_), KP(new_page_buff));
//       } else if (OB_FAIL(write_item_(new_page_buff, data_item))) {
//         STORAGE_LOG(WARN, "fail to write item", KR(ret), K(fd_), KP(new_page_buff), K(data_item));
//       } else if (OB_FAIL(wbp_->put_page(page_handle))) {
//         STORAGE_LOG(WARN, "fail to put page", KR(ret), K(fd_), K(page_handle));
//       } else if (OB_FAIL(update_prealloc_info_())) {
//         STORAGE_LOG(WARN, "fail to update prealloc_info_", KR(ret), K(fd_));
//       } else {
//         root_item_ = meta_item;
//         root_item_.virtual_page_id_ = data_item_array_.empty() ?
//                     data_item.virtual_page_id_ : data_item_array_.at(0).virtual_page_id_;
//         data_item_array_.reset();
//         //print page content
//         ObSharedNothingTmpFileTreePageHeader tmp_page_header;
//         ObSharedNothingTmpFileDataItem first_item;
//         ObSharedNothingTmpFileDataItem last_item;
//         read_page_simple_content_(new_page_buff, tmp_page_header, first_item, last_item);
//         STORAGE_LOG(INFO, "dump tree page", KR(ret), K(fd_), K(leaf_page_offset), KP(new_page_buff),
//                                         K(tmp_page_header), K(first_item), K(last_item));
//       }
//       if (OB_FAIL(ret)) {
//         int tmp_ret = OB_SUCCESS;
//         if (OB_TMP_FAIL(rollback_prealloc_info_(origin_arr_index, origin_index_in_entry))) {
//           STORAGE_LOG(WARN, "fail to rollback prealloc_info_", KR(ret), KR(tmp_ret), K(fd_), K(origin_arr_index), K(origin_index_in_entry));
//         } else if (page_handle.is_valid()) {
//           if (OB_TMP_FAIL(wbp_->free_page(leaf_page_offset))) {
//             STORAGE_LOG(WARN, "fail to free page", KR(ret), KR(tmp_ret), K(fd_), K(leaf_page_offset));
//           } else {
//             level_page_num_array_.reset();
//           }
//         }
//       }
//     }
//   }
//   return ret;
// }

// int ObSharedNothingTmpFileMetaTree::try_to_fill_rightmost_leaf_page_(
//     ObTmpFilePageHandle &page_handle,
//     const ObSharedNothingTmpFileDataItem &data_item,
//     ObSEArray<ObTmpFileWriteCacheKey, 2> &level_new_pages,
//     ObSharedNothingTmpFileMetaItem &meta_item,
//     int16_t &modify_origin_page_level)
// {
//   int ret = OB_SUCCESS;
//   meta_item.reset();
//   modify_origin_page_level = -1;
//   if (OB_UNLIKELY(!data_item.is_valid()
//                   || !level_new_pages.empty()
//                   || level_page_num_array_.empty())) {
//     ret = OB_INVALID_ARGUMENT;
//     STORAGE_LOG(ERROR, "invalid argument", KR(ret), K(fd_), K(data_item), K(level_new_pages), K(level_page_num_array_));
//   } else {
//     const int16_t MAX_PAGE_DATA_ITEM_NUM =
//         MIN(MAX_PAGE_ITEM_COUNT, (ObTmpFileGlobal::PAGE_SIZE - PAGE_HEADER_SIZE) / sizeof(data_item));
//     ObSharedNothingTmpFileTreePageHeader page_header;
//     char *leaf_page_buff = page_handle.get_page()->get_buffer();
//     if (OB_FAIL(read_page_header_(leaf_page_buff, page_header))) {
//       STORAGE_LOG(WARN, "fail to read header", KR(ret), K(fd_), KP(leaf_page_buff));
//     } else if (OB_UNLIKELY(0 == page_header.item_num_)) {
//       ret = OB_ERR_UNEXPECTED;
//       STORAGE_LOG(ERROR, "leaf page item_num is 0", KR(ret), K(fd_), KP(leaf_page_buff));
//     } else {
//       //check last data item
//       ObSharedNothingTmpFileDataItem origin_last_item;
//       if (OB_FAIL(read_item_(leaf_page_buff, page_header.item_num_ - 1, origin_last_item))) {
//         STORAGE_LOG(WARN, "fail to read item", KR(ret), K(fd_), KP(leaf_page_buff), K(page_header));
//       } else if (OB_UNLIKELY(data_item.virtual_page_id_ != origin_last_item.virtual_page_id_ + origin_last_item.physical_page_num_)) {
//         ret = OB_ERR_UNEXPECTED;
//         STORAGE_LOG(ERROR, "unexpected data_item or origin_last_item", KR(ret), K(fd_), K(data_item), K(origin_last_item));
//       }
//     }
//     if (OB_SUCC(ret)) {
//       if (page_header.item_num_ < MAX_PAGE_DATA_ITEM_NUM) {
//         if (OB_FAIL(write_item_(leaf_page_buff, data_item))) {
//           STORAGE_LOG(WARN, "fail to write items", KR(ret), K(fd_), KP(leaf_page_buff), K(data_item));
//         } else {
//           modify_origin_page_level = 0;
//         }
//       } else {
//         ObTmpFilePageHandle new_page_handle;
//         ObTmpFileWriteCacheKey leaf_page_offset(fd_, 0, level_page_num_array_.at(0));
//         if (OB_FAIL(level_new_pages.push_back(ObTmpFileWriteCacheKey()))) {
//           STORAGE_LOG(WARN, "fail to push back", KR(ret), K(fd_));
//         } else if (OB_FAIL(alloc_new_meta_page_(leaf_page_offset, new_page_handle, meta_item))) {
//           STORAGE_LOG(WARN, "fail to alloc page from write cache", KR(ret), K(fd_), K(leaf_page_offset));
//         } else {
//           level_new_pages.at(0) = leaf_page_offset;
//           level_page_num_array_.at(0)++;
//           leaf_page_buff = new_page_handle.get_page()->get_buffer();
//         }
//         if (FAILEDx(init_page_header_(leaf_page_buff, 0 /*level*/))) {
//           STORAGE_LOG(WARN, "fail to init page header", KR(ret), K(fd_), KP(leaf_page_buff));
//         } else if (OB_FAIL(write_item_(leaf_page_buff, data_item))) {
//           STORAGE_LOG(WARN, "fail to write item", KR(ret), K(fd_), KP(leaf_page_buff), K(data_item));
//         } else if (OB_FAIL(wbp_->put_page(new_page_handle))) {
//           STORAGE_LOG(WARN, "fail to put page", KR(ret), K(fd_), K(new_page_handle));
//         } else {
//           meta_item.virtual_page_id_ = data_item.virtual_page_id_;
//         }
//       }
//     }
//     if (OB_SUCC(ret)) {
//       //print page content
//       ObSharedNothingTmpFileTreePageHeader tmp_page_header;
//       ObSharedNothingTmpFileDataItem first_item;
//       ObSharedNothingTmpFileDataItem last_item;
//       read_page_simple_content_(leaf_page_buff, tmp_page_header, first_item, last_item);
//       STORAGE_LOG(DEBUG, "dump tree page", KR(ret), K(fd_), K(level_page_num_array_),
//                         KP(leaf_page_buff), K(tmp_page_header), K(first_item), K(last_item));
//     }
//   }
//   return ret;
// }

// int ObSharedNothingTmpFileMetaTree::cascade_modification_at_internal_(
//     ObSEArray<ObTmpFilePageHandle, 2> &right_page_handles,
//     const ObSharedNothingTmpFileMetaItem &new_leaf_page_info,
//     ObSEArray<ObTmpFileWriteCacheKey, 2> &level_new_pages,
//     int16_t &modify_origin_page_level)
// {
//   int ret = OB_SUCCESS;
//   if (OB_UNLIKELY(1 >= right_page_handles.count()
//                   || root_item_.page_level_ + 1 != right_page_handles.count()
//                   || !new_leaf_page_info.is_valid()
//                   || 1 != level_new_pages.count())) {
//     ret = OB_INVALID_ARGUMENT;
//     STORAGE_LOG(ERROR, "invalid argument", KR(ret), K(fd_), K(right_page_handles), K(new_leaf_page_info), K(level_new_pages), K(root_item_));
//   } else {
//     ObSharedNothingTmpFileMetaItem prev_meta_item = new_leaf_page_info;
//     ObSharedNothingTmpFileMetaItem meta_item;
//     const int16_t origin_level_count = root_item_.page_level_ + 1;
//     int16_t cur_level = 1;
//     while (OB_SUCC(ret)) {
//       char *internal_page_buff = nullptr;
//       if (cur_level < origin_level_count) {
//         internal_page_buff = right_page_handles.at(cur_level).get_page()->get_buffer();
//       }
//       if (cur_level > origin_level_count) {
//         //we need to change root_item_
//         break;
//       } else if (FALSE_IT(meta_item.reset())) {
//       } else if (OB_FAIL(try_to_fill_rightmost_internal_page_(internal_page_buff, cur_level, prev_meta_item,
//                                                 level_new_pages, meta_item, modify_origin_page_level))) {
//         STORAGE_LOG(WARN, "fail to try to fill rightmost internal page", KR(ret), K(fd_), K(right_page_handles),
//                           K(cur_level), K(prev_meta_item), K(level_new_pages), K(meta_item), K(modify_origin_page_level));
//       } else {
//         if (meta_item.is_valid()) {
//           prev_meta_item = meta_item;
//         } else {
//           break;
//         }
//         cur_level++;
//       }
//     }
//     if (OB_SUCC(ret)) {
//       if (OB_UNLIKELY(origin_level_count == level_new_pages.count())) {
//         ret = OB_ERR_UNEXPECTED;
//         STORAGE_LOG(ERROR, "unexpected level_new_pages", KR(ret), K(fd_), K(level_new_pages), K(origin_level_count));
//       } else if (OB_FAIL(update_prealloc_info_())) {
//         STORAGE_LOG(WARN, "fail to update prealloc_info_", KR(ret), K(fd_));
//       } else if (origin_level_count < level_new_pages.count()) {
//         if (OB_UNLIKELY(!meta_item.is_valid()
//                         || 0 <= modify_origin_page_level)) {
//           ret = OB_ERR_UNEXPECTED;
//           STORAGE_LOG(ERROR, "unexpected meta_item or modify_origin_page_level", KR(ret), K(fd_), K(meta_item),
//                                     K(level_new_pages), K(origin_level_count), K(modify_origin_page_level));
//         } else {
//           root_item_ = meta_item;
//         }
//       }
//     }
//   }
//   return ret;
// }

// int ObSharedNothingTmpFileMetaTree::try_to_fill_rightmost_internal_page_(
//     char *internal_page_buff,
//     const int16_t page_level,
//     const ObSharedNothingTmpFileMetaItem &child_page_info,
//     ObSEArray<ObTmpFileWriteCacheKey, 2> &level_new_pages,
//     ObSharedNothingTmpFileMetaItem &meta_item,
//     int16_t &modify_origin_page_level)
// {
//   int ret = OB_SUCCESS;
//   meta_item.reset();
//   bool is_new_level = NULL == internal_page_buff;
//   if (OB_UNLIKELY((is_new_level && page_level <= root_item_.page_level_)
//                   || page_level < 1
//                   || page_level > level_page_num_array_.count()
//                   || page_level != level_new_pages.count()
//                   || !child_page_info.is_valid()
//                   || 0 <= modify_origin_page_level)) {
//     ret = OB_INVALID_ARGUMENT;
//     STORAGE_LOG(ERROR, "invalid argument", KR(ret), K(fd_), K(page_level), K(child_page_info),
//                                       K(is_new_level), K(root_item_), K(modify_origin_page_level));
//   } else {
//     bool need_new_page = false;
//     if (!is_new_level) {
//         const int16_t MAX_PAGE_META_ITEM_NUM =
//               MIN(MAX_PAGE_ITEM_COUNT, (ObTmpFileGlobal::PAGE_SIZE - PAGE_HEADER_SIZE) / sizeof(meta_item));
//       ObSharedNothingTmpFileTreePageHeader page_header;
//       //only a writing thread.
//       if (OB_FAIL(read_page_header_(internal_page_buff, page_header))) {
//         STORAGE_LOG(WARN, "fail to read page header", KR(ret), K(fd_), KP(internal_page_buff));
//       } else if (page_header.item_num_ < MAX_PAGE_META_ITEM_NUM) {
//         if (page_header.item_num_ <= 0) {
//           //the rightmost page in internal level must has items
//           ret = OB_ERR_UNEXPECTED;
//           STORAGE_LOG(ERROR, "unexpected item_num", KR(ret), K(fd_), K(page_header));
//         } else if (OB_FAIL(write_item_(internal_page_buff, child_page_info))) {
//           STORAGE_LOG(WARN, "fail to write items", KR(ret), K(fd_), KP(internal_page_buff), K(child_page_info));
//         } else {
//           modify_origin_page_level = page_level;
//         }
//       } else {
//         need_new_page = true;
//       }
//     } else {
//       need_new_page = true;
//     }
//     if (OB_SUCC(ret) && need_new_page) {
//       ObTmpFilePageHandle new_page_handle;
//       if (is_new_level && OB_FAIL(level_page_num_array_.push_back(0))) {
//         STORAGE_LOG(WARN, "fail to push back", KR(ret), K(fd_));
//       } else if (OB_FAIL(level_new_pages.push_back(ObTmpFileWriteCacheKey()))) {
//         STORAGE_LOG(WARN, "fail to push back", KR(ret), K(fd_));
//       } else if (OB_UNLIKELY(page_level >= level_page_num_array_.count())) {
//         ret = OB_ERR_UNEXPECTED;
//         STORAGE_LOG(WARN, "unexpected page_level or level_page_num_array_", KR(ret), K(fd_), K(page_level),
//                                                           K(root_item_), K(level_page_num_array_));
//       } else {
//         ObTmpFileWriteCacheKey internal_page_offset(fd_, -1, -1);
//         internal_page_offset.tree_level_ = page_level;
//         internal_page_offset.level_page_index_ = level_page_num_array_.at(page_level);
//         if (OB_FAIL(alloc_new_meta_page_(internal_page_offset, new_page_handle, meta_item))) {
//           STORAGE_LOG(WARN, "fail to alloc new meta page", KR(ret), K(fd_), K(internal_page_offset));
//         } else {
//           level_new_pages.at(page_level) = internal_page_offset;
//           level_page_num_array_.at(page_level)++;
//           internal_page_buff = new_page_handle.get_page()->get_buffer();
//         }
//       }
//       if (FAILEDx(init_page_header_(internal_page_buff, page_level))) {
//         STORAGE_LOG(WARN, "fail to init page header", KR(ret), K(fd_), KP(internal_page_buff));
//       } else if (is_new_level && OB_FAIL(write_item_(internal_page_buff, root_item_))) {
//         STORAGE_LOG(WARN, "fail to write item", KR(ret), K(fd_), KP(internal_page_buff), K(child_page_info));
//       } else if (OB_FAIL(write_item_(internal_page_buff, child_page_info))) {
//         STORAGE_LOG(WARN, "fail to write item", KR(ret), K(fd_), KP(internal_page_buff), K(child_page_info));
//       } else if (OB_FAIL(wbp_->put_page(new_page_handle))) {
//         STORAGE_LOG(WARN, "fail to put page", KR(ret), K(fd_), K(new_page_handle));
//       } else {
//         if (is_new_level) {
//           meta_item.virtual_page_id_ = root_item_.virtual_page_id_;
//         } else {
//           meta_item.virtual_page_id_ = child_page_info.virtual_page_id_;
//         }
//       }
//     }
//     if (OB_SUCC(ret)) {
//       //print page content
//       ObSharedNothingTmpFileTreePageHeader tmp_page_header;
//       ObSharedNothingTmpFileMetaItem first_item;
//       ObSharedNothingTmpFileMetaItem last_item;
//       read_page_simple_content_(internal_page_buff, tmp_page_header, first_item, last_item);
//       STORAGE_LOG(INFO, "dump tree page", KR(ret), K(fd_), K(level_page_num_array_),
//                                 KP(internal_page_buff), K(tmp_page_header), K(first_item), K(last_item));
//     }
//   }
//   return ret;
// }

// int ObSharedNothingTmpFileMetaTree::rollback_insert_(
//     ObSEArray<ObTmpFilePageHandle, 2> &right_page_handles,
//     const ObSEArray<ObTmpFileWriteCacheKey, 2> &level_new_pages,
//     const int16_t modify_origin_page_level,
//     const int16_t origin_arr_index,
//     const int16_t origin_index_in_entry)
// {
//   int ret = OB_SUCCESS;
//   if (OB_UNLIKELY(level_new_pages.count() > root_item_.page_level_ + 2
//                   || level_new_pages.count() > level_page_num_array_.count()
//                   || (0 <= modify_origin_page_level && modify_origin_page_level != level_new_pages.count())
//                   || modify_origin_page_level >= right_page_handles.count())) {
//     ret = OB_ERR_UNEXPECTED;
//     STORAGE_LOG(ERROR, "unexpected level_new_pages or modify_origin_page_level", KR(ret), K(fd_),
//         K(right_page_handles), K(level_new_pages), K(level_page_num_array_), K(root_item_), K(modify_origin_page_level));
//   } else if (OB_FAIL(rollback_prealloc_info_(origin_arr_index, origin_index_in_entry))) {
//     STORAGE_LOG(WARN, "fail to rollback prealloc_info_", KR(ret), K(fd_), K(origin_arr_index), K(origin_index_in_entry));
//   } else {
//     if (0 <= modify_origin_page_level) {
//       char *modify_page_buf = right_page_handles.at(modify_origin_page_level).get_page()->get_buffer();
//       if (OB_FAIL(remove_page_item_from_tail_(modify_page_buf, 1))) {
//         STORAGE_LOG(WARN, "fail to remove page item from tail", KR(ret), K(fd_), KP(modify_page_buf));
//       }
//     }
//     ARRAY_FOREACH_N(level_new_pages, level, level_cnt) {
//       if (level_new_pages.at(level).is_valid()) {
//         if (OB_FAIL(wbp_->free_page(level_new_pages.at(level)))) {
//           STORAGE_LOG(ERROR, "fail to free meta page in write cache", KR(ret), K(fd_), K(level_new_pages.at(level)));
//         } else {
//           if (level > root_item_.page_level_) {
//             if (OB_UNLIKELY(1 != level_page_num_array_.at(level)
//                             || level + 1 != level_page_num_array_.count())) {
//               ret = OB_ERR_UNEXPECTED;
//               STORAGE_LOG(ERROR, "unexpected level_page_num_array_", KR(ret), K(fd_), K(level_page_num_array_), K(level));
//             } else {
//               level_page_num_array_.pop_back();
//             }
//           } else {
//             if (OB_UNLIKELY(1 >= level_page_num_array_.at(level))) {
//               ret = OB_ERR_UNEXPECTED;
//               STORAGE_LOG(ERROR, "unexpected level_page_num_array_", KR(ret), K(fd_), K(level_page_num_array_), K(level));
//             } else {
//               level_page_num_array_.at(level)--;
//             }
//           }
//         }
//       }
//     }
//     if (OB_SUCC(ret)) {
//       if (root_item_.page_level_ + 1 < level_page_num_array_.count()) {
//         if (OB_UNLIKELY(0 != level_page_num_array_.at(level_page_num_array_.count() - 1))) {
//           ret = OB_ERR_UNEXPECTED;
//           STORAGE_LOG(ERROR, "unexpected level_page_num_array_", KR(ret), K(fd_), K(level_page_num_array_));
//         } else {
//           level_page_num_array_.pop_back();
//           if (OB_UNLIKELY(root_item_.page_level_ + 1 != level_page_num_array_.count())) {
//             ret = OB_ERR_UNEXPECTED;
//             STORAGE_LOG(ERROR, "unexpected root_item_ or level_page_num_array_", KR(ret), K(fd_), K(root_item_),
//                                                             K(level_page_num_array_));
//           }
//         }
//       }
//     }
//   }
//   STORAGE_LOG(INFO, "fail to insert, finish rollback to before", KR(ret), KPC(this));
//   return ret;
// }

int ObSharedNothingTmpFileMetaTree::search_data_items(
    const int64_t start_offset,
    const int64_t read_size,
    const int64_t timeout_ms,
    ObIArray<ObSharedNothingTmpFileDataItem> &data_items)
{
  int ret = OB_SUCCESS;
  data_items.reset();
  const int16_t FULL_PAGE_META_ITEM_NUM =
            MIN(MAX_PAGE_ITEM_COUNT, (ObTmpFileGlobal::PAGE_SIZE - PAGE_HEADER_SIZE) / sizeof(ObSharedNothingTmpFileMetaItem));
  const int64_t end_offset = start_offset + read_size;
  int64_t offset = start_offset;
  SpinRLockGuard guard(lock_);
  if (OB_UNLIKELY(start_offset < released_offset_
                  || read_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid argument", KR(ret), K(start_offset), K(read_size), KPC(this));
  } else if (!root_item_.is_valid()) {
    //read from data_item_array_
    if (OB_FAIL(search_data_items_from_array_(end_offset, offset, data_items))) {
      STORAGE_LOG(WARN, "fail to search data items from array", KR(ret), K(end_offset), K(offset), KPC(this));
    }
  } else {
    const int64_t target_virtual_page_id = start_offset / ObTmpFileGlobal::PAGE_SIZE;
    ObSharedNothingTmpFileMetaItem meta_item = root_item_;
    ObSharedNothingTmpFileMetaItem next_level_meta_item;
    //read from meta tree
    //root page index must be 0
    int32_t level_page_index = 0;
    //we use a array to simulate a stack
    ObSEArray<BacktraceNode, 2> search_path;
    while (OB_SUCC(ret)
          && 0 < meta_item.page_level_) {
      next_level_meta_item.reset();
      int16_t item_index = -1;
      ObSharedNothingTmpFileTreePageHeader page_header;
      char *page_buff = NULL;
      ObTmpFilePageHandle write_cache_page_handle;
      ObTmpPageValueHandle read_cache_page_handle;
      if (OB_FAIL(get_page_(meta_item, level_page_index, page_buff, write_cache_page_handle, read_cache_page_handle, timeout_ms))) {
        STORAGE_LOG(WARN, "fail to get page", KR(ret), K(meta_item), K(level_page_index), K(timeout_ms), KPC(this));
      } else if (OB_FAIL(read_item_(page_buff, target_virtual_page_id, item_index, next_level_meta_item))) {
        STORAGE_LOG(WARN, "fail to read item", KR(ret), KP(page_buff),
                                  K(target_virtual_page_id), K(meta_item), K(level_page_index), KPC(this));
      } else if (OB_FAIL(search_path.push_back(BacktraceNode(meta_item /*page info*/,
                                                            level_page_index, /*page index in level*/
                                                            item_index /*item index on the page*/)))) {
        STORAGE_LOG(WARN, "fail to push back", KR(ret), K(fd_), K(meta_item), K(level_page_index), K(item_index));
      } else {
        meta_item = next_level_meta_item;
        level_page_index = level_page_index * FULL_PAGE_META_ITEM_NUM + item_index;
      }
      write_cache_page_handle.reset();
      read_cache_page_handle.reset();
    }
    if (OB_SUCC(ret)) {
      if (0 == meta_item.page_level_) {
        if (OB_FAIL(get_items_of_leaf_page_(meta_item, level_page_index, end_offset,
                true /*need read from specified item index*/, offset, data_items, timeout_ms))) {
          STORAGE_LOG(WARN, "fail to get items of leaf page", KR(ret), K(meta_item),
                                K(level_page_index), K(end_offset), K(offset), K(timeout_ms), KPC(this));
        } else if (offset < end_offset
            && OB_FAIL(backtrace_search_data_items_(end_offset, timeout_ms, offset, search_path, data_items))) {
          STORAGE_LOG(WARN, "fail to backtrace search data items", KR(ret),
                                    K(end_offset), K(offset), K(search_path), K(timeout_ms), KPC(this));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "unexpected page type", KR(ret), K(meta_item), KPC(this));
      }
    }
  }
  return ret;
}

int ObSharedNothingTmpFileMetaTree::search_data_items_from_array_(
    const int64_t end_offset,
    int64_t &cur_offset,
    common::ObIArray<ObSharedNothingTmpFileDataItem> &data_items)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(cur_offset < 0
                  || cur_offset >= end_offset
                  || data_item_array_.empty()
                  || data_item_array_.count() > MAX_DATA_ITEM_ARRAY_COUNT)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid argument", KR(ret), K(fd_), K(cur_offset), K(end_offset), K(data_item_array_));
  } else {
    const int64_t target_virtual_page_id = cur_offset / ObTmpFileGlobal::PAGE_SIZE;
    int16_t index = -1;
    ARRAY_FOREACH_N(data_item_array_, i, cnt) {
      const ObSharedNothingTmpFileDataItem &data_item = data_item_array_.at(i);
      if (OB_UNLIKELY(!data_item.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "unexpected data_item", KR(ret), K(fd_), K(i), K(data_item));
      } else if (data_item.virtual_page_id_ <= target_virtual_page_id) {
        index = i;
      } else {
        break;
      }
    }
    if (OB_UNLIKELY(0 > index)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected index", KR(ret), K(fd_), K(index), K(cur_offset), K(data_item_array_));
    } else {
      for (int16_t i = index; OB_SUCC(ret) && i < data_item_array_.count() && cur_offset < end_offset; i++) {
        const ObSharedNothingTmpFileDataItem &data_item = data_item_array_.at(i);
        if (OB_UNLIKELY(i > index && cur_offset != data_item.virtual_page_id_ * ObTmpFileGlobal::PAGE_SIZE)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(ERROR, "unexpected virtual_page_id", KR(ret), K(fd_), K(cur_offset), K(i), K(index), K(data_item));
        } else if (OB_FAIL(data_items.push_back(data_item))) {
          STORAGE_LOG(WARN, "fail to push back", KR(ret), K(fd_), K(data_item));
        } else {
          cur_offset = (data_item.virtual_page_id_ + data_item.physical_page_num_) * ObTmpFileGlobal::PAGE_SIZE;
        }
      }
    }
    if (OB_SUCC(ret) && cur_offset < end_offset) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected offset", KR(ret), K(fd_), K(cur_offset), K(end_offset));
    }
  }
  return ret;
}

int ObSharedNothingTmpFileMetaTree::get_items_of_leaf_page_(
    const ObSharedNothingTmpFileMetaItem &page_info,
    const int32_t level_page_index,
    const int64_t end_offset,
    const bool need_find_index,
    int64_t &cur_offset,
    common::ObIArray<ObSharedNothingTmpFileDataItem> &data_items,
    const int64_t timeout_ms)
{
  int ret = OB_SUCCESS;
  char *page_buff = NULL;
  int16_t item_index = -1;
  int64_t tmp_offset = -1;
  const int64_t target_virtual_page_id = cur_offset / ObTmpFileGlobal::PAGE_SIZE;
  ObSharedNothingTmpFileDataItem data_item;
  ObSharedNothingTmpFileTreePageHeader page_header;
  ObTmpFilePageHandle write_cache_page_handle;
  ObTmpPageValueHandle read_cache_page_handle;
  if (OB_FAIL(get_page_(page_info, level_page_index, page_buff, write_cache_page_handle, read_cache_page_handle, timeout_ms))) {
    STORAGE_LOG(WARN, "fail to get page", KR(ret), K(fd_), K(page_info), K(level_page_index), K(timeout_ms));
  } else if (OB_FAIL(read_page_header_(page_buff, page_header))) {
    STORAGE_LOG(WARN, "fail to read page header", KR(ret), K(fd_), KP(page_buff));
  } else {
    if (!need_find_index) {
      if (OB_UNLIKELY(0 != cur_offset % ObTmpFileGlobal::PAGE_SIZE)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "unexpected cur_offset", KR(ret), K(fd_), K(cur_offset));
      } else {
        //read from beginning of the page
        item_index = 0;
      }
    } else {
      //get the specified item_index based on target_virtual_page_id
      if (OB_FAIL(read_item_(page_buff, target_virtual_page_id, item_index, data_item))) {
        STORAGE_LOG(WARN, "fail to read item", KR(ret), K(fd_), KP(page_buff), K(target_virtual_page_id));
      } else if (FALSE_IT(tmp_offset = (data_item.virtual_page_id_ + data_item.physical_page_num_) * ObTmpFileGlobal::PAGE_SIZE)) {
      } else if (cur_offset >= tmp_offset) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "unexpected offset", KR(ret), K(fd_), K(cur_offset), K(tmp_offset), K(data_item));
      } else if (OB_FAIL(data_items.push_back(data_item))) {
        STORAGE_LOG(WARN, "fail to push back", KR(ret), K(fd_), K(data_item));
      } else {
        cur_offset = tmp_offset;
        item_index++;
      }
    }
    if (OB_SUCC(ret)) {
      while (OB_SUCC(ret)
            && cur_offset < end_offset
            && item_index < page_header.item_num_) {
        data_item.reset();
        if (OB_FAIL(read_item_(page_buff, item_index, data_item))) {
          STORAGE_LOG(WARN, "fail to read item", KR(ret), K(fd_), KP(page_buff), K(item_index), K(page_info), K(level_page_index));
        } else if (OB_UNLIKELY(cur_offset != data_item.virtual_page_id_ * ObTmpFileGlobal::PAGE_SIZE)) {
          ret = OB_ERR_UNEXPECTED;
          //print page content
          //NOTE: control print frequence
          ObSharedNothingTmpFileTreePageHeader tmp_page_header;
          ObArray<ObSharedNothingTmpFileDataItem> items;
          read_page_content_(page_buff, tmp_page_header, items);
          STORAGE_LOG(ERROR, "unexpected virtual_page_id, dump tree page", KR(ret), K(fd_), KP(page_buff), K(cur_offset),
                                  K(item_index), K(data_item), K(target_virtual_page_id), K(tmp_page_header), K(items));
        } else if (OB_FAIL(data_items.push_back(data_item))) {
          STORAGE_LOG(WARN, "fail to push back", KR(ret), K(fd_), K(data_item));
        } else {
          cur_offset = (data_item.virtual_page_id_ + data_item.physical_page_num_) * ObTmpFileGlobal::PAGE_SIZE;
          item_index++;
        }
      }
    }
    if (OB_FAIL(ret)) {
      //print page content
      ObSharedNothingTmpFileTreePageHeader tmp_page_header;
      ObSharedNothingTmpFileDataItem first_item;
      ObSharedNothingTmpFileDataItem last_item;
      read_page_simple_content_(page_buff, page_header, first_item, last_item);
      STORAGE_LOG(WARN, "dump tree page", KR(ret), K(fd_), K(page_info), K(level_page_index),
                                KP(page_buff), K(tmp_page_header), K(first_item), K(last_item));
    }
  }
  write_cache_page_handle.reset();
  read_cache_page_handle.reset();
  return ret;
}

int ObSharedNothingTmpFileMetaTree::backtrace_search_data_items_(
    const int64_t end_offset,
    const int64_t timeout_ms,
    int64_t &offset,
    ObIArray<BacktraceNode> &search_path,
    ObIArray<ObSharedNothingTmpFileDataItem> &data_items)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(search_path.empty()
                  || 1 != search_path.at(search_path.count() - 1).page_meta_info_.page_level_
                  || data_items.empty()
                  || offset >= end_offset)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid argument", KR(ret), K(fd_), K(search_path), K(data_items),
                                                    K(end_offset), K(offset));
  } else {
    const int16_t FULL_PAGE_META_ITEM_NUM =
            MIN(MAX_PAGE_ITEM_COUNT, (ObTmpFileGlobal::PAGE_SIZE - PAGE_HEADER_SIZE) / sizeof(ObSharedNothingTmpFileMetaItem));
    ObSEArray<ObSharedNothingTmpFileMetaItem, 4> meta_items;
    while (OB_SUCC(ret)
          && !search_path.empty()
          && offset < end_offset) {
      meta_items.reset();
      bool reach_last_item = false;
      int64_t last_node_index = search_path.count() - 1;
      //meta_item points to a page
      const ObSharedNothingTmpFileMetaItem &page_info = search_path.at(last_node_index).page_meta_info_;
      //the page index in its level
      const int32_t level_page_index = search_path.at(last_node_index).level_page_index_;
      //current pos need to be processed on this page.
      int16_t cur_item_index = search_path.at(last_node_index).prev_item_index_ + 1;
      if (OB_FAIL(get_items_of_internal_page_(page_info, level_page_index, cur_item_index,
                                      end_offset, reach_last_item, meta_items, timeout_ms))) {
        STORAGE_LOG(WARN, "fail to get items of internal page", KR(ret), K(fd_), K(page_info),
                      K(level_page_index), K(cur_item_index), K(end_offset), K(timeout_ms));
      } else if (1 == page_info.page_level_) {
        //process level_0
        int64_t i = 0;
        const int64_t cnt = meta_items.count();
        const int32_t start_leaf_level_page_index = level_page_index * FULL_PAGE_META_ITEM_NUM + cur_item_index;
        for (; OB_SUCC(ret) && i < cnt && offset < end_offset; i++) {
          const ObSharedNothingTmpFileMetaItem &leaf_page_info = meta_items.at(i);
          bool need_lock = ((i + 1 == cnt) && reach_last_item);
          if (OB_FAIL(get_items_of_leaf_page_(leaf_page_info, start_leaf_level_page_index + i, end_offset,
                          false /*need read from specified item index*/, offset, data_items, timeout_ms))) {
            STORAGE_LOG(WARN, "fail to get items of leaf page", KR(ret), K(fd_), K(leaf_page_info),
                  K(start_leaf_level_page_index + i), K(end_offset), K(need_lock), K(offset), K(timeout_ms));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_UNLIKELY(cnt != i || (!reach_last_item && offset < end_offset))) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(ERROR, "unexpected meta_items", KR(ret), K(fd_), K(meta_items), K(i),
                                              K(reach_last_item), K(offset), K(end_offset));
          } else {
            search_path.pop_back();
          }
        }
      } else if (page_info.page_level_ > 1) {
        if (meta_items.count() == 1) {
          const int32_t child_level_page_index = level_page_index * FULL_PAGE_META_ITEM_NUM + cur_item_index;
          search_path.at(last_node_index).prev_item_index_ = cur_item_index;
          if (OB_FAIL(search_path.push_back(BacktraceNode(meta_items.at(0), child_level_page_index, -1)))) {
            STORAGE_LOG(WARN, "fail to push back", KR(ret), K(fd_), K(meta_items.at(0)), K(child_level_page_index));
          }
        } else if (meta_items.empty()) {
          search_path.pop_back();
        } else {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(ERROR, "unexpected meta_items", KR(ret), K(fd_), K(meta_items));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "unexpected page info", KR(ret), K(fd_), K(page_info));
      }
    }
    if (OB_SUCC(ret) && end_offset > offset) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected offset", KR(ret), K(fd_), K(end_offset), K(offset));
    }
  }
  return ret;
}

int ObSharedNothingTmpFileMetaTree::get_items_of_internal_page_(
    const ObSharedNothingTmpFileMetaItem &page_info,
    const int32_t level_page_index,
    const int16_t item_index,
    const int64_t end_offset,
    bool &reach_last_item,
    ObIArray<ObSharedNothingTmpFileMetaItem> &meta_items,
    const int64_t timeout_ms)
{
  int ret = OB_SUCCESS;
  reach_last_item = false;
  meta_items.reset();
  int16_t cur_item_index = item_index;
  ObSharedNothingTmpFileTreePageHeader page_header;
  ObSharedNothingTmpFileMetaItem meta_item;
  char *page_buff = NULL;
  ObTmpFilePageHandle write_cache_page_handle;
  ObTmpPageValueHandle read_cache_page_handle;
  //when truncate, the page must in read/write cache(because we already hold a p_handle about this page if in read cache)
  if (OB_FAIL(get_page_(page_info, level_page_index, page_buff, write_cache_page_handle, read_cache_page_handle, timeout_ms))) {
    STORAGE_LOG(WARN, "fail to get page", KR(ret), K(fd_), K(page_info), K(level_page_index), K(timeout_ms));
  } else if (OB_FAIL(read_page_header_(page_buff, page_header))) {
    STORAGE_LOG(WARN, "fail to read page header", KR(ret), K(fd_), KP(page_buff));
  } else if (OB_UNLIKELY(0 >= page_header.item_num_
                        || item_index > page_header.item_num_
                        || item_index < 0)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "unexpected page_header", KR(ret), K(fd_), K(page_header), K(item_index));
  } else {
    int64_t end_virtual_page_id = upper_align(end_offset, ObTmpFileGlobal::PAGE_SIZE) / ObTmpFileGlobal::PAGE_SIZE;
    while (OB_SUCC(ret)
          && cur_item_index < page_header.item_num_) {
      meta_item.reset();
      if (OB_FAIL(read_item_(page_buff, cur_item_index, meta_item))) {
        STORAGE_LOG(WARN, "fail to read item", KR(ret), K(fd_), KP(page_buff), K(cur_item_index));
      } else if (meta_item.virtual_page_id_ >= end_virtual_page_id) {
        break;
      } else if (OB_FAIL(meta_items.push_back(meta_item))) {
        STORAGE_LOG(WARN, "fail to push back", KR(ret), K(fd_), K(meta_item));
      } else if (FALSE_IT(cur_item_index++)) {
      } else if (1 != page_info.page_level_) {
        break;
      }
    }
  }
  write_cache_page_handle.reset();
  read_cache_page_handle.reset();
  if (OB_SUCC(ret) && cur_item_index == page_header.item_num_) {
    reach_last_item = true;
  }
  return ret;
}

int ObSharedNothingTmpFileMetaTree::calc_and_set_page_checksum_(char* page_buff)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(page_buff)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid argument", KR(ret), K(fd_), KP(page_buff));
  } else {
    ObSharedNothingTmpFileTreePageHeader page_header = *((ObSharedNothingTmpFileTreePageHeader *)(page_buff));
    page_header.checksum_ = ob_crc64(page_buff + PAGE_HEADER_SIZE, ObTmpFileGlobal::PAGE_SIZE - PAGE_HEADER_SIZE);
    MEMCPY(page_buff, &page_header, PAGE_HEADER_SIZE);
  }
  return ret;
}

int ObSharedNothingTmpFileMetaTree::get_last_data_item_write_tail(
    const int64_t timeout_ms,
    ObSharedNothingTmpFileDataItem &last_data_item)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (!root_item_.is_valid()) {
    if (OB_UNLIKELY(data_item_array_.empty())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected data_item_array_", KR(ret), KPC(this));
    } else {
      last_data_item = data_item_array_.at(data_item_array_.count() - 1);
      if (OB_UNLIKELY(0 >= last_data_item.physical_page_num_)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "unexpected physical_page_num", KR(ret), K(last_data_item), KPC(this));
      }
    }
  } else {
    ObSharedNothingTmpFileTreePageHeader page_header;
    ObTmpFilePageHandle leaf_page_right_page_handle;
    ObSharedNothingTmpFileMetaItem page_info;
    bool is_leaf_right_page_full = false;
    int16_t page_remain_cnt = 0;
    if (OB_FAIL(fast_get_rightmost_leaf_page_for_write_(leaf_page_right_page_handle, page_remain_cnt))) {
      STORAGE_LOG(WARN, "fail to fast get rightmost leaf page for write", KR(ret), KPC(this));
    } else if (!leaf_page_right_page_handle.is_valid() &&
                OB_FAIL(get_rightmost_leaf_page_for_write_(timeout_ms, page_info, leaf_page_right_page_handle))) {
      STORAGE_LOG(WARN, "fail to get rightmost leaf page for write", KR(ret), K(timeout_ms), KPC(this));
    } else if (OB_UNLIKELY(!leaf_page_right_page_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected leaf_page_right_page_handle", KR(ret), K(leaf_page_right_page_handle), KPC(this));
    } else {
      //we don't need to worry about the rightmost leaf page being evicted,
      // because we set is_writing_ = true.
      char *leaf_page_buff = leaf_page_right_page_handle.get_page()->get_buffer();
      if (OB_FAIL(read_page_header_(leaf_page_buff, page_header))) {
        STORAGE_LOG(WARN, "fail to read page header", KR(ret), KP(leaf_page_buff), KPC(this));
      } else if (OB_UNLIKELY(0 >= page_header.item_num_)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "unexpected item_num", KR(ret), KP(leaf_page_buff), K(page_header), KPC(this));
      } else if (OB_FAIL(read_item_(leaf_page_buff, page_header.item_num_ - 1, last_data_item))) {
        STORAGE_LOG(WARN, "fail to read item", KR(ret), KP(leaf_page_buff), K(page_header), KPC(this));
      } else if (OB_UNLIKELY(0 >= last_data_item.physical_page_num_)) {
        ret = OB_ERR_UNEXPECTED;
        //print page content
        //NOTE: control print frequence
        ObSharedNothingTmpFileTreePageHeader tmp_page_header;
        ObArray<ObSharedNothingTmpFileDataItem> items;
        read_page_content_(leaf_page_buff, tmp_page_header, items);
        STORAGE_LOG(ERROR, "unexpected physical_page_num, dump tree page", KR(ret), K(last_data_item), KP(leaf_page_buff),
                                      K(page_info), K(level_page_num_array_), K(tmp_page_header), K(items), KPC(this));
      }
    }
  }
  return ret;
}

int ObSharedNothingTmpFileMetaTree::clear(
    const int64_t last_truncate_offset,
    const int64_t total_file_size)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  int64_t end_truncate_offset = upper_align(total_file_size, ObTmpFileGlobal::PAGE_SIZE);

  if (OB_UNLIKELY(last_truncate_offset < released_offset_
                  || last_truncate_offset > total_file_size)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid argument", KR(ret), K(last_truncate_offset), K(total_file_size), KPC(this));
  } else {
    while (true) {
      if (OB_FAIL(truncate_(end_truncate_offset))) {
        STORAGE_LOG(WARN, "fail to truncate_", KR(ret), K(last_truncate_offset), K(end_truncate_offset), K(total_file_size), KPC(this));
      } else {
        break;
      }
      ob_usleep(100 * 1000); // 100ms
      ret = OB_SUCCESS;
    }
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(root_item_.is_valid()
                            || !level_page_num_array_.empty()
                            || !data_item_array_.empty())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected root_item_ or array_", KR(ret), KPC(this));
    } else if (OB_FAIL(release_prealloc_data_())) {
      STORAGE_LOG(WARN, "fail to release prealloc data", KR(ret), KPC(this));
    } else if (OB_UNLIKELY(stat_info_.all_type_page_occupy_disk_cnt_ != stat_info_.all_type_page_released_cnt_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected stat_info_", KR(ret), K(stat_info_), KPC(this));
    }
  }
  return ret;
}

//clear leaf page or internal page in meta tree
int ObSharedNothingTmpFileMetaTree::release_meta_page_(
    const ObSharedNothingTmpFileMetaItem &page_info,
    const int32_t page_index_in_level)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(level_page_num_array_.count() <= page_info.page_level_
                  || 0 > page_index_in_level)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "unexpected level_page_num_array_ or page_info", KR(ret), K(fd_), K(level_page_num_array_),
                                                                          K(page_info), K(page_index_in_level));
  } else {
    ObTmpFileWriteCacheKey cache_key(fd_, page_info.page_level_, page_index_in_level);
    ObTmpFilePageHandle page_handle;
    if (OB_FAIL(wbp_->get_page(cache_key, page_handle))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        STORAGE_LOG(WARN, "fail to get page in write cache", KR(ret), K(fd_), K(cache_key));
      } else {
        STORAGE_LOG(INFO, "page not exist in write cache", KR(ret), K(fd_), K(cache_key));
        ret = OB_SUCCESS;
      }
    } else if (OB_FAIL(wbp_->free_page(cache_key))) {
      STORAGE_LOG(ERROR, "fail to free meta page in write cache", KR(ret), K(fd_), K(page_info), K(cache_key));
    }
  }
  if (FAILEDx(release_tmp_file_page_(page_info.block_index_, page_info.physical_page_id_, 1))) {
    STORAGE_LOG(WARN, "fail to release tmp file page", KR(ret), K(fd_), K(page_info));
  } else {
    stat_info_.this_epoch_meta_page_released_cnt_++;
  }
  return ret;
}

int ObSharedNothingTmpFileMetaTree::release_tmp_file_page_(
    const int64_t block_index, const int64_t begin_page_id, const int64_t page_num)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(block_manager_->release_page(block_index, begin_page_id, page_num))) {
    STORAGE_LOG(ERROR, "fail to release tmp file page",
        KR(ret), K(fd_), K(block_index), K(begin_page_id), K(page_num));
  } else {
    stat_info_.all_type_page_released_cnt_ += page_num;
  }

  return ret;
}

int ObSharedNothingTmpFileMetaTree::truncate(
    const int64_t last_truncate_offset,
    const int64_t end_truncate_offset)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  STORAGE_LOG(DEBUG, "begin truncate array or meta tree", KR(ret), K(fd_), K(released_offset_), K(last_truncate_offset), K(end_truncate_offset));
  if (OB_UNLIKELY(last_truncate_offset < released_offset_
                  || 0 != released_offset_ % ObTmpFileGlobal::PAGE_SIZE
                  || last_truncate_offset > end_truncate_offset)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid argument", KR(ret), K(last_truncate_offset), K(end_truncate_offset), KPC(this));
  } else {
    while (true) {
      if (OB_FAIL(truncate_(end_truncate_offset))) {
        STORAGE_LOG(WARN, "fail to truncate_", KR(ret), K(last_truncate_offset), K(end_truncate_offset), KPC(this));
      } else {
        break;
      }
      ob_usleep(100 * 1000); // 100ms
      ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret)) {
      if (!root_item_.is_valid()) {
        //NOTE: end_truncate_offset >= max offset in tree.
        // released_offset_ = end_truncate_offset;
      }
    }
  }

  STORAGE_LOG(INFO, "finish truncate array or meta tree", KR(ret), K(fd_), K(released_offset_), K(last_truncate_offset), K(end_truncate_offset));
  return ret;
}

int ObSharedNothingTmpFileMetaTree::truncate_(
    const int64_t end_truncate_offset)
{
  int ret = OB_SUCCESS;
  if (!root_item_.is_valid()) {
    if (OB_FAIL(truncate_array_(end_truncate_offset))) {
      STORAGE_LOG(WARN, "fail to truncate array", KR(ret), K(fd_), K(end_truncate_offset));
    }
  } else {
    const int16_t FULL_PAGE_META_ITEM_NUM =
            MIN(MAX_PAGE_ITEM_COUNT, (ObTmpFileGlobal::PAGE_SIZE - PAGE_HEADER_SIZE) / sizeof(ObSharedNothingTmpFileMetaItem));
    ObSEArray<std::pair<int32_t, int16_t>, 4> item_index_arr;
    if (OB_FAIL(calculate_truncate_index_path_(item_index_arr))) {
      STORAGE_LOG(WARN, "fail to calculate truncate index path", KR(ret), K(fd_), K(item_index_arr));
    } else if (OB_UNLIKELY(item_index_arr.empty())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected item_index_arr", KR(ret), K(fd_), K(item_index_arr));
    } else {
      ObSharedNothingTmpFileMetaItem meta_item = root_item_;
      ObSharedNothingTmpFileMetaItem next_level_meta_item;
      ObSEArray<BacktraceNode, 2> truncate_path;
      ObSEArray<ObTmpFilePageHandle, 2> write_cache_p_handles;
      ObSEArray<ObTmpPageValueHandle, 2> read_cache_p_handles;
      while (OB_SUCC(ret)
            && 0 < meta_item.page_level_) {
        next_level_meta_item.reset();
        int16_t item_index = -1;
        int32_t level_page_index = -1;
        ObSharedNothingTmpFileTreePageHeader page_header;
        char *page_buff = NULL;
        ObTmpFilePageHandle write_cache_p_handle;
        ObTmpPageValueHandle read_cache_p_handle;
        if (OB_UNLIKELY(item_index_arr.count() <= meta_item.page_level_)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(ERROR, "unexpected item_index_arr", KR(ret), K(fd_), K(item_index_arr), K(meta_item));
        } else if (FALSE_IT(level_page_index = item_index_arr.at(meta_item.page_level_).first)) {
        } else if (FALSE_IT(item_index = item_index_arr.at(meta_item.page_level_).second)) {
        } else if (OB_FAIL(get_page_(meta_item, level_page_index, page_buff, write_cache_p_handle, read_cache_p_handle))) {
          STORAGE_LOG(WARN, "fail to get page", KR(ret), K(fd_), K(meta_item));
        } else if (OB_FAIL(write_cache_p_handles.push_back(write_cache_p_handle))) {
          STORAGE_LOG(WARN, "fail to push back", KR(ret), K(fd_), K(write_cache_p_handle));
        } else if (OB_FAIL(read_cache_p_handles.push_back(read_cache_p_handle))) {
          STORAGE_LOG(WARN, "fail to push back", KR(ret), K(fd_), K(read_cache_p_handle));
        } else if (OB_FAIL(read_item_(page_buff, item_index, next_level_meta_item))) {
          STORAGE_LOG(WARN, "fail to read item", KR(ret), K(fd_), KP(page_buff), K(item_index));
        } else if (OB_FAIL(truncate_path.push_back(BacktraceNode(meta_item /*page info*/,
                                                                level_page_index, /*page index in its level*/
                                                                item_index /*item index on the page*/)))) {
          STORAGE_LOG(WARN, "fail to push back", KR(ret), K(fd_), K(meta_item));
        } else {
          meta_item = next_level_meta_item;
        }
        if (OB_FAIL(ret)) {
          write_cache_p_handle.reset();
          read_cache_p_handle.reset();
        }
      }
      if (OB_SUCC(ret)) {
        bool release_last_item = false;
        if (0 == meta_item.page_level_) {
          ObArray<ObSharedNothingTmpFileMetaItem> reserved_meta_items(8 * 1024); //block_size = 8K
          if (item_index_arr.count() > 1 && OB_FAIL(reserved_meta_items.reserve(FULL_PAGE_META_ITEM_NUM))) {
            STORAGE_LOG(WARN, "fail to reserve", KR(ret), K(fd_));
          } else if (OB_FAIL(release_items_of_leaf_page_(meta_item, item_index_arr.at(0).first, end_truncate_offset,
                                                    item_index_arr.at(0).second, release_last_item))) {
            STORAGE_LOG(WARN, "fail to release items of leaf page", KR(ret), K(fd_), K(meta_item),
                                                      K(end_truncate_offset), K(item_index_arr));
          } else if (release_last_item) {
            if (OB_FAIL(release_meta_page_(meta_item, item_index_arr.at(0).first))) {
              STORAGE_LOG(WARN, "fail to release meta page", KR(ret), K(fd_), K(meta_item), K(item_index_arr.at(0).first));
            //even release_offset == end_offset,We won't end here, because the upper-level pages may need to be released
            } else if (OB_FAIL(backtrace_truncate_tree_(end_truncate_offset, truncate_path, reserved_meta_items,
                                                                        write_cache_p_handles, read_cache_p_handles))) {
              STORAGE_LOG(WARN, "fail to backtrace truncate tree", KR(ret), K(fd_), K(end_truncate_offset), K(truncate_path),
                                                K(reserved_meta_items), K(write_cache_p_handles), K(read_cache_p_handles));
            }
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(ERROR, "unexpected page type", KR(ret), K(fd_), K(meta_item));
        }
      }
      for (int16_t i = 0; i < write_cache_p_handles.count(); i++) {
        write_cache_p_handles.at(i).reset();
      }
      for (int16_t i = 0; i < read_cache_p_handles.count(); i++) {
        read_cache_p_handles.at(i).reset();
      }
    }
  }
  STORAGE_LOG(DEBUG, "truncate_ meta tree end", K(end_truncate_offset));
  return ret;
}

int ObSharedNothingTmpFileMetaTree::truncate_array_(
    const int64_t end_offset)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(end_offset < 0
                  || data_item_array_.count() > MAX_DATA_ITEM_ARRAY_COUNT)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid argument", KR(ret), K(fd_), K(end_offset), K(data_item_array_));
  } else if (data_item_array_.empty()) {
    STORAGE_LOG(INFO, "no data to truncate", KR(ret), K(fd_), K(data_item_array_), K(root_item_));
  } else {
    const ObSharedNothingTmpFileDataItem &last_item = data_item_array_.at(data_item_array_.count() - 1);
    if (OB_UNLIKELY(!last_item.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected last item", KR(ret), K(fd_), K(last_item));
    } else if (end_offset >= (last_item.virtual_page_id_ + last_item.physical_page_num_) * ObTmpFileGlobal::PAGE_SIZE) {
      //clear all items
      ARRAY_FOREACH_N(data_item_array_, i, cnt) {
        const ObSharedNothingTmpFileDataItem &item = data_item_array_.at(i);
        if (OB_FAIL(release_tmp_file_page_(item.block_index_,
                                          item.physical_page_id_, item.physical_page_num_))) {
          STORAGE_LOG(WARN, "fail to release tmp file page", KR(ret), K(fd_), K(item), K(i), K(cnt));
        } else if (i + 1 == cnt) {
          released_offset_ = (item.virtual_page_id_ + item.physical_page_num_) * ObTmpFileGlobal::PAGE_SIZE;
        }
      }
      if (OB_SUCC(ret)) {
        data_item_array_.reset();
      }
    } else {
      //clear some items
      const int64_t target_virtual_page_id = end_offset / ObTmpFileGlobal::PAGE_SIZE;
      int16_t index = 0;
      ARRAY_FOREACH_N(data_item_array_, i, cnt) {
        const ObSharedNothingTmpFileDataItem &data_item = data_item_array_.at(i);
        if (OB_UNLIKELY(!data_item.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(ERROR, "unexpected data item", KR(ret), K(fd_), K(data_item));
        } else if (data_item.virtual_page_id_ <= target_virtual_page_id) {
          index = i;
        } else {
          break;
        }
      }
      if (OB_SUCC(ret)) {
        if (0 == index) {
          //do nothing
        } else {
          //release items before "index"
          int16_t array_cnt = data_item_array_.count();
          for (int16_t i = 0; OB_SUCC(ret) && i < index; i++) {
            ObSharedNothingTmpFileDataItem &data_item = data_item_array_.at(i);
            if (OB_UNLIKELY(!data_item.is_valid())) {
              ret = OB_ERR_UNEXPECTED;
              STORAGE_LOG(ERROR, "unexpected data item", KR(ret), K(fd_), K(data_item));
            } else if (OB_FAIL(release_tmp_file_page_(data_item.block_index_,
                                              data_item.physical_page_id_,
                                              data_item.physical_page_num_))) {
              STORAGE_LOG(WARN, "fail to release tmp file page", KR(ret), K(fd_), K(data_item), K(i), K(index));
            } else if (i + 1 == index) {
              released_offset_ = (data_item.virtual_page_id_ + data_item.physical_page_num_) * ObTmpFileGlobal::PAGE_SIZE;
            } else {
              data_item.reset();
            }
          }
          if (OB_SUCC(ret)) {
            //move items
            for (int16_t i = index; i < array_cnt; i++) {
              data_item_array_.at(i - index) = data_item_array_.at(i);
            }
            for (int16_t i = 0; i < index; i++) {
              data_item_array_.pop_back();
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObSharedNothingTmpFileMetaTree::calculate_truncate_index_path_(
    ObIArray<std::pair<int32_t, int16_t>> &item_index_arr)
{
  int ret = OB_SUCCESS;
  item_index_arr.reset();
  if (OB_UNLIKELY(!root_item_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "unexpected root_item_", KR(ret), K(fd_), K(root_item_));
  } else {
    const int16_t FULL_PAGE_META_ITEM_NUM =
              MIN(MAX_PAGE_ITEM_COUNT, (ObTmpFileGlobal::PAGE_SIZE - PAGE_HEADER_SIZE) / sizeof(ObSharedNothingTmpFileMetaItem));
    int32_t child_level_page_index = -1;
    ARRAY_FOREACH_N(level_page_num_array_, i, cnt) {
      int16_t item_index = -1;
      if (!last_truncate_leaf_info_.is_valid()) {
        child_level_page_index = 0;
        item_index = 0;
      } else {
        int32_t level_page_index = -1;
        int32_t level_page_num = level_page_num_array_.at(i);
        if (0 == i) {
          level_page_index = last_truncate_leaf_info_.page_index_in_leaf_level_;
          item_index = last_truncate_leaf_info_.item_index_in_page_ + 1;
          if (last_truncate_leaf_info_.release_to_end_in_page_) {
            level_page_index++;
            item_index = 0;
          }
        } else { //internal level
          item_index = child_level_page_index % FULL_PAGE_META_ITEM_NUM;
          level_page_index = child_level_page_index / FULL_PAGE_META_ITEM_NUM;
        }
        if (level_page_index >= level_page_num) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(ERROR, "no data to truncate but root_item_ is valid", KR(ret), K(fd_),
                              K(i), K(level_page_index), K(level_page_num), K(level_page_num_array_));
        } else {
          child_level_page_index = level_page_index;
        }
      }
      if (FAILEDx(item_index_arr.push_back(std::make_pair(child_level_page_index, item_index)))) {
        STORAGE_LOG(WARN, "fail to push back", KR(ret), K(fd_), K(child_level_page_index), K(item_index));
      }
    }
  }
  return ret;
}

int ObSharedNothingTmpFileMetaTree::release_items_of_leaf_page_(
    const ObSharedNothingTmpFileMetaItem &page_info,
    const int32_t level_page_index,
    const int64_t end_offset,
    const int16_t begin_release_index,
    bool &release_last_item)
{
  int ret = OB_SUCCESS;
  release_last_item = false;
  char *page_buff = NULL;
  int16_t item_index = -1;
  int16_t end_release_index = -1;
  int64_t tmp_release_offset = released_offset_;
  ObSharedNothingTmpFileDataItem data_item;
  ObSharedNothingTmpFileTreePageHeader page_header;
  ObTmpFilePageHandle write_cache_page_handle;
  ObTmpPageValueHandle read_cache_page_handle;
  if (OB_UNLIKELY(0 != tmp_release_offset % ObTmpFileGlobal::PAGE_SIZE
        || 0 > begin_release_index
        || PAGE_HEADER_SIZE + (begin_release_index + 1) * sizeof(ObSharedNothingTmpFileDataItem) > ObTmpFileGlobal::PAGE_SIZE
        || begin_release_index >= MAX_PAGE_ITEM_COUNT)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "unexpected release_offset or begin_release_index", KR(ret), K(fd_), K(tmp_release_offset), K(begin_release_index));
  } else if (OB_FAIL(get_page_(page_info, level_page_index, page_buff, write_cache_page_handle, read_cache_page_handle))) {
    STORAGE_LOG(WARN, "fail to get page", KR(ret), K(fd_), K(page_info), K(level_page_index));
  } else if (OB_FAIL(read_page_header_(page_buff, page_header))) {
    STORAGE_LOG(WARN, "fail to read page header", KR(ret), K(fd_), KP(page_buff));
  } else if (OB_UNLIKELY(0 == page_header.item_num_ || begin_release_index == page_header.item_num_)) {
    //the item must not be cleared (corresponds to an unfilled data page)
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "unexpected page_header or begin_release_index", KR(ret), K(fd_), K(page_header), K(begin_release_index));
  } else {
    //get end_release_index
    int64_t target_virtual_page_id = end_offset / ObTmpFileGlobal::PAGE_SIZE;
    item_index = -1;
    data_item.reset();
    if (OB_FAIL(read_item_(page_buff, target_virtual_page_id, item_index, data_item))) {
      STORAGE_LOG(WARN, "fail to read item", KR(ret), K(fd_), KP(page_buff), K(target_virtual_page_id));
    } else if (OB_UNLIKELY(!data_item.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected data item", KR(ret), K(fd_), K(data_item));
    } else if (target_virtual_page_id >= data_item.virtual_page_id_ + data_item.physical_page_num_) {
      if (OB_UNLIKELY(item_index + 1 != page_header.item_num_)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "unexpected item_index", KR(ret), K(fd_), K(item_index));
      } else {
        end_release_index = item_index;
        release_last_item = true;
      }
    } else {
      end_release_index = item_index - 1;
    }
    if (OB_SUCC(ret)) {
      item_index = begin_release_index;
      while (OB_SUCC(ret)
            && item_index <= end_release_index) {
        data_item.reset();
        if (OB_FAIL(read_item_(page_buff, item_index, data_item))) {
          STORAGE_LOG(WARN, "fail to read item", KR(ret), K(fd_), KP(page_buff), K(item_index));
        } else if (OB_UNLIKELY(!data_item.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(ERROR, "unexpected data item", KR(ret), K(fd_), K(data_item));
        } else if (OB_FAIL(release_tmp_file_page_(data_item.block_index_,
                                                  data_item.physical_page_id_,
                                                  data_item.physical_page_num_))) {
          STORAGE_LOG(WARN, "fail to release tmp file page", KR(ret), K(fd_), K(data_item),
                                    K(begin_release_index), K(item_index), K(end_release_index));
        } else {
          item_index++;
        }
      }
    }
    if (OB_SUCC(ret) && begin_release_index <= end_release_index) {
      tmp_release_offset = (data_item.virtual_page_id_ + data_item.physical_page_num_) * ObTmpFileGlobal::PAGE_SIZE;
      if (OB_UNLIKELY(tmp_release_offset > end_offset
                      || tmp_release_offset <= released_offset_)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "unexpected release_offset", KR(ret), K(fd_), K(tmp_release_offset), K(end_offset), K(released_offset_));
      } else {
        released_offset_ = tmp_release_offset;
        last_truncate_leaf_info_.page_index_in_leaf_level_ = level_page_index;
        last_truncate_leaf_info_.item_index_in_page_ = end_release_index;
        last_truncate_leaf_info_.release_to_end_in_page_ = release_last_item;
      }
    }
    if (OB_FAIL(ret)) {
      //print page content
      //NOTE: control print frequence
      ObSharedNothingTmpFileTreePageHeader tmp_page_header;
      ObArray<ObSharedNothingTmpFileDataItem> items;
      read_page_content_(page_buff, tmp_page_header, items);
      STORAGE_LOG(WARN, "fail to release leaf page items, dump tree page", KR(ret), K(fd_), KP(page_buff), K(page_info),
              K(level_page_index), K(begin_release_index), K(end_release_index), K(end_offset), K(tmp_page_header), K(items));
    }
  }
  write_cache_page_handle.reset();
  read_cache_page_handle.reset();
  return ret;
}

int ObSharedNothingTmpFileMetaTree::backtrace_truncate_tree_(
    const int64_t end_offset,
    ObIArray<BacktraceNode> &search_path,
    ObIArray<ObSharedNothingTmpFileMetaItem> &meta_items,
    ObSEArray<ObTmpFilePageHandle, 2> &write_cache_p_handles,
    ObSEArray<ObTmpPageValueHandle, 2> &read_cache_p_handles)
{
  int ret = OB_SUCCESS;
  const int16_t FULL_PAGE_META_ITEM_NUM =
            MIN(MAX_PAGE_ITEM_COUNT, (ObTmpFileGlobal::PAGE_SIZE - PAGE_HEADER_SIZE) / sizeof(ObSharedNothingTmpFileMetaItem));
  bool need_finish = false;
  while (OB_SUCC(ret)
        && !search_path.empty()
        && !need_finish) {
    meta_items.reuse();
    bool reach_last_item = false;
    int64_t last_node_index = search_path.count() - 1;
    //meta_item points to a page
    const ObSharedNothingTmpFileMetaItem &page_info = search_path.at(last_node_index).page_meta_info_;
    //the page index in its level
    const int32_t level_page_index = search_path.at(last_node_index).level_page_index_;
    //current pos need to be processed on this page.
    int16_t cur_item_index = search_path.at(last_node_index).prev_item_index_ + 1;
    if (OB_UNLIKELY(write_cache_p_handles.count() != search_path.count()
                    || read_cache_p_handles.count() != search_path.count())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected p_handles or search_path", KR(ret), K(fd_),
                          K(write_cache_p_handles), K(read_cache_p_handles), K(search_path));
    //must not fail
    } else if (OB_FAIL(get_items_of_internal_page_(page_info, level_page_index, cur_item_index,
                                                end_offset, reach_last_item, meta_items))) {
      STORAGE_LOG(ERROR, "fail to get items of internal page", KR(ret), K(fd_), K(page_info),
                                  K(level_page_index), K(cur_item_index), K(end_offset));
    } else if (1 == page_info.page_level_) {
      int16_t release_page_cnt = 0;
      const int32_t start_leaf_level_page_index = level_page_index * FULL_PAGE_META_ITEM_NUM + cur_item_index;
      ARRAY_FOREACH_X(meta_items, i, cnt, OB_SUCC(ret) && !need_finish) {
        bool release_last_item = false;
        if (OB_FAIL(release_items_of_leaf_page_(meta_items.at(i), start_leaf_level_page_index + i, end_offset,
                                                  0 /*release from start of the page*/, release_last_item))) {
          STORAGE_LOG(WARN, "fail to release items of leaf page", KR(ret), K(fd_), K(meta_items.at(i)),
                                                      K(start_leaf_level_page_index + i), K(end_offset));
        } else if (release_last_item) {
          if (OB_FAIL(release_meta_page_(meta_items.at(i), start_leaf_level_page_index + i))) {
            STORAGE_LOG(WARN, "fail to release meta page", KR(ret), K(fd_), K(meta_items.at(i)),
                                                  K(start_leaf_level_page_index + i));
          } else {
            release_page_cnt++;
          }
        } else {
          need_finish = true;
        }
      }
      if (OB_SUCC(ret)) {
        if (!reach_last_item) {
          need_finish = true;
        } else if (reach_last_item && (meta_items.empty() || release_page_cnt == meta_items.count())) {
          if (OB_FAIL(release_meta_page_(page_info, level_page_index))) {
            STORAGE_LOG(WARN, "fail to release meta page", KR(ret), K(fd_), K(page_info), K(level_page_index));
          } else {
            search_path.pop_back();
            write_cache_p_handles.at(last_node_index).reset();
            write_cache_p_handles.pop_back();
            read_cache_p_handles.at(last_node_index).reset();
            read_cache_p_handles.pop_back();
          }
        }
      }
    } else if (1 < page_info.page_level_) {
      if (meta_items.count() == 1) {
        const int32_t child_level_page_index = level_page_index * FULL_PAGE_META_ITEM_NUM + cur_item_index;
        search_path.at(last_node_index).prev_item_index_ = cur_item_index;
        char *unused_page_buff = NULL;
        ObTmpFilePageHandle write_cache_p_handle;
        ObTmpPageValueHandle read_cache_p_handle;
        if (OB_FAIL(get_page_(meta_items.at(0), child_level_page_index, unused_page_buff,
                                                write_cache_p_handle, read_cache_p_handle))) {
          STORAGE_LOG(WARN, "fail to get page", KR(ret), K(fd_), K(meta_items.at(0)), K(child_level_page_index));
        } else if (OB_FAIL(write_cache_p_handles.push_back(write_cache_p_handle))) {
          STORAGE_LOG(WARN, "fail to push back", KR(ret), K(fd_), K(write_cache_p_handle));
        } else if (OB_FAIL(read_cache_p_handles.push_back(read_cache_p_handle))) {
          STORAGE_LOG(WARN, "fail to push back", KR(ret), K(fd_), K(read_cache_p_handle));
        } else if (OB_FAIL(search_path.push_back(BacktraceNode(meta_items.at(0), child_level_page_index, -1)))) {
          STORAGE_LOG(WARN, "fail to push back", KR(ret), K(fd_), K(meta_items.at(0)), K(child_level_page_index));
        }
        if (OB_FAIL(ret)) {
          write_cache_p_handle.reset();
          read_cache_p_handle.reset();
        }
      } else if (meta_items.empty()) {
        if (reach_last_item) {
          if (OB_FAIL(release_meta_page_(page_info, level_page_index))) {
            STORAGE_LOG(WARN, "fail to release meta page", KR(ret), K(fd_), K(page_info), K(level_page_index));
          } else {
            search_path.pop_back();
            write_cache_p_handles.at(last_node_index).reset();
            write_cache_p_handles.pop_back();
            read_cache_p_handles.at(last_node_index).reset();
            read_cache_p_handles.pop_back();
          }
        } else {
          need_finish = true;
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "unexpected meta_items", KR(ret), K(fd_), K(meta_items));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected page info", KR(ret), K(fd_), K(page_info));
    }
  }
  if (OB_SUCC(ret) && !need_finish) {
    //all pages are released
    if (OB_FAIL(check_tree_is_empty_())) {
      STORAGE_LOG(WARN, "unexpected, tree is not empty", KR(ret), K(fd_));
    } else {
      root_item_.reset();
      level_page_num_array_.reset();
      last_truncate_leaf_info_.reset();
      ++stat_info_.tree_epoch_;
      stat_info_.this_epoch_meta_page_released_cnt_ = 0;
      STORAGE_LOG(INFO, "meta tree pages are all truncated", KR(ret), K(fd_), K(stat_info_));
    }
  }
  return ret;
}

int ObSharedNothingTmpFileMetaTree::check_tree_is_empty_()
{
  int ret = OB_SUCCESS;
  int64_t total_page_num = 0;
  ARRAY_FOREACH(level_page_num_array_, i) {
    total_page_num += level_page_num_array_.at(i);
  }
  if (OB_UNLIKELY(total_page_num != stat_info_.this_epoch_meta_page_released_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "unexpected total_page_num", KR(ret), K(fd_), K(total_page_num),
                                                  K(level_page_num_array_), K(stat_info_));
  }
  return ret;
}

int ObSharedNothingTmpFileMetaTree::get_page_(
    const ObSharedNothingTmpFileMetaItem &page_info,
    const int32_t level_page_index,
    char *&page_buff,
    ObTmpFilePageHandle &write_cache_page_handle,
    ObTmpPageValueHandle &read_cache_page_handle,
    const int64_t timeout_ms)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!page_info.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "invalid argument", KR(ret), K(fd_), K(page_info));
  } else {
    ObTmpFileWriteCacheKey cache_key(fd_, page_info.page_level_, level_page_index);
    if (OB_FAIL(wbp_->get_page(cache_key, write_cache_page_handle))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        STORAGE_LOG(WARN, "fail to get page in write cache", KR(ret), K(fd_), K(cache_key));
      }
    } else {
      page_buff = write_cache_page_handle.get_page()->get_buffer();
    }
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      bool need_load_from_disk = false;
      ObTmpPageCacheKey key(cache_key, MTL_ID());
      if (OB_SUCC(ObTmpPageCache::get_instance().get_page(key, read_cache_page_handle))) {
        page_buff = read_cache_page_handle.value_->get_buffer();
      } else if (OB_ENTRY_NOT_EXIST != ret) {
        STORAGE_LOG(ERROR, "fail to read from read_cache", KR(ret), K(fd_), K(key));
      } else {
        ret = OB_SUCCESS;
        need_load_from_disk = true;
        if (OB_FAIL(ObTmpPageCache::get_instance().load_page(key, page_info.block_index_, page_info.physical_page_id_,
                                                  callback_allocator_, read_cache_page_handle, timeout_ms))) {
          STORAGE_LOG(WARN, "fail to load page from disk", KR(ret), K(fd_), K(key), K(page_info), K(timeout_ms));
        } else {
          page_buff = read_cache_page_handle.value_->get_buffer();
        }
        STORAGE_LOG(DEBUG, "load page from disk", KR(ret), K(fd_), K(need_load_from_disk), K(page_info), K(level_page_index));
      }
      if (FAILEDx(check_page_(page_buff))) {
        STORAGE_LOG(ERROR, "the page is invalid or corrupted", KR(ret), K(fd_), KP(page_buff));
      }
      STORAGE_LOG(DEBUG, "load page from read cache or disk", KR(ret), K(fd_), K(need_load_from_disk), K(page_info), K(level_page_index));
    }
    if (OB_SUCC(ret) && OB_ISNULL(page_buff)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected page_buff", KR(ret), K(fd_), KP(page_buff));
    }
  }
  return ret;
}

int ObSharedNothingTmpFileMetaTree::check_page_(const char* const page_buff)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(page_buff)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", KR(ret), K(fd_), KP(page_buff));
  } else {
    ObSharedNothingTmpFileTreePageHeader page_header = *((ObSharedNothingTmpFileTreePageHeader *)(page_buff));
    const uint64_t checksum = ob_crc64(page_buff + PAGE_HEADER_SIZE, ObTmpFileGlobal::PAGE_SIZE - PAGE_HEADER_SIZE);
    if (OB_UNLIKELY(PAGE_MAGIC_NUM != page_header.magic_number_
                    || checksum != page_header.checksum_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "checksum or magic_number is not equal, page is invalid", KR(ret), K(fd_),
                                                KP(page_buff), K(page_header), K(checksum), K(PAGE_MAGIC_NUM));
    }
  }
  return ret;
}

//get page and put into write cache (if not exists)
int ObSharedNothingTmpFileMetaTree::cache_page_for_write_(
    const ObTmpFileWriteCacheKey &cache_key,
    const ObSharedNothingTmpFileMetaItem &page_info,
    const int64_t timeout_ms,
    ObTmpFilePageHandle &page_handle)
{
  int ret = OB_SUCCESS;
  ObTmpFilePageId page_id;
  page_id.block_index_ = page_info.block_index_;
  page_id.page_index_in_block_ = page_info.physical_page_id_;
  if (OB_UNLIKELY(!cache_key.is_valid()
                  || !page_info.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "unexpected cache_key or page_info", KR(ret), K(fd_), K(cache_key), K(page_info));
  } else if (OB_FAIL(wbp_->alloc_page(cache_key, page_id, timeout_ms, page_handle))) {
    STORAGE_LOG(WARN, "fail to alloc page", KR(ret), K(fd_), K(cache_key), K(page_id), K(timeout_ms));
  } else {
    ObTmpPageValueHandle p_handle;
    char *new_page_buff = page_handle.get_page()->get_buffer();
    ObTmpPageCacheKey key(cache_key, MTL_ID());
    if (OB_SUCC(ObTmpPageCache::get_instance().get_page(key, p_handle))) {
      MEMCPY(new_page_buff, p_handle.value_->get_buffer(), ObTmpFileGlobal::PAGE_SIZE);
    } else if (OB_ENTRY_NOT_EXIST != ret) {
      STORAGE_LOG(ERROR, "fail to read from read_cache", KR(ret), K(fd_), K(key));
    } else {
      ret = OB_SUCCESS;
      p_handle.reset();
      if (OB_FAIL(ObTmpPageCache::get_instance().load_page(key, page_info.block_index_,
                        page_info.physical_page_id_, callback_allocator_, p_handle, timeout_ms))) {
        STORAGE_LOG(WARN, "fail to load page from disk", KR(ret), K(fd_), K(key), K(page_info), K(timeout_ms));
      } else {
        MEMCPY(new_page_buff, p_handle.value_->get_buffer(), ObTmpFileGlobal::PAGE_SIZE);
      }
    }
    if (FAILEDx(check_page_(new_page_buff))) {
      STORAGE_LOG(ERROR, "the page is invalid or corrupted", KR(ret), K(fd_), KP(new_page_buff));
    } else if (OB_FAIL(wbp_->put_page(page_handle))) {
      STORAGE_LOG(WARN, "fail to put page", KR(ret), K(fd_), K(page_handle));
    }
    STORAGE_LOG(INFO, "load page to write cache", KR(ret), K(fd_), K(page_info), K(cache_key));
  }
  return ret;
}

//NOTE: The virtual_page_id for this meta_item should be set outside the function
int ObSharedNothingTmpFileMetaTree::alloc_new_meta_page_(
    const ObTmpFileWriteCacheKey page_key,
    const int64_t timeout_ms,
    ObTmpFilePageHandle &page_handle,
    ObSharedNothingTmpFileMetaItem &meta_item)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!prealloc_info_.is_empty() && !prealloc_info_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "unexpected prealloc_info_", KR(ret), K(fd_), K(prealloc_info_));
  } else {
    ObSEArray<ObTmpFileBlockRange, 4> page_entry_arr;
    int64_t target_block_index = -1;
    int32_t target_page_index_in_block = -1;
    if (prealloc_info_.is_empty()) {
      //alloc new entry
      const int16_t origin_entry_cnt = prealloc_info_.page_entry_arr_.count();
      int64_t alloc_page_num = MIN(ObTmpFileGlobal::TMP_FILE_MAX_SHARED_PRE_ALLOC_PAGE_NUM, prealloc_info_.next_alloced_page_num_);
      int64_t actual_alloc_page_num = 0;
      if (OB_FAIL(block_manager_->alloc_page_range(1, alloc_page_num, page_entry_arr))) {
        STORAGE_LOG(WARN, "fail to alloc page entry", KR(ret), K(fd_), K(alloc_page_num));
      } else if (OB_UNLIKELY(page_entry_arr.empty())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "unexpected page_entry_arr", KR(ret), K(fd_), K(page_entry_arr));
      } else if (OB_FAIL(prealloc_info_.page_entry_arr_.reserve(origin_entry_cnt + page_entry_arr.count()))) {
        STORAGE_LOG(WARN, "fail to reserve", KR(ret), K(fd_), K(origin_entry_cnt), K(page_entry_arr));
      }
      if (OB_FAIL(ret)) {
        if (!page_entry_arr.empty()) {
          int tmp_ret = OB_SUCCESS;
          for (int16_t i = 0; OB_SUCC(tmp_ret) && i < page_entry_arr.count(); i++) {
            if (OB_TMP_FAIL(release_tmp_file_page_(page_entry_arr.at(i).block_index_,
                                                   page_entry_arr.at(i).page_index_,
                                                   page_entry_arr.at(i).page_cnt_))) {
              STORAGE_LOG(WARN, "fail to release tmp file page", KR(ret), KR(tmp_ret), K(fd_), K(page_entry_arr), K(i));
            }
          }
        }
      } else {
        ARRAY_FOREACH_N(page_entry_arr, i, cnt) {
          if (OB_UNLIKELY(!page_entry_arr.at(i).is_valid())) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(ERROR, "unexpected page_entry_arr", KR(ret), K(fd_), K(page_entry_arr), K(i));
          //must success, because we have reserved.
          } else if (OB_FAIL(prealloc_info_.page_entry_arr_.push_back(page_entry_arr.at(i)))) {
            STORAGE_LOG(WARN, "fail to push back", KR(ret), K(fd_), K(page_entry_arr), K(i));
          } else {
            stat_info_.all_type_page_occupy_disk_cnt_ += page_entry_arr.at(i).page_cnt_;
            actual_alloc_page_num += page_entry_arr.at(i).page_cnt_;
          }
        }
        if (OB_SUCC(ret)) {
          target_block_index = page_entry_arr.at(0).block_index_;
          target_page_index_in_block = page_entry_arr.at(0).page_index_;
          //TODO: Set rules
          prealloc_info_.next_alloced_page_num_ = MIN(ObTmpFileGlobal::TMP_FILE_MAX_SHARED_PRE_ALLOC_PAGE_NUM, 2 * MIN(alloc_page_num, actual_alloc_page_num));
          if (actual_alloc_page_num < 2) {
            //NOTE: we can not reset prealloc_info_page_entry_arr_., we need record during the insertion period.
            prealloc_info_.next_entry_arr_index_ = -1;
            prealloc_info_.next_page_index_in_entry_ = -1;
          } else if (1 == page_entry_arr.at(0).page_cnt_) {
            prealloc_info_.next_entry_arr_index_ = origin_entry_cnt + 1;
            prealloc_info_.next_page_index_in_entry_ = 0;
          } else {
            prealloc_info_.next_entry_arr_index_ = origin_entry_cnt;
            prealloc_info_.next_page_index_in_entry_ = 1;
          }
        } else if (OB_ALLOCATE_MEMORY_FAILED == ret) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(ERROR, "unexpected result", KR(ret), K(fd_), K(page_entry_arr));
        }
      }
    } else {
      //use old entry
      const int16_t cur_entry_arr_index = prealloc_info_.next_entry_arr_index_;
      const int16_t cur_page_index_in_entry = prealloc_info_.next_page_index_in_entry_;
      ObTmpFileBlockRange &cur_page_entry = prealloc_info_.page_entry_arr_.at(cur_entry_arr_index);
      if (OB_UNLIKELY(cur_page_index_in_entry >= cur_page_entry.page_cnt_)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "unexpected prealloc_info_", KR(ret), K(fd_), K(prealloc_info_));
      } else {
        target_block_index = cur_page_entry.block_index_;
        target_page_index_in_block = cur_page_entry.page_index_ + cur_page_index_in_entry;
        if (cur_page_index_in_entry + 1 == cur_page_entry.page_cnt_) {
          if (cur_entry_arr_index + 1 == prealloc_info_.page_entry_arr_.count()) {
            prealloc_info_.next_entry_arr_index_ = -1;
            prealloc_info_.next_page_index_in_entry_ = -1;
          } else {
            prealloc_info_.next_entry_arr_index_++;
            prealloc_info_.next_page_index_in_entry_ = 0;
          }
        } else {
          prealloc_info_.next_page_index_in_entry_++;
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObTmpFilePageId page_id;
      page_id.block_index_ = target_block_index;
      page_id.page_index_in_block_ = target_page_index_in_block;
      if (OB_FAIL(wbp_->alloc_page(page_key, page_id, timeout_ms, page_handle))) {
        STORAGE_LOG(WARN, "fail to alloc page", KR(ret), K(fd_), K(page_key), K(page_id), K(timeout_ms));
      } else {
        meta_item.page_level_ = page_key.tree_level_;
        meta_item.block_index_ = page_id.block_index_;
        meta_item.physical_page_id_ = page_id.page_index_in_block_;
      }
      STORAGE_LOG(DEBUG, "alloc new meta page", KR(ret), K(fd_), K(page_key), K(page_id), K(prealloc_info_));
    }
  }
  return ret;
}

//This function deletes the used page entries in array.
int ObSharedNothingTmpFileMetaTree::update_prealloc_info_()
{
  int ret = OB_SUCCESS;
  STORAGE_LOG(DEBUG, "before update prealloc_info_", KR(ret), K(fd_), K(prealloc_info_));
  if (prealloc_info_.is_empty()) {
    prealloc_info_.page_entry_arr_.reset();
  } else {
    if (OB_UNLIKELY(!prealloc_info_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected prealloc_info_", KR(ret), K(fd_), K(prealloc_info_));
    } else if (0 < prealloc_info_.next_entry_arr_index_) {
      const int16_t entry_cnt = prealloc_info_.page_entry_arr_.count();
      const int16_t dis = prealloc_info_.next_entry_arr_index_ - 0;
      for (int16_t i = prealloc_info_.next_entry_arr_index_; OB_SUCC(ret) && i < entry_cnt; i++) {
        if (OB_UNLIKELY(!prealloc_info_.page_entry_arr_.at(i).is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(ERROR, "unexpected page_entry_arr_", KR(ret), K(fd_), K(prealloc_info_));
        } else {
          prealloc_info_.page_entry_arr_.at(i - dis) = prealloc_info_.page_entry_arr_.at(i);
        }
      }
      for (int16_t i = 0; i < dis; i++) {
        prealloc_info_.page_entry_arr_.pop_back();
      }
    }
    if (OB_SUCC(ret)) {
      prealloc_info_.next_entry_arr_index_ = 0;
    }
  }
  STORAGE_LOG(DEBUG, "after update prealloc_info_", KR(ret), K(fd_), K(prealloc_info_));
  return ret;
}

int ObSharedNothingTmpFileMetaTree::rollback_prealloc_info_(
    const int16_t origin_arr_index,
    const int16_t origin_index_in_entry)
{
  int ret = OB_SUCCESS;
  STORAGE_LOG(DEBUG, "before rollback prealloc_info_", KR(ret), K(fd_), K(prealloc_info_));
  if (OB_UNLIKELY((origin_arr_index == prealloc_info_.next_entry_arr_index_ && origin_index_in_entry > prealloc_info_.next_page_index_in_entry_))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "unexpected prealloc_info_", KR(ret), K(fd_), K(origin_arr_index), K(origin_index_in_entry), K(prealloc_info_));
  } else if (-1 != origin_arr_index) {
    if (OB_UNLIKELY(0 > origin_index_in_entry)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected origin_index_in_entry", KR(ret), K(fd_),
                                      K(origin_arr_index), K(origin_index_in_entry), K(prealloc_info_));
    } else {
      prealloc_info_.next_entry_arr_index_ = origin_arr_index;
      prealloc_info_.next_page_index_in_entry_ = origin_index_in_entry;
    }
  } else {
    if (!prealloc_info_.page_entry_arr_.empty()) {
      prealloc_info_.next_entry_arr_index_ = 0;
      prealloc_info_.next_page_index_in_entry_ = 0;
    }
  }
  STORAGE_LOG(DEBUG, "after rollback prealloc_info_", KR(ret), K(fd_), K(prealloc_info_));
  return ret;
}

int ObSharedNothingTmpFileMetaTree::release_prealloc_data_()
{
  int ret = OB_SUCCESS;
  if (prealloc_info_.is_empty()) {
    //do nothing
  } else if (OB_UNLIKELY(0 != prealloc_info_.next_entry_arr_index_
                        || !prealloc_info_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "unexpected prealloc_info_", KR(ret), K(fd_), K(prealloc_info_));
  } else {
    int16_t cur_page_index = prealloc_info_.next_page_index_in_entry_;
    for (int16_t i = 0; OB_SUCC(ret) && i < prealloc_info_.page_entry_arr_.count(); i++) {
    ObTmpFileBlockRange &page_entry = prealloc_info_.page_entry_arr_.at(i);
      if (OB_FAIL(release_tmp_file_page_(page_entry.block_index_,
                                        page_entry.page_index_ + cur_page_index,
                                        page_entry.page_cnt_ - cur_page_index))) {
        STORAGE_LOG(WARN, "fail to release tmp file page", KR(ret), K(fd_), K(page_entry));
      } else {
        cur_page_index = 0;
      }
    }
  }
  return ret;
}

int ObSharedNothingTmpFileMetaTree::init_page_header_(
    char* page_buff,
    const int16_t page_level)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(page_buff)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid argument", KR(ret), K(fd_), KP(page_buff));
  } else {
    ObSharedNothingTmpFileTreePageHeader page_header;
    page_header.item_num_ = 0;
    page_header.page_level_ = page_level;
    page_header.magic_number_ = PAGE_MAGIC_NUM;
    MEMCPY(page_buff, &page_header, PAGE_HEADER_SIZE);
  }
  return ret;
}

int ObSharedNothingTmpFileMetaTree::read_page_header_(
    const char* page_buff,
    ObSharedNothingTmpFileTreePageHeader &page_header)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == page_buff)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid argument", KR(ret), K(fd_), KP(page_buff));
  } else {
    page_header = *((ObSharedNothingTmpFileTreePageHeader *)(page_buff));
  }
  return ret;
}

int ObSharedNothingTmpFileMetaTree::remove_page_item_from_tail_(
    char* page_buff,
    const int16_t remove_item_num)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == page_buff
                  || 0 > remove_item_num)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid argument", KR(ret), K(fd_), KP(page_buff), K(remove_item_num));
  } else {
    ObSharedNothingTmpFileTreePageHeader page_header = *((ObSharedNothingTmpFileTreePageHeader *)(page_buff));
    page_header.item_num_ -= remove_item_num;
    if (OB_UNLIKELY(page_header.item_num_ < 0)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected item_num", KR(ret), K(fd_), K(page_header));
    } else {
      MEMCPY(page_buff, &page_header, PAGE_HEADER_SIZE);
    }
  }
  return ret;
}

template<typename ItemType>
int ObSharedNothingTmpFileMetaTree::read_item_(
    const char* page_buff,
    const int64_t target_virtual_page_id,
    int16_t &item_index,
    ItemType &item)
{
  int ret = OB_SUCCESS;
  item_index = -1;
  item.reset();
  ObSharedNothingTmpFileTreePageHeader page_header = *((ObSharedNothingTmpFileTreePageHeader *)(page_buff));
  int16_t item_num = page_header.item_num_;
  if (OB_UNLIKELY(NULL == page_buff
                  || target_virtual_page_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid argument", KR(ret), K(fd_), KP(page_buff), K(target_virtual_page_id));
  } else if (OB_UNLIKELY(PAGE_HEADER_SIZE + item_num * sizeof(item) > ObTmpFileGlobal::PAGE_SIZE
                         || item_num > MAX_PAGE_ITEM_COUNT)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "unexpected item_num", KR(ret), K(fd_), K(page_header));
  } else {
    int16_t left = 0;
    int16_t right = item_num - 1;
    const char *items_buff = page_buff + PAGE_HEADER_SIZE;
    //find the last index less than or equal to the target_virtual_page_id
    while (left <= right) {
      int16_t mid = (left + right) / 2;
      ItemType mid_item = *((ItemType *)(items_buff + mid * sizeof(item)));
      if (OB_UNLIKELY(!mid_item.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "unexpected mid item", KR(ret), K(fd_), K(mid_item));
      } else if (mid_item.virtual_page_id_ <= target_virtual_page_id) {
        left = mid + 1;
      } else {
        right = mid - 1;
      }
    }
    if (OB_UNLIKELY(right < 0)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected dichotomy result", KR(ret), K(fd_), K(right));
    } else {
      item_index = right;
      item = *((ItemType *)(items_buff + item_index * sizeof(item)));
      if (OB_UNLIKELY(!item.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "unexpected item", KR(ret), K(fd_), K(item));
      } else if (item.virtual_page_id_ > target_virtual_page_id) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "unexpected virtual_page_id", KR(ret), K(fd_), K(item), K(target_virtual_page_id));
      }
    }
  }
  if (OB_FAIL(ret)) {
    //print page content
    ObSharedNothingTmpFileTreePageHeader tmp_page_header;
    ItemType first_item;
    ItemType last_item;
    read_page_simple_content_(page_buff, tmp_page_header, first_item, last_item);
    STORAGE_LOG(WARN, "fail to read item, dump tree page", KR(ret), K(fd_), KP(page_buff), K(target_virtual_page_id),
                                                              K(tmp_page_header), K(first_item), K(last_item));
  }
  return ret;
}

// XXX 增加 page 物理范围校验；
template<typename ItemType>
int ObSharedNothingTmpFileMetaTree::read_item_(
    const char* page_buff,
    const int16_t item_index,
    ItemType &item)
{
  int ret = OB_SUCCESS;
  item.reset();
  ObSharedNothingTmpFileTreePageHeader page_header = *((ObSharedNothingTmpFileTreePageHeader *)(page_buff));
  if (OB_UNLIKELY(NULL == page_buff
                  || item_index < 0
                  || item_index >= page_header.item_num_
                  || PAGE_HEADER_SIZE + page_header.item_num_ * sizeof(item) > ObTmpFileGlobal::PAGE_SIZE
                  || PAGE_HEADER_SIZE + (item_index + 1) * sizeof(item) > ObTmpFileGlobal::PAGE_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid argument", KR(ret), K(fd_), KP(page_buff), K(item_index), K(page_header));
  } else {
    const char *items_buff = page_buff + PAGE_HEADER_SIZE;
    item = *((ItemType *)(items_buff + item_index * sizeof(item)));
  }
  return ret;
}

template<typename ItemType>
int ObSharedNothingTmpFileMetaTree::rewrite_item_(
    char* page_buff,
    const int16_t item_index,
    const ItemType &item)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == page_buff
                  || item_index < 0
                  || PAGE_HEADER_SIZE + (item_index + 1) * sizeof(item) > ObTmpFileGlobal::PAGE_SIZE
                  || !item.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid argument", KR(ret), K(fd_), KP(page_buff), K(item_index), K(item));
  } else {
    char *items_buff = page_buff + PAGE_HEADER_SIZE;
    MEMCPY(items_buff + item_index * sizeof(item), &item, sizeof(item));
  }
  return ret;
}

template<typename ItemType>
int ObSharedNothingTmpFileMetaTree::write_items_(
    char* page_buff,
    const ObIArray<ItemType> &items,
    const int64_t begin_index,
    int16_t &count)
{
  int ret = OB_SUCCESS;
  count = 0;
  if (OB_UNLIKELY(NULL == page_buff
                  || items.empty()
                  || begin_index < 0
                  || begin_index >= items.count())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid argument", KR(ret), K(fd_), KP(page_buff), K(items), K(begin_index));
  } else {
    ObSharedNothingTmpFileTreePageHeader page_header = *((ObSharedNothingTmpFileTreePageHeader *)(page_buff));
    char *items_buff = page_buff + PAGE_HEADER_SIZE;
    int16_t item_index = page_header.item_num_;
    for (int64_t i = begin_index; i < items.count()
        && PAGE_HEADER_SIZE + (item_index + 1) * sizeof(items.at(i)) <= ObTmpFileGlobal::PAGE_SIZE && item_index < MAX_PAGE_ITEM_COUNT; i++) {
      MEMCPY(items_buff + item_index * sizeof(items.at(i)), &items.at(i), sizeof(items.at(i)));
      item_index++;
      count++;
    }
    page_header.item_num_ = item_index;
    MEMCPY(page_buff, &page_header, PAGE_HEADER_SIZE);
  }
  return ret;
}

template<typename ItemType>
int ObSharedNothingTmpFileMetaTree::write_item_(
    char* page_buff,
    ItemType &item)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == page_buff)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid argument", KR(ret), K(fd_), KP(page_buff), K(item));
  } else {
    ObSharedNothingTmpFileTreePageHeader page_header = *((ObSharedNothingTmpFileTreePageHeader *)(page_buff));
    char *items_buff = page_buff + PAGE_HEADER_SIZE;
    int16_t item_index = page_header.item_num_;
    MEMCPY(items_buff + item_index * sizeof(item), &item, sizeof(item));
    item_index++;
    page_header.item_num_ = item_index;
    MEMCPY(page_buff, &page_header, PAGE_HEADER_SIZE);
  }
  return ret;
}

void ObSharedNothingTmpFileMetaTree::print_meta_tree_overview_info()
{
  SpinRLockGuard guard(lock_);
  STORAGE_LOG(INFO, "dump meta tree", KPC(this));
}

void ObSharedNothingTmpFileMetaTree::print_meta_tree_total_info()
{
  int ret = OB_SUCCESS;
  ObArray<ObSharedNothingTmpFileDataItem> data_items;
  ObArray<ObSharedNothingTmpFileMetaItem> meta_items;
  SpinRLockGuard guard(lock_);
  STORAGE_LOG(INFO, "dump meta tree", KPC(this));
  ARRAY_FOREACH_N(level_page_num_array_, level, level_cnt) {
    const int32_t level_page_num = level_page_num_array_.at(level);
    if (OB_UNLIKELY(0 >= level_page_num)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected level_page_num", KR(ret), K(fd_), K(level_page_num));
    } else {
      for (int32_t page_index = 0; OB_SUCC(ret) && page_index < level_page_num; page_index++) {
        ObTmpFileWriteCacheKey cache_key(fd_, level, page_index);
        ObTmpFilePageHandle page_handle;
        if (OB_FAIL(wbp_->get_page(cache_key, page_handle))) {
          if (OB_ENTRY_NOT_EXIST != ret) {
            STORAGE_LOG(WARN, "fail to get page in write cache", KR(ret), K(fd_), K(cache_key));
          } else {
            STORAGE_LOG(INFO, "page not exist in write cache", KR(ret), K(fd_), K(cache_key));
            ret = OB_SUCCESS;
          }
        } else {
          ObSharedNothingTmpFileTreePageHeader tmp_page_header;
          char *page_buff = page_handle.get_page()->get_buffer();
          if (0 == level) {
            data_items.reset();
            read_page_content_(page_buff, tmp_page_header, data_items);
            STORAGE_LOG(INFO, "dump cached leaf page", KR(ret), K(fd_), KP(page_buff), K(cache_key), K(tmp_page_header), K(data_items));
          } else {
            meta_items.reset();
            read_page_content_(page_buff, tmp_page_header, meta_items);
            STORAGE_LOG(INFO, "dump cached internal page", KR(ret), K(fd_), KP(page_buff), K(cache_key), K(tmp_page_header), K(meta_items));
          }
        }
      }
    }
  }
}

template<typename ItemType>
void ObSharedNothingTmpFileMetaTree::read_page_content_(
    const char *page_buff,
    ObSharedNothingTmpFileTreePageHeader &page_header,
    ObIArray<ItemType> &items)
{
  int ret = OB_SUCCESS;
  items.reset();
  if (OB_ISNULL(page_buff)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid argument", KR(ret), K(fd_), KP(page_buff));
  } else {
    const char *items_buff = page_buff + PAGE_HEADER_SIZE;
    int16_t index = 0;
    page_header = *((ObSharedNothingTmpFileTreePageHeader *)(page_buff));
    while (OB_SUCC(ret)
          && index < page_header.item_num_
          && PAGE_HEADER_SIZE + (index + 1) * sizeof(ItemType) <= ObTmpFileGlobal::PAGE_SIZE) {
      ItemType item = *((ItemType *)(items_buff + index * sizeof(ItemType)));
      if (OB_FAIL(items.push_back(item))) {
        STORAGE_LOG(WARN, "fail to push back", KR(ret), K(fd_), K(item));
      } else {
        index++;
      }
    }
  }
}

template<typename ItemType>
void ObSharedNothingTmpFileMetaTree::read_page_simple_content_(
     const char *page_buff,
     ObSharedNothingTmpFileTreePageHeader &page_header,
     ItemType &first_item,
     ItemType &last_item)
{
  int ret = OB_SUCCESS;
  first_item.reset();
  last_item.reset();
  if (OB_ISNULL(page_buff)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid argument", KR(ret), K(fd_), KP(page_buff));
  } else {
    const char *items_buff = page_buff + PAGE_HEADER_SIZE;
    page_header = *((ObSharedNothingTmpFileTreePageHeader *)(page_buff));
    if (OB_UNLIKELY(0 > page_header.item_num_
                    || PAGE_HEADER_SIZE + page_header.item_num_ * sizeof(ItemType) > ObTmpFileGlobal::PAGE_SIZE)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected item_num_", KR(ret), K(fd_), KP(page_buff), K(page_header));
    } else if (0 == page_header.item_num_) {
    } else {
      first_item = *((ItemType *)(items_buff));
      last_item = *((ItemType *)(items_buff + (page_header.item_num_ - 1) * sizeof(ItemType)));
    }
  }
}

int ObSharedNothingTmpFileMetaTree::copy_info(ObTmpFileMetaTreeInfo &tree_info)
{
  int ret = OB_SUCCESS;
  const int64_t abs_timeout_us = ObTimeUtility::current_time() + 100 * 1000L;
  if (OB_FAIL(lock_.rdlock(abs_timeout_us))) {
    STORAGE_LOG(WARN, "fail to rdlock", KR(ret), K(fd_), K(abs_timeout_us));
  } else {
    int64_t cached_page_num = 0;
    int64_t total_page_num = 0;
    for (int16_t i = 0; i < level_page_num_array_.count(); i++) {
      total_page_num += level_page_num_array_.at(i);
    }
    tree_info.meta_tree_epoch_ = stat_info_.tree_epoch_;
    tree_info.meta_tree_level_cnt_ = level_page_num_array_.count();
    tree_info.meta_size_ = total_page_num * ObTmpFileGlobal::PAGE_SIZE;
    tree_info.cached_meta_page_num_ = cached_page_num;
    lock_.rdunlock();
  }
  return ret;
}

}
}
