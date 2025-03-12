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

ObSharedNothingTmpFileMetaTree::ObSharedNothingTmpFileMetaTree() :
    is_inited_(false),
    fd_(-1),
    wbp_(nullptr),
    callback_allocator_(nullptr),
    block_manager_(nullptr),
    tree_epoch_(0),
    root_item_(),
    data_item_array_(),
    level_page_range_array_(),
    is_writing_(false),
    lock_(common::ObLatchIds::TMP_FILE_LOCK),
    last_truncate_leaf_info_(),
    released_offset_(0),
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
  tree_epoch_ = 0;
  root_item_.reset();
  data_item_array_.reset();
  level_page_range_array_.reset();
  is_writing_ = false;
  last_truncate_leaf_info_.reset();
  released_offset_ = 0;
  stat_info_.reset();
}

int ObSharedNothingTmpFileMetaTree::init(const int64_t fd,
                                         ObTmpWriteBufferPool *wbp,
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
    level_page_range_array_.set_attr(ObMemAttr(MTL_ID(), "TFTreeLevelArr"));
    fd_ = fd;
    wbp_ = wbp;
    callback_allocator_ = callback_allocator;
    block_manager_ = block_manager;
    is_inited_ = true;
  }
  return ret;
}

int ObSharedNothingTmpFileMetaTree::prepare_for_insert_items()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (!root_item_.is_valid()) {
    //do nothing
  } else {
    ObSharedNothingTmpFileMetaItem page_info;
    if (OB_FAIL(get_rightmost_leaf_page_for_write_(page_info))) {
      STORAGE_LOG(WARN, "fail to get rightmost leaf page for write", KR(ret), KPC(this));
    }
  }
  return ret;
}

int ObSharedNothingTmpFileMetaTree::get_rightmost_leaf_page_for_write_(
    ObSharedNothingTmpFileMetaItem &page_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_writing_
                  || level_page_range_array_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "unexpected is_writing_ or level_array_", KR(ret), K(fd_), K(is_writing_), K(level_page_range_array_));
  } else {
    is_writing_ = true; //need to be protected by lock_
    ObSharedNothingTmpFileMetaItem meta_item = root_item_;
    uint32_t parent_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
    ObSharedNothingTmpFileMetaItem next_level_meta_item;
    while (OB_SUCC(ret)
          && 0 < meta_item.page_level_) {
      next_level_meta_item.reset();
      if (OB_FAIL(get_last_item_of_internal_page_(parent_page_id, meta_item, next_level_meta_item))) {
        STORAGE_LOG(WARN, "fail to get last item of internal page", KR(ret), K(fd_), K(parent_page_id), K(meta_item));
      } else {
        parent_page_id = meta_item.buffer_page_id_;
        meta_item = next_level_meta_item;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(0 != meta_item.page_level_)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "unexpected page_level_", KR(ret), K(meta_item));
      } else if (OB_FAIL(cache_page_for_write_(parent_page_id, meta_item))) {
        STORAGE_LOG(WARN, "fail to cache page for write", KR(ret), K(fd_), K(parent_page_id), K(meta_item));
      } else {
        page_info = meta_item;
      }
    }

    //there is no need to remove the successfully loaded rightmost page from the write cache
    if (OB_FAIL(ret)) {
      is_writing_ = false;
    }
  }
  return ret;
}

int ObSharedNothingTmpFileMetaTree::get_last_item_of_internal_page_(
    const uint32_t parent_page_id,
    ObSharedNothingTmpFileMetaItem &page_info,
    ObSharedNothingTmpFileMetaItem &last_meta_item)
{
  int ret = OB_SUCCESS;
  ObSharedNothingTmpFileTreePageHeader page_header;
  if (OB_FAIL(cache_page_for_write_(parent_page_id, page_info))) {
    STORAGE_LOG(WARN, "fail to cache page for write", KR(ret), K(fd_), K(parent_page_id), K(page_info));
  } else {
    char* page_buff = NULL;
    uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
    int32_t level_page_index = level_page_range_array_[page_info.page_level_].evicted_page_num_ +
                                  level_page_range_array_[page_info.page_level_].cached_page_num_ - 1;
    ObTmpFilePageUniqKey page_key(page_info.page_level_, level_page_index);
    if (OB_FAIL(wbp_->read_page(fd_, page_info.buffer_page_id_, page_key, page_buff, next_page_id))) {
      STORAGE_LOG(ERROR, "fail to read from write cache", KR(ret), K(fd_), K(page_info), K(page_key), K(level_page_range_array_));
    } else if (OB_FAIL(read_page_header_(page_buff, page_header))) {
      STORAGE_LOG(WARN, "fail to read page header", KR(ret), K(fd_), KP(page_buff));
    } else if (OB_UNLIKELY(0 >= page_header.item_num_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected item_num", KR(ret), K(page_header));
    } else if (OB_FAIL(read_item_(page_buff, page_header.item_num_ - 1, last_meta_item))) {
      STORAGE_LOG(WARN, "fail to read item", KR(ret), K(fd_), KP(page_buff), K(page_header));
    }
  }
  return ret;
}

int ObSharedNothingTmpFileMetaTree::insert_items(
    const ObIArray<ObSharedNothingTmpFileDataItem> &data_items)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (OB_UNLIKELY(data_items.empty())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", KR(ret), K(data_items));
  } else if (OB_UNLIKELY(!data_items.at(0).is_valid() ||
                         released_offset_ > data_items.at(0).virtual_page_id_ * ObTmpFileGlobal::PAGE_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid argument", KR(ret), K(data_items.at(0)), KPC(this));
  } else {
    int64_t write_count = 0;
    const int64_t data_item_count = data_items.count();
    ObSEArray<int16_t, 2> level_origin_page_write_counts;
    ObSEArray<ObSEArray<uint32_t, 2>, 2> level_new_pages;
    if (!root_item_.is_valid() && OB_FAIL(try_to_insert_items_to_array_(data_items, level_new_pages))) {
      STORAGE_LOG(WARN, "fail to try to insert items to array", KR(ret), K(data_items), KPC(this));
    } else if (root_item_.is_valid()) {
      //we set is_writing_ in previous step,
      //  so, we need not to worry about end pages of each level will be evicted.
      if (OB_UNLIKELY(!is_writing_)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "unexpected is_writing_", KR(ret), KPC(this));
      } else if (OB_FAIL(try_to_fill_rightmost_leaf_page_(data_items, level_new_pages, write_count,
                                                          level_origin_page_write_counts))) {
        STORAGE_LOG(WARN, "fail to try to fill rightmost leaf page", KR(ret), K(data_items), K(level_new_pages), KPC(this));
      } else if (write_count < data_item_count) {
        ObSEArray<ObSharedNothingTmpFileMetaItem, 1> meta_items;
        ObSharedNothingTmpFileMetaItem meta_item;
        if (level_page_range_array_.count() <= 1) {
          //there is only a root page
          if (OB_FAIL(meta_items.push_back(root_item_))) {
            STORAGE_LOG(WARN, "fail to push back", KR(ret), K(fd_), K(root_item_));
          }
        }
        //alloc new leaf pages
        while (OB_SUCC(ret)
              && write_count < data_item_count) {
          meta_item.reset();
          if (OB_FAIL(add_new_page_and_fill_items_at_leaf_(data_items, write_count, meta_item, level_new_pages))) {
            STORAGE_LOG(WARN, "fail to add new page and fill items at leaf", KR(ret),
                                  K(data_items), K(write_count), K(level_new_pages), KPC(this));
          } else if (OB_FAIL(meta_items.push_back(meta_item))) {
            STORAGE_LOG(WARN, "fail to push back", KR(ret), K(fd_), K(meta_item));
          }
        }
        //cascade to modify the internal pages
        if (FAILEDx(cascade_modification_at_internal_(meta_items,
                                  level_origin_page_write_counts, level_new_pages))) {
          STORAGE_LOG(WARN, "fail to cascade modification at internal", KR(ret), K(meta_items),
                                        K(level_origin_page_write_counts), K(level_new_pages), KPC(this));
        }
      }
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(finish_insert_(ret, level_origin_page_write_counts, level_new_pages))) {
        STORAGE_LOG(WARN, "fail to finish insert", KR(tmp_ret), KR(ret),
                      K(level_origin_page_write_counts), K(level_new_pages), KPC(this));
      }
      if (OB_SUCC(ret)) {
        ret = tmp_ret;
      }
      is_writing_ = false;
    }
  }
  if (OB_SUCC(ret)) {
    ARRAY_FOREACH_N(data_items, i, cnt) {
      stat_info_.all_type_page_flush_cnt_ += data_items.at(i).physical_page_num_;
    }
  }
  return ret;
}

int ObSharedNothingTmpFileMetaTree::try_to_insert_items_to_array_(
    const ObIArray<ObSharedNothingTmpFileDataItem> &data_items,
    ObIArray<ObSEArray<uint32_t, 2>> &level_new_pages)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(data_items.empty()
                  || !level_new_pages.empty()
                  || !level_page_range_array_.empty()
                  || MAX_DATA_ITEM_ARRAY_COUNT > MAX_PAGE_ITEM_COUNT
                  || is_writing_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid argument", KR(ret), K(fd_), K(data_items), K(level_page_range_array_),
                            K(level_new_pages), K(MAX_DATA_ITEM_ARRAY_COUNT), K(MAX_PAGE_ITEM_COUNT), K(is_writing_));
  } else if (!data_item_array_.empty()) {
    const ObSharedNothingTmpFileDataItem &last_item = data_item_array_.at(data_item_array_.count() - 1);
    if (OB_UNLIKELY(data_items.at(0).virtual_page_id_ != last_item.virtual_page_id_ + last_item.physical_page_num_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected data_items or data_item_array_", KR(ret), K(fd_), K(data_items), K(last_item));
    }
  }
  if (OB_SUCC(ret)) {
    bool need_build_tree = false;
    const int64_t total_count = data_item_array_.count() + data_items.count();
    if (total_count <= MAX_DATA_ITEM_ARRAY_COUNT) {
      const int16_t array_capacity = data_item_array_.get_capacity();
      if (array_capacity < total_count) {
        if (OB_FAIL(data_item_array_.reserve(MIN(MAX(total_count, 2 * array_capacity), MAX_DATA_ITEM_ARRAY_COUNT)))) {
          STORAGE_LOG(WARN, "fail to reserve for data_item_array_", KR(ret), K(fd_), K(total_count), K(array_capacity));
          if (OB_ALLOCATE_MEMORY_FAILED == ret) {
            need_build_tree = true;
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
    if (true == need_build_tree) {
      ret = OB_SUCCESS;
      ObTmpFilePageUniqKey leaf_page_offset(0/*tree level*/, 0/*level page index*/);
      ObSEArray<uint32_t, 2> new_pages;
      if (OB_FAIL(new_pages.push_back(ObTmpFileGlobal::INVALID_PAGE_ID))) {
        STORAGE_LOG(WARN, "fail to push back", KR(ret), K(fd_));
      } else if (OB_FAIL(level_new_pages.push_back(new_pages))) {
        STORAGE_LOG(WARN, "fail to push back", KR(ret), K(fd_), K(new_pages));
      } else {
        uint32_t new_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
        char *new_page_buff = NULL;
        int16_t count = 0;
        if (OB_FAIL(wbp_->alloc_page(fd_, leaf_page_offset, new_page_id, new_page_buff))) {
          STORAGE_LOG(WARN, "fail to alloc page from write cache", KR(ret), K(fd_), K(leaf_page_offset));
        } else if (FALSE_IT(stat_info_.meta_page_alloc_cnt_++)) {
        } else if (OB_FAIL(init_page_header_(new_page_buff, 0 /*level*/))) {
          STORAGE_LOG(WARN, "fail to init page header", KR(ret), K(fd_), KP(new_page_buff));
        } else if (!data_item_array_.empty() &&
                    OB_FAIL(write_items_(new_page_buff, data_item_array_, 0/*begin_index*/, count))) {
          STORAGE_LOG(WARN, "fail to write items", KR(ret), K(fd_), K(data_item_array_), KP(new_page_buff));
        } else if (OB_FAIL(wbp_->notify_dirty(fd_, new_page_id, leaf_page_offset))) {
          STORAGE_LOG(ERROR, "fail to notify dirty for meta", KR(ret), K(fd_), K(new_page_id), K(leaf_page_offset));
        } else {
          level_new_pages.at(0).at(0) = new_page_id;
          is_writing_ = true; //must be protected by lock
          root_item_.page_level_ = 0;
          root_item_.buffer_page_id_ = new_page_id;
          root_item_.virtual_page_id_ = data_item_array_.empty() ?
                      data_items.at(0).virtual_page_id_ : data_item_array_.at(0).virtual_page_id_;
          //print page content
          ObSharedNothingTmpFileTreePageHeader tmp_page_header;
          ObSharedNothingTmpFileDataItem first_item;
          ObSharedNothingTmpFileDataItem last_item;
          read_page_simple_content_(new_page_buff, tmp_page_header, first_item, last_item);
          STORAGE_LOG(INFO, "dump tree page", KR(ret), K(fd_), K(new_page_id), K(leaf_page_offset),
                                    KP(new_page_buff), K(tmp_page_header), K(first_item), K(last_item));
        }
      }
    }
  }
  return ret;
}

int ObSharedNothingTmpFileMetaTree::try_to_fill_rightmost_leaf_page_(
    const ObIArray<ObSharedNothingTmpFileDataItem> &data_items,
    const ObIArray<ObSEArray<uint32_t, 2>> &level_new_pages,
    int64_t &write_count,
    ObIArray<int16_t> &level_origin_page_write_counts)
{
  int ret = OB_SUCCESS;
  write_count = 0;
  if (OB_UNLIKELY(data_items.empty() || !data_items.at(0).is_valid()
                  || (level_new_pages.empty() && level_page_range_array_.empty())
                  || !level_origin_page_write_counts.empty())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid argument", KR(ret), K(fd_), K(data_items), K(level_new_pages),
                                      K(level_page_range_array_), K(level_origin_page_write_counts));
  } else {
    const int16_t MAX_PAGE_DATA_ITEM_NUM =
        MIN(MAX_PAGE_ITEM_COUNT, (ObTmpFileGlobal::PAGE_SIZE - PAGE_HEADER_SIZE) / sizeof(data_items.at(0)));
    uint32_t page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
    int32_t level_page_index = -1;
    int16_t count = 0;
    ObSharedNothingTmpFileTreePageHeader page_header;
    char *leaf_page_buff = NULL;
    uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
    if (!level_new_pages.empty()) {
      page_id = level_new_pages.at(0/*level*/).at(0);
      level_page_index = 0;
    } else {
      page_id = level_page_range_array_.at(0/*level*/).end_page_id_;
      level_page_index = level_page_range_array_.at(0).evicted_page_num_ + level_page_range_array_.at(0).cached_page_num_ - 1;
    }
    ObTmpFilePageUniqKey leaf_page_offset(0, level_page_index);
    if (OB_FAIL(wbp_->read_page(fd_, page_id, leaf_page_offset, leaf_page_buff, next_page_id))) {
      STORAGE_LOG(ERROR, "fail to read from write cache", KR(ret), K(fd_), K(page_id), K(leaf_page_offset),
                                                                K(level_new_pages), K(level_page_range_array_));
    } else if (OB_FAIL(read_page_header_(leaf_page_buff, page_header))) {
      STORAGE_LOG(WARN, "fail to read header", KR(ret), K(fd_), KP(leaf_page_buff));
    } else if (0 == page_header.item_num_) {
      //maybe the item have been cleared (corresponds to an unfilled data page)
      //or maybe this is a newly allocated meta page
      //so "0 == item_num"
      //but we do not need to rewrite page info (change virtual_page_id)
      STORAGE_LOG(INFO, "item_num is 0", KR(ret), K(fd_), KP(leaf_page_buff), K(page_id));
    } else {
      //check last data item
      ObSharedNothingTmpFileDataItem origin_last_item;
      if (OB_FAIL(read_item_(leaf_page_buff, page_header.item_num_ - 1, origin_last_item))) {
        STORAGE_LOG(WARN, "fail to read item", KR(ret), K(fd_), KP(leaf_page_buff), K(page_header));
      } else if (OB_UNLIKELY(data_items.at(0).virtual_page_id_ != origin_last_item.virtual_page_id_ + origin_last_item.physical_page_num_)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "unexpected data_items or origin_last_item", KR(ret), K(fd_), K(data_items), K(origin_last_item));
      }
    }
    if (OB_SUCC(ret)) {
      if (!level_page_range_array_.empty() && OB_FAIL(level_origin_page_write_counts.push_back(count))) {
        STORAGE_LOG(WARN, "fail to push back", KR(ret), K(fd_), K(count));
      } else if (page_header.item_num_ < MAX_PAGE_DATA_ITEM_NUM) {
        if (OB_FAIL(write_items_(leaf_page_buff, data_items, 0, count))) {
          STORAGE_LOG(WARN, "fail to write items", KR(ret), K(fd_), KP(leaf_page_buff), K(data_items));
        } else if (OB_FAIL(wbp_->notify_dirty(fd_, page_id, leaf_page_offset))) {
          STORAGE_LOG(ERROR, "fail to notify dirty for meta", KR(ret), K(fd_), K(page_id), K(leaf_page_offset));
        } else {
          write_count += count;
          if (!level_page_range_array_.empty()) {
            level_origin_page_write_counts.at(0) = count;
          }
        }
        if (OB_SUCC(ret)) {
          //print page content
          ObSharedNothingTmpFileTreePageHeader tmp_page_header;
          ObSharedNothingTmpFileDataItem first_item;
          ObSharedNothingTmpFileDataItem last_item;
          read_page_simple_content_(leaf_page_buff, tmp_page_header, first_item, last_item);
          STORAGE_LOG(DEBUG, "dump tree page", KR(ret), K(fd_), K(page_id), K(leaf_page_offset),
                            KP(leaf_page_buff), K(tmp_page_header), K(first_item), K(last_item));
        }
      }
    }
  }
  return ret;
}

int ObSharedNothingTmpFileMetaTree::add_new_page_and_fill_items_at_leaf_(
    const ObIArray<ObSharedNothingTmpFileDataItem> &data_items,
    int64_t &write_count,
    ObSharedNothingTmpFileMetaItem &meta_item,
    ObIArray<ObSEArray<uint32_t, 2>> &level_new_pages)
{
  int ret = OB_SUCCESS;
  meta_item.reset();
  uint32_t new_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  char *new_page_buff = NULL;
  int16_t count = 0;
  int32_t level_page_index = 0;
  if (!level_page_range_array_.empty()) {
    level_page_index += (level_page_range_array_.at(0).evicted_page_num_ +
                          level_page_range_array_.at(0).cached_page_num_);
  }
  if (!level_new_pages.empty()) {
    level_page_index += level_new_pages.at(0).count();
  }
  ObTmpFilePageUniqKey leaf_page_offset(0, level_page_index);
  if (OB_UNLIKELY(!level_new_pages.empty() && 1 != level_new_pages.count())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid argument", KR(ret), K(fd_), K(level_new_pages));
  } else if (OB_UNLIKELY(data_items.count() <= write_count || !data_items.at(write_count).is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid argument", KR(ret), K(fd_), K(data_items), K(write_count));
  } else if (level_new_pages.empty()) {
    ObSEArray<uint32_t, 2> new_pages;
    if (OB_FAIL(level_new_pages.push_back(new_pages))) {
      STORAGE_LOG(WARN, "fail to push back", KR(ret), K(fd_), K(new_pages));
    }
  }
  if (FAILEDx(level_new_pages.at(0).push_back(ObTmpFileGlobal::INVALID_PAGE_ID))) {
    STORAGE_LOG(WARN, "fail to push back", KR(ret), K(fd_));
  } else if (OB_FAIL(wbp_->alloc_page(fd_, leaf_page_offset, new_page_id, new_page_buff))) {
    STORAGE_LOG(WARN, "fail to alloc page from write cache", KR(ret), K(fd_), K(leaf_page_offset),
                                              K(level_new_pages), K(level_page_range_array_));
  } else if (FALSE_IT(stat_info_.meta_page_alloc_cnt_++)) {
  } else if (OB_FAIL(init_page_header_(new_page_buff, 0 /*level*/))) {
    STORAGE_LOG(WARN, "fail to init page header", KR(ret), K(fd_), KP(new_page_buff));
  } else if (OB_FAIL(write_items_(new_page_buff, data_items, write_count/*begin_index*/, count))) {
    STORAGE_LOG(WARN, "fail to write items", KR(ret), K(fd_), KP(new_page_buff), K(data_items), K(write_count));
  } else if (OB_FAIL(wbp_->notify_dirty(fd_, new_page_id, leaf_page_offset))) {
    STORAGE_LOG(ERROR, "fail to notify dirty for meta", KR(ret), K(fd_), K(new_page_id), K(leaf_page_offset));
  } else {
    meta_item.page_level_ = 0;
    meta_item.buffer_page_id_ = new_page_id;
    meta_item.virtual_page_id_ = data_items.at(write_count).virtual_page_id_;
    write_count += count;
    level_new_pages.at(0).at(level_new_pages.at(0).count() - 1) = new_page_id;
  }
  if (OB_SUCC(ret)) {
    //print page content
    ObSharedNothingTmpFileTreePageHeader tmp_page_header;
    ObSharedNothingTmpFileDataItem first_item;
    ObSharedNothingTmpFileDataItem last_item;
    read_page_simple_content_(new_page_buff, tmp_page_header, first_item, last_item);
    STORAGE_LOG(INFO, "dump tree page", KR(ret), K(fd_), K(new_page_id), K(leaf_page_offset),
                              KP(new_page_buff), K(tmp_page_header), K(first_item), K(last_item));
  }
  return ret;
}

int ObSharedNothingTmpFileMetaTree::cascade_modification_at_internal_(
    const ObIArray<ObSharedNothingTmpFileMetaItem> &new_leaf_page_infos,
    ObIArray<int16_t> &level_origin_page_write_counts,
    ObIArray<ObSEArray<uint32_t, 2>> &level_new_pages)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(new_leaf_page_infos.empty()
                  || level_new_pages.empty())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid argument", KR(ret), K(fd_), K(new_leaf_page_infos), K(level_new_pages));
  } else {
    ObSEArray<ObSharedNothingTmpFileMetaItem, 1> prev_meta_items;
    ObSEArray<ObSharedNothingTmpFileMetaItem, 1> meta_items;
    ObSharedNothingTmpFileMetaItem meta_item;
    const int16_t origin_level_count = level_page_range_array_.count();
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
                 OB_FAIL(try_to_fill_rightmost_internal_page_(prev_meta_items, cur_level, write_count,
                                                level_origin_page_write_counts))) {
        STORAGE_LOG(WARN, "fail to try to fill rightmost internal page", KR(ret), K(fd_), K(prev_meta_items),
                                          K(cur_level), K(level_origin_page_write_counts));
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
            if (OB_FAIL(add_new_page_and_fill_items_at_internal_(prev_meta_items, cur_level, write_count,
                                                                              meta_item, level_new_pages))) {
              STORAGE_LOG(WARN, "fail to add new page and fill items at internal", KR(ret), K(fd_),
                                                    K(prev_meta_items), K(cur_level), K(write_count));
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
                  || page_level >= level_page_range_array_.count()
                  || page_level != level_origin_page_write_counts.count())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid argument", KR(ret), K(fd_), K(meta_items), K(page_level), K(level_page_range_array_),
                                                                            K(level_origin_page_write_counts));
  } else {
    const int16_t MAX_PAGE_META_ITEM_NUM =
            MIN(MAX_PAGE_ITEM_COUNT, (ObTmpFileGlobal::PAGE_SIZE - PAGE_HEADER_SIZE) / sizeof(meta_items.at(0)));
    uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
    char *internal_page_buff = NULL;
    ObSharedNothingTmpFileTreePageHeader page_header;
    int16_t count = 0;
    uint32_t page_id = level_page_range_array_[page_level].end_page_id_;
    int32_t level_page_index = level_page_range_array_[page_level].evicted_page_num_ +
                                  level_page_range_array_[page_level].cached_page_num_ - 1;
    ObTmpFilePageUniqKey internal_page_offset(page_level, level_page_index);
    //only a writing thread.
    if (OB_FAIL(wbp_->read_page(fd_, page_id, internal_page_offset, internal_page_buff, next_page_id))) {
      STORAGE_LOG(ERROR, "fail to read from write cache", KR(ret), K(fd_), K(page_id), K(internal_page_offset));
    } else if (OB_FAIL(read_page_header_(internal_page_buff, page_header))) {
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
      } else if (OB_FAIL(wbp_->notify_dirty(fd_, page_id, internal_page_offset))) {
        STORAGE_LOG(ERROR, "fail to notify dirty for meta", KR(ret), K(fd_), K(page_id), K(internal_page_offset));
      } else {
        write_count += count;
        level_origin_page_write_counts.at(page_level) = count;
        //print page content
        ObSharedNothingTmpFileTreePageHeader tmp_page_header;
        ObSharedNothingTmpFileMetaItem first_item;
        ObSharedNothingTmpFileMetaItem last_item;
        read_page_simple_content_(internal_page_buff, tmp_page_header, first_item, last_item);
        STORAGE_LOG(INFO, "dump tree page", KR(ret), K(fd_), K(page_id), K(internal_page_offset),
                                  KP(internal_page_buff), K(tmp_page_header), K(first_item), K(last_item));
      }
    }
  }
  return ret;
}

int ObSharedNothingTmpFileMetaTree::add_new_page_and_fill_items_at_internal_(
    const ObIArray<ObSharedNothingTmpFileMetaItem> &meta_items,
    const int16_t page_level,
    int64_t &write_count,
    ObSharedNothingTmpFileMetaItem &meta_item,
    ObIArray<ObSEArray<uint32_t, 2>> &level_new_pages)
{
  int ret = OB_SUCCESS;
  uint32_t new_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  char *new_page_buff = NULL;
  int16_t count = 0;
  int32_t level_page_index = 0;
  if (page_level < level_page_range_array_.count()) {
    level_page_index += (level_page_range_array_.at(page_level).evicted_page_num_ +
                          level_page_range_array_.at(page_level).cached_page_num_);
  }
  if (page_level < level_new_pages.count()) {
    level_page_index += level_new_pages.at(page_level).count();
  }
  ObTmpFilePageUniqKey internal_page_offset(page_level, level_page_index);
  if (OB_UNLIKELY(meta_items.count() <= write_count || !meta_items.at(write_count).is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid argument", KR(ret), K(fd_), K(meta_items), K(write_count));
  } else if (OB_UNLIKELY(level_new_pages.count() != page_level
                          && level_new_pages.count() != page_level + 1)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "unexpected level_new_pages", KR(ret), K(fd_), K(page_level), K(level_new_pages));
  } else if (level_new_pages.count() == page_level) {
    ObSEArray<uint32_t, 2> new_pages;
    if (OB_FAIL(level_new_pages.push_back(new_pages))) {
      STORAGE_LOG(WARN, "fail to push back", KR(ret), K(fd_), K(new_pages));
    }
  }
  if (FAILEDx(level_new_pages.at(page_level).push_back(ObTmpFileGlobal::INVALID_PAGE_ID))) {
    STORAGE_LOG(WARN, "fail to push back", KR(ret), K(fd_), K(page_level));
  } else if (OB_FAIL(wbp_->alloc_page(fd_, internal_page_offset, new_page_id, new_page_buff))) {
    STORAGE_LOG(WARN, "fail to alloc page from write cache", KR(ret), K(fd_), K(internal_page_offset));
  } else if (FALSE_IT(stat_info_.meta_page_alloc_cnt_++)) {
  } else if (OB_FAIL(init_page_header_(new_page_buff, page_level))) {
    STORAGE_LOG(WARN, "fail to init page header", KR(ret), K(fd_), KP(new_page_buff));
  } else if (OB_FAIL(write_items_(new_page_buff, meta_items, write_count/*begin_index*/, count))) {
    STORAGE_LOG(WARN, "fail to write items", KR(ret), K(fd_), KP(new_page_buff), K(meta_items), K(write_count));
  } else if (OB_FAIL(wbp_->notify_dirty(fd_, new_page_id, internal_page_offset))) {
    STORAGE_LOG(ERROR, "fail to notify dirty for meta", KR(ret), K(fd_), K(new_page_id), K(internal_page_offset));
  } else {
    meta_item.page_level_ = page_level;
    meta_item.buffer_page_id_ = new_page_id;
    meta_item.virtual_page_id_ = meta_items.at(write_count).virtual_page_id_;
    write_count += count;
    level_new_pages.at(page_level).at(level_new_pages.at(page_level).count() - 1) = new_page_id;
  }
  if (OB_SUCC(ret)) {
    //print page content
    ObSharedNothingTmpFileTreePageHeader tmp_page_header;
    ObSharedNothingTmpFileMetaItem first_item;
    ObSharedNothingTmpFileMetaItem last_item;
    read_page_simple_content_(new_page_buff, tmp_page_header, first_item, last_item);
    STORAGE_LOG(INFO, "dump tree page", KR(ret), K(fd_), K(new_page_id), K(internal_page_offset),
                              KP(new_page_buff), K(tmp_page_header), K(first_item), K(last_item));
  }
  return ret;
}

int ObSharedNothingTmpFileMetaTree::finish_insert_(
    const int return_ret,
    ObIArray<int16_t> &level_origin_page_write_counts,
    ObIArray<ObSEArray<uint32_t, 2>> &level_new_pages)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(level_origin_page_write_counts.count() > level_page_range_array_.count())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "unexpected new record level page infos", KR(ret), K(fd_),
                          K(level_origin_page_write_counts), K(level_page_range_array_));
  } else if (OB_SUCCESS == return_ret) {
    if (OB_FAIL(level_page_range_array_.reserve(level_new_pages.count()))) {
      STORAGE_LOG(WARN, "fail to reserve for level_page_range_array_", KR(ret), K(fd_), K(level_new_pages.count()));
    }
    ARRAY_FOREACH_N(level_new_pages, level, level_cnt) {
      ARRAY_FOREACH_N(level_new_pages.at(level), i, new_page_cnt) {
        const uint32_t new_page_id = level_new_pages.at(level).at(i);
        if (0 == i && level_page_range_array_.count() <= level) {
          if (OB_FAIL(level_page_range_array_.push_back(
                  LevelPageRangeInfo(new_page_id, ObTmpFileGlobal::INVALID_PAGE_ID, new_page_id, 1, 0, 0)))) {
            STORAGE_LOG(ERROR, "fail to push back", KR(ret), K(fd_), K(new_page_id));
          }
        } else if (OB_UNLIKELY(level_page_range_array_.count() <= level)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(ERROR, "unexpected level_page_range_array_", KR(ret), K(fd_), K(level_page_range_array_), K(level));
        } else {
          uint32_t end_page_id_in_array = level_page_range_array_[level].end_page_id_;
          int32_t level_prev_page_index = level_page_range_array_[level].evicted_page_num_ +
                                              level_page_range_array_[level].cached_page_num_ - 1;
          if (ObTmpFileGlobal::INVALID_PAGE_ID != end_page_id_in_array
              && OB_FAIL(wbp_->link_page(fd_, new_page_id, end_page_id_in_array,
                                         ObTmpFilePageUniqKey(level, level_prev_page_index)))) {
            STORAGE_LOG(ERROR, "fail to link page in write cache", KR(ret), K(fd_), K(level_page_range_array_),
                                                                            K(new_page_id), K(level));
          } else {
            if (ObTmpFileGlobal::INVALID_PAGE_ID == end_page_id_in_array) {
              level_page_range_array_[level].start_page_id_ = new_page_id;
            }
            level_page_range_array_[level].end_page_id_ = new_page_id;
            level_page_range_array_[level].cached_page_num_++;
          }
        }
      }
    }
    if (OB_SUCC(ret) && !data_item_array_.empty()) {
      data_item_array_.reset();
    }
  } else { //fail
    //rollback
    ARRAY_FOREACH_N(level_origin_page_write_counts, i, cnt) {
      const int16_t remove_cnt = level_origin_page_write_counts.at(i);
      if (0 != remove_cnt) {
        char *page_buff = NULL;
        uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
        uint32_t end_page_id = level_page_range_array_[i].end_page_id_;
        int32_t level_page_index = level_page_range_array_[i].evicted_page_num_ +
                                      level_page_range_array_[i].cached_page_num_ - 1;
        ObTmpFilePageUniqKey page_key(i, level_page_index);
        if (OB_FAIL(wbp_->read_page(fd_, end_page_id, page_key, page_buff, next_page_id))) {
          STORAGE_LOG(ERROR, "fail to read from write cache", KR(ret), K(fd_), K(end_page_id), K(page_key));
        } else if (OB_FAIL(remove_page_item_from_tail_(page_buff, remove_cnt))) {
          STORAGE_LOG(WARN, "fail to remove page item from tail", KR(ret), K(fd_), KP(page_buff), K(remove_cnt));
        }
      }
    }
    ARRAY_FOREACH_N(level_new_pages, level, level_cnt) {
      int32_t level_page_index = 0;
      if (level_page_range_array_.count() > level) {
        level_page_index += (level_page_range_array_[level].evicted_page_num_ +
                              level_page_range_array_[level].cached_page_num_);
      }
      ARRAY_FOREACH_N(level_new_pages.at(level), i, new_page_cnt) {
        const uint32_t new_page_id = level_new_pages.at(level).at(i);
        if (ObTmpFileGlobal::INVALID_PAGE_ID != new_page_id) {
          uint32_t unused_next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
          ObTmpFilePageUniqKey page_key(level, level_page_index + i);
          if (OB_FAIL(wbp_->free_page(fd_, new_page_id, page_key, unused_next_page_id))) {
            STORAGE_LOG(ERROR, "fail to free meta page in write cache", KR(ret), K(fd_), K(new_page_id), K(page_key));
          } else {
            stat_info_.meta_page_free_cnt_++;
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (level_page_range_array_.empty()) {
        if (OB_UNLIKELY(stat_info_.meta_page_alloc_cnt_ != stat_info_.meta_page_free_cnt_)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(ERROR, "unexpected stat_info_", KR(ret), K(fd_), K(return_ret), K(stat_info_));
        } else {
          root_item_.reset();
        }
      }
    }
    STORAGE_LOG(INFO, "fail to insert, finish rollback to before", KR(ret), K(return_ret), KPC(this));
  }
  return ret;
}

int ObSharedNothingTmpFileMetaTree::search_data_items(
    const int64_t start_offset,
    const int64_t read_size,
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
      STORAGE_LOG(WARN, "fail to get data items from array", KR(ret), K(end_offset), K(offset), KPC(this));
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
      ObTmpPageValueHandle p_handle;
      if (OB_FAIL(get_page_(meta_item, level_page_index, page_buff, p_handle))) {
        STORAGE_LOG(WARN, "fail to get page", KR(ret), K(meta_item), K(level_page_index), KPC(this));
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
      p_handle.reset();
    }
    if (OB_SUCC(ret)) {
      if (0 == meta_item.page_level_) {
        if (OB_FAIL(get_items_of_leaf_page_(meta_item, level_page_index, end_offset,
                true /*need read from specified item index*/, offset, data_items))) {
          STORAGE_LOG(WARN, "fail to get items of leaf page", KR(ret), K(meta_item),
                                      K(level_page_index), K(end_offset), K(offset), KPC(this));
        } else if (offset < end_offset
            && OB_FAIL(backtrace_search_data_items_(end_offset, offset, search_path, data_items))) {
          STORAGE_LOG(WARN, "fail to backtrace search data items", KR(ret),
                                    K(end_offset), K(offset), K(search_path), KPC(this));
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
    common::ObIArray<ObSharedNothingTmpFileDataItem> &data_items)
{
  int ret = OB_SUCCESS;
  char *page_buff = NULL;
  int16_t item_index = -1;
  int64_t tmp_offset = -1;
  const int64_t target_virtual_page_id = cur_offset / ObTmpFileGlobal::PAGE_SIZE;
  ObSharedNothingTmpFileDataItem data_item;
  ObSharedNothingTmpFileTreePageHeader page_header;
  ObTmpPageValueHandle p_handle;
  if (OB_FAIL(get_page_(page_info, level_page_index, page_buff, p_handle))) {
    STORAGE_LOG(WARN, "fail to get page", KR(ret), K(fd_), K(page_info), K(level_page_index));
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
  p_handle.reset();
  return ret;
}

int ObSharedNothingTmpFileMetaTree::backtrace_search_data_items_(
    const int64_t end_offset,
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
                                      end_offset, reach_last_item, meta_items))) {
        STORAGE_LOG(WARN, "fail to get items of internal page", KR(ret), K(fd_), K(page_info),
                      K(level_page_index), K(cur_item_index), K(end_offset));
      } else if (1 == page_info.page_level_) {
        //process level_0
        int64_t i = 0;
        const int64_t cnt = meta_items.count();
        const int32_t start_leaf_level_page_index = level_page_index * FULL_PAGE_META_ITEM_NUM + cur_item_index;
        for (; OB_SUCC(ret) && i < cnt && offset < end_offset; i++) {
          const ObSharedNothingTmpFileMetaItem &leaf_page_info = meta_items.at(i);
          bool need_lock = ((i + 1 == cnt) && reach_last_item);
          if (OB_FAIL(get_items_of_leaf_page_(leaf_page_info, start_leaf_level_page_index + i, end_offset,
                          false /*need read from specified item index*/, offset, data_items))) {
            STORAGE_LOG(WARN, "fail to get items of leaf page", KR(ret), K(fd_), K(leaf_page_info),
                                K(start_leaf_level_page_index + i), K(end_offset), K(need_lock), K(offset));
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
    ObIArray<ObSharedNothingTmpFileMetaItem> &meta_items)
{
  int ret = OB_SUCCESS;
  reach_last_item = false;
  meta_items.reset();
  int16_t cur_item_index = item_index;
  ObSharedNothingTmpFileTreePageHeader page_header;
  ObSharedNothingTmpFileMetaItem meta_item;
  char *page_buff = NULL;
  ObTmpPageValueHandle p_handle;
  //when truncate, the page must in read/write cache(because we already hold a p_handle about this page if in read cache)
  if (OB_FAIL(get_page_(page_info, level_page_index, page_buff, p_handle))) {
    STORAGE_LOG(WARN, "fail to get page", KR(ret), K(fd_), K(page_info), K(level_page_index));
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
  p_handle.reset();
  if (OB_SUCC(ret) && cur_item_index == page_header.item_num_) {
    reach_last_item = true;
  }
  return ret;
}

int ObSharedNothingTmpFileMetaTree::flush_meta_pages_for_block(
    const int64_t block_index,
    const ObTmpFileTreeEvictType flush_type,
    char *block_buff,
    int64_t &write_offset,
    ObTmpFileTreeFlushContext &flush_context,
    ObIArray<ObTmpFileTreeIOInfo> &tree_io_array)
{
  int ret = OB_SUCCESS;
  tree_io_array.reset();
  SpinWLockGuard guard(lock_);
  if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX == block_index
                  || ObTmpFileTreeEvictType::INVALID == flush_type
                  || NULL == block_buff
                  || 0 != write_offset % ObTmpFileGlobal::PAGE_SIZE
                  || level_page_range_array_.count() >= INT16_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid argument", KR(ret), K(block_index), K(flush_type),
                          KP(block_buff), K(write_offset), KPC(this));
  } else if (!root_item_.is_valid()) {
    if (!level_page_range_array_.empty()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected level_page_range_array_", KR(ret), KPC(this));
    } else {
      flush_context.is_meta_reach_end_ = true;
    }
  } else {
    int16_t level = 0;
    bool need_use_context = false;
    bool need_flush = true;
    const int16_t level_cnt = level_page_range_array_.count();
    ObTmpFileTreeIOInfo tree_io_info;
    if (flush_context.is_valid()) {
      if (flush_context.tree_epoch_ != tree_epoch_) {
        STORAGE_LOG(INFO, "the tree_epoch_ in flush context is not equal to current tree_epoch_",
            K(fd_), K(flush_context), K(tree_epoch_));
        need_flush = false;
        flush_context.is_meta_reach_end_ = true;
      } else {
        //flush context means: in this round of flushing,
        //  the meta tree has already flushed some pages in previous blocks,
        //  so we need "flush context" to avoid repeated flushing.
        level = flush_context.last_flush_level_;
        need_use_context = true;
        if (last_truncate_leaf_info_.is_valid()) {
          if (OB_UNLIKELY(level_cnt <= level)) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(ERROR, "unexpected item_index_arr", KR(ret), K(level), KPC(this));
          } else if (flush_context.last_flush_page_index_in_level_ < level_page_range_array_.at(level).evicted_page_num_) {
            need_flush = false;
            flush_context.is_meta_reach_end_ = true;
          }
        }
      }
    }
    if (OB_SUCC(ret) && need_flush) {
      if (OB_FAIL(tree_io_array.reserve(level_cnt))) {
        STORAGE_LOG(WARN, "fail to reserve", KR(ret), K(fd_), K(level_cnt));
      }
      while (OB_SUCC(ret)
            && level < level_cnt
            && write_offset < ObTmpFileGlobal::SN_BLOCK_SIZE)
      {
        uint32_t flush_start_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
        int32_t level_page_index = -1;
        if (need_use_context) {
          level_page_index = flush_context.last_flush_page_index_in_level_;
          ObTmpFilePageUniqKey page_key(level, level_page_index);
          if (wbp_->is_dirty(fd_, flush_context.last_flush_page_id_, page_key)) {
            //do nothing, we just skip this level.
          } else if (OB_FAIL(wbp_->get_next_page_id(fd_, flush_context.last_flush_page_id_,
                                                    page_key, flush_start_page_id))) {
            STORAGE_LOG(ERROR, "fail to get next meta page id", KR(ret), K(fd_), K(flush_context), K(page_key));
          } else {
            level_page_index++;
          }
          //only used the first time
          need_use_context = false;
        } else {
          uint32_t flushed_page_id = level_page_range_array_[level].flushed_end_page_id_;
          level_page_index = level_page_range_array_[level].flushed_page_num_;
          if (ObTmpFileGlobal::INVALID_PAGE_ID == flushed_page_id) {
            flush_start_page_id = level_page_range_array_[level].start_page_id_;
          } else {
            level_page_index--;
            ObTmpFilePageUniqKey page_key(level, level_page_index);
            if (wbp_->is_dirty(fd_, flushed_page_id, page_key)) {
              //flushed page -> write cache
              //we only write rightmost page, so pages before flushed_end_page_id_ can not be dirty.
              flush_start_page_id = flushed_page_id;
            } else if (OB_FAIL(wbp_->get_next_page_id(fd_, flushed_page_id, page_key, flush_start_page_id))) {
              STORAGE_LOG(ERROR, "fail to get next meta page id", KR(ret), K(fd_), K(flushed_page_id), K(page_key));
            } else {
              level_page_index++;
            }
          }
        }
        if (OB_SUCC(ret) && ObTmpFileGlobal::INVALID_PAGE_ID != flush_start_page_id) {
          tree_io_info.reset();
          tree_io_info.tree_epoch_ = tree_epoch_;
          tree_io_info.block_index_ = block_index;
          if (0 == level && OB_FAIL(flush_leaf_pages_(flush_start_page_id, level_page_index, flush_type,
                                        block_buff, write_offset, tree_io_info))) {
            STORAGE_LOG(WARN, "fail to flush leaf pages", KR(ret), K(flush_start_page_id),
                                K(level_page_index), K(flush_type), KP(block_buff), K(write_offset), KPC(this));
          } else if (0 < level && OB_FAIL(flush_internal_pages_(flush_start_page_id, level, level_page_index,
                                            flush_type, block_buff, write_offset, tree_io_info))) {
            STORAGE_LOG(WARN, "fail to flush internal pages", KR(ret), K(flush_start_page_id),
                        K(level), K(level_page_index), K(flush_type), KP(block_buff), K(write_offset), KPC(this));
          } else if (0 == tree_io_info.flush_nums_) {
            //do nothing
            STORAGE_LOG(INFO, "no meta page flush in this level", KR(ret), K(fd_), K(level), K(level_page_range_array_));
          } else if (OB_UNLIKELY(!tree_io_info.is_valid())) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(ERROR, "unexpected tree_io_info", KR(ret), K(tree_io_info), KPC(this));
          //set the page flush information on the parent node, so that we can flush parent pages in advance.
          } else if (OB_FAIL(modify_meta_items_at_parent_level_(tree_io_info, level_page_index))) {
            STORAGE_LOG(WARN, "fail to modify meta items at parent level", KR(ret),
                                              K(tree_io_info), K(level_page_index), KPC(this));
          } else if (OB_FAIL(tree_io_array.push_back(tree_io_info))) {
            STORAGE_LOG(ERROR, "fail to push back", KR(ret), K(fd_), K(tree_io_info));
          } else {
            flush_context.tree_epoch_ = tree_epoch_;
            flush_context.last_flush_level_ = level;
            flush_context.last_flush_page_id_ = tree_io_info.flush_end_page_id_;
            flush_context.last_flush_page_index_in_level_ = level_page_index + tree_io_info.flush_nums_ - 1;
            stat_info_.meta_page_flushing_cnt_ += tree_io_info.flush_nums_;
            stat_info_.all_type_page_flush_cnt_ += tree_io_info.flush_nums_;
          }
        }
        if (OB_SUCC(ret) && level_cnt == level + 1) {
          flush_context.is_meta_reach_end_ = true;
        }
        level++;
      }
    }
  }
  STORAGE_LOG(INFO, "finish flush meta pages for block", KR(ret), K(fd_), K(tree_io_array));
  return ret;
}

int ObSharedNothingTmpFileMetaTree::flush_leaf_pages_(
    const uint32_t flush_start_page_id,
    const int32_t start_page_index_in_level,
    const ObTmpFileTreeEvictType flush_type,
    char *block_buff,
    int64_t &write_offset,
    ObTmpFileTreeIOInfo &tree_io_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_PAGE_ID == flush_start_page_id
                  || 0 > start_page_index_in_level
                  || 0 != write_offset % ObTmpFileGlobal::PAGE_SIZE
                  || level_page_range_array_.empty())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid argument", KR(ret), K(fd_), K(flush_start_page_id), K(start_page_index_in_level),
                                                    K(write_offset), K(level_page_range_array_));
  } else {
    tree_io_info.physical_start_page_id_ = write_offset / ObTmpFileGlobal::PAGE_SIZE;
    tree_io_info.page_level_ = 0;
    uint32_t cur_page_id = flush_start_page_id;
    const uint32_t end_page_id = level_page_range_array_[0].end_page_id_;
    int32_t page_index_in_level = start_page_index_in_level;
    while (OB_SUCC(ret)
          && ObTmpFileGlobal::INVALID_PAGE_ID != cur_page_id
          && write_offset + ObTmpFileGlobal::PAGE_SIZE <= ObTmpFileGlobal::SN_BLOCK_SIZE
          && (ObTmpFileTreeEvictType::FULL == flush_type || end_page_id != cur_page_id)) {
      char *page_buff = NULL;
      uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
      ObTmpFilePageUniqKey page_key(0, page_index_in_level);
      if (OB_UNLIKELY(!wbp_->is_dirty(fd_, cur_page_id, page_key))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "unexpected page state", KR(ret), K(fd_), K(cur_page_id));
      } else if (OB_FAIL(wbp_->read_page(fd_, cur_page_id, page_key, page_buff, next_page_id))) {
        STORAGE_LOG(ERROR, "fail to read from write cache", KR(ret), K(fd_), K(cur_page_id), K(page_key));
      //change page state to write back
      } else if (OB_FAIL(wbp_->notify_write_back(fd_, cur_page_id, page_key))) {
        STORAGE_LOG(ERROR, "fail to notify write back for meta", KR(ret), K(fd_), K(cur_page_id), K(page_key));
      } else {
        ObTmpPageCacheKey cache_key(tree_io_info.block_index_,
                                    write_offset / ObTmpFileGlobal::PAGE_SIZE, MTL_ID());
        ObTmpPageCacheValue cache_value(page_buff);
        MEMCPY(block_buff + write_offset, page_buff, ObTmpFileGlobal::PAGE_SIZE);
        if (OB_FAIL(calc_and_set_page_checksum_(block_buff + write_offset))) {
          STORAGE_LOG(WARN, "fail to calc and set page checksum", KR(ret), K(fd_), KP(block_buff + write_offset));
        } else {
          ObTmpPageCacheValue cache_value(block_buff + write_offset);
          ObTmpPageCache::get_instance().try_put_page_to_cache(cache_key, cache_value);
          write_offset += ObTmpFileGlobal::PAGE_SIZE;
          if (flush_start_page_id == cur_page_id) {
            tree_io_info.flush_start_page_id_ = cur_page_id;
            tree_io_info.flush_start_level_page_index_ = page_index_in_level;
          }
          tree_io_info.flush_end_page_id_ = cur_page_id;
          tree_io_info.flush_nums_++;
          cur_page_id = next_page_id;
          page_index_in_level++;
        }
      }
    }
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

int ObSharedNothingTmpFileMetaTree::flush_internal_pages_(
    const uint32_t flush_start_page_id,
    const int16_t level,
    const int32_t start_page_index_in_level,
    const ObTmpFileTreeEvictType flush_type,
    char *block_buff,
    int64_t &write_offset,
    ObTmpFileTreeIOInfo &tree_io_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_PAGE_ID == flush_start_page_id
                  || level >= level_page_range_array_.count()
                  || level < 1
                  || 0 > start_page_index_in_level
                  || ObTmpFileTreeEvictType::INVALID == flush_type
                  || NULL == block_buff
                  || 0 != write_offset % ObTmpFileGlobal::PAGE_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid argument", KR(ret), K(fd_), K(flush_start_page_id), K(level), K(start_page_index_in_level),
                    K(flush_type), KP(block_buff), K(level_page_range_array_), K(write_offset));
  } else {
    const int16_t FULL_PAGE_META_ITEM_NUM =
            MIN(MAX_PAGE_ITEM_COUNT, (ObTmpFileGlobal::PAGE_SIZE - PAGE_HEADER_SIZE) / sizeof(ObSharedNothingTmpFileMetaItem));
    tree_io_info.physical_start_page_id_ = write_offset / ObTmpFileGlobal::PAGE_SIZE;
    tree_io_info.page_level_ = level;
    uint32_t cur_page_id = flush_start_page_id;
    const uint32_t end_page_id = level_page_range_array_[level].end_page_id_;
    ObSharedNothingTmpFileMetaItem last_item;
    int32_t page_index_in_level = start_page_index_in_level;
    while (OB_SUCC(ret)
          && ObTmpFileGlobal::INVALID_PAGE_ID != cur_page_id
          && write_offset + ObTmpFileGlobal::PAGE_SIZE <= ObTmpFileGlobal::SN_BLOCK_SIZE
          && (ObTmpFileTreeEvictType::FULL == flush_type || end_page_id != cur_page_id)) {
      char * page_buff = NULL;
      uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
      ObTmpFilePageUniqKey page_key(level, page_index_in_level);
      if (OB_UNLIKELY(!wbp_->is_dirty(fd_, cur_page_id, page_key))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "unexpected page state", KR(ret), K(fd_), K(cur_page_id));
      } else if (OB_FAIL(wbp_->read_page(fd_, cur_page_id, page_key, page_buff, next_page_id))) {
        STORAGE_LOG(ERROR, "fail to read from write cache", KR(ret), K(fd_), K(cur_page_id), K(page_key));
      } else {
        //check whether the page is satisfied for flush
        last_item.reset();
        ObSharedNothingTmpFileTreePageHeader page_header;
        int32_t rightmost_child_page_index = -1;
        if (OB_FAIL(read_page_header_(page_buff, page_header))) {
          STORAGE_LOG(WARN, "fail to read page header", KR(ret), K(fd_), KP(page_buff));
        } else if (page_header.item_num_ <= 0) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(ERROR, "unexpected item_num", KR(ret), K(fd_), K(page_header));
        } else if (OB_FAIL(read_item_(page_buff, page_header.item_num_ - 1/*item_index*/, last_item))) {
          STORAGE_LOG(WARN, "fail to read item", KR(ret), K(fd_), KP(page_buff), K(page_header));
        } else if (FALSE_IT(rightmost_child_page_index = page_index_in_level * FULL_PAGE_META_ITEM_NUM + page_header.item_num_ - 1)) {
        } else if (!is_page_flushed(last_item)
                   || (is_page_in_write_cache(last_item)
                       && wbp_->is_dirty(fd_, last_item.buffer_page_id_, ObTmpFilePageUniqKey(level - 1, rightmost_child_page_index)))) {
          //we can be sure that the following pages in this level will not satisfy
          break;
        //change page state to write back
        } else if (OB_FAIL(wbp_->notify_write_back(fd_, cur_page_id, page_key))) {
          STORAGE_LOG(ERROR, "fail to notify write back for meta", KR(ret), K(fd_), K(cur_page_id), K(page_key));
        } else {
          ObTmpPageCacheKey cache_key(tree_io_info.block_index_,
                                      write_offset / ObTmpFileGlobal::PAGE_SIZE, MTL_ID());
          MEMCPY(block_buff + write_offset, page_buff, ObTmpFileGlobal::PAGE_SIZE);
          if (OB_FAIL(modify_child_pages_location(block_buff + write_offset))) {
            STORAGE_LOG(WARN, "fail to modify child pages location", KR(ret), K(fd_), KP(block_buff + write_offset));
          } else if (OB_FAIL(calc_and_set_page_checksum_(block_buff + write_offset))) {
            STORAGE_LOG(WARN, "fail to calc and set page checksum", KR(ret), K(fd_), KP(block_buff + write_offset));
          } else {
            ObTmpPageCacheValue cache_value(block_buff + write_offset);
            ObTmpPageCache::get_instance().try_put_page_to_cache(cache_key, cache_value);
            write_offset += ObTmpFileGlobal::PAGE_SIZE;
            if (flush_start_page_id == cur_page_id) {
              tree_io_info.flush_start_page_id_ = cur_page_id;
              tree_io_info.flush_start_level_page_index_ = page_index_in_level;
            }
            tree_io_info.flush_end_page_id_ = cur_page_id;
            tree_io_info.flush_nums_++;
            cur_page_id = next_page_id;
            page_index_in_level++;
          }
        }
      }
    }
  }
  return ret;
}

int ObSharedNothingTmpFileMetaTree::modify_child_pages_location(
    char *page_buff)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(page_buff)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid argument", KR(ret), K(fd_), KP(page_buff));
  } else {
    ObSharedNothingTmpFileTreePageHeader page_header;
    if (OB_FAIL(read_page_header_(page_buff, page_header))) {
      STORAGE_LOG(WARN, "fail to read page header", KR(ret), K(fd_), KP(page_buff));
    } else {
      int16_t item_index = 0;
      ObSharedNothingTmpFileMetaItem meta_item;
      while (OB_SUCC(ret)
              && item_index < page_header.item_num_) {
        meta_item.reset();
        if (OB_FAIL(read_item_(page_buff, item_index, meta_item))) {
          STORAGE_LOG(WARN, "fail to read item", KR(ret), K(fd_), KP(page_buff), K(item_index));
        } else if (ObTmpFileGlobal::INVALID_PAGE_ID == meta_item.buffer_page_id_) {
          //do nothing
        } else {
          meta_item.buffer_page_id_ = ObTmpFileGlobal::INVALID_PAGE_ID;
          if (OB_FAIL(rewrite_item_(page_buff, item_index, meta_item))) {
            STORAGE_LOG(WARN, "fail to rewrite item", KR(ret), K(fd_), KP(page_buff), K(item_index));
          }
        }
        item_index++;
      }
    }
  }
  return ret;
}

int ObSharedNothingTmpFileMetaTree::modify_meta_items_at_parent_level_(
    const ObTmpFileTreeIOInfo &tree_io,
    const int32_t start_page_index_in_level)
{
  int ret = OB_SUCCESS;
  const uint32_t cur_level = tree_io.page_level_ + 1;
  if (OB_UNLIKELY(cur_level > level_page_range_array_.count()
                  || !tree_io.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid argument", KR(ret), K(fd_), K(tree_io), K(level_page_range_array_));
  } else if (cur_level == level_page_range_array_.count()) {
    //this must be the root page being flushed
    if (OB_UNLIKELY(1 != tree_io.flush_nums_
                    || tree_io.flush_start_page_id_ != tree_io.flush_end_page_id_
                    || tree_io.flush_start_page_id_ != root_item_.buffer_page_id_
                    || 0 != start_page_index_in_level)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected tree_io", KR(ret), K(fd_), K(tree_io), K(root_item_), K(start_page_index_in_level));
    //NOTE: It doesn't matter if we release this page early, because under wlock.
    } else if (is_page_flushed(root_item_)
              && OB_FAIL(release_tmp_file_page_(root_item_.block_index_, root_item_.physical_page_id_, 1))) {
      STORAGE_LOG(WARN, "fail to release tmp file page", KR(ret), K(fd_), K(root_item_));
    } else {
      root_item_.block_index_ = tree_io.block_index_;
      root_item_.physical_page_id_ = tree_io.physical_start_page_id_;
    }
  } else {
    const int16_t FULL_PAGE_META_ITEM_NUM =
            MIN(MAX_PAGE_ITEM_COUNT, (ObTmpFileGlobal::PAGE_SIZE - PAGE_HEADER_SIZE) / sizeof(ObSharedNothingTmpFileMetaItem));
    int32_t child_level_page_index = start_page_index_in_level;
    uint32_t physical_page_id = tree_io.physical_start_page_id_;
    ObSharedNothingTmpFileMetaItem meta_item;
    bool is_end = false;
    bool has_find = false;
    uint32_t last_flushed_page_id = level_page_range_array_.at(cur_level).flushed_end_page_id_;
    //the child page of last meta item on last_flushed_page may be flushed again.
    uint32_t cur_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
    int32_t cur_level_page_index = level_page_range_array_.at(cur_level).flushed_page_num_;
    if (ObTmpFileGlobal::INVALID_PAGE_ID == last_flushed_page_id) {
      cur_page_id = level_page_range_array_.at(cur_level).start_page_id_;
    } else {
      cur_page_id = last_flushed_page_id;
      cur_level_page_index--;
    }
    while (OB_SUCC(ret)
            && ObTmpFileGlobal::INVALID_PAGE_ID != cur_page_id
            && !is_end) {
      int16_t item_index = 0;
      char *page_buff = NULL;
      uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
      ObSharedNothingTmpFileTreePageHeader page_header;
      ObTmpFilePageUniqKey page_key(cur_level, cur_level_page_index);
      //this upper layer page must be in write cache
      if (OB_FAIL(wbp_->read_page(fd_, cur_page_id, page_key, page_buff, next_page_id))) {
        STORAGE_LOG(ERROR, "fail to read from write cache", KR(ret), K(fd_), K(cur_page_id), K(page_key));
      } else if (OB_FAIL(read_page_header_(page_buff, page_header))) {
        STORAGE_LOG(WARN, "fail to read page header", KR(ret), K(fd_), KP(page_buff));
      } else if (OB_UNLIKELY(0 >= page_header.item_num_)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "unexpected page_header", KR(ret), K(fd_), K(page_header));
      } else {
        while (OB_SUCC(ret)
                && item_index < page_header.item_num_
                && !is_end) {
          meta_item.reset();
          if (OB_FAIL(read_item_(page_buff, item_index, meta_item))) {
            STORAGE_LOG(WARN, "fail to read item", KR(ret), K(fd_), KP(page_buff), K(item_index));
          } else {
            if (has_find || cur_level_page_index * FULL_PAGE_META_ITEM_NUM + item_index == child_level_page_index) {
              if (OB_UNLIKELY((!has_find && tree_io.flush_start_page_id_ != meta_item.buffer_page_id_)
                              || cur_level_page_index * FULL_PAGE_META_ITEM_NUM + item_index != child_level_page_index)) {
                ret = OB_ERR_UNEXPECTED;
                STORAGE_LOG(ERROR, "unexpected meta_item", KR(ret), K(fd_), K(has_find), K(tree_io), K(meta_item),
                          K(child_level_page_index), K(cur_page_id), K(cur_level_page_index), K(item_index));
              } else {
                has_find = true;
                if (is_page_flushed(meta_item)
                    && OB_FAIL(release_tmp_file_page_(meta_item.block_index_, meta_item.physical_page_id_, 1))) {
                  STORAGE_LOG(WARN, "fail to release tmp file page", KR(ret), K(fd_), K(meta_item), K(item_index), K(page_header));
                } else {
                  meta_item.block_index_ = tree_io.block_index_;
                  meta_item.physical_page_id_ = physical_page_id;
                  physical_page_id++;
                  child_level_page_index++;
                  if (OB_FAIL(rewrite_item_(page_buff, item_index, meta_item))) {
                    STORAGE_LOG(WARN, "fail to rewrite item", KR(ret), K(fd_), KP(page_buff), K(item_index), K(meta_item));
                  } else if (tree_io.flush_end_page_id_ == meta_item.buffer_page_id_) {
                    is_end = true;
                  }
                }
              }
            }
            item_index++;
          }
        }
      }
      if (OB_SUCC(ret) && has_find && OB_FAIL(wbp_->notify_dirty(fd_, cur_page_id, page_key))) {
        STORAGE_LOG(ERROR, "fail to notify dirty for meta", KR(ret), K(fd_), K(cur_page_id), K(page_key));
      }
      cur_page_id = next_page_id;
      cur_level_page_index++;
    }
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(!is_end
            || physical_page_id - tree_io.physical_start_page_id_ != tree_io.flush_nums_)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "unexpected is_end or flush_nums_", KR(ret), K(fd_),
                                      K(is_end), K(physical_page_id), K(tree_io));
      }
    }
  }
  return ret;
}

int ObSharedNothingTmpFileMetaTree::update_after_flush(
    const common::ObIArray<ObTmpFileTreeIOInfo> &tree_io_array)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (OB_UNLIKELY(tree_io_array.empty())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", KR(ret), K(fd_), K(tree_io_array), K(root_item_));
  } else {
    ARRAY_FOREACH_N(tree_io_array, i, cnt) {
      bool tree_io_is_empty = false;
      const ObTmpFileTreeIOInfo &tree_io = tree_io_array.at(i);
      const int16_t origin_tree_flush_num = tree_io.flush_nums_;
      bool end_page_flush_again = false;
      if (OB_UNLIKELY(!tree_io.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(ERROR, "invalid argument", KR(ret), K(tree_io), KPC(this));
      } else if (tree_io.tree_epoch_ != tree_epoch_) {
        STORAGE_LOG(INFO, "the tree_epoch_ in tree_io is not equal to current tree_epoch_",
            K(fd_), K(tree_io), K(tree_epoch_));
      } else if (tree_io.page_level_ >= level_page_range_array_.count()) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(ERROR, "invalid argument", KR(ret), K(tree_io), KPC(this));
      } else {
        const uint32_t flushed_end_page_id_in_array = level_page_range_array_[tree_io.page_level_].flushed_end_page_id_;
        const uint32_t start_page_id_in_array = level_page_range_array_[tree_io.page_level_].start_page_id_;
        uint32_t next_page_id_in_array = ObTmpFileGlobal::INVALID_PAGE_ID;
        int32_t level_page_index = level_page_range_array_[tree_io.page_level_].flushed_page_num_;
        if (ObTmpFileGlobal::INVALID_PAGE_ID != flushed_end_page_id_in_array) {
          level_page_index--;
          ObTmpFilePageUniqKey page_key(tree_io.page_level_, level_page_index);
          if (OB_FAIL(wbp_->get_next_page_id(fd_, flushed_end_page_id_in_array, page_key, next_page_id_in_array))) {
            STORAGE_LOG(ERROR, "fail to get next meta page id", KR(ret), K(fd_), K(flushed_end_page_id_in_array), K(page_key));
          } else if (OB_UNLIKELY(flushed_end_page_id_in_array != tree_io.flush_start_page_id_
                                 && !wbp_->is_cached(fd_, flushed_end_page_id_in_array, page_key))) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(ERROR, "unexpected flushed_end_page_id_in_array or tree_io", KR(ret), K(tree_io),
                                                        K(flushed_end_page_id_in_array), KPC(this));
          } else if (OB_UNLIKELY(flushed_end_page_id_in_array != tree_io.flush_start_page_id_
                                 && next_page_id_in_array != tree_io.flush_start_page_id_)) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(ERROR, "unexpected page_id_in_array or tree_io", KR(ret), K(tree_io),
                                K(flushed_end_page_id_in_array), K(next_page_id_in_array), KPC(this));
          } else if (flushed_end_page_id_in_array == tree_io.flush_start_page_id_) {
            end_page_flush_again = true;
          } else {
            level_page_index++;
          }
        } else {
          if (level_page_index > tree_io.flush_start_level_page_index_) {
            int64_t truncated_num = level_page_index - tree_io.flush_start_level_page_index_;
            if (truncated_num >= tree_io.flush_nums_) {
              tree_io_is_empty = true;
            } else {
              ObTmpFileTreeIOInfo& tree_io_mutable_ref = const_cast<ObTmpFileTreeIOInfo &>(tree_io);
              tree_io_mutable_ref.flush_start_page_id_ = start_page_id_in_array;
              tree_io_mutable_ref.flush_nums_ -= truncated_num;
            }
          } else if (OB_UNLIKELY(start_page_id_in_array != tree_io.flush_start_page_id_)) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(ERROR, "unexpected tree_io", KR(ret), K(tree_io), KPC(this));
          }
        }
        if (OB_SUCC(ret) && !tree_io_is_empty) {
          uint32_t cur_page_id = tree_io.flush_start_page_id_;
          int16_t num = 0;
          //change page state from writeback to cached
          while (OB_SUCC(ret) && ObTmpFileGlobal::INVALID_PAGE_ID != cur_page_id) {
            ObTmpFilePageUniqKey page_key(tree_io.page_level_, level_page_index);
            uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
            if (cur_page_id == tree_io.flush_end_page_id_ && wbp_->is_dirty(fd_, cur_page_id, page_key)) {
              //do nothing
              STORAGE_LOG(INFO, "page is dirty again, do not change page status", KR(ret), K(fd_), K(cur_page_id));
            } else if (OB_FAIL(wbp_->notify_write_back_succ(fd_, cur_page_id, page_key))) {
              STORAGE_LOG(ERROR, "fail to notify write back succ for meta", KR(ret), K(fd_), K(cur_page_id), K(page_key));
            }
            if (OB_SUCC(ret)) {
              num++;
              if (OB_UNLIKELY(num > tree_io.flush_nums_)) {
                ret = OB_ERR_UNEXPECTED;
                STORAGE_LOG(ERROR, "unexpected num", KR(ret), K(tree_io), K(num), K(cur_page_id), KPC(this));
              } else if (cur_page_id == tree_io.flush_end_page_id_) {
                break;
              } else if (OB_FAIL(wbp_->get_next_page_id(fd_, cur_page_id, page_key, next_page_id))) {
                STORAGE_LOG(ERROR, "fail to get next meta page id", KR(ret), K(fd_), K(cur_page_id), K(tree_io), K(page_key));
              } else {
                cur_page_id = next_page_id;
                level_page_index++;
              }
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_UNLIKELY(tree_io.flush_nums_ != num
                            || cur_page_id != tree_io.flush_end_page_id_)) {
              ret = OB_ERR_UNEXPECTED;
              STORAGE_LOG(ERROR, "unexpected page level or flush num", KR(ret), K(tree_io), K(num), K(cur_page_id), KPC(this));
            } else {
              level_page_range_array_[tree_io.page_level_].flushed_end_page_id_ = tree_io.flush_end_page_id_;
              if (end_page_flush_again) {
                num--;
              }
              level_page_range_array_[tree_io.page_level_].flushed_page_num_ += num;
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        stat_info_.meta_page_flushing_cnt_ -= origin_tree_flush_num;
      }
    }
  }
  STORAGE_LOG(INFO, "finish update after flush", KR(ret), K(fd_), K(level_page_range_array_));
  return ret;
}

int ObSharedNothingTmpFileMetaTree::prepare_for_write_tail(
    ObSharedNothingTmpFileDataItem &last_data_item)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (!root_item_.is_valid()) {
    if (OB_UNLIKELY(data_item_array_.empty())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected data_item_array_ count", KR(ret), K(fd_), K(data_item_array_));
    } else {
      last_data_item = data_item_array_.at(data_item_array_.count() - 1);
      if (OB_UNLIKELY(0 >= last_data_item.physical_page_num_)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "unexpected physical_page_num", KR(ret), K(last_data_item), KPC(this));
      }
    }
  } else {
    ObSharedNothingTmpFileTreePageHeader page_header;
    ObSharedNothingTmpFileMetaItem page_info;
    if (OB_UNLIKELY(level_page_range_array_.empty())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected level_page_range_array_", KR(ret), KPC(this));
    } else if (OB_FAIL(get_rightmost_leaf_page_for_write_(page_info))) {
      STORAGE_LOG(WARN, "fail to get rightmost leaf page for write", KR(ret), KPC(this));
    } else {
      //we don't need to worry about the rightmost leaf page being evicted,
      // because we set is_writing_ = true.
      char *leaf_page_buff = NULL;
      uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
      int32_t leaf_level_page_index = level_page_range_array_.at(0).evicted_page_num_ +
                                        level_page_range_array_.at(0).cached_page_num_ - 1;
      if (OB_FAIL(wbp_->read_page(fd_, page_info.buffer_page_id_, ObTmpFilePageUniqKey(0, leaf_level_page_index),
                                  leaf_page_buff, next_page_id))) {
        STORAGE_LOG(ERROR, "fail to read from write cache", KR(ret), K(fd_), K(page_info), K(leaf_level_page_index));
      } else if (OB_FAIL(read_page_header_(leaf_page_buff, page_header))) {
        STORAGE_LOG(WARN, "fail to read page header", KR(ret), KP(leaf_page_buff), KPC(this));
      } else if (OB_UNLIKELY(0 >= page_header.item_num_)) {
        //There is no concurrent writing
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
                                      K(page_info), K(leaf_level_page_index), K(tmp_page_header), K(items), KPC(this));
      }
    }
  }
  return ret;
}

//After the tail is written
int ObSharedNothingTmpFileMetaTree::finish_write_tail(
    const ObSharedNothingTmpFileDataItem &last_data_item,
    const bool release_tail_in_disk)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (!root_item_.is_valid()) {
    if (OB_UNLIKELY(data_item_array_.empty()
                    || last_data_item != data_item_array_[data_item_array_.count() - 1])) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected data_item_array_ or last_data_item", KR(ret), K(last_data_item), KPC(this));
    } else if (release_tail_in_disk) {
      if (0 == --data_item_array_[data_item_array_.count() - 1].physical_page_num_) {
        data_item_array_.pop_back();
      }
    }
  } else {
    if (OB_UNLIKELY(level_page_range_array_.empty()
                    || !is_writing_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected level_array_ or is_writing_", KR(ret), KPC(this));
    } else if (release_tail_in_disk) {
      char *leaf_page_buff = NULL;
      uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
      uint32_t page_id = level_page_range_array_[0].end_page_id_;
      int32_t level_page_index = level_page_range_array_.at(0).evicted_page_num_ +
                                    level_page_range_array_.at(0).cached_page_num_ - 1;
      ObSharedNothingTmpFileTreePageHeader page_header;
      ObSharedNothingTmpFileDataItem data_item;
      ObTmpFilePageUniqKey page_key(0, level_page_index);
      if (OB_FAIL(wbp_->read_page(fd_, page_id, page_key, leaf_page_buff, next_page_id))) {
        STORAGE_LOG(ERROR, "fail to read from write cache", KR(ret), K(fd_), K(page_id), K(page_key));
      } else if (OB_FAIL(read_page_header_(leaf_page_buff, page_header))) {
        STORAGE_LOG(WARN, "fail to read page header", KR(ret), KP(leaf_page_buff), KPC(this));
      } else if (OB_FAIL(read_item_(leaf_page_buff, page_header.item_num_ - 1, data_item))) {
        STORAGE_LOG(WARN, "fail to read item", KR(ret), KP(leaf_page_buff), K(page_header), KPC(this));
      } else if (OB_UNLIKELY(last_data_item != data_item)) {
        ret = OB_ERR_UNEXPECTED;
        //print page content
        //NOTE: control print frequence
        ObSharedNothingTmpFileTreePageHeader tmp_page_header;
        ObArray<ObSharedNothingTmpFileDataItem> items;
        read_page_content_(leaf_page_buff, tmp_page_header, items);
        STORAGE_LOG(ERROR, "unexpected virtual_page_id, dump tree page", KR(ret), KP(leaf_page_buff), K(page_id), K(page_key),
                                            K(last_data_item), K(data_item), K(tmp_page_header), K(items), KPC(this));
      } else if (FALSE_IT(data_item.physical_page_num_--)) {
      } else if (0 == data_item.physical_page_num_) {
        if (OB_FAIL(remove_page_item_from_tail_(leaf_page_buff, 1/*remove_num*/))) {
          STORAGE_LOG(WARN, "unexpected item_num", KR(ret), KP(leaf_page_buff), KPC(this));
        }
      } else if (OB_FAIL(rewrite_item_(leaf_page_buff, page_header.item_num_ - 1, data_item))) {
        STORAGE_LOG(WARN, "fail to rewrite item", KR(ret), KP(leaf_page_buff),
                                                        K(page_header), K(data_item), KPC(this));
      }
      if (FAILEDx(wbp_->notify_dirty(fd_, page_id, page_key))) {
        STORAGE_LOG(ERROR, "fail to notify dirty for meta", KR(ret), K(fd_), K(page_id), K(page_key));
      }
    }
    if (OB_SUCC(ret)) {
      is_writing_ = false;
    }
  }
  if (OB_SUCC(ret) && release_tail_in_disk) {
    if (OB_FAIL(release_tmp_file_page_(last_data_item.block_index_,
            last_data_item.physical_page_id_ + last_data_item.physical_page_num_ - 1, 1))) {
      STORAGE_LOG(WARN, "fail to release tmp file page", KR(ret), K(fd_), K(last_data_item));
    }
  }
  STORAGE_LOG(INFO, "finish write tail", KR(ret), K(fd_), K(release_tail_in_disk));
  return ret;
}

int ObSharedNothingTmpFileMetaTree::evict_meta_pages(
    const int64_t expected_page_num,
    const ObTmpFileTreeEvictType flush_type,
    int64_t &actual_evict_page_num)
{
  int ret = OB_SUCCESS;
  actual_evict_page_num = 0;
  if (OB_UNLIKELY(0 >= expected_page_num
                  || ObTmpFileTreeEvictType::INVALID == flush_type)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", KR(ret), K(fd_), K(expected_page_num), K(flush_type));
  } else {
    SpinWLockGuard guard(lock_);
    if (!root_item_.is_valid()) {
      //do nothing
    } else {
      ObSEArray<uint32_t, 5> evict_pages;
      ARRAY_FOREACH_X(level_page_range_array_, level, level_cnt,
                        OB_SUCC(ret) && actual_evict_page_num < expected_page_num) {
        evict_pages.reset();
        uint32_t next_page_id = level_page_range_array_[level].start_page_id_;
        const uint32_t end_evict_page = level_page_range_array_[level].flushed_end_page_id_;
        const uint32_t end_page_id = level_page_range_array_[level].end_page_id_;
        const int32_t start_level_page_index = level_page_range_array_.at(level).evicted_page_num_;
        uint32_t cur_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
        int32_t level_page_index = start_level_page_index;
        if (ObTmpFileGlobal::INVALID_PAGE_ID == end_evict_page) {
          //no pages need to be evicted at this level
          continue;
        }
        while (OB_SUCC(ret)
              && ObTmpFileGlobal::INVALID_PAGE_ID != next_page_id
              && actual_evict_page_num < expected_page_num
              && (!is_writing_ || end_page_id != next_page_id) //not evict end page if is writing
              && (ObTmpFileTreeEvictType::FULL == flush_type || end_page_id != next_page_id)) {
          ObTmpFilePageUniqKey page_key(level, level_page_index);
          if (OB_UNLIKELY(end_evict_page != next_page_id && !wbp_->is_cached(fd_, next_page_id, page_key))) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(ERROR, "unexpected next_page_id", KR(ret), K(next_page_id), KPC(this));
          } else if (end_evict_page == next_page_id && !wbp_->is_cached(fd_, next_page_id, page_key)) {
            break;
          } else if (level > 0) {
            //check whether the page is satisfied for evict
            char * page_buff = NULL;
            ObSharedNothingTmpFileMetaItem last_item;
            uint32_t unused_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
            if (OB_FAIL(wbp_->read_page(fd_, next_page_id, page_key, page_buff, unused_page_id))) {
              STORAGE_LOG(ERROR, "fail to read from write cache", KR(ret), K(fd_), K(next_page_id), K(page_key));
            } else {
              last_item.reset();
              ObSharedNothingTmpFileTreePageHeader page_header;
              if (OB_FAIL(read_page_header_(page_buff, page_header))) {
                STORAGE_LOG(WARN, "fail to read page header", KR(ret), K(fd_), KP(page_buff));
              } else if (page_header.item_num_ <= 0) {
                ret = OB_ERR_UNEXPECTED;
                STORAGE_LOG(ERROR, "unexpected item_num", KR(ret), K(page_header), KPC(this));
              } else if (OB_FAIL(read_item_(page_buff, page_header.item_num_ - 1/*item_index*/, last_item))) {
                STORAGE_LOG(WARN, "fail to read item", KR(ret), KP(page_buff), K(page_header), KPC(this));
              } else if (is_page_in_write_cache(last_item)) {
                if (OB_UNLIKELY(end_evict_page != next_page_id)) {
                  ret = OB_ERR_UNEXPECTED;
                  STORAGE_LOG(ERROR, "unexpected next_page", KR(ret), K(next_page_id), K(last_item), KPC(this));
                } else {
                  break;
                }
              }
            }
          }
          if (OB_SUCC(ret)) {
            cur_page_id = next_page_id;
            next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
            if (OB_FAIL(evict_pages.push_back(cur_page_id))) {
              STORAGE_LOG(WARN, "fail to push back", KR(ret), K(fd_), K(cur_page_id));
            } else if (OB_FAIL(wbp_->get_next_page_id(fd_, cur_page_id, page_key, next_page_id))) {
              STORAGE_LOG(ERROR, "fail to get next meta page id", KR(ret), K(fd_), K(cur_page_id), K(page_key));
            } else {
              actual_evict_page_num++;
              level_page_index++;
              if (end_evict_page == cur_page_id) {
                break;
              }
            }
          }
        }
        if (OB_SUCC(ret) && !evict_pages.empty()) {
          if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_PAGE_ID == cur_page_id)) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(ERROR, "unexpected cur_page_id", KR(ret), K(cur_page_id), K(level), K(level_page_index), KPC(this));
          } else if (OB_FAIL(modify_meta_items_during_evict_(evict_pages, level + 1, start_level_page_index))) {
            STORAGE_LOG(WARN, "fail to modify meta items during evict", KR(ret), K(evict_pages),
                                                            K(level), K(start_level_page_index), KPC(this));
          } else {
            uint32_t unused_next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
            ARRAY_FOREACH_N(evict_pages, i, cnt) {
              //change page state from flushed/cached to evicted/invalid
              if (OB_FAIL(wbp_->free_page(fd_, evict_pages.at(i),
                          ObTmpFilePageUniqKey(level, start_level_page_index + i), unused_next_page_id))) {
                STORAGE_LOG(ERROR, "fail to free meta page in write cache", KR(ret), K(fd_), K(evict_pages.at(i)),
                                                                K(level), K(start_level_page_index + i));
              } else {
                stat_info_.meta_page_free_cnt_++;
              }
            }
          }
          if (OB_SUCC(ret)) {
            if (end_page_id == cur_page_id) {
              //all pages in this level are evicted
              level_page_range_array_[level].start_page_id_ = ObTmpFileGlobal::INVALID_PAGE_ID;
              level_page_range_array_[level].end_page_id_ = ObTmpFileGlobal::INVALID_PAGE_ID;
              level_page_range_array_[level].flushed_end_page_id_ = ObTmpFileGlobal::INVALID_PAGE_ID;
            } else if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_PAGE_ID == next_page_id)) {
              ret = OB_ERR_UNEXPECTED;
              STORAGE_LOG(ERROR, "unexpected next_page_id", KR(ret), K(next_page_id), K(cur_page_id), KPC(this));
            } else if (end_evict_page == cur_page_id) {
              //flushed_end_page is not equal to end_page,
              // and pages are evicted to flushed_end_page(including flushed_end_page)
              level_page_range_array_[level].start_page_id_ = next_page_id;
              level_page_range_array_[level].flushed_end_page_id_ = ObTmpFileGlobal::INVALID_PAGE_ID;
            } else {
              //pages are not evicted to flushed_end_page
              level_page_range_array_[level].start_page_id_ = next_page_id;
            }
            if (OB_SUCC(ret)) {
              level_page_range_array_[level].cached_page_num_ -= evict_pages.count();
              level_page_range_array_[level].evicted_page_num_ += evict_pages.count();
            }
          }
        }
      }
    }
  }
  STORAGE_LOG(INFO, "finish evict meta pages", KR(ret), K(fd_), K(level_page_range_array_));
  return ret;
}

int ObSharedNothingTmpFileMetaTree::modify_meta_items_during_evict_(
    const ObIArray<uint32_t> &evict_pages,
    const int16_t level,
    const int32_t start_page_index_in_level)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(evict_pages.empty()
                  || level > level_page_range_array_.count())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "unexpected evict_pages", KR(ret), K(fd_), K(evict_pages),
                                            K(level), K(level_page_range_array_));
  } else {
    char *page_buff = NULL;
    const int64_t level_count = level_page_range_array_.count();
    const int64_t evict_page_count = evict_pages.count();
    int64_t array_index = 0;
    bool has_find = false;
    if (level < level_count) {
      const int16_t FULL_PAGE_META_ITEM_NUM =
            MIN(MAX_PAGE_ITEM_COUNT, (ObTmpFileGlobal::PAGE_SIZE - PAGE_HEADER_SIZE) / sizeof(ObSharedNothingTmpFileMetaItem));
      uint32_t cur_page_id = level_page_range_array_[level].start_page_id_;
      const uint32_t end_page_id = level_page_range_array_[level].end_page_id_;
      int32_t level_page_index = level_page_range_array_[level].evicted_page_num_;
      int32_t evict_page_index_in_level = start_page_index_in_level;
      ObSharedNothingTmpFileMetaItem meta_item;
      if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_PAGE_ID == cur_page_id
                      || ObTmpFileGlobal::INVALID_PAGE_ID == end_page_id
                      || 0 > evict_page_index_in_level)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "unexpected page_id", KR(ret), K(fd_), K(level_page_range_array_),
                                                        K(level), K(evict_page_index_in_level));
      } else {
        while(OB_SUCC(ret)
              && ObTmpFileGlobal::INVALID_PAGE_ID != cur_page_id
              && array_index < evict_page_count) {
          ObSharedNothingTmpFileTreePageHeader page_header;
          char *page_buff = NULL;
          uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
          if (OB_FAIL(wbp_->read_page(fd_, cur_page_id, ObTmpFilePageUniqKey(level, level_page_index), page_buff, next_page_id))) {
            STORAGE_LOG(ERROR, "fail to read from write cache", KR(ret), K(fd_), K(cur_page_id), K(level), K(level_page_index));
          } else if (OB_FAIL(read_page_header_(page_buff, page_header))) {
            STORAGE_LOG(WARN, "fail to read page header", KR(ret), K(fd_), KP(page_buff));
          } else {
            int16_t item_index = 0;
            while (OB_SUCC(ret)
                  && item_index < page_header.item_num_
                  && array_index < evict_page_count) {
              meta_item.reset();
              if (OB_FAIL(read_item_(page_buff, item_index, meta_item))) {
                STORAGE_LOG(WARN, "fail to read item", KR(ret), K(fd_), KP(page_buff), K(item_index));
              } else {
                if (has_find || level_page_index * FULL_PAGE_META_ITEM_NUM + item_index == evict_page_index_in_level) {
                  if (OB_UNLIKELY(meta_item.buffer_page_id_ != evict_pages.at(array_index)
                                  || level_page_index * FULL_PAGE_META_ITEM_NUM + item_index != evict_page_index_in_level
                                  || !is_page_in_write_cache(meta_item)
                                  || !is_page_flushed(meta_item))) {
                    ret = OB_ERR_UNEXPECTED;
                    STORAGE_LOG(ERROR, "unexpected meta_item", KR(ret), K(fd_), K(evict_pages), K(array_index), K(meta_item),
                                              K(evict_page_index_in_level), K(cur_page_id), K(level_page_index), K(item_index));
                  } else {
                    has_find = true;
                    meta_item.buffer_page_id_ = ObTmpFileGlobal::INVALID_PAGE_ID;
                    array_index++;
                    evict_page_index_in_level++;
                    if (OB_FAIL(rewrite_item_(page_buff, item_index, meta_item))) {
                      STORAGE_LOG(WARN, "fail to rewrite item", KR(ret), K(fd_), KP(page_buff), K(item_index));
                    }
                  }
                }
              }
              item_index++;
            }
          }
          //it is not necessary to mark it as dirty.
          cur_page_id = next_page_id;
          level_page_index++;
        }
        if (OB_SUCC(ret) && evict_page_count != array_index) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "unexpected array_index", KR(ret), K(fd_), K(array_index), K(evict_pages), K(level_page_range_array_));
        }
      }
    } else { //level == level_count
      if (OB_UNLIKELY(!is_page_in_write_cache(root_item_)
                      || !is_page_flushed(root_item_)
                      || 1 != evict_page_count
                      || evict_pages.at(0) != root_item_.buffer_page_id_
                      || 0 != start_page_index_in_level)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "unexpected root_item_ or evict_pages", KR(ret), K(fd_), K(root_item_),
                                                        K(evict_pages), K(start_page_index_in_level));
      } else {
        root_item_.buffer_page_id_ = ObTmpFileGlobal::INVALID_PAGE_ID;
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
                  || last_truncate_offset > total_file_size
                  || is_writing_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid argument", KR(ret), K(last_truncate_offset), K(total_file_size), KPC(this));
  } else {
    while (true) {
      if (OB_FAIL(truncate_(end_truncate_offset))) {
        STORAGE_LOG(WARN, "fail to truncate_", KR(ret), K(last_truncate_offset), K(end_truncate_offset), K(total_file_size), KPC(this));
      } else {
        break;
      }
      usleep(100 * 1000); // 100ms
      ret = OB_SUCCESS;
    }
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(root_item_.is_valid()
                            || !level_page_range_array_.empty()
                            || !data_item_array_.empty())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected root_item_ or array_", KR(ret), KPC(this));
    } else if (OB_UNLIKELY(stat_info_.all_type_page_flush_cnt_ != stat_info_.all_type_flush_page_released_cnt_
                           || stat_info_.meta_page_alloc_cnt_ != stat_info_.meta_page_free_cnt_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected stat_info_", KR(ret), KPC(this));
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
  if (OB_UNLIKELY(level_page_range_array_.count() <= page_info.page_level_
                  || 0 > page_index_in_level)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "unexpected level_page_range_array_ or page_info", KR(ret), K(fd_), K(level_page_range_array_),
                                                                          K(page_info), K(page_index_in_level));
  } else if (is_page_in_write_cache(page_info)) {
    const uint32_t start_page_id_in_array = level_page_range_array_.at(page_info.page_level_).start_page_id_;
    uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
    if (OB_FAIL(wbp_->free_page(fd_, page_info.buffer_page_id_,
                ObTmpFilePageUniqKey(page_info.page_level_, page_index_in_level), next_page_id))) {
      STORAGE_LOG(ERROR, "fail to free meta page in write cache", KR(ret), K(fd_), K(page_info), K(page_index_in_level));
    } else if (FALSE_IT(stat_info_.meta_page_free_cnt_++)) {
    } else if (OB_UNLIKELY(start_page_id_in_array != page_info.buffer_page_id_)) {
      //NOTE: pages must be released sequentially (from front to back in array)
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected level_page_range_array_", KR(ret), K(fd_), K(level_page_range_array_), K(page_info));
    } else {
      level_page_range_array_.at(page_info.page_level_).start_page_id_ = next_page_id;
      if (start_page_id_in_array == level_page_range_array_.at(page_info.page_level_).end_page_id_) {
        //next_page_id must be invalid
        level_page_range_array_.at(page_info.page_level_).end_page_id_ = ObTmpFileGlobal::INVALID_PAGE_ID;
      }
      if (ObTmpFileGlobal::INVALID_PAGE_ID == level_page_range_array_.at(page_info.page_level_).flushed_end_page_id_) {
        level_page_range_array_.at(page_info.page_level_).flushed_page_num_ += 1;
      }
      if (start_page_id_in_array == level_page_range_array_.at(page_info.page_level_).flushed_end_page_id_) {
        level_page_range_array_.at(page_info.page_level_).flushed_end_page_id_ = ObTmpFileGlobal::INVALID_PAGE_ID;
      }
      level_page_range_array_.at(page_info.page_level_).cached_page_num_ -= 1;
      level_page_range_array_.at(page_info.page_level_).evicted_page_num_ += 1;
    }
  }
  if (OB_SUCC(ret) && is_page_flushed(page_info)) {
    if (OB_FAIL(release_tmp_file_page_(page_info.block_index_, page_info.physical_page_id_, 1))) {
      STORAGE_LOG(WARN, "fail to release tmp file page", KR(ret), K(fd_), K(page_info));
    }
  }
  return ret;
}

int ObSharedNothingTmpFileMetaTree::release_tmp_file_page_(
    const int64_t block_index, const int64_t begin_page_id, const int64_t page_num)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(block_manager_->release_tmp_file_page(block_index, begin_page_id, page_num))) {
    STORAGE_LOG(ERROR, "fail to release tmp file page",
        KR(ret), K(fd_), K(block_index), K(begin_page_id), K(page_num));
  } else {
    stat_info_.all_type_flush_page_released_cnt_ += page_num;
  }

  // ignore ret
  for (int64_t i = 0; i < page_num; ++i) {
    int64_t page_id = begin_page_id + i;
    ObTmpPageCacheKey key(block_index, page_id, MTL_ID());
    ObTmpPageCache::get_instance().erase(key);
  }

  return ret;
}

int ObSharedNothingTmpFileMetaTree::truncate(
    const int64_t last_truncate_offset,
    const int64_t end_truncate_offset)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (OB_UNLIKELY(last_truncate_offset < released_offset_
                  || 0 != released_offset_ % ObTmpFileGlobal::PAGE_SIZE
                  || last_truncate_offset > end_truncate_offset
                  || is_writing_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid argument", KR(ret), K(last_truncate_offset), K(end_truncate_offset), KPC(this));
  } else {
    while (true) {
      if (OB_FAIL(truncate_(end_truncate_offset))) {
        STORAGE_LOG(WARN, "fail to truncate_", KR(ret), K(last_truncate_offset), K(end_truncate_offset), KPC(this));
      } else {
        break;
      }
      usleep(100 * 1000); // 100ms
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
    ObArray<std::pair<int32_t, int16_t>> item_index_arr;
    if (OB_FAIL(calculate_truncate_index_path_(item_index_arr))) {
      STORAGE_LOG(WARN, "fail to calculate truncate index path", KR(ret), K(fd_), K(item_index_arr));
    } else if (OB_UNLIKELY(item_index_arr.empty())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected item_index_arr", KR(ret), K(fd_), K(item_index_arr));
    } else {
      ObSharedNothingTmpFileMetaItem meta_item = root_item_;
      ObSharedNothingTmpFileMetaItem next_level_meta_item;
      ObSEArray<BacktraceNode, 2> truncate_path;
      ObSEArray<ObTmpPageValueHandle, 2> p_handles;
      while (OB_SUCC(ret)
            && 0 < meta_item.page_level_) {
        next_level_meta_item.reset();
        int16_t item_index = -1;
        int32_t level_page_index = -1;
        ObSharedNothingTmpFileTreePageHeader page_header;
        char *page_buff = NULL;
        ObTmpPageValueHandle p_handle;
        if (OB_UNLIKELY(item_index_arr.count() <= meta_item.page_level_)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(ERROR, "unexpected item_index_arr", KR(ret), K(fd_), K(item_index_arr), K(meta_item));
        } else if (FALSE_IT(level_page_index = item_index_arr.at(meta_item.page_level_).first)) {
        } else if (FALSE_IT(item_index = item_index_arr.at(meta_item.page_level_).second)) {
        } else if (OB_FAIL(get_page_(meta_item, level_page_index, page_buff, p_handle))) {
          STORAGE_LOG(WARN, "fail to get page", KR(ret), K(fd_), K(meta_item));
        } else if (OB_FAIL(p_handles.push_back(p_handle))) {
          STORAGE_LOG(WARN, "fail to push back", KR(ret), K(fd_), K(p_handle));
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
          p_handle.reset();
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
            } else if (OB_FAIL(backtrace_truncate_tree_(end_truncate_offset, truncate_path, reserved_meta_items, p_handles))) {
              STORAGE_LOG(WARN, "fail to backtrace truncate tree", KR(ret), K(fd_), K(end_truncate_offset), K(truncate_path),
                                                                              K(reserved_meta_items), K(p_handles));
            }
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(ERROR, "unexpected page type", KR(ret), K(fd_), K(meta_item));
        }
      }
      for (int16_t i = 0; i < p_handles.count(); i++) {
        p_handles.at(i).reset();
      }
    }
  }
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
    ARRAY_FOREACH_N(level_page_range_array_, i, cnt) {
      int16_t item_index = -1;
      if (!last_truncate_leaf_info_.is_valid()) {
        child_level_page_index = 0;
        item_index = 0;
      } else {
        int32_t level_page_index = -1;
        int32_t level_page_num = level_page_range_array_.at(i).evicted_page_num_ + level_page_range_array_.at(i).cached_page_num_;
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
                              K(i), K(level_page_index), K(level_page_num), K(level_page_range_array_));
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
  ObTmpPageValueHandle p_handle;
  if (OB_UNLIKELY(0 != tmp_release_offset % ObTmpFileGlobal::PAGE_SIZE
        || 0 > begin_release_index
        || PAGE_HEADER_SIZE + (begin_release_index + 1) * sizeof(ObSharedNothingTmpFileDataItem) > ObTmpFileGlobal::PAGE_SIZE
        || begin_release_index >= MAX_PAGE_ITEM_COUNT)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "unexpected release_offset or begin_release_index", KR(ret), K(fd_), K(tmp_release_offset), K(begin_release_index));
  } else if (OB_FAIL(get_page_(page_info, level_page_index, page_buff, p_handle))) {
    STORAGE_LOG(WARN, "fail to get page", KR(ret), K(fd_), K(page_info), K(level_page_index));
  } else if (OB_FAIL(read_page_header_(page_buff, page_header))) {
    STORAGE_LOG(WARN, "fail to read page header", KR(ret), K(fd_), KP(page_buff));
  } else if (0 == page_header.item_num_ || begin_release_index == page_header.item_num_) {
    //maybe the item have been cleared (corresponds to an unfilled data page)
    release_last_item = true;
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
  p_handle.reset();
  return ret;
}

int ObSharedNothingTmpFileMetaTree::backtrace_truncate_tree_(
    const int64_t end_offset,
    ObIArray<BacktraceNode> &search_path,
    ObIArray<ObSharedNothingTmpFileMetaItem> &meta_items,
    ObIArray<ObTmpPageValueHandle> &p_handles)
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
    if (OB_UNLIKELY(p_handles.count() != search_path.count())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected p_handles or search_path", KR(ret), K(fd_), K(p_handles), K(search_path));
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
            p_handles.at(last_node_index).reset();
            p_handles.pop_back();
          }
        }
      }
    } else if (1 < page_info.page_level_) {
      if (meta_items.count() == 1) {
        const int32_t child_level_page_index = level_page_index * FULL_PAGE_META_ITEM_NUM + cur_item_index;
        search_path.at(last_node_index).prev_item_index_ = cur_item_index;
        char *unused_page_buff = NULL;
        ObTmpPageValueHandle p_handle;
        if (OB_FAIL(get_page_(meta_items.at(0), child_level_page_index, unused_page_buff, p_handle))) {
          STORAGE_LOG(WARN, "fail to get page", KR(ret), K(fd_), K(meta_items.at(0)), K(child_level_page_index));
        } else if (OB_FAIL(p_handles.push_back(p_handle))) {
          STORAGE_LOG(WARN, "fail to push back", KR(ret), K(fd_), K(p_handle));
        } else if (OB_FAIL(search_path.push_back(BacktraceNode(meta_items.at(0), child_level_page_index, -1)))) {
          STORAGE_LOG(WARN, "fail to push back", KR(ret), K(fd_), K(meta_items.at(0)), K(child_level_page_index));
        }
        if (OB_FAIL(ret)) {
          p_handle.reset();
        }
      } else if (meta_items.empty()) {
        if (reach_last_item) {
          if (OB_FAIL(release_meta_page_(page_info, level_page_index))) {
            STORAGE_LOG(WARN, "fail to release meta page", KR(ret), K(fd_), K(page_info), K(level_page_index));
          } else {
            search_path.pop_back();
            p_handles.at(last_node_index).reset();
            p_handles.pop_back();
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
    } else if (OB_UNLIKELY(stat_info_.all_type_page_flush_cnt_ != stat_info_.all_type_flush_page_released_cnt_
                           || stat_info_.meta_page_alloc_cnt_ != stat_info_.meta_page_free_cnt_)) {
      //TODO: do not throw errors in the future
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected stat_info_", KR(ret), K(fd_), K(stat_info_));
    } else {
      ++tree_epoch_;
      root_item_.reset();
      level_page_range_array_.reset();
      last_truncate_leaf_info_.reset();
      STORAGE_LOG(INFO, "meta tree pages are all truncated", KR(ret), K(fd_), K(tree_epoch_));
    }
  }
  return ret;
}

int ObSharedNothingTmpFileMetaTree::check_tree_is_empty_()
{
  int ret = OB_SUCCESS;
  ARRAY_FOREACH_N(level_page_range_array_, i, cnt) {
    if (OB_UNLIKELY(0 != level_page_range_array_.at(i).cached_page_num_
        || ObTmpFileGlobal::INVALID_PAGE_ID != level_page_range_array_.at(i).start_page_id_
        || ObTmpFileGlobal::INVALID_PAGE_ID != level_page_range_array_.at(i).end_page_id_
        || ObTmpFileGlobal::INVALID_PAGE_ID != level_page_range_array_.at(i).flushed_end_page_id_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected level_page_range_array_", KR(ret), K(fd_), K(level_page_range_array_), K(i));
    }
  }
  return ret;
}

//total_need_flush_rightmost_page_num will not consider "is_writing_ = true" or "ObTmpFileTreeEvictType"
int ObSharedNothingTmpFileMetaTree::get_need_flush_page_num(
    int64_t &total_need_flush_page_num,
    int64_t &total_need_flush_rightmost_page_num) const
{
  int ret = OB_SUCCESS;
  total_need_flush_page_num = 0;
  total_need_flush_rightmost_page_num = 0;
  SpinRLockGuard guard(lock_);
  ARRAY_FOREACH_N(level_page_range_array_, level, level_cnt) {
    const uint32_t start_page_id = level_page_range_array_[level].start_page_id_;
    const uint32_t flushed_end_page_id = level_page_range_array_[level].flushed_end_page_id_;
    const uint32_t end_page_id = level_page_range_array_[level].end_page_id_;
    if (ObTmpFileGlobal::INVALID_PAGE_ID != start_page_id) {
      if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_PAGE_ID == end_page_id)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "unexpected end_page_id", KR(ret), K(fd_), K(level_page_range_array_), K(level));
      } else {
        uint32_t cur_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
        int32_t level_page_index = level_page_range_array_[level].flushed_page_num_;
        if (ObTmpFileGlobal::INVALID_PAGE_ID == flushed_end_page_id) {
          cur_page_id = start_page_id;
        } else {
          cur_page_id = flushed_end_page_id;
          level_page_index--;
        }
        while (OB_SUCC(ret)
               && ObTmpFileGlobal::INVALID_PAGE_ID != cur_page_id) {
          uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
          ObTmpFilePageUniqKey page_key(level, level_page_index);
          if (wbp_->is_dirty(fd_, cur_page_id, page_key)) {
            total_need_flush_page_num++;
            if (end_page_id == cur_page_id) {
              total_need_flush_rightmost_page_num++;
              break;
            }
          }
          if (OB_FAIL(wbp_->get_next_page_id(fd_, cur_page_id, page_key, next_page_id))) {
            STORAGE_LOG(ERROR, "fail to get next meta page id", KR(ret), K(fd_), K(cur_page_id), K(page_key));
          } else {
            cur_page_id = next_page_id;
            level_page_index++;
          }
        }
        if (OB_SUCC(ret) && end_page_id != cur_page_id && ObTmpFileGlobal::INVALID_PAGE_ID != cur_page_id) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(ERROR, "unexpected cur_page_id", KR(ret), K(fd_), K(cur_page_id),
                                                    K(level_page_range_array_), K(level));
        }
      }
    }
  }
  return ret;
}

//total_need_evict_rightmost_page_num will not consider "is_writing_ = true" or "ObTmpFileTreeEvictType"
int ObSharedNothingTmpFileMetaTree::get_need_evict_page_num(
    int64_t &total_need_evict_page_num,
    int64_t &total_need_evict_rightmost_page_num) const
{
  int ret = OB_SUCCESS;
  total_need_evict_page_num = 0;
  total_need_evict_rightmost_page_num = 0;
  SpinRLockGuard guard(lock_);
  ARRAY_FOREACH_N(level_page_range_array_, level, level_cnt) {
    const uint32_t start_page_id = level_page_range_array_[level].start_page_id_;
    const uint32_t flushed_end_page_id = level_page_range_array_[level].flushed_end_page_id_;
    const uint32_t end_page_id = level_page_range_array_[level].end_page_id_;
    if (ObTmpFileGlobal::INVALID_PAGE_ID != flushed_end_page_id) {
      if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_PAGE_ID == start_page_id
                      || ObTmpFileGlobal::INVALID_PAGE_ID == end_page_id)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "unexpected start_page_id or end_page_id", KR(ret), K(fd_), K(level_page_range_array_), K(level));
      } else {
        uint32_t cur_page_id = start_page_id;
        int32_t level_page_index = level_page_range_array_[level].evicted_page_num_;
        while (OB_SUCC(ret)
               && ObTmpFileGlobal::INVALID_PAGE_ID != cur_page_id) {
          uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
          ObTmpFilePageUniqKey page_key(level, level_page_index);
          if (OB_UNLIKELY(flushed_end_page_id != cur_page_id && !wbp_->is_cached(fd_, cur_page_id, page_key))) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(ERROR, "unexpected cur_page_id", KR(ret), K(fd_), K(cur_page_id), K(flushed_end_page_id));
          } else if (!wbp_->is_cached(fd_, cur_page_id, page_key)) {
            break;
          } else if (FALSE_IT(total_need_evict_page_num++)) {
          } else if (end_page_id == cur_page_id && FALSE_IT(total_need_evict_rightmost_page_num++)) {
          } else if (flushed_end_page_id == cur_page_id) {
            break;
          } else if (OB_FAIL(wbp_->get_next_page_id(fd_, cur_page_id, page_key, next_page_id))) {
            STORAGE_LOG(ERROR, "fail to get next meta page id", KR(ret), K(fd_), K(cur_page_id), K(page_key));
          } else {
            cur_page_id = next_page_id;
            level_page_index++;
          }
        }
        if (OB_SUCC(ret) && flushed_end_page_id != cur_page_id) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(ERROR, "unexpected cur_page_id", KR(ret), K(fd_), K(cur_page_id),
                                                    K(level_page_range_array_), K(level));
        }
      }
    }
  }
  return ret;
}

//NOTE: 这个页被取出来时，并没有加锁，到时候可能需要为write_cache的读写单独弄一把锁
int ObSharedNothingTmpFileMetaTree::get_page_(
    const ObSharedNothingTmpFileMetaItem &page_info,
    const int32_t level_page_index,
    char *&page_buff,
    ObTmpPageValueHandle &p_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!page_info.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "invalid argument", KR(ret), K(fd_), K(page_info));
  } else {
    if (!is_page_in_write_cache(page_info)) {
      bool need_load_from_disk = false;
      ObTmpPageCacheKey key(page_info.block_index_, page_info.physical_page_id_, MTL_ID());
      if (OB_SUCC(ObTmpPageCache::get_instance().get_page(key, p_handle))) {
        page_buff = p_handle.value_->get_buffer();
      } else if (OB_ENTRY_NOT_EXIST != ret) {
        STORAGE_LOG(ERROR, "fail to read from read_cache", KR(ret), K(fd_), K(key));
      } else {
        ret = OB_SUCCESS;
        need_load_from_disk = true;
        if (OB_FAIL(ObTmpPageCache::get_instance().load_page(key, callback_allocator_, p_handle))) {
          STORAGE_LOG(WARN, "fail to load page from disk", KR(ret), K(fd_), K(key));
        } else {
          page_buff = p_handle.value_->get_buffer();
        }
      }
      if (FAILEDx(check_page_(page_buff))) {
        STORAGE_LOG(ERROR, "the page is invalid or corrupted", KR(ret), K(fd_), KP(page_buff));
      }
      STORAGE_LOG(INFO, "load page from disk", KR(ret), K(fd_), K(need_load_from_disk), K(page_info), K(level_page_index));
    } else {
      //still in write cache
      uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
      ObTmpFilePageUniqKey page_key(page_info.page_level_, level_page_index);
      if (OB_FAIL(wbp_->read_page(fd_, page_info.buffer_page_id_, page_key, page_buff, next_page_id))) {
        STORAGE_LOG(ERROR, "fail to read from write cache", KR(ret), K(fd_), K(page_info), K(page_key));
      }
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
//put page_id into level_page_range_array_
int ObSharedNothingTmpFileMetaTree::cache_page_for_write_(
    const uint32_t parent_page_id,
    ObSharedNothingTmpFileMetaItem &page_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!page_info.is_valid()
                  || page_info.page_level_ >= level_page_range_array_.count()
                  || (ObTmpFileGlobal::INVALID_PAGE_ID != parent_page_id
                      && page_info.page_level_ + 1 >= level_page_range_array_.count()))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "invalid argument", KR(ret), K(fd_), K(page_info), K(level_page_range_array_), K(parent_page_id));
  } else {
    if (!is_page_in_write_cache(page_info)) {
      bool need_load_from_disk = false;
      uint32_t new_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
      char *new_page_buff = NULL;
      int32_t level_page_index = level_page_range_array_[page_info.page_level_].evicted_page_num_ - 1;
      ObTmpFilePageUniqKey page_key(page_info.page_level_, level_page_index);
      if (OB_UNLIKELY(!is_page_flushed(page_info)
                      || 0 < level_page_range_array_[page_info.page_level_].cached_page_num_
                      || 0 > level_page_index
                      || ObTmpFileGlobal::INVALID_PAGE_ID != level_page_range_array_[page_info.page_level_].end_page_id_)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "unexpected page_info", KR(ret), K(fd_), K(page_info), K(level_page_range_array_));
      } else if (OB_FAIL(wbp_->alloc_page(fd_, page_key, new_page_id, new_page_buff))) {
        STORAGE_LOG(WARN, "fail to alloc meta page", KR(ret), K(fd_), K(page_info), K(level_page_range_array_));
      } else if (FALSE_IT(stat_info_.meta_page_alloc_cnt_++)) {
      } else if (OB_ISNULL(new_page_buff)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "unexpected null page buff", KR(ret), K(fd_), KP(new_page_buff));
      } else if (OB_FAIL(wbp_->notify_load(fd_, new_page_id, page_key))) {
        STORAGE_LOG(ERROR, "fail to notify load for meta", KR(ret), K(fd_), K(new_page_id), K(page_key));
      } else {
        ObTmpPageValueHandle p_handle;
        ObTmpPageCacheKey key(page_info.block_index_, page_info.physical_page_id_, MTL_ID());
        if (OB_SUCC(ObTmpPageCache::get_instance().get_page(key, p_handle))) {
          MEMCPY(new_page_buff, p_handle.value_->get_buffer(), ObTmpFileGlobal::PAGE_SIZE);
        } else if (OB_ENTRY_NOT_EXIST != ret) {
          STORAGE_LOG(ERROR, "fail to read from read_cache", KR(ret), K(fd_), K(key));
        } else {
          ret = OB_SUCCESS;
          p_handle.reset();
          need_load_from_disk = true;
          if (OB_FAIL(ObTmpPageCache::get_instance().load_page(key, callback_allocator_, p_handle))) {
            STORAGE_LOG(WARN, "fail to load page from disk", KR(ret), K(fd_), K(key));
          } else {
            MEMCPY(new_page_buff, p_handle.value_->get_buffer(), ObTmpFileGlobal::PAGE_SIZE);
          }
        }
        if (FAILEDx(check_page_(new_page_buff))) {
          STORAGE_LOG(ERROR, "the page is invalid or corrupted", KR(ret), K(fd_), KP(new_page_buff));
        }
        if (OB_SUCC(ret)) {
          //change page state to cached
          if (OB_FAIL(wbp_->notify_load_succ(fd_, new_page_id, page_key))) {
            STORAGE_LOG(ERROR, "fail to notify load succ for meta", KR(ret), K(fd_), K(new_page_id), K(page_key));
          }
        } else {
          int tmp_ret = OB_SUCCESS;
          //change page state to invalid
          if (OB_TMP_FAIL(wbp_->notify_load_fail(fd_, new_page_id, page_key))) {
            STORAGE_LOG(ERROR, "fail to notify load fail for meta", KR(tmp_ret), K(fd_), K(new_page_id), K(page_key));
          }
        }
        p_handle.reset();
      }
      if (OB_SUCC(ret)) {
        int64_t origin_block_index = page_info.block_index_;
        int16_t origin_physical_page_id = page_info.physical_page_id_;
        page_info.buffer_page_id_ = new_page_id;
        page_info.block_index_ = ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX;
        page_info.physical_page_id_ = -1;
        if (ObTmpFileGlobal::INVALID_PAGE_ID == parent_page_id) {
          root_item_ = page_info;
        } else {
          char *parent_page_buff = NULL;
          uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
          ObSharedNothingTmpFileTreePageHeader page_header;
          int32_t parent_level_page_index = level_page_range_array_[page_info.page_level_ + 1].evicted_page_num_
                                              + level_page_range_array_[page_info.page_level_ + 1].cached_page_num_ - 1;
          ObTmpFilePageUniqKey parent_page_offset(page_info.page_level_ + 1, parent_level_page_index);
          if (OB_FAIL(wbp_->read_page(fd_, parent_page_id, parent_page_offset, parent_page_buff, next_page_id))) {
            STORAGE_LOG(ERROR, "fail to read from write cache", KR(ret), K(fd_), K(parent_page_id), K(parent_page_offset));
          } else if (OB_FAIL(read_page_header_(parent_page_buff, page_header))) {
            STORAGE_LOG(WARN, "fail to read page header", KR(ret), K(fd_), KP(parent_page_buff));
          } else if (OB_FAIL(rewrite_item_(parent_page_buff, page_header.item_num_ - 1, page_info))) {
            STORAGE_LOG(WARN, "fail to rewrite item", KR(ret), K(fd_), K(page_header), K(page_info), KP(parent_page_buff));
          } else if (OB_FAIL(wbp_->notify_dirty(fd_, parent_page_id, parent_page_offset))) {
            STORAGE_LOG(ERROR, "fail to notify dirty for meta", KR(ret), K(fd_), K(parent_page_id), K(parent_page_offset));
          }
        }
        if (OB_SUCC(ret)) {
          int16_t page_level = page_info.page_level_;
          if (OB_FAIL(wbp_->notify_dirty(fd_, new_page_id, page_key))) {
            STORAGE_LOG(ERROR, "fail to notify dirty", KR(ret), K(fd_), K(new_page_id), K(page_key));
          } else if (OB_FAIL(release_tmp_file_page_(origin_block_index, origin_physical_page_id, 1))) {
            STORAGE_LOG(WARN, "fail to release tmp file page", KR(ret), K(fd_), K(origin_block_index), K(origin_physical_page_id));
          } else {
            level_page_range_array_[page_level].start_page_id_ = new_page_id;
            level_page_range_array_[page_level].end_page_id_ = new_page_id;
            level_page_range_array_[page_level].cached_page_num_++;
            level_page_range_array_[page_level].evicted_page_num_--;
            level_page_range_array_[page_level].flushed_page_num_--;
          }
        }
      } else if (ObTmpFileGlobal::INVALID_PAGE_ID != new_page_id) { //fail
        uint32_t unused_next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(wbp_->free_page(fd_, new_page_id, page_key, unused_next_page_id))) {
          STORAGE_LOG(ERROR, "fail to free meta page", KR(tmp_ret), KR(ret), K(fd_), K(new_page_id), K(page_key));
        } else {
          stat_info_.meta_page_free_cnt_++;
        }
      }
      STORAGE_LOG(INFO, "load page to write cache", KR(ret), K(fd_), K(need_load_from_disk), K(page_info), K(page_key));
    } else {
      //still in write cache
      //do nothing
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
  ARRAY_FOREACH_N(level_page_range_array_, level, level_cnt) {
    const uint32_t start_page_id = level_page_range_array_[level].start_page_id_;
    const uint32_t end_page_id = level_page_range_array_[level].end_page_id_;
    if (ObTmpFileGlobal::INVALID_PAGE_ID != start_page_id) {
      uint32_t cur_page_id = start_page_id;
      int32_t level_page_index = level_page_range_array_[level].evicted_page_num_;
      while (OB_SUCC(ret)
             && ObTmpFileGlobal::INVALID_PAGE_ID != cur_page_id) {
        char *page_buff = NULL;
        uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
        ObTmpFilePageUniqKey page_key(level, level_page_index);
        if (OB_FAIL(wbp_->read_page(fd_, cur_page_id, page_key, page_buff, next_page_id))) {
          STORAGE_LOG(WARN, "fail to read from write cache", KR(ret), K(fd_), K(cur_page_id), K(page_key));
        } else {
          ObSharedNothingTmpFileTreePageHeader tmp_page_header;
          if (0 == level) {
            data_items.reset();
            read_page_content_(page_buff, tmp_page_header, data_items);
            STORAGE_LOG(INFO, "dump cached leaf page", KR(ret), K(fd_), KP(page_buff), K(cur_page_id), K(page_key), K(tmp_page_header), K(data_items));
          } else {
            meta_items.reset();
            read_page_content_(page_buff, tmp_page_header, meta_items);
            STORAGE_LOG(INFO, "dump cached internal page", KR(ret), K(fd_), KP(page_buff), K(cur_page_id), K(page_key), K(tmp_page_header), K(meta_items));
          }
        }
        cur_page_id = next_page_id;
        level_page_index++;
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

int ObSharedNothingTmpFileMetaTree::copy_info(ObSNTmpFileInfo &tmp_file_info)
{
  int ret = OB_SUCCESS;
  const int64_t abs_timeout_us = ObTimeUtility::current_time() + 100 * 1000L;
  if (OB_FAIL(lock_.rdlock(abs_timeout_us))) {
    STORAGE_LOG(WARN, "fail to rdlock", KR(ret), K(fd_), K(abs_timeout_us));
  } else {
    int64_t cached_page_num = 0;
    int64_t total_page_num = 0;
    for (int16_t i = 0; i < level_page_range_array_.count(); i++) {
      cached_page_num += level_page_range_array_.at(i).cached_page_num_;
      total_page_num += level_page_range_array_.at(i).cached_page_num_ + level_page_range_array_.at(i).evicted_page_num_;
    }
    tmp_file_info.meta_tree_epoch_ = tree_epoch_;
    tmp_file_info.meta_tree_level_cnt_ = level_page_range_array_.count();
    tmp_file_info.meta_size_ = total_page_num * ObTmpFileGlobal::PAGE_SIZE;
    tmp_file_info.cached_meta_page_num_ = cached_page_num;
    tmp_file_info.write_back_meta_page_num_ = stat_info_.meta_page_flushing_cnt_;
    tmp_file_info.all_type_page_flush_cnt_ = stat_info_.all_type_page_flush_cnt_;
    lock_.rdunlock();
  }
  return ret;
}

}
}
