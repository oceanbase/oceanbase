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

#include "storage/tmp_file/ob_shared_nothing_tmp_file.h"
#include "storage/tmp_file/ob_tmp_file_io_ctx.h"
#include "storage/tmp_file/ob_tmp_file_page_cache_controller.h"

namespace oceanbase
{
namespace tmp_file
{
void ObSNTmpFileInfo::reset()
{
  meta_tree_epoch_ = 0;
  meta_tree_level_cnt_ = 0;
  meta_size_ = 0;
  cached_meta_page_num_ = 0;
  write_back_meta_page_num_ = 0;
  all_type_page_flush_cnt_ = 0;
  ObTmpFileInfo::reset();
}

ObSharedNothingTmpFile::InnerFlushContext::InnerFlushContext()
      : flush_seq_(ObTmpFileGlobal::INVALID_FLUSH_SEQUENCE),
        data_finished_continuous_flush_info_num_(0),
        meta_finished_continuous_flush_info_num_(0),
        data_flush_infos_(),
        meta_flush_infos_(),
        need_to_wait_for_the_previous_data_flush_req_to_complete_(false),
        need_to_wait_for_the_previous_meta_flush_req_to_complete_(false)
{
  data_flush_infos_.set_attr(ObMemAttr(MTL_ID(), "TmpFileFInfo"));
  meta_flush_infos_.set_attr(ObMemAttr(MTL_ID(), "TmpFileFInfo"));
}

void ObSharedNothingTmpFile::InnerFlushContext::reset()
{
  meta_finished_continuous_flush_info_num_ = 0;
  data_finished_continuous_flush_info_num_ = 0;
  meta_flush_infos_.reset();
  data_flush_infos_.reset();
  flush_seq_ = ObTmpFileGlobal::INVALID_FLUSH_SEQUENCE;
  need_to_wait_for_the_previous_data_flush_req_to_complete_ = false;
  need_to_wait_for_the_previous_meta_flush_req_to_complete_ = false;
}

int ObSharedNothingTmpFile::InnerFlushContext::update_finished_continuous_flush_info_num(const bool is_meta, const int64_t end_pos)
{
  int ret = OB_SUCCESS;
  if (is_meta) {
    if (OB_UNLIKELY(end_pos < meta_finished_continuous_flush_info_num_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected flush info num", KR(ret), K(end_pos), K(meta_finished_continuous_flush_info_num_));
    } else {
      meta_finished_continuous_flush_info_num_ = end_pos;
    }
  } else {
    if (OB_UNLIKELY(end_pos < data_finished_continuous_flush_info_num_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected flush info num", KR(ret), K(end_pos), K(data_finished_continuous_flush_info_num_));
    } else {
      data_finished_continuous_flush_info_num_ = end_pos;
    }
  }
  return ret;
}

ObSharedNothingTmpFile::InnerFlushInfo::InnerFlushInfo()
  : update_meta_data_done_(false),
    flush_data_page_disk_begin_id_(ObTmpFileGlobal::INVALID_PAGE_ID),
    flush_data_page_num_(-1),
    flush_virtual_page_id_(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID),
    file_size_(0),
    flush_meta_page_array_()
{
  flush_meta_page_array_.set_attr(ObMemAttr(MTL_ID(), "TFFlushMetaArr"));
}

void ObSharedNothingTmpFile::InnerFlushInfo::reset()
{
  update_meta_data_done_ = false;
  flush_data_page_disk_begin_id_ = ObTmpFileGlobal::INVALID_PAGE_ID;
  flush_data_page_num_ = -1;
  flush_virtual_page_id_ = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
  file_size_ = 0;
  flush_meta_page_array_.reset();
}

int ObSharedNothingTmpFile::InnerFlushInfo::init_by_tmp_file_flush_info(const ObTmpFileFlushInfo& flush_info)
{
  int ret = OB_SUCCESS;
  flush_data_page_disk_begin_id_ = flush_info.flush_data_page_disk_begin_id_;
  flush_data_page_num_ = flush_info.flush_data_page_num_;
  flush_virtual_page_id_ = flush_info.flush_virtual_page_id_;
  file_size_ = flush_info.file_size_;
  if (!flush_info.flush_meta_page_array_.empty()
      && OB_FAIL(flush_meta_page_array_.assign(flush_info.flush_meta_page_array_))) {
    LOG_WARN("fail to assign flush_meta_page_array_", KR(ret), K(flush_info));
  }
  return ret;
}

ObSharedNothingTmpFile::ObSharedNothingTmpFile()
    : ObITmpFile(),
      tmp_file_block_manager_(nullptr),
      page_cache_controller_(nullptr),
      eviction_mgr_(nullptr),
      is_in_data_eviction_list_(false),
      is_in_meta_eviction_list_(false),
      meta_page_flush_level_(-1),
      flushed_data_page_num_(0),
      flushed_page_id_(ObTmpFileGlobal::INVALID_PAGE_ID),
      flushed_page_virtual_id_(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID),
      meta_tree_(),
      meta_flush_node_(*this),
      data_eviction_node_(*this),
      meta_eviction_node_(*this),
      truncate_lock_(common::ObLatchIds::TMP_FILE_LOCK),
      inner_flush_ctx_()
{
  mode_ = ObTmpFileMode::SHARED_NOTHING;
}

ObSharedNothingTmpFile::~ObSharedNothingTmpFile()
{
  reset();
}

int ObSharedNothingTmpFile::init(const uint64_t tenant_id, const int64_t fd, const int64_t dir_id,
                                 ObTmpFileBlockManager *block_manager,
                                 ObIAllocator *callback_allocator,
                                 ObIAllocator *wbp_index_cache_allocator,
                                 ObIAllocator *wbp_index_cache_bkt_allocator,
                                 ObTmpFilePageCacheController *pc_ctrl,
                                 const char* label)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), KPC(this));
  } else if (OB_FAIL(ObITmpFile::init(tenant_id, dir_id, fd, &pc_ctrl->get_write_buffer_pool(),
                                      &pc_ctrl->get_flush_priority_mgr(),
                                      callback_allocator,
                                      wbp_index_cache_allocator,
                                      wbp_index_cache_bkt_allocator,
                                      label))) {
    LOG_WARN("init ObITmpFile failed", KR(ret), K(tenant_id), K(dir_id), K(fd), KP(label));
  } else {
    if (OB_ISNULL(block_manager) || OB_ISNULL(pc_ctrl)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), KP(block_manager), KP(pc_ctrl));
    } else if (OB_FAIL(meta_tree_.init(fd, &pc_ctrl->get_write_buffer_pool(), callback_allocator, block_manager))) {
      LOG_WARN("fail to init meta tree", KR(ret), K(fd));
    } else {
      tmp_file_block_manager_ = block_manager;
      page_cache_controller_ = pc_ctrl;
      eviction_mgr_ = &pc_ctrl->get_eviction_manager();
    }
    if (OB_FAIL(ret)) {
      is_inited_ = false;
    }
  }

  LOG_INFO("tmp file init over", KR(ret), K(fd), K(dir_id));
  return ret;
}

void ObSharedNothingTmpFile::reset()
{
  if (is_inited_) {
    tmp_file_block_manager_ = nullptr;
    page_cache_controller_ = nullptr;
    eviction_mgr_ = nullptr;
    is_in_data_eviction_list_ = false;
    is_in_meta_eviction_list_ = false;
    meta_page_flush_level_ = -1;
    flushed_data_page_num_ = 0;
    flushed_page_id_ = ObTmpFileGlobal::INVALID_PAGE_ID;
    flushed_page_virtual_id_ = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
    meta_tree_.reset();
    meta_flush_node_.unlink();
    data_eviction_node_.unlink();
    meta_eviction_node_.unlink();
    inner_flush_ctx_.reset();
    ObITmpFile::reset();
  }
}

int ObSharedNothingTmpFile::release_resource()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    int64_t fd_backup = fd_;
    int64_t free_cnt = 0;

    LOG_INFO("tmp file release_resource start", KR(ret), K(fd_), KPC(this));

    if (cached_page_nums_ > 0) {
      uint32_t cur_page_id = begin_page_id_;
      while (OB_SUCC(ret) && cur_page_id != ObTmpFileGlobal::INVALID_PAGE_ID) {
        uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
        if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == begin_page_virtual_id_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("begin page virtual id is invalid", KR(ret), K(fd_), K(begin_page_virtual_id_));
        } else if (OB_FAIL(wbp_->free_page(fd_, cur_page_id, ObTmpFilePageUniqKey(begin_page_virtual_id_), next_page_id))) {
          LOG_ERROR("fail to free page", KR(ret), K(fd_), K(cur_page_id), K(begin_page_virtual_id_));
        } else {
          free_cnt++;
          cur_page_id = next_page_id;
          begin_page_virtual_id_ += 1;
        }
      }
    }
    if (OB_SUCC(ret) && cached_page_nums_ != free_cnt) {
      LOG_ERROR("tmp file release resource, cached_page_nums_ and free_cnt are not equal", KR(ret), K(fd_), K(free_cnt), KPC(this));
    }

    LOG_INFO("tmp file release resource, free wbp page phase over", KR(ret), K(fd_), KPC(this));

    if (FAILEDx(meta_tree_.clear(truncated_offset_, file_size_))) {
      LOG_ERROR("fail to clear meta tree", KR(ret), K(fd_), K(truncated_offset_), K(file_size_));
    }
  }

  LOG_INFO("tmp file release resource over", KR(ret), "fd", fd_);
  return ret;
}

int ObSharedNothingTmpFile::inner_delete_file_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(eviction_mgr_->remove_file(*this))) {
    LOG_WARN("fail to remove file from eviction manager", KR(ret), KPC(this));
  } else if (OB_FAIL(flush_prio_mgr_->remove_file(*this))) {
    LOG_WARN("fail to remove file from flush priority manager", KR(ret),KPC(this));
  }

  return ret;
}

int ObSharedNothingTmpFile::inner_seal_()
{
  int ret = OB_SUCCESS;

  return ret;
}

int ObSharedNothingTmpFile::inner_read_from_disk_(const int64_t expected_read_disk_size,
                                                  ObTmpFileIOCtx &io_ctx)
{
  int ret = OB_SUCCESS;
  int64_t total_kv_cache_page_read_cnt = 0;
  int64_t total_uncached_page_read_cnt = 0;
  int64_t kv_cache_page_read_hits = 0;
  int64_t uncached_page_read_hits = 0;
  common::ObArray<ObSharedNothingTmpFileDataItem> data_items;
  if (OB_FAIL(meta_tree_.search_data_items(io_ctx.get_read_offset_in_file(),
                                           expected_read_disk_size, data_items))) {
    LOG_WARN("fail to search data items", KR(ret), K(fd_), K(expected_read_disk_size), K(io_ctx));
  } else if (OB_UNLIKELY(data_items.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no data item found", KR(ret), K(fd_), K(expected_read_disk_size), K(io_ctx));
  }

  // Iterate to read each block.
  int64_t remain_read_size = expected_read_disk_size;
  for (int64_t i = 0; OB_SUCC(ret) && i < data_items.count() && 0 < remain_read_size; i++) {
    const int64_t block_index = data_items[i].block_index_;
    const int64_t read_start_virtual_page_id = get_page_virtual_id_(io_ctx.get_read_offset_in_file(),
                                                                    false /*is_open_interval*/);
    const int64_t start_page_id_in_data_item = read_start_virtual_page_id - data_items[i].virtual_page_id_;
    const int64_t begin_offset_in_block =
        (data_items[i].physical_page_id_ + start_page_id_in_data_item) * ObTmpFileGlobal::PAGE_SIZE;
    const int64_t end_offset_in_block =
        (data_items[i].physical_page_id_ + data_items[i].physical_page_num_) * ObTmpFileGlobal::PAGE_SIZE;
    const int64_t begin_read_offset_in_block = begin_offset_in_block +
                                               (0 == i?
                                                get_offset_in_page_(io_ctx.get_read_offset_in_file()) : 0);
    const int64_t end_read_offset_in_block = (data_items.count() - 1  == i?
                                              begin_read_offset_in_block + remain_read_size :
                                              end_offset_in_block);

    ObTmpBlockValueHandle block_value_handle;
    if (!io_ctx.is_disable_block_cache() &&
        OB_SUCC(ObTmpBlockCache::get_instance().get_block(ObTmpBlockCacheKey(block_index, MTL_ID()),
                                                          block_value_handle))) {
      LOG_DEBUG("hit block cache", K(block_index), K(fd_), K(io_ctx));
      char *read_buf = io_ctx.get_todo_buffer();
      const int64_t read_size = end_read_offset_in_block - begin_read_offset_in_block;
      ObTmpFileIOCtx::ObBlockCacheHandle block_cache_handle(block_value_handle, read_buf,
                                                            begin_read_offset_in_block,
                                                            read_size);
      if (OB_FAIL(io_ctx.get_block_cache_handles().push_back(block_cache_handle))) {
        LOG_WARN("Fail to push back into block_handles", KR(ret), K(fd_));
      } else if (OB_FAIL(io_ctx.update_data_size(read_size))) {
        LOG_WARN("fail to update data size", KR(ret), K(read_size));
      } else {
        kv_cache_page_read_hits++;
        total_kv_cache_page_read_cnt += (get_page_end_offset_(io_ctx.get_read_offset_in_file()) -
                                         get_page_begin_offset_(io_ctx.get_read_offset_in_file() - read_size)) /
                                        ObTmpFileGlobal::PAGE_SIZE;
        remain_read_size -= read_size;
        LOG_DEBUG("succ to read data from cached block",
              KR(ret), K(fd_), K(block_index), K(begin_offset_in_block), K(end_offset_in_block),
              K(begin_read_offset_in_block), K(end_read_offset_in_block),
              K(remain_read_size), K(read_size), K(expected_read_disk_size),
              K(data_items[i]), K(io_ctx));
      }
    } else if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret && OB_SUCCESS != ret)) {
      LOG_WARN("fail to get block", KR(ret), K(fd_), K(block_index));
    } else { // not hit block cache, read page from disk.
      ret = OB_SUCCESS;
      const int64_t read_size = end_read_offset_in_block - begin_read_offset_in_block;
      if (io_ctx.is_disable_page_cache()) {
        if (OB_FAIL(inner_direct_read_from_block_(block_index,
                                                  begin_read_offset_in_block,
                                                  end_read_offset_in_block,
                                                  io_ctx))) {
          LOG_WARN("fail to direct read from block",
              KR(ret), K(fd_), K(block_index), K(begin_offset_in_block), K(end_offset_in_block),
              K(begin_read_offset_in_block), K(end_read_offset_in_block),
              K(remain_read_size), K(expected_read_disk_size),
              K(data_items[i]), K(io_ctx), KPC(this));
        } else {
          uncached_page_read_hits++;
          total_uncached_page_read_cnt += (get_page_end_offset_(io_ctx.get_read_offset_in_file()) -
                                           get_page_begin_offset_(io_ctx.get_read_offset_in_file() - read_size)) /
                                          ObTmpFileGlobal::PAGE_SIZE;
        }
      } else {
        if (OB_FAIL(inner_cached_read_from_block_(block_index,
                                                  begin_read_offset_in_block,
                                                  end_read_offset_in_block,
                                                  io_ctx,
                                                  total_kv_cache_page_read_cnt,
                                                  total_uncached_page_read_cnt,
                                                  kv_cache_page_read_hits,
                                                  uncached_page_read_hits))) {
          LOG_WARN("fail to cached read from block",
              KR(ret), K(fd_), K(block_index), K(begin_offset_in_block), K(end_offset_in_block),
              K(begin_read_offset_in_block), K(end_read_offset_in_block),
              K(remain_read_size),K(expected_read_disk_size),
              K(data_items[i]), K(io_ctx), KPC(this));
        }
      }
      if (OB_SUCC(ret)) {
        remain_read_size -= read_size;
        LOG_DEBUG("succ to read from block",
              KR(ret), K(fd_), K(block_index), K(begin_offset_in_block), K(end_offset_in_block),
              K(begin_read_offset_in_block), K(end_read_offset_in_block),
              K(remain_read_size), K(expected_read_disk_size),
              K(data_items[i]), K(io_ctx), KPC(this));
      }
    }
  } // end for

  if (OB_SUCC(ret)) {
    io_ctx.update_read_kv_cache_page_stat(total_kv_cache_page_read_cnt, kv_cache_page_read_hits);
    io_ctx.update_sn_read_uncached_page_stat(total_uncached_page_read_cnt, uncached_page_read_hits);
  }
  return ret;
}

int ObSharedNothingTmpFile::inner_direct_read_from_block_(const int64_t block_index,
                                                          const int64_t begin_read_offset_in_block,
                                                          const int64_t end_read_offset_in_block,
                                                          ObTmpFileIOCtx &io_ctx)
{
  int ret = OB_SUCCESS;
  const int64_t expected_read_size = end_read_offset_in_block - begin_read_offset_in_block;
  ObTmpFileBlockHandle block_handle;

  if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX == block_index ||
                  expected_read_size <= 0 || expected_read_size > ObTmpFileGlobal::SN_BLOCK_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd_), K(block_index),
                                 K(begin_read_offset_in_block),
                                 K(end_read_offset_in_block));
  } else if (OB_FAIL(tmp_file_block_manager_->get_tmp_file_block_handle(block_index, block_handle))) {
    LOG_WARN("fail to get tmp file block_handle", KR(ret), K(fd_), K(block_index));
  } else if (OB_ISNULL(block_handle.get()) || OB_UNLIKELY(!block_handle.get()->get_macro_block_id().is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get tmp file block_handle", KR(ret), K(fd_), K(block_handle));
  } else {
    char *read_buf = io_ctx.get_todo_buffer();
    ObTmpFileIOCtx::ObIOReadHandle io_read_handle(read_buf,
                                                  0 /*offset_in_src_data_buf_*/,
                                                  expected_read_size, block_handle);
    ObTmpPageCacheReadInfo read_info;
    if (OB_FAIL(io_ctx.get_io_handles().push_back(io_read_handle))) {
      LOG_WARN("Fail to push back into io_handles", KR(ret), K(fd_));
    } else if (OB_FAIL(read_info.init_read(block_handle.get()->get_macro_block_id(),
                                           expected_read_size, begin_read_offset_in_block,
                                           io_ctx.get_io_flag(), io_ctx.get_io_timeout_ms(),
                                           &io_ctx.get_io_handles().at(io_ctx.get_io_handles().count()-1).handle_))) {
      LOG_WARN("fail to init sn read info", KR(ret), K(fd_), K(block_handle), K(expected_read_size),
                                            K(begin_read_offset_in_block), K(io_ctx));
    } else if (OB_FAIL(ObTmpPageCache::get_instance().direct_read(read_info, *callback_allocator_))) {
      LOG_WARN("fail to direct_read", KR(ret), K(fd_), K(read_info), K(io_ctx), KP(callback_allocator_));
    }
  }

  // Update read offset and read size.
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(io_ctx.update_data_size(expected_read_size))) {
    LOG_WARN("fail to update data size", KR(ret), K(expected_read_size));
  }

  return ret;
}

int ObSharedNothingTmpFile::inner_cached_read_from_block_(const int64_t block_index,
                                                          const int64_t begin_read_offset_in_block,
                                                          const int64_t end_read_offset_in_block,
                                                          ObTmpFileIOCtx &io_ctx,
                                                          int64_t &total_kv_cache_page_read_cnt,
                                                          int64_t &total_uncached_page_read_cnt,
                                                          int64_t &kv_cache_page_read_hits,
                                                          int64_t &uncached_page_read_hits)
{
  int ret = OB_SUCCESS;
  const int64_t begin_page_idx_in_block = get_page_virtual_id_(begin_read_offset_in_block, false);
  const int64_t end_page_idx_in_block = get_page_virtual_id_(end_read_offset_in_block, true);
  ObTmpFileBlockPageBitmap bitmap;
  ObTmpFileBlockPageBitmapIterator iterator;
  ObArray<ObTmpPageValueHandle> page_value_handles;

  if (OB_FAIL(collect_pages_in_block_(block_index, begin_page_idx_in_block, end_page_idx_in_block,
                                      bitmap, page_value_handles))) {
    LOG_WARN("fail to collect pages in block", KR(ret), K(fd_), K(block_index),
                                               K(begin_page_idx_in_block),
                                               K(end_page_idx_in_block));
  } else if (OB_FAIL(iterator.init(&bitmap, begin_page_idx_in_block, end_page_idx_in_block))) {
    LOG_WARN("fail to init iterator", KR(ret), K(fd_), K(block_index),
                                      K(begin_page_idx_in_block), K(end_page_idx_in_block));
  } else {
    int64_t already_read_cached_page_num = 0;
    while (OB_SUCC(ret) && iterator.has_next()) {
      bool is_in_cache = false;
      int64_t begin_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
      int64_t end_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
      int64_t begin_read_offset = -1;
      int64_t end_read_offset = -1;
      if (OB_FAIL(iterator.next_range(is_in_cache, begin_page_id, end_page_id))) {
        LOG_WARN("fail to next range", KR(ret), K(fd_));
      } else if (OB_UNLIKELY(begin_page_id > end_page_id || begin_page_id < 0 ||  end_page_id < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid range", KR(ret), K(fd_), K(begin_page_id), K(end_page_id));
      } else {
        begin_read_offset = begin_page_id == begin_page_idx_in_block ?
                            begin_read_offset_in_block :
                            get_page_begin_offset_by_virtual_id_(begin_page_id);
        end_read_offset = end_page_id == end_page_idx_in_block ?
                          end_read_offset_in_block :
                          get_page_end_offset_by_virtual_id_(end_page_id);
      }

      if (OB_FAIL(ret)) {
      } else if (is_in_cache) {
        if (OB_FAIL(inner_read_continuous_cached_pages_(begin_read_offset, end_read_offset,
                                                        page_value_handles, already_read_cached_page_num,
                                                        io_ctx))) {
          LOG_WARN("fail to inner read continuous cached pages", KR(ret), K(fd_), K(begin_read_offset),
                                                                 K(end_read_offset), K(io_ctx));
        } else {
          total_kv_cache_page_read_cnt += end_page_id - begin_page_id + 1;
          already_read_cached_page_num += (end_page_id - begin_page_id + 1);
          kv_cache_page_read_hits++;
        }
      } else {
        if (OB_FAIL(inner_read_continuous_uncached_pages_(block_index, begin_read_offset,
                                                          end_read_offset, io_ctx))) {
          LOG_WARN("fail to inner read continuous uncached pages", KR(ret), K(fd_), K(block_index),
                                                                   K(begin_read_offset),
                                                                   K(end_read_offset),
                                                                   K(io_ctx));
        } else {
          total_uncached_page_read_cnt += (get_page_end_offset_(end_read_offset) -
                                           get_page_begin_offset_(begin_read_offset)) /
                                          ObTmpFileGlobal::PAGE_SIZE;
          uncached_page_read_hits++;
        }
      }
    }
  }

  return ret;
}

int ObSharedNothingTmpFile::collect_pages_in_block_(const int64_t block_index,
                                                    const int64_t begin_page_idx_in_block,
                                                    const int64_t end_page_idx_in_block,
                                                    ObTmpFileBlockPageBitmap &bitmap,
                                                    ObIArray<ObTmpPageValueHandle> &page_value_handles)
{
  int ret = OB_SUCCESS;
  bitmap.reset();
  page_value_handles.reset();
  if (OB_FAIL(page_value_handles.reserve(ObTmpFileGlobal::BLOCK_PAGE_NUMS))) {
    LOG_WARN("fail to reserve", KR(ret), K(fd_));
  } else {
    for (int64_t page_idx_in_block = begin_page_idx_in_block;
        OB_SUCC(ret) && page_idx_in_block <= end_page_idx_in_block;
        page_idx_in_block++) {
      ObTmpPageCacheKey key(block_index, page_idx_in_block, tenant_id_);
      ObTmpPageValueHandle p_handle;
      if (OB_SUCC(ObTmpPageCache::get_instance().get_page(key, p_handle))) {
        if (OB_FAIL(page_value_handles.push_back(p_handle))) {
          LOG_WARN("fail to push back", KR(ret), K(fd_), K(key));
        } else if (OB_FAIL(bitmap.set_bitmap(page_idx_in_block, true))) {
          LOG_WARN("fail to set bitmap", KR(ret), K(fd_), K(key));
        }
      } else if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        if (OB_FAIL(bitmap.set_bitmap(page_idx_in_block, false))) {
          LOG_WARN("fail to set bitmap", KR(ret), K(fd_), K(key));
        }
      } else {
        LOG_WARN("fail to get page from cache", KR(ret), K(fd_), K(key));
      }
    }
  }

  return ret;
}

int ObSharedNothingTmpFile::inner_read_continuous_uncached_pages_(const int64_t block_index,
                                                                  const int64_t begin_read_offset_in_block,
                                                                  const int64_t end_read_offset_in_block,
                                                                  ObTmpFileIOCtx &io_ctx)
{
  int ret = OB_SUCCESS;
  ObArray<ObTmpPageCacheKey> page_keys;
  const int64_t begin_page_idx = get_page_virtual_id_(begin_read_offset_in_block, false);
  const int64_t end_page_idx = get_page_virtual_id_(end_read_offset_in_block, true);
  const int64_t block_read_begin_offset = get_page_begin_offset_(begin_read_offset_in_block);
  const int64_t block_read_end_offset = get_page_end_offset_(end_read_offset_in_block);
  const int64_t block_read_size = block_read_end_offset - block_read_begin_offset;  // read and cached completed pages from disk
  const int64_t usr_read_begin_offset = begin_read_offset_in_block;
  const int64_t usr_read_end_offset = end_read_offset_in_block;
  // from loaded disk block buf, from "offset_in_block_buf" read "usr_read_size" size data to user's read buf
  const int64_t offset_in_block_buf = usr_read_begin_offset - block_read_begin_offset;
  const int64_t usr_read_size = block_read_size -
                                (usr_read_begin_offset - block_read_begin_offset) -
                                (block_read_end_offset - usr_read_end_offset);

  for (int64_t page_id = begin_page_idx; OB_SUCC(ret) && page_id <= end_page_idx; page_id++) {
    ObTmpPageCacheKey key(block_index, page_id, tenant_id_);
    if (OB_FAIL(page_keys.push_back(key))) {
      LOG_WARN("fail to push back", KR(ret), K(fd_), K(key));
    }
  }

  ObTmpFileBlockHandle block_handle;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(tmp_file_block_manager_->get_tmp_file_block_handle(block_index, block_handle))) {
    LOG_WARN("fail to get tmp file block_handle", KR(ret), K(fd_), K(block_index));
  } else {
    char *user_read_buf = io_ctx.get_todo_buffer();
    ObTmpFileIOCtx::ObIOReadHandle io_read_handle(user_read_buf,
                                                  offset_in_block_buf,
                                                  usr_read_size, block_handle);
    ObTmpPageCacheReadInfo read_info;

    if (OB_FAIL(io_ctx.get_io_handles().push_back(io_read_handle))) {
      LOG_WARN("Fail to push back into io_handles", KR(ret), K(fd_));
    } else if (OB_FAIL(read_info.init_read(block_handle.get()->get_macro_block_id(),
                                           page_keys.count() * ObTmpFileGlobal::PAGE_SIZE,
                                           block_read_begin_offset,
                                           io_ctx.get_io_flag(), io_ctx.get_io_timeout_ms(),
                                           &io_ctx.get_io_handles().at(io_ctx.get_io_handles().count()-1).handle_))) {
      LOG_WARN("fail to init sn read info", KR(ret), K(fd_), K(block_handle), K(page_keys.count()),
                                            K(block_read_begin_offset), K(io_ctx));
    } else if (OB_FAIL(ObTmpPageCache::get_instance().cached_read(page_keys, read_info, *callback_allocator_))) {
      LOG_WARN("fail to cached_read", KR(ret), K(fd_), K(read_info), K(io_ctx), KP(callback_allocator_));
    } else if (OB_FAIL(io_ctx.update_data_size(usr_read_size))) {
      LOG_WARN("fail to update data size", KR(ret), K(fd_), K(usr_read_size));
    }
  }

  return ret;
}

int ObSharedNothingTmpFile::inner_read_continuous_cached_pages_(const int64_t begin_read_offset_in_block,
                                                                const int64_t end_read_offset_in_block,
                                                                const ObArray<ObTmpPageValueHandle> &page_value_handles,
                                                                const int64_t start_array_idx,
                                                                ObTmpFileIOCtx &io_ctx)
{
  int ret = OB_SUCCESS;
  const int64_t begin_page_idx = get_page_virtual_id_(begin_read_offset_in_block, false);
  const int64_t end_page_idx = get_page_virtual_id_(end_read_offset_in_block, true);
  const int64_t iter_array_cnt = end_page_idx - begin_page_idx + 1;

  if (OB_UNLIKELY(start_array_idx < 0 || start_array_idx + iter_array_cnt > page_value_handles.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid array idx", KR(ret), K(fd_), K(start_array_idx), K(iter_array_cnt),
             K(begin_page_idx), K(end_page_idx), K(page_value_handles.count()));
  }

  int64_t read_offset = begin_read_offset_in_block;
  int64_t cur_array_idx = start_array_idx;
  for (int64_t cached_page_idx = begin_page_idx; OB_SUCC(ret) && cached_page_idx <= end_page_idx; cached_page_idx++) {
    ObTmpPageValueHandle p_handle = page_value_handles.at(cur_array_idx++);
    char *read_buf = io_ctx.get_todo_buffer();
    const int64_t cur_page_end_offset = get_page_end_offset_by_virtual_id_(cached_page_idx);
    const int64_t read_size = MIN(cur_page_end_offset, end_read_offset_in_block) - read_offset;
    const int64_t read_offset_in_page = get_offset_in_page_(read_offset);
    ObTmpFileIOCtx::ObPageCacheHandle page_handle(p_handle, read_buf,
                                                  read_offset_in_page,
                                                  read_size);
    if (OB_FAIL(io_ctx.get_page_cache_handles().push_back(page_handle))) {
      LOG_WARN("Fail to push back into page_handles", KR(ret), K(fd_));
    } else if (OB_FAIL(io_ctx.update_data_size(read_size))) {
      LOG_WARN("fail to update data size", KR(ret), K(fd_), K(read_size));
    } else {
      read_offset += read_size;
    }
  } // end for

  return ret;
}

int ObSharedNothingTmpFile::swap_page_to_disk_(const ObTmpFileIOCtx &io_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(page_cache_controller_->invoke_swap_and_wait(
          MIN(io_ctx.get_todo_size(), ObTmpFileGlobal::TMP_FILE_WRITE_BATCH_PAGE_NUM * ObTmpFileGlobal::PAGE_SIZE),
          io_ctx.get_io_timeout_ms()))) {
    LOG_WARN("fail to invoke swap and wait", KR(ret), K(io_ctx), K(fd_));
  }

  return ret;
}

int ObSharedNothingTmpFile::load_disk_tail_page_and_rewrite_(ObTmpFileIOCtx &io_ctx)
{
  int ret = OB_SUCCESS;
  // `file_size_` is already under multi-write lock's protection, no need to fetch meta lock.
  const int64_t has_written_size = get_offset_in_page_(file_size_);
  const int64_t tail_page_virtual_id = get_page_virtual_id_(file_size_, true /*is_open_interval*/);
  const int64_t write_size = MIN(ObTmpFileGlobal::PAGE_SIZE - has_written_size, io_ctx.get_todo_size());
  char *write_buff = io_ctx.get_todo_buffer();
  uint32_t new_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  ObSharedNothingTmpFileDataItem data_item;
  bool block_meta_tree_flushing = false;
  bool has_update_file_meta = false;

  if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == tail_page_virtual_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("begin page virtual id is invalid", KR(ret), K(fd_), K(tail_page_virtual_id), K(file_size_));
  } else if (OB_UNLIKELY(has_written_size + write_size > ObTmpFileGlobal::PAGE_SIZE)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("need write size is invalid", KR(ret), K(fd_), K(has_written_size), K(write_size));
  } else if (OB_ISNULL(write_buff)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("write buffer is null", KR(ret), K(fd_), K(write_buff));
  } else if (OB_FAIL(meta_tree_.prepare_for_write_tail(data_item))) {
    LOG_WARN("fail to prepare for write tail", KR(ret), K(fd_));
  } else {
    block_meta_tree_flushing = true;
  }

  if (OB_SUCC(ret)) {
    blocksstable::MacroBlockId macro_block_id;
    int64_t last_page_begin_offset_in_block =
                      (data_item.physical_page_id_ + data_item.physical_page_num_ - 1)
                      * ObTmpFileGlobal::PAGE_SIZE;
    char *page_buf = nullptr;
    if (OB_FAIL(tmp_file_block_manager_->get_macro_block_id(data_item.block_index_, macro_block_id))) {
      LOG_WARN("fail to get macro block id", KR(ret), K(fd_), K(data_item.block_index_));
    } else if (OB_UNLIKELY(!macro_block_id.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("macro block id is invalid", KR(ret), K(fd_), K(data_item.block_index_));
    } else if (OB_FAIL(wbp_->alloc_page(fd_, ObTmpFilePageUniqKey(tail_page_virtual_id), new_page_id, page_buf))) {
      LOG_WARN("fail to alloc page", KR(ret), K(fd_), K(tail_page_virtual_id),
               K(new_page_id), KP(page_buf));
    } else {
      // load last unfilled page from disk
      blocksstable::ObMacroBlockHandle mb_handle;
      blocksstable::ObMacroBlockReadInfo info;
      info.io_desc_ = io_ctx.get_io_flag();
      info.io_desc_.set_mode(ObIOMode::READ);
      info.macro_block_id_ = macro_block_id;
      info.size_ = has_written_size;
      info.offset_ = last_page_begin_offset_in_block;
      info.buf_ = page_buf;
      info.io_callback_ = nullptr;
      info.io_timeout_ms_ = io_ctx.get_io_timeout_ms();

      if (OB_FAIL(mb_handle.async_read(info))) {
        LOG_ERROR("fail to async write block", KR(ret), K(fd_), K(info));
      } else if (OB_FAIL(mb_handle.wait())) {
        LOG_WARN("fail to wait", KR(ret), K(fd_), K(info));
      } else if (mb_handle.get_data_size() < has_written_size) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to read expected size", KR(ret), K(fd_), K(info), K(has_written_size));
      } else if (OB_UNLIKELY(!io_ctx.check_buf_range_valid(write_buff, write_size))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid buf range", KR(ret), K(fd_), K(write_buff), K(write_size), K(io_ctx));
      } else {
        // fill last page in memory
        MEMCPY(page_buf + has_written_size, write_buff, write_size);
      }
      if (FAILEDx(wbp_->notify_dirty(fd_, new_page_id, ObTmpFilePageUniqKey(tail_page_virtual_id)))) {
        LOG_WARN("fail to notify dirty", KR(ret), K(fd_), K(new_page_id));
      }
    }
  }

  // update meta data
  if (OB_SUCC(ret)) {
    common::TCRWLock::WLockGuard guard(meta_lock_);
    if (OB_UNLIKELY(is_deleting_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("file is deleting", KR(ret), K(fd_));
    } else if (OB_FAIL(meta_tree_.finish_write_tail(data_item, true /*release_tail_in_disk*/))) {
      LOG_WARN("fail to finish write tail page", KR(ret), K(fd_));
    } else if (OB_FAIL(io_ctx.update_data_size(write_size))) {
      LOG_WARN("fail to update data size", KR(ret), K(fd_), K(write_size));
    } else if (OB_FAIL(page_idx_cache_.push(new_page_id))) {
      LOG_WARN("fail to push back page idx array", KR(ret), K(fd_), K(new_page_id));
    } else {
      cached_page_nums_ = 1;
      file_size_ += write_size;
      begin_page_id_ = new_page_id;
      begin_page_virtual_id_ = tail_page_virtual_id;
      end_page_id_ = new_page_id;
      has_update_file_meta = true;
    }

    if (FAILEDx(insert_or_update_data_flush_node_())) {
      LOG_WARN("fail to insert or update flush data list", KR(ret), K(fd_), KPC(this));
    } else if (OB_FAIL(insert_or_update_meta_flush_node_())) {
      LOG_WARN("fail to insert or update flush meta list", KR(ret), K(fd_), KPC(this));
    }

    if (OB_FAIL(ret) && has_update_file_meta) {
      cached_page_nums_ = 0;
      file_size_ -= write_size;
      begin_page_id_ = ObTmpFileGlobal::INVALID_PAGE_ID;
      begin_page_virtual_id_ = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
      end_page_id_ = ObTmpFileGlobal::INVALID_PAGE_ID;
      page_idx_cache_.reset();
    }
    LOG_DEBUG("load_disk_tail_page_and_rewrite_ end", KR(ret), K(fd_), K(end_page_id_), KPC(this));
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (block_meta_tree_flushing) {
      if (OB_TMP_FAIL(meta_tree_.finish_write_tail(data_item, false /*release_tail_in_disk*/))) {
        LOG_WARN("fail to modify items after tail load", KR(tmp_ret), K(fd_));
      }
    }

    if (new_page_id != ObTmpFileGlobal::INVALID_PAGE_ID) {
      uint32_t unused_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
      if (OB_TMP_FAIL(wbp_->free_page(fd_, new_page_id, ObTmpFilePageUniqKey(tail_page_virtual_id), unused_page_id))) {
        LOG_WARN("fail to free page", KR(tmp_ret), K(fd_), K(new_page_id));
      }
    }
  }
  return ret;
}

// attention:
// last_page_lock_ could promise that there are not truncating task,
// evicting task or flush generator task for the last page.
// however, it might happen that append write for the last page is processing after
// flush generator task has been processed over.
// thus, this function should consider the concurrence problem between append write and callback of flush task.
int ObSharedNothingTmpFile::append_write_memory_tail_page_(ObTmpFileIOCtx &io_ctx)
{
  int ret = OB_SUCCESS;
  char *page_buff = nullptr;
  const int64_t has_written_size = get_offset_in_page_(file_size_);
  const int64_t need_write_size = MIN(ObTmpFileGlobal::PAGE_SIZE - has_written_size,
                                      io_ctx.get_todo_size());
  const int64_t end_page_virtual_id = get_end_page_virtual_id_();
  char *write_buff = io_ctx.get_todo_buffer();
  uint32_t unused_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  ObSharedNothingTmpFileDataItem rightest_data_item;

  if (OB_UNLIKELY(has_written_size + need_write_size > ObTmpFileGlobal::PAGE_SIZE)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("need write size is invalid", KR(ret), K(fd_), K(has_written_size), K(need_write_size));
  } else if (OB_ISNULL(write_buff)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("write buffer is null", KR(ret), K(fd_), K(write_buff));
  } else if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == end_page_virtual_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("end page virtual id is invalid", KR(ret), K(fd_), K(end_page_virtual_id), K(file_size_));
  } else if (OB_FAIL(wbp_->read_page(fd_, end_page_id_, ObTmpFilePageUniqKey(end_page_virtual_id),
                                     page_buff, unused_page_id))) {
    LOG_WARN("fail to fetch page", KR(ret), K(fd_), K(end_page_id_), K(end_page_virtual_id));
  } else if (OB_ISNULL(page_buff)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("page buff is null", KR(ret), K(fd_), K(end_page_id_));
  } else if ((wbp_->is_write_back(fd_, end_page_id_, ObTmpFilePageUniqKey(end_page_virtual_id)) ||
              wbp_->is_cached(fd_, end_page_id_, ObTmpFilePageUniqKey(end_page_virtual_id))) &&
             OB_FAIL(meta_tree_.prepare_for_write_tail(rightest_data_item))) {
    LOG_WARN("fail to prepare for write tail", KR(ret), K(fd_),
             K(end_page_id_), K(end_page_virtual_id), K(rightest_data_item));
  } else if (OB_UNLIKELY(!io_ctx.check_buf_range_valid(write_buff, need_write_size))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid buf range", KR(ret), K(fd_), K(write_buff), K(need_write_size), K(io_ctx));
  } else {
    MEMCPY(page_buff + has_written_size, write_buff, need_write_size);
  }

  // update meta data
  if (OB_SUCC(ret)) {
    common::TCRWLock::WLockGuard guard(meta_lock_);
    const bool is_cached = wbp_->is_cached(fd_, end_page_id_, ObTmpFilePageUniqKey(end_page_virtual_id));
    const bool is_write_back = wbp_->is_write_back(fd_, end_page_id_, ObTmpFilePageUniqKey(end_page_virtual_id));
    if (OB_UNLIKELY(is_deleting_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("file is deleting", KR(ret), K(fd_));
    } else if (is_cached || is_write_back) {
      // due to the appending writing for the last page which is flushing or flushed into disk,
      // the page carbon in memory and disk will be different.
      // thus, we need to rollback the flush status of the last page,
      if (OB_FAIL(wbp_->notify_dirty(fd_, end_page_id_, ObTmpFilePageUniqKey(end_page_virtual_id)))) {
        LOG_WARN("fail to notify dirty", KR(ret), K(fd_), K(end_page_id_), K(end_page_virtual_id));
      } else if (is_cached) {
        // for the last page, if the status of flushed_page_id_ page is not cached,
        // we will treat this page as a non-flushed page
        flushed_data_page_num_--;
        if (0 == flushed_data_page_num_) {
          LOG_INFO("flushed_page_id_ has been written", KPC(this));
          flushed_page_id_ = ObTmpFileGlobal::INVALID_PAGE_ID;
          flushed_page_virtual_id_ = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
        }
      } else if (is_write_back) {
        write_back_data_page_num_--;
      }

      // modify meta tree
      if (OB_FAIL(ret)) {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(meta_tree_.finish_write_tail(rightest_data_item, false /*release_tail_in_disk*/))) {
          LOG_WARN("fail to modify finish write tail page", KR(tmp_ret), K(fd_));
        }
      } else {
        // add file into flush list if file is not flushing,
        // because we produce 1 dirty data page and 1 dirty meta page after writing tail page
        if (OB_FAIL(meta_tree_.finish_write_tail(rightest_data_item, true /*release_tail_in_disk*/))) {
          LOG_WARN("fail to finish write tail page", KR(ret), K(fd_));
        } else if (OB_FAIL(insert_or_update_data_flush_node_())) {
          LOG_WARN("fail to insert or update flush data list", KR(ret), K(fd_), KPC(this));
        } else if (OB_FAIL(insert_or_update_meta_flush_node_())) {
          LOG_WARN("fail to insert or update flush meta list", KR(ret), K(fd_), KPC(this));
        }
      }
    }

    if (FAILEDx(io_ctx.update_data_size(need_write_size))) {
      LOG_WARN("fail to update data size", KR(ret), K(fd_), K(need_write_size));
    } else {
      file_size_ += need_write_size;
    }

    LOG_DEBUG("append_write_memory_tail_page end", KR(ret), K(fd_), K(end_page_id_), KPC(this));
  }

  return ret;
}

// Attention!!!!
// in order to prevent concurrency problems of eviction list
// from the operation from eviction manager and flush manager,
// before eviction manager calls this function,
// it removes the file's node from its' eviction list,
// but still keeps the file's `is_in_data_eviction_list_` be true.
// when this function run over,
// if `remain_flushed_page_num` > 0, this function will reinsert node into the list of eviction manager again;
// otherwise, this function will set `is_in_data_eviction_list_` be false.
int ObSharedNothingTmpFile::evict_data_pages(const int64_t expected_evict_page_num,
                                             int64_t &actual_evict_page_num,
                                             int64_t &remain_flushed_page_num)
{
  int ret = OB_SUCCESS;
  bool lock_last_page = false;
  common::TCRWLock::WLockGuard meta_lock_guard(meta_lock_);
  const int64_t end_page_virtual_id = get_end_page_virtual_id_();
  actual_evict_page_num = 0;
  remain_flushed_page_num = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSharedNothingTmpFile has not been inited", KR(ret), K(tenant_id_), K(fd_), KPC(this));
  } else if (OB_UNLIKELY(is_deleting_)) {
    // actual_evict_page_num = 0;
    // remain_flushed_page_num = 0;
    is_in_data_eviction_list_ = false;
    LOG_INFO("try to evict data pages when file is deleting", K(fd_), K(tenant_id_), KPC(this));
  } else if (OB_UNLIKELY(expected_evict_page_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd_), K(expected_evict_page_num));
  } else if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == end_page_virtual_id ||
                         0 == flushed_data_page_num_)) {
    is_in_data_eviction_list_ = false;
    LOG_INFO("tmp file has no flushed data pages need to be evicted", KR(ret), K(fd_), K(flushed_data_page_num_), KPC(this));
  } else if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_PAGE_ID == flushed_page_id_ ||
                         ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == flushed_page_virtual_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid flush status", KR(ret), K(fd_), K(flushed_page_id_), K(flushed_page_virtual_id_), KPC(this));
  } else if (OB_UNLIKELY(!is_in_data_eviction_list_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN( "the file is not in data eviction list", K(fd_), K(is_in_data_eviction_list_), KPC(this));
  } else if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == begin_page_virtual_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid begin page virtual id", KR(ret), K(fd_), K(begin_page_virtual_id_), KPC(this));
  } else {
    bool need_to_evict_last_page = false;
    if (flushed_data_page_num_ == cached_page_nums_ && expected_evict_page_num >= flushed_data_page_num_ &&
        wbp_->is_cached(fd_, end_page_id_, ObTmpFilePageUniqKey(end_page_virtual_id))) {
      need_to_evict_last_page = true;
      // last page should be evicted, try to lock the last page.
      if (OB_FAIL(last_page_lock_.trylock())) {
        // fail to get the lock of last data page, it will not be evicted.
        ret = OB_SUCCESS;
      } else {
        lock_last_page = true;
      }
    }

    int64_t remain_evict_page_num = 0;
    if (lock_last_page) {
      remain_evict_page_num = flushed_data_page_num_;
    } else if (need_to_evict_last_page) { // need_to_evict_last_page && !lock_last_page
      remain_evict_page_num = flushed_data_page_num_ - 1;
    } else {
      remain_evict_page_num = MIN(flushed_data_page_num_, expected_evict_page_num);
    }

    const int64_t evict_end_virtual_id = begin_page_virtual_id_ + remain_evict_page_num;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(page_idx_cache_.truncate(evict_end_virtual_id))) {
      LOG_WARN("fail to truncate page idx cache", KR(ret), K(fd_), K(evict_end_virtual_id), KPC(this));
    }

    // evict data pages
    const bool evict_last_page = lock_last_page;
    while (OB_SUCC(ret) && remain_evict_page_num > 0) {
      uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
      uint32_t first_cached_page_idx = ObTmpFileGlobal::INVALID_PAGE_ID;

      if (OB_UNLIKELY(!wbp_->is_cached(fd_, begin_page_id_, ObTmpFilePageUniqKey(begin_page_virtual_id_)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the page is not cached", KR(ret), K(fd_), K(begin_page_id_), K(begin_page_virtual_id_), KPC(this));
      } else if (OB_FAIL(wbp_->free_page(fd_, begin_page_id_, ObTmpFilePageUniqKey(begin_page_virtual_id_), next_page_id))) {
        LOG_WARN("fail to free page", KR(ret), K(fd_), K(begin_page_id_), K(begin_page_virtual_id_), K(next_page_id), KPC(this));
      } else {
        if (begin_page_id_ == flushed_page_id_) {
          flushed_page_id_ = ObTmpFileGlobal::INVALID_PAGE_ID;
          flushed_page_virtual_id_ = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
        }
        begin_page_id_ = next_page_id;
        if (ObTmpFileGlobal::INVALID_PAGE_ID == begin_page_id_) {
          end_page_id_ = ObTmpFileGlobal::INVALID_PAGE_ID;
          begin_page_virtual_id_ = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
        } else {
          begin_page_virtual_id_ += 1;
        }
        actual_evict_page_num++;
        remain_evict_page_num--;
        flushed_data_page_num_--;
        cached_page_nums_--;
      }
    } // end while

    if (OB_FAIL(ret)) {
    } else if (flushed_data_page_num_ > 0) {
      if (OB_FAIL(eviction_mgr_->add_file(false/*is_meta*/, *this))) {
        LOG_WARN("fail to add file to eviction mgr", KR(ret), KPC(this));
      }
    } else {
      is_in_data_eviction_list_ = false;
      LOG_DEBUG("all data pages of file have been evicted", KPC(this));
    }

    if (OB_SUCC(ret)) {
      remain_flushed_page_num = flushed_data_page_num_;
    }

    if (lock_last_page) {
      last_page_lock_.unlock();
    }

    LOG_DEBUG("evict data page of tmp file over", KR(ret), K(fd_), K(is_deleting_),
              K(expected_evict_page_num), K(actual_evict_page_num),
              K(remain_flushed_page_num),
              K(begin_page_id_), K(begin_page_virtual_id_),
              K(flushed_page_id_), K(flushed_page_virtual_id_), KPC(this));
  }

  return ret;
}

// Attention!!!!
// in order to prevent concurrency problems of eviction list
// from the operation from eviction manager and flush manager,
// before eviction manager calls this function,
// it removes the file's node from its' eviction list,
// but still keeps the file's `is_in_meta_eviction_list_` be true.
// when this function run over,
// if `remain_flushed_page_num` > 0, this function will reinsert node into the list of eviction manager again;
// otherwise, this function will set `is_in_meta_eviction_list_` be false.
int ObSharedNothingTmpFile::evict_meta_pages(const int64_t expected_evict_page_num,
                                             int64_t &actual_evict_page_num)
{
  int ret = OB_SUCCESS;
  // TODO: wanyue.wy, wuyuefei.wyf
  // if meta_tree_ could protect the consistency by itself, this lock could be removed
  common::TCRWLock::WLockGuard meta_lock_guard(meta_lock_);
  actual_evict_page_num = 0;
  int64_t total_need_evict_page_num = 0;
  int64_t total_need_evict_rightmost_page_num = 0;
  int64_t remain_need_evict_page_num = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSharedNothingTmpFile has not been inited", KR(ret), K(tenant_id_), K(fd_), KPC(this));
  } else if (OB_UNLIKELY(is_deleting_)) {
    // actual_evict_page_num = 0;
    is_in_meta_eviction_list_ = false;
    LOG_INFO("try to evict data pages when file is deleting", K(fd_), K(tenant_id_), KPC(this));
  } else if (OB_UNLIKELY(expected_evict_page_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd_), K(expected_evict_page_num));
  } else if (OB_UNLIKELY(!is_in_meta_eviction_list_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN( "the file is not in meta eviction list", K(fd_), K(is_in_meta_eviction_list_), KPC(this));
  } else if (OB_FAIL(meta_tree_.get_need_evict_page_num(total_need_evict_page_num, total_need_evict_rightmost_page_num))) {
    LOG_WARN( "fail to get need evict page num", KR(ret), K(fd_), KPC(this));
  } else if (OB_UNLIKELY(total_need_evict_page_num <= 0)) {
    is_in_meta_eviction_list_ = false;
    LOG_INFO("meta tree has no flushed pages need to be evicted", K(fd_), K(total_need_evict_page_num), KPC(this));
  } else if (OB_FAIL(meta_tree_.evict_meta_pages(expected_evict_page_num,
                          ObTmpFileTreeEvictType::FULL, actual_evict_page_num))) {
    LOG_WARN("fail to evict meta pages", KR(ret), K(fd_), K(expected_evict_page_num), KPC(this));
  } else {
    remain_need_evict_page_num = total_need_evict_page_num - actual_evict_page_num;
    if (remain_need_evict_page_num > 0) {
      if (OB_FAIL(eviction_mgr_->add_file(true/*is_meta*/, *this))) {
        LOG_WARN("fail to add file to eviction mgr", KR(ret), K(fd_), KPC(this));
      }
    } else {
      is_in_meta_eviction_list_ = false;
      LOG_DEBUG("all meta pages are evicted", KPC(this));
    }
    LOG_DEBUG("evict meta page of tmp file over", KR(ret), K(fd_), K(is_deleting_),
              K(expected_evict_page_num), K(actual_evict_page_num), K(remain_need_evict_page_num), KPC(this));
  }

  return ret;
}

int ObSharedNothingTmpFile::truncate(const int64_t truncate_offset)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard truncate_lock_guard(truncate_lock_);

  if (OB_FAIL(ObITmpFile::truncate(truncate_offset))) {
    LOG_WARN("fail to truncate tmp file", KR(ret), K(truncate_offset), KPC(this));
  }

  return ret;
}

int ObSharedNothingTmpFile::truncate_persistent_pages_(const int64_t truncate_offset)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(meta_tree_.truncate(truncated_offset_, truncate_offset))) {
    LOG_WARN("fail to truncate meta tree", KR(ret), K(fd_), K(truncated_offset_), K(truncate_offset), KPC(this));
  }

  return ret;
}

int ObSharedNothingTmpFile::truncate_the_first_wbp_page_()
{
  int ret = OB_SUCCESS;
  bool is_flushed_page = false;
  bool is_write_back_page = false;
  bool is_dirty_page = false;
  uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;

  if (ObTmpFileGlobal::INVALID_PAGE_ID == begin_page_id_ ||
      ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == begin_page_virtual_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("begin_page_id_ is already INVALID", KR(ret), K(fd_), K(begin_page_id_),
             K(begin_page_virtual_id_));
  } else if (wbp_->is_cached(fd_, begin_page_id_, ObTmpFilePageUniqKey(begin_page_virtual_id_))) {
    // [begin_page_id_, flushed_page_id_] has been flushed
    is_flushed_page = true;
  } else if (wbp_->is_write_back(fd_, begin_page_id_, ObTmpFilePageUniqKey(begin_page_virtual_id_))) {
    is_write_back_page = true;
  } else if (wbp_->is_dirty(fd_, begin_page_id_, ObTmpFilePageUniqKey(begin_page_virtual_id_))) {
    is_dirty_page = true;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("begin page state is unexpected", KR(ret), KPC(this));
    wbp_->print_page_entry(begin_page_id_);
  }

  if (FAILEDx(wbp_->free_page(fd_, begin_page_id_, ObTmpFilePageUniqKey(begin_page_virtual_id_), next_page_id))) {
    LOG_WARN("fail to free page", KR(ret), K(fd_), K(begin_page_id_), K(begin_page_virtual_id_));
  } else {
    if (is_flushed_page) {
      if (flushed_data_page_num_ <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("flushed_data_page_num_ is unexpected", KR(ret), KPC(this));
      } else if (1 == flushed_data_page_num_) {
        if (flushed_page_id_ != begin_page_id_ && wbp_->is_cached(fd_, flushed_page_id_,
                                                        ObTmpFilePageUniqKey(flushed_page_virtual_id_))) {
          // the page of flushed_page_id_ might not be flushed if it was the tail page of file and was appended.
          // thus, only if the page is flushed, we can check the equality of flushed_page_id_ and begin_page_id_
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("flushed_page_id_ or flushed_data_page_num_ is unexpected", KR(ret), KPC(this));
        }
      }
      if (OB_SUCC(ret)) {
        flushed_data_page_num_--;
      }
    } else if (is_write_back_page) {
      write_back_data_page_num_--;
    } else if (is_dirty_page) {
      if (flushed_page_id_ == begin_page_id_ && flushed_data_page_num_ != 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("flushed_page_id_ or flushed_data_page_num_ is unexpected", KR(ret), KPC(this));
      }
    }

    if (OB_SUCC(ret)) {
      if (flushed_page_id_ == begin_page_id_) {
        flushed_page_id_ = ObTmpFileGlobal::INVALID_PAGE_ID;
        flushed_page_virtual_id_ = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
        LOG_INFO("all flushed page has been truncated", KPC(this));
      }
    }

    if (OB_SUCC(ret)) {
      cached_page_nums_--;
      begin_page_id_ = next_page_id;
      if (ObTmpFileGlobal::INVALID_PAGE_ID == begin_page_id_) {
        end_page_id_ = ObTmpFileGlobal::INVALID_PAGE_ID;
        begin_page_virtual_id_ = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
      } else {
        begin_page_virtual_id_ += 1;
      }
    }
  }
  return ret;
}

void ObSharedNothingTmpFile::get_dirty_meta_page_num_with_lock(int64_t &non_rightmost_dirty_page_num, int64_t &rightmost_dirty_page_num)
{
  common::TCRWLock::RLockGuard guard(meta_lock_);
  get_dirty_meta_page_num(non_rightmost_dirty_page_num, rightmost_dirty_page_num);
}

void ObSharedNothingTmpFile::get_dirty_meta_page_num(int64_t &non_rightmost_dirty_page_num, int64_t &rightmost_dirty_page_num) const
{
  int ret = OB_SUCCESS;
  int64_t total_need_flush_page_num = 0;
  if (OB_FAIL(meta_tree_.get_need_flush_page_num(total_need_flush_page_num, rightmost_dirty_page_num))) {
    non_rightmost_dirty_page_num = 1;
    rightmost_dirty_page_num = 1;
    LOG_WARN("fail to get need flush page num", KR(ret), KPC(this));
  } else {
    non_rightmost_dirty_page_num = total_need_flush_page_num - rightmost_dirty_page_num;
  }
}

int64_t ObSharedNothingTmpFile::get_dirty_data_page_size_() const
{
  int ret = OB_SUCCESS;

  int64_t dirty_size = 0;
  // cached_page_nums == flushed_data_page_num + dirty_data_page_num
  if (0 == cached_page_nums_ || flushed_data_page_num_ + write_back_data_page_num_ == cached_page_nums_) {
    dirty_size = 0;
  } else if (OB_UNLIKELY(cached_page_nums_ < flushed_data_page_num_ + write_back_data_page_num_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("page num is invalid", KR(ret), K(cached_page_nums_), K(flushed_data_page_num_),
              K(write_back_data_page_num_), KPC(this));
    dirty_size = 0;
  } else if (0 == file_size_ % ObTmpFileGlobal::PAGE_SIZE) {
    dirty_size =
      (cached_page_nums_ - flushed_data_page_num_ - write_back_data_page_num_) * ObTmpFileGlobal::PAGE_SIZE;
  } else {
    dirty_size =
      (cached_page_nums_ - flushed_data_page_num_ - write_back_data_page_num_ - 1) * ObTmpFileGlobal::PAGE_SIZE
      + file_size_ % ObTmpFileGlobal::PAGE_SIZE;
  }

  if (dirty_size < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("dirty_size is unexpected", KR(ret), K(dirty_size), KPC(this));
    dirty_size = 0;
  }
  return dirty_size;
}

int ObSharedNothingTmpFile::copy_info_for_virtual_table(ObTmpFileInfo &tmp_file_info)
{
  int ret = OB_SUCCESS;
  const int64_t abs_timeout_us = common::ObTimeUtility::current_time() + 100 * 1000L;
  common::TCRWLock::RLockGuardWithTimeout lock_guard(meta_lock_, abs_timeout_us, ret);
  ObSNTmpFileInfo &sn_tmp_file_info = static_cast<ObSNTmpFileInfo&>(tmp_file_info);

  if (OB_FAIL(ret)) {
    LOG_WARN("fail to get lock for reading in virtual table", KR(ret), KPC(this));
  } else if (OB_FAIL(sn_tmp_file_info.init(trace_id_, tenant_id_, dir_id_, fd_,
                                           file_size_, truncated_offset_, is_deleting_,
                                           cached_page_nums_, write_back_data_page_num_,
                                           flushed_data_page_num_, ref_cnt_,
                                           birth_ts_, this, label_.ptr(),
                                           write_req_cnt_, unaligned_write_req_cnt_,
                                           write_persisted_tail_page_cnt_, lack_page_cnt_, last_modify_ts_,
                                           read_req_cnt_, unaligned_read_req_cnt_,
                                           total_truncated_page_read_cnt_, total_kv_cache_page_read_cnt_,
                                           total_uncached_page_read_cnt_, total_wbp_page_read_cnt_,
                                           truncated_page_read_hits_, kv_cache_page_read_hits_,
                                           uncached_page_read_hits_, wbp_page_read_hits_,
                                           total_read_size_, last_access_ts_))) {
    LOG_WARN("fail to init tmp_file_info", KR(ret), KPC(this));
  } else if (OB_FAIL(meta_tree_.copy_info(sn_tmp_file_info))) {
    LOG_WARN("fail to copy tree info", KR(ret), KPC(this));
  }
  return ret;
};

int ObSharedNothingTmpFile::remove_meta_flush_node()
{
  int ret = OB_SUCCESS;
  common::TCRWLock::WLockGuard guard(meta_lock_);
  if (OB_FAIL(flush_prio_mgr_->remove_file(true, *this))) {
    LOG_WARN("fail to remove flush node", KR(ret), KPC(this));
  }
  return ret;
}

int ObSharedNothingTmpFile::reinsert_meta_flush_node()
{
  int ret = OB_SUCCESS;
  common::TCRWLock::WLockGuard guard(meta_lock_);
  if (OB_FAIL(reinsert_meta_flush_node_())) {
    LOG_WARN("fail to reinsert flush node", KR(ret), KPC(this));
  }

  return ret;
}

int ObSharedNothingTmpFile::reinsert_meta_flush_node_()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(nullptr != meta_flush_node_.get_next())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("flush node should not have next", KR(ret), K(fd_));
  } else if (OB_UNLIKELY(is_deleting_)) {
    // do nothing
  } else {
    int64_t non_rightmost_dirty_page_num = 0;
    int64_t rightmost_dirty_page_num = 0;
    get_dirty_meta_page_num(non_rightmost_dirty_page_num, rightmost_dirty_page_num);
    if (0 == non_rightmost_dirty_page_num && 0 == rightmost_dirty_page_num) {
      // no need to reinsert
      meta_page_flush_level_ = -1;
    } else if (OB_FAIL(flush_prio_mgr_->insert_meta_flush_list(*this, non_rightmost_dirty_page_num,
                                                               rightmost_dirty_page_num))) {
      LOG_WARN("fail to insert meta flush list", KR(ret), K(fd_),
               K(non_rightmost_dirty_page_num), K(rightmost_dirty_page_num));
    }
  }

  return ret;
}

bool ObSharedNothingTmpFile::is_in_meta_flush_list()
{
  common::TCRWLock::RLockGuard guard(meta_lock_);
  return OB_NOT_NULL(meta_flush_node_.get_next());
}

bool ObSharedNothingTmpFile::is_flushing_()
{
  return inner_flush_ctx_.is_flushing();
}

int64_t ObSharedNothingTmpFile::get_cached_page_num()
{
  common::TCRWLock::RLockGuard guard(meta_lock_);
  return cached_page_nums_;
}

// ATTENTION! call this need to be protected it in meta_lock_
int ObSharedNothingTmpFile::insert_or_update_meta_flush_node_()
{
  int ret = OB_SUCCESS;
  if (!is_flushing_()) {
    int64_t non_rightmost_dirty_page_num = 0;
    int64_t rightmost_dirty_page_num = 0;
    get_dirty_meta_page_num(non_rightmost_dirty_page_num, rightmost_dirty_page_num);
    if (meta_page_flush_level_ >= 0 && OB_ISNULL(meta_flush_node_.get_next())) {
      // flush task mgr is processing this file.
      // it will add this file to according flush list in the end
    } else if (0 == non_rightmost_dirty_page_num + rightmost_dirty_page_num) {
      // do nothing
    } else if (meta_page_flush_level_ < 0) {
      if (OB_FAIL(flush_prio_mgr_->insert_meta_flush_list(*this, non_rightmost_dirty_page_num,
                                                          rightmost_dirty_page_num))) {
        LOG_WARN("fail to get list idx", KR(ret),
            K(non_rightmost_dirty_page_num), K(rightmost_dirty_page_num), KPC(this));
      }
    } else if (OB_FAIL(flush_prio_mgr_->update_meta_flush_list(*this, non_rightmost_dirty_page_num,
                                                               rightmost_dirty_page_num))) {
      LOG_WARN("fail to update flush list", KR(ret),
          K(non_rightmost_dirty_page_num), K(rightmost_dirty_page_num), KPC(this));
    }
  }
  return ret;
}

int ObSharedNothingTmpFile::cal_end_position_(
    ObArray<InnerFlushInfo> &flush_infos,
    const int64_t start_pos,
    int64_t &end_pos,
    int64_t &flushed_data_page_num)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(start_pos >= flush_infos.count() || start_pos < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid flushed info num",
        KR(ret), K(start_pos), K(flush_infos.count()), K(inner_flush_ctx_), KPC(this));
  } else if (OB_UNLIKELY(flush_infos[start_pos].has_data() && flush_infos[start_pos].has_meta())) {
    // we require that each flush info only represents one type of data or meta
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid flush info", KR(ret), K(start_pos), K(flush_infos[start_pos]), KPC(this));
  } else if (flush_infos[start_pos].update_meta_data_done_) {
    for (end_pos = start_pos; OB_SUCC(ret) && end_pos < flush_infos.count(); end_pos++) {
      if (OB_UNLIKELY(flush_infos[end_pos].has_data() && flush_infos[end_pos].has_meta()) ||
          OB_UNLIKELY(!flush_infos[end_pos].has_data() && !flush_infos[end_pos].has_meta())) {
        ret = OB_ERR_UNEXPECTED; // flush_info contains one and only one type of pages
        LOG_WARN("invalid flush info", KR(ret), K(flush_infos[end_pos]), KPC(this));
      } else if (flush_infos[end_pos].update_meta_data_done_) {
        if (flush_infos[end_pos].has_data()) {
          flushed_data_page_num += flush_infos[end_pos].flush_data_page_num_;
        }
      } else {
        break;
      }
    } // end for
  }
  return ret;
}

int ObSharedNothingTmpFile::update_meta_after_flush(const int64_t info_idx, const bool is_meta, bool &reset_ctx)
{
  int ret = OB_SUCCESS;
  reset_ctx = false;
  common::TCRWLock::WLockGuard guard(meta_lock_);
  int64_t start_pos = is_meta ? inner_flush_ctx_.meta_finished_continuous_flush_info_num_
                              : inner_flush_ctx_.data_finished_continuous_flush_info_num_;
  int64_t end_pos = start_pos;
  ObArray<InnerFlushInfo> &flush_infos_ = is_meta ? inner_flush_ctx_.meta_flush_infos_
                                                  : inner_flush_ctx_.data_flush_infos_;
  int64_t flushed_data_page_num = 0;
  if (OB_UNLIKELY(info_idx < 0 || info_idx >= flush_infos_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid idx", KR(ret), K(info_idx), K(is_meta), KPC(this));
  } else if (FALSE_IT(flush_infos_[info_idx].update_meta_data_done_ = true)) {
  } else if (OB_FAIL(cal_end_position_(flush_infos_, start_pos, end_pos, flushed_data_page_num))) {
    LOG_WARN("fail to cal end position for update after flush", KR(ret), K(info_idx), KPC(this));
  } else if (start_pos < end_pos) { // have new continuous finished flush infos
    if (is_meta && OB_FAIL(update_meta_tree_after_flush_(start_pos, end_pos))) {
      LOG_WARN("fail to update meta tree", KR(ret), K(start_pos), K(end_pos), KPC(this));
    } else if (!is_meta && OB_FAIL(update_file_meta_after_flush_(start_pos, end_pos, flushed_data_page_num))) {
      LOG_WARN("fail to update file meta", KR(ret), K(start_pos), K(end_pos), K(flushed_data_page_num), KPC(this));
    } else {
      // When the flush info segment between [start_pos, end_pos] is updated, the corresponding pages can be immediately freed.
      // However, the flush context in flush_mgr may still contain stale flush positions.
      // Setting the reset_ctx flag clears the context in the flush manager, enabling the file to regenerate the context
      // using its new metadata during the next flush, thereby preventing access the illegal pages.
      reset_ctx = true;
      if (is_meta) {
        inner_flush_ctx_.need_to_wait_for_the_previous_meta_flush_req_to_complete_ = true;
      } else {
        inner_flush_ctx_.need_to_wait_for_the_previous_data_flush_req_to_complete_ = true;
      }
    }
    LOG_DEBUG("update meta based a set of flush info over", KR(ret), K(info_idx), K(start_pos),
              K(end_pos), K(inner_flush_ctx_), KPC(this));
  }

  if (FAILEDx(inner_flush_ctx_.update_finished_continuous_flush_info_num(is_meta, end_pos))) {
    LOG_WARN("fail to update finished continuous flush info num", KR(ret), K(start_pos), K(end_pos), KPC(this));
  } else {
    int tmp_ret = OB_SUCCESS;
    if (inner_flush_ctx_.is_data_finished()) {
      if (OB_ISNULL(data_flush_node_.get_next()) && OB_TMP_FAIL(reinsert_data_flush_node_())) {
        LOG_ERROR("fail to reinsert data flush node", KR(tmp_ret), K(is_meta), K(inner_flush_ctx_), KPC(this));
      }
      inner_flush_ctx_.need_to_wait_for_the_previous_data_flush_req_to_complete_ = false;
      LOG_DEBUG("flush data has completed, reinsert into flush list", KPC(this));
    }
    if (inner_flush_ctx_.is_meta_finished()) {
      if (OB_ISNULL(meta_flush_node_.get_next()) && OB_TMP_FAIL(reinsert_meta_flush_node_())) {
        LOG_ERROR("fail to reinsert meta flush node", KR(tmp_ret), K(is_meta), K(inner_flush_ctx_), KPC(this));
      }
      inner_flush_ctx_.need_to_wait_for_the_previous_meta_flush_req_to_complete_ = false;
    }

    if (inner_flush_ctx_.is_all_finished()) {
      inner_flush_ctx_.reset();
    }
  }

  LOG_DEBUG("update_meta_after_flush finish", KR(ret), K(info_idx), K(is_meta), K(inner_flush_ctx_), KPC(this));
  return ret;
}

// 1. skip truncated flush infos
// 2. abort updating file meta for tail data page if it is appending written after generating flushing task
int ObSharedNothingTmpFile::remove_useless_page_in_data_flush_infos_(const int64_t start_pos,
                                                                     const int64_t end_pos,
                                                                     const int64_t flushed_data_page_num,
                                                                     int64_t &new_start_pos,
                                                                     int64_t &new_flushed_data_page_num)
{
  int ret = OB_SUCCESS;
  ObArray<InnerFlushInfo> &flush_infos_ = inner_flush_ctx_.data_flush_infos_;
  new_start_pos = start_pos;
  new_flushed_data_page_num = flushed_data_page_num;

  if (OB_UNLIKELY(start_pos >= end_pos || end_pos < 1 || flushed_data_page_num <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid param", KR(ret), K(fd_), K(start_pos), K(end_pos), K(flushed_data_page_num));
  } else if (start_pos >= flush_infos_.count() || end_pos > flush_infos_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid param", KR(ret), K(fd_), K(start_pos), K(end_pos), K(flush_infos_), KPC(this));
  } else {
    // skip truncated flush infos
    for (int64_t i = start_pos; OB_SUCC(ret) && i < end_pos; i++) {
      int64_t flushed_start_offset = get_page_begin_offset_by_virtual_id_(flush_infos_[i].flush_virtual_page_id_);
      int64_t flushed_end_offset = flushed_start_offset +
                                   flush_infos_[i].flush_data_page_num_ * ObTmpFileGlobal::PAGE_SIZE;
      if (truncated_offset_ <= flushed_start_offset) {
        // the following flushing pages are not affected by truncate_offset
        break;
      } else if (truncated_offset_ < flushed_end_offset) {
        // although a page is truncated partly, it will also be flushed whole page
        int64_t flush_info_flushed_size = flushed_end_offset - truncated_offset_;
        int64_t flush_info_data_page_num =
                common::upper_align(flush_info_flushed_size, ObTmpFileGlobal::PAGE_SIZE) / ObTmpFileGlobal::PAGE_SIZE;
        uint32_t flush_info_virtual_page_id =
                common::lower_align(truncated_offset_, ObTmpFileGlobal::PAGE_SIZE) / ObTmpFileGlobal::PAGE_SIZE;
        new_flushed_data_page_num -= (flush_infos_[i].flush_data_page_num_ - flush_info_data_page_num);

        LOG_INFO("due to truncating, modify flush info", KR(ret), K(fd_),
                 K(start_pos), K(end_pos), K(flushed_data_page_num), K(i),
                 K(new_start_pos), K(new_flushed_data_page_num),
                 K(truncated_offset_), K(flushed_start_offset), K(flushed_end_offset), K(flush_infos_[i]),
                 K(flush_info_data_page_num), K(flush_info_virtual_page_id));
        flush_infos_[i].flush_data_page_num_ = flush_info_data_page_num;
        flush_infos_[i].flush_virtual_page_id_ = flush_info_virtual_page_id;
        break;
      } else {
        // all pages of this flush_info have been truncated, abort it
        new_start_pos++;
        new_flushed_data_page_num -= flush_infos_[i].flush_data_page_num_;
        LOG_INFO("due to truncating, abort flush info", KR(ret), K(fd_),
                 K(start_pos), K(end_pos), K(flushed_data_page_num), K(i),
                 K(new_start_pos), K(new_flushed_data_page_num),
                 K(truncated_offset_), K(flushed_start_offset), K(flushed_end_offset), K(flush_infos_[i]));
      }
    } // end for

    // abort updating file meta for tail data page
    if (OB_SUCC(ret) && new_flushed_data_page_num > 0 && new_start_pos < end_pos) {
      if (flush_infos_[end_pos - 1].file_size_ > 0 && flush_infos_[end_pos - 1].file_size_ != file_size_) {
        // the last page has been written after flush task has been generated.
        // we will discard the flushed last page when update meta data
        // (append write has correct the meta data of the last page, here only need to ignore it)

        int64_t discard_page_virtual_id = flush_infos_[end_pos - 1].flush_virtual_page_id_ +
                                          flush_infos_[end_pos - 1].flush_data_page_num_ - 1;
        uint32_t discard_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;

        if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == discard_page_virtual_id)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("discard page virtual id is invalid", KR(ret), K(fd_),
                   K(discard_page_virtual_id), K(flush_infos_[end_pos - 1]));
        } else if (OB_FAIL(get_physical_page_id_in_wbp_(discard_page_virtual_id, discard_page_id))) {
          LOG_WARN("fail to get physical page id in wbp", KR(ret), K(fd_), K(discard_page_virtual_id));
        } else if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_PAGE_ID == discard_page_id)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("discard page id is invalid", KR(ret), K(fd_), K(discard_page_id));
        } else if (OB_UNLIKELY(!wbp_->is_dirty(fd_, discard_page_id, ObTmpFilePageUniqKey(discard_page_virtual_id)))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("discard page is not dirty", KR(ret), K(fd_), K(discard_page_id), K(discard_page_virtual_id));
        } else {
          new_flushed_data_page_num -= 1;
          flush_infos_[end_pos - 1].flush_data_page_num_ -= 1;
        }
      }
    }
  }
  return ret;
}

int ObSharedNothingTmpFile::update_file_meta_after_flush_(const int64_t start_pos,
                                                          const int64_t end_pos,
                                                          const int64_t flushed_data_page_num)
{
  int ret = OB_SUCCESS;
  ObArray<InnerFlushInfo> &flush_infos_ = inner_flush_ctx_.data_flush_infos_;
  LOG_DEBUG("update_file_meta_after_flush start",
      KR(ret), K(start_pos), K(end_pos), K(flushed_data_page_num), KPC(this));
  int64_t new_start_pos = start_pos;
  int64_t new_flushed_data_page_num = flushed_data_page_num;

  if (OB_UNLIKELY(start_pos >= end_pos || end_pos < 1 || flushed_data_page_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param", KR(ret), K(fd_), K(end_pos), K(flushed_data_page_num));
  } else if (start_pos >= flush_infos_.count() || end_pos > flush_infos_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param", KR(ret), K(start_pos), K(end_pos), K(flush_infos_), KPC(this));
  } else if (OB_FAIL(remove_useless_page_in_data_flush_infos_(start_pos, end_pos, flushed_data_page_num,
                                                              new_start_pos, new_flushed_data_page_num))) {
    LOG_WARN("fail to remove useless page in flush infos", KR(ret), K(fd_), K(start_pos), K(end_pos),
             K(flushed_data_page_num), K(end_pos));
  } else if (0 == new_flushed_data_page_num) {
    // do nothing
  } else if (OB_UNLIKELY(new_flushed_data_page_num < 0 || new_start_pos >= end_pos)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param", KR(ret), K(fd_), K(start_pos), K(end_pos), K(flushed_data_page_num),
              K(new_start_pos), K(new_flushed_data_page_num));
  } else { // exist multiple continuous pages have been flushed over
    uint32_t last_flushed_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
    uint32_t cur_flush_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
    int64_t cur_flush_page_virtual_id = flush_infos_[new_start_pos].flush_virtual_page_id_;
    if (OB_FAIL(get_physical_page_id_in_wbp_(flush_infos_[new_start_pos].flush_virtual_page_id_, cur_flush_page_id))) {
      LOG_WARN("fail to get physical page id in wbp",
               KR(ret),K(fd_),  K(start_pos), K(end_pos), K(flushed_data_page_num),
               K(new_start_pos), K(new_flushed_data_page_num),
               K(flush_infos_[new_start_pos]));
    } else {
      int64_t write_back_succ_data_page_num = 0;

      // update each flush info
      for (int64_t i = new_start_pos; OB_SUCC(ret) && i < end_pos; ++i) {
        const InnerFlushInfo& flush_info = flush_infos_[i];
        const int64_t cur_flushed_data_page_num = flush_info.flush_data_page_num_;

        if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_PAGE_ID == cur_flush_page_id)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid next flush page id", KR(ret), K(fd_), K(cur_flush_page_id),
                   K(begin_page_id_), K(flushed_page_id_), K(end_page_id_));
        } else {
          int64_t cur_page_virtual_id_in_flush_info = cur_flush_page_virtual_id;
          // update each page of flush info
          for (int64_t j = 0; OB_SUCC(ret) && j < cur_flushed_data_page_num; ++j) {
            if (OB_FAIL(wbp_->notify_write_back_succ(fd_, cur_flush_page_id, ObTmpFilePageUniqKey(cur_page_virtual_id_in_flush_info)))) {
              LOG_WARN("fail to mark page as clean", KR(ret), K(fd_),
                       K(flushed_page_id_),  K(cur_page_virtual_id_in_flush_info), K(cur_flush_page_id),
                       K(flushed_data_page_num), K(new_flushed_data_page_num));
            } else if (FALSE_IT(last_flushed_page_id = cur_flush_page_id)) {
            } else if (OB_FAIL(wbp_->get_next_page_id(fd_, cur_flush_page_id,
                    ObTmpFilePageUniqKey(cur_page_virtual_id_in_flush_info), cur_flush_page_id))) {
              LOG_WARN("fail to get next page id", KR(ret), K(fd_), K(cur_flush_page_id),
                  K(cur_page_virtual_id_in_flush_info));
            } else {
              cur_page_virtual_id_in_flush_info += 1;
            }
          } // end for
          if (OB_SUCC(ret)) {
            write_back_succ_data_page_num += cur_flushed_data_page_num;
            cur_flush_page_virtual_id += cur_flushed_data_page_num;
          }
        }
      } // end for

      // update file meta
      if (OB_SUCC(ret)) {
        if (write_back_succ_data_page_num != new_flushed_data_page_num) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("write_back_succ_data_page_num not correct",
              KR(ret), K(fd_), K(write_back_succ_data_page_num),
              K(flushed_data_page_num), K(new_flushed_data_page_num), KPC(this));
        } else {
          flushed_page_id_ = last_flushed_page_id;
          flushed_page_virtual_id_ = cur_flush_page_virtual_id - 1;
          flushed_data_page_num_ += new_flushed_data_page_num;
          write_back_data_page_num_ -= new_flushed_data_page_num;
          if (write_back_data_page_num_ < 0) {
            int tmp_ret = OB_ERR_UNEXPECTED;
            LOG_WARN("write_back_data_page_num_ is unexpected", KR(tmp_ret), K(fd_),
                K(write_back_data_page_num_), K(flushed_data_page_num), K(new_flushed_data_page_num), KPC(this));
            write_back_data_page_num_ = 0;
          }

          if (!is_deleting_ && !is_in_data_eviction_list_ && OB_ISNULL(data_eviction_node_.get_next())) {
            if (OB_FAIL(eviction_mgr_->add_file(false/*is_meta*/, *this))) {
              LOG_WARN("fail to insert into eviction list", KR(ret), K(fd_));
            } else {
              is_in_data_eviction_list_ = true;
            }
          }
        }
      } // update file meta over
    }
  } // handle continuous success flush infos over

  LOG_DEBUG("update_file_meta_after_flush finish", KR(ret), K(fd_),
            K(start_pos), K(end_pos), K(flushed_data_page_num),
            K(new_start_pos), K(new_flushed_data_page_num), KPC(this));
  return ret;
}

int ObSharedNothingTmpFile::update_meta_tree_after_flush_(const int64_t start_pos, const int64_t end_pos)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  ObArray<InnerFlushInfo> &flush_infos_ = inner_flush_ctx_.meta_flush_infos_;
  for (int64_t i = start_pos; i < end_pos; i++) {
    // ATTENTION! need to alloc memory inside, caller must retry update meta data if alloc fail
    if (OB_FAIL(meta_tree_.update_after_flush(flush_infos_[i].flush_meta_page_array_))) {
      LOG_ERROR("fail to update meta items", KR(ret), K(fd_), K(flush_infos_[i]), KPC(this));
    } else {
      LOG_INFO("succ to update meta items", KR(ret), K(fd_), K(flush_infos_[i]), KPC(this));
    }
  }

  if (OB_SUCC(ret)) {
    if (!is_deleting_ && !is_in_meta_eviction_list_ && OB_ISNULL(meta_eviction_node_.get_next())) {
      int64_t total_need_evict_page_num = 1;
      int64_t total_need_evict_rightmost_page_num = 1;
      if (OB_TMP_FAIL(meta_tree_.get_need_evict_page_num(total_need_evict_page_num,
                                                         total_need_evict_rightmost_page_num))) {
        LOG_ERROR("fail to get_need_evict_page_num", KR(tmp_ret),
            K(total_need_evict_page_num), K(total_need_evict_rightmost_page_num), KPC(this));
      }

      if (total_need_evict_page_num > 0) {
        if (OB_FAIL(eviction_mgr_->add_file(true/*is_meta*/, *this))) {
          LOG_WARN("fail to insert into eviction list", KR(ret), K(fd_));
        } else {
          is_in_meta_eviction_list_ = true;
        }
      }
    }
  }
  return ret;
}

int ObSharedNothingTmpFile::generate_data_flush_info_(
    ObTmpFileFlushTask &flush_task,
    ObTmpFileFlushInfo &info,
    ObTmpFileDataFlushContext &data_flush_context,
    const int64_t flush_sequence,
    const bool need_flush_tail)
{
  int ret = OB_SUCCESS;

  uint32_t copy_begin_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  int64_t copy_begin_page_virtual_id = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;

  uint32_t copy_end_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  if (OB_FAIL(cal_next_flush_page_id_from_flush_ctx_or_file_(data_flush_context,
                                                             copy_begin_page_id,
                                                             copy_begin_page_virtual_id))) {
    LOG_WARN("fail to calculate next_flush_page_id", KR(ret),
        K(flush_task), K(info), K(data_flush_context), KPC(this));
  } else if (ObTmpFileGlobal::INVALID_PAGE_ID == copy_begin_page_id) {
    ret = OB_ITER_END;
    LOG_DEBUG("no more data to flush", KR(ret), K(fd_), KPC(this));
  } else if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == copy_begin_page_virtual_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("next_flush_page_virtual_id is invalid", KR(ret), K(fd_), K(copy_begin_page_id),
             K(copy_begin_page_virtual_id), K(flush_task), K(info), K(data_flush_context),
             K(flush_sequence), K(need_flush_tail), KPC(this));
  } else if (OB_FAIL(get_flush_end_page_id_(copy_end_page_id, need_flush_tail))) {
    LOG_WARN("fail to get_flush_end_page_id_", KR(ret),
        K(flush_task), K(info), K(data_flush_context), KPC(this));
  } else if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_FLUSH_SEQUENCE != inner_flush_ctx_.flush_seq_
              && flush_sequence != inner_flush_ctx_.flush_seq_
              && flush_sequence != flush_task.get_flush_seq())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("flush sequence not match",
        KR(ret), K(inner_flush_ctx_.flush_seq_), K(flush_task), KPC(this));
  } else if (ObTmpFileFlushTask::TaskType::META == flush_task.get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("flush task type is unexpected", KR(ret), K(flush_task), K(data_flush_context), KPC(this));
  } else if (OB_FAIL(collect_flush_data_page_id_(flush_task, info, data_flush_context,
                                                 copy_begin_page_id, copy_begin_page_virtual_id,
                                                 copy_end_page_id,
                                                 flush_sequence, need_flush_tail))) {
    LOG_WARN("fail to copy flush data from wbp", KR(ret),
        K(flush_task), K(info), K(data_flush_context), KPC(this));
  }

  LOG_DEBUG("generate_data_flush_info_ end",
      KR(ret), K(fd_), K(data_flush_context), K(info), K(flush_task), KPC(this));
  return ret;
}

int ObSharedNothingTmpFile::generate_data_flush_info(
    ObTmpFileFlushTask &flush_task,
    ObTmpFileFlushInfo &info,
    ObTmpFileDataFlushContext &data_flush_context,
    const int64_t flush_sequence,
    const bool need_flush_tail)
{
  int ret = OB_SUCCESS;
  info.reset();

  if (!truncate_lock_.try_rdlock()) {
    ret = OB_ITER_END;
    LOG_WARN("fail to get truncate lock", KR(ret), K(fd_), KPC(this));
  } else {
    common::TCRWLock::RLockGuard guard(meta_lock_);
    if (inner_flush_ctx_.need_to_wait_for_the_previous_data_flush_req_to_complete_) {
      ret = OB_ITER_END;
      LOG_INFO("need_to_wait_for_the_previous_data_flush_req_to_complete_", KR(ret), K(fd_), KPC(this));
    } else if (OB_FAIL(generate_data_flush_info_(flush_task, info,
                                                 data_flush_context, flush_sequence, need_flush_tail))) {
      STORAGE_LOG(WARN, "fail to generate_data_flush_info_", KR(ret), K(flush_task),
                  K(info), K(data_flush_context), K(flush_sequence), K(need_flush_tail), KPC(this));
    }
    if (OB_FAIL(ret)) {
      truncate_lock_.unlock();
    }
  }

  return ret;
}

int ObSharedNothingTmpFile::collect_flush_data_page_id_(
    ObTmpFileFlushTask &flush_task,
    ObTmpFileFlushInfo &info,
    ObTmpFileDataFlushContext &data_flush_context,
    const uint32_t copy_begin_page_id,
    const int64_t copy_begin_page_virtual_id,
    const uint32_t copy_end_page_id,
    const int64_t flush_sequence,
    const bool need_flush_tail)
{
  int ret = OB_SUCCESS;
  char *buf = flush_task.get_data_buf();
  int64_t write_offset = flush_task.get_total_page_num() * ObTmpFileGlobal::PAGE_SIZE;

  bool has_last_page_lock = false;
  int64_t next_disk_page_id = flush_task.get_next_free_page_id();
  int64_t flushing_page_num = 0;
  int64_t origin_info_num = inner_flush_ctx_.data_flush_infos_.size();
  int64_t cur_page_file_offset = -1;
  uint32_t cur_flushed_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  uint32_t cur_page_id = copy_begin_page_id;
  int64_t cur_page_virtual_id = copy_begin_page_virtual_id;
  int64_t collected_page_cnt = 0;

  if (OB_UNLIKELY(OB_STORAGE_OBJECT_MGR.get_macro_object_size() <= write_offset)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid buf or write_offset", KR(ret), KP(buf), K(write_offset), K(flush_task), KPC(this));
  } else if (OB_FAIL(inner_flush_ctx_.data_flush_infos_.push_back(InnerFlushInfo()))) {
    LOG_WARN("fail to push back empty flush info", KR(ret), K(fd_), K(info), K(flush_task), KPC(this));
    ret = OB_ITER_END; // override error code, we will handle this err code in flush mgr
  }
  while (OB_SUCC(ret) && cur_page_id != copy_end_page_id && write_offset < OB_STORAGE_OBJECT_MGR.get_macro_object_size()) {
    if (need_flush_tail && cur_page_id == end_page_id_ && file_size_ % ObTmpFileGlobal::PAGE_SIZE != 0) {
      if (OB_SUCC(last_page_lock_.trylock())) {
        has_last_page_lock = true;
      } else {
        LOG_WARN("fail to get last page lock", KR(ret), K(fd_));
        ret = OB_SUCCESS; // ignore error to continue flushing the copied data
        break;
      }
    }
    char *page_buf = nullptr;
    uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
    bool original_state_is_dirty = wbp_->is_dirty(fd_, cur_page_id, ObTmpFilePageUniqKey(cur_page_virtual_id));
    if (OB_FAIL(wbp_->read_page(fd_, cur_page_id, ObTmpFilePageUniqKey(cur_page_virtual_id), page_buf, next_page_id))) {
      LOG_WARN("fail to read page", KR(ret), K(fd_), K(cur_page_id));
    } else if (OB_FAIL(wbp_->notify_write_back(fd_, cur_page_id, ObTmpFilePageUniqKey(cur_page_virtual_id)))) {
      LOG_WARN("fail to notify write back", KR(ret), K(fd_), K(cur_page_id));
    } else if (OB_FAIL(flush_task.get_flush_page_id_arr().push_back(cur_page_id))) {
      LOG_ERROR("fail to push back flush page id", KR(ret), K(fd_), K(cur_page_id), KPC(this));
      ret = OB_ITER_END; // override error code
    } else {
      // ObTmpPageCacheKey cache_key(flush_task.get_block_index(),
      //                             write_offset / ObTmpFileGlobal::PAGE_SIZE, tenant_id_);
      // ObTmpPageCacheValue cache_value(page_buf);
      // ObTmpPageCache::get_instance().try_put_page_to_cache(cache_key, cache_value);

      write_offset += ObTmpFileGlobal::PAGE_SIZE;
      flushing_page_num += 1;
      cur_flushed_page_id = cur_page_id;
      cur_page_id = next_page_id;
      cur_page_virtual_id += 1;
      collected_page_cnt += 1;
      if (original_state_is_dirty) {
        write_back_data_page_num_++;
      }
    }
  }

  if (OB_SUCC(ret) && 0 == flushing_page_num) {
    ret = OB_ITER_END;
  }
  if (OB_SUCC(ret) && ObTmpFileGlobal::INVALID_PAGE_ID == cur_flushed_page_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("cur_flushed_page_id is unexpected", KR(ret), K(cur_flushed_page_id), KPC(this));
  }

  if (OB_SUCC(ret)) {
    int64_t flush_info_idx = inner_flush_ctx_.data_flush_infos_.size() - 1;
    // set flush_info in flush_task
    info.flush_data_page_disk_begin_id_ = next_disk_page_id;
    info.flush_data_page_num_ = flushing_page_num;
    info.batch_flush_idx_ = flush_info_idx;
    info.flush_virtual_page_id_ = copy_begin_page_virtual_id;
    info.has_last_page_lock_ = has_last_page_lock;
    // record file_size to check if the last page is appended while flushing
    if (has_last_page_lock) {
      info.file_size_ = file_size_;
    }

    info.fd_ = fd_;
    // set flush_info in file inner_flush_ctx
    if (OB_FAIL(info.file_handle_.init(this))) {
      LOG_WARN("fail to init tmp file handle", KR(ret), K(fd_), K(flush_task), KPC(this));
    } else if (OB_FAIL(inner_flush_ctx_.data_flush_infos_.at(flush_info_idx).init_by_tmp_file_flush_info(info))) {
      LOG_WARN("fail to init_by_tmp_file_flush_info", KR(ret), K(fd_), K(flush_task), KPC(this));
    }
  }

  if (OB_SUCC(ret)) {
    // maintain flushed_page_id recorded in flush_mgr
    data_flush_context.set_flushed_page_id(cur_flushed_page_id);
    data_flush_context.set_flushed_page_virtual_id(cur_page_virtual_id - 1);
    data_flush_context.set_next_flush_page_id(cur_page_id);
    data_flush_context.set_next_flush_page_virtual_id(cur_page_virtual_id);
    data_flush_context.set_is_valid(true);
    if (has_last_page_lock) {
      data_flush_context.set_has_flushed_last_partially_written_page(true);
    }
    flush_task.set_data_length(write_offset);
    flush_task.set_buffer_pool_ptr(wbp_);
    flush_task.set_type(ObTmpFileFlushTask::DATA);

    inner_flush_ctx_.flush_seq_ = flush_sequence;
  } else {
    LOG_WARN("fail to generate data flush info", KR(ret), K(fd_), K(need_flush_tail),
        K(flush_sequence), K(data_flush_context), K(info), K(flush_task), KPC(this));
    for (int32_t i = 0; i < collected_page_cnt; ++i) {
      flush_task.get_flush_page_id_arr().pop_back();
    }
    if (inner_flush_ctx_.data_flush_infos_.size() == origin_info_num + 1) {
      inner_flush_ctx_.data_flush_infos_.pop_back();
    }
    if (has_last_page_lock) {
      last_page_lock_.unlock();
    }
  }
  return ret;
}

int ObSharedNothingTmpFile::generate_meta_flush_info_(
    ObTmpFileFlushTask &flush_task,
    ObTmpFileFlushInfo &info,
    ObTmpFileTreeFlushContext &meta_flush_context,
    const int64_t flush_sequence,
    const bool need_flush_tail)
{
  int ret = OB_SUCCESS;

  ObArray<InnerFlushInfo> &flush_infos_ = inner_flush_ctx_.meta_flush_infos_;
  int64_t origin_info_num = flush_infos_.size();

  const int64_t block_index = flush_task.get_block_index();
  char *buf = flush_task.get_data_buf();
  int64_t write_offset = flush_task.get_total_page_num() * ObTmpFileGlobal::PAGE_SIZE;

  ObTmpFileTreeEvictType flush_type = need_flush_tail ?
                                      ObTmpFileTreeEvictType::FULL : ObTmpFileTreeEvictType::MAJOR;
  if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_FLUSH_SEQUENCE != inner_flush_ctx_.flush_seq_
        && flush_sequence != inner_flush_ctx_.flush_seq_
        && flush_sequence != flush_task.get_flush_seq())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("flush sequence not match", KR(ret), K(flush_sequence), K(inner_flush_ctx_.flush_seq_),
             K(flush_task), KPC(this));
  } else if (ObTmpFileFlushTask::TaskType::DATA == flush_task.get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("flush task type is unexpected", KR(ret), K(flush_task), K(meta_flush_context), KPC(this));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(OB_STORAGE_OBJECT_MGR.get_macro_object_size() <= write_offset)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid buf or write_offset", KR(ret), KP(buf), K(write_offset), K(flush_task), KPC(this));
  } else if (OB_FAIL(flush_infos_.push_back(InnerFlushInfo()))) {
    LOG_WARN("fail to push back empty flush info", KR(ret), K(fd_), K(info), K(flush_task), KPC(this));
    ret = OB_ITER_END; // override error code, we will handle this err code in flush mgr
  } else if (OB_FAIL(meta_tree_.flush_meta_pages_for_block(block_index, flush_type, buf, write_offset,
                                                           meta_flush_context, info.flush_meta_page_array_))) {
    LOG_WARN("fail to flush meta pages for block", KR(ret), K(fd_), K(flush_task), K(meta_flush_context), KPC(this));
  } else if (0 == info.flush_meta_page_array_.count()) {
    ret = OB_ITER_END;
  }

  if (OB_SUCC(ret)) {
    int64_t flush_info_idx = flush_infos_.size() - 1;
    info.batch_flush_idx_ = flush_info_idx;

    info.fd_ = fd_;
    // set flush_info in flush_task
    if (OB_FAIL(info.file_handle_.init(this))) {
      LOG_WARN("fail to init tmp file handle", KR(ret), K(fd_), K(flush_task), KPC(this));
    } else if (OB_FAIL(inner_flush_ctx_.meta_flush_infos_.at(flush_info_idx).init_by_tmp_file_flush_info(info))) {
      LOG_WARN("fail to init_by_tmp_file_flush_info", KR(ret), K(fd_), K(flush_task), KPC(this));
    }
  }

  if (OB_SUCC(ret)) {
    flush_task.set_data_length(write_offset);
    flush_task.set_type(ObTmpFileFlushTask::META);
    inner_flush_ctx_.flush_seq_ = flush_sequence;
  } else {
    LOG_WARN("fail to generate meta flush info", KR(ret), K(fd_), K(flush_task),
        K(meta_flush_context), K(flush_sequence), K(need_flush_tail));
    if (flush_infos_.size() == origin_info_num + 1) {
      flush_infos_.pop_back();
    }
  }

  LOG_INFO("generate_meta_flush_info_ end", KR(ret), K(fd_), K(need_flush_tail),
           K(inner_flush_ctx_), K(meta_flush_context), K(info), K(flush_task), KPC(this));
  return ret;
}

int ObSharedNothingTmpFile::generate_meta_flush_info(
    ObTmpFileFlushTask &flush_task,
    ObTmpFileFlushInfo &info,
    ObTmpFileTreeFlushContext &meta_flush_context,
    const int64_t flush_sequence,
    const bool need_flush_tail)
{
  int ret = OB_SUCCESS;
  info.reset();

  common::TCRWLock::RLockGuard guard(meta_lock_);
  if (inner_flush_ctx_.need_to_wait_for_the_previous_meta_flush_req_to_complete_) {
    ret = OB_ITER_END;
    LOG_INFO("need_to_wait_for_the_previous_meta_flush_req_to_complete_", KR(ret), K(fd_), KPC(this));
  } else if (OB_FAIL(generate_meta_flush_info_(flush_task, info,
                                               meta_flush_context, flush_sequence, need_flush_tail))) {
    STORAGE_LOG(WARN, "fail to generate_meta_flush_info_", KR(ret), K(flush_task),
                K(info), K(meta_flush_context), K(flush_sequence), K(need_flush_tail), KPC(this));
  }

  return ret;
}

int ObSharedNothingTmpFile::insert_meta_tree_item(const ObTmpFileFlushInfo &info, int64_t block_index)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  common::TCRWLock::WLockGuard guard(meta_lock_);
  if (info.has_data()) {
    ObSharedNothingTmpFileDataItem data_item;
    data_item.block_index_ = block_index;
    data_item.physical_page_id_ = info.flush_data_page_disk_begin_id_;
    data_item.physical_page_num_ = info.flush_data_page_num_;
    data_item.virtual_page_id_ = info.flush_virtual_page_id_;
    ObSEArray<ObSharedNothingTmpFileDataItem, 1> data_items;

    if (OB_FAIL(data_items.push_back(data_item))) {
      LOG_WARN("fail to push back data item", KR(ret), K(info), K(block_index), KPC(this));
    } else if (OB_FAIL(meta_tree_.prepare_for_insert_items())) {
      LOG_WARN("fail to prepare for insert items", KR(ret), K(info), K(block_index), KPC(this));
    } else if (OB_FAIL(meta_tree_.insert_items(data_items))) {
      LOG_WARN("fail to insert data items", KR(ret), K(info), K(block_index), KPC(this));
    }

    if (OB_SUCC(ret) && info.has_last_page_lock_) {
      last_page_lock_.unlock();
    }

  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("flush info does not contain data info", KR(ret), K(info), KPC(this));
  }

  if (!is_deleting_ && !is_in_meta_eviction_list_ && OB_ISNULL(meta_eviction_node_.get_next())) {
    int64_t total_need_evict_page_num = 1;
    int64_t total_need_evict_rightmost_page_num = 1;
    if (OB_TMP_FAIL(meta_tree_.get_need_evict_page_num(total_need_evict_page_num,
                                                       total_need_evict_rightmost_page_num))) {
      LOG_ERROR("fail to get_need_evict_page_num", KR(tmp_ret),
          K(total_need_evict_page_num), K(total_need_evict_rightmost_page_num), KPC(this));
    }

    if (total_need_evict_page_num > 0) {
      if (OB_TMP_FAIL(eviction_mgr_->add_file(true/*is_meta*/, *this))) {
        LOG_WARN("fail to insert into eviction list", KR(ret), K(fd_), KPC(this));
      } else {
        is_in_meta_eviction_list_ = true;
      }
    }
  }

  // reinsert meta flush node to allow meta pages to be flushed multiple times in one round of flushing
  if (!is_deleting_ && inner_flush_ctx_.is_meta_finished() && OB_ISNULL(meta_flush_node_.get_next())) {
    if (OB_TMP_FAIL(reinsert_meta_flush_node_())) {
      LOG_WARN("fail to reinsert flush node", KR(ret), K(fd_), K(info), K(block_index), KPC(this));
    }
  }

  LOG_DEBUG("insert_meta_tree_item end", KR(ret), KPC(this));
  return ret;
}

int ObSharedNothingTmpFile::copy_finish()
{
  int ret = OB_SUCCESS;
  truncate_lock_.unlock();
  return ret;
}

int ObSharedNothingTmpFile::cal_next_flush_page_id_from_flush_ctx_or_file_(
    const ObTmpFileDataFlushContext &data_flush_context,
    uint32_t &next_flush_page_id,
    int64_t &next_flush_page_virtual_id)
{
  int ret = OB_SUCCESS;
  if (data_flush_context.is_valid()) {
    // use next_flush_page_id from data_flush_ctx
    if (data_flush_context.has_flushed_last_partially_written_page()) {
      next_flush_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
      next_flush_page_virtual_id = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
    } else {
      next_flush_page_id = data_flush_context.get_next_flush_page_id();
      next_flush_page_virtual_id = data_flush_context.get_next_flush_page_virtual_id();
      int64_t truncate_page_virtual_id = get_page_virtual_id_(truncated_offset_, false /*is_open_interval*/);
      if (ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID != truncate_page_virtual_id &&
          truncate_page_virtual_id > data_flush_context.get_flushed_page_virtual_id()) {
        if (OB_FAIL(get_next_flush_page_id_(next_flush_page_id, next_flush_page_virtual_id))) {
          LOG_ERROR("origin next flush page has been truncated, fail to get_next_flush_page_id_",
              KR(ret), K(data_flush_context), KPC(this));
        } else {
          LOG_INFO("origin next flush page has been truncated",
              KR(ret), K(next_flush_page_id), K(next_flush_page_virtual_id), K(data_flush_context), KPC(this));
        }
      }

      if (OB_SUCC(ret) && ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID != truncate_page_virtual_id &&
          truncate_page_virtual_id <= data_flush_context.get_flushed_page_virtual_id()) {
        uint32_t last_flushed_page_id = data_flush_context.get_flushed_page_id();
        int64_t last_flushed_page_virtual_id = data_flush_context.get_flushed_page_virtual_id();
        if (ObTmpFileGlobal::INVALID_PAGE_ID != last_flushed_page_id) {
          if (!wbp_->is_write_back(fd_, last_flushed_page_id, ObTmpFilePageUniqKey(last_flushed_page_virtual_id))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("last flushed page state not match",
                KR(ret), K(last_flushed_page_id), K(data_flush_context), KPC(this));
          }
        }
      }
    }
  } else {
    // cal next_flush_page_id from file meta when doing flush for the first time
    if (OB_FAIL(get_next_flush_page_id_(next_flush_page_id, next_flush_page_virtual_id))) {
      LOG_WARN("fail to get_next_flush_page_id_", KR(ret), K(fd_), K(data_flush_context));
    }
  }

  if (OB_SUCC(ret) && ObTmpFileGlobal::INVALID_PAGE_ID != next_flush_page_id) {
    if (!wbp_->is_dirty(fd_, next_flush_page_id, ObTmpFilePageUniqKey(next_flush_page_virtual_id)) &&
        !wbp_->is_write_back(fd_, next_flush_page_id, ObTmpFilePageUniqKey(next_flush_page_virtual_id))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("next_flush_page_id state not validate",
          KR(ret), K(next_flush_page_id), K(data_flush_context), KPC(this));
    }
  }

  return ret;
}

// output next page id after flushed_page_id_ in normal case，or output flushed_page_id for write operation appending last page
int ObSharedNothingTmpFile::get_next_flush_page_id_(uint32_t& next_flush_page_id, int64_t& next_flush_page_virtual_id) const
{
  int ret = OB_SUCCESS;
  next_flush_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  next_flush_page_virtual_id = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
  if (ObTmpFileGlobal::INVALID_PAGE_ID == flushed_page_id_) {
    // all pages are dirty
    if (OB_UNLIKELY((ObTmpFileGlobal::INVALID_PAGE_ID == begin_page_id_ &&
                     ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID != begin_page_virtual_id_) ||
                    (ObTmpFileGlobal::INVALID_PAGE_ID != begin_page_id_ &&
                     ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == begin_page_virtual_id_) )) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("begin_page_id_ and begin_page_virtual_id_ not match", KR(ret), K(begin_page_id_), K(begin_page_virtual_id_), KPC(this));
    } else {
      next_flush_page_id = begin_page_id_;
      next_flush_page_virtual_id = begin_page_virtual_id_;
    }
  } else if (wbp_->is_dirty(fd_, flushed_page_id_, ObTmpFilePageUniqKey(flushed_page_virtual_id_))) {
    // start from flushed_page_id_ if flushed_page_id_ is dirty caused by appending write
    if (OB_UNLIKELY((ObTmpFileGlobal::INVALID_PAGE_ID == flushed_page_id_ &&
                     ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID != flushed_page_virtual_id_) ||
                    (ObTmpFileGlobal::INVALID_PAGE_ID != flushed_page_id_ &&
                     ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == flushed_page_virtual_id_) )) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("flushed_page_id_ and flushed_page_virtual_id_ not match",
                KR(ret), K(flushed_page_id_), K(flushed_page_virtual_id_), KPC(this));
    } else {
      next_flush_page_id = flushed_page_id_;
      next_flush_page_virtual_id = flushed_page_virtual_id_;
    }
  } else if (OB_FAIL(wbp_->get_next_page_id(fd_, flushed_page_id_, ObTmpFilePageUniqKey(flushed_page_virtual_id_), next_flush_page_id))){
    // start from the next page, could return INVALID_PAGE_ID if flushed_page_id_ == end_page_id_
    LOG_WARN("fail to get next page id", KR(ret), K(fd_), K(begin_page_id_), K(flushed_page_id_), K(end_page_id_));
  } else if (ObTmpFileGlobal::INVALID_PAGE_ID == next_flush_page_id) {
    next_flush_page_virtual_id = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
  } else {
    next_flush_page_virtual_id = flushed_page_virtual_id_ + 1;
  }

  LOG_DEBUG("get_next_flush_page_id_", KR(ret), K(fd_), K(begin_page_id_), K(flushed_page_id_), KPC(this));
  return ret;
}

int ObSharedNothingTmpFile::get_physical_page_id_in_wbp_(const int64_t virtual_page_id, uint32_t& page_id) const
{
  int ret = OB_SUCCESS;
  int64_t end_page_virtual_id = get_end_page_virtual_id_();
  page_id = ObTmpFileGlobal::INVALID_PAGE_ID;

  if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == virtual_page_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd_), K(virtual_page_id));
  } else if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == begin_page_virtual_id_ ||
                         ObTmpFileGlobal::INVALID_PAGE_ID == begin_page_id_ ||
                         ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == end_page_virtual_id ||
                         ObTmpFileGlobal::INVALID_PAGE_ID == end_page_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no pages exist in wbp", KR(ret), K(begin_page_id_), K(begin_page_virtual_id_),
             K(end_page_id_), K(end_page_virtual_id), KPC(this));
  } else if (OB_UNLIKELY(virtual_page_id < begin_page_virtual_id_ || virtual_page_id > end_page_virtual_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the page doesn't exist in wbp", KR(ret), K(virtual_page_id), K(end_page_virtual_id), K(begin_page_virtual_id_), KPC(this));
  } else if (virtual_page_id == begin_page_virtual_id_) {
    page_id = begin_page_id_;
  } else if (ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == flushed_page_virtual_id_ ||
      virtual_page_id < flushed_page_virtual_id_) {
    if (OB_FAIL(wbp_->get_page_id_by_virtual_id(fd_, virtual_page_id, begin_page_id_, page_id))) {
      LOG_WARN("fail to get page id by virtual id", KR(ret), K(virtual_page_id), K(begin_page_id_), KPC(this));
    }
  } else if (virtual_page_id == flushed_page_virtual_id_) {
    page_id = flushed_page_id_;
  } else if (virtual_page_id == end_page_virtual_id) {
    page_id = end_page_id_;
  } else { // virtual_page_id < end_page_virtual_id
    if (OB_FAIL(wbp_->get_page_id_by_virtual_id(fd_, virtual_page_id, flushed_page_id_, page_id))) {
      LOG_WARN("fail to get page id by virtual id", KR(ret), K(virtual_page_id), K(flushed_page_id_), KPC(this));
    }
  }

  return ret;
}

// output the last page to be flushed
int ObSharedNothingTmpFile::get_flush_end_page_id_(uint32_t& flush_end_page_id, const bool need_flush_tail) const
{
  int ret = OB_SUCCESS;
  const int64_t INVALID_PAGE_ID = ObTmpFileGlobal::INVALID_PAGE_ID;
  flush_end_page_id = INVALID_PAGE_ID;
  if (file_size_ % ObTmpFileGlobal::PAGE_SIZE == 0) {
    flush_end_page_id = INVALID_PAGE_ID;
  } else { // determined whether to flush last page based on flag if last page is not full
    flush_end_page_id = need_flush_tail ? INVALID_PAGE_ID : end_page_id_;
  }

  LOG_DEBUG("get_flush_end_page_id_", K(fd_), K(need_flush_tail), K(flush_end_page_id), KPC(this));
  return ret;
}

}  // end namespace tmp_file
}  // end namespace oceanbase
