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

#include "storage/ddl/ob_cg_block_tmp_files_iterator.h"

namespace oceanbase
{
namespace storage
{
/**
* -----------------------------------ObCGBlockFilesIterator-----------------------------------
*/
void ObCGBlockFilesIterator::reset()
{
  can_put_cg_block_back_ = true;
  total_data_size_ = 0;
  while (!cg_block_files_.empty()) {
    ObCGBlockFile *cg_block_file = cg_block_files_.get_first();
    if (OB_LIKELY(nullptr != cg_block_file)) {
        cg_block_file->~ObCGBlockFile();
        ob_free(cg_block_file);
        cg_block_file = nullptr;
    }
    cg_block_files_.pop_front();
  }
  allocator_.reset();
}
int ObCGBlockFilesIterator::push_back_cg_block_files(ObIArray<ObCGBlockFile *> &cg_block_files)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(cg_block_files.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the cg block files is empty", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < cg_block_files.count(); ++i) {
      ObCGBlockFile *cg_block_file = cg_block_files.at(i);
      if (OB_FAIL(push_back_cg_block_file(cg_block_file))) {
        LOG_WARN("fail to push back cg block file", K(ret));
      }
    }
  }
  return ret;
}

int ObCGBlockFilesIterator::push_back_cg_block_file(ObCGBlockFile *cg_block_file)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == cg_block_file)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cg block file is null", K(ret));
  } else if (OB_UNLIKELY(!cg_block_file->is_opened() ||
                          cg_block_file->get_data_size() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cg block file is not opened or has not data",
        K(ret), K(cg_block_file->is_opened()), K(cg_block_file->get_data_size()));
  } else if (OB_FAIL(cg_block_files_.push_back(cg_block_file))) {
    LOG_WARN("fail to push back cg block file", K(ret), KPC(cg_block_file));
  } else {
    total_data_size_ += cg_block_file->get_data_size();
  }
  return ret;
}

int ObCGBlockFilesIterator::get_next_cg_block(ObCGBlock &cg_block)
{
  int ret = OB_SUCCESS;
  cg_block.reset();
  while (OB_SUCC(ret)) {
    if (cg_block_files_.empty()) {
      ret = OB_ITER_END;
    } else {
      ObCGBlockFile *cg_block_file = cg_block_files_.get_first();
      if (OB_UNLIKELY(nullptr == cg_block_file)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cg block file is null", K(ret));
      } else if (OB_FAIL(cg_block_file->get_next_cg_block(cg_block))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          if (OB_FAIL(cg_block_files_.pop_front())) {
            LOG_WARN("fail to pop front cg block file", K(ret));
          } else if (OB_FAIL(cg_block_file->close())) {
            LOG_WARN("fail to close cg block file", K(ret), KPC(cg_block_file));
          } else {
            cg_block_file->~ObCGBlockFile();
            ob_free(cg_block_file);
            cg_block_file = nullptr;
          }
        } else {
          LOG_WARN("fail to get next cg block", K(ret));
        }
      } else {
        total_data_size_ -= (cg_block.get_macro_buffer_size() - cg_block.get_cg_block_offset());
        can_put_cg_block_back_ = true;
        if (total_data_size_ < 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("total data size is invalid", K(ret), K(total_data_size_));
        }
        break;
      }
    }
  }
  return ret;
}

int ObCGBlockFilesIterator::put_cg_block_back(const ObCGBlock &cg_block)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!cg_block.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cg block is not initialized", K(ret), K(cg_block));
  } else if (OB_UNLIKELY(!can_put_cg_block_back_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can not put cg block back", K(ret));
  } else {
    if (OB_UNLIKELY(cg_block_files_.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cg block files is empty", K(ret));
    } else {
      ObCGBlockFile *cg_block_file = cg_block_files_.get_first();
      if (OB_UNLIKELY(nullptr == cg_block_file)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cg block file is null", K(ret));
      } else if (OB_FAIL(cg_block_file->put_cg_block_back(cg_block))) {
        LOG_WARN("fail to put cg block back", K(ret));
      } else {
        total_data_size_ += (cg_block.get_macro_buffer_size() - cg_block.get_cg_block_offset());
        can_put_cg_block_back_ = false;
      }
    }
  }
  return ret;
}

int ObCGBlockFilesIterator::get_remain_block_files(ObIArray<ObCGBlockFile *> &block_files)
{
  int ret = OB_SUCCESS;
  block_files.reuse();
  while (OB_SUCC(ret) && cg_block_files_.size() > 0) {
    ObCGBlockFile *cg_block_file = cg_block_files_.get_first();
    if (OB_UNLIKELY(nullptr == cg_block_file)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cg block file is null", K(ret));
    } else if (OB_FAIL(block_files.push_back(cg_block_file))) {
      LOG_WARN("push back block file failed", K(ret), KPC(cg_block_file));
    } else if (OB_FAIL(cg_block_files_.pop_front())) {
      LOG_WARN("fail to pop front cg block file", K(ret));
    }
  }
  if (OB_FAIL(ret) && block_files.count() > 0) {
    for (int64_t i = 0; i < block_files.count(); ++i) {
      ObCGBlockFile *&cg_block_file = block_files.at(i);
      if (OB_LIKELY(nullptr != cg_block_file)) {
        cg_block_file->~ObCGBlockFile();
        ob_free(cg_block_file);
        cg_block_file = nullptr;
      }
    }
  }
  return ret;
}

} //end storage
} // end oceanbase
