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

#define USING_LOG_PREFIX COMMON

#include "common/ob_segmented_buffer.h"
#include "lib/allocator/ob_malloc.h"

namespace oceanbase
{
namespace common
{
int ObSegmentedBufffer::append(char *ptr, const int64_t len)
{
  int ret = OB_SUCCESS;
  if (nullptr == ptr || len < 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const static int tail_size = sizeof(char *);
    if (nullptr == block_) {
      if (nullptr == (block_ = (char *)ob_malloc(block_size_, attr_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        *reinterpret_cast<char **>(block_ + block_size_ - tail_size) = nullptr;
        head_ = block_;
        pos_ = 0;
      }
    }
    if (OB_SUCC(ret)) {
      int64_t left = len;
      while (OB_SUCC(ret) && left > 0) {
        const int64_t inner_pos = pos_ % block_size_;
        const int64_t len1 = std::min(block_size_ - inner_pos - tail_size, left);
        MEMCPY(block_ + inner_pos, ptr + len - left, len1);
        pos_ += len1;
        left -= len1;

        if (left > 0) {
          LOG_INFO("alloc new block");
          char *new_block = nullptr;
          if (nullptr == (new_block = (char *)ob_malloc(block_size_, attr_))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
          } else {
            *reinterpret_cast<char **>(new_block + block_size_ - tail_size) = nullptr;
            *reinterpret_cast<char **>(block_ + block_size_ - tail_size) = new_block;
            pos_ += tail_size;
            block_ = new_block;
          }
        }
      }
    }
  }

  return ret;
}

int64_t ObSegmentedBufffer::size() const
{
  return 0 == pos_ ? 0 :
    (block_size_ - sizeof(char *)) * ((pos_ - pos_ % block_size_) / block_size_) + pos_ % block_size_;
}

int ObSegmentedBufffer::padding(int64_t len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(head_)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    const int64_t batch = block_size_;
    int64_t left = len;
    while (left > 0) {
      int64_t wlen = std::min(left, batch);
      ret = append(head_, wlen);
      if (OB_SUCC(ret)) {
        left -= wlen;
      }
    }
  }
  return ret;
}

void ObSegmentedBufffer::destory()
{
  char *cur = head_;
  while (cur != nullptr) {
    char *next = *reinterpret_cast<char **>(cur + block_size_ - sizeof(char *));
    ob_free(cur);
    cur = next;
  }
  head_ = nullptr;
}

int ObSegmentedBufffer::dump_to_file(const char *file_name)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(file_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret));
  } else {
    int fd = -1;
    fd = ::open(file_name, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    if (fd < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("create new file failed", KCSTRING(file_name), KCSTRING(strerror(errno)));
    } else {
      ObSegmentedBuffferIterator sbi(*this);
      char *buf = nullptr;
      int64_t len = 0;
      while ((buf = sbi.next(len)) != nullptr) {
        ssize_t size = ::write(fd, buf, len);
        UNUSED(size);
      }
      close(fd);
    }
  }
  return ret;
}


char *ObSegmentedBuffferIterator::next(int64_t &len)
{
  char *buf = nullptr;
  if (next_buf_ != nullptr) {
    buf = next_buf_;
    next_buf_ = *reinterpret_cast<char **>(next_buf_ + sb_.block_size_ - sizeof(char *));
    len = next_buf_ != nullptr ? sb_.block_size_ - sizeof(char *) : sb_.pos_ % sb_.block_size_;
  }
  return buf;
}

} // namespace common
} // namespace oceanbase
