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

#define USING_LOG_PREFIX SHARE

#include "share/redolog/ob_log_file_reader.h"
#include "share/redolog/ob_log_file_handler.h"
#include "share/redolog/ob_log_definition.h"
#include "storage/ob_file_system_router.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
namespace share
{
static const char *MEMORY_LABEL = "LogFileReader";

ObLogReadFdKey::ObLogReadFdKey()
{
  reset();
}

void ObLogReadFdKey::reset()
{
  MEMSET(path_, 0, sizeof(path_));
  path_[0] = '\0';
}

bool ObLogReadFdKey::is_valid() const
{
   const int64_t len = STRLEN(path_);
   return len > 0 && len < MAX_PATH_SIZE;
}

uint64_t ObLogReadFdKey::hash() const
{
  uint64_t hash_val = 0;
  if (is_valid()) {
    hash_val = common::murmurhash(&path_, static_cast<int32_t>(STRLEN(path_)), hash_val);
  }
  return hash_val;
}

bool ObLogReadFdKey::operator==(const ObLogReadFdKey &other) const
{
  return 0 == STRNCMP(path_, other.path_, MAX_PATH_SIZE);
}

bool ObLogReadFdKey::operator!=(const ObLogReadFdKey &other) const
{
  return !(*this == other);
}

ObLogReadFdCacheItem::ObLogReadFdCacheItem()
  : key_(), in_map_(false),
    io_fd_(), ref_cnt_(0), timestamp_(OB_INVALID_TIMESTAMP),
    prev_(nullptr), next_(nullptr)
{
}

void ObLogReadFdCacheItem::reset()
{
  int ret = OB_SUCCESS;
  if (ref_cnt_ > 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ref count not zero when reset", K(ret), K(*this));
  }
  key_.reset();
  in_map_ = false;
  io_fd_.reset();
  ref_cnt_ = 0;
  timestamp_ = OB_INVALID_TIMESTAMP;
  prev_ = nullptr;
  next_ = nullptr;
}

void ObLogReadFdCacheItem::inc_ref()
{
  ATOMIC_INC(&ref_cnt_);
}

void ObLogReadFdCacheItem::dec_ref()
{
  ATOMIC_DEC(&ref_cnt_);
}

int64_t ObLogReadFdCacheItem::get_ref()
{
  return ATOMIC_LOAD(&ref_cnt_);
}

void ObLogReadFdHandle::reset()
{
  if (nullptr != fd_item_) {
    fd_item_->dec_ref();
    if (is_local_) {
      THE_IO_DEVICE->close(fd_item_->io_fd_);
      fd_item_->reset();
      OB_DELETE(ObLogReadFdCacheItem, MEMORY_LABEL, fd_item_);
      is_local_ = false;
    }
    fd_item_ = nullptr;
  }
}

int ObLogReadFdHandle::set_read_fd(ObLogReadFdCacheItem *fd_item, const bool is_local)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(fd_item)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    reset();
    fd_item_ = fd_item;
    fd_item_->inc_ref();
    is_local_ = is_local;
  }
  return ret;
}

common::ObIOFd ObLogReadFdHandle::get_read_fd() const
{
  return nullptr == fd_item_ ? common::ObIOFd() : fd_item_->io_fd_;
}

void ObLogFileReader2::EvictTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(reader_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("reader_ is null", K(ret));
  } else {
    reader_->do_clear_work();
  }
}

ObLogFileReader2 & ObLogFileReader2::get_instance()
{
  static ObLogFileReader2 instance_;
  return instance_;
}

ObLogFileReader2::ObLogFileReader2()
  : quick_map_(), lock_(),
    max_cache_fd_cnt_(MAX_OPEN_FILE_CNT), cache_evict_time_in_us_(CACHE_EVICT_TIME_IN_US),
    head_(nullptr), tail_(nullptr),
    timer_(), evict_task_(this), is_inited_(false)
{
}

ObLogFileReader2::~ObLogFileReader2()
{
  destroy();
}

int ObLogFileReader2::init()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already inited", K(ret));
  } else if (OB_FAIL(quick_map_.create(MAP_BUCKET_INIT_CNT, "LogFileReaderM"))) {
    LOG_WARN("already inited", K(ret));
  } else if (OB_FAIL(timer_.init("ObLogFileReader2"))) {
    LOG_WARN("init timer fail", K(ret));
  } else if (OB_FAIL(timer_.schedule(evict_task_, cache_evict_time_in_us_, true))) {
    LOG_WARN("schedule timer fail", K(ret));
  } else {
    is_inited_ = true;
  }

  if (OB_UNLIKELY(!is_inited_)) {
    destroy();
  }
  return ret;
}

void ObLogFileReader2::destroy()
{
  timer_.stop();
  timer_.wait();
  timer_.destroy();
  quick_map_.destroy();
  ObLogReadFdCacheItem *cur = tail_;
  while (nullptr != cur) {
    ObLogReadFdCacheItem *prev = cur->prev_;
    THE_IO_DEVICE->close(cur->io_fd_);
    cur->reset();
    OB_DELETE(ObLogReadFdCacheItem, MEMORY_LABEL, cur);
    cur = prev;
  }
  head_ = nullptr;
  tail_ = nullptr;
  is_inited_ = false;
}

int ObLogFileReader2::pread(
    const ObLogReadFdHandle &fd_handle,
    void *buf,
    const int64_t count,
    const int64_t offset,
    int64_t &read_size)
{
  int ret = OB_SUCCESS;
  read_size = -1;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!fd_handle.is_valid() || count <= 0 || offset < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument.", K(ret), K(fd_handle), K(count), K(offset));
  } else {
    common::ObIOFd target_io_fd = fd_handle.get_read_fd();
    if (!target_io_fd.is_normal_file()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid fd", K(ret), K(target_io_fd));
    } else if (OB_FAIL(THE_IO_DEVICE->pread(target_io_fd, offset, count, buf, read_size))) {
      LOG_ERROR("fail to pread", K(ret), K(target_io_fd), K(offset), K(count), K(errno), KERRMSG);
    }
  }
  return ret;
}

int ObLogFileReader2::move_item_to_head(ObLogReadFdCacheItem &item)
{
  int ret = OB_SUCCESS;
  if (nullptr == head_ || nullptr == tail_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("head or tail is null", K(ret), KP(head_), KP(tail_), K(item));
  } else if (head_ == &item) {
    // do nothing
  } else {
    if (tail_ == &item) {
      tail_ = item.prev_;
    } else {
      item.next_->prev_ = item.prev_;
    }
    item.prev_->next_ = item.next_;

    head_->prev_ = &item;
    item.next_ = head_;
    item.prev_ = nullptr;
    head_ = &item;
  }
  return ret;
}

int ObLogFileReader2::evict_fd_from_map(const ObLogReadFdKey &fd_key)
{
  int ret = OB_SUCCESS;
  ObLogReadFdCacheItem *target = nullptr;

  if (OB_UNLIKELY(!fd_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid fd key", K(ret), K(fd_key));
  } else if (OB_FAIL(quick_map_.erase_refactored(fd_key, &target)) && OB_HASH_NOT_EXIST != ret) {
    LOG_WARN("erase item from map fail", K(ret), K(fd_key));
  } else if (OB_HASH_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
  } else if (OB_ISNULL(target)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("erased item is null", K(ret), K(fd_key));
  } else {
    target->in_map_ = false;
  }
  return ret;
}

int ObLogFileReader2::evict_fd(const char* log_dir, const uint32_t file_id)
{
  int ret = OB_SUCCESS;
  char file_path[MAX_PATH_SIZE] = {'\0'};

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!ObLogFileHandler::is_valid_file_id(file_id) || OB_ISNULL(log_dir)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument.", K(ret), K(file_id), KP(log_dir));
  } else if (OB_FAIL(ObLogFileHandler::format_file_path(file_path, sizeof(file_path),
      log_dir, file_id))) {
    LOG_WARN("format_file_path fail", K(ret), K(file_id), K(log_dir));
  } else if (OB_FAIL(evict_fd(file_path))) {
    LOG_WARN("evict fd fail", K(ret), K(file_path));
  }
  return ret;
}

int ObLogFileReader2::evict_fd(const char* file_path)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    lib::ObMutexGuard guard(lock_);
    ObLogReadFdKey fd_key;
    STRNCPY(fd_key.path_, file_path, MAX_PATH_SIZE - 1);
    fd_key.path_[MAX_PATH_SIZE - 1] = 0;
    if (OB_FAIL(evict_fd_from_map(fd_key))) {
      LOG_WARN("move item to tail fail", K(ret), K(fd_key));
    }
  }
  return ret;
}

int ObLogFileReader2::get_fd(
    const char* log_dir,
    const uint32_t file_id,
    ObLogReadFdHandle &fd_handle)
{
  const int64_t start_time = ObTimeUtility::current_time();
  int ret = OB_SUCCESS;
  char file_path[MAX_PATH_SIZE] = {'\0'};
  bool hit_cache = false;
  ObLogReadFdCacheItem *ret_item = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!ObLogFileHandler::is_valid_file_id(file_id) || OB_ISNULL(log_dir)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument.", K(ret), K(file_id), KP(log_dir));
  } else if (OB_FAIL(ObLogFileHandler::format_file_path(file_path, sizeof(file_path),
      log_dir, file_id))) {
    LOG_WARN("format_file_path fail", K(ret), K(file_id), K(log_dir));
  } else {
    ObLogReadFdKey fd_key;
    STRNCPY(fd_key.path_, file_path, MAX_PATH_SIZE);
    {
      lib::ObMutexGuard guard(lock_);
      if (OB_FAIL(try_get_cache(fd_key, ret_item)) && OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("get fd from cache fail", K(ret), K(fd_key));
      } else {
        hit_cache = (ret == OB_SUCCESS);
        ret = OB_SUCCESS;
        if (hit_cache) {
          if (OB_FAIL(fd_handle.set_read_fd(ret_item, false/*is_local*/))) {
            LOG_ERROR("set read handle fail", K(ret), K(fd_key));
          }
        }
      }
    }

    if (OB_SUCC(ret) && !hit_cache) {
      common::ObIOFd ret_io_fd;
      if (OB_FAIL(open_fd(fd_key, ret_io_fd))) {
        LOG_WARN("prepare fd fail", K(ret), K(fd_key));
      } else {
        bool is_tmp = false;
        lib::ObMutexGuard guard(lock_);
        if (OB_FAIL(put_new_item(fd_key, ret_io_fd, ret_item, is_tmp))) {
          LOG_WARN("put new item fail", K(ret), K(fd_key), K(is_tmp));
        } else if (OB_FAIL(fd_handle.set_read_fd(ret_item, is_tmp))) {
          LOG_ERROR("set read handle fail", K(ret), K(fd_key), K(ret_io_fd), K(is_tmp));
        }
      }
    }
  }

  if (OB_SUCC(ret) && !hit_cache) {
    const int64_t duration = ObTimeUtility::current_time() - start_time;
    LOG_TRACE("get_fd cost", K(duration), K(hit_cache), K(*ret_item), KP(ret_item));
  }
  return ret;
}

int ObLogFileReader2::do_clear_work()
{
  int ret = OB_SUCCESS;

  ObLogReadFdCacheItem *recycle_item = nullptr;
  ObLogReadFdCacheItem *cur = nullptr;
  {
    lib::ObMutexGuard guard(lock_);
    cur = tail_;
  }
  while (OB_SUCC(ret) && nullptr != cur) {
    { // lock start
      lib::ObMutexGuard guard(lock_);
      if (ObTimeUtility::fast_current_time() - cur->timestamp_ < cache_evict_time_in_us_) {
        ret = OB_ITER_END; // stop this round, take a rest
      } else if (cur->in_map_) { // evict old fd in map
        if (OB_FAIL(evict_fd_from_map(cur->key_))) {
          LOG_WARN("erase item from map fail", K(ret), K(*cur));
        } else {
          LOG_TRACE("evict fd from map", K(*cur));
        }
      }
      if (OB_SUCC(ret)) { // release 0 ref count fd
        ObLogReadFdCacheItem *prev = cur->prev_;
        if (0 == cur->get_ref()) {
          recycle_item = cur;
          LOG_TRACE("recycle item", K(*recycle_item), KP(recycle_item), KP(head_), KP(tail_));
          if (tail_ == cur) {
            tail_ = cur->prev_;
          }
          if (head_ == cur) {
            head_ = head_->next_;
          }
          if (nullptr != prev) {
            prev->next_ = cur->next_;
          }
          if (nullptr != cur->next_) {
            cur->next_->prev_ = cur->prev_;
          }
        }
        cur = prev;
      }
    } // lock end

    if (OB_SUCC(ret) && nullptr != recycle_item) {
      THE_IO_DEVICE->close(recycle_item->io_fd_);
      recycle_item->reset();
      OB_DELETE(ObLogReadFdCacheItem, MEMORY_LABEL, recycle_item);
    }
  } // end while

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObLogFileReader2::open_fd(const ObLogReadFdKey &fd_key, common::ObIOFd &ret_io_fd)
{
  int ret = OB_SUCCESS;
  ret_io_fd.reset();

  if (OB_UNLIKELY(!fd_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(fd_key));
  } else if (OB_FAIL(ObLogFileHandler::open(fd_key.path_, O_RDONLY | O_DIRECT, 0, ret_io_fd))) {
    LOG_WARN("failed to open file", K(ret), K(fd_key));
  } else if (OB_UNLIKELY(!ret_io_fd.is_normal_file())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get invalid fd", K(ret), K(fd_key), K(ret_io_fd));
  }
  return ret;
}

int ObLogFileReader2::put_new_item(
    const ObLogReadFdKey &fd_key,
    common::ObIOFd &open_io_fd,
    ObLogReadFdCacheItem *&ret_item,
    bool &is_tmp)
{
  int ret = OB_SUCCESS;
  ObLogReadFdCacheItem *new_item = nullptr;
  is_tmp = false;

  if (OB_UNLIKELY(!fd_key.is_valid()) || OB_UNLIKELY(nullptr != ret_item)
      || OB_UNLIKELY(!open_io_fd.is_normal_file())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(ret_item), K(fd_key), K(open_io_fd));
  } else if (NULL == (new_item = OB_NEW(ObLogReadFdCacheItem, MEMORY_LABEL))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc new item fail", K(ret));
  } else {
    new_item->reset();
    ret_item = new_item;
    ret_item->key_ = fd_key;
    ret_item->io_fd_ = open_io_fd;
  }

  if (OB_SUCC(ret)) {
    if (quick_map_.size() >= max_cache_fd_cnt_) {
      is_tmp = true;
      LOG_DEBUG("cached map is full, return temporary item", K(quick_map_.size()), K(*ret_item));
    } else {
      if (OB_FAIL(quick_map_.set_refactored(fd_key, new_item))
          && OB_HASH_EXIST != ret) {
        LOG_WARN("set new item fail", K(ret), K(fd_key), K(*new_item));
      } else if (OB_HASH_EXIST == ret) {
        // some thread already put new, close self and get from cache again
        THE_IO_DEVICE->close(open_io_fd);
        if (nullptr != new_item) {
          new_item->reset();
          OB_DELETE(ObLogReadFdCacheItem, MEMORY_LABEL, new_item);
        }
        if (OB_FAIL(try_get_cache(fd_key, ret_item))) {
          LOG_ERROR("get cache item fail", K(ret), K(fd_key));
        }
      } else {
        ret_item->in_map_ = true;
        ret_item->timestamp_ = ObTimeUtility::fast_current_time();
        ret_item->prev_ = nullptr;
        ret_item->next_ = head_;
        if (nullptr != head_) {
          head_->prev_ = ret_item;
        }
        head_ = ret_item;
        if (nullptr == tail_) {
          tail_ = head_;
        }
        LOG_DEBUG("add item to head", KP(ret_item), KP(head_), KP(tail_), K(*ret_item));
      }
    }
  }

  if (OB_FAIL(ret)) {
    if (open_io_fd.is_normal_file()) {
      THE_IO_DEVICE->close(open_io_fd); // ignore ret
    }
    if (nullptr != new_item) {
      new_item->reset();
      OB_DELETE(ObLogReadFdCacheItem, MEMORY_LABEL, new_item);
      ret_item = nullptr;
    }
  }
  return ret;
}

int ObLogFileReader2::try_get_cache(const ObLogReadFdKey &fd_key, ObLogReadFdCacheItem *&ret_item)
{
  int ret = OB_SUCCESS;
  ret_item = nullptr;
  ObLogReadFdCacheItem *p_item = nullptr;

  if (OB_UNLIKELY(nullptr != ret_item) || OB_UNLIKELY(!fd_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(ret_item), K(fd_key));
  } else if (OB_FAIL(quick_map_.get_refactored(fd_key, p_item))
      && OB_HASH_NOT_EXIST != ret) {
    LOG_WARN("get from map fail", K(ret), K(fd_key));
  } else if (OB_HASH_NOT_EXIST == ret) {
    // cache miss
  } else if (OB_ISNULL(p_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null cache item", K(ret), K(fd_key));
  } else if (p_item->timestamp_ == OB_INVALID_TIMESTAMP
      || ObTimeUtility::fast_current_time() - p_item->timestamp_ > cache_evict_time_in_us_) {
    if (OB_FAIL(evict_fd_from_map(fd_key))) {
      LOG_WARN("retire expired fd from map fail", K(ret), K(fd_key), K(*p_item));
    } else {
      ret = OB_HASH_NOT_EXIST;
    }
  } else if (OB_FAIL(move_item_to_head(*p_item))) {
    LOG_WARN("move item to head fail", K(ret), K(*p_item));
  } else {
    ret_item = p_item;
    ret_item->timestamp_ = ObTimeUtility::fast_current_time();
    LOG_DEBUG("move item to head", KP(ret_item), KP(head_), KP(tail_), K(*ret_item));
  }
  return ret;
}

} // namespace share
} // namespace oceanbase
