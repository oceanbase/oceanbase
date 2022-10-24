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

#include <sys/vfs.h>
#include "share/redolog/ob_log_file_group.h"
#include "share/redolog/ob_log_file_handler.h"
#include "share/config/ob_server_config.h"
#include "share/ob_io_device_helper.h"
#include "share/ob_unit_getter.h"
#include "lib/statistic_event/ob_stat_event.h"
#include "lib/stat/ob_diagnose_info.h"

using namespace oceanbase::share;
using namespace oceanbase::storage;

namespace oceanbase
{
namespace common
{
ObLogFileGroup::ObLogFileGroup()
  : is_inited_(false),
    min_file_id_(OB_INVALID_FILE_ID),
    min_using_file_id_(OB_INVALID_FILE_ID),
    max_file_id_(OB_INVALID_FILE_ID),
    log_dir_(nullptr),
    total_disk_size_(0)
{
}

ObLogFileGroup::~ObLogFileGroup()
{
  destroy();
}

int ObLogFileGroup::init(const char *log_dir)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already inited", K(ret));
  } else if (OB_ISNULL(log_dir) || OB_UNLIKELY(0 == STRLEN(log_dir))) {
    LOG_WARN("invalid args", K(ret), K(log_dir));
  } else {
    struct statfs buf;
    if (0 != ::statfs(log_dir, &buf)) {
      ret = OB_IO_ERROR;
      LOG_WARN("failed to statfs", K(ret), K(log_dir), K(errno), KERRMSG);
    } else {
      total_disk_size_ = (int64_t)buf.f_bsize * (int64_t)buf.f_blocks;
    }
  }

  if (OB_SUCC(ret)) {
    log_dir_ = log_dir;
    is_inited_ = true;
  }
  return ret;
}

void ObLogFileGroup::destroy()
{
  min_file_id_ = OB_INVALID_FILE_ID;
  min_using_file_id_ = OB_INVALID_FILE_ID;
  max_file_id_ = OB_INVALID_FILE_ID;
  log_dir_ = nullptr;
  total_disk_size_ = 0;
  is_inited_ = false;
}

int ObLogFileGroup::get_file_id_range(int64_t &min_file_id, int64_t &max_file_id)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    bool need_scan_dir = false;
    min_file_id = ATOMIC_LOAD(&min_file_id_);
    max_file_id = ATOMIC_LOAD(&max_file_id_);

    if (OB_INVALID_FILE_ID == min_file_id || OB_INVALID_FILE_ID == max_file_id) {
      need_scan_dir = true;
    } else {
      int tmp_ret = OB_SUCCESS;
      bool b_exist = false;
      // check min file id
      if (!need_scan_dir
          && OB_UNLIKELY(common::OB_SUCCESS != (tmp_ret = check_file_existence(log_dir_, min_file_id, b_exist)))) {
        need_scan_dir = true;
        LOG_WARN("failed to check file existence", K(tmp_ret), K_(log_dir), K(min_file_id));
      } else if (!need_scan_dir && !b_exist) {
        need_scan_dir = true;
        LOG_WARN("min file does not exist", K(min_file_id), K(b_exist));
      }

      // check max file id
      if (!need_scan_dir
          && OB_UNLIKELY(common::OB_SUCCESS != (tmp_ret = check_file_existence(log_dir_, max_file_id, b_exist)))) {
        need_scan_dir = true;
        LOG_WARN("failed to check file existence", K(tmp_ret), K_(log_dir), K(max_file_id));
      } else if (!need_scan_dir && !b_exist) {
        need_scan_dir = true;
        LOG_WARN("max file does not exist", K(max_file_id), K(b_exist));
      }
    }

    if (need_scan_dir) {
      ObGetFileIdRangeFunctor functor(log_dir_);
      if (OB_FAIL(THE_IO_DEVICE->scan_dir(log_dir_, functor))) {
        LOG_WARN("fail to scan dir", K(ret), K_(log_dir));
      } else {
        min_file_id = functor.get_min_file_id();
        max_file_id = functor.get_max_file_id();
        if (OB_INVALID_FILE_ID == min_file_id || OB_INVALID_FILE_ID == max_file_id) {
          ret = OB_ENTRY_NOT_EXIST;
          if (REACH_TIME_INTERVAL(10 * 1000 * 1000L)) {
            LOG_INFO("log dir is empty", K(ret), K_(log_dir));
          }
        } else {
          update_min_file_id(min_file_id);
          update_max_file_id(max_file_id);
          LOG_INFO("get min/max file id from IO", K_(log_dir), K(min_file_id), K(max_file_id), K(lbt()));
        }
      }
    }
  }
  return ret;
}

int ObLogFileGroup::get_total_disk_space(int64_t &total_space) const
{
  int ret = OB_SUCCESS;
  total_space = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    total_space = total_disk_size_;
  }
  return ret;
}

int ObLogFileGroup::get_total_used_size(int64_t &total_size) const
{
  int ret = OB_SUCCESS;
  const uint64_t min_file_id = ATOMIC_LOAD(&min_file_id_);
  const uint64_t max_file_id = ATOMIC_LOAD(&max_file_id_);
  total_size = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    ObGetFileSizeFunctor functor(log_dir_);
    if (OB_FAIL(THE_IO_DEVICE->scan_dir(log_dir_, functor))) {
      LOG_WARN("fail to scan dir", K(ret), K_(log_dir));
    } else {
      total_size = functor.get_total_size();
    }
  }
  return ret;
}

void ObLogFileGroup::update_min_file_id(const int64_t file_id)
{
  while (true) {
    const int64_t orig_min_file_id = ATOMIC_LOAD(&min_file_id_);
    if (ObLogFileHandler::is_valid_file_id(orig_min_file_id) && file_id <= orig_min_file_id) {
      break;
    } else if (ATOMIC_BCAS(&min_file_id_, orig_min_file_id, file_id)) {
      break;
    } else {
      // do nothing
    }
  }
}

void ObLogFileGroup::update_max_file_id(const int64_t file_id)
{
  while (true) {
    const int64_t orig_max_file_id = ATOMIC_LOAD(&max_file_id_);
    if (ObLogFileHandler::is_valid_file_id(orig_max_file_id) && file_id <= orig_max_file_id) {
      break;
    } else if (ATOMIC_BCAS(&max_file_id_, orig_max_file_id, file_id)) {
      break;
    } else {
      // do nothing
    }
  }
}

int ObLogFileGroup::check_file_existence(const char *dir, const int64_t file_id, bool &b_exist)
{
  int ret = OB_SUCCESS;
  b_exist = false;
  char full_path[common::MAX_PATH_SIZE] = { 0 };
  if (OB_ISNULL(dir)
      || OB_UNLIKELY(0 == STRLEN(dir))
      || OB_UNLIKELY(!ObLogFileHandler::is_valid_file_id(file_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(dir), K(file_id));
  } else if (OB_FAIL(ObLogFileHandler::format_file_path(
      full_path, sizeof(full_path), dir, file_id))) {
    LOG_WARN("failed to format file path", K(ret), K(dir), K(file_id));
  } else if (OB_FAIL(THE_IO_DEVICE->exist(full_path, b_exist))) {
    LOG_WARN("failed to check existence", K(ret), K(full_path));
  }
  return ret;
}

} // namespace common
} // namespace oceanbase
