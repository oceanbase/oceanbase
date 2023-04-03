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
#include <errno.h>
#include "common/storage/ob_io_device.h"
#include "lib/oblog/ob_log.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/config/ob_server_config.h"
#include "share/ob_device_manager.h"
#include "share/ob_errno.h"
#include "share/ob_force_print_log.h"
#include "share/ob_io_device_helper.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace share
{
/**
 * --------------------------------ObGetFileIdRangeFunctor------------------------------------
 */
int ObGetFileIdRangeFunctor::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(entry));
  } else {
    bool is_number = true;
    const char* entry_name = entry->d_name;
    for (int64_t i = 0; is_number && i < sizeof(entry->d_name); ++i) {
      if ('\0' == entry_name[i]) {
        break;
      } else if (!isdigit(entry_name[i])) {
        is_number = false;
      }
    }
    if (!is_number) {
      // do nothing, skip invalid file like tmp
    } else {
      uint32_t file_id = static_cast<uint32_t>(strtol(entry->d_name, nullptr, 10));
      if (OB_INVALID_FILE_ID == min_file_id_ || file_id < min_file_id_) {
        min_file_id_ = file_id;
      }
      if (OB_INVALID_FILE_ID == max_file_id_ || file_id > max_file_id_) {
        max_file_id_ = file_id;
      }
    }
  }
  return ret;
}

/**
 * --------------------------------ObGetFileSizeFunctor------------------------------------
 */
int ObGetFileSizeFunctor::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(entry));
  } else {
    char full_path[common::MAX_PATH_SIZE] = { 0 };
    int p_ret = snprintf(full_path, sizeof(full_path), "%s/%s", dir_, entry->d_name);
    if (p_ret < 0 || p_ret >= sizeof(full_path)) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("file name too long", K(ret), K_(dir), K(entry->d_name));
    } else {
      ObIODFileStat statbuf;
      if (OB_FAIL(THE_IO_DEVICE->stat(full_path, statbuf))
          && OB_NO_SUCH_FILE_OR_DIRECTORY != ret) {
        LOG_WARN("fail to stat file", K(full_path), K(statbuf));
      } else if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
        // file could be renamed (tmp file) or recycled concurrently, ignore
        ret = OB_SUCCESS;
      } else if (S_ISREG(statbuf.mode_)) {
        total_size_ += statbuf.size_;
      } else if (S_ISDIR(statbuf.mode_)) {
        ObGetFileSizeFunctor functor(full_path);
        if (OB_FAIL(THE_IO_DEVICE->scan_dir(full_path, functor))) {
          LOG_WARN("fail to scan dir", K(ret), K(full_path), K(entry->d_name));
        } else {
          total_size_ += functor.get_total_size();
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected file type", K(full_path), K(statbuf));
      }
    }
  }
  return ret;
}

/**
 * --------------------------------ObIODeviceWrapper------------------------------------
 */
ObIODeviceWrapper &ObIODeviceWrapper::get_instance()
{
  static ObIODeviceWrapper instance;
  return instance;
}

ObIODeviceWrapper::ObIODeviceWrapper()
  : local_device_(NULL), is_inited_(false)
{
}

ObIODeviceWrapper::~ObIODeviceWrapper()
{
  destroy();
}

/*io device helper hold one local/ofs device, and never free*/
int get_local_device_from_mgr(share::ObLocalDevice*& local_device)
{
  int ret = OB_SUCCESS;

  common::ObIODevice* device = nullptr;
  common::ObString storage_info(OB_LOCAL_PREFIX);
  //for the local and nfs, storage_prefix and storage info are same
  if(OB_FAIL(common::ObDeviceManager::get_instance().get_device(storage_info, storage_info, device))) {
    LOG_WARN("fail to get local device", K(ret));
  } else if (OB_ISNULL(local_device = static_cast<share::ObLocalDevice*>(device))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get local device", K(ret));
  }

  if (OB_FAIL(ret)) {
    //if fail, release the resource
    if (nullptr != local_device) {
      common::ObDeviceManager::get_instance().release_device((ObIODevice*&)local_device);
      local_device = nullptr;
    }
  }
  return ret;
}

int ObIODeviceWrapper::init(
    const char *data_dir,
    const char *sstable_dir,
    const int64_t block_size,
    const int64_t data_disk_percentage,
    const int64_t data_disk_size)
{
  int ret = OB_SUCCESS;
  const int64_t MAX_IOD_OPT_CNT = 5;
  ObIODOpt iod_opt_array[MAX_IOD_OPT_CNT];
  ObIODOpts iod_opts;
  iod_opts.opts_ = iod_opt_array;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already inited", K(ret));
  } else if (OB_FAIL(get_local_device_from_mgr(local_device_))) {
    LOG_WARN("fail to get the device", K(ret));
  } else if (OB_ISNULL(data_dir) || OB_ISNULL(sstable_dir)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(data_dir), KP(sstable_dir));
  } else if ('/' != data_dir[0] && '.' != data_dir[0]) {
    ret = OB_IO_ERROR;
    LOG_ERROR("unknown storage type, not support", K(ret), K(data_dir));
  } else {
    THE_IO_DEVICE = local_device_;
    iod_opt_array[0].set("data_dir", data_dir);
    iod_opt_array[1].set("sstable_dir", sstable_dir);
    iod_opt_array[2].set("block_size", block_size);
    iod_opt_array[3].set("datafile_disk_percentage", data_disk_percentage);
    iod_opt_array[4].set("datafile_size", data_disk_size);
    iod_opts.opt_cnt_ = MAX_IOD_OPT_CNT;
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(THE_IO_DEVICE)) {
    if (OB_FAIL(THE_IO_DEVICE->init(iod_opts))) {
      LOG_WARN("fail to init io device", K(ret), K(data_dir), K(sstable_dir), K(block_size),
          K(data_disk_percentage), K(data_disk_size));
    } else {
      is_inited_ = true;
      LOG_INFO("finish to init io device", K(ret), K(data_dir), K(sstable_dir), K(block_size),
          K(data_disk_percentage), K(data_disk_size));
    }
  }

  if (IS_NOT_INIT) {
    destroy();
  }

  return ret;
}

void ObIODeviceWrapper::destroy()
{
  if (is_inited_) {
    THE_IO_DEVICE = nullptr;
    if (NULL != local_device_) {
      local_device_->destroy();
      common::ObDeviceManager::get_instance().release_device((ObIODevice*&)local_device_);
      local_device_ = NULL;
    }

    is_inited_ = false;
    LOG_INFO("io device wrapper destroy");
  }
}
} // namespace share
} // namespace oceanbase
