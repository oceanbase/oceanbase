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

#include "common/log/ob_log_dir_scanner.h"
#include <dirent.h>
#include "common/log/ob_log_entry.h"

using namespace oceanbase::common;

int ObSimpleLogFile::assign(const char *filename, FileType &type)
{
  int ret = OB_SUCCESS;
  if (NULL == filename) {
    SHARE_LOG(ERROR, "parameter filename=NULL. SHOULD NOT REACH HERE");
    ret = OB_ERROR;
  } else {
    char basename[OB_MAX_FILE_NAME_LENGTH];
    int f_len = static_cast<int32_t>(strlen(filename));
    if (f_len >= OB_MAX_FILE_NAME_LENGTH) {
      type = UNKNOWN;
    } else {
      int ckpt_len = static_cast<int32_t>(strlen(DEFAULT_CKPT_EXTENSION));
      if (f_len > ckpt_len + 1
          && 0 == STRCMP(filename + f_len - ckpt_len, DEFAULT_CKPT_EXTENSION)
          && '.' == *(filename + f_len - ckpt_len - 1)) {
        f_len -= ckpt_len + 1;
        strncpy(basename, filename, f_len);
        basename[f_len] = '\0';
        type = CKPT;
      } else {
        strncpy(basename, filename, f_len);
        basename[f_len] = '\0';
        type = LOG;
      }

      if (!is_log_id(basename)) {
        type = UNKNOWN;
      } else {
        strcpy(name, filename);
        id = str_to_uint64(basename);
      }
    }
  }

  return ret;
}

bool ObSimpleLogFile::is_log_id(const char *str) const
{
  bool ret = true;
  if (str == NULL || (*str) == '\0' || (*str) == '0') {
    ret = false;
  } else {
    while ((*str) != '\0') {
      if ((*str) < '0' || (*str) > '9') {
        ret = false;
        break;
      }
      str ++;
    }
  }
  return ret;
}

uint64_t ObSimpleLogFile::str_to_uint64(const char *str) const
{
  uint64_t u = 0U;
  if (NULL == str) {
    u = 0U;
  } else {
    u = atoll(str);
  }
  return u;
}

bool ObSimpleLogFile::operator< (const ObSimpleLogFile &r) const
{
  return id < r.id;
}

ObLogDirScanner::ObLogDirScanner()
{
  reset();
}

ObLogDirScanner::~ObLogDirScanner()
{
}

int ObLogDirScanner::init(const char *log_dir)
{
  int ret = OB_SUCCESS;

  if (is_init_) {
    SHARE_LOG(ERROR, "ObLogDirScanner has been initialized.");
    ret = OB_INIT_TWICE;
  } else {
    ret = search_log_dir_(log_dir);
    if (OB_SUCC(ret)) {
      is_init_ = true;
      SHARE_LOG(INFO, "ObLogDirScanner initialize successfully",
                K_(min_log_id), K_(max_log_id), K_(max_ckpt_id));
    } else if (OB_DISCONTINUOUS_LOG == ret) {
      is_init_ = true;
      _SHARE_LOG(INFO, "ObLogDirScanner initialize successfully"
                "[min_log_id_=%lu max_log_id_=%lu max_ckpt_id_=%lu], "
                "but log files in \"%s\" directory is not continuous",
                min_log_id_, max_log_id_, max_ckpt_id_, log_dir);
    } else {
      SHARE_LOG(ERROR, "search_log_dir_ error, ObLogDirScanner initialize failed",
                KCSTRING(log_dir), K(ret));
    }
  }

  return ret;
}

int ObLogDirScanner::get_min_log_id(uint64_t &log_id) const
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_inner_stat())) {
    log_id = 0;
  } else {
    if (has_log()) {
      log_id = min_log_id_;
    } else {
      log_id = 0;
      ret = OB_ENTRY_NOT_EXIST;
    }
  }

  return ret;
}

int ObLogDirScanner::get_max_log_id(uint64_t &log_id) const
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_inner_stat())) {
    log_id = 0;
  } else {
    if (has_log()) {
      log_id = max_log_id_;
    } else {
      log_id = 0;
      ret = OB_ENTRY_NOT_EXIST;
    }
  }

  return ret;
}

int ObLogDirScanner::get_max_ckpt_id(uint64_t &ckpt_id) const
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_inner_stat())) {
    ckpt_id = 0;
  } else {
    if (has_ckpt()) {
      ckpt_id = max_ckpt_id_;
    } else {
      ckpt_id = 0;
      ret = OB_ENTRY_NOT_EXIST;
    }
  }

  return ret;
}

bool ObLogDirScanner::has_log() const
{
  return has_log_;
}

bool ObLogDirScanner::has_ckpt() const
{
  return has_ckpt_;
}

int ObLogDirScanner::search_log_dir_(const char *log_dir)
{
  int ret = OB_SUCCESS;

  int func_ret = 0;

  common::ObVector<ObSimpleLogFile> log_files(NULL, ObModIds::OB_SLOG_SCANNER);
  DIR *plog_dir = opendir(log_dir);
  if (OB_UNLIKELY(NULL == plog_dir)) {
    func_ret = mkdir(log_dir, S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH);
    if (func_ret != 0
        && EEXIST != errno) {
      ret = OB_ERROR;
      SHARE_LOG(ERROR, "mkdir error", K(ret), KCSTRING(log_dir), KERRMSG);
    } else {
      plog_dir = opendir(log_dir);
      if (NULL == plog_dir) {
        ret = OB_ERROR;
        SHARE_LOG(ERROR, "opendir error", K(ret), KCSTRING(log_dir), KERRMSG);
      }
    }
  }

  if (OB_SUCC(ret)) {
    struct dirent entry;
    memset(&entry, 0x00, sizeof(struct dirent));
    struct dirent *pentry = &entry;

    ObSimpleLogFile log_file;
    ObSimpleLogFile::FileType file_type;

    func_ret = readdir_r(plog_dir, pentry, &pentry);
    while (OB_SUCC(ret) && 0 == func_ret && NULL != pentry) {
      if (OB_FAIL(log_file.assign(pentry->d_name, file_type))) {
        SHARE_LOG(WARN, "assign log file fail", K(file_type), KCSTRING(pentry->d_name));
      } else {
        if (ObSimpleLogFile::LOG == file_type) {
          if (OB_FAIL(log_files.push_back(log_file))) {
            SHARE_LOG(WARN, "fail to push log file", KCSTRING(log_file.name));
          } else {
            SHARE_LOG(DEBUG, "find a valid log file", KCSTRING(log_file.name));
          }
        } else if (ObSimpleLogFile::CKPT == file_type) {
          SHARE_LOG(DEBUG, "find a valid checkpoint file", KCSTRING(log_file.name));
          if (max_ckpt_id_ < log_file.id) {
            max_ckpt_id_ = log_file.id;
            has_ckpt_ = true;
          }
        } else {
          _SHARE_LOG(DEBUG, "ignore file(\"%s\"): \"%s\" is not valid log file",
                    pentry->d_name, pentry->d_name);
        }
        if (OB_SUCC(ret)) {
          func_ret = readdir_r(plog_dir, pentry, &pentry);
        }
      }
    }
    if (0 != func_ret) {
      SHARE_LOG(ERROR, "readdir_r error", K(func_ret), K(pentry), K(plog_dir));
      ret = OB_ERROR;
    }
    func_ret = closedir(plog_dir);
    if (func_ret < 0) {
      SHARE_LOG(ERROR, "closedir", KCSTRING(log_dir), KERRMSG);
      ret = OB_SUCCESS;
    }
  }

  if (OB_SUCC(ret)) {
    lib::ob_sort(log_files.begin(), log_files.end());
    ret = check_continuity_(log_files, min_log_id_, max_log_id_);
  }

  return ret;
}

int ObLogDirScanner::check_continuity_(const ObVector<ObSimpleLogFile> &files, uint64_t &min_file_id,
                                       uint64_t &max_file_id)
{
  int ret = OB_SUCCESS;

  if (files.size() == 0) {
    min_file_id = max_file_id = 0;
    has_log_ = false;
  } else {
    has_log_ = true;
    int size = files.size();
    min_file_id = max_file_id = files[size - 1].id;
    int i = size - 1;
    uint64_t pre_id = files[i].id;
    i--;
    for (; i >= 0; --i) {
      if (files[i].id != (pre_id - 1)) {
        break;
      } else {
        min_file_id = files[i].id;
      }
      pre_id = files[i].id;
    }

    if (i >= 0) {
      ret = OB_DISCONTINUOUS_LOG;
    }
  }

  return ret;
}

void ObLogDirScanner::reset()
{
  min_log_id_ = 0;
  max_log_id_ = 0;
  max_ckpt_id_ = 0;
  has_log_ = false;
  has_ckpt_ = false;
  is_init_ = false;
}
