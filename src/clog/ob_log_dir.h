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

#ifndef OCEANBASE_CLOG_OB_LOG_DIR_
#define OCEANBASE_CLOG_OB_LOG_DIR_

#include <fcntl.h>
#include <stdint.h>
#include <dirent.h>
#include <sys/types.h>
#include "ob_log_define.h"

namespace oceanbase {
namespace clog {
class ObDir {
public:
  ObDir() : is_inited_(false), dir_fd_(-1)
  {
    dir_name_[0] = '\0';
  }
  ~ObDir()
  {
    destroy();
  }
  const char* get_dir_name() const
  {
    return dir_name_;
  }
  int get_dir_fd() const
  {
    return dir_fd_;
  }
  int init(const char* dir_name);
  void destroy();
  template <typename Function>
  int for_each(Function& fn) const
  {
    int ret = common::OB_SUCCESS;
    if (!is_inited_) {
      ret = common::OB_NOT_INIT;
    } else {
      DIR* dir = NULL;
      if (NULL == (dir = opendir(dir_name_))) {
        ret = common::OB_IO_ERROR;
        CLOG_LOG(ERROR, "opendir error", K(ret), K_(dir_name), KERRMSG, K(errno));
      } else {
        while (OB_SUCC(ret)) {
          struct dirent entry;
          struct dirent* pentry = &entry;
          memset(&entry, 0, sizeof(struct dirent));
          if (0 != readdir_r(dir, pentry, &pentry)) {
            ret = common::OB_IO_ERROR;
            CLOG_LOG(WARN, "readdir error", K(ret), K_(dir_name), K(errno));
          } else if (NULL == pentry) {
            ret = common::OB_ITER_END;
          } else if (!fn(dir_name_, pentry->d_name)) {
            ret = common::OB_EAGAIN;
          } else {
            // do nothing
          }
        }
        if (common::OB_ITER_END == ret) {
          // rewrite ret
          ret = common::OB_SUCCESS;
        }
        if (0 != closedir(dir)) {
          ret = common::OB_IO_ERROR;
          CLOG_LOG(WARN, "closedir error", K(ret), K_(dir_name), KERRMSG);
        } else {
          dir = NULL;
        }
      }
    }
    return ret;
  }

public:
  TO_STRING_KV(K_(is_inited), K_(dir_fd), K_(dir_name));

private:
  static const int OPEN_MODE = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
  static const int MKDIR_MODE = OPEN_MODE | S_IXUSR;

private:
  bool is_inited_;
  int dir_fd_;
  char dir_name_[common::MAX_PATH_SIZE];
};

class ObILogDir {
public:
  ObILogDir()
  {}
  virtual ~ObILogDir()
  {}
  virtual int get_file_id_range(file_id_t& min_file_id, file_id_t& max_file_id) const = 0;
  virtual int get_min_file_id(file_id_t& file_id) const = 0;
  virtual int get_max_file_id(file_id_t& file_id) const = 0;
  virtual const char* get_dir_name() const = 0;
  // Suitable for openat() usage scenarios
  // Return the relative path of the fd of the directory and the file, which can optimize the path parsing process in
  // the process of opening the file, especially the directory is a soft link scenario
  virtual int get_path(const file_id_t file_id, char* path, const int64_t size, int& dir_fd) const = 0;
  virtual int get_tmp_path(const file_id_t file_id, char* path, const int64_t size, int& dir_fd) const = 0;
  virtual int get_total_size(int64_t& total_size) const = 0;

public:
  VIRTUAL_TO_STRING_KV("", "");

protected:
  class GetFileIdRange {
  public:
    explicit GetFileIdRange() : min_file_id_(common::OB_INVALID_FILE_ID), max_file_id_(common::OB_INVALID_FILE_ID)
    {}
    ~GetFileIdRange()
    {}
    bool operator()(const char* dir_name, const char* entry);
    file_id_t get_min_file_id() const
    {
      return min_file_id_;
    }
    file_id_t get_max_file_id() const
    {
      return max_file_id_;
    }

  private:
    file_id_t min_file_id_;
    file_id_t max_file_id_;
  };
  class GetFileSizeFunctor {
  public:
    explicit GetFileSizeFunctor(const ObILogDir* log_dir) : total_size_(0), log_dir_(log_dir)
    {}
    ~GetFileSizeFunctor()
    {}

  public:
    bool operator()(const char* dir_name, const char* entry);
    int64_t get_total_size() const
    {
      return total_size_;
    }

  private:
    int get_size_(const file_id_t file_id, int64_t& size);
    int64_t total_size_;
    const ObILogDir* log_dir_;
  };
};

class ObLogDir : public ObILogDir {
public:
  ObLogDir() : is_inited_(false), dir_()
  {}
  ~ObLogDir()
  {}
  int init(const char* dir_name);
  void destroy();
  int get_file_id_range(file_id_t& min_file_id, file_id_t& max_file_id) const;
  int get_min_file_id(file_id_t& file_id) const;
  int get_max_file_id(file_id_t& file_id) const;
  const char* get_dir_name() const
  {
    return dir_.get_dir_name();
  }
  int get_path(const file_id_t file_id, char* path, const int64_t size, int& dir_fd) const;
  int get_tmp_path(const file_id_t file_id, char* path, const int64_t size, int& dir_fd) const;
  int get_total_size(int64_t& total_size) const;

public:
  TO_STRING_KV(K_(is_inited), K_(dir));

private:
  bool is_inited_;
  ObDir dir_;
};
}  // end namespace clog
}  // end namespace oceanbase

#endif  // OCEANBASE_CLOG_OB_LOG_DIR_
