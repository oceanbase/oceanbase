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

#include "lib/file/file_directory_utils.h"
#include "ob_all_virtual_cgroup_config.h"
#include <fts.h>
namespace oceanbase
{
using namespace lib;

namespace observer
{

ObAllVirtualCgroupConfig::ObAllVirtualCgroupConfig() : is_inited_(false)
{
}

ObAllVirtualCgroupConfig::~ObAllVirtualCgroupConfig()
{
  reset();
}

int ObAllVirtualCgroupConfig::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObServerConfig::get_instance().self_addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))
              == false)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "ip_to_string() fail", K(ret));
  }
  return ret;
}

void ObAllVirtualCgroupConfig::reset()
{
  is_inited_ = false;
}

int ObAllVirtualCgroupConfig::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    if (OB_FAIL(read_cgroup_path_dir_(cgroup_link_path))) {
      ret = OB_ITER_END;
    } else {
      scanner_it_ = scanner_.begin();
    }
    is_inited_ = true;
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "fail to get next row", K(ret));
      }
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

int ObAllVirtualCgroupConfig::check_cgroup_dir_exist_(const char *cgroup_path)
{
  int ret = OB_SUCCESS;
  bool exist_cgroup = false;
  int link_len = 0;
  if (OB_ISNULL(cgroup_path)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid arguments.", K(cgroup_path), K(ret));
  } else if (OB_FAIL(FileDirectoryUtils::is_exists(cgroup_path, exist_cgroup))) {
    SERVER_LOG(WARN, "fail check file exist", K(cgroup_path), K(ret));
  } else if (!exist_cgroup) {
    ret = OB_FILE_NOT_EXIST;
    SERVER_LOG(WARN, "no cgroup directory found. disable cgroup support", K(cgroup_path), K(ret));
  } else if (-1 == (link_len = readlink(cgroup_path, cgroup_origin_path_, PATH_BUFSIZE))) {
    SERVER_LOG(WARN, "The named file is not a symbolic link", K(cgroup_path), K(ret));
    snprintf(cgroup_origin_path_, PATH_BUFSIZE, "%s", cgroup_path);
  } else if (link_len > 0 && cgroup_origin_path_[link_len - 1] == '/') {
    cgroup_origin_path_[link_len - 1] = '\0';
  } else {
    cgroup_origin_path_[link_len] = '\0';
  }
  return ret;
}

int ObAllVirtualCgroupConfig::read_cgroup_path_dir_(const char *cgroup_path)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_cgroup_dir_exist_(cgroup_path))) {
    SERVER_LOG(WARN, "cgroup config file not exist : ", K(ret), K(cgroup_path));
  } else {
    char *dir[] = {cgroup_origin_path_, NULL};
    FTS *ftsp = fts_open(dir, FTS_NOCHDIR, NULL);
    if (OB_NOT_NULL(ftsp)) {
      FTSENT *node = NULL;
      while ((node = fts_read(ftsp)) != NULL && OB_SUCC(ret)) {
        if (node->fts_info == FTS_D) {
          ret = add_cgroup_config_info_(node->fts_path);
        }
      }
      fts_close(ftsp);
    }
    if (OB_FAIL(ret)) {
      SERVER_LOG(WARN, "cgroup config read : ", K(ret), K(cgroup_origin_path_));
    }
  }
  return ret;
}

int ObAllVirtualCgroupConfig::add_cgroup_config_info_(const char *cgroup_path)
{
  int ret = OB_SUCCESS;
  const int64_t col_count = output_column_ids_.count();
  for (int64_t i = 0; i < col_count && OB_SUCC(ret); ++i) {
    const uint64_t col_id = output_column_ids_.at(i);
    ObObj *cells = cur_row_.cells_;
    switch (col_id) {
      case SVR_IP: {
        cells[i].set_varchar(ip_buf_);
        cells[i].set_collation_type(
            ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      }
      case SVR_PORT: {
        cells[i].set_int(GCONF.self_addr_.get_port());
        break;
      }
      case CFS_QUOTA_US: {
        char path[PATH_BUFSIZE];
        snprintf(path, PATH_BUFSIZE, "%s/%s", cgroup_path, "cpu.cfs_quota_us");
        FILE *file = fopen(path, "r");
        if (NULL == file) {
          cells[i].set_int(0);
        } else {
          char buf[VALUE_BUFSIZE];
          fscanf(file, "%s", buf);
          int cfs_quota_us = atoi(buf);
          cells[i].set_int(cfs_quota_us);
        }
        if (NULL != file) {
          fclose(file);
        }
        break;
      }
      case CFS_PERIOD_US: {
        char path[PATH_BUFSIZE];
        snprintf(path, PATH_BUFSIZE, "%s/%s", cgroup_path, "cpu.cfs_period_us");
        FILE *file = fopen(path, "r");
        if (NULL == file) {
          cells[i].set_int(0);
        } else {
          char buf[VALUE_BUFSIZE];
          fscanf(file, "%s", buf);
          int cfs_period_us = atoi(buf);
          cells[i].set_int(cfs_period_us);
        }
        if (NULL != file) {
          fclose(file);
        }
        break;
      }
      case SHARES: {
        char path[PATH_BUFSIZE];
        snprintf(path, PATH_BUFSIZE, "%s/%s", cgroup_path, "cpu.shares");
        FILE *file = fopen(path, "r");
        if (NULL == file) {
          cells[i].set_int(0);
        } else {
          char buf[VALUE_BUFSIZE];
          fscanf(file, "%s", buf);
          int shares = atoi(buf);
          cells[i].set_int(shares);
        }
        if (NULL != file) {
          fclose(file);
        }
        break;
      }
      case CGROUP_PATH: {
        int path_len = strlen(cgroup_path) - strlen(root_cgroup_path);
        if (path_len <= 0) {
          cells[i].set_varchar("");
        } else {
          strncpy(cgroup_path_buf_, cgroup_path + strlen(root_cgroup_path), path_len);
          cgroup_path_buf_[path_len] = '\0';
          cells[i].set_varchar(cgroup_path_buf_);
        }
        cells[i].set_collation_type(
            ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "unexpected column id", K(col_id), K(i), K(ret));
        break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    // scanner最大支持64M，因此暂不考虑溢出的情况
    if (OB_FAIL(scanner_.add_row(cur_row_))) {
      SERVER_LOG(WARN, "fail to add row", K(ret), K(cur_row_));
      if (OB_SIZE_OVERFLOW == ret) {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase