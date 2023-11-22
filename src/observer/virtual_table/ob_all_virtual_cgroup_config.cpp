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
    if (OB_FAIL(read_cgroup_path_dir_(root_cgroup_))) {
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
  if (OB_ISNULL(cgroup_path)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid arguments.", K(cgroup_path), K(ret));
  } else if (OB_FAIL(FileDirectoryUtils::is_exists(cgroup_path, exist_cgroup))) {
    SERVER_LOG(WARN, "fail check file exist", K(cgroup_path), K(ret));
  } else if (!exist_cgroup) {
    ret = OB_FILE_NOT_EXIST;
    SERVER_LOG(WARN, "no cgroup directory found. disable cgroup support", K(cgroup_path), K(ret));
  }
  return ret;
}

int ObAllVirtualCgroupConfig::get_cur_cgroup_path_sub_paths_(const char *cur_cgroup_path, common::ObIArray<char *> &result)
{
  int ret = OB_SUCCESS;
  struct dirent *dp;
  DIR *dfd;
  if((dfd = opendir(cur_cgroup_path)) == NULL) {
    ret = OB_FILE_NOT_EXIST;
    SERVER_LOG(WARN, "open cgroup_path failed : ", K(ret), K(cur_cgroup_path));
  } else {
    common::ObSEArray<char *, 16> tmp_array;
    while((dp = readdir(dfd)) != NULL && OB_SUCC(ret)) {
      if(strcmp(dp->d_name, ".") != 0 && strcmp(dp -> d_name, "..") != 0 && dp->d_type == DT_DIR) {
        char* next_path = (char *)ob_malloc(PATH_BUFSIZE, "CGROUP_PATH_BUF");
        if (OB_ISNULL(next_path)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          SERVER_LOG(WARN, "cgroup sub path not memory : ", K(ret), K(cur_cgroup_path));
        } else {
          sprintf(next_path, "%s/%s", cur_cgroup_path, dp->d_name);
          if(OB_FAIL(tmp_array.push_back(next_path))) {
            SERVER_LOG(WARN, "tmp array add sub path failed : ", K(ret), K(cur_cgroup_path));
          }
        }
      }
    }
    closedir(dfd);
    if (OB_SUCC(ret)) {
      for (int i = tmp_array.count() - 1; i >= 0 && OB_SUCC(ret); i--) {
        if (OB_ISNULL(tmp_array[i])) {
          ret = OB_NULL_CHECK_ERROR;
          SERVER_LOG(WARN, "tmp array get null : ", K(ret), K(tmp_array));
        } else if (OB_FAIL(result.push_back(tmp_array[i]))) {
          SERVER_LOG(WARN, "push to result failed : ", K(ret), K(tmp_array));
        }
      }
    } else {
      for (int i = 0; i < tmp_array.count() && OB_SUCC(ret); i++) {
        if (OB_NOT_NULL(tmp_array[i])) {
          ob_free(tmp_array[i]);
        }
      }
    }
  }
  return ret;
}

int ObAllVirtualCgroupConfig::read_cgroup_path_dir_(const char *cgroup_path)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<char *, 16> path_stack;
  if (OB_FAIL(check_cgroup_dir_exist_(cgroup_path))) {
    SERVER_LOG(WARN, "cgroup config file not exist : ", K(ret), K(cgroup_path));
  } else if(OB_FAIL(get_cur_cgroup_path_sub_paths_(cgroup_path, path_stack))) {
    SERVER_LOG(WARN, "get root cgroup sub path failed : ", K(ret), K(cgroup_path));
  } else {
    while (!path_stack.empty() && OB_SUCC(ret)) {
      char *cur_path = NULL;
      if (OB_FAIL(path_stack.pop_back(cur_path))) {
        SERVER_LOG(WARN, "cgroup path stack pop failed", K(ret), K(path_stack));
      } else if (OB_ISNULL(cur_path)) {
        ret = OB_ERR_NULL_VALUE;
        SERVER_LOG(WARN, "cgroup path stack pop null", K(ret), K(path_stack));
      } else if (OB_FAIL(add_cgroup_config_info_(cur_path))) {
        SERVER_LOG(WARN, "add cgroup config info failed : ", K(ret), K(cur_path));
      } else if (OB_FAIL(get_cur_cgroup_path_sub_paths_(cur_path, path_stack))) {
        SERVER_LOG(WARN, "get cur cgroup sub path failed : ", K(ret), K(cur_path));
      }
      if (OB_NOT_NULL(cur_path)) {
        ob_free(cur_path);
      }
    }
  }
  for (int i = 0; i < path_stack.count(); i++) {
    if (OB_NOT_NULL(path_stack[i])) {
      ob_free(path_stack[i]);
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
        fclose(file);
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
        fclose(file);
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
        fclose(file);
        break;
      }
      case CGROUP_PATH: {
        int path_len = strlen(cgroup_path) - strlen(root_cgroup_);
        strncpy(cgroup_path_buf_, cgroup_path + strlen(root_cgroup_), path_len);
        cgroup_path_buf_[path_len] = '\0';
        cells[i].set_varchar(cgroup_path_buf_);
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