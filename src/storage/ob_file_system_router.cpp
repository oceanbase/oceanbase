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
#include "ob_file_system_router.h"
#include "observer/ob_server_struct.h"
#include "share/config/ob_server_config.h"
#include "share/ob_io_device_helper.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace rootserver;
using namespace blocksstable;
namespace storage {
/**
 * -------------------------------------ObFileSystemRouter-----------------------------------------
 */
ObFileSystemRouter & ObFileSystemRouter::get_instance()
{
  static ObFileSystemRouter instance_;
  return instance_;
}

ObFileSystemRouter::ObFileSystemRouter()
{
  data_dir_[0] = '\0';
  slog_dir_[0] = '\0';
  clog_dir_[0] = '\0';
  sstable_dir_[0] = '\0';

  clog_file_spec_.retry_write_policy_ = "normal";
  clog_file_spec_.log_create_policy_ = "normal";
  clog_file_spec_.log_write_policy_ = "truncate";

  slog_file_spec_.retry_write_policy_ = "normal";
  slog_file_spec_.log_create_policy_ = "normal";
  slog_file_spec_.log_write_policy_ = "truncate";
  svr_seq_ = 0;
  is_inited_ = false;
}

void ObFileSystemRouter::reset()
{
  data_dir_[0] = '\0';
  slog_dir_[0] = '\0';
  clog_dir_[0] = '\0';
  sstable_dir_[0] = '\0';

  clog_file_spec_.retry_write_policy_ = "normal";
  clog_file_spec_.log_create_policy_ = "normal";
  clog_file_spec_.log_write_policy_ = "truncate";

  slog_file_spec_.retry_write_policy_ = "normal";
  slog_file_spec_.log_create_policy_ = "normal";
  slog_file_spec_.log_write_policy_ = "truncate";

  svr_seq_ = 0;
  is_inited_ = false;
}

int ObFileSystemRouter::init(
    const char *data_dir,
    const char *cluster_name,
    const int64_t cluster_id,
    const char *zone,
    const ObAddr &svr_addr)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_ISNULL(data_dir) || OB_ISNULL(cluster_name) || OB_UNLIKELY(!svr_addr.is_valid())
      || OB_UNLIKELY(cluster_id < 0) || OB_ISNULL(zone)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(data_dir), KP(cluster_name), K(svr_addr),
        K(cluster_id), KP(zone));
  } else if (OB_FAIL(init_local_dirs(data_dir))) {
    LOG_WARN("init local dir fail", K(ret), K(data_dir));
  } else {
    clog_file_spec_.retry_write_policy_ = "normal";
    clog_file_spec_.log_create_policy_ = "normal";
    clog_file_spec_.log_write_policy_ = "truncate";

    slog_file_spec_.retry_write_policy_ = "normal";
    slog_file_spec_.log_create_policy_ = "normal";
    slog_file_spec_.log_write_policy_ = "truncate";
    is_inited_ = true;
  }

  if (IS_NOT_INIT) {
    reset();
  }
  return ret;
}

int ObFileSystemRouter::get_tenant_clog_dir(
    const uint64_t tenant_id,
    char (&tenant_clog_dir)[common::MAX_PATH_SIZE])
{
  int ret = OB_SUCCESS;
  int pret = 0;
  pret = snprintf(tenant_clog_dir, MAX_PATH_SIZE, "%s/tenant_%" PRIu64,
                  clog_dir_, tenant_id);
  if (pret < 0 || pret >= MAX_PATH_SIZE) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_ERROR("construct tenant clog path fail", K(ret), K(tenant_id));
  }
  return ret;
}

int ObFileSystemRouter::init_local_dirs(const char* data_dir)
{
  int ret = OB_SUCCESS;
  int pret = 0;

  if (OB_SUCC(ret)) {
    pret = snprintf(data_dir_, MAX_PATH_SIZE, "%s", data_dir);
    if (pret < 0 || pret >= MAX_PATH_SIZE) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_ERROR("construct data dir fail", K(ret), K(data_dir));
    }
  }

  if (OB_SUCC(ret)) {
    pret = snprintf(slog_dir_, MAX_PATH_SIZE, "%s/slog", data_dir);
    if (pret < 0 || pret >= MAX_PATH_SIZE) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_ERROR("construct slog path fail", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    pret = snprintf(clog_dir_, MAX_PATH_SIZE, "%s/clog", data_dir);
    if (pret < 0 || pret >= MAX_PATH_SIZE) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_ERROR("construct clog path fail", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    pret = snprintf(sstable_dir_, MAX_PATH_SIZE, "%s/sstable", data_dir);
    if (pret < 0 || pret >= MAX_PATH_SIZE) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_ERROR("construct sstable path fail", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("succeed to construct local dir",
      K(data_dir_), K(slog_dir_), K(clog_dir_), K(sstable_dir_));
  }

  return ret;
}

} // namespace storage
} // namespace oceanbase
