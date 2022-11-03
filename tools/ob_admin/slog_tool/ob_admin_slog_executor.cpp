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
#include "ob_admin_slog_executor.h"
#include <iostream>
#include "storage/slog_ckpt/ob_server_checkpoint_slog_handler.h"
#include "storage/slog_ckpt/ob_tenant_checkpoint_slog_handler.h"
#include "share/ob_local_device.h"
#include "share/ob_device_manager.h"
#include "common/storage/ob_io_device.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/slog/ob_storage_logger_manager.h"
#include "share/redolog/ob_log_file_handler.h"
#include "storage/blocksstable/ob_log_file_spec.h"

namespace oceanbase
{
  using namespace storage;
namespace tools
{

ObAdminSlogExecutor::ObAdminSlogExecutor()
  : ObAdminExecutor(),
    tenant_id_(OB_INVALID_TENANT_ID),
    log_file_id_(0),
    period_scan_(false),
    dir_op_(),
    offset_(0),
    parse_count_(-1)
{
}

int ObAdminSlogExecutor::execute(int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  ObTenantCheckpointSlogHandler tenant_handler;
  ObServerCheckpointSlogHandler server_handler;
  ObStorageLogReplayer replayer;

  char tenant_slog_dir[OB_MAX_FILE_NAME_LENGTH];

  if (OB_FAIL(parse_args(argc - 1, argv + 1))) {
    LOG_WARN("fail to parse dir path", K(ret));
  } else if (0 == STRLEN(data_dir_)) {
    if (OB_FAIL(load_config())) {
      LOG_WARN("fail to load config", K(ret));
    } else {
      STRCPY(data_dir_, config_mgr_.get_config().data_dir.str());
    }
  }
  if (OB_FAIL(ret)) {
     // do nothing
  } else if (OB_FAIL(prepare_io())) {
    LOG_WARN("fail to prepare io", K(ret));
  } else if (OB_FAIL(init_slogger_mgr())) {
    LOG_WARN("fail to init_slogger_mgr", K(ret));
  } else if (period_scan_) {
    if (OB_FAIL(scan_periodically())) {
      LOG_WARN("fail to scan slog file and check integrity", K(ret));
    }
  } else if (OB_UNLIKELY(tenant_id_ == OB_INVALID_TENANT_ID || log_file_id_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id_), K(log_file_id_));
  } else if (OB_FAIL(SLOGGERMGR.get_tenant_slog_dir(tenant_id_, tenant_slog_dir))) {
    LOG_WARN("fail to get_tenant_slog_dir", K(ret));
  } else if (OB_FAIL(parse_log(tenant_slog_dir, log_file_id_))) {
    LOG_WARN("fail to parse slog file", K(ret));
  }

  return ret;
}

int ObAdminSlogExecutor::scan_periodically()
{
  int ret = OB_SUCCESS;
  int64_t min_file_id = 0;
  int64_t max_file_id = 0;

  while (true) {
    THE_IO_DEVICE->scan_dir(SLOGGERMGR.get_root_dir(), dir_op_);
    ObLogFileHandler handler;
    for (int64_t i = 0; OB_SUCC(ret) && i < dir_op_.size_; i++) {
      if (OB_FAIL(concat_dir(SLOGGERMGR.get_root_dir(), dir_op_.d_names_[i]))) {
        LOG_WARN("fail to construct tenant slog path", K(ret));
      } else if (OB_FAIL(handler.init(slog_dir_, FILE_MAX_SIZE))) {
        LOG_WARN("fail to init log file handler", K(ret));
      } else if (OB_FAIL(handler.get_file_id_range(min_file_id, max_file_id))) {
        LOG_WARN("fail to get file id range", K(ret));
      } else {
        for (int64_t j = min_file_id; OB_SUCC(ret) && j <= max_file_id; j++) {
          if (OB_FAIL(parse_log(slog_dir_, j))) {
            if (OB_READ_NOTHING == ret) {
              LOG_WARN("this file has been deleted", K(ret), K(slog_dir_), "file_id: ", j);
              continue;
            }
            LOG_WARN("fail to parse slog", K(ret), K(min_file_id), K(max_file_id), K(j));
          } else {
            break;
          }
        }
      }
      min_file_id = 0;
      max_file_id = 0;
    }

    if (OB_SUCC(ret)) {
      fprintf(stderr, "\nsuccessfully print all slogs");
      usleep(300000000);
    } else {
      fprintf(stderr, "\nfail to print all slogs, ret=%d", ret);
      break;
    }
  }

  return ret;
}

int ObAdminSlogExecutor::parse_log(const char *slog_dir, const int64_t file_id)
{
  int ret = OB_SUCCESS;

  ObStorageLogReplayer replayer;
  ObTenantCheckpointSlogHandler tenant_handler;
  ObServerCheckpointSlogHandler server_handler;
  if (OB_FAIL(replayer.init(slog_dir, storage_env_.slog_file_spec_))) {
    LOG_WARN("fail to init slog replayer", K(ret));
  } else if (OB_FAIL(replayer.register_redo_module(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE, &tenant_handler))) {
    LOG_WARN("fail to register tenant handler module", K(ret));
  } else if (OB_FAIL(replayer.register_redo_module(ObRedoLogMainType::OB_REDO_LOG_SERVER_TENANT, &server_handler))) {
    LOG_WARN("fail to register server handler module", K(ret));
  } else if (OB_FAIL(replayer.parse_log(slog_dir, file_id, storage_env_.slog_file_spec_, stdout, offset_, parse_count_))) {
    LOG_WARN("fail to parse slog file", K(ret), K(slog_dir), K(file_id));
  }

  return ret;
}

int ObAdminSlogExecutor::concat_dir(const char *root_dir, const char *dir)
{
  int ret = OB_SUCCESS;
  MEMSET(slog_dir_, 0, common::MAX_PATH_SIZE);
  int pret = 0;
  pret = snprintf(slog_dir_, common::MAX_PATH_SIZE, "%s/%s", root_dir, dir);
  if (pret < 0 || pret >= common::MAX_PATH_SIZE) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("fail to construct tenant slog path", K(ret), KP(root_dir), KP(dir));
  }
  return ret;
}

int ObAdminSlogExecutor::parse_args(int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  int opt = 0;
  const char* opt_string = "hsd:u:f:o:c:";
  struct option longopts[] =
    {{"help", 0, NULL, 'h' },
     {"period_scan", 0, NULL, 's'},
     {"data_dir", 1, NULL, 'd' },
     {"tenant_id", 1, NULL, 'u' },
     {"log_file_id", 1, NULL, 'f' },
     {"offset", 1, NULL, 'o'},
     {"parse count", 1, NULL, 'c'}};

  while ((opt = getopt_long(argc, argv, opt_string, longopts, NULL)) != -1) {
    switch (opt) {
      case 'h': {
        print_usage();
        break;
      }
      case 's':
        // period_scan_ = true;
        break;
      case 'd':
        STRCPY(data_dir_, optarg);
        break;
      case 'u':
        tenant_id_ = static_cast<uint64_t>(strtol(optarg, NULL, 10));
        break;
      case 'f': {
        log_file_id_ = static_cast<int64_t>(strtol(optarg, NULL, 10));
        break;
      }
      case 'o':
        offset_ = static_cast<int64_t>(strtol(optarg, NULL, 10));
        break;
      case 'c':
        parse_count_ = static_cast<int64_t>(strtol(optarg, NULL, 10));
        break;
      default: {
        print_usage();
        ret = OB_INVALID_ARGUMENT;
      }
    }
  }
  LOG_INFO("finish parse_args", K(ret), K_(data_dir), K_(tenant_id), K_(log_file_id));
  return ret;
}

void ObAdminSlogExecutor::print_usage()
{
  fprintf(stderr, "\nUsage: ob_admin slog_tool -d data_dir -u tenant_id -f log_file_id\n"
                    "       -d --data_dir specify the data dir path, get data_dir from config if it is not sepcified\n"
                    "       -u --tenant_id specify the tenant whose slog is to be dumped, server tenant id is 500\n"
                    "       -f --log_file_id specify the slog file id which is to be dumped\n"
                    "       -o --offset specify the offset from which to start scanning / default value is 0, meaning reading from the begining of the file\n"
                    "       -c --parse_count specify the count of slogs needed to be read / default value is -1, meaning reading all slogs left\n"
                    "eg.    1.ob_admin slog_tool -d /home/fenggu.yh/ob1.obs0/store -u 1 -f 1 \n\n");
}

ObLogDirEntryOperation::~ObLogDirEntryOperation()
{
  reset();
}

void ObLogDirEntryOperation::reset()
{
  size_ = 0;
  for (int64_t i = 0; i < MAX_TENANT_NUM; i++) {
    MEMSET(d_names_[i], 0, MAX_D_NAME_LEN);
  }
}

int ObLogDirEntryOperation::func(const dirent *entry)
{
  STRNCPY(d_names_[size_], entry->d_name, MAX_D_NAME_LEN);
  size_++;
  return OB_SUCCESS;
}

}
}
