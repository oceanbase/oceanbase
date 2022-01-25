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
#include "storage/blocksstable/slog/ob_base_storage_logger.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_table_mgr.h"
#include "storage/ob_tenant_config_mgr.h"
#include "storage/ob_tenant_file_mgr.h"
#include "../dumpsst/ob_admin_dumpsst_utils.h"

using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;

namespace oceanbase
{
namespace tools
{

ObAdminSlogExecutor::ObAdminSlogExecutor()
  : log_dir_(NULL),
    log_file_id_(0)
{
}

int ObAdminSlogExecutor::execute(int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  reset();
  ObPartitionMetaRedoModule partition_module;
  ObBaseFileMgr file_mgr;

  if (OB_FAIL(parse_cmd(argc - 1, argv + 1))) {
    LOG_WARN("fail to parse cmd", K(ret));
  } else {
    if (NULL != log_dir_ && log_file_id_ > 0) {
      if (OB_FAIL(SLOGGER.register_redo_module(OB_REDO_LOG_PARTITION, &partition_module))) {
        LOG_WARN("fail to register partition module", K(ret));
      } else if (OB_FAIL(SLOGGER.register_redo_module(OB_REDO_LOG_TABLE_MGR,
                                                      &ObTableMgr::get_instance()))) {
        LOG_WARN("fail to register table module", K(ret));
      } else if (OB_FAIL(SLOGGER.register_redo_module(OB_REDO_LOG_TENANT_CONFIG,
                                                      &ObTenantConfigMgr::get_instance()))) {
        LOG_WARN("fail to register tenant config module", K(ret));
      } else if (OB_FAIL(SLOGGER.register_redo_module(OB_REDO_LOG_TENANT_FILE, &file_mgr))) {
        LOG_WARN("fail to register tenant file module", K(ret));
      } else if (OB_FAIL(SLOGGER.parse_log(log_dir_, log_file_id_, stdout))) {
        LOG_WARN("fail to parse log file", K(ret));
      }
    }
  }

  return ret;
}

void ObAdminSlogExecutor::reset()
{
  log_dir_ = NULL;
  log_file_id_ = 0;
}

int ObAdminSlogExecutor::parse_cmd(int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  int opt = 0;
  const char* opt_string = "hf:";
  struct option longopts[] =
    {{"help", 0, NULL, 'h' },
     {"log_file_path", 1, NULL, 'f' }};

  while ((opt = getopt_long(argc, argv, opt_string, longopts, NULL)) != -1) {
    switch (opt) {
      case 'h': {
        print_usage();
        break;
      }
      case 'f': {
        //find the last separator from the right of log_file_pathַ
        char *p = strrchr(optarg, '/');
        if (NULL == p) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("log_file_path is not correct, ", K(ret), K(optarg));
        } else {
          //separate the log_dir and log_name by '\0'
          *p = '\0';
          log_dir_ = optarg;
          log_file_id_ = atoi(p + 1);
        }
        break;
      }
      default: {
        print_usage();
        ret = OB_INVALID_ARGUMENT;
      }
    }
  }
  return ret;
}

void ObAdminSlogExecutor::print_usage()
{
  fprintf(stderr, "\nUsage: ob_admin slog_tool -f log_file_name\n");
}

}
}
