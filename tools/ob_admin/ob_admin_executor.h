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

#ifndef OB_ADMIN_EXECUTOR_H_
#define OB_ADMIN_EXECUTOR_H_
#include <stdlib.h>
#include <stdio.h>
#include <getopt.h>
#include "share/ob_define.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "share/config/ob_config_manager.h"
#include "observer/ob_server_reload_config.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{

namespace common
{

class ObIODevice;

}
namespace tools
{
class ObAdminExecutor
{
public:
  ObAdminExecutor();
  virtual ~ObAdminExecutor();
  virtual int execute(int argc, char *argv[]) = 0;

protected:
  ObIODevice *get_device_inner();
  int prepare_io();
  int init_slogger_mgr();
  int load_config();

protected:
  share::ObTenantBase mock_server_tenant_;
  blocksstable::ObStorageEnv storage_env_;
  observer::ObServerReloadConfig reload_config_;
  common::ObConfigManager config_mgr_;
  char data_dir_[common::OB_MAX_FILE_NAME_LENGTH] = {0};
  char slog_dir_[common::OB_MAX_FILE_NAME_LENGTH] = {0};
  char clog_dir_[common::OB_MAX_FILE_NAME_LENGTH] = {0};
  char sstable_dir_[common::OB_MAX_FILE_NAME_LENGTH] = {0};
};

}
}

#endif /* OB_ADMIN_EXECUTOR_H_ */
