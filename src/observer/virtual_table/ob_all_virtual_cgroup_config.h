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

#ifndef SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_CGROUP_CONFIG_H_
#define SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_CGROUP_CONFIG_H_

#include "share/ob_virtual_table_scanner_iterator.h"

namespace oceanbase
{
namespace observer
{
class ObAllVirtualCgroupConfig : public common::ObVirtualTableScannerIterator
{
  enum COLUMN_ID_LIST
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    CFS_QUOTA_US,
    CFS_PERIOD_US,
    SHARES,
    CGROUP_PATH
  };

public:
  ObAllVirtualCgroupConfig();
  virtual ~ObAllVirtualCgroupConfig() override;
  virtual int inner_open() override;
  virtual void reset() override;
  virtual int inner_get_next_row(common::ObNewRow *&row) override;
private:
  static const int32_t PATH_BUFSIZE = 256;
  static const int32_t VALUE_BUFSIZE = 32;
  static constexpr const char *const root_cgroup_path = "/sys/fs/cgroup/cpu";
  static constexpr const char *const cgroup_link_path = "cgroup";
  bool is_inited_;
  char ip_buf_[common::OB_IP_STR_BUFF];
  char cgroup_path_buf_[PATH_BUFSIZE];
  char cgroup_origin_path_[PATH_BUFSIZE];
private:
  int check_cgroup_dir_exist_(const char *cgroup_path);
  int read_cgroup_path_dir_(const char *cgroup_path);
  int add_cgroup_config_info_(const char *cgroup_path);
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualCgroupConfig);
};

}
}
#endif /* SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_CGROUP_CONFIG_H_ */