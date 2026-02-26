/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OBDEV_SRC_SHARE_EXTERNAL_TABLE_OB_HDFS_TABLE_DEVICE_H
#define OBDEV_SRC_SHARE_EXTERNAL_TABLE_OB_HDFS_TABLE_DEVICE_H

#include "lib/restore/ob_object_device.h"
#include "share/external_table/ob_hdfs_storage_info.h"


namespace oceanbase
{
namespace share
{
class ObHDFSTableDevice final : public common::ObObjectDevice
{
public:
  ObHDFSTableDevice();
  ~ObHDFSTableDevice();

  virtual void destroy() override;
  virtual int setup_storage_info(const ObIODOpts &opts) override;
  virtual common::ObObjectStorageInfo &get_storage_info() { return external_storage_info_; }

private:
  ObHDFSStorageInfo external_storage_info_;
  DISALLOW_COPY_AND_ASSIGN(ObHDFSTableDevice);
};

} // namespace share
} // namespace oceanbase
# endif /* OBDEV_SRC_SHARE_EXTERNAL_TABLE_OB_HDFS_TABLE_DEVICE_H */