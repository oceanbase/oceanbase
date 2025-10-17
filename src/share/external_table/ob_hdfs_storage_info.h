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

#ifndef OBDEV_SRC_SHARE_EXTERNAL_TABLE_OB_HDFS_STORAGE_INFO_H
#define OBDEV_SRC_SHARE_EXTERNAL_TABLE_OB_HDFS_STORAGE_INFO_H

#include "lib/restore/ob_storage_info.h"

namespace oceanbase
{
namespace share
{
// The limit on the maximum length of single config in hdfs is 128
static constexpr int64_t OB_MAX_HDFS_SINGLE_CONF_LENGTH = 128;
// The limit on the maximum length of other configs in hdfs is 1024
static constexpr int64_t OB_MAX_HDFS_CONFS_LENGTH = 1024;

const int64_t OB_MAX_HDFS_BACKUP_EXTENSION_LENGTH = 1536;

const char *const KRB5CONF = "krb5conf=";
const char *const PRINCIPAL = "principal=";
const char *const KEYTAB = "keytab=";
const char *const TICKET_CACHE_PATH = "ticiket_cache_path=";
const char *const HADOOP_USERNAME = "username=";
const char *const HDFS_CONFIGS = "configs=";

class ObHDFSStorageInfo : public common::ObObjectStorageInfo
{

public:
  ObHDFSStorageInfo() {
    hdfs_extension_[0] = '\0';
  }
  virtual ~ObHDFSStorageInfo();

  bool operator ==(const ObHDFSStorageInfo &external_storage_info) const;
  bool operator !=(const ObHDFSStorageInfo &external_storage_info) const;

  // Override methods
  virtual int assign(const ObObjectStorageInfo &storage_info) override;
  virtual int clone(
      common::ObIAllocator &allocator,
      common::ObObjectStorageInfo *&storage_info) const override;
  virtual void reset() override;
  // virtual int get_storage_info_str(char *storage_info, const int64_t info_len) const override;

  int64_t hash() const;
  virtual int validate_arguments() const override;
  // the following two functions are designed for ObDeviceManager, which manages all devices by a device_map_
  virtual int64_t get_device_map_key_len() const override;
  virtual int get_device_map_key_str(char *key_str, const int64_t len) const override;

private:
  virtual int parse_storage_info_(const char *storage_info, bool &has_needed_extension) override;
  virtual int get_info_str_(char *storage_info, const int64_t info_len) const override;
  virtual int append_extension_str_(char *storage_info, const int64_t info_len) const override;

public:
  char hdfs_extension_[OB_MAX_HDFS_BACKUP_EXTENSION_LENGTH];
};
} // namespace sql
} // namespace oceanbase

#endif /* OBDEV_SRC_SHARE_EXTERNAL_TABLE_OB_HDFS_STORAGE_INFO_H */