/**
 * Copyright (c) 2021 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PubL v2. You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
 * Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SHARE_DEVICE_OB_DEVICE_CONNECTIVITY_H_
#define OCEANBASE_SHARE_DEVICE_OB_DEVICE_CONNECTIVITY_H_

#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "common/storage/ob_device_common.h"

namespace oceanbase
{
namespace common
{
  class ObIODirentEntry;
}
namespace share
{
class ObBackupDest;

class ObDeviceConnectivityCheckManager final
{
public:
  ObDeviceConnectivityCheckManager() {}
  ~ObDeviceConnectivityCheckManager() {}
  int check_device_connectivity(const share::ObBackupDest &storage_dest);
private:
  DISALLOW_COPY_AND_ASSIGN(ObDeviceConnectivityCheckManager);
};

class ObDeviceCheckFile final
{
public:
  ObDeviceCheckFile() {}
  ~ObDeviceCheckFile() {}
  int delete_permission_check_file(const share::ObBackupDest &storage_dest);
  int check_io_permission(const share::ObBackupDest &storage_dest);

private:
  int get_check_file_path_(const share::ObBackupDest &storage_dest, char *path);
  int get_permission_check_file_path_(const share::ObBackupDest &storage_dest,
                                      const bool is_appender,
                                      char *path);
  int check_appender_permission_(const share::ObBackupDest &storage_dest);
  bool is_permission_error_(const int result);
  // Convert time string, return like '2023-04-28T12:00:00' if concat is 'T'.
  int storage_time_to_strftime_(const int64_t &ts_s,
                                char *buf,
                                const int64_t buf_len,
                                int64_t &pos,
                                const char concat);

private:
  static const char OB_SS_SUFFIX[];
  static constexpr int64_t OB_MAX_DEVICE_CHECK_FILE_NAME_LENGTH = 256;
  static constexpr int64_t OB_STORAGE_MAX_TIME_STR_LEN = 50; // time string max length
private:
  DISALLOW_COPY_AND_ASSIGN(ObDeviceCheckFile);
};

class ObDirPrefixEntryFilter : public ObBaseDirEntryOperator
{
public:
  ObDirPrefixEntryFilter(common::ObIArray<common::ObIODirentEntry> &d_entrys)
      : is_inited_(false), d_entrys_(d_entrys)
  {
    filter_str_[0] = '\0';
  }
  virtual ~ObDirPrefixEntryFilter() = default;
  int init(const char *filter_str, const int32_t filter_str_len);
  virtual int func(const dirent *entry) override;
private:
  bool is_inited_;
  char filter_str_[common::MAX_PATH_SIZE];
  common::ObIArray<common::ObIODirentEntry> &d_entrys_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObDirPrefixEntryFilter);
};

}  // namespace share
}  // namespace oceanbase

#endif  // OCEANBASE_SHARE_DEVICE_OB_DEVICE_CONNECTIVITY_H_
