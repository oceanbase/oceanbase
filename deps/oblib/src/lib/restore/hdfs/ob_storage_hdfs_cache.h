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
#ifndef SRC_LIBRARY_SRC_LIB_RESTORE_OB_STORAGE_HDFS_CACHE_H_
#define SRC_LIBRARY_SRC_LIB_RESTORE_OB_STORAGE_HDFS_CACHE_H_

#include <hdfs/hdfs.h>
#include <memory>

#include "sql/engine/connector/ob_java_helper.h"

#include "lib/string/ob_string.h"
#include "lib/lock/ob_mutex.h"
#include "lib/restore/ob_i_storage.h"

namespace oceanbase
{
namespace common
{

class ObHdfsFsClient;

// max allowed idle duration for a hdfs client: 10min
static constexpr int64_t MAX_HDFS_CLIENT_IDLE_DURATION_US = 10LL * 60LL * 1000LL * 1000LL;
static constexpr int64_t STOP_HDFS_TIMEOUT_US = 10 * 1000L;   // 10ms
static constexpr char OB_STORAGE_HDFS_ALLOCATOR[] = "StorageHDFS";
static constexpr int MAX_HDFS_CLIENT_NUM = 97;

typedef lib::ObLockGuard<lib::ObMutex> LockGuard;

class ObHdfsFsClient
{
public:
  ObHdfsFsClient();
  virtual ~ObHdfsFsClient();
  int init();
  bool try_stop(const int64_t timeout = STOP_HDFS_TIMEOUT_US);
  void increase();
  void release();
  bool is_stopped() const;
  bool is_valid_client() const { return nullptr != hdfs_fs_; }
  void set_hdfs_fs(hdfsFS hdfs_fs) { hdfs_fs_ = hdfs_fs; }
  hdfsFS get_hdfs_fs() { return hdfs_fs_; }

private:
  hdfsFS hdfs_fs_;
  SpinRWLock lock_;
  bool is_inited_;
  bool stopped_;
  int64_t ref_cnt_;
  int64_t last_modified_ts_;

  DISALLOW_COPY_AND_ASSIGN(ObHdfsFsClient);
};

class ObHdfsCacheUtils
{
public:
  static int
  create_hdfs_fs_handle(const ObString &namenode,
                        ObHdfsFsClient *hdfs_client,
                        ObObjectStorageInfo *storage_info);

  static int get_namenode_and_path_from_uri(char *namenode,
                                            const int64_t namenode_len,
                                            char *path, const int64_t path_len,
                                            const ObString &uri);

private:
  static int parse_hdfs_auth_info_(ObObjectStorageInfo *storage_info,
                                   char *krb5conf_path, const int64_t conf_len,
                                   char *principal, const int64_t principal_len,
                                   char *keytab_path, const int64_t keytab_len,
                                   char *ticiket_path, const int64_t ticket_len,
                                   char *configs, const int64_t configs_len);

  static int check_kerberized_(const char *krb5conf_path, const char *principal,
                               const char *keytab_path, const char *ticiket_path,
                               const char *configs, bool &is_kerberized,
                               bool &using_ticket_cache, bool &using_keytab,
                               bool &using_principal);

  static int create_fs_(ObHdfsFsClient *hdfs_client,
                        const ObString &namenode,
                        ObObjectStorageInfo *storage_info);

private:
  DISALLOW_COPY_AND_ASSIGN(ObHdfsCacheUtils);
};

// Cache for HDFS file system
class ObHdfsFsCache
{
public:
  ~ObHdfsFsCache() = default;
  static ObHdfsFsCache *instance()
  {
    static ObHdfsFsCache s_instance;
    return &s_instance;
  }

  // This function is thread-safe
  int get_connection(const ObString &namenode,
                     ObHdfsFsClient *&hdfs_client,
                     ObObjectStorageInfo *storage_info);
private:
  int clean_hdfs_client_map_();

private:
  SpinRWLock lock_;
  hash::ObHashMap<int64_t, ObHdfsFsClient *> hdfs_client_map_;
  bool is_inited_map_;

private:
  ObHdfsFsCache();
  ObHdfsFsCache(const ObHdfsFsCache &) = delete;
  const ObHdfsFsCache &operator=(const ObHdfsFsCache &) = delete;
};

} // namespace common
} // namespace oceanbase

#endif /* SRC_LIBRARY_SRC_LIB_RESTORE_OB_STORAGE_HDFS_CACHE_H_ */