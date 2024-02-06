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

#ifndef _OB_EXTERNAL_TABLE_FILE_MANAGER_H_
#define _OB_EXTERNAL_TABLE_FILE_MANAGER_H_

#include "share/ob_rpc_struct.h"
#include "observer/ob_server_struct.h"

namespace oceanbase {

namespace share {

struct ObExternalFileInfo {
  ObExternalFileInfo() : file_id_(INT64_MAX), file_size_(0) {}
  common::ObString file_url_;
  int64_t file_id_;
  common::ObAddr file_addr_;
  int64_t file_size_;
  TO_STRING_KV(K_(file_url), K_(file_id), K_(file_addr), K_(file_size));
  OB_UNIS_VERSION(1);
};

class ObExternalTableFilesKey : public ObIKVCacheKey
{
public:
  ObExternalTableFilesKey() : tenant_id_(OB_INVALID_ID),
    table_id_(OB_INVALID_ID),
    partition_id_(OB_INVALID_ID)
  {}
  virtual ~ObExternalTableFilesKey() {}
  bool operator ==(const ObIKVCacheKey &other) const override {
    const ObExternalTableFilesKey &other_key = reinterpret_cast<const ObExternalTableFilesKey&>(other);
    return this->tenant_id_ == other_key.tenant_id_
        && this->table_id_ == other_key.table_id_
        && this->partition_id_ == other_key.partition_id_;
  }
  uint64_t hash() const override {
    return common::murmurhash(this, sizeof(ObExternalTableFilesKey), 0);
  }
  uint64_t get_tenant_id() const override { return tenant_id_; }
  int64_t size() const override { return sizeof(*this); }
  int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const override;
  TO_STRING_KV(K(tenant_id_), K(table_id_), K(partition_id_));
public:
  uint64_t tenant_id_;
  uint64_t table_id_;
  uint64_t partition_id_;
};

class ObExternalTableFiles : public ObIKVCacheValue
{
public:
  ObExternalTableFiles() : create_ts_(0) {}
  virtual ~ObExternalTableFiles() {}
  int64_t size() const override;
  int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const override;
  TO_STRING_KV(K(file_urls_), K(file_ids_), K(create_ts_));
public:
  ObArrayWrap<ObString> file_urls_;
  ObArrayWrap<int64_t> file_ids_;
  ObArrayWrap<int64_t> file_sizes_;
  int64_t create_ts_;
};

class ObExternalTableFileManager
{
public:
  static const int64_t CACHE_EXPIRE_TIME = 20 * 1000000L; //20s
  static const int64_t MAX_VERSION = INT64_MAX;
  static const int64_t LOAD_CACHE_LOCK_CNT = 16;
  static const int64_t LOCK_TIMEOUT = 2 * 1000000L;

  ObExternalTableFileManager() {}

  int init();

  static ObExternalTableFileManager &get_instance();

  int get_external_files(
      const uint64_t tenant_id,
      const uint64_t table_id,
      const bool is_local_file_on_disk,
      common::ObIAllocator &allocator,
      common::ObIArray<ObExternalFileInfo> &external_files,
      common::ObIArray<ObNewRange *> *range_filter = NULL);

  int get_external_files_by_part_id(
      const uint64_t tenant_id,
      const uint64_t table_id,
      const uint64_t partition_id,
      const bool is_local_file_on_disk,
      common::ObIAllocator &allocator,
      common::ObIArray<ObExternalFileInfo> &external_files,
      common::ObIArray<ObNewRange *> *range_filter = NULL);

  int flush_cache(
      const uint64_t tenant_id,
      const uint64_t table_id);

  int update_inner_table_file_list(
      const uint64_t tenant_id,
      const uint64_t table_id,
      common::ObIArray<common::ObString> &file_urls,
      common::ObIArray<int64_t> &file_sizes);

  int get_all_records_from_inner_table(ObIAllocator &allocator,
                                    int64_t tenant_id,
                                    int64_t table_id,
                                    int64_t partition_id,
                                    ObIArray<ObString> &file_urls,
                                    ObIArray<int64_t> &file_ids);
  int clear_inner_table_files(
      const uint64_t tenant_id,
      const uint64_t table_id,
      ObMySQLTransaction &trans);

  int get_external_file_list_on_device(const ObString &location,
                            ObIArray<ObString> &file_urls,
                            ObIArray<int64_t> &file_sizes,
                            const ObString &access_info,
                            ObIAllocator &allocator);

private:

  int update_inner_table_files_list_one_part(
      const uint64_t tenant_id,
      const uint64_t table_id,
      const uint64_t partition_id,
      ObMySQLTransaction &trans,
      common::ObIArray<common::ObString> &file_urls,
      common::ObIArray<int64_t> &file_sizes);

  bool is_cache_value_timeout(const ObExternalTableFiles &ext_files) {
    return ObTimeUtil::current_time() - ext_files.create_ts_ > CACHE_EXPIRE_TIME;
  }
  int fill_cache_from_inner_table(
      const ObExternalTableFilesKey &key,
      const ObExternalTableFiles *&ext_files,
      ObKVCacheHandle &handle);
  int lock_for_refresh(
      ObMySQLTransaction &trans,
      const uint64_t tenant_id,
      const uint64_t object_id);
private:
  common::ObSpinLock fill_cache_locks_[LOAD_CACHE_LOCK_CNT];
  common::ObKVCache<ObExternalTableFilesKey, ObExternalTableFiles> kv_cache_;
};


}
}

#endif /* _OB_EXTERNAL_TABLE_FILE_MANAGER_H_ */
