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

#ifndef SQL_ENGIENE_TABLE_OB_PCACHED_EXTERNAL_FILE_SERVICE_H_
#define SQL_ENGIENE_TABLE_OB_PCACHED_EXTERNAL_FILE_SERVICE_H_

#include "lib/hash/ob_hashmap.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "ob_external_data_access_mgr.h"
#include "lib/container/ob_se_array.h"

namespace oceanbase
{
namespace sql
{

class ObExternalFilePathMap final
{
private:
  // do not hold url memory
  struct PathMapKey
  {
    PathMapKey() : url_(), modify_time_(-1) {}
    PathMapKey(const common::ObString &url, const int64_t modify_time)
        : url_(url), modify_time_(modify_time)
    {}

    void reset()
    {
      url_.reset();
      modify_time_ = -1;
    }

    bool is_valid() const
    {
      return !url_.empty() && modify_time_ >= 0;
    }

    bool operator==(const PathMapKey &other) const
    {
      return modify_time_ == other.modify_time_ && url_ == other.url_;
    }

    int hash(uint64_t &hash_val) const
    {
      hash_val = 0;
      hash_val = url_.hash();
      hash_val = common::murmurhash(&modify_time_, sizeof(modify_time_), hash_val);
      return OB_SUCCESS;
    }

    TO_STRING_KV(K(url_), K(modify_time_));

    common::ObString url_;
    int64_t modify_time_;
  };

  struct MacroMapKey
  {
    MacroMapKey() : server_seq_id_(UINT64_MAX), offset_idx_(-1) {}
    MacroMapKey(const uint64_t server_seq_id, const int64_t offset_idx)
        : server_seq_id_(server_seq_id), offset_idx_(offset_idx)
    {}

    bool is_valid() const
    {
      return server_seq_id_ < UINT64_MAX && offset_idx_ >= 0;
    }

    bool operator==(const MacroMapKey &other) const
    {
      return server_seq_id_ == other.server_seq_id_ && offset_idx_ == other.offset_idx_;
    }

    int hash(uint64_t &hash_val) const
    {
      hash_val = 0;
      hash_val = common::murmurhash(&server_seq_id_, sizeof(server_seq_id_), hash_val);
      hash_val = common::murmurhash(&offset_idx_, sizeof(offset_idx_), hash_val);
      return OB_SUCCESS;
    }

    TO_STRING_KV(K(server_seq_id_), K(offset_idx_));

    uint64_t server_seq_id_;
    int64_t offset_idx_;
  };

  //if key already exist, execute update_callback.
  template <typename KeyType_, typename ValueType_>
  class GetExistingValueUpdateCallback
  {
  public:
    GetExistingValueUpdateCallback(ValueType_ &value)
        : is_exist_(false), value_(value)
    {}

    void operator()(const common::hash::HashMapPair<KeyType_, ValueType_> &entry)
    {
      is_exist_ = true;
      value_ = entry.second;
    };

    bool is_exist() const { return is_exist_; }

  private:
    bool is_exist_;
    ValueType_ &value_;
  };

  template <typename KeyType_, typename ValueType_>
  class EmptySetCallback
  {
  public:
    EmptySetCallback() {}

    int operator()(const common::hash::HashMapPair<KeyType_, ValueType_> &entry)
    {
      return OB_SUCCESS;
    };
  };

  class CollectDeletionKeyCallback
  {
  public:
    using DeletionKeys = common::ObSEArray<MacroMapKey, 128>;

  public:
    CollectDeletionKeyCallback(const uint64_t server_seq_id, DeletionKeys &deletion_keys)
        : server_seq_id_(server_seq_id), deletion_keys_(deletion_keys)
    {}

    int operator()(const common::hash::HashMapPair<MacroMapKey, blocksstable::MacroBlockId> &entry)
    {
      int ret = OB_SUCCESS;
      if (server_seq_id_ == entry.first.server_seq_id_) {
        if (OB_FAIL(deletion_keys_.push_back(entry.first))) {
          OB_LOG(WARN, "fail to push back deletion key", KR(ret), K(entry.first));
        }
      }
      return ret;
    };

  private:
    const uint64_t server_seq_id_;
    DeletionKeys &deletion_keys_;
  };

public:
  ObExternalFilePathMap();
  ~ObExternalFilePathMap();

  int init(const uint64_t tenant_id);
  void destroy();

  int get_or_generate(
      const common::ObString &url,
      const int64_t modify_time,
      const int64_t offset,
      blocksstable::MacroBlockId &macro_id);

private:
  int generate_server_seq_id_(uint64_t &server_seq_id) const;
  int generate_macro_id_(
      const MacroMapKey &macro_map_key,
      blocksstable::MacroBlockId &macro_id);
  int get_or_generate_server_seq_id_(
      const PathMapKey &path_map_key,
      uint64_t &server_seq_id);
  int get_or_generate_macro_id_(
      const MacroMapKey &macro_map_key,
      blocksstable::MacroBlockId &macro_id);

private:
  static const int64_t N_WAY = 16;
  static const int64_t DEFAULT_BLOCK_SIZE = 128 * 1024LL;

  bool is_inited_;
  ObBlockAllocMgr mem_limiter_;
  ObVSliceAlloc allocator_;
  // path url + modify_time -> path id
  hash::ObHashMap<PathMapKey, uint64_t> path_id_map_;
  // path id + offset_idx -> macro block id
  hash::ObHashMap<MacroMapKey, blocksstable::MacroBlockId> macro_id_map_;
};

class ObPCachedExternalFileService
{
public:
  ObPCachedExternalFileService();
  virtual ~ObPCachedExternalFileService();

  int init(const uint64_t tenant_id);
  static int mtl_init(ObPCachedExternalFileService *&accesser);
  int start();
  void stop();
  void wait();
  void destroy();

  int async_read(
     const ObExternalAccessFileInfo &external_file_info,
     ObExternalReadInfo &external_read_info,
     blocksstable::ObStorageObjectHandle &io_handle);

  int add_prefetch_task(
      const char *url,
      const ObObjectStorageInfo *access_info,
      const int64_t offset_idx);

  int get_io_callback_allocator(common::ObIAllocator *&allocator);

private:
  static const int64_t IO_CALLBACK_MEM_LIMIT = 128 * 1024LL * 1024; // 128MB

  bool is_inited_;
  bool is_stopped_;
  uint64_t tenant_id_;
  common::ObConcurrentFIFOAllocator io_callback_allocator_;
  ObExternalFilePathMap path_map_;
};

class ObExternalRemoteIOCallback : public common::ObIOCallback
{
public:
  ObExternalRemoteIOCallback();
  virtual ~ObExternalRemoteIOCallback();

  static int construct_io_callback(
      const blocksstable::ObStorageObjectReadInfo *read_info,
      common::ObIOInfo &io_info);
  static void free_io_callback_and_detach_original(common::ObIOCallback *&io_callback);

  int init(
      common::ObIAllocator *allocator,
      common::ObIOCallback *original_callback,
      char *user_data_buf,
      const int64_t offset_idx,
      const char *url,
      const ObObjectStorageInfo *access_info);

  virtual ObIAllocator *get_allocator() override { return allocator_; }
  virtual const char *get_data() override
  {
    return OB_NOT_NULL(ori_callback_) ? ori_callback_->get_data() : user_data_buf_;
  }
  virtual int64_t size() const override
  {
    return sizeof(ObExternalRemoteIOCallback);
  }
  virtual int alloc_data_buf(const char *io_data_buffer, const int64_t data_size) override
  {
    UNUSED(io_data_buffer);
    UNUSED(data_size);
    return OB_NOT_SUPPORTED;
  }
  virtual int inner_process(const char *data_buffer, const int64_t size) override;
  virtual const char *get_cb_name() const override
  {
    return "ObExternalRemoteIOCallback";
  }

  ObIOCallback *clear_original_io_callback()
  {
    ObIOCallback *original_io_callback = ori_callback_;
    ori_callback_ = nullptr;
    return original_io_callback;
  }

  VIRTUAL_TO_STRING_KV(K(is_inited_), KP(allocator_), KPC(ori_callback_),
      KP(user_data_buf_), K(offset_idx_), K(url_), KPC(access_info_));

private:
  bool is_inited_;
  common::ObIAllocator *allocator_;
  common::ObIOCallback *ori_callback_;
  char *user_data_buf_;
  int64_t offset_idx_;
  const char *url_;
  const ObObjectStorageInfo *access_info_;
};

} // sql
} // oceanbase

#endif // SQL_ENGIENE_TABLE_OB_PCACHED_EXTERNAL_FILE_SERVICE_H_