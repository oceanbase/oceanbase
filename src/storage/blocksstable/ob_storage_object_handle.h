/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_OBJECT_HANDLE_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_OBJECT_HANDLE_H_

#include "share/io/ob_io_define.h"
#include "storage/blocksstable/ob_macro_block_id.h"

namespace oceanbase
{
namespace storage
{
  class ObBloomFilterBuildTask;
  class ObSSTabletPersister;
  class ObStorageIOPipelineTaskInfo;
}
namespace blocksstable
{
struct ObStorageObjectReadInfo;
struct ObStorageObjectWriteInfo;
class ObMacroBlockWriter;

class ObStorageObjectHandle final
{
  // for call set_macro_block_id
  friend class ObObjectManager;
  friend class ObBlockManager; // in ObBlockManager::alloc_object
  friend class storage::ObBloomFilterBuildTask; // in construct_func
  friend class blocksstable::ObMacroBlockWriter; // int ObMacroBlockWriter::alloc_block_from_device
  friend class storage::ObSSTabletPersister;
  friend class storage::ObStorageIOPipelineTaskInfo;
public:
  ObStorageObjectHandle() = default;
  ~ObStorageObjectHandle();
  ObStorageObjectHandle(const ObStorageObjectHandle &other);
  ObStorageObjectHandle &operator=(const ObStorageObjectHandle &other);
  void reset();
  void reuse();
  void reset_macro_id();
  bool is_valid() const { return io_handle_.is_valid(); }
  bool is_empty() const { return io_handle_.is_empty(); }
  OB_INLINE bool is_finished() const { return io_handle_.is_empty() || io_handle_.is_finished(); }
  const char *get_buffer() { return io_handle_.get_buffer(); }
  const MacroBlockId& get_macro_id() const { return macro_id_; }
  common::ObIOHandle &get_io_handle() { return io_handle_; }
  bool is_limit_net_bandwidth_req() const { return io_handle_.is_limit_net_bandwidth_req(); }
  int64_t get_data_size() const { return io_handle_.get_data_size(); }
  int64_t get_user_io_size() const { return io_handle_.get_user_io_size(); }
  int async_read(const ObStorageObjectReadInfo &read_info);
  int async_write(const ObStorageObjectWriteInfo &write_info);
  int wait();
  int get_io_ret() const;
  int get_io_time_us(int64_t &io_time_us) const;
  int check_is_finished(bool &is_finished);
  void cancel();
  TO_STRING_KV(K_(macro_id), K_(io_handle));

private:
  int set_macro_block_id(const MacroBlockId &macro_block_id);
  int report_bad_block() const;
  static uint64_t get_tenant_id();
  int sn_async_read(const ObStorageObjectReadInfo &read_info);
  int sn_async_write(const ObStorageObjectWriteInfo &write_info);
  void print_slow_io_info(int ret_code) const;

private:
  MacroBlockId macro_id_;
  common::ObIOHandle io_handle_;
};

} // namespace blocksstable
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_OB_OBJECT_HANDLE_H_
