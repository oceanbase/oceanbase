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

#ifndef OB_AGGREGATED_STORAGE_META_IO_H_
#define OB_AGGREGATED_STORAGE_META_IO_H_

#include "lib/literals/ob_literals.h"
#include "share/cache/ob_kv_storecache.h"
#include "storage/meta_mem/ob_meta_obj_struct.h"
#include "storage/meta_mem/ob_storage_meta_cache.h"
#include "storage/blockstore/ob_shared_object_reader_writer.h"


namespace oceanbase
{
namespace storage
{

struct ObAggregatedStorageMetaInfo
{
  ObStorageMetaValue::MetaType meta_type_;
  ObStorageMetaKey meta_key_;
  ObStorageMetaValueHandle cache_handle_;
  TO_STRING_KV(K_(meta_type), K_(meta_key), K_(cache_handle));
};

class ObAggregatedStorageMetaIOInfo
{
public:
  ObAggregatedStorageMetaIOInfo();
  ObAggregatedStorageMetaIOInfo(const ObAggregatedStorageMetaIOInfo &aggr_infos);
  ~ObAggregatedStorageMetaIOInfo();
  int init(const common::ObIArray<AggregatedInfo> &aggr_infos);
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(aggr_params), K_(first_meta_offset), K_(total_meta_size), K_(macro_id), K_(disk_type));
  common::ObSEArray<ObAggregatedStorageMetaInfo, 4> aggr_params_;
  uint64_t first_meta_offset_;
  uint64_t total_meta_size_;
  blocksstable::MacroBlockId macro_id_;
  ObMetaDiskAddr::DiskType disk_type_;
};


class ObAggregatedStorageMetaIOCallback : public ObSharedObjectIOCallback
{
public:
  ObAggregatedStorageMetaIOCallback(
    common::ObIAllocator *io_allocator,
    const ObAggregatedStorageMetaIOInfo &aggr_io_info,
    const ObMetaDiskAddr &meta_addr);
  virtual ~ObAggregatedStorageMetaIOCallback();
  virtual int do_process(const char *buf, const int64_t buf_len) override;
  virtual int64_t size() const override { return sizeof(*this); }
  const char *get_cb_name() const override { return "AggregatedStorageMetaIOCB"; }
  bool is_valid() const;

  INHERIT_TO_STRING_KV("ObSharedObjectIOCallback", ObSharedObjectIOCallback,
      K_(aggr_io_info));

private:
  ObAggregatedStorageMetaIOInfo aggr_io_info_;
  DISALLOW_COPY_AND_ASSIGN(ObAggregatedStorageMetaIOCallback);
};

}
}

#endif