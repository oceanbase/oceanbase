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

#ifndef _OB_TENANT_CONFIG_META_CHECKPOINT_WRITER_H
#define _OB_TENANT_CONFIG_META_CHECKPOINT_WRITER_H

#include "storage/ob_pg_meta_block_writer.h"

namespace oceanbase {
namespace storage {

class ObTenantConfigMetaItem : public ObIPGMetaItem {
public:
  ObTenantConfigMetaItem();
  virtual ~ObTenantConfigMetaItem() = default;
  virtual int serialize(const char*& buf, int64_t& buf_len) override;
  virtual int16_t get_item_type() const override
  {
    return TENANT_CONFIG_META;
  }
  bool is_valid() const
  {
    return nullptr != tenant_units_;
  }
  void set_tenant_units(share::TenantUnits& tenant_units);

private:
  share::TenantUnits* tenant_units_;
  common::ObArenaAllocator allocator_;
};

class ObTenantConfigMetaCheckpointWriter final {
public:
  ObTenantConfigMetaCheckpointWriter();
  ~ObTenantConfigMetaCheckpointWriter() = default;
  int write_checkpoint(blocksstable::ObSuperBlockMetaEntry& meta_entry);
  void reset();
  common::ObIArray<blocksstable::MacroBlockId>& get_meta_block_list();

private:
  ObPGMetaItemWriter writer_;
};

}  // namespace storage
}  // namespace oceanbase

#endif  // _OB_TENANT_CONFIG_META_CHECKPOINT_WRITER_H
