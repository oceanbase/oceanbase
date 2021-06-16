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

#ifndef OCEANBASE_STORAGE_OB_INTERM_MACRO_MGR_H_
#define OCEANBASE_STORAGE_OB_INTERM_MACRO_MGR_H_

#include "ob_resource_map.h"
#include "blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/ob_macro_block_struct.h"
#include "storage/blocksstable/ob_store_file_system.h"

namespace oceanbase {
namespace storage {

class ObIntermMacroMgr;

struct ObIntermMacroKey {
  OB_UNIS_VERSION(1);

public:
  ObIntermMacroKey();
  virtual ~ObIntermMacroKey();
  inline int64_t hash() const;
  bool operator==(const ObIntermMacroKey& other) const;
  TO_STRING_KV(K_(execution_id), K_(task_id));
  uint64_t execution_id_;
  uint64_t task_id_;
};

class ObIntermMacroValue {
public:
  ObIntermMacroValue();
  virtual ~ObIntermMacroValue();
  int set_macro_block(blocksstable::ObMacroBlocksWriteCtx& macro_block_ctx);
  void reset();
  blocksstable::ObMacroBlocksWriteCtx& get_macro_blocks()
  {
    return macro_block_write_ctx_;
  }
  int64_t get_deep_copy_size() const
  {
    return sizeof(*this);
  }
  // after deep copy, macro_block_ctx_ will be cleared
  int deep_copy(char* buf, const int64_t buf_len, ObIntermMacroValue*& value);
  TO_STRING_KV(K_(macro_block_write_ctx));

private:
  blocksstable::ObMacroBlocksWriteCtx macro_block_write_ctx_;
};

class ObIntermMacroHandle : public ObResourceHandle<ObIntermMacroValue> {
  friend class ObIntermMacroMgr;

public:
  ObIntermMacroHandle();
  virtual ~ObIntermMacroHandle();
  void reset();

private:
  DISALLOW_COPY_AND_ASSIGN(ObIntermMacroHandle);
};

class ObIntermMacroMgr {
public:
  static ObIntermMacroMgr& get_instance();
  int init();
  int put(const ObIntermMacroKey& key, blocksstable::ObMacroBlocksWriteCtx& macro_block_ctx);
  int get(const ObIntermMacroKey& key, ObIntermMacroHandle& handle);
  int remove(const ObIntermMacroKey& key);
  void destroy();
  int dec_handle_ref(ObIntermMacroHandle& handle);

private:
  ObIntermMacroMgr();
  virtual ~ObIntermMacroMgr();

private:
  static const int64_t DEFAULT_BUCKET_NUM = 1543L;
  static const int64_t TOTAL_LIMIT = 1 * 1024L * 1024L * 1024L;
  static const int64_t HOLD_LIMIT = 8 * 1024L * 1024L;
  static const int64_t PAGE_SIZE = common::OB_MALLOC_BIG_BLOCK_SIZE;
  ObResourceMap<ObIntermMacroKey, ObIntermMacroValue> map_;
  bool is_inited_;
};

}  // end namespace storage
}  // end namespace oceanbase

#define INTERM_MACRO_MGR (::oceanbase::storage::ObIntermMacroMgr::get_instance())

#endif  // OCEANBASE_STORAGE_OB_INTERM_MACRO_MGR_H_
