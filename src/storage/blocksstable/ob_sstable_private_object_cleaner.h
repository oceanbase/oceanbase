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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_SSTABLE_PRIVATE_OBJECT_CLEANER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_SSTABLE_PRIVATE_OBJECT_CLEANER_H_

#include "lib/lock/ob_spin_lock.h"
#include "lib/container/ob_se_array.h"
#include "storage/blocksstable/ob_macro_block_id.h"

namespace oceanbase
{
namespace blocksstable
{

class ObDataStoreDesc;

class ObSSTablePrivateObjectCleaner final
{
private:
  static const int64_t CLEANER_MACRO_ID_LOCAL_ARRAY_SIZE = 32;

public:
  ObSSTablePrivateObjectCleaner();
  ~ObSSTablePrivateObjectCleaner();
  void reset();
  int add_new_macro_block_id(const MacroBlockId &macro_id);
  int mark_succeed();
  static int get_cleaner_from_data_store_desc(ObDataStoreDesc &data_store_desc, ObSSTablePrivateObjectCleaner *&cleaner);
  TO_STRING_KV(K_(new_macro_block_ids), K_(is_ss_mode), K_(task_succeed));

private:
  void clean();
  DISALLOW_COPY_AND_ASSIGN(ObSSTablePrivateObjectCleaner);

private:
  common::ObSEArray<MacroBlockId, CLEANER_MACRO_ID_LOCAL_ARRAY_SIZE> new_macro_block_ids_;
  SpinRWLock lock_;
  bool is_ss_mode_;
  bool task_succeed_;
};

} // namespace blocksstable
} // namespace oceanbase

#endif