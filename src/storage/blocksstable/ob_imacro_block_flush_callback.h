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

#ifndef OB_STORAGE_BLOCKSSTABLE_OB_IMACRO_BLOCK_FLUSH_CALLBACK_H_
#define OB_STORAGE_BLOCKSSTABLE_OB_IMACRO_BLOCK_FLUSH_CALLBACK_H_

namespace oceanbase
{

namespace blocksstable
{
class ObMacroBlockHandle;
class ObMacroBlocksWriteCtx;
struct ObLogicMacroBlockId;

class ObIMacroBlockFlushCallback
{
public:
  ObIMacroBlockFlushCallback() {}
  virtual ~ObIMacroBlockFlushCallback() {}
  virtual int write(const ObMacroBlockHandle &macro_handle,
                    const ObLogicMacroBlockId &logic_id,
                    char *buf,
                    const int64_t buf_len,
                    const int64_t data_seq) = 0;
  virtual int wait() = 0;
};

}  // end namespace blocksstable
}  // end namespace oceanbase

#endif //OB_STORAGE_BLOCKSSTABLE_OB_IMACRO_BLOCK_FLUSH_CALLBACK_H_
