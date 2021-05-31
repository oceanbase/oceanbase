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

#include "ob_macro_meta_block_reader.h"
#include "ob_macro_block_meta_mgr.h"

using namespace oceanbase::common;
namespace oceanbase {
namespace blocksstable {
ObMacroMetaBlockReader::ObMacroMetaBlockReader()
{}

ObMacroMetaBlockReader::~ObMacroMetaBlockReader()
{}

int ObMacroMetaBlockReader::parse(const ObMacroBlockCommonHeader& common_header,
    const ObLinkedMacroBlockHeader& linked_header, const char* buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  ObObj endkey[OB_MAX_ROWKEY_COLUMN_NUMBER];
  ObMacroBlockMeta meta;
  int64_t start_block_index = 0;
  int64_t pos = 0;
  MacroBlockId block_id;

  if (ObMacroBlockCommonHeader::MacroMeta != common_header.get_attr()) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(ERROR, "check common header failed.", K(common_header), K(ret));
  } else {
    STORAGE_LOG(INFO,
        "parse macro meta macro block, ",
        K(linked_header.total_previous_count_),
        K(linked_header.meta_data_count_));
    start_block_index = linked_header.total_previous_count_;

    for (int64_t i = 0; OB_SUCC(ret) && i < linked_header.meta_data_count_; ++i) {
      MEMSET(&meta, 0, sizeof(meta));
      meta.endkey_ = endkey;
      block_id.reset();
      block_id.set_local_block_id(static_cast<int32_t>(i + start_block_index));
      if (OB_FAIL(meta.deserialize(buf, buf_len, pos))) {
        STORAGE_LOG(WARN, "parse one meta error.", K(block_id), K(i + start_block_index), K(ret));
      } else if (ObMacroBlockCommonHeader::Free == meta.attr_) {
        // free meta, do nothing
      } else if (OB_FAIL(ObMacroBlockMetaMgr::get_instance().set_meta(block_id, meta))) {
        STORAGE_LOG(WARN, "Fail to set macro meta, ", K(ret), K(block_id));
      }
    }
  }
  return ret;
}

}  // namespace blocksstable
}  // namespace oceanbase
