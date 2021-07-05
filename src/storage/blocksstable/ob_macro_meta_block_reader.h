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

#ifndef OB_MACRO_META_BLOCK_READER_H_
#define OB_MACRO_META_BLOCK_READER_H_

#include "ob_meta_block_reader.h"

namespace oceanbase {
namespace blocksstable {

class ObMacroMetaBlockReader : public ObMetaBlockReader {
public:
  ObMacroMetaBlockReader();
  virtual ~ObMacroMetaBlockReader();

protected:
  virtual int parse(const ObMacroBlockCommonHeader& common_header, const ObLinkedMacroBlockHeader& linked_header,
      const char* buf, const int64_t buf_len);
};

}  // namespace blocksstable
}  // namespace oceanbase

#endif /* OB_MACRO_META_BLOCK_READER_H_ */
