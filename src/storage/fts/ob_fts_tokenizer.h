/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_FTS_TOKENIZER_H_
#define OB_FTS_TOKENIZER_H_

#include "ob_fts_plugin_helper.h"

namespace oceanbase
{
namespace storage
{

class ObFTSTokenizer
{
public:
  static int tokenize(
      ObIAllocator &alloc,
      const ObString &query_text,
      const ObString &parser_name,
      const ObString &parser_properties,
      const ObObjMeta &meta,
      ObIArray<ObString> &query_tokens);
  static int tokenize(
      ObIAllocator &alloc,
      const ObString &query_text,
      const ObString &parser_name,
      const ObString &parser_properties,
      const ObObjMeta &meta,
      ObIArray<ObString> &query_tokens,
      ObIArray<int64_t> &token_ids,
      ObIArray<int64_t> &token_positions,
      bool &has_duplicate_tokens);

private:
  static int segment(
      ObIAllocator &alloc,
      const ObString &query_text,
      const ObString &parser_name,
      const ObString &parser_properties,
      const ObObjMeta &meta,
      const share::schema::ObFTSIndexType fts_index_type,
      ObFTTokenMap &token_map);
};

} // namespace storage
} // namespace storage

#endif // OB_FTS_TOKENIZER_H_
