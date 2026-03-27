/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LIB_AI_SPLIT_DOCUMENT_OB_AI_SPLIT_DOCUMENT_UTIL_H_
#define OCEANBASE_LIB_AI_SPLIT_DOCUMENT_OB_AI_SPLIT_DOCUMENT_UTIL_H_

#include "ob_ai_split_document.h"
#include "share/rc/ob_tenant_base.h"
#include "lib/alloc/malloc_hook.h"


namespace oceanbase
{
namespace common
{

class ObAiSplitDocumentUtil
{
public:
  static int split_text_by_unit(const ObString &content,
                                const ObAiSplitDocParams &params,
                                ObIAllocator &allocator,
                                ObArray<ObAiSplitDocChunk> &chunks);
  static int create_doc_split_iterator(const ObString &content,
                                       const ObAiSplitDocParams &params,
                                       ObIAllocator &allocator,
                                       ObDocSplitIterator *&iterator);
};
} // namespace common
} // namespace oceanbase
#endif /* OCEANBASE_LIB_AI_SPLIT_DOCUMENT_OB_AI_SPLIT_DOCUMENT_UTIL_H_ */
