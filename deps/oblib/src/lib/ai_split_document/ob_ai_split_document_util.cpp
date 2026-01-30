/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX LIB

#include "ob_ai_split_document_util.h"
#include "lib/oblog/ob_log_module.h"
#include <unicode/brkiter.h>
#include <unicode/utext.h>

namespace oceanbase
{
namespace common
{

int ObAiSplitDocumentUtil::split_text_by_unit(const ObString &content,
                                              const ObAiSplitDocParams &params,
                                              ObIAllocator &allocator,
                                              ObArray<ObAiSplitDocChunk> &chunks)
{
  int ret = OB_SUCCESS;

  UText *utext = NULL;
  icu::BreakIterator *bi = NULL;
  UErrorCode status = U_ZERO_ERROR;
  const int64_t max_unit = params.max_;
  const int64_t overlap_unit = params.overlap_;
  const int64_t chunk_step = max_unit - overlap_unit;
  const int64_t chunks_count = chunks.count();

  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(MTL_ID(), "AiSplitDoc"));
  // Create UText
  if (OB_SUCC(ret)) {
    utext = utext_openUTF8(NULL, content.ptr(), content.length(), &status);
    if (U_FAILURE(status)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to open UText", K(ret), K(status));
    }
  }

  // Create BreakIterator
  if (OB_SUCC(ret)) {
    if (params.by_ == ObAiSplitByUnit::WORD) {
      bi = icu::BreakIterator::createWordInstance(icu::Locale::getDefault(), status);
    } else if (params.by_ == ObAiSplitByUnit::SENTENCE) {
      bi = icu::BreakIterator::createSentenceInstance(icu::Locale::getDefault(), status);
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid split by unit", K(ret), K(params.by_));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(bi) || U_FAILURE(status)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to create BreakIterator", K(ret), K(status));
    }
  }

  // Set UText to BreakIterator
  if (OB_SUCC(ret)) {
    status = U_ZERO_ERROR;
    bi->setText(utext, status);
    if (U_FAILURE(status)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to set UText to BreakIterator", K(ret), K(status));
    }
  }

  int unit_count_in_chunk = 0;
  int next_window_start = 0;
  int window_start = 0;
  int unit_since_window_start = 0;
  int boundary = bi->next();
  int chunk_id = 0;

  while (OB_SUCC(ret) && boundary != icu::BreakIterator::DONE) {
    if ((params.by_ == ObAiSplitByUnit::WORD && bi->getRuleStatus() != UBRK_WORD_NONE) ||
        (params.by_ == ObAiSplitByUnit::SENTENCE)) {
      unit_count_in_chunk++;
      unit_since_window_start++;
    }
    if (unit_count_in_chunk >= max_unit) {
      while (window_start < boundary && window_start < content.length() && isspace(content[window_start])) {
        window_start++;
      }
      if (window_start < boundary) {
        // construct chunk if there is meaningful content
        ObString chunk_text;
        ObAiSplitDocChunk chunk;
        int window_end = boundary;
        // trim space only at the end of the chunk
        while (window_end > window_start && isspace(content[window_end - 1])) {
          window_end--;
        }
        chunk_text.assign_ptr(content.ptr() + window_start, window_end - window_start);
        OX (chunk.init(chunk_id++, window_start, window_end - window_start, chunk_text));
        OZ (chunks.push_back(chunk));
      }
      unit_count_in_chunk = overlap_unit;
      if (overlap_unit == 0) {
        next_window_start = boundary;
      }
      window_start = next_window_start;
    }
    if (unit_since_window_start >= chunk_step) {
      next_window_start = boundary;
      unit_since_window_start = 0;
    }
    boundary = bi->next();
  }

  if (OB_SUCC(ret)){
    if (unit_count_in_chunk > overlap_unit || chunks_count == chunks.count()) {
      int end = content.length();
      while (window_start < end && isspace(content[window_start])) {
        window_start++;
      }
      while (end > window_start && isspace(content[end - 1])) {
        end--;
      }
      if (window_start < end) {
        // construct chunk if there is meaningful content
        ObString chunk_text;
        ObAiSplitDocChunk chunk;
        chunk_text.assign_ptr(content.ptr() + window_start, end - window_start);
        OX (chunk.init(chunk_id++, window_start, end - window_start, chunk_text));
        OZ (chunks.push_back(chunk));
      }
    }
  }

  if (OB_NOT_NULL(bi)) {
    delete bi;
  }
  if (OB_NOT_NULL(utext)) {
    utext_close(utext);
  }
  return ret;
}

// Helper template function to create iterator
template<typename IteratorType>
static int create_iterator_impl(ObIAllocator &allocator, ObDocSplitIterator *&iterator)
{
  int ret = OB_SUCCESS;
  void *buf = allocator.alloc(sizeof(IteratorType));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for iterator", K(ret));
  } else {
    iterator = new (buf) IteratorType();
  }
  return ret;
}

int ObAiSplitDocumentUtil::create_doc_split_iterator(const ObString &content,
                                                     const ObAiSplitDocParams &params,
                                                     ObIAllocator &allocator,
                                                     ObDocSplitIterator *&iterator)
{
  int ret = OB_SUCCESS;
  if (params.type_ == ObAiSplitContentType::TEXT) {
    OZ (create_iterator_impl<ObTextSplitIterator>(allocator, iterator));
  } else if (params.type_ == ObAiSplitContentType::MARKDOWN) {
    OZ (create_iterator_impl<ObMarkdownSplitIterator>(allocator, iterator));
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid content type", K(ret), K(params.type_));
    FORWARD_USER_ERROR(ret, "invalid parameters value for parameters.type, type must be 'text' or 'markdown'");
  }
  return ret;
}

} // namespace common
} // namespace oceanbase
