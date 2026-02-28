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

#include "ob_ai_split_document.h"
#include "ob_ai_split_document_util.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/json/ob_json.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace common
{


#define EXTRACT_JSON_ELEM_STR_WITH_PROCESS(json_key, str_val, process) \
  if (elem.first.case_compare(json_key) == 0) { \
    if (elem.second->json_type() != ObJsonNodeType::J_STRING) { \
      ret = OB_INVALID_ARGUMENT; \
      LOG_WARN("invalid json type", K(ret), K(elem.first), K(elem.second->json_type())); \
      FORWARD_USER_ERROR(ret, "invalid parameters type for parameters." #json_key ", " #json_key " must be a string"); \
    } else { \
      str_val = ObString(elem.second->get_data_length(), elem.second->get_data()); \
      process; \
    } \
  } else

#define EXTRACT_JSON_ELEM_INT(json_key, int_val) \
  if (elem.first.case_compare(json_key) == 0) { \
    if (elem.second->json_type() != ObJsonNodeType::J_INT && elem.second->json_type() != ObJsonNodeType::J_UINT) { \
      ret = OB_INVALID_ARGUMENT; \
      LOG_WARN("invalid json type", K(ret), K(elem.first), K(elem.second->json_type())); \
      FORWARD_USER_ERROR(ret, "invalid parameters type for parameters." #json_key ", " #json_key " must be an integer"); \
    } else { \
      int_val = elem.second->get_int(); \
    } \
  } else

#define EXTRACT_JSON_ELEM_END() \
  { \
    ret = OB_INVALID_ARGUMENT; \
    LOG_WARN("unknown json key param", K(ret), K(elem.first)); \
    FORWARD_USER_ERROR_MSG(ret, "unknown parameter key '%.*s'", elem.first.length(), elem.first.ptr()); \
  }

int ObAiSplitDocInput::init(const ObString &content, const ObIJsonBase *params_node)
{
  int ret = OB_SUCCESS;
  content_ = content;
  OZ (params_.init(params_node));
  return ret;
}

int ObAiSplitDocParams::init(const ObIJsonBase *params_node)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_ISNULL(params_node)) {
    // do nothing
  } else if (params_node->json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("parameters should be a JSON object", K(ret), K(params_node->json_type()));
    FORWARD_USER_ERROR(ret, "parameters should be a JSON object");
  } else {
    ObString type_str;
    ObString by_str;
    JsonObjectIterator iter = params_node->object_iterator();
    while (!iter.end() && OB_SUCC(ret)) {
      ObJsonObjPair elem;
      OZ(iter.get_elem(elem));
      if (OB_SUCC(ret)) {
        EXTRACT_JSON_ELEM_STR_WITH_PROCESS("type", type_str, parse_type(type_str))
        EXTRACT_JSON_ELEM_STR_WITH_PROCESS("by", by_str, parse_by(by_str))
        EXTRACT_JSON_ELEM_INT("max", max_)
        EXTRACT_JSON_ELEM_INT("overlap", overlap_)
        EXTRACT_JSON_ELEM_END()
      }
      iter.next();
    }
  }
  // check validity after init
  OZ(check_validity());
  return ret;
}

int ObAiSplitDocParams::parse_type(const ObString &type_str)
{
  int ret = OB_SUCCESS;
  if (0 == type_str.case_compare("text")) {
    type_ = ObAiSplitContentType::TEXT;
  } else if (0 == type_str.case_compare("markdown")) {
    type_ = ObAiSplitContentType::MARKDOWN;
  } else {
    type_ = ObAiSplitContentType::MAX_CONTENT_TYPE;
  }
  return ret;
}

int ObAiSplitDocParams::parse_by(const ObString &by_str)
{
  int ret = OB_SUCCESS;
  if (0 == by_str.case_compare("sentence")) {
    by_ = ObAiSplitByUnit::SENTENCE;
  } else if (0 == by_str.case_compare("word")) {
    by_ = ObAiSplitByUnit::WORD;
  } else {
    by_ = ObAiSplitByUnit::MAX_UNIT_TYPE;
  }
  return ret;
}

int ObAiSplitDocParams::check_validity() const
{
  int ret = OB_SUCCESS;
  if (max_ <= 0 || max_ > 1000) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parameters", K(ret), K(max_), K(overlap_));
    FORWARD_USER_ERROR_MSG(ret, "invalid value for parameters.max, max must be greater than 0 and less than 1000");
  } else if (overlap_ < 0 || overlap_ > max_/2) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parameters", K(ret), K(max_), K(overlap_));
    FORWARD_USER_ERROR_MSG(ret, "invalid value for parameters.overlap, overlap must be greater than 0 and less than max/2");
  } else if (by_ == ObAiSplitByUnit::MAX_UNIT_TYPE) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid split by unit", K(ret), K(by_));
    FORWARD_USER_ERROR(ret, "invalid parameters value for parameters.by, by must be 'sentence' or 'word'");
  } else if (type_ == ObAiSplitContentType::MAX_CONTENT_TYPE) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid content type", K(ret), K(type_));
    FORWARD_USER_ERROR(ret, "invalid parameters value for parameters.type, type must be 'text' or 'markdown'");
  }
  return ret;
}

int ObAiSplitDocAdapter::init(ObIAllocator &allocator, const ObString &content, const ObAiSplitDocParams &params)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("adapter already initialized", K(ret));
  } else {
    OX (reset());
    OX (allocator_ = &allocator);
    OZ (ObAiSplitDocumentUtil::create_doc_split_iterator(content, params, allocator, iterator_));
    OZ (iterator_->open(content, allocator, params));
    OX (is_inited_ = true);
  }
  return ret;
}

int ObAiSplitDocAdapter::get_next_row()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("adapter not initialized", K(ret));
  } else {
    OZ (iterator_->get_next_row(cur_chunk_));
    OX (cur_idx_++);
  }
  return ret;
}

void ObAiSplitDocAdapter::reset()
{
  if (OB_NOT_NULL(iterator_) && OB_NOT_NULL(allocator_)) {
    OB_DELETEx(ObDocSplitIterator, allocator_, iterator_);
  }
  iterator_ = nullptr;
  allocator_ = nullptr;
  is_inited_ = false;
  cur_idx_ = -1;
  cur_chunk_.reset();
}

ObDocSplitIterator::~ObDocSplitIterator()
{
}

ObTextSplitIterator::~ObTextSplitIterator()
{
  close();
}

int ObTextSplitIterator::open(const ObString &content, ObIAllocator &allocator, const ObAiSplitDocParams &params)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("word split iterator already initialized", K(ret));
  } else {
    content_.assign_ptr(content.ptr(), content.length());
    params_.assign(params);
    chunk_id_ = 0;
    chunk_start_offset_ = 0;
    next_chunk_start_ = 0;
    unit_since_window_start_ = 0;
    current_boundary_ = 0;
    is_done_ = false;
    allocator_ = &allocator;

    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(MTL_ID(), "AiSplitDoc"));
    // Create BreakIterator
    UErrorCode status = U_ZERO_ERROR;
    if (params.by_ == ObAiSplitByUnit::WORD) {
      bi_ = icu::BreakIterator::createWordInstance(icu::Locale::getDefault(), status);
    } else if (params.by_ == ObAiSplitByUnit::SENTENCE) {
      bi_ = icu::BreakIterator::createSentenceInstance(icu::Locale::getDefault(), status);
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid split by unit", K(ret), K(params.by_));
      FORWARD_USER_ERROR(ret, "invalid parameters value for parameters.by, by must be 'word' or 'sentence'");
    }

    if (OB_SUCC(ret)) {
      if (U_FAILURE(status)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to create BreakIterator", K(ret), K(status));
      } else if (OB_ISNULL(bi_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to create BreakIterator", K(ret));
      }
    }

    // Create UText
    if (OB_SUCC(ret)) {
      utext_ = utext_openUTF8(NULL, content.ptr(), content.length(), &status);
      if (U_FAILURE(status)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to open UText", K(ret), K(status));
      } else if (OB_ISNULL(utext_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to open UText", K(ret));
      }
    }

    // Set UText to BreakIterator
    if (OB_SUCC(ret)) {
      status = U_ZERO_ERROR;
      bi_->setText(utext_, status);
      if (U_FAILURE(status)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to set UText to BreakIterator", K(ret), K(status));
      }
    }

    if (OB_SUCC(ret)) {
      is_inited_ = true;
    } else {
      close();
    }
  }
  return ret;
}

int ObTextSplitIterator::get_next_row(ObAiSplitDocChunk &chunk)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("text split iterator not initialized", K(ret));
  } else if (is_done_) {
    ret = OB_ITER_END;
  } else {
    chunk.reset();
    const int64_t max_unit = params_.max_;
    const int64_t overlap_unit = params_.overlap_;
    const int64_t chunk_step = max_unit - overlap_unit;
    int64_t window_start = chunk_start_offset_;
    bool chunk_found = false;
    int64_t unit_count_in_chunk = chunk_start_offset_ == 0 ? 0 : overlap_unit;
    int64_t boundary = current_boundary_;

    while (boundary != icu::BreakIterator::DONE && !chunk_found) {
      boundary = bi_->next();
      if (boundary == icu::BreakIterator::DONE) {
      } else {
        if ((params_.by_ == ObAiSplitByUnit::WORD && bi_->getRuleStatus() != UBRK_WORD_NONE) ||
            (params_.by_ == ObAiSplitByUnit::SENTENCE)) {
          unit_count_in_chunk++;
          unit_since_window_start_++;
        }

        if (unit_count_in_chunk >= max_unit) {
          while (window_start < boundary && window_start < content_.length() && isspace(content_[window_start])) {
            window_start++;
          }
          if (window_start < boundary) {
            int64_t window_end = boundary;
            while (window_end > window_start && isspace(content_[window_end - 1])) {
              window_end--;
            }
            chunk.init(chunk_id_++, window_start, window_end - window_start, ObString(window_end - window_start, content_.ptr() + window_start));
            chunk_found = true;
          }
          unit_count_in_chunk = overlap_unit;
          if (overlap_unit == 0) {
            next_chunk_start_ = boundary;
          }
          window_start = next_chunk_start_;
          chunk_start_offset_ = window_start;
        }

        if (unit_since_window_start_ >= chunk_step) {
          next_chunk_start_ = boundary;
          unit_since_window_start_ = 0;
        }
      }
    }
    current_boundary_ = boundary;

    if (!chunk_found && boundary == icu::BreakIterator::DONE) {
      if (unit_count_in_chunk > overlap_unit || chunk_id_ == 0) {
        int64_t end = content_.length();
        while (window_start < end && isspace(content_[window_start])) {
          window_start++;
        }
        while (end > window_start && isspace(content_[end - 1])) {
          end--;
        }
        if (window_start < end) {
          chunk.init(chunk_id_++, window_start, end - window_start, ObString(end - window_start, content_.ptr() + window_start));
          chunk_found = true;
        }
      }
      is_done_ = true;
    }

    if (!chunk_found) {
      ret = OB_ITER_END;
    }
  }
  return ret;
}

int ObTextSplitIterator::close()
{
  int ret = OB_SUCCESS;

  is_inited_ = false;
  content_.reset();
  params_.reset();
  chunk_start_offset_ = 0;
  next_chunk_start_ = 0;
  unit_since_window_start_ = 0;
  chunk_id_ = 0;
  current_boundary_ = 0;
  is_done_ = false;

  if (OB_NOT_NULL(bi_)) {
    delete bi_;
    bi_ = NULL;
  }
  if (OB_NOT_NULL(utext_)) {
    utext_close(utext_);
    utext_ = NULL;
  }
  return ret;
}

ObMarkdownSplitIterator::~ObMarkdownSplitIterator()
{
  close();
}

int ObMarkdownSplitIterator::open(const ObString &content, ObIAllocator &allocator, const ObAiSplitDocParams &params)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("markdown split iterator already initialized", K(ret));
  } else {
    allocator_ = &allocator;
    content_.assign_ptr(content.ptr(), content.length());
    params_.assign(params);
    chunk_id_ = 0;
    section_start_offset_ = 0;
    is_done_ = false;
    section_title_.reset();
    section_content_.reset();
    row_alloc_.clear();
    row_alloc_.set_tenant_id(MTL_ID());

    void *buf = allocator.alloc(sizeof(ObTextSplitIterator));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for text iterator", K(ret));
    } else {
      iterator_ = new (buf) ObTextSplitIterator();
    }

    if (OB_SUCC(ret)) {
      is_inited_ = true;
    } else {
      close();
    }
  }
  return ret;
}

int ObMarkdownSplitIterator::get_next_row(ObAiSplitDocChunk &chunk)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("markdown split iterator not initialized", K(ret));
  } else if (is_done_) {
    ret = OB_ITER_END;
  } else {
    bool chunk_found = false;
    if (!section_title_.empty()) {
      row_alloc_.clear();
    }

    chunk.reset();
    ObAiSplitDocChunk temp_chunk;
    while (OB_SUCC(ret) && !chunk_found) {
      if (OB_NOT_NULL(iterator_) && iterator_->is_inited()) {
        if (OB_FAIL(iterator_->get_next_row(temp_chunk))) {
          if (ret == OB_ITER_END) {
            iterator_->close();
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to get next chunk from iterator", K(ret));
          }
        } else {
          int64_t section_start_in_content = section_content_.ptr() - content_.ptr();
          chunk.init(chunk_id_++,
                    section_start_in_content + temp_chunk.chunk_offset_,
                    temp_chunk.chunk_length_,
                    temp_chunk.chunk_text_);
          if (!section_title_.empty()) {
            const int64_t total_len = section_title_.length() + temp_chunk.chunk_text_.length();
            char* buf = static_cast<char*>(row_alloc_.alloc(total_len));
            CK (OB_NOT_NULL(buf));
            OX (MEMCPY(buf, section_title_.ptr(), section_title_.length()));
            OX (MEMCPY(buf + section_title_.length(), temp_chunk.chunk_text_.ptr(), temp_chunk.chunk_text_.length()));
            OX (chunk.chunk_text_.assign_ptr(buf, total_len));
          }
          chunk_found = true;
        }
      } else {
        if (OB_FAIL(get_next_section(section_title_, section_content_))) {
          if (ret == OB_ITER_END) {
            is_done_ = true;
          } else {
            LOG_WARN("failed to get next section", K(ret));
          }
        } else if (!section_title_.empty() && is_empty_section(section_content_)) {
          int64_t section_start_in_content = section_title_.ptr() - content_.ptr();
          chunk.init(chunk_id_++,
                    section_start_in_content,
                    section_title_.length(),
                    section_title_);
          chunk_found = true;
        } else if (OB_FAIL(iterator_->open(section_content_, *allocator_, params_))) {
          LOG_WARN("failed to open iterator with section content", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObMarkdownSplitIterator::close()
{
  int ret = OB_SUCCESS;

  is_inited_ = false;
  content_.reset();
  params_.reset();
  chunk_id_ = 0;
  section_start_offset_ = 0;
  is_done_ = false;
  section_title_.reset();
  section_content_.reset();

  if (OB_NOT_NULL(iterator_)) {
    iterator_->close();
    ObTextSplitIterator *text_iterator = static_cast<ObTextSplitIterator*>(iterator_);
    if (OB_NOT_NULL(allocator_) && OB_NOT_NULL(text_iterator)) {
      OB_DELETEx(ObTextSplitIterator, allocator_, text_iterator);
      iterator_ = nullptr;
    }
  }
  row_alloc_.clear();
  allocator_ = nullptr;
  return ret;
}

int ObMarkdownSplitIterator::get_next_section(ObString &section_title, ObString &section_content)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("markdown split iterator not initialized", K(ret));
  } else if (section_start_offset_ >= content_.length()) {
    ret = OB_ITER_END;
  } else {
    section_title.reset();
    section_content.reset();
    bool has_title = false;
    bool section_end = false;
    int section_content_start = section_start_offset_;
    int section_content_end = section_content_start;
    int64_t line_start = section_start_offset_;
    int64_t line_end = line_start;

    while (OB_SUCC(ret) && line_start < content_.length() && !section_end) {
      OZ (get_next_line(line_start, line_end));
      if (OB_SUCC(ret)) {
        if (is_title_line(ObString(line_end - line_start, content_.ptr() + line_start), DEFAULT_TITLE_LEVEL)) {
          if (!has_title) {
            has_title = true;
            int64_t title_end = (line_end < content_.length()) ? line_end + 1 : content_.length();
            section_title.assign_ptr(content_.ptr() + line_start, title_end - line_start);
            section_content_start = title_end;
          } else {
            section_end = true;
            section_content_end = line_start;
          }
        }
      }
      line_start = line_end + 1;
    }

    if (OB_SUCC(ret)) {
      if (!section_end) {
        section_content_end = line_end < content_.length() ? line_end + 1 : line_end;
      }
      section_content.assign_ptr(content_.ptr() + section_content_start,
                                 section_content_end - section_content_start);
      section_start_offset_ = section_content_end;
    }
  }
  return ret;
}

bool ObMarkdownSplitIterator::is_empty_section(const ObString& section_content)
{
  int res = true;
  if (section_content.length() == 0) {
    res = true;
  } else {
    int pos = 0;
    while (pos < section_content.length() && isspace(section_content[pos])) {
      pos++;
    }
    res = pos == section_content.length();
  }
  return res;
}

bool ObMarkdownSplitIterator::is_title_line(const ObString& line, int title_level)
{
  bool res = true;
  if (line.length() == 0 || line[0] != '#') {
    res = false;
  } else {
    int count = 0;
    int pos = 0;
    while (pos < line.length() && line[pos] == '#') {
      count++;
      pos++;
    }
    if (count != title_level) res = false;
  }
  return res;
}


int ObMarkdownSplitIterator::get_next_line(int64_t line_start_offset, int64_t &line_end_offset)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("markdown split iterator not initialized", K(ret));
  } else if (line_start_offset >= content_.length()) {
    ret = OB_ITER_END;
  } else {
    line_end_offset = line_start_offset;
    while (line_end_offset < content_.length() && content_[line_end_offset] != '\n') {
      line_end_offset++;
    }
  }
  return ret;
}

}

}