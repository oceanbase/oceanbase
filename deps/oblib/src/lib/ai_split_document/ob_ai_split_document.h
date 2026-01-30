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

#ifndef OCEANBASE_LIB_AI_SPLIT_DOCUMENT_OB_AI_SPLIT_DOCUMENT_H_
#define OCEANBASE_LIB_AI_SPLIT_DOCUMENT_OB_AI_SPLIT_DOCUMENT_H_

#include "lib/string/ob_string.h"
#include "lib/container/ob_se_array.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/json_type/ob_json_base.h"
#include "lib/json_type/ob_json_parse.h"
#include "share/ob_define.h"
#include <unicode/brkiter.h>
#include <unicode/utext.h>

namespace oceanbase
{
namespace common
{

enum class ObAiSplitContentType
{
  TEXT = 0,
  MARKDOWN = 1,
  MAX_CONTENT_TYPE = 2,
};

enum class ObAiSplitByUnit
{
  WORD = 0,
  SENTENCE = 1,
  MAX_UNIT_TYPE = 2,
};

// Parameters structure for AI split document
struct ObAiSplitDocParams
{
  ObAiSplitContentType type_;    // text or markdown, default: markdown
  ObAiSplitByUnit by_;           // word or sentence, default: word
  int64_t max_;                  // max words/sentences per chunk, default: 256
  int64_t overlap_;              // overlap words/sentences between chunks, default: 0

  ObAiSplitDocParams()
    : type_(ObAiSplitContentType::MARKDOWN),
      by_(ObAiSplitByUnit::WORD),
      max_(256),
      overlap_(0) {}

  void reset()
  {
    type_ = ObAiSplitContentType::MARKDOWN;
    by_ = ObAiSplitByUnit::WORD;
    max_ = 256;
    overlap_ = 0;
  }
  int init(const ObIJsonBase *params_node);
  void assign(const ObAiSplitDocParams &other)
  {
    type_ = other.type_;
    by_ = other.by_;
    max_ = other.max_;
    overlap_ = other.overlap_;
  }
  int check_validity() const;
private:
  int parse_type(const ObString &type_str);
  int parse_by(const ObString &by_str);
public:
  TO_STRING_KV(K_(type), K_(by), K_(max), K_(overlap));
};

// Chunk data structure for AI split document
struct ObAiSplitDocChunk
{
  int64_t chunk_id_;        // Chunk ID (0-based)
  int64_t chunk_offset_;    // Byte offset in original document
  int64_t chunk_length_;    // Byte length of chunk
  ObString chunk_text_;     // Chunk text content

  ObAiSplitDocChunk()
    : chunk_id_(0),
      chunk_offset_(0),
      chunk_length_(0),
      chunk_text_(){}
  ~ObAiSplitDocChunk()
  {
    chunk_id_ = 0;
    chunk_offset_ = 0;
    chunk_length_ = 0;
    chunk_text_.reset();
  }

  void init(int64_t id, int64_t offset, int64_t length, const ObString &text)
  {
    chunk_id_ = id;
    chunk_offset_ = offset;
    chunk_length_ = length;
    chunk_text_ = text;
  }

  void reset() {
    chunk_id_ = 0;
    chunk_offset_ = 0;
    chunk_length_ = 0;
    chunk_text_.reset();
  }
  void assign(const ObAiSplitDocChunk &other)
  {
    chunk_id_ = other.chunk_id_;
    chunk_offset_ = other.chunk_offset_;
    chunk_length_ = other.chunk_length_;
    chunk_text_ = other.chunk_text_;
  }

  TO_STRING_KV(K_(chunk_id), K_(chunk_offset), K_(chunk_length), K_(chunk_text));
};

// Input structure for AI split document (stored in jt.input_)
struct ObAiSplitDocInput
{
  ObString content_;
  ObAiSplitDocParams params_;

  ObAiSplitDocInput()
    : content_(),
      params_() {}

  int init(const ObString &content, const ObIJsonBase *params_node);
  void reset()
  {
    content_.reset();
    params_.reset();
  }

  TO_STRING_KV(K_(content), K_(params));
};

class ObDocSplitIterator
{
public:
	ObDocSplitIterator(){};
	virtual ~ObDocSplitIterator();
	virtual int open(const ObString &content, ObIAllocator &allocator, const ObAiSplitDocParams &params) = 0;
	virtual int get_next_row(ObAiSplitDocChunk &chunk) = 0;
	virtual int close() = 0;
};

class ObTextSplitIterator : public ObDocSplitIterator
{
public:
  ObTextSplitIterator() : is_inited_(false),
                          content_(),
                          params_(),
                          chunk_id_(0),
                          chunk_start_offset_(0),
                          next_chunk_start_(0),
                          unit_since_window_start_(0),
                          current_boundary_(0),
                          is_done_(false),
                          allocator_(nullptr),
                          bi_(NULL),
                          utext_(NULL) {}
  virtual ~ObTextSplitIterator() override;
  inline bool is_inited() const { return is_inited_; }
  virtual int open(const ObString &content, ObIAllocator &allocator, const ObAiSplitDocParams &params) override;
  virtual int get_next_row(ObAiSplitDocChunk &chunk) override;
  virtual int close() override;
protected:
  bool is_inited_;
  ObString content_;
  ObAiSplitDocParams params_;
  int64_t chunk_id_;
  int64_t chunk_start_offset_;
  int64_t next_chunk_start_;
  int64_t unit_since_window_start_;
  int64_t current_boundary_;
  bool is_done_;
  ObIAllocator *allocator_;
  icu::BreakIterator *bi_;
  UText *utext_;
public:
  TO_STRING_KV(K_(is_inited), K_(content), K_(params), K_(chunk_id), K_(chunk_start_offset), K_(next_chunk_start), K_(unit_since_window_start), K_(current_boundary), K_(is_done), KP_(allocator), KP_(bi), KP_(utext));
};

class ObMarkdownSplitIterator : public ObDocSplitIterator
{
public:
  ObMarkdownSplitIterator() : is_inited_(false),
                              content_(),
                              params_(),
                              chunk_id_(0),
                              section_start_offset_(0),
                              section_title_(),
                              section_content_(),
                              is_done_(false),
                              row_alloc_(),
                              allocator_(nullptr),
                              iterator_(nullptr){}
  virtual ~ObMarkdownSplitIterator() override;
  virtual int open(const ObString &content, ObIAllocator &allocator, const ObAiSplitDocParams &params) override;
  virtual int get_next_row(ObAiSplitDocChunk &chunk) override;
  virtual int close() override;
private:
  const static int64_t DEFAULT_TITLE_LEVEL = 1;
  int get_next_section(ObString &section_title, ObString &section_content);
  int get_next_line(int64_t line_start_offset, int64_t &line_end_offset);
  bool is_empty_section(const ObString& section_content);
  bool is_title_line(const ObString& line, int title_level);
protected:
  bool is_inited_;
  ObString content_;
  ObAiSplitDocParams params_;
  int64_t chunk_id_;
  int64_t section_start_offset_;
  ObString section_title_;
  ObString section_content_;
  bool is_done_;
  common::ObArenaAllocator row_alloc_;
  ObIAllocator *allocator_;
  ObTextSplitIterator *iterator_;
public:
  TO_STRING_KV(K_(is_inited), K_(content), K_(params), K_(chunk_id), K_(section_start_offset), K_(is_done), K_(section_title), K_(section_content), KP_(allocator), KP_(iterator));
};

// adapter for ObDocSplitIterator
class ObAiSplitDocAdapter
{
public:
  ObAiSplitDocAdapter()
    : is_inited_(false),
      cur_chunk_(),
      cur_idx_(-1),
      allocator_(nullptr),
      iterator_(nullptr) {}
  ~ObAiSplitDocAdapter(){ close(); }
  // useful interface for jsontable op
  int init(ObIAllocator &allocator, const ObString &content, const ObAiSplitDocParams &params);
  int get_next_row();
  inline int64_t get_cur_idx() const { return cur_idx_; }
  inline const ObAiSplitDocChunk &get_cur_chunk() const { return cur_chunk_; }

  // common interface
  inline bool is_inited() const { return is_inited_; }
  void reset();
  void close()
  {
    reset();
  }

private:
  bool is_inited_;
  ObAiSplitDocChunk cur_chunk_;
  int64_t cur_idx_;
  ObIAllocator* allocator_;
  ObDocSplitIterator* iterator_;
  TO_STRING_KV(K_(is_inited), K_(cur_chunk), K_(cur_idx), KP_(allocator), KP_(iterator));
};



} // end namespace common
} // end namespace oceanbase

#endif /* OCEANBASE_LIB_AI_SPLIT_DOCUMENT_OB_AI_SPLIT_DOCUMENT_H_ */
