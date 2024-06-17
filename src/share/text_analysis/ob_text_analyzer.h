/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SHARE_OB_TEXT_ANALYZER_H_
#define OCEANBASE_SHARE_OB_TEXT_ANALYZER_H_

#include "share/text_analysis/ob_token_stream.h"
namespace oceanbase
{
namespace share
{

struct ObTextAnalysisCtx final
{
public:
  ObTextAnalysisCtx()
    : cs_(nullptr),
      filter_stopword_(true),
      need_grouping_(false)
  {}
  ~ObTextAnalysisCtx() = default;
  bool is_valid() const { return nullptr != cs_; }
  void reset()
  {
    cs_ = nullptr;
    filter_stopword_ = true;
    need_grouping_ = false;
  }
  TO_STRING_KV(KP_(cs), K_(filter_stopword), K_(need_grouping));
public:
  const ObCharsetInfo *cs_;
  bool filter_stopword_;
  bool need_grouping_;
  // language type
  // word segment plugin type
  // specified normalization tricks
};

class ObITextAnalyzer
{
public:
  ObITextAnalyzer() : allocator_(nullptr), ctx_(nullptr), analyze_pipeline_(), is_inited_(false) {}
  virtual ~ObITextAnalyzer() { reset(); }

  virtual void reset();
  virtual int init(const ObTextAnalysisCtx &ctx, ObIAllocator &allocator);
  virtual int analyze(const ObDatum &document, ObITokenStream *&token_stream) = 0;
  VIRTUAL_TO_STRING_KV(KPC_(ctx), K_(analyze_pipeline), K_(is_inited));
protected:
  virtual int inner_init(const ObTextAnalysisCtx &ctx, ObIAllocator &allocator) = 0;
  int add_tokenizer(const ObTextTokenizer::TokenizerType &type);
  int add_normalizer(
      const ObTokenNormalizer::TokenNormalizerType &type,
      const ObTextAnalysisCtx &ctx);
  ObITokenStream *get_tail_token_stream();
private:
  template<typename T>
  int add_token_stream(ObITokenStream *&token_stream);
protected:
  ObIAllocator *allocator_;
  const ObTextAnalysisCtx *ctx_;
  ObSEArray<ObITokenStream *, 4> analyze_pipeline_;
  bool is_inited_;
};

class ObEnglishTextAnalyzer final : public ObITextAnalyzer
{
public:
  ObEnglishTextAnalyzer() : ObITextAnalyzer() {}
  virtual ~ObEnglishTextAnalyzer() {}

  virtual int analyze(const ObDatum &document, ObITokenStream *&token_stream) override;
protected:
  virtual int inner_init(const ObTextAnalysisCtx &ctx, ObIAllocator &allocator) override;
};

} // namespace share
} // namespace oceanbase

#endif
