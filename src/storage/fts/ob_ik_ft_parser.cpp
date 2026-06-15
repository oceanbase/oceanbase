/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE_FTS

#include "src/storage/fts/ob_ik_ft_parser.h"

#include "lib/charset/ob_charset.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_service.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "storage/fts/ob_fts_plugin_helper.h"
#include "storage/fts/ob_fts_parser_property.h"
#include "storage/fts/dict/ob_ft_range_dict.h"
#include "storage/fts/dict/ob_ft_dict_cache_loader.h"
#include "storage/fts/ik/ob_ik_cjk_processor.h"
#include "storage/fts/ik/ob_ik_letter_processor.h"
#include "storage/fts/ik/ob_ik_quantifier_processor.h"
#include "storage/fts/ik/ob_ik_surrogate_processor.h"
#include "plugin/sys/ob_plugin_mgr.h"
#include "storage/tablelock/ob_table_lock_service.h"
#include "storage/tablelock/ob_table_lock_rpc_struct.h"

using namespace oceanbase::plugin;

namespace oceanbase
{
namespace storage
{
int ObIKFTParser::init(const ObFTParserParam &param)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Parser already inited once", K(ret));
  } else {
    coll_type_ = ObCollationType::CS_TYPE_INVALID;
    if (OB_ISNULL(param.cs_) || OB_ISNULL(param.cs_->name)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Invalid parser param.", K(ret));
    } else if (CS_TYPE_INVALID == (coll_type_ = ObCharset::collation_type(param.cs_->name))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Invalid collation type.", K(ret));
    } else if (OB_FAIL(init_dict(param))) {
      LOG_WARN("Failed to init dict", K(ret));
    } else if (OB_FAIL(init_ctx(param))) {
      LOG_WARN("Failed to init ctx", K(ret));
    } else if (OB_FAIL(init_segmenter(param))) {
      LOG_WARN("Failed to init segmenters", K(ret));
    } else if (OB_FAIL(arb_.prepare())) {
      LOG_WARN("Failed to prepare arbitrator", K(ret));
    }

    if (OB_FAIL(ret)) {
      reset();
    } else {
      is_inited_ = true;
    }
  }

  return ret;
}

int ObIKFTParser::get_next_token(const char *&word,
                                 int64_t &word_len,
                                 int64_t &char_cnt,
                                 int64_t &word_freq)
{
  int ret = OB_SUCCESS;
  const char *output_word;
  int64_t len;
  int64_t offset;
  int64_t cnt;

  if (!IS_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Parser has not been inited", K(ret));
  } else {
    bool accept_token = false;
    while (OB_SUCC(ret) && !accept_token) {
      if (OB_FAIL(produce())) {
        LOG_WARN("Failed to produce new token", K(ret));
      } else if (OB_FAIL(ctx_->get_next_token(output_word, len, offset, cnt))) {
        if (OB_ITER_END == ret) {
          // ok, end this iter
        } else {
          LOG_WARN("Failed to get next token", K(ret));
        }
      } else {
        bool is_stop = false;
        // if (!OB_ISNULL(dict_stop_)
        //     && OB_FAIL(dict_stop_->match(ObString(len, output_word + offset), is_stop))) {
        //   LOG_WARN("Failed to match stopwords", K(ret));
        // } else
        if (!is_stop) {
          word = output_word + offset;
          word_len = len;
          char_cnt = cnt;
          word_freq = 1;
          accept_token = true;
        } else {
        }
      }
    }
  }

  return ret;
}

int ObIKFTParser::reuse_parser(const char *fulltext, const int64_t fulltext_len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("Ik ft parser has not been inited", K(ret));
  } else if (OB_UNLIKELY(nullptr == fulltext || 0 >= fulltext_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("There are invalid fulltext", K(ret), KP(fulltext), K(fulltext_len));
  } else {
    if (OB_FAIL(ctx_->reuse_context(fulltext, fulltext_len))) {
      LOG_WARN("Failed to reuse context", K(ret));
    }
    for (ObList<ObIIKProcessor *, ObIAllocator>::iterator iter = segmenters_.begin();
        OB_SUCC(ret) && iter != segmenters_.end();
        iter++) {
      (*iter)->reuse();
    }
    scratch_alloc_.reset_remain_one_page();
  }
  return ret;
}

int ObIKFTParser::produce()
{
  int ret = OB_SUCCESS;
  // Loop until end or has data to output
  while (OB_SUCC(ret) && ctx_->is_results_exhaust() && !ctx_->iter_end()) {
    if (OB_FAIL(process_next_batch())) {
      if (OB_ITER_END == ret) {
        // ok
      } else {
        LOG_WARN("Failed to load next batch", K(ret));
      }
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObIKFTParser::process_one_char(TokenizeContext &ctx,
                                   const char *ch,
                                   const uint8_t char_len,
                                   const ObFTCharUtil::CharType type)
{
  int ret = OB_SUCCESS;
  // proces by char with all segmenters
  for (ObList<ObIIKProcessor *, ObIAllocator>::iterator iter = segmenters_.begin();
       OB_SUCC(ret) && iter != segmenters_.end();
       iter++) {
    if (OB_FAIL((*iter)->process(ctx))) {
      LOG_WARN("Failed to process segmenter", K(ret));
    }
  }
  return ret;
}

int ObIKFTParser::process_next_batch()
{
  int ret = OB_SUCCESS;
  ctx_->reset_resource();

  // handle next segmenter
  bool do_seg = false;

  if (ctx_->iter_end()) {
    ret = OB_ITER_END;
  } else {
    ctx_->calc_buffer_start_cursor();
    while (OB_SUCC(ret) && !do_seg && !ctx_->iter_end()) {
      const char *ch;
      uint8_t char_len = 0;
      ObFTCharUtil::CharType type = ObFTCharUtil::CharType::USELESS;
      if (OB_FAIL(ctx_->current_char_and_type(ch, char_len, type))) {
        if (OB_LIKELY(OB_ITER_END == ret)) {
        } else {
          LOG_WARN("Failed to get current char and type", K(ret));
        }
      } else if (OB_FAIL(process_one_char(*ctx_, ch, char_len, type))) {
        LOG_WARN("Failed to process one char", K(ret));
      } else {
        // 1. check segmention
        if (ctx_->handle_size() >= HANDLE_SIZE_LIMIT && type == ObFTCharUtil::CharType::USELESS) {
          do_seg = true;
        }

        // 2. move to next;
        if (OB_FAIL(ctx_->step_next())) {
          if (OB_LIKELY(OB_ITER_END == ret)) {
          } else {
            LOG_WARN("Failed to step next", K(ret));
          }
        }
      } // end of one batch
    }

    if (OB_SUCC(ret) || OB_ITER_END == ret) {
      if (OB_FAIL(arb_.process(*ctx_))) {
        LOG_WARN("Failed to process arbitrator", K(ret));
      } else if (OB_FAIL(arb_.output_result(*ctx_))) {
        LOG_WARN("Failed to make result list");
      } else {
        arb_.reuse();
      }
    }
  }

  return ret;
}

int ObIKFTParserDesc::init(ObPluginParam *param)
{
  is_inited_ = true;
  return OB_SUCCESS;
}

int ObIKFTParserDesc::deinit(ObPluginParam *param)
{
  is_inited_ = false;
  return OB_SUCCESS;
}

int ObIKFTParserDesc::segment(ObFTParserParam *param, ObITokenIterator *&iter) const
{
  int ret = OB_SUCCESS;
  ObIKFTParser *parser = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("default ft parser desc hasn't be initialized", K(ret), K(is_inited_));
  } else if (OB_ISNULL(param) || OB_ISNULL(param->metadata_alloc_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("there are invalid arguments", K(ret), KPC(param));
  } else if (OB_ISNULL(parser = OB_NEWx(ObIKFTParser,
                                        param->metadata_alloc_,
                                        *(param->metadata_alloc_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate ik ft parser", K(ret));
  } else if (OB_FAIL(parser->init(*param))) {
    LOG_WARN("fail to init ik parser", K(ret), KPC(param));
  } else {
    iter = parser;
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(param) && OB_NOT_NULL(param->metadata_alloc_)) {
    OB_DELETEx(ObIKFTParser, param->metadata_alloc_, parser);
  }

  return ret;
}

void ObIKFTParserDesc::free_token_iter(ObFTParserParam *param,
                                       ObITokenIterator *&iter) const
{
  if (OB_NOT_NULL(iter)) {
    abort_unless(nullptr != param);
    abort_unless(nullptr != param->metadata_alloc_);
    iter->~ObITokenIterator();
    param->metadata_alloc_->free(iter);
  }
}


int ObIKFTParserDesc::get_add_word_flag(ObProcessTokenFlag &flag) const
{
  int ret = OB_SUCCESS;
  flag.set_casedown_token();
  flag.set_groupby_token();
  return ret;
}

int ObIKFTParser::init_dict(const plugin::ObFTParserParam &param)
{
  int ret = OB_SUCCESS;

  ObFTDictDesc::BuildMode build_mode = param.is_ddl_mode_
                                      ? ObFTDictDesc::BuildMode::DDL_EXE : ObFTDictDesc::BuildMode::DML_OR_SELECT_EXE;
  const ObCharsetType charset = ObCharsetType::CHARSET_UTF8MB4;
  const ObCollationType collation = ObCollationType::CS_TYPE_UTF8MB4_BIN;
  const bool need_casedown = param.need_casedown_;
  ObFTDictDesc main_dict_desc(charset, collation, param.ik_param_.main_dict_id_, param.ik_param_.main_dict_name_, need_casedown);
  ObFTDictDesc quan_dict_desc(charset, collation, param.ik_param_.quan_dict_id_, param.ik_param_.quan_dict_name_, need_casedown);
  ObFTDictDesc stopword_dict_desc(charset, collation, param.ik_param_.stopword_dict_id_, param.ik_param_.stopword_dict_name_, need_casedown);

  if (OB_FAIL(init_cache_loader(build_mode))) {
    LOG_WARN("Failed to init cache loader", K(ret), K(build_mode));
  } else if (OB_FAIL(init_single_dict(main_dict_desc, cache_main_))) {
    LOG_WARN("Failed to init main dict", K(ret), K(main_dict_desc));
  } else if (OB_FAIL(init_single_dict(quan_dict_desc, cache_quan_))) {
    LOG_WARN("Failed to init quantifier dict", K(ret), K(quan_dict_desc));
  } else if (OB_FAIL(init_single_dict(stopword_dict_desc, cache_stop_))) {
    LOG_WARN("Failed to init stopword dict", K(ret), K(stopword_dict_desc));
  } else if (OB_FAIL(build_dict_from_cache(main_dict_desc, cache_main_, dict_main_))) {
    LOG_WARN("Failed to build dict main", K(ret));
  } else if (OB_FAIL(build_dict_from_cache(quan_dict_desc, cache_quan_, dict_quan_))) {
    LOG_WARN("Failed to build dict quantifier", K(ret));
  } else if (OB_FAIL(build_dict_from_cache(stopword_dict_desc, cache_stop_, dict_stop_))) {
    LOG_WARN("Failed to build dict stopword", K(ret));
  }

  return ret;
}

int ObIKFTParser::init_cache_loader(const ObFTDictDesc::BuildMode build_mode)
{
  int ret = OB_SUCCESS;

  if (OB_NOT_NULL(cache_loader_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Cache loader already initialized", K(ret));
  } else {
    switch (build_mode) {
      case ObFTDictDesc::BuildMode::DDL_EXE: {
        ObFTDictCacheLoaderDDL *loader = OB_NEWx(ObFTDictCacheLoaderDDL, &metadata_alloc_);
        if (OB_ISNULL(loader)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("Failed to allocate DDL cache loader", K(ret));
        } else {
          cache_loader_ = loader;
        }
        break;
      }
      case ObFTDictDesc::BuildMode::DML_OR_SELECT_EXE: {
        ObFTDictCacheLoaderExec *loader = OB_NEWx(ObFTDictCacheLoaderExec, &metadata_alloc_);
        if (OB_ISNULL(loader)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("Failed to allocate Exec cache loader", K(ret));
        } else {
          cache_loader_ = loader;
        }
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unknown build mode", K(ret), K(build_mode));
    }
  }

  return ret;
}

int ObIKFTParser::init_single_dict(const ObFTDictDesc &desc, ObFTCacheRangeContainer &container)
{
  int ret = OB_SUCCESS;
  constexpr int64_t MAX_RETRY_COUNT = 3;
  int64_t retry_count = 0;

  while (OB_SUCC(ret) && retry_count < MAX_RETRY_COUNT) {
    if (OB_FAIL(cache_loader_->load_cache(desc, container)) && OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("Failed to build cache, try to load cache again", K(ret), K(desc));
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      retry_count++;
      ret = OB_SUCCESS;
    } else {
      break;
    }
    container.reset();
  }

  return ret;
}

int ObIKFTParser::init_ctx(const ObFTParserParam &param)
{
  int ret = OB_SUCCESS;

  if (coll_type_ == common::CS_TYPE_INVALID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Illegal collation type", K(ret));
  } else if (OB_ISNULL(ctx_ = OB_NEWx(TokenizeContext,
                                      &metadata_alloc_,
                                      metadata_alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to alloc ctx", K(ret));
  } else if (OB_FAIL(ctx_->init(coll_type_,
                                param.fulltext_,
                                param.ft_length_,
                                param.ik_param_.mode_ == ObFTIKParam::Mode::SMART))) {
    LOG_WARN("Failed to init ctx", K(ret));
  }
  if (OB_FAIL(ret)) {
    OB_DELETEx(TokenizeContext, &metadata_alloc_, ctx_);
  }
  return ret;
}

int ObIKFTParser::init_segmenter(const ObFTParserParam &param)
{
  int ret = OB_SUCCESS;
  // do have an order
  ObIKLetterProcessor *letter_seg = nullptr;
  ObIKQuantifierProcessor *cnqsg = nullptr;
  ObIKCJKProcessor *cjksg = nullptr;
  ObIKSurrogateProcessor *surrogate_seg = nullptr;
  if (OB_ISNULL(letter_seg = OB_NEWx(ObIKLetterProcessor, &metadata_alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to alloc letter segmenter", K(ret));
  } else if (OB_ISNULL(dict_quan_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Dict quan is null.", K(ret));
  } else if (OB_ISNULL(cnqsg = OB_NEWx(ObIKQuantifierProcessor, &metadata_alloc_, *dict_quan_, scratch_alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to alloc cn quantifier segmenter", K(ret));
  } else if (OB_ISNULL(dict_main_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Dict main is null.", K(ret));
  } else if (OB_ISNULL(cjksg = OB_NEWx(ObIKCJKProcessor, &metadata_alloc_, *dict_main_, scratch_alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to alloc cjk segmenter", K(ret));
  } else if (OB_ISNULL(surrogate_seg = OB_NEWx(ObIKSurrogateProcessor, &metadata_alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to alloc surrogate segmenter", K(ret));
  } else if (OB_FAIL(segmenters_.push_back(letter_seg))) {
    LOG_WARN("Failed to push back letter segmenter", K(ret));
  } else if (FALSE_IT(letter_seg = nullptr)) {
  } else if (OB_FAIL(segmenters_.push_back(cnqsg))) {
    LOG_WARN("Failed to push back cn quantifier segmenter", K(ret));
  } else if (FALSE_IT(cnqsg = nullptr)) {
  } else if (OB_FAIL(segmenters_.push_back(cjksg))) {
    LOG_WARN("Failed to push back cjk segmenter", K(ret));
  } else if (FALSE_IT(cjksg = nullptr)) {
  } else if (OB_FAIL(segmenters_.push_back(surrogate_seg))) {
    LOG_WARN("Failed to push back surrogate segmenter");
  } else if (OB_FALSE_IT(surrogate_seg = nullptr)) {
  }
  // push back by order, quantifier is before cjk

  if (OB_FAIL(ret)) {
    OB_DELETEx(ObIKLetterProcessor, &metadata_alloc_, letter_seg);
    OB_DELETEx(ObIKQuantifierProcessor, &metadata_alloc_, cnqsg);
    OB_DELETEx(ObIKCJKProcessor, &metadata_alloc_, cjksg);
    OB_DELETEx(ObIKSurrogateProcessor, &metadata_alloc_, surrogate_seg);
  }
  return ret;
}

void ObIKFTParser::reset()
{
  // In-transaction locks are automatically released on transaction commit/rollback
  // No manual unlock needed

  if (!OB_ISNULL(ctx_)) {
    ctx_->~TokenizeContext();
    metadata_alloc_.free(ctx_);
  }

  for (ObIIKProcessor *segmenter : segmenters_) {
    if (!OB_ISNULL(segmenter)) {
      segmenter->~ObIIKProcessor();
      metadata_alloc_.free(segmenter);
    }
  }
  segmenters_.clear();

  cache_main_.reset();
  cache_quan_.reset();
  cache_stop_.reset();

  if (!OB_ISNULL(dict_main_)) {
    dict_main_->~ObIFTDict();
    metadata_alloc_.free(dict_main_);
  }
  if (!OB_ISNULL(dict_quan_)) {
    dict_quan_->~ObIFTDict();
    metadata_alloc_.free(dict_quan_);
  }
  if (!OB_ISNULL(dict_stop_)) {
    dict_stop_->~ObIFTDict();
    metadata_alloc_.free(dict_stop_);
  }

  if (!OB_ISNULL(cache_loader_)) {
    cache_loader_->~ObFTDictCacheLoaderBase();
    metadata_alloc_.free(cache_loader_);
    cache_loader_ = nullptr;
  }

  is_inited_ = false;
}

int ObIKFTParser::build_dict_from_cache(const ObFTDictDesc &desc,
                                        ObFTCacheRangeContainer &container,
                                        ObIFTDict *&dict)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dict = OB_NEWx(ObFTRangeDict, &metadata_alloc_, &container, desc))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to alloc dict", K(ret));
  } else if (OB_FAIL(dict->init())) {
    LOG_WARN("Failed to init dict", K(ret));
  }
  if (OB_FAIL(ret)) {
    OB_DELETEx(ObIFTDict, &metadata_alloc_, dict);
  }
  return ret;
}

} //  namespace storage
} //  namespace oceanbase
