/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE_FTS

#include "src/storage/fts/ob_ik_ft_parser.h"

#include "lib/charset/ob_charset.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/utility.h"
#include "share/ob_server_struct.h"
#include "storage/fts/ob_fts_struct.h"
#include "storage/fts/ob_fts_plugin_helper.h"
#include "storage/fts/dict/ob_ft_dict.h"
#include "storage/fts/dict/ob_ft_dict_def.h"
#include "storage/fts/dict/ob_ft_dict_hub.h"
#include "storage/fts/dict/ob_ft_range_dict.h"
#include "storage/fts/ik/ob_ik_arbitrator.h"
#include "storage/fts/ik/ob_ik_cjk_processor.h"
#include "storage/fts/ik/ob_ik_letter_processor.h"
#include "storage/fts/ik/ob_ik_processor.h"
#include "storage/fts/ik/ob_ik_quantifier_processor.h"
#include "storage/fts/ik/ob_ik_surrogate_processor.h"
#include "plugin/sys/ob_plugin_mgr.h"

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

int ObIKFTParser::produce()
{
  int ret = OB_SUCCESS;
  // Loop until end or has data to output
  while (OB_SUCC(ret) && ctx_->result_list().empty() && !ctx_->iter_end()) {
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
    while (OB_SUCC(ret) && !do_seg && !ctx_->iter_end()) {
      const char *ch;
      uint8_t char_len = 0;
      ObFTCharUtil::CharType type = ObFTCharUtil::CharType::USELESS;
      if (OB_FAIL(ctx_->current_char(ch, char_len))) {
        LOG_WARN("Failed to get current char", K(ret));
      } else if (OB_FAIL(ctx_->current_char_type(type))) {
        LOG_WARN("Failed to get current char type", K(ret));
      } else if (OB_FAIL(process_one_char(*ctx_, ch, char_len, type))) {
        LOG_WARN("Failed to process one char", K(ret));
      } else {
        // 1. check segmention
        if (ctx_->handle_size() > SEGMENT_LIMIT && type == ObFTCharUtil::CharType::USELESS) {
          do_seg = true;
        }

        // 2. move to next;
        if (OB_FAIL(ctx_->step_next())) {
          if (OB_ITER_END == ret) {
          } else {
            LOG_WARN("Failed to step next", K(ret));
          }
        }
      } // end of one batch
    }

    if (OB_SUCC(ret) || OB_ITER_END == ret) {
      ObIKArbitrator arb;
      if (OB_FAIL(arb.process(*ctx_))) {
        LOG_WARN("Failed to process arbitrator", K(ret));
      } else if (OB_FAIL(arb.output_result(*ctx_))) {
        LOG_WARN("Failed to make result list");
      }
    } else {
      // Already logged.
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
  void *buf = nullptr;
  ObFTDictHub *hub = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("default ft parser desc hasn't be initialized", K(ret), K(is_inited_));
  } else if (OB_ISNULL(param) || OB_UNLIKELY(!param->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(param));
  } else if (OB_FAIL(ObFTParsePluginData::instance().get_dict_hub(hub))) {
    LOG_WARN("Failed to get dict hub.", K(ret));
  } else if (OB_ISNULL(buf = param->allocator_->alloc(sizeof(ObIKFTParser)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate ik ft parser", K(ret));
  } else {
    ObIKFTParser *parser = new (buf) ObIKFTParser(*(param->allocator_), hub);
    if (OB_FAIL(parser->init(*param))) {
      LOG_WARN("fail to init ik parser", K(ret), KPC(param));
      param->allocator_->free(parser);
    } else {
      iter = parser;
    }
  }
  return ret;
}

void ObIKFTParserDesc::free_token_iter(ObFTParserParam *param,
                                       ObITokenIterator *&iter) const
{
  iter->~ObITokenIterator();
  param->allocator_->free(iter);
}


int ObIKFTParserDesc::get_add_word_flag(ObAddWordFlag &flag) const
{
  int ret = OB_SUCCESS;
  flag.set_casedown();
  flag.set_groupby_word();
  return ret;
}

int ObIKFTParser::init_dict(const plugin::ObFTParserParam &param)
{
  int ret = OB_SUCCESS;
  ObIFTDict *tmp_dict = nullptr;

  if (OB_ISNULL(hub_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Dict hub is not inited", K(ret));
  }

  ObFTRangeDict *dict = nullptr;
  ObFTDictDesc main_dict_desc("main_dict",
                              ObFTDictType::DICT_IK_MAIN,
                              ObCharsetType::CHARSET_UTF8MB4,
                              ObCollationType::CS_TYPE_UTF8MB4_BIN);

  ObFTDictDesc quan_dict_desc("quan_dict",
                              ObFTDictType::DICT_IK_QUAN,
                              ObCharsetType::CHARSET_UTF8MB4,
                              ObCollationType::CS_TYPE_UTF8MB4_BIN);

  ObFTDictDesc stopword_dict_desc("stopword",
                                  ObFTDictType::DICT_IK_STOP,
                                  ObCharsetType::CHARSET_UTF8MB4,
                                  ObCollationType::CS_TYPE_UTF8MB4_BIN);

  if (should_read_newest_table()) {
    cache_main_.reset();
    cache_quan_.reset();
    if (OB_FAIL(build_cache(main_dict_desc, cache_main_))) {
      LOG_WARN("Failed to read newest main table", K(ret));
    } else if (OB_FAIL(build_cache(quan_dict_desc, cache_quan_))) {
      LOG_WARN("Failed to read newest quan table", K(ret));
    }
  } else {
    if (OB_FAIL(init_single_dict(main_dict_desc, cache_main_))) {
      LOG_WARN("Failed to init main dict", K(ret));
    } else if (OB_FAIL(init_single_dict(quan_dict_desc, cache_quan_))) {
      LOG_WARN("Failed to init quantifier dict", K(ret));
    } else if (OB_FAIL(init_single_dict(stopword_dict_desc, cache_stop_))) {
      LOG_WARN("Failed to init stopword dict", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    // already logged.
  } else if (OB_FAIL(build_dict_from_cache(main_dict_desc, cache_main_, dict_main_))) {
    LOG_WARN("Failed to build dict main", K(ret));
  } else if (OB_FAIL(build_dict_from_cache(quan_dict_desc, cache_quan_, dict_quan_))) {
    LOG_WARN("Failed to build dict quantifier", K(ret));
  } else if (OB_FAIL(build_dict_from_cache(stopword_dict_desc, cache_stop_, dict_stop_))) {
    LOG_WARN("Failed to build dict stopword", K(ret));
  }

  return ret;
}

int ObIKFTParser::init_single_dict(ObFTDictDesc desc, ObFTCacheRangeContainer &container)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(load_cache(desc, container))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      if (OB_FAIL(build_cache(desc, container))) {
        LOG_WARN("Failed to read newest main table", K(ret));
      }
    } else {
      LOG_WARN("Failed to load cache", K(ret));
    }
  }
  return ret;
}

int ObIKFTParser::init_ctx(const ObFTParserParam &param)
{
  int ret = OB_SUCCESS;

  if (coll_type_ == common::CS_TYPE_INVALID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Illegal collation type", K(ret));
  } else if (OB_ISNULL(ctx_ = static_cast<TokenizeContext *>(
                           allocator_.alloc(sizeof(TokenizeContext))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to alloc ctx", K(ret));
  } else if (FALSE_IT(new (ctx_) TokenizeContext(coll_type_,
                                                 allocator_,
                                                 param.fulltext_,
                                                 param.ft_length_,
                                                 param.ik_param_.mode_
                                                     == ObFTIKParam::Mode::SMART))) {
  } else if (OB_FAIL(ctx_->init())) {
    LOG_WARN("Failed to init ctx", K(ret));
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
  if (OB_ISNULL(letter_seg = static_cast<ObIKLetterProcessor *>(
                    allocator_.alloc(sizeof(ObIKLetterProcessor))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to alloc letter segmenter", K(ret));
  } else if (FALSE_IT(new (letter_seg) ObIKLetterProcessor())) {
  } else if (OB_ISNULL(dict_quan_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Dict quan is null.", K(ret));
  } else if (OB_ISNULL(cnqsg = static_cast<ObIKQuantifierProcessor *>(
                           allocator_.alloc(sizeof(ObIKQuantifierProcessor))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to alloc cn quantifier segmenter", K(ret));
  } else if (FALSE_IT(new (cnqsg) ObIKQuantifierProcessor(*dict_quan_, allocator_))) {
  } else if (OB_ISNULL(dict_main_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Dict main is null.", K(ret));
  } else if (OB_ISNULL(cjksg = static_cast<ObIKCJKProcessor *>(
                           allocator_.alloc(sizeof(ObIKCJKProcessor))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to alloc cjk segmenter", K(ret));
  } else if (FALSE_IT(new (cjksg) ObIKCJKProcessor(*dict_main_, allocator_))) {
  } else if (OB_ISNULL(surrogate_seg = static_cast<ObIKSurrogateProcessor *>(
                           allocator_.alloc(sizeof(ObIKSurrogateProcessor))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to alloc surrogate segmenter", K(ret));
  } else if (FALSE_IT(new (surrogate_seg) ObIKSurrogateProcessor())) {
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
    if (!OB_ISNULL(letter_seg)) {
      letter_seg->~ObIKLetterProcessor();
      allocator_.free(letter_seg);
    }
    if (!OB_ISNULL(cjksg)) {
      cjksg->~ObIKCJKProcessor();
      allocator_.free(cjksg);
    }
    if (!OB_ISNULL(cnqsg)) {
      cnqsg->~ObIKQuantifierProcessor();
      allocator_.free(cnqsg);
    }
    if (!OB_ISNULL(surrogate_seg)) {
      surrogate_seg->~ObIKSurrogateProcessor();
      allocator_.free(surrogate_seg);
    }
  }
  return ret;
}

void ObIKFTParser::reset()
{
  if (!OB_ISNULL(ctx_)) {
    ctx_->~TokenizeContext();
    allocator_.free(ctx_);
  }

  for (ObIIKProcessor *segmenter : segmenters_) {
    if (!OB_ISNULL(segmenter)) {
      segmenter->~ObIIKProcessor();
      allocator_.free(segmenter);
    }
  }
  segmenters_.clear();

  cache_main_.reset();
  cache_quan_.reset();
  cache_stop_.reset();

  if (!OB_ISNULL(dict_main_)) {
    dict_main_->~ObIFTDict();
    allocator_.free(dict_main_);
  }
  if (!OB_ISNULL(dict_quan_)) {
    dict_quan_->~ObIFTDict();
    allocator_.free(dict_quan_);
  }
  if (!OB_ISNULL(dict_stop_)) {
    dict_stop_->~ObIFTDict();
    allocator_.free(dict_stop_);
  }

  is_inited_ = false;
}

bool ObIKFTParser::should_read_newest_table() const { return false; }
int ObIKFTParser::build_cache(const ObFTDictDesc &desc, ObFTCacheRangeContainer &container)
{
  int ret = OB_SUCCESS;
  const uint64_t dict_name = static_cast<uint64_t>(desc.type_); // fake name by id;
  ObFTDictInfo info;

  container.reset();
  if (OB_FAIL(ObFTRangeDict::build_cache(desc, container))) {
    LOG_WARN("Failed to build cache", K(ret));
  } else if (FALSE_IT(info.range_count_ = container.get_handles().size())) {
  } else if (OB_FAIL(hub_->put_dict_info(dict_name, info))) {
    LOG_WARN("Failed to put dict info", K(ret));
  }

  return ret;
}

int ObIKFTParser::load_cache(const ObFTDictDesc &desc, ObFTCacheRangeContainer &container)
{
  int ret = OB_SUCCESS;
  ObFTDictInfo info;
  container.reset();
  const uint64_t dict_name = static_cast<uint64_t>(desc.type_); // fake name by id;
  if (OB_FAIL(hub_->get_dict_info(dict_name, info)) && OB_ENTRY_NOT_EXIST != ret) {
    LOG_WARN("Failed to get dict info", K(ret));
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    // dict not exist, make new one, by caller
  } else if (OB_FAIL(ObFTRangeDict::try_load_cache(desc, info.range_count_, container))
             && OB_ENTRY_NOT_EXIST != ret) {
    LOG_WARN("Failed to load cache", K(ret));
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    // dict not exist, make new one, by caller
  }
  return ret;
}

int ObIKFTParser::build_dict_from_cache(const ObFTDictDesc &desc,
                                        ObFTCacheRangeContainer &container,
                                        ObIFTDict *&dict)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(hub_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Hub is null", K(ret));
  } else if (OB_ISNULL(dict
                       = static_cast<ObFTRangeDict *>(allocator_.alloc(sizeof(ObFTRangeDict))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to alloc dict", K(ret));
  } else if (OB_FALSE_IT(new (dict) ObFTRangeDict(allocator_, &container, desc))) {
  } else if (OB_FAIL(dict->init())) {
    LOG_WARN("Failed to init dict", K(ret));
  }
  return ret;
}

} //  namespace storage
} //  namespace oceanbase
