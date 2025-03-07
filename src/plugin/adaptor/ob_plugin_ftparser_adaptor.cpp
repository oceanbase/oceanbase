/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SHARE

#include "plugin/adaptor/ob_plugin_ftparser_adaptor.h"
#include "storage/fts/ob_fts_struct.h"

using namespace oceanbase::storage;

namespace oceanbase {
namespace plugin {

ObTokenIteratorAdaptor::ObTokenIteratorAdaptor(const ObPluginFTParser &ftparser, ObFTParserParam *param)
    : ftparser_(ftparser), param_(param)
{}

int ObTokenIteratorAdaptor::get_next_token(const char *&word, int64_t &word_len, int64_t &char_cnt, int64_t &word_freq)
{
  int ret = OB_SUCCESS;
  ret = ftparser_.next_token(param_, const_cast<char **>(&word), &word_len, &char_cnt, &word_freq);
  if (OB_FAIL(ret) && OB_ITER_END != ret) {
    LOG_WARN("next token return fail", K(ret));
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////
int ObFtParserAdaptor::init_adaptor(const ObPluginFTParser &ftparser, int64_t ftparser_sizeof)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
  } else if (ftparser_sizeof <= 0 || ftparser_sizeof > sizeof(ftparser_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ftparser sizeof", K(ftparser_sizeof), K(sizeof(ftparser_)));
  } else {
    inited_ = true;
    memset(&ftparser_, 0, sizeof(ftparser_));
    memcpy(&ftparser_, &ftparser, ftparser_sizeof);
  }
  return ret;
}

int ObFtParserAdaptor::init(ObPluginParam *param)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(param)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(param));
  } else if (OB_NOT_NULL(ftparser_.init) && OB_FAIL(ftparser_.init(param))) {
    LOG_WARN("failed to call ftparser init", K(ret), KPC(param));
  }
  return ret;
}

int ObFtParserAdaptor::deinit(ObPluginParam *param)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(param)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(param));
  } else if (OB_NOT_NULL(ftparser_.deinit) && OB_FAIL(ftparser_.deinit(param))) {
    LOG_WARN("failed to call ftparser deinit", K(ret), KPC(param));
  }
  return ret;
}

int ObFtParserAdaptor::segment(ObFTParserParam *param, ObITokenIterator *&iter) const
{
  int ret = OB_SUCCESS;
  ObTokenIteratorAdaptor *iter_adaptor = nullptr;
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(param) || OB_ISNULL(param->allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(param));
  } else if (OB_ISNULL(ftparser_.scan_begin) || OB_ISNULL(ftparser_.next_token)) {
    ret = OB_NOT_SUPPORTED;
  } else if (OB_FAIL(ftparser_.scan_begin(param))) {
    LOG_WARN("failed to call ftparser.init", K(ret));
  } else if (OB_ISNULL(iter_adaptor = static_cast<ObTokenIteratorAdaptor *>(param->allocator_->alloc(sizeof(ObTokenIteratorAdaptor))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (FALSE_IT(new (iter_adaptor) ObTokenIteratorAdaptor(ftparser_, param))) {
  } else if (FALSE_IT(iter = iter_adaptor)) {
  }
  return ret;
}

void ObFtParserAdaptor::free_token_iter(ObFTParserParam *param, ObITokenIterator *&iter) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(param) || OB_ISNULL(iter)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_NOT_NULL(ftparser_.scan_end)) {
    int tmp_ret = ftparser_.scan_end(param);
    if (tmp_ret != OB_SUCCESS) {
      LOG_WARN("failed to call ftparser.scan_end(ignore)", K(tmp_ret));
    }
  } else {
    ObTokenIteratorAdaptor *iter_adaptor = static_cast<ObTokenIteratorAdaptor *>(iter);
    iter_adaptor->~ObTokenIteratorAdaptor();
    param->allocator_->free(iter_adaptor);
    iter = nullptr;
  }
}

int ObFtParserAdaptor::get_add_word_flag(ObAddWordFlag &flag) const
{
  int ret = OB_SUCCESS;
  uint64_t iflag = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(ftparser_.get_add_word_flag)) {
    ret = OB_NOT_SUPPORTED;
  } else if (OB_FAIL(ftparser_.get_add_word_flag(&iflag))) {
    LOG_WARN("failed to get add_word_flag", K(ret));
  } else {
    flag.set_flag(iflag);
  }
  return ret;
}

} // namespace plugin
} // namespace oceanbase
