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

#pragma once

#include "oceanbase/ob_plugin_ftparser.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/oblog/ob_log_module.h"
#include "plugin/adaptor/ob_plugin_adaptor.h"
#include "plugin/interface/ob_plugin_ftparser_intf.h"

class ObCharsetInfo;

namespace oceanbase {
namespace plugin {

class ObPluginParam;

class ObTokenIteratorAdaptor final : public ObITokenIterator
{
public:
  ObTokenIteratorAdaptor(const ObPluginFTParser &ftparser, ObFTParserParam *param);
  virtual ~ObTokenIteratorAdaptor() = default;

  virtual int get_next_token(
      const char *&word,
      int64_t &word_len,
      int64_t &char_cnt,
      int64_t &word_freq) override;

  TO_STRING_KV(KP(param_));

private:
  const ObPluginFTParser &ftparser_;
  ObFTParserParam        *param_ = nullptr;
};

class ObFtParserAdaptor final : public ObIFTParserDesc
{
public:
  ObFtParserAdaptor() = default;
  virtual ~ObFtParserAdaptor() = default;

  int init_adaptor(const ObPluginFTParser &ftparser, int64_t ftparser_sizeof);

  virtual int init(ObPluginParam *param) override;
  virtual int deinit(ObPluginParam *param) override;

  virtual int segment(ObFTParserParam *param, ObITokenIterator *&iter) const override;
  virtual void free_token_iter(ObFTParserParam *param, ObITokenIterator *&iter) const override;
  virtual int get_add_word_flag(storage::ObAddWordFlag &flag) const override;

private:
  bool             inited_ = false;
  ObPluginFTParser ftparser_;
};

} // namespace plugin
} // namespace oceanbase
