/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_ALL_VIRTUAL_KEYWORDS_H
#define OB_ALL_VIRTUAL_KEYWORDS_H

#include "share/ob_virtual_table_scanner_iterator.h"
#include "sql/parser/ob_non_reserved_keywords.h"

namespace oceanbase
{
namespace observer
{

class ObAllVirtualKeywords : public common::ObVirtualTableScannerIterator
{
public:
  ObAllVirtualKeywords();
  virtual ~ObAllVirtualKeywords();

  virtual int inner_open() override;
  virtual int inner_get_next_row(common::ObNewRow *&row) override;
  virtual void reset() override;

private:
  enum KEYWORDS_COLUMN
  {
    KEYWORD_NAME = common::OB_APP_MIN_COLUMN_ID,
    KEYWORD_LENGTH,
    KEYWORD_TYPE,
    RESERVED
  };
  struct KeywordInfo {
    const NonReservedKeyword *keywords_;
    int32_t count_;
    const char *keyword_type_;  // 'SQL' or 'PL'
    int is_reserved_;           // 1: reserved, 0: non-reserved

    KeywordInfo() : keywords_(NULL), count_(0), keyword_type_(NULL), is_reserved_(0) {}
    TO_STRING_KV("keywords_is_null", (NULL == keywords_),
                 K_(count), KP(keyword_type_), K_(is_reserved));
  };

  // 0: SQL reserved, 1: SQL non-reserved, 2: PL reserved, 3: PL non-reserved
  static const int KEYWORD_SEGMENT_COUNT = 4;

  int64_t iter_idx_;
  bool is_inited_;
  KeywordInfo keyword_infos_[KEYWORD_SEGMENT_COUNT];

private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualKeywords);
};

} // namespace observer
} // namespace oceanbase

#endif /* OB_ALL_VIRTUAL_KEYWORDS_H */
