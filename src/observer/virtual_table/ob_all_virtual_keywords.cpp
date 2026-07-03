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

#include "observer/virtual_table/ob_all_virtual_keywords.h"

using namespace oceanbase;
using namespace oceanbase::observer;
using namespace oceanbase::common;

namespace
{
struct KeywordSource {
  const NonReservedKeyword *(*getter_func_)(int32_t *count);  // keyword array getter function
  const char *keyword_type_;                                  // 'SQL' or 'PL'
  int is_reserved_;                                           // 1: reserved, 0: non-reserved

  KeywordSource(const NonReservedKeyword *(*getter_func)(int32_t *count),
                const char *keyword_type,
                int is_reserved)
      : getter_func_(getter_func), keyword_type_(keyword_type), is_reserved_(is_reserved)
  {}
  TO_STRING_KV(KP_(keyword_type), K_(is_reserved));
};

const KeywordSource MYSQL_KEYWORD_SOURCES[] = {
  {get_mysql_reserved_keywords, "SQL", 1},        // 0: SQL reserved
  {get_mysql_non_reserved_keywords, "SQL", 0},    // 1: SQL non-reserved
  {get_mysql_pl_reserved_keywords, "PL", 1},      // 2: PL reserved
  {get_mysql_pl_non_reserved_keywords, "PL", 0},  // 3: PL non-reserved
};

#if defined(OB_BUILD_ORACLE_PARSER) && defined(OB_BUILD_ORACLE_PL)
const KeywordSource ORACLE_KEYWORD_SOURCES[] = {
  {get_oracle_reserved_keywords, "SQL", 1},        // 0: SQL reserved
  {get_oracle_non_reserved_keywords, "SQL", 0},    // 1: SQL non-reserved
  {get_oracle_pl_reserved_keywords, "PL", 1},      // 2: PL reserved
  {get_oracle_pl_non_reserved_keywords, "PL", 0},  // 3: PL non-reserved
};
#endif

} // namespace

ObAllVirtualKeywords::ObAllVirtualKeywords()
    : ObVirtualTableScannerIterator(),
      iter_idx_(0),
      is_inited_(false)
{
  MEMSET(keyword_infos_, 0, sizeof(keyword_infos_));
}

ObAllVirtualKeywords::~ObAllVirtualKeywords()
{
  reset();
}

void ObAllVirtualKeywords::reset()
{
  iter_idx_ = 0;
  is_inited_ = false;
  MEMSET(keyword_infos_, 0, sizeof(keyword_infos_));
  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualKeywords::inner_open()
{
  int ret = OB_SUCCESS;
#if defined(OB_BUILD_ORACLE_PARSER) && defined(OB_BUILD_ORACLE_PL)
  const KeywordSource *const sources = lib::is_oracle_mode() ? ORACLE_KEYWORD_SOURCES
                                                             : MYSQL_KEYWORD_SOURCES;
#else
  // Oracle mode is not supported in CE builds; oracle keyword functions are unavailable.
  const KeywordSource *const sources = MYSQL_KEYWORD_SOURCES;
#endif
  iter_idx_ = 0;
  is_inited_ = false;
  MEMSET(keyword_infos_, 0, sizeof(keyword_infos_));
  // fetch keyword info arrays from the sources
  for (int64_t i = 0; OB_SUCC(ret) && i < KEYWORD_SEGMENT_COUNT; ++i) {
    KeywordInfo &info = keyword_infos_[i];
    const KeywordSource &source = sources[i];
    info.keywords_ = source.getter_func_(&info.count_);
    info.keyword_type_ = source.keyword_type_;
    info.is_reserved_ = source.is_reserved_;
    if (info.count_ < 0
        || OB_ISNULL(info.keyword_type_)
        || (info.count_ > 0 && OB_ISNULL(info.keywords_))) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "invalid keyword metadata", KR(ret), K(i), K(source), K(info));
    }
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

int ObAllVirtualKeywords::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(cur_row_.cells_) || !is_inited_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "virtual table is not initialized", KR(ret), K_(is_inited));
  }
  if (OB_SUCC(ret)) {
    const NonReservedKeyword *keyword = NULL;
    const char *keyword_type = NULL;
    int is_reserved = 0;
    int64_t keyword_idx = iter_idx_;
    // loop through all keyword arrays to get the next keyword
    for (int64_t i = 0; OB_SUCC(ret) && i < KEYWORD_SEGMENT_COUNT; ++i) {
      if (keyword_infos_[i].count_ < 0
          || OB_ISNULL(keyword_infos_[i].keyword_type_)
          || (keyword_infos_[i].count_ > 0 && OB_ISNULL(keyword_infos_[i].keywords_))) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "invalid keyword info", KR(ret), K(i), K(keyword_infos_[i]));
      } else if (keyword_idx < keyword_infos_[i].count_) {
        // the current keyword array has not been exhausted yet
        // get the keyword and break loop
        keyword = keyword_infos_[i].keywords_ + keyword_idx;
        keyword_type = keyword_infos_[i].keyword_type_;
        is_reserved = keyword_infos_[i].is_reserved_;
        break;
      } else {
        // the keyword array has been exhausted
        // adjust keyword_idx and continue loop to search the next keyword array
        keyword_idx -= keyword_infos_[i].count_;
      }
    }  // end for-loop

    if (OB_SUCC(ret)) {
      if (NULL == keyword) {
        ret = OB_ITER_END;
      } else if (OB_ISNULL(keyword->keyword_name) || OB_ISNULL(keyword_type)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "invalid keyword metadata",
                   KR(ret), K_(iter_idx), KP(keyword_type), K(is_reserved));
      }
    }

    // fill the row with the keyword information
    if (OB_SUCC(ret)) {
      const ObCollationType collation_type = ObCharset::get_default_collation(ObCharset::get_default_charset());
      for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); ++i) {
        const uint64_t col_id = output_column_ids_.at(i);
        switch (col_id) {
          case KEYWORD_NAME:
            cur_row_.cells_[i].set_varchar(keyword->keyword_name);
            cur_row_.cells_[i].set_collation_type(collation_type);
            break;
          case KEYWORD_LENGTH:
            cur_row_.cells_[i].set_int(static_cast<int64_t>(strlen(keyword->keyword_name)));
            break;
          case KEYWORD_TYPE:
            cur_row_.cells_[i].set_varchar(keyword_type);
            cur_row_.cells_[i].set_collation_type(collation_type);
            break;
          case RESERVED:
            cur_row_.cells_[i].set_int(is_reserved);
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "invalid col_id", KR(ret), K(col_id));
            break;
        }
      }

      if (OB_SUCC(ret)) {
        ++iter_idx_;
        row = &cur_row_;
      }
    }
  }

  return ret;
}
