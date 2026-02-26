/**
 * Copyright (c) 2022 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_OBSERVER_OB_TABLE_FTS_CONTEXT_H_
#define OCEANBASE_OBSERVER_OB_TABLE_FTS_CONTEXT_H_

#include "lib/string/ob_string.h"
namespace oceanbase
{
namespace  sql {
class ObTextRetrievalInfo;
}

namespace table
{
struct ObTableFtsCtx
{
public:
  ObTableFtsCtx() {
    reset();
  }
  ~ObTableFtsCtx() {}
  void reset() {
    rowkey_doc_tid_ = OB_INVALID_ID;
    doc_rowkey_tid_ = OB_INVALID_ID;
    rowkey_doc_schema_ = nullptr;
    doc_rowkey_schema_ = nullptr;
    is_text_retrieval_scan_ = false;
    need_tsc_with_doc_id_ = false;
    text_retrieval_info_ = nullptr;
    search_text_.reset();
  }

  TO_STRING_KV(K_(rowkey_doc_tid),
               K_(doc_rowkey_tid),
               KP_(rowkey_doc_schema),
               KP_(doc_rowkey_schema),
               K_(is_text_retrieval_scan),
               K_(search_text));

  OB_INLINE bool is_tsc_with_doc_id() const { return need_tsc_with_doc_id_; }
  OB_INLINE uint64_t get_rowkey_doc_tid() const { return rowkey_doc_tid_; }
  OB_INLINE uint64_t get_doc_rowkey_tid() const { return doc_rowkey_tid_; }
  OB_INLINE const share::schema::ObTableSchema* get_rowkey_doc_schema() const { return rowkey_doc_schema_; }
  OB_INLINE const share::schema::ObTableSchema* get_doc_rowkey_schema() const { return doc_rowkey_schema_; }
  OB_INLINE bool is_text_retrieval_scan() const { return is_text_retrieval_scan_; }
  OB_INLINE sql::ObTextRetrievalInfo* get_text_retrieval_info() const { return text_retrieval_info_; }
  OB_INLINE ObString& get_search_text() { return search_text_; }
public:
  uint64_t rowkey_doc_tid_;
  uint64_t doc_rowkey_tid_;
  const share::schema::ObTableSchema *rowkey_doc_schema_;
  const share::schema::ObTableSchema *doc_rowkey_schema_;
  bool is_text_retrieval_scan_;
  bool need_tsc_with_doc_id_;
  sql::ObTextRetrievalInfo *text_retrieval_info_;
  ObString search_text_;
};

} // end namespace table
} // end namespace oceanbase

#endif /* OCEANBASE_OBSERVER_OB_TABLE_FTS_CG_SERVICE_H_ */