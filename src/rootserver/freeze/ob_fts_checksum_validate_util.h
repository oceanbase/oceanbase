//Copyright (c) 2024 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OB_ROOTSERVER_FREEZE_FTS_CHECKSUM_VALIDATE_UTIL_H_
#define OB_ROOTSERVER_FREEZE_FTS_CHECKSUM_VALIDATE_UTIL_H_
#include "lib/container/ob_se_array.h"
#include "share/ob_delegate.h"
namespace oceanbase
{
namespace share {
} // namespace share
namespace rootserver
{

struct ObFTSIndexInfo
{
  ObFTSIndexInfo()
    : fts_index_id_(0),
      doc_word_index_id_(0)
  {}
  ObFTSIndexInfo(const int64_t index_id, const int64_t doc_word_index_id)
    : fts_index_id_(index_id),
      doc_word_index_id_(doc_word_index_id)
  {}
  TO_STRING_KV(K_(fts_index_id), K_(doc_word_index_id));
  int64_t fts_index_id_;
  int64_t doc_word_index_id_;
};

struct ObFTSGroup
{
  ObFTSGroup();
  ~ObFTSGroup() {}
  bool is_valid() const
  {
    return data_table_id_ > 0 && rowkey_doc_index_id_ > 0 && doc_rowkey_index_id_ > 0
      && index_info_.count() >= 0;
  }
  CONST_DELEGATE_WITH_RET(index_info_, count, int64_t);
  CONST_DELEGATE_WITH_RET(index_info_, at, const ObFTSIndexInfo&);
  DELEGATE_WITH_RET(index_info_, push_back, int);
  TO_STRING_KV(K_(data_table_id), K_(rowkey_doc_index_id), K(doc_rowkey_index_id_),
    "index_cnt", index_info_.count(), K_(index_info));
  int64_t data_table_id_;
  int64_t rowkey_doc_index_id_;
  int64_t doc_rowkey_index_id_;
  ObSEArray<ObFTSIndexInfo, 4> index_info_;
};

struct ObFTSGroupArray
{
  ObFTSGroupArray();
  ~ObFTSGroupArray() {}
  CONST_DELEGATE_WITH_RET(fts_groups_, count, int64_t);
  CONST_DELEGATE_WITH_RET(fts_groups_, at, const ObFTSGroup&);
  DELEGATE_WITH_RET(fts_groups_, push_back, int);
  DELEGATE_WITH_RET(fts_groups_, reuse, void);
  bool need_check_fts() const;
  TO_STRING_KV(K_(fts_groups));
  common::ObSEArray<ObFTSGroup, 4> fts_groups_;
};


} // namespace rootserver
} // namespace oceanbase

#endif // OB_ROOTSERVER_FREEZE_FTS_CHECKSUM_VALIDATE_UTIL_H_
