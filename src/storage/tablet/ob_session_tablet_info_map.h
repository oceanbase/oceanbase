/*
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
#ifndef OCEANBASE_STORAGE_TABLET_OB_SESSION_TABLET_INFO_MAP_H
#define OCEANBASE_STORAGE_TABLET_OB_SESSION_TABLET_INFO_MAP_H

#include "lib/hash/ob_hashmap.h"
#include "share/ob_ls_id.h"
#include "common/ob_tablet_id.h"

namespace oceanbase
{
namespace storage
{

struct ObGTTTabletInfo final
{
public:
  OB_UNIS_VERSION(1);
public:
  ObGTTTabletInfo()
    : table_id_(OB_INVALID_ID),
      gtt_session_scope_unique_id_(INT64_MAX),
      gtt_trans_scope_unique_id_(INT64_MAX),
      session_id_(0)
  {}
  explicit ObGTTTabletInfo(const uint64_t table_id, const int64_t gtt_session_scope_unique_id,
      const int64_t gtt_trans_scope_unique_id, const uint32_t session_id)
    : table_id_(table_id),
      gtt_session_scope_unique_id_(gtt_session_scope_unique_id),
      gtt_trans_scope_unique_id_(gtt_trans_scope_unique_id),
      session_id_(session_id)
  {}
  ~ObGTTTabletInfo() = default;
  bool is_valid() const
  {
    return table_id_ != OB_INVALID_ID && gtt_session_scope_unique_id_ != INT64_MAX && gtt_trans_scope_unique_id_ != INT64_MAX && session_id_ != 0;
  }
  TO_STRING_KV(K_(table_id), K_(gtt_session_scope_unique_id), K_(gtt_trans_scope_unique_id), K_(session_id));
public:
  uint64_t table_id_;
  int64_t gtt_session_scope_unique_id_;
  int64_t gtt_trans_scope_unique_id_;
  uint32_t session_id_;
};

// for session level temporary table, sequence is gtt_session_scope_unique_id_
// for transaction level temporary table, sequence is gtt_trans_scope_unique_id_
struct ObSessionTabletInfoKey final
{
public:
  ObSessionTabletInfoKey()
    : table_id_(OB_INVALID_ID),
      sequence_(INT64_MAX),
      session_id_(0)
  {}
  explicit ObSessionTabletInfoKey(const uint64_t table_id, const int64_t sequence,
    const uint32_t session_id)
    : table_id_(table_id),
      sequence_(sequence),
      session_id_(session_id)
  {}
  ~ObSessionTabletInfoKey() = default;
  bool is_valid() const
  {
    return table_id_ != OB_INVALID_ID && sequence_ != INT64_MAX && session_id_ != 0;
  }
  TO_STRING_KV(K_(table_id), K_(sequence), K_(session_id));
public:
  uint64_t table_id_;
  int64_t sequence_;
  uint32_t session_id_;
};

struct ObSessionTabletInfo final
{
public:
  OB_UNIS_VERSION(1);

public:
  ObSessionTabletInfo()
    : table_id_(OB_INVALID_ID),
      sequence_(INT64_MAX),
      session_id_(UINT32_MAX),
      ls_id_(),
      tablet_id_(),
      transfer_seq_(0),
      is_creator_(false)
  {}
  explicit ObSessionTabletInfo(const common::ObTabletID &tablet_id,
    const share::ObLSID &ls_id, const uint64_t table_id, const int64_t sequence,
    const uint32_t session_id, const int64_t transfer_seq)
    : table_id_(table_id),
      sequence_(sequence),
      session_id_(session_id),
      ls_id_(ls_id),
      tablet_id_(tablet_id),
      transfer_seq_(transfer_seq),
      is_creator_(false)
  {}
  ~ObSessionTabletInfo() = default;
  bool is_valid() const
  {
     return table_id_ != OB_INVALID_ID
            && sequence_ != INT64_MAX
            && session_id_ != UINT32_MAX
            && ls_id_.is_valid()
            && tablet_id_.is_valid();
  }
  inline const common::ObTabletID &get_tablet_id() const { return tablet_id_; }
  inline const share::ObLSID &get_ls_id() const { return ls_id_; }
  inline uint64_t get_table_id() const { return table_id_; }
  inline int64_t get_sequence() const { return sequence_; }
  inline uint32_t get_session_id() const { return session_id_; }
  inline int64_t get_transfer_seq() const { return transfer_seq_; }
  int init(const common::ObTabletID &tablet_id,
        const share::ObLSID &ls_id, const uint64_t table_id,
        const int64_t sequence, const uint32_t session_id,
        const int64_t transfer_seq);
  void reset()
  {
    table_id_ = OB_INVALID_ID;
    sequence_ = INT64_MAX;
    session_id_ = UINT32_MAX;
    ls_id_.reset();
    tablet_id_.reset();
    transfer_seq_ = 0;
    is_creator_ = false;
  }
  TO_STRING_KV(K_(table_id),
               K_(sequence),
               K_(session_id),
               K_(ls_id),
               K_(tablet_id),
               K_(transfer_seq),
               K_(is_creator));
public:
  uint64_t table_id_;
  int64_t sequence_;
  uint32_t session_id_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  int64_t transfer_seq_;
  bool is_creator_;
};

class ObSessionTabletInfoMap final
{
public:
  OB_UNIS_VERSION(1);
public:
  ObSessionTabletInfoMap();
  ~ObSessionTabletInfoMap() = default;
  int add_session_tablet(
      const common::ObIArray<uint64_t> &table_ids,
      const int64_t sequence,
      const uint32_t session_id);
  int get_session_tablet(
      const ObSessionTabletInfoKey &session_tablet_info_key,
      ObSessionTabletInfo &session_tablet_info);
  int get_session_tablet_if_not_exist_add(
      const ObSessionTabletInfoKey &session_tablet_info_key,
      ObSessionTabletInfo &session_tablet_info);
  int remove_session_tablet(const uint64_t table_id);
  void reset() { tablet_infos_.reset(); }
  bool is_empty() const { return tablet_infos_.count() == 0; }
  int get_table_ids_by_session_id_and_sequence(
      const uint32_t session_id,
      const int64_t sequence,
      common::ObIArray<uint64_t> &table_ids);
  // if not exist, return OB_SUCCESS
  // if all exist, return OB_ENTRY_EXIST
  // else return OB_ERR_UNEXPECTED
  int check_session_tablet_by_table_id_from_inner_table(
      const common::ObIArray<uint64_t> &table_ids,
      const uint32_t session_id,
      const int64_t sequence);
  TO_STRING_KV(K_(tablet_infos));
private:
  const static int64_t MAX_SESSION_TABLET_COUNT = 64;
  int inner_get_session_tablet(
      const uint64_t table_id,
      const int64_t sequence,
      const int32_t session_id,
      ObSessionTabletInfo &session_tablet_info);
private:
  common::ObSEArray<ObSessionTabletInfo, MAX_SESSION_TABLET_COUNT> tablet_infos_;
  lib::ObMutex mutex_;
  DISALLOW_COPY_AND_ASSIGN(ObSessionTabletInfoMap);
};

}
}

#endif // OCEANBASE_STORAGE_TABLET_OB_SESSION_TABLET_INFO_MAP_H