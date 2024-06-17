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

#ifndef OCEANBASE_DUP_TABLE_TABLETS_H
#define OCEANBASE_DUP_TABLE_TABLETS_H

#include "common/ob_tablet_id.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/hash/ob_hashset.h"
#include "lib/list/ob_dlist.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/queue/ob_fixed_queue.h"
#include "ob_dup_table_base.h"
#include "ob_dup_table_ts_sync.h"

#define READABLE_DLIST_FOREACH_X(curr, dlist, extra_condition)     \
  for (;OB_NOT_NULL(curr) && curr != (dlist).get_header()         \
        && (extra_condition);                                     \
        curr = curr->get_next())

namespace oceanbase
{

namespace logservice
{
class ObLogHandler;
}
namespace transaction
{
class ObDupTableLSHandler;

//**********************************************************************
//******  ObLSDupTabletsMgr
//**********************************************************************

struct DupTabletInfo
{
  int64_t update_dup_schema_ts_;

  void reset() { update_dup_schema_ts_ = 0; }

  DupTabletInfo() { reset(); }

  TO_STRING_KV(K(update_dup_schema_ts_));
};

typedef common::hash::
    ObHashMap<common::ObTabletID, DupTabletInfo, common::hash::NoPthreadDefendMode>
        DupTabletIdMap;

enum class DupTabletSetChangeFlag
{
  UNKNOWN = 0,
  UNUSED,
  TEMPORARY,
  CHANGE_LOGGING,
  CONFIRMING,
  CONFIRMED,

};

enum class DupTableRelatedSetOpType
{
  INVALID = 0,
  OLD_GC,
  NEW_GC,
  MEGRE_READABLE,
  OPERATED_NEW_GC, // this type means this set in operate
};

static const char *get_dup_tablet_flag_str(const DupTabletSetChangeFlag &flag)
{
  const char *flag_str = nullptr;

  switch (flag) {
  case DupTabletSetChangeFlag::UNKNOWN: {
    flag_str = "UNKNOWN";
    break;
  }

  case DupTabletSetChangeFlag::UNUSED: {
    flag_str = "UNUSED";
    break;
  }
  case DupTabletSetChangeFlag::TEMPORARY: {
    flag_str = "TEMPORARY";
    break;
  }
  case DupTabletSetChangeFlag::CHANGE_LOGGING: {
    flag_str = "CHANGE_LOGGING";
    break;
  }
  case DupTabletSetChangeFlag::CONFIRMING: {
    flag_str = "CONFIRMING";
    break;
  }
  case DupTabletSetChangeFlag::CONFIRMED: {
    flag_str = "CONFIRMED";
    break;
  }
  };

  return flag_str;
}

class DupTabletSetChangeStatus
{
  OB_UNIS_VERSION(1);

public:

  void init()
  {
    reset();
    flag_ = DupTabletSetChangeFlag::UNUSED;
  }

  void reset()
  {
    flag_ = DupTabletSetChangeFlag::UNKNOWN;
    tablet_change_scn_.reset();
    need_confirm_scn_.reset();
    readable_version_.set_min();
    trx_ref_ = 0;
    is_logging = false;
  }

  DupTabletSetChangeStatus() { reset(); }
  ~DupTabletSetChangeStatus() { reset(); }

  const share::SCN &get_readable_version() const { return readable_version_; }
  const share::SCN &get_tablet_change_scn() const { return tablet_change_scn_; }
  const share::SCN &get_need_confirm_scn() const { return need_confirm_scn_; }
  const DupTabletSetChangeFlag &get_tablet_set_change_flag() const { return flag_; }
  int64_t get_trx_ref() const { return trx_ref_; }

  bool is_valid() const { return flag_ != DupTabletSetChangeFlag::UNKNOWN; }
  bool need_log() const
  {
    return flag_ == DupTabletSetChangeFlag::TEMPORARY
           || flag_ == DupTabletSetChangeFlag::CHANGE_LOGGING
           || flag_ == DupTabletSetChangeFlag::CONFIRMING
           || flag_ == DupTabletSetChangeFlag::CONFIRMED;
    // TODO submit log if readable_version has changed.
  }

  bool need_reserve(const share::SCN &min_reserve_scn) const
  {
    return !tablet_change_scn_.is_valid()
           || (tablet_change_scn_.is_valid() && tablet_change_scn_ >= min_reserve_scn);
  }

  bool is_unlog() const { return !tablet_change_scn_.is_valid(); }
  bool is_free() const { return flag_ == DupTabletSetChangeFlag::UNUSED; }
  bool is_modifiable() const { return flag_ == DupTabletSetChangeFlag::TEMPORARY; }

  void set_logging() { is_logging = true; }
  void clean_logging() { is_logging = false; }
  bool check_logging() const { return is_logging; }

  bool is_change_logging() const { return flag_ == DupTabletSetChangeFlag::CHANGE_LOGGING; }
  bool is_confirming() const { return flag_ == DupTabletSetChangeFlag::CONFIRMING; }
  bool can_be_confirmed_anytime() const
  {
    return (trx_ref_ == 0 && readable_version_ >= need_confirm_scn_ && readable_version_.is_valid()
            && need_confirm_scn_.is_valid() && flag_ == DupTabletSetChangeFlag::CONFIRMING)
           || flag_ == DupTabletSetChangeFlag::CONFIRMED;
  }
  bool has_confirmed() const { return DupTabletSetChangeFlag::CONFIRMED == flag_; }

  void set_temporary() { flag_ = DupTabletSetChangeFlag::TEMPORARY; }
  void set_confirm_invalid() { need_confirm_scn_.set_max(); }
  int prepare_serialize()
  {
    int ret = OB_SUCCESS;
    if (DupTabletSetChangeFlag::TEMPORARY == flag_) {
      flag_ = DupTabletSetChangeFlag::CHANGE_LOGGING;
    }
    return ret;
  }

  int tablet_change_log_submitted(const share::SCN &tablet_change_scn, const bool submit_result)
  {
    int ret = OB_SUCCESS;
    if (!is_change_logging() || tablet_change_scn_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      DUP_TABLE_LOG(WARN, "unexpected flag after submitted", K(ret), KPC(this));
    } else if (submit_result) {
      tablet_change_scn_ = tablet_change_scn;
    } else {
      // do nothing
    }

    return ret;
  }

  int prepare_confirm(const share::SCN &tablet_change_scn, const bool sync_result)
  {
    int ret = OB_SUCCESS;
    if (!is_change_logging() || tablet_change_scn_ != tablet_change_scn) {
      ret = OB_ERR_UNEXPECTED;
      DUP_TABLE_LOG(WARN, "unexpected flag after submitted", K(ret), KPC(this));
    } else if (sync_result) {
      flag_ = DupTabletSetChangeFlag::CONFIRMING;
      need_confirm_scn_ = share::SCN::max(need_confirm_scn_, tablet_change_scn_);
    } else if (is_change_logging()) {
      tablet_change_scn_.set_invalid();
    }
    DUP_TABLE_LOG(DEBUG, "finish prepare confirm", K(tablet_change_scn), K(tablet_change_scn_));
    return ret;
  }

  int inc_active_tx()
  {
    int ret = OB_SUCCESS;
    trx_ref_++;
    return ret;
  }

  int dec_active_tx()
  {
    int ret = OB_SUCCESS;
    trx_ref_--;
    return ret;
  }

  int push_need_confirm_scn(const share::SCN &need_confirm_scn)
  {
    int ret = OB_SUCCESS;

    if (need_confirm_scn > need_confirm_scn_) {
      need_confirm_scn_ = need_confirm_scn;
    }

    return ret;
  }

  int push_readable_scn(const share::SCN &readable_scn)
  {
    int ret = OB_SUCCESS;

    if (readable_scn > need_confirm_scn_) {
      ret = OB_ERR_UNEXPECTED;
      DUP_TABLE_LOG(WARN, "unexpected need_confirm_scn_", K(ret), KPC(this));
    } else if (readable_scn > readable_version_) {
      readable_version_ = readable_scn;
    }

    return ret;
  }

  int try_set_confirmed(const bool can_be_confirmed)
  {
    int ret = OB_SUCCESS;

    if (can_be_confirmed) {
      if (can_be_confirmed_anytime()) {
        flag_ = DupTabletSetChangeFlag::CONFIRMED;
      } else {
        ret = OB_EAGAIN;
      }
    }
    return ret;
  }

  TO_STRING_KV(K(flag_),
               K(tablet_change_scn_),
               K(need_confirm_scn_),
               K(readable_version_),
               K(trx_ref_));

public:
  DupTabletSetChangeFlag flag_;
  share::SCN tablet_change_scn_;
  share::SCN need_confirm_scn_;
  share::SCN readable_version_;
  int64_t trx_ref_;
  bool is_logging;
};

struct DupTabletSetAttribute
{
  DupTabletSetCommonHeader common_header_;
  DupTabletSetChangeStatus change_status_;

  void reuse()
  {

    change_status_.init();
    change_status_.set_temporary();
    // common_header_.clean_sp_op();
    // common_header_.set_free();
    common_header_.reuse();
  }

  void reset()
  {
    common_header_.reset();
    change_status_.reset();
  }
  DupTabletSetAttribute(const uint64_t uid) : common_header_(uid), change_status_() { reuse(); }
  DupTabletSetAttribute() { reset(); }

  TO_STRING_KV(K(common_header_), K(change_status_));

  OB_UNIS_VERSION(1);
};

struct RelatedSetAttribute
{
  DupTabletSetCommonHeader related_common_header_;
  DupTabletSetChangeStatus related_change_status_;
  DupTableRelatedSetOpType related_set_type_;
  void reuse()
  {
    related_change_status_.init();
    related_change_status_.set_temporary();
    related_common_header_.reuse();
    related_set_type_ = DupTableRelatedSetOpType::INVALID;
  }

  void reset()
  {
    related_common_header_.reset();
    related_change_status_.reset();
    related_set_type_ = DupTableRelatedSetOpType::INVALID;
  }

  RelatedSetAttribute(const uint64_t uid) : related_common_header_(uid), related_change_status_() { reuse(); }
  RelatedSetAttribute() { reset(); }

  DupTableRelatedSetOpType get_related_set_op_type() const { return related_set_type_; }
  void set_related_set_op_type(DupTableRelatedSetOpType related_set_type) { related_set_type_ = related_set_type; }
  const DupTabletSetCommonHeader &get_related_common_header() const { return related_common_header_; }
  void set_related_common_header(const DupTabletSetCommonHeader &related_common_header) {
    related_common_header_.copy_tablet_set_common_header(related_common_header);
  }

  TO_STRING_KV(K(related_change_status_), K(related_common_header_), K(related_set_type_));

  OB_UNIS_VERSION(1);
};

class DupTabletChangeMap : public common::ObDLinkBase<DupTabletChangeMap>, public DupTabletIdMap
{
public:
  NEED_SERIALIZE_AND_DESERIALIZE;
  static int deserialize_set_attribute(const char *buf,
                                       int64_t data_len,
                                       int64_t pos,
                                       DupTabletSetAttribute &deser_attr);

  DupTabletChangeMap(const uint64_t set_id) : dup_set_attr_(set_id) { reuse(); }

  void reuse()
  {
    dup_set_attr_.reuse();
    related_set_attr_.reset();
    DupTabletIdMap::clear();
    last_gc_scan_ts_ = 0;
  }

  void destroy()
  {
    dup_set_attr_.reset();
    common::ObDLinkBase<DupTabletChangeMap>::reset();
    DupTabletIdMap::destroy();
    last_gc_scan_ts_ = 0;
  }

  int create(const uint64_t tenant_id, const int64_t bucket_num);

  DupTabletSetChangeStatus *get_change_status()
  {
    DupTabletSetChangeStatus *change_status_ptr = nullptr;
    if (dup_set_attr_.common_header_.is_readable_set()) {
      change_status_ptr = nullptr;
    } else {
      change_status_ptr = &dup_set_attr_.change_status_;
    }
    return change_status_ptr;
  }
  DupTabletSetCommonHeader &get_common_header() { return dup_set_attr_.common_header_; }
  const DupTabletSetCommonHeader &get_RO_common_header() const { return dup_set_attr_.common_header_; }
  void set_related_set_op_type (DupTableRelatedSetOpType type) { related_set_attr_.set_related_set_op_type(type); }
  DupTableRelatedSetOpType get_related_set_op_type() const { return related_set_attr_.get_related_set_op_type(); }
  const DupTabletSetCommonHeader &get_related_common_header() const { return related_set_attr_.get_related_common_header(); }
  const RelatedSetAttribute &get_related_set_attr() { return related_set_attr_; }
  void set_related_common_header(const DupTabletSetCommonHeader &common_header) {
    related_set_attr_.set_related_common_header(common_header);
  }
  bool need_reserve(const share::SCN &scn) const
  {
    return dup_set_attr_.change_status_.need_reserve(scn);
  }
  share::SCN get_tablet_change_scn() { return dup_set_attr_.change_status_.tablet_change_scn_; }
  const DupTabletSetChangeStatus &get_RO_change_status() const
  {
    return dup_set_attr_.change_status_;
  }

  bool need_gc_scan(int64_t gc_start_time) {
    bool bool_ret = false;

    if (!get_common_header().is_readable_set()) {
      DUP_TABLE_LOG_RET(WARN, OB_ERR_UNEXPECTED, "not readable set, not need gc",
                        K(gc_start_time), KPC(this));
      bool_ret = false;
    } else if (get_related_set_op_type() != DupTableRelatedSetOpType::INVALID) {
      DUP_TABLE_LOG(INFO, "this readable set used for other operation, should skip gc", KPC(this));
      bool_ret = false;
    } else {
      if (last_gc_scan_ts_ <= 0
          || gc_start_time > last_gc_scan_ts_) {
        bool_ret = true;
      } else {
        bool_ret = false;
      }
    }

    DUP_TABLE_LOG(INFO, "check need gc scan", K(bool_ret),K(last_gc_scan_ts_),K(gc_start_time),KPC(this));

    return bool_ret;
  }
  void set_last_gc_scan_ts(const int64_t gc_start_time) {
    if (gc_start_time < last_gc_scan_ts_) {
      DUP_TABLE_LOG(INFO, "not update last_gc_scan_ts_ with min value",
                    K(gc_start_time), KPC(this));
    } else {
      last_gc_scan_ts_ = gc_start_time;
    }

    DUP_TABLE_LOG(INFO, "set last gc scn ts", K(last_gc_scan_ts_),K(gc_start_time),KPC(this));
  }
  int64_t get_last_gc_scan_ts() { return last_gc_scan_ts_; }

  const DupTabletSetAttribute & get_RO_attribute() const  {return dup_set_attr_;}
  const RelatedSetAttribute & get_RO_related_attribute() const  {return related_set_attr_;}

  bool is_logging() const { return dup_set_attr_.change_status_.check_logging(); }
  void set_logging() { dup_set_attr_.change_status_.set_logging(); }
  void clean_logging() { dup_set_attr_.change_status_.clean_logging(); }

  TO_STRING_KV(K(dup_set_attr_), K(DupTabletIdMap::size()), K(DupTabletIdMap::created()),
               K(related_set_attr_), K(last_gc_scan_ts_));

private:
  DupTabletSetAttribute dup_set_attr_;
  RelatedSetAttribute related_set_attr_;
  int64_t last_gc_scan_ts_;
};

class TabletsSerCallBack : public IHashSerCallBack
{
public:
  TabletsSerCallBack(char *buf, int64_t buf_len, int64_t pos) : IHashSerCallBack(buf, buf_len, pos)
  {}
  int operator()(const common::hash::HashMapPair<common::ObTabletID, DupTabletInfo> &hash_pair);
};

class TabletsDeSerCallBack : public IHashDeSerCallBack
{
public:
  TabletsDeSerCallBack(const char *buf, int64_t buf_len, int64_t pos, int64_t deser_time)
      : IHashDeSerCallBack(buf, buf_len, pos), deser_time_(deser_time)
  {}
  int operator()(DupTabletChangeMap &dup_tablet_map);

private:
  int64_t deser_time_;
};

class TabletsGetSizeCallBack
{
public:
  int64_t operator()(const common::hash::HashMapPair<common::ObTabletID, DupTabletInfo> &hash_pair);
};

class DupTabletLogBody
{
  OB_UNIS_VERSION(1);

public:
  static int deserialize_dup_attribute(const char *buf,
                                       const int64_t data_len,
                                       int64_t pos,
                                       DupTabletSetAttribute &deser_attr);

public:
  DupTabletLogBody(DupTabletChangeMap &hash_map) : hash_map_(hash_map) {}

  void reset() {}

  const DupTabletChangeMap &get_cur_map_ref() { return hash_map_; }

  TO_STRING_KV(K(hash_map_));

private:
  DupTabletChangeMap &hash_map_;
};

// ***********************************************************************************************
// How dup tablet move between different sets when its dup attribute changed:
//  1. new set : store tablets which be discovered as a part of a dup table
//  2. old set : store tablets which has lost dup_table attribute
//  3. readable set : store tablets which can be read
// ***********************************************************************************************
//
//                            |
//                            | acquire dup attribute
//                            v
//  discard dup attribute   +------------------------------+
// <----------------------- |        new dup tablet        | <+
//                          +------------------------------+  |
//                            |                               |
//                            | confirmed by lease follower   |
//                            v                               |
//                          +------------------------------+  |
//                          |     readable dup tablet      |  | acquire dup attribute
//                          +------------------------------+  |
//                            |                              |
//                            | discard dup attribute         |
//                            v                               |
//                          +------------------------------+  |
//                          |        old dup tablet        | -+
//                          +------------------------------+
//                            |
//                            | confirmed by lease follower
//                            v
//
// ***********************************************************************************************
// * How dup tablet change state when it move between dup tablet sets
// ***********************************************************************************************
//
//   |
//   | insert into new/old tablets
//   v
// +--------------------------------------------------+
// |                    TEMPORARY                     |
// +--------------------------------------------------+
//   |
//   | mark logging but not serialize in the first log
//   | set log ts after submitted log
//   v
// +--------------------------------------------------+
// |                     LOGGING                      |
// +--------------------------------------------------+
//   |
//   | invoke log cb success
//   v
// +--------------------------------------------------+
// |                     DURABLE                      |
// +--------------------------------------------------+
//   |
//   | confirmed replay_ts by lease follower
//   | move into confirmed_new/confirmed_old tablets
//   v
// +--------------------------------------------------+
// |                    CONFIRMED                     |
// +--------------------------------------------------+
//   |
//   | move into confirmed_new/confirmed_old tablets
//   | serialize in the second log
//   v
//
//
//
//   |
//   | replay or apply the second log
//   v
//
//
//
//   |
//   | [new]move to readable/[old]remove from old
//   v
// +--------------------------------------------------+
// |                     READABLE                     |
// +--------------------------------------------------+
//
// ***********************************************************************************************
// *  If move a tablet from old to readable without confirm
// *  Problem:
// *  1. Leader (A)  tablet1(readable->old); submit lease log(log_ts = n);
// tablet1(old->new->readable); switch_to_follower
// *     Follower (B) replay log n=>tablet1(readable->old); switch_to_leader
// *  2. Follower(A) tablet1(readable),replay_ts = n
// *     Leaser(B) tablet1(old); confirm A replay_ts > n ; tablet1(old->delete)
// ***********************************************************************************************

class ObLSDupTabletsMgr
{
public:
  ObLSDupTabletsMgr()
      : changing_new_set_(nullptr), removing_old_set_(nullptr), tablet_set_diag_info_log_buf_(nullptr),tablet_id_diag_info_log_buf_(nullptr)
  {
    reset();
  }
  ~ObLSDupTabletsMgr() { destroy(); }
  int init(ObDupTableLSHandler *dup_ls_handle);
  void destroy();
  void reset();

  bool is_master() { return ATOMIC_LOAD(&is_master_); }

  int offline();

  const static int64_t MAX_CONFIRMING_TABLET_COUNT;

public:
  int check_readable(const common::ObTabletID &tablet_id,
                     bool &readable,
                     const share::SCN &snapshot,
                     DupTableInterfaceStat interface_stat);
  int search_dup_tablet_for_read(const common::ObTabletID &tablet_id, bool &is_dup_table);
  // For part_ctx, check_dup_table will be invoked after submit_log in LS which has dup_table
  // tablets. It will bring performance effect for normal part_ctx without dup_table tablets.
  int search_dup_tablet_in_redo_log(const common::ObTabletID &tablet_id,
                                    bool &is_dup_table,
                                    const share::SCN &from_scn,
                                    const share::SCN &to_scn);
  int gc_tmporary_dup_tablets(const int64_t gc_ts, const int64_t max_task_interval);
  // new gc methods
  int scan_readable_set_for_gc(const int64_t leader_takeover_ts);

  int refresh_dup_tablet(const common::ObTabletID &tablet_id,
                         bool is_dup_table,
                         int64_t refresh_time);

  int prepare_serialize(int64_t &max_ser_size,
                        DupTabletSetIDArray &unique_id_array,
                        const int64_t max_log_buf_len);
  int serialize_tablet_log(const DupTabletSetIDArray &unique_id_array,
                           char *buf,
                           const int64_t buf_len,
                           int64_t &pos);
  int deserialize_tablet_log(DupTabletSetIDArray &unique_id_array,
                             const char *buf,
                             const int64_t data_len,
                             int64_t &pos);

  int tablet_log_submitted(const bool submit_result,
                           const share::SCN &tablet_log_scn,
                           const bool for_replay,
                           const DupTabletSetIDArray &unique_id_array);

  int tablet_log_synced(const bool sync_result,
                        const share::SCN &scn,
                        const bool for_replay,
                        const DupTabletSetIDArray &unique_id_array,
                        bool &modify_readable_set,
                        const int64_t start_sync_time);

  int try_to_confirm_tablets(const share::SCN &confirm_scn);
  // bool need_log_tablets();
  int64_t get_dup_tablet_count();
  int64_t get_readable_tablet_count();
  bool has_dup_tablet();
  int64_t get_readable_tablet_set_count();
  int64_t get_need_confirm_tablet_set_count();
  bool check_removing_tablet_exist();
  bool check_changing_new_tablet_exist();
  int64_t get_all_tablet_set_count();

  int leader_takeover(const bool is_resume, const bool recover_all_readable_from_ckpt);
  int leader_revoke(const bool is_logging);

  void print_tablet_diag_info_log(bool is_master);

  // TO_STRING_KV(KPC(changing_new_set_),
  //              K(need_confirm_new_queue_.get_size()),
  //              K(old_tablets_),
  //              K(readable_tablets_));
  int get_tablets_stat(ObDupLSTabletsStatIterator &collect_iter, const share::ObLSID &ls_id);
  int get_tablet_set_stat(ObDupLSTabletSetStatIterator &collect_iter, const share::ObLSID &ls_id);

private:
  class GcDiscardedDupTabletHandler
  {
  public:
    GcDiscardedDupTabletHandler(int64_t update_ts,
                                int64_t gc_time_interval,
                                const DupTabletSetCommonHeader &common_header,
                                DupTabletChangeMap &old_tablets)
        : gc_ts_(update_ts), gc_time_interval_(gc_time_interval), gc_tablet_cnt_(0),
          ret_(OB_SUCCESS), src_common_header_(common_header), old_tablets_(old_tablets)
    {}
    bool operator()(common::hash::HashMapPair<common::ObTabletID, DupTabletInfo> &hash_pair);
    int64_t get_gc_tablet_cnt() const { return gc_tablet_cnt_; }
    int get_ret() const { return ret_; }

  private:
    int64_t gc_ts_;
    int64_t gc_time_interval_;
    int64_t gc_tablet_cnt_;
    int ret_;
    DupTabletSetCommonHeader src_common_header_;
    DupTabletChangeMap &old_tablets_;
  };

  class GcOneReadableSetHandler
  {
  public:
    GcOneReadableSetHandler(int64_t update_ts,
                            int64_t tablet_gc_window,
                            DupTabletChangeMap &old_tablets,
                            int64_t max_gc_tablet_cnt)
        : gc_ts_(update_ts), tablet_gc_window_(tablet_gc_window), gc_tablet_cnt_(0),
          max_gc_tablet_cnt_(max_gc_tablet_cnt), old_tablets_(old_tablets)
    {}

    int operator()(common::hash::HashMapPair<common::ObTabletID, DupTabletInfo> &hash_pair);
    int64_t get_gc_tablet_cnt() const { return gc_tablet_cnt_; }

  private:
    int64_t gc_ts_;
    int64_t tablet_gc_window_;
    int64_t gc_tablet_cnt_;
    int64_t max_gc_tablet_cnt_;
    DupTabletChangeMap &old_tablets_;
  };

  class ConfirmedDupTabletHandler
  {
    /**
     *  1. src == new : move to readable
     *  2. src == old : remvo from old
     */
  public:
    ConfirmedDupTabletHandler(DupTabletChangeMap &readable_tablets) : readable_(readable_tablets) {}
    int operator()(common::hash::HashMapPair<common::ObTabletID, DupTabletInfo> &hash_pair);

  private:
    DupTabletChangeMap &readable_;
  };

  class DiagInfoGenerator
  {
  public:
    DiagInfoGenerator(char *info_buf,
                      int64_t info_buf_len,
                      int64_t info_buf_pos,
                      uint64_t tablet_set_id)
        : info_buf_(info_buf), info_buf_len_(info_buf_len), info_buf_pos_(info_buf_pos),
          tablet_set_id_(tablet_set_id)
    {
      iter_count_ = 0;
    }

    int64_t get_buf_pos() { return info_buf_pos_; }

    int operator()(const common::hash::HashMapPair<common::ObTabletID, DupTabletInfo> &hash_pair);

  private:
    char *info_buf_;
    int64_t info_buf_len_;
    int64_t info_buf_pos_;
    uint64_t tablet_set_id_;
    int64_t iter_count_;
  };

  class CollectTabletsHandler
  {
  public:
    CollectTabletsHandler(const int64_t collect_ts,
                          const share::ObLSID ls_id,
                          const uint64_t tenant_id,
                          const ObAddr &addr,
                          const bool is_master,
                          const int64_t tablet_set_id,
                          const TabletSetAttr attr,
                          // const int64_t tablet_gc_window,
                          ObDupLSTabletsStatIterator &collect_iter)
        : collect_ts_(collect_ts), ls_id_(ls_id), tenant_id_(tenant_id), addr_(addr),
          is_master_(is_master), tablet_set_id_(tablet_set_id), attr_(attr),
          // tablet_gc_window_(tablet_gc_window)
          collect_iter_(collect_iter)
    {}
    int operator()(const common::hash::HashMapPair<common::ObTabletID, DupTabletInfo> &hash_pair);

  private:
    int64_t collect_ts_;
    share::ObLSID ls_id_;
    uint64_t tenant_id_;
    common::ObAddr addr_;
    bool is_master_;
    int64_t tablet_set_id_;
    TabletSetAttr attr_;
    // int64_t tablet_gc_window_;
    ObDupLSTabletsStatIterator &collect_iter_;
  };

private:
  int process_prepare_ser_err_test_();

  int lose_dup_tablet_(const common::ObTabletID &tablet_id);
  int discover_dup_tablet_(const common::ObTabletID &tablet_id, const int64_t refresh_time);
  int collect_confirmed_dup_tablet_(const share::SCN &max_replayed_scn);

  int init_free_tablet_pool_();
  int destroy_free_tablet_pool_();

  int alloc_one_free_tablet_set_(const uint64_t uid);

  int get_free_tablet_set(DupTabletChangeMap *&free_set,
                          const bool force_alloc = false,
                          const uint64_t target_id = 0);

  // If get a free tablet set, need set tablet set type and push into queue
  int get_target_tablet_set_(const DupTabletSetCommonHeader &target_common_header,
                             DupTabletChangeMap *&target_set,
                             const bool construct_target_set = false,
                             const bool force_search_target_set = false);

  int check_and_recycle_empty_readable_set_(DupTabletChangeMap *need_free_set, bool &need_remove);
  int return_tablet_set_(DupTabletChangeMap *need_free_set);

  int clean_readable_tablets_(const share::SCN &min_reserve_tablet_scn);
  int clean_durable_confirming_tablets_(const share::SCN &min_reserve_tablet_scn);
  int clean_unlog_tablets_();
  int construct_empty_block_confirm_task_(const int64_t trx_ref);
  int search_special_op_(uint64_t special_op_type);
  int clear_all_special_op_();
  int construct_clean_confirming_set_task_();
  int construct_clean_all_readable_set_task_();
  int try_exec_special_op_(DupTabletChangeMap *op_tablet_set,
                           const share::SCN &min_reserve_tablet_scn,
                           const bool for_replay);
  bool need_seralize_readable_set() { return true; }

  int prepare_serialize_readable_tablet_set_(int64_t &max_ser_size,
                                             DupTabletSetIDArray &unique_id_array,
                                             const int64_t max_log_buf_len);
  int prepare_serialize_confirming_tablet_set_(int64_t &max_ser_size,
                                               DupTabletSetIDArray &unique_id_array,
                                               const int64_t max_log_buf_len);

  int cal_single_set_max_ser_size_(DupTabletChangeMap *hash_map,
                                   int64_t &max_ser_size,
                                   const int64_t ser_size_limit,
                                   DupTabletSetIDArray &id_array);

  int merge_into_readable_tablets_(DupTabletChangeMap *change_map_ptr, const bool for_replay);

  int validate_replay_dup_tablet_set(const DupTabletSetCommonHeader &target_common_header,
                                     const DupTabletSetChangeStatus &target_change_status,
                                     DupTabletChangeMap *replay_target_set);
  int prepare_serialize_src_set_with_related_set_(DupTabletChangeMap *src_set,
                                                  int64_t &max_ser_size,
                                                  const int64_t max_log_buf_len,
                                                  DupTabletSetIDArray &unique_id_array,
                                                  DupTabletChangeMap *related_set);

  int remove_src_and_related_set_header_from_array_(DupTabletChangeMap *src_set,
                                                    DupTabletChangeMap *related_set,
                                                    DupTabletSetIDArray &unique_id_array);
  DupTabletChangeMap *get_need_gc_set_();
  int remove_tablet_from_readable_set_();
  bool is_busy_in_readable_change_();
private:
  //
  static int64_t GC_DUP_TABLETS_TIME_INTERVAL;  // 5 min
  static int64_t GC_DUP_TABLETS_FAILED_TIMEOUT; // 25 min
  const static int64_t GC_TIMEOUT;              // 1s

  const static int64_t RESERVED_FREE_SET_COUNT;
  const static int64_t MAX_FREE_SET_COUNT;
  const static int64_t MAX_GC_TABLET_COUNT;

  static int64_t MAX_READABLE_SET_SER_INTERVAL;

public:
  TO_STRING_KV(K(free_set_pool_.get_size()),
               KPC(changing_new_set_),
               K(need_confirm_new_queue_.get_size()),
               K(readable_tablets_list_.get_size()),
               KPC(removing_old_set_),
               K(last_no_free_set_time_),
               K(extra_free_set_alloc_count_));

private:
  SpinRWLock dup_tablets_lock_;

  // ObDupTableLSHandler *dup_ls_handle_ptr_;
  share::ObLSID ls_id_;
  bool is_master_;
  bool is_stopped_;

  common::ObDList<DupTabletChangeMap> free_set_pool_;
  DupTabletChangeMap *changing_new_set_;
  common::ObDList<DupTabletChangeMap> need_confirm_new_queue_;
  common::ObDList<DupTabletChangeMap> readable_tablets_list_;
  DupTabletChangeMap *removing_old_set_;

  /*  1. gc one round means iter all readable set
   *  2. use readable_set_in_gc_ point to readable set not finish gc in one round
   *  3. use gc_start_time_ mark gc one round start time one round
   */
  DupTabletChangeMap *readable_set_in_gc_;
  int64_t gc_start_time_;
  int64_t last_no_free_set_time_;
  int64_t extra_free_set_alloc_count_;

  int64_t last_readable_sync_succ_time_;
  share::SCN last_readable_log_entry_scn_;

  char *tablet_set_diag_info_log_buf_;
  char *tablet_id_diag_info_log_buf_;
};

class ObLSDupTablets
{
public:
  void reset()
  {
    ls_id_.reset();
    array_.reset();
  }
  share::ObLSID get_ls_id() const { return ls_id_; }
  ObTabletIDArray &get_array() { return array_; }
  const ObTabletIDArray &get_array() const { return array_; }
  void set_ls_id(const share::ObLSID &ls_id) { ls_id_ = ls_id; }

private:
  share::ObLSID ls_id_;
  ObTabletIDArray array_;
};

class ObTenantDupTabletSchemaHelper
{
public:
  typedef common::hash::ObHashSet<common::ObTabletID, hash::NoPthreadDefendMode> TabletIDSet;

public:
  ObTenantDupTabletSchemaHelper() {}

public:
  int refresh_and_get_tablet_set(TabletIDSet &tenant_dup_tablet_set);

private:
  int get_all_dup_tablet_set_(TabletIDSet &tablets_set);

private:
};

} // namespace transaction

} // namespace oceanbase

#endif
