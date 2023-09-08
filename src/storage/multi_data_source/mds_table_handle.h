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
#ifndef STORAGE_MULTI_DATE_SOURCE_MDS_TABLE_HANDLE_H
#define STORAGE_MULTI_DATE_SOURCE_MDS_TABLE_HANDLE_H

#include "lib/ob_errno.h"
#include "mds_table_impl.h"
#include "lib/guard/ob_light_shared_gaurd.h"

namespace oceanbase
{
namespace storage
{
class ObLS;
class ObTabletPointer;
namespace mds
{
class MdsTableHandle
{
public:
  MdsTableHandle() : mds_table_id_(UINT8_MAX) {}
  MdsTableHandle(const MdsTableHandle &rhs) = default;
  MdsTableHandle &operator=(const MdsTableHandle &rhs) = default;
  bool operator==(const MdsTableHandle &rhs) const;
  ~MdsTableHandle();
  int get_tablet_id(common::ObTabletID &tablet_id) const;
  int get_ls_id(share::ObLSID &ls_id) const;
  template <typename MdsTableType>
  int init(ObIAllocator &allocator,
           const ObTabletID tablet_id,
           const share::ObLSID ls_id,
           ObTabletPointer *pointer,
           ObMdsTableMgr *mgr_handle = nullptr);
  template <typename UnitKey, typename UnitValue>
  int get_mds_unit(MdsUnit<UnitKey, UnitValue> *&p_mds_unit);
  int fill_virtual_info(ObIArray<MdsNodeInfoForVirtualTable> &mds_node_info_array) const;
  int mark_removed_from_t3m(ObTabletPointer *pointer) const;
  int mark_switched_to_empty_shell() const;
  template <int N>
  int forcely_reset_mds_table(const char (&reason)[N]);
  /******************************Single Key Unit Access Interface**********************************/
  template <typename T>
  int set(T &&data, MdsCtx &ctx, const int64_t lock_timeout_us = 0);
  template <typename T>
  int replay(T &&data, MdsCtx &ctx, const share::SCN &scn);
  template <typename T, typename OP, ENABLE_IF_LIKE_FUNCTION(OP, int(const T&))>
  int get_latest(OP &&read_op, bool &is_committed, const int64_t read_seq = 0) const;
  template <typename T, typename OP, ENABLE_IF_LIKE_FUNCTION(OP, int(const T&))>
  int get_snapshot(OP &&read_op,
                   const share::SCN snapshot = share::SCN::max_scn(),
                   const int64_t read_seq = 0,
                   const int64_t timeout_us = 0) const;
  template <typename T, typename OP, ENABLE_IF_LIKE_FUNCTION(OP, int(const T&))>
  int get_by_writer(OP &&read_op,
                    const MdsWriter &writer,
                    const share::SCN snapshot = share::SCN::max_scn(),
                    const int64_t read_seq = 0,
                    const int64_t timeout_us = 0) const;
  template <typename T>
  int is_locked_by_others(bool &is_locked, const MdsWriter &self = MdsWriter()) const;
  /************************************************************************************************/

  /******************************Multi Key Unit Access Interface***********************************/
  template <typename Key, typename Value>
  int set(const Key &key, Value &&data, MdsCtx &ctx, const int64_t lock_timeout_us = 0);
  template <typename Key, typename Value>
  int replay(const Key &key, Value &&data, MdsCtx &ctx, const share::SCN &scn);
  template <typename Key, typename Value>
  int remove(const Key &key, MdsCtx &ctx, const int64_t lock_timeout_us = 0);
  template <typename Key, typename Value>
  int replay_remove(const Key &key, MdsCtx &ctx, share::SCN &scn);
  template <typename Key, typename Value, typename OP>
  int get_latest(const Key &key, OP &&read_op, bool &is_committed, const int64_t read_seq = 0) const;
  template <typename Key, typename Value, typename OP>
  int get_snapshot(const Key &key,
                   OP &&read_op,
                   const share::SCN snapshot = share::SCN::max_scn(),
                   const int64_t read_seq = 0,
                   const int64_t timeout_us = 0) const;
  template <typename Key, typename Value, typename OP>
  int get_by_writer(const Key &key,
                    OP &&read_op,
                    const MdsWriter &writer,
                    const share::SCN snapshot = share::SCN::max_scn(),
                    const int64_t read_seq = 0,
                    const int64_t timeout_us = 0) const;
  template <typename Key, typename Value>
  int is_locked_by_others(const Key &key,
                          bool &is_locked,
                          const MdsWriter &self = MdsWriter()) const;
  /************************************************************************************************/
  template <typename DUMP_OP, ENABLE_IF_LIKE_FUNCTION(DUMP_OP, int(const MdsDumpKV &))>
  int for_each_unit_from_small_key_to_big_from_old_node_to_new_to_dump(DUMP_OP &&for_each_op,
                                                                       const int64_t mds_construct_sequence,
                                                                       const bool for_flush) const;
  int flush(share::SCN need_advanced_rec_scn_lower_limit);
  int is_flushing(bool &is_flushing) const;
  void on_flush(const share::SCN &flush_scn, const int flush_ret);
  int try_recycle(const share::SCN &recycle_scn);// release nodes
  int sync_ref_until_last() const;
  int get_ref_cnt(int64_t &ref_cnt) const;
  int get_node_cnt(int64_t &valid_cnt) const;
  int get_rec_scn(share::SCN &rec_scn) const;
  bool is_valid() const;
  void reset();
  MdsTableBase *get_mds_table_ptr() { return p_mds_table_base_.ptr(); }
  TO_STRING_KV(K_(p_mds_table_base), K_(mds_table_id));
public:// compile error message
  template <typename DUMP_OP, ENABLE_IF_NOT_LIKE_FUNCTION(DUMP_OP, int(const MdsDumpKV &))>
  int for_each_unit_from_small_key_to_big_from_old_node_to_new_to_dump(DUMP_OP &&for_each_op,
                                                                       const int64_t mds_construct_sequence,
                                                                       const bool for_flush) const {
    static_assert(OB_TRAIT_IS_FUNCTION_LIKE(DUMP_OP, int(const MdsDumpKV &)),
                  "for_each_op required to be used like: int for_each_op(const MdsDumpKV &)");
    return OB_NOT_SUPPORTED;
  }
private:
  uint8_t mds_table_id_;
  ObLightSharedPtr<MdsTableBase> p_mds_table_base_;
};

inline void construct_lock_guard(mds::MdsRLockGuard &guard, mds::MdsLock &lock) {
  guard.~MdsRLockGuard();
  new (&guard) mds::MdsRLockGuard(lock);
}

template <typename UnitKey, typename UnitValue>
struct ObMdsKvRowScanIterator {// will lock unit when init, but won't lock row when get_next_kv_row()
  using KvRowIter = typename mds::MdsUnit<UnitKey, UnitValue>::iterator;
  using KvRow = mds::KvPair<UnitKey, mds::Row<UnitKey, UnitValue>>;
  ObMdsKvRowScanIterator();
  int init(mds::MdsTableHandle &mds_table_handle);
  int get_next_kv_row(KvRow *&p_kv_row);
  TO_STRING_KV(KP(this), K_(is_inited), K_(is_first_scan),\
               K(typeid(UnitKey).name()), K(typeid(UnitValue).name()))
private:
  bool is_inited_;
  bool is_first_scan_;
  mds::MdsUnit<UnitKey, UnitValue> *p_mds_unit_;
  KvRowIter kv_row_iter_;
  mds::MdsRLockGuard unit_guard_;
};

template <typename UnitKey, typename UnitValue>
struct ObMdsNodeScanIterator {// will lock row when init, but won't lock node when get_next()
  using KvRow = mds::KvPair<UnitKey, mds::Row<UnitKey, UnitValue>>;
  using KvRowIter = typename mds::MdsUnit<UnitKey, UnitValue>::iterator;
  using NodeIter = typename KvRowIter::row_type::iterator;
  ObMdsNodeScanIterator();
  int init(KvRow *&p_kv_row);
  int get_next_kv_node(UnitKey &key, mds::UserMdsNode<UnitKey, UnitValue> *&p_node);
  bool is_valid() const;
  void reset();
  TO_STRING_KV(KP(this), K_(is_inited), K_(is_first_scan),\
               K(typeid(UnitKey).name()), K(typeid(UnitValue).name()))
private:
  bool is_inited_;
  bool is_first_scan_;
  KvRow *p_mds_kv_row_;
  NodeIter node_iter_;
  mds::MdsRLockGuard row_guard_;
};

template <typename UnitKey, typename UnitValue>
struct ObMdsUnitRowNodeScanIterator {// will add mds table ref when init to make sure inner iter safe
  using KvRow = mds::KvPair<UnitKey, mds::Row<UnitKey, UnitValue>>;
  using KvRowIter = typename mds::MdsUnit<UnitKey, UnitValue>::iterator;
  using NodeIter = typename KvRowIter::row_type::iterator;
  ObMdsUnitRowNodeScanIterator();
  int init(mds::MdsTableHandle &mds_table_handle);
  int get_next(UnitKey &key, mds::UserMdsNode<UnitKey, UnitValue> *&p_node);
  TO_STRING_KV(KP(this), K_(is_inited), K_(is_first_scan), K_(mds_table_handle),\
               K(typeid(UnitKey).name()), K(typeid(UnitValue).name()))
private:
  bool is_inited_;
  bool is_first_scan_;
  mds::MdsTableHandle mds_table_handle_;
  ObMdsKvRowScanIterator<UnitKey, UnitValue> row_scan_iter_;
  ObMdsNodeScanIterator<UnitKey, UnitValue> node_scan_iter_;
};

}
}
}

#ifndef STORAGE_MULTI_DATE_SOURCE_MDS_TABLE_HANDLE_H_IPP
#define STORAGE_MULTI_DATE_SOURCE_MDS_TABLE_HANDLE_H_IPP
#include "mds_table_handle.ipp"
#endif

#endif