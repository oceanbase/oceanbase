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
#include "storage/multi_data_source/runtime_utility/common_define.h"
#include "storage/multi_data_source/compile_utility/mds_dummy_key.h"
#include "storage/tablet/ob_tablet_create_delete_mds_user_data.h"

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
  int get_latest(OP &&read_op, bool &is_committed) const;
  template <typename OP, ENABLE_IF_LIKE_FUNCTION(OP, int(const UserMdsNode<DummyKey, ObTabletCreateDeleteMdsUserData>&))>
  int get_tablet_status_node(OP &&read_op, const int64_t read_seq = 0) const;
  template <typename T, typename OP, ENABLE_IF_LIKE_FUNCTION(OP, int(const T&))>
  int get_snapshot(OP &&read_op,
                   const share::SCN snapshot = share::SCN::max_scn(),
                   const int64_t timeout_us = 0) const;
  template <typename T, typename OP, ENABLE_IF_LIKE_FUNCTION(OP, int(const T&))>
  int get_by_writer(OP &&read_op,
                    const MdsWriter &writer,
                    const share::SCN snapshot = share::SCN::max_scn(),
                    const transaction::ObTxSEQ read_seq = transaction::ObTxSEQ::MAX_VAL(),
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
  int get_latest(const Key &key, OP &&read_op, bool &is_committed) const;
  template <typename Key, typename Value, typename OP>
  int get_snapshot(const Key &key,
                   OP &&read_op,
                   const share::SCN snapshot = share::SCN::max_scn(),
                   const int64_t timeout_us = 0) const;
  template <typename Key, typename Value, typename OP>
  int get_by_writer(const Key &key,
                    OP &&read_op,
                    const MdsWriter &writer,
                    const share::SCN snapshot,// if readed node's writer is not input writer, compared with snapshot
                    const transaction::ObTxSEQ read_seq = transaction::ObTxSEQ::MAX_VAL(),// if readed node's writer is input writer, compared with read_seq
                    const int64_t timeout_us = 0) const;
  template <typename Key, typename Value>
  int is_locked_by_others(const Key &key,
                          bool &is_locked,
                          const MdsWriter &self = MdsWriter()) const;
  /************************************************************************************************/
  template <ScanRowOrder SCAN_ROW_ORDER,
            ScanNodeOrder SCAN_NODE_ORDER,
            typename DUMP_OP,
            ENABLE_IF_LIKE_FUNCTION(DUMP_OP, int(const MdsDumpKV &))>
  int scan_all_nodes_to_dump(DUMP_OP &&for_each_op,
                             const int64_t mds_construct_sequence,
                             const bool for_flush) const;
  int flush(share::SCN need_advanced_rec_scn_lower_limit, share::SCN max_decided_scn);
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
  template <ScanRowOrder SCAN_ROW_ORDER,
            ScanNodeOrder SCAN_NODE_ORDER,
            typename DUMP_OP,
            ENABLE_IF_NOT_LIKE_FUNCTION(DUMP_OP, int(const MdsDumpKV &))>
  int scan_all_nodes_to_dump(DUMP_OP &&for_each_op,
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

}
}
}

#ifndef STORAGE_MULTI_DATE_SOURCE_MDS_TABLE_HANDLE_H_IPP
#define STORAGE_MULTI_DATE_SOURCE_MDS_TABLE_HANDLE_H_IPP
#include "mds_table_handle.ipp"
#endif

#endif