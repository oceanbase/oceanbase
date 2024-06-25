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

#ifndef OCEANBASE_STORAGE_TABLET_OB_TABLET_MDS_PART
#define OCEANBASE_STORAGE_TABLET_OB_TABLET_MDS_PART

#include "lib/ob_errno.h"
#include "common/meta_programming/ob_meta_serialization.h"
#include "storage/multi_data_source/mds_table_handle.h"
#include "storage/meta_mem/ob_tablet_pointer.h"
#include "storage/tablet/ob_mds_range_query_iterator.h"
#include "storage/tablet/ob_tablet_mds_data.h"
#include "storage/tablet/ob_tablet_member_wrapper.h"
#include "storage/tablet/ob_tablet_obj_load_helper.h"
#include "storage/ls/ob_ls_switch_checker.h"

namespace oceanbase
{
namespace storage
{
class ObTabletCreateDeleteHelper;
class ObMdsRowIterator;

class ObITabletMdsInterface
{
  friend class ObTabletCreateDeleteHelper;
  friend class ObTenantDirectLoadMgr; // TODO(@bowen.gbw): refactor later
public:
  // new mds
  // Currently, we only support read LATEST multi source data, so please pass MAX_SCN as snapshot.
  // Other value will cause OB_NOT_SUPPOTED error.
  // Snapshot read operation will be implemented after multi source data dumped into macro blocks.
  template <typename T>// general set for dummy key unit
  int set(T &&data, mds::MdsCtx &ctx, const int64_t lock_timeout_us = 0);
  template <typename Key, typename Value>// general set for multi key unit
  int set(const Key &key, Value &&data, mds::MdsCtx &ctx, const int64_t lock_timeout_us = 0);
  template <typename Key, typename Value>// general remove for multi key unit
  int remove(const Key &key, mds::MdsCtx &ctx, const int64_t lock_timeout_us = 0);
  // sometimes mds ndoes needed be forcely released, e.g.: ls offline
  template <typename T>
  int is_locked_by_others(bool &is_locked, const mds::MdsWriter &self = mds::MdsWriter()) const;

  int check_tablet_status_written(bool &written);
  // specialization get for each module
  int get_latest_tablet_status(ObTabletCreateDeleteMdsUserData &data, bool &is_committed) const;
  int get_tablet_status(const share::SCN &snapshot,
                        ObTabletCreateDeleteMdsUserData &data,
                        const int64_t timeout = ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US) const;
  int get_latest_ddl_data(ObTabletBindingMdsUserData &data, bool &is_committed) const;
  int get_ddl_data(const share::SCN &snapshot,
                   ObTabletBindingMdsUserData &data,
                   const int64_t timeout = ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US) const;
  int get_autoinc_seq(ObIAllocator &allocator,
                      const share::SCN &snapshot,
                      share::ObTabletAutoincSeq &data,
                      const int64_t timeout = ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US) const;
  int fill_virtual_info(ObIArray<mds::MdsNodeInfoForVirtualTable> &mds_node_info_array) const;
  TO_STRING_KV(KP(this), "is_inited", check_is_inited_(), "ls_id", get_tablet_meta_().ls_id_,
               "tablet_id", get_tablet_id_(), KP(get_tablet_pointer_()));
  int get_mds_table_rec_scn(share::SCN &rec_scn);
  int mds_table_flush(const share::SCN &recycle_scn);
  // get tablet status from MDS, and check whether state is TRANSFER_IN and redo scn is valid.
  // @param [in] written : if current tablet status is TRANSFER_IN, set true if redo_scn is valid, otherwise set fasle
  // @return OB_STATE_NOT_MATCH : tablet status is not TRANSFER_IN.
  //         OB_EMPTY_RESULT : never has tablet status written.
  //         OB_LS_OFFLINE : read meet ls offline
  //         other error...
  // CAUTIONS: this interface is only for transfer! anyone else shouldn't call this!
  int check_transfer_in_redo_written(bool &written);
protected:// implemented by ObTablet
  // TODO(@bowen.gbw): remove these virtual functions later
  virtual bool check_is_inited_() const = 0;
  virtual const ObTabletMeta &get_tablet_meta_() const = 0;
  virtual int get_mds_table_handle_(mds::MdsTableHandle &handle,
                                    const bool create_if_not_exist) const = 0;
  virtual ObTabletPointer *get_tablet_pointer_() const = 0;
  template <typename K, typename T>
  int get_mds_data_from_tablet(
      const K &key,
      const share::SCN &snapshot,
      const int64_t timeout_us,
      const common::ObFunction<int(const T&)> &read_op) const;
  template <typename K, typename T>
  int read_data_from_mds_sstable(
      common::ObIAllocator &allocator,
      const K &key,
      const share::SCN &snapshot,
      const int64_t timeout_us,
      const common::ObFunction<int(const T&)> &read_op) const;
  int read_raw_data(
      common::ObIAllocator &allocator,
      const uint8_t mds_unit_id,
      const common::ObString &udf_key,
      const share::SCN &snapshot,
      const int64_t timeout_us,
      mds::MdsDumpKV &kv) const;
  int mds_table_scan(
      ObTableScanParam &scan_param,
      ObStoreCtx &store_ctx,
      ObMdsRowIterator &iter) const;
  template <typename K, typename T>
  int mds_range_query(
      ObTableScanParam &scan_param,
      ObStoreCtx &store_ctx,
      ObMdsRangeQueryIterator<K, T> &iter) const;

  template <typename T>
  int replay(T &&mds,
             mds::MdsCtx &ctx,
             const share::SCN &scn);
private:
  template <typename Key, typename Value>
  int replay(const Key &key,
             Value &&mds,
             mds::MdsCtx &ctx,
             const share::SCN &scn);
  template <typename Key, typename Value>
  int replay_remove(const Key &key,
                    mds::MdsCtx &ctx,
                    const share::SCN &scn);// called only by ObTabletReplayExecutor
  template <typename T, typename OP>
  int get_latest(OP &&read_op, bool &is_committed) const;
  template <typename T, typename OP>
  int get_snapshot(OP &&read_op,
                   const share::SCN snapshot,
                   const int64_t timeout_us) const;// general get for dummy key unit
  template <typename Key, typename Value, typename OP>
  int get_snapshot(const Key &key,
                   OP &&read_op,
                   const share::SCN snapshot,
                   const int64_t timeout_us) const;// general get for multi key unit
  template <typename T, typename OP>
  int cross_ls_get_latest(const ObITabletMdsInterface *another, OP &&read_op, bool &is_committed) const;
  template <typename Key, typename Value, typename OP>
  int cross_ls_get_snapshot(const ObITabletMdsInterface *another,
                            const Key &key,
                            OP &&read_op,
                            const share::SCN snapshot,
                            const int64_t timeout_us) const;
  static int get_tablet_handle_and_base_ptr(const share::ObLSID &ls_id,
                                            const common::ObTabletID &tablet_id,
                                            ObTabletHandle &tablet_handle,
                                            ObITabletMdsInterface *&base_ptr);
  common::ObTabletID get_tablet_id_() const;
  template <typename T>
  int obj_to_string_holder_(const T &obj, ObStringHolder &holder) const;
  template <typename T>
  int fill_virtual_info_by_obj_(const T &obj, const mds::NodePosition position, ObIArray<mds::MdsNodeInfoForVirtualTable> &mds_node_info_array) const;
  template <typename T>
  int fill_virtual_info_from_mds_sstable(ObIArray<mds::MdsNodeInfoForVirtualTable> &mds_node_info_array) const;
};

struct GetTabletStatusNodeFromMdsTableOp
{
  GetTabletStatusNodeFromMdsTableOp(ObTabletCreateDeleteMdsUserData &tablet_status, share::SCN &redo_scn)
  : tablet_status_(tablet_status),
  redo_scn_(redo_scn) {}
  int operator()(const mds::UserMdsNode<mds::DummyKey, ObTabletCreateDeleteMdsUserData> &node) {
    tablet_status_.assign(node.user_data_);
    redo_scn_ = node.redo_scn_;
    MDS_LOG(TRACE, "read tablet status in mds_table", K(node));
    return OB_SUCCESS;
  }
  ObTabletCreateDeleteMdsUserData &tablet_status_;
  share::SCN &redo_scn_;
};

struct ReadTabletStatusOp
{
  ReadTabletStatusOp(ObTabletCreateDeleteMdsUserData &tablet_status) : tablet_status_(tablet_status) {}
  int operator()(const ObTabletCreateDeleteMdsUserData &data)
  {
    return tablet_status_.assign(data);
  }
  ObTabletCreateDeleteMdsUserData &tablet_status_;
};

struct ReadBindingInfoOp
{
  ReadBindingInfoOp(ObTabletBindingMdsUserData &ddl_data) : ddl_data_(ddl_data) {}
  int operator()(const ObTabletBindingMdsUserData &data)
  {
    return ddl_data_.assign(data);
  }
  ObTabletBindingMdsUserData &ddl_data_;
};

struct ReadAutoIncSeqOp
{
  ReadAutoIncSeqOp(common::ObIAllocator &allocator, share::ObTabletAutoincSeq &auto_inc_seq)
    : allocator_(allocator), auto_inc_seq_(auto_inc_seq) {}
  int operator()(const share::ObTabletAutoincSeq &data)
  {
    return auto_inc_seq_.assign(allocator_, data);
  }
  common::ObIAllocator &allocator_;
  share::ObTabletAutoincSeq &auto_inc_seq_;
};

template <>
int ObITabletMdsInterface::mds_range_query<compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo>(
    ObTableScanParam &scan_param,
    ObStoreCtx &store_ctx,
    ObMdsRangeQueryIterator<compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo> &iter) const;
}
}

#ifndef INCLUDE_OB_TABLET_MDS_PART_IPP
#define INCLUDE_OB_TABLET_MDS_PART_IPP
#include "ob_i_tablet_mds_interface.ipp"
#endif

#endif
