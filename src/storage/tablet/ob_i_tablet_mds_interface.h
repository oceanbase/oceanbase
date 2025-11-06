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
#include "common/meta_programming/ob_meta_copy.h"
#include "storage/multi_data_source/mds_table_handle.h"
#include "storage/meta_mem/ob_tablet_pointer.h"
#include "storage/tablet/ob_mds_row_iterator.h"
#include "storage/tablet/ob_tablet_mds_data.h"
#include "storage/tablet/ob_tablet_mds_node_filter.h"
#include "storage/tablet/ob_tablet_member_wrapper.h"
#include "storage/tablet/ob_tablet_obj_load_helper.h"
#include "storage/ls/ob_ls_switch_checker.h"

namespace oceanbase
{
namespace storage
{
class ObTabletCreateDeleteHelper;
class ObMdsRowIterator;
template <typename K, typename T>
class ObMdsRangeQueryIterator;

template <typename T>
struct MdsDefaultDeepCopyOperation {
  MdsDefaultDeepCopyOperation(T &value, ObIAllocator *alloc) : value_(value), alloc_(alloc) {}
  int operator()(const T &value) {
    int ret = OB_SUCCESS;
    if (nullptr == alloc_) {
      ret = meta::copy_or_assign(value, value_);
    } else {
      ret = meta::copy_or_assign(value, value_, *alloc_);
    }
    return ret;
  }
  T &value_;
  ObIAllocator *alloc_;
};

class ObITabletMdsInterface
{
  friend class ObTabletCreateDeleteHelper;
  friend class ObTenantDirectLoadMgr; // TODO(@gaishun.gs): refactor later
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

  int check_tablet_status_written(bool &written) const;
  // belows are wrapper interfaces for default getter for simple data structure
  // specialization get for each module
  int get_latest_tablet_status(ObTabletCreateDeleteMdsUserData &data,
                               mds::MdsWriter &writer,
                               mds::TwoPhaseCommitState &trans_stat,
                               share::SCN &trans_version,
                               const int64_t read_seq = 0) const;
  int get_tablet_status(const share::SCN &snapshot,
                        ObTabletCreateDeleteMdsUserData &data,
                        const int64_t timeout = ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US) const;
  int get_latest_ddl_data(ObTabletBindingMdsUserData &data,
                          mds::MdsWriter &writer,
                          mds::TwoPhaseCommitState &trans_stat,
                          share::SCN &trans_version,
                          const int64_t read_seq = 0) const;
  int get_ddl_data(const share::SCN &snapshot,
                   ObTabletBindingMdsUserData &data,
                   const int64_t timeout = ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US) const;
  int get_autoinc_seq(ObIAllocator &allocator,
                      const share::SCN &snapshot,
                      share::ObTabletAutoincSeq &data,
                      const int64_t timeout = ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US) const;


  int get_split_data(ObTabletSplitMdsUserData &data,
                     const int64_t timeout) const;
  int split_partkey_compare(const blocksstable::ObDatumRowkey &rowkey,
                            const ObITableReadInfo &rowkey_read_info,
                            const ObIArray<uint64_t> &partkey_projector,
                            int &cmp_ret,
                            const int64_t timeout) const;
  int fill_virtual_info(ObIArray<mds::MdsNodeInfoForVirtualTable> &mds_node_info_array) const;
  int get_direct_load_auto_inc_seq(ObDirectLoadAutoIncSeqData &data) const;
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
  template <typename T>
  int get_latest_committed_data(T &value, ObIAllocator *alloc = nullptr);
protected:// implemented by ObTablet
  // if trans_stat < BEFORE_PREPARE, trans_version is explained as prepare_version(which is MAX).
  // else if trans_stat < ON_PREAPRE, trans_version is explained as prepare_version(which is MIN).
  // else if trans_stat < ON_COMMIT, trans_version is explained as prepare_version(which is a valid data).
  // else if trans_stat == ON_COMMIT, trans_version is explained as commit_version(which is a valid data).
  template <typename T, typename T2 = T, ENABLE_IF_NOT_LIKE_FUNCTION(T2, int(const T &))>
  int get_latest(T &value,
                 mds::MdsWriter &writer,// FIXME(zk250686): should not exposed, will be removed later
                 mds::TwoPhaseCommitState &trans_stat,// FIXME(zk250686): should not exposed, will be removed later
                 share::SCN &trans_version,// FIXME(zk250686): should not exposed, will be removed later
                 ObIAllocator *alloc = nullptr,
                 const int64_t read_seq = 0) const {
    MdsDefaultDeepCopyOperation<T> default_get_op(value, alloc);
    return get_latest<T, MdsDefaultDeepCopyOperation<T> &>(default_get_op, writer, trans_stat, trans_version, read_seq);
  }
  template <typename T, typename T2 = T, ENABLE_IF_NOT_LIKE_FUNCTION(T2, int(const T &))>
  int get_latest_committed(T &value, ObIAllocator *alloc = nullptr) const {
    MdsDefaultDeepCopyOperation<T> default_get_op(value, alloc);
    return get_latest_committed<T, MdsDefaultDeepCopyOperation<T> &>(default_get_op);
  }
  template <typename T, typename T2 = T, ENABLE_IF_NOT_LIKE_FUNCTION(T2, int(const T &))>
  int get_snapshot(T &value,
                   const share::SCN snapshot,
                   const int64_t timeout_us,
                   ObIAllocator *alloc = nullptr,
                   const int64_t read_seq = 0) const {
    MdsDefaultDeepCopyOperation<T> default_get_op(value, alloc);
    return get_snapshot<T, MdsDefaultDeepCopyOperation<T> &>(default_get_op, snapshot, timeout_us, read_seq);
  }
  // belows are general get interfaces, which could be customized for complicated data structure
  template <typename T, typename OP, ENABLE_IF_LIKE_FUNCTION(OP, int(const T &))>
  int get_latest(OP &&read_op,
                 mds::MdsWriter &writer,// FIXME(zk250686): should not exposed, will be removed later
                 mds::TwoPhaseCommitState &trans_stat,// FIXME(zk250686): should not exposed, will be removed later
                 share::SCN &trans_version,// FIXME(zk250686): should not exposed, will be removed later
                 const int64_t read_seq = 0) const;
  template <typename T, typename OP, ENABLE_IF_LIKE_FUNCTION(OP, int(const T &))>
  int get_latest_committed(OP &&read_op) const;
  template <typename T, typename OP, ENABLE_IF_LIKE_FUNCTION(OP, int(const T &))>
  int get_snapshot(OP &&read_op,
                   const share::SCN snapshot,
                   const int64_t timeout_us) const;
  template <typename Key, typename Value, typename OP>
  int get_snapshot(const Key &key,
                   OP &&read_op,
                   const share::SCN snapshot,
                   const int64_t timeout_us) const;
  // TODO(@gaishun.gs): remove these virtual functions later
  virtual bool check_is_inited_() const = 0;
  virtual const ObTabletMeta &get_tablet_meta_() const = 0;
  virtual int get_mds_table_handle_(mds::MdsTableHandle &handle,
                                    const bool create_if_not_exist) const = 0;
  virtual ObTabletBasePointer *get_tablet_pointer_() const = 0;
  template <typename K, typename V>
  int read_data_from_tablet_cache(const K &key,
                                  const common::ObFunction<int(const V&)> &read_op,
                                  bool &applied_success) const;
  template <typename K, typename V>
  int read_data_from_mds_sstable(common::ObIAllocator &allocator,
                                 const K &key,
                                 const share::SCN &snapshot,
                                 const int64_t timeout_us,
                                 const common::ObFunction<int(const V&)> &read_op) const;
  template <typename K, typename V>
  int read_data_from_cache_or_mds_sstable(common::ObIAllocator &allocator,
                                          const K &key,
                                          const share::SCN &snapshot,
                                          const int64_t timeout_us,
                                          const common::ObFunction<int(const V&)> &read_op) const;
  template <typename K, typename V>
  int get_mds_data_from_tablet(
    const K &key,
    const share::SCN &snapshot,
    const int64_t timeout_us,
    const common::ObFunction<int(const V&)> &read_op) const;
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
  int get_tablet_handle_from_this(
    ObTabletHandle &tablet_handle) const;
  template <typename K, typename T>
  int mds_range_query(
      ObTableScanParam &scan_param,
      ObMdsRangeQueryIterator<K, T> &iter) const;

  template <typename T>
  int replay(T &&mds,
             mds::MdsCtx &ctx,
             const share::SCN &scn);
  int get_src_tablet_handle_and_base_ptr_(ObTabletHandle &tablet_handle,
                                          ObITabletMdsInterface *&base_ptr) const;
  template <typename T, typename OP>
  int cross_ls_get_latest(const ObITabletMdsInterface *another,
                          OP &&read_op,
                          mds::MdsWriter &writer,// FIXME(zk250686): should not exposed, will be removed later
                          mds::TwoPhaseCommitState &trans_stat,// FIXME(zk250686): should not exposed, will be removed later
                          share::SCN &trans_version,// FIXME(zk250686): should not exposed, will be removed later
                          const int64_t read_seq = 0) const;
  template <typename Key, typename Value>
  int replay(const Key &key,
             Value &&mds,
             mds::MdsCtx &ctx,
             const share::SCN &scn);
private:
  template <typename Key, typename Value>
  int replay_remove(const Key &key,
                    mds::MdsCtx &ctx,
                    const share::SCN &scn);// called only by ObTabletReplayExecutor
  template <typename Key, typename Value, typename OP>
  int cross_ls_get_snapshot(const ObITabletMdsInterface *another,
                            const Key &key,
                            OP &&read_op,
                            const share::SCN snapshot,
                            const int64_t timeout_us) const;
  template <typename T>
  int cross_ls_get_latest_committed(const ObITabletMdsInterface *another,
                                    T &value,
                                    ObIAllocator *alloc = nullptr) const;
  common::ObTabletID get_tablet_id_() const;
  template <typename T>
  int obj_to_string_holder_(const T &obj, ObStringHolder &holder) const;
  template <typename T>
  int fill_virtual_info_by_obj_(const T &obj, const mds::NodePosition position, ObIArray<mds::MdsNodeInfoForVirtualTable> &mds_node_info_array) const;
  template <typename K, typename T>
  int fill_virtual_info_from_mds_sstable(ObIArray<mds::MdsNodeInfoForVirtualTable> &mds_node_info_array) const;
  template <class T, ENABLE_IF_IS_SAME_CLASS(T, ObTabletCreateDeleteMdsUserData)>
  int check_mds_data_complete_(bool &is_complete) const  { is_complete = true; return OB_SUCCESS; } // Only for tablet_Status, which doesn't need data integrity check.
  template <class T, ENABLE_IF_NOT_SAME_CLASS(T, ObTabletCreateDeleteMdsUserData)>
  int check_mds_data_complete_(bool &is_complete) const;
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

struct ReadAutoIncSeqValueOp
{
  ReadAutoIncSeqValueOp(uint64_t &auto_inc_seq_value)
    : auto_inc_seq_value_(auto_inc_seq_value) {}
  int operator()(const share::ObTabletAutoincSeq &data)
  {
    return data.get_autoinc_seq_value(auto_inc_seq_value_);
  }
  uint64_t &auto_inc_seq_value_;
};

struct ReadSplitDataOp
{
  ReadSplitDataOp(ObTabletSplitMdsUserData &split_data) : split_data_(split_data) {}
  int operator()(const ObTabletSplitMdsUserData &data)
  {
    return split_data_.assign(data);
  }
  ObTabletSplitMdsUserData &split_data_;
};

struct ReadSplitDataPartkeyCompareOp
{
  ReadSplitDataPartkeyCompareOp(const blocksstable::ObDatumRowkey &rowkey,
                                const ObITableReadInfo &rowkey_read_info,
                                const ObIArray<uint64_t> &partkey_projector,
                                int &cmp_ret)
    : rowkey_(rowkey), rowkey_read_info_(rowkey_read_info), partkey_projector_(partkey_projector),
      cmp_ret_(cmp_ret) {}
  int operator()(const ObTabletSplitMdsUserData &data)
  {
    return data.partkey_compare(rowkey_, rowkey_read_info_, partkey_projector_, cmp_ret_);
  }
  const blocksstable::ObDatumRowkey &rowkey_;
  const ObITableReadInfo &rowkey_read_info_;
  const ObIArray<uint64_t> &partkey_projector_;
  int &cmp_ret_;
};

struct ReadDirectLoadAutoIncSeqOp
{
  ReadDirectLoadAutoIncSeqOp(ObDirectLoadAutoIncSeqData &inc_seq)
   : inc_seq_(inc_seq) {}
  ~ReadDirectLoadAutoIncSeqOp() = default;
  int operator()(const ObDirectLoadAutoIncSeqData &data)
  {
    return inc_seq_.set_seq_val(data.get_seq_val());
  }
  ObDirectLoadAutoIncSeqData &inc_seq_;
};

}
}

#ifndef INCLUDE_OB_TABLET_MDS_PART_IPP
#define INCLUDE_OB_TABLET_MDS_PART_IPP
#include "ob_i_tablet_mds_interface.ipp"
#endif

#endif
