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
#ifndef STORAGE_MULTI_DATA_SOURCE_MDS_TABLE_IMPL_H
#define STORAGE_MULTI_DATA_SOURCE_MDS_TABLE_IMPL_H

#include "lib/lock/ob_small_spin_lock.h"
#include "storage/multi_data_source/compile_utility/mds_dummy_key.h"
#include "storage/multi_data_source/runtime_utility/common_define.h"
#include "mds_unit.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/ob_define.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/utility.h"
#include "ob_clock_generator.h"
#include "ob_tablet_id.h"
#include "share/ob_errno.h"
#include "share/scn.h"
#include "storage/multi_data_source/runtime_utility/mds_factory.h"
#include "lib/ob_errno.h"
#include "mds_ctx.h"
#include "mds_node.h"
#include "lib/utility/ob_print_utils.h"
#include "adapter_define/mds_dump_node.h"
#include "mds_writer.h"
#include "common/ob_tablet_id.h"
#include "share/ob_ls_id.h"
#include "mds_table_base.h"
#include "compile_utility/map_type_index_in_tuple.h"
#include "storage/multi_data_source/compile_utility/compile_mapper.h"

namespace oceanbase
{
namespace storage
{
namespace mds
{
class MdsDumpNode;
class MdsTableHandle;

typedef DropFirstElemtTuple<char
#define GENERATE_TEST_MDS_TABLE
#define _GENERATE_MDS_UNIT_(KEY_TYPE, VALUE_TYPE, NEED_MULTI_VERSION) \
        ,MdsUnit<KEY_TYPE, VALUE_TYPE>
#include "compile_utility/mds_register.h"
#undef _GENERATE_MDS_UNIT_
#undef GENERATE_TEST_MDS_TABLE
>::type UnitTestMdsTable;

typedef DropFirstElemtTuple<char
#define GENERATE_NORMAL_MDS_TABLE
#define _GENERATE_MDS_UNIT_(KEY_TYPE, VALUE_TYPE, NEED_MULTI_VERSION) \
        ,MdsUnit<KEY_TYPE, VALUE_TYPE>
#include "compile_utility/mds_register.h"
#undef _GENERATE_MDS_UNIT_
#undef GENERATE_NORMAL_MDS_TABLE
>::type NormalMdsTable;

typedef DropFirstElemtTuple<char
#define GENERATE_LS_INNER_MDS_TABLE
#define _GENERATE_MDS_UNIT_(KEY_TYPE, VALUE_TYPE, NEED_MULTI_VERSION) \
        ,MdsUnit<KEY_TYPE, VALUE_TYPE>
#include "compile_utility/mds_register.h"
#undef _GENERATE_MDS_UNIT_
#undef GENERATE_LS_INNER_MDS_TABLE
>::type LsInnerMdsTable;

typedef ObTuple<UnitTestMdsTable, NormalMdsTable, LsInnerMdsTable> MdsTableTypeTuple;

template <typename MdsTableType>
struct GET_MDS_TABLE_ID {
  static constexpr uint8_t value = MdsTableTypeTuple::get_element_index<MdsTableType>();
};

template <typename MdsTableType, typename K, typename V>
struct GET_MDS_UNIT_ID {
  static constexpr uint8_t value = MdsTableType::template get_element_index<MdsUnit<K, V>>();
};

template <typename MdsTableType>
class MdsTableImpl final : public MdsTableBase
{
  friend class MdsDumpNode;
  friend class MdsTableHandle;
public:
  MdsTableImpl();
  virtual ~MdsTableImpl() override;
  virtual int set(int64_t unit_id,
                  void *key,
                  void *data,
                  bool is_rvalue,
                  MdsCtx &ctx,
                  const int64_t lock_timeout_us) override;
  virtual int replay(int64_t unit_id,
                     void *key,
                     void *data,
                     bool is_rvalue,
                     MdsCtx &ctx,
                     const share::SCN &scn) override;
  virtual int remove(int64_t unit_id,
                     void *key,
                     MdsCtx &ctx,
                     const int64_t lock_timeout_us) override;
  virtual int replay_remove(int64_t unit_id,
                            void *key,
                            MdsCtx &ctx,
                            const share::SCN &scn) override;
  virtual int get_latest(int64_t unit_id,
                         void *key,
                         ObFunction<int(void *)> &op,
                         MdsWriter &writer,// FIXME(xuwang.txw): should not exposed, will be removed later
                         TwoPhaseCommitState &trans_stat,// FIXME(xuwang.txw): should not exposed, will be removed later
                         share::SCN &trans_version,// FIXME(xuwang.txw): should not exposed, will be removed later
                         const int64_t read_seq) const override;
  virtual int get_latest_committed(int64_t unit_id,
                                   void *key,
                                   ObFunction<int(void *)> &op) const override;
  virtual int get_tablet_status_node(ObFunction<int(void *)> &op,
                                     const int64_t read_seq) const override;
  virtual int get_snapshot(int64_t unit_id,
                           void *key,
                           ObFunction<int(void *)> &op,
                           const share::SCN &snapshot,
                           const int64_t timeout_us) const override;
  virtual int get_by_writer(int64_t unit_id,
                            void *key,
                            ObFunction<int(void *)> &op,
                            const MdsWriter &writer,
                            const share::SCN &snapshot,
                            const transaction::ObTxSEQ read_seq,
                            const int64_t timeout_us) const override;
  virtual int is_locked_by_others(int64_t unit_id,
                                  void *key,
                                  bool &is_locked,
                                  const MdsWriter &self = MdsWriter()) const override;
  virtual int scan_all_nodes_to_dump(ObFunction<int(const MdsDumpKV&)> &for_each_op,
                                     const int64_t mds_construct_sequence,
                                     const bool for_flush,
                                     const ScanRowOrder scan_row_order,
                                     const ScanNodeOrder scan_node_order) const override;
  virtual int operate(const ObFunction<int(MdsTableBase &)> &operation) override;
  virtual int flush(share::SCN need_advanced_rec_scn_lower_limit, share::SCN max_decided_scn) override;
  int calculate_flush_scn_and_need_dumped_nodes_cnt_(share::SCN &flush_scn, int64_t &need_dumped_nodes_cnt);
  void calculate_rec_scn_and_advance_to_it_(share::SCN on_flush_scn);
  void on_flush_(const share::SCN &flush_scn, const int flush_ret);
  virtual void on_flush(const share::SCN &flush_scn, const int flush_ret) override;
  virtual int try_recycle(const share::SCN recycle_scn) override;
  virtual int fill_virtual_info(ObIArray<MdsNodeInfoForVirtualTable> &mds_node_info_array) const override {
    ForEachUnitFillVirtualInfoHelper helper(mds_node_info_array);
    return unit_tuple_.for_each(helper);
  }
  virtual int forcely_remove_nodes(const char *reason, share::SCN redo_scn_limit) override;
  template <typename DUMP_OP, ENABLE_IF_LIKE_FUNCTION(DUMP_OP, int(const MdsDumpKV &))>
  int scan_all_nodes_to_dump(DUMP_OP &&for_each_op,
                             const int64_t mds_construct_sequence,
                             const bool for_flush,
                             const ScanRowOrder scan_row_order,
                             const ScanNodeOrder scan_node_order);
  TO_STRING_KV(KP(this), K_(ls_id), K_(tablet_id), K_(flushing_scn), K_(rec_scn), K_(max_aborted_scn),
               K_(last_inner_recycled_scn), K_(total_node_cnt), K_(construct_sequence), K_(debug_info));
  template <typename SCAN_OP>
  int for_each_scan_row(FowEachRowAction action_type, SCAN_OP &&op);
  MdsTableType &unit_tuple() { return unit_tuple_; }
private:// helper define
  struct ForEachUnitFillVirtualInfoHelper {
    ForEachUnitFillVirtualInfoHelper(ObIArray<MdsNodeInfoForVirtualTable> &array) : array_(array), idx_(0) {}
    template <typename K, typename V>
    int operator()(const MdsUnit<K, V> &unit) {
      return unit.fill_virtual_info(array_, idx_++);
    }
  private:
    ObIArray<MdsNodeInfoForVirtualTable> &array_;
    int64_t idx_;
  };
  template <typename DUMP_OP, ENABLE_IF_LIKE_FUNCTION(DUMP_OP, int(const MdsDumpKV &))>
  struct ForEachUnitDumpHelper {// this operator applied on all row in units
    ForEachUnitDumpHelper(DUMP_OP &op,
                          const share::SCN flusing_scn,
                          const bool for_flush,
                          const ScanRowOrder scan_row_order,
                          const ScanNodeOrder scan_node_order)
    : op_(op),
    flusing_scn_(flusing_scn),
    for_flush_(for_flush),
    scan_row_order_(scan_row_order),
    scan_node_order_(scan_node_order) {}
    template <typename K, typename V>
    int operator()(MdsUnit<K, V> &unit) {
      uint8_t mds_table_id = GET_MDS_TABLE_ID<MdsTableType>::value;
      uint8_t mds_unit_id = GET_MDS_UNIT_ID<MdsTableType, K, V>::value;
      return unit.scan_KV_row(op_,
                              flusing_scn_,
                              mds_table_id,
                              mds_unit_id,
                              for_flush_,
                              scan_row_order_,
                              scan_node_order_);
    }
  private:
    DUMP_OP &op_;
    const share::SCN flusing_scn_;
    const bool for_flush_;
    const ScanRowOrder scan_row_order_;
    const ScanNodeOrder scan_node_order_;
  };
  template <typename SCAN_OP>
  struct ForEachUnitScanRowHelper {
    ForEachUnitScanRowHelper(FowEachRowAction action, SCAN_OP &op) : op_(op), action_type_(action) {}
    template <typename K, typename V>
    int operator()(MdsUnit<K, V> &unit) { return unit.for_each_row(action_type_, op_); }
  private:
    SCAN_OP &op_;
    FowEachRowAction action_type_;
  };
  template <typename DUMP_OP, ENABLE_IF_LIKE_FUNCTION(DUMP_OP, int(const MdsDumpKV &))>
  int for_each_to_dump_node_(DUMP_OP &&op,
                             const share::SCN flushing_scn,
                             const bool for_flush,
                             const ScanRowOrder scan_row_order,
                             const ScanNodeOrder scan_node_order) {
    ForEachUnitDumpHelper<DUMP_OP> for_each_op(op, flushing_scn, for_flush, scan_row_order, scan_node_order);
    return unit_tuple_.for_each(for_each_op);
  }
  MdsTableType unit_tuple_;
};

}
}
}

#ifndef STORAGE_MULTI_DATA_SOURCE_MDS_TABLE_IMPL_H_IPP
#define STORAGE_MULTI_DATA_SOURCE_MDS_TABLE_IMPL_H_IPP
#include "mds_table_impl.ipp"
#endif

#endif