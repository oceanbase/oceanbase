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

#include "storage/tablet/ob_tablet_mds_node_dump_operator.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "lib/string/ob_string.h"
#include "share/ob_errno.h"
#include "share/ob_tablet_autoincrement_param.h"
#include "storage/tablet/ob_tablet_mds_data.h"
#include "storage/tablet/ob_tablet_create_delete_mds_user_data.h"
#include "storage/multi_data_source/adapter_define/mds_dump_node.h"
#include "storage/multi_data_source/compile_utility/mds_dummy_key.h"
#include "storage/multi_data_source/mds_table_handle.h"

#define USING_LOG_PREFIX MDS

namespace oceanbase
{
namespace storage
{
ObTabletDumpMdsNodeOperator::ObTabletDumpMdsNodeOperator(ObTabletMdsData &mds_data, common::ObIAllocator &allocator)
  : mds_data_(mds_data),
    allocator_(allocator)
{
}

template <>
int ObTabletDumpMdsNodeOperator::dump<mds::DummyKey, ObTabletCreateDeleteMdsUserData>(const mds::MdsDumpKV &kv, bool &dumped)
{
  int ret = OB_SUCCESS;
  constexpr uint8_t table_id = mds::TupleTypeIdx<mds::MdsTableTypeTuple, mds::NormalMdsTable>::value;
  constexpr uint8_t unit_id = mds::TupleTypeIdx<mds::NormalMdsTable, mds::MdsUnit<mds::DummyKey, ObTabletCreateDeleteMdsUserData>>::value;
  const mds::MdsDumpKey &key = kv.k_;
  const mds::MdsDumpNode &node = kv.v_;

  if (table_id == key.mds_table_id_ && unit_id == key.mds_unit_id_) {
    const mds::TwoPhaseCommitState &state = node.status_.get_state();
    switch (state) {
      case mds::TwoPhaseCommitState::STATE_INIT:
      case mds::TwoPhaseCommitState::BEFORE_PREPARE:
      case mds::TwoPhaseCommitState::ON_ABORT:
      case mds::TwoPhaseCommitState::ON_PREPARE:
        if (OB_FAIL(mds_data_.tablet_status_.uncommitted_kv_.ptr_->assign(kv, allocator_))) {
          LOG_WARN("failed to copy mds dump kv", K(ret));
        }
        break;
      case mds::TwoPhaseCommitState::ON_COMMIT:
        if (OB_FAIL(mds_data_.tablet_status_.committed_kv_.ptr_->assign(kv, allocator_))) {
          LOG_WARN("failed to copy mds dump kv", K(ret));
        }
        break;
      case mds::TwoPhaseCommitState::STATE_END:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid state", K(ret), K(table_id), K(unit_id), K(state));
        break;
    }

    if (OB_SUCC(ret)) {
      dumped = true;
    }
  }

  return ret;
}

template <>
int ObTabletDumpMdsNodeOperator::dump<mds::DummyKey, ObTabletBindingMdsUserData>(const mds::MdsDumpKV &kv, bool &dumped)
{
  int ret = OB_SUCCESS;
  constexpr uint8_t table_id = mds::TupleTypeIdx<mds::MdsTableTypeTuple, mds::NormalMdsTable>::value;
  constexpr uint8_t unit_id = mds::TupleTypeIdx<mds::NormalMdsTable, mds::MdsUnit<mds::DummyKey, ObTabletBindingMdsUserData>>::value;
  const mds::MdsDumpKey &key = kv.k_;
  const mds::MdsDumpNode &node = kv.v_;

  if (table_id == key.mds_table_id_ && unit_id == key.mds_unit_id_) {
    const mds::TwoPhaseCommitState &state = node.status_.get_state();
    switch (state) {
      case mds::TwoPhaseCommitState::STATE_INIT:
      case mds::TwoPhaseCommitState::BEFORE_PREPARE:
      case mds::TwoPhaseCommitState::ON_ABORT:
      case mds::TwoPhaseCommitState::ON_PREPARE:
        if (OB_FAIL(mds_data_.aux_tablet_info_.uncommitted_kv_.ptr_->assign(kv, allocator_))) {
          LOG_WARN("failed to copy mds dump kv", K(ret));
        }
        break;
      case mds::TwoPhaseCommitState::ON_COMMIT:
        if (OB_FAIL(mds_data_.aux_tablet_info_.committed_kv_.ptr_->assign(kv, allocator_))) {
          LOG_WARN("failed to copy mds dump kv", K(ret));
        }
        break;
      case mds::TwoPhaseCommitState::STATE_END:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid state", K(ret), K(table_id), K(unit_id), K(state));
        break;
    }

    if (OB_SUCC(ret)) {
      dumped = true;
    }
  }

  return ret;
}

template <>
int ObTabletDumpMdsNodeOperator::dump<mds::DummyKey, share::ObTabletAutoincSeq>(const mds::MdsDumpKV &kv, bool &dumped)
{
  int ret = OB_SUCCESS;
  constexpr uint8_t table_id = mds::TupleTypeIdx<mds::MdsTableTypeTuple, mds::NormalMdsTable>::value;
  constexpr uint8_t unit_id = mds::TupleTypeIdx<mds::NormalMdsTable, mds::MdsUnit<mds::DummyKey, share::ObTabletAutoincSeq>>::value;
  const mds::MdsDumpKey &key = kv.k_;
  const mds::MdsDumpNode &node = kv.v_;

  if (table_id == key.mds_table_id_ && unit_id == key.mds_unit_id_) {
    const mds::TwoPhaseCommitState &state = node.status_.get_state();
    const common::ObString &user_data = kv.v_.user_data_;
    int64_t pos = 0;
    share::ObTabletAutoincSeq *auto_inc_seq = mds_data_.auto_inc_seq_.ptr_;
    if (OB_UNLIKELY(state != mds::TwoPhaseCommitState::ON_COMMIT)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid state", K(ret), K(state));
    } else if (OB_FAIL(auto_inc_seq->deserialize(allocator_, user_data.ptr(), user_data.length(), pos))) {
      LOG_WARN("failed to deserialize", K(ret));
    } else {
      dumped = true;
    }
  }

  return ret;
}

template <>
int ObTabletDumpMdsNodeOperator::dump<compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo>(const mds::MdsDumpKV &kv, bool &dumped)
{
  int ret = OB_SUCCESS;
  constexpr uint8_t table_id = mds::TupleTypeIdx<mds::MdsTableTypeTuple, mds::NormalMdsTable>::value;
  constexpr uint8_t unit_id = mds::TupleTypeIdx<mds::NormalMdsTable, mds::MdsUnit<compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo>>::value;
  const mds::MdsDumpKey &key = kv.k_;
  const mds::MdsDumpNode &node = kv.v_;

  if (table_id == key.mds_table_id_ && unit_id == key.mds_unit_id_) {
    const mds::TwoPhaseCommitState &state = node.status_.get_state();
    ObTabletDumpedMediumInfo *medium_info_list = mds_data_.medium_info_list_.ptr_;
    if (OB_UNLIKELY(state != mds::TwoPhaseCommitState::ON_COMMIT)) {
      ret = OB_SUCCESS;
      LOG_WARN("invalid state", K(ret), K(state));
    } else if (OB_FAIL(medium_info_list->append(key, node))) {
      LOG_WARN("failed to copy mds dump node", K(ret));
    } else {
      dumped = true;
    }
  }

  return ret;
}

int ObTabletDumpMdsNodeOperator::operator()(const mds::MdsDumpKV &kv)
{
  int ret = OB_SUCCESS;
  bool dumped = false;

  if (OB_SUCC(ret) && !dumped) {
    if (OB_UNLIKELY(OB_SUCCESS != (ret = dump<mds::DummyKey, ObTabletCreateDeleteMdsUserData>(kv, dumped)))) {
      LOG_WARN("failed to dump tablet status", K(ret), K(kv));
    }
  }

  if (OB_SUCC(ret) && !dumped) {
    if (OB_UNLIKELY(OB_SUCCESS != (ret = dump<mds::DummyKey, ObTabletBindingMdsUserData>(kv, dumped)))) {
      LOG_WARN("failed to dump aux tablet info", K(ret), K(kv));
    }
  }

  if (OB_SUCC(ret) && !dumped) {
    if (OB_UNLIKELY(OB_SUCCESS != (ret = dump<mds::DummyKey, share::ObTabletAutoincSeq>(kv, dumped)))) {
      LOG_WARN("failed to dump auto inc seq", K(ret), K(kv));
    }
  }

  if (OB_SUCC(ret) && !dumped) {
    if (OB_UNLIKELY(OB_SUCCESS != (ret = dump<compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo>(kv, dumped)))) {
      LOG_WARN("failed to dump medium info", K(ret), K(kv));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!dumped)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected kv type, not dumped", K(ret), K(kv), K(dumped));
  }

  return ret;
}


ObTabletMediumInfoNodeOperator::ObTabletMediumInfoNodeOperator(ObTabletDumpedMediumInfo &medium_info_list, common::ObIAllocator &allocator)
  : medium_info_list_(medium_info_list),
    allocator_(allocator)
{
}

int ObTabletMediumInfoNodeOperator::operator()(const mds::MdsDumpKV &kv)
{
  int ret = OB_SUCCESS;
  constexpr uint8_t table_id = mds::TupleTypeIdx<mds::MdsTableTypeTuple, mds::NormalMdsTable>::value;
  constexpr uint8_t unit_id = mds::TupleTypeIdx<mds::NormalMdsTable, mds::MdsUnit<compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo>>::value;
  const mds::MdsDumpKey &key = kv.k_;
  const mds::MdsDumpNode &node = kv.v_;

  if (table_id == key.mds_table_id_ && unit_id == key.mds_unit_id_) {
    const mds::TwoPhaseCommitState &state = node.status_.get_state();
    if (OB_UNLIKELY(state != mds::TwoPhaseCommitState::ON_COMMIT)) {
      ret = OB_SUCCESS;
      LOG_WARN("invalid state", K(ret), K(state));
    } else if (OB_FAIL(medium_info_list_.append(key, node))) {
      LOG_WARN("failed to copy mds dump node", K(ret));
    }
  }

  return ret;

  return ret;
}
} // namespace storage
} // namespace oceanbase
