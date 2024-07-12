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

#include "storage/tablet/ob_tablet_full_memory_mds_data.h"
#include "lib/allocator/ob_allocator.h"
#include "storage/blockstore/ob_shared_block_reader_writer.h"
#include "storage/tablet/ob_tablet_mds_data.h"
#include "storage/tablet/ob_tablet_obj_load_helper.h"
#include "storage/multi_data_source/mds_table_impl.h"
#include "storage/tablet/ob_tablet_mds_table_mini_merger.h"

#define USING_LOG_PREFIX STORAGE

using namespace oceanbase::common;

namespace oceanbase
{
namespace storage
{
ObTabletFullMemoryMdsData::ObTabletFullMemoryMdsData()
  : is_inited_(false),
    tablet_status_uncommitted_kv_(),
    tablet_status_committed_kv_(),
    aux_tablet_info_uncommitted_kv_(),
    aux_tablet_info_committed_kv_(),
    medium_info_list_(),
    auto_inc_seq_()
{
}

ObTabletFullMemoryMdsData::~ObTabletFullMemoryMdsData()
{
  reset();
}

int ObTabletFullMemoryMdsData::init(common::ObArenaAllocator &allocator)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K_(is_inited));
  } else if (OB_FAIL(medium_info_list_.medium_info_list_.init_for_first_creation(allocator))) {
    LOG_WARN("failed to init medium info list", K(ret));
  } else {
    is_inited_ = true;
  }

  return ret;
}

int ObTabletFullMemoryMdsData::init(common::ObArenaAllocator &allocator, const ObTabletMdsData &mds_data)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K_(is_inited));
  } else if (OB_FAIL(read_mds_dump_kv(allocator, mds_data.tablet_status_.uncommitted_kv_, tablet_status_uncommitted_kv_))) {
    LOG_WARN("failed to read mds dump kv", K(ret));
  } else if (OB_FAIL(read_mds_dump_kv(allocator, mds_data.tablet_status_.committed_kv_, tablet_status_committed_kv_))) {
    LOG_WARN("failed to read mds dump kv", K(ret));
  } else if (OB_FAIL(read_mds_dump_kv(allocator, mds_data.aux_tablet_info_.uncommitted_kv_, aux_tablet_info_uncommitted_kv_))) {
    LOG_WARN("failed to read mds dump kv", K(ret));
  } else if (OB_FAIL(read_mds_dump_kv(allocator, mds_data.aux_tablet_info_.committed_kv_, aux_tablet_info_committed_kv_))) {
    LOG_WARN("failed to read mds dump kv", K(ret));
  } else if (OB_FAIL(read_medium_info_list(allocator, mds_data.medium_info_list_, medium_info_list_.medium_info_list_))) {
    LOG_WARN("failed to assign medium info list", K(ret), K(mds_data));
  } else if (OB_FAIL(read_auto_inc_seq(allocator, mds_data.auto_inc_seq_, auto_inc_seq_))) {
    LOG_WARN("failed to read auto inc seq", K(ret));
  } else {
    medium_info_list_.extra_medium_info_.info_ = mds_data.extra_medium_info_.info_;
    medium_info_list_.extra_medium_info_.last_medium_scn_ = mds_data.extra_medium_info_.last_medium_scn_;

    is_inited_ = true;
  }

  return ret;
}

void ObTabletFullMemoryMdsData::reset()
{
  tablet_status_uncommitted_kv_.reset();
  tablet_status_committed_kv_.reset();
  aux_tablet_info_uncommitted_kv_.reset();
  aux_tablet_info_committed_kv_.reset();
  medium_info_list_.reset();
  auto_inc_seq_.reset();
  is_inited_ = false;
}

int ObTabletFullMemoryMdsData::assign(const ObTabletFullMemoryMdsData &other, common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;

  if (this != &other) {
    reset();

    if (OB_FAIL(tablet_status_uncommitted_kv_.assign(other.tablet_status_uncommitted_kv_, allocator))) {
      LOG_WARN("failed to assign", K(ret), K(other));
    } else if (OB_FAIL(tablet_status_committed_kv_.assign(other.tablet_status_committed_kv_, allocator))) {
      LOG_WARN("failed to assign", K(ret), K(other));
    } else if (OB_FAIL(aux_tablet_info_uncommitted_kv_.assign(other.aux_tablet_info_uncommitted_kv_, allocator))) {
      LOG_WARN("failed to assign", K(ret), K(other));
    } else if (OB_FAIL(aux_tablet_info_committed_kv_.assign(other.aux_tablet_info_committed_kv_, allocator))) {
      LOG_WARN("failed to assign", K(ret), K(other));
    } else if (OB_FAIL(medium_info_list_.assign(allocator, other.medium_info_list_))) {
      LOG_WARN("failed to assign", K(ret), K(other));
    } else if (OB_FAIL(auto_inc_seq_.assign(allocator, other.auto_inc_seq_))) {
      LOG_WARN("failed to assign", K(ret), K(other));
    } else {
      is_inited_ = true;
    }
  }

  return ret;
}

int ObTabletFullMemoryMdsData::read_mds_dump_kv(
    common::ObArenaAllocator &allocator,
    const ObTabletComplexAddr<mds::MdsDumpKV> &mds_dump_kv_addr,
    mds::MdsDumpKV &dump_kv)
{
  int ret = OB_SUCCESS;
  mds::MdsDumpKV *ptr = nullptr;

  if (OB_UNLIKELY(!mds_dump_kv_addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(mds_dump_kv_addr));
  } else if (OB_FAIL(ObTabletMdsData::load_mds_dump_kv(allocator, mds_dump_kv_addr, ptr))) {
    LOG_WARN("failed to load mds dump kv", K(ret));
  } else if (nullptr == ptr) {
    // do nothing
    dump_kv.reset();
  } else if (OB_FAIL(dump_kv.assign(*ptr, allocator))) {
    LOG_WARN("failed to copy mds dump kv", K(ret));
  }

  ObTabletObjLoadHelper::free(allocator, ptr);

  return ret;
}

int ObTabletFullMemoryMdsData::read_medium_info_list(
    common::ObArenaAllocator &allocator,
    const ObTabletComplexAddr<ObTabletDumpedMediumInfo> &medium_info_list_addr,
    ObTabletDumpedMediumInfo &medium_info_list)
{
  int ret = OB_SUCCESS;
  ObTabletDumpedMediumInfo *ptr = nullptr;

  if (OB_UNLIKELY(!medium_info_list_addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(medium_info_list_addr));
  } else if (OB_FAIL(ObTabletMdsData::load_medium_info_list(allocator, medium_info_list_addr, ptr))) {
    LOG_WARN("failed to load medium info list", K(ret));
  } else if (nullptr == ptr && OB_FAIL(medium_info_list.init_for_first_creation(allocator))) {
    LOG_WARN("failed to init medium info list", K(ret));
  } else if (nullptr != ptr && OB_FAIL(medium_info_list.assign(*ptr, allocator))) {
    LOG_WARN("failed to copy medium info list", K(ret));
  }

  ObTabletObjLoadHelper::free(allocator, ptr);

  return ret;
}

int ObTabletFullMemoryMdsData::read_auto_inc_seq(
    common::ObArenaAllocator &allocator,
    const ObTabletComplexAddr<share::ObTabletAutoincSeq> &auto_inc_seq_addr,
    share::ObTabletAutoincSeq &auto_inc_seq)
{
  int ret = OB_SUCCESS;
  share::ObTabletAutoincSeq *ptr = nullptr;

  if (OB_FAIL(ObTabletMdsData::load_auto_inc_seq(allocator, auto_inc_seq_addr, ptr))) {
    LOG_WARN("failed to load auto inc seq", K(ret), K(auto_inc_seq_addr));
  } else if (nullptr == ptr) {
    // do nothing
  } else if (OB_FAIL(auto_inc_seq.assign(allocator, *ptr))) {
    LOG_WARN("failed to copy auto inc seq", K(ret));
  }

  return ret;
}

int ObTabletFullMemoryMdsData::scan_all_mds_data_with_op(ObMdsMiniMergeOperator &op) const
{
  int ret = OB_SUCCESS;
  // key from small to big
  // 1. dump tablet_status
  // 2. dump aux tablet info
  // 3. dump auto inc seq
  // 4. dump medium info list

  TIMEGUARD_INIT(STORAGE, 10_ms);
  ObArenaAllocator allocator;
  share::SCN tablet_status_create_commit_scn;
  mds::MdsDumpKV tmp_mds_kv;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletFullMemoryMdsData not init", K(ret));
  } else if (OB_UNLIKELY(aux_tablet_info_uncommitted_kv_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected valid uncommitted kv", K(ret), K(aux_tablet_info_uncommitted_kv_));
  } else if (tablet_status_uncommitted_kv_.is_valid()) {
    LOG_INFO("tablet status uncommitted kv is valid", K(ret), K(tablet_status_uncommitted_kv_));
  }

  if (OB_FAIL(ret)) {
  } else if (!tablet_status_committed_kv_.v_.user_data_.empty() && CLICK_FAIL(op(tablet_status_committed_kv_))) {
    LOG_WARN("failed to dump tablet status", K(ret), K(tablet_status_committed_kv_));
  } else if (!aux_tablet_info_committed_kv_.v_.user_data_.empty() && CLICK_FAIL(op(aux_tablet_info_committed_kv_))) {
    LOG_WARN("failed to dump aux tablet info", K(ret), K(aux_tablet_info_committed_kv_));
  }

  if (OB_SUCC(ret)) {
    int64_t pos = 0;
    ObTabletCreateDeleteMdsUserData data;
    const common::ObString &str = tablet_status_committed_kv_.v_.user_data_;
    if (str.empty()) {
      LOG_INFO("tablet status user data is empty", K(ret), K(tablet_status_committed_kv_));
    } else if (CLICK_FAIL(data.deserialize(str.ptr(), str.length(), pos))) {
      LOG_WARN("failed to deserialize user data", K(ret), K(str));
    } else if (OB_UNLIKELY(!data.create_commit_scn_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect invalid create commit scn", K(ret), K(data.create_commit_scn_));
    } else {
      tablet_status_create_commit_scn = data.create_commit_scn_;
    }
  }

  if (OB_SUCC(ret)) {
    mds::MdsDumpKV tmp_mds_kv;
    if (!auto_inc_seq_.is_valid()) {
      LOG_INFO("auto inc seq is invalid", K(ret), K(auto_inc_seq_));
    } else if (CLICK_FAIL(mock_convert_auto_inc_seq_to_mds_dump_kv(allocator,
        auto_inc_seq_, tablet_status_create_commit_scn, tmp_mds_kv))) {
      LOG_WARN("failed to convert auto_inc_seq to mds_dump_kv",
          K(ret), K(auto_inc_seq_), K(tablet_status_create_commit_scn));
    } else if (CLICK_FAIL(op(tmp_mds_kv))) {
      LOG_WARN("failed to dump auto_inc_seq", K(ret), K(tmp_mds_kv), K(auto_inc_seq_));
    }
  }

  if (OB_SUCC(ret)) {
    mds::MdsDumpKV tmp_mds_kv;
    const common::ObSEArray<compaction::ObMediumCompactionInfo*, 1> &array = medium_info_list_.medium_info_list_.medium_info_list_;
    for (int64_t i = 0; OB_SUCC(ret) && i < array.count(); ++i) {
      const compaction::ObMediumCompactionInfo *medium_info = array.at(i);
      if (OB_ISNULL(medium_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, medium info is null", K(ret), K(i), KP(medium_info), K(array));
      } else if (CLICK_FAIL(mock_convert_medium_info_to_mds_dump_kv(allocator,
          *medium_info, tablet_status_create_commit_scn, tmp_mds_kv))) {
        LOG_WARN("failed to convert medium_info to mds_dump_kv",
            K(ret), K(i), KPC(medium_info), K(tablet_status_create_commit_scn));
      } else if (CLICK_FAIL(op(tmp_mds_kv))) {
        LOG_WARN("failed to dump medium_info", K(ret),
            K(i), K(tmp_mds_kv), K(aux_tablet_info_committed_kv_));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (CLICK_FAIL(op.finish())) {
    LOG_WARN("failed to finish dump op", K(ret), KPC(this));
  }

  return ret;
}

template <typename UnitKey>
static int inner_mock_mds_node(
    common::ObArenaAllocator &allocator,
    const UnitKey &key,
    const share::SCN tablet_status_create_commit_scn,
    mds::MdsDumpNode &node)
{
  int ret = OB_SUCCESS;
  node.status_.union_.field_.node_type_ = mds::MdsNodeType::SET;
  node.status_.union_.field_.state_ = mds::TwoPhaseCommitState::ON_COMMIT;
  node.status_.union_.field_.is_dumped_ = true;
  node.allocator_ = &allocator;
  node.writer_id_ = mds::MdsWriter::DEFAULT_WRITER_ID;
  node.seq_no_ = transaction::ObTxSEQ::MIN_VAL();
  node.redo_scn_ = share::SCN::plus(tablet_status_create_commit_scn, 1);
  node.end_scn_ = share::SCN::plus(tablet_status_create_commit_scn, 1);
  node.trans_version_ = share::SCN::plus(tablet_status_create_commit_scn, 1);

  const int64_t user_data_size = key.get_serialize_size();
  char *user_data_buffer = nullptr;
  int64_t pos = 0;
  if (OB_ISNULL(user_data_buffer = (char*)allocator.alloc(user_data_size))) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("fail to alloc buffer", K(ret), K(user_data_size));
  } else if (OB_FAIL(key.serialize(user_data_buffer, user_data_size, pos))) {
    LOG_WARN("failed to serialize auto_inc_seq", K(ret));
  } else {
    node.user_data_.assign(user_data_buffer, user_data_size);
    node.crc_check_number_ = node.generate_hash();
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(user_data_buffer)) {
      allocator.free(user_data_buffer);
    }
  }
  return ret;
}

int ObTabletFullMemoryMdsData::mock_convert_auto_inc_seq_to_mds_dump_kv(
      common::ObArenaAllocator &allocator,
      const share::ObTabletAutoincSeq &auto_inc_seq,
      const share::SCN tablet_status_create_commit_scn,
      mds::MdsDumpKV &dump_kv)
{
  int ret = OB_SUCCESS;
  dump_kv.reset();
  mds::MdsDumpKey &key = dump_kv.k_;
  mds::MdsDumpNode &node = dump_kv.v_;

  constexpr uint8_t table_id = mds::TupleTypeIdx<mds::MdsTableTypeTuple, mds::NormalMdsTable>::value;
  constexpr uint8_t unit_id = mds::TupleTypeIdx<mds::NormalMdsTable, mds::MdsUnit<mds::DummyKey, share::ObTabletAutoincSeq>>::value;
  mds::DummyKey dummy_key;
  if (OB_FAIL(key.init(table_id, unit_id, dummy_key, allocator))) {
    LOG_WARN("failed to init mds dump key", K(ret));
  } else {
    node.mds_table_id_ = table_id;
    node.mds_unit_id_ = unit_id;
    node.status_.union_.field_.writer_type_ = mds::WriterType::AUTO_INC_SEQ;
    if (OB_FAIL(inner_mock_mds_node(allocator, auto_inc_seq, tablet_status_create_commit_scn, node))) {
      LOG_WARN("failed to inner mock mds node", K(ret),
          K(auto_inc_seq), K(tablet_status_create_commit_scn));
    }
  }
  return ret;
}


int ObTabletFullMemoryMdsData::mock_convert_medium_info_to_mds_dump_kv(
      common::ObArenaAllocator &allocator,
      const compaction::ObMediumCompactionInfo& medium_info,
      const share::SCN tablet_status_create_commit_scn,
      mds::MdsDumpKV &dump_kv)
{
  int ret = OB_SUCCESS;
  dump_kv.reset();
  mds::MdsDumpKey &key = dump_kv.k_;
  mds::MdsDumpNode &node = dump_kv.v_;
  constexpr uint8_t table_id = mds::TupleTypeIdx<mds::MdsTableTypeTuple, mds::NormalMdsTable>::value;
  constexpr uint8_t unit_id = mds::TupleTypeIdx<mds::NormalMdsTable, mds::MdsUnit<compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo>>::value;

  compaction::ObMediumCompactionInfoKey medium_key(medium_info.medium_snapshot_);
  if (OB_FAIL(key.init(table_id, unit_id, medium_key, allocator))) {
    LOG_WARN("failed to init mds dump key", K(ret));
  } else {
    node.mds_table_id_ = table_id;
    node.mds_unit_id_ = unit_id;
    node.status_.union_.field_.writer_type_ = mds::WriterType::MEDIUM_INFO;
    if (OB_FAIL(inner_mock_mds_node(allocator, medium_info, tablet_status_create_commit_scn, node))) {
      LOG_WARN("failed to inner mock mds node", K(ret),
          K(medium_info), K(tablet_status_create_commit_scn));
    }
  }
  return ret;
}

int ObTabletFullMemoryMdsData::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
      tablet_status_uncommitted_kv_,
      tablet_status_committed_kv_,
      aux_tablet_info_uncommitted_kv_,
      aux_tablet_info_committed_kv_,
      medium_info_list_,
      auto_inc_seq_);

  return ret;
}

int ObTabletFullMemoryMdsData::deserialize(common::ObIAllocator &allocator, const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K_(is_inited));
  } else if (OB_FAIL(tablet_status_uncommitted_kv_.deserialize(allocator, buf, data_len, pos))) {
    LOG_WARN("failed to deserialize", K(ret));
  } else if (OB_FAIL(tablet_status_committed_kv_.deserialize(allocator, buf, data_len, pos))) {
    LOG_WARN("failed to deserialize", K(ret));
  } else if (OB_FAIL(aux_tablet_info_uncommitted_kv_.deserialize(allocator, buf, data_len, pos))) {
    LOG_WARN("failed to deserialize", K(ret));
  } else if (OB_FAIL(aux_tablet_info_committed_kv_.deserialize(allocator, buf, data_len, pos))) {
    LOG_WARN("failed to deserialize", K(ret));
  } else if (OB_FAIL(medium_info_list_.deserialize(allocator, buf, data_len, pos))) {
    LOG_WARN("failed to deserialize", K(ret));
  } else if (OB_FAIL(auto_inc_seq_.deserialize(allocator, buf, data_len, pos))) {
    LOG_WARN("failed to deserialize", K(ret));
  } else {
    is_inited_ = true;
  }

  return ret;
}

int64_t ObTabletFullMemoryMdsData::get_serialize_size() const
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
      tablet_status_uncommitted_kv_,
      tablet_status_committed_kv_,
      aux_tablet_info_uncommitted_kv_,
      aux_tablet_info_committed_kv_,
      medium_info_list_,
      auto_inc_seq_);

  return len;
}
} // namespace storage
} // namespace oceanbase
