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

#include <gtest/gtest.h>

#define private public
#define protected public

#include "lib/oblog/ob_log.h"
#include "lib/allocator/page_arena.h"
#include "share/ob_ls_id.h"
#include "common/ob_tablet_id.h"
#include "mtlenv/mock_tenant_module_env.h"
#include "mtlenv/storage/medium_info_helper.h"
#include "share/rc/ob_tenant_base.h"
#include "unittest/storage/test_tablet_helper.h"
#include "unittest/storage/test_dml_common.h"
#include "unittest/storage/init_basic_struct.h"
#include "unittest/storage/schema_utils.h"
#include "storage/tablet/ob_tablet_medium_info_reader.h"
#include "storage/tablet/ob_tablet_persister.h"
#include "storage/multi_data_source/mds_table_handler.h"
#include "storage/multi_data_source/runtime_utility/mds_factory.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::unittest;

#define USING_LOG_PREFIX STORAGE

#define MOCK_INSERT_MDS_TABLE(id) \
        { \
          mds::MdsCtx ctx##id(mds::MdsWriter(transaction::ObTransID(id))); \
          compaction::ObMediumCompactionInfoKey key##id(id); \
          compaction::ObMediumCompactionInfo info##id; \
          ret = MediumInfoHelper::build_medium_compaction_info(allocator_, info##id, id); \
          ASSERT_EQ(OB_SUCCESS, ret); \
          \
          ret = mds_table_.set(key##id, info##id, ctx##id); \
          ASSERT_EQ(OB_SUCCESS, ret); \
          \
          ctx##id.on_redo(mock_scn(id)); \
          ctx##id.before_prepare(); \
          ctx##id.on_prepare(mock_scn(id)); \
          ctx##id.on_commit(mock_scn(id), mock_scn(id)); \
        }

#define MOCK_INSERT_TABLET(id) \
        { \
          compaction::ObMediumCompactionInfoKey key##id(id); \
          compaction::ObMediumCompactionInfo info##id; \
          MediumInfoHelper::build_medium_compaction_info(allocator_, info##id, id); \
          mds::MdsDumpKey dump_key##id; \
          ret = convert(key##id, dump_key##id); \
          ASSERT_EQ(OB_SUCCESS, ret); \
          mds::MdsDumpNode dump_node##id; \
          ret = convert(info##id, dump_node##id); \
          ASSERT_EQ(OB_SUCCESS, ret); \
          \
          ret = medium_info_list.append(dump_key##id, dump_node##id); \
          ASSERT_EQ(OB_SUCCESS, ret); \
        }


namespace oceanbase
{
namespace storage
{
class TestMediumInfoReader : public ::testing::Test
{
public:
  TestMediumInfoReader();
  virtual ~TestMediumInfoReader() = default;
public:
  virtual void SetUp() override;
  virtual void TearDown() override;
  static void SetUpTestCase();
  static void TearDownTestCase();
public:
  static int create_ls(const uint64_t tenant_id, const share::ObLSID &ls_id, ObLSHandle &ls_handle);
  static int remove_ls(const share::ObLSID &ls_id);
  int create_tablet(const common::ObTabletID &tablet_id, ObTabletHandle &tablet_handle);
public:
  int convert(const compaction::ObMediumCompactionInfoKey &key, mds::MdsDumpKey &dump_key);
  int convert(const compaction::ObMediumCompactionInfo &info, mds::MdsDumpNode &dump_node);
public:
  static constexpr uint64_t TENANT_ID = 1001;
  static const share::ObLSID LS_ID;

  mds::MdsTableHandle mds_table_;
  common::ObArenaAllocator allocator_; // for medium info
};

const share::ObLSID TestMediumInfoReader::LS_ID(1001);

TestMediumInfoReader::TestMediumInfoReader()
  : mds_table_(),
    allocator_()
{
}

void TestMediumInfoReader::SetUp()
{
}

void TestMediumInfoReader::TearDown()
{
}

void TestMediumInfoReader::SetUpTestCase()
{
  int ret = OB_SUCCESS;
  ret = MockTenantModuleEnv::get_instance().init();
  ASSERT_EQ(OB_SUCCESS, ret);

  ObServerCheckpointSlogHandler::get_instance().is_started_ = true;

  // create ls
  ObLSHandle ls_handle;
  ret = create_ls(TENANT_ID, LS_ID, ls_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestMediumInfoReader::TearDownTestCase()
{
  int ret = OB_SUCCESS;

  // remove ls
  ret = remove_ls(LS_ID);
  ASSERT_EQ(OB_SUCCESS, ret);

  MockTenantModuleEnv::get_instance().destroy();
}

int TestMediumInfoReader::create_ls(const uint64_t tenant_id, const share::ObLSID &ls_id, ObLSHandle &ls_handle)
{
  int ret = OB_SUCCESS;
  ret = TestDmlCommon::create_ls(tenant_id, ls_id, ls_handle);
  return ret;
}

int TestMediumInfoReader::remove_ls(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ret = MTL(ObLSService*)->remove_ls(ls_id, false);
  return ret;
}

int TestMediumInfoReader::create_tablet(const common::ObTabletID &tablet_id, ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  const uint64_t table_id = 1234567;
  share::schema::ObTableSchema table_schema;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  mds_table_.reset();

  if (OB_FAIL(MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get ls", K(ret));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ret), KP(ls));
  } else if (OB_FAIL(build_test_schema(table_schema, table_id))) {
    LOG_WARN("failed to build table schema");
  } else if (OB_FAIL(TestTabletHelper::create_tablet(ls_handle, tablet_id, table_schema, allocator_))) {
    LOG_WARN("failed to create tablet", K(ret));
  } else if (OB_FAIL(ls->get_tablet(tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret));
  } else if (OB_FAIL(tablet_handle.get_obj()->inner_get_mds_table(mds_table_, true/*not_exist_create*/))) {
    LOG_WARN("failed to get mds table", K(ret));
  }

  return ret;
}

int TestMediumInfoReader::convert(const compaction::ObMediumCompactionInfoKey &key, mds::MdsDumpKey &dump_key)
{
  int ret = OB_SUCCESS;
  constexpr uint8_t table_id = mds::TupleTypeIdx<mds::MdsTableTypeTuple, mds::NormalMdsTable>::value;
  constexpr uint8_t unit_id = mds::TupleTypeIdx<mds::NormalMdsTable, mds::MdsUnit<compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo>>::value;

  dump_key.mds_table_id_ = table_id;
  dump_key.mds_unit_id_ = unit_id;
  dump_key.crc_check_number_ = 0;
  dump_key.allocator_ = &allocator_;

  const int64_t length = key.get_serialize_size();
  if (0 == length) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("key length should not be 0", K(ret), K(length));
  } else {
    char *buffer = static_cast<char*>(allocator_.alloc(length));
    int64_t pos = 0;
    if (OB_ISNULL(buffer)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret), K(length));
    } else if (OB_FAIL(key.serialize(buffer, length, pos))) {
      LOG_WARN("failed to serialize", K(ret));
    } else {
      dump_key.key_.assign(buffer, length);
    }

    if (OB_FAIL(ret)) {
      if (nullptr != buffer) {
        allocator_.free(buffer);
      }
    }
  }

  return ret;
}

int TestMediumInfoReader::convert(const compaction::ObMediumCompactionInfo &info, mds::MdsDumpNode &dump_node)
{
  int ret = OB_SUCCESS;
  constexpr uint8_t table_id = mds::TupleTypeIdx<mds::MdsTableTypeTuple, mds::NormalMdsTable>::value;
  constexpr uint8_t unit_id = mds::TupleTypeIdx<mds::NormalMdsTable, mds::MdsUnit<compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo>>::value;

  dump_node.mds_table_id_ = table_id;
  dump_node.mds_unit_id_ = unit_id;
  dump_node.crc_check_number_ = 0;
  dump_node.status_.union_.field_.node_type_ = mds::MdsNodeType::SET;
  dump_node.status_.union_.field_.writer_type_ = mds::WriterType::TRANSACTION;
  dump_node.status_.union_.field_.state_ = mds::TwoPhaseCommitState::ON_COMMIT;
  dump_node.allocator_ = &allocator_;
  dump_node.writer_id_ = 1;
  dump_node.seq_no_ = 1;
  dump_node.redo_scn_ = share::SCN::max_scn();
  dump_node.end_scn_ = share::SCN::max_scn();
  dump_node.trans_version_ = share::SCN::max_scn();

  const int64_t length = info.get_serialize_size();
  if (0 == length) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("info length should not be 0", K(ret), K(length));
  } else {
    char *buffer = static_cast<char*>(allocator_.alloc(length));
    int64_t pos = 0;
    if (OB_ISNULL(buffer)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret), K(length));
    } else if (OB_FAIL(info.serialize(buffer, length, pos))) {
      LOG_WARN("failed to serialize", K(ret));
    } else {
      dump_node.user_data_.assign(buffer, length);
    }

    if (OB_FAIL(ret)) {
      if (nullptr != buffer) {
        allocator_.free(buffer);
      }
    }
  }

  return ret;
}

TEST_F(TestMediumInfoReader, pure_mds_table)
{
  int ret = OB_SUCCESS;

  // create tablet
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTabletHandle tablet_handle;
  ret = create_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTablet *tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);

  // insert data into mds table
  MOCK_INSERT_MDS_TABLE(1);
  MOCK_INSERT_MDS_TABLE(2);

  // iterate
  {
    common::ObArenaAllocator allocator;
    ObTabletMediumInfoReader reader(*tablet);
    ret = reader.init(allocator);
    ASSERT_EQ(OB_SUCCESS, ret);

    compaction::ObMediumCompactionInfoKey key;
    compaction::ObMediumCompactionInfo info;

    key.reset();
    info.reset();
    ret = reader.get_next_medium_info(allocator, key, info);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(1, key.medium_snapshot_);
    ASSERT_EQ(1, info.medium_snapshot_);

    key.reset();
    info.reset();
    ret = reader.get_next_medium_info(allocator, key, info);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(2, key.medium_snapshot_);
    ASSERT_EQ(2, info.medium_snapshot_);

    key.reset();
    info.reset();
    ret = reader.get_next_medium_info(allocator, key, info);
    ASSERT_EQ(OB_ITER_END, ret);
    reader.reset();
  }
  {
    common::ObArenaAllocator allocator;
    ObTabletMediumInfoReader reader(*tablet);
    ASSERT_EQ(OB_SUCCESS, reader.init(allocator));
    int64_t medium_snapshot = 0;
    ret = reader.get_max_medium_snapshot(medium_snapshot);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(medium_snapshot, 2);
  }
  {
    common::ObArenaAllocator allocator;
    ObTabletMediumInfoReader reader(*tablet);
    ASSERT_EQ(OB_SUCCESS, reader.init(allocator));
    int64_t medium_snapshot = 0;
    ret = reader.get_min_medium_snapshot(medium_snapshot);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(medium_snapshot, 1);
  }
}

TEST_F(TestMediumInfoReader, pure_dump_data)
{
  int ret = OB_SUCCESS;

  // create tablet
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTabletHandle tablet_handle;
  ObTabletHandle new_tablet_handle;
  ret = create_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTablet *tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);

  ObTabletPointer *tablet_ptr = static_cast<ObTabletPointer*>(tablet->get_pointer_handle().get_resource_ptr());
  ret = tablet_ptr->try_gc_mds_table();
  ASSERT_EQ(OB_SUCCESS, ret);

  // insert data into tablet
  {
    common::ObIAllocator &allocator = *tablet_handle.get_allocator();
    ObTabletComplexAddr<ObTabletDumpedMediumInfo> &medium_info_list_complex_addr = tablet->mds_data_.medium_info_list_;
    ret = ObTabletObjLoadHelper::alloc_and_new(allocator, medium_info_list_complex_addr.ptr_);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = medium_info_list_complex_addr.ptr_->init_for_first_creation(allocator);
    ASSERT_EQ(OB_SUCCESS, ret);
    ObTabletDumpedMediumInfo &medium_info_list = *medium_info_list_complex_addr.ptr_;

    constexpr uint8_t table_id = mds::TupleTypeIdx<mds::MdsTableTypeTuple, mds::NormalMdsTable>::value;
    constexpr uint8_t unit_id = mds::TupleTypeIdx<mds::NormalMdsTable, mds::MdsUnit<compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo>>::value;

    MOCK_INSERT_TABLET(1);
    MOCK_INSERT_TABLET(2);

    // persist
    ret = ObTabletPersister::persist_and_transform_tablet(*tablet, new_tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    tablet = new_tablet_handle.get_obj();
    ASSERT_NE(nullptr, tablet);
  }

  // iterate
  {
    common::ObArenaAllocator allocator;
    ObTabletMediumInfoReader reader(*tablet);
    ret = reader.init(allocator);
    ASSERT_EQ(OB_SUCCESS, ret);

    compaction::ObMediumCompactionInfoKey key;
    compaction::ObMediumCompactionInfo info;

    key.reset();
    info.reset();
    ret = reader.get_next_medium_info(allocator, key, info);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(1, key.medium_snapshot_);
    ASSERT_EQ(1, info.medium_snapshot_);

    key.reset();
    info.reset();
    ret = reader.get_next_medium_info(allocator, key, info);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(2, key.medium_snapshot_);
    ASSERT_EQ(2, info.medium_snapshot_);

    key.reset();
    info.reset();
    ret = reader.get_next_medium_info(allocator, key, info);
    ASSERT_EQ(OB_ITER_END, ret);
  }
}

TEST_F(TestMediumInfoReader, mds_table_dump_data_overlap)
{
  int ret = OB_SUCCESS;

  // create tablet
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTabletHandle tablet_handle;
  ObTabletHandle new_tablet_handle;
  ret = create_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTablet *tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);

  // insert data into mds table
  MOCK_INSERT_MDS_TABLE(2);
  MOCK_INSERT_MDS_TABLE(3);

  // insert data into mds data
  {
    common::ObIAllocator &allocator = *tablet_handle.get_allocator();
    ObTabletComplexAddr<ObTabletDumpedMediumInfo> &medium_info_list_complex_addr = tablet->mds_data_.medium_info_list_;
    ret = ObTabletObjLoadHelper::alloc_and_new(allocator, medium_info_list_complex_addr.ptr_);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = medium_info_list_complex_addr.ptr_->init_for_first_creation(allocator);
    ASSERT_EQ(OB_SUCCESS, ret);
    ObTabletDumpedMediumInfo &medium_info_list = *medium_info_list_complex_addr.ptr_;

    constexpr uint8_t table_id = mds::TupleTypeIdx<mds::MdsTableTypeTuple, mds::NormalMdsTable>::value;
    constexpr uint8_t unit_id = mds::TupleTypeIdx<mds::NormalMdsTable, mds::MdsUnit<compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo>>::value;

    MOCK_INSERT_TABLET(1);
    MOCK_INSERT_TABLET(2);

    // persist
    ret = ObTabletPersister::persist_and_transform_tablet(*tablet, new_tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    tablet = new_tablet_handle.get_obj();
    ASSERT_NE(nullptr, tablet);
  }

  // iterate
  {
    common::ObArenaAllocator allocator;
    ObTabletMediumInfoReader reader(*tablet);
    ret = reader.init(allocator);
    ASSERT_EQ(OB_SUCCESS, ret);

    compaction::ObMediumCompactionInfoKey key;
    compaction::ObMediumCompactionInfo info;

    key.reset();
    info.reset();
    ret = reader.get_next_medium_info(allocator, key, info);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(1, key.medium_snapshot_);
    ASSERT_EQ(1, info.medium_snapshot_);

    key.reset();
    info.reset();
    ret = reader.get_next_medium_info(allocator, key, info);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(2, key.medium_snapshot_);
    ASSERT_EQ(2, info.medium_snapshot_);

    key.reset();
    info.reset();
    ret = reader.get_next_medium_info(allocator, key, info);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(3, key.medium_snapshot_);
    ASSERT_EQ(3, info.medium_snapshot_);

    key.reset();
    info.reset();
    ret = reader.get_next_medium_info(allocator, key, info);
    ASSERT_EQ(OB_ITER_END, ret);
  }
  {
    common::ObArenaAllocator allocator;
    ObTabletMediumInfoReader reader(*tablet);
    ASSERT_EQ(OB_SUCCESS, reader.init(allocator));
    int64_t medium_snapshot = 0;
    ret = reader.get_max_medium_snapshot(medium_snapshot);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(medium_snapshot, 3);
  }
  {
    common::ObArenaAllocator allocator;
    ObTabletMediumInfoReader reader(*tablet);
    ASSERT_EQ(OB_SUCCESS, reader.init(allocator));
    int64_t medium_snapshot = 0;
    ret = reader.get_min_medium_snapshot(medium_snapshot);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(medium_snapshot, 1);
  }
  {
    common::ObArenaAllocator allocator;
    ObTabletMediumInfoReader reader(*tablet);
    ASSERT_EQ(OB_SUCCESS, reader.init(allocator));
    ObArenaAllocator tmp_allocator;
    compaction::ObMediumCompactionInfoKey key(1);
    compaction::ObMediumCompactionInfo medium_info;
    ret = reader.get_specified_medium_info(tmp_allocator, key, medium_info);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(medium_info.medium_snapshot_, 1);
  }
  {
    common::ObArenaAllocator allocator;
    ObTabletMediumInfoReader reader(*tablet);
    ASSERT_EQ(OB_SUCCESS, reader.init(allocator));
    ObArenaAllocator tmp_allocator;
    compaction::ObMediumCompactionInfoKey key(10);
    compaction::ObMediumCompactionInfo medium_info;
    ret = reader.get_specified_medium_info(tmp_allocator, key, medium_info);
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
  }
}

TEST_F(TestMediumInfoReader, mds_table_dump_data_no_overlap)
{
  int ret = OB_SUCCESS;

  // create tablet
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTabletHandle tablet_handle;
  ObTabletHandle new_tablet_handle;
  ret = create_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTablet *tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);

  // insert data into mds table
  MOCK_INSERT_MDS_TABLE(5);
  MOCK_INSERT_MDS_TABLE(8);

  // insert data into mds data
  {
    common::ObIAllocator &allocator = *tablet_handle.get_allocator();
    ObTabletComplexAddr<ObTabletDumpedMediumInfo> &medium_info_list_complex_addr = tablet->mds_data_.medium_info_list_;
    ret = ObTabletObjLoadHelper::alloc_and_new(allocator, medium_info_list_complex_addr.ptr_);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = medium_info_list_complex_addr.ptr_->init_for_first_creation(allocator);
    ASSERT_EQ(OB_SUCCESS, ret);
    ObTabletDumpedMediumInfo &medium_info_list = *medium_info_list_complex_addr.ptr_;

    constexpr uint8_t table_id = mds::TupleTypeIdx<mds::MdsTableTypeTuple, mds::NormalMdsTable>::value;
    constexpr uint8_t unit_id = mds::TupleTypeIdx<mds::NormalMdsTable, mds::MdsUnit<compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo>>::value;

    MOCK_INSERT_TABLET(1);
    MOCK_INSERT_TABLET(2);

    // persist
    ret = ObTabletPersister::persist_and_transform_tablet(*tablet, new_tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    tablet = new_tablet_handle.get_obj();
    ASSERT_NE(nullptr, tablet);
  }

  // iterate
  {
    common::ObArenaAllocator allocator;
    ObTabletMediumInfoReader reader(*tablet);
    ret = reader.init(allocator);
    ASSERT_EQ(OB_SUCCESS, ret);

    compaction::ObMediumCompactionInfoKey key;
    compaction::ObMediumCompactionInfo info;

    key.reset();
    info.reset();
    ret = reader.get_next_medium_info(allocator, key, info);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(1, key.medium_snapshot_);
    ASSERT_EQ(1, info.medium_snapshot_);

    key.reset();
    info.reset();
    ret = reader.get_next_medium_info(allocator, key, info);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(2, key.medium_snapshot_);
    ASSERT_EQ(2, info.medium_snapshot_);

    key.reset();
    info.reset();
    ret = reader.get_next_medium_info(allocator, key, info);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(5, key.medium_snapshot_);
    ASSERT_EQ(5, info.medium_snapshot_);

    key.reset();
    info.reset();
    ret = reader.get_next_medium_info(allocator, key, info);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(8, key.medium_snapshot_);
    ASSERT_EQ(8, info.medium_snapshot_);

    key.reset();
    info.reset();
    ret = reader.get_next_medium_info(allocator, key, info);
    ASSERT_EQ(OB_ITER_END, ret);
    reader.reset();
  }
  {
    common::ObArenaAllocator allocator;
    ObTabletMediumInfoReader reader(*tablet);
    ASSERT_EQ(OB_SUCCESS, reader.init(allocator));
    int64_t medium_snapshot = 0;
    ret = reader.get_max_medium_snapshot(medium_snapshot);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(medium_snapshot, 8);
  }
  {
    common::ObArenaAllocator allocator;
    ObTabletMediumInfoReader reader(*tablet);
    ASSERT_EQ(OB_SUCCESS, reader.init(allocator));
    int64_t medium_snapshot = 0;
    ret = reader.get_min_medium_snapshot(medium_snapshot);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(medium_snapshot, 1);
  }
}

TEST_F(TestMediumInfoReader, mds_table_dump_data_full_inclusion)
{
  int ret = OB_SUCCESS;

  // create tablet
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTabletHandle tablet_handle;
  ObTabletHandle new_tablet_handle;
  ret = create_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTablet *tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);

  // insert data into mds table
  MOCK_INSERT_MDS_TABLE(1);
  MOCK_INSERT_MDS_TABLE(2);
  MOCK_INSERT_MDS_TABLE(3);
  MOCK_INSERT_MDS_TABLE(4);
  MOCK_INSERT_MDS_TABLE(5);

  // insert data into mds data
  {
    common::ObIAllocator &allocator = *tablet_handle.get_allocator();
    ObTabletComplexAddr<ObTabletDumpedMediumInfo> &medium_info_list_complex_addr = tablet->mds_data_.medium_info_list_;
    ret = ObTabletObjLoadHelper::alloc_and_new(allocator, medium_info_list_complex_addr.ptr_);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = medium_info_list_complex_addr.ptr_->init_for_first_creation(allocator);
    ASSERT_EQ(OB_SUCCESS, ret);
    ObTabletDumpedMediumInfo &medium_info_list = *medium_info_list_complex_addr.ptr_;

    constexpr uint8_t table_id = mds::TupleTypeIdx<mds::MdsTableTypeTuple, mds::NormalMdsTable>::value;
    constexpr uint8_t unit_id = mds::TupleTypeIdx<mds::NormalMdsTable, mds::MdsUnit<compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo>>::value;

    MOCK_INSERT_TABLET(2);
    MOCK_INSERT_TABLET(4);

    // persist
    ret = ObTabletPersister::persist_and_transform_tablet(*tablet, new_tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    tablet = new_tablet_handle.get_obj();
    ASSERT_NE(nullptr, tablet);
  }

  // iterate
  {
    common::ObArenaAllocator allocator;
    ObTabletMediumInfoReader reader(*tablet);
    ret = reader.init(allocator);
    ASSERT_EQ(OB_SUCCESS, ret);

    compaction::ObMediumCompactionInfoKey key;
    compaction::ObMediumCompactionInfo info;

    key.reset();
    info.reset();
    ret = reader.get_next_medium_info(allocator, key, info);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(1, key.medium_snapshot_);
    ASSERT_EQ(1, info.medium_snapshot_);

    key.reset();
    info.reset();
    ret = reader.get_next_medium_info(allocator, key, info);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(2, key.medium_snapshot_);
    ASSERT_EQ(2, info.medium_snapshot_);

    key.reset();
    info.reset();
    ret = reader.get_next_medium_info(allocator, key, info);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(3, key.medium_snapshot_);
    ASSERT_EQ(3, info.medium_snapshot_);

    key.reset();
    info.reset();
    ret = reader.get_next_medium_info(allocator, key, info);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(4, key.medium_snapshot_);
    ASSERT_EQ(4, info.medium_snapshot_);

    key.reset();
    info.reset();
    ret = reader.get_next_medium_info(allocator, key, info);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(5, key.medium_snapshot_);
    ASSERT_EQ(5, info.medium_snapshot_);

    key.reset();
    info.reset();
    ret = reader.get_next_medium_info(allocator, key, info);
    ASSERT_EQ(OB_ITER_END, ret);
  }
}
} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_medium_info_reader.log*");
  OB_LOGGER.set_file_name("test_medium_info_reader.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
