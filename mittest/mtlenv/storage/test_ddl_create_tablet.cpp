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

#define USING_LOG_PREFIX STORAGE

#include <gtest/gtest.h>

#define private public
#define protected public
#undef private

#include "mtlenv/mock_tenant_module_env.h"
#include "share/ob_rpc_struct.h"
#include "storage/multi_data_source/buffer_ctx.h"
#include "storage/multi_data_source/mds_ctx.h"
#include "storage/multi_data_source/runtime_utility/mds_factory.h"
#include "storage/schema_utils.h"
#include "storage/test_dml_common.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"

namespace oceanbase
{

using namespace common;
using namespace lib;
using namespace share;
using namespace storage;
using namespace share::schema;

// Description: To test the function that the 43x observer replays 42x's tablet created clog.
// Mock ObBatchCreateTabletArg in 42x, copied from 42x.
class ObMock42xCreateTabletArg final
{
  OB_UNIS_VERSION(1);
public:
  ObMock42xCreateTabletArg()
  { reset(); }
  ~ObMock42xCreateTabletArg()
  { reset(); };
  void reset();
  int serialize_for_create_tablet_schemas(char *buf,
      const int64_t data_len,
      int64_t &pos) const;
  int64_t get_serialize_size_for_create_tablet_schemas() const;
  int deserialize_create_tablet_schemas(const char *buf,
      const int64_t data_len,
      int64_t &pos);
  TO_STRING_KV(K_(id), K_(major_frozen_scn), K_(table_schemas),
      K_(tablets), K_(need_check_tablet_cnt), K_(is_old_mds), K_(create_tablet_schemas));
public:
  share::ObLSID id_;
  share::SCN major_frozen_scn_;
  common::ObSArray<share::schema::ObTableSchema> table_schemas_;
  common::ObSArray<ObCreateTabletInfo> tablets_;
  bool need_check_tablet_cnt_;
  bool is_old_mds_;
  common::ObSArray<storage::ObCreateTabletSchema*> create_tablet_schemas_;
  ObArenaAllocator allocator_;
};

void ObMock42xCreateTabletArg::reset()
{
  id_.reset();
  major_frozen_scn_.reset();
  tablets_.reset();
  table_schemas_.reset();
  need_check_tablet_cnt_ = false;
  is_old_mds_ = false;
  // reset by ObBatchCreateTabletArg who holds the buffer.
  // for (int64_t i = 0; i < create_tablet_schemas_.count(); ++i) {
  //   create_tablet_schemas_[i]->~ObCreateTabletSchema();
  // }
  // create_tablet_schemas_.reset();
  allocator_.reset();
}

int64_t ObMock42xCreateTabletArg::get_serialize_size_for_create_tablet_schemas() const
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  len += serialization::encoded_length_vi64(create_tablet_schemas_.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < create_tablet_schemas_.count(); ++i) {
    if (OB_ISNULL(create_tablet_schemas_.at(i))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("create_tablet_schema is NULL", KR(ret), K(i), KPC(this));
    } else {
      len += create_tablet_schemas_.at(i)->get_serialize_size();
    }
  }
  return len;
}


int ObMock42xCreateTabletArg::serialize_for_create_tablet_schemas(char *buf,
    const int64_t data_len,
    int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::encode_vi64(buf, data_len, pos, create_tablet_schemas_.count()))) {
    STORAGE_LOG(WARN, "failed to encode schema count", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < create_tablet_schemas_.count(); ++i) {
    if (OB_ISNULL(create_tablet_schemas_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null tx service ptr", KR(ret), K(i), KPC(this));
    } else if (OB_FAIL(create_tablet_schemas_.at(i)->serialize(buf, data_len, pos))) {
      STORAGE_LOG(WARN, "failed to serialize schema", K(ret));
    }
  }
  return ret;
}

int ObMock42xCreateTabletArg::deserialize_create_tablet_schemas(const char *buf,
    const int64_t data_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0) || OB_UNLIKELY(pos > data_len)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(buf), K(data_len), K(pos), K(ret));
  } else if (pos == data_len) {
    //do nothing
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &count))) {
    STORAGE_LOG(WARN, "failed to decode schema count", K(ret));
  } else if (count < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "count invalid", KR(ret), K(buf), K(data_len), K(pos), K(count));
  } else if (count == 0) {
    STORAGE_LOG(INFO, "upgrade, count is 0", KR(ret), K(buf), K(data_len), K(pos), K(count));
  } else if (OB_FAIL(create_tablet_schemas_.reserve(count))) {
    STORAGE_LOG(WARN, "failed to reserve schema array", K(ret), K(count), K(buf), K(data_len), K(pos));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      ObCreateTabletSchema *create_tablet_schema = NULL;
      void *create_tablet_schema_ptr = allocator_.alloc(sizeof(ObCreateTabletSchema));
      if (OB_ISNULL(create_tablet_schema_ptr)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate storage schema", KR(ret));
      } else if (FALSE_IT(create_tablet_schema = new (create_tablet_schema_ptr)ObCreateTabletSchema())) {
      } else if (OB_FAIL(create_tablet_schema->deserialize(allocator_, buf, data_len, pos))) {
        create_tablet_schema->~ObCreateTabletSchema();
        STORAGE_LOG(WARN,"failed to deserialize schema", K(ret), K(buf), K(data_len), K(pos));
      } else if (OB_FAIL(create_tablet_schemas_.push_back(create_tablet_schema))) {
        create_tablet_schema->~ObCreateTabletSchema();
        STORAGE_LOG(WARN, "failed to add schema", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      reset();
    }
  }
  return ret;
}

OB_DEF_SERIALIZE(ObMock42xCreateTabletArg)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, id_, major_frozen_scn_, tablets_, table_schemas_, need_check_tablet_cnt_, is_old_mds_);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(serialize_for_create_tablet_schemas(buf, buf_len, pos))) {
    LOG_WARN("failed to serialize_for_create_tablet_schemas", KR(ret), KPC(this));
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObMock42xCreateTabletArg)
{
  int len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, id_, major_frozen_scn_, tablets_, table_schemas_, need_check_tablet_cnt_, is_old_mds_);
  len += get_serialize_size_for_create_tablet_schemas();
  return len;
}

OB_DEF_DESERIALIZE(ObMock42xCreateTabletArg)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, id_, major_frozen_scn_, tablets_, table_schemas_, need_check_tablet_cnt_);
  if (OB_SUCC(ret)) {
    if (pos == data_len) {
      is_old_mds_ = true;
    } else {
      LST_DO_CODE(OB_UNIS_DECODE, is_old_mds_);
      if (OB_FAIL(ret)) {
      } else if (pos == data_len) {
      } else if (OB_FAIL(deserialize_create_tablet_schemas(buf, data_len, pos))) {
        LOG_WARN("failed to deserialize_for_create_tablet_schemas", KR(ret));
      }
    }
  }
  return ret;
}

class TestDDLCreateTablet : public ::testing::Test
{
public:
  TestDDLCreateTablet()
    : arena_allocator_(),
      buf_len_(0),
      buf_(nullptr),
      ls_tablet_service_(nullptr)
    { }
  ~TestDDLCreateTablet() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
protected:
  int build_create_tablet_arg(
      share::schema::ObTableSchema &data_table_schema,
      ObCreateTabletSchema &data_table_tablet_schema,
      obrpc::ObBatchCreateTabletArg &arg);
  int build_create_tablet_mock_arg(
      const obrpc::ObBatchCreateTabletArg &arg,
      ObMock42xCreateTabletArg &mock_arg);
  int build_mds_buf(const ObMock42xCreateTabletArg &mock_arg);
protected:
  static constexpr int64_t TEST_TENANT_ID = 1;
  static constexpr int64_t TEST_LS_ID = 1001;
  static constexpr int64_t TEST_TABLET_ID = 2323233;
  ObArenaAllocator arena_allocator_;
  int64_t buf_len_;
  char *buf_;
  ObLSTabletService *ls_tablet_service_;
};

void TestDDLCreateTablet::SetUpTestCase()
{
  STORAGE_LOG(INFO, "TestDDLCreateTablet::SetUpTestCase");
  ASSERT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  ObServerCheckpointSlogHandler::get_instance().is_started_ = true;

  // create ls
  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, TestDmlCommon::create_ls(TEST_TENANT_ID, ObLSID(TEST_LS_ID), ls_handle));
}

void TestDDLCreateTablet::TearDownTestCase()
{
  STORAGE_LOG(INFO, "TestDDLCreateTablet::TearDownTestCase");
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->remove_ls(ObLSID(TEST_LS_ID)));
  MockTenantModuleEnv::get_instance().destroy();
}

void TestDDLCreateTablet::SetUp()
{
  STORAGE_LOG(INFO, "TestDDLCreateTablet::SetUp");
  // 1. build ObBatchCreateTabletArg.
  share::schema::ObTableSchema data_table_schema;
  ObCreateTabletSchema data_table_tablet_schema;
  obrpc::ObBatchCreateTabletArg arg;
  ASSERT_EQ(OB_SUCCESS, build_create_tablet_arg(data_table_schema, data_table_tablet_schema, arg));

  // 2. Mock 42x arg.
  ObMock42xCreateTabletArg mock_arg;
  ASSERT_EQ(OB_SUCCESS, build_create_tablet_mock_arg(arg, mock_arg));

  // 3. Use mock arg to build serialized buf.
  ASSERT_EQ(OB_SUCCESS, build_mds_buf(mock_arg));

  // 4. create ls.
  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->get_ls(ObLSID(TEST_LS_ID), ls_handle, ObLSGetMod::STORAGE_MOD));
  ls_tablet_service_ = ls_handle.get_ls()->get_tablet_svr();
}

void TestDDLCreateTablet::TearDown()
{
  arena_allocator_.reset();
  buf_len_ = 0;
  buf_ = nullptr;
  ls_tablet_service_ = nullptr;
}

int TestDDLCreateTablet::build_create_tablet_arg(
    share::schema::ObTableSchema &data_table_schema,
    ObCreateTabletSchema &data_table_tablet_schema,
    obrpc::ObBatchCreateTabletArg &arg)
{
  int ret = OB_SUCCESS;
  data_table_schema.reset();
  data_table_tablet_schema.reset();
  arg.reset();
  STORAGE_LOG(INFO, "TestDDLCreateTablet::build_create_tablet_arg");
  const uint64_t tenant_id = TEST_TENANT_ID;
  const ObLSID &ls_id = ObLSID(TEST_LS_ID);
  const common::ObTabletID &data_tablet_id = common::ObTabletID(TEST_TABLET_ID);
  obrpc::ObCreateTabletInfo tablet_info;
  ObArray<common::ObTabletID> tablet_id_array;
  ObArray<int64_t> tablet_schema_index_array;
  TestSchemaUtils::prepare_data_schema(data_table_schema);
  if (OB_FAIL(tablet_id_array.push_back(data_tablet_id))) {
    STORAGE_LOG(WARN, "failed to push tablet id into array", K(ret), K(data_tablet_id));
  } else if (OB_FAIL(tablet_schema_index_array.push_back(0))) {
    STORAGE_LOG(WARN, "failed to push index into array", K(ret));
  } else if (OB_FAIL(tablet_info.init(tablet_id_array, data_tablet_id, tablet_schema_index_array,
      lib::get_compat_mode(), false/*is_create_bind_hidden_tablets*/))) {
    STORAGE_LOG(WARN, "failed to init tablet info", K(ret), K(tablet_id_array),
        K(data_tablet_id), K(tablet_schema_index_array));
  } else if (OB_FAIL(arg.tablets_.push_back(tablet_info))) {
    STORAGE_LOG(WARN, "push back tablet info failed", K(ret), K(tablet_info));
  } else if (OB_FAIL(arg.init_create_tablet(ls_id, share::SCN::min_scn(), false/*need_check_tablet_cnt*/))) {
    STORAGE_LOG(WARN, "failed to init create tablet", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(data_table_tablet_schema.init(arena_allocator_, data_table_schema, lib::get_compat_mode(),
         false/*skip_column_info*/, ObCreateTabletSchema::STORAGE_SCHEMA_VERSION_V2))) {
    STORAGE_LOG(WARN, "failed to init storage schema", K(ret), K(data_table_schema));
  } else if (OB_FAIL(arg.create_tablet_schemas_.push_back(&data_table_tablet_schema))) {
    STORAGE_LOG(WARN, "push back tablet schema failed", K(ret), K(data_table_tablet_schema));
  }
  STORAGE_LOG(INFO, "build_create_tablet_arg done", K(ret), K(data_table_schema), K(arg));
  return ret;
}


int TestDDLCreateTablet::build_create_tablet_mock_arg(
      const obrpc::ObBatchCreateTabletArg &arg,
      ObMock42xCreateTabletArg &mock_arg)
{
  int ret = OB_SUCCESS;
  mock_arg.reset();
  STORAGE_LOG(INFO, "TestDDLCreateTablet::build_create_tablet_mock_arg", K(ret), K(arg));
  mock_arg.id_                    = arg.id_;
  mock_arg.major_frozen_scn_      = arg.major_frozen_scn_;
  mock_arg.need_check_tablet_cnt_ = arg.need_check_tablet_cnt_;
  mock_arg.is_old_mds_            = arg.is_old_mds_;
  if (OB_FAIL(mock_arg.table_schemas_.assign(arg.table_schemas_))) {
    STORAGE_LOG(WARN, "assign table schemas failed", K(ret));
  } else if (OB_FAIL(mock_arg.tablets_.assign(arg.tablets_))) {
    STORAGE_LOG(WARN, "assign tablet infos failed", K(ret));
  } else if (OB_FAIL(mock_arg.create_tablet_schemas_.assign(arg.create_tablet_schemas_))) {
    STORAGE_LOG(WARN, "assign tablet schema failed", K(ret));
  }
  STORAGE_LOG(INFO, "build_create_tablet_mock_arg done", K(ret), K(arg), K(mock_arg),
      "serialized_size_for_arg", arg.get_serialize_size(),
      "serialized_size_for_mock_arg", mock_arg.get_serialize_size());
  return ret;
}

int TestDDLCreateTablet::build_mds_buf(const ObMock42xCreateTabletArg &mock_arg)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  buf_len_ = mock_arg.get_serialize_size();
  STORAGE_LOG(INFO, "TestDDLCreateTablet::build_mds_buf", K(ret), K(buf_len_), K(mock_arg));
  if (OB_ISNULL(buf_ = (char*)arena_allocator_.alloc(buf_len_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc memory", K(ret), K(buf_len_));
  } else if (OB_FAIL(mock_arg.serialize(buf_, buf_len_, pos))) {
    STORAGE_LOG(WARN, "fail to serialize", K(ret), K(mock_arg));
  }
  return ret;
}

TEST_F(TestDDLCreateTablet, LeaderCreateTablet)
{
  int ret = OB_SUCCESS;
  mds::BufferCtx *ctx = nullptr;
  STORAGE_LOG(INFO, "TestDDLCreateTablet::LeaderCreateTablet", K(ret), K(buf_len_));
  if (OB_FAIL(mds::MdsFactory::create_buffer_ctx(transaction::ObTxDataSourceType::CREATE_TABLET_NEW_MDS, transaction::ObTransID(1), ctx))) {
    STORAGE_LOG(WARN, "create_buffer_ctx failed", K(ret));
  } else if (OB_FAIL(ObTabletCreateMdsHelper::on_register(buf_, buf_len_, *ctx))) {
    STORAGE_LOG(WARN, "on register failed", K(ret));
  }
  STORAGE_LOG(INFO, "Leader process done", K(ret), K(buf_len_));
  ASSERT_EQ(OB_SUCCESS, ret);

  // remove tablet, to make follower replay to create tablet.
  ret = ls_tablet_service_->do_remove_tablet(ObLSID(TEST_LS_ID), common::ObTabletID(TEST_TABLET_ID));
  STORAGE_LOG(INFO, "Leader remove tablet done", K(ret), K(buf_len_));
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestDDLCreateTablet, FollowerCreateTablet)
{
  int ret = OB_SUCCESS;
  mds::BufferCtx *ctx = nullptr;
  STORAGE_LOG(INFO, "TestDDLCreateTablet::FollowerCreateTablet", K(ret), K(buf_len_));
  if (OB_FAIL(mds::MdsFactory::create_buffer_ctx(transaction::ObTxDataSourceType::CREATE_TABLET_NEW_MDS, transaction::ObTransID(1), ctx))) {
    STORAGE_LOG(WARN, "create_buffer_ctx failed", K(ret));
  } else if (OB_FAIL(ObTabletCreateMdsHelper::on_replay(buf_, buf_len_, share::SCN::base_scn(), *ctx))) {
    STORAGE_LOG(WARN, "on replay failed", K(ret));
  }
  STORAGE_LOG(INFO, "Follower process done", K(ret), K(buf_len_));
  ASSERT_EQ(OB_SUCCESS, ret);
}

} // namespace oceanbase.

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  system("rm -f test_ddl_create_tablet.log*");
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_ddl_create_tablet.log", true);
  return RUN_ALL_TESTS();
}
