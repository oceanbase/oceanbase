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
#ifndef TEST_GC_UTIL_H_
#define TEST_GC_UTIL_H_
#endif

#define protected public
#define private public

#include <gtest/gtest.h>
#include "lib/utility/ob_test_util.h"
#include "storage/tablet/ob_tablet.h"
#include "close_modules/shared_storage/storage/shared_storage/ob_ss_object_access_util.h"
#include "close_modules/shared_storage/storage/incremental/atomic_protocol/ob_atomic_tablet_meta_define.h"
#include "close_modules/shared_storage/storage/incremental/atomic_protocol/ob_atomic_file.h"
#include "close_modules/shared_storage/storage/incremental/atomic_protocol/ob_atomic_define.h"
#include "close_modules/shared_storage/storage/incremental/sslog/ob_sslog_define.h"
#include "close_modules/shared_storage/storage/incremental/atomic_protocol/ob_atomic_sstablelist_define.h"


namespace oceanbase
{

class TestRunCtx
{
public:
  uint64_t tenant_id_ = 1;
  int64_t tenant_epoch_ = 0;
  ObLSID ls_id_;
  int64_t ls_epoch_;
  ObTabletID tablet_id_;
  int64_t time_sec_ = 0;
  ObLS *ls_;
};

TestRunCtx RunCtx;
static void insert_sslog(
    const sslog::ObSSLogMetaType sslog_type,
    const int64_t op_id,
    const ObAtomicMetaInfo::State state,
    const ObSSTableGCInfo &gc_info)
{
  int64_t affected_rows = 0;
  ObSSMetaReadParam param;
  share::SCN transfer_scn;
  transfer_scn.set_min();
  param.set_tablet_level_param(ObSSMetaReadParamType::TABLET_KEY, ObSSMetaReadResultType::READ_WHOLE_ROW, false, sslog_type, RunCtx.ls_id_, RunCtx.tablet_id_, transfer_scn);

  ObAtomicExtraInfo extra_info;
  extra_info.meta_info_.op_id_ = op_id;
  extra_info.meta_info_.epoch_id_ = 1;
  extra_info.meta_info_.state_ = state;
  extra_info.meta_info_.not_exist_ = false;
  int64_t gc_info_size = gc_info.get_serialize_size();
  char *gc_info_buf = (char *)mtl_malloc(gc_info_size, "gc_unittest");
  int64_t pos = 0;

  ASSERT_EQ(OB_SUCCESS, gc_info.serialize(gc_info_buf, gc_info_size, pos));
  ASSERT_EQ(OB_SUCCESS, extra_info.gc_info_.assign(ObString(pos, gc_info_buf)));

  ObAtomicMetaKey meta_key;
  const common::ObString meta_value = "bizhu_test";


  ASSERT_EQ(OB_SUCCESS, ObAtomicFile::get_meta_key(param, meta_key));
  ASSERT_EQ(OB_SUCCESS, ObAtomicFile::insert_sslog_row(param.meta_type_,
                                                       meta_key.get_string_key(),
                                                       meta_value,
                                                       extra_info,
                                                       affected_rows));
}

static void gen_block_id(
    const int64_t op_id,
    const ObStorageObjectType type,
    const int64_t seq,
    MacroBlockId &block_id,
    const int64_t cg_id = 0)
{
  blocksstable::ObStorageObjectOpt opt;
  if (ObStorageObjectType::SHARED_TABLET_SUB_META == type) {
    opt.set_ss_tablet_sub_meta_opt(RunCtx.ls_id_.id(), RunCtx.tablet_id_.id(), op_id,
        seq /* start_seq */, RunCtx.tablet_id_.is_ls_inner_tablet(), 0);

  } else if (ObStorageObjectType::SHARED_MAJOR_DATA_MACRO == type) {
    opt.set_ss_share_object_opt(type, RunCtx.tablet_id_.is_ls_inner_tablet(), RunCtx.ls_id_.id(),
        RunCtx.tablet_id_.id(), seq, cg_id /* column_group_id */, 0);
  } else {
    opt.set_ss_share_object_opt(type, RunCtx.tablet_id_.is_ls_inner_tablet(), RunCtx.ls_id_.id(),
        RunCtx.tablet_id_.id(), (op_id << 32) | (seq & 0xFFFFFFFF), cg_id /* column_group_id */, 0);

  }
  ASSERT_EQ(OB_SUCCESS, ObObjectManager::ss_get_object_id(opt, block_id));
  ASSERT_TRUE(block_id.is_valid());
}

static void write_block(
    const MacroBlockId &block_id)
{

  ObStorageObjectWriteInfo write_info;

  const int64_t WRITE_IO_SIZE = DIO_READ_ALIGN_SIZE * 256; // 1MB
  char write_buf[WRITE_IO_SIZE];
  write_buf[0] = '\0';
  const int64_t mid_offset = WRITE_IO_SIZE / 2;
  memset(write_buf, 'a', mid_offset);
  memset(write_buf + mid_offset, 'b', WRITE_IO_SIZE - mid_offset);
  write_info.io_desc_.set_wait_event(1);
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = WRITE_IO_SIZE;
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = MTL_ID();

  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(block_id));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_write_file(write_info, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  write_object_handle.reset();
}

template<typename T>
static void update_sslog(
    const sslog::ObSSLogMetaType sslog_type,
    const int64_t op_id,
    const ObAtomicMetaInfo::State state,
    const T *gc_info = NULL,
    const ObSSMetaUpdateMetaInfo *meta_info = NULL)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  int64_t pos = 0;
  ObSSMetaReadParam param;
  share::SCN transfer_scn;
  transfer_scn.set_min();
  param.set_tablet_level_param(ObSSMetaReadParamType::TABLET_KEY, ObSSMetaReadResultType::READ_WHOLE_ROW, false, sslog_type, RunCtx.ls_id_, RunCtx.tablet_id_, transfer_scn);

  ObAtomicMetaKey meta_key;
  ASSERT_EQ(OB_SUCCESS, ObAtomicFile::get_meta_key(param, meta_key));

  // read
  share::SCN row_scn_ret;
  ObString value;
  ObSSLogIteratorGuard iter(true, true);
  param.set_tablet_level_param(ObSSMetaReadParamType::TABLET_KEY, ObSSMetaReadResultType::READ_WHOLE_ROW, false, sslog_type, RunCtx.ls_id_, RunCtx.tablet_id_, transfer_scn);
  ASSERT_EQ(OB_SUCCESS, ObAtomicFile::read_meta_row(param,
                                                    share::SCN::invalid_scn(),
                                                    iter));
  sslog::ObSSLogMetaType type;
  ObString key;
  ObString info;

  ret = iter.get_next_row(row_scn_ret, type, key, value, info);
  if (OB_FAIL(ret)) {
    SERVER_LOG(WARN, "failed to get_next_row", K(key), K(value), K(sslog_type), K(op_id), K(state));
  }
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(type, param.meta_type_);
  ASSERT_EQ(key, meta_key.get_string_key());
  ASSERT_EQ(true, row_scn_ret > SCN::min_scn());


  ObAtomicExtraInfo check_extra_info;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, check_extra_info.deserialize(info.ptr(), info.length(), pos));
  SERVER_LOG(INFO, "read for update sslog row", K(key), K(value), K(check_extra_info), K(sslog_type), K(op_id), K(state));

  // update
  ObAtomicExtraInfo extra_info;
  extra_info.assign(check_extra_info);
  extra_info.meta_info_.op_id_ = op_id;
  extra_info.meta_info_.state_ = state;

  if (NULL != gc_info)
  {
    const int64_t gc_info_size = gc_info->get_serialize_size();
    char *gc_info_buf = (char *)mtl_malloc(gc_info_size, "gc_unittest");
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, gc_info->serialize(gc_info_buf, gc_info_size, pos));
    ASSERT_EQ(OB_SUCCESS, extra_info.gc_info_.assign(ObString(pos, gc_info_buf)));
  }

  if (NULL != meta_info) {
    extra_info.meta_info_.not_exist_ = false;
    pos = 0;
    ObSSTabletSSLogValue ret_meta_value;
    ObMetaDiskAddr addr;
    addr.type_ = 2;
    addr.second_id_ = 0;
    addr.size_ = ObLogConstants::MAX_LOG_FILE_SIZE;
    ASSERT_TRUE(addr.is_valid());

    common::ObArenaAllocator allocator;
    ASSERT_EQ(OB_SUCCESS, ret_meta_value.deserialize(allocator, addr, value.ptr(), value.length(), pos));
    SERVER_LOG(INFO, "read for update sslog row (update_meta_info)", K(key), K(value), K(check_extra_info), K(ret_meta_value.get_meta_info()), K(sslog_type), K(op_id), K(state));

    ObSSTabletSSLogValue meta_value;
    meta_value.set_meta_info(*meta_info);
    meta_value.tablet_ = ret_meta_value.tablet_;
    meta_value.data_version_ = DATA_CURRENT_VERSION;
    meta_value.macro_info_ = ret_meta_value.tablet_->macro_info_addr_.get_ptr();
    const int64_t meta_value_size = meta_value.get_serialize_size();
    char *meta_value_buf = (char*)mtl_malloc(meta_value_size, "gc_unittest");
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, meta_value.serialize(meta_value_buf, meta_value_size, pos));
    value.assign_ptr(meta_value_buf, meta_value_size);
  }

  // write
  ASSERT_EQ(OB_SUCCESS, ObAtomicFile::update_sslog_row(param.meta_type_,
                                                       meta_key.get_string_key(),
                                                       value,
                                                       info,
                                                       extra_info,
                                                       false,
                                                       affected_rows));
}

void wait_minor_finish(common::ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  SERVER_LOG(INFO, "wait minor begin");

  ObSqlString sql;
  int64_t affected_rows = 0;
  int64_t row_cnt = 0;
  do {
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select count(*) as row_cnt from oceanbase.__all_virtual_table_mgr where tenant_id=%lu and tablet_id=%lu and table_type=0;",
          RunCtx.tenant_id_, RunCtx.tablet_id_.id()));
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
      sqlclient::ObMySQLResult *result = res.get_result();
      ASSERT_NE(nullptr, result);
      ASSERT_EQ(OB_SUCCESS, result->next());
      ASSERT_EQ(OB_SUCCESS, result->get_int("row_cnt", row_cnt));
    }
    usleep(100 * 1000);
    SERVER_LOG(INFO, "minor result", K(row_cnt));
  } while (row_cnt > 0);
  SERVER_LOG(INFO, "minor finished", K(row_cnt));
}

void wait_major_finish(common::ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  SERVER_LOG(INFO, "wait minor begin");

  ObSqlString sql;
  int64_t affected_rows = 0;
  static int64_t old_major_scn = 1;
  int64_t new_major_scn = 1;
  int64_t scn = 0;
  do {
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select frozen_scn, (frozen_scn - last_scn) as result from oceanbase.CDB_OB_MAJOR_COMPACTION where tenant_id=%lu;",
          RunCtx.tenant_id_));
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
      sqlclient::ObMySQLResult *result = res.get_result();
      ASSERT_NE(nullptr, result);
      ASSERT_EQ(OB_SUCCESS, result->next());
      ASSERT_EQ(OB_SUCCESS, result->get_int("result", scn));
      ASSERT_EQ(OB_SUCCESS, result->get_int("frozen_scn", new_major_scn));
    }
    SERVER_LOG(INFO, "shared result", K(scn), K(new_major_scn));
    usleep(100 * 1000); // 100_ms
  } while (0 != scn || old_major_scn == new_major_scn);
  old_major_scn = new_major_scn;
  SERVER_LOG(INFO, "major finished", K(new_major_scn));
}

void get_ss_checkpoint_scn(common::ObMySQLProxy &sql_proxy, int64_t &ss_checkpoint_scn)
{
  int ret = OB_SUCCESS;
  ss_checkpoint_scn = 0;
  ObSqlString sql;
  // common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select ss_checkpoint_scn from oceanbase.__all_virtual_ss_ls_meta where tenant_id=%lu and ls_id=%ld;",
        RunCtx.tenant_id_, RunCtx.ls_id_.id()));
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
    sqlclient::ObMySQLResult *result = res.get_result();
    ASSERT_NE(nullptr, result);
    ASSERT_EQ(OB_SUCCESS, result->next());
    ASSERT_EQ(OB_SUCCESS, result->get_int("ss_checkpoint_scn", ss_checkpoint_scn));
  }
}

void wait_upload_sstable(common::ObMySQLProxy &sql_proxy, const int64_t ss_checkpoint_scn)
{
  int64_t new_ss_checkpoint_scn = 0;
  while (new_ss_checkpoint_scn <= ss_checkpoint_scn) {
    get_ss_checkpoint_scn(sql_proxy, new_ss_checkpoint_scn);
    SERVER_LOG(INFO, "wait upload sstable", K(RunCtx.tenant_epoch_), K(RunCtx.ls_id_), K(RunCtx.ls_epoch_), K(RunCtx.tablet_id_), K(new_ss_checkpoint_scn), K(ss_checkpoint_scn));
    usleep(100 * 1000);
  }
}

}
