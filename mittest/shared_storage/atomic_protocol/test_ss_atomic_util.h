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
#ifndef USING_LOG_PREFIX
#define USING_LOG_PREFIX STORAGETEST
#endif
#ifndef OCEANBASE_SENSITIVE_TEST_ATOMIC_UTIL_
#define OCEANBASE_SENSITIVE_TEST_ATOMIC_UTIL_

#define private public
#define protected public
#include "lib/utility/ob_macro_utils.h"                 // DISALLOW_COPY_AND_ASSIGN
#include "share/ob_define.h"
#include "storage/incremental/atomic_protocol/ob_atomic_file_mgr.h"
#include "storage/incremental/atomic_protocol/ob_atomic_file.h"
#include "storage/incremental/atomic_protocol/ob_atomic_sstablelist_op.h"
#include "storage/incremental/atomic_protocol/ob_atomic_overwrite_op.h"
#include "storage/incremental/atomic_protocol/ob_atomic_tablet_meta_op.h"
#include "storage/incremental/atomic_protocol/ob_atomic_tablet_meta_file.h"


namespace oceanbase
{
namespace storage
{
const int64_t MAX_BUF_SIZE = 1024;
typedef ObDefaultSSMetaSSLogValue<ObSSLSMeta> ObSSLSMetaSSLogValue;

#define GET_MINI_SSTABLE_LIST_HANDLE(file_handle, lsid, tabletid, suffix) \
  GET_MINI_SSTABLE_LIST_HANDLE_V2(file_handle, lsid, tabletid, SCN::min_scn(), suffix)

#define GET_MINI_SSTABLE_LIST_HANDLE_V2(file_handle, lsid, tabletid, create_scn, suffix) \
  ATOMIC_SET(&MTL(ObAtomicFileMgr*)->gc_task_.stop_, true); \
  ObLSID ls_id##suffix(lsid); \
  ObTabletID tablet_id##suffix(tabletid); \
  ObAtomicFileHandle<ObAtomicSSTableListFile> file_handle##suffix;      \
  ASSERT_EQ(OB_SUCCESS, MTL(ObAtomicFileMgr*)->get_tablet_file_handle(ls_id##suffix, \
                                            tablet_id##suffix,                  \
                                            ObAtomicFileType::MINI_SSTABLE_LIST,                       \
                                            create_scn, \
                                            file_handle##suffix));              \
  ObAtomicSSTableListFile *sstablelist_file##suffix = file_handle##suffix.get_atomic_file(); \
  ASSERT_NE(nullptr, file_handle##suffix.get_atomic_file());       \
  ASSERT_EQ(ls_id##suffix, sstablelist_file##suffix->ls_id_);         \
  ASSERT_EQ(tablet_id##suffix, sstablelist_file##suffix->tablet_id_); \
  ASSERT_EQ(ObAtomicFileType::MINI_SSTABLE_LIST, sstablelist_file##suffix->type_);

#define GET_MINI_SSTABLE_LIST_HANDLE_DEFAULT(file_handle, suffix) \
  GET_MINI_SSTABLE_LIST_HANDLE(file_handle, 1010, 200001, suffix)

#define GET_LS_META_HANLDE(file_handle, lsid, suffix) \
  ATOMIC_SET(&MTL(ObAtomicFileMgr*)->gc_task_.stop_, true); \
  ObLSID ls_id##suffix(lsid); \
  ObAtomicFileHandle<ObAtomicDefaultFile<ObSSLSMetaSSLogValue>> file_handle##suffix; \
  ASSERT_EQ(OB_SUCCESS, MTL(ObAtomicFileMgr*)->get_ls_file_handle(ls_id##suffix,  \
                                           ObAtomicFileType::LS_META, \
                                           file_handle##suffix));           \
  ObAtomicDefaultFile<ObSSLSMetaSSLogValue> *ls_meta_file##suffix = file_handle##suffix.get_atomic_file(); \
  ASSERT_NE(nullptr, file_handle##suffix.get_atomic_file());                \
  ASSERT_EQ(ls_id##suffix, ls_meta_file##suffix->ls_id_);                           \
  ASSERT_EQ(ObTabletID(), ls_meta_file##suffix->tablet_id_);                \
  ASSERT_EQ(ObAtomicFileType::LS_META, ls_meta_file##suffix->type_);

#define GET_LS_META_HANLDE_DEFAULT(file_handle, suffix) \
  GET_LS_META_HANLDE(file_handle, 1010, suffix)

#define GET_TABLET_META_HANLDE(file_handle, lsid, tablet_id, suffix)    \
  ATOMIC_SET(&MTL(ObAtomicFileMgr*)->gc_task_.stop_, true);             \
  ObLSID ls_id##suffix(lsid);                                           \
  ObTabletID tablet_id##suffix(tablet_id);                              \
  ObAtomicFileHandle<ObAtomicTabletMetaFile> file_handle##suffix;           \
  ASSERT_EQ(OB_SUCCESS, MTL(ObAtomicFileMgr*)->get_tablet_file_handle(ls_id##suffix, \
                                                                      tablet_id##suffix,    \
                                                                      ObAtomicFileType::TABLET_META, \
                                                                      SCN::min_scn(), \
                                                                      file_handle##suffix)); \
  ObAtomicTabletMetaFile *tablet_meta_file##suffix = file_handle##suffix.get_atomic_file(); \
  ASSERT_NE(nullptr, file_handle##suffix.get_atomic_file());            \
  ASSERT_EQ(ls_id##suffix, tablet_meta_file##suffix->ls_id_);               \
  ASSERT_EQ(tablet_id##suffix, tablet_meta_file##suffix->tablet_id_);            \
  ASSERT_EQ(ObAtomicFileType::TABLET_META, tablet_meta_file##suffix->type_);

#define GET_TABLET_META_HANLDE_DEFAULT(file_handle, suffix, ls_id, tablet_id) \
  GET_TABLET_META_HANLDE(file_handle, ls_id, tablet_id, suffix)

#define GET_ONE_SSTABLE_INFO(write_info, op_id) \
  ObSSTableInfo write_info;                                  \
  write_info.op_id_ = op_id;                                 \
  { \
    int64_t cur_us = ObTimeUtility::current_time();          \
    write_info.start_scn_.convert_from_ts(cur_us);    \
    write_info.end_scn_.convert_from_ts(cur_us+1000); \
  }

#define GENERATE_OP_ID(op_id) \
  uint64_t op_id = 0; \
  ASSERT_EQ(OB_SUCCESS, sstablelist_file->generate_next_op_id_(op_id)); \
  tablet_cur_op_id += 1;

#define GENERATE_SSTABLE_LIST_FILE_OPT(op_id, opt) \
  ObStorageObjectOpt opt; \
  ASSERT_EQ(OB_SUCCESS, sstablelist_file->generate_file_opt_(op_id, opt));

#define GENERATE_ADD_SSTABEL_TASK_INFO(task_info, write_info) \
  GENERATE_SSTABEL_TASK_INFO(task_info, write_info, ObAtomicOpType::SS_LIST_ADD_OP)

#define GENERATE_REMOVE_SSTABEL_TASK_INFO(task_info, write_info) \
  GENERATE_SSTABEL_TASK_INFO(task_info, write_info, ObAtomicOpType::SS_LIST_REMOVE_OP)

#define GENERATE_SSTABEL_TASK_INFO(task_info, write_info, op_type) \
  ObSSTableTaskInfo task_info;                            \
  task_info.type_ = op_type;                                \
  task_info.seq_step_ = 1;                                  \
  task_info.start_data_seq_ = 1;                            \
  task_info.parallel_ = 1;                                  \
  task_info.start_scn_ = write_info.start_scn_;              \
  task_info.end_scn_ = write_info.end_scn_;                  \
  task_info.output_ = write_info;

#define CREATE_SSTABLE_LIST_ADD_OP_WITH_RECONFIRM(op_handle) \
  CREATE_SSTABLE_LIST_ADD_OP(op_handle) \
  tablet_cur_op_id += 1;

#define CREATE_SSTABLE_LIST_ADD_OP(op_handle) \
  ObAtomicOpHandle<ObAtomicSSTableListAddOp> op_handle; \
  ASSERT_EQ(OB_SUCCESS, sstablelist_file->create_op(op_handle, true, ObString()));               \
  ASSERT_NE(nullptr, op_handle.get_atomic_op());                                               \
  ASSERT_NE(nullptr, sstablelist_file->current_op_handle_.get_atomic_op());                    \
  ASSERT_EQ(op_handle.get_atomic_op(), sstablelist_file->current_op_handle_.get_atomic_op());  \
  tablet_cur_op_id += 1;

#define CREATE_SSTABLE_LIST_ADD_OP_WITH_GC_INFO(op_handle, suffix, gc_info) \
  ObAtomicOpHandle<ObAtomicSSTableListAddOp> op_handle; \
  ASSERT_EQ(OB_SUCCESS, sstablelist_file##suffix->create_op(op_handle, true, gc_info));               \
  ASSERT_NE(nullptr, op_handle.get_atomic_op());                                               \
  ASSERT_NE(nullptr, sstablelist_file##suffix->current_op_handle_.get_atomic_op());                    \
  ASSERT_EQ(op_handle.get_atomic_op(), sstablelist_file##suffix->current_op_handle_.get_atomic_op());  \
  tablet_cur_op_id += 1;

#define CREATE_SSTABLE_LIST_ADD_OP_WITH_SUFFIX(op_handle, suffix) \
  CREATE_SSTABLE_LIST_ADD_OP_WITH_GC_INFO(op_handle, suffix, ObString())

#define CREATE_LS_META_WRITE_OP_WITH_RECONFIRM(op_handle, need_check_sswriter) \
  CREATE_LS_META_WRITE_OP(op_handle, need_check_sswriter)

#define CREATE_LS_META_WRITE_OP(op_handle, need_check_sswriter) \
  ObAtomicOpHandle<ObAtomicOverwriteOp> op_handle; \
  ASSERT_EQ(OB_SUCCESS, ls_meta_file->create_op(op_handle, need_check_sswriter, ObString()));               \
  ASSERT_NE(nullptr, op_handle.get_atomic_op());                                               \
  ASSERT_NE(nullptr, ls_meta_file->current_op_handle_.get_atomic_op());                    \
  ASSERT_EQ(op_handle.get_atomic_op(), ls_meta_file->current_op_handle_.get_atomic_op()); \
  tablet_cur_op_id += 1;

#define CREATE_TABLET_META_WRITE_OP_WITH_RECONFIRM(op_handle) \
  CREATE_TABLET_META_WRITE_OP(op_handle)                      \
  tablet_cur_op_id += 1;

#define CREATE_TABLET_META_WRITE_OP(op_handle)                              \
  ObAtomicOpHandle<ObAtomicTabletMetaOp> op_handle;                         \
  ASSERT_EQ(OB_SUCCESS, tablet_meta_file->create_op(op_handle, true, ObString()));            \
  ASSERT_NE(nullptr, op_handle.get_atomic_op());                            \
  ASSERT_NE(nullptr, tablet_meta_file->current_op_handle_.get_atomic_op()); \
  ASSERT_EQ(op_handle.get_atomic_op(), tablet_meta_file->current_op_handle_.get_atomic_op());

#define DO_ONE_SSTABLE_LIST_ADD_OP(sstablelist_file, suffix) \
  DO_ONE_SSTABLE_LIST_ADD_OP_WITH_INJECT(sstablelist_file, suffix, false, 0,;)

#define ABORT_ONE_SSTABLE_LIST_ADD_OP(sstablelist_file, suffix) \
  DO_ONE_SSTABLE_LIST_ADD_OP_WITH_INJECT(sstablelist_file, suffix, true, 0,;)

#define DO_ONE_SSTABLE_LIST_ADD_OP_WITH_INJECT(sstablelist_file, suffix, abort, inject_stmt_pos, inject_stmt) \
  INJECT_STMT(1, inject_stmt_pos, inject_stmt) \
  bool need_abort = false; \
  ObAtomicOpHandle<ObAtomicSSTableListAddOp> op_handle##suffix; \
  OZ(sstablelist_file->create_op(op_handle##suffix, true, ObString())); \
  tablet_cur_op_id += 1; \
  if (ret == OB_SUCCESS) { \
    need_abort = true; \
  } \
  INJECT_STMT(2, inject_stmt_pos, inject_stmt) \
  GET_ONE_SSTABLE_INFO(write_info##suffix, tablet_cur_op_id); \
  INJECT_STMT(3, inject_stmt_pos, inject_stmt) \
  GENERATE_ADD_SSTABEL_TASK_INFO(task_info##suffix, write_info##suffix) \
  OZ(op_handle##suffix.get_atomic_op()->write_add_task_info(task_info##suffix)); \
  INJECT_STMT(4, inject_stmt_pos, inject_stmt) \
  if ((ret != OB_SUCCESS && need_abort) || abort) { \
    ASSERT_EQ(OB_SUCCESS, sstablelist_file->abort_op(op_handle##suffix)); \
  } else { \
    if (ret == OB_SUCCESS) { \
      OZ(sstablelist_file->finish_op(op_handle##suffix)); \
    } \
    if (ret == OB_SUCCESS || ret == OB_TIMEOUT) \
    { \
      write_ss_list.push_back(write_info##suffix); \
      write_ss_list.op_id_ = tablet_cur_op_id; \
      write_ss_list.op_type_ = ObAtomicOpType::SS_LIST_ADD_OP; \
    } \
  } \
  OB_MOCK_OBJ_SRORAGE.process_point_ = 0;

#define INJECT_STMT(pos, inject_pos, inject_stmt) \
  if (pos == inject_pos) { \
    inject_stmt; \
  }

#define TEST_TIMEOUT_SUCC_REQUEST_WITH_RECONFIRM(inject_point_array, total_size, expect_ret) \
  TEST_TIMEOUT_REQUEST_WITH_RECONFIRM(inject_point_array, true, total_size, expect_ret)

#define TEST_TIMEOUT_FAIL_REQUEST_WITH_RECONFIRM(inject_point_array, total_size, expect_ret) \
  TEST_TIMEOUT_REQUEST_WITH_RECONFIRM(inject_point_array, false, total_size, expect_ret)

#define TEST_TIMEOUT_REQUEST_WITH_RECONFIRM(inject_point_array, is_succ, total_size, expect_ret) \
  OB_MOCK_OBJ_SRORAGE.process_point_for_write_ = false; \
  for (int i = 0; i < inject_point_array.size(); i++) { \
    std::cout << i << "-----" << inject_point_array[i] << std::endl;\
    for (int j = 1; j <= total_size; j++) {                      \
      ObInjectedErr *ptr = (ObInjectedErr*)ob_malloc(sizeof(ObInjectedErr), ObNewModIds::TEST); \
      ObInjectedErr *injected_err = new(ptr) ObInjectedErr(); \
      {                                                  \
        int ret = OB_SUCCESS;                              \
        std::cout << j << "-----" << inject_point_array[i] << std::endl;\
        ObInjectErrKey err_key(inject_point_array[i]);   \
        injected_err->is_succ_ = is_succ;                 \
        injected_err->is_timeout_ = true;                 \
        injected_err->set_need_wait();                 \
        OB_MOCK_OBJ_SRORAGE.inject_err_to_object_type(err_key, injected_err);\
        GET_MINI_SSTABLE_LIST_HANDLE(file_handle, 1012, 200007, 1) \
        DO_ONE_SSTABLE_LIST_ADD_OP(sstablelist_file1, i) \
        ASSERT_EQ(ret, expect_ret); \
      }                                                  \
      MTL(ObAtomicFileMgr*)->gc_task_.process_(); \
      ObAtomicFileKey key(ObAtomicFileType::MINI_SSTABLE_LIST, ObLSID(1012), ObTabletID(200007)); \
      ObAtomicFile *atomic_file = NULL; \
      ASSERT_EQ(OB_ENTRY_NOT_EXIST, MTL(ObAtomicFileMgr*)->atomic_file_map_.get(key, atomic_file)); \
      {                                                  \
        int ret = OB_SUCCESS;                              \
        int idx2 = i + 1;                                \
        ObInjectErrKey err_key(j);                       \
        ObInjectedErr *ptr2 = (ObInjectedErr*)ob_malloc(sizeof(ObInjectedErr), ObNewModIds::TEST); \
        ObInjectedErr *injected_err2 = new(ptr2) ObInjectedErr(); \
        injected_err2->is_need_wake_waiting_ = true;       \
        injected_err2->need_wakeup_err_ = injected_err;   \
        OB_MOCK_OBJ_SRORAGE.inject_err_to_object_type(err_key, injected_err2);\
        GET_MINI_SSTABLE_LIST_HANDLE(file_handle, 1012, 200007, 1) \
        DO_ONE_SSTABLE_LIST_ADD_OP(sstablelist_file1, idx2)\
        ASSERT_EQ(ret, OB_SUCCESS);                      \
      }                                                  \
                                                         \
      { \
        ObMockObjectManager::inject_err_map.clear(); \
        int ret = OB_SUCCESS;                              \
        GET_MINI_SSTABLE_LIST_HANDLE(file_handle, 1012, 200007, 1) \
        DO_ONE_SSTABLE_LIST_ADD_OP(sstablelist_file1, 3) \
        ASSERT_EQ(ret, OB_SUCCESS);                      \
      } \
      MTL(ObAtomicFileMgr*)->gc_task_.process_(); \
      ASSERT_EQ(OB_ENTRY_NOT_EXIST, MTL(ObAtomicFileMgr*)->atomic_file_map_.get(key, atomic_file)); \
    } \
  }

#define TEST_TIMEOUT_SUCC_REQUEST_WITHOUT_RECONFIRM(inject_point_array, total_size, expect_ret) \
  TEST_TIMEOUT_REQUEST_WITHOUT_RECONFIRM(inject_point_array, true, total_size, expect_ret)

#define TEST_TIMEOUT_FAIL_REQUEST_WITHOUT_RECONFIRM(inject_point_array, total_size, expect_ret) \
  TEST_TIMEOUT_REQUEST_WITHOUT_RECONFIRM(inject_point_array, false, total_size, expect_ret)

#define TEST_TIMEOUT_REQUEST_WITHOUT_RECONFIRM(inject_point_array, is_succ, total_size, expect_ret) \
  OB_MOCK_OBJ_SRORAGE.process_point_for_write_ = false; \
  for (int i = 0; i < inject_point_array.size(); i++) { \
    std::cout << i << "-----" << inject_point_array[i] << std::endl;\
    for (int j = 1; j <= total_size; j++) {                      \
      ObInjectedErr *ptr = (ObInjectedErr*)ob_malloc(sizeof(ObInjectedErr), ObNewModIds::TEST); \
      ObInjectedErr *injected_err = new(ptr) ObInjectedErr(); \
      {                                                  \
        int ret = OB_SUCCESS;                              \
        ObInjectErrKey err_key(inject_point_array[i]);   \
        injected_err->is_succ_ = is_succ;                 \
        injected_err->is_timeout_ = true;                 \
        injected_err->set_need_wait();                 \
        OB_MOCK_OBJ_SRORAGE.inject_err_to_object_type(err_key, injected_err);\
        GET_MINI_SSTABLE_LIST_HANDLE(file_handle, 1011, 200008, 1) \
        { \
          std::cout << j << "-----" << 1 << std::endl;\
          DO_ONE_SSTABLE_LIST_ADD_OP(sstablelist_file1, i) \
          ASSERT_EQ(ret, expect_ret);                   \
        } \
        ret = OB_SUCCESS; \
        int idx2 = i + 1; \
        ObInjectErrKey err_key2(j); \
        ObInjectedErr *ptr2 = (ObInjectedErr*)ob_malloc(sizeof(ObInjectedErr), ObNewModIds::TEST); \
        ObInjectedErr *injected_err2 = new(ptr2) ObInjectedErr(); \
        injected_err2->is_need_wake_waiting_ = true;       \
        injected_err2->need_wakeup_err_ = injected_err;   \
        OB_MOCK_OBJ_SRORAGE.inject_err_to_object_type(err_key2, injected_err2);\
        { \
          std::cout << j << "-----" << 2 << std::endl;\
          DO_ONE_SSTABLE_LIST_ADD_OP(sstablelist_file1, idx2) \
        } \
        ASSERT_EQ(ret, OB_SUCCESS);                      \
      }                                                  \
                                                         \
      { \
        ObMockObjectManager::inject_err_map.clear(); \
        int ret = OB_SUCCESS;                              \
        GET_MINI_SSTABLE_LIST_HANDLE(file_handle, 1011, 200008, 1) \
        DO_ONE_SSTABLE_LIST_ADD_OP(sstablelist_file1, 3) \
        ASSERT_EQ(ret, OB_SUCCESS);                      \
      } \
      MTL(ObAtomicFileMgr*)->gc_task_.process_(); \
      ObAtomicFileKey key(ObAtomicFileType::MINI_SSTABLE_LIST, ObLSID(1011), ObTabletID(200008)); \
      ObAtomicFile *atomic_file = NULL; \
      ASSERT_EQ(OB_ENTRY_NOT_EXIST, MTL(ObAtomicFileMgr*)->atomic_file_map_.get(key, atomic_file)); \
    }                                                    \
  }

#define TEST_INJECT_SSWRITER_CHANGE(inject_point_array, expect_ret) \
  TEST_INJECT_ERR_REQUEST(inject_point_array, false, true, false, true, expect_ret)

#define TEST_INJECT_ERR_REQUEST(inject_point_array, shuffle, sswrite_change, request_timeout, exec_succ, expect_ret) \
  for (int i = 0; i < inject_point_array.size(); i++) { \
    { \
      int ret = OB_SUCCESS; \
      ObInjectErrKey err_key(inject_point_array[i]); \
      tablet_cur_op_id += 1; \
      ObInjectedErr *ptr = (ObInjectedErr*)ob_malloc(sizeof(ObInjectedErr), ObNewModIds::TEST); \
      ObInjectedErr *injected_err = new(ptr) ObInjectedErr(); \
      injected_err->is_sswriter_change_ = sswrite_change; \
      injected_err->is_succ_ = exec_succ; \
      injected_err->is_timeout_ = request_timeout; \
      injected_err->is_shuffled_ = shuffle; \
      OB_MOCK_OBJ_SRORAGE.inject_err_to_object_type(err_key, injected_err); \
      GET_MINI_SSTABLE_LIST_HANDLE_DEFAULT(file_handle, 1) \
      DO_ONE_SSTABLE_LIST_ADD_OP(sstablelist_file1, i) \
      if (ret != expect_ret) { \
        std::cout << "-----" << i << "-----" << std::endl; \
      } \
      ASSERT_EQ(ret, expect_ret); \
    } \
    MTL(ObAtomicFileMgr*)->gc_task_.process_(); \
    ObAtomicFileKey key(ObAtomicFileType::MINI_SSTABLE_LIST, ObLSID(1010), ObTabletID(200001)); \
    ObAtomicFile *atomic_file = NULL; \
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, MTL(ObAtomicFileMgr*)->atomic_file_map_.get(key, atomic_file)); \
  }

#define DO_ONE_SSTABLE_LIST_ADD_OP_V2(sstablelist_file, gc_info, abort, suffix) \
  bool need_abort = false; \
  ObAtomicOpHandle<ObAtomicSSTableListAddOp> op_handle##suffix; \
  OZ(sstablelist_file->create_op(op_handle##suffix, true, gc_info)); \
  tablet_cur_op_id += 1; \
  if (ret == OB_SUCCESS) { \
    need_abort = true; \
  } \
  if ((ret != OB_SUCCESS && need_abort) || abort) { \
    ASSERT_EQ(OB_SUCCESS, sstablelist_file->abort_op(op_handle##suffix)); \
  } else if (ret == OB_SUCCESS) { \
    OZ(sstablelist_file->finish_op(op_handle##suffix)); \
  }

#define DO_ONE_SSTABLE_LIST_FAIL_OP(sstablelist_file, gc_info, suffix) \
  bool need_abort = false; \
  ObAtomicOpHandle<ObAtomicSSTableListAddOp> op_handle##suffix; \
  OZ(sstablelist_file->create_op(op_handle##suffix, true, gc_info)); \
  tablet_cur_op_id += 1; \
  if (ret == OB_SUCCESS) { \
    ASSERT_EQ(OB_SUCCESS, sstablelist_file->fail_op(op_handle##suffix)); \
  }

#define CREATE_SSTABLELIST_ADD_PARALLEL_OP(op_handle, suffix, need_check_sswriter, gc_info) \
  ObAtomicOpHandle<ObAtomicSSTableListAddOp> op_handle; \
  auto last_cnt##op_handle = sstablelist_file##suffix->op_handle_array_.count(); \
  ASSERT_EQ(OB_SUCCESS, sstablelist_file##suffix->create_op_parallel(op_handle, need_check_sswriter, gc_info)); \
  ASSERT_NE(nullptr, op_handle.get_atomic_op());                                               \
  ASSERT_EQ(last_cnt##op_handle+1, sstablelist_file##suffix->op_handle_array_.count());                    \

#define FINISH_SSTABLELIST_ADD_PARALLEL_OP(suffix, op_handle) \
  ASSERT_EQ(OB_SUCCESS, sstablelist_file##suffix->finish_op_parallel(op_handle));

#define FAIL_SSTABLELIST_ADD_PARALLEL_OP(suffix, op_handle) \
  ASSERT_EQ(OB_SUCCESS, sstablelist_file##suffix->fail_op_parallel(op_handle));

#define ABORT_SSTABLELIST_ADD_PARALLEL_OP(suffix, op_handle) \
  ASSERT_EQ(OB_SUCCESS, sstablelist_file##suffix->abort_op_parallel(op_handle));

const int MINI_OP_ID = 0;
const int MINI_CURRENT = 1;
const int MINI_SSTABLE_TASK = 2;
const int SHARE_MINI_SSTABLE_LIST = 3;

template<typename Array>
bool is_array_same(const Array &lhs, const Array &rhs)
{
  bool ret = true;
  if (lhs.count() != rhs.count()) {
    ret = false;
  } else {
    ARRAY_FOREACH(lhs, i) {
      if (lhs[i] != rhs[i]) {
        LOG_INFO("array not same", K(i), K(lhs[i]), K(rhs[i]));
        ret = false;
        break;
      }
    }
  }
  return ret;
}

template<typename Array>
bool is_array_prefix_same(const Array &lhs, const Array &rhs)
{
  bool ret = true;
  int iter_len = MIN(lhs.count(), rhs.count());
  for (int i =0; i < iter_len; i++) {
    if (lhs[i] != rhs[i]) {
      LOG_INFO("array not same", K(i), K(lhs[i]), K(rhs[i]));
      ret = false;
      break;
    }
  }
  return ret;
}

void mock_switch_sswriter();

}  // namespace storage
}  // namespace oceanbase
#endif
