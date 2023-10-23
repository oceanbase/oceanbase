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

// the MDS FRAME must know the defination of some class type to generate legal CPP codes, including:
// 1. DATA type defination if you need multi source data support.
//    1.a. KEY type defination if you need multi source data support with multi key support.
// 2. HELPER FUNCTION type and inherited class of BufferCtx definations if you need multi source
//    transaction support.
// inlcude those classes definations header file in below MACRO BLOCK
// CAUTION: MAKE SURE your header file is as CLEAN as possible to avoid recursive dependencies!
#if defined (NEED_MDS_REGISTER_DEFINE) && !defined (NEED_GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION)
  #include "src/storage/multi_data_source/compile_utility/mds_dummy_key.h"
  #include "src/storage/multi_data_source/mds_ctx.h"
  #include "src/storage/multi_data_source/test/example_user_data_define.h"
  #include "src/storage/multi_data_source/test/example_user_helper_define.h"
  #include "src/storage/tablet/ob_tablet_create_delete_mds_user_data.h"
  #include "src/storage/tablet/ob_tablet_create_mds_helper.h"
  #include "src/storage/tablet/ob_tablet_delete_mds_helper.h"
  #include "src/storage/tablet/ob_tablet_binding_helper.h"
  #include "src/storage/tablet/ob_tablet_binding_mds_user_data.h"
  #include "src/share/ob_tablet_autoincrement_param.h"
  #include "src/storage/compaction/ob_medium_compaction_info.h"
  #include "src/storage/tablet/ob_tablet_start_transfer_mds_helper.h"
  #include "src/storage/tablet/ob_tablet_finish_transfer_mds_helper.h"
  #include "src/share/balance/ob_balance_task_table_operator.h"
#endif
/**************************************************************************************************/

/**********************generate mds frame code with multi source transaction***********************/
// GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(HELPER_CLASS, BUFFER_CTX_TYPE, ID, ENUM_NAME) is an
// interface MACRO to transfer necessary information to MDS FRAME to generate codes in transaction
// layer, mostly in ob_multi_data_source.cpp and ob_trans_part_ctx.cpp.
//
// @param HELPER_CLASS the class must has two static method signatures(or COMPILE ERROR):
//                     1. static int on_register(const char* buf,
//                                               const int64_t len,
//                                               storage::mds::BufferCtx &ctx);// on leader
//                     2. static int on_replay(const char* buf,
//                                             const int64_t len,
//                                             const share::SCN &scn,
//                                             storage::mds::BufferCtx &ctx);// on follower
//                     the actual ctx's type is BUFFER_CTX_TYPE user registered.
// @param BUFFER_CTX_TYPE must inherited from storage::mds::BufferCtx.
// @param ID for FRAME code reflection reason and compat reason, write the number by INC logic.
// @param ENUM_NAME transaction layer needed, will be defined in enum class
//                  ObTxDataSourceType(ob_multi_data_source.h)
#define GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(HELPER_CLASS, BUFFER_CTX_TYPE, ID, ENUM_NAME) \
_GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION_(HELPER_CLASS, BUFFER_CTX_TYPE, ID, ENUM_NAME)
#ifdef NEED_GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::unittest::ExampleUserHelperFunction1,\
                                          ::oceanbase::storage::mds::MdsCtx,\
                                          14,\
                                          TEST1)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::unittest::ExampleUserHelperFunction2,\
                                          ::oceanbase::storage::mds::MdsCtx,\
                                          15,\
                                          TEST2)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::unittest::ExampleUserHelperFunction3,\
                                          ::oceanbase::unittest::ExampleUserHelperCtx,\
                                          16,\
                                          TEST3)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::storage::ObTabletCreateMdsHelper,\
                                          ::oceanbase::storage::mds::MdsCtx,\
                                          3,\
                                          CREATE_TABLET_NEW_MDS)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::storage::ObTabletDeleteMdsHelper,\
                                          ::oceanbase::storage::mds::MdsCtx,\
                                          4,\
                                          DELETE_TABLET_NEW_MDS)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::storage::ObTabletUnbindMdsHelper,\
                                          ::oceanbase::storage::mds::MdsCtx,\
                                          7,\
                                          UNBIND_TABLET_NEW_MDS)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::storage::ObTabletStartTransferOutHelper,\
                                          ::oceanbase::storage::mds::MdsCtx,\
                                          20,\
                                          START_TRANSFER_OUT)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::storage::ObTabletStartTransferInHelper,\
                                          ::oceanbase::storage::mds::MdsCtx,\
                                          21,\
                                          START_TRANSFER_IN)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::storage::ObTabletFinishTransferOutHelper,\
                                          ::oceanbase::storage::mds::MdsCtx,\
                                          22,\
                                          FINISH_TRANSFER_OUT)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::storage::ObTabletFinishTransferInHelper,\
                                          ::oceanbase::storage::mds::MdsCtx,\
                                          23,\
                                          FINISH_TRANSFER_IN)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::share::ObBalanceTaskMDSHelper,\
                                          ::oceanbase::storage::mds::MdsCtx,\
                                          24,\
                                          TRANSFER_TASK)
#undef GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION
#endif
/**************************************************************************************************/

/**********************generate mds frame code with multi source data******************************/
// GENERATE_MDS_UNIT(KEY_TYPE, VALUE_TYPE, NEED_MULTI_VERSION) is an
// interface MACRO to transfer necessary information to MDS FRAME to generate codes on MdsTable.
// 1. There are some requirements about class type KEY_TYPE and VALUE_TYPE, if your type is not
//    satisfied with below requirments, COMPILE ERROR will occured.
// 2. If you want store your data on normal tablet, write register macro in
//    GENERATE_NORMAL_MDS_TABLE block, if you want store your data on LS INNER tablet, write
//    register macro in GENERATE_LS_INNER_MDS_TABLE block.
//
// @param KEY_TYPE if you do not need multi row specfied by key, use DummyKey, otherwise, your Key
//                 type must support:
//                 1. has both [bool T::operator==(const T &)] and [bool T::operator<(const T &)],
//                    or has [int T::compare(const Key &, bool &)]
//                 2. be able to_string: [int64_t T::to_string(char *, const int64_t)]
//                 3. serializeable(the classic 3 serialize methods)
//                 4. copiable, Frame code will try call(ordered):
//                    a. copy construction: [T::T(const T &)]
//                    b. assign operator: [T &T::operator=(const T &)]
//                    c. assign method: [int T::assign(const T &)]
//                    d. COMPILE ERROR if neither of above method exists.
// @param VALUE_TYPE must support:
//                   1. be able to string: [int64_t to_string(char *, const int64_t)]
//                   2. serializeable(the classic 3 serialize methods)
//                   3. copiable as KEY_TYPE
//                   4. (optional) if has [void on_redo(const share::SCN&)] will be called.
//                      (optional) if has [void on_prepare(const share::SCN&)] will be called.
//                      (optional) if has [void on_commit(const share::SCN&)] will be called.
//                      (optional) if has [void on_abort()] will be called.
//                      CAUTION: DO NOT do heavy action in these optinal functions!
//                   5. (optional) optimized if VALUE_TYPE support rvalue sematic.
// @param NEED_MULTI_VERSION if setted true, will remain multi version data,
//                           or just remain uncommitted and the last committed data.
#define GENERATE_MDS_UNIT(KEY_TYPE, VALUE_TYPE, NEED_MULTI_VERSION) \
_GENERATE_MDS_UNIT_(KEY_TYPE, VALUE_TYPE, NEED_MULTI_VERSION)
#ifdef GENERATE_TEST_MDS_TABLE
  GENERATE_MDS_UNIT(::oceanbase::storage::mds::DummyKey,\
                    ::oceanbase::unittest::ExampleUserData1,\
                    true) // no need multi row, need multi version
                          // (complicated data with rvalue optimized)
  GENERATE_MDS_UNIT(::oceanbase::storage::mds::DummyKey,\
                    ::oceanbase::unittest::ExampleUserData2,\
                    true) // no need multi row, need multi version(simple data)
  GENERATE_MDS_UNIT(::oceanbase::unittest::ExampleUserKey,\
                    ::oceanbase::unittest::ExampleUserData1,\
                    false)// need multi row, no need multi version
#endif

#ifdef GENERATE_NORMAL_MDS_TABLE
  GENERATE_MDS_UNIT(::oceanbase::storage::mds::DummyKey,\
                    ::oceanbase::storage::ObTabletCreateDeleteMdsUserData,\
                    false)
  GENERATE_MDS_UNIT(::oceanbase::storage::mds::DummyKey,\
                    ::oceanbase::storage::ObTabletBindingMdsUserData,\
                    false)
  GENERATE_MDS_UNIT(::oceanbase::storage::mds::DummyKey,\
                    ::oceanbase::share::ObTabletAutoincSeq,\
                    false)
  GENERATE_MDS_UNIT(::oceanbase::compaction::ObMediumCompactionInfoKey,\
                    ::oceanbase::compaction::ObMediumCompactionInfo,\
                    false)
#endif

#ifdef GENERATE_LS_INNER_MDS_TABLE
  GENERATE_MDS_UNIT(::oceanbase::storage::mds::DummyKey,\
                    ::oceanbase::unittest::ExampleUserData1,\
                    true) // replace this line if you are the first user to register LS INNER TABLET
#endif
/**************************************************************************************************/
