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

// ################################### 使用多源事务兼容性占位须知 ##################################
// # 占位代码需要书写于宏定义块【NEED_GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION】中
// # 占位方式: 通过【注释】占位，占位需要信息：Helper类型名/Ctx类型名/Enum数值/Enum命名
// #
// # 注意：
// # 0. 在‘余留位置’之前占位
// # 1. 始终先在master占位，保证master分支是其他所有分支的超集
// # 2. master占位之后，开发分支上不可修改注册宏中的对应信息，否则FARM会认为占位冲突，如果有这种场景，需要先修改master占位
// # 3. 所有类型名的书写从全局命名空间'::oceanbase'开始
// # 4. Enum数值采用递增方式占位
// # 5. Enum命名不可同前文重复
// # 6. 由于采用注释方式占位，因此【不需要】书写对应的类型定义并包含在【NEED_MDS_REGISTER_DEFINE】中
// ############################################################################################

// ################################### 使用多源数据兼容性占位须知 ##################################
// # 占位代码需要书写于宏定义块【GENERATE_MDS_UNIT】中，更近一步的:
// # 1. 如果想要添加tablet级别元数据，则将占位信息增加至【GENERATE_NORMAL_MDS_TABLE】中
// # 2. 如果想要添加日志流级别元数据，则将占位信息增加至【GENERATE_LS_INNER_MDS_TABLE】中
// # 占位方式: 通过【定义】占位，占位需要信息：Key类型名/Value类型名/多版本语义支持
// #
// # 注意：
// # 0. 在‘余留位置’之前占位
// # 1. 始终先在master占位，保证master分支是其他所有分支的超集
// # 2. master占位之后，开发分支上不可修改注册宏中的对应信息，否则FARM会认为占位冲突，如果有这种场景，需要先修改master占位
// # 3. 所有类型名的书写从全局命名空间'::oceanbase'开始
// # 4. 若Key类型名非'::oceanbase::storage::mds::DummyKey'，则需要提供对应的Key类型定义，并将对应头文件包含在【NEED_MDS_REGISTER_DEFINE】中
// # 5. 需要提供对应的Value类型定义，并将对应头文件包含在【NEED_MDS_REGISTER_DEFINE】中
// # 6. Key/Value的类型仅需要空定义，不需要定义成员方法和变量，但需要实现框架所需的接口，以通过框架的编译期检查，包括(占位时实现为空)：
// #    a. 打印函数：[int64_t T::to_string(char *, const int64_t) const]
// #    b. 比较函数的某种实现，例如：[bool T::operator==(const T &) const] and [bool T::operator<(const T &) const]
// #    c. 序列化函数的某种实现，例如：[int serialize(char *, const int64_t, int64_t &) const] and [int deserialize(const char *, const int64_t, int64_t &)] and [int64_t get_serialize_size() const]
// #    d. 拷贝/移动函数的某种实现，例如：[int T::assign(const T &)]
// ############################################################################################

// the MDS FRAME must know the defination of some class type to generate legal CPP codes, including:
// 1. DATA type defination if you need multi source data support.
//    1.a. KEY type defination if you need multi source data support with multi key support.
// 2. HELPER FUNCTION type and inherited class of BufferCtx definations if you need multi source
//    transaction support.
// inlcude those classes definations header file in below MACRO BLOCK
// CAUTION: MAKE SURE your header file is as CLEAN as possible to avoid recursive dependencies!
#if defined (NEED_MDS_REGISTER_DEFINE) && !defined (NEED_GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION)
  #include "src/storage/ddl/ob_ddl_change_tablet_to_table_helper.h"
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
  #include "src/storage/tablet/ob_tablet_transfer_tx_ctx.h"
  #include "src/storage/multi_data_source/ob_tablet_create_mds_ctx.h"
  #include "src/storage/multi_data_source/ob_start_transfer_in_mds_ctx.h"
  #include "src/storage/multi_data_source/ob_finish_transfer_in_mds_ctx.h"
  #include "src/share/ob_standby_upgrade.h"
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
                                          ::oceanbase::storage::mds::ObTabletCreateMdsCtx,\
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
                                          ::oceanbase::storage::mds::ObStartTransferInMdsCtx,\
                                          21,\
                                          START_TRANSFER_IN)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::storage::ObTabletFinishTransferOutHelper,\
                                          ::oceanbase::storage::mds::MdsCtx,\
                                          22,\
                                          FINISH_TRANSFER_OUT)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::storage::ObTabletFinishTransferInHelper,\
                                          ::oceanbase::storage::mds::ObFinishTransferInMdsCtx,\
                                          23,\
                                          FINISH_TRANSFER_IN)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::share::ObBalanceTaskMDSHelper,\
                                          ::oceanbase::storage::mds::MdsCtx,\
                                          24,\
                                          TRANSFER_TASK)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::storage::ObTabletStartTransferOutPrepareHelper,\
                                          ::oceanbase::storage::mds::MdsCtx,\
                                          25,\
                                          START_TRANSFER_OUT_PREPARE)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::storage::ObTabletStartTransferOutV2Helper,\
                                          ::oceanbase::storage::ObTransferOutTxCtx,\
                                          26,\
                                          START_TRANSFER_OUT_V2)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::storage::ObStartTransferMoveTxHelper,\
                                          ::oceanbase::storage::ObTransferMoveTxCtx,\
                                          27,\
                                          TRANSFER_MOVE_TX_CTX)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::storage::ObStartTransferDestPrepareHelper,\
                                          ::oceanbase::storage::ObTransferDestPrepareTxCtx,\
                                          28,\
                                          TRANSFER_DEST_PREPARE)
  // GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::storage::ObTabletUnbindLobMdsHelper,\
  //                                         ::oceanbase::storage::mds::MdsCtx,\
  //                                         29,\
  //                                         UNBIND_LOB_TABLET)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::storage::ObChangeTabletToTableHelper,\
                                          ::oceanbase::storage::mds::MdsCtx,\
                                          30,\
                                          CHANGE_TABLET_TO_TABLE_MDS)
  // GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::storage::ObTabletSplitMdsHelper,\
  //                                         ::oceanbase::storage::mds::MdsCtx,\
  //                                         31,\
  //                                         TABLET_SPLIT)
  // GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::storage::ObTabletAbortTransferHelper,\
  //                                         ::oceanbase::storage::mds::ObAbortTransferInMdsCtx,\
  //                                         32,\
  //                                         TRANSFER_IN_ABORTED)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::share::ObUpgradeDataVersionMDSHelper, \
                                          ::oceanbase::storage::mds::MdsCtx, \
                                          33,\
                                          STANDBY_UPGRADE_DATA_VERSION)
  GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION(::oceanbase::storage::ObTabletBindingMdsHelper,\
                                          ::oceanbase::storage::mds::MdsCtx,\
                                          34,\
                                          TABLET_BINDING)
  // # 余留位置（此行之前占位）
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
  // GENERATE_MDS_UNIT(::oceanbase::storage::mds::DummyKey,\
  //                   ::oceanbase::storage::ObTabletSplitMdsUserData,\
  //                   false)
  // # 余留位置（此行之前占位）
#endif

#ifdef GENERATE_LS_INNER_MDS_TABLE
  GENERATE_MDS_UNIT(::oceanbase::storage::mds::DummyKey,\
                    ::oceanbase::unittest::ExampleUserData1,\
                    true) // replace this line if you are the first user to register LS INNER TABLET
  // # 余留位置（此行之前占位）
#endif
/**************************************************************************************************/
