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
#pragma once

#include "lib/container/ob_array.h"
#include "share/detect/ob_detectable_id.h"
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_share_info.h"

namespace oceanbase {
namespace sql {
class ObDfo;
class ObPxSqcHandler;
class ObPxSqcMeta;
class ObPxCoordInfo;
namespace dtl {
class ObDTLIntermResultKey;
class ObDTLIntermResultInfo;
class ObDtlChannel;
} // end namespace dtl
} // end namespace sql
} // end namespace oceanbase

namespace oceanbase {
namespace common{

class ObPeerTaskState;
class ObDetectManager;
// Dm: detect manager, declared in ob_detect_manager.h
class ObDetectManagerUtils {
public:
  static int generate_detectable_id(ObDetectableId &detectable_id, uint64_t tenant_id);
  static void prepare_register_dm_info(common::ObRegisterDmInfo &register_dm_info, sql::ObPxSqcHandler *handler);

  static int qc_register_detectable_id_into_dm(common::ObDetectableId &detectable_id,
                                               bool &register_detectable_id, uint64_t tenant_id,
                                               sql::ObPxCoordInfo& coord_info);
  static void qc_unregister_detectable_id_from_dm(const common::ObDetectableId &detectable_id,
                                                  bool &register_detectable_id);
  static int qc_register_check_item_into_dm(sql::ObDfo &dfo,
                                            const ObIArray<common::ObPeerTaskState> &peer_states,
                                            const ObIArray<sql::dtl::ObDtlChannel *> &dtl_channels);
  static void qc_unregister_check_item_from_dm(sql::ObDfo *dfo, ObDetectManager* dm=nullptr);
  static void qc_unregister_all_check_items_from_dm(const ObIArray<sql::ObDfo *> &dfos);

  static int sqc_register_into_dm(sql::ObPxSqcHandler *sqc_handler, sql::ObPxSqcMeta &sqc);
  static void sqc_unregister_detectable_id_from_dm(const common::ObDetectableId &detectable_id);
  static void sqc_unregister_check_item_from_dm(const common::ObDetectableId &detectable_id,
                                                const uint64_t &node_sequence_id);

  static int single_dfo_register_check_item_into_dm(const common::ObRegisterDmInfo &register_dm_info,
                                                    const sql::dtl::ObDTLIntermResultKey &key,
                                                    sql::dtl::ObDTLIntermResultInfo *result_info);
  static int temp_table_register_check_item_into_dm(const common::ObDetectableId &qc_detectable_id,
                                                    const common::ObAddr &qc_addr,
                                                    const sql::dtl::ObDTLIntermResultKey &dtl_int_key,
                                                    sql::dtl::ObDTLIntermResultInfo *&row_store);
  // both for single_dfo and temp_table
  static void intern_result_unregister_check_item_from_dm(sql::dtl::ObDTLIntermResultInfo *result_info);

  static int p2p_datahub_register_check_item_into_dm(const common::ObRegisterDmInfo &register_dm_info,
                                                     const sql::ObP2PDhKey &p2p_key,
                                                     uint64_t &node_sequence_id_);
  static void p2p_datahub_unregister_check_item_from_dm(const common::ObDetectableId &detectable_id,
                                                        uint64_t node_sequence_id);
};

} // end namespace common
} // end namespace oceanbase
