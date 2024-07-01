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

#define private public
#define protected public

#include "lib/ob_errno.h"
#include "lib/net/ob_addr.h"
#include "common/ob_member.h"
#include "common/ob_learner_list.h"
#include "storage/ob_locality_manager.h"
namespace oceanbase
{
namespace storage
{
enum MOCKLOCALITY : int8_t
{
  IDC_MODE_IDC_LEADER = 0,
  IDC_MODE_IDC_FOLLOWER = 1,
  IDC_MODE_REGION_LEADER = 2,
  IDC_MODE_REGION_FOLLOWER = 3,
  IDC_MODE_DIFF_REGION_LEADER = 4,
  IDC_MODE_DIFF_REGION_FOLLOWER = 5,
  REGION_MODE_REGION_FOLLOWER = 6,
  REGION_MODE_REGION_LEADER = 7,
  REGION_MODE_DIFF_REGION_FOLLOWER = 8,
  REGION_MODE_DIFF_REGION_LEADER = 9,
  MAX_LOCALITY_MANAGER
};

class MockLocalityManager : public ObLocalityManager
{
public:
  MockLocalityManager() : ObLocalityManager() {}
  virtual ~MockLocalityManager() {}
  int init_manager(const common::ObAddr &self)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(is_inited_)) {
      ret = OB_INIT_TWICE;
      LOG_WARN("ObLocalityManager init twice", K(ret));
    } else if (!self.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(self));
    } else if (OB_FAIL(server_locality_cache_.init())) {
      LOG_WARN("server_locality_cache_ init failed", K(ret), K(self));
    } else {
      self_ = self;
      is_inited_ = true;
    }
    return ret;
  }
};

static int mock_replica_member(const common::ObAddr &addr, const common::ObRegion &region,
    const common::ObReplicaType type, common::ObReplicaMember &replica)
{
  int ret = OB_SUCCESS;
  replica.reset();
  replica.replica_type_  = type;
  replica.region_ = region;
  replica.memstore_percent_ = 0;
  replica.server_ = addr;
  replica.timestamp_ = 0;
  replica.flag_ = 0;
  return ret;
}

static int mock_addr(const char *ipport, common::ObAddr &addr)
{
  int ret = OB_SUCCESS;
  addr.reset();
  if (OB_ISNULL(ipport)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ipport is nullptr", K(ret));
  } else if (OB_FAIL(addr.parse_from_cstring(ipport))) {
    LOG_WARN("failed to parse from cstring", K(ret));
  }
  return ret;
}

static int mock_learner_list(const common::ObAddr &addr, common::GlobalLearnerList &learner_list)
{
  int ret = OB_SUCCESS;
  learner_list.reset();
  if (OB_FAIL(learner_list.add_server(addr))) {
    LOG_WARN("failed to add server", K(ret), K(addr));
  } else if (!learner_list.contains(addr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret), K(learner_list), K(addr));
  }
  return ret;
}

static int mock_addr_list(const int addr_count, common::ObIArray<common::ObAddr> &addr_list)
{
  int ret = OB_SUCCESS;
  addr_list.reset();
  const int64_t addr_num = 6;
  const char * addr_array[addr_num] = {"192.168.1.1:1234", "192.168.1.2:1234", "192.168.1.3:1234", "192.168.1.4:1234", "192.168.1.5:1234", "192.168.1.6:1234"};
  if (addr_count < 0 || addr_count > 6) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(addr_count));
  } else {
    common::ObAddr addr;
    for (int i = 0; i < addr_count && OB_SUCC(ret); i++) {
      if (OB_FAIL(mock_addr(addr_array[i], addr))) {
        LOG_WARN("failed to mock addr", K(ret));
      } else if (OB_FAIL(addr_list.push_back(addr))) {
        LOG_WARN("failed to add addr", K(ret), K(addr));
      }
    }
  }
  return ret;
}

static int mock_dst_addr(common::ObAddr &addr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(mock_addr("192.168.1.7:1234", addr))) {
    LOG_WARN("failed to mock dst addr", K(ret));
  }
  return ret;
}

static int mock_locality_manager(const MOCKLOCALITY mode, MockLocalityManager &locality_manager)
{
  int ret = OB_SUCCESS;
  common::ObArray<common::ObAddr> addr_list;
  common::ObAddr addr;
  switch (mode) {
  case MOCKLOCALITY::IDC_MODE_IDC_LEADER: {
    if (OB_FAIL(mock_addr_list(5/*addr_count*/, addr_list))) {
      LOG_WARN("failed to mock addr list", K(ret));
    } else if (OB_FAIL(locality_manager.record_server_idc(addr_list.at(0), "idc1"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(0)));
    } else if (OB_FAIL(locality_manager.record_server_region(addr_list.at(0), "region1"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(0)));
    } else if (OB_FAIL(locality_manager.record_server_idc(addr_list.at(1), "idc2"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(1)));
    } else if (OB_FAIL(locality_manager.record_server_region(addr_list.at(1), "region1"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(1)));
    } else if (OB_FAIL(locality_manager.record_server_idc(addr_list.at(2), "idc2"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(2)));
    } else if (OB_FAIL(locality_manager.record_server_region(addr_list.at(2), "region1"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(2)));
    } else if (OB_FAIL(locality_manager.record_server_idc(addr_list.at(3), "idc1"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(3)));
    } else if (OB_FAIL(locality_manager.record_server_region(addr_list.at(3), "region2"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(3)));
    } else if (OB_FAIL(locality_manager.record_server_idc(addr_list.at(4), "idc1"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(4)));
    } else if (OB_FAIL(locality_manager.record_server_region(addr_list.at(4), "region2"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(4)));
    }
    break;
  }
  case MOCKLOCALITY::IDC_MODE_IDC_FOLLOWER: {
    if (OB_FAIL(mock_addr_list(5/*addr_count*/, addr_list))) {
      LOG_WARN("failed to mock addr list", K(ret));
    } else if (OB_FAIL(locality_manager.record_server_idc(addr_list.at(0), "idc1"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(0)));
    } else if (OB_FAIL(locality_manager.record_server_region(addr_list.at(0), "region1"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(0)));
    } else if (OB_FAIL(locality_manager.record_server_idc(addr_list.at(1), "idc1"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(1)));
    } else if (OB_FAIL(locality_manager.record_server_region(addr_list.at(1), "region1"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(1)));
    } else if (OB_FAIL(locality_manager.record_server_idc(addr_list.at(2), "idc2"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(2)));
    } else if (OB_FAIL(locality_manager.record_server_region(addr_list.at(2), "region1"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(2)));
    } else if (OB_FAIL(locality_manager.record_server_idc(addr_list.at(3), "idc2"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(3)));
    } else if (OB_FAIL(locality_manager.record_server_region(addr_list.at(3), "region1"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(3)));
    } else if (OB_FAIL(locality_manager.record_server_idc(addr_list.at(4), "idc1"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(4)));
    } else if (OB_FAIL(locality_manager.record_server_region(addr_list.at(4), "region2"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(4)));
    }
    break;
  }
  case MOCKLOCALITY::IDC_MODE_REGION_LEADER: {
    if (OB_FAIL(mock_addr_list(3/*addr_count*/, addr_list))) {
      LOG_WARN("failed to mock addr list", K(ret));
    } else if (OB_FAIL(locality_manager.record_server_idc(addr_list.at(0), "idc2"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(0)));
    } else if (OB_FAIL(locality_manager.record_server_region(addr_list.at(0), "region1"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(0)));
    } else if (OB_FAIL(locality_manager.record_server_idc(addr_list.at(1), "idc1"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(1)));
    } else if (OB_FAIL(locality_manager.record_server_region(addr_list.at(1), "region2"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(1)));
    } else if (OB_FAIL(locality_manager.record_server_idc(addr_list.at(2), "idc1"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(2)));
    } else if (OB_FAIL(locality_manager.record_server_region(addr_list.at(2), "region2"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(2)));
    }
    break;
  }
  case MOCKLOCALITY::IDC_MODE_REGION_FOLLOWER: {
    if (OB_FAIL(mock_addr_list(3/*addr_count*/, addr_list))) {
      LOG_WARN("failed to mock addr list", K(ret));
    } else if (OB_FAIL(locality_manager.record_server_idc(addr_list.at(0), "idc2"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(0)));
    } else if (OB_FAIL(locality_manager.record_server_region(addr_list.at(0), "region1"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(0)));
    } else if (OB_FAIL(locality_manager.record_server_idc(addr_list.at(1), "idc2"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(1)));
    } else if (OB_FAIL(locality_manager.record_server_region(addr_list.at(1), "region1"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(1)));
    } else if (OB_FAIL(locality_manager.record_server_idc(addr_list.at(2), "idc1"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(2)));
    } else if (OB_FAIL(locality_manager.record_server_region(addr_list.at(2), "region2"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(2)));
    }
    break;
  }
  case MOCKLOCALITY::IDC_MODE_DIFF_REGION_LEADER: {
    if (OB_FAIL(mock_addr_list(1/*addr_count*/, addr_list))) {
      LOG_WARN("failed to mock addr list", K(ret));
    } else if (OB_FAIL(locality_manager.record_server_idc(addr_list.at(0), "idc1"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(0)));
    } else if (OB_FAIL(locality_manager.record_server_region(addr_list.at(0), "region2"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(0)));
    }
    break;
  }
  case MOCKLOCALITY::IDC_MODE_DIFF_REGION_FOLLOWER: {
    if (OB_FAIL(mock_addr_list(2/*addr_count*/, addr_list))) {
      LOG_WARN("failed to mock addr list", K(ret));
    } else if (OB_FAIL(locality_manager.record_server_idc(addr_list.at(0), "idc1"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(0)));
    } else if (OB_FAIL(locality_manager.record_server_region(addr_list.at(0), "region2"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(0)));
    } else if (OB_FAIL(locality_manager.record_server_idc(addr_list.at(1), "idc2"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(1)));
    } else if (OB_FAIL(locality_manager.record_server_region(addr_list.at(1), "region2"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(1)));
    }
    break;
  }
  case MOCKLOCALITY::REGION_MODE_REGION_FOLLOWER: {
    if (OB_FAIL(mock_addr_list(4/*addr_count*/, addr_list))) {
      LOG_WARN("failed to mock addr list", K(ret));
    } else if (OB_FAIL(locality_manager.record_server_idc(addr_list.at(0), "idc1"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(0)));
    } else if (OB_FAIL(locality_manager.record_server_region(addr_list.at(0), "region1"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(0)));
    } else if (OB_FAIL(locality_manager.record_server_idc(addr_list.at(1), "idc2"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(1)));
    } else if (OB_FAIL(locality_manager.record_server_region(addr_list.at(1), "region1"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(1)));
    } else if (OB_FAIL(locality_manager.record_server_idc(addr_list.at(2), "idc1"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(2)));
    } else if (OB_FAIL(locality_manager.record_server_region(addr_list.at(2), "region2"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(2)));
    } else if (OB_FAIL(locality_manager.record_server_idc(addr_list.at(3), "idc2"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(3)));
    } else if (OB_FAIL(locality_manager.record_server_region(addr_list.at(3), "region2"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(3)));
    }
    break;
  }
  case MOCKLOCALITY::REGION_MODE_REGION_LEADER: {
    if (OB_FAIL(mock_addr_list(3/*addr_count*/, addr_list))) {
      LOG_WARN("failed to mock addr list", K(ret));
    } else if (OB_FAIL(locality_manager.record_server_idc(addr_list.at(0), "idc1"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(0)));
    } else if (OB_FAIL(locality_manager.record_server_region(addr_list.at(0), "region1"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(0)));
    } else if (OB_FAIL(locality_manager.record_server_idc(addr_list.at(1), "idc1"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(1)));
    } else if (OB_FAIL(locality_manager.record_server_region(addr_list.at(1), "region2"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(1)));
    } else if (OB_FAIL(locality_manager.record_server_idc(addr_list.at(2), "idc1"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(2)));
    } else if (OB_FAIL(locality_manager.record_server_region(addr_list.at(2), "region2"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(2)));
    }
    break;
  }
  case MOCKLOCALITY::REGION_MODE_DIFF_REGION_LEADER: {
    if (OB_FAIL(mock_addr_list(1/*addr_count*/, addr_list))) {
      LOG_WARN("failed to mock addr list", K(ret));
    } else if (OB_FAIL(locality_manager.record_server_idc(addr_list.at(0), "idc1"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(0)));
    } else if (OB_FAIL(locality_manager.record_server_region(addr_list.at(0), "region2"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(0)));
    }
    break;
  }
  case MOCKLOCALITY::REGION_MODE_DIFF_REGION_FOLLOWER: {
    if (OB_FAIL(mock_addr_list(2/*addr_count*/, addr_list))) {
      LOG_WARN("failed to mock addr list", K(ret));
    } else if (OB_FAIL(locality_manager.record_server_idc(addr_list.at(0), "idc1"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(0)));
    } else if (OB_FAIL(locality_manager.record_server_region(addr_list.at(0), "region2"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(0)));
    } else if (OB_FAIL(locality_manager.record_server_idc(addr_list.at(1), "idc2"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(1)));
    } else if (OB_FAIL(locality_manager.record_server_region(addr_list.at(1), "region2"))) {
      LOG_WARN("failed to record server region", K(ret), K(addr_list.at(1)));
    }
    break;
  }
  default:
    break;
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(mock_dst_addr(addr))) {
    LOG_WARN("failed to mock dst addr", K(ret));
  } else if (OB_FAIL(locality_manager.record_server_idc(addr, "idc1"))) {
    LOG_WARN("failed to record server idc", K(ret));
  } else if (OB_FAIL(locality_manager.record_server_region(addr, "region1"))) {
    LOG_WARN("failed to record server region", K(ret));
  }
  return ret;
}

static int mock_leader_addr(common::ObAddr &addr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(mock_addr("192.168.1.1:1234", addr))) {
    LOG_WARN("failed to mock leader addr", K(ret));
  }
  return ret;
}

static int mock_rs_recommand_addr(common::ObAddr &addr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(mock_addr("192.168.1.4:1234", addr))) {
    LOG_WARN("failed to mock recommand addr", K(ret));
  }
  return ret;
}

static int mock_migrate_arg(const common::ObReplicaMember &replica, ObMigrationOpArg &mock_arg)
{
  int ret = OB_SUCCESS;
  mock_arg.reset();
  const share::ObLSID ls_id(1);
  mock_arg.ls_id_ = ls_id;
  mock_arg.type_ = ObMigrationOpType::TYPE::MIGRATE_LS_OP;
  mock_arg.cluster_id_  = 0;
  mock_arg.priority_ = ObMigrationOpPriority::PRIO_HIGH;
  mock_arg.src_ = replica;
  common::ObReplicaMember dst_replica;
  common::ObAddr addr;
  common::ObReplicaType type = common::ObReplicaType::REPLICA_TYPE_FULL;
  common::ObRegion region("shanghai");
  if (OB_FAIL(mock_dst_addr(addr))) {
    LOG_WARN("failed to mock addr", K(ret));
  } else if (OB_FAIL(mock_replica_member(addr, region, type, dst_replica))) {
    LOG_WARN("failed to mock replica member", K(ret));
  }
  mock_arg.dst_ = dst_replica;
  mock_arg.paxos_replica_number_ = 3;
  return ret;
}

static int mock_valid_ls_meta(obrpc::ObFetchLSMetaInfoResp &res)
{
  int ret = OB_SUCCESS;
  const share::ObLSID ls_id(1);
  res.version_ = CLUSTER_CURRENT_VERSION;
  res.has_transfer_table_ = false;
  res.ls_meta_package_.ls_meta_.tenant_id_ = 1001;
  res.ls_meta_package_.ls_meta_.ls_id_ = ls_id;
  res.ls_meta_package_.ls_meta_.gc_state_ = logservice::LSGCState::NORMAL;
  res.ls_meta_package_.ls_meta_.rebuild_seq_ = 0;
  res.ls_meta_package_.ls_meta_.clog_checkpoint_scn_.set_base();
  res.ls_meta_package_.ls_meta_.migration_status_ = ObMigrationStatus::OB_MIGRATION_STATUS_NONE;
  res.ls_meta_package_.ls_meta_.restore_status_ = share::ObLSRestoreStatus::NONE;
  res.ls_meta_package_.dup_ls_meta_.ls_id_ = ls_id;
  const palf::LSN lsn(184467440737095516);
  res.ls_meta_package_.palf_meta_.prev_log_info_.lsn_ = lsn;
  res.ls_meta_package_.palf_meta_.curr_lsn_ = lsn;
  return ret;
}

static int mock_migrate_arg_for_checkpoint(ObMigrationOpArg &mock_arg)
{
  int ret = OB_SUCCESS;
  mock_arg.reset();
  common::ObReplicaMember src_replica;
  common::ObReplicaType type = common::ObReplicaType::REPLICA_TYPE_FULL;
  common::ObRegion region("shanghai");
  common::ObAddr src_addr;
  if (OB_FAIL(mock_addr("192.168.1.1:1234", src_addr))) {
    LOG_WARN("failed to mock ", K(ret));
  } else if (OB_FAIL(mock_replica_member(src_addr, region, type, src_replica))) {
    LOG_WARN("failed to mock replica member", K(ret));
  } else if (OB_FAIL(mock_migrate_arg(src_replica, mock_arg))) {
    LOG_WARN("failed to mock migrate arg", K(ret));
  }
  return ret;
}

static int mock_migrate_arg_for_rs_recommand(ObMigrationOpArg &mock_arg)
{
  int ret = OB_SUCCESS;
  mock_arg.reset();
  common::ObReplicaMember src_replica;
  common::ObReplicaType type = common::ObReplicaType::REPLICA_TYPE_READONLY;
  common::ObRegion region("shanghai");
  common::ObAddr src_addr;
  if (OB_FAIL(mock_rs_recommand_addr(src_addr))) {
    LOG_WARN("failed to mock ", K(ret));
  } else if (OB_FAIL(mock_replica_member(src_addr, region, type, src_replica))) {
    LOG_WARN("failed to mock replica member", K(ret));
  } else if (OB_FAIL(mock_migrate_arg(src_replica, mock_arg))) {
    LOG_WARN("failed to mock migrate arg", K(ret));
  } else {
    mock_arg.dst_.replica_type_ = common::ObReplicaType::REPLICA_TYPE_READONLY;
    mock_arg.data_src_ = src_replica;
  }
  return ret;
}

static int mock_migrate_arg_for_location(ObMigrationOpArg &mock_arg)
{
  int ret = OB_SUCCESS;
  mock_arg.reset();
  common::ObReplicaMember src_replica;
  common::ObReplicaType type = common::ObReplicaType::REPLICA_TYPE_READONLY;
  common::ObRegion region("shanghai");
  common::ObAddr src_addr;
  if (OB_FAIL(mock_addr("192.168.1.4:1234", src_addr))) {
    LOG_WARN("failed to mock ", K(ret));
  } else if (OB_FAIL(mock_replica_member(src_addr, region, type, src_replica))) {
    LOG_WARN("failed to mock replica member", K(ret));
  } else if (OB_FAIL(mock_migrate_arg(src_replica, mock_arg))) {
    LOG_WARN("failed to mock migrate arg", K(ret));
  }
  return ret;
}

static int mock_migrate_arg_for_r_type(ObMigrationOpArg &mock_arg)
{
  int ret = OB_SUCCESS;
  mock_arg.reset();

  if (OB_FAIL(mock_migrate_arg_for_location(mock_arg))) {
    LOG_WARN("failed to mock ", K(ret), K(mock_arg));
  } else {
    mock_arg.dst_.replica_type_ = common::ObReplicaType::REPLICA_TYPE_READONLY;
  }
  return ret;
}

static share::SCN mock_ckpt_inc(share::SCN &local_ls_checkpoint_scn)
{
  share::SCN result;
  result = share::SCN::scn_inc(local_ls_checkpoint_scn);
  return result;
}

static int get_checkpoint_policy(const ObMigrationOpArg &arg, const uint64_t tenant_id,
    ObStorageHASrcProvider::ChooseSourcePolicy &policy)
{
  int ret = OB_SUCCESS;
  bool enable_choose_source_policy = false;
  const char *str = "idc";
  if (OB_FAIL(ObStorageHAChooseSrcHelper::get_policy_type(arg, tenant_id,
      enable_choose_source_policy, str, policy))) {
    LOG_WARN("failed to get policy type", K(ret), K(arg), K(tenant_id));
  }
  return ret;
}


static int get_recommand_policy(const ObMigrationOpArg &arg, const uint64_t tenant_id,
    ObStorageHASrcProvider::ChooseSourcePolicy &policy)
{
  int ret = OB_SUCCESS;
  bool enable_choose_source_policy = true;
  const char *str = "idc";
  if (OB_FAIL(ObStorageHAChooseSrcHelper::get_policy_type(arg, tenant_id,
      enable_choose_source_policy, str, policy))) {
    LOG_WARN("failed to get policy type", K(ret), K(arg), K(tenant_id));
  }
  return ret;
}

static int get_idc_policy(const ObMigrationOpArg &arg, const uint64_t tenant_id,
    ObStorageHASrcProvider::ChooseSourcePolicy &policy)
{
  int ret = OB_SUCCESS;
  bool enable_choose_source_policy = true;
  const char *str = "idc";
  if (OB_FAIL(ObStorageHAChooseSrcHelper::get_policy_type(arg, tenant_id,
      enable_choose_source_policy, str, policy))) {
    LOG_WARN("failed to get policy type", K(ret), K(arg), K(tenant_id));
  }
  return ret;
}

static int get_region_policy(const ObMigrationOpArg &arg, const uint64_t tenant_id,
    ObStorageHASrcProvider::ChooseSourcePolicy &policy)
{
  int ret = OB_SUCCESS;
  bool enable_choose_source_policy = true;
  const char *str = "region";
  if (OB_FAIL(ObStorageHAChooseSrcHelper::get_policy_type(arg, tenant_id,
      enable_choose_source_policy, str, policy))) {
    LOG_WARN("failed to get policy type", K(ret), K(arg), K(tenant_id));
  }
  return ret;
}

static int mock_migrate_arg_for_rebuild(ObMigrationOpArg &mock_arg)
{
  int ret = OB_SUCCESS;
  mock_arg.reset();
  common::ObReplicaMember src_replica;
  common::ObReplicaType type = common::ObReplicaType::REPLICA_TYPE_FULL;
  common::ObRegion region("shanghai");
  common::ObAddr src_addr;
  if (OB_FAIL(mock_addr("192.168.1.4:1234", src_addr))) {
    LOG_WARN("failed to mock ", K(ret));
  } else if (OB_FAIL(mock_replica_member(src_addr, region, type, src_replica))) {
    LOG_WARN("failed to mock replica member", K(ret));
  } else if (OB_FAIL(mock_migrate_arg(src_replica, mock_arg))) {
    LOG_WARN("failed to mock migrate arg", K(ret));
  } else {
    mock_arg.type_ = ObMigrationOpType::TYPE::REBUILD_LS_OP;
  }
  return ret;
}

static int mock_migrate_arg_init_fail(ObMigrationOpArg &mock_arg)
{
  int ret = OB_SUCCESS;
  mock_arg.reset();
  common::ObReplicaMember src_replica;
  common::ObReplicaType type = common::ObReplicaType::REPLICA_TYPE_FULL;
  common::ObRegion region("shanghai");
  common::ObAddr src_addr;
  if (OB_FAIL(mock_addr("192.168.1.4:1234", src_addr))) {
    LOG_WARN("failed to mock ", K(ret));
  } else if (OB_FAIL(mock_replica_member(src_addr, region, type, src_replica))) {
    LOG_WARN("failed to mock replica member", K(ret));
  } else if (OB_FAIL(mock_migrate_arg(src_replica, mock_arg))) {
    LOG_WARN("failed to mock migrate arg", K(ret));
  } else {
    mock_arg.type_ = ObMigrationOpType::TYPE::MAX_LS_OP;
  }
  return ret;
}

static int mock_check_replica_type_addr(common::ObAddr &addr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(mock_addr("192.168.1.4:1234", addr))) {
    LOG_WARN("failed to mock leader addr", K(ret));
  }
  return ret;
}

}
}
