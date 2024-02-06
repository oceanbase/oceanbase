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

#define USING_LOG_PREFIX COMMON

#include "ob_admin_dump_ckpt_executor.h"
#include <iostream>
#include "storage/slog_ckpt/ob_server_checkpoint_reader.h"
#include "storage/slog_ckpt/ob_tenant_storage_checkpoint_reader.h"
#include "storage/slog_ckpt/ob_server_checkpoint_slog_handler.h"
#include "storage/ls/ob_ls_meta.h"
#include "storage/tablet/ob_tablet_meta.h"

namespace oceanbase
{

using namespace storage;
namespace tools
{

const char *ObAdminDumpCkptExecutor::checkpoint_meta_type_strs_[] = {
    "tenant_meta",
    "ls_meta",
    "tablet",
};

const char *ObAdminDumpCkptExecutor::get_checkpoint_meta_type_str(const ObCheckpointType type)
{
  return checkpoint_meta_type_strs_[static_cast<int>(type)];
}

ObAdminDumpCkptExecutor::ObAdminDumpCkptExecutor()
  : ObAdminExecutor(),
    tenant_id_(OB_SERVER_TENANT_ID),
    meta_type_(ObCheckpointType::MAX_TYPE_NUM)
{
}

int ObAdminDumpCkptExecutor::execute(int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  FILE *stream = stdout;
  common::ObArray<omt::ObTenantMeta> meta_list;

  if (OB_FAIL(parse_args(argc - 1, argv + 1))) {
    LOG_WARN("fail to parse dir path", K(ret));
  } else if (0 == STRLEN(data_dir_)) {
    if (OB_FAIL(load_config())) {
      LOG_WARN("fail to load config", K(ret));
    } else {
      STRCPY(data_dir_, config_mgr_.get_config().data_dir.str());
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(prepare_io())) {
    LOG_WARN("fail to prepare io", K(ret));
  } else if (OB_FAIL(init_slogger_mgr())) {
    LOG_WARN("fail to init_slogger_mgr", K(ret));
  } else if (OB_SERVER_TENANT_ID == tenant_id_) { // tenant not specifed or 500 is specified
    if (OB_FAIL(dump_server_ckpt(stream))) {
      LOG_WARN("fail to dump server ckpt", K(ret));
    }
  } else { // a user tenant is sepcified
    if (OB_FAIL(dump_tenant_ckpt(stream))) {
      LOG_WARN("fail to dump tenant ckpt", K(ret));
    }
  }

  return ret;
}

int ObAdminDumpCkptExecutor::parse_args(int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  int opt = 0;
  const char* opt_string = "hd:u:t:";
  struct option longopts[] =
    {{"help", 0, NULL, 'h' },
     {"data_dir", 1, NULL, 'd' },
     {"tenant_id", 1, NULL, 'u' },
     {"type", 1, NULL, 't' }};

  while ((opt = getopt_long(argc, argv, opt_string, longopts, NULL)) != -1) {
    switch (opt) {
      case 'h': {
        print_usage();
        break;
      }
      case 'd':
        STRCPY(data_dir_, optarg);
        break;
      case 'u':
        tenant_id_ = static_cast<uint64_t>(strtol(optarg, NULL, 10));
        break;
      case 't': {
        bool found = false;
        for (int i = 0; i < static_cast<int>(ObCheckpointType::MAX_TYPE_NUM); i++) {
          ObCheckpointType type = static_cast<ObCheckpointType>(i);
          const char *type_str = get_checkpoint_meta_type_str(type);
          if (0 == STRCMP(type_str, optarg)) {
            found = true;
            meta_type_ = type;
            break;
          }
        }
        if (!found) {
          ret = OB_INVALID_ARGUMENT;
          LOG_ERROR("invalid checkpoint type specifed by -t", K(ret));
        }
        break;
      }

      default: {
        print_usage();
        ret = OB_INVALID_ARGUMENT;
      }
    }
  }
  LOG_INFO("parse_arg finish", K(ret), K_(data_dir), K_(tenant_id), K_(meta_type));
  return ret;
}


void ObAdminDumpCkptExecutor::print_usage()
{
  fprintf(stderr, "\nUsage: ob_admin dump_ckpt -d data_dir [-u tenant_id] [-t meta_type] \n"
                    "       -d --data_dir specify the data dir path, get data_dir from config if it is not sepcified \n"
                    "       -u --tenant_id specify the tenant whose ckpt is to be dumped, if not specified,\n"
                    "                      set it to 500(server tenant) which means to dump the server checkpoint\n"
                    "       -t --type specify meta type(tenant_meta/ls_meta/tablet) which is to be dumped, dump meta of all type if not specified\n"
                    "   eg. ob_admin dump_ckpt -d /home/fenggu.yh/ob1.obs0/store -u 1 -t tablet\n\n");
}

int ObAdminDumpCkptExecutor::get_tenant_meta_from_ckpt(const uint64_t tenant_id, common::ObIArray<omt::ObTenantMeta> &metas)
{

  int ret = OB_SUCCESS;
  const ObServerSuperBlock &super_block = OB_SERVER_BLOCK_MGR.get_server_super_block();
  ObServerCheckpointReader server_ckpt_reader;

  metas.reset();

  if (OB_UNLIKELY(!super_block.is_valid())) {
    ret = OB_ERR_SYS;
    LOG_WARN("super block is invalid", K(ret), K(super_block));
  } else if (OB_FAIL(server_ckpt_reader.read_checkpoint(super_block))) {
    LOG_WARN("fail to read checkpoint", K(ret), K(super_block));
  } else {
    const common::ObArray<omt::ObTenantMeta> &meta_list = server_ckpt_reader.get_tenant_meta_list();
    for (int i = 0; i < meta_list.count() && OB_SUCC(ret); i++) {
      if (OB_SERVER_TENANT_ID ==  tenant_id) { // get all tenant meta
        if (OB_FAIL(metas.push_back(meta_list.at(i)))) {
          LOG_WARN("fail to push_back meta", K(ret));
        }
      } else if (meta_list.at(i).unit_.tenant_id_ == tenant_id) { // get specified tenant meta
        if (OB_FAIL(metas.push_back(meta_list.at(i)))) {
          LOG_WARN("fail to push_back meta", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && 0 == metas.count()) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_INFO("tenant meta not found", K(ret), K(tenant_id));
    }
  }

  return ret;
}

int ObAdminDumpCkptExecutor::dump_tenant_metas(common::ObIArray<omt::ObTenantMeta> &metas, FILE *stream)
{
  int ret = OB_SUCCESS;
  if (fprintf(stream, "=========================tenant meta====================\n") < 0) {
    ret = OB_ERR_SYS;
    LOG_WARN("fail to fprintf", K(ret));
  }
  for (int i = 0; i < metas.count() && OB_SUCC(ret); i++) {
    if (fprintf(stream, "%d. %s\n", i + 1, to_cstring(metas.at(i))) < 0) {
      ret = OB_ERR_SYS;
      LOG_WARN("fail to fprintf", K(ret));
    }
  }

  return ret;
}

int ObAdminDumpCkptExecutor::dump_all_ls_metas_from_ckpt(const blocksstable::MacroBlockId &entry_block, FILE *stream)
{
  int ret = OB_SUCCESS;

  ObTenantStorageCheckpointReader tenant_storage_ckpt_reader;
  ObArray<blocksstable::MacroBlockId> meta_block_list;

  ObTenantStorageCheckpointReader::ObCheckpointMetaOp dump_ls_meta_op =
      std::bind(&ObAdminDumpCkptExecutor::dump_ls_meta,
      this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, stream);

  if (fprintf(stream, "======================ls meta=====================\n") < 0) {
    ret = OB_ERR_SYS;
    LOG_WARN("fail to fprintf", K(ret));
  } else if (OB_FAIL(tenant_storage_ckpt_reader.iter_read_checkpoint_item(
      entry_block, dump_ls_meta_op, meta_block_list))) {
    LOG_WARN("fail to iter_read_checkpoint_item", K(ret), K(entry_block));
  } else if (fprintf(stream, "checkpoint_block_id_list: [%s]\n", to_cstring(meta_block_list)) < 0) {
    ret = OB_ERR_SYS;
    LOG_WARN("fail to fprintf", K(ret));
  }

  return ret;
}

int ObAdminDumpCkptExecutor::dump_ls_meta(
  const ObMetaDiskAddr &addr, const char *buf, const int64_t buf_len, FILE *stream)
{
  UNUSED(addr);
  int ret = OB_SUCCESS;
  ObLSMeta ls_meta;
  int64_t pos = 0;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(ls_meta.deserialize(buf, buf_len, pos))) {
    LOG_WARN("fail to deserialize ls", K(ret));
  } else if (fprintf(stream, "%s\n", to_cstring(ls_meta)) < 0) {
    ret = OB_ERR_SYS;
    LOG_WARN("fail to fprintf", K(ret));
  }

  return ret;
}

int ObAdminDumpCkptExecutor::dump_all_tablets_from_ckpt(const blocksstable::MacroBlockId &entry_block, FILE *stream)
{
  int ret = OB_SUCCESS;
  ObTenantStorageCheckpointReader tenant_storage_ckpt_reader;
  ObArray<blocksstable::MacroBlockId> meta_block_list;

  ObTenantStorageCheckpointReader::ObCheckpointMetaOp dump_tablet_op =
      std::bind(&ObAdminDumpCkptExecutor::dump_tablet,
      this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, stream);

  if (fprintf(stream, "======================tablet=====================\n") < 0) {
    ret = OB_ERR_SYS;
    LOG_WARN("fail to fprintf", K(ret));
  } else if (OB_FAIL(tenant_storage_ckpt_reader.iter_read_checkpoint_item(
      entry_block, dump_tablet_op, meta_block_list))) {
    LOG_WARN("fail to iter_read_checkpoint_item", K(ret), K(entry_block));
  } else if (fprintf(stream, "checkpoint_block_id_list: %s\n", to_cstring(meta_block_list)) < 0) {
    ret = OB_ERR_SYS;
    LOG_WARN("fail to fprintf", K(ret));
  }

  return ret;
}

int ObAdminDumpCkptExecutor::dump_tablet(
  const ObMetaDiskAddr &addr, const char *buf, const int64_t buf_len, FILE *stream)
{
  UNUSED(addr);
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObTabletMeta tablet_meta;
  int64_t pos = 0;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(tablet_meta.deserialize(buf, buf_len, pos))) {
    LOG_WARN("fail to deserialize tablet", K(ret));
  } else if (fprintf(stream, "%s\n", to_cstring(tablet_meta)) < 0) {
    ret = OB_ERR_SYS;
    LOG_WARN("fail to fprintf", K(ret));
  }

  return ret;
}

int ObAdminDumpCkptExecutor::dump_server_ckpt(FILE *stream)
{
  int ret = OB_SUCCESS;
  common::ObArray<omt::ObTenantMeta> meta_list;
  if (ObCheckpointType::MAX_TYPE_NUM != meta_type_ && ObCheckpointType::TENANT_META != meta_type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("server tenant only has ckpt of tenant_meta", K(ret));
  } else if (OB_FAIL(get_tenant_meta_from_ckpt(OB_SERVER_TENANT_ID, meta_list))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("server checkpoint is empty");
    } else {
      LOG_WARN("fail to get tenant metas", K(ret));
    }
  } else if (OB_FAIL(dump_tenant_metas(meta_list, stream))) {
    LOG_WARN("fail to dump tenant metas", K(ret));
  }

  return ret;
}

int ObAdminDumpCkptExecutor::dump_tenant_ckpt(FILE *stream)
{
  int ret = OB_SUCCESS;
  omt::ObTenantMeta tenant_meta;

  ObServerCheckpointSlogHandler ckpt_slog_hander;

  if (OB_FAIL(ckpt_slog_hander.init())) {
    LOG_WARN("fail to init ObServerCheckpointSlogHandler", K(ret));
  } else if (OB_FAIL(ckpt_slog_hander.load_all_tenant_metas())) {
    LOG_WARN("fail to load_all_tenant_metas", K(ret));
  } else {
    LOG_INFO("succ to load_all_tenant_metas", K(ret));
    const ObServerCheckpointSlogHandler::TENANT_META_MAP &tenant_meta_map = ckpt_slog_hander.get_tenant_meta_map();
    if (OB_FAIL(tenant_meta_map.get_refactored(tenant_id_, tenant_meta))) {
      LOG_WARN("fail to get tenant meta", K(ret));
    } else if (fprintf(stream, "tenant_meta=> %s\n", to_cstring(tenant_meta)) < 0) {
      ret = OB_ERR_SYS;
      LOG_WARN("fail to fprintf tenant meta", K(ret));
    } else if (ObCheckpointType::TENANT_META == meta_type_) {
      // has dumped, do nothing
    } else if (ObCheckpointType::LS_META == meta_type_) {
      if (OB_FAIL(dump_all_ls_metas_from_ckpt(tenant_meta.super_block_.ls_meta_entry_, stream))) {
        LOG_WARN("fail to dump all ls metas", K(ret), K(tenant_meta));
      }
    } else if (ObCheckpointType::TABLET == meta_type_) {
      if (OB_FAIL(dump_all_tablets_from_ckpt(tenant_meta.super_block_.tablet_meta_entry_, stream))) {
        LOG_WARN("fail to dump all tablets", K(ret), K(tenant_meta));
      }
    } else if (ObCheckpointType::MAX_TYPE_NUM == meta_type_) { // dump all type
      if (OB_FAIL(dump_all_ls_metas_from_ckpt(tenant_meta.super_block_.ls_meta_entry_, stream))) {
        LOG_WARN("fail to dump all ls metas", K(ret), K(tenant_meta));
      } else if (OB_FAIL(dump_all_tablets_from_ckpt(tenant_meta.super_block_.tablet_meta_entry_, stream))) {
        LOG_WARN("fail to dump all tablets", K(ret), K(tenant_meta));
      }
    }
    ckpt_slog_hander.destroy();
  }

  return ret;
}

} // namespace tools
} // namespace oceanbase
