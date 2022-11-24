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

#define USING_LOG_PREFIX SERVER

#include "observer/ob_dump_task_generator.h"
#include "lib/alloc/memory_dump.h"
#include "lib/allocator/ob_mem_leak_checker.h"
#include "lib/utility/ob_fast_convert.h"
#include "share/ob_define.h"
#include "share/ob_errno.h"
#include "sql/parser/ob_parser.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
using namespace common;
using namespace sql;
namespace observer
{
int ObDumpTaskGenerator::read_cmd(char *buf, int64_t len, int64_t &real_size)
{
  int ret = OB_SUCCESS;
  FILE *fp = fopen("etc/dump.config", "r");
  if (nullptr == fp) {
    ret = OB_ERR_SYS;
    LOG_WARN("open config file failed", K(ret), K(strerror(errno)));
  } else {
    fseek(fp, 0, SEEK_END);
    int64_t size = ftell(fp);
    rewind(fp);
    if (size > len) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("cmd too long", K(ret), K(size), K(len));
    } else {
      fread(buf, 1, size, fp);
      real_size = size;
    }
    fclose(fp);
  }
  return ret;
}

int ObDumpTaskGenerator::generate_task_from_file()
{
  int ret = OB_SUCCESS;
  auto &mem_dump = ObMemoryDump::get_instance();
  ObArenaAllocator allocator;
  ObMemAttr attr(common::OB_SERVER_TENANT_ID, "dumpParser", ObCtxIds::DEFAULT_CTX_ID,
                 lib::OB_HIGH_ALLOC);
  allocator.set_attr(attr);
  ObParser parser(allocator, SMO_DEFAULT);
  ParseResult parse_result;
  ParseNode *stmt_node = nullptr;
  ParseNode *node = nullptr;
  const int64_t len = 128;
  char buf[len];
  int64_t real_size = 0;
  ObString cmd;
  if (!mem_dump.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(read_cmd(buf, len, real_size))) {
    LOG_WARN("read cmd failed", K(ret));
  } else if(FALSE_IT(cmd.assign_ptr(buf, static_cast<int32_t>(real_size)))) {
  } else if (OB_FAIL(parser.parse(cmd, parse_result))) {
    LOG_WARN("parse failed", K(cmd), K(ret));
  } else if(nullptr == parse_result.result_tree_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("nullptr", K(cmd), K(ret));
  } else if (OB_ISNULL(stmt_node = parse_result.result_tree_->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("nullptr", K(cmd), K(ret));
  } else if (stmt_node->type_ != T_DUMP_MEMORY) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support", K(cmd), K(stmt_node->type_));
  } else if (OB_ISNULL(node = stmt_node->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("nullptr", K(cmd), K(ret));
  } else {
    LOG_INFO("read command", K(cmd));
    if (SET_LEAK_MOD == node->value_) {
      char str[lib::AOBJECT_LABEL_SIZE + 1];
      snprintf(str, sizeof(str), "%.*s",
               (int32_t)node->children_[0]->str_len_, node->children_[0]->str_value_);
      reset_mem_leak_checker_label(str);
    } else if (SET_LEAK_RATE == node->value_) {
      reset_mem_leak_checker_rate(node->children_[0]->value_);
    } else if (MEMORY_LEAK == node->value_) {
      dump_memory_leak();
    } else {
      ObMemoryDumpTask *task = mem_dump.alloc_task();
      if (OB_ISNULL(task)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc task failed", K(ret));
      } else {
        task->type_ = node->value_ <= 1 ? DUMP_CONTEXT : DUMP_CHUNK;
        task->dump_all_ = 0 == node->value_ || 2 == node->value_;
        char atoi_buf[32];
        if (CONTEXT_ALL == node->value_) {
          // do-nothing
        } else if (CONTEXT == node->value_) {
          snprintf(atoi_buf, sizeof(atoi_buf), "%.*s",
                   (int32_t)node->children_[0]->str_len_, node->children_[0]->str_value_);
          task->p_context_ = (void*)std::stoll(atoi_buf, nullptr, 0);
          task->slot_idx_ = node->children_[1]->value_;
        } else if (CHUNK_ALL == node->value_) {
          // do-nothing
        } else if (CHUNK_OF_TENANT_CTX == node->value_) {
          task->dump_tenant_ctx_ = true;
          task->tenant_id_ = node->children_[0]->value_;
          uint64_t ctx_id = 0;
          if (!get_global_ctx_info().is_valid_ctx_name(node->children_[1]->str_value_, ctx_id)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid ctx", K(node->children_[1]->str_value_));
          } else {
            task->ctx_id_ = ctx_id;
          }
        } else if (CHUNK == node->value_) {
          snprintf(atoi_buf, sizeof(atoi_buf), "%.*s",
                   (int32_t)node->children_[0]->str_len_, node->children_[0]->str_value_);
          task->p_chunk_ = (void*)std::stoll(atoi_buf, nullptr, 0);
        }
        LOG_INFO("task info", K(*task));
        if (OB_FAIL(mem_dump.push(task))) {
          LOG_WARN("push task failed", K(ret));
          mem_dump.free_task(task);
        }
      }
    }
  }
  return ret;
}

int ObDumpTaskGenerator::generate_mod_stat_task()
{
  int ret = OB_SUCCESS;
  auto &mem_dump = ObMemoryDump::get_instance();
  ObMemoryDumpTask *task = mem_dump.alloc_task();
  if (OB_ISNULL(task)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc task failed");
  } else {
    task->type_ = STAT_LABEL;
    COMMON_LOG(INFO, "task info", K(*task));
    if (OB_FAIL(mem_dump.push(task))) {
      LOG_WARN("push task failed", K(ret));
      mem_dump.free_task(task);
    }
  }
  return ret;
}

void ObDumpTaskGenerator::dump_memory_leak()
{
  int ret = OB_SUCCESS;
  int fd = -1;
  if (-1 == (fd = ::open(ObMemoryDump::LOG_FILE,
                         O_CREAT | O_WRONLY | O_APPEND, S_IRUSR | S_IWUSR))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("create new file failed", K(strerror(errno)));
  } else {
    ObMemAttr attr(common::OB_SERVER_TENANT_ID, "dumpLeak", ObCtxIds::DEFAULT_CTX_ID,
                   lib::OB_HIGH_ALLOC);
    const int buf_len = 1L << 20;
    char *buf = (char*)ob_malloc(buf_len, attr);
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc failed", K(ret));
    } else {
      common::ObMemLeakChecker::mod_info_map_t tmp_map;
      if (OB_FAIL(tmp_map.create(1024))) {
        LOG_WARN("create map failed", K(ret));
      } else if (OB_FAIL(get_mem_leak_checker().load_leak_info_map(tmp_map))) {
        LOG_WARN("load map failed", K(ret));
      } else {
        int64_t pos = 0;
        pos += snprintf(buf + pos, buf_len - pos,
                        "\n######## LEAK_CHECKER (origin_str = %s, label_ = %s, check_type = %d, "
                        "current_ts = %ld)########\n",
                        get_mem_leak_checker().get_str(),
                        get_mem_leak_checker().get_label(),
                        get_mem_leak_checker().get_check_type(),
                        ObTimeUtility::current_time());
        for (auto it = tmp_map->begin(); it != tmp_map->end(); ++it) {
          pos += snprintf(buf + pos, buf_len - pos, "bt=%s, count=%ld, bytes=%ld\n",
              it->first.bt_, it->second.first, it->second.second);
          if (pos > buf_len / 2) {
            ::write(fd, buf, pos);
            pos = 0;
          }
        }
        if (pos > 0) {
          ::write(fd, buf, pos);
          pos = 0;
        }
      }
    }
    if (buf != nullptr) {
      ob_free(buf);
    }
  }
  if (fd >= 0) {
    ::close(fd);
  }
}

}
}
