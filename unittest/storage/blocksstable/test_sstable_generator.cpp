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

#include "test_sstable_generator.h"
#include "storage/ob_i_store.h"
#include "storage/blocksstable/ob_data_file.h"
namespace oceanbase
{
using namespace common;
using namespace blocksstable;
using namespace storage;
namespace unittest
{
TestSSTableGenerator::TestSSTableGenerator()
  : desc_(),
    data_file_(),
    logger_(),
    image_(),
    writer_(),
    marker_(),
    row_count_(0),
    mod_(ObModIds::TEST),
    arena_(ModuleArena::DEFAULT_PAGE_SIZE, mod_)
{
  path_[0] = 0;
}
int TestSSTableGenerator::open(
    ObDataStoreDesc &desc,
    const char *path,
    const int64_t row_count)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = desc_.assign(desc))) {
    STORAGE_LOG(WARN, "desc fail to assign.", K(ret));
  } else {
    row_count_ = row_count;
    memcpy(path_, path, strlen(path));
    char cmd[1024];
    sprintf(cmd, "rm -rf %s", path_);
    system(cmd);
    sprintf(cmd, "mkdir -p %s", path_);
    system(cmd);
    ObBlockFile::FileLocation location;
    location.disk_no_ = 1;
    location.install_sequence_ = 0;
    char file_name[1024];
    sprintf(file_name, "%s/test.sst", path_);
    memcpy(location.path_, file_name, strlen(file_name) + 1);

    ObMacroDataSeq start_seq(0);
    ObBlockFile::FileSpec spec;
    spec.data_file_size_ = 64 * 1024 * 1024;
    spec.macro_block_size_ = 2 * 1024 * 1024;

    if (OB_SUCCESS != (ret = data_file_.format(location, spec))) {
      STORAGE_LOG(WARN, "data file fail to format.", K(ret));
    } else if (OB_SUCCESS != (ret = data_file_.open(location, &image_))) {
      STORAGE_LOG(WARN, "data file fail to open.", K(ret));
    } else if (OB_SUCCESS != (ret = image_.initialize(&data_file_, &logger_))) {
      STORAGE_LOG(WARN, "image fail to initialize.", K(ret));
    } else if (OB_SUCCESS != (ret = logger_.init(data_file_, path, 2 * 1024 * 1024))) {
      STORAGE_LOG(WARN, "logger fail to init.", K(ret));
    //} else if (OB_SUCCESS != (ret = logger_.replay())) {
    //  STORAGE_LOG(WARN, "logger fail to replay.", K(ret));
    } else if (OB_SUCCESS != (ret = marker_.initialize(&data_file_, &image_))) {
      STORAGE_LOG(WARN, "marker fail to initialize.", K(ret));
    } else if (OB_SUCCESS != (ret = marker_.register_storage_meta(&image_))) {
      STORAGE_LOG(WARN, "marker failt to register.", K(ret));
    } else if (OB_SUCCESS != (ret = marker_.mark_init())) {
      STORAGE_LOG(WARN, "marker fail to mark init.", K(ret));
    } else if (OB_SUCCESS != (ret = writer_.open(&data_file_, desc_, start_seq))) {
      STORAGE_LOG(WARN, "macro block writer fail to open.", K(ret));
    }
  }
  return ret;
}

int TestSSTableGenerator::generate()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < row_count_; ++ i) {
    if (OB_SUCCESS != (ret = generate_row(i))) {
      STORAGE_LOG(WARN, "sstable generator fail to generate row.", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_SUCCESS != (ret = writer_.close())) {
      STORAGE_LOG(WARN, "sstable generator fail to close.", K(ret));
    }
  }
  return ret;
}

int TestSSTableGenerator::close()
{
  int ret = OB_SUCCESS;
  char cmd[1024];
  sprintf(cmd, "rm -rf %s", path_);
  system(cmd);
  return ret;
}

int TestSSTableGenerator::generate_row(const int64_t index)
{
  int ret = OB_SUCCESS;
  arena_.reuse();
  for (int64_t i = 0; OB_SUCC(ret) && i < desc_.column_cnt_; ++ i) {
    switch (desc_.col_desc_array_.at(i).col_type_.get_type()) {
      case ObIntType: {
          cells_[i].set_int(index);
          break;
        }
      case ObVarcharType: {
          char *buf = arena_.alloc(1024);
          sprintf(buf, "%ld", index);
          ObString str(0, static_cast<int32_t>(strlen(buf)), buf);
          cells_[i].set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
          cells_[i].set_varchar(str);
          break;
        }
      default:
        STORAGE_LOG(WARN, "not supported");
        ret = OB_NOT_SUPPORTED;
    }
  }

  if (OB_SUCC(ret)) {
    ObStoreRow row;
    row.row_val_.cells_ = cells_;
    row.row_val_.count_ = desc_.column_cnt_;
    row.flag_.set_flag(ObDmlFlag::DF_INSERT);
    if (OB_SUCCESS != (ret = writer_.append_row(row))) {
      STORAGE_LOG(WARN, "macro block writer fail to append row.", K(ret));
    }
  }
  return ret;
}
}//end namespace unittest
}//end namespace oceanbase
