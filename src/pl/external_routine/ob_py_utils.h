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

#ifndef OCEANBASE_SRC_PL_EXTERNAL_ROUTINE_OB_PYTHON_UTILS_H_
#define OCEANBASE_SRC_PL_EXTERNAL_ROUTINE_OB_PYTHON_UTILS_H_

#include <Python.h>

#include "lib/string/ob_string.h"
#include "pl/external_routine/cpython/ob_py_helper.h"
#include "lib/hash/ob_hashmap.h"

namespace oceanbase
{
namespace common
{
class ObDatum;
class ObObj;
}

namespace pl
{

typedef PyObject ObPyObject;
typedef PyThreadState ObPyThreadState;
using ModuleMap = common::hash::ObHashMap<ObString, ObPyObject*>;

class ObPySubInterContext
{
public:
  ObPySubInterContext() : is_inited_(false), acquire_times_(0), release_times_(0), sub_tstate_(nullptr) {}
  virtual ~ObPySubInterContext();
  bool is_valid() {
    return is_inited_ && sub_tstate_ != nullptr;
  }

  int init();
  inline ObPyThreadState *get_sub_tstate() { return sub_tstate_; }
  inline uint64_t get_acquire_times() { return acquire_times_; }
  inline uint64_t get_release_times() { return release_times_; }
  ObPyThreadState *acquire_sub_inter();
  void release_sub_inter();
  ModuleMap &get_module_map() { return module_map_; }
private:
  inline void inc_acquire_times() { ++acquire_times_; }
  inline void inc_release_times() { ++release_times_; }
  int import_module(const ObString &module_name, ObPyObject *&py_module);
  static int create_odps_module();
private:
  bool is_inited_;
  uint64_t acquire_times_;
  uint64_t release_times_;

  ObPyThreadState *sub_tstate_;
  ModuleMap module_map_;
};

class ObPyUtils
{
public:
  static int load_routine_py(ObPyThreadState *tstate,
                             const ObString &py_source,
                             const int64_t udf_id,
                             const ObString &func_name,
                             ObPyObject *&py_func);
  static void xdec_ref(ObPyObject *pyo) {
    if (pyo != nullptr) {
      ob_py_dec_ref(pyo);
    }
  };
  static ObPyObject *ob_py_none() {
    return ob_py_none_ptr;
  }
  static int exception_check();

  static int ob_to_py_type(ObPySubInterContext &ctx,
                           const common::ObObj &obj,
                           ObPyObject *&py_obj);
  static int ob_from_py_type(ObPySubInterContext &ctx,
                             ObIAllocator &allocator,
                             ObPyObject *py_obj,
                             common::ObObj &obj,
                             const ObObjMeta &meta);
  static void ob_py_end_sub_inter(void *py_sub_inter_ctx);
  static int verify_py_valid();
  static int is_dest_type(ObPySubInterContext &ctx,
                          ObPyObject *py_obj,
                          bool &is_dest,
                          const ObString &module_name,
                          const ObString &type_name);
  static int decimal_from_py(ObIAllocator &allocator,
                             common::ObObj &obj,
                             ObPyObject *py_obj);
  static int datetime_from_py(ObIAllocator &allocator,
                              common::ObObj &obj,
                              ObPyObject *py_obj);
  static int date_from_py(ObIAllocator &allocator,
                          common::ObObj &obj,
                          ObPyObject *py_obj);
  static const ObString builtins_module_name;
  static const ObString decimal_module_name;
  static const ObString datetime_module_name;
  static const ObString decimal_type_name;
  static const ObString datetime_type_name;
  static const ObString date_type_name;
};

} // namespace pl

} // namespace oceanbase

#endif // OCEANBASE_SRC_PL_EXTERNAL_ROUTINE_OB_PYTHON_UTILS_H_
