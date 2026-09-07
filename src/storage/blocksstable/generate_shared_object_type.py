#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright 2025 - 2025 Alibaba Inc. All Rights Reserved.
# Author:
#  shell> python3 generate_shared_object_type.py
# author: zhaomiao
import os
import sys
import re
import glob

# global variables
copyright = '''/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */'''

# store all configed object types
all_storage_object_types = []
storage_object_type_strs = []

# Base class function signatures for validation
BASE_FUNCTION_SIGNATURES = {
    'is_valid': 'bool is_valid(const MacroBlockId &file_id) const',
    'opt_to_string': 'int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const',
    'get_object_id': 'int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const',
}

def extract_function_signature(code_str):
    """Extract function signature from complete function definition"""
    if not code_str or code_str == 'OB_NOT_SUPPORTED':
        return None

    # Remove leading/trailing whitespace
    code_str = code_str.strip()

    # Find the first '{' to identify the end of function signature
    brace_pos = code_str.find('{')
    if brace_pos == -1:
        return None

    # Extract the signature part (everything before the first '{')
    signature = code_str[:brace_pos].strip()

    # Remove any leading 'virtual' keyword if present
    if signature.startswith('virtual '):
        signature = signature[8:].strip()

    return signature

def validate_function_signature(config_signature, base_signature, function_name, obj_type):
    """Validate that config function signature matches base class signature"""
    if not config_signature:
        return True  # Skip validation if no signature found

    # Normalize signatures by removing extra spaces
    config_norm = re.sub(r'\s+', ' ', config_signature.strip())
    base_norm = re.sub(r'\s+', ' ', base_signature.strip())

    if config_norm != base_norm:
        print(f"ERROR: Function signature mismatch for {function_name} in {obj_type}")
        print(f"  Expected: {base_signature}")
        print(f"  Found:    {config_signature}")
        return False

    return True

def validate_config_functions():
    """Validate all function signatures in config against base class"""
    global all_storage_object_types
    print("Validating function signatures...")
    all_valid = True

    for cfg in all_storage_object_types:
        obj_type = cfg.get('obj_type')
        for func_name, base_signature in BASE_FUNCTION_SIGNATURES.items():
            if func_name in cfg and cfg[func_name] != 'OB_NOT_SUPPORTED':
                config_signature = extract_function_signature(cfg[func_name])
                if not validate_function_signature(config_signature, base_signature, func_name, obj_type):
                    all_valid = False
    if all_valid:
        print("✅ All function signatures are valid!")
    else:
        print("❌ Function signature validation failed!")
        sys.exit(1)

    return all_valid

def validate_write_strategies():
    """Validate all write_strategy configurations"""
    global all_storage_object_types
    print("Validating write strategies...")
    all_valid = True

    for cfg in all_storage_object_types:
        obj_type = cfg.get('obj_type')
        if cfg.get('write_strategy'):
            try:
                strategy_mask = convert_write_strategy_to_mask(cfg['write_strategy'])
                print(f"  ✅ {obj_type}: write_strategy = {cfg['write_strategy']} => 0x{strategy_mask:02x}")
            except ValueError as e:
                print(f"  ❌ {obj_type}: {e}")
                all_valid = False

    if all_valid:
        print("✅ All write strategies are valid!")
    else:
        print("❌ Write strategy validation failed!")
        sys.exit(1)

    return all_valid

def convert_write_strategy_to_mask(write_strategy):
    """
    Convert write strategy list to uint8_t bitmask

    Strategy to bit mapping:
    - WRITE_THROUGH: bit 0 (0x01)
    - WRITE_BACK: bit 1 (0x02)
    - WRITE_THROUGH_AND_TRY_WRITE_LCACHE: bit 2 (0x04)

    Examples:
    - ["WRITE_THROUGH"] => 0x01
    - ["WRITE_THROUGH", "WRITE_THROUGH_AND_TRY_WRITE_LCACHE"] => 0x05
    - ["WRITE_THROUGH", "WRITE_BACK", "WRITE_THROUGH_AND_TRY_WRITE_LCACHE"] => 0x07

    Raises:
        ValueError: if strategy is not one of WRITE_THROUGH, WRITE_BACK, WRITE_THROUGH_AND_TRY_WRITE_LCACHE
    """
    VALID_STRATEGIES = {"WRITE_THROUGH", "WRITE_BACK", "WRITE_THROUGH_AND_TRY_WRITE_LCACHE"}

    mask = 0
    if not write_strategy:
        return mask

    # Note: bit mask here is corresponding to ObStorageObjectWriteStrategy enum class
    for strategy in write_strategy:
        if strategy == "WRITE_THROUGH":
            mask |= 0x01  # bit 0
        elif strategy == "WRITE_BACK":
            mask |= 0x02  # bit 1
        elif strategy == "WRITE_THROUGH_AND_TRY_WRITE_LCACHE":
            mask |= 0x04  # bit 2
        else:
            raise ValueError(f"Invalid write strategy: '{strategy}'. Must be one of: {VALID_STRATEGIES}")

    return mask

def extract_function_body(code_str):
    """Extract function body from complete function definition"""
    if not code_str or code_str == 'OB_NOT_SUPPORTED':
        return code_str

    # Remove leading/trailing whitespace
    code_str = code_str.strip()

    # If the code starts with '{', it's already just the function body
    if code_str.startswith('{'):
        return code_str

    # If it contains a function signature, extract the body
    # Look for the first '{' and return everything from there
    brace_pos = code_str.find('{')
    if brace_pos != -1:
        return code_str[brace_pos:]

    # If no '{' found, return as is (might be just a return statement)
    return code_str

def def_storage_object_type_cfg(**keywords):
    """process storage object type config"""
    global all_storage_object_types, storage_object_type_strs

    obj_type = keywords.get('obj_type', 'UNKNOWN')
    if obj_type != 'none':
        all_storage_object_types[obj_type] = keywords
        storage_object_type_strs.append(obj_type)

def start_generate_h(h_file_name):
    """start to generate header file"""
    global h_f
    h_f = open(h_file_name, 'w')
    head = copyright + '''

#ifndef OCEANBASE_BLOCKSSTABLE_OB_STORAGE_OBJECT_TYPE_H_
#define OCEANBASE_BLOCKSSTABLE_OB_STORAGE_OBJECT_TYPE_H_

#include "common/storage/ob_device_common.h"

namespace oceanbase
{
namespace blocksstable
{
class MacroBlockId;
class ObStorageObjectOpt;

#define STI(object_type) (ObStorageObjectTypeInstance::get_instance(object_type))

// Generated enum class
enum class ObStorageObjectType : uint8_t
{
'''
    h_f.write(head)

def end_generate_h():
    """end to generate header file"""
    global h_f
    end = '''
};
static constexpr uint8_t SS_OBJECT_MAX_TYPE_VAL = static_cast<uint8_t>(ObStorageObjectType::MAX);
const char *get_storage_objet_type_str(const ObStorageObjectType type);

class ObStorageObjectTypeBase
{
public:
  ObStorageObjectTypeBase() : type_(ObStorageObjectType::MAX) {}
  ObStorageObjectTypeBase(ObStorageObjectType type) : type_(type) {}
  virtual ~ObStorageObjectTypeBase() {}

  ObStorageObjectType get_type() const { return type_; }
  const char *get_type_str() const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  //the ObjectType is macro type, true or false
  bool is_macro() const { return is_macro_data() || is_macro_meta(); }
  bool has_write_back_strategy() const;
  bool has_write_through_and_try_write_lcache_strategy() const;
  // the ObjectType is macro data type, true or false
  virtual bool is_macro_data() const { return false; }
  // the ObjectType is tenant data type, true or false
  virtual bool is_tenant_data() const { return false; }
  // the ObjectType is macro meta type, true or false
  virtual bool is_macro_meta() const { return false; }
  //the ObjectType is tablet meta type, true or false
  virtual bool is_tablet_meta() const { return false; }
  //the ObjectType is tenant meta type, true or false
  virtual bool is_tenant_meta() const { return false; }
  //the ObjectType is private type, true or false
  virtual bool is_private() const { return false; }
  //the ObjectType is shared type, true or false
  virtual bool is_shared() const { return false; }
  //the ObjectType only store in local cache, true or false
  virtual bool is_pin_local() const { return false; }
  //whether need fsync when write
  virtual bool need_fsync() const { return true; }
  //the ObjectType whether use reserved disk space
  virtual bool use_reserved_disk_space() const { return false; }
  //whether can append write
  virtual bool can_append_write() const { return false; }
  //whether use fd cache when reading local cache file of this type, true or false
  virtual bool is_support_fd_cache() const { return false; }
  //whether path include inner tablet
  virtual bool is_path_include_inner_tablet() const { return false; }
  //the ObjectType only store in remote object storage, true or false
  virtual bool is_direct_read() const { return false; }
  // write strategy of this type object, including WRITE_THROUGH, WRITE_BACK and WRITE_THROUGH_AND_TRY_WRITE_LCACHE
  virtual uint8_t write_strategy() const { return 0; }
  //whether this type of object exists overwrite with 'different content', true or false
  virtual bool is_overwrite() const { return false; }
  // whether this type of object can read out of bounds
  virtual bool is_read_out_of_bounds() const { return true; }
  //the ObjectType is major type, true or false
  virtual bool is_major() const { return false; }
  //the ObjectType is mds type, true or false
  virtual bool is_mds() const { return false; }
  //the ObjectType is tmp type, true or false
  virtual bool is_tmp_file() const { return false; }
  //whether this type of object support sn mode, true or false
  virtual bool is_support_sn() const { return false; }
  //the ObjectType which 500 tenant can write
  virtual bool server_tenant_can_have() const { return false; }
  // the ObjectType is store in table, true or false
  virtual bool is_store_in_table() const { return false; }
  // check macro block id valid
  virtual bool is_valid(const MacroBlockId &file_id) const { return false; }
  virtual bool has_effective_tablet_id() const { return false; }
  virtual bool is_shared_tablet_sub_meta() const { return is_shared() && is_tablet_meta() && !is_store_in_table(); }
  virtual bool is_shared_tablet_sub_meta_in_table() const { return is_shared() && is_tablet_meta() && is_store_in_table(); }
  virtual int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const { return OB_SUCCESS; }
  virtual int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const { return OB_SUCCESS; }
  void set_ss_object_first_id_(const uint64_t incarnation_id, const uint64_t column_group_id, MacroBlockId &object_id) const;

protected:
  ObStorageObjectType type_;
};

class ObStorageObjectTypeInstance
{
public:
  static const ObStorageObjectTypeBase &get_instance(ObStorageObjectType type);
};

'''
    h_f.write(end)

def start_generate_cpp(cpp_file_name):
    """start to generate source file"""
    global cpp_f
    cpp_f = open(cpp_file_name, 'w')
    head = copyright + '''
#define USING_LOG_PREFIX STORAGE
#include "ob_storage_object_type.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/meta_store/ob_tenant_storage_meta_service.h"

namespace oceanbase
{
namespace blocksstable
{

// Generated string array
static const char *ob_storage_object_type_strs[] = {
'''
    cpp_f.write(head)

def end_generate_cpp():
    """end to generate source file"""
    global cpp_f
    end = '''
};


const char *get_storage_objet_type_str(const ObStorageObjectType type)
{
  return STI(type).get_type_str();
}

/**
 * ---------------------------------------ObStorageObjectTypeBase----------------------------------------
 */
const char *ObStorageObjectTypeBase::get_type_str() const
{
  STATIC_ASSERT(ARRAYSIZEOF(ob_storage_object_type_strs) == (int64_t)ObStorageObjectType::MAX + 1,
                "ob_storage_object_type string array size mismatch enum ObStorageObjectType count");
  const char *str = NULL;
    if (type_ < ObStorageObjectType::MAX) {
    str = ob_storage_object_type_strs[static_cast<int64_t>(type_)];
  } else {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "invalid ObStorageObjectType", K_(type));
  }
  return str;
}

int64_t ObStorageObjectTypeBase::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(type), "type_str", get_type_str());
  J_OBJ_END();
  return pos;
}


bool ObStorageObjectTypeBase::has_write_back_strategy() const
{
  return (write_strategy() & (1 << (uint8_t)ObStorageObjectWriteStrategy::WRITE_BACK));
}

bool ObStorageObjectTypeBase::has_write_through_and_try_write_lcache_strategy() const
{
  return (write_strategy() & (1 << (uint8_t)ObStorageObjectWriteStrategy::WRITE_THROUGH_AND_TRY_WRITE_LCACHE));
}

void ObStorageObjectTypeBase::set_ss_object_first_id_(
  const uint64_t incarnation_id, const uint64_t column_group_id, MacroBlockId &object_id) const
{
  object_id.set_version_v2();
  object_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  object_id.set_storage_object_type((uint64_t)type_);
  object_id.set_incarnation_id(incarnation_id);
  object_id.set_column_group_id(column_group_id);
}

'''
    cpp_f.write(end)

def print_enum_values():
    """print enum values"""
    global h_f, all_storage_object_types
    id = 0
    used_ids = set()  # Track used ids to detect duplicates
    for cfg in all_storage_object_types:
        obj_type = cfg.get('obj_type')
        if 'id' in cfg and cfg['id'] is not None:
            id = cfg['id']
            if id in used_ids:
                print(f"❌ Duplicate id {id} found for {obj_type}!")
                sys.exit(1)
            used_ids.add(id)
            h_f.write(f'    {obj_type} = {id},\n')
        else:
            print("❌ id is not specified!")
            sys.exit(1)
    h_f.write('    MAX')

def print_string_array():
    """print string array"""
    global cpp_f, all_storage_object_types

    # Find the maximum id value to determine array size
    max_id = -1
    for cfg in all_storage_object_types:
        if 'id' in cfg and cfg['id'] is not None:
            max_id = max(max_id, cfg['id'])

    if max_id < 0:
        print("❌ No valid id found in storage object types!")
        sys.exit(1)

    # MAX enum value is max_id + 1, so array size is max_id + 2 (0 to max_id+1)
    array_size = max_id + 2

    # Initialize array with empty strings for holes
    str_array = [""] * array_size

    # Fill in the actual type strings at their id positions
    for cfg in all_storage_object_types:
        obj_type = cfg.get('obj_type')
        if 'id' in cfg and cfg['id'] is not None:
            id_val = cfg['id']
            if id_val < 0 or id_val >= array_size - 1:  # -1 because MAX is at array_size-1
                print(f"❌ Invalid id {id_val} for {obj_type}!")
                sys.exit(1)
            str_array[id_val] = obj_type

    # Set MAX at the last position
    str_array[array_size - 1] = "MAX"

    # Write the array (holes are already filled with empty strings)
    for i in range(array_size):
        if i < array_size - 1:
            cpp_f.write(f'  "{str_array[i]}",\n')
        else:
            # Last element (MAX) without trailing comma
            cpp_f.write(f'  "{str_array[i]}"')

def generate_class_name(obj_type):
    parts = obj_type.split("_")
    capitalized_parts = [part.capitalize() for part in parts]
    combined = "".join(capitalized_parts)
    class_name = f"Ob{combined}Type"
    return class_name


def generate_class_declarations():
    """generate class declaration"""
    global h_f, all_storage_object_types

    for cfg in all_storage_object_types:
        obj_type = cfg.get('obj_type')
        class_name = generate_class_name(obj_type)
        h_f.write(f"""
/**
 * ---------------------------------------{class_name}----------------------------------------
 */
class {class_name} : public ObStorageObjectTypeBase
{{
public:
  {class_name}() : ObStorageObjectTypeBase(ObStorageObjectType::{obj_type}) {{}}
  virtual ~{class_name}() {{}}
""")

        # generate virtual function declaration according to config
        if cfg.get('data_type') == 'macro_data':
            h_f.write('  virtual bool is_macro_data() const { return true; }\n')
        elif cfg.get('data_type') == 'tenant_data':
            h_f.write('  virtual bool is_tenant_data() const{ return true; }\n')
        elif cfg.get('data_type') == 'macro_meta':
            h_f.write('  virtual bool is_macro_meta() const { return true; }\n')
        elif cfg.get('data_type') == 'tablet_meta':
            h_f.write('  virtual bool is_tablet_meta() const { return true; }\n')
        elif cfg.get('data_type') == 'tenant_meta':
            h_f.write('  virtual bool is_tenant_meta() const { return true; }\n')

        if cfg.get('access_mode') == 'private':
            h_f.write('  virtual bool is_private() const { return true; }\n')
        elif cfg.get('access_mode') == 'shared':
            h_f.write('  virtual bool is_shared() const { return true; }\n')

        if cfg.get('read_odirect'):
            h_f.write('  virtual bool is_direct_read() const { return true; }\n')
        if cfg.get('write_strategy'):
            # convert strategy list to bitmask
            strategy_mask = convert_write_strategy_to_mask(cfg['write_strategy'])
            h_f.write(f'  virtual uint8_t write_strategy() const {{ return {strategy_mask}; }}\n')
        if cfg.get('is_pin_local'):
            h_f.write('  virtual bool is_pin_local() const { return true; }\n')

        if not cfg.get('need_fsync', True):
            h_f.write('  virtual bool need_fsync() const { return false; }\n')

        if cfg.get('use_reserved_disk_space'):
            h_f.write('  virtual bool use_reserved_disk_space() const { return true; }\n')

        if cfg.get('can_append_write'):
            h_f.write('  virtual bool can_append_write() const { return true; }\n')

        if cfg.get('is_support_fd_cache'):
            h_f.write('  virtual bool is_support_fd_cache() const { return true; }\n')

        if cfg.get('is_overwrite'):
            h_f.write('  virtual bool is_overwrite() const { return true; }\n')

        if not cfg.get('is_read_out_of_bounds', True):
            h_f.write('  virtual bool is_read_out_of_bounds() const { return false; }\n')

        if cfg.get('is_mds'):
            h_f.write('  virtual bool is_mds() const { return true; }\n')

        if cfg.get('is_major'):
            h_f.write('  virtual bool is_major() const { return true; }\n')

        if cfg.get('is_tmp'):
            h_f.write('  virtual bool is_tmp_file() const { return true; }\n')

        if cfg.get('is_support_sn'):
            h_f.write('  virtual bool is_support_sn() const { return true; }\n')

        if cfg.get('server_tenant_can_have'):
            h_f.write('  virtual bool server_tenant_can_have() const { return true; }\n')

        if cfg.get('is_path_include_inner_tablet'):
            h_f.write('  virtual bool is_path_include_inner_tablet() const { return true; }\n')

        if cfg.get('is_store_in_table'):
            h_f.write('  virtual bool is_store_in_table() const { return true; }\n')

        # generate virtual function declaration
        if cfg.get('is_valid') and cfg['is_valid'] != 'OB_NOT_SUPPORTED':
            h_f.write('  virtual bool is_valid(const MacroBlockId &file_id) const;\n')

        if cfg.get('has_effective_tablet_id'):
            h_f.write('  virtual bool has_effective_tablet_id() const { return true; }\n')

        if cfg.get('opt_to_string') and cfg['opt_to_string'] != 'OB_NOT_SUPPORTED':
            h_f.write('  virtual int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const;\n')

        if cfg.get('get_object_id') and cfg['get_object_id'] != 'OB_NOT_SUPPORTED':
            h_f.write('  virtual int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const;\n')

        h_f.write('};\n')

def generate_class_implementations():
    """generate class implementation"""
    global cpp_f, all_storage_object_types

    for cfg in all_storage_object_types:
        obj_type = cfg.get('obj_type')
        class_name = generate_class_name(obj_type)
        cpp_f.write(f"""
/**
 * ---------------------------------------{class_name}----------------------------------------
 */
""")
        # generate virtual function implementation
        if cfg.get('is_valid') and cfg['is_valid'] != 'OB_NOT_SUPPORTED':
            cpp_f.write(f'\nbool {class_name}::is_valid(const MacroBlockId &file_id) const\n')
            cpp_f.write(extract_function_body(cfg['is_valid']))
            cpp_f.write('\n')  # Add blank line after function

        if cfg.get('opt_to_string') and cfg['opt_to_string'] != 'OB_NOT_SUPPORTED':
            cpp_f.write(f'\nint {class_name}::opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,\n')
            cpp_f.write('  const ObStorageObjectOpt &opt) const\n')
            cpp_f.write(extract_function_body(cfg['opt_to_string']))
            cpp_f.write('\n')  # Add blank line after function

        if cfg.get('get_object_id') and cfg['get_object_id'] != 'OB_NOT_SUPPORTED':
            cpp_f.write(f'\nint {class_name}::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const\n')
            cpp_f.write(extract_function_body(cfg['get_object_id']))
            cpp_f.write('\n')  # Add blank line after function
    cpp_f.write('\n')

def generate_get_instance_method():
    """generate get_instance method"""
    global cpp_f, all_storage_object_types

    cpp_f.write('\nconst ObStorageObjectTypeBase &ObStorageObjectTypeInstance::get_instance(ObStorageObjectType type)\n{\n')
    cpp_f.write('  switch (type) {\n')

    # generate static instance
    for cfg in all_storage_object_types:
        obj_type = cfg.get('obj_type')
        class_name = generate_class_name(obj_type)
        cpp_f.write(f'    case ObStorageObjectType::{obj_type}: {{\n')
        cpp_f.write(f'      static const {class_name} instance;\n')
        cpp_f.write(f'      return instance;\n    }}\n')
    cpp_f.write(f'    default: {{\n')
    cpp_f.write(f'      static const ObStorageObjectTypeBase instance;\n')
    cpp_f.write(f'      return instance;\n    }}\n')
    cpp_f.write('  }\n')
    cpp_f.write('}\n')

def clean_files(globstr):
    """clean files"""
    print(f"clean files by glob [{globstr}]")
    for f in glob.glob(os.path.join('.', globstr)):
        print(f"remove {f} ...")
        os.remove(f)

def main():
    # clean old files
    clean_files("ob_storage_object_type.h")
    clean_files("ob_storage_object_type.cpp")

    # execute config file, collect types
    exec(open("ob_storage_object_type_def.py", encoding="utf-8").read())
    # now all_storage_object_types is all types defined

    print(f"Found {len(all_storage_object_types)} storage object types:")
    for obj in all_storage_object_types:
        print(f"  - {obj.get('obj_type')}")

    if len(all_storage_object_types) == 0:
        print("Warning: No storage object types found. Please check ob_storage_object_type_def.py")
        return

    # Validate function signatures before generating code
    validate_config_functions()

    # Validate write strategy before generating code
    validate_write_strategies()

    # generate code using all_storage_object_types
    print("Generating ob_storage_object_type.h...")
    start_generate_h("ob_storage_object_type.h")
    print_enum_values()
    end_generate_h()
    generate_class_declarations()

    # add namespace end
    h_f.write('''
} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_BLOCKSSTABLE_OB_STORAGE_OBJECT_TYPE_H_
''')
    h_f.close()

    print("Generating ob_storage_object_type.cpp...")
    start_generate_cpp("ob_storage_object_type.cpp")
    print_string_array()
    end_generate_cpp()
    generate_class_implementations()
    generate_get_instance_method()

    # add namespace end
    cpp_f.write('''
} // end namespace blocksstable
} // end namespace oceanbase
''')
    cpp_f.close()

    print("Generation completed successfully!")

if __name__ == "__main__":
    main()
