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

# Hive Metastore Thrift Interface Documentation

## Overview
This repository contains the Thrift interface definition files for Apache Hive Metastore, specifically tailored for C++ code generation.

## File Origin
The `hive_metastore.thrift` file in this directory is **copied verbatim** from the official Apache Hive 4.0.1 release.
Source reference: [Apache Hive GitHub Repository](https://github.com/apache/hive/tags/rel/release-4.0.1)

## Required Installation

### Prerequisites
- Apache Thrift compiler (version 0.13.0 or later recommended)
- C++ development tools (gcc/g++, make, cmake) (!!! Will use observer development tools)
- Boost libraries (for C++ Thrift runtime)

### System Dependencies
For Ubuntu/Debian:
```bash
sudo apt-get update
sudo apt-get install build-essential cmake libboost-all-dev libssl-dev libevent-dev
```

For CentOS/RHEL:
```bash
sudo yum groupinstall "Development Tools"
sudo yum install cmake boost-devel openssl-devel libevent-devel
```

## Code Generation Usage
To generate C++ client code from the Thrift definition:
```bash
cd src/share/catalog/hive/thrift
${THRIFT_HOME}/bin/thrift -I ${THRIFT_HOME} -strict --gen cpp -out ./gen_cpp share/fb303/if/fb303.thrift
${THRIFT_HOME}/bin/thrift -I ${THRIFT_HOME} -strict --gen cpp -out ./gen_cpp hive_metastore.thrift
```

The generated code will be output to a gen_cpp directory. Ensure you have:

1. Proper include paths configured for Thrift dependencies.
2. Required permissions to write to the target directory.

## How To
### How to install thrift?

#### Method : Build from Source (Recommended)

1. **Download Thrift source:**
```bash
# wget the thrift from official website
tar -xzf thrift-0.16.0.tar.gz
cd thrift-0.16.0
```

2. **Configure and build:**

>  Note: some dev machine without the compatible boost version, then should download and set the --with-boost configure.

```bash
./configure --prefix=/usr/local --enable-static --with-c_glib=yes  --with-cpp=yes  --without-erlang --without-nodejs --without-python --without-py3 --without-perl --without-php --without-php_extension --without-ruby --without-haskell --without-go --without-swift --without-dotnetcore --without-qt5 --enable-tutorial=no --enable-tests=no  CFLAGS="-g -O2 -fPIC" CXXFLAGS="-g -O2 -fPIC" --with-boost=/PATH/TO/BOOST
make -j$(nproc)
sudo make install
```

3. **Verify installation:**
```bash
thrift --version
```


### Environment Setup

After installation, set the `THRIFT_HOME` environment variable:

```bash
export THRIFT_HOME=/usr/local  # or wherever thrift is installed
export PATH=$THRIFT_HOME/bin:$PATH
```

Add to your shell profile (`.bashrc`, `.zshrc`, etc.) for persistence.

### Generated Files Structure

After running the code generation commands, you'll get:

```
gen_cpp/
├── hive_metastore_constants.cpp
├── hive_metastore_constants.h
├── hive_metastore_types.cpp
├── hive_metastore_types.h
├── hive_metastore.cpp
├── hive_metastore.h
├── fb303_constants.cpp
├── fb303_constants.h
├── fb303_types.cpp
├── fb303_types.h
├── fb303.cpp
└── fb303.h
```

### Troubleshooting

**Common Issues:**

1. **"thrift: command not found"**
   - Ensure Thrift is installed and in your PATH
   - Check `which thrift` and `thrift --version`

2. **"Cannot find Thrift headers"**
   - Verify `THRIFT_HOME` is set correctly
   - Check include paths in your build configuration

3. **Compilation errors with generated code**
   - Ensure you're using compatible Thrift and Boost versions
   - Check that all required dependencies are installed

4. **STD library may cause performance issue in OBSERVER**
   - Can traverse thirft repo to fbthrift repo.
   - Should test and valid the performance issue.

**Version Compatibility:**
- Hive 4.0.1 thrift files work best with Thrift 0.13.0+
- For older Hive versions, use corresponding Thrift versions
