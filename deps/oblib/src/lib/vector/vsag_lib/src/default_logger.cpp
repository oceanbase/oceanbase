

// Copyright 2024-present the vsag project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "./default_logger.h"

namespace vsag {

void
DefaultLogger::SetLevel(Logger::Level log_level) {
    spdlog::set_level((spdlog::level::level_enum)log_level);
}

void
DefaultLogger::Trace(const std::string& msg) {
    spdlog::trace(msg);
}

void
DefaultLogger::Debug(const std::string& msg) {
    spdlog::debug(msg);
}

void
DefaultLogger::Info(const std::string& msg) {
    spdlog::info(msg);
}

void
DefaultLogger::Warn(const std::string& msg) {
    spdlog::warn(msg);
}

void
DefaultLogger::Error(const std::string& msg) {
    spdlog::error(msg);
}

void
DefaultLogger::Critical(const std::string& msg) {
    spdlog::critical(msg);
}

}  // namespace vsag
