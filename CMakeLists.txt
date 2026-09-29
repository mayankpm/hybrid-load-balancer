cmake_minimum_required(VERSION 3.20)
project(hybrid_load_balancer LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

option(HLB_SANITIZE "Build with a sanitizer: thread, address or undefined" "")

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  add_compile_options(-Wall -Wextra -Wpedantic)
  if(HLB_SANITIZE)
    add_compile_options(-fsanitize=${HLB_SANITIZE} -fno-omit-frame-pointer -g)
    add_link_options(-fsanitize=${HLB_SANITIZE})
  endif()
endif()

# Link the runtime statically on MinGW so the binaries run without the
# toolchain's DLLs on PATH.
if(MINGW)
  add_link_options(-static)
endif()

find_package(Threads REQUIRED)

add_library(hlbcore STATIC
  src/common/log.cpp
  src/common/platform.cpp
  src/net/socket.cpp
  src/net/poller.cpp
  src/net/event_loop.cpp
  src/net/tcp_conn.cpp
  src/http/http.cpp
  src/lb/metrics.cpp
  src/lb/backend.cpp
  src/lb/health_checker.cpp
  src/lb/config.cpp
  src/lb/worker.cpp
  src/lb/tcp_session.cpp
  src/lb/http_session.cpp
  src/lb/load_balancer.cpp
)
target_include_directories(hlbcore PUBLIC src)
target_link_libraries(hlbcore PUBLIC Threads::Threads)
if(WIN32)
  target_link_libraries(hlbcore PUBLIC ws2_32 winmm)
endif()

# Test backends and client, shared by tests, the benchmark and the demo backend.
add_library(hlbtestkit STATIC src/testkit/servers.cpp)
target_link_libraries(hlbtestkit PUBLIC hlbcore)

add_executable(hybridlb apps/hybridlb.cpp)
target_link_libraries(hybridlb PRIVATE hlbcore)

add_executable(demo_backend apps/demo_backend.cpp)
target_link_libraries(demo_backend PRIVATE hlbtestkit)

add_executable(lbbench apps/lbbench.cpp)
target_link_libraries(lbbench PRIVATE hlbtestkit)

add_executable(lbtests
  tests/main.cpp
  tests/http_parser_test.cpp
  tests/balancer_test.cpp
  tests/config_test.cpp
  tests/l4_test.cpp
  tests/l7_test.cpp
  tests/failover_test.cpp
  tests/stress_test.cpp
)
target_link_libraries(lbtests PRIVATE hlbtestkit)

enable_testing()
foreach(suite http balancer config l4 l7 failover stress)
  add_test(NAME ${suite} COMMAND lbtests ${suite}.)
endforeach()
