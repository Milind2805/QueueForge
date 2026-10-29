cmake_minimum_required(VERSION 3.16)
project(queueforge LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(Threads REQUIRED)

add_library(queueforge INTERFACE)
target_include_directories(queueforge INTERFACE include)
target_link_libraries(queueforge INTERFACE Threads::Threads)

add_executable(demo src/main.cpp)
target_link_libraries(demo PRIVATE queueforge)

add_executable(broker src/broker.cpp)
target_link_libraries(broker PRIVATE queueforge)

add_executable(worker src/worker.cpp)
target_link_libraries(worker PRIVATE queueforge)

# Tests 
include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

enable_testing()
add_executable(queue_tests tests/test_queue.cpp)
target_link_libraries(queue_tests PRIVATE queueforge GTest::gtest_main)
include(GoogleTest)
gtest_discover_tests(queue_tests)
