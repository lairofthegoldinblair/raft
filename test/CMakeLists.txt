add_executable(raft-unit-test test.cc)

add_executable(raft-protocol-operation-test protocol_operation_test.cc)

add_executable(raft-asio-unit-test asio_test.cc)

add_executable(raft-linearizability-unit-test linearizability_test.cc)

add_executable(raft-circular-buffer-unit-test circular_buffer_test.cc)

target_link_libraries( raft-unit-test  ${Boost_FILESYSTEM_LIBRARY} ${Boost_LOG_LIBRARY}
${Boost_SYSTEM_LIBRARY} ${Boost_THREAD_LIBRARY} ${LIB_PTHREAD} ${Boost_UNIT_TEST_FRAMEWORK_LIBRARY} )

target_link_libraries( raft-protocol-operation-test  ${Boost_FILESYSTEM_LIBRARY} ${Boost_LOG_LIBRARY}
${Boost_SYSTEM_LIBRARY} ${Boost_THREAD_LIBRARY} ${LIB_PTHREAD} ${Boost_UNIT_TEST_FRAMEWORK_LIBRARY} )

target_link_libraries( raft-asio-unit-test  ${Boost_FILESYSTEM_LIBRARY} ${Boost_LOG_LIBRARY}
${Boost_SYSTEM_LIBRARY} ${Boost_THREAD_LIBRARY} ${LIB_PTHREAD} ${Boost_UNIT_TEST_FRAMEWORK_LIBRARY} )

target_link_libraries( raft-linearizability-unit-test  ${Boost_FILESYSTEM_LIBRARY} ${Boost_LOG_LIBRARY}
${Boost_SYSTEM_LIBRARY} ${Boost_THREAD_LIBRARY} ${LIB_PTHREAD} ${Boost_UNIT_TEST_FRAMEWORK_LIBRARY} )

target_link_libraries( raft-circular-buffer-unit-test  ${Boost_FILESYSTEM_LIBRARY} ${Boost_LOG_LIBRARY}
${Boost_SYSTEM_LIBRARY} ${Boost_THREAD_LIBRARY} ${LIB_PTHREAD} ${Boost_UNIT_TEST_FRAMEWORK_LIBRARY} )

