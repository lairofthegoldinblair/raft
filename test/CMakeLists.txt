add_executable(raft-unit-test test.cc)

add_executable(raft-asio-unit-test asio_test.cc)

add_executable(raft-linearizability-unit-test linearizability_test.cc)

target_link_libraries( raft-unit-test  ${Boost_LOG_LIBRARY}
${Boost_SYSTEM_LIBRARY} ${Boost_THREAD_LIBRARY} ${LIB_PTHREAD} ${Boost_UNIT_TEST_FRAMEWORK_LIBRARY} )

target_link_libraries( raft-asio-unit-test  ${Boost_LOG_LIBRARY}
${Boost_SYSTEM_LIBRARY} ${Boost_THREAD_LIBRARY} ${LIB_PTHREAD} ${Boost_UNIT_TEST_FRAMEWORK_LIBRARY} )

target_link_libraries( raft-linearizability-unit-test  ${Boost_LOG_LIBRARY}
${Boost_SYSTEM_LIBRARY} ${Boost_THREAD_LIBRARY} ${LIB_PTHREAD} ${Boost_UNIT_TEST_FRAMEWORK_LIBRARY} )

