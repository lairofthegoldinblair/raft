add_executable(raft-unit-test test.cc)

target_link_libraries( raft-unit-test raft ${Boost_UNIT_TEST_FRAMEWORK_LIBRARY} )


