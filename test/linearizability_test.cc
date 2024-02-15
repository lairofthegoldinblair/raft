#define BOOST_TEST_MODULE LinearizabilityTests
#include "flatbuffers/raft_flatbuffer_messages.hh"
#include "flatbuffers/serialization.hh"
#include "log.hh"
#include "native/messages.hh"
#include "native/serialization.hh"
#include "state_machine/session.hh"

#include "test_utilities.hh"

#include <boost/algorithm/string/predicate.hpp>
#include <boost/mpl/list.hpp>

#include <boost/test/unit_test.hpp>

// Test types corresponding to native and flatbuffers
class native_test_type
{
public:
  typedef raft::native::messages messages_type;
  typedef raft::native::builders builders_type;
  typedef raft::native::serialization serialization_type;
};

class flatbuffers_test_type
{
public:
  typedef raft::fbs::messages messages_type;
  typedef raft::fbs::builders builders_type;
  typedef raft::fbs::serialization serialization_type;
};

// Helper for comparing results
using raft::test::string_slice_compare;

typedef boost::mpl::list<native_test_type, flatbuffers_test_type> test_types;

BOOST_AUTO_TEST_CASE_TEMPLATE(TestOpenSessionRequestSerialization, _TestType, test_types)
{
  typedef typename _TestType::messages_type::open_session_request_traits_type::arg_type open_session_request_arg_type;
  typedef typename _TestType::messages_type::open_session_request_traits_type open_session_request_traits;
  typedef typename _TestType::builders_type::open_session_request_builder_type open_session_request_builder;
  typedef typename _TestType::serialization_type serialization_type;
  auto msg = open_session_request_builder().finish();
  auto result = serialization_type::serialize(std::move(msg));
  auto msg2 = serialization_type::deserialize_open_session_request(std::move(result));  
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TestOpenSessionResponseSerialization, _TestType, test_types)
{
  typedef typename _TestType::messages_type::open_session_response_traits_type::arg_type open_session_response_arg_type;
  typedef typename _TestType::messages_type::open_session_response_traits_type open_session_response_traits;
  typedef typename _TestType::builders_type::open_session_response_builder_type open_session_response_builder;
  typedef typename _TestType::serialization_type serialization_type;
  uint64_t session_id = 92344543U;
  auto msg = open_session_response_builder().session_id(session_id).finish();
  BOOST_CHECK_EQUAL(session_id, open_session_response_traits::session_id(msg));
  auto result = serialization_type::serialize(std::move(msg));
  auto msg2 = serialization_type::deserialize_open_session_response(std::move(result));  
  BOOST_CHECK_EQUAL(session_id, open_session_response_traits::session_id(msg2));
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TestCloseSessionRequestSerialization, _TestType, test_types)
{
  typedef typename _TestType::messages_type::close_session_request_traits_type::arg_type close_session_request_arg_type;
  typedef typename _TestType::messages_type::close_session_request_traits_type close_session_request_traits;
  typedef typename _TestType::builders_type::close_session_request_builder_type close_session_request_builder;
  typedef typename _TestType::serialization_type serialization_type;
  uint64_t session_id = 92344543U;
  auto msg = close_session_request_builder().session_id(session_id).finish();
  BOOST_CHECK_EQUAL(session_id, close_session_request_traits::session_id(msg));
  auto result = serialization_type::serialize(std::move(msg));
  auto msg2 = serialization_type::deserialize_close_session_request(std::move(result));  
  BOOST_CHECK_EQUAL(session_id, close_session_request_traits::session_id(msg2));
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TestCloseSessionResponseSerialization, _TestType, test_types)
{
  typedef typename _TestType::messages_type::close_session_response_traits_type::arg_type close_session_response_arg_type;
  typedef typename _TestType::messages_type::close_session_response_traits_type close_session_response_traits;
  typedef typename _TestType::builders_type::close_session_response_builder_type close_session_response_builder;
  typedef typename _TestType::serialization_type serialization_type;
  auto msg = close_session_response_builder().finish();
  auto result = serialization_type::serialize(std::move(msg));
  auto msg2 = serialization_type::deserialize_close_session_response(std::move(result));  
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TestLinearizableCommandSerialization, _TestType, test_types)
{
  typedef typename _TestType::messages_type::linearizable_command_request_traits_type::arg_type linearizable_command_request_arg_type;
  typedef typename _TestType::messages_type::linearizable_command_request_traits_type linearizable_command_request_traits;
  typedef typename _TestType::builders_type::linearizable_command_request_builder_type linearizable_command_request_builder;
  typedef typename _TestType::serialization_type serialization_type;
  uint64_t session_id = 92344543U;
  uint64_t unack = 82348235U;
  uint64_t seq = 92377713U;
  auto msg = linearizable_command_request_builder().session_id(session_id).first_unacknowledged_sequence_number(unack).sequence_number(seq).finish();
  BOOST_CHECK_EQUAL(session_id, linearizable_command_request_traits::session_id(msg));
  BOOST_CHECK_EQUAL(unack, linearizable_command_request_traits::first_unacknowledged_sequence_number(msg));
  BOOST_CHECK_EQUAL(seq, linearizable_command_request_traits::sequence_number(msg));
  auto result = serialization_type::serialize(std::move(msg));
  auto msg2 = serialization_type::deserialize_linearizable_command_request(std::move(result));  
  BOOST_CHECK_EQUAL(session_id, linearizable_command_request_traits::session_id(msg2));
  BOOST_CHECK_EQUAL(unack, linearizable_command_request_traits::first_unacknowledged_sequence_number(msg2));
  BOOST_CHECK_EQUAL(seq, linearizable_command_request_traits::sequence_number(msg2));
}

// This protocol mock is a non-replicated log with the property that every command added is automatically and immediately "committed"
// from the point of view of the state machine and session manager
template<typename _Messages>
struct protocol_mock
{
  typedef _Messages messages_type;
  typedef typename messages_type::client_result_type client_result_type;
  typedef typename messages_type::log_entry_type log_entry_type;
  typedef typename messages_type::log_entry_traits_type log_entry_traits_type;
  typedef typename messages_type::log_entry_traits_type::const_arg_type log_entry_const_arg_type;
  typedef raft::in_memory_log<log_entry_type, log_entry_traits_type> log_type;
  typedef typename log_type::index_type log_index_type;
  typedef typename std::function<void(log_entry_const_arg_type, uint64_t, std::size_t)> response_type;

  enum state { LEADER, FOLLOWER, CANDIDATE };

  uint64_t last_applied_index_=0;
  // Last committed index is ALWAYS equal to log.index_end()
  uint64_t last_committed_index_=0;
  response_type state_machine_;
  log_type log;
  uint64_t term=1;
  uint64_t cluster_clock = 1000000;
  bool is_leader = false;
  uint64_t leader_id_ = 100;

  uint64_t leader_id() const
  {
    return leader_id_;
  }

  template<typename _Callback>
  void set_state_machine(_Callback && state_machine)
  {
    state_machine_ = std::move(state_machine);
  }  
  
  std::tuple<client_result_type, uint64_t, uint64_t> on_command(std::pair<raft::slice, raft::util::call_on_delete> && req,
                                                                std::chrono::time_point<std::chrono::steady_clock> clock_now)
  {
    if (is_leader) {
      auto indices = log.append(log_entry_traits_type::create_command(term, cluster_clock, std::move(req)));
      last_committed_index_ = log.index_end();
      return { messages_type::client_result_success(), indices.first, term };
    } else {
      return { messages_type::client_result_not_leader(), leader_id_, term};
    }
  }

  void apply_log_entries()
  {
    if (last_applied_index_ < last_committed_index_) {
      state_machine_(&log.entry(last_applied_index_), last_applied_index_, leader_id_);
    }
  }
  
  void on_command_applied(uint64_t idx)
  {
    last_applied_index_ = idx + 1;
    if (last_applied_index_ < last_committed_index_) {
      state_machine_(&log.entry(last_applied_index_), last_applied_index_, leader_id_);
    }
  }
};

template<typename _Messages>
struct communicator_mock
{
  typedef uint32_t endpoint_type;
  typedef _Messages messages_type;
  typedef typename messages_type::open_session_response_traits_type::arg_type open_session_response_arg_type;
  typedef typename messages_type::close_session_response_traits_type::arg_type close_session_response_arg_type;
  typedef typename messages_type::client_response_traits_type::arg_type client_response_arg_type;

  std::map<endpoint_type, std::vector<open_session_response_arg_type>> open_session_responses;
  std::map<endpoint_type, std::vector<close_session_response_arg_type>> close_session_responses;
  std::map<endpoint_type, std::vector<client_response_arg_type>> client_responses;

  void send_open_session_response(open_session_response_arg_type && resp, endpoint_type ep)
  {
    open_session_responses[ep].push_back(std::move(resp));
  }
  void send_close_session_response(close_session_response_arg_type && resp, endpoint_type ep)
  {
    close_session_responses[ep].push_back(std::move(resp));
  }
  void send_client_response(client_response_arg_type && resp, endpoint_type ep)
  {
    client_responses[ep].push_back(std::move(resp));
  }
};

// This is the state machine.
template<typename _Messages, typename _Serialization>
struct logger
{
  typedef _Messages messages_type;
  typedef _Serialization serialization_type;
  typedef typename messages_type::linearizable_command_request_traits_type linearizable_command_request_traits_type;
  typedef typename messages_type::log_entry_command_traits_type log_entry_command_traits_type;
  typedef typename serialization_type::log_entry_command_view_deserialization_type log_entry_command_view_deserialization_type;
  typedef raft::checkpoint_data_store<messages_type> checkpoint_data_store_type;
  typedef typename checkpoint_data_store_type::checkpoint_data_ptr checkpoint_data_ptr;

  struct continuation
  {
    log_entry_command_view_deserialization_type cmd;
    std::function<void(bool, std::pair<raft::slice, raft::util::call_on_delete> &&)> callback;

    template<typename _Callback>
    continuation(log_entry_command_view_deserialization_type && _cmd, _Callback && cb)
      :
      cmd(std::move(_cmd)),
      callback(std::move(cb))
    {
    }
  };
  std::vector<std::string> commands;
  std::unique_ptr<continuation> cont;
  std::vector<uint8_t> checkpoint_buffer_;
  enum checkpoint_restore_state { START, READ_COMMANDS_SIZE, READ_COMMAND_SIZE, READ_COMMAND };
  checkpoint_restore_state state_;
  raft::slice checkpoint_slice_;
  std::size_t checkpoint_commands_size_;
  std::size_t checkpoint_command_size_;
  
  bool async = false;
  void complete()
  {
    if (!cont) {
      return;
    }
    ;
    auto c = linearizable_command_request_traits_type::command(log_entry_command_traits_type::linearizable_command(cont->cmd.view()));
    commands.emplace_back(reinterpret_cast<const char *>(c.data()), c.size());
    // Must reset cont before the callback because the callback may add a new async command
    // from the log.   If we reset it after that then we'll lose a async call.
    auto tmp = std::move(cont);
    cont.reset();
    std::string resp = commands.back();
    raft::slice s = raft::slice::create(resp);
    tmp->callback(true, std::make_pair(std::move(s), raft::util::call_on_delete([str = std::move(resp)](){})));
  }
  template<typename _Callback>
  void on_command(log_entry_command_view_deserialization_type && cmd, _Callback && cb)
  {
    cont = std::make_unique<continuation>(std::move(cmd), std::move(cb));
    if (!async) {
      complete();
    }
  }
  void checkpoint(checkpoint_data_ptr ckpt)
  {
    boost::endian::little_uint32_t sz = commands.size();
    ckpt->write(reinterpret_cast<const uint8_t *>(&sz), sizeof(boost::endian::little_uint32_t));    
    for(auto & c : commands) {
      sz = c.size();
      ckpt->write(reinterpret_cast<const uint8_t *>(&sz), sizeof(boost::endian::little_uint32_t));
      ckpt->write(reinterpret_cast<const uint8_t *>(c.c_str()), c.size());
    }
  }
  void restore_checkpoint_block(raft::slice && s, bool is_final)
  {
    if (nullptr == s.data()) {
      state_ = START;
    }
    checkpoint_slice_ = std::move(s);

    switch(state_) {
    case START:
      commands.resize(0);
      if (1024 > checkpoint_buffer_.capacity()) {
        checkpoint_buffer_.reserve(1024);
      }
      checkpoint_buffer_.resize(0);
      while (true) {
        if (0 == checkpoint_slice_.size()) {
          state_ = READ_COMMANDS_SIZE;
          return;
        case READ_COMMANDS_SIZE:
          ;
        }
        BOOST_ASSERT(0 < checkpoint_slice_.size());
        if (0 == checkpoint_buffer_.size() && s.size() >= sizeof(boost::endian::little_uint32_t)) {
          checkpoint_commands_size_ = *reinterpret_cast<const boost::endian::little_uint32_t *>(checkpoint_slice_.data());
          checkpoint_slice_ += sizeof(boost::endian::little_uint32_t);
          break;
        } else {
          std::size_t to_insert = sizeof(boost::endian::little_uint32_t) > checkpoint_buffer_.size() ? sizeof(boost::endian::little_uint32_t) - checkpoint_buffer_.size() : 0;
          to_insert = to_insert > checkpoint_slice_.size() ? checkpoint_slice_.size() : to_insert;
          auto begin = reinterpret_cast<const uint8_t *>(checkpoint_slice_.data());
          auto end = begin + to_insert;
          checkpoint_buffer_.insert(checkpoint_buffer_.end(), begin, end);
          checkpoint_slice_ += to_insert;
          if (checkpoint_buffer_.size() >= sizeof(boost::endian::little_uint32_t)) {
            checkpoint_commands_size_ = *reinterpret_cast<const boost::endian::little_uint32_t *>(&checkpoint_buffer_[0]);
            checkpoint_buffer_.resize(0);
            break;
          }
        }
      }
      while(commands.size() < checkpoint_commands_size_) {
        while (true) {
          if (0 == checkpoint_slice_.size()) {
            state_ = READ_COMMAND_SIZE;
            return;
          case READ_COMMAND_SIZE:
            ;
          }
          BOOST_ASSERT(0 < checkpoint_slice_.size());
          if (0 == checkpoint_buffer_.size() && s.size() >= sizeof(boost::endian::little_uint32_t)) {
            checkpoint_command_size_ = *reinterpret_cast<const boost::endian::little_uint32_t *>(checkpoint_slice_.data());
            checkpoint_slice_ += sizeof(boost::endian::little_uint32_t);
            break;
          } else {
            std::size_t to_insert = sizeof(boost::endian::little_uint32_t) > checkpoint_buffer_.size() ? sizeof(boost::endian::little_uint32_t) - checkpoint_buffer_.size() : 0;
            to_insert = to_insert > checkpoint_slice_.size() ? checkpoint_slice_.size() : to_insert;
            auto begin = reinterpret_cast<const uint8_t *>(checkpoint_slice_.data());
            auto end = begin + to_insert;
            checkpoint_buffer_.insert(checkpoint_buffer_.end(), begin, end);
            checkpoint_slice_ += to_insert;
            if (checkpoint_buffer_.size() >= sizeof(boost::endian::little_uint32_t)) {
              checkpoint_command_size_ = *reinterpret_cast<const boost::endian::little_uint32_t *>(&checkpoint_buffer_[0]);
              checkpoint_buffer_.resize(0);
              break;
            }
          }
        }
        commands.push_back(std::string());
        while(commands.back().size() < checkpoint_command_size_) {
          if (0 == checkpoint_slice_.size()) {
            state_ = READ_COMMAND;
            return;
          case READ_COMMAND:
            ;
          }
          std::size_t to_append =  checkpoint_command_size_ - commands.back().size();
          to_append = to_append > checkpoint_slice_.size() ? checkpoint_slice_.size() : to_append;
          commands.back().append(reinterpret_cast<const char *>(checkpoint_slice_.data()), to_append);
          checkpoint_slice_ += to_append;
        }
        if (commands.size() == checkpoint_commands_size_ && !is_final) {
          throw std::runtime_error("INVALID CHECKPOINT");
        }
      }
    }
  }
};

template<typename _TestType>
struct SessionManagerMockTestFixture
{
  typedef typename _TestType::messages_type messages_type;
  typedef typename _TestType::messages_type::log_entry_traits_type::const_arg_type log_entry_const_arg_type;
  typedef typename _TestType::messages_type::open_session_response_traits_type open_session_response_traits;
  typedef typename _TestType::messages_type::close_session_response_traits_type close_session_response_traits;
  typedef typename _TestType::messages_type::client_response_traits_type client_response_traits;
  typedef typename _TestType::builders_type builders_type;
  typedef typename _TestType::builders_type::open_session_request_builder_type open_session_request_builder;
  typedef typename _TestType::builders_type::close_session_request_builder_type close_session_request_builder;
  typedef typename _TestType::builders_type::linearizable_command_request_builder_type linearizable_command_request_builder;
  typedef typename _TestType::serialization_type serialization_type;
  typedef protocol_mock<messages_type> protocol_type;
  typedef communicator_mock<messages_type> communicator_type;
  typedef logger<messages_type, serialization_type> state_machine_type;
  typedef typename raft::state_machine::client_session_manager<messages_type, builders_type, serialization_type, protocol_type, communicator_type, state_machine_type> session_manager_type;

  protocol_type protocol;
  communicator_type communicator;
  state_machine_type state_machine;
  session_manager_type session_manager;
  std::chrono::time_point<std::chrono::steady_clock> now;

  SessionManagerMockTestFixture()
    :
    session_manager(protocol, communicator, state_machine),
    now(std::chrono::steady_clock::now())
  {
    protocol.set_state_machine([this](log_entry_const_arg_type le, uint64_t idx, size_t leader_id) { this->session_manager.apply(le, idx, leader_id); });
  }
  
  ~SessionManagerMockTestFixture() {}

  void open_session(uint64_t endpoint, uint64_t log_index, uint64_t num_sessions_expired=0)
  {
    // This logic does not support async
    BOOST_ASSERT(!state_machine.async);
    auto open_session_base = communicator.open_session_responses.size();
    auto close_session_base = communicator.close_session_responses.size();
    auto client_response_base = communicator.client_responses.size();
    auto session_manager_base = session_manager.size();
    auto state_machine_base = state_machine.commands.size();
    auto log_base = protocol.log.index_end();
    // The following assumes that endpoint is not in open_session_responses
    BOOST_REQUIRE(communicator.open_session_responses.end() == communicator.open_session_responses.find(endpoint));
    session_manager.on_open_session(endpoint, open_session_request_builder().finish(), now);
    BOOST_CHECK_EQUAL(open_session_base, communicator.open_session_responses.size());
    BOOST_CHECK_EQUAL(close_session_base, communicator.close_session_responses.size());
    BOOST_CHECK_EQUAL(client_response_base, communicator.client_responses.size());
    BOOST_CHECK_EQUAL(log_index, protocol.last_applied_index_);
    BOOST_CHECK_EQUAL(session_manager_base, session_manager.size());
    BOOST_CHECK_EQUAL(state_machine_base, state_machine.commands.size());
    BOOST_CHECK_EQUAL(log_base+1U, protocol.log.index_end());
    BOOST_CHECK_EQUAL(log_index+1U, protocol.log.index_end());
    BOOST_TEST(protocol.last_applied_index_ < protocol.last_committed_index_);
    // Apply the generated log entry to the session manager
    protocol.apply_log_entries();
    BOOST_TEST(protocol.last_applied_index_ == protocol.last_committed_index_);
    BOOST_REQUIRE_EQUAL(open_session_base+1U, communicator.open_session_responses.size());
    BOOST_REQUIRE(communicator.open_session_responses.end() != communicator.open_session_responses.find(endpoint));
    BOOST_REQUIRE_EQUAL(1U, communicator.open_session_responses.at(endpoint).size());
    BOOST_CHECK_EQUAL(log_index, open_session_response_traits::session_id(communicator.open_session_responses.at(endpoint)[0]));
    BOOST_CHECK_EQUAL(close_session_base, communicator.close_session_responses.size());
    BOOST_CHECK_EQUAL(client_response_base, communicator.client_responses.size());
    BOOST_CHECK_EQUAL(log_base+1U, protocol.log.index_end());
    BOOST_CHECK_EQUAL(log_index+1U, protocol.log.index_end());
    BOOST_CHECK_EQUAL(session_manager_base+1U-num_sessions_expired, session_manager.size());
    BOOST_CHECK_EQUAL(state_machine_base, state_machine.commands.size());
    communicator.open_session_responses.erase(endpoint);
  }

  void send_command(uint64_t endpoint,
                    uint64_t session_id,
                    uint64_t unack,
                    uint64_t seq,
                    const std::string & cmd,
                    uint64_t log_index,
                    bool is_duplicate,
                    bool is_session_expired,
                    uint64_t num_sessions_expired)
  {
    auto is_async = state_machine.async;
    auto open_session_base = communicator.open_session_responses.size();
    auto close_session_base = communicator.close_session_responses.size();
    auto client_response_base = communicator.client_responses.size();
    auto session_manager_base = session_manager.size();
    auto state_machine_base = state_machine.commands.size();
    auto log_base = protocol.log.index_end();
    // The following assumes that endpoint is not in client_responses
    BOOST_REQUIRE(communicator.client_responses.end() == communicator.client_responses.find(endpoint));
    auto msg = linearizable_command_request_builder().session_id(session_id).first_unacknowledged_sequence_number(unack).sequence_number(seq).command(raft::slice::create(cmd)).finish();
    session_manager.on_linearizable_command(endpoint, std::move(msg), now);
    BOOST_CHECK_EQUAL(open_session_base, communicator.open_session_responses.size());
    BOOST_CHECK_EQUAL(close_session_base, communicator.close_session_responses.size());
    BOOST_CHECK_EQUAL(client_response_base, communicator.client_responses.size());
    BOOST_CHECK_EQUAL(session_manager_base, session_manager.size());
    BOOST_CHECK_EQUAL(state_machine_base, state_machine.commands.size());
    BOOST_CHECK_EQUAL(log_base+1U, protocol.log.index_end());
    BOOST_CHECK_EQUAL(log_index+1U, protocol.log.index_end());
    BOOST_TEST(protocol.last_applied_index_ < protocol.last_committed_index_);
    protocol.apply_log_entries();
    if (!is_async) {
      BOOST_TEST(protocol.last_applied_index_ == protocol.last_committed_index_);
    }
    BOOST_CHECK_EQUAL(open_session_base, communicator.open_session_responses.size());
    BOOST_CHECK_EQUAL(close_session_base, communicator.close_session_responses.size());
    BOOST_CHECK_EQUAL(log_base+1U, protocol.log.index_end());
    BOOST_CHECK_EQUAL(log_index+1U, protocol.log.index_end());
    if (!is_async || is_duplicate || is_session_expired) {
      BOOST_REQUIRE_EQUAL(client_response_base + 1U, communicator.client_responses.size());
      BOOST_REQUIRE(communicator.client_responses.end() != communicator.client_responses.find(endpoint));
      BOOST_REQUIRE_EQUAL(1U, communicator.client_responses.at(endpoint).size());
      if (is_session_expired) {
        BOOST_CHECK(messages_type::client_result_session_expired() == client_response_traits::result(communicator.client_responses.at(endpoint)[0]));
      } else {
        BOOST_CHECK(messages_type::client_result_success() == client_response_traits::result(communicator.client_responses.at(endpoint)[0]));
        BOOST_CHECK_EQUAL(log_index, client_response_traits::index(communicator.client_responses.at(endpoint)[0]));
        BOOST_CHECK_EQUAL(protocol.leader_id_, client_response_traits::leader_id(communicator.client_responses.at(endpoint)[0]));
      }
      communicator.client_responses.erase(endpoint);
    } else {
      BOOST_CHECK_EQUAL(client_response_base, communicator.client_responses.size());
    }
    BOOST_CHECK_EQUAL(session_manager_base - num_sessions_expired, session_manager.size());
    if (!is_async) {
      BOOST_REQUIRE(0<state_machine.commands.size());
    }
    if (!is_duplicate && !is_async && !is_session_expired) {
      BOOST_CHECK(boost::algorithm::equals(cmd, state_machine.commands.back()));
      BOOST_CHECK_EQUAL(state_machine_base+1U, state_machine.commands.size());
    } else {
      BOOST_CHECK_EQUAL(state_machine_base, state_machine.commands.size());
      if (is_async && !is_duplicate && !is_session_expired) {
        BOOST_CHECK(!!state_machine.cont);
      } else {
        BOOST_CHECK(!state_machine.cont);
      }
    }
  }

  void send_command(uint64_t endpoint,
                    uint64_t session_id,
                    uint64_t unack,
                    uint64_t seq,
                    const std::string & cmd,
                    uint64_t log_index)
  {
    send_command(endpoint, session_id, unack, seq, cmd, log_index, false, false, 0);
  }
  
  void send_duplicate_command(uint64_t endpoint,
                              uint64_t session_id,
                              uint64_t unack,
                              uint64_t seq,
                              const std::string & cmd,
                              uint64_t log_index)
  {
    send_command(endpoint, session_id, unack, seq, cmd, log_index, true, false, 0);
  }
  
  void send_session_expired_command(uint64_t endpoint,
                                    uint64_t session_id,
                                    uint64_t unack,
                                    uint64_t seq,
                                    const std::string & cmd,
                                    uint64_t log_index)
  {
    send_command(endpoint, session_id, unack, seq, cmd, log_index, false, true, 0);
  }
  
  void send_session_self_expired_command(uint64_t endpoint,
                                    uint64_t session_id,
                                    uint64_t unack,
                                    uint64_t seq,
                                    const std::string & cmd,
                                    uint64_t log_index)
  {
    send_command(endpoint, session_id, unack, seq, cmd, log_index, false, true, 1);
  }
  

  void send_state_machine_completion(uint64_t endpoint, const std::string & cmd, uint64_t log_index)
  {
    auto client_response_base = communicator.client_responses.size();
    std::size_t state_machine_base = state_machine.commands.size();
    if (!state_machine.cont) {
      std::cout << "Expected !!state_machine.cont" << std::endl;
    }
    BOOST_CHECK(!!state_machine.cont);
    state_machine.complete();
    BOOST_CHECK_EQUAL(state_machine_base+1U, state_machine.commands.size());
    BOOST_CHECK(boost::algorithm::equals(cmd, state_machine.commands.back()));
    BOOST_REQUIRE_EQUAL(client_response_base + 1U, communicator.client_responses.size());
    BOOST_REQUIRE(communicator.client_responses.end() != communicator.client_responses.find(endpoint));
    BOOST_REQUIRE_EQUAL(1U, communicator.client_responses.at(endpoint).size());
    BOOST_CHECK(messages_type::client_result_success() == client_response_traits::result(communicator.client_responses.at(endpoint)[0]));
    BOOST_CHECK_EQUAL(log_index, client_response_traits::index(communicator.client_responses.at(endpoint)[0]));
    BOOST_CHECK_EQUAL(protocol.leader_id_, client_response_traits::leader_id(communicator.client_responses.at(endpoint)[0]));
    BOOST_CHECK(string_slice_compare(cmd, client_response_traits::response(communicator.client_responses.at(endpoint)[0])));
    communicator.client_responses.erase(endpoint);
  }
  
  void TestLinearizableCommand()
  {
    // Now make leader and open a session
    protocol.is_leader = true;
    open_session(0, 0);
  
    // Create and send a command.   Do this multiple times to validate at-most-once functionality
    for(std::size_t idx=1; idx<=3; ++idx) {
      if (idx > 1) {
        send_duplicate_command(0, 0, 0, 0, "foo", idx);
      } else {
        send_command(0, 0, 0, 0, "foo", idx);
      }
    }

    // Make a new session and send command using the same sequence number as the other session
    open_session(1, 4);
    send_command(1, 4, 0, 0, "bar", 5);

    // Send a second request on session 0, letting it know that first command response hasn't been received
    send_command(0, 0, 0, 1, "baz", 6);

    // Send a third request on session 0, letting it know that first two command responses have been received
    send_command(0, 0, 2, 2, "bat", 7);

    // Resend duplicate which is caught even though memo is cleared
    send_duplicate_command(0, 0, 0, 0, "foo", 8);

    // Resend duplicate caught by memo but tries (unsuccessfully) to lower unack
    send_duplicate_command(0, 0, 0, 2, "bat", 9);

    // One last non-duplicate command
    send_command(0, 0, 3, 3, "boo", 10);
  }  

  void TestLinearizableCommandSessionTimeout()
  {
    // Now make leader and open a session
    protocol.is_leader = true;
    open_session(0, 0);
  
    // Create and send a command.
    send_command(0, 0, 0, 0, "foo", 1);

    // Move cluster clock so that session will be timed out
    protocol.cluster_clock += 2*session_manager.session_timeout_nanos();

    // New session will time out old one
    open_session(1, 2, 1);

    // This will be a session timeout
    send_session_expired_command(0, 0, 1, 1, "bar", 3);    
  }  

  // This is not a critical behavior.  It amounts to whether
  // we timeout sessions before or after we try to apply a command.
  // The current implementation does do that and this tests it.
  void TestLinearizableCommandSelfSessionTimeout()
  {
    // Now make leader and open a session
    protocol.is_leader = true;
    open_session(0, 0);
  
    // Create and send a command.
    send_command(0, 0, 0, 0, "foo", 1);

    // Move cluster clock so that session will be timed out
    protocol.cluster_clock += 2*session_manager.session_timeout_nanos();

    // This will be a session timeout
    send_session_self_expired_command(0, 0, 1, 1, "bar", 2);    
  }  

  void TestAsyncLinearizableCommand()
  {
    // Now make leader and open a session
    protocol.is_leader = true;
    open_session(0, 0);

    // Set state machine to async after session because that validation logic doesn't support
    // async
    state_machine.async = true;
    // Create and send a command.   Do this multiple times to validate at-most-once functionality
    for(std::size_t idx=1; idx<=3; ++idx) {
      if (idx > 1) {
        send_duplicate_command(0, 0, 0, 0, "foo", idx);
        BOOST_CHECK(!state_machine.cont);
      } else {
        send_command(0, 0, 0, 0, "foo", idx);
        send_state_machine_completion(0, "foo", idx);
      }
    }
    std::array<const char *, 3> cmds = { "bar", "baz", "bat" };
    for(std::size_t i=0; i<2; ++i) {
      send_command(0, 0, i+1, i+1, cmds[i], i+4);
    }
    for(std::size_t i=0; i<2; ++i) {
      send_state_machine_completion(0, cmds[i], i+4);
    }
    BOOST_CHECK(!state_machine.cont);
  }
};

BOOST_AUTO_TEST_CASE_TEMPLATE(TestOnOpenCloseSessionMock, _TestType, test_types)
{
  typedef typename _TestType::messages_type messages_type;
  typedef typename _TestType::messages_type::log_entry_traits_type::const_arg_type log_entry_const_arg_type;
  typedef typename _TestType::messages_type::open_session_response_traits_type open_session_response_traits;
  typedef typename _TestType::messages_type::close_session_response_traits_type close_session_response_traits;
  typedef typename _TestType::builders_type builders_type;
  typedef typename _TestType::builders_type::open_session_request_builder_type open_session_request_builder;
  typedef typename _TestType::builders_type::close_session_request_builder_type close_session_request_builder;
  typedef typename _TestType::serialization_type serialization_type;
  typedef protocol_mock<messages_type> protocol_type;
  typedef communicator_mock<messages_type> communicator_type;
  typedef logger<messages_type, serialization_type> state_machine_type;
  typedef typename raft::state_machine::client_session_manager<messages_type, builders_type, serialization_type, protocol_type, communicator_type, state_machine_type> session_manager_type;

  protocol_type protocol;
  communicator_type communicator;
  state_machine_type state_machine;
  session_manager_type session_manager(protocol, communicator, state_machine);

  protocol.state_machine_ = [&session_manager](log_entry_const_arg_type le, uint64_t idx, size_t leader_id) { session_manager.apply(le, idx, leader_id); };
  
  // First test open session when not leader
  protocol.is_leader = false;
  protocol.leader_id_ = 100;
  auto now = std::chrono::steady_clock::now();  
  session_manager.on_open_session(0, open_session_request_builder().finish(), now);
  BOOST_REQUIRE_EQUAL(1U, communicator.open_session_responses.size());
  BOOST_REQUIRE(communicator.open_session_responses.end() != communicator.open_session_responses.find(0));
  BOOST_REQUIRE_EQUAL(1U, communicator.open_session_responses.at(0).size());
  BOOST_CHECK_EQUAL(0U, open_session_response_traits::session_id(communicator.open_session_responses.at(0)[0]));
  BOOST_REQUIRE_EQUAL(0U, communicator.close_session_responses.size());
  communicator.open_session_responses.clear();

  // Now make leader, open session will be pending commit
  protocol.is_leader = true;
  session_manager.on_open_session(0, open_session_request_builder().finish(), now);
  BOOST_CHECK_EQUAL(0U, communicator.open_session_responses.size());
  BOOST_CHECK_EQUAL(0U, communicator.close_session_responses.size());
  BOOST_CHECK_EQUAL(0U, protocol.last_applied_index_);
  BOOST_CHECK_EQUAL(1U, protocol.last_committed_index_);
  BOOST_CHECK_EQUAL(0U, session_manager.size());
  // Apply the generated log entry to the session manager
  protocol.apply_log_entries();
  // protocol.state_machine_(&protocol.log.entry(0), 0, protocol.leader_id());
  BOOST_CHECK_EQUAL(1U, protocol.last_applied_index_);
  BOOST_CHECK_EQUAL(1U, protocol.last_committed_index_);
  BOOST_CHECK_EQUAL(protocol.last_applied_index_, protocol.last_committed_index_);
  BOOST_REQUIRE_EQUAL(1U, communicator.open_session_responses.size());
  BOOST_REQUIRE(communicator.open_session_responses.end() != communicator.open_session_responses.find(0));
  BOOST_REQUIRE_EQUAL(1U, communicator.open_session_responses.at(0).size());
  BOOST_CHECK_EQUAL(0U, open_session_response_traits::session_id(communicator.open_session_responses.at(0)[0]));
  BOOST_REQUIRE_EQUAL(0U, communicator.close_session_responses.size());
  BOOST_CHECK_EQUAL(1U, session_manager.size());
  communicator.open_session_responses.clear();

  // Request several more sessions
  for(uint64_t ep=1; ep<=10; ++ep) {
    session_manager.on_open_session(ep, open_session_request_builder().finish(), now);
    BOOST_CHECK_EQUAL(0U, communicator.open_session_responses.size());
    BOOST_CHECK_EQUAL(0U, communicator.close_session_responses.size());
    BOOST_CHECK_EQUAL(1U, protocol.last_applied_index_);
    BOOST_CHECK_EQUAL(ep+1U, protocol.last_committed_index_);
    BOOST_CHECK_EQUAL(1U, session_manager.size());
  }
  // Now apply the log entries to create the sessions
  protocol.apply_log_entries();
  BOOST_CHECK_EQUAL(protocol.last_applied_index_, protocol.last_committed_index_);
  BOOST_REQUIRE_EQUAL(10U, communicator.open_session_responses.size());
  for(uint64_t ep=1; ep<=10; ++ep) {
    BOOST_REQUIRE(communicator.open_session_responses.end() != communicator.open_session_responses.find(ep));
    BOOST_REQUIRE_EQUAL(1U, communicator.open_session_responses.at(ep).size());
    BOOST_CHECK_EQUAL(ep, open_session_response_traits::session_id(communicator.open_session_responses.at(ep)[0]));
    BOOST_CHECK_EQUAL(11, session_manager.size());
  }
  BOOST_REQUIRE_EQUAL(0U, communicator.close_session_responses.size());
  BOOST_CHECK_EQUAL(protocol.last_applied_index_, protocol.last_committed_index_);
  communicator.open_session_responses.clear();

  // Now close a session.   Note the endpoint does not have to correspond to the endpoint the session
  // was created on.   Then close the same session a couple more times.
  for(uint64_t idx=11; idx<14U; ++idx) {
    session_manager.on_close_session(100U, close_session_request_builder().session_id(6).finish(), now);
    BOOST_CHECK_EQUAL(0U, communicator.open_session_responses.size());
    BOOST_CHECK_EQUAL(0U, communicator.close_session_responses.size());
    BOOST_CHECK_EQUAL(idx, protocol.last_applied_index_);
    BOOST_CHECK_EQUAL(idx+1U, protocol.last_committed_index_);
    protocol.apply_log_entries();
    BOOST_CHECK_EQUAL(protocol.last_applied_index_, protocol.last_committed_index_);
    BOOST_CHECK_EQUAL(0U, communicator.open_session_responses.size());
    BOOST_REQUIRE_EQUAL(1U, communicator.close_session_responses.size());
    BOOST_REQUIRE(communicator.close_session_responses.end() != communicator.close_session_responses.find(100));
    BOOST_REQUIRE_EQUAL(1U, communicator.close_session_responses.at(100).size());
    BOOST_CHECK_EQUAL(protocol.last_applied_index_, protocol.last_committed_index_);
    BOOST_CHECK_EQUAL(10U, session_manager.size());
    communicator.close_session_responses.clear();
  }

  // Lastly close a session, but also crank up the cluster time so that the rest of the sessions get timed out
  protocol.cluster_clock += 2*session_manager.session_timeout_nanos();
  session_manager.on_close_session(100U, close_session_request_builder().session_id(5).finish(), now);
  BOOST_CHECK_EQUAL(0U, communicator.open_session_responses.size());
  BOOST_CHECK_EQUAL(0U, communicator.close_session_responses.size());
  BOOST_CHECK_EQUAL(0U, communicator.client_responses.size());
  BOOST_CHECK_EQUAL(14U, protocol.last_applied_index_);
  BOOST_CHECK_EQUAL(15U, protocol.last_committed_index_);
  protocol.apply_log_entries();
  BOOST_CHECK_EQUAL(protocol.last_applied_index_, protocol.last_committed_index_);
  BOOST_CHECK_EQUAL(0U, communicator.open_session_responses.size());
  BOOST_REQUIRE_EQUAL(1U, communicator.close_session_responses.size());
  BOOST_REQUIRE(communicator.close_session_responses.end() != communicator.close_session_responses.find(100));
  BOOST_REQUIRE_EQUAL(1U, communicator.close_session_responses.at(100).size());
  BOOST_CHECK_EQUAL(0U, communicator.client_responses.size());
  BOOST_CHECK_EQUAL(0U, session_manager.size());
  communicator.close_session_responses.clear();
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TestLinearizableCommandMock, _TestType, test_types)
{
  SessionManagerMockTestFixture<_TestType> t;
  t.TestLinearizableCommand();
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TestLinearizableCommandSessionTimeoutMock, _TestType, test_types)
{
  SessionManagerMockTestFixture<_TestType> t;
  t.TestLinearizableCommandSessionTimeout();
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TestLinearizableCommandSelfSessionTimeoutMock, _TestType, test_types)
{
  SessionManagerMockTestFixture<_TestType> t;
  t.TestLinearizableCommandSelfSessionTimeout();
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TestAsyncLinearizableCommandMock, _TestType, test_types)
{
  SessionManagerMockTestFixture<_TestType> t;
  t.TestAsyncLinearizableCommand();
}

// Test cases to implement:
// Handling of a client session waiting for commit of an entry that fails to commit (look at the duplicated log entry logic in protocol)
// Restoring a checkpoint that overwrites uncommitted entries or log is shorter than checkpoint range.   Both of these may delete uncommitted log entries.
// Log entries that are committed by virtue of applying a checkpoint.

template<typename _TestType>
struct SessionManagerTestFixture : public raft::test::RaftTestFixtureBase<_TestType>
{
  typedef typename _TestType::messages_type messages_type;
  typedef typename _TestType::messages_type::log_entry_traits_type::const_arg_type log_entry_const_arg_type;
  typedef typename _TestType::messages_type::open_session_response_traits_type open_session_response_traits;
  typedef typename _TestType::messages_type::close_session_response_traits_type close_session_response_traits;
  typedef typename _TestType::messages_type::client_response_traits_type client_response_traits;
  typedef typename _TestType::builders_type builders_type;
  typedef typename _TestType::builders_type::open_session_request_builder_type open_session_request_builder;
  typedef typename _TestType::builders_type::close_session_request_builder_type close_session_request_builder;
  typedef typename _TestType::builders_type::linearizable_command_request_builder_type linearizable_command_request_builder;
  typedef typename _TestType::serialization_type serialization_type;
  typedef typename _TestType::messages_type::client_result_type client_result_type;
  typedef communicator_mock<messages_type> client_communicator_type;
  typedef logger<messages_type, serialization_type> state_machine_type;
  typedef raft::protocol<raft::test::generic_communicator_metafunction, raft::test::native_client_metafunction, messages_type> raft_type;
  typedef typename raft::state_machine::client_session_manager<messages_type, builders_type, serialization_type, raft_type, client_communicator_type, state_machine_type> session_manager_type;

  client_communicator_type communicator;
  state_machine_type state_machine;
  session_manager_type session_manager;

  SessionManagerTestFixture()
    :
    raft::test::RaftTestFixtureBase<_TestType>(),
    session_manager(*this->protocol.get(), communicator, state_machine)
  {
    this->protocol->set_state_machine([this](log_entry_const_arg_type le, uint64_t idx, size_t leader_id) { this->session_manager.apply(le, idx, leader_id); });
  }
  
  ~SessionManagerTestFixture() {}

  void open_session(uint64_t endpoint, uint64_t term, uint64_t log_index, uint64_t num_sessions_expired=0)
  {
    // This logic does not support async
    BOOST_ASSERT(!state_machine.async);
    // Must be leader
    BOOST_CHECK_EQUAL(raft_type::LEADER, this->protocol->get_state());
    auto open_session_base = communicator.open_session_responses.size();
    auto close_session_base = communicator.close_session_responses.size();
    auto client_response_base = communicator.client_responses.size();
    auto session_manager_base = session_manager.size();
    auto state_machine_base = state_machine.commands.size();
    auto log_base = this->protocol->log_index_end();
    // The following assumes that endpoint is not in open_session_responses
    BOOST_REQUIRE(communicator.open_session_responses.end() == communicator.open_session_responses.find(endpoint));
    session_manager.on_open_session(endpoint, open_session_request_builder().finish(), this->now);
    BOOST_CHECK_EQUAL(open_session_base, communicator.open_session_responses.size());
    BOOST_CHECK_EQUAL(close_session_base, communicator.close_session_responses.size());
    BOOST_CHECK_EQUAL(client_response_base, communicator.client_responses.size());
    BOOST_CHECK_EQUAL(log_index, this->protocol->applied_index());
    BOOST_CHECK_EQUAL(session_manager_base, session_manager.size());
    BOOST_CHECK_EQUAL(state_machine_base, state_machine.commands.size());
    BOOST_CHECK_EQUAL(log_base+1U, this->protocol->log_index_end());
    BOOST_CHECK_EQUAL(log_index+1U, this->protocol->log_index_end());
    // This will have advanced the cluster clock because protocol::on_command was called
    this->initial_cluster_time = this->protocol->cluster_time();
    // Now must commit log entry which will apply it to the session manager and create the session
    this->commit_one_log_entry(term, log_index);

    BOOST_TEST(this->protocol->applied_index() == this->protocol->commit_index());
    BOOST_REQUIRE_EQUAL(open_session_base+1U, communicator.open_session_responses.size());
    BOOST_REQUIRE(communicator.open_session_responses.end() != communicator.open_session_responses.find(endpoint));
    BOOST_REQUIRE_EQUAL(1U, communicator.open_session_responses.at(endpoint).size());
    BOOST_CHECK_EQUAL(log_index, open_session_response_traits::session_id(communicator.open_session_responses.at(endpoint)[0]));
    BOOST_CHECK_EQUAL(close_session_base, communicator.close_session_responses.size());
    BOOST_CHECK_EQUAL(client_response_base, communicator.client_responses.size());
    BOOST_CHECK_EQUAL(log_base+1U, this->protocol->log_index_end());
    BOOST_CHECK_EQUAL(log_index+1U, this->protocol->log_index_end());
    BOOST_CHECK_EQUAL(session_manager_base+1U-num_sessions_expired, session_manager.size());
    BOOST_CHECK_EQUAL(state_machine_base, state_machine.commands.size());
    communicator.open_session_responses.erase(endpoint);
  }

  void send_command(uint64_t endpoint,
                    uint64_t session_id,
                    uint64_t unack,
                    uint64_t seq,
                    const std::string & cmd,
                    uint64_t term,
                    uint64_t log_index,
                    bool is_duplicate,
                    bool is_session_expired,
                    uint64_t num_sessions_expired)
  {
    auto is_async = state_machine.async;
    auto open_session_base = communicator.open_session_responses.size();
    auto close_session_base = communicator.close_session_responses.size();
    auto client_response_base = communicator.client_responses.size();
    auto session_manager_base = session_manager.size();
    auto state_machine_base = state_machine.commands.size();
    auto log_base = this->protocol->log_index_end();
    // The following assumes that endpoint is not in client_responses
    BOOST_REQUIRE(communicator.client_responses.end() == communicator.client_responses.find(endpoint));
    auto msg = linearizable_command_request_builder().session_id(session_id).first_unacknowledged_sequence_number(unack).sequence_number(seq).command(raft::slice::create(cmd)).finish();
    session_manager.on_linearizable_command(endpoint, std::move(msg), this->now);
    BOOST_CHECK_EQUAL(open_session_base, communicator.open_session_responses.size());
    BOOST_CHECK_EQUAL(close_session_base, communicator.close_session_responses.size());
    BOOST_CHECK_EQUAL(client_response_base, communicator.client_responses.size());
    BOOST_CHECK_EQUAL(session_manager_base, session_manager.size());
    BOOST_CHECK_EQUAL(state_machine_base, state_machine.commands.size());
    BOOST_CHECK_EQUAL(log_base+1U, this->protocol->log_index_end());
    BOOST_CHECK_EQUAL(log_index+1U, this->protocol->log_index_end());

    // Now must commit log entry which will apply it to the session manager and initiate the command on
    // the state machine.
    this->commit_one_log_entry(term, log_index);

    if (!is_async) {
      BOOST_TEST(this->protocol->applied_index() == this->protocol->commit_index());
    }
    BOOST_CHECK_EQUAL(open_session_base, communicator.open_session_responses.size());
    BOOST_CHECK_EQUAL(close_session_base, communicator.close_session_responses.size());
    BOOST_CHECK_EQUAL(log_base+1U, this->protocol->log_index_end());
    BOOST_CHECK_EQUAL(log_index+1U, this->protocol->log_index_end());
    if (!is_async || is_duplicate || is_session_expired) {
      BOOST_REQUIRE_EQUAL(client_response_base + 1U, communicator.client_responses.size());
      BOOST_REQUIRE(communicator.client_responses.end() != communicator.client_responses.find(endpoint));
      BOOST_REQUIRE_EQUAL(1U, communicator.client_responses.at(endpoint).size());
      if (is_session_expired) {
        BOOST_CHECK(messages_type::client_result_session_expired() == client_response_traits::result(communicator.client_responses.at(endpoint)[0]));
      } else {
        BOOST_CHECK(messages_type::client_result_success() == client_response_traits::result(communicator.client_responses.at(endpoint)[0]));
        BOOST_CHECK_EQUAL(log_index, client_response_traits::index(communicator.client_responses.at(endpoint)[0]));
        BOOST_CHECK_EQUAL(this->protocol->leader_id(), client_response_traits::leader_id(communicator.client_responses.at(endpoint)[0]));
      }
      communicator.client_responses.erase(endpoint);
    } else {
      BOOST_CHECK_EQUAL(client_response_base, communicator.client_responses.size());
    }
    BOOST_CHECK_EQUAL(session_manager_base - num_sessions_expired, session_manager.size());
    if (!is_async) {
      BOOST_REQUIRE(0<state_machine.commands.size());
    }
    if (!is_duplicate && !is_async && !is_session_expired) {
      BOOST_CHECK(boost::algorithm::equals(cmd, state_machine.commands.back()));
      BOOST_CHECK_EQUAL(state_machine_base+1U, state_machine.commands.size());
    } else {
      BOOST_CHECK_EQUAL(state_machine_base, state_machine.commands.size());
      if (is_async && !is_duplicate && !is_session_expired) {
        BOOST_CHECK(!!state_machine.cont);
      } else {
        BOOST_CHECK(!state_machine.cont);
      }
    }
  }

  void send_command(uint64_t endpoint,
                    uint64_t session_id,
                    uint64_t unack,
                    uint64_t seq,
                    const std::string & cmd,
                    uint64_t term,
                    uint64_t log_index)
  {
    send_command(endpoint, session_id, unack, seq, cmd, term, log_index, false, false, 0);
  }
  
  void send_duplicate_command(uint64_t endpoint,
                              uint64_t session_id,
                              uint64_t unack,
                              uint64_t seq,
                              const std::string & cmd,
                              uint64_t term,
                              uint64_t log_index)
  {
    send_command(endpoint, session_id, unack, seq, cmd, term, log_index, true, false, 0);
  }
  
  void send_session_expired_command(uint64_t endpoint,
                                    uint64_t session_id,
                                    uint64_t unack,
                                    uint64_t seq,
                                    const std::string & cmd,
                                    uint64_t term,
                                    uint64_t log_index)
  {
    send_command(endpoint, session_id, unack, seq, cmd, term, log_index, false, true, 0);
  }
  
  void send_session_self_expired_command(uint64_t endpoint,
                                         uint64_t session_id,
                                         uint64_t unack,
                                         uint64_t seq,
                                         const std::string & cmd,
                                         uint64_t term,
                                         uint64_t log_index)
  {
    send_command(endpoint, session_id, unack, seq, cmd, term, log_index, false, true, 1);
  }
  

  void send_state_machine_completion(uint64_t endpoint, const std::string & cmd, uint64_t log_index)
  {
    auto client_response_base = communicator.client_responses.size();
    std::size_t state_machine_base = state_machine.commands.size();
    if (!state_machine.cont) {
      std::cout << "Expected !!state_machine.cont" << std::endl;
    }
    BOOST_CHECK(!!state_machine.cont);
    state_machine.complete();
    BOOST_CHECK_EQUAL(state_machine_base+1U, state_machine.commands.size());
    BOOST_CHECK(boost::algorithm::equals(cmd, state_machine.commands.back()));
    BOOST_REQUIRE_EQUAL(client_response_base + 1U, communicator.client_responses.size());
    BOOST_REQUIRE(communicator.client_responses.end() != communicator.client_responses.find(endpoint));
    BOOST_REQUIRE_EQUAL(1U, communicator.client_responses.at(endpoint).size());
    BOOST_CHECK(messages_type::client_result_success() == client_response_traits::result(communicator.client_responses.at(endpoint)[0]));
    BOOST_CHECK_EQUAL(log_index, client_response_traits::index(communicator.client_responses.at(endpoint)[0]));
    BOOST_CHECK_EQUAL(this->protocol->leader_id(), client_response_traits::leader_id(communicator.client_responses.at(endpoint)[0]));
    BOOST_CHECK(string_slice_compare(cmd, client_response_traits::response(communicator.client_responses.at(endpoint)[0])));
    communicator.client_responses.erase(endpoint);
  }
  
  void TestLinearizableCommand()
  {
    // Now make leader and open a session
    uint64_t term = 1;
    this->make_leader(term);
    BOOST_CHECK_EQUAL(1, this->protocol->log_index_end());
    
    open_session(0, term, 1);
  
    // Create and send a command.   Do this multiple times to validate at-most-once functionality
    for(std::size_t idx=2; idx<=4; ++idx) {
      if (idx > 2) {
        send_duplicate_command(0, 1, 0, 0, "foo", term, idx);
      } else {
        send_command(0, 1, 0, 0, "foo", term, idx);
      }
    }

    // Make a new session and send command using the same sequence number as the other session
    open_session(1, term, 5);
    send_command(1, 5, 0, 0, "bar", term, 6);

    // Send a second request on session 0, letting it know that first command response hasn't been received
    send_command(0, 1, 0, 1, "baz", term, 7);

    // Send a third request on session 0, letting it know that first two command responses have been received
    send_command(0, 1, 2, 2, "bat", term, 8);

    // Resend duplicate which is caught even though memo is cleared
    send_duplicate_command(0, 1, 0, 0, "foo", term, 9);

    // Resend duplicate caught by memo but tries (unsuccessfully) to lower unack
    send_duplicate_command(0, 1, 0, 2, "bat", term, 10);

    // One last non-duplicate command
    send_command(0, 1, 3, 3, "boo", term, 11);
  }  

  void TestLinearizableCommandSessionTimeout()
  {
    // Now make leader and open a session
    uint64_t term = 1;
    this->make_leader(term);
    BOOST_CHECK_EQUAL(1, this->protocol->log_index_end());
    
    open_session(0, term, 1);
  
    // Create and send a command.
    send_command(0, 1, 0, 0, "foo", term, 2);

    // Move cluster clock so that session will be timed out
    // New session will time out old one
    this->now += std::chrono::nanoseconds(2*session_manager.session_timeout_nanos());
    open_session(1, term, 3, 1);

    // This will be a session timeout
    send_session_expired_command(0, 1, 1, 1, "bar", term, 4);
  }  

  // This is not a critical behavior.  It amounts to whether
  // we timeout sessions before or after we try to apply a command.
  // The current implementation does do that and this tests it.
  void TestLinearizableCommandSelfSessionTimeout()
  {
    // Now make leader and open a session
    uint64_t term = 1;
    this->make_leader(term);
    BOOST_CHECK_EQUAL(1, this->protocol->log_index_end());
  
    open_session(0, term, 1);
  
    // Create and send a command.
    send_command(0, 1, 0, 0, "foo", term, 2);

    // Move cluster clock so that session will be timed out
    this->now += std::chrono::nanoseconds(2*session_manager.session_timeout_nanos());

    // This will be a session timeout
    send_session_self_expired_command(0, 1, 1, 1, "bar", term, 3);    
  }  

  void TestAsyncLinearizableCommand()
  {
    // Now make leader and open a session
    uint64_t term = 1;
    this->make_leader(term);
    BOOST_CHECK_EQUAL(1, this->protocol->log_index_end());

    open_session(0, term, 1);
  
    // Set state machine to async after session because that validation logic doesn't support
    // async
    state_machine.async = true;
    // Create and send a command.   Do this multiple times to validate at-most-once functionality
    for(std::size_t idx=2; idx<=4; ++idx) {
      if (idx > 2) {
        send_duplicate_command(0, 1, 0, 0, "foo", term, idx);
        BOOST_CHECK(!state_machine.cont);
      } else {
        send_command(0, 1, 0, 0, "foo", term, idx);
        send_state_machine_completion(0, "foo", idx);
      }
    }
    std::array<const char *, 3> cmds = { "bar", "baz", "bat" };
    for(std::size_t i=0; i<2; ++i) {
      send_command(0, 1, i+1, i+1, cmds[i], term, i+5);
    }
    for(std::size_t i=0; i<2; ++i) {
      send_state_machine_completion(0, cmds[i], i+5);
    }
    BOOST_CHECK(!state_machine.cont);
  }

  void TestReadOnlyQuery()
  {
    typedef typename messages_type::append_entry_request_traits_type append_entry_request_traits;
    typedef typename _TestType::messages_type::append_entry_request_traits_type::arg_type append_entry_request_arg_type;
    typedef typename _TestType::builders_type::append_entry_response_builder_type append_entry_response_builder;
    // Now make leader and open a session
    uint64_t term = 1;
    this->make_leader(term);
    BOOST_CHECK_EQUAL(1, this->protocol->log_index_end());
    
    open_session(0, term, 1);
  
    send_command(0, 1, 0, 0, "foo", term, 2);

    // Everything is committed and state machine is up to date.
    // Read only query should send some heartbeats immediately
    auto client_index = this->protocol->log_index_end();
    bool called = false;
    client_result_type cr = messages_type::client_result_fail();
    auto cb = [&cr, &called](client_result_type result) { cr = result; called = true; };
    BOOST_TEST(0 == this->protocol->request_id());
    this->protocol->async_linearizable_read_only_query_fence(std::move(cb), this->now);
    BOOST_TEST(1 == this->protocol->request_id());
    BOOST_TEST(!called);
    BOOST_CHECK_EQUAL(this->num_known_peers()-1, this->comm.q.size());
    std::size_t expected = 1;
    while(this->comm.q.size() > 0) {
      auto req_request_id = append_entry_request_traits::request_id(boost::get<append_entry_request_arg_type>(this->comm.q.back()));
      BOOST_CHECK_EQUAL(this->protocol->request_id(), req_request_id);
      BOOST_CHECK_EQUAL(expected, append_entry_request_traits::recipient_id(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(term, append_entry_request_traits::term_number(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(0U, append_entry_request_traits::leader_id(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(0U, append_entry_request_traits::log_index_begin(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(0U, append_entry_request_traits::previous_log_term(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(0U, append_entry_request_traits::num_entries(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
      auto resp = append_entry_response_builder().request_id(req_request_id).recipient_id(expected).term_number(term).request_term_number(term).index_begin(0).index_end(client_index).success(true).finish();
      this->protocol->on_append_entry_response(std::move(resp), this->now);
      if (expected > 1) {
        BOOST_TEST(called);
        BOOST_TEST(messages_type::client_result_success() == cr);
      } else {
        BOOST_TEST(!called);
      }
      expected += 1;
      this->comm.q.pop_back();
    }
  }  

  void TestAsyncReadOnlyQuery()
  {
    typedef typename messages_type::append_entry_request_traits_type append_entry_request_traits;
    typedef typename _TestType::messages_type::append_entry_request_traits_type::arg_type append_entry_request_arg_type;
    typedef typename _TestType::builders_type::append_entry_response_builder_type append_entry_response_builder;
    // Now make leader and open a session
    uint64_t term = 1;
    this->make_leader(term);
    BOOST_CHECK_EQUAL(1, this->protocol->log_index_end());
    
    open_session(0, term, 1);
  
    // Set state machine to async after session because that validation logic doesn't support
    // async
    state_machine.async = true;

    send_command(0, 1, 0, 0, "foo", term, 2);

    // Everything is committed but state machine is not up to date
    // Read only query should send some heartbeats immediately
    auto client_index = this->protocol->log_index_end();
    bool called = false;
    client_result_type cr = messages_type::client_result_fail();
    auto cb = [&cr, &called](client_result_type result) { cr = result; called = true; };
    BOOST_TEST(0 == this->protocol->request_id());
    this->protocol->async_linearizable_read_only_query_fence(std::move(cb), this->now);
    BOOST_TEST(1 == this->protocol->request_id());
    BOOST_TEST(!called);
    BOOST_CHECK_EQUAL(this->num_known_peers()-1, this->comm.q.size());
    std::size_t expected = 1;
    while(this->comm.q.size() > 0) {
      auto req_request_id = append_entry_request_traits::request_id(boost::get<append_entry_request_arg_type>(this->comm.q.back()));
      BOOST_CHECK_EQUAL(this->protocol->request_id(), req_request_id);
      BOOST_CHECK_EQUAL(expected, append_entry_request_traits::recipient_id(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(term, append_entry_request_traits::term_number(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(0U, append_entry_request_traits::leader_id(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(0U, append_entry_request_traits::log_index_begin(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(0U, append_entry_request_traits::previous_log_term(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(0U, append_entry_request_traits::num_entries(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
      auto resp = append_entry_response_builder().request_id(req_request_id).recipient_id(expected).term_number(term).request_term_number(term).index_begin(0).index_end(client_index).success(true).finish();
      this->protocol->on_append_entry_response(std::move(resp), this->now);
      BOOST_TEST(!called);
      expected += 1;
      this->comm.q.pop_back();
    }

    // Now if we complete the state machine command application, the read only query will
    // be runnable
    send_state_machine_completion(0, "foo", 2);
    BOOST_TEST(called);
    BOOST_TEST(messages_type::client_result_success() == cr);
  }  

  void TestReadOnlyQueryNoLongerLeader()
  {
    typedef typename messages_type::append_entry_request_traits_type append_entry_request_traits;
    typedef typename _TestType::messages_type::append_entry_request_traits_type::arg_type append_entry_request_arg_type;
    typedef typename _TestType::builders_type::append_entry_response_builder_type append_entry_response_builder;
    // Now make leader and open a session
    uint64_t term = 1;
    this->make_leader(term);
    BOOST_CHECK_EQUAL(1, this->protocol->log_index_end());
    
    open_session(0, term, 1);
  
    send_command(0, 1, 0, 0, "foo", term, 2);

    // Everything is committed and state machine is up to date.
    // Read only query should send some heartbeats immediately
    auto client_index = this->protocol->log_index_end();
    bool called = false;
    client_result_type cr = messages_type::client_result_fail();
    auto cb = [&cr, &called](client_result_type result) { cr = result; called = true; };
    BOOST_TEST(0 == this->protocol->request_id());
    this->protocol->async_linearizable_read_only_query_fence(std::move(cb), this->now);
    BOOST_TEST(1 == this->protocol->request_id());
    BOOST_TEST(!called);
    BOOST_CHECK_EQUAL(this->num_known_peers()-1, this->comm.q.size());
    std::size_t expected = 1;
    while(this->comm.q.size() > 0) {
      auto req_request_id = append_entry_request_traits::request_id(boost::get<append_entry_request_arg_type>(this->comm.q.back()));
      BOOST_CHECK_EQUAL(this->protocol->request_id(), req_request_id);
      BOOST_CHECK_EQUAL(expected, append_entry_request_traits::recipient_id(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(term, append_entry_request_traits::term_number(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(0U, append_entry_request_traits::leader_id(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(0U, append_entry_request_traits::log_index_begin(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(0U, append_entry_request_traits::previous_log_term(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(0U, append_entry_request_traits::num_entries(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
      if (expected > 1) {
        // Peers 2,3 and 4 formed a new quorum and elected a new leader and committed new entry
        auto resp = append_entry_response_builder().request_id(req_request_id).recipient_id(expected).term_number(term+1).request_term_number(term).index_begin(client_index+1).index_end(client_index+1).success(true).finish();
        this->protocol->on_append_entry_response(std::move(resp), this->now);
        BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
        BOOST_TEST(called);
        BOOST_TEST(messages_type::client_result_not_leader() == cr);
        if (2 == expected) {
          BOOST_TEST(this->protocol->log_header_sync_required());
          this->protocol->on_log_header_sync(this->now);
        }
      } else {
        auto resp = append_entry_response_builder().request_id(req_request_id).recipient_id(expected).term_number(term).request_term_number(term).index_begin(0).index_end(client_index).success(true).finish();
        this->protocol->on_append_entry_response(std::move(resp), this->now);
        BOOST_TEST(!called);
      }
      expected += 1;
      this->comm.q.pop_back();
    }
  }  

  void TestReadOnlyQueryUnknownCommitIndex()
  {
    typedef typename messages_type::append_entry_request_traits_type append_entry_request_traits;
    typedef typename _TestType::messages_type::append_entry_request_traits_type::arg_type append_entry_request_arg_type;
    typedef typename _TestType::builders_type::append_entry_response_builder_type append_entry_response_builder;
    // Start out as follower and have the leader append some entries that are not known to be committed
    uint64_t term = 1;
    this->become_follower_with_vote_request(term);
    this->send_noop(1, 1, 0, 0, 0);
    this->send_open_session(1, 1, 1, 1, 0);
    this->send_linearizable_command(1, 1, 1, 2, 1, 0, raft::slice::create("foo"));
    this->send_linearizable_command(1, 1, 1, 3, 1, 0, raft::slice::create("bar"));
    // We now assume that the leader has committed these entries but hasn't yet informed us of that fact.
    // Now we become leader (which is fine since we have all of the necessary log entries), but don't process the
    // NOOP append entry responses.
    this->make_leader(++term, false);
    BOOST_TEST(0U == this->protocol->commit_index());
    bool called = false;
    client_result_type cr = messages_type::client_result_fail();
    auto cb = [&cr, &called](client_result_type result) { cr = result; called = true; };
    BOOST_TEST(0 == this->protocol->request_id());
    this->protocol->async_linearizable_read_only_query_fence(std::move(cb), this->now);
    BOOST_TEST(1 == this->protocol->request_id());
    BOOST_TEST(!called);
    BOOST_TEST(this->num_known_peers() == this->comm.q.size()+1);
    // For this test, we assume that the all of the entries from the previous leader were committed
    this->protocol->on_log_sync(5, this->now);
    for(uint64_t p=1; p!=this->num_known_peers(); ++p) {
      auto resp = append_entry_response_builder().recipient_id(p).request_id(0).term_number(term).request_term_number(term).index_begin(4).index_end(5).success(true).finish();
      this->protocol->on_append_entry_response(std::move(resp), this->now);
      if (p == 1) {
        BOOST_TEST(0U == this->protocol->commit_index());
      } else {
        BOOST_TEST(5U == this->protocol->commit_index());
      }
    }    
    std::size_t expected = 1;
    while(this->comm.q.size() > 0) {
      auto req_request_id = append_entry_request_traits::request_id(boost::get<append_entry_request_arg_type>(this->comm.q.back()));
      BOOST_CHECK_EQUAL(this->protocol->request_id(), req_request_id);
      BOOST_CHECK_EQUAL(expected, append_entry_request_traits::recipient_id(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(term, append_entry_request_traits::term_number(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(0U, append_entry_request_traits::leader_id(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(0U, append_entry_request_traits::log_index_begin(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(0U, append_entry_request_traits::previous_log_term(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(0U, append_entry_request_traits::num_entries(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
      auto resp = append_entry_response_builder().request_id(req_request_id).recipient_id(expected).term_number(term).request_term_number(term).index_begin(0).index_end(5).success(true).finish();
      this->protocol->on_append_entry_response(std::move(resp), this->now);
      if (expected > 1) {
        BOOST_TEST(called);
        BOOST_TEST(messages_type::client_result_success() == cr);
      } else {
        BOOST_TEST(!called);
      }
      expected += 1;
      this->comm.q.pop_back();
    }
  }  

  void TestReadOnlyQueryUnknownCommitIndexOutOfDatePeers()
  {
    typedef typename messages_type::append_entry_request_traits_type append_entry_request_traits;
    typedef typename _TestType::messages_type::append_entry_request_traits_type::arg_type append_entry_request_arg_type;
    typedef typename _TestType::builders_type::append_entry_response_builder_type append_entry_response_builder;
    // Start out as follower and have the leader append some entries that are not known to be committed
    uint64_t term = 1;
    this->become_follower_with_vote_request(term);
    this->send_noop(1, 1, 0, 0, 0);
    this->send_open_session(1, 1, 1, 1, 0);
    this->send_linearizable_command(1, 1, 1, 2, 1, 0, raft::slice::create("foo"));
    this->send_linearizable_command(1, 1, 1, 3, 1, 0, raft::slice::create("bar"));
    // Now we become leader (which is fine since we have all of the necessary log entries), but don't process the
    // NOOP append entry responses.
    this->make_leader(++term, false);
    BOOST_TEST(0U == this->protocol->commit_index());
    BOOST_TEST(this->comm.q.size() == 0);
    // For this test, we assume that only the former leader had all of our log entries.
    // That means we don't commit yet and have to send old entries (we do assume that the other peers got the noop from
    // the old leader).
    this->protocol->on_log_sync(5, this->now);
    for(uint64_t p=1; p!=this->num_known_peers(); ++p) {
      if (p == 1) {
        auto resp = append_entry_response_builder().recipient_id(p).request_id(0).term_number(term).request_term_number(term).index_begin(4).index_end(5).success(true).finish();
        this->protocol->on_append_entry_response(std::move(resp), this->now);
      } else {
        auto resp = append_entry_response_builder().recipient_id(p).request_id(0).term_number(term).request_term_number(term).index_begin(1).index_end(1).success(false).finish();
        this->protocol->on_append_entry_response(std::move(resp), this->now);
      }
      BOOST_TEST(0U == this->protocol->commit_index());
    }
    BOOST_TEST(this->comm.q.size() == 0);
    // Now a client submits a read only query which triggers heartbeats at a new request id
    bool called = false;
    client_result_type cr = messages_type::client_result_fail();
    auto cb = [&cr, &called](client_result_type result) { cr = result; called = true; };
    BOOST_TEST(0 == this->protocol->request_id());
    this->protocol->async_linearizable_read_only_query_fence(std::move(cb), this->now);
    BOOST_TEST(1 == this->protocol->request_id());
    BOOST_TEST(!called);
    BOOST_TEST(this->num_known_peers() == this->comm.q.size()+1);
    std::size_t expected = 1;
    while(this->comm.q.size() > 0) {
      auto & msg (boost::get<append_entry_request_arg_type>(this->comm.q.back()));
      auto req_request_id = append_entry_request_traits::request_id(msg);
      BOOST_CHECK_EQUAL(this->protocol->request_id(), req_request_id);
      BOOST_CHECK_EQUAL(expected, append_entry_request_traits::recipient_id(msg));
      BOOST_CHECK_EQUAL(term, append_entry_request_traits::term_number(msg));
      BOOST_CHECK_EQUAL(0U, append_entry_request_traits::leader_id(msg));
      BOOST_CHECK_EQUAL(0U, append_entry_request_traits::log_index_begin(msg));
      BOOST_CHECK_EQUAL(0U, append_entry_request_traits::previous_log_term(msg));
      BOOST_CHECK_EQUAL(0U, append_entry_request_traits::num_entries(msg));
      if (expected == 1) {
        auto resp = append_entry_response_builder().request_id(req_request_id).recipient_id(expected).term_number(term).request_term_number(term).index_begin(0).index_end(5).success(true).finish();
        this->protocol->on_append_entry_response(std::move(resp), this->now);
      } else {
        auto resp = append_entry_response_builder().request_id(req_request_id).recipient_id(expected).term_number(term).request_term_number(term).index_begin(0).index_end(1).success(true).finish();
        this->protocol->on_append_entry_response(std::move(resp), this->now);
      }
      BOOST_TEST(!called);
      BOOST_TEST(0U == this->protocol->commit_index());
      expected += 1;
      this->comm.q.pop_back();
    }
    // Retransmit timer goes off and we resend append entries to finish commit
    this->now = this->now + std::chrono::milliseconds(1);
    this->protocol->on_timer(this->now);
    BOOST_TEST(0U == this->comm.q.size());
    this->now = this->now + std::chrono::milliseconds(1);
    this->protocol->on_timer(this->now);
    BOOST_TEST(3U == this->comm.q.size());
    expected = 2;
    while(this->comm.q.size() > 0) {
      auto & msg (boost::get<append_entry_request_arg_type>(this->comm.q.back()));
      auto req_request_id = append_entry_request_traits::request_id(msg);
      BOOST_CHECK_EQUAL(this->protocol->request_id(), req_request_id);
      BOOST_CHECK_EQUAL(expected, append_entry_request_traits::recipient_id(msg));
      BOOST_CHECK_EQUAL(term, append_entry_request_traits::term_number(msg));
      BOOST_CHECK_EQUAL(0U, append_entry_request_traits::leader_id(msg));
      BOOST_CHECK_EQUAL(1U, append_entry_request_traits::log_index_begin(msg));
      BOOST_CHECK_EQUAL(1U, append_entry_request_traits::previous_log_term(msg));
      BOOST_CHECK_EQUAL(4U, append_entry_request_traits::num_entries(msg));
      auto resp = append_entry_response_builder().request_id(req_request_id).recipient_id(expected).term_number(term).request_term_number(term).index_begin(2).index_end(5).success(true).finish();
      this->protocol->on_append_entry_response(std::move(resp), this->now);
      BOOST_TEST(called);
      BOOST_TEST(5U == this->protocol->commit_index());
      expected += 1;
      this->comm.q.pop_back();
    }
  }

  void TestReadOnlyQueryUnknownCommitIndexOutOfDatePeersThenLoseLeadership()
  {
    typedef typename messages_type::append_entry_request_traits_type append_entry_request_traits;
    typedef typename _TestType::messages_type::append_entry_request_traits_type::arg_type append_entry_request_arg_type;
    typedef typename _TestType::builders_type::append_entry_response_builder_type append_entry_response_builder;
    // Start out as follower and have the leader append some entries that are not known to be committed
    uint64_t term = 1;
    this->become_follower_with_vote_request(term);
    this->send_noop(1, 1, 0, 0, 0);
    this->send_open_session(1, 1, 1, 1, 0);
    this->send_linearizable_command(1, 1, 1, 2, 1, 0, raft::slice::create("foo"));
    this->send_linearizable_command(1, 1, 1, 3, 1, 0, raft::slice::create("bar"));
    // Now we become leader (which is fine since we have all of the necessary log entries), but don't process the
    // NOOP append entry responses.
    this->make_leader(++term, false);
    BOOST_TEST(0U == this->protocol->commit_index());
    BOOST_TEST(this->comm.q.size() == 0);
    // For this test, we assume that only the former leader had all of our log entries.
    // That means we don't commit yet and have to send old entries (we do assume that the other peers got the noop from
    // the old leader).   In this variant, we lose leadership while trying to bring peers up to date.
    this->protocol->on_log_sync(5, this->now);
    for(uint64_t p=1; p!=this->num_known_peers(); ++p) {
      if (p == 1) {
        auto resp = append_entry_response_builder().recipient_id(p).request_id(0).term_number(term).request_term_number(term).index_begin(4).index_end(5).success(true).finish();
        this->protocol->on_append_entry_response(std::move(resp), this->now);
      } else {
        auto resp = append_entry_response_builder().recipient_id(p).request_id(0).term_number(term).request_term_number(term).index_begin(1).index_end(1).success(false).finish();
        this->protocol->on_append_entry_response(std::move(resp), this->now);
      }
      BOOST_TEST(0U == this->protocol->commit_index());
    }
    BOOST_TEST(this->comm.q.size() == 0);
    // Now a client submits a read only query which triggers heartbeats at a new request id
    bool called = false;
    client_result_type cr = messages_type::client_result_fail();
    auto cb = [&cr, &called](client_result_type result) { cr = result; called = true; };
    BOOST_TEST(0 == this->protocol->request_id());
    this->protocol->async_linearizable_read_only_query_fence(std::move(cb), this->now);
    BOOST_TEST(1 == this->protocol->request_id());
    BOOST_TEST(!called);
    BOOST_TEST(this->num_known_peers() == this->comm.q.size()+1);
    std::size_t expected = 1;
    while(this->comm.q.size() > 0) {
      auto & msg (boost::get<append_entry_request_arg_type>(this->comm.q.back()));
      auto req_request_id = append_entry_request_traits::request_id(msg);
      BOOST_CHECK_EQUAL(this->protocol->request_id(), req_request_id);
      BOOST_CHECK_EQUAL(expected, append_entry_request_traits::recipient_id(msg));
      BOOST_CHECK_EQUAL(term, append_entry_request_traits::term_number(msg));
      BOOST_CHECK_EQUAL(0U, append_entry_request_traits::leader_id(msg));
      BOOST_CHECK_EQUAL(0U, append_entry_request_traits::log_index_begin(msg));
      BOOST_CHECK_EQUAL(0U, append_entry_request_traits::previous_log_term(msg));
      BOOST_CHECK_EQUAL(0U, append_entry_request_traits::num_entries(msg));
      if (expected == 1) {
        auto resp = append_entry_response_builder().request_id(req_request_id).recipient_id(expected).term_number(term).request_term_number(term).index_begin(0).index_end(5).success(true).finish();
        this->protocol->on_append_entry_response(std::move(resp), this->now);
      } else {
        auto resp = append_entry_response_builder().request_id(req_request_id).recipient_id(expected).term_number(term).request_term_number(term).index_begin(0).index_end(1).success(true).finish();
        this->protocol->on_append_entry_response(std::move(resp), this->now);
      }
      BOOST_TEST(!called);
      BOOST_TEST(0U == this->protocol->commit_index());
      expected += 1;
      this->comm.q.pop_back();
    }
    this->become_follower_with_vote_request(term+1, 5, term);
    BOOST_TEST(called);
    BOOST_TEST(cr == messages_type::client_result_not_leader());
  }

  void TestCheckpointSessions()
  {
    // Now make leader and open a session
    uint64_t term = 1;
    this->make_leader(term);
    BOOST_CHECK_EQUAL(1, this->protocol->log_index_end());
    
    open_session(0, term, 1);
  
    // Create and send a command.
    send_command(0, 1, 0, 0, "foo", term, 2);

    // Make a new session and send command using the same sequence number as the other session
    open_session(1, term, 3);
    send_command(1, 3, 0, 0, "bar", term, 4);

    BOOST_TEST(0U == this->l.index_begin());
    BOOST_TEST(5U == this->l.index_end());
    BOOST_TEST(!this->l.empty());
    session_manager.on_checkpoint_request(this->now);
    BOOST_TEST(5U == this->l.index_begin());
    BOOST_TEST(5U == this->l.index_end());
    BOOST_TEST(this->l.empty());

    // This is a bit funky.    We create a new protocol instance
    // that shares the underlying checkpoint store so we can load the just
    // created checkpoint into it without doing a full transfer.!
    {
      // Glue log to log_header_write
      typename raft_type::communicator_type comm2;
      typename raft_type::log_type l2;
      std::shared_ptr<raft_type> protocol2;
      raft::test::log_header_write_test log_header_write2;
      client_communicator_type communicator2;
      state_machine_type state_machine2;

      l2.set_log_header_writer(&log_header_write2);
      protocol2.reset(new raft_type(comm2, l2, this->store, *this->cm.get(), this->now));
      session_manager_type session_manager2(*protocol2.get(), communicator2, state_machine2);
      protocol2->set_state_machine([&session_manager2](log_entry_const_arg_type le, uint64_t idx, size_t leader_id) { session_manager2.apply(le, idx, leader_id); });
      protocol2->set_state_machine_for_checkpoint([&session_manager2](raft::checkpoint_block b, bool is_final) {
                                                    session_manager2.restore_checkpoint_block(raft::slice(b.block_data_, b.block_length_), is_final);
                                                  });
      protocol2->load_checkpoint(this->now);
      BOOST_TEST_REQUIRE(2U == state_machine2.commands.size());
      BOOST_TEST(boost::algorithm::equals("foo", state_machine2.commands[0]));
      BOOST_TEST(boost::algorithm::equals("bar", state_machine2.commands[1]));
      BOOST_TEST(term == protocol2->last_checkpoint_term());
      BOOST_TEST(5U == protocol2->last_checkpoint_index_end());
      BOOST_TEST(this->initial_cluster_time == protocol2->last_checkpoint_cluster_time());
      BOOST_TEST(0U == protocol2->current_term());
      BOOST_TEST(5U == protocol2->applied_index());
      BOOST_TEST(5U == protocol2->commit_index());
      BOOST_TEST(5U == l2.index_begin());
      BOOST_TEST(5U == l2.index_end());
      BOOST_TEST(l2.empty());
      BOOST_TEST(2U == session_manager2.size());
    }
  }  
};

BOOST_AUTO_TEST_CASE_TEMPLATE(TestLinearizableCommand, _TestType, test_types)
{
  SessionManagerTestFixture<_TestType> t;
  t.TestLinearizableCommand();
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TestLinearizableCommandSessionTimeout, _TestType, test_types)
{
  SessionManagerTestFixture<_TestType> t;
  t.TestLinearizableCommandSessionTimeout();
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TestAsyncLinearizableCommand, _TestType, test_types)
{
  SessionManagerTestFixture<_TestType> t;
  t.TestAsyncLinearizableCommand();
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TestReadOnlyQuery, _TestType, test_types)
{
  SessionManagerTestFixture<_TestType> t;
  t.TestReadOnlyQuery();
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TestAsyncReadOnlyQuery, _TestType, test_types)
{
  SessionManagerTestFixture<_TestType> t;
  t.TestAsyncReadOnlyQuery();
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TestReadOnlyQueryNoLongerLeader, _TestType, test_types)
{
  SessionManagerTestFixture<_TestType> t;
  t.TestReadOnlyQueryNoLongerLeader();
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TestReadOnlyQueryUnknownCommitIndex, _TestType, test_types)
{
  SessionManagerTestFixture<_TestType> t;
  t.TestReadOnlyQueryUnknownCommitIndex();
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TestReadOnlyQueryUnknownCommitIndexOutOfDatePeers, _TestType, test_types)
{
  SessionManagerTestFixture<_TestType> t;
  t.TestReadOnlyQueryUnknownCommitIndexOutOfDatePeers();
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TestReadOnlyQueryUnknownCommitIndexOutOfDatePeersThenLoseLeadership, _TestType, test_types)
{
  SessionManagerTestFixture<_TestType> t;
  t.TestReadOnlyQueryUnknownCommitIndexOutOfDatePeersThenLoseLeadership();
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TestCheckpointSessions, _TestType, test_types)
{
  SessionManagerTestFixture<_TestType> t;
  t.TestCheckpointSessions();
}
