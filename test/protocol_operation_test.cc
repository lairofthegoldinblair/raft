#include <map>
#include <set>
#include <boost/log/core.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/sinks/text_file_backend.hpp>
#include <boost/mpl/list.hpp>
#include "flatbuffers/raft_flatbuffer_messages.hh"
#include "native/messages.hh"
#include "util/protocol_operation.hh"

#define BOOST_TEST_MODULE RaftProtocolOperationTests
#include <boost/test/unit_test.hpp>

struct log_init
{
  log_init()
  {
    boost::log::core::get()->set_filter(boost::log::trivial::severity >= boost::log::trivial::trace);
  }    
};

static log_init _log_init_;

// Helper for comparing results
static int32_t string_slice_compare(std::string_view str, raft::slice && sl)
{
  std::string_view tmp(raft::slice::buffer_cast<const char *>(sl), raft::slice::buffer_size(sl));
  return str.compare(tmp);
}

// Test types corresponding to native and flatbuffers
class native_test_type
{
public:
  typedef raft::native::messages messages_type;
  typedef raft::native::builders builders_type;
};

class flatbuffers_test_type
{
public:
  typedef raft::fbs::messages messages_type;
  typedef raft::fbs::builders builders_type;
};

template<typename _TestType>
struct RaftProtocolOperationTestFixture
{
  typedef RaftProtocolOperationTestFixture<_TestType> this_type;
  typedef typename _TestType::messages_type::vote_request_traits_type vote_request_traits;
  typedef typename _TestType::builders_type::vote_request_builder_type vote_request_builder;
  typedef typename _TestType::messages_type::vote_request_traits_type::arg_type vote_request_arg_type;
  typedef typename _TestType::messages_type::vote_response_traits_type vote_response_traits;
  typedef typename _TestType::builders_type::vote_response_builder_type vote_response_builder;
  typedef typename _TestType::messages_type::vote_response_traits_type::arg_type vote_response_arg_type;
  typedef typename _TestType::messages_type::append_entry_request_traits_type append_entry_request_traits;
  typedef typename _TestType::builders_type::append_entry_request_builder_type append_entry_request_builder;
  typedef typename _TestType::messages_type::append_entry_request_traits_type::arg_type append_entry_request_arg_type;
  typedef typename _TestType::messages_type::append_entry_response_traits_type append_entry_response_traits;
  typedef typename _TestType::builders_type::append_entry_response_builder_type append_entry_response_builder;
  typedef typename _TestType::messages_type::append_entry_response_traits_type::arg_type append_entry_response_arg_type;
  typedef typename _TestType::messages_type::append_checkpoint_chunk_request_traits_type append_checkpoint_chunk_request_traits;
  typedef typename _TestType::builders_type::append_checkpoint_chunk_request_builder_type append_checkpoint_chunk_request_builder;
  typedef typename _TestType::messages_type::append_checkpoint_chunk_request_traits_type::arg_type append_checkpoint_chunk_request_arg_type;
  typedef typename _TestType::messages_type::append_checkpoint_chunk_response_traits_type append_checkpoint_chunk_response_traits;
  typedef typename _TestType::builders_type::append_checkpoint_chunk_response_builder_type append_checkpoint_chunk_response_builder;
  typedef typename _TestType::messages_type::append_checkpoint_chunk_response_traits_type::arg_type append_checkpoint_chunk_response_arg_type;

  typedef typename _TestType::messages_type::open_session_request_traits_type open_session_request_traits;
  typedef typename _TestType::builders_type::open_session_request_builder_type open_session_request_builder;
  typedef typename _TestType::messages_type::open_session_request_traits_type::arg_type open_session_request_arg_type;
  typedef typename _TestType::messages_type::close_session_request_traits_type close_session_request_traits;
  typedef typename _TestType::builders_type::close_session_request_builder_type close_session_request_builder;
  typedef typename _TestType::messages_type::close_session_request_traits_type::arg_type close_session_request_arg_type;
  typedef typename _TestType::messages_type::linearizable_command_request_traits_type linearizable_command_request_traits;
  typedef typename _TestType::builders_type::linearizable_command_request_builder_type linearizable_command_request_builder;
  typedef typename _TestType::messages_type::linearizable_command_request_traits_type::arg_type linearizable_command_request_arg_type;

  typedef typename _TestType::builders_type::log_entry_builder_type log_entry_builder;

  struct test_protocol
  {
    typedef std::size_t client_endpoint_type;
    vote_request_arg_type vote_request_message;
    vote_response_arg_type vote_response_message;
    append_entry_request_arg_type append_entry_request_message;
    append_entry_response_arg_type append_entry_response_message;
    append_checkpoint_chunk_request_arg_type append_checkpoint_chunk_request_message;
    append_checkpoint_chunk_response_arg_type append_checkpoint_chunk_response_message;
    std::map<client_endpoint_type, std::chrono::time_point<std::chrono::steady_clock>> open_session_request_messages;
    std::map<client_endpoint_type, std::pair<close_session_request_arg_type, std::chrono::time_point<std::chrono::steady_clock>>> close_session_request_messages;
    std::map<client_endpoint_type, std::pair<linearizable_command_request_arg_type, std::chrono::time_point<std::chrono::steady_clock>>> linearizable_command_request_messages;
    std::map<uint64_t, std::chrono::time_point<std::chrono::steady_clock>> log_sync_messages;
    std::set<std::chrono::time_point<std::chrono::steady_clock>> log_header_sync_messages;

    void on_vote_request(vote_request_arg_type && msg)
    {
      vote_request_message = std::move(msg);
    }
    void on_vote_response(vote_response_arg_type && msg)
    {
      vote_response_message = std::move(msg);
    }
    void on_append_entry_request(append_entry_request_arg_type && msg)
    {
      append_entry_request_message = std::move(msg);
    }
    void on_append_entry_response(append_entry_response_arg_type && msg)
    {
      append_entry_response_message = std::move(msg);
    }
    void on_append_checkpoint_chunk_request(append_checkpoint_chunk_request_arg_type && msg)
    {
      append_checkpoint_chunk_request_message = std::move(msg);
    }
    void on_append_checkpoint_chunk_response(append_checkpoint_chunk_response_arg_type && msg)
    {
      append_checkpoint_chunk_response_message = std::move(msg);
    }
    void on_open_session(const client_endpoint_type & ep,
                         open_session_request_arg_type && req,
                         std::chrono::time_point<std::chrono::steady_clock> clock_now)
    {
      open_session_request_messages[ep] = clock_now;
    }
    void on_close_session(const client_endpoint_type & ep,
                          close_session_request_arg_type && req,
                          std::chrono::time_point<std::chrono::steady_clock> clock_now)
    {
      close_session_request_messages[ep] = std::make_pair(std::move(req), clock_now);
    }
    void on_linearizable_command(const client_endpoint_type & ep,
                                 linearizable_command_request_arg_type && req,
                                 std::chrono::time_point<std::chrono::steady_clock> clock_now)
    {
      linearizable_command_request_messages[ep] = std::make_pair(std::move(req), clock_now);
    }
    void on_log_sync(uint64_t index, std::chrono::time_point<std::chrono::steady_clock> clock_now)
    {
      log_sync_messages[index] = clock_now;
    }
    void on_log_header_sync(std::chrono::time_point<std::chrono::steady_clock> clock_now)
    {
      log_header_sync_messages.insert(clock_now);
    }
  };
  
  typedef raft::util::protocol_operation<test_protocol> protocol_operation_type;
  typedef typename raft::util::protocol_operation<test_protocol>::queue_type protocol_operation_queue_type;
  typedef raft::util::vote_request_operation<typename _TestType::messages_type, test_protocol> vote_request_operation_type;
  typedef raft::util::vote_response_operation<typename _TestType::messages_type, test_protocol> vote_response_operation_type;
  typedef raft::util::append_entry_request_operation<typename _TestType::messages_type, test_protocol> append_entry_request_operation_type;
  typedef raft::util::append_entry_response_operation<typename _TestType::messages_type, test_protocol> append_entry_response_operation_type;
  typedef raft::util::append_checkpoint_chunk_request_operation<typename _TestType::messages_type, test_protocol> append_checkpoint_chunk_request_operation_type;
  typedef raft::util::append_checkpoint_chunk_response_operation<typename _TestType::messages_type, test_protocol> append_checkpoint_chunk_response_operation_type;

  typedef raft::util::open_session_request_operation<typename _TestType::messages_type, test_protocol, typename test_protocol::client_endpoint_type> open_session_request_operation_type;
  typedef raft::util::close_session_request_operation<typename _TestType::messages_type, test_protocol, typename test_protocol::client_endpoint_type> close_session_request_operation_type;
  typedef raft::util::linearizable_command_request_operation<typename _TestType::messages_type, test_protocol, typename test_protocol::client_endpoint_type> linearizable_command_request_operation_type;

  typedef raft::util::log_sync_operation<test_protocol> log_sync_operation_type;
  typedef raft::util::log_header_sync_operation<test_protocol> log_header_sync_operation_type;

  vote_request_operation_type * create_vote_request_operation(uint64_t request_id)
  {
    return new vote_request_operation_type(vote_request_builder().recipient_id(99).term_number(1).candidate_id(887).request_id(request_id).last_log_index(888542).last_log_term(16).finish());
  }
  vote_response_operation_type * create_vote_response_operation(uint64_t request_id)
  {
    return new vote_response_operation_type(vote_response_builder().peer_id(1).term_number(1).request_term_number(1).request_id(request_id).granted(false).finish());
  }
  append_entry_request_operation_type * create_append_entry_request_operation(uint64_t request_id)
  {
    log_entry_builder leb1;
    {	
      auto cb = leb1.term(0).configuration();
      cb.from().server().id(333334323).address("127.0.0.1:7777");
      cb.to();
    }
    auto le1 = leb1.finish();
    auto le2 = log_entry_builder().term(93443434542).data("fjasdjfa;sldfjalsdjfldskfjsdlkfjasldfjl").finish();

    uint64_t recipient_id = 9032345;
    auto msg = append_entry_request_builder().request_id(request_id).recipient_id(recipient_id).term_number(99234).leader_id(23445234).previous_log_index(734725345).previous_log_term(3492385345).leader_commit_index(3483458).entry(le1).entry(le2).finish();
    return new append_entry_request_operation_type(std::move(msg));
  }
  append_entry_response_operation_type * create_append_entry_response_operation(uint64_t request_id)
  {
    auto msg = append_entry_response_builder().recipient_id(222).term_number(10).request_term_number(1).request_id(request_id).begin_index(236).last_index(851).success(false).finish();
    return new append_entry_response_operation_type(std::move(msg));    
  }
  append_checkpoint_chunk_request_operation_type * create_append_checkpoint_chunk_request_operation(uint64_t request_id, const std::string & data_str)
  {
    auto msg = append_checkpoint_chunk_request_builder().recipient_id(222).term_number(10).leader_id(1).request_id(request_id).checkpoint_begin(236).checkpoint_end(8643).data(raft::slice::create(data_str)).checkpoint_done(false).finish();
    return new append_checkpoint_chunk_request_operation_type(std::move(msg));    
  }  
  append_checkpoint_chunk_response_operation_type * create_append_checkpoint_chunk_response_operation(uint64_t request_id)
  {
    auto msg = append_checkpoint_chunk_response_builder().recipient_id(222).term_number(10).request_term_number(1).request_id(request_id).bytes_stored(236).finish();
    return new append_checkpoint_chunk_response_operation_type(std::move(msg));    
  }

  open_session_request_operation_type * create_open_session_request_operation(const std::size_t & ep, std::chrono::time_point<std::chrono::steady_clock> clock_now)
  {
    auto msg = open_session_request_builder().finish();
    return new open_session_request_operation_type(ep, std::move(msg), clock_now);
  }
  close_session_request_operation_type * create_close_session_request_operation(const std::size_t & ep, uint64_t session_id, std::chrono::time_point<std::chrono::steady_clock> clock_now)
  {
    auto msg = close_session_request_builder().session_id(session_id).finish();
    return new close_session_request_operation_type(ep, std::move(msg), clock_now);
  }
  linearizable_command_request_operation_type * create_linearizable_command_request_operation(const std::size_t & ep, uint64_t session_id, const std::string & command_str, std::chrono::time_point<std::chrono::steady_clock> clock_now)
  {
    auto msg = linearizable_command_request_builder().session_id(session_id).first_unacknowledged_sequence_number(62355342).sequence_number(823545).command(raft::slice::create(command_str)).finish();
    return new linearizable_command_request_operation_type(ep, std::move(msg), clock_now);
  }
  
  log_sync_operation_type * create_log_sync_operation(uint64_t index, std::chrono::time_point<std::chrono::steady_clock> clock_now)
  {
    return new log_sync_operation_type(index, clock_now);
  }
  log_header_sync_operation_type * create_log_header_sync_operation(std::chrono::time_point<std::chrono::steady_clock> clock_now)
  {
    return new log_header_sync_operation_type(clock_now);
  }

  void RequestVoteOperationTest()
  {
    uint64_t request_id = 192345;
    {  
      protocol_operation_type * op = create_vote_request_operation(request_id);
      op->destroy();
    }
    {  
      test_protocol proto;
      protocol_operation_type * op = create_vote_request_operation(request_id);
      op->complete(&proto);
      BOOST_CHECK_EQUAL(99U, vote_request_traits::recipient_id(proto.vote_request_message));
      BOOST_CHECK_EQUAL(1U, vote_request_traits::term_number(proto.vote_request_message));
      BOOST_CHECK_EQUAL(887U, vote_request_traits::candidate_id(proto.vote_request_message));
      BOOST_CHECK_EQUAL(request_id, vote_request_traits::request_id(proto.vote_request_message));
      BOOST_CHECK_EQUAL(888542U, vote_request_traits::last_log_index(proto.vote_request_message));
      BOOST_CHECK_EQUAL(16U, vote_request_traits::last_log_term(proto.vote_request_message));
    }
    {  
      protocol_operation_queue_type op_queue;
      for(std::size_t i=0; i<10; ++i) {
        protocol_operation_type * op = create_vote_request_operation(request_id + i);
        op_queue.push_back(*op);
      }
      BOOST_REQUIRE_EQUAL(10U, op_queue.size());
      for(std::size_t i=0; i<10; ++i) {
        test_protocol proto;
        auto & front = op_queue.front();
        op_queue.pop_front();
        front.complete(&proto);
        BOOST_CHECK_EQUAL(99U, vote_request_traits::recipient_id(proto.vote_request_message));
        BOOST_CHECK_EQUAL(1U, vote_request_traits::term_number(proto.vote_request_message));
        BOOST_CHECK_EQUAL(887U, vote_request_traits::candidate_id(proto.vote_request_message));
        BOOST_CHECK_EQUAL(request_id + i, vote_request_traits::request_id(proto.vote_request_message));
        BOOST_CHECK_EQUAL(888542U, vote_request_traits::last_log_index(proto.vote_request_message));
        BOOST_CHECK_EQUAL(16U, vote_request_traits::last_log_term(proto.vote_request_message));
      }
      BOOST_CHECK(op_queue.empty());
    }
  }
  void VoteResponseOperationTest()
  {
    uint64_t request_id = 192345;
    {  
      protocol_operation_type * op = create_vote_response_operation(request_id);
      op->destroy();
    }
    {  
      test_protocol proto;
      protocol_operation_type * op = create_vote_response_operation(request_id);
      op->complete(&proto);
      BOOST_CHECK_EQUAL(1U, vote_response_traits::peer_id(proto.vote_response_message));
      BOOST_CHECK_EQUAL(1U, vote_response_traits::term_number(proto.vote_response_message));
      BOOST_CHECK_EQUAL(1U, vote_response_traits::request_term_number(proto.vote_response_message));
      BOOST_CHECK_EQUAL(request_id, vote_response_traits::request_id(proto.vote_response_message));
      BOOST_CHECK(!vote_response_traits::granted(proto.vote_response_message));
    }
    {  
      protocol_operation_queue_type op_queue;
      for(std::size_t i=0; i<10; ++i) {
        protocol_operation_type * op = create_vote_response_operation(request_id + i);
        op_queue.push_back(*op);
      }
      BOOST_REQUIRE_EQUAL(10U, op_queue.size());
      for(std::size_t i=0; i<10; ++i) {
        test_protocol proto;
        auto & front = op_queue.front();
        op_queue.pop_front();
        front.complete(&proto);
        BOOST_CHECK_EQUAL(1U, vote_response_traits::peer_id(proto.vote_response_message));
        BOOST_CHECK_EQUAL(1U, vote_response_traits::term_number(proto.vote_response_message));
        BOOST_CHECK_EQUAL(1U, vote_response_traits::request_term_number(proto.vote_response_message));
        BOOST_CHECK_EQUAL(request_id+i, vote_response_traits::request_id(proto.vote_response_message));
        BOOST_CHECK(!vote_response_traits::granted(proto.vote_response_message));
      }
      BOOST_CHECK(op_queue.empty());
    }
  }
  void AppendEntryOperationTest()
  {
    uint64_t request_id = 192345;
    {  
      protocol_operation_type * op = create_append_entry_request_operation(request_id);
      op->destroy();
    }
    {  
      test_protocol proto;
      protocol_operation_type * op = create_append_entry_request_operation(request_id);
      op->complete(&proto);
      BOOST_CHECK_EQUAL(request_id, append_entry_request_traits::request_id(proto.append_entry_request_message));
      BOOST_CHECK_EQUAL(9032345U, append_entry_request_traits::recipient_id(proto.append_entry_request_message));
      BOOST_CHECK_EQUAL(2U, append_entry_request_traits::num_entries(proto.append_entry_request_message));
    }
    {  
      protocol_operation_queue_type op_queue;
      for(std::size_t i=0; i<10; ++i) {
        protocol_operation_type * op = create_append_entry_request_operation(request_id + i);
        op_queue.push_back(*op);
      }
      BOOST_REQUIRE_EQUAL(10U, op_queue.size());
      for(std::size_t i=0; i<10; ++i) {
        test_protocol proto;
        auto & front = op_queue.front();
        op_queue.pop_front();
        front.complete(&proto);
        BOOST_CHECK_EQUAL(request_id+i, append_entry_request_traits::request_id(proto.append_entry_request_message));
        BOOST_CHECK_EQUAL(9032345U, append_entry_request_traits::recipient_id(proto.append_entry_request_message));
        BOOST_CHECK_EQUAL(2U, append_entry_request_traits::num_entries(proto.append_entry_request_message));
      }
      BOOST_CHECK(op_queue.empty());
    }
  }
  void AppendEntryResponseOperationTest()
  {
    uint64_t request_id = 192345;
    {  
      protocol_operation_type * op = create_append_entry_response_operation(request_id);
      op->destroy();
    }
    {  
      test_protocol proto;
      protocol_operation_type * op = create_append_entry_response_operation(request_id);
      op->complete(&proto);
      BOOST_CHECK_EQUAL(222U, append_entry_response_traits::recipient_id(proto.append_entry_response_message));
      BOOST_CHECK_EQUAL(10U, append_entry_response_traits::term_number(proto.append_entry_response_message));
      BOOST_CHECK_EQUAL(1U, append_entry_response_traits::request_term_number(proto.append_entry_response_message));
      BOOST_CHECK_EQUAL(request_id, append_entry_response_traits::request_id(proto.append_entry_response_message));
      BOOST_CHECK_EQUAL(236U, append_entry_response_traits::begin_index(proto.append_entry_response_message));
      BOOST_CHECK_EQUAL(851U, append_entry_response_traits::last_index(proto.append_entry_response_message));
      BOOST_CHECK(!append_entry_response_traits::success(proto.append_entry_response_message));
    }
    {  
      protocol_operation_queue_type op_queue;
      for(std::size_t i=0; i<10; ++i) {
        protocol_operation_type * op = create_append_entry_response_operation(request_id + i);
        op_queue.push_back(*op);
      }
      BOOST_REQUIRE_EQUAL(10U, op_queue.size());
      for(std::size_t i=0; i<10; ++i) {
        test_protocol proto;
        auto & front = op_queue.front();
        op_queue.pop_front();
        front.complete(&proto);
        BOOST_CHECK_EQUAL(222U, append_entry_response_traits::recipient_id(proto.append_entry_response_message));
        BOOST_CHECK_EQUAL(10U, append_entry_response_traits::term_number(proto.append_entry_response_message));
        BOOST_CHECK_EQUAL(1U, append_entry_response_traits::request_term_number(proto.append_entry_response_message));
        BOOST_CHECK_EQUAL(request_id+i, append_entry_response_traits::request_id(proto.append_entry_response_message));
        BOOST_CHECK_EQUAL(236U, append_entry_response_traits::begin_index(proto.append_entry_response_message));
        BOOST_CHECK_EQUAL(851U, append_entry_response_traits::last_index(proto.append_entry_response_message));
        BOOST_CHECK(!append_entry_response_traits::success(proto.append_entry_response_message));
      }
      BOOST_CHECK(op_queue.empty());
    }
  }
  void AppendCheckpointChunkOperationTest()
  {
    uint64_t request_id = 192345;
    std::string data_str("This is some checkpoint chunk data");
    {  
      protocol_operation_type * op = create_append_checkpoint_chunk_request_operation(request_id, data_str);
      op->destroy();
    }
    {  
      test_protocol proto;
      protocol_operation_type * op = create_append_checkpoint_chunk_request_operation(request_id, data_str);
      op->complete(&proto);
      BOOST_CHECK_EQUAL(222U, append_checkpoint_chunk_request_traits::recipient_id(proto.append_checkpoint_chunk_request_message));
      BOOST_CHECK_EQUAL(10U, append_checkpoint_chunk_request_traits::term_number(proto.append_checkpoint_chunk_request_message));
      BOOST_CHECK_EQUAL(1U, append_checkpoint_chunk_request_traits::leader_id(proto.append_checkpoint_chunk_request_message));
      BOOST_CHECK_EQUAL(request_id, append_checkpoint_chunk_request_traits::request_id(proto.append_checkpoint_chunk_request_message));
      BOOST_CHECK_EQUAL(236U, append_checkpoint_chunk_request_traits::checkpoint_begin(proto.append_checkpoint_chunk_request_message));
      BOOST_CHECK_EQUAL(8643U, append_checkpoint_chunk_request_traits::checkpoint_end(proto.append_checkpoint_chunk_request_message));
      BOOST_CHECK_EQUAL(0, string_slice_compare(data_str, append_checkpoint_chunk_request_traits::data(proto.append_checkpoint_chunk_request_message)));
      BOOST_CHECK(!append_checkpoint_chunk_request_traits::checkpoint_done(proto.append_checkpoint_chunk_request_message));
    }
    {  
      protocol_operation_queue_type op_queue;
      for(std::size_t i=0; i<10; ++i) {
        protocol_operation_type * op = create_append_checkpoint_chunk_request_operation(request_id + i, data_str);
        op_queue.push_back(*op);
      }
      BOOST_REQUIRE_EQUAL(10U, op_queue.size());
      for(std::size_t i=0; i<10; ++i) {
        test_protocol proto;
        auto & front = op_queue.front();
        op_queue.pop_front();
        front.complete(&proto);
        BOOST_CHECK_EQUAL(222U, append_checkpoint_chunk_request_traits::recipient_id(proto.append_checkpoint_chunk_request_message));
        BOOST_CHECK_EQUAL(10U, append_checkpoint_chunk_request_traits::term_number(proto.append_checkpoint_chunk_request_message));
        BOOST_CHECK_EQUAL(1U, append_checkpoint_chunk_request_traits::leader_id(proto.append_checkpoint_chunk_request_message));
        BOOST_CHECK_EQUAL(request_id+i, append_checkpoint_chunk_request_traits::request_id(proto.append_checkpoint_chunk_request_message));
        BOOST_CHECK_EQUAL(236U, append_checkpoint_chunk_request_traits::checkpoint_begin(proto.append_checkpoint_chunk_request_message));
        BOOST_CHECK_EQUAL(8643U, append_checkpoint_chunk_request_traits::checkpoint_end(proto.append_checkpoint_chunk_request_message));
        BOOST_CHECK_EQUAL(0, string_slice_compare(data_str, append_checkpoint_chunk_request_traits::data(proto.append_checkpoint_chunk_request_message)));
        BOOST_CHECK(!append_checkpoint_chunk_request_traits::checkpoint_done(proto.append_checkpoint_chunk_request_message));
      }
      BOOST_CHECK(op_queue.empty());
    }
  }
  void AppendCheckpointChunkResponseOperationTest()
  {
    uint64_t request_id = 192345;
    {  
      protocol_operation_type * op = create_append_checkpoint_chunk_response_operation(request_id);
      op->destroy();
    }
    {  
      test_protocol proto;
      protocol_operation_type * op = create_append_checkpoint_chunk_response_operation(request_id);
      op->complete(&proto);
      BOOST_CHECK_EQUAL(222U, append_checkpoint_chunk_response_traits::recipient_id(proto.append_checkpoint_chunk_response_message));
      BOOST_CHECK_EQUAL(10U, append_checkpoint_chunk_response_traits::term_number(proto.append_checkpoint_chunk_response_message));
      BOOST_CHECK_EQUAL(1U, append_checkpoint_chunk_response_traits::request_term_number(proto.append_checkpoint_chunk_response_message));
      BOOST_CHECK_EQUAL(request_id, append_checkpoint_chunk_response_traits::request_id(proto.append_checkpoint_chunk_response_message));
      BOOST_CHECK_EQUAL(236U, append_checkpoint_chunk_response_traits::bytes_stored(proto.append_checkpoint_chunk_response_message));
    }
    {  
      protocol_operation_queue_type op_queue;
      for(std::size_t i=0; i<10; ++i) {
        protocol_operation_type * op = create_append_checkpoint_chunk_response_operation(request_id + i);
        op_queue.push_back(*op);
      }
      BOOST_REQUIRE_EQUAL(10U, op_queue.size());
      for(std::size_t i=0; i<10; ++i) {
        test_protocol proto;
        auto & front = op_queue.front();
        op_queue.pop_front();
        front.complete(&proto);
        BOOST_CHECK_EQUAL(222U, append_checkpoint_chunk_response_traits::recipient_id(proto.append_checkpoint_chunk_response_message));
        BOOST_CHECK_EQUAL(10U, append_checkpoint_chunk_response_traits::term_number(proto.append_checkpoint_chunk_response_message));
        BOOST_CHECK_EQUAL(1U, append_checkpoint_chunk_response_traits::request_term_number(proto.append_checkpoint_chunk_response_message));
        BOOST_CHECK_EQUAL(request_id+i, append_checkpoint_chunk_response_traits::request_id(proto.append_checkpoint_chunk_response_message));
        BOOST_CHECK_EQUAL(236U, append_checkpoint_chunk_response_traits::bytes_stored(proto.append_checkpoint_chunk_response_message));
      }
      BOOST_CHECK(op_queue.empty());
    }
  }
  void OpenSessionRequestOperationTest()
  {
    // Client endpoints are passed by reference so we must allocate them
    std::vector<std::size_t> endpoints;
    for(std::size_t i=0; i<10; ++i) {
      endpoints.push_back(i);
    }
    auto now = std::chrono::steady_clock::now();
    {  
      protocol_operation_type * op = create_open_session_request_operation(endpoints[0], now);
      op->destroy();
    }
    {  
      test_protocol proto;
      protocol_operation_type * op = create_open_session_request_operation(endpoints[0], now);
      op->complete(&proto);
      BOOST_CHECK_EQUAL(1U, proto.open_session_request_messages.size());
      BOOST_REQUIRE(proto.open_session_request_messages.end() != proto.open_session_request_messages.find(0));
      BOOST_CHECK(now == proto.open_session_request_messages.at(0));
    }
    {  
      protocol_operation_queue_type op_queue;
      for(std::size_t i=0; i<10; ++i) {
        protocol_operation_type * op = create_open_session_request_operation(endpoints[i], now + std::chrono::seconds(i));
        op_queue.push_back(*op);
      }
      BOOST_REQUIRE_EQUAL(10U, op_queue.size());
      for(std::size_t i=0; i<10; ++i) {
        test_protocol proto;
        auto & front = op_queue.front();
        op_queue.pop_front();
        front.complete(&proto);
        BOOST_CHECK_EQUAL(1U, proto.open_session_request_messages.size());
        BOOST_REQUIRE(proto.open_session_request_messages.end() != proto.open_session_request_messages.find(i));
        BOOST_CHECK(now+std::chrono::seconds(i) == proto.open_session_request_messages.at(i));
      }
      BOOST_CHECK(op_queue.empty());
    }
  }
  void CloseSessionRequestOperationTest()
  {
    // Client endpoints are passed by reference so we must allocate them
    std::vector<std::size_t> endpoints;
    for(std::size_t i=0; i<10; ++i) {
      endpoints.push_back(i);
    }
    auto now = std::chrono::steady_clock::now();
    uint64_t session_id = 723545234U;
    {  
      protocol_operation_type * op = create_close_session_request_operation(endpoints[0], session_id, now);
      op->destroy();
    }
    {  
      test_protocol proto;
      protocol_operation_type * op = create_close_session_request_operation(endpoints[0], session_id, now);
      op->complete(&proto);
      BOOST_CHECK_EQUAL(1U, proto.close_session_request_messages.size());
      BOOST_REQUIRE(proto.close_session_request_messages.end() != proto.close_session_request_messages.find(0));
      BOOST_CHECK_EQUAL(session_id, close_session_request_traits::session_id(proto.close_session_request_messages.at(0).first));
      BOOST_CHECK(now == proto.close_session_request_messages.at(0).second);
    }
    {  
      protocol_operation_queue_type op_queue;
      for(std::size_t i=0; i<10; ++i) {
        protocol_operation_type * op = create_close_session_request_operation(endpoints[i], session_id+i, now + std::chrono::seconds(i));
        op_queue.push_back(*op);
      }
      BOOST_REQUIRE_EQUAL(10U, op_queue.size());
      for(std::size_t i=0; i<10; ++i) {
        test_protocol proto;
        auto & front = op_queue.front();
        op_queue.pop_front();
        front.complete(&proto);
        BOOST_CHECK_EQUAL(1U, proto.close_session_request_messages.size());
        BOOST_REQUIRE(proto.close_session_request_messages.end() != proto.close_session_request_messages.find(i));
        BOOST_CHECK_EQUAL(session_id+i, close_session_request_traits::session_id(proto.close_session_request_messages.at(i).first));
        BOOST_CHECK(now+std::chrono::seconds(i) == proto.close_session_request_messages.at(i).second);
      }
      BOOST_CHECK(op_queue.empty());
    }
  }
  void LinearizableCommandOperationTest()
  {
    // Client endpoints are passed by reference so we must allocate them
    std::vector<std::size_t> endpoints;
    for(std::size_t i=0; i<10; ++i) {
      endpoints.push_back(i);
    }
    auto now = std::chrono::steady_clock::now();
    uint64_t session_id = 723545234U;
    std::string command_str("This is a command string");
    {  
      protocol_operation_type * op = create_linearizable_command_request_operation(endpoints[0], session_id, command_str, now);
      op->destroy();
    }
    {  
      test_protocol proto;
      protocol_operation_type * op = create_linearizable_command_request_operation(endpoints[0], session_id, command_str, now);
      op->complete(&proto);
      BOOST_CHECK_EQUAL(1U, proto.linearizable_command_request_messages.size());
      BOOST_REQUIRE(proto.linearizable_command_request_messages.end() != proto.linearizable_command_request_messages.find(0));
      BOOST_CHECK_EQUAL(session_id, linearizable_command_request_traits::session_id(proto.linearizable_command_request_messages.at(0).first));
      BOOST_CHECK_EQUAL(62355342U, linearizable_command_request_traits::first_unacknowledged_sequence_number(proto.linearizable_command_request_messages.at(0).first));
      BOOST_CHECK_EQUAL(823545U, linearizable_command_request_traits::sequence_number(proto.linearizable_command_request_messages.at(0).first));
      BOOST_CHECK_EQUAL(0, string_slice_compare(command_str, linearizable_command_request_traits::command(proto.linearizable_command_request_messages.at(0).first)));
      BOOST_CHECK(now == proto.linearizable_command_request_messages.at(0).second);
    }
    {  
      protocol_operation_queue_type op_queue;
      for(std::size_t i=0; i<10; ++i) {
        protocol_operation_type * op = create_linearizable_command_request_operation(endpoints[i], session_id, command_str, now + std::chrono::seconds(i));
        op_queue.push_back(*op);
      }
      BOOST_REQUIRE_EQUAL(10U, op_queue.size());
      for(std::size_t i=0; i<10; ++i) {
        test_protocol proto;
        auto & front = op_queue.front();
        op_queue.pop_front();
        front.complete(&proto);
        BOOST_CHECK_EQUAL(1U, proto.linearizable_command_request_messages.size());
        BOOST_REQUIRE(proto.linearizable_command_request_messages.end() != proto.linearizable_command_request_messages.find(i));
        BOOST_CHECK_EQUAL(session_id, linearizable_command_request_traits::session_id(proto.linearizable_command_request_messages.at(i).first));
        BOOST_CHECK_EQUAL(62355342U, linearizable_command_request_traits::first_unacknowledged_sequence_number(proto.linearizable_command_request_messages.at(i).first));
        BOOST_CHECK_EQUAL(823545U, linearizable_command_request_traits::sequence_number(proto.linearizable_command_request_messages.at(i).first));
        BOOST_CHECK_EQUAL(0, string_slice_compare(command_str, linearizable_command_request_traits::command(proto.linearizable_command_request_messages.at(i).first)));
        BOOST_CHECK(now+std::chrono::seconds(i) == proto.linearizable_command_request_messages.at(i).second);
      }
      BOOST_CHECK(op_queue.empty());
    }
  }
  void LogSyncOperationTest()
  {
    auto now = std::chrono::steady_clock::now();
    uint64_t index = 723545234U;
    {  
      protocol_operation_type * op = create_log_sync_operation(index, now);
      op->destroy();
    }
    {  
      test_protocol proto;
      protocol_operation_type * op = create_log_sync_operation(index, now);
      op->complete(&proto);
      BOOST_CHECK_EQUAL(1U, proto.log_sync_messages.size());
      BOOST_REQUIRE(proto.log_sync_messages.end() != proto.log_sync_messages.find(index));
      BOOST_CHECK(now == proto.log_sync_messages.at(index));
    }
    {  
      protocol_operation_queue_type op_queue;
      for(std::size_t i=0; i<10; ++i) {
        protocol_operation_type * op = create_log_sync_operation(index+i, now + std::chrono::seconds(i));
        op_queue.push_back(*op);
      }
      BOOST_REQUIRE_EQUAL(10U, op_queue.size());
      for(std::size_t i=0; i<10; ++i) {
        test_protocol proto;
        auto & front = op_queue.front();
        op_queue.pop_front();
        front.complete(&proto);
        BOOST_CHECK_EQUAL(1U, proto.log_sync_messages.size());
        BOOST_REQUIRE(proto.log_sync_messages.end() != proto.log_sync_messages.find(index+i));
        BOOST_CHECK(now + std::chrono::seconds(i) == proto.log_sync_messages.at(index+i));
      }
      BOOST_CHECK(op_queue.empty());
    }
  }
  void LogHeaderSyncOperationTest()
  {
    auto now = std::chrono::steady_clock::now();
    {  
      protocol_operation_type * op = create_log_header_sync_operation(now);
      op->destroy();
    }
    {  
      test_protocol proto;
      protocol_operation_type * op = create_log_header_sync_operation(now);
      op->complete(&proto);
      BOOST_REQUIRE_EQUAL(1U, proto.log_header_sync_messages.size());
      BOOST_CHECK(now == *proto.log_header_sync_messages.begin());
    }
    {  
      protocol_operation_queue_type op_queue;
      for(std::size_t i=0; i<10; ++i) {
        protocol_operation_type * op = create_log_header_sync_operation(now + std::chrono::seconds(i));
        op_queue.push_back(*op);
      }
      BOOST_REQUIRE_EQUAL(10U, op_queue.size());
      for(std::size_t i=0; i<10; ++i) {
        test_protocol proto;
        auto & front = op_queue.front();
        op_queue.pop_front();
        front.complete(&proto);
        BOOST_REQUIRE_EQUAL(1U, proto.log_header_sync_messages.size());
        BOOST_CHECK(now + std::chrono::seconds(i) == *proto.log_header_sync_messages.begin());
      }
      BOOST_CHECK(op_queue.empty());
    }
  }
  void HeterogeneousQueueTest()
  {
    uint64_t request_id = 192345;
    {  
      protocol_operation_queue_type op_queue;
      for(std::size_t i=0; i<10; ++i) {
        protocol_operation_type * op = (0 == i%2) ?
          static_cast<protocol_operation_type *>(create_vote_request_operation(request_id + i)) :
          static_cast<protocol_operation_type *>(create_vote_response_operation(request_id + i));
        op_queue.push_back(*op);
      }
      BOOST_REQUIRE_EQUAL(10U, op_queue.size());
      for(std::size_t i=0; i<10; ++i) {
        test_protocol proto;
        auto & front = op_queue.front();
        op_queue.pop_front();
        front.complete(&proto);
        if (0 == i%2) {
          BOOST_CHECK_EQUAL(99U, vote_request_traits::recipient_id(proto.vote_request_message));
          BOOST_CHECK_EQUAL(1U, vote_request_traits::term_number(proto.vote_request_message));
          BOOST_CHECK_EQUAL(887U, vote_request_traits::candidate_id(proto.vote_request_message));
          BOOST_CHECK_EQUAL(request_id + i, vote_request_traits::request_id(proto.vote_request_message));
          BOOST_CHECK_EQUAL(888542U, vote_request_traits::last_log_index(proto.vote_request_message));
          BOOST_CHECK_EQUAL(16U, vote_request_traits::last_log_term(proto.vote_request_message));
        } else {
          BOOST_CHECK_EQUAL(1U, vote_response_traits::peer_id(proto.vote_response_message));
          BOOST_CHECK_EQUAL(1U, vote_response_traits::term_number(proto.vote_response_message));
          BOOST_CHECK_EQUAL(1U, vote_response_traits::request_term_number(proto.vote_response_message));
          BOOST_CHECK_EQUAL(request_id+i, vote_response_traits::request_id(proto.vote_response_message));
          BOOST_CHECK(!vote_response_traits::granted(proto.vote_response_message));
        }
      }
      BOOST_CHECK(op_queue.empty());
    }
  }
};

typedef boost::mpl::list<native_test_type, flatbuffers_test_type> test_types;

BOOST_AUTO_TEST_CASE_TEMPLATE(RequestVoteOperationTest, _TestType, test_types)
{
  RaftProtocolOperationTestFixture<_TestType> fixture;
  fixture.RequestVoteOperationTest();
}

BOOST_AUTO_TEST_CASE_TEMPLATE(VoteResponseOperationTest, _TestType, test_types)
{
  RaftProtocolOperationTestFixture<_TestType> fixture;
  fixture.VoteResponseOperationTest();
}

BOOST_AUTO_TEST_CASE_TEMPLATE(AppendEntryOperationTest, _TestType, test_types)
{
  RaftProtocolOperationTestFixture<_TestType> fixture;
  fixture.AppendEntryOperationTest();
}

BOOST_AUTO_TEST_CASE_TEMPLATE(AppendEntryResponseOperationTest, _TestType, test_types)
{
  RaftProtocolOperationTestFixture<_TestType> fixture;
  fixture.AppendEntryResponseOperationTest();
}

BOOST_AUTO_TEST_CASE_TEMPLATE(AppendCheckpointChunkOperationTest, _TestType, test_types)
{
  RaftProtocolOperationTestFixture<_TestType> fixture;
  fixture.AppendCheckpointChunkOperationTest();
}

BOOST_AUTO_TEST_CASE_TEMPLATE(AppendCheckpointChunkResponseOperationTest, _TestType, test_types)
{
  RaftProtocolOperationTestFixture<_TestType> fixture;
  fixture.AppendCheckpointChunkResponseOperationTest();
}

BOOST_AUTO_TEST_CASE_TEMPLATE(OpenSessionRequestOperationTest, _TestType, test_types)
{
  RaftProtocolOperationTestFixture<_TestType> fixture;
  fixture.OpenSessionRequestOperationTest();
}

BOOST_AUTO_TEST_CASE_TEMPLATE(CloseSessionRequestOperationTest, _TestType, test_types)
{
  RaftProtocolOperationTestFixture<_TestType> fixture;
  fixture.CloseSessionRequestOperationTest();
}

BOOST_AUTO_TEST_CASE_TEMPLATE(LinearizableCommandOperationTest, _TestType, test_types)
{
  RaftProtocolOperationTestFixture<_TestType> fixture;
  fixture.LinearizableCommandOperationTest();
}

BOOST_AUTO_TEST_CASE_TEMPLATE(LogSyncOperationTest, _TestType, test_types)
{
  RaftProtocolOperationTestFixture<_TestType> fixture;
  fixture.LogSyncOperationTest();
}

BOOST_AUTO_TEST_CASE_TEMPLATE(LogHeaderSyncOperationTest, _TestType, test_types)
{
  RaftProtocolOperationTestFixture<_TestType> fixture;
  fixture.LogHeaderSyncOperationTest();
}

BOOST_AUTO_TEST_CASE_TEMPLATE(HeterogeneousQueueTest, _TestType, test_types)
{
  RaftProtocolOperationTestFixture<_TestType> fixture;
  fixture.HeterogeneousQueueTest();
}

