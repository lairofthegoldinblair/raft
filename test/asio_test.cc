#include <array>
#include <chrono>
#include <thread>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/asio.hpp>
#include <boost/format.hpp>
#include <boost/log/core.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/sinks/text_file_backend.hpp>
#include <boost/mpl/list.hpp>

#include "asio/asio_server.hh"
#include "asio/asio_block_device.hh"
#include "leveldb_log.hh"
#include "native/messages.hh"
#include "native/serialization.hh"
#include "flatbuffers/raft_flatbuffer_messages.hh"
#include "flatbuffers/serialization.hh"
#include "posix_file.hh"
#include "protocol.hh"

#define BOOST_TEST_MODULE RaftTests
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

BOOST_AUTO_TEST_CASE(LevelDBLogWriterTest)
{
  raft::posix::writable_file device("raft_test_log.bin");
  raft::leveldb::log_writer<raft::posix::writable_file> writer(device);
  std::vector<uint8_t> entry({ 0, 1, 2, 3, 4 });
  writer.append_record(&entry[0], entry.size());
}

BOOST_AUTO_TEST_CASE(LevelDBAsioLogWriterTest)
{
  boost::asio::io_service ios;
  int fd = ::open("raft_test_log.bin", O_WRONLY);
  raft::asio::writable_file device(ios, fd);
  raft::leveldb::log_writer<raft::asio::writable_file> writer(device);
  std::vector<uint8_t> entry({ 100, 101, 102, 104, 105 });
  writer.append_record(&entry[0], entry.size());
  ios.run();
  ::close(fd);
}

BOOST_AUTO_TEST_CASE(LevelDBAsioSequenceLogWriterTest)
{
  boost::asio::io_service ios;
  int fd = ::open("raft_test_log.bin", O_WRONLY);
  raft::asio::writable_file device(ios, fd);
  raft::leveldb::log_writer<raft::asio::writable_file> writer(device);
  for(int i=0; i<10000; ++i) {
    std::string a((boost::format("This begins log record %1%") % i).str());
    std::string b((boost::format("This continues the log record %1%") % i).str());
    std::string c((boost::format("This finishes log record %1%") % i).str());
    std::array<raft::slice, 3> slices = { raft::slice::create(a), raft::slice::create(b), raft::slice::create(c) };
    writer.append_record(slices);
  }
  
  ios.run();
  ::close(fd);
}

// TODO: Do some templating in log writer so we don't need to do this

struct stringstream_adapter
{
  std::stringstream & str_;
  stringstream_adapter(std::stringstream & str) : str_(str) {}
  
  void write(const uint8_t * buf, std::size_t sz)
  {
    str_.write(reinterpret_cast<const char *>(buf), sz);
  }

  void flush()
  {
    str_.flush();
  }
};

BOOST_AUTO_TEST_CASE(LevelDBStringStreamLogReaderTest)
{
  std::stringstream sstr;
  stringstream_adapter adapted(sstr);
  raft::leveldb::log_writer<stringstream_adapter> writer(adapted);
  for(int i=0; i<10000; ++i) {
    std::string a((boost::format("This begins log record %1%") % i).str());
    std::string b((boost::format("This continues the log record %1%") % i).str());
    std::string c((boost::format("This finishes log record %1%") % i).str());
    std::array<raft::slice, 3> slices = { raft::slice::create(a), raft::slice::create(b), raft::slice::create(c) };
    writer.append_record(slices);
  }

  std::string log_buffer = sstr.str();
  {
    // Lets read all of the records
    raft::leveldb::record_reader r;
    int next_record = 0;
    for(std::size_t next_block=0; next_block < log_buffer.size(); next_block += raft::leveldb::BLOCK_SIZE) {
      std::size_t block_size = (std::min)(raft::leveldb::BLOCK_SIZE, log_buffer.size()-next_block);
      r.write_block(raft::slice(reinterpret_cast<const uint8_t *>(&log_buffer[next_block]), block_size));
      r.run_all_enabled();
      while(r.can_read_record()) {
  	auto record_buffers = r.read_record();
  	if (0 == record_buffers.size()) {
  	  // EOF on stream
  	  break;
  	}
  	std::string expected((boost::format("This begins log record %1%"
  					    "This continues the log record %1%"
  					    "This finishes log record %1%") % next_record).str());
  	BOOST_CHECK_EQUAL(expected.size(), raft::slice::total_size(record_buffers));
  	std::string actual;
  	for(auto & s : record_buffers) {
  	  actual.append(raft::slice::buffer_cast<const char *>(s), raft::slice::buffer_size(s));
  	}
  	BOOST_CHECK(boost::algorithm::equals(actual, expected));
  	next_record += 1;
  	r.run_all_enabled();
      }
    }
    BOOST_CHECK_EQUAL(10000, next_record);
  }
  {
    // Lets read all of the records starting from the third block for which the
    // first complete record is 701.
    // Set syncing mode to suppress errors
    raft::leveldb::record_reader r(true);
    int next_record = 701;
    for(std::size_t next_block=2*raft::leveldb::BLOCK_SIZE; next_block < log_buffer.size(); next_block += raft::leveldb::BLOCK_SIZE) {
      std::size_t block_size = (std::min)(raft::leveldb::BLOCK_SIZE, log_buffer.size()-next_block);
      r.write_block(raft::slice(reinterpret_cast<const uint8_t *>(&log_buffer[next_block]), block_size));
      r.run_all_enabled();
      while(r.can_read_record()) {
  	auto record_buffers = r.read_record();
  	if (0 == record_buffers.size()) {
  	  // EOF on stream
  	  break;
  	}
  	std::string expected((boost::format("This begins log record %1%"
  					    "This continues the log record %1%"
  					    "This finishes log record %1%") % next_record).str());
  	BOOST_CHECK_EQUAL(expected.size(), raft::slice::total_size(record_buffers));
  	std::string actual;
  	for(auto & s : record_buffers) {
  	  actual.append(raft::slice::buffer_cast<const char *>(s), raft::slice::buffer_size(s));
  	}
  	BOOST_CHECK(boost::algorithm::equals(actual, expected));
  	next_record += 1;
  	r.run_all_enabled();
      }
    }
    BOOST_CHECK_EQUAL(10000, next_record);
  }
}

BOOST_AUTO_TEST_CASE(LevelDBMiddleFragmentSingleSliceTest)
{
  std::stringstream sstr;
  stringstream_adapter adapted(sstr);
  raft::leveldb::log_writer<stringstream_adapter> writer(adapted);
  std::vector<raft::slice> slices;
  std::stringstream expected;
  expected << "This begins the log record";
  for(int i=0; i<3000; ++i) {
    expected << (boost::format("This continues the log record %1%") % i).str();
  }
  expected << "This finishes the log record";
  std::string rec = expected.str();  
  slices.push_back(raft::slice::create(rec));
  writer.append_record(slices);

  std::string log_buffer = sstr.str();
  {
    // Lets read all of the records
    raft::leveldb::record_reader r;
    int next_record = 0;
    for(std::size_t next_block=0; next_block < log_buffer.size(); next_block += raft::leveldb::BLOCK_SIZE) {
      std::size_t block_size = (std::min)(raft::leveldb::BLOCK_SIZE, log_buffer.size()-next_block);
      r.write_block(raft::slice(reinterpret_cast<const uint8_t *>(&log_buffer[next_block]), block_size));
      r.run_all_enabled();
      while(r.can_read_record()) {
  	auto record_buffers = r.read_record();
  	if (0 == record_buffers.size()) {
  	  // EOF on stream
  	  break;
  	}
  	std::stringstream actual;
  	for(auto & s : record_buffers) {
  	  actual.write(raft::slice::buffer_cast<const char *>(s), raft::slice::buffer_size(s));
  	}
  	BOOST_CHECK(boost::algorithm::equals(actual.str(), expected.str()));
  	next_record += 1;
  	r.run_all_enabled();
      }
    }
    BOOST_CHECK_EQUAL(1, next_record);
  }
}

BOOST_AUTO_TEST_CASE(LevelDBMiddleFragmentManySlicesTest)
{
  std::stringstream sstr;
  stringstream_adapter adapted(sstr);
  raft::leveldb::log_writer<stringstream_adapter> writer(adapted);
  std::vector<std::string> strings;
  std::vector<raft::slice> slices;
  std::stringstream expected;
  strings.push_back("This begins the log record");
  expected << strings.back();
  slices.push_back(raft::slice::create(strings.back()));
  for(int i=0; i<3000; ++i) {
    strings.push_back((boost::format("This continues the log record %1%") % i).str());
    expected << strings.back();
    slices.push_back(raft::slice::create(strings.back()));
  }
  strings.push_back("This finishes the log record");
  expected << strings.back();
  slices.push_back(raft::slice::create(strings.back()));
  writer.append_record(slices);

  std::string log_buffer = sstr.str();
  {
    // Lets read all of the records
    raft::leveldb::record_reader r;
    int next_record = 0;
    for(std::size_t next_block=0; next_block < log_buffer.size(); next_block += raft::leveldb::BLOCK_SIZE) {
      std::size_t block_size = (std::min)(raft::leveldb::BLOCK_SIZE, log_buffer.size()-next_block);
      r.write_block(raft::slice(reinterpret_cast<const uint8_t *>(&log_buffer[next_block]), block_size));
      r.run_all_enabled();
      while(r.can_read_record()) {
  	auto record_buffers = r.read_record();
  	if (0 == record_buffers.size()) {
  	  // EOF on stream
  	  break;
  	}
  	std::stringstream actual;
  	for(auto & s : record_buffers) {
  	  actual.write(raft::slice::buffer_cast<const char *>(s), raft::slice::buffer_size(s));
  	}
  	BOOST_CHECK(boost::algorithm::equals(actual.str(), expected.str()));
  	next_record += 1;
  	r.run_all_enabled();
      }
    }
    BOOST_CHECK_EQUAL(1, next_record);
  }
}

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

// typedef boost::mpl::list<flatbuffers_test_type> test_types;
typedef boost::mpl::list<native_test_type, flatbuffers_test_type> test_types;

BOOST_AUTO_TEST_CASE_TEMPLATE(RaftRequestVoteSerializationTest, _TestType, test_types)
{
  typedef typename _TestType::messages_type::request_vote_traits_type request_vote_traits;
  typedef typename _TestType::builders_type::request_vote_builder_type request_vote_builder;
  typedef typename _TestType::serialization_type serialization_type;
  
  auto msg = request_vote_builder().recipient_id(99).term_number(1).candidate_id(887).request_id(192345).last_log_index(888542).last_log_term(16).finish();

  auto result = serialization_type::serialize(std::move(msg));

  auto msg2 = serialization_type::deserialize_request_vote(std::move(result));
  BOOST_CHECK_EQUAL(99U, request_vote_traits::recipient_id(msg2));
  BOOST_CHECK_EQUAL(1U, request_vote_traits::term_number(msg2));
  BOOST_CHECK_EQUAL(887U, request_vote_traits::candidate_id(msg2));
  BOOST_CHECK_EQUAL(192345U, request_vote_traits::request_id(msg2));
  BOOST_CHECK_EQUAL(888542U, request_vote_traits::last_log_index(msg2));
  BOOST_CHECK_EQUAL(16U, request_vote_traits::last_log_term(msg2));
}

BOOST_AUTO_TEST_CASE_TEMPLATE(RaftVoteResponseSerializationTest, _TestType, test_types)
{
  typedef typename _TestType::messages_type::vote_response_traits_type vote_response_traits;
  typedef typename _TestType::builders_type::vote_response_builder_type vote_response_builder;
  typedef typename _TestType::serialization_type serialization_type;
  
  auto msg = vote_response_builder().peer_id(1).term_number(1).request_term_number(1).request_id(192345).granted(false).finish();

  auto result = serialization_type::serialize(std::move(msg));

  auto msg2 = serialization_type::deserialize_vote_response(std::move(result));
  BOOST_CHECK_EQUAL(1U, vote_response_traits::peer_id(msg2));
  BOOST_CHECK_EQUAL(1U, vote_response_traits::term_number(msg2));
  BOOST_CHECK_EQUAL(1U, vote_response_traits::request_term_number(msg2));
  BOOST_CHECK_EQUAL(192345U, vote_response_traits::request_id(msg2));
  BOOST_CHECK(!vote_response_traits::granted(msg2));
}

BOOST_AUTO_TEST_CASE_TEMPLATE(RaftAsioSerializationTest, _TestType, test_types)
{
  typedef typename _TestType::messages_type::append_entry_traits_type append_entry_traits;
  typedef typename _TestType::builders_type::append_entry_builder_type append_entry_builder;
  typedef typename _TestType::builders_type::log_entry_builder_type log_entry_builder;
  typedef raft::asio::serialization<typename _TestType::messages_type, typename _TestType::serialization_type> serialization_type;
  
  log_entry_builder leb1;
  {	
    auto cb = leb1.term(0).configuration();
    cb.from().server().id(333334323).address("127.0.0.1:7777");
    cb.to();
  }
  auto le1 = leb1.finish();
  auto le2 = log_entry_builder().term(93443434542).data("fjasdjfa;sldfjalsdjfldskfjsdlkfjasldfjl").finish();

  uint64_t request_id = 597134;
  uint64_t recipient_id = 9032345;
  auto msg = append_entry_builder().request_id(request_id).recipient_id(recipient_id).term_number(99234).leader_id(23445234).previous_log_index(734725345).previous_log_term(3492385345).leader_commit_index(3483458).entry(le1).entry(le2).finish();

  auto result = serialization_type::serialize(boost::asio::buffer(new uint8_t [1024], 1024), std::move(msg));
  auto header = boost::asio::buffer_cast<const raft::asio::rpc_header *>(result.first[0]);
  BOOST_CHECK_EQUAL(2U, header->operation);
  BOOST_CHECK_EQUAL(boost::asio::buffer_size(result.first), header->payload_length+sizeof(raft::asio::rpc_header));
  auto msg1 = serialization_type::deserialize_append_entry(result.first[1], std::move(result.second));
  BOOST_CHECK_EQUAL(request_id, append_entry_traits::request_id(msg1));
  BOOST_CHECK_EQUAL(recipient_id, append_entry_traits::recipient_id(msg1));
  BOOST_CHECK_EQUAL(2U, append_entry_traits::num_entries(msg1));
}

BOOST_AUTO_TEST_CASE_TEMPLATE(RaftAppendResponseSerializationTest, _TestType, test_types)
{
  typedef typename _TestType::messages_type::append_entry_response_traits_type append_entry_response_traits;
  typedef typename _TestType::builders_type::append_response_builder_type append_response_builder;
  typedef typename _TestType::serialization_type serialization_type;
  
  auto msg = append_response_builder().recipient_id(222).term_number(10).request_term_number(1).request_id(192345).begin_index(236).last_index(851).success(false).finish();

  auto result = serialization_type::serialize(std::move(msg));

  auto msg2 = serialization_type::deserialize_append_entry_response(std::move(result));
  BOOST_CHECK_EQUAL(222U, append_entry_response_traits::recipient_id(msg2));
  BOOST_CHECK_EQUAL(10U, append_entry_response_traits::term_number(msg2));
  BOOST_CHECK_EQUAL(1U, append_entry_response_traits::request_term_number(msg2));
  BOOST_CHECK_EQUAL(192345U, append_entry_response_traits::request_id(msg2));
  BOOST_CHECK_EQUAL(236U, append_entry_response_traits::begin_index(msg2));
  BOOST_CHECK_EQUAL(851U, append_entry_response_traits::last_index(msg2));
  BOOST_CHECK(!append_entry_response_traits::success(msg2));
}

BOOST_AUTO_TEST_CASE_TEMPLATE(RaftAppendCheckpointChunkSerializationTest, _TestType, test_types)
{
  typedef typename _TestType::messages_type::append_checkpoint_chunk_traits_type append_checkpoint_chunk_traits;
  typedef typename _TestType::builders_type::append_checkpoint_chunk_builder_type append_checkpoint_chunk_builder;
  typedef typename _TestType::serialization_type serialization_type;

  std::string data_str("This is some checkpoint chunk data");
  auto msg = append_checkpoint_chunk_builder().recipient_id(222).term_number(10).leader_id(1).request_id(192345).checkpoint_begin(236).checkpoint_end(8643).data(raft::slice::create(data_str)).checkpoint_done(false).finish();

  auto result = serialization_type::serialize(std::move(msg));

  auto msg2 = serialization_type::deserialize_append_checkpoint_chunk(std::move(result));
  BOOST_CHECK_EQUAL(222U, append_checkpoint_chunk_traits::recipient_id(msg2));
  BOOST_CHECK_EQUAL(10U, append_checkpoint_chunk_traits::term_number(msg2));
  BOOST_CHECK_EQUAL(1U, append_checkpoint_chunk_traits::leader_id(msg2));
  BOOST_CHECK_EQUAL(192345U, append_checkpoint_chunk_traits::request_id(msg2));
  BOOST_CHECK_EQUAL(236U, append_checkpoint_chunk_traits::checkpoint_begin(msg2));
  BOOST_CHECK_EQUAL(8643U, append_checkpoint_chunk_traits::checkpoint_end(msg2));
  BOOST_CHECK_EQUAL(0, string_slice_compare(data_str, append_checkpoint_chunk_traits::data(msg2)));
  BOOST_CHECK(!append_checkpoint_chunk_traits::checkpoint_done(msg2));
}

BOOST_AUTO_TEST_CASE_TEMPLATE(RaftAppendCheckpointChunkResponseSerializationTest, _TestType, test_types)
{
  typedef typename _TestType::messages_type::append_checkpoint_chunk_response_traits_type append_checkpoint_chunk_response_traits;
  typedef typename _TestType::builders_type::append_checkpoint_chunk_response_builder_type append_checkpoint_chunk_response_builder;
  typedef typename _TestType::serialization_type serialization_type;
  
  auto msg = append_checkpoint_chunk_response_builder().recipient_id(222).term_number(10).request_term_number(1).request_id(192345).bytes_stored(236).finish();

  auto result = serialization_type::serialize(std::move(msg));

  auto msg2 = serialization_type::deserialize_append_checkpoint_chunk_response(std::move(result));
  BOOST_CHECK_EQUAL(222U, append_checkpoint_chunk_response_traits::recipient_id(msg2));
  BOOST_CHECK_EQUAL(10U, append_checkpoint_chunk_response_traits::term_number(msg2));
  BOOST_CHECK_EQUAL(1U, append_checkpoint_chunk_response_traits::request_term_number(msg2));
  BOOST_CHECK_EQUAL(192345U, append_checkpoint_chunk_response_traits::request_id(msg2));
  BOOST_CHECK_EQUAL(236U, append_checkpoint_chunk_response_traits::bytes_stored(msg2));
}

BOOST_AUTO_TEST_CASE_TEMPLATE(RaftOpenSessionRequestSerializationTest, _TestType, test_types)
{
  typedef typename _TestType::builders_type::open_session_request_builder_type open_session_builder;
  typedef raft::asio::serialization<typename _TestType::messages_type, typename _TestType::serialization_type> serialization_type;
  
  open_session_builder bld;
  auto msg = bld.finish();

  auto result = serialization_type::serialize(boost::asio::buffer(new uint8_t [1024], 1024), std::move(msg));
  auto header = boost::asio::buffer_cast<const raft::asio::rpc_header *>(result.first[0]);
  BOOST_CHECK_EQUAL(serialization_type::OPEN_SESSION_REQUEST, header->operation);
  BOOST_CHECK_EQUAL(boost::asio::buffer_size(result.first), header->payload_length+sizeof(raft::asio::rpc_header));
  auto msg1 = serialization_type::deserialize_open_session_request(result.first[1], std::move(result.second));
}

BOOST_AUTO_TEST_CASE_TEMPLATE(RaftOpenSessionResponseSerializationTest, _TestType, test_types)
{
  typedef typename _TestType::messages_type::open_session_response_traits_type open_session_response_traits;
  typedef typename _TestType::builders_type::open_session_response_builder_type open_session_builder;
  typedef raft::asio::serialization<typename _TestType::messages_type, typename _TestType::serialization_type> serialization_type;
  
  open_session_builder bld;
  auto msg = bld.session_id(9975314).finish();

  auto result = serialization_type::serialize(boost::asio::buffer(new uint8_t [1024], 1024), std::move(msg));
  auto header = boost::asio::buffer_cast<const raft::asio::rpc_header *>(result.first[0]);
  BOOST_CHECK_EQUAL(serialization_type::OPEN_SESSION_RESPONSE, header->operation);
  BOOST_CHECK_EQUAL(boost::asio::buffer_size(result.first), header->payload_length+sizeof(raft::asio::rpc_header));
  auto msg1 = serialization_type::deserialize_open_session_response(result.first[1], std::move(result.second));
  BOOST_CHECK_EQUAL(9975314U, open_session_response_traits::session_id(msg1));
}

BOOST_AUTO_TEST_CASE_TEMPLATE(RaftCloseSessionRequestSerializationTest, _TestType, test_types)
{
  typedef typename _TestType::messages_type::close_session_request_traits_type close_session_request_traits;
  typedef typename _TestType::builders_type::close_session_request_builder_type close_session_builder;
  typedef raft::asio::serialization<typename _TestType::messages_type, typename _TestType::serialization_type> serialization_type;
  
  close_session_builder bld;
  auto msg = bld.session_id(8886234).finish();

  auto result = serialization_type::serialize(boost::asio::buffer(new uint8_t [1024], 1024), std::move(msg));
  auto header = boost::asio::buffer_cast<const raft::asio::rpc_header *>(result.first[0]);
  BOOST_CHECK_EQUAL(serialization_type::CLOSE_SESSION_REQUEST, header->operation);
  BOOST_CHECK_EQUAL(boost::asio::buffer_size(result.first), header->payload_length+sizeof(raft::asio::rpc_header));
  auto msg1 = serialization_type::deserialize_close_session_request(result.first[1], std::move(result.second));
  BOOST_CHECK_EQUAL(8886234U, close_session_request_traits::session_id(msg1));
}

BOOST_AUTO_TEST_CASE_TEMPLATE(RaftCloseSessionResponseSerializationTest, _TestType, test_types)
{
  typedef typename _TestType::builders_type::close_session_response_builder_type close_session_builder;
  typedef raft::asio::serialization<typename _TestType::messages_type, typename _TestType::serialization_type> serialization_type;
  
  close_session_builder bld;
  auto msg = bld.finish();

  auto result = serialization_type::serialize(boost::asio::buffer(new uint8_t [1024], 1024), std::move(msg));
  auto header = boost::asio::buffer_cast<const raft::asio::rpc_header *>(result.first[0]);
  BOOST_CHECK_EQUAL(serialization_type::CLOSE_SESSION_RESPONSE, header->operation);
  BOOST_CHECK_EQUAL(boost::asio::buffer_size(result.first), header->payload_length+sizeof(raft::asio::rpc_header));
  auto msg1 = serialization_type::deserialize_close_session_response(result.first[1], std::move(result.second));
}

BOOST_AUTO_TEST_CASE_TEMPLATE(RaftLinearizableCommandSerializationTest, _TestType, test_types)
{
  typedef typename _TestType::messages_type::linearizable_command_traits_type linearizable_command_traits;
  typedef typename _TestType::builders_type::linearizable_command_builder_type linearizable_command_builder;
  typedef raft::asio::serialization<typename _TestType::messages_type, typename _TestType::serialization_type> serialization_type;
  
  linearizable_command_builder bld;
  std::string command_str("This is a command");
  auto msg = bld.session_id(8886234).first_unacknowledged_sequence_number(62355342).sequence_number(823545).command(raft::slice::create(command_str)).finish();

  auto result = serialization_type::serialize(boost::asio::buffer(new uint8_t [1024], 1024), std::move(msg));
  auto header = boost::asio::buffer_cast<const raft::asio::rpc_header *>(result.first[0]);
  BOOST_CHECK_EQUAL(serialization_type::LINEARIZABLE_COMMAND, header->operation);
  BOOST_CHECK_EQUAL(boost::asio::buffer_size(result.first), header->payload_length+sizeof(raft::asio::rpc_header));
  auto msg1 = serialization_type::deserialize_linearizable_command(result.first[1], std::move(result.second));
  BOOST_CHECK_EQUAL(8886234U, linearizable_command_traits::session_id(msg1));
  BOOST_CHECK_EQUAL(62355342U, linearizable_command_traits::first_unacknowledged_sequence_number(msg1));
  BOOST_CHECK_EQUAL(823545U, linearizable_command_traits::sequence_number(msg1));
  BOOST_CHECK_EQUAL(0, string_slice_compare(command_str, linearizable_command_traits::command(msg1)));
}

BOOST_AUTO_TEST_CASE_TEMPLATE(RaftClientResponseSerializationTest, _TestType, test_types)
{
  typedef typename _TestType::messages_type::client_response_traits_type client_response_traits;
  typedef typename _TestType::builders_type::client_response_builder_type client_response_builder;
  typedef raft::asio::serialization<typename _TestType::messages_type, typename _TestType::serialization_type> serialization_type;
  
  client_response_builder bld;
  auto msg = bld.result(_TestType::messages_type::client_result_success()).index(8384).leader_id(23).finish();

  auto result = serialization_type::serialize(boost::asio::buffer(new uint8_t [1024], 1024), std::move(msg));
  auto header = boost::asio::buffer_cast<const raft::asio::rpc_header *>(result.first[0]);
  BOOST_REQUIRE(nullptr != header);
  BOOST_CHECK_EQUAL(5, header->operation);
  BOOST_CHECK_EQUAL(boost::asio::buffer_size(result.first), header->payload_length+sizeof(raft::asio::rpc_header));
  auto msg1 = serialization_type::deserialize_client_response(result.first[1], std::move(result.second));
  BOOST_TEST(client_response_traits::result(msg1) == _TestType::messages_type::client_result_success());
  BOOST_TEST(client_response_traits::index(msg1) == 8384);
  BOOST_TEST(client_response_traits::leader_id(msg1) == 23);
}

// This is the state machine.
template<typename _Messages, typename _Serialization>
struct logger
{
  typedef _Messages messages_type;
  typedef _Serialization serialization_type;
  typedef typename messages_type::linearizable_command_traits_type linearizable_command_traits_type;
  typedef typename messages_type::log_entry_command_traits_type log_entry_command_traits_type;
  typedef typename serialization_type::log_entry_command_view_deserialization_type log_entry_command_view_deserialization_type;
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
  bool async = false;
  void complete()
  {
    if (!cont) {
      return;
    }
    ;
    auto c = linearizable_command_traits_type::command(log_entry_command_traits_type::linearizable_command(cont->cmd.view()));
    commands.emplace_back(reinterpret_cast<const char *>(c.data()), c.size());
    // Must reset cont before the callback because the callback may add a new async command
    // from the log.   If we reset it after that then we'll lose a async call.
    auto tmp = std::move(cont);
    cont.reset();
    tmp->callback(true, std::make_pair(raft::slice(), raft::util::call_on_delete()));
  }
  template<typename _Callback>
  void on_command(log_entry_command_view_deserialization_type && cmd, _Callback && cb)
  {
    cont = std::make_unique<continuation>(std::move(cmd), std::move(cb));
    if (!async) {
      complete();
    }
  }
};

BOOST_AUTO_TEST_CASE_TEMPLATE(RaftAsioTest, _TestType, test_types)
{
  typedef typename _TestType::builders_type::log_entry_builder_type log_entry_builder;
  typedef typename _TestType::builders_type::open_session_request_builder_type open_session_request_builder;
  typedef typename _TestType::messages_type::open_session_response_traits_type open_session_response_traits;
  typedef typename _TestType::builders_type::close_session_request_builder_type close_session_request_builder;
  typedef typename _TestType::messages_type::close_session_response_traits_type close_session_response_traits;
  typedef typename _TestType::builders_type::linearizable_command_builder_type linearizable_command_builder;
  typedef typename _TestType::messages_type::client_response_traits_type client_response_traits;
  typedef raft::asio::serialization<typename _TestType::messages_type, typename _TestType::serialization_type> serialization_type;
  typedef logger<typename _TestType::messages_type, typename _TestType::serialization_type> state_machine_type;
  typedef raft::asio::tcp_server<typename _TestType::messages_type, typename _TestType::builders_type, typename _TestType::serialization_type, state_machine_type> tcp_server_type;
  boost::asio::io_service ios;
  auto make_config = []() {
    log_entry_builder leb;
    {	
      auto cb = leb.term(0).configuration();
      {
	auto scb = cb.from();
	scb.server().id(0).address("127.0.0.1:9133");
	scb.server().id(1).address("127.0.0.1:9134");
	scb.server().id(2).address("127.0.0.1:9135");
      }
      cb.to();
    }
    return leb.finish();
  };

  // 913x ports are for peers to connect to one another
  // 813x ports are for clients to connect
  boost::asio::ip::tcp::endpoint ep1(boost::asio::ip::tcp::v4(), 9133);
  boost::asio::ip::tcp::endpoint cep1(boost::asio::ip::tcp::v4(), 8133);
  tcp_server_type s1(ios, 0, ep1, cep1, make_config(), (boost::format("%1%/tmp/raft_log_dir_1") % ::getenv("HOME")).str());
  boost::asio::ip::tcp::endpoint ep2(boost::asio::ip::tcp::v4(), 9134);
  boost::asio::ip::tcp::endpoint cep2(boost::asio::ip::tcp::v4(), 8134);
  tcp_server_type s2(ios, 1, ep2, cep2, make_config(), (boost::format("%1%/tmp/raft_log_dir_2") % ::getenv("HOME")).str());
  boost::asio::ip::tcp::endpoint ep3(boost::asio::ip::tcp::v4(), 9135);
  boost::asio::ip::tcp::endpoint cep3(boost::asio::ip::tcp::v4(), 8135);
  tcp_server_type s3(ios, 2, ep3, cep3, make_config(), (boost::format("%1%/tmp/raft_log_dir_3") % ::getenv("HOME")).str());

  std::array<const tcp_server_type *, 3> servers = { &s1, &s2, &s3 };

  std::array<boost::asio::ip::tcp::endpoint, 3> client_endpoints = { cep1, cep2, cep3 };

  bool shutdown = false;
  std::thread server_thread([&shutdown, &ios]() {
                              while(!shutdown) {
                                boost::system::error_code ec;
                                ios.run_one(ec);
                              }
                            });

  // Wait for leadership to settle
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  // Validate leadership
  uint64_t leader_id = std::numeric_limits<uint64_t>::max();
  
  // Create a client session
  for(std::size_t i=0; i<3; ++i) {
    std::array<boost::asio::ip::tcp::endpoint, 1> eps = { client_endpoints[i] };
    boost::asio::ip::tcp::socket client_socket(ios);
    boost::asio::connect(client_socket, eps);
    uint64_t session_id = 0;
    {
      open_session_request_builder bld;
      auto msg = bld.finish();
      auto result = serialization_type::serialize(boost::asio::buffer(new uint8_t [1024], 1024), std::move(msg));
      auto bytes_transferred = boost::asio::write(client_socket, result.first);
      BOOST_TEST(bytes_transferred == boost::asio::buffer_size(result.first));
      raft::asio::rpc_header header;
      bytes_transferred = boost::asio::read(client_socket, boost::asio::buffer(&header, sizeof(raft::asio::rpc_header)));
      BOOST_TEST(bytes_transferred == sizeof(raft::asio::rpc_header));
      BOOST_TEST(header.magic == raft::asio::rpc_header::MAGIC());
      BOOST_TEST(header.payload_length > 0);
      BOOST_TEST(header.operation == serialization_type::OPEN_SESSION_RESPONSE);
      uint8_t *  buf = new uint8_t [header.payload_length];
      raft::util::call_on_delete deleter([ptr = buf](){ delete [] ptr; });    
      bytes_transferred = boost::asio::read(client_socket, boost::asio::buffer(&buf[0], header.payload_length));
      BOOST_TEST(bytes_transferred == header.payload_length);
      boost::asio::const_buffer asio_buf(buf, header.payload_length);
      auto resp = serialization_type::deserialize_open_session_response(asio_buf, std::move(deleter));
      session_id = open_session_response_traits::session_id(resp);
    }
    if (session_id > 0) {
      // Should  only be at most one leader
      BOOST_TEST(leader_id == std::numeric_limits<uint64_t>::max());
      leader_id = i;
      close_session_request_builder bld;
      auto msg = bld.session_id(session_id).finish();
      auto result = serialization_type::serialize(boost::asio::buffer(new uint8_t [1024], 1024), std::move(msg));
      auto bytes_transferred = boost::asio::write(client_socket, result.first);
      BOOST_TEST(bytes_transferred == boost::asio::buffer_size(result.first));
      raft::asio::rpc_header header;
      bytes_transferred = boost::asio::read(client_socket, boost::asio::buffer(&header, sizeof(raft::asio::rpc_header)));
      BOOST_TEST(bytes_transferred == sizeof(raft::asio::rpc_header));
      BOOST_TEST(header.magic == raft::asio::rpc_header::MAGIC());
      BOOST_TEST(header.payload_length > 0);
      BOOST_TEST(header.operation == serialization_type::CLOSE_SESSION_RESPONSE);
      uint8_t *  buf = new uint8_t [header.payload_length];
      raft::util::call_on_delete deleter([ptr = buf](){ delete [] ptr; });    
      bytes_transferred = boost::asio::read(client_socket, boost::asio::buffer(&buf[0], header.payload_length));
      BOOST_TEST(bytes_transferred == header.payload_length);
      boost::asio::const_buffer asio_buf(buf, header.payload_length);
      auto resp = serialization_type::deserialize_close_session_response(asio_buf, std::move(deleter));
    }
  }
  // Should be exactly one leader
  BOOST_TEST(leader_id != std::numeric_limits<uint64_t>::max());

  // Append the following strings to the state machine
  std::array<std::string, 4> cmds = { "foo", "bar", "baz", "bat" };
  {
    std::array<boost::asio::ip::tcp::endpoint, 1> eps = { client_endpoints[leader_id] };
    boost::asio::ip::tcp::socket client_socket(ios);
    boost::asio::connect(client_socket, eps);
    uint64_t session_id = 0;
    {
      open_session_request_builder bld;
      auto msg = bld.finish();
      auto result = serialization_type::serialize(boost::asio::buffer(new uint8_t [1024], 1024), std::move(msg));
      auto bytes_transferred = boost::asio::write(client_socket, result.first);
      BOOST_TEST(bytes_transferred == boost::asio::buffer_size(result.first));
      raft::asio::rpc_header header;
      bytes_transferred = boost::asio::read(client_socket, boost::asio::buffer(&header, sizeof(raft::asio::rpc_header)));
      BOOST_TEST(bytes_transferred == sizeof(raft::asio::rpc_header));
      BOOST_TEST(header.magic == raft::asio::rpc_header::MAGIC());
      BOOST_TEST(header.payload_length > 0);
      BOOST_TEST(header.operation == serialization_type::OPEN_SESSION_RESPONSE);
      uint8_t *  buf = new uint8_t [header.payload_length];
      raft::util::call_on_delete deleter([ptr = buf](){ delete [] ptr; });    
      bytes_transferred = boost::asio::read(client_socket, boost::asio::buffer(&buf[0], header.payload_length));
      BOOST_TEST(bytes_transferred == header.payload_length);
      boost::asio::const_buffer asio_buf(buf, header.payload_length);
      auto resp = serialization_type::deserialize_open_session_response(asio_buf, std::move(deleter));
      session_id = open_session_response_traits::session_id(resp);
    }
    BOOST_TEST(session_id > 0);
    for(std::size_t i=0; i<4; ++i) {
      linearizable_command_builder bld;
      auto msg = bld.session_id(session_id).first_unacknowledged_sequence_number(i).sequence_number(i).command(raft::slice::create(cmds[i])).finish();
      auto result = serialization_type::serialize(boost::asio::buffer(new uint8_t [1024], 1024), std::move(msg));
      auto bytes_transferred = boost::asio::write(client_socket, result.first);
      BOOST_TEST(bytes_transferred == boost::asio::buffer_size(result.first));
      raft::asio::rpc_header header;
      bytes_transferred = boost::asio::read(client_socket, boost::asio::buffer(&header, sizeof(raft::asio::rpc_header)));
      BOOST_TEST(bytes_transferred == sizeof(raft::asio::rpc_header));
      BOOST_TEST(header.magic == raft::asio::rpc_header::MAGIC());
      BOOST_TEST(header.payload_length > 0);
      // TODO: Add enum to serialization_type
      BOOST_TEST(header.operation == 5);
      uint8_t *  buf = new uint8_t [header.payload_length];
      raft::util::call_on_delete deleter([ptr = buf](){ delete [] ptr; });    
      bytes_transferred = boost::asio::read(client_socket, boost::asio::buffer(&buf[0], header.payload_length));
      BOOST_TEST(bytes_transferred == header.payload_length);
      boost::asio::const_buffer asio_buf(buf, header.payload_length);
      auto resp = serialization_type::deserialize_client_response(asio_buf, std::move(deleter));
      BOOST_TEST(_TestType::messages_type::client_result_success() == client_response_traits::result(resp));
      BOOST_TEST(leader_id == client_response_traits::leader_id(resp));
    }
    if (session_id > 0) {
      close_session_request_builder bld;
      auto msg = bld.session_id(session_id).finish();
      auto result = serialization_type::serialize(boost::asio::buffer(new uint8_t [1024], 1024), std::move(msg));
      auto bytes_transferred = boost::asio::write(client_socket, result.first);
      BOOST_TEST(bytes_transferred == boost::asio::buffer_size(result.first));
      raft::asio::rpc_header header;
      bytes_transferred = boost::asio::read(client_socket, boost::asio::buffer(&header, sizeof(raft::asio::rpc_header)));
      BOOST_TEST(bytes_transferred == sizeof(raft::asio::rpc_header));
      BOOST_TEST(header.magic == raft::asio::rpc_header::MAGIC());
      BOOST_TEST(header.payload_length > 0);
      BOOST_TEST(header.operation == serialization_type::CLOSE_SESSION_RESPONSE);
      uint8_t *  buf = new uint8_t [header.payload_length];
      raft::util::call_on_delete deleter([ptr = buf](){ delete [] ptr; });    
      bytes_transferred = boost::asio::read(client_socket, boost::asio::buffer(&buf[0], header.payload_length));
      BOOST_TEST(bytes_transferred == header.payload_length);
      boost::asio::const_buffer asio_buf(buf, header.payload_length);
      auto resp = serialization_type::deserialize_close_session_response(asio_buf, std::move(deleter));
    }
  }

  shutdown = true;
  server_thread.join();

  for(auto s : servers) {
    BOOST_TEST_REQUIRE(cmds.size() == s->state_machine().commands.size());
    for(std::size_t i=0; i<cmds.size(); ++i) {
      BOOST_TEST(boost::algorithm::equals(cmds[i], s->state_machine().commands[i]));
    }
  }
}

BOOST_AUTO_TEST_CASE(CallOnDeleteVectorTest)
{
  {
    std::vector<raft::util::call_on_delete> v(2);
    v.emplace_back([buf = new uint8_t [3]]() { delete [] buf; });
    v.emplace_back([buf = new uint32_t]() { delete buf; });
    v.emplace_back([buf = ::malloc(10)]() { ::free(buf); });
  }
  {
    std::vector<raft::util::call_on_delete> v;
    v.emplace_back([buf = new uint8_t [3]]() { delete [] buf; });
    auto cod = std::move(v[0]);
  }
  {
    raft::util::call_on_delete cod([buf = new uint8_t [3000]]() { delete [] buf; });
    {
      std::vector<raft::util::call_on_delete> v;
      v.emplace_back([buf = new uint8_t [3]]() { delete [] buf; });
      cod = std::move(v[0]);
    }
  }
}

BOOST_AUTO_TEST_CASE(CallOnDeleteDequeTest)
{
  {
    std::deque<raft::util::call_on_delete> q;
    q.emplace_back([buf = new uint8_t [3]]() { delete [] buf; });
    q.emplace_back([buf = new uint32_t]() { delete buf; });
    q.emplace_back([buf = ::malloc(10)]() { ::free(buf); });
  }
  {
    std::deque<raft::util::call_on_delete> q;
    q.emplace_back([buf = new uint8_t [3]]() { delete [] buf; });
    auto cod = std::move(q.front());
    BOOST_CHECK_EQUAL(1, q.size());
  }
  {
    std::deque<raft::util::call_on_delete> q;
    q.emplace_back([buf = new uint8_t [3]]() { delete [] buf; });
    auto cod = std::move(q.front());
    BOOST_CHECK_EQUAL(1, q.size());
    q.pop_front();
    BOOST_CHECK_EQUAL(0, q.size());    
  }
  {
    raft::util::call_on_delete cod([buf = new uint8_t [3000]]() { delete [] buf; });
    {
      std::deque<raft::util::call_on_delete> q;
      q.emplace_back([buf = new uint8_t [3]]() { delete [] buf; });
      cod = std::move(q.front());
      q.pop_front();
    }
  }
}

BOOST_AUTO_TEST_CASE(RaftFlatBufferRequestVoteTest)
{
  flatbuffers::FlatBufferBuilder fbb;
  raft::fbs::request_voteBuilder rvb(fbb);
  rvb.add_request_id(82345);
  rvb.add_recipient_id(0);
  rvb.add_term_number(3);
  rvb.add_candidate_id(1);
  rvb.add_last_log_index(3);
  rvb.add_last_log_term(2);
  auto rv = rvb.Finish();
  auto m = raft::fbs::Createraft_message(fbb, raft::fbs::any_message_request_vote, rv.Union());
  fbb.Finish(m);

  const raft::fbs::raft_message * msg = raft::fbs::Getraft_message(fbb.GetBufferPointer());
  BOOST_CHECK_EQUAL(raft::fbs::any_message_request_vote, msg->message_type());
  auto rv_msg = static_cast<const raft::fbs::request_vote * >(msg->message());
  BOOST_CHECK_EQUAL(0, rv_msg->recipient_id());

  typedef raft::fbs::request_vote_traits request_vote_traits;
  request_vote_traits::arg_type arg(msg, [](){});
  BOOST_CHECK_EQUAL(82345U, request_vote_traits::request_id(arg));
  BOOST_CHECK_EQUAL(0U, request_vote_traits::recipient_id(arg));
  BOOST_CHECK_EQUAL(3U, request_vote_traits::term_number(arg));
  BOOST_CHECK_EQUAL(1U, request_vote_traits::candidate_id(arg));
  BOOST_CHECK_EQUAL(3U, request_vote_traits::last_log_index(arg));
  BOOST_CHECK_EQUAL(2U, request_vote_traits::last_log_term(arg));
}

BOOST_AUTO_TEST_CASE(RaftFlatBufferAppendEntryTest)
{
  const char * lit = "This is my log entry data";
  flatbuffers::FlatBufferBuilder fbb;
  std::vector<flatbuffers::Offset<raft::fbs::log_entries>> entries;
  {
    flatbuffers::FlatBufferBuilder nested_fbb;
    {
      auto str = nested_fbb.CreateString(lit);
      raft::fbs::log_entryBuilder leb(nested_fbb);
      leb.add_type(raft::fbs::log_entry_type_COMMAND);
      leb.add_term(2834);
      leb.add_data(str);
      nested_fbb.FinishSizePrefixed(leb.Finish());
    }    
    auto v = fbb.CreateVector<uint8_t>(nested_fbb.GetBufferPointer(), nested_fbb.GetSize());
    entries.push_back(raft::fbs::Createlog_entries(fbb, v));
  }
  {
    flatbuffers::FlatBufferBuilder nested_fbb;
    {
      ::flatbuffers::Offset<::flatbuffers::Vector<uint8_t>> conf;
      {
	flatbuffers::FlatBufferBuilder nested_nested_fbb;
	std::vector<flatbuffers::Offset<raft::fbs::server_description>> servers_vec;
	const char * server0 = "127.0.0.1:8342";
	servers_vec.push_back(raft::fbs::Createserver_description(nested_nested_fbb, 0, nested_nested_fbb.CreateString(server0)));
	const char * server1 = "127.0.0.1:8343";
	servers_vec.push_back(raft::fbs::Createserver_description(nested_nested_fbb, 1, nested_nested_fbb.CreateString(server1)));
	auto from_desc = raft::fbs::Createsimple_configuration_description(nested_nested_fbb, nested_nested_fbb.CreateVector(servers_vec));
	servers_vec.clear();
	auto to_desc = raft::fbs::Createsimple_configuration_description(nested_nested_fbb, nested_nested_fbb.CreateVector(servers_vec));
	nested_nested_fbb.FinishSizePrefixed(raft::fbs::Createconfiguration_description(nested_nested_fbb, from_desc, to_desc));
	conf = nested_fbb.CreateVector(nested_nested_fbb.GetBufferPointer(), nested_nested_fbb.GetSize());
      }
      raft::fbs::log_entryBuilder leb(nested_fbb);
      leb.add_type(raft::fbs::log_entry_type_CONFIGURATION);
      leb.add_term(2833);
      leb.add_configuration(conf);
      nested_fbb.FinishSizePrefixed(leb.Finish());
    }    
    auto v = fbb.CreateVector<uint8_t>(nested_fbb.GetBufferPointer(), nested_fbb.GetSize());
    entries.push_back(raft::fbs::Createlog_entries(fbb, v));
  }
  
  auto e = fbb.CreateVector(entries);  

  raft::fbs::append_entryBuilder aeb(fbb);
  aeb.add_request_id(9988);
  aeb.add_recipient_id(10345);
  aeb.add_term_number(82342);
  aeb.add_leader_id(7342);
  aeb.add_previous_log_index(3434);
  aeb.add_previous_log_term(55234);
  aeb.add_leader_commit_index(525123);
  aeb.add_entries(e);
  auto ae = aeb.Finish();
  auto m = raft::fbs::Createraft_message(fbb, raft::fbs::any_message_append_entry, ae.Union());

  fbb.Finish(m);

  const raft::fbs::raft_message * msg = raft::fbs::Getraft_message(fbb.GetBufferPointer());
  BOOST_CHECK_EQUAL(raft::fbs::any_message_append_entry, msg->message_type());
  auto ae_msg = static_cast<const raft::fbs::append_entry * >(msg->message());
  BOOST_CHECK_EQUAL(9988U, ae_msg->request_id());
  BOOST_CHECK_EQUAL(10345U, ae_msg->recipient_id());
  BOOST_CHECK_EQUAL(2U, ae_msg->entries()->size());
  {
    auto le = flatbuffers::GetSizePrefixedRoot<raft::fbs::log_entry>(ae_msg->entries()->Get(0)->entry()->Data());
    BOOST_CHECK_EQUAL(raft::fbs::log_entry_type_COMMAND, le->type());
    BOOST_CHECK_EQUAL(2834, le->term());
    BOOST_CHECK_EQUAL(strlen(lit), le->data()->size());
    BOOST_CHECK_EQUAL(0, ::memcmp(lit, le->data()->data(), le->data()->size()));
  }
  {
    auto le = flatbuffers::GetSizePrefixedRoot<raft::fbs::log_entry>(ae_msg->entries()->Get(1)->entry()->Data());
    BOOST_CHECK_EQUAL(raft::fbs::log_entry_type_CONFIGURATION, le->type());
    BOOST_CHECK_EQUAL(2833, le->term());
    auto cfg = ::flatbuffers::GetSizePrefixedRoot<raft::fbs::configuration_description>(le->configuration()->Data());
    BOOST_REQUIRE(nullptr != cfg);
    BOOST_REQUIRE(nullptr != cfg->from());
    BOOST_REQUIRE(nullptr != cfg->from()->servers());
    BOOST_CHECK_EQUAL(2U, cfg->from()->servers()->size());
    BOOST_CHECK_EQUAL(0, cfg->from()->servers()->Get(0)->id());
    BOOST_CHECK_EQUAL(1, cfg->from()->servers()->Get(1)->id());
  }
  typedef raft::fbs::append_entry_traits append_entry_traits;
  append_entry_traits::arg_type arg(msg, [](){});
  BOOST_CHECK_EQUAL(9988U, append_entry_traits::request_id(arg));
  BOOST_CHECK_EQUAL(10345U, append_entry_traits::recipient_id(arg));
  BOOST_CHECK_EQUAL(2U, append_entry_traits::num_entries(arg));
  {
    typedef raft::fbs::log_entry_traits log_entry_traits;
    auto le = &append_entry_traits::get_entry(arg, 0);
    BOOST_CHECK_EQUAL(2834U, log_entry_traits::term(le));
    BOOST_REQUIRE(log_entry_traits::is_command(le));
    BOOST_CHECK_EQUAL(::strlen(lit), raft::slice::buffer_size(log_entry_traits::data(le)));
  }
  {
    typedef raft::fbs::log_entry_traits let;
    typedef raft::fbs::configuration_description_traits cdt;
    typedef raft::fbs::simple_configuration_description_traits scdt;
    typedef raft::fbs::server_description_traits sdt;
    auto le = &append_entry_traits::get_entry(arg, 1);
    BOOST_CHECK_EQUAL(2833U, let::term(le));
    BOOST_REQUIRE(let::is_configuration(le));
    BOOST_REQUIRE_EQUAL(2U, scdt::size(&cdt::from(&let::configuration(le))));
    BOOST_REQUIRE_EQUAL(0U, scdt::size(&cdt::to(&let::configuration(le))));
    {
      std::string_view expected("127.0.0.1:8342");
      BOOST_CHECK_EQUAL(0U, sdt::id(&scdt::get(&cdt::from(&let::configuration(le)), 0)));
      BOOST_CHECK_EQUAL(0, expected.compare(sdt::address(&scdt::get(&cdt::from(&let::configuration(le)), 0))));
    }
    {
      std::string_view expected("127.0.0.1:8343");
      BOOST_CHECK_EQUAL(1U, sdt::id(&scdt::get(&cdt::from(&let::configuration(le)), 1)));
      BOOST_CHECK_EQUAL(0, expected.compare(sdt::address(&scdt::get(&cdt::from(&let::configuration(le)), 1))));
    }
  }
}

BOOST_AUTO_TEST_CASE(RaftFlatBufferServerDescriptionTraitsTest)
{
  typedef raft::fbs::server_description_traits sdt;
  flatbuffers::FlatBufferBuilder fbb;
  fbb.Finish(raft::fbs::Createserver_descriptionDirect(fbb, 92341, "77.22.1.8"));
  auto sd = flatbuffers::GetRoot<raft::fbs::server_description>(fbb.GetBufferPointer());
  BOOST_CHECK_EQUAL(92341U, sdt::id(sd));
  std::string_view expected("77.22.1.8");
  BOOST_CHECK_EQUAL(0, expected.compare(sdt::address(sd)));
}

BOOST_AUTO_TEST_CASE(RaftFlatBufferServerDescriptionBuilderTest)
{
  typedef raft::fbs::server_description_traits sdt;
  typedef raft::fbs::server_description_builder sdb;
  for(int i=0; i<2; ++i) {
    flatbuffers::FlatBufferBuilder fbb;
    if (0 == i) {
      fbb.Finish(sdb(fbb).id(92341).address("77.22.1.8").finish());
    } else {
      sdb(fbb, [&fbb](::flatbuffers::Offset<raft::fbs::server_description> val) { fbb.Finish(val); }).id(92341).address("77.22.1.8");
    }
    auto sd = flatbuffers::GetRoot<raft::fbs::server_description>(fbb.GetBufferPointer());
    BOOST_CHECK_EQUAL(92341U, sdt::id(sd));
    std::string_view expected("77.22.1.8");
    BOOST_CHECK_EQUAL(0, expected.compare(sdt::address(sd)));
  }
}

BOOST_AUTO_TEST_CASE(RaftFlatBufferSimpleConfigurationDescriptionTraitsTest)
{
  typedef raft::fbs::simple_configuration_description_traits scdt;
  typedef raft::fbs::server_description_traits sdt;
  flatbuffers::FlatBufferBuilder fbb;
  std::vector<flatbuffers::Offset<raft::fbs::server_description>> servers;
  servers.push_back(raft::fbs::Createserver_descriptionDirect(fbb, 92341, "77.22.1.8"));
  servers.push_back(raft::fbs::Createserver_descriptionDirect(fbb, 23, "7.22.1.9"));
  fbb.Finish(raft::fbs::Createsimple_configuration_descriptionDirect(fbb, &servers));
  auto sd = flatbuffers::GetRoot<raft::fbs::simple_configuration_description>(fbb.GetBufferPointer());
  BOOST_CHECK_EQUAL(2U, scdt::size(sd));
  {
    std::string_view expected("77.22.1.8");
    BOOST_CHECK_EQUAL(92341U, sdt::id(&scdt::get(sd, 0)));
    BOOST_CHECK_EQUAL(0, expected.compare(sdt::address(&scdt::get(sd, 0))));
  }
  {
    std::string_view expected("7.22.1.9");
    BOOST_CHECK_EQUAL(23U, sdt::id(&scdt::get(sd, 1)));
    BOOST_CHECK_EQUAL(0, expected.compare(sdt::address(&scdt::get(sd, 1))));
  }
}

BOOST_AUTO_TEST_CASE(RaftFlatBufferSimpleConfigurationDescriptionBuilderTest)
{
  typedef raft::fbs::simple_configuration_description_traits scdt;
  typedef raft::fbs::simple_configuration_description_builder scdb;
  typedef raft::fbs::server_description_traits sdt;
  for(int i=0; i<2; ++i) {
    flatbuffers::FlatBufferBuilder fbb;
    {
      std::unique_ptr<scdb> bld(0==i ?
				new scdb(fbb) :
				new scdb(fbb, [&fbb](::flatbuffers::Offset<raft::fbs::simple_configuration_description> val) { fbb.Finish(val); }));
      bld->server().id(92341).address("77.22.1.8");
      bld->server().id(23).address("77.22.1.9");
      if (0 == i) {
	fbb.Finish(bld->finish());
      }
    }
    auto sd = flatbuffers::GetRoot<raft::fbs::simple_configuration_description>(fbb.GetBufferPointer());
    BOOST_CHECK_EQUAL(2U, scdt::size(sd));
    {
      std::string_view expected("77.22.1.8");
      BOOST_CHECK_EQUAL(92341U, sdt::id(&scdt::get(sd, 0)));
      BOOST_CHECK_EQUAL(0, expected.compare(sdt::address(&scdt::get(sd, 0))));
    }
    {
      std::string_view expected("77.22.1.9");
      BOOST_CHECK_EQUAL(23U, sdt::id(&scdt::get(sd, 1)));
      BOOST_CHECK_EQUAL(0, expected.compare(sdt::address(&scdt::get(sd, 1))));
    }
  }
}

BOOST_AUTO_TEST_CASE(RaftFlatBufferCreateBootstrapEntryTest)
{
  typedef raft::fbs::log_entry_traits let;
  typedef raft::fbs::configuration_description_traits cdt;
  typedef raft::fbs::simple_configuration_description_traits scdt;
  typedef raft::fbs::server_description_traits sdt;
  auto msg = let::create_bootstrap_log_entry(831, "23.22.1.78");
  BOOST_CHECK_EQUAL(0U, let::term(msg.first));
  BOOST_REQUIRE(let::is_configuration(msg.first));
  BOOST_CHECK_EQUAL(1U, scdt::size(&cdt::from(&let::configuration(msg.first))));
  BOOST_CHECK_EQUAL(0U, scdt::size(&cdt::to(&let::configuration(msg.first))));
  std::string_view expected("23.22.1.78");
  BOOST_CHECK_EQUAL(831U, sdt::id(&scdt::get(&cdt::from(&let::configuration(msg.first)), 0)));
  BOOST_CHECK_EQUAL(0, expected.compare(sdt::address(&scdt::get(&cdt::from(&let::configuration(msg.first)), 0))));
}

class flatbuffer_test_communicator
{
public:
  typedef size_t endpoint;

  void send(endpoint ep, const std::string& address, raft::fbs::flatbuffer_builder_adapter && msg)
  {
    q.push_front(std::move(msg));
  }
  
  void send(endpoint ep, const std::string& address, std::pair<const raft::fbs::raft_message *, raft::util::call_on_delete> && msg)
  {
    q2.push_front(std::move(msg));
  }
  
  void vote_request(endpoint ep, const std::string & address,
		    uint64_t request_id,
		    uint64_t recipient_id,
		    uint64_t term_number,
		    uint64_t candidate_id,
		    uint64_t last_log_index,
		    uint64_t last_log_term)
  {
    raft::fbs::request_vote_sender<flatbuffer_test_communicator> sender(*this, ep, address);
    sender.send(request_id,
                recipient_id,
		term_number,
		candidate_id,
		last_log_index,
		last_log_term);
  }

  template<typename EntryProvider>
  void append_entry(endpoint ep, const std::string& address,
	    uint64_t request_id,
	    uint64_t recipient_id,
	    uint64_t term_number,
	    uint64_t leader_id,
	    uint64_t previous_log_index,
	    uint64_t previous_log_term,
	    uint64_t leader_commit_index,
	    uint64_t num_entries,
	    EntryProvider entries)
  {
    raft::fbs::append_entry_sender<flatbuffer_test_communicator> sender(*this, ep, address);
    sender.send(request_id,
                recipient_id,
		term_number,
		leader_id,
		previous_log_index,
		previous_log_term,
		leader_commit_index,
		num_entries,
		entries);
  }
	
  void append_entry_response(endpoint ep, const std::string& address,
			     uint64_t recipient_id,
			     uint64_t term_number,
			     uint64_t request_term_number,
			     uint64_t request_id,
			     uint64_t begin_index,
			     uint64_t last_index,
			     bool success)
  {
    raft::fbs::append_entry_response_sender<flatbuffer_test_communicator> sender(*this, ep, address);
    sender.send(recipient_id, term_number, request_term_number, request_id, begin_index, last_index, success);
  }

  void vote_response(endpoint ep, const std::string& address,
		     uint64_t peer_id,
		     uint64_t term_number,
		     uint64_t request_term_number,
		     uint64_t request_id,
		     bool granted)
  {
    raft::fbs::vote_response_sender<flatbuffer_test_communicator> sender(*this, ep, address);
    sender.send(peer_id, term_number, request_term_number, request_id, granted);
  }

  void append_checkpoint_chunk(endpoint ep, const std::string& address,
			       uint64_t request_id,
			       uint64_t recipient_id,
			       uint64_t term_number,
			       uint64_t leader_id,
			       const raft::fbs::checkpoint_header & last_checkpoint_header,
			       uint64_t checkpoint_begin,
			       uint64_t checkpoint_end,
			       bool checkpoint_done,
			       raft::slice data)
  {
  }		       
  
  void append_checkpoint_chunk_response(endpoint ep, const std::string& address,
					uint64_t recipient_id,
					uint64_t term_number,
					uint64_t request_term_number,
					uint64_t request_id,
					uint64_t bytes_stored)
  {
    raft::fbs::append_checkpoint_chunk_response_sender<flatbuffer_test_communicator> sender(*this, ep, address);
    sender.send(recipient_id, term_number, request_term_number, bytes_stored);
  }

  std::deque<raft::fbs::flatbuffer_builder_adapter> q;
  std::deque<std::pair<const raft::fbs::raft_message *, raft::util::call_on_delete>> q2;
};

struct flatbuffer_communicator_metafunction
{
  template <typename _Messages>
  struct apply
  {
    typedef flatbuffer_test_communicator type;
  };
};

class configuration_description_view
{
private:
  const raft::native::configuration_description & description_;
public:
  configuration_description_view(const raft::native::configuration_description & desc)
    :
    description_(desc)
  {
  }

  std::size_t from_size() const
  {
    return description_.from.servers.size();
  }

  std::size_t from_id(std::size_t i) const
  {
    return description_.from.servers[i].id;
  }

  const std::string & from_address(std::size_t i) const
  {
    return description_.from.servers[i].address;
  }

  std::size_t to_size() const
  {
    return description_.to.servers.size();
  }

  std::size_t to_id(std::size_t i) const
  {
    return description_.to.servers[i].id;
  }

  const std::string & to_address(std::size_t i) const
  {
    return description_.to.servers[i].address;
  }
};
  
struct native_client_metafunction
{
  template <typename _Messages>
  struct apply
  {
    typedef raft::native::client<_Messages> type;
  };
};

typedef raft::protocol<flatbuffer_communicator_metafunction, native_client_metafunction, raft::fbs::messages> test_raft_type;

BOOST_AUTO_TEST_CASE(RaftFlatBufferConstructProtocolTest)
{
  test_raft_type::communicator_type comm;
  test_raft_type::client_type c;
  test_raft_type::log_type l;
  test_raft_type::checkpoint_data_store_type store;

  raft::native::configuration_description five_servers;
  five_servers.from.servers = {{0, "192.168.1.1"}, {1, "192.168.1.2"}, {2, "192.168.1.3"}, {3, "192.168.1.4"},  {4, "192.168.1.5"}};
  test_raft_type::configuration_manager_type cm(0);
  
  l.append(raft::fbs::log_entry_traits::create_configuration(0, 0, configuration_description_view(five_servers)));
  l.update_header(0, test_raft_type::INVALID_PEER_ID);
  test_raft_type proto(comm, l, store, cm);

  flatbuffers::FlatBufferBuilder fbb;
  raft::fbs::request_voteBuilder rvb(fbb);
  uint64_t term=1;
  rvb.add_recipient_id(0);
  rvb.add_term_number(term);
  rvb.add_candidate_id(1);
  rvb.add_last_log_index(1);
  rvb.add_last_log_term(0);
  auto rv = rvb.Finish();
  auto m = raft::fbs::Createraft_message(fbb, raft::fbs::any_message_request_vote, rv.Union());
  fbb.Finish(m);

  raft::fbs::request_vote_traits::arg_type arg(raft::fbs::Getraft_message(fbb.GetBufferPointer()), [](){});
  proto.on_request_vote(std::move(arg));
  BOOST_CHECK(proto.log_header_sync_required());
  BOOST_CHECK_EQUAL(term, proto.current_term());
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, proto.get_state());
  BOOST_CHECK_EQUAL(0U, comm.q2.size());
  proto.on_log_header_sync();
  BOOST_CHECK(!proto.log_header_sync_required());
  BOOST_CHECK_EQUAL(term, proto.current_term());
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, proto.get_state());
  BOOST_REQUIRE(0U < comm.q2.size());
  BOOST_CHECK_EQUAL(1U, comm.q2.size());
  {
    typedef raft::fbs::vote_response_traits traits;
    const auto & msg = comm.q2.front();
    BOOST_CHECK_EQUAL(0U, traits::peer_id(msg));
    BOOST_CHECK_EQUAL(term, traits::term_number(msg));
    BOOST_CHECK_EQUAL(term, traits::request_term_number(msg));
  }
}



