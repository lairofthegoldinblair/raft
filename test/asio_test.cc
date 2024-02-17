#define BOOST_TEST_MODULE RaftTests
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

#include "test_utilities.hh"

#include "asio/asio_server.hh"
#include "asio/asio_block_device.hh"
#include "leveldb_log.hh"
#include "native/messages.hh"
#include "native/serialization.hh"
#include "flatbuffers/raft_flatbuffer_messages.hh"
#include "flatbuffers/serialization.hh"
#include "posix_file.hh"
#include "protocol.hh"

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
  typedef typename _TestType::messages_type::vote_request_traits_type vote_request_traits;
  typedef typename _TestType::builders_type::vote_request_builder_type vote_request_builder;
  typedef typename _TestType::serialization_type serialization_type;
  
  auto msg = vote_request_builder().recipient_id(99).term_number(1).candidate_id(887).request_id(192345).log_index_end(888542).last_log_term(16).finish();

  auto result = serialization_type::serialize(std::move(msg));

  auto msg2 = serialization_type::deserialize_vote_request(std::move(result));
  BOOST_CHECK_EQUAL(99U, vote_request_traits::recipient_id(msg2));
  BOOST_CHECK_EQUAL(1U, vote_request_traits::term_number(msg2));
  BOOST_CHECK_EQUAL(887U, vote_request_traits::candidate_id(msg2));
  BOOST_CHECK_EQUAL(192345U, vote_request_traits::request_id(msg2));
  BOOST_CHECK_EQUAL(888542U, vote_request_traits::log_index_end(msg2));
  BOOST_CHECK_EQUAL(16U, vote_request_traits::last_log_term(msg2));
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
  typedef typename _TestType::messages_type::append_entry_request_traits_type append_entry_request_traits;
  typedef typename _TestType::builders_type::append_entry_request_builder_type append_entry_request_builder;
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
  auto msg = append_entry_request_builder().request_id(request_id).recipient_id(recipient_id).term_number(99234).leader_id(23445234).log_index_begin(734725345).previous_log_term(3492385345).leader_commit_index_end(3483458).entry(le1).entry(le2).finish();

  auto result = serialization_type::serialize(boost::asio::buffer(new uint8_t [1024], 1024), std::move(msg));
  auto header = boost::asio::buffer_cast<const raft::asio::rpc_header *>(result.first[0]);
  BOOST_CHECK_EQUAL(serialization_type::APPEND_ENTRY_REQUEST, header->operation);
  BOOST_CHECK_EQUAL(boost::asio::buffer_size(result.first), header->payload_length+sizeof(raft::asio::rpc_header));
  auto msg1 = serialization_type::deserialize_append_entry_request(result.first[1], std::move(result.second));
  BOOST_CHECK_EQUAL(request_id, append_entry_request_traits::request_id(msg1));
  BOOST_CHECK_EQUAL(recipient_id, append_entry_request_traits::recipient_id(msg1));
  BOOST_CHECK_EQUAL(2U, append_entry_request_traits::num_entries(msg1));
}

BOOST_AUTO_TEST_CASE_TEMPLATE(RaftAppendResponseSerializationTest, _TestType, test_types)
{
  typedef typename _TestType::messages_type::append_entry_response_traits_type append_entry_response_traits;
  typedef typename _TestType::builders_type::append_entry_response_builder_type append_entry_response_builder;
  typedef typename _TestType::serialization_type serialization_type;
  
  auto msg = append_entry_response_builder().recipient_id(222).term_number(10).request_term_number(1).request_id(192345).index_begin(236).index_end(851).success(false).finish();

  auto result = serialization_type::serialize(std::move(msg));

  auto msg2 = serialization_type::deserialize_append_entry_response(std::move(result));
  BOOST_CHECK_EQUAL(222U, append_entry_response_traits::recipient_id(msg2));
  BOOST_CHECK_EQUAL(10U, append_entry_response_traits::term_number(msg2));
  BOOST_CHECK_EQUAL(1U, append_entry_response_traits::request_term_number(msg2));
  BOOST_CHECK_EQUAL(192345U, append_entry_response_traits::request_id(msg2));
  BOOST_CHECK_EQUAL(236U, append_entry_response_traits::index_begin(msg2));
  BOOST_CHECK_EQUAL(851U, append_entry_response_traits::index_end(msg2));
  BOOST_CHECK(!append_entry_response_traits::success(msg2));
}

BOOST_AUTO_TEST_CASE_TEMPLATE(RaftAppendCheckpointChunkSerializationTest, _TestType, test_types)
{
  typedef typename _TestType::messages_type::append_checkpoint_chunk_request_traits_type append_checkpoint_chunk_request_traits;
  typedef typename _TestType::builders_type::append_checkpoint_chunk_request_builder_type append_checkpoint_chunk_request_builder;
  typedef typename _TestType::serialization_type serialization_type;

  std::string data_str("This is some checkpoint chunk data");
  auto msg = append_checkpoint_chunk_request_builder().recipient_id(222).term_number(10).leader_id(1).request_id(192345).checkpoint_begin(236).checkpoint_end(8643).data(raft::slice::create(data_str)).checkpoint_done(false).finish();

  auto result = serialization_type::serialize(std::move(msg));

  auto msg2 = serialization_type::deserialize_append_checkpoint_chunk_request(std::move(result));
  BOOST_CHECK_EQUAL(222U, append_checkpoint_chunk_request_traits::recipient_id(msg2));
  BOOST_CHECK_EQUAL(10U, append_checkpoint_chunk_request_traits::term_number(msg2));
  BOOST_CHECK_EQUAL(1U, append_checkpoint_chunk_request_traits::leader_id(msg2));
  BOOST_CHECK_EQUAL(192345U, append_checkpoint_chunk_request_traits::request_id(msg2));
  BOOST_CHECK_EQUAL(236U, append_checkpoint_chunk_request_traits::checkpoint_begin(msg2));
  BOOST_CHECK_EQUAL(8643U, append_checkpoint_chunk_request_traits::checkpoint_end(msg2));
  BOOST_CHECK_EQUAL(0, string_slice_compare(data_str, append_checkpoint_chunk_request_traits::data(msg2)));
  BOOST_CHECK(!append_checkpoint_chunk_request_traits::checkpoint_done(msg2));
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

BOOST_AUTO_TEST_CASE_TEMPLATE(RaftRequestVoteAsioSerializationTest, _TestType, test_types)
{
  typedef typename _TestType::messages_type::vote_request_traits_type vote_request_traits;
  typedef typename _TestType::builders_type::vote_request_builder_type vote_request_builder;
  typedef raft::asio::serialization<typename _TestType::messages_type, typename _TestType::serialization_type> serialization_type;
  
  auto msg = vote_request_builder().recipient_id(99).term_number(1).candidate_id(887).request_id(192345).log_index_end(888542).last_log_term(16).finish();

  auto result = serialization_type::serialize(boost::asio::buffer(new uint8_t [1024], 1024), std::move(msg));
  auto header = boost::asio::buffer_cast<const raft::asio::rpc_header *>(result.first[0]);
  BOOST_CHECK_EQUAL(serialization_type::VOTE_REQUEST, header->operation);
  BOOST_CHECK_EQUAL(boost::asio::buffer_size(result.first), header->payload_length+sizeof(raft::asio::rpc_header));
  auto msg2 = serialization_type::deserialize_vote_request(result.first[1], std::move(result.second));
  BOOST_CHECK_EQUAL(99U, vote_request_traits::recipient_id(msg2));
  BOOST_CHECK_EQUAL(1U, vote_request_traits::term_number(msg2));
  BOOST_CHECK_EQUAL(887U, vote_request_traits::candidate_id(msg2));
  BOOST_CHECK_EQUAL(192345U, vote_request_traits::request_id(msg2));
  BOOST_CHECK_EQUAL(888542U, vote_request_traits::log_index_end(msg2));
  BOOST_CHECK_EQUAL(16U, vote_request_traits::last_log_term(msg2));
}

BOOST_AUTO_TEST_CASE_TEMPLATE(RaftVoteResponseAsioSerializationTest, _TestType, test_types)
{
  typedef typename _TestType::messages_type::vote_response_traits_type vote_response_traits;
  typedef typename _TestType::builders_type::vote_response_builder_type vote_response_builder;
  typedef raft::asio::serialization<typename _TestType::messages_type, typename _TestType::serialization_type> serialization_type;
  
  auto msg = vote_response_builder().peer_id(1).term_number(1).request_term_number(1).request_id(192345).granted(false).finish();

  auto result = serialization_type::serialize(boost::asio::buffer(new uint8_t [1024], 1024), std::move(msg));
  auto header = boost::asio::buffer_cast<const raft::asio::rpc_header *>(result.first[0]);
  BOOST_CHECK_EQUAL(serialization_type::VOTE_RESPONSE, header->operation);
  BOOST_CHECK_EQUAL(boost::asio::buffer_size(result.first), header->payload_length+sizeof(raft::asio::rpc_header));
  auto msg2 = serialization_type::deserialize_vote_response(result.first[1], std::move(result.second));
  BOOST_CHECK_EQUAL(1U, vote_response_traits::peer_id(msg2));
  BOOST_CHECK_EQUAL(1U, vote_response_traits::term_number(msg2));
  BOOST_CHECK_EQUAL(1U, vote_response_traits::request_term_number(msg2));
  BOOST_CHECK_EQUAL(192345U, vote_response_traits::request_id(msg2));
  BOOST_CHECK(!vote_response_traits::granted(msg2));
}

BOOST_AUTO_TEST_CASE_TEMPLATE(RaftAppendEntryResponseAsioSerializationTest, _TestType, test_types)
{
  typedef typename _TestType::messages_type::append_entry_response_traits_type append_entry_response_traits;
  typedef typename _TestType::builders_type::append_entry_response_builder_type append_entry_response_builder;
  typedef raft::asio::serialization<typename _TestType::messages_type, typename _TestType::serialization_type> serialization_type;
  
  auto msg = append_entry_response_builder().recipient_id(222).term_number(10).request_term_number(1).request_id(192345).index_begin(236).index_end(851).success(false).finish();

  auto result = serialization_type::serialize(boost::asio::buffer(new uint8_t [1024], 1024), std::move(msg));
  auto header = boost::asio::buffer_cast<const raft::asio::rpc_header *>(result.first[0]);
  BOOST_CHECK_EQUAL(serialization_type::APPEND_ENTRY_RESPONSE, header->operation);
  BOOST_CHECK_EQUAL(boost::asio::buffer_size(result.first), header->payload_length+sizeof(raft::asio::rpc_header));
  auto msg2 = serialization_type::deserialize_append_entry_response(result.first[1], std::move(result.second));
  BOOST_CHECK_EQUAL(222U, append_entry_response_traits::recipient_id(msg2));
  BOOST_CHECK_EQUAL(10U, append_entry_response_traits::term_number(msg2));
  BOOST_CHECK_EQUAL(1U, append_entry_response_traits::request_term_number(msg2));
  BOOST_CHECK_EQUAL(192345U, append_entry_response_traits::request_id(msg2));
  BOOST_CHECK_EQUAL(236U, append_entry_response_traits::index_begin(msg2));
  BOOST_CHECK_EQUAL(851U, append_entry_response_traits::index_end(msg2));
  BOOST_CHECK(!append_entry_response_traits::success(msg2));
}

BOOST_AUTO_TEST_CASE_TEMPLATE(RaftAppendCheckpointChunkAsioSerializationTest, _TestType, test_types)
{
  typedef typename _TestType::messages_type::append_checkpoint_chunk_request_traits_type append_checkpoint_chunk_request_traits;
  typedef typename _TestType::builders_type::append_checkpoint_chunk_request_builder_type append_checkpoint_chunk_request_builder;
  typedef raft::asio::serialization<typename _TestType::messages_type, typename _TestType::serialization_type> serialization_type;

  std::string data_str("This is some checkpoint chunk data");
  auto msg = append_checkpoint_chunk_request_builder().recipient_id(222).term_number(10).leader_id(1).request_id(192345).checkpoint_begin(236).checkpoint_end(8643).data(raft::slice::create(data_str)).checkpoint_done(false).finish();

  auto result = serialization_type::serialize(boost::asio::buffer(new uint8_t [1024], 1024), std::move(msg));
  auto header = boost::asio::buffer_cast<const raft::asio::rpc_header *>(result.first[0]);
  BOOST_CHECK_EQUAL(serialization_type::APPEND_CHECKPOINT_CHUNK_REQUEST, header->operation);
  BOOST_CHECK_EQUAL(boost::asio::buffer_size(result.first), header->payload_length+sizeof(raft::asio::rpc_header));
  auto msg2 = serialization_type::deserialize_append_checkpoint_chunk_request(result.first[1], std::move(result.second));
  BOOST_CHECK_EQUAL(222U, append_checkpoint_chunk_request_traits::recipient_id(msg2));
  BOOST_CHECK_EQUAL(10U, append_checkpoint_chunk_request_traits::term_number(msg2));
  BOOST_CHECK_EQUAL(1U, append_checkpoint_chunk_request_traits::leader_id(msg2));
  BOOST_CHECK_EQUAL(192345U, append_checkpoint_chunk_request_traits::request_id(msg2));
  BOOST_CHECK_EQUAL(236U, append_checkpoint_chunk_request_traits::checkpoint_begin(msg2));
  BOOST_CHECK_EQUAL(8643U, append_checkpoint_chunk_request_traits::checkpoint_end(msg2));
  BOOST_CHECK_EQUAL(0, string_slice_compare(data_str, append_checkpoint_chunk_request_traits::data(msg2)));
  BOOST_CHECK(!append_checkpoint_chunk_request_traits::checkpoint_done(msg2));
}

BOOST_AUTO_TEST_CASE_TEMPLATE(RaftAppendCheckpointChunkResponseAsioSerializationTest, _TestType, test_types)
{
  typedef typename _TestType::messages_type::append_checkpoint_chunk_response_traits_type append_checkpoint_chunk_response_traits;
  typedef typename _TestType::builders_type::append_checkpoint_chunk_response_builder_type append_checkpoint_chunk_response_builder;
  typedef raft::asio::serialization<typename _TestType::messages_type, typename _TestType::serialization_type> serialization_type;
  
  auto msg = append_checkpoint_chunk_response_builder().recipient_id(222).term_number(10).request_term_number(1).request_id(192345).bytes_stored(236).finish();

  auto result = serialization_type::serialize(boost::asio::buffer(new uint8_t [1024], 1024), std::move(msg));
  auto header = boost::asio::buffer_cast<const raft::asio::rpc_header *>(result.first[0]);
  BOOST_CHECK_EQUAL(serialization_type::APPEND_CHECKPOINT_CHUNK_RESPONSE, header->operation);
  BOOST_CHECK_EQUAL(boost::asio::buffer_size(result.first), header->payload_length+sizeof(raft::asio::rpc_header));
  auto msg2 = serialization_type::deserialize_append_checkpoint_chunk_response(result.first[1], std::move(result.second));
  BOOST_CHECK_EQUAL(222U, append_checkpoint_chunk_response_traits::recipient_id(msg2));
  BOOST_CHECK_EQUAL(10U, append_checkpoint_chunk_response_traits::term_number(msg2));
  BOOST_CHECK_EQUAL(1U, append_checkpoint_chunk_response_traits::request_term_number(msg2));
  BOOST_CHECK_EQUAL(192345U, append_checkpoint_chunk_response_traits::request_id(msg2));
  BOOST_CHECK_EQUAL(236U, append_checkpoint_chunk_response_traits::bytes_stored(msg2));
}

BOOST_AUTO_TEST_CASE_TEMPLATE(RaftOpenSessionRequestAsioSerializationTest, _TestType, test_types)
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

BOOST_AUTO_TEST_CASE_TEMPLATE(RaftOpenSessionResponseAsioSerializationTest, _TestType, test_types)
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

BOOST_AUTO_TEST_CASE_TEMPLATE(RaftCloseSessionRequestAsioSerializationTest, _TestType, test_types)
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

BOOST_AUTO_TEST_CASE_TEMPLATE(RaftCloseSessionResponseAsioSerializationTest, _TestType, test_types)
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

BOOST_AUTO_TEST_CASE_TEMPLATE(RaftLinearizableCommandAsioSerializationTest, _TestType, test_types)
{
  typedef typename _TestType::messages_type::linearizable_command_request_traits_type linearizable_command_request_traits;
  typedef typename _TestType::builders_type::linearizable_command_request_builder_type linearizable_command_request_builder;
  typedef raft::asio::serialization<typename _TestType::messages_type, typename _TestType::serialization_type> serialization_type;
  
  linearizable_command_request_builder bld;
  std::string command_str("This is a command");
  auto msg = bld.session_id(8886234).first_unacknowledged_sequence_number(62355342).sequence_number(823545).command(raft::slice::create(command_str)).finish();

  auto result = serialization_type::serialize(boost::asio::buffer(new uint8_t [1024], 1024), std::move(msg));
  auto header = boost::asio::buffer_cast<const raft::asio::rpc_header *>(result.first[0]);
  BOOST_CHECK_EQUAL(serialization_type::LINEARIZABLE_COMMAND_REQUEST, header->operation);
  BOOST_CHECK_EQUAL(boost::asio::buffer_size(result.first), header->payload_length+sizeof(raft::asio::rpc_header));
  auto msg1 = serialization_type::deserialize_linearizable_command_request(result.first[1], std::move(result.second));
  BOOST_CHECK_EQUAL(8886234U, linearizable_command_request_traits::session_id(msg1));
  BOOST_CHECK_EQUAL(62355342U, linearizable_command_request_traits::first_unacknowledged_sequence_number(msg1));
  BOOST_CHECK_EQUAL(823545U, linearizable_command_request_traits::sequence_number(msg1));
  BOOST_CHECK_EQUAL(0, string_slice_compare(command_str, linearizable_command_request_traits::command(msg1)));
}

BOOST_AUTO_TEST_CASE_TEMPLATE(RaftClientResponseAsioSerializationTest, _TestType, test_types)
{
  typedef typename _TestType::messages_type::client_response_traits_type client_response_traits;
  typedef typename _TestType::builders_type::client_response_builder_type client_response_builder;
  typedef raft::asio::serialization<typename _TestType::messages_type, typename _TestType::serialization_type> serialization_type;
  
  client_response_builder bld;
  auto msg = bld.result(_TestType::messages_type::client_result_success()).index(8384).leader_id(23).finish();

  auto result = serialization_type::serialize(boost::asio::buffer(new uint8_t [1024], 1024), std::move(msg));
  auto header = boost::asio::buffer_cast<const raft::asio::rpc_header *>(result.first[0]);
  BOOST_REQUIRE(nullptr != header);
  BOOST_CHECK_EQUAL(serialization_type::CLIENT_RESPONSE, header->operation);
  BOOST_CHECK_EQUAL(boost::asio::buffer_size(result.first), header->payload_length+sizeof(raft::asio::rpc_header));
  auto msg1 = serialization_type::deserialize_client_response(result.first[1], std::move(result.second));
  BOOST_TEST(client_response_traits::result(msg1) == _TestType::messages_type::client_result_success());
  BOOST_TEST(client_response_traits::index(msg1) == 8384);
  BOOST_TEST(client_response_traits::leader_id(msg1) == 23);
}

BOOST_AUTO_TEST_CASE_TEMPLATE(RaftSetConfigurationRequestAsioSerializationTest, _TestType, test_types)
{
  typedef typename _TestType::messages_type::server_description_traits_type server_description_traits;
  typedef typename _TestType::messages_type::simple_configuration_description_traits_type simple_configuration_description_traits;
  typedef typename _TestType::messages_type::set_configuration_request_traits_type set_configuration_request_traits;
  typedef typename _TestType::builders_type::set_configuration_request_builder_type set_configuration_request_builder;
  typedef raft::asio::serialization<typename _TestType::messages_type, typename _TestType::serialization_type> serialization_type;
  
  set_configuration_request_builder bld;
  {
    auto cfg = bld.old_id(99).new_configuration();
    cfg.server().id(0).address("192.168.1.1");
    cfg.server().id(1).address("192.168.1.2");
    cfg.server().id(2).address("192.168.1.3");
  }
  auto msg = bld.finish();
  auto result = serialization_type::serialize(boost::asio::buffer(new uint8_t [1024], 1024), std::move(msg));
  auto header = boost::asio::buffer_cast<const raft::asio::rpc_header *>(result.first[0]);
  BOOST_REQUIRE(nullptr != header);
  BOOST_CHECK_EQUAL(serialization_type::SET_CONFIGURATION_REQUEST, header->operation);
  BOOST_CHECK_EQUAL(boost::asio::buffer_size(result.first), header->payload_length+sizeof(raft::asio::rpc_header));
  auto msg1 = serialization_type::deserialize_set_configuration_request(result.first[1], std::move(result.second));
  BOOST_TEST(set_configuration_request_traits::old_id(msg1) == 99);
  auto cfg1 = &set_configuration_request_traits::new_configuration(msg1);
  BOOST_TEST_REQUIRE(simple_configuration_description_traits::size(cfg1) == 3);
  BOOST_TEST(server_description_traits::id(&simple_configuration_description_traits::get(cfg1, 0)) == 0);
  BOOST_TEST(server_description_traits::address(&simple_configuration_description_traits::get(cfg1, 0)).compare("192.168.1.1") == 0);
  BOOST_TEST(server_description_traits::id(&simple_configuration_description_traits::get(cfg1, 1)) == 1);
  BOOST_TEST(server_description_traits::address(&simple_configuration_description_traits::get(cfg1, 1)).compare("192.168.1.2") == 0);
  BOOST_TEST(server_description_traits::id(&simple_configuration_description_traits::get(cfg1, 2)) == 2);
  BOOST_TEST(server_description_traits::address(&simple_configuration_description_traits::get(cfg1, 2)).compare("192.168.1.3") == 0);
}

BOOST_AUTO_TEST_CASE_TEMPLATE(RaftSetConfigurationResponseAsioSerializationTest, _TestType, test_types)
{
  typedef typename _TestType::messages_type::server_description_traits_type server_description_traits;
  typedef typename _TestType::messages_type::simple_configuration_description_traits_type simple_configuration_description_traits;
  typedef typename _TestType::messages_type::set_configuration_response_traits_type set_configuration_response_traits;
  typedef typename _TestType::builders_type::set_configuration_response_builder_type set_configuration_response_builder;
  typedef raft::asio::serialization<typename _TestType::messages_type, typename _TestType::serialization_type> serialization_type;
  
  set_configuration_response_builder bld;
  {
    auto cfg = bld.result(_TestType::messages_type::client_result_fail()).bad_servers();
    cfg.server().id(0).address("192.168.1.1");
    cfg.server().id(1).address("192.168.1.2");
    cfg.server().id(2).address("192.168.1.3");
  }
  auto msg = bld.finish();
  auto result = serialization_type::serialize(boost::asio::buffer(new uint8_t [1024], 1024), std::move(msg));
  auto header = boost::asio::buffer_cast<const raft::asio::rpc_header *>(result.first[0]);
  BOOST_REQUIRE(nullptr != header);
  BOOST_CHECK_EQUAL(serialization_type::SET_CONFIGURATION_RESPONSE, header->operation);
  BOOST_CHECK_EQUAL(boost::asio::buffer_size(result.first), header->payload_length+sizeof(raft::asio::rpc_header));
  auto msg1 = serialization_type::deserialize_set_configuration_response(result.first[1], std::move(result.second));
  BOOST_TEST(set_configuration_response_traits::result(msg1) == _TestType::messages_type::client_result_fail());
  auto cfg1 = &set_configuration_response_traits::bad_servers(msg1);
  BOOST_TEST_REQUIRE(simple_configuration_description_traits::size(cfg1) == 3);
  BOOST_TEST(server_description_traits::id(&simple_configuration_description_traits::get(cfg1, 0)) == 0);
  BOOST_TEST(server_description_traits::address(&simple_configuration_description_traits::get(cfg1, 0)).compare("192.168.1.1") == 0);
  BOOST_TEST(server_description_traits::id(&simple_configuration_description_traits::get(cfg1, 1)) == 1);
  BOOST_TEST(server_description_traits::address(&simple_configuration_description_traits::get(cfg1, 1)).compare("192.168.1.2") == 0);
  BOOST_TEST(server_description_traits::id(&simple_configuration_description_traits::get(cfg1, 2)) == 2);
  BOOST_TEST(server_description_traits::address(&simple_configuration_description_traits::get(cfg1, 2)).compare("192.168.1.3") == 0);
}

BOOST_AUTO_TEST_CASE_TEMPLATE(RaftGetConfigurationRequestAsioSerializationTest, _TestType, test_types)
{
  typedef typename _TestType::messages_type::get_configuration_request_traits_type get_configuration_request_traits;
  typedef typename _TestType::builders_type::get_configuration_request_builder_type get_configuration_request_builder;
  typedef raft::asio::serialization<typename _TestType::messages_type, typename _TestType::serialization_type> serialization_type;
  
  get_configuration_request_builder bld;
  auto msg = bld.finish();
  auto result = serialization_type::serialize(boost::asio::buffer(new uint8_t [1024], 1024), std::move(msg));
  auto header = boost::asio::buffer_cast<const raft::asio::rpc_header *>(result.first[0]);
  BOOST_REQUIRE(nullptr != header);
  BOOST_CHECK_EQUAL(serialization_type::GET_CONFIGURATION_REQUEST, header->operation);
  BOOST_CHECK_EQUAL(boost::asio::buffer_size(result.first), header->payload_length+sizeof(raft::asio::rpc_header));
  auto msg1 = serialization_type::deserialize_get_configuration_request(result.first[1], std::move(result.second));
}

BOOST_AUTO_TEST_CASE_TEMPLATE(RaftGetConfigurationResponseAsioSerializationTest, _TestType, test_types)
{
  typedef typename _TestType::messages_type::server_description_traits_type server_description_traits;
  typedef typename _TestType::messages_type::simple_configuration_description_traits_type simple_configuration_description_traits;
  typedef typename _TestType::messages_type::get_configuration_response_traits_type get_configuration_response_traits;
  typedef typename _TestType::builders_type::get_configuration_response_builder_type get_configuration_response_builder;
  typedef raft::asio::serialization<typename _TestType::messages_type, typename _TestType::serialization_type> serialization_type;
  
  get_configuration_response_builder bld;
  {
    auto cfg = bld.id(99).configuration();
    cfg.server().id(0).address("192.168.1.1");
    cfg.server().id(1).address("192.168.1.2");
    cfg.server().id(2).address("192.168.1.3");
  }
  auto msg = bld.finish();
  auto result = serialization_type::serialize(boost::asio::buffer(new uint8_t [1024], 1024), std::move(msg));
  auto header = boost::asio::buffer_cast<const raft::asio::rpc_header *>(result.first[0]);
  BOOST_REQUIRE(nullptr != header);
  BOOST_CHECK_EQUAL(serialization_type::GET_CONFIGURATION_RESPONSE, header->operation);
  BOOST_CHECK_EQUAL(boost::asio::buffer_size(result.first), header->payload_length+sizeof(raft::asio::rpc_header));
  auto msg1 = serialization_type::deserialize_get_configuration_response(result.first[1], std::move(result.second));
  BOOST_TEST(get_configuration_response_traits::id(msg1) == 99);
  auto cfg1 = &get_configuration_response_traits::configuration(msg1);
  BOOST_TEST_REQUIRE(simple_configuration_description_traits::size(cfg1) == 3);
  BOOST_TEST(server_description_traits::id(&simple_configuration_description_traits::get(cfg1, 0)) == 0);
  BOOST_TEST(server_description_traits::address(&simple_configuration_description_traits::get(cfg1, 0)).compare("192.168.1.1") == 0);
  BOOST_TEST(server_description_traits::id(&simple_configuration_description_traits::get(cfg1, 1)) == 1);
  BOOST_TEST(server_description_traits::address(&simple_configuration_description_traits::get(cfg1, 1)).compare("192.168.1.2") == 0);
  BOOST_TEST(server_description_traits::id(&simple_configuration_description_traits::get(cfg1, 2)) == 2);
  BOOST_TEST(server_description_traits::address(&simple_configuration_description_traits::get(cfg1, 2)).compare("192.168.1.3") == 0);
}

BOOST_AUTO_TEST_CASE_TEMPLATE(RaftAsioWriteQueueTest, _TestType, test_types)
{
  typedef typename _TestType::messages_type::linearizable_command_request_traits_type linearizable_command_request_traits;
  typedef typename _TestType::builders_type::linearizable_command_request_builder_type linearizable_command_request_builder;
  typedef raft::asio::serialization<typename _TestType::messages_type, typename _TestType::serialization_type> serialization_type;
  typedef std::deque<std::pair<std::array<boost::asio::const_buffer, 2>, raft::util::call_on_delete>> write_queue_type;

  write_queue_type write_queue;

  std::string command_str("This is a command");

  uint64_t next_push_sequence_number = 0;
  uint64_t next_pop_sequence_number = 0;

  for ( ; next_push_sequence_number < 10; ++next_push_sequence_number) {
    linearizable_command_request_builder bld;
    auto msg = bld.session_id(8886234).first_unacknowledged_sequence_number(62355342).sequence_number(next_push_sequence_number).command(raft::slice::create(command_str)).finish();    
    auto result = serialization_type::serialize(boost::asio::buffer(new uint8_t [1024], 1024), std::move(msg));
    BOOST_TEST_REQUIRE((nullptr != result.first[0].data() && nullptr != result.first[1].data()));
    auto header = boost::asio::buffer_cast<const raft::asio::rpc_header *>(result.first[0]);
    BOOST_CHECK_EQUAL(serialization_type::LINEARIZABLE_COMMAND_REQUEST, header->operation);
    BOOST_CHECK_EQUAL(boost::asio::buffer_size(result.first), header->payload_length+sizeof(raft::asio::rpc_header));
    write_queue.push_front(std::move(result));
  }
  for (; next_pop_sequence_number<10; ++next_pop_sequence_number) {
    BOOST_TEST_REQUIRE((nullptr != write_queue.back().first[0].data() && nullptr != write_queue.back().first[1].data()));
    auto result = std::move(write_queue.back());
    auto msg1 = serialization_type::deserialize_linearizable_command_request(result.first[1], std::move(result.second));
    BOOST_CHECK_EQUAL(8886234U, linearizable_command_request_traits::session_id(msg1));
    BOOST_CHECK_EQUAL(62355342U, linearizable_command_request_traits::first_unacknowledged_sequence_number(msg1));
    BOOST_CHECK_EQUAL(next_pop_sequence_number, linearizable_command_request_traits::sequence_number(msg1));
    BOOST_CHECK_EQUAL(0, string_slice_compare(command_str, linearizable_command_request_traits::command(msg1)));
    write_queue.pop_back();
  }
  for ( ; next_push_sequence_number < 15; ++next_push_sequence_number) {
    linearizable_command_request_builder bld;
    auto msg = bld.session_id(8886234).first_unacknowledged_sequence_number(62355342).sequence_number(next_push_sequence_number).command(raft::slice::create(command_str)).finish();    
    auto result = serialization_type::serialize(boost::asio::buffer(new uint8_t [1024], 1024), std::move(msg));
    BOOST_TEST_REQUIRE((nullptr != result.first[0].data() && nullptr != result.first[1].data()));
    auto header = boost::asio::buffer_cast<const raft::asio::rpc_header *>(result.first[0]);
    BOOST_CHECK_EQUAL(serialization_type::LINEARIZABLE_COMMAND_REQUEST, header->operation);
    BOOST_CHECK_EQUAL(boost::asio::buffer_size(result.first), header->payload_length+sizeof(raft::asio::rpc_header));
    write_queue.push_front(std::move(result));
  }
  for (; next_pop_sequence_number<13; ++next_pop_sequence_number) {
    BOOST_TEST_REQUIRE((nullptr != write_queue.back().first[0].data() && nullptr != write_queue.back().first[1].data()));
    auto result = std::move(write_queue.back());
    auto msg1 = serialization_type::deserialize_linearizable_command_request(result.first[1], std::move(result.second));
    BOOST_CHECK_EQUAL(8886234U, linearizable_command_request_traits::session_id(msg1));
    BOOST_CHECK_EQUAL(62355342U, linearizable_command_request_traits::first_unacknowledged_sequence_number(msg1));
    BOOST_CHECK_EQUAL(next_pop_sequence_number, linearizable_command_request_traits::sequence_number(msg1));
    BOOST_CHECK_EQUAL(0, string_slice_compare(command_str, linearizable_command_request_traits::command(msg1)));
    write_queue.pop_back();
  }
  for ( ; next_push_sequence_number < 20; ++next_push_sequence_number) {
    linearizable_command_request_builder bld;
    auto msg = bld.session_id(8886234).first_unacknowledged_sequence_number(62355342).sequence_number(next_push_sequence_number).command(raft::slice::create(command_str)).finish();    
    auto result = serialization_type::serialize(boost::asio::buffer(new uint8_t [1024], 1024), std::move(msg));
    BOOST_TEST_REQUIRE((nullptr != result.first[0].data() && nullptr != result.first[1].data()));
    auto header = boost::asio::buffer_cast<const raft::asio::rpc_header *>(result.first[0]);
    BOOST_CHECK_EQUAL(serialization_type::LINEARIZABLE_COMMAND_REQUEST, header->operation);
    BOOST_CHECK_EQUAL(boost::asio::buffer_size(result.first), header->payload_length+sizeof(raft::asio::rpc_header));
    write_queue.push_front(std::move(result));
  }
  for (; next_pop_sequence_number<20; ++next_pop_sequence_number) {
    BOOST_TEST_REQUIRE((nullptr != write_queue.back().first[0].data() && nullptr != write_queue.back().first[1].data()));
    auto result = std::move(write_queue.back());
    auto msg1 = serialization_type::deserialize_linearizable_command_request(result.first[1], std::move(result.second));
    BOOST_CHECK_EQUAL(8886234U, linearizable_command_request_traits::session_id(msg1));
    BOOST_CHECK_EQUAL(62355342U, linearizable_command_request_traits::first_unacknowledged_sequence_number(msg1));
    BOOST_CHECK_EQUAL(next_pop_sequence_number, linearizable_command_request_traits::sequence_number(msg1));
    BOOST_CHECK_EQUAL(0, string_slice_compare(command_str, linearizable_command_request_traits::command(msg1)));
    write_queue.pop_back();
  }
  BOOST_CHECK_EQUAL(0U, write_queue.size());
}

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

struct auto_io_service_thread
{
  bool shutdown_;
  std::thread server_thread_;
  auto_io_service_thread(boost::asio::io_service & ios)
    :
    shutdown_(false),
    server_thread_([this, &ios]() {
                     while(!this->shutdown_) {
                       boost::system::error_code ec;
                       ios.run_one(ec);
                     }
                   })
  {
  }
  ~auto_io_service_thread()
  {
    shutdown();
  }

  void shutdown()
  {
    shutdown_ = true;
    if (server_thread_.joinable()) {
      server_thread_.join();
    }
  }
};

template<typename _TestType>
class RaftAsioTestFixture
{
public:
  typedef typename _TestType::messages_type::log_entry_type log_entry_type;
  typedef typename _TestType::builders_type::log_entry_builder_type log_entry_builder;
  typedef typename _TestType::builders_type::open_session_request_builder_type open_session_request_builder;
  typedef typename _TestType::messages_type::open_session_response_traits_type open_session_response_traits;
  typedef typename _TestType::builders_type::close_session_request_builder_type close_session_request_builder;
  typedef typename _TestType::messages_type::close_session_response_traits_type close_session_response_traits;
  typedef typename _TestType::builders_type::linearizable_command_request_builder_type linearizable_command_request_builder;
  typedef typename _TestType::builders_type::set_configuration_request_builder_type set_configuration_request_builder;
  typedef typename _TestType::messages_type::set_configuration_response_traits_type set_configuration_response_traits;
  typedef typename _TestType::builders_type::get_configuration_request_builder_type get_configuration_request_builder;
  typedef typename _TestType::messages_type::get_configuration_response_traits_type get_configuration_response_traits;
  typedef typename _TestType::messages_type::client_response_traits_type client_response_traits;
  
  typedef typename _TestType::messages_type::simple_configuration_description_traits_type simple_configuration_description_traits;
  typedef typename _TestType::messages_type::server_description_traits_type server_description_traits;  
  typedef raft::asio::serialization<typename _TestType::messages_type, typename _TestType::serialization_type> serialization_type;
  typedef logger<typename _TestType::messages_type, typename _TestType::serialization_type> state_machine_type;
  typedef raft::asio::tcp_server<typename _TestType::messages_type, typename _TestType::builders_type, typename _TestType::serialization_type, state_machine_type> tcp_server_type;

  boost::asio::io_service ios;
  boost::asio::ip::tcp::endpoint ep1;
  boost::asio::ip::tcp::endpoint cep1;
  tcp_server_type s1;
  boost::asio::ip::tcp::endpoint ep2;
  boost::asio::ip::tcp::endpoint cep2;
  tcp_server_type s2;
  boost::asio::ip::tcp::endpoint ep3;
  boost::asio::ip::tcp::endpoint cep3;
  tcp_server_type s3;

  std::array<const tcp_server_type *, 3> servers;

  std::array<boost::asio::ip::tcp::endpoint, 3> client_endpoints;
  uint64_t leader_id = std::numeric_limits<uint64_t>::max();
  uint64_t configuration_id = 0;
  std::array<std::string, 4> cmds;
  auto_io_service_thread server_thread;

  template<typename _Config>
  RaftAsioTestFixture(_Config make_config)
    :
    // 913x ports are for peers to connect to one another
    // 813x ports are for clients to connect
    ep1(boost::asio::ip::tcp::v4(), 9133),
    cep1(boost::asio::ip::tcp::v4(), 8133),
    s1(ios, 0, ep1, cep1, make_config(0), (boost::format("%1%/tmp/raft_log_dir_1") % ::getenv("HOME")).str()),
    ep2(boost::asio::ip::tcp::v4(), 9134),
    cep2(boost::asio::ip::tcp::v4(), 8134),
    s2(ios, 1, ep2, cep2, make_config(1), (boost::format("%1%/tmp/raft_log_dir_2") % ::getenv("HOME")).str()),
    ep3(boost::asio::ip::tcp::v4(), 9135),
    cep3(boost::asio::ip::tcp::v4(), 8135),
    s3(ios, 2, ep3, cep3, make_config(2), (boost::format("%1%/tmp/raft_log_dir_3") % ::getenv("HOME")).str()),
    servers({ &s1, &s2, &s3 }),
    client_endpoints({ cep1, cep2, cep3 }),
    cmds({ "foo", "bar", "baz", "bat" }),
    server_thread(ios)
  {
  }

  template<typename _Container>
  void determine_leadership(const _Container & ids)
  {
  
    // Wait for leadership to settle
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // Validate leadership
    leader_id = std::numeric_limits<uint64_t>::max();
  
    // Create a client session
    for(auto i : ids) {
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
    BOOST_TEST_REQUIRE(leader_id != std::numeric_limits<uint64_t>::max());
  }

  void append_strings()
  {
    
    // Append the following strings to the state machine
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
        linearizable_command_request_builder bld;
        auto msg = bld.session_id(session_id).first_unacknowledged_sequence_number(i).sequence_number(i).command(raft::slice::create(cmds[i])).finish();
        auto result = serialization_type::serialize(boost::asio::buffer(new uint8_t [1024], 1024), std::move(msg));
        auto bytes_transferred = boost::asio::write(client_socket, result.first);
        BOOST_TEST(bytes_transferred == boost::asio::buffer_size(result.first));
        raft::asio::rpc_header header;
        bytes_transferred = boost::asio::read(client_socket, boost::asio::buffer(&header, sizeof(raft::asio::rpc_header)));
        BOOST_TEST(bytes_transferred == sizeof(raft::asio::rpc_header));
        BOOST_TEST(header.magic == raft::asio::rpc_header::MAGIC());
        BOOST_TEST(header.payload_length > 0);
        BOOST_TEST(header.operation == serialization_type::CLIENT_RESPONSE);
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
  }

  void update_configuration(std::size_t num_servers)
  {
    BOOST_TEST_MESSAGE("Updating Raft configuration to have " << num_servers << " servers");
    {
      std::array<boost::asio::ip::tcp::endpoint, 1> eps = { client_endpoints[leader_id] };
      boost::asio::ip::tcp::socket client_socket(ios);
      boost::asio::connect(client_socket, eps);
      set_configuration_request_builder bld;
      {
        auto cfg = bld.old_id(configuration_id).new_configuration();
        for(std::size_t i=0; i<num_servers; ++i) {
          cfg.server().id(i).address((boost::format("127.0.0.1:%1%") % (9133+i)).str().c_str());
        }
      }
      auto msg = bld.finish();
      auto result = serialization_type::serialize(boost::asio::buffer(new uint8_t [1024], 1024), std::move(msg));
      auto bytes_transferred = boost::asio::write(client_socket, result.first);
      BOOST_TEST_MESSAGE("Wrote SET_CONFIGURATION_REQUEST with bytes_transferred=" << bytes_transferred);
      BOOST_TEST(bytes_transferred == boost::asio::buffer_size(result.first));
      raft::asio::rpc_header header;
      bytes_transferred = boost::asio::read(client_socket, boost::asio::buffer(&header, sizeof(raft::asio::rpc_header)));
      BOOST_TEST(bytes_transferred == sizeof(raft::asio::rpc_header));
      BOOST_TEST(header.magic == raft::asio::rpc_header::MAGIC());
      BOOST_TEST(header.payload_length > 0);
      BOOST_TEST(header.operation == serialization_type::SET_CONFIGURATION_RESPONSE);
      uint8_t *  buf = new uint8_t [header.payload_length];
      raft::util::call_on_delete deleter([ptr = buf](){ delete [] ptr; });    
      bytes_transferred = boost::asio::read(client_socket, boost::asio::buffer(&buf[0], header.payload_length));
      BOOST_TEST(bytes_transferred == header.payload_length);
      boost::asio::const_buffer asio_buf(buf, header.payload_length);
      auto resp = serialization_type::deserialize_set_configuration_response(asio_buf, std::move(deleter));
      BOOST_TEST(_TestType::messages_type::client_result_success() == set_configuration_response_traits::result(resp));      
      BOOST_TEST(0 == simple_configuration_description_traits::size(&set_configuration_response_traits::bad_servers(resp)));
    }  
    // Wait for log records to propagate to clients
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }

  void  get_configuration(std::size_t num_servers)
  {
    {
      std::array<boost::asio::ip::tcp::endpoint, 1> eps = { client_endpoints[leader_id] };
      boost::asio::ip::tcp::socket client_socket(ios);
      boost::asio::connect(client_socket, eps);
      get_configuration_request_builder bld;
      auto msg = bld.finish();
      auto result = serialization_type::serialize(boost::asio::buffer(new uint8_t [1024], 1024), std::move(msg));
      auto bytes_transferred = boost::asio::write(client_socket, result.first);
      BOOST_TEST_MESSAGE("Wrote GET_CONFIGURATION_REQUEST with bytes_transferred=" << bytes_transferred);
      BOOST_TEST(bytes_transferred == boost::asio::buffer_size(result.first));
      raft::asio::rpc_header header;
      bytes_transferred = boost::asio::read(client_socket, boost::asio::buffer(&header, sizeof(raft::asio::rpc_header)));
      BOOST_TEST(bytes_transferred == sizeof(raft::asio::rpc_header));
      BOOST_TEST(header.magic == raft::asio::rpc_header::MAGIC());
      BOOST_TEST(header.payload_length > 0);
      BOOST_TEST(header.operation == serialization_type::GET_CONFIGURATION_RESPONSE);
      uint8_t *  buf = new uint8_t [header.payload_length];
      raft::util::call_on_delete deleter([ptr = buf](){ delete [] ptr; });    
      bytes_transferred = boost::asio::read(client_socket, boost::asio::buffer(&buf[0], header.payload_length));
      BOOST_TEST(bytes_transferred == header.payload_length);
      boost::asio::const_buffer asio_buf(buf, header.payload_length);
      auto resp = serialization_type::deserialize_get_configuration_response(asio_buf, std::move(deleter));
      BOOST_TEST(_TestType::messages_type::client_result_success() == get_configuration_response_traits::result(resp));
      configuration_id = get_configuration_response_traits::id(resp);
      BOOST_TEST_REQUIRE(num_servers == simple_configuration_description_traits::size(&get_configuration_response_traits::configuration(resp)));
      for(std::size_t i=0; i<num_servers; ++i) {
        auto & server = simple_configuration_description_traits::get(&get_configuration_response_traits::configuration(resp), i);
        BOOST_TEST(i == server_description_traits::id(&server));
        BOOST_TEST(0 == (boost::format("127.0.0.1:%1%") % (9133+i)).str().compare(server_description_traits::address(&server)));
      }
    }  
  }

  void shutdown_and_check_state_machines()
  {
    server_thread.shutdown();

    for(auto s : servers) {
      BOOST_TEST_REQUIRE(cmds.size() == s->state_machine().commands.size());
      for(std::size_t i=0; i<cmds.size(); ++i) {
        BOOST_TEST(boost::algorithm::equals(cmds[i], s->state_machine().commands[i]));
      }
    }
  }
};

BOOST_AUTO_TEST_CASE_TEMPLATE(RaftAsioTest, _TestType, test_types)
{
  typedef typename _TestType::messages_type::log_entry_type log_entry_type;
  typedef typename _TestType::builders_type::log_entry_builder_type log_entry_builder;
  auto make_config = [](std::size_t i) {
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
  RaftAsioTestFixture<_TestType> test(make_config);
  std::set<std::size_t> candidates = { 0, 1, 2 };
  test.determine_leadership(candidates);
  test.append_strings();
  test.shutdown_and_check_state_machines();
}

BOOST_AUTO_TEST_CASE_TEMPLATE(RaftAsioGetConfigurationTest, _TestType, test_types)
{
  typedef typename _TestType::messages_type::log_entry_type log_entry_type;
  typedef typename _TestType::builders_type::log_entry_builder_type log_entry_builder;
  auto make_config = [](std::size_t i) {
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
  RaftAsioTestFixture<_TestType> test(make_config);
  std::set<std::size_t> candidates = { 0, 1, 2 };
  test.determine_leadership(candidates);
  test.get_configuration(3);
}

BOOST_AUTO_TEST_CASE_TEMPLATE(RaftAsioAddServerTest, _TestType, test_types)
{
  typedef typename _TestType::messages_type::log_entry_type log_entry_type;
  typedef typename _TestType::builders_type::log_entry_builder_type log_entry_builder;
  // Start with configuration with just server 0
  auto make_config = [](std::size_t i) {
                       if (0 == i) {
                         log_entry_builder leb;
                         {	
                           auto cb = leb.term(0).configuration();
                           {
                             auto scb = cb.from();
                             scb.server().id(0).address("127.0.0.1:9133");
                           }
                           cb.to();
                         }
                         return leb.finish();
                       } else {
                         return std::pair<const log_entry_type *, raft::util::call_on_delete>(nullptr, raft::util::call_on_delete());
                       }
                     };
  RaftAsioTestFixture<_TestType> test(make_config);
  std::set<std::size_t> candidates = { 0 };
  test.determine_leadership(candidates);
  test.get_configuration(1);
  test.append_strings();
  test.update_configuration(3);
  test.get_configuration(3);
  test.shutdown_and_check_state_machines();
}

BOOST_AUTO_TEST_CASE_TEMPLATE(RaftAsioRemoveServerTest, _TestType, test_types)
{
  typedef typename _TestType::messages_type::log_entry_type log_entry_type;
  typedef typename _TestType::builders_type::log_entry_builder_type log_entry_builder;
  auto make_config = [](std::size_t i) {
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
  RaftAsioTestFixture<_TestType> test(make_config);
  std::set<std::size_t> candidates = { 0, 1, 2 };
  test.determine_leadership(candidates);
  test.append_strings();
  test.get_configuration(3);
  test.update_configuration(2);
  test.get_configuration(2);
  test.shutdown_and_check_state_machines();
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
  raft::fbs::vote_requestBuilder rvb(fbb);
  rvb.add_request_id(82345);
  rvb.add_recipient_id(0);
  rvb.add_term_number(3);
  rvb.add_candidate_id(1);
  rvb.add_log_index_end(3);
  rvb.add_last_log_term(2);
  auto rv = rvb.Finish();
  auto m = raft::fbs::Createraft_message(fbb, raft::fbs::any_message_vote_request, rv.Union());
  fbb.Finish(m);

  const raft::fbs::raft_message * msg = raft::fbs::Getraft_message(fbb.GetBufferPointer());
  BOOST_CHECK_EQUAL(raft::fbs::any_message_vote_request, msg->message_type());
  auto rv_msg = static_cast<const raft::fbs::vote_request * >(msg->message());
  BOOST_CHECK_EQUAL(0, rv_msg->recipient_id());

  typedef raft::fbs::vote_request_traits vote_request_traits;
  vote_request_traits::arg_type arg(msg, [](){});
  BOOST_CHECK_EQUAL(82345U, vote_request_traits::request_id(arg));
  BOOST_CHECK_EQUAL(0U, vote_request_traits::recipient_id(arg));
  BOOST_CHECK_EQUAL(3U, vote_request_traits::term_number(arg));
  BOOST_CHECK_EQUAL(1U, vote_request_traits::candidate_id(arg));
  BOOST_CHECK_EQUAL(3U, vote_request_traits::log_index_end(arg));
  BOOST_CHECK_EQUAL(2U, vote_request_traits::last_log_term(arg));
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

  raft::fbs::append_entry_requestBuilder aeb(fbb);
  aeb.add_request_id(9988);
  aeb.add_recipient_id(10345);
  aeb.add_term_number(82342);
  aeb.add_leader_id(7342);
  aeb.add_log_index_begin(3434);
  aeb.add_previous_log_term(55234);
  aeb.add_leader_commit_index_end(525123);
  aeb.add_entries(e);
  auto ae = aeb.Finish();
  auto m = raft::fbs::Createraft_message(fbb, raft::fbs::any_message_append_entry_request, ae.Union());

  fbb.Finish(m);

  const raft::fbs::raft_message * msg = raft::fbs::Getraft_message(fbb.GetBufferPointer());
  BOOST_CHECK_EQUAL(raft::fbs::any_message_append_entry_request, msg->message_type());
  auto ae_msg = static_cast<const raft::fbs::append_entry_request * >(msg->message());
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
  typedef raft::fbs::append_entry_request_traits append_entry_request_traits;
  append_entry_request_traits::arg_type arg(msg, [](){});
  BOOST_CHECK_EQUAL(9988U, append_entry_request_traits::request_id(arg));
  BOOST_CHECK_EQUAL(10345U, append_entry_request_traits::recipient_id(arg));
  BOOST_CHECK_EQUAL(2U, append_entry_request_traits::num_entries(arg));
  {
    typedef raft::fbs::log_entry_traits log_entry_traits;
    auto le = &append_entry_request_traits::get_entry(arg, 0);
    BOOST_CHECK_EQUAL(2834U, log_entry_traits::term(le));
    BOOST_REQUIRE(log_entry_traits::is_command(le));
    BOOST_CHECK_EQUAL(::strlen(lit), raft::slice::buffer_size(log_entry_traits::data(le)));
  }
  {
    typedef raft::fbs::log_entry_traits let;
    typedef raft::fbs::configuration_description_traits cdt;
    typedef raft::fbs::simple_configuration_description_traits scdt;
    typedef raft::fbs::server_description_traits sdt;
    auto le = &append_entry_request_traits::get_entry(arg, 1);
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

class flatbuffer_base_communicator
{
public:
  typedef size_t endpoint;

  void send(endpoint ep, const std::string& address, std::pair<const raft::fbs::raft_message *, raft::util::call_on_delete> && msg)
  {
    q2.push_front(std::move(msg));
  }
  std::deque<std::pair<const raft::fbs::raft_message *, raft::util::call_on_delete>> q2;
};

struct flatbuffer_communicator_metafunction
{
  template <typename _Messages>
  struct apply
  {
    typedef raft::util::builder_communicator<raft::fbs::messages, raft::fbs::builders, flatbuffer_base_communicator> type;
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
  
typedef raft::protocol<flatbuffer_communicator_metafunction, raft::fbs::messages> test_raft_type;

BOOST_AUTO_TEST_CASE(RaftFlatBufferConstructProtocolTest)
{
  test_raft_type::communicator_type comm;
  test_raft_type::log_type l;
  test_raft_type::checkpoint_data_store_type store;

  raft::native::configuration_description five_servers;
  five_servers.from.servers = {{0, "192.168.1.1"}, {1, "192.168.1.2"}, {2, "192.168.1.3"}, {3, "192.168.1.4"},  {4, "192.168.1.5"}};
  test_raft_type::configuration_manager_type cm(0);
  
  l.append(raft::fbs::log_entry_traits::create_configuration(0, 0, configuration_description_view(five_servers)));
  l.update_header(0, test_raft_type::INVALID_PEER_ID());
  test_raft_type proto(comm, l, store, cm);

  flatbuffers::FlatBufferBuilder fbb;
  raft::fbs::vote_requestBuilder rvb(fbb);
  uint64_t term=1;
  rvb.add_recipient_id(0);
  rvb.add_term_number(term);
  rvb.add_candidate_id(1);
  rvb.add_log_index_end(1);
  rvb.add_last_log_term(0);
  auto rv = rvb.Finish();
  auto m = raft::fbs::Createraft_message(fbb, raft::fbs::any_message_vote_request, rv.Union());
  fbb.Finish(m);

  raft::fbs::vote_request_traits::arg_type arg(raft::fbs::Getraft_message(fbb.GetBufferPointer()), [](){});
  proto.on_vote_request(std::move(arg));
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



