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

#include "asio/asio_server.hh"
#include "asio/asio_block_device.hh"
#include "leveldb_log.hh"
#include "messages.hh"
#include "posix_file.hh"
#include "protocol.hh"

#define BOOST_TEST_MODULE RaftTests
#include <boost/test/unit_test.hpp>

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

BOOST_AUTO_TEST_CASE(RaftAsioSerializationTest)
{
  raft::native::messages::append_entry_type msg;
  msg.recipient_id = 9032345;
  msg.term_number = 99234;
  msg.leader_id = 23445234;
  msg.previous_log_index = 734725345;
  msg.previous_log_term = 3492385345;
  msg.leader_commit_index = 3483458;
  raft::native::messages::append_entry_type::log_entry_type e;
  e.term = 93443434542;
  e.type = raft::native::messages::append_entry_type::log_entry_type::COMMAND;
  e.data = "fjasdjfa;sldfjalsdjfldskfjsdlkfjasldfjl";
  msg.entry.push_back(std::move(e));
  e.term = 93443434534;
  e.type = raft::native::messages::append_entry_type::log_entry_type::CONFIGURATION;
  raft::server_description s;
  s.id = 333334323;
  s.address = "127.0.0.1:7777";
  e.configuration.from.servers.push_back(std::move(s));
  msg.entry.push_back(std::move(e));

  std::array<uint8_t, 64*1024> buf;
  auto result = raft::asio::serialization::serialize(boost::asio::buffer(buf), msg);
  auto header = boost::asio::buffer_cast<const raft::asio::rpc_header *>(result);
  BOOST_CHECK_EQUAL(2U, header->operation);
  BOOST_CHECK_EQUAL(boost::asio::buffer_size(result), header->payload_length+sizeof(raft::asio::rpc_header));
  raft::native::messages::append_entry_type msg1;
  raft::asio::serialization::deserialize(result+sizeof(raft::asio::rpc_header), msg1);
  BOOST_CHECK_EQUAL(msg.recipient_id, msg1.recipient_id);
  BOOST_CHECK_EQUAL(2U, msg1.entry.size());
}

#include "flatbuffers/raft_generated.h"
#include "flatbuffers/raft_flatbuffer_messages.hh"

class flatbuffer_test_communicator
{
public:
  typedef size_t endpoint;

  void send(endpoint ep, const std::string& address, raft::fbs::flatbuffer_builder_adapter && msg)
  {
    q.push_front(std::move(msg));
  }
  
  void vote_request(endpoint ep, const std::string & address,
		    uint64_t recipient_id,
		    uint64_t term_number,
		    uint64_t candidate_id,
		    uint64_t last_log_index,
		    uint64_t last_log_term)
  {
    raft::fbs::request_vote_sender<flatbuffer_test_communicator> sender(*this, ep, address);
    sender.send(recipient_id,
		term_number,
		candidate_id,
		last_log_index,
		last_log_term);
  }

  template<typename EntryProvider>
  void append_entry(endpoint ep, const std::string& address,
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
    sender.send(recipient_id,
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
			     uint64_t begin_index,
			     uint64_t last_index,
			     bool success)
  {
    raft::fbs::append_entry_response_sender<flatbuffer_test_communicator> sender(*this, ep, address);
    sender.send(recipient_id, term_number, request_term_number, begin_index, last_index, success);
  }

  void vote_response(endpoint ep, const std::string& address,
		     uint64_t peer_id,
		     uint64_t term_number,
		     uint64_t request_term_number,
		     bool granted)
  {
    raft::fbs::vote_response_sender<flatbuffer_test_communicator> sender(*this, ep, address);
    sender.send(peer_id, term_number, request_term_number, granted);
  }

  void append_checkpoint_chunk(endpoint ep, const std::string& address,
			       uint64_t recipient_id,
			       uint64_t term_number,
			       uint64_t leader_id,
			       uint64_t last_checkpoint_index,
			       uint64_t last_checkpoint_term,
			       raft::configuration_checkpoint<raft::configuration_description> last_checkpoint_configuration,
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
					uint64_t bytes_stored)
  {
    raft::fbs::append_checkpoint_chunk_response_sender<flatbuffer_test_communicator> sender(*this, ep, address);
    sender.send(recipient_id, term_number, request_term_number, bytes_stored);
  }

  std::deque<raft::fbs::flatbuffer_builder_adapter> q;
};

struct flatbuffer_communicator_metafunction
{
  template <typename _Messages>
  struct apply
  {
    typedef flatbuffer_test_communicator type;
  };
};

typedef raft::protocol<flatbuffer_communicator_metafunction, raft::fbs::messages> test_raft_type;

BOOST_AUTO_TEST_CASE(RaftFlatBufferConstructProtocolTest)
{
  test_raft_type::communicator_type comm;
  test_raft_type::client_type c;
  test_raft_type::log_type l;
  test_raft_type::checkpoint_data_store_type store;

  raft::configuration_description five_servers;
  five_servers.from.servers = {{0, "192.168.1.1"}, {1, "192.168.1.2"}, {2, "192.168.1.3"}, {3, "192.168.1.4"},  {4, "192.168.1.5"}};
  test_raft_type::configuration_manager_type cm(0);
  
  l.append(raft::fbs::log_entry_traits::create_configuration(0, raft::configuration_description_view(five_servers)));
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

  proto.on_request_vote(raft::fbs::Getraft_message(fbb.GetBufferPointer()));
  BOOST_CHECK(proto.log_header_sync_required());
  BOOST_CHECK_EQUAL(term, proto.current_term());
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, proto.get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  proto.on_log_header_sync();
  BOOST_CHECK(!proto.log_header_sync_required());
  BOOST_CHECK_EQUAL(term, proto.current_term());
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, proto.get_state());
  BOOST_CHECK_EQUAL(1U, comm.q.size());
}

BOOST_AUTO_TEST_CASE(RaftFlatBufferRequestVoteTest)
{
  flatbuffers::FlatBufferBuilder fbb;
  raft::fbs::request_voteBuilder rvb(fbb);
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
  BOOST_CHECK_EQUAL(0U, request_vote_traits::recipient_id(msg));
  BOOST_CHECK_EQUAL(3U, request_vote_traits::term_number(msg));
  BOOST_CHECK_EQUAL(1U, request_vote_traits::candidate_id(msg));
  BOOST_CHECK_EQUAL(3U, request_vote_traits::last_log_index(msg));
  BOOST_CHECK_EQUAL(2U, request_vote_traits::last_log_term(msg));
}

BOOST_AUTO_TEST_CASE(RaftFlatBufferAppendEntryTest)
{
  flatbuffers::FlatBufferBuilder fbb;
  std::vector<flatbuffers::Offset<raft::fbs::log_entry>> entries;
  const char * lit = "This is my log entry data";
  auto str = fbb.CreateString(lit);
  raft::fbs::log_entryBuilder leb(fbb);
  leb.add_type(raft::fbs::log_entry_type_COMMAND);
  leb.add_term(2834);
  leb.add_data(str);
  entries.push_back(leb.Finish());

  std::vector<flatbuffers::Offset<raft::fbs::server_description>> servers_vec;
  const char * server0 = "127.0.0.1:8342";
  servers_vec.push_back(raft::fbs::Createserver_description(fbb, 0, fbb.CreateString(server0)));
  const char * server1 = "127.0.0.1:8343";
  servers_vec.push_back(raft::fbs::Createserver_description(fbb, 1, fbb.CreateString(server1)));
  auto from_desc = raft::fbs::Createsimple_configuration_description(fbb, fbb.CreateVector(servers_vec));
  servers_vec.clear();
  auto to_desc = raft::fbs::Createsimple_configuration_description(fbb, fbb.CreateVector(servers_vec));
  auto conf = raft::fbs::Createconfiguration_description(fbb, from_desc, to_desc);
  raft::fbs::log_entryBuilder leb2(fbb);
  leb2.add_type(raft::fbs::log_entry_type_CONFIGURATION);
  leb2.add_term(2833);
  leb2.add_configuration(conf);
  entries.push_back(leb2.Finish());
  
  auto e = fbb.CreateVector(entries);  

  raft::fbs::append_entryBuilder aeb(fbb);
  aeb.add_recipient_id(10345);
  aeb.add_term_number(82342);
  aeb.add_leader_id(7342);
  aeb.add_previous_log_index(3434);
  aeb.add_previous_log_term(55234);
  aeb.add_leader_commit_index(525123);
  aeb.add_entry(e);
  auto ae = aeb.Finish();
  auto m = raft::fbs::Createraft_message(fbb, raft::fbs::any_message_append_entry, ae.Union());

  fbb.Finish(m);

  const raft::fbs::raft_message * msg = raft::fbs::Getraft_message(fbb.GetBufferPointer());
  BOOST_CHECK_EQUAL(raft::fbs::any_message_append_entry, msg->message_type());
  auto ae_msg = static_cast<const raft::fbs::append_entry * >(msg->message());
  BOOST_CHECK_EQUAL(10345U, ae_msg->recipient_id());
  BOOST_CHECK_EQUAL(2U, ae_msg->entry()->size());
  BOOST_CHECK_EQUAL(raft::fbs::log_entry_type_COMMAND, ae_msg->entry()->Get(0)->type());
  BOOST_CHECK_EQUAL(2834, ae_msg->entry()->Get(0)->term());
  BOOST_CHECK_EQUAL(strlen(lit), ae_msg->entry()->Get(0)->data()->size());
  BOOST_CHECK_EQUAL(0, ::memcmp(lit, ae_msg->entry()->Get(0)->data()->data(), ae_msg->entry()->Get(0)->data()->size()));
  BOOST_CHECK_EQUAL(raft::fbs::log_entry_type_CONFIGURATION, ae_msg->entry()->Get(1)->type());
  BOOST_CHECK_EQUAL(2833, ae_msg->entry()->Get(1)->term());
  BOOST_CHECK_EQUAL(2U, ae_msg->entry()->Get(1)->configuration()->from()->servers()->size());
  BOOST_CHECK_EQUAL(0, ae_msg->entry()->Get(1)->configuration()->from()->servers()->Get(0)->id());
  BOOST_CHECK_EQUAL(1, ae_msg->entry()->Get(1)->configuration()->from()->servers()->Get(1)->id());

  typedef raft::fbs::append_entry_traits append_entry_traits;
  BOOST_CHECK_EQUAL(10345U, append_entry_traits::recipient_id(msg));
}

BOOST_AUTO_TEST_CASE(RaftAsioTest)
{
  boost::asio::io_service ios;
  raft::simple_configuration_description config;
  config.servers = {{0, "127.0.0.1:9133"}, {1, "127.0.0.1:9134"}, {2, "127.0.0.1:9135"}};
  boost::asio::ip::tcp::endpoint ep1(boost::asio::ip::tcp::v4(), 9133);
  raft::asio::tcp_server s1(ios, 0, ep1, config, "/Users/dblair/tmp/raft_log_dir_1");
  boost::asio::ip::tcp::endpoint ep2(boost::asio::ip::tcp::v4(), 9134);
  raft::asio::tcp_server s2(ios, 1, ep2, config, "/Users/dblair/tmp/raft_log_dir_2");
  boost::asio::ip::tcp::endpoint ep3(boost::asio::ip::tcp::v4(), 9135);
  raft::asio::tcp_server s3(ios, 2, ep3, config, "/Users/dblair/tmp/raft_log_dir_3");

  for(std::size_t i=0; i<1000; ++i) {
    boost::system::error_code ec;
    ios.run_one(ec);
  }
  // ios.run();
  
}

