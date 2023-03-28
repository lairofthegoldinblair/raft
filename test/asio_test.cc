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
#include "native/messages.hh"
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
  raft::native::server_description s;
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

BOOST_AUTO_TEST_CASE(RaftAsioTest)
{
  boost::asio::io_service ios;
  raft::native::simple_configuration_description config;
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

#include "flatbuffers/raft_generated.h"
#include "flatbuffers/raft_flatbuffer_messages.hh"

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
  
  l.append(raft::fbs::log_entry_traits::create_configuration(0, configuration_description_view(five_servers)));
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



