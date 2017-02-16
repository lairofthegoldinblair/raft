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

BOOST_AUTO_TEST_CASE(LevelDBReaderTest)
{
  raft::leveldb::record_reader r;
}

BOOST_AUTO_TEST_CASE(RaftAsioSerializationTest)
{
  raft::messages::append_entry_type msg;
  msg.recipient_id = 9032345;
  msg.term_number = 99234;
  msg.leader_id = 23445234;
  msg.previous_log_index = 734725345;
  msg.previous_log_term = 3492385345;
  msg.leader_commit_index = 3483458;
  raft::messages::append_entry_type::log_entry_type e;
  e.term = 93443434542;
  e.type = raft::messages::append_entry_type::log_entry_type::COMMAND;
  e.data = "fjasdjfa;sldfjalsdjfldskfjsdlkfjasldfjl";
  msg.entry.push_back(std::move(e));
  e.term = 93443434534;
  e.type = raft::messages::append_entry_type::log_entry_type::CONFIGURATION;
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
  raft::messages::append_entry_type msg1;
  raft::asio::serialization::deserialize(result+sizeof(raft::asio::rpc_header), msg1);
  BOOST_CHECK_EQUAL(msg.recipient_id, msg1.recipient_id);
  BOOST_CHECK_EQUAL(2U, msg1.entry.size());
}

#include "flatbuffers/raft_generated.h"
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
  BOOST_CHECK_EQUAL(2833, ae_msg->entry()->Get(1)->term());
  BOOST_CHECK_EQUAL(2U, ae_msg->entry()->Get(1)->configuration()->from()->servers()->size());
  BOOST_CHECK_EQUAL(0, ae_msg->entry()->Get(1)->configuration()->from()->servers()->Get(0)->id());
  BOOST_CHECK_EQUAL(1, ae_msg->entry()->Get(1)->configuration()->from()->servers()->Get(1)->id());

  class fbs_log_entry_adapter
  {
  private:
    const raft::fbs::log_entry * entry_;
  public:
    explicit fbs_log_entry_adapter(const raft::fbs::log_entry * entry)
      :
      entry_(entry)
    {
    }
    std::size_t from_servers_size() const
    {
      return entry_->configuration() != nullptr &&
	entry_->configuration()->from() != nullptr &&
	entry_->configuration()->from()->servers() != nullptr ?
	entry_->configuration()->from()->servers()->size() :
	0;
    }      
    uint64_t from_servers_id(std::size_t idx) const
    {
      return entry_->configuration()->from()->servers()->Get(idx)->id();
    }      
    const char * from_servers_address(std::size_t i) const
    {
      return entry_->configuration()->from()->servers()->Get(i)->address()->data();
    }          
    std::size_t to_servers_size() const
    {
      return entry_->configuration() != nullptr &&
	entry_->configuration()->to() != nullptr &&
	entry_->configuration()->to()->servers() != nullptr ?
	entry_->configuration()->to()->servers()->size() :
	0;
    }      
    uint64_t to_servers_id(std::size_t i) const
    {
      return entry_->configuration()->to()->servers()->Get(i)->id();
    }      
    const char * to_servers_address(std::size_t i) const
    {
      return entry_->configuration()->to()->servers()->Get(i)->address()->data();
    }          
  };
  
  class fbs_append_entry_adapter
  {
  private:
    const raft::fbs::append_entry * msg_;    
  public:
    uint64_t recipient_id() const
    {
      return msg_->recipient_id();
    }
    fbs_log_entry_adapter entry(std::size_t idx) const
    {
      return fbs_log_entry_adapter(msg_->entry()->Get(idx));
    }
  };
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

