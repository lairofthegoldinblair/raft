#define BOOST_TEST_MODULE CircularBufferTests
#include "examples/circular_buffer/circular_buffer.hh"

#include "native/messages.hh"
#include "native/serialization.hh"
#include "flatbuffers/raft_flatbuffer_messages.hh"
#include "flatbuffers/serialization.hh"

#include "asio/asio_checkpoint.hh"
#include "asio/asio_server.hh"

#include "test_utilities.hh"

#include <boost/filesystem.hpp>
#include <boost/log/core.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/sinks/text_file_backend.hpp>
#include <boost/test/unit_test.hpp>

struct log_init
{
  log_init()
  {
    boost::log::core::get()->set_filter(boost::log::trivial::severity >= boost::log::trivial::warning);
  }    
};

static log_init _log_init_;

static void create_test_directory(boost::filesystem::path & dir)
{
  int ret = ::mkdir(dir.c_str(), S_IRUSR | S_IWUSR | S_IXUSR);
  if (-1 == ret) {
    throw std::runtime_error((boost::format("Failed to create test directory %1%") % dir).str());
  }
}

// Ensure we start with a clean log directory
static std::string log_directory(uint32_t i)
{
  boost::filesystem::path dir(::getenv("HOME"));
  dir /= "tmp";
  if (!boost::filesystem::exists(dir)) {
    create_test_directory(dir);
  }
  dir /= (boost::format("raft_log_dir_%1%") % i).str();
  if (boost::filesystem::exists(dir)) {
    boost::filesystem::remove_all(dir);
  }
  create_test_directory(dir);
  return dir.c_str();
}

template<typename _TestType>
std::pair<const typename _TestType::messages_type::checkpoint_header_type *, raft::util::call_on_delete> make_checkpoint_header()
{
  typedef typename _TestType::builders_type::append_checkpoint_chunk_request_builder_type append_checkpoint_chunk_request_builder;
  typedef typename _TestType::messages_type::append_checkpoint_chunk_request_traits_type append_checkpoint_chunk_request_traits_type;
  append_checkpoint_chunk_request_builder accb;
  {
    auto chb = accb.last_checkpoint_header();
    {
      auto cdb = chb.index(0).log_entry_index_end(0).last_log_entry_term(0).last_log_entry_cluster_time(0).configuration();
      {
        raft::test::RaftTestFixtureBase<_TestType>::add_five_servers(cdb.from());
	
      }
      {
        auto fsb = cdb.to();
      }
    }
  }
  auto tmp = std::make_unique<typename append_checkpoint_chunk_request_traits_type::arg_type>(std::move(accb.finish()));
  const auto & header = append_checkpoint_chunk_request_traits_type::last_checkpoint_header(*tmp);
  raft::util::call_on_delete deleter([ptr = std::move(tmp)](){});
  return std::make_pair(&header, std::move(deleter));
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

typedef boost::mpl::list<native_test_type, flatbuffers_test_type> test_types;

BOOST_AUTO_TEST_CASE_TEMPLATE(CircularBufferInMemoryCheckpointTest, _TestType, test_types)
{
  typedef logger<typename _TestType::messages_type, typename _TestType::serialization_type, raft::test::in_memory_checkpoint_metafunction> state_machine_type;
  typedef typename state_machine_type::checkpoint_data_store_type checkpoint_data_store_type;

  
  state_machine_type state_machine;
  state_machine.commands.push_back("foo");
  state_machine.commands.push_back("bar");
  state_machine.commands.push_back("bat");

  checkpoint_data_store_type store;
  auto ckpt = store.create(make_checkpoint_header<_TestType>());
  state_machine.start_checkpoint();
  while(!state_machine.checkpoint(std::chrono::steady_clock::now(), ckpt, [](std::chrono::time_point<std::chrono::steady_clock> , bool){}));
  BOOST_TEST(25U == ckpt->size());
}

template<typename _TestType>
struct checkpoint_metafunction
{
  template <typename _Messages>
  struct apply
  {
    typedef raft::asio::checkpoint_data_store<_Messages, typename _TestType::serialization_type, raft::asio::single_thread_checkpoint_policy> type;
  };
};

BOOST_AUTO_TEST_CASE_TEMPLATE(CircularBufferAsioCheckpointTest, _TestType, test_types)
{
  typedef logger<typename _TestType::messages_type, typename _TestType::serialization_type, checkpoint_metafunction<_TestType> > state_machine_type;
  typedef typename state_machine_type::checkpoint_data_store_type checkpoint_data_store_type;
  typedef typename state_machine_type::checkpoint_data_store_type::checkpoint_data_ptr checkpoint_data_ptr;

  
  state_machine_type state_machine;
  state_machine.commands.push_back("foo");
  state_machine.commands.push_back("bar");
  state_machine.commands.push_back("bat");

  boost::asio::io_context ioc;
  raft::asio::single_thread_checkpoint_policy::service_type checkpoint_service(ioc);
  checkpoint_data_store_type store(checkpoint_service, log_directory(1));

  struct checkpointer
  {
    state_machine_type & machine;
    checkpoint_data_ptr ckpt;

    void on_write(std::chrono::time_point<std::chrono::steady_clock> clock_now, bool complete)
    {
      if (!complete) {
        machine.checkpoint(clock_now, ckpt, [this](std::chrono::time_point<std::chrono::steady_clock> clock_now, bool complete) { this->on_write(clock_now, complete); });
      }
    }
  };
  checkpointer ckpt = { state_machine, store.create(make_checkpoint_header<_TestType>()) };
  ckpt.machine.start_checkpoint();
  ckpt.machine.checkpoint(std::chrono::steady_clock::now(),
                          ckpt.ckpt,
                          [&ckpt](std::chrono::time_point<std::chrono::steady_clock> clock_now, bool complete) { ckpt.on_write(clock_now, complete); });
  ioc.run();
  bool commited = false;
  store.commit(std::chrono::steady_clock::now(), ckpt.ckpt, [&commited](std::chrono::time_point<std::chrono::steady_clock> clock_now) { commited = true; });
  ioc.reset();
  ioc.run();
  BOOST_TEST(commited);
}

BOOST_AUTO_TEST_CASE_TEMPLATE(CircularBufferAsioCheckpointPerfTest, _TestType, test_types)
{
  typedef logger<typename _TestType::messages_type, typename _TestType::serialization_type, checkpoint_metafunction<_TestType> > state_machine_type;
  typedef typename state_machine_type::checkpoint_data_store_type checkpoint_data_store_type;
  typedef typename state_machine_type::checkpoint_data_store_type::checkpoint_data_ptr checkpoint_data_ptr;

  
  char buf[51];
  memset(&buf[0], int('a'), 50);
  buf[50] = 0;
  state_machine_type state_machine;
  for(std::size_t i=0; i<1000000; ++i) {
    sprintf(&buf[0], "%ld", i);
    state_machine.commands.push_back(buf);
  }

  boost::asio::io_context ioc;
  raft::asio::single_thread_checkpoint_policy::service_type checkpoint_service(ioc);
  checkpoint_data_store_type store(checkpoint_service, log_directory(1));

  struct checkpointer
  {
    state_machine_type & machine;
    checkpoint_data_ptr ckpt;

    void on_write(std::chrono::time_point<std::chrono::steady_clock> clock_now, bool complete)
    {
      if (!complete) {
        machine.checkpoint(clock_now, ckpt, [this](std::chrono::time_point<std::chrono::steady_clock> clock_now, bool complete) { this->on_write(clock_now, complete); });
      }
    }
  };
  auto tick = std::chrono::steady_clock::now();
  checkpointer ckpt = { state_machine, store.create(make_checkpoint_header<_TestType>()) };
  ckpt.machine.start_checkpoint();
  ckpt.machine.checkpoint(std::chrono::steady_clock::now(),
                          ckpt.ckpt,
                          [&ckpt](std::chrono::time_point<std::chrono::steady_clock> clock_now, bool complete) { ckpt.on_write(clock_now, complete); });
  ioc.run();
  bool commited = false;
  store.commit(std::chrono::steady_clock::now(), ckpt.ckpt, [&commited](std::chrono::time_point<std::chrono::steady_clock> clock_now) { commited = true; });
  ioc.reset();
  ioc.run();
  BOOST_TEST(commited);
  auto tock = std::chrono::steady_clock::now();
  std::cout << "Time to write: " << std::chrono::duration_cast<std::chrono::milliseconds>(tock-tick).count() << "ms" << std::endl;
}
