#include <string>
#include <vector>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/format.hpp>
#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>
#include <boost/program_options.hpp>
#include <boost/timer/timer.hpp>
#include "asio/asio_client.hh"
#include "flatbuffers/raft_flatbuffer_messages.hh"
#include "flatbuffers/serialization.hh"

namespace po = boost::program_options;

template<typename _TestType>
int run_client(int argc, char ** argv)
{
  typedef raft::asio::synchronous_client<typename _TestType::messages_type, typename _TestType::builders_type, typename _TestType::serialization_type> client_type;

  std::vector<std::string> servers;
  uint32_t num_commands;
  uint32_t command_size;
  std::string loglev;
  po::options_description desc("Allowed options");
  desc.add_options()
    ("help", "produce help message")
    ("server", po::value<std::vector<std::string>>(&servers), "server in initial cluster")
    ("num-commands", po::value<uint32_t>(&num_commands)->default_value(100), "number of commands to send to the server")
    ("command-size", po::value<uint32_t>(&command_size)->default_value(50), "size of each command to send")
    ("loglevel", po::value<std::string>(&loglev)->default_value("info"), "log level (trace,debug,info,warning,error)")
    ;

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);

  if (0 < vm.count("help") || 0 == vm.count("server")) {
    std::cerr << desc << "\n";
    return 1;
  }

  if (boost::algorithm::iequals("trace", loglev)) {
    boost::log::core::get()->set_filter(boost::log::trivial::severity >= boost::log::trivial::trace);
  } else if (boost::algorithm::iequals("debug", loglev)) {
    boost::log::core::get()->set_filter(boost::log::trivial::severity >= boost::log::trivial::debug);
  } else if (boost::algorithm::iequals("info", loglev)) {
    boost::log::core::get()->set_filter(boost::log::trivial::severity >= boost::log::trivial::info);
  } else if (boost::algorithm::iequals("warning", loglev)) {
    boost::log::core::get()->set_filter(boost::log::trivial::severity >= boost::log::trivial::warning);
  } else if (boost::algorithm::iequals("error", loglev)) {
    boost::log::core::get()->set_filter(boost::log::trivial::severity >= boost::log::trivial::error);
  }      

  // 8133 ports are for clients to connect
  try {
    std::vector<boost::asio::ip::tcp::endpoint> eps;
    for(auto & s : servers) {
      eps.emplace_back(boost::asio::ip::make_address_v4(s), 8133);
    }
    client_type cli (std::move(eps));

    std::vector<uint8_t> buf(command_size, 100);
    boost::timer::cpu_timer timer;
    timer.start();
    auto sess = cli.open_session();
    for(uint32_t i=0; i<num_commands; ++i) {
      snprintf(reinterpret_cast<char *>(&buf[0]), command_size, "This is command %u", i);
      sess.send_command(raft::slice(&buf[0], command_size));
    }
    timer.stop();
    BOOST_LOG_TRIVIAL(info) << "[main] total time: " << timer.format()
                            << ", rate: " << (1000000000LL * num_commands) / timer.elapsed().wall
                            << " commands/sec";
  } catch(std::exception & ex) {
    std::cerr << "Client exiting with exception : " << ex.what() << std::endl;
    return 1;
  }
  return 0;
}

class flatbuffers_test_type
{
public:
  typedef raft::fbs::messages messages_type;
  typedef raft::fbs::builders builders_type;
  typedef raft::fbs::serialization serialization_type;
};

int main(int argc, char ** argv)
{
  return run_client<flatbuffers_test_type>(argc, argv);
}
