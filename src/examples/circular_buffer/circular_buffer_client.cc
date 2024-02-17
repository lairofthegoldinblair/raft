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
  std::string loglev;
  po::options_description global("Global options");
  global.add_options()
    ("help", "produce help message")
    ("server", po::value<std::vector<std::string>>(&servers), "server in initial cluster")
    ("loglevel", po::value<std::string>(&loglev)->default_value("info"), "log level (trace,debug,info,warning,error)")
    ("command", po::value<std::string>(), "command to execute")
    ("subargs", po::value<std::vector<std::string> >(), "Arguments for command")
    ;

  po::positional_options_description pos;
  pos.add("command", 1).add("subargs", -1);
  
  po::variables_map vm;
  po::parsed_options parsed = po::command_line_parser(argc, argv).
    options(global).
    positional(pos).
    allow_unregistered().
    run();
  
  po::store(parsed, vm);
  po::notify(vm);

  if (0 < vm.count("help") && 0 == vm.count("command")) {
    std::cerr << global << "\n";
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
  std::vector<boost::asio::ip::tcp::endpoint> eps;
  try {
    for(auto & s : servers) {
      eps.emplace_back(boost::asio::ip::make_address_v4(s), 8133);
    }
  } catch(std::exception & ex) {
    std::cerr << "Client exiting with exception : " << ex.what() << std::endl;
    return 1;
  }  

  std::string cmd = vm["command"].as<std::string>();

  if (cmd == "append") {
    po::options_description inner_desc("append options");
    inner_desc.add_options()
      ("help", "produce help message")
      ("num-commands", po::value<uint32_t>(), "number of commands to send to the server")
      ("command-size", po::value<uint32_t>(), "size of each command to send")
      ;
    // Collect all the unrecognized options from the first pass. This will include the
    // (positional) command name, so we need to erase that.
    std::vector<std::string> opts = po::collect_unrecognized(parsed.options, po::include_positional);
    opts.erase(opts.begin());

    // Parse again...
    po::store(po::command_line_parser(opts).options(inner_desc).run(), vm);
    if (vm.count("help")) {
      std::cout << inner_desc << std::endl;
      return 0;
    }
    uint32_t num_commands = vm.count("num-commands") > 0 ? vm["num-commands"].as<uint32_t>() : 100U;
    uint32_t command_size  = vm.count("command-size") > 0 ? vm["command-size"].as<uint32_t>() : 50U;
    try {
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
  } else if (cmd == "get_configuration") {
    // get_configuration command has the following options:
    po::options_description inner_desc("get_configuration options");
    inner_desc.add_options()
      ("help", "produce help message")
      ;
    // Collect all the unrecognized options from the first pass. This will include the
    // (positional) command name, so we need to erase that.
    std::vector<std::string> opts = po::collect_unrecognized(parsed.options, po::include_positional);
    opts.erase(opts.begin());

    // Parse again...
    po::parsed_options inner_parsed = po::command_line_parser(opts).options(inner_desc).run();
    po::store(inner_parsed, vm);
    if (vm.count("help")) {
      std::cout << inner_desc << std::endl;
      return 0;
    }
    try {
      client_type cli (std::move(eps));
      boost::timer::cpu_timer timer;
      timer.start();
      auto ret = cli.get_configuration();
      timer.stop();
      BOOST_LOG_TRIVIAL(info) << "[main] get_configuration time: " << timer.format();
      if (ret.first != std::numeric_limits<uint64_t>::max()) {
        std::cout << "{ \"id\": " << ret.first << ", \"servers\": [ ";
        for(std::size_t i=0; i<ret.second.size(); ++i) {
          if (i>0) {
            std::cout << ", ";
          }
          std::cout << "{ \"id\": " << ret.second[i].first
                    << ", \"address\": \"" << ret.second[i].second << "\" }";
        }
        std::cout << " ] }";
        // Make JSON and print
        return 0;
      } else {
        return 1;
      }
    } catch(std::exception & ex) {
      std::cerr << "Client exiting with exception : " << ex.what() << std::endl;
      return 1;
    }
  } else if (cmd == "set_configuration") {
    // set_configuration command has the following options:
    po::options_description inner_desc("set_configuration options");
    inner_desc.add_options()
      ("help", "produce help message")
      ("configuration-id", po::value<uint64_t>(), "identifier of existing configuration")
      ("id", po::value<std::vector<uint64_t>>(), "identifiers of servers in configuration")
      ("address", po::value<std::vector<std::string>>(), "addresses of servers in configuration")
      ;
    // Collect all the unrecognized options from the first pass. This will include the
    // (positional) command name, so we need to erase that.
    std::vector<std::string> opts = po::collect_unrecognized(parsed.options, po::include_positional);
    opts.erase(opts.begin());

    // Parse again...
    po::parsed_options inner_parsed = po::command_line_parser(opts).options(inner_desc).run();
    po::store(inner_parsed, vm);
    if (vm.count("help")) {
      std::cout << inner_desc << std::endl;
      return 0;
    }

    uint64_t old_id=0;
    if (vm.count("configuration-id") > 0) {
      old_id = vm["configuration-id"].as<uint64_t>();
    } else {
      std::cerr << "Must specify existing configuration id" << std::endl;
      return 1;
    }
    std::vector<uint64_t> config_ids;
    std::vector<std::string> config_addresses;

    if (vm.count("id") > 0) {
      config_ids = vm["id"].as<std::vector<uint64_t>>();
    } 
    if (vm.count("address") > 0) {
      config_addresses = vm["address"].as<std::vector<std::string>>();
    } 
    if (config_addresses.size() != config_ids.size()) {
      std::cerr << "Must specify same number of ids and addresses" << std::endl;
      return 1;
    }

    if (config_addresses.size() == 0) {
      std::cerr << "Cannot specify an empty configuration" << std::endl;
      return 1;
    }

    std::vector<std::pair<uint64_t, std::string>> config;
    for(std::size_t i=0; i<config_addresses.size(); ++i) {
      config.emplace_back(config_ids[i], config_addresses[i]);
    }
    try {
      client_type cli (std::move(eps));
      boost::timer::cpu_timer timer;
      timer.start();
      auto ret = cli.set_configuration(old_id, config);
      timer.stop();
      BOOST_LOG_TRIVIAL(info) << "[main] set_configuration time: " << timer.format();
      return ret ? 0 : 1;
    } catch(std::exception & ex) {
      std::cerr << "Client exiting with exception : " << ex.what() << std::endl;
      return 1;
    }
  }
  // unrecognised command
  throw po::invalid_option_value(cmd);
  
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
