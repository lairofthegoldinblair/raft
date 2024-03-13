#include <boost/algorithm/string/predicate.hpp>
#include <boost/format.hpp>
#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>
#include <boost/program_options.hpp>
#include "asio/asio_server.hh"
#include "flatbuffers/raft_flatbuffer_messages.hh"
#include "flatbuffers/serialization.hh"
#include "protocol.hh"
#include "examples/circular_buffer/circular_buffer.hh"

namespace po = boost::program_options;

template<typename _TestType>
int run_server(int argc, char ** argv)
{
  typedef typename _TestType::messages_type::log_entry_type log_entry_type;
  typedef typename _TestType::builders_type::log_entry_builder_type log_entry_builder;
  typedef typename _TestType::builders_type::open_session_request_builder_type open_session_request_builder;
  typedef typename _TestType::messages_type::open_session_response_traits_type open_session_response_traits;
  typedef typename _TestType::builders_type::close_session_request_builder_type close_session_request_builder;
  typedef typename _TestType::messages_type::close_session_response_traits_type close_session_response_traits;
  typedef typename _TestType::builders_type::linearizable_command_request_builder_type linearizable_command_request_builder;
  typedef typename _TestType::messages_type::client_response_traits_type client_response_traits;
  typedef raft::asio::serialization<typename _TestType::messages_type, typename _TestType::serialization_type> serialization_type;
  typedef logger<typename _TestType::messages_type, typename _TestType::serialization_type, raft::asio::checkpoint_metafunction<typename _TestType::serialization_type>> state_machine_type;
  typedef raft::asio::tcp_server<typename _TestType::messages_type, typename _TestType::builders_type, typename _TestType::serialization_type, state_machine_type> tcp_server_type;

  uint32_t id=0;
  uint32_t checkpoint_interval;
  std::string listen;
  std::vector<std::string> servers;
  std::string journal;
  std::string loglev;
  po::options_description desc("Allowed options");
  desc.add_options()
    ("help", "produce help message")
    ("id", po::value<uint32_t>(&id), "My Id")
    ("listen", po::value<std::string>(&listen), "listen address for this server")
    ("server", po::value<std::vector<std::string>>(&servers), "server in initial cluster")
    ("checkpoint-interval", po::value<uint32_t>(&checkpoint_interval)->default_value(100000), "checkpoint interaval (measured in number of log records)")
    ("journal", po::value<std::string>(&journal), "journal file")
    ("loglevel", po::value<std::string>(&loglev)->default_value("info"), "log level (trace,debug,info,warning,error)")
    ;

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);

  if (0 < vm.count("help") || 0 == vm.count("id") || 0 == vm.count("listen") || 0 == vm.count("journal")) {
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

  try {
    boost::asio::io_service ios;
    auto make_config = [&servers]() {
                         if (servers.size() > 0) {
                           log_entry_builder leb;
                           {
                             auto cb = leb.term(0).configuration();
                             {
                               auto scb = cb.from();
                               for(std::size_t i=0; i<servers.size(); ++i) {
                                 scb.server().id(i).address((boost::format("%1%:9133") % servers[i]).str().c_str());
                               }
                             }
                             cb.to();
                           }
                           return leb.finish();
                         } else {
                           return std::pair<const log_entry_type *, raft::util::call_on_delete>(nullptr, raft::util::call_on_delete());
                         }
                       };

    // 913x ports are for peers to connect to one another                                                                                                                                                               
    // 813x ports are for clients to connect                                                                                                                                                                            
    boost::asio::ip::tcp::endpoint ep1(boost::asio::ip::make_address_v4(listen), 9133);
    boost::asio::ip::tcp::endpoint cep1(boost::asio::ip::make_address_v4(listen), 8133);
    tcp_server_type s1(ios, id, ep1, cep1, make_config(), journal);
    s1.checkpoint_interval(checkpoint_interval);

    ios.run();

    return 0;
  } catch (std::exception & ex) {
    BOOST_LOG_TRIVIAL(error) << "[run_server] Exiting with unhandled exception: " << ex.what();
    return 1;
  } catch (...) {
    BOOST_LOG_TRIVIAL(error) << "[run_server] Exiting with unknown exception";
    return 1;
  }
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
  try {
    return run_server<flatbuffers_test_type>(argc, argv);
  } catch (std::exception & ex) {
    BOOST_LOG_TRIVIAL(error) << "[main] Exiting with unhandled exception: " << ex.what();
    return 1;
  } catch (...) {
    BOOST_LOG_TRIVIAL(error) << "[main] Exiting with unknown exception";
    return 1;
  }
}
