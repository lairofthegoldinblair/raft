#include <string>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/circular_buffer.hpp>
#include <boost/format.hpp>
#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>
#include <boost/program_options.hpp>
#include "asio/asio_server.hh"
#include "flatbuffers/raft_flatbuffer_messages.hh"
#include "flatbuffers/serialization.hh"
#include "protocol.hh"

namespace po = boost::program_options;

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

  logger()
    :
    commands(1000000)
  {
  }
  
  boost::circular_buffer<std::string> commands;
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
    commands.push_back(std::string(reinterpret_cast<const char *>(c.data()), c.size()));
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

template<typename _TestType>
int run_server(int argc, char ** argv)
{
  typedef typename _TestType::builders_type::log_entry_builder_type log_entry_builder;
  typedef typename _TestType::builders_type::open_session_request_builder_type open_session_request_builder;
  typedef typename _TestType::messages_type::open_session_response_traits_type open_session_response_traits;
  typedef typename _TestType::builders_type::close_session_request_builder_type close_session_request_builder;
  typedef typename _TestType::messages_type::close_session_response_traits_type close_session_response_traits;
  typedef typename _TestType::builders_type::linearizable_command_request_builder_type linearizable_command_request_builder;
  typedef typename _TestType::messages_type::client_response_traits_type client_response_traits;
  typedef raft::asio::serialization<typename _TestType::messages_type, typename _TestType::serialization_type> serialization_type;
  typedef logger<typename _TestType::messages_type, typename _TestType::serialization_type> state_machine_type;
  typedef raft::asio::tcp_server<typename _TestType::messages_type, typename _TestType::builders_type, typename _TestType::serialization_type, state_machine_type> tcp_server_type;

  uint32_t id=0;
  uint32_t checkpoint_interval;
  std::vector<std::string> servers;
  std::string journal;
  std::string loglev;
  po::options_description desc("Allowed options");
  desc.add_options()
    ("help", "produce help message")
    ("id", po::value<uint32_t>(&id), "My Id")
    ("server", po::value<std::vector<std::string>>(&servers), "server in initial cluster")
    ("checkpoint-interval", po::value<uint32_t>(&checkpoint_interval)->default_value(100000), "checkpoint interaval (measured in number of log records)")
    ("journal", po::value<std::string>(&journal), "journal file")
    ("loglevel", po::value<std::string>(&loglev)->default_value("info"), "log level (trace,debug,info,warning,error)")
    ;

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);

  if (0 < vm.count("help") || 0 == vm.count("id") || 0 == vm.count("server") || 0 == vm.count("journal")) {
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
                       };

    // 913x ports are for peers to connect to one another                                                                                                                                                               
    // 813x ports are for clients to connect                                                                                                                                                                            
    boost::asio::ip::tcp::endpoint ep1(boost::asio::ip::make_address_v4(servers[id]), 9133);
    boost::asio::ip::tcp::endpoint cep1(boost::asio::ip::make_address_v4(servers[id]), 8133);
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
