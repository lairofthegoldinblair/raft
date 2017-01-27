#ifndef __RAFT_ASIO_SERVER_HH__
#define __RAFT_ASIO_SERVER_HH__

#include "server.hh"

namespace raft {
  namespace asio {
    class tcp_server
    {
    private:
      boost::asio::deadline_timer timer_;

      std::unique_ptr<boost::asio::ip::tcp::acceptor> listener_;
      boost::asio::ip::tcp::socket * listen_socket_;

      raft::server::communicator_type comm_;
      raft::server::client_type c_;
      raft::server::log_type l_;
      raft::server::checkpoint_data_store_type store_;
      raft::server::configuration_manager_type config_manager_;
      std::shared_ptr<raft::server> protocol_;

    public:
      tcp_server(boost::asio::io_service & ios,
		 uint64_t server_id,
		 const boost::asio::ip::tcp::endpoint & e,
		 const raft::server::simple_configuration_description_type & config)
	:
	timer_(ios),
	listener_(nullptr),
	listen_socket_(nullptr),
	config_manager_(server_id)
      {
	std::vector<raft::server::log_entry_type> entries;
	entries.push_back(raft::server::log_entry_type());
	entries.back().type = raft::server::log_entry_type::CONFIGURATION;
	entries.back().term = 0;
	entries.back().configuration.from = config;
	l_.append(entries);
	l_.update_header(entries.back().term, raft::server::INVALID_PEER_ID);
	protocol_.reset(new raft::server(comm_, c_, l_, store_, config_manager_));

	// Setup periodic timer callbacks from ASIO
	timer_.expires_from_now(boost::posix_time::milliseconds(1));
	timer_.async_wait([this](boost::system::error_code ec) {
	    if (!!this->protocol_) {
	      this->protocol_->on_timer();
	      this->timer_.expires_from_now(boost::posix_time::milliseconds(1));
	    }
	  }
	  );

	// Setup listener for interprocess communication
	if (config_manager_.configuration().includes_self()) {
	  // TODO: Handle v6, multiple interfaces
	  using boost::asio::ip::tcp;
	  listener_.reset(new tcp::acceptor(ios, e));
	}
      }
    };
  }
}

#endif
