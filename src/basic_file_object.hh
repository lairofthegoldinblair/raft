#ifndef __BASIC_FILE_OBJECT_H__
#define __BASIC_FILE_OBJECT_H__

#include <boost/asio/basic_io_object.hpp>

template<typename FileService>
class basic_file_object : public boost::asio::basic_io_object<FileService>
{
public:
  typedef typename FileService::native_handle_type native_handle_type;
  typedef basic_file_object<FileService> lowest_layer_type;
private:
public:

  explicit basic_file_object(boost::asio::io_service & ios)
    :
    boost::asio::basic_io_object<FileService>(ios)
  {
  }

  explicit basic_file_object(boost::asio::io_service & ios,
			     native_handle_type & handle)
    :
    boost::asio::basic_io_object<FileService>(ios)
  {
    boost::system::error_code ec;
    this->get_service().assign(this->get_implementation(), handle, ec);
    boost::asio::detail::throw_error(ec, "error");
  }

  bool is_open() const
  {
    return this->get_service().is_open(this->get_implementation());
  }

  template<typename MutableBufferSequence, typename ReadHandler>
  BOOST_ASIO_INITFN_RESULT_TYPE(ReadHandler, void (boost::system::error_code, std::size_t))
    async_read(const MutableBufferSequence & buffers,
	       BOOST_ASIO_MOVE_ARG(ReadHandler) handler)
  {
    return this->get_service().async_read(this->get_implementation(), buffers, handler);
  }  

  template<typename ConstBufferSequence, typename WriteHandler>
  BOOST_ASIO_INITFN_RESULT_TYPE(WriteHandler, void (boost::system::error_code, std::size_t))
    async_write(const ConstBufferSequence & buffers,
		BOOST_ASIO_MOVE_ARG(WriteHandler) handler)
  {
    return this->get_service().async_write(this->get_implementation(), buffers, handler);
  }  

  template<typename FileOpHandler>
  BOOST_ASIO_INITFN_RESULT_TYPE(FileOpHandler, void (boost::system::error_code))
    async_sync(BOOST_ASIO_MOVE_ARG(FileOpHandler) handler)
  {
    return this->get_service().async_sync(this->get_implementation(), handler);
  }  
};

#endif
