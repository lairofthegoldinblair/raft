#ifndef __RAFT_CALL_ON_DELETE__
#define __RAFT_CALL_ON_DELETE__

#include <memory>

namespace raft {
  namespace util {
    /**
     * I want move-only invokable so I am using a hand-rolled type erasure wrapper.
     * In c++23 I could use std::move_only_function<void()>.
     */
    class move_only_nullary_function
    {
    private:
      class impl
      {
      public:
	virtual ~impl() {}
	virtual void operator()() = 0;
      };

      template <typename _Function>
      class wrapper : public impl
      {
      private:
	_Function fun_;
      public:
	explicit wrapper(_Function && f)
	  :
	  fun_(std::move(f))
	{
	}
	void operator()() override
	{
	  fun_();
	}
      };

      std::unique_ptr<impl> pimpl_;

    public:
      template<typename _Function>
      move_only_nullary_function(_Function && f)
	:
	pimpl_(new wrapper<_Function>(std::move(f)))
      {
      }

      move_only_nullary_function(move_only_nullary_function && f)
	:
	pimpl_(std::move(f.pimpl_))
      {
      }

      move_only_nullary_function() = default;

      move_only_nullary_function(const move_only_nullary_function & ) = delete;

      move_only_nullary_function & operator=(move_only_nullary_function && f)
      {
	pimpl_ = std::move(f.pimpl_);
	return *this;
      }

      move_only_nullary_function & operator=(const move_only_nullary_function & ) = delete;

      void operator()()
      {
	pimpl_->operator()();
      }

      operator bool() const
      {
	return !!pimpl_;
      }
	
    };

    /**
     * We need type erasure for this application.   Since messages may be embedded in different
     * structures from which they are extracted we have to hide the type of the underlying storage
     * to be deleted in order to free the message.
     */
    class call_on_delete
    {
    private:
      move_only_nullary_function fun_;

    public:
      call_on_delete()
      {
      }
      
      template <typename _Callable>
      call_on_delete(_Callable && fun)
	:
	fun_(std::move(fun))
      {
      }

      call_on_delete(const call_on_delete &) = delete;
      call_on_delete(call_on_delete && cod)
	:
	fun_(std::move(cod.fun_))
      {
      }
      
      call_on_delete& operator=(const call_on_delete &) = delete;

      call_on_delete& operator=(call_on_delete && rhs)
      {
	if (fun_) {
	  fun_();
	}
	fun_ = std::move(rhs.fun_);
	return *this;
      }

      ~call_on_delete()
      {
	if (fun_) {
	  fun_();
	}
      }

      operator bool() const
      {
	return fun_;
      }
    };
}
}
#endif
