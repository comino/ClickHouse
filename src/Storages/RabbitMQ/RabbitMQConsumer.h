#pragma once

#include <Core/Names.h>
#include <base/types.h>
#include <IO/ReadBuffer.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/saturatedDuration.h>

#include <functional>

namespace Poco
{
class Logger;
}

namespace AMQP
{
class TcpChannel;
}

namespace DB
{

class RabbitMQHandler;
class RabbitMQConnection;
using ChannelPtr = std::unique_ptr<AMQP::TcpChannel>;
static constexpr auto SANITY_TIMEOUT = 1000 * 60 * 10; /// 10min.
using LoggerPtr = std::shared_ptr<Poco::Logger>;

class RabbitMQConsumer
{

public:
    RabbitMQConsumer(
        RabbitMQHandler & event_handler_,
        std::vector<String> & queues_,
        size_t channel_id_base_,
        const String & channel_base_,
        LoggerPtr log_,
        uint32_t queue_size_);

    struct CommitInfo
    {
        UInt64 delivery_tag = 0;
        String channel_id;
        std::vector<UInt64> failed_delivery_tags;
    };

    struct MessageData
    {
        String message;
        String message_id;
        UInt64 timestamp = 0;
        bool redelivered = false;
        UInt64 delivery_tag = 0;
        String channel_id;
    };

    const MessageData & currentMessage() { return current; }
    /// Read on the event loop thread, which is the one that sets it.
    String getChannelID() const;
    UInt16 getPrefetchCount() const { return prefetch_count; }

    /// Return read buffer containing next available message
    /// or nullptr if there are no messages to process.
    ReadBufferPtr consume();

    bool needChannelUpdate();
    void updateChannel(RabbitMQConnection & connection);

    void stop();
    bool isConsumerStopped() const { return stopped.load(); }

    /// Sent on the event loop thread; the call returns once the frame has been handed to AMQP-CPP.
    bool ackMessages(const CommitInfo & commit_info);
    bool nackMessages(const CommitInfo & commit_info, bool requeue = false);

    bool hasPendingMessages() { return !received.empty(); }

    /// Queues a delivery and wakes a reader parked in `waitForMessages`. Runs on the event loop thread; public for tests.
    void pushMessage(MessageData && message);

    void waitForMessages(std::optional<uint64_t> timeout_ms = std::nullopt, std::function<bool()> is_cancelled = {})
    {
        std::unique_lock lock(mutex);
        if (!timeout_ms)
            timeout_ms = SANITY_TIMEOUT;
        cv.wait_for(lock, saturatedMilliseconds(*timeout_ms),
            [&]{ return !received.empty() || isConsumerStopped() || (is_cancelled && is_cancelled()); });
    }

    /// Wake a source parked in `waitForMessages` so it can re-check its cancellation state.
    void wakeUp();

    void closeConnections();

    /// True once the broker has answered the channel.close sent by closeConnections(). A consumer
    /// that never had a channel reports true, as there is nothing to wait for.
    bool isChannelCloseCompleted() const { return !close_state || close_state->completed; }

    /// Stop expecting that answer, so a callback dispatched later becomes a no-op.
    void abandonChannelClose() { if (close_state) close_state->abandoned = true; }

private:
    void subscribe();
    bool ackMessagesImpl(const CommitInfo & commit_info);
    bool nackMessagesImpl(const CommitInfo & commit_info, bool requeue);
    void updateCommitInfo(CommitInfo record);

    /// Shared by value into the close() callbacks, mirroring RabbitMQProducer::finish, so that a
    /// late dispatch after the wait was abandoned cannot touch a destroyed consumer.
    struct ChannelCloseState
    {
        std::atomic<bool> completed = false;
        std::atomic<bool> abandoned = false;
    };
    std::shared_ptr<ChannelCloseState> close_state;

    ChannelPtr consumer_channel; /// Touched only on the event loop thread (see RabbitMQHandler::run).
    RabbitMQHandler & event_handler; /// Used concurrently, but is thread safe.

    const std::vector<String> queues;
    const String channel_base;
    const size_t channel_id_base;
    /// basic.qos per queue subscription. It bounds `received`, which must never block the event loop thread:
    /// the streaming thread waits for that thread's answers, see https://github.com/ClickHouse/ClickHouse/issues/54775.
    const UInt16 prefetch_count;

    LoggerPtr log;
    std::atomic<bool> stopped;

    String channel_id;
    UInt64 channel_id_counter = 0;

    enum class State : uint8_t
    {
        NONE,
        INITIALIZING,
        OK,
        ERROR,
    };
    std::atomic<State> state = State::NONE;
    size_t subscriptions_num = 0;

    ConcurrentBoundedQueue<MessageData> received;
    MessageData current;

    UInt64 last_commited_delivery_tag = 0;

    std::condition_variable cv;
    std::mutex mutex;
};

}
