package com.netflix.conductor.dao.queue;


import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public interface Queue extends Closeable {

    /**
     *
     * @return Returns the name of the queue
     */
    public String getName();

    /**
     *
     * @return Time in milliseconds before the messages that are popped and not acknowledge are pushed back into the queue.
     * @see #ack(String)
     */
    public int getUnackTime();

    /**
     *
     * @param messages messages to be pushed onto the queue
     * @return Returns the list of message ids
     */
    public List<String> push(List<Message> messages);

    /**
     *
     * @param messageCount number of messages to be popped out of the queue.
     * @param wait Amount of time to wait if there are no messages in queue
     * @param unit Time unit for the wait period
     * @return messages.  Can be less than the messageCount if there are fewer messages available than the message count.  If the popped messages are not acknowledge in a timely manner, they are pushed back into the queue.
     * @see #peek(int)
     * @see #ack(String)
     * @see #getUnackTime()
     *
     */
    public List<Message> pop(int messageCount, int wait, TimeUnit unit);

    /**
     * Provides a peek into the queue without taking messages out.
     * @param messageCount number of messages to be peeked.
     * @return List of peeked messages.
     * @see #pop(int, int, TimeUnit)
     */
    public List<Message> peek(int messageCount);

    /**
     * Provides an acknowledgement for the message.  Once ack'ed the message is removed from the queue forever.
     * @param messageId ID of the message to be acknowledged
     * @return true if the message was found pending acknowledgement and is now ack'ed.  false if the message id is invalid or message is no longer present in the queue.
     */
    public boolean ack(String messageId);


    /**
     * Bulk version for {@link #ack(String)}
     * @param messages Messages to be acknowledged.  Each message MUST be populated with id and shard information.
     */
    public void ack(List<Message> messages);

    /**
     * Sets the unack timeout on the message (changes the default timeout to the new value).  Useful when extended lease is required for a message by consumer before sending ack.
     * @param messageId ID of the message to be acknowledged
     * @param timeout time in milliseconds for which the message will remain in un-ack state.  If no ack is received after the timeout period has expired, the message is put back into the queue
     * @return true if the message id was found and updated with new timeout.  false otherwise.
     */
    public boolean setUnackTimeout(String messageId, long timeout);


    /**
     * Updates the timeout for the message.
     * @param messageId ID of the message to be acknowledged
     * @param timeout time in milliseconds for which the message will remain invisible and not popped out of the queue.
     * @return true if the message id was found and updated with new timeout.  false otherwise.
     */
    public boolean setTimeout(String messageId, long timeout);

    /**
     *
     * @param messageId  Remove the message from the queue
     * @return true if the message id was found and removed.  False otherwise.
     */
    public boolean remove(String messageId);


    /**
     *
     * @param messageId message to be retrieved.
     * @return Retrieves the message stored in the queue by the messageId.  Null if not found.
     */
    public Message get(String messageId);

    /**
     *
     * @return Size of the queue.
     * @see #shardSizes()
     */
    public long size();

    /**
     *
     * @return Map of shard name to the # of messages in the shard.
     * @see #size()
     */
    public Map<String, Map<String, Long>> shardSizes();

    /**
     * Truncates the entire queue.  Use with caution!
     */
    public void clear();
}