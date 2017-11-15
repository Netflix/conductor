package com.netflix.conductor.dao.queue;


import java.util.concurrent.TimeUnit;

/**
 * @author Viren
 *
 */
public class Message {

    private String id;

    private String payload;

    private long timeout;

    private int priority;

    private String shard;

    public Message() {

    }

    public Message(String id, String payload) {
        this.id = id;
        this.payload = payload;
    }

    /**
     * @return the id
     */
    public String getId() {
        return id;
    }

    /**
     * @param id
     *            the id to set
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * @return the payload
     */
    public String getPayload() {
        return payload;
    }

    /**
     * @param payload the payload to set
     *
     */
    public void setPayload(String payload) {
        this.payload = payload;
    }

    /**
     *
     * @param timeout Timeout in milliseconds - The message is only given to the consumer after the specified milliseconds have elapsed.
     */
    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    /**
     * Helper method for the {@link #setTimeout(long)}
     * @param time timeout time
     * @param unit unit for the time
     * @see #setTimeout(long)
     */
    public void setTimeout(long time, TimeUnit unit) {
        this.timeout = TimeUnit.MILLISECONDS.convert(time, unit);
    }

    /**
     *
     * @return Returns the timeout for the message
     */
    public long getTimeout() {
        return timeout;
    }

    /**
     * Sets the message priority.  Higher priority message is retrieved ahead of lower priority ones
     * @param priority priority for the message.
     */
    public void setPriority(int priority) {
        if(priority < 0 || priority > 99){
            throw new IllegalArgumentException("prioirty MUST be between 0 and 99 (inclusive)");
        }
        this.priority = priority;
    }

    public int getPriority() {
        return priority;
    }

    /**
     * @return the shard
     */
    public String getShard() {
        return shard;
    }

    /**
     * @param shard the shard to set
     *
     */
    public void setShard(String shard) {
        this.shard = shard;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Message other = (Message) obj;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "Message [id=" + id + ", payload=" + payload + ", timeout=" + timeout + ", priority=" + priority + "]";
    }


}
