package com.avast.clients.rabbitmq.javaapi;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings({"unused", "WeakerAccess"})
public class MessageProperties {
    private String contentType;
    private String contentEncoding;
    private Map<String, Object> headers;
    private Integer deliveryMode;
    private Integer priority;
    private String correlationId;
    private String replyTo;
    private String expiration;
    private String messageId;
    private Date timestamp;
    private String type;
    private String userId;
    private String appId;
    private String clusterId;

    public static Builder newBuilder() {
        return new Builder();
    }

    public MessageProperties(
            String contentType,
            String contentEncoding,
            Map<String, Object> headers,
            Integer deliveryMode,
            Integer priority,
            String correlationId,
            String replyTo,
            String expiration,
            String messageId,
            Date timestamp,
            String type,
            String userId,
            String appId,
            String clusterId) {
        this.contentType = contentType;
        this.contentEncoding = contentEncoding;
        this.headers = headers == null ? null : Collections.unmodifiableMap(new HashMap<>(headers));
        this.deliveryMode = deliveryMode;
        this.priority = priority;
        this.correlationId = correlationId;
        this.replyTo = replyTo;
        this.expiration = expiration;
        this.messageId = messageId;
        this.timestamp = timestamp;
        this.type = type;
        this.userId = userId;
        this.appId = appId;
        this.clusterId = clusterId;
    }

    public MessageProperties() {
    }

    public int getClassId() {
        return 60;
    }

    public String getClassName() {
        return "basic";
    }

    public Builder builder() {
        return new Builder()
                .contentType(contentType)
                .contentEncoding(contentEncoding)
                .headers(headers)
                .deliveryMode(deliveryMode)
                .priority(priority)
                .correlationId(correlationId)
                .replyTo(replyTo)
                .expiration(expiration)
                .messageId(messageId)
                .timestamp(timestamp)
                .type(type)
                .userId(userId)
                .appId(appId)
                .clusterId(clusterId);
    }

    public String getContentType() {
        return this.contentType;
    }

    public String getContentEncoding() {
        return this.contentEncoding;
    }

    public Map<String, Object> getHeaders() {
        return this.headers;
    }

    public Integer getDeliveryMode() {
        return this.deliveryMode;
    }

    public Integer getPriority() {
        return this.priority;
    }

    public String getCorrelationId() {
        return this.correlationId;
    }

    public String getReplyTo() {
        return this.replyTo;
    }

    public String getExpiration() {
        return this.expiration;
    }

    public String getMessageId() {
        return this.messageId;
    }

    public Date getTimestamp() {
        return this.timestamp;
    }

    public String getType() {
        return this.type;
    }

    public String getUserId() {
        return this.userId;
    }

    public String getAppId() {
        return this.appId;
    }

    public String getClusterId() {
        return this.clusterId;
    }

    public static final class Builder {
        private String contentType;
        private String contentEncoding;
        private Map<String, Object> headers;
        private Integer deliveryMode;
        private Integer priority;
        private String correlationId;
        private String replyTo;
        private String expiration;
        private String messageId;
        private Date timestamp;
        private String type;
        private String userId;
        private String appId;
        private String clusterId;

        public Builder() {
        }

        public Builder contentType(String contentType) {
            this.contentType = contentType;
            return this;
        }

        public Builder contentEncoding(String contentEncoding) {
            this.contentEncoding = contentEncoding;
            return this;
        }

        public Builder headers(Map<String, Object> headers) {
            this.headers = headers;
            return this;
        }

        public Builder deliveryMode(Integer deliveryMode) {
            this.deliveryMode = deliveryMode;
            return this;
        }

        public Builder priority(Integer priority) {
            this.priority = priority;
            return this;
        }

        public Builder correlationId(String correlationId) {
            this.correlationId = correlationId;
            return this;
        }

        public Builder replyTo(String replyTo) {
            this.replyTo = replyTo;
            return this;
        }

        public Builder expiration(String expiration) {
            this.expiration = expiration;
            return this;
        }

        public Builder messageId(String messageId) {
            this.messageId = messageId;
            return this;
        }

        public Builder timestamp(Date timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder type(String type) {
            this.type = type;
            return this;
        }

        public Builder userId(String userId) {
            this.userId = userId;
            return this;
        }

        public Builder appId(String appId) {
            this.appId = appId;
            return this;
        }

        public Builder clusterId(String clusterId) {
            this.clusterId = clusterId;
            return this;
        }

        public MessageProperties build() {
            return new MessageProperties
                    (contentType
                            , contentEncoding
                            , headers
                            , deliveryMode
                            , priority
                            , correlationId
                            , replyTo
                            , expiration
                            , messageId
                            , timestamp
                            , type
                            , userId
                            , appId
                            , clusterId
                    );
        }
    }
}