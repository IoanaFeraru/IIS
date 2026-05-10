package org.datasource.jdbc.views.events;

import lombok.Value;

@Value
public class EventsView {
    private String id;
    private String userId;
    private String eventType;
    private String productId;
    private String sessionId;
    private String metadata;
    private String occurredAt;
}
