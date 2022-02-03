package de.apnmt.organization.messaging.sender;

import de.apnmt.aws.common.config.AwsCloudProperties;
import de.apnmt.aws.common.util.TracingUtil;
import de.apnmt.common.event.ApnmtEvent;
import de.apnmt.common.event.value.ClosingTimeEventDTO;
import de.apnmt.common.sender.ApnmtEventSender;
import io.awspring.cloud.messaging.core.NotificationMessagingTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

@Repository
public class ClosingTimeEventSender implements ApnmtEventSender<ClosingTimeEventDTO> {

    private static final Logger log = LoggerFactory.getLogger(ClosingTimeEventSender.class);

    private NotificationMessagingTemplate notificationMessagingTemplate;
    private AwsCloudProperties awsCloudProperties;

    public ClosingTimeEventSender(NotificationMessagingTemplate notificationMessagingTemplate, AwsCloudProperties awsCloudProperties) {
        this.notificationMessagingTemplate = notificationMessagingTemplate;
        this.awsCloudProperties = awsCloudProperties;
    }

    @Override
    public void send(String topic, ApnmtEvent<ClosingTimeEventDTO> event) {
        log.info("Send event {} to topic {}", event, topic);
        String traceId = TracingUtil.createTraceId(awsCloudProperties.getTracing().getXRay().isEnabled());
        event.setTraceId(traceId);
        this.notificationMessagingTemplate.convertAndSend(topic, event);
    }

}
