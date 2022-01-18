package de.apnmt.organization.messaging.sender;

import de.apnmt.common.event.ApnmtEvent;
import de.apnmt.common.event.value.OpeningHourEventDTO;
import de.apnmt.common.sender.ApnmtEventSender;
import io.awspring.cloud.messaging.core.NotificationMessagingTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

@Repository
public class OpeningHourEventSender implements ApnmtEventSender<OpeningHourEventDTO> {

    private final Logger log = LoggerFactory.getLogger(OpeningHourEventSender.class);

    private NotificationMessagingTemplate notificationMessagingTemplate;

    public OpeningHourEventSender(NotificationMessagingTemplate notificationMessagingTemplate) {
        this.notificationMessagingTemplate = notificationMessagingTemplate;
    }

    @Override
    public void send(String topic, ApnmtEvent<OpeningHourEventDTO> event) {
        this.log.info("Send event {} to SNS topic {}", event, topic);
        this.notificationMessagingTemplate.convertAndSend(topic, event);
    }

}
