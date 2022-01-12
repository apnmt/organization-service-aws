package de.apnmt.organization.messaging.sender;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;

import com.amazonaws.services.sqs.model.Message;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import de.apnmt.aws.common.test.AbstractEventSenderIT;
import de.apnmt.aws.common.test.SqsMessage;
import de.apnmt.common.ApnmtTestUtil;
import de.apnmt.common.TopicConstants;
import de.apnmt.common.event.ApnmtEvent;
import de.apnmt.common.event.ApnmtEventType;
import de.apnmt.common.event.value.OpeningHourEventDTO;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

@SpringBootTest
class OpeningHourEventSenderIT extends AbstractEventSenderIT {

    @Autowired
    private OpeningHourEventSender openingHourEventSender;

    @Test
    void eventSenderTest() throws InterruptedException, JsonProcessingException {
        ApnmtEvent<OpeningHourEventDTO> event = ApnmtTestUtil.createOpeningHourEvent(ApnmtEventType.openingHourCreated);
        this.openingHourEventSender.send(getTopic(), event);

        Thread.sleep(3000);
        await().pollInterval(Duration.ofMillis(500)).atMost(Duration.ofMillis(10000)).until(assertEvent(event));
    }

    private Callable<Boolean> assertEvent(ApnmtEvent<OpeningHourEventDTO> event) throws JsonProcessingException {
        List<Message> events = sqsAsync.receiveMessage("/000000000000/" + getQueue()).getMessages();
        assertThat(events).hasSize(1);
        assertThat(events.get(0)).isNotNull();
        SqsMessage sqsMessage = this.objectMapper.readValue(events.get(0).getBody(), SqsMessage.class);
        TypeReference<ApnmtEvent<OpeningHourEventDTO>> eventType = new TypeReference<>() {
        };
        ApnmtEvent<OpeningHourEventDTO> eventResult = this.objectMapper.readValue(sqsMessage.getMessage(), eventType);
        assertThat(eventResult).isEqualTo(event);
        return () -> true;
    }

    @Override
    protected String getTopic() {
        return TopicConstants.OPENING_HOUR_CHANGED_TOPIC;
    }

    @Override
    protected String getQueue() {
        return "opening-hour-queue";
    }

}
