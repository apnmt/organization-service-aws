package de.apnmt.organization.messaging.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.apnmt.aws.common.config.AwsCloudProperties;
import de.apnmt.aws.common.util.TracingUtil;
import de.apnmt.common.event.ApnmtEvent;
import de.apnmt.common.event.value.OrganizationActivationEventDTO;
import de.apnmt.organization.common.async.controller.OrganizationActivationEventConsumer;
import de.apnmt.organization.common.service.OrganizationService;
import de.apnmt.organization.messaging.QueueConstants;
import io.awspring.cloud.messaging.config.annotation.NotificationMessage;
import io.awspring.cloud.messaging.listener.SqsMessageDeletionPolicy;
import io.awspring.cloud.messaging.listener.annotation.SqsListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;

@Controller
public class OrganizationActivationSqsEventConsumer extends OrganizationActivationEventConsumer {

    private static final TypeReference<ApnmtEvent<OrganizationActivationEventDTO>> EVENT_TYPE = new TypeReference<>() {
    };

    private final Logger log = LoggerFactory.getLogger(OrganizationActivationSqsEventConsumer.class);

    @Value("${spring.application.name}")
    public String appName;

    private final ObjectMapper objectMapper;
    private final AwsCloudProperties awsCloudProperties;

    public OrganizationActivationSqsEventConsumer(OrganizationService organizationService, ObjectMapper objectMapper, AwsCloudProperties awsCloudProperties) {
        super(organizationService);
        this.objectMapper = objectMapper;
        this.awsCloudProperties = awsCloudProperties;
    }

    @SqsListener(value = QueueConstants.ORGANIZATION_ACTIVATION_QUEUE, deletionPolicy = SqsMessageDeletionPolicy.ON_SUCCESS)
    public void receiveEvent(@NotificationMessage String message) {
        try {
            this.log.info("Received event {} from queue {}", message, QueueConstants.ORGANIZATION_ACTIVATION_QUEUE);
            ApnmtEvent<OrganizationActivationEventDTO> event = this.objectMapper.readValue(message, EVENT_TYPE);
            TracingUtil.beginTracing(appName, event.getTraceId(), awsCloudProperties.getTracing().getXRay().isEnabled());
            super.receiveEvent(event);
        } catch (JsonProcessingException e) {
            this.log.error("Malformed message {} for queue {}. Event will be ignored.", message, QueueConstants.ORGANIZATION_ACTIVATION_QUEUE);
            TracingUtil.addException(e, awsCloudProperties.getTracing().getXRay().isEnabled());
        } finally {
            TracingUtil.endTracing(awsCloudProperties.getTracing().getXRay().isEnabled());
        }
    }

}
