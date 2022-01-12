package de.apnmt.organization.messaging.consumer;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import de.apnmt.aws.common.test.AbstractEventConsumerIT;
import de.apnmt.common.ApnmtTestUtil;
import de.apnmt.common.TopicConstants;
import de.apnmt.common.event.ApnmtEvent;
import de.apnmt.common.event.value.OrganizationActivationEventDTO;
import de.apnmt.organization.common.domain.Organization;
import de.apnmt.organization.common.repository.OrganizationRepository;
import de.apnmt.organization.messaging.QueueConstants;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class OrganizationActivationSqsEventConsumerIT extends AbstractEventConsumerIT {

    @Autowired
    private OrganizationRepository organizationRepository;

    @BeforeAll
    static void beforeAll() throws IOException, InterruptedException {
        AbstractEventConsumerIT.beforeAll(TopicConstants.ORGANIZATION_ACTIVATION_CHANGED_TOPIC, QueueConstants.ORGANIZATION_ACTIVATION_QUEUE);
    }

    @BeforeEach
    public void initTest() {
        this.organizationRepository.deleteAll();
    }

    @Test
    void organizationActivationTest() throws InterruptedException {
        Organization organization = new Organization().name("Test").mail("test@test.de").phone("12345678").ownerId(1L).active(false);
        this.organizationRepository.saveAndFlush(organization);

        int databaseSizeBeforeCreate = this.organizationRepository.findAll().size();
        ApnmtEvent<OrganizationActivationEventDTO> event = ApnmtTestUtil.createOrganizationActivationEvent(organization.getId(), true);

        this.notificationMessagingTemplate.convertAndSend(TopicConstants.ORGANIZATION_ACTIVATION_CHANGED_TOPIC, event);

        Thread.sleep(1000);

        List<Organization> organizations = this.organizationRepository.findAll();
        assertThat(organizations).hasSize(databaseSizeBeforeCreate);
        Optional<Organization> maybe = this.organizationRepository.findById(organization.getId());
        assertThat(maybe).isPresent();
        assertThat(maybe.get().getActive()).isTrue();
    }

    @Test
    void organizationDeactivationTest() throws InterruptedException {
        Organization organization = new Organization().name("Test").mail("test@test.de").phone("12345678").ownerId(1L).active(true);
        this.organizationRepository.saveAndFlush(organization);

        int databaseSizeBeforeCreate = this.organizationRepository.findAll().size();
        ApnmtEvent<OrganizationActivationEventDTO> event = ApnmtTestUtil.createOrganizationActivationEvent(organization.getId(), false);

        this.notificationMessagingTemplate.convertAndSend(TopicConstants.ORGANIZATION_ACTIVATION_CHANGED_TOPIC, event);

        Thread.sleep(1000);

        List<Organization> organizations = this.organizationRepository.findAll();
        assertThat(organizations).hasSize(databaseSizeBeforeCreate);
        Optional<Organization> maybe = this.organizationRepository.findById(organization.getId());
        assertThat(maybe).isPresent();
        assertThat(maybe.get().getActive()).isFalse();
    }

}
