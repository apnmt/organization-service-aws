package de.apnmt.organization.config;

import com.amazonaws.xray.sql.TracingDataSource;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import javax.sql.DataSource;

@Configuration
@ConditionalOnProperty(
    value = "cloud.aws.tracing.xray.enabled",
    havingValue = "true")
public class TracingDatabaseConfiguration extends HikariConfig {

    @Value("${spring.datasource.hikari.auto-commit}")
    private boolean autoCommit;

    @Bean
    @Primary
    @ConfigurationProperties(prefix = "spring.datasource")
    public DataSource dataSource(DataSourceProperties dataSourceProperties) {
        HikariDataSource hikariDataSource = DataSourceBuilder
            .create(dataSourceProperties.getClassLoader())
            .type(HikariDataSource.class)
            .driverClassName(dataSourceProperties.getDriverClassName())
            .url(dataSourceProperties.getUrl())
            .username(dataSourceProperties.getUsername())
            .password(dataSourceProperties.getPassword())
            .build();
        hikariDataSource.setAutoCommit(autoCommit);
        return TracingDataSource
            .decorate(hikariDataSource);
    }

}
