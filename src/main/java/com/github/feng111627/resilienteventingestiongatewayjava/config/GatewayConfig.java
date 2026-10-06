package com.github.feng111627.resilienteventingestiongatewayjava.config;

import com.github.feng111627.resilienteventingestiongatewayjava.processing.RetryPolicy;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * 网关装配：有界执行器、重试策略等。
 */
@Configuration
@EnableConfigurationProperties({RetryPolicy.class, GatewayProperties.class})
public class GatewayConfig {
}
