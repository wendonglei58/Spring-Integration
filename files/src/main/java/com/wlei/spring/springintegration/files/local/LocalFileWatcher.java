package com.wlei.spring.springintegration.files.local;

import lombok.extern.log4j.Log4j2;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.*;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlowBuilder;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.file.FileReadingMessageSource;
import org.springframework.integration.file.dsl.Files;
import org.springframework.integration.file.remote.aop.RotatingServerAdvice;
import org.springframework.integration.file.remote.aop.RotationPolicy;
import org.springframework.integration.file.transformer.FileToStringTransformer;
import org.springframework.integration.handler.GenericHandler;
import org.springframework.integration.handler.LoggingHandler;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * TODO
 *
 * @author Wendong Lei
 * @version 1.0
 * @since 9/1/2021
 **/
@Configuration
@Log4j2
public class LocalFileWatcher {
    @Bean
    public MessageChannel fileInputChannel() {
        return new DirectChannel();
    }

    @Bean
    public MessageChannel processFileChannel() {
        return new DirectChannel();
    }

    @Bean
    public IntegrationFlow fileWatch() {
        return IntegrationFlows.from(Files.inboundAdapter(new File("D:/dev/one"))
                .autoCreateDirectory(true).preventDuplicates(true), pollSpec -> pollSpec.poller(p -> p.fixedRate(3000).advice(myAdvice())))
                .transform(fileToStringTransformer())
                .handle(loggingHandler()).get();
    }

    @Bean
    public RotatingServerAdvice myAdvice() {
        List<RotationPolicy.KeyDirectory> keyDirectories = new ArrayList<>();
        keyDirectories.add(new RotationPolicy.KeyDirectory("test1", "D:/dev/one"));
        keyDirectories.add(new RotationPolicy.KeyDirectory("test2", "D:/dev/two"));
        return new RotatingServerAdvice(new DirectoryRotationPolicy(keyDirectories, false));
    }

    @Bean
    @Transformer(inputChannel = "fileInputChannel", outputChannel = "processFileChannel")
    public FileToStringTransformer fileToStringTransformer() {
        return new FileToStringTransformer();
    }

    @Bean
    @ServiceActivator(inputChannel = "processFileChannel")
    public MessageHandler loggingHandler() {
        return new LoggingHandler("Info");
    }
}
