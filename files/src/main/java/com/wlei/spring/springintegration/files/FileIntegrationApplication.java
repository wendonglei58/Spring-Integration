package com.wlei.spring.springintegration.files;

import com.rabbitmq.client.AMQP;
import org.apache.commons.net.ftp.FTPFile;
import org.springframework.amqp.core.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ImageBanner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.io.FileSystemResource;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.integration.file.FileHeaders;
import org.springframework.integration.file.FileNameGenerator;
import org.springframework.integration.file.dsl.Files;
import org.springframework.integration.file.remote.session.SessionFactory;
import org.springframework.integration.ftp.dsl.Ftp;
import org.springframework.integration.ftp.session.DefaultFtpSessionFactory;
import org.springframework.integration.ftp.session.DefaultFtpsSessionFactory;
import org.springframework.integration.transformer.GenericTransformer;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.ReflectionUtils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;

/**
 * {@link com.wlei.spring.springintegration.files
 *
 * @author wendo
 * @version 1.0
 * @since 7/5/2021
 **/
@SpringBootApplication
public class FileIntegrationApplication {
    public static void main(String[] args) {
        SpringApplication.run(FileIntegrationApplication.class);
    }
    @Configuration
    public static class FtpConfig {
        @Bean
        SessionFactory<FTPFile> ftpFileSessionFactory(@Value("${ftp.host}") String host, @Value("${ftp.user}") String user,
                                                      @Value("${ftp.port}") int port, @Value("${ftp.pass}") String pw) {
            DefaultFtpSessionFactory sessionFactory = new DefaultFtpSessionFactory();
            sessionFactory.setHost(host);
            sessionFactory.setPort(port);
            sessionFactory.setUsername(user);
            sessionFactory.setPassword(pw);
            return sessionFactory;
        }
    }
    @Configuration
    public static class AmqpConfig {
        @Bean
        Exchange exchange() {
            return ExchangeBuilder.directExchange("ascii").durable(true).build();
        }
        @Bean
        Queue queue() {
            return QueueBuilder.durable("ascii").build();
        }
        @Bean
        Binding binding() {
            return BindingBuilder
                    .bind(this.queue())
                    .to(this.exchange())
                    .with("ascii").noargs();
        }

    }

    @Bean
    IntegrationFlow files(@Value("${input-directory:${user.home}/Documents/in}") File in,
                          Environment env) {
        GenericTransformer<File, Message<String>> fileStringGenericTransformer = (File file) -> {
            try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                 PrintStream printStream = new PrintStream(bos)) {
                ImageBanner imageBanner = new ImageBanner(new FileSystemResource(file));
                imageBanner.printBanner(env, getClass(), printStream);
                return MessageBuilder
                        .withPayload(new String(bos.toByteArray()))
                        .setHeader(FileHeaders.FILENAME, file.getAbsoluteFile().getName())
                        .build();
            }catch (IOException ex) {
                ReflectionUtils.rethrowRuntimeException(ex);
            }
            return null;
        };
//        return IntegrationFlows.from(Files.inboundAdapter(in)
//                .autoCreateDirectory(true)
//                .preventDuplicates(true).patternFilter("*.jpg"),
//                poller -> poller.poller(pm -> pm.fixedRate(1000)))
//                .transform(File.class, fileStringGenericTransformer)
//                .handle(Ftp.outboundAdapter(ftpSessionFactory).fileNameGenerator(message -> {
//                    String fileName = (String) message.getHeaders().get(FileHeaders.FILENAME);
//                    return fileName.split("\\.")[0] + ".txt";
//                }).remoteDirectory("/upload"))
//                .get();
        return IntegrationFlows.from(Files.inboundAdapter(in)
                .autoCreateDirectory(true)
                .preventDuplicates(true)
                .patternFilter("*.jpg"), poller -> poller.poller(pm -> pm.fixedRate(1000)))
                .transform(fileStringGenericTransformer)
                .channel(asciiProcessor()).get();

    }
    @Bean
    IntegrationFlow ftp(SessionFactory<FTPFile> ftpSessionFactory) {
        return IntegrationFlows.from(asciiProcessor())
                .handle(Ftp.outboundAdapter(ftpSessionFactory)
                        .fileNameGenerator(message -> message.getHeaders().get(FileHeaders.FILENAME).toString().split("\\.")[0] + ".txt")
                        .remoteDirectory("/upload"))
                .get();
    }
    @Bean
    IntegrationFlow amqp(AmqpTemplate amqpTemplate) {
        return IntegrationFlows.from(this.asciiProcessor())
                .handle(Amqp.outboundAdapter(amqpTemplate).exchangeName("ascii").routingKey("ascii"))
                .get();
    }
    @Bean
    MessageChannel asciiProcessor() {
        return MessageChannels.p().get();
    }
}
