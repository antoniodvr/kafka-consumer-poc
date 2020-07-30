package com.github.antoniodvr.kafkaconsumerpoc;

import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class App {

    private static final Logger logger = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        CommandLineParser commandLineParser = CommandLineParser.parse(args);
        int consumersNumber = commandLineParser.consumersNumber;
        List<String> topics = commandLineParser.topics;
        Properties kafkaProperties = commandLineParser.kafkaProperties;

        logger.info("Starting {} consumers [topics={}]", consumersNumber, topics);
        ExecutorService executor = Executors.newFixedThreadPool(consumersNumber);

        final List<ConsumerRunnable> consumers = new ArrayList<>();
        for (int i = 0; i < consumersNumber; i++) {
            UUID consumerId = UUID.randomUUID();
            ConsumerRunnable consumer = new ConsumerRunnable(kafkaProperties, consumerId, topics);
            consumers.add(consumer);
            executor.submit(consumer);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("\n\nShutting down {} consumers", consumersNumber);
            for (ConsumerRunnable consumer : consumers) {
                consumer.shutdown();
            }
            executor.shutdown();
            try {
                executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                logger.error("Error during await termination", e);
            }
        }));

    }

    static class CommandLineParser {

        private static final Option CONSUMERS_NUMBER_OPTION = Option.builder("c").longOpt("consumers-num")
                .argName("")
                .desc("Number of consumers")
                .hasArg()
                .type(Number.class)
                .required()
                .build();

        private static final Option TOPICS_OPTION = Option.builder("t").longOpt("topics")
                .argName("")
                .desc("List of topics")
                .numberOfArgs(Option.UNLIMITED_VALUES)
                .required()
                .build();

        private static final Option KAFKA_CONFIG_OPTION = Option.builder("k").longOpt("kafka-properties")
                .argName("")
                .desc("Kafka properties location")
                .hasArg()
                .type(File.class)
                .required()
                .build();

        private final int consumersNumber;
        private final List<String> topics;
        private final Properties kafkaProperties;

        private CommandLineParser(Options options, String[] args) throws ParseException, IOException {
            Stream.of(CONSUMERS_NUMBER_OPTION, TOPICS_OPTION, KAFKA_CONFIG_OPTION).forEach(options::addOption);
            CommandLine cli = new DefaultParser().parse(options, args);
            consumersNumber = ((Number) cli.getParsedOptionValue(CONSUMERS_NUMBER_OPTION.getOpt())).intValue();
            topics = Arrays.asList(cli.getOptionValues(TOPICS_OPTION.getOpt()));
            File kafkaPropertiesFile = (File) cli.getParsedOptionValue(KAFKA_CONFIG_OPTION.getOpt());
            kafkaProperties = new Properties();
            kafkaProperties.load(new FileInputStream(kafkaPropertiesFile));
        }

        static CommandLineParser parse(String[] args) {
            final Options options = new Options();
            CommandLineParser commandLineParser = null;
            try {
                commandLineParser = new CommandLineParser(options, args);
            } catch (ParseException e) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp(App.class.getName(), options, true);
                System.exit(1);
            } catch (IOException e) {
                System.out.println("\nCannot read Kafka properties file");
                System.exit(1);
            }
            return commandLineParser;
        }

    }
}
