package demo.consumers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;


public class ConsumerDemoWithThreads {

    public final static String BOOTSTRAP_SERVER = "127.0.0.1:9092";
    public final static String GROUP_ID = "my-sixth-application";
    public final static String OFFSET = "earliest";
    public final static String TOPIC = "first_topic";

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class);

        // latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        // create the consumer runnable
        logger.info("Creating the consumer thread");
        ConsumerThread myConsumerThread = new ConsumerThread(latch, BOOTSTRAP_SERVER, GROUP_ID, TOPIC);

        // start the thread
        Thread myThread = new Thread(myConsumerThread);
        myThread.start();

        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught shutdown hook");
            myConsumerThread.shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application exited");
        }

        ));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application interrupted ", e);
        } finally {
            logger.info("Application is closing");
        }
    }
}
