import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*

class ConsumerDemoCooperative

private val logger = LoggerFactory.getLogger(ConsumerDemoCooperative::class.java.simpleName)

fun main() {
    val properties = Properties()
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-group")
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor::class.java.name)

    val consumer = KafkaConsumer<String, String>(properties)
    consumer.subscribe(listOf("demo_java"))
    consumer.setShutdownHook()

    try {
        while (true) {
            logger.info("Polling... after 1000ms")
            consumer.poll(Duration.ofMillis(1000)).forEach {
                logger.info(
                    StringBuffer().appendLine("Key: ${it.key()}")
                        .appendLine("Value: ${it.value()}")
                        .appendLine("Partition: ${it.partition()}")
                        .appendLine("Offset: ${it.offset()}")
                        .toString()
                )
            }
        }
    } catch (e: WakeupException) {
        logger.info("Detected a wakeup signal")
    } catch (e: Exception) {
        logger.error("Detected an unexpected exception", e)
    } finally {
        logger.info("Closing consumer")
        consumer.close()
        logger.info("Consumer closed")
    }
}

private fun KafkaConsumer<String, String>.setShutdownHook() {
    val mainThread = Thread.currentThread()
    Runtime.getRuntime().addShutdownHook(Thread {
        wakeup()
        try {
            mainThread.join()
        } catch (e: Exception) {
            logger.error("Join to main thread got some error", e)
        }
    })
}