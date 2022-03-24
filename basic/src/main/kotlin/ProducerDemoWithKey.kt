import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.time.Instant
import java.time.ZoneOffset
import java.util.*

class ProducerDemoWithKey

private val logger = LoggerFactory.getLogger(ProducerDemoWithKey::class.java.simpleName)

fun main() {
    val properties = Properties()
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)

    (1..10).forEach {
        val producer = KafkaProducer<String, String>(properties)
        val record = ProducerRecord<String, String>("demo_java", "key $it", "hello world $it")

        producer.send(record) { metadata, exception ->
            if (exception == null) {
                logger.info(
                    StringBuffer().appendLine("Topic: ${metadata.topic()}")
                        .appendLine("Partition: ${metadata.partition()}")
                        .appendLine("Offset: ${metadata.offset()}")
                        .appendLine("Timestamp: ${Instant.ofEpochMilli(metadata.timestamp()).atOffset(ZoneOffset.ofHours(8))}").toString()
                )
            } else {
                logger.error("Error while publishing")
            }
        }
        producer.flush()
        producer.close()
    }
}