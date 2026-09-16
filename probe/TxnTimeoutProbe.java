// Ask the broker for a transaction timeout above its cap and record the refusal.
//
// SC106 says a pipeline can require a transaction timeout the broker will not
// grant. That rested on a configured value read from the broker plus the
// documented behaviour of exceeding it, and a paper that measures one half of
// a claim should not assert the other. The Python client in this project does
// not implement transactions, so this is the smallest Java program that does:
// initTransactions() with transaction.timeout.ms above
// transaction.max.timeout.ms, and whatever the broker says in reply.
//
// Prints one JSON object on stdout so the experiment can read it without
// parsing a stack trace.
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

public class TxnTimeoutProbe {
    static String json(String... kv) {
        StringBuilder b = new StringBuilder("{");
        for (int i = 0; i < kv.length; i += 2) {
            if (i > 0) b.append(",");
            b.append('"').append(kv[i]).append("\":\"")
             .append(kv[i + 1] == null ? "" : kv[i + 1].replace("\\", "\\\\").replace("\"", "\\\""))
             .append('"');
        }
        return b.append("}").toString();
    }

    public static void main(String[] args) {
        String bootstrap = args.length > 0 ? args[0] : "localhost:19092";
        int timeoutMs = args.length > 1 ? Integer.parseInt(args[1]) : 1_800_000;
        String txnId = args.length > 2 ? args[2] : "streamcheck-probe";

        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
              "org.apache.kafka.common.serialization.StringSerializer");
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
              "org.apache.kafka.common.serialization.StringSerializer");
        p.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, txnId);
        p.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, timeoutMs);
        p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 20_000);
        p.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(p)) {
            producer.initTransactions();
            System.out.println(json(
                "requested_ms", String.valueOf(timeoutMs),
                "refused", "false",
                "outcome", "accepted",
                "exception", "",
                "message", "the broker granted the requested transaction timeout"));
        } catch (Exception e) {
            System.out.println(json(
                "requested_ms", String.valueOf(timeoutMs),
                "refused", "true",
                "outcome", "refused",
                "exception", e.getClass().getName(),
                "message", String.valueOf(e.getMessage())));
        }
    }
}
