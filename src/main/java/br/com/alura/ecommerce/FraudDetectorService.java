package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class FraudDetectorService {
    public static void main(String[] args) {
        // Cria um objeto de consumer
        var consumer = new KafkaConsumer<String, String>(properties());

        // Associação do Consumer com o Topico
        consumer.subscribe(Collections.singleton("ECOMMERCE_NEW_ORDER"));

        while(true) {
            var records = consumer.poll(Duration.ofMillis(100));

            if (!records.isEmpty()) {
                System.out.println("Foram enocntrados " + records.count() + " registros!");

                for (var record : records) {
                    System.out.println("____________________________________________________");
                    System.out.println("Processando uma nova ordem, checando por fraudes...");
                    System.out.println(record.key());
                    System.out.println(record.value());
                    System.out.println(record.partition());
                    System.out.println(record.offset());

                    // Simular fraude
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                System.out.println("Ordem processada!");
            }
        }

    }

    private static Properties properties(){
        // Cria um objeto de propriedades
        var properties = new Properties();

        // Seta suas propriedades no objeto criado
        //LOCAL DO SERVER
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        //DESERIALIZADOR STRING/BYTES
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, FraudDetectorService.class.getName());

        //Retorna as propriedades
        return properties;
    }
}
