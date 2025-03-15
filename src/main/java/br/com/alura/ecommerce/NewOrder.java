package br.com.alura.ecommerce;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class NewOrder {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // Cria um objeto de producer, seu record e define a menssagem
        var producer = new KafkaProducer<String, String>(properties());
        var message = "123,456,789";
        var record = new ProducerRecord<String, String>("ECOMMERCE_NEW_ORDER", message, message);

        //ENVIO DE MENSAGEM
        producer.send(record, (data, ex) -> {
            if(ex != null){
                ex.printStackTrace();
                return;
            }

            System.out.println("Sucesso ao Enviar: " + data.topic()
                    + "/ Partition: " + data.partition()
                    + "/ Offset: " + data.offset()
                    + "/ Timestamp: " + data.timestamp());
        }).get();
    }

    private static Properties properties(){
        // Cria um objeto de propriedades
        var properties = new Properties();

        // Seta suas propriedades no objeto criado
        //LOCAL DO SERVER
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        //SERIALIZADOR STRING/BYTES
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Retorna as propriedades
        return properties;
    }
}