package io.tpd.kafkaexample;
 
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.StringUtils;

import com.google.gson.JsonObject; 
import com.google.gson.JsonParser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

@SpringBootApplication
public class KafkaExampleApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaExampleApplication.class, args);
    }

    @Autowired
    private KafkaProperties kafkaProperties;
    
    @Value("${tpd.topic-name-coordinates}")
    private String coordinatesTopicName;
    
    @Value("${spring.kafka.zookeeper}")
    private String zookeeperAddress;
   
    private List<String> kafkaBrokerAddresses;
    
    @PostConstruct
    public void init() {
    	kafkaBrokerAddresses = getKafkaBrokerAddresses(zookeeperAddress);
    }
     
    @Bean
    public KafkaAdmin admin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, StringUtils.arrayToCommaDelimitedString(kafkaBrokerAddresses.toArray()));
        return new KafkaAdmin(configs);
    }

    @Bean
    public Map<String, Object> producerConfigs() {
        Map<String, Object> props =
                new HashMap<>(kafkaProperties.buildProducerProperties());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                JsonSerializer.class);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, StringUtils.arrayToCommaDelimitedString(kafkaBrokerAddresses.toArray()));
        
        return props;
    }

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
    
    @Bean
    public NewTopic CoordinatesTopic() { 
        return new NewTopic(coordinatesTopicName, 3, (short) 1);
    }
    
    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        final JsonDeserializer<Object> jsonDeserializer = new JsonDeserializer<>();
        jsonDeserializer.addTrustedPackages("*");
        Map<String, Object> props = new HashMap<>(kafkaProperties.buildConsumerProperties());
    	props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, StringUtils.arrayToCommaDelimitedString(kafkaBrokerAddresses.toArray()));
        
        return new DefaultKafkaConsumerFactory<>(
        		props, new StringDeserializer(), jsonDeserializer
        );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());

        return factory;
    }

    private List<String> getKafkaBrokerAddresses(String zookeeperAddress) {
    	ArrayList<String> list = new ArrayList<String>();
    	try {
		    ZooKeeper zk = new ZooKeeper(zookeeperAddress, 10000, null);
		    List<String> ids = zk.getChildren("/brokers/ids", false);
		   
		    for (String id : ids) {
		        String brokerInfo = new String(zk.getData("/brokers/ids/" + id, false, null));
		        JsonParser parser = new JsonParser();
		        JsonObject json = (JsonObject) parser.parse(brokerInfo);
		        list.add(json.get("endpoints").getAsString().replace("PLAINTEXT:", ""));
		    }
		    zk.close();
	    }
	    catch(Exception e) {
	    	e.printStackTrace();
	    }
    	return list; 
    }
}
