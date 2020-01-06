package io.tpd.kafkaexample;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;
  
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import com.github.zavierjack1.geoobject.Coordinate;

@RestController
public class CoordinatesController {

    private static final Logger logger = LoggerFactory.getLogger(CoordinatesController.class);

    private final KafkaTemplate<String, Object> template;
    private final String topicName;
    private final int messagesPerRequest;
    private List<Coordinate> coordinates = new ArrayList<Coordinate>();

    public CoordinatesController(
            final KafkaTemplate<String, Object> template,
            @Value("${tpd.topic-name-coordinates}") final String topicName,
            @Value("${tpd.messages-per-request}") final int messagesPerRequest) {
        this.template = template;
        this.topicName = topicName;
        this.messagesPerRequest = messagesPerRequest;
    }
    
    @GetMapping("/Gen/Coordinates")
    public String genCoordinates() throws Exception {
        IntStream.range(0, messagesPerRequest)
                .forEach(i -> this.template.send(topicName, String.valueOf(i), 
                        new Coordinate(new Random().nextDouble()*10000, new Random().nextDouble()*10000)
                	)
                );
        logger.info("All messages received");
        return "generated Coordinates";
    }
    
    @GetMapping("/Coordinates")
    public String getKafkaMessages() throws Exception {
        return coordinates.toString();
    }
 
    @KafkaListener(topics = "coordinates", clientIdPrefix = "json",
            containerFactory = "kafkaListenerContainerFactory")
    public void listenAsObject(ConsumerRecord<String, Coordinate> cr,
                               @Payload Coordinate payload) {
        logger.info("Logger 1 [JSON] received key {}: Type [{}] | Payload: {} | Record: {}", cr.key(),
                typeIdHeader(cr.headers()), payload, cr.toString());
        
        try{
        	coordinates.add(cr.value());
        }
        catch(Exception e) {
        	e.printStackTrace();
        }
    }

    private static String typeIdHeader(Headers headers) {
        return StreamSupport.stream(headers.spliterator(), false)
                .filter(header -> header.key().equals("__TypeId__"))
                .findFirst().map(header -> new String(header.value())).orElse("N/A");
    }
}
