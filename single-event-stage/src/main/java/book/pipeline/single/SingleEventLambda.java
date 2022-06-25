package book.pipeline.single;

import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import book.pipeline.common.WeatherEvent;

public class SingleEventLambda {
    private final ObjectMapper objectMapper = 
        new ObjectMapper()
            .configure(
                DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    
    public void handler(SNSEvent event) {
        event.getRecords().forEach(this::processSNSRecord);
    }

    private void processSNSRecord(SNSEvent.SNSRecord snsRecord) {
        try {
            final WeatherEvent weatherEvent = 
                objectMapper.readValue(snsRecord.getSNS().getMessage(), WeatherEvent.class);

                System.out.println("Received weather event:");
                System.out.println(weatherEvent);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}