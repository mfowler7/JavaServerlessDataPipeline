package book.pipeline.bulk;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import book.pipeline.common.WeatherEvent;

public class BulkEventsLambda {
    static String FAN_OUT_TOPIC_ENV = "FAN_OUT_TOPIC";
    private final ObjectMapper objectMapper = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final AmazonSNS sns;
    private final AmazonS3 s3;
    private final String snsTopic;

    public BulkEventsLambda() {
        this(AmazonSNSClientBuilder.defaultClient(), AmazonS3ClientBuilder.defaultClient());
    }

    public BulkEventsLambda(AmazonSNS sns, AmazonS3 s3) {
        this.sns = sns;
        this.s3 = s3;
        this.snsTopic = System.getenv(FAN_OUT_TOPIC_ENV);

        if (this.snsTopic == null) {
            throw new RuntimeException(String.format("%s must be set", FAN_OUT_TOPIC_ENV));
        }
    }

    public void handler(S3Event event) {

        List<WeatherEvent> events = event.getRecords().stream()
            .map(this::getObjectFromS3)
            .map(this::readWeatherEvents)
            .flatMap(List::stream)
            .collect(Collectors.toList());

        // Serialize and publish WeatherEvent messages to SNS.
        events.stream()
            .map(this::weatherEventToSnsMessage)
            .forEach(this::publishToSns);

        // Record in CloudWatch the number of SNS messages published.
        System.out.print("Published " + events.size() + " weather events to SNS.");
    }

    List<WeatherEvent> readWeatherEvents(InputStream inputStream) {
        try (InputStream is = inputStream) {
            return Arrays.asList(
                objectMapper.readValue(is, WeatherEvent[].class));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String weatherEventToSnsMessage(WeatherEvent weatherEvent) {
        try {
            return objectMapper.writeValueAsString(weatherEvent);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private InputStream getObjectFromS3(S3EventNotification.S3EventNotificationRecord record) {
        String bucket = record.getS3().getBucket().getName();
        String key = record.getS3().getObject().getKey();

        return s3.getObject(bucket, key).getObjectContent();
    }

    private void publishToSns(String message) {
        sns.publish(snsTopic, message);
    }
}