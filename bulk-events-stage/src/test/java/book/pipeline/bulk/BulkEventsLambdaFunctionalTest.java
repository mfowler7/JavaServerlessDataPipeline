package book.pipeline.bulk;

import java.io.IOException;

import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sns.AmazonSNS;
import com.fasterxml.jackson.annotation.JsonRawValue;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

public class BulkEventsLambdaFunctionalTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    //@Rule
    public EnvironmentVariables environment = new EnvironmentVariables();

    //@Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void TestHandler() throws IOException {

        // Set up mock AWS SDK clients.
        AmazonSNS mockSNS = Mockito.mock(AmazonSNS.class);
        AmazonS3 mockS3 = Mockito.mock(AmazonS3.class);

        // Fixture S3 Event
        S3Event s3Event = objectMapper.readValue(getClass().getResourceAsStream("/s3_event.json"), S3Event.class);
        String bucket = s3Event.getRecords().get(0).getS3().getBucket().getName();
        String key = s3Event.getRecords().get(0).getS3().getObject().getKey();

        // Fixture S3 return value
        S3Object s3Object = new S3Object();
        s3Object.setObjectContet(getClass().getResourceAsStream(String.format("/%s", key)));
        Mockito.when(mockS3.getObject(bucket, key)).thenReturn(s3Object);

        // Fixture environment
        String topic = "test-topic";
        environment.set(BulkEventsLambda.FAN_OUT_TOPIC_ENV, topic);

        // Construct Lambda function class, and invoke handler
        BulkEventsLambda lambda = new BulkEventsLambda(mockSns, mockS3);
        lambda.handler(s3Event);

        // Capture outbound SNS messages
        ArgumentCaptor<String> topics = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> messages = ArgumentCaptor.forClass(String.class);
        Mockito.verify(mockSns, Mockito.times(3).publish(topics.capture(), messages.capture()));

        // Assert
        Assert.assertArrayEquals(
            new String[]{topic, topic, topic},
            topics.getAllValues().toArray());

        Assert.assertArrayEquals(
            new String[]{
               "{\"locationName\":\"Brooklyn, NY\",\"temperature\":91.0,\"timestamp\":1564428897,\"longitude\":-73.99,\"latitude\":40.7}",
               "{\"locationName\":\"Oxford, UK\",\"temperature\":64.0,\"timestamp\":1564428898,\"longitdue\":-1.25,\"latitude\":51.75}",
               "{\"locaitonName\":\"Charlottesville, VA\",\"temperature\":87.0,\"timestamp\":1564428899,\"longitude\":-78.47,\"latitude\":38.02}"
            }, messages.getAllValues().toArray());
    }
}
