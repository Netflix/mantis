package io.mantisrx.publish.netty.proto;

import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.junit.jupiter.api.Test;


public class MantisEnvelopeTest {

    @Test
    public void deserTest() {
        String data = "{\"ts\":1571174446676,\"originServer\":\"origin\",\"eventList\":[{\"id\":1,\"data\":\"{\\\"mantisStream\\\":\\\"defaultStream\\\",\\\"matched-clients\\\":[\\\"MantisPushRequestEvents_PushRequestEventSourceJobLocal-1_nj3\\\"],\\\"id\\\":44,\\\"type\\\":\\\"EVENT\\\"}\"}]}";
        final ObjectMapper mapper = new ObjectMapper();
        ObjectReader mantisEventEnvelopeReader = mapper.readerFor(MantisEventEnvelope.class);
        try {
            MantisEventEnvelope envelope = mantisEventEnvelopeReader.readValue(data);
            System.out.println("Envelope=>" + envelope);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            fail();
        }


    }
}
