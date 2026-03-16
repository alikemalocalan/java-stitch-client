package com.stitchdata.client;

import com.cognitect.transit.Reader;
import com.cognitect.transit.TransitFactory;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class CallbackExampleTest {

    private static Map makePerson(int id, String name) {
        Map result = new HashMap();
        result.put("id", id);
        result.put("name", name);
        return result;
    }

    private static class CollectingFlushHandler implements FlushHandler {
        final List names = new ArrayList();

        public void onFlush(List arg) {
            names.addAll(arg);
        }
    }

    private static class DummyStitchClient extends StitchClient {
        final List<Map> receivedPeople = new ArrayList<Map>();

        DummyStitchClient(FlushHandler flushHandler) {
            super("", 1, "token", "namespace", "people", Arrays.asList(new String[]{"id"}),
                    StitchClientBuilder.DEFAULT_BATCH_SIZE_BYTES,
                    StitchClientBuilder.DEFAULT_BATCH_DELAY_MILLIS,
                    flushHandler,
                    null);
        }

        @Override
        StitchResponse sendToStitch(String body) throws IOException {
            ByteArrayInputStream bais = new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8));
            Reader reader = TransitFactory.reader(TransitFactory.Format.JSON, bais);
            List records = (List) reader.read();

            for (Object record : records) {
                Map data = (Map) ((Map) record).get("data");
                receivedPeople.add(data);
            }

            return new StitchResponse(200, "ok", null);
        }
    }

    @Test
    public void callbackExampleFlowShouldPushPeopleAndInvokeCallbackWithNames() throws IOException {
        Map[] people = new Map[]{
                makePerson(1, "Jerry Garcia"),
                makePerson(2, "Omar Rodgriguez Lopez"),
                makePerson(3, "Nina Simone"),
                makePerson(4, "Joni Mitchell"),
                makePerson(5, "David Bowie")
        };

        CollectingFlushHandler flushHandler = new CollectingFlushHandler();
        DummyStitchClient stitch = new DummyStitchClient(flushHandler);

        try (stitch) {
            for (Map person : people) {
                StitchMessage message = StitchMessage.newUpsert()
                        .withSequence(System.currentTimeMillis())
                        .withData(person);
                stitch.push(message, person.get("name"));
            }
        }

        assertEquals(people.length, stitch.receivedPeople.size());
        assertEquals(Arrays.asList(
                "Jerry Garcia",
                "Omar Rodgriguez Lopez",
                "Nina Simone",
                "Joni Mitchell",
                "David Bowie"), flushHandler.names);
    }
}
