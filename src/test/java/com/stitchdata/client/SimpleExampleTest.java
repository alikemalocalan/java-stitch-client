package com.stitchdata.client;

import com.cognitect.transit.Reader;
import com.cognitect.transit.TransitFactory;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class SimpleExampleTest {

    private static Map<String, Object> makePerson(int id, String name) {
        Map<String, Object> result = new HashMap<>();
        result.put("id", id);
        result.put("name", name);
        return result;
    }

    private static class DummyStitchClient extends StitchClient {
        final List<Map<String, Object>> receivedPeople = new ArrayList<Map<String, Object>>();

        DummyStitchClient() {
            super("", 1, "token", "namespace", "people", Arrays.asList("id"),
                    StitchClientBuilder.DEFAULT_BATCH_SIZE_BYTES,
                    StitchClientBuilder.DEFAULT_BATCH_DELAY_MILLIS,
                    null,
                    null);
        }

        @Override
        StitchResponse sendToStitch(String body) throws IOException {
            ByteArrayInputStream bais = new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8));
            Reader reader = TransitFactory.reader(TransitFactory.Format.JSON, bais);
            List records = (List) reader.read();

            for (Object record : records) {
                Map row = (Map) record;
                Map<String, Object> data = (Map<String, Object>) row.get("data");
                receivedPeople.add(data);
            }

            return new StitchResponse(200, "ok", null);
        }
    }

    @Test
    public void simpleExampleFlowShouldPushAllPeople() throws IOException {
        Map<String, Object>[] people = new Map[]{
                makePerson(1, "Jerry Garcia"),
                makePerson(2, "Omar Rodgriguez Lopez"),
                makePerson(3, "Nina Simone"),
                makePerson(4, "Joni Mitchell"),
                makePerson(5, "David Bowie")
        };

        DummyStitchClient stitch = new DummyStitchClient();
        try (DummyStitchClient ignored = stitch) {
            for (Map<String, Object> person : people) {
                stitch.push(
                        StitchMessage.newUpsert()
                                .withSequence(System.currentTimeMillis())
                                .withData(person));
            }
        }

        assertEquals(people.length, stitch.receivedPeople.size());
        for (int i = 0; i < people.length; i++) {
            Number expectedId = (Number) people[i].get("id");
            Number actualId = (Number) stitch.receivedPeople.get(i).get("id");
            assertEquals(expectedId.longValue(), actualId.longValue());
            assertEquals(people[i].get("name"), stitch.receivedPeople.get(i).get("name"));
        }
    }
}