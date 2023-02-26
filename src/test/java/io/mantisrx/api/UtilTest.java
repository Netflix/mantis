package io.mantisrx.api;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

public class UtilTest {

    @Test
    public void testGetTagList() {
        String[] tags = Util.getTaglist("/jobconnectbyname/rx-sps-tracker?clientId=testClientId", "testTargetId", "us-east-1");
        assertArrayEquals(new String[]{
                "clientId", "testClientId",
                "SessionId", "testTargetId",
                "urlPath", "/jobconnectbyname/rx-sps-tracker",
                "region", "us-east-1"}, tags);

        tags = Util.getTaglist("/jobconnectbyname/rx-sps-tracker?clientId=testClientId&MantisApiTag=tag1:value1", "testTargetId", "us-east-1");
        assertArrayEquals(new String[]{
                "tag1", "value1",
                "clientId", "testClientId",
                "SessionId", "testTargetId",
                "urlPath", "/jobconnectbyname/rx-sps-tracker",
                "region", "us-east-1"}, tags);

        tags = Util.getTaglist("/jobconnectbyname/rx-sps-tracker?clientId=testClientId&MantisApiTag=tag1:value1&MantisApiTag=clientId:testClientId2", "testTargetId", "us-east-1");
        assertArrayEquals(new String[]{
                "tag1", "value1",
                "clientId", "testClientId2",
                "SessionId", "testTargetId",
                "urlPath", "/jobconnectbyname/rx-sps-tracker",
                "region", "us-east-1"}, tags);
    }
}
