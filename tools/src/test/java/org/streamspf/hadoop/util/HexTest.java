package org.streamspf.hadoop.util;

import org.streamspf.hadoop.util.Hex;
import org.streamspf.hadoop.hbase.exceptions.DecoderException;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class HexTest {
    @Test
    public void testAll() throws DecoderException {
        assertEquals("something", Bytes.toString(Hex.fromHex(Hex.toHex("something".getBytes()))));
    }
}
