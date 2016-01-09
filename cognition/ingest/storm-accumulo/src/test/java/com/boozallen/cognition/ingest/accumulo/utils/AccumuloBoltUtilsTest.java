package com.boozallen.cognition.ingest.accumulo.utils;

import com.boozallen.cognition.ingest.storm.vo.LogRecord;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.Test;

import java.util.Date;

import static com.boozallen.cognition.ingest.accumulo.utils.AccumuloBoltUtils.getEventRecordId;
import static com.boozallen.cognition.ingest.accumulo.utils.AccumuloBoltUtils.getShard;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author bentse
 */
public class AccumuloBoltUtilsTest {

    @Test
    public void testGetEventRecordId(@Injectable LogRecord record,
                                     @Mocked Date date) {
        new Expectations() {{
            record.getUUID();
            result = "test";
            date.getTime();
            result = 1000000;
        }};
        String result = getEventRecordId(record, "PREFIX_", 1);
        assertThat(result, is("PREFIX_0_1000000_test"));
    }

    @Test
    public void testGetShard() {
        assertThat(getShard(1, 18), is("1"));
        assertThat(getShard(10, 18), is("a"));
        assertThat(getShard(17, 18), is("h"));
        assertThat(getShard(18, 18), is("0"));
        assertThat(getShard(19, 18), is("1"));

        assertThat(getShard(18, 36), is("i"));
        assertThat(getShard(35, 36), is("z"));
    }
}