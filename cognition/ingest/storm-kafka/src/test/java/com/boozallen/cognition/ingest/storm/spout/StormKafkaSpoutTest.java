package com.boozallen.cognition.ingest.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import com.google.common.util.concurrent.RateLimiter;
import mockit.*;
import org.apache.commons.configuration.Configuration;
import org.hamcrest.core.Is;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

import java.util.Map;

public class StormKafkaSpoutTest {
    @Tested
    StormKafkaSpout spout;
    @Injectable
    KafkaSpout kafkaSpout;
    @Injectable
    RateLimiter rateLimiter;

    @Test
    public void testIsRateLimited() throws Exception {
        spout.permitsPerSecond = spout.DEFAULT_PERMITS_PER_SECOND;
        Assert.assertThat(spout.isRateLimited(), Is.is(false));
        spout.permitsPerSecond = 1D;
        Assert.assertThat(spout.isRateLimited(), Is.is(true));
    }

    @Test
    public void testOpen(
            final @Injectable SpoutConfig spoutConfig,
            final @Injectable Map conf,
            final @Injectable TopologyContext context,
            final @Injectable SpoutOutputCollector collector,
            final @Injectable KafkaSpout kafkaSpout) throws Exception {

        spout.rateLimiter = null;
        spout.kafkaSpout = kafkaSpout;

        new Expectations(spout) {{
            spout.setupKafkaSpout();
        }};

        spout.permitsPerSecond = spout.DEFAULT_PERMITS_PER_SECOND;
        spout.open(conf, context, collector);
        Assert.assertNull(spout.rateLimiter);

        spout.permitsPerSecond = 1D;
        spout.open(conf, context, collector);
        Assert.assertNotNull(spout.rateLimiter);
    }

    @Test
    public void testNextTupleWithRateLimiter() throws Exception {
        spout.rateLimiter = rateLimiter;
        spout.kafkaSpout = kafkaSpout;

        new StrictExpectations() {{
            rateLimiter.acquire();
            kafkaSpout.nextTuple();
        }};

        spout.permitsPerSecond = 1D;
        spout.nextTuple();
    }

    @Test
    public void testNextTupleWithoutRateLimiter() throws Exception {
        spout.rateLimiter = rateLimiter;
        spout.kafkaSpout = kafkaSpout;

        new StrictExpectations() {{
            kafkaSpout.nextTuple();
        }};

        spout.permitsPerSecond = spout.DEFAULT_PERMITS_PER_SECOND;
        spout.nextTuple();

        new Verifications() {{
            rateLimiter.acquire();
            times = 0;
        }};
    }
}
