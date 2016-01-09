package com.boozallen.cognition.ingest.storm.bolt.enrich;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.boozallen.cognition.ingest.storm.bolt.AbstractLogRecordBolt;
import com.boozallen.cognition.ingest.storm.vo.LogRecord;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Tested;
import org.apache.commons.configuration.XMLConfiguration;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.text.ParseException;
import java.util.*;

import static com.boozallen.cognition.ingest.storm.bolt.enrich.ElasticSearchJsonBolt.*;
import static com.boozallen.cognition.ingest.storm.util.ConfigurationMapEntryUtils.extractMapList;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class ElasticSearchJsonBoltTest {
    @Tested
    ElasticSearchJsonBolt bolt;
    
	private URL getResource(String resource) {
		String resourceName = String.format("/%s/%s", this.getClass().getSimpleName(), resource);
		return this.getClass().getResource(resourceName);
	}
      
    @Test
    public void testConfigure() throws Exception {
        URL resource = getResource("config.xml");
        
        XMLConfiguration conf = new XMLConfiguration(resource);
        bolt.configure(conf);

        assertNotNull(bolt.timeSeriesIndexFieldName);
        assertThat(bolt.fieldTypeMappings.size(), is(5));
    }

    @Test
    public void testConfigureBolt() throws Exception {
        URL resource = getResource("configureBolt.xml");
        XMLConfiguration conf = new XMLConfiguration(resource);
        bolt.configureBolt(conf);

        assertThat(bolt.esJsonField, is("custom_es_json_field"));
        assertThat(bolt.indexName, is("custom_index_name"));
        assertThat(bolt.indexField, is("custom_index_field"));
    }

    @Test
    public void testConfigureBoltDefault() throws Exception {
        URL resource = getResource("configureDefault.xml");
        XMLConfiguration conf = new XMLConfiguration(resource);
        bolt.configureBolt(conf);

        assertThat(bolt.esJsonField, is(ES_JSON_FIELD_DEFAULT));
        assertThat(bolt.indexName, is(INDEX_NAME_DEFAULT));
        assertThat(bolt.indexField, is(INDEX_FIELD_DEFAULT));
    }

    @Test
    public void testConfigureTimeSeriesIndex() throws Exception {
        URL resource = getResource("configureTimeSeriesIndex.xml");
        XMLConfiguration conf = new XMLConfiguration(resource);
        bolt.configureTimeSeriesIndex(conf.subset("timeSeriesIndex"));

        assertThat(bolt.timeSeriesIndexFieldName, is("postedTime"));
        assertThat(bolt.timeSeriesIndexInputDateFormat, is("yyyy-MM-dd'T'HH:mm:ss.SSSX"));
        assertThat(bolt.timeSeriesIndexOutputDateFormat, is("yyyy.MM.dd.HH"));
    }

    @Test
    public void testConfigureTimeSeriesIndexDefault() throws Exception {
        URL resource = getResource("configureDefault.xml");
        XMLConfiguration conf = new XMLConfiguration(resource);
        bolt.configureTimeSeriesIndex(conf.subset("timeSeriesIndex"));

        assertNull(bolt.timeSeriesIndexFieldName);
        assertNull(bolt.timeSeriesIndexInputDateFormat);
        assertNull(bolt.timeSeriesIndexOutputDateFormat);
    }

    @Test
    public void testConfigureFieldTypeMapping() throws Exception {
        URL resource = getResource("configureFieldTypeMapping.xml");
        XMLConfiguration conf = new XMLConfiguration(resource);
        bolt.configureFieldTypeMapping(conf.subset("fieldTypeMapping"));

        assertThat(bolt.fieldTypeMappings.size(), is(2));
        Map<String, String> geo_coordinates = bolt.fieldTypeMappings.get("geo_coordinates");
        assertThat(geo_coordinates.get(ElasticSearchJsonBolt.FIELD_NAME), is("geo_coordinates"));
        assertThat(geo_coordinates.get(ElasticSearchJsonBolt.FIELD_TYPE), is("array"));
    }

    @Test
    public void testExecute(
            @Injectable Tuple input,
            @Injectable BasicOutputCollector collector,
            @Injectable LogRecord logRecord) throws Exception {

        bolt.esJsonField = "field";

        new Expectations(bolt) {{
            input.getValueByField(AbstractLogRecordBolt.RECORD);
            result = logRecord;
            bolt.indexRecord(logRecord);
            logRecord.getValue(bolt.esJsonField);
            result = "value";

            collector.emit(new Values(logRecord));
            collector.emit(ElasticSearchJsonBolt.ES_JSON, new Values("value"));
        }};
        bolt.execute(input, collector);
    }

    @Test
    public void testGetFieldTypeMapping() throws Exception {
        URL resource = getResource("configureFieldTypeMapping.xml");
        XMLConfiguration conf = new XMLConfiguration(resource);
        Map<String, Map<String, String>> fieldTypeMappings = extractMapList(conf.subset("fieldTypeMapping"), "entry", FIELD_NAME, FIELD_TYPE, DATE_FORMAT);

        assertThat(fieldTypeMappings.size(), is(2));
        Map<String, String> mapping = bolt.getFieldTypeMapping(fieldTypeMappings, "geo_coordinates");
        assertThat(mapping.get(ElasticSearchJsonBolt.FIELD_TYPE), is("array"));
    }

    @Test
    public void testGetFieldTypeMappingEmpty() throws Exception {
        URL resource = getResource("configureFieldTypeMapping.xml");
        XMLConfiguration conf = new XMLConfiguration(resource);
        Map<String, Map<String, String>> fieldTypeMappings = extractMapList(conf.subset("fieldTypeMapping"), "entry", FIELD_NAME, FIELD_TYPE, DATE_FORMAT);

        assertThat(fieldTypeMappings.size(), is(2));
        Map<String, String> mapping = bolt.getFieldTypeMapping(fieldTypeMappings, "field_that_does_not_exist");
        assertThat(mapping.isEmpty(), is(true));
    }

    @Test
    public void testAddFieldByTypeArrayString(@Injectable XContentBuilder source,
                                              @Injectable String key,
                                              @Injectable Map<String, String> fieldTypeMapping)
            throws IOException, ParseException {

        new Expectations() {{
            fieldTypeMapping.get(ElasticSearchJsonBolt.FIELD_TYPE);
            result = "array";
            source.field(key, Arrays.asList("a", "b"));
        }};

        bolt.addFieldByType(source, key, "[\"a\",\"b\"]", fieldTypeMapping);
    }

    @Test
    public void testAddFieldByTypeArrayNumber(@Injectable XContentBuilder source,
                                              @Injectable String key,
                                              @Injectable Map<String, String> fieldTypeMapping)
            throws IOException, ParseException {

        new Expectations() {{
            fieldTypeMapping.get(ElasticSearchJsonBolt.FIELD_TYPE);
            result = "array";
            source.field(key, Arrays.asList(54.999444D, -1.544167D));
        }};

        bolt.addFieldByType(source, key, "[54.999444,-1.544167]", fieldTypeMapping);
    }

    @Test
    public void testAddFieldByTypeDate(@Injectable XContentBuilder source,
                                       @Injectable String key,
                                       @Injectable Map<String, String> fieldTypeMapping)
            throws IOException, ParseException {

        Calendar instance = Calendar.getInstance();
        instance.clear();
        instance.setTimeZone(TimeZone.getTimeZone("UTC"));
        instance.set(2015, Calendar.MARCH, 14, 9, 26, 53);
        Date date = instance.getTime();
        new Expectations() {{
            fieldTypeMapping.get(ElasticSearchJsonBolt.FIELD_TYPE);
            result = "date";
            fieldTypeMapping.get(ElasticSearchJsonBolt.DATE_FORMAT);
            result = "yyyy-MM-dd'T'HH:mm:ss.SSSX";
            source.field(key, date);
        }};

        bolt.addFieldByType(source, key, "2015-03-14T09:26:53.000Z", fieldTypeMapping);
    }

    @Test
    public void testAddFieldByTypeMicroseconds(@Injectable XContentBuilder source,
                                               @Injectable String key,
                                               @Injectable Map<String, String> fieldTypeMapping)
            throws IOException, ParseException {

        new Expectations() {{
            fieldTypeMapping.get(ElasticSearchJsonBolt.FIELD_TYPE);
            result = "microseconds";
            source.field(key, new Date(1420082040000L));
        }};

        bolt.addFieldByType(source, key, "1420082040", fieldTypeMapping);
    }

    @Test
    public void testAddFieldByTypeNoMapping(@Injectable XContentBuilder source,
                                            @Injectable String key,
                                            @Injectable String value)
            throws IOException, ParseException {

        new Expectations() {{
            source.field(key, value);
        }};

        bolt.addFieldByType(source, key, value, Collections.EMPTY_MAP);
    }
}
