package com.sdugar.lucene;


import org.apache.lucene.queryparser.classic.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URISyntaxException;

import static com.sdugar.lucene.LuceneMain.createIndex;
import static com.sdugar.lucene.LuceneMain.searchIndexWithPhraseQuery;
import static com.sdugar.lucene.LuceneMain.searchIndexWithQueryParser;

/**
 * Unit test for simple LuceneMain.
 */
public class LuceneMainTestBase {

    public static final Logger LOG = LoggerFactory.getLogger( LuceneMainTestBase.class );

    @BeforeClass
    public void setup() {
        LOG.info("setup completed");
    }

    @Test
    public void basicTest() {
        Assert.assertTrue( true );
        LOG.info("Basic test cleared");
    }

    @Test
    public void queriesTest() throws IOException, ParseException, URISyntaxException {
        createIndex();

        searchIndexWithPhraseQuery("french", "fries", 0);
        searchIndexWithPhraseQuery("hamburger", "steak", 0);
        searchIndexWithPhraseQuery("hamburger", "steak", 1);
        searchIndexWithPhraseQuery("hamburger", "steak", 2);
        searchIndexWithPhraseQuery("hamburger", "steak", 3);

        searchIndexWithQueryParser("french fries"); // BooleanQuery
        searchIndexWithQueryParser("\"french fries\""); // PhaseQuery
        searchIndexWithQueryParser("\"hamburger steak\"~1"); // PhaseQuery
        searchIndexWithQueryParser("\"hamburger steak\"~2"); // PhaseQuery
    }

}
