package com.sdugar.lucene;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;

/**
 * Lucene Main...
 *
 */
public class LuceneMain
{
    private static final Logger LOG = LoggerFactory.getLogger( LuceneMain.class );
    public static Path FILES_TO_INDEX_PATH;
    public static Path INDEX_DIRECTORY_PATH;

    public static final String FIELD_PATH = "path";
    public static final String FIELD_CONTENTS = "contents";

    public static final String FILES_TO_INDEX = "/filesToIndex";

    public static final String INDEX_DIRECTORY = "/tmp/indexDirectory";

    static {
        try {
            FILES_TO_INDEX_PATH = Paths.get( LuceneMain.class.getResource( FILES_TO_INDEX ).toURI() );
            final Path  indexDirPath = Paths.get( INDEX_DIRECTORY );
            if ( !Files.isReadable( indexDirPath ) ) {
                INDEX_DIRECTORY_PATH = Files.createDirectory( indexDirPath );
            } else {
                INDEX_DIRECTORY_PATH = indexDirPath;
            }
        } catch (Exception ex) {
            LOG.error("Failed to init critical dir paths indexDir {} -- docDir {} ... exiting", INDEX_DIRECTORY, FILES_TO_INDEX);
            System.exit( 1 );
        }

        if ( !Files.isReadable(FILES_TO_INDEX_PATH) ) {
            LOG.error( "Cannot read the directory... {} ... exiting", FILES_TO_INDEX );
            System.exit( 1 );
        }
    }

    static void indexDoc(IndexWriter writer, Path file) throws IOException {
        try (InputStream stream = Files.newInputStream(file)) {
            Document doc = new Document();
            doc.add(new StringField(FIELD_PATH, file.toString(), Field.Store.YES));
            doc.add(new TextField("contents", new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))));

            if (writer.getConfig().getOpenMode() == IndexWriterConfig.OpenMode.CREATE) {
                LOG.info("adding " + file);
                writer.addDocument(doc);
            } else {
                writer.updateDocument(new Term("path", file.toString()), doc);
            }
        }
    }

    public static void createIndex() throws IOException {
        final Directory indexDir = FSDirectory.open( INDEX_DIRECTORY_PATH );
        final Analyzer analyzer = new StandardAnalyzer();
        final IndexWriterConfig iwc = new IndexWriterConfig( analyzer );
        iwc.setOpenMode( IndexWriterConfig.OpenMode.CREATE );
        try (final IndexWriter indexWriter = new IndexWriter(indexDir, iwc)) {
            if ( Files.isDirectory( FILES_TO_INDEX_PATH ) ) {
                Files.walkFileTree( FILES_TO_INDEX_PATH, new SimpleFileVisitor<Path>() {

                    @Override
                    public FileVisitResult visitFile(Path p, BasicFileAttributes attrs) {
                        try {
                            indexDoc( indexWriter, p );
                        } catch (Exception ex) {
                            // ignore
                        }
                        return FileVisitResult.CONTINUE;
                    }
                } );
            } else {
                indexDoc( indexWriter, FILES_TO_INDEX_PATH );
            }
        }

    }

    public static void searchIndexWithQueryParser(String searchString) throws IOException, ParseException {
        LOG.info( "Searching for '" + searchString + "' using QueryParser" );
        DirectoryReader directory = DirectoryReader.open( FSDirectory.open( INDEX_DIRECTORY_PATH ) );
        IndexSearcher indexSearcher = new IndexSearcher( directory );

        QueryParser queryParser = new QueryParser( FIELD_CONTENTS, new StandardAnalyzer() );
        Query query = queryParser.parse( searchString );
        TopDocs docs = indexSearcher.search( query, 100 );
        displayHits( query, docs, indexSearcher );
    }

    public static void displayQuery(Query query) {
        LOG.info("Query: " + query.toString());
    }

    public static void searchIndexWithPhraseQuery(String string1, String string2, int slop) throws IOException,
            ParseException, URISyntaxException {
        Directory directory = FSDirectory.open(INDEX_DIRECTORY_PATH);
        IndexSearcher indexSearcher = new IndexSearcher(DirectoryReader.open(directory));

        Term term1 = new Term(FIELD_CONTENTS, string1);
        Term term2 = new Term(FIELD_CONTENTS, string2);
        PhraseQuery phraseQuery = new PhraseQuery.Builder()
                .add( term1 )
                .add( term2 )
                .setSlop( slop ).build();
        TopDocs docs = indexSearcher.search(phraseQuery, 100);
        displayHits(phraseQuery, docs, indexSearcher);
    }

    public static void displayHits(Query query, TopDocs matched, IndexSearcher searcher) throws IOException {
        LOG.info("-------------------------------------------");
        LOG.info( "Type of query: " + query.getClass().getSimpleName() );
        displayQuery(query);
        LOG.info("Number of hits: " + matched.totalHits);
        LOG.info("...........................................");
        Arrays.stream( matched.scoreDocs ).forEach( doc -> {
            try {
                LOG.info("Matched Path {}", searcher.doc(doc.doc).get(FIELD_PATH));
            } catch (IOException e) {
                // ignore
            }
        } );
        LOG.info("-------------------------------------------");
    }
}
