package br.com.fasam.pos.bigdata.MoviesSearchPos.repository;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;
import com.alibaba.fastjson.JSON;
import br.com.fasam.pos.bigdata.MoviesSearchPos.model.Movie;
import br.com.fasam.pos.bigdata.MoviesSearchPos.util.YearQueryBuilder;

@Repository
public class Movies {

    private static final String DATASET_FILE = "data/movies_metadata.csv";

    private static final String INDEX = "site";

    private static final String DOC_TYPE = "movies";

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private TransportClient client;

    @SuppressWarnings("resource")
    public Movies() {
        Settings settings = Settings.builder().put("cluster.name", "elasticsearch").build();
        try {
            InetSocketAddress transportAddress = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9300);
            this.client = new PreBuiltTransportClient(settings).addTransportAddress(new TransportAddress(transportAddress));
        } catch (UnknownHostException e) {
            logger.error(e.getMessage(), e);
        }

        try {
            client.prepareIndex(INDEX, DOC_TYPE).get().getResult().toString();
        } catch (ActionRequestValidationException e) {
            try {

                mapIndex();

                indexDocuments();

            } catch (IOException e1) {
                logger.error(e1.getMessage(), e1);
            }
        }
    }

    private void mapIndex() throws IOException {
        boolean exists = client.admin().indices().prepareExists(INDEX).execute().actionGet().isExists();

        if (!exists) {
            XContentBuilder mapping = XContentFactory.jsonBuilder().startObject()
                .startObject("properties")
                    .startObject("popularity").field("type", "double").endObject()
                    .startObject("title").field("type", "text").endObject()
                    .startObject("overview").field("type", "text").endObject()
                    .startObject("release_date").field("type", "date").endObject()
                .endObject()
            .endObject();

            client.admin().indices().prepareCreate(INDEX).addMapping(DOC_TYPE, mapping).execute().actionGet();
        }
    }

    private void indexDocuments() throws IOException {

        File file = new File(DATASET_FILE);

        String[] headerNames = getHeaderNames(file);

        Iterable<CSVRecord> records = getCSVRecords(file);

        BulkRequestBuilder prepareBulk = client.prepareBulk();
        int bulkCount = 0;
        for (CSVRecord record : records) {
            Map<String, Object> movie = new HashMap<>();

            int fieldPos = 0;
            for (String fieldName : headerNames) {
                try {
                    movie.put(fieldName, record.get(fieldPos++));
                } catch (Exception e2) {
                    String error = e2.getCause() != null ? e2.getCause().getMessage() : e2.getMessage();
                    logger.error(String.format("Filed: %s, Error: %s.", fieldName, error));
                }
            }

            try {

                IndexRequestBuilder source = client.prepareIndex(INDEX, DOC_TYPE).setSource(movie);
                prepareBulk.add(source);
                bulkCount++;
                //source.get();
                if (bulkCount > 500) {
                    prepareBulk.get();
                    bulkCount = 0;
                    prepareBulk = client.prepareBulk();
                }
            } catch (Exception e3) {
                logger.error(e3.getMessage(), e3);
            }
        }
    }

    private String[] getHeaderNames(File file) throws FileNotFoundException {
        try (Scanner scan = new Scanner(file)) {

            String header = scan.nextLine();
            String[] headerNames = header.split(",");
            return headerNames;
        }
    }

    private Iterable<CSVRecord> getCSVRecords(File file) throws FileNotFoundException, IOException {
        Reader in = new FileReader(file);
        Iterable<CSVRecord> records = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(in);
        return records;
    }

    public List<Movie> findTop10() {
        return findTop10ByFilters(null, null, null);
    }

    public List<Movie> findTop10ByFilters(String title, String overview, Integer releaseYear) {

        SearchRequestBuilder searchRequest = client.prepareSearch(INDEX).setTypes(DOC_TYPE).addSort("popularity", SortOrder.DESC);
        addFilter(title, overview, releaseYear, searchRequest);
        SearchResponse searchResponse = searchRequest.execute().actionGet();

        List<SearchHit> searchHits = Arrays.asList(searchResponse.getHits().getHits());

        List<Movie> filmes = new ArrayList<>();
        searchHits.forEach(hit -> {
            filmes.add(JSON.parseObject(hit.getSourceAsString(), Movie.class));
        });

        return filmes;
    }

    private void addFilter(String title, String overview, Integer releaseYear, SearchRequestBuilder searchRequest) {

        BoolQueryBuilder filter = QueryBuilders.boolQuery();

        if (StringUtils.isNotBlank(title)) {
            filter.must(QueryBuilders.matchQuery("title", title));
        }

        if (StringUtils.isNotBlank(overview)) {
            filter.must(QueryBuilders.matchQuery("overview", overview));
        }

        if (releaseYear != null) {
            filter.must(YearQueryBuilder.buildQuery("release_date", releaseYear));
        }

        searchRequest.setQuery(filter);
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        this.client.close();
    }
}
