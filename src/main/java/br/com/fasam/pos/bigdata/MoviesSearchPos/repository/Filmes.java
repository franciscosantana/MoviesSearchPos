package br.com.fasam.pos.bigdata.MoviesSearchPos.repository;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;
import com.alibaba.fastjson.JSON;
import br.com.fasam.pos.bigdata.MoviesSearchPos.model.Filme;

@Repository
public class Filmes {

    private static final String INDEX = "site";

    private static final String DOC_TYPE = "movies";

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private TransportClient client;

    @SuppressWarnings("resource")
    public Filmes() {
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

                File file = new File("data/movies_metadata.csv");

                Reader in = new FileReader(file);

                Scanner scan = new Scanner(file);

                String header = scan.nextLine();
                String[] headerVals = header.split(",");

                Iterable<CSVRecord> records = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(in);

                BulkRequestBuilder prepareBulk = client.prepareBulk();
                int bulkCount = 0;
                for (CSVRecord record : records) {
                    Map<String, Object> movie = new HashMap<>();

                    int count = 0;
                    for (String s : headerVals) {
                        String valueString = "";
                        Object value = null;
                        try {

                            switch (s) {
                                case "popularity":
                                    valueString = record.get(count);
                                    if (valueString != null && !valueString.trim().equals("")) {
                                        value = Double.valueOf(valueString);
                                    }
                                    break;
                                case "release_date":
                                    valueString = record.get(count);
                                    if (valueString != null && !valueString.trim().equals("")) {
                                        String[] valueArray = valueString.split("-");
                                        if (valueArray.length == 3) {
                                            value = LocalDate.of(Integer.valueOf(valueArray[0]), Integer.valueOf(valueArray[1]), Integer.valueOf(valueArray[2]));
                                        }
                                    }
                                    break;
                                case "title":
                                    value = record.get(count);
                                case "overview":
                                    value = record.get(count);
                                    break;
                                default:
                                    count++;
                                    continue;
                            }

                        } catch (Exception e2) {
                            String error = e2.getCause() != null ? e2.getCause().getMessage() : e2.getMessage();
                            logger.error(String.format("Filed: %s, Value: %s, Error: %s.", s, valueString, error));
                        }
                        movie.put(s, value);
                        count++;
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

            } catch (IOException e1) {
                logger.error(e1.getMessage(), e1);
            }
        }
    }

    public List<Filme> getTopFilmes() {
        List<Filme> filmes = new ArrayList<>();
        SearchResponse response = client.prepareSearch().addSort("popularity", SortOrder.DESC).execute().actionGet();
        List<SearchHit> searchHits = Arrays.asList(response.getHits().getHits());
        searchHits.forEach(hit -> {
            filmes.add(JSON.parseObject(hit.getSourceAsString(), Filme.class));
        });

        return filmes;
    }

    public List<Filme> getSearchFilmes(String titulo, String desc, Integer ano) {
        List<Filme> filmes = new ArrayList<>();
        SearchResponse searchResponse = client.prepareSearch("movies").
                setTypes("movie")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.termQuery("title", titulo))
                .setFrom(0).setSize(60).setExplain(true).get();
        List<SearchHit> searchHits = Arrays.asList(searchResponse.getHits().getHits());
        searchHits.forEach(
                hit -> {
                   filmes.add(JSON.parseObject(hit.getSourceAsString(), Filme.class));
                }
        );

        return filmes;
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        this.client.close();
    }
}
