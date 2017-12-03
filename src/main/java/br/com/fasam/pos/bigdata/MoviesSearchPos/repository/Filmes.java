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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.stereotype.Repository;

import br.com.fasam.pos.bigdata.MoviesSearchPos.model.Filme;

@Repository
public class Filmes {
	private TransportClient client;

	@SuppressWarnings("resource")
	public Filmes() {
		Settings settings = Settings.builder().put("cluster.name", "elasticsearch").build();
		InetSocketAddress transportAddress;
		try {
			transportAddress = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9300);
			this.client = new PreBuiltTransportClient(settings).addTransportAddress(new TransportAddress(transportAddress));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		
		try {
			client.prepareIndex("site", "movies").get().getResult().toString();
		} catch (ActionRequestValidationException e) {
			try {
				Reader in = new FileReader("movies_metadata.csv");
				
				Scanner scan = new Scanner(new File("movies_metadata.csv"));
				
				String header = scan.nextLine();
				String[] headerVals = header.split(",");
				
				try {
					
					Iterable<CSVRecord> records = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(in);
					
					BulkRequestBuilder prepareBulk = client.prepareBulk();
					int bulkCount = 0;
					for (CSVRecord record : records) {
						Map<String, Object> movie = new HashMap<>();
						
						int count = 0;
						for(String s: headerVals) {
							try{
								movie.put(s, record.get(count++));
							} catch (Exception e2) {
								break;
							}
						}
						try {
							IndexRequestBuilder source = client.prepareIndex("site2", "movies").setSource(movie);
							prepareBulk.add(source);
							bulkCount++;
							//source.get();
							if(bulkCount>500) {
								prepareBulk.get();
								bulkCount = 0;
								prepareBulk = client.prepareBulk();
							}
						}catch (Exception e3) {
						}
						
					}
				} catch (IOException e1) {
					e1.printStackTrace();
				}

			} catch (FileNotFoundException e1) {
				e1.printStackTrace();
			}
		}
	}

	public List<Filme> getTopFilmes() {
		List<Filme> filmes = new ArrayList<>();
		// Seu código deve vir daqui para baixo

		return filmes;
	}

	public List<Filme> getSearchFilmes(String titulo, String desc, Integer ano) {
		List<Filme> filmes = new ArrayList<>();
		// Seu código deve vir daqui para baixo

		return filmes;
	}

	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		this.client.close();
	}
}
