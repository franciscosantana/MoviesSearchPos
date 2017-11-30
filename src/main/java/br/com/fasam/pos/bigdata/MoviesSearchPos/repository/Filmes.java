package br.com.fasam.pos.bigdata.MoviesSearchPos.repository;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

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
