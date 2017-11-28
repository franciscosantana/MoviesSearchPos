package br.com.fasam.pos.bigdata.MoviesSearchPos.repository;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Repository;

import br.com.fasam.pos.bigdata.MoviesSearchPos.model.Filme;

@Repository
public class Filmes {

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
	
}
