package br.com.fasam.pos.bigdata.MoviesSearchPos.controller;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.ModelAndView;
import br.com.fasam.pos.bigdata.MoviesSearchPos.model.Movie;
import br.com.fasam.pos.bigdata.MoviesSearchPos.repository.Movies;

@Controller
public class IndexController {
    
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    
	@Autowired
	private Movies filmes;

	@GetMapping(path = { "/", "/index", "/home" })
	public ModelAndView index() {
		ModelAndView mav = new ModelAndView("/index");
		List<Movie> topFilmes = filmes.findTop10();
		mav.addObject("movies", topFilmes);
		
		logger.info("Index path achieved.");

		return mav;
	}

	@GetMapping("/search")
	public ModelAndView search(@RequestParam("title") String title, @RequestParam("overview") String overview, @RequestParam("year") Integer year) {
		ModelAndView mav = new ModelAndView("/search");
		List<Movie> findedFilmes = filmes.findTop10ByFilters(title, overview, year);
		mav.addObject("movies", findedFilmes);
		mav.addObject("title", title);
        mav.addObject("overview", overview);
        mav.addObject("year", year);
		
		logger.debug("Search method achieved.");
		
		return mav;
	}

}
