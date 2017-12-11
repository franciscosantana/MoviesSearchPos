package br.com.fasam.pos.bigdata.MoviesSearchPos.model;

import java.io.Serializable;
import java.time.LocalDate;
import lombok.Data;

@Data
public class Filme implements Serializable {
    
    /**  */
    private static final long serialVersionUID = 6826284861432912199L;
    
    private String title;
    private String overview;
    private LocalDate releaseDate;
    private Double popularity;
    
}
