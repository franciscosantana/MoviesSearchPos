package br.com.fasam.pos.bigdata.MoviesSearchPos.model;

import java.io.Serializable;
import java.util.Date;
import lombok.Data;

@Data
public class Movie implements Serializable {
    
    /**  */
    private static final long serialVersionUID = 6826284861432912199L;
    
    private String title;
    private String overview;
    private Date releaseDate;
    private Double popularity;
    
}
