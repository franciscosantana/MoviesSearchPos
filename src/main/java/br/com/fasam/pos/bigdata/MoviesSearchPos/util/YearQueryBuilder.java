package br.com.fasam.pos.bigdata.MoviesSearchPos.util;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;

public class YearQueryBuilder {

    public static QueryBuilder buildQuery(String field, Integer year) {

        Interval interval = new Interval(new DateTime(year, 1, 1, 0, 0, DateTimeZone.UTC), new DateTime(year, 12, 1, 0, 0, DateTimeZone.UTC));

        return QueryBuilders.rangeQuery(field).gte(interval.getStart().toString()).lt(interval.getEnd().toString());
    }

}
