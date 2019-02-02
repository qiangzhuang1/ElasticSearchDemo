package com.example.ElasticSearchDemo.service;

import com.alibaba.fastjson.JSONObject;
import com.example.ElasticSearchDemo.entity.BookEntity;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.stats.StatsAggregationBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * Created by Tom on 2018/11/17.
 */
@Service
public class BookService {

    private String indexName = "book"; //相当于数据库名称
    private String indexType = "java";	//相当于数据表名称

    @Autowired
    private TransportClient client;

    public GetResponse getById(String id){
        return this.client.prepareGet(indexName,indexType,id).get();
    }

    public IndexResponse add(String name,String publishDate,Double price,String author) throws Exception {
        XContentBuilder content = XContentFactory.jsonBuilder()
                .startObject()
                .field("id","201902010000193948") //字段Id值写死
                .field("name",name)
                .field("publishDate",publishDate)
                .field("price",price)
                .field("author",author)
                .endObject();
        //不指定 _id 的值   IndexResponse response = this.client.prepareIndex(indexName,indexType,).setSource(content).get();
        //指定Id的值
        IndexResponse response = this.client.prepareIndex(indexName,indexType,"201901281824550003").setSource(content).get();
        return response;
    }

    public DeleteResponse remove(String id) {
       return this.client.prepareDelete(indexName,indexType,id).get();
    }

    public UpdateResponse modify(String id, String name, String publishDate,Double price,String author) throws Exception {
        UpdateRequest request = new UpdateRequest(indexName,indexType,id);

        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject();

        if(name != null){
            builder.field("name",name);
        }
        if(publishDate != null){
            builder.field("publishDate",publishDate);
        }
        if(price != null){
            builder.field("price",price);
        }
        if(author != null){
            builder.field("author",author);
        }
        builder.endObject();

        request.doc(builder);
        return this.client.update(request).get();
    }

    public List<BookEntity> matchAllQuery() {
        MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
        SearchResponse searchResponse = this.client.prepareSearch(indexName).setTypes(indexType).setQuery(matchAllQueryBuilder).get();
        SearchHits searchHits =  searchResponse.getHits();
        List<BookEntity> bookEntityList = new ArrayList<BookEntity>();
        for(SearchHit hit:searchHits.getHits()){
            BookEntity bookEntity = JSONObject.parseObject(hit.getSourceAsString(), BookEntity.class);
            bookEntityList.add(bookEntity);
        }
        return bookEntityList;
    }


    public List<BookEntity> matchQueryAndSort(String name) {
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("name", name);
        SearchResponse searchResponse = this.client.prepareSearch(indexName).setTypes(indexType).setQuery(matchQueryBuilder)
                .addSort("publishDate",SortOrder.DESC).get();
        SearchHits searchHits =  searchResponse.getHits();
        List<BookEntity> bookEntityList = new ArrayList<BookEntity>();
        for(SearchHit hit:searchHits.getHits()){
            BookEntity bookEntity = JSONObject.parseObject(hit.getSourceAsString(), BookEntity.class);
            bookEntityList.add(bookEntity);
        }
        return bookEntityList;
    }

    public List<BookEntity> getPageQuery(Integer pageNum, Integer pageSize) {
        MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
        SearchResponse searchResponse = this.client.prepareSearch(indexName).setTypes(indexType).setQuery(matchAllQueryBuilder)
                .setFrom(pageNum).setSize(pageSize).addSort("publishDate",SortOrder.DESC).get();
        SearchHits searchHits =  searchResponse.getHits();
        List<BookEntity> bookEntityList = new ArrayList<BookEntity>();
        for(SearchHit hit:searchHits.getHits()){
            BookEntity bookEntity = JSONObject.parseObject(hit.getSourceAsString(), BookEntity.class);
            bookEntityList.add(bookEntity);
        }
        return bookEntityList;
    }

    public void priceAggregations() {
        //有两种方式能计算 第一种
        StatsAggregationBuilder aggregation =
                AggregationBuilders
                        .stats("agg")
                        .field("price");
        SearchResponse searchResponse = this.client.prepareSearch(indexName).setTypes(indexType)
                .addAggregation(aggregation)
                .execute().actionGet();
        Stats agg = searchResponse.getAggregations().get("agg");
        System.out.println(agg.getMin());
        System.out.println(agg.getMax());
        System.out.println(agg.getAvg());
        System.out.println(agg.getSum());
        System.out.println(agg.getCount());
        //第二种
        MinAggregationBuilder minPrice = AggregationBuilders.min("min_price").field("price");
        //MaxAggregationBuilder maxPrice = AggregationBuilders.max("max_Price").field("price");
        //AvgAggregationBuilder avgPrice = AggregationBuilders.avg("avg_Price").field("price");
        //SumAggregationBuilder sumPrice = AggregationBuilders.sum("sum_Price").field("price");
        Min aggMin = searchResponse.getAggregations().get("min_price");
        SearchResponse searchResponse1 = this.client.prepareSearch(indexName).setTypes(indexType)
                .addAggregation(minPrice)
                .execute().actionGet();
        System.out.println(aggMin.getValue());
    }

    public String priceAndPublishDateGroup() {
        SearchResponse searchResponse = this.client.prepareSearch(indexName).setTypes(indexType)
                .addAggregation(AggregationBuilders.terms("publishDateAgg").field("publishDate")
                .subAggregation(AggregationBuilders.terms("priceAgg").field("price"))).setSize(0)
                .execute().actionGet();

        Terms genders = searchResponse.getAggregations().get("publishDateAgg");
        for (Terms.Bucket entry : genders.getBuckets()) {
            System.out.println(entry.getKey());      // Term
            System.out.println(entry.getDocCount()); // Doc count
        }
        String jsonData = "";
        Map<String, Aggregation> stringAggregationMap = searchResponse.getAggregations().asMap();
        Set<Map.Entry<String, Aggregation>> entries = stringAggregationMap.entrySet();
        Iterator<Map.Entry<String, Aggregation>> it = entries.iterator();
        while (it.hasNext()) {
            Map.Entry<String, Aggregation> next = it.next();

            System.out.println(next.getKey()+"---------------"+next.getValue());
            jsonData = next.getValue().toString();
        }
        return jsonData;
    }
}
