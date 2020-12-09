package com.es.esapi;

import com.alibaba.fastjson.JSON;
import com.es.esapi.modal.User;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class EsApiApplicationTests {

    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient client;

    @Test
    void contextLoads() {
    }

    // 索引创建
    @Test
    void createIndex() throws IOException {
        // 1.创建索引请求
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("test_index");
        CreateIndexResponse createIndexResponse = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        System.out.println(createIndexResponse.index());
    }

    // 获取索引
    @Test
    void existIndex() throws IOException {
        GetIndexRequest getIndexRequest = new GetIndexRequest("test_index");
        boolean exists = client.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    // 删除索引
    @Test
    void deleteIndex() throws IOException {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("test_index");
        AcknowledgedResponse delete = client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
        System.out.println(delete.isAcknowledged());
    }

    // 添加文档
    @Test
    void addDocument() throws IOException {
        User user = new User();
        user.setName("赵松");
        user.setAge(25);
        // 创建请求
        IndexRequest indexRequest = new IndexRequest("test_index");
        // 相当于 es中的 PUT /test_index/_doc/1 命令
        indexRequest.id("1");
        // 可通过这两种方式设置超时时间，也可以不设置
        indexRequest.timeout(TimeValue.timeValueSeconds(1));
//        indexRequest.timeout("1s");
        indexRequest.source(JSON.toJSONString(user), XContentType.JSON);

        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println(indexResponse.status());
        System.out.println(indexResponse.toString());
    }

    // 获取文档 判断是否存在
    @Test
    void existDocument() throws IOException {
        GetRequest getRequest = new GetRequest("test_index", "1");
        // 不获取返回的 _source 上下文
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");

        boolean exists = client.exists(getRequest, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    // 获取文档
    @Test
    void getDocument() throws IOException {
        GetRequest getRequest = new GetRequest("test_index", "1");
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        System.out.println(getResponse.getSourceAsMap());
        System.out.println(getResponse);
    }

    // 更新文档
    @Test
    void updateDocument() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("test_index", "1");
        updateRequest.timeout("1s");

        User user = new User();
        user.setName("赵松test");
        user.setAge(25);

        updateRequest.doc(JSON.toJSONString(user), XContentType.JSON);

        UpdateResponse update = client.update(updateRequest, RequestOptions.DEFAULT);
        System.out.println(update.status());
    }

    // 删除文档
    @Test
    void deleteDocument() throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest("test_index", "1");
        deleteRequest.timeout("1s");

        DeleteResponse deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println(deleteResponse.status());
    }

    // 批量操作文档
    @Test
    void bulkDocument() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("5s");

        User user = new User();
        user.setName("赵松test");
        user.setAge(25);

        ArrayList<User> arrayList = new ArrayList<>();
        arrayList.add(user);
        arrayList.add(user);
        arrayList.add(user);
        arrayList.add(user);
        arrayList.add(user);
        // 批量处理请求
        for (int i = 0; i < arrayList.size(); i++) {
            // 新增 其他操作只需要创建对应的request就行
            bulkRequest.add(
                    new IndexRequest("test_index").id("" + (i + 1)).source(JSON.toJSONString(arrayList.get(i)), XContentType.JSON)
            );
        }

        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(bulkResponse.hasFailures());
    }

    // 查询文档
    @Test
    void searchDocument() throws IOException {
        SearchRequest searchRequest = new SearchRequest("test_index");

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
//        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("age", 25);
//        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("name", "赵松test");
        MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();

        sourceBuilder.query(matchAllQueryBuilder);
        // TimeUnit 高并发的类
        sourceBuilder.timeout(new TimeValue(10, TimeUnit.SECONDS));

        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        System.out.println(searchResponse);
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            System.out.println(hit.getSourceAsMap());
        }

    }

}
