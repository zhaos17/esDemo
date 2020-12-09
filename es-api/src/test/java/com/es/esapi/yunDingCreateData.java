package com.es.esapi;

import com.alibaba.fastjson.JSON;
import com.es.esapi.modal.User;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;

@SpringBootTest
public class yunDingCreateData {

    @Autowired
    @Qualifier("yunDingRestHighLevelClient")
    private RestHighLevelClient client;

    @Test
    void contextLoads() {
    }

    // 获取文档
    @Test
    void getDocument() throws IOException {
        GetRequest getRequest = new GetRequest("overall_store_day_v4", "1e99f3a0272ac608a716556b930606f_20200114");
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        System.out.println(getResponse.getSourceAsMap());
        System.out.println(getResponse);
    }

    // 批量操作文档
    @Test
    void bulkDocument() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("5s");

        User user = new User();
        user.setName("zhaosTest");
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

}
