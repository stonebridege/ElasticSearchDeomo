package com.stonebridge.elasticsearchdeomo;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@SpringBootTest
class ElasticSearchDeomoApplicationTests {
    @Autowired
    RestHighLevelClient client;

    @Test
    public void testGet() throws IOException {
        //1.构建请求
        GetRequest getRequest = new GetRequest("test_post", "1");
        //=========设置可选参数==========
        //1.设置可选的参数
        String[] includes = new String[]{"user", "message"};
        //2.设置不选的参数
        String[] excludes = Strings.EMPTY_ARRAY;
        //3.1.设置获取源数据(source)
        //3.2.设置可选的参数
        //3.3.设置不选的参数
        FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includes, excludes);
        getRequest.fetchSourceContext(fetchSourceContext);
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        // 获取结果
        if (getResponse.isExists()) {
            String sourceAsString = getResponse.getSourceAsString();//检索文档(String形式)
            System.out.println("ID:" + getResponse.getId());  //ID:1
            System.out.println("version:" + getResponse.getVersion());  // version:2
            System.out.println(sourceAsString);  // {"message":"trying out es1","user":"tomas"}
        }
    }

    @Test
    public void testGet1() throws IOException {
        //1.构建请求
        GetRequest getRequest = new GetRequest("test_post", "1");
        //=========设置可选参数==========
        //1.设置可选的参数
        String[] includes = Strings.EMPTY_ARRAY;
        //2.设置不选的参数
        String[] excludes = new String[]{"user", "message"};
        //3.1.设置获取源数据(source)
        //3.2.设置可选的参数
        //3.3.设置不选的参数
        FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includes, excludes);
        getRequest.fetchSourceContext(fetchSourceContext);
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        // 获取结果
        if (getResponse.isExists()) {
            System.out.println(getResponse.getSourceAsString());  // {"postDate":"2019-07-18"}
            System.out.println(Arrays.toString(getResponse.getSourceAsBytes()));
            System.out.println(getResponse.getSourceAsMap());
        }
    }

    @Test
    public void testGet2() throws IOException {
        //1.构建请求
        GetRequest getRequest = new GetRequest("test_post", "1");
        //异步查询
        ActionListener<GetResponse> listener = new ActionListener<GetResponse>() {
            //查询成功时的立马执行的方法
            @Override
            public void onResponse(GetResponse getResponse) {
                long version = getResponse.getVersion();
                String sourceAsString = getResponse.getSourceAsString();//检索文档(String形式)
                System.out.println("version:" + version);  // version:2
                System.out.println(sourceAsString);
            }

            //查询失败时的立马执行的方法
            @Override
            public void onFailure(Exception e) {
                e.printStackTrace();
            }
        };
        //执行异步请求
        client.getAsync(getRequest, RequestOptions.DEFAULT, listener);
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testAdd() throws IOException {
//        PUT test_post/_doc/2
//        {
//            "user":"tomas",
//                "postDate":"2019-07-18",
//                "message":"trying out es1"
//        }
        //1.构建请求
        IndexRequest request = new IndexRequest("test_post");
        request.id("5");
        //2.1.构建文档方式1，要标注XContentType.JSON
        //================================================================
        String jsonStr = "{\n" +
                "  \"user\":\"tomas\",\n" +
                "  \"postDate\":\"2022-07-28\",\n" +
                "  \"message\":\"trying out es1\"\n" +
                "}";
        request.source(jsonStr, XContentType.JSON);

//        //2.2.构建文档方式2
//        //================================================================
//        Map<String, Object> jsonMap = new HashMap<>();
//        jsonMap.put("user", "tomas");
//        jsonMap.put("postDate", "2022-03-07");
//        jsonMap.put("message", "trying out es1");
//        request.source(jsonMap);
//
//        //2.3.构建文档方式3
//        //================================================================
//        XContentBuilder builder = XContentFactory.jsonBuilder();
//        builder.startObject();
//        {
//            builder.field("user", "tomas");
//            builder.field("user", "2022-03-07");
//            builder.field("user", "trying out es1");
//        }
//        builder.endObject();
//        request.source(builder);
//
//        //2.4.构建文档方式4
//        //================================================================
//        request.source("user", "tomas", "user", "2022-03-07", "user", "trying out es1");
//
//        //4.可选参数
//        request.timeout("1s");
//        request.timeout(TimeValue.timeValueSeconds(1));
//
//        request.version(2);
//        request.versionType(VersionType.EXTERNAL);

        //5.执行
        //5.1.同步新增操作
        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
        //6.获取结果
        System.out.println(indexResponse.getIndex());
        System.out.println(indexResponse.getId());
        System.out.println(indexResponse.getResult());
        //7.判断处理结果
        DocWriteResponse.Result indexResponseResult = indexResponse.getResult();
        if (indexResponseResult == DocWriteResponse.Result.CREATED) {
            System.out.println("新增：" + indexResponseResult);
        } else if (indexResponseResult == DocWriteResponse.Result.UPDATED) {
            System.out.println("UPDATED：" + indexResponseResult);
        }

        ReplicationResponse.ShardInfo shardInfo = indexResponse.getShardInfo();
        if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
            System.out.println("处理成功的分片数少于总分片！");
        }
        if (shardInfo.getFailed() > 0) {
            for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                String reason = failure.reason();//处理潜在的失败原因
                System.out.println(reason);
            }
        }
    }
}
