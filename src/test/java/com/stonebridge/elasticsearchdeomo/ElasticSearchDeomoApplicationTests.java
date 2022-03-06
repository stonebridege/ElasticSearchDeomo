package com.stonebridge.elasticsearchdeomo;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.Arrays;

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
}
