package com.barton.es;

import com.xiaoleilu.hutool.log.Log;
import com.xiaoleilu.hutool.log.LogFactory;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.ClearScrollRequestBuilder;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.util.Date;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

/**
 * create by barton on 2018-8-2
 */
public class Test {
    private static final Log log = LogFactory.get();
    public static void main(String [] args) throws Exception {
        TransportClient client = EsClientUtil.getClient();
        //1> 创建index
        /*TransportClient client = EsClientUtil.getClient();
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .field("user", "ccse")
                .field("postDate", new Date())
                .field("message", "this is Elasticsearch")
                .endObject();

        IndexResponse response = client.prepareIndex("estest", "test_1").setSource(builder).get();
        log.info("ddddd{}",response.toString());
        System.out.println("创建成功!");*/

        /*##################################################*/


        //2> get (根据id查询)
        /*GetResponse response = client.prepareGet("estest", "test_1", "2Ul8-WQBWhqkLKrb72g0").get();
        log.info("{}",response);*/

        /*##################################################*/

        //3>根据id 删除
        /*DeleteResponse dresponse = client.prepareDelete("estest", "test_1", "2Ul8-WQBWhqkLKrb72g0").get();
        log.info("{}",dresponse);
        GetResponse gResponse = client.prepareGet("estest", "test_1", "2Ul8-WQBWhqkLKrb72g0").get();
        log.info("{}",gResponse);*/

        /*##################################################*/

        //4>通过查询条件删除
        /*BulkByScrollResponse response =
                DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                        .filter(QueryBuilders.matchQuery("postDate", "2018-08-02T07:36:59.127Z")) //查询条件
                        .source("estest") //index(索引名)
                        .get();  //执行

        long deleted = response.getDeleted(); //删除文档的数量
        log.info("{}",response);
        log.info("{}",deleted);

        /*##################################################*/

        //5>异步删除执行
        /*DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                .filter(QueryBuilders.matchQuery("postDate", "2018-08-02T07:33:09.382Z"))      //查询
                .source("estest")                //index(索引名)
                .execute(new ActionListener<BulkByScrollResponse>() {
                    public void onResponse(BulkByScrollResponse bulkByScrollResponse) {
                        long deleted = bulkByScrollResponse.getDeleted();   //删除文档的数量
                    }

                    public void onFailure(Exception e) {

                    }     //回调监听

                });*/

        /*##################################################*/


        //6>根据id更新
        /*UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index("estest");
        updateRequest.type("test_1");
        updateRequest.id("3kmC-WQBWhqkLKrbxGiv");
        updateRequest.doc(jsonBuilder()
                .startObject()
                .field("user", "demo")
                .endObject());
        client.update(updateRequest).get();*/

        //7>查询
        QueryBuilder qb = termQuery("user", "demo");

        SearchResponse scrollResp = client.prepareSearch("estest")
                .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)//按时间排序
                .setScroll(new TimeValue(60000)) //为了使用 scroll，初始搜索请求应该在查询中指定 scroll 参数，告诉 Elasticsearch 需要保持搜索的上下文环境多长时间（滚动时间）
                .setQuery(qb)
                .setSize(100).get(); //max of 100 hits will be returned for each scroll
        //Scroll until no hits are returned
        do {
            for (SearchHit hit : scrollResp.getHits().getHits()) {
                //Handle the hit...
                log.info("{}",hit);
            }

            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
        } while(scrollResp.getHits().getHits().length != 0); // Zero hits mark the end of the scroll and the while loop.


        client.close();
    }


    /**
     * 清除滚动ID
     * @param client
     * @param scrollIdList
     * @return
     */
    public static boolean clearScroll(Client client, List<String> scrollIdList){
        ClearScrollRequestBuilder clearScrollRequestBuilder = client.prepareClearScroll();
        clearScrollRequestBuilder.setScrollIds(scrollIdList);
        ClearScrollResponse response = clearScrollRequestBuilder.get();
        return response.isSucceeded();
    }
    /**
     * 清除滚动ID
     * @param client
     * @param scrollId
     * @return
     */
    public static boolean clearScroll(Client client, String scrollId){
        ClearScrollRequestBuilder clearScrollRequestBuilder = client.prepareClearScroll();
        clearScrollRequestBuilder.addScrollId(scrollId);
        ClearScrollResponse response = clearScrollRequestBuilder.get();
        return response.isSucceeded();
    }
}
