package org.ekstep.mvcjobs.samza.test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.gson.Gson;
import org.apache.samza.config.Config;
import org.ekstep.common.Platform;
import org.ekstep.mvcjobs.samza.service.util.MVCProcessorCassandraIndexer;
import org.ekstep.searchindex.util.HTTPUtil;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.*;

import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Config.class, HTTPUtil.class})
@PowerMockIgnore({"javax.management.*", "sun.security.ssl.*", "javax.net.ssl.*" , "javax.crypto.*"})
public class MVCProcessorCassandraTestNew extends MVCCassandraTestSetup {

    static final String keyspace = Platform.config.hasPath("cassandra.keyspace")
            ? Platform.config.getString("cassandra.keyspace")
            : "dock_content_store";
    static final String table = "content_data";

    private static String createKeyspace = "CREATE KEYSPACE IF NOT EXISTS " + keyspace
            + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}";
    private static String createTable = "CREATE TABLE IF NOT EXISTS " + keyspace + "." + table
            + " (content_id text PRIMARY KEY,body blob,externallink text,last_updated_on timestamp,level1_concept list<text>,level1_name list<text>,level2_concept list<text>,level2_name list<text>,level3_concept list<text>,level3_name list<text>,ml_content_text text,ml_content_text_vector frozen<set<double>>,ml_keywords list<text>,oldbody blob,screenshots blob,source text,sourceurl text,stageicons blob,textbook_name list<text>);";

    private String uniqueId = "do_113041248230580224116";
    private String sourceUrl = "https://diksha.gov.in/play/content/do_30030488";
    private String event2ContentText = "This is the content text for addition of two numbers.";
    private List<Double> event3VectorList = Arrays.asList(0.2961231768131256, 0.13621050119400024, 0.655802309513092, -0.33641257882118225);
    private Set<Double> event3Vector = new HashSet<>(event3VectorList);

    private String eventData = "{\"identifier\":\"do_113041248230580224116\",\"action\":\"update-es-index\",\"stage\":1,\"ownershipType\":[\"createdBy\"],\"code\":\"test.res.1\",\"channel\":\"in.ekstep\",\"language\":[\"English\"],\"mediaType\":\"content\",\"osId\":\"org.ekstep.quiz.app\",\"languageCode\":[\"en\"],\"version\":2,\"versionKey\":\"1591949601174\",\"license\":\"CC BY 4.0\",\"idealScreenDensity\":\"hdpi\",\"framework\":\"NCFCOPY\",\"s3Key\":\"content/do_113041248230580224116/artifact/validecml_1591949596304.zip\",\"createdBy\":\"95e4942d-cbe8-477d-aebd-ad8e6de4bfc8\",\"compatibilityLevel\":1,\"name\":\"Resource Content 1\",\"status\":\"Draft\",\"level1Concept\":[\"Addition\"],\"level1Name\":[\"Math-Magic\"],\"textbook_name\":[\"How Many Times?\"],\"sourceURL\":\"https://diksha.gov.in/play/content/do_30030488\",\"source\":\"Diksha 1\"}";
    private String eventData2 = "{\"action\":\"update-ml-keywords\",\"stage\":\"2\",\"ml_Keywords\":[\"maths\",\"addition\",\"add\"],\"ml_contentText\":\"This is the content text for addition of two numbers.\"}";
    private String eventData3 = "{\"action\":\"update-ml-contenttextvector\",\"stage\":3,\"ml_contentTextVector\":[[0.2961231768131256, 0.13621050119400024, 0.655802309513092, -0.33641257882118225]]}";

//    private String getResp = "{\"id\":\"api.content.read\",\"ver\":\"1.0\",\"ts\":\"2020-07-21T05:38:46.301Z\",\"params\":{\"resmsgid\":\"7224a4d0-cb14-11ea-9313-0912071b8abe\",\"msgid\":\"722281f0-cb14-11ea-9313-0912071b8abe\",\"status\":\"successful\",\"err\":null,\"errmsg\":null},\"responseCode\":\"OK\",\"result\":{\"content\":{\"ownershipType\":[\"createdBy\"],\"code\":\"test.res.1\",\"channel\":\"in.ekstep\",\"language\":[\"English\"],\"mediaType\":\"content\",\"osId\":\"org.ekstep.quiz.app\",\"languageCode\":[\"en\"],\"version\":2,\"versionKey\":\"1591949601174\",\"license\":\"CC BY 4.0\",\"idealScreenDensity\":\"hdpi\",\"framework\":\"NCFCOPY\",\"s3Key\":\"content/do_113041248230580224116/artifact/validecml_1591949596304.zip\",\"createdBy\":\"95e4942d-cbe8-477d-aebd-ad8e6de4bfc8\",\"compatibilityLevel\":1,\"name\":\"Resource Content 1\",\"status\":\"Draft\",\"level1Concept\":[\"Addition\"],\"level1Name\":[\"Math-Magic\"],\"textbook_name\":[\"How Many Times?\"],\"sourceURL\":\"https://diksha.gov.in/play/content/do_30030488\",\"source\":\"Diksha 1\"}}}";
    private String postReqStage1Resp = "{\"id\":\"api.daggit\",\"params\":{\"err\":\"null\",\"errmsg\":\"Dag Initialization failed\",\"msgid\":\"\",\"resmsgid\":\"null\",\"status\":\"success\"},\"responseCode\":\"OK\",\"result\":{\"execution_date\":\"2020-07-08\",\"experiment_name\":\"Content_tagging_20200708-141923\",\"status\":200},\"ts\":\"2020-07-08 14:19:23:1594198163\",\"ver\":\"v1\"}";
    private String postReqStage2Resp = "{\"ets\":\"2020-07-14 15:27:23:1594720643\",\"id\":\"api.ml.vector\",\"params\":{\"err\":\"null\",\"errmsg\":\"null\",\"msgid\":\"\",\"resmsgid\":\"null\",\"status\":\"success\"},\"result\":{\"action\":\"get_BERT_embedding\",\"vector\":[[]]}}";

    MVCProcessorCassandraIndexer cassandraManager = new MVCProcessorCassandraIndexer();

    @BeforeClass
    public static void setup() throws Exception {
        executeScript(createKeyspace, createTable);
    }

    @Test
    public void testInsertToCassandraForStage1() throws Exception  {
        PowerMockito.mockStatic(HTTPUtil.class);
        when(HTTPUtil.makePostRequest(Mockito.anyString(),Mockito.anyString())).thenReturn(postReqStage1Resp);

        cassandraManager.insertintoCassandra(getEvent(eventData), uniqueId);
        Thread.sleep(2000);
        ResultSet resultSet = getSession().execute("SELECT content_id,sourceURL FROM " +keyspace+ "." +table+" WHERE content_id='"+uniqueId+"';");
        List<Row> rows = resultSet.all();
        int count = rows.size();
        Assert.assertTrue(count == 1);
        Row row = rows.get(0);
        Assert.assertTrue(uniqueId.equals(row.getString("content_id")));
        Assert.assertTrue(sourceUrl.equals(row.getString("sourceURL")));
    }

    @Test
    public void testInsertToCassandraForStage2() throws Exception  {
        PowerMockito.mockStatic(HTTPUtil.class);
        when(HTTPUtil.makePostRequest(Mockito.anyString(),Mockito.anyString())).thenReturn(postReqStage2Resp);


        cassandraManager.insertintoCassandra(getEvent(eventData2), uniqueId);
        Thread.sleep(2000);
        ResultSet resultSet = getSession().execute("SELECT content_id,ml_content_text FROM " +keyspace+ "." +table+" WHERE content_id='"+uniqueId+"';");
        List<Row> rows = resultSet.all();
        int count = rows.size();
        Assert.assertTrue(count == 1);
        Row row = rows.get(0);
        Assert.assertTrue(uniqueId.equals(row.getString("content_id")));
//        Assert.assertTrue(event2.equals(row.getString("action")));
        Assert.assertTrue(event2ContentText.equals(row.getString("ml_content_text")));
    }
    @Test
    public void testInsertToCassandraForStage3() throws Exception  {

        cassandraManager.insertintoCassandra(getEvent(eventData3), uniqueId);
        Thread.sleep(2000);
        ResultSet resultSet = getSession().execute("SELECT content_id,ml_content_text_vector FROM " +keyspace+ "." +table+" WHERE content_id='"+uniqueId+"';");
        List<Row> rows = resultSet.all();
        int count = rows.size();
        Assert.assertTrue(count == 1);
        Row row = rows.get(0);
        Assert.assertTrue(uniqueId.equals(row.getString("content_id")));
        Assert.assertTrue(event3Vector.equals(row.getSet("ml_content_text_vector", Double.class)));
    }

    public Map<String, Object> getEvent(String message) throws IOException {
        return  new Gson().fromJson(message, Map.class);
    }
}
