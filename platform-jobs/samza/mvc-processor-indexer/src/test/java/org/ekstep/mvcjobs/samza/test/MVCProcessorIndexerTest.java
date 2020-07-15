package org.ekstep.mvcjobs.samza.test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.MapUtils;
import org.apache.samza.config.Config;
import org.ekstep.mvcjobs.samza.service.util.MVCProcessorIndexer;
import org.ekstep.mvcsearchindex.elasticsearch.ElasticSearchUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.Map;

import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ElasticSearchUtil.class, Config.class, Cluster.class, Session.class, MVCProcessorIndexer.class})
@PowerMockIgnore({"javax.management.*", "sun.security.ssl.*", "javax.net.ssl.*" , "javax.crypto.*"})
public class MVCProcessorIndexerTest {
    private String KEYSPACE_CREATE_SCRIPT = "CREATE KEYSPACE IF NOT EXISTS sunbirddev_content_store WITH replication = {'class': 'SimpleStrategy','replication_factor': '1'};";
    ObjectMapper mapper = new ObjectMapper();
    private String uniqueId = "do_113041248230580224116";
    private String eventData = "{\"eventData\":{\"identifier\":\"do_113041248230580224116\",\"action\":\"update-es-index\",\"stage\":1}}";
    private String doc = "{\"action\":\"update-es-index\",\"eid\":\"MVC_JOB_PROCESSOR\",\"ets\":1591603456223,\"mid\":\"LP.1591603456223.a5d1c6f0-a95e-11ea-b80d-b75468d19fe4\",\"actor\":{\"id\":\"UPDATE ES INDEX\",\"type\":\"System\"},\"context\":{\"pdata\":{\"ver\":\"1.0\",\"id\":\"org.ekstep.platform\"},\"channel\":\"01285019302823526477\"},\"object\":{\"ver\":\"1.0\",\"id\":\"do_113041248230580224116\"},\"eventData\":{\"identifier\":\"do_113041248230580224116\",\"action\":\"update-es-index\",\"stage\":1}}";
    private Config configMock;
    private Session session = null;
    private Cluster cluster;
    @Before
    public void setup(){
        executeCassandraQuery(KEYSPACE_CREATE_SCRIPT);
        MockitoAnnotations.initMocks(this);
        configMock = Mockito.mock(Config.class);
        stub(configMock.get("nested.fields")).toReturn("badgeAssertions,targets,badgeAssociations,plugins,me_totalTimeSpent,me_totalPlaySessionCount,me_totalTimeSpentInSec,batches");
    }

  @Test
    public void testUpsertDocument() throws Exception {
        PowerMockito.mockStatic(ElasticSearchUtil.class);
        PowerMockito.doNothing().when(ElasticSearchUtil.class);
        ElasticSearchUtil.addDocumentWithId(Mockito.anyString(),Mockito.anyString(),Mockito.anyString());
      MVCProcessorIndexer mvcProcessorIndexer = new MVCProcessorIndexer();
        mvcProcessorIndexer.upsertDocument(uniqueId,getEvent(doc));
}
     @Test
     public void testRemoveExtraParams() throws IOException {
         MVCProcessorIndexer mvcProcessorIndexer = new MVCProcessorIndexer();
         Map<String, Object> message = mvcProcessorIndexer.removeExtraParams(getEvent(eventData));
         Assert.assertTrue(MapUtils.isNotEmpty(message));
    }
    public  Map<String, Object> getEvent(String message) throws IOException {
       return mapper.readValue(message,Map.class);
    }
    public void executeCassandraQuery(String query) {
        if(null == session || session.isClosed()){
            cluster = Cluster.builder()
                    .addContactPoints("127.0.0.1")
                    .build();
           session = cluster.newSession();
        }
            session.execute(query);
    }
}
