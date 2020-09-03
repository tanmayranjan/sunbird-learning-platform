package org.ekstep.mvcjobs.samza.test;

import com.datastax.driver.core.Session;
import org.ekstep.mvcjobs.samza.service.util.CassandraConnector;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.*;

public class MVCCassandraTestSetup {

    private static Session session = null;
//    private JobLogger LOGGER = new JobLogger(MVCCassandraTestSetup.class);


    @AfterClass
    public static void afterTest() throws Exception {
        tearEmbeddedCassandraSetup();
    }

    @BeforeClass
    public static void before() throws Exception {
        setupEmbeddedCassandra();
    }

    protected static Session getSession() {
        return session;
    }

    private static void setupEmbeddedCassandra() throws Exception {
        InputStream inputStream = MVCCassandraTestSetup.class.getResourceAsStream("/mvc-cassandra-unit.yaml");
//        int i;
//        while((i = inputStream.read())!=-1) {
//            char c = (char)i;
//
//            // prints character
//            System.out.print(c);
//        }
//        File file = new File("test.yaml");
//        try(OutputStream outputStream = new FileOutputStream(file)){
//            IOUtils.copy(inputStream, outputStream);
//        } catch (FileNotFoundException e) {
//            // handle exception here
//        } catch (IOException e) {
//            // handle exception here
//        }
//        EmbeddedCassandraServerHelper.startEmbeddedCassandra(file, ".", 100000L);
        EmbeddedCassandraServerHelper.startEmbeddedCassandra("/mvc-cassandra-unit.yaml", 200000L);
        session = CassandraConnector.getSession();
    }

    private static void tearEmbeddedCassandraSetup() {
        try {
            if (session !=null && !session.isClosed()) {
                session.close();
                EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    protected static void executeScript(String... querys) throws Exception {
        session = CassandraConnector.getSession();
        for (String query : querys) {
            session.execute(query);
        }
    }

}
