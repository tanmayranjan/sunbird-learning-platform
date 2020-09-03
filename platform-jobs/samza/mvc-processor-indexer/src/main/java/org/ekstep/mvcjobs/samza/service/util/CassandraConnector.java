package org.ekstep.mvcjobs.samza.service.util;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import org.apache.commons.lang3.StringUtils;
import org.ekstep.common.Platform;
import org.ekstep.jobs.samza.util.JobLogger;

import java.net.InetSocketAddress;
import java.util.*;

public class CassandraConnector {
    private  static  JobLogger LOGGER = new JobLogger(CassandraConnector.class);
    private static String keyspace;

  static  String arr[],table = "content_data";
   static Session session;
    static public Session getSession() {
        if(session != null) {
            LOGGER.info("CassandraSession Exists");
            return session;
        }
        String serverIP = Platform.config.hasPath("cassandra.lp.connection") ? Platform.config.getString("cassandra.lp.connection") : "127.0.0.1:9042";
        if(serverIP == null) {
            LOGGER.info("Server ip of cassandra is null");
        }
        LOGGER.info("Server ip of cassandra is " + serverIP);
        List<String> connectionInfo = Arrays.asList(serverIP.split(","));
        List<InetSocketAddress> addressList = getSocketAddress(connectionInfo);
        Cluster cluster = Cluster.builder()
                .addContactPointsWithPorts(addressList)
                .build();

        session = cluster.connect();
        keyspace = Platform.config.hasPath("cassandra.keyspace")
                ? Platform.config.getString("cassandra.keyspace")
                : "dock_content_store";
        LOGGER.info("Cassandra keyspace is " + keyspace);
//        session = cluster.connect(Platform.config.getString("cassandra.keyspace"));
        LOGGER.info("The server IP " + serverIP + "\n Session created " + session);
        return session;
    }
    public static void updateContentProperties(String contentId, Map<String, Object> map) {
        Session session = getSession();
        if (null == map || map.isEmpty())
            return;
        String query = getUpdateQuery(map.keySet());
        if(query == null)
            return;
        PreparedStatement ps = session.prepare(query);
        Object[] values = new Object[map.size() + 1];
        try {
            int i = 0;
            for (Map.Entry<String, Object> entry : map.entrySet()) {

                if (null == entry.getValue()) {
                    continue;
                }  else {
                    values[i] = entry.getValue();
                }

                i += 1;
            }
            values[i] = contentId;
            BoundStatement bound = ps.bind(values);
            LOGGER.info("Executing the statement to insert into cassandra for identifier  " + contentId);
            session.execute(bound);
        } catch (Exception e) {
          System.out.println("Exception " + e);
          LOGGER.info("Exception while inserting data into cassandra " + e);
        }
    }
    private static String getUpdateQuery(Set<String> properties) {
        StringBuilder sb = new StringBuilder();
        if (null != properties && !properties.isEmpty()) {
            sb.append("UPDATE " + keyspace + "." + table + " SET last_updated_on = dateOf(now()), ");
            StringBuilder updateFields = new StringBuilder();
            for (String property : properties) {
                if (StringUtils.isBlank(property))
                    return null;
                updateFields.append(property.trim()).append(" = ?, ");
            }
            sb.append(StringUtils.removeEnd(updateFields.toString(), ", "));
            sb.append(" where content_id = ?");
        }
        return sb.toString();
    }
    private static List<InetSocketAddress> getSocketAddress(List<String> hosts) {
        List<InetSocketAddress> connectionList = new ArrayList<>();
        for (String connection : hosts) {
            String[] conn = connection.split(":");
            String host = conn[0];
            int port = Integer.valueOf(conn[1]);
            connectionList.add(new InetSocketAddress(host, port));
        }
        return connectionList;
    }
}
