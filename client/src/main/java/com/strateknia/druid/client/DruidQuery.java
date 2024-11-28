package com.strateknia.druid.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Properties;

public class DruidQuery {
    private static final Logger log = LoggerFactory.getLogger(DruidQuery.class);

    public static void main(String[] args) {

        String url = "jdbc:avatica:remote:url=http://ec2-54-158-173-230.compute-1.amazonaws.com:8888/druid/v2/sql/avatica/;transparent_reconnection=true";

        Properties properties = new Properties();
        properties.put("sqlTimeZone", "Etc/UTC");
        properties.put("user", "admin");
        properties.put("password", "password1");

        try(Connection conn = DriverManager.getConnection(url, properties)) {
            try (
                    final Statement statement = conn.createStatement();
                    final ResultSet rs = statement.executeQuery(
                            "SELECT \"line\", COUNT(*) AS \"count\" " +
                                    "FROM \"topic_hello\" " +
                                    "GROUP BY 1"
                    )
            ) {
                while(rs.next()) {
                    log.info("{}", rs.getString("line"));
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
