package org.datasource.jdbc.views.users;

import org.datasource.jdbc.JDBCDataSourceConnector;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

@Service
public class UsersViewBuilder {
    private static Logger logger = Logger.getLogger(UsersViewBuilder.class.getName());

    private String SQL_USERS_SELECT =
            "SELECT id, email, full_name, country_code, city, created_at, last_login_at, is_active FROM USERS FETCH FIRST 40000 ROWS ONLY";

    private List<UsersView> usersViewList = new ArrayList<>();

    public List<UsersView> getViewList() {
        return this.usersViewList;
    }

    public UsersViewBuilder build() {
        logger.info(">>> Building UsersView ...");
        try (Connection jdbcConnection = jdbcConnector.getConnection()) {
            Statement selectStmt = jdbcConnection.createStatement();
            ResultSet rs = selectStmt.executeQuery(SQL_USERS_SELECT);

            usersViewList = new ArrayList<>();
            while (rs.next()) {
                String id = rs.getString("id");
                String email = rs.getString("email");
                String fullName = rs.getString("full_name");
                String countryCode = rs.getString("country_code");
                String city = rs.getString("city");
                String createdAt = rs.getString("created_at");
                String lastLoginAt = rs.getString("last_login_at");
                String isActive = rs.getString("is_active");

                this.usersViewList.add(new UsersView(id, email, fullName, countryCode,
                        city, createdAt, lastLoginAt, isActive));
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return this;
    }

    private JDBCDataSourceConnector jdbcConnector;

    public UsersViewBuilder(JDBCDataSourceConnector jdbcConnector) {
        this.jdbcConnector = jdbcConnector;
    }
}
