package com.uber.profiling.reporters;

import com.uber.profiling.Reporter;

import java.sql.*;
import java.util.Map;

public class AuroraOutputReporter implements Reporter {

    public AuroraOutputReporter() {
    }

    private final String FUNNY_PASSWORD = "password";


    public void report(String profilerName, Map<String, Object> metrics) {
        System.out.println("report()");
        doStuff();
    }

    public void close() {
        System.out.println("close()");
    }

    public void doStuff() {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException ex) {
            System.out.println("jdbc driver not found");
        }
        try {
            Connection con = DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/", "root", FUNNY_PASSWORD);

            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery("select * from emp");
            while (rs.next())
                System.out.println(rs.getInt(1) + "  " + rs.getString(2) + "  " + rs.getString(3));
            con.close();
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }
}