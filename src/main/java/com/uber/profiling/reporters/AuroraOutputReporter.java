package com.uber.profiling.reporters;

import com.uber.profiling.Reporter;
import com.uber.profiling.util.JsonUtils;

import java.sql.*;
import java.util.Map;

public class AuroraOutputReporter implements Reporter {

    public AuroraOutputReporter() {
    }

    private final String FUNNY_PASSWORD = "password";

    public void report(String profilerName, Map<String, Object> metrics) {
        if (metrics.containsKey("stacktrace")) {
            System.out.println("AuroraOutputReporter(): Storing stacktrace.");
            String startEpoch = metrics.get("startEpoch").toString();
            String endEpoch = metrics.get("endEpoch").toString();
            String stackTraceRaw = JsonUtils.serialize(metrics);
            storeStacktrace(startEpoch, endEpoch, stackTraceRaw.trim());
        }

    }

    public void close() {
        System.out.println("close()");
    }

    public void storeStacktrace(String startEpoch, String endEpoch, String stackTraceRaw) {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException ex) {
            System.out.println("AuroraOutputReporter(): jdbc driver not found");
        }
        try {

            Connection con = DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/timeseries_profiler_data", "root", FUNNY_PASSWORD);

            String insert = String.format("INSERT INTO stacktrace (timestamps, stacktrace_raw) VALUES (?, ?)");
            PreparedStatement ps = con.prepareStatement(insert);
            ps.setString(1, startEpoch + "." + endEpoch);
            ps.setString(2, stackTraceRaw);

            System.out.println("stackTraceRaw:\n" + stackTraceRaw);

            int res = ps.executeUpdate();
            con.close();
        } catch (SQLException ex) {
            System.out.println("AuroraOutputReporter(): SQLException thrown.");
            System.out.println(ex.getMessage());
        }
    }
}