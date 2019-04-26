package md.example;

import java.io.*;
import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.apache.commons.lang3.StringUtils;

public class Main {
    private static final String FILE_NAME = "Interview";
    private static final String SEPARATOR = System.getProperty("line.separator");
    private static final String PATH = Main.class.getClassLoader().getResource("Interview.csv").getFile();
    private static final String OUTPUT_PATH = Main.class.getClassLoader().getResource("Interview.csv").getPath().replaceAll("Interview.csv", "");

    public static void main(String[] args) throws SQLException, ClassNotFoundException {

       Logger logger = Logger.getLogger(Main.class.getName());

        List<String[]> parsedData = new ArrayList<>();
        List<String[]> goodData = new ArrayList<>();
        List<String[]> badData = new ArrayList<>();

        CsvParser parser = new CsvParser(new CsvParserSettings());

        try {
            parsedData = parser.parseAll(new FileReader(PATH));
        } catch (FileNotFoundException e) {
            logger.info(String.valueOf(e));
        }

        parsedData.stream().findFirst().ifPresent(badData::add);

        parsedData.forEach(strings -> {
            if (Arrays.asList(strings).contains(null)) badData.add(strings);
            else goodData.add(strings);
        });

        insertSql(goodData);

        DateFormat df = new SimpleDateFormat("yyyy-MM-dd_HH.mm.ss");
        try (PrintWriter pw = new PrintWriter(OUTPUT_PATH + "bad-data-<" + df.format(new Date()) + ">.csv")) {
            badData.forEach(s -> pw.println(String.join(",", s).replace("null", "")));
        } catch (FileNotFoundException e) {
            logger.info(String.valueOf(e));
        }

        try (FileWriter fw = new FileWriter(OUTPUT_PATH + "ResultLog.log")) {
            fw.write("#records received: " + parsedData.size() + SEPARATOR);
            fw.write("#records successful: " + goodData.size() + SEPARATOR);
            fw.write("#records failed: " + badData.size() + SEPARATOR);
        } catch (IOException e) {
            logger.info(String.valueOf(e));
        }
    }

    private static void insertSql(List<String[]> goodData) throws ClassNotFoundException, SQLException {
        String param = StringUtils.join(goodData.get(0), ",");
        String str = IntStream.range(0, goodData.get(0).length).boxed().map(i -> "?").collect(Collectors.joining(","));
        Class.forName("org.sqlite.JDBC");
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + OUTPUT_PATH + "test.db")) {
            try (Statement stat = conn.createStatement()) {
                stat.executeUpdate("drop table if exists " + FILE_NAME + ";");
                stat.executeUpdate("create table " + FILE_NAME + "(" + param + ")" + ";");
                try (PreparedStatement prep = conn.prepareStatement("insert into " + FILE_NAME + " values (" + str + ");")) {

                    Logger logger = Logger.getLogger(Main.class.getName());

                    AtomicInteger i = new AtomicInteger();

                    goodData.stream().skip(0).forEach(line -> {
                        for (int j = 0; j < goodData.get(0).length; j++) {
                            try {
                                prep.setString(j + 1, line[j]);
                            } catch (SQLException e) {
                                logger.info(String.valueOf(e));
                            }
                        }
                        try {
                            prep.addBatch();
                        } catch (SQLException e) {
                            logger.info(String.valueOf(e));
                        }

                        i.getAndIncrement();

                        if (i.get() % 1000 == 0 || i.get() == goodData.size() - 1) {
                            try {
                                prep.executeBatch(); // Execute every 1000 items.
                            } catch (SQLException e) {
                                logger.info(String.valueOf(e));
                            }
                        }
                    });
                }
            }
        }
    }
}
