package ch.innovate.testjdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;

public class MySQLAccess {
  private Connection connect = null;
  private Statement statement = null;
  private PreparedStatement preparedStatement = null;
  private ResultSet resultSet = null;

  public void readDataBase() throws Exception {
    try {
      // This will load the MySQL driver, each DB has its own driver
      Class.forName("com.mysql.jdbc.Driver");
      // Setup the connection with the DB
      connect = DriverManager
          .getConnection("jdbc:mysql://localhost/sakila?"
              + "user=felix&password=Chasper99");

      // Statements allow to issue SQL queries to the database
      //FHO statement = connect.createStatement();
      // Result set get the result of the SQL query
      //FHO resultSet = statement
      //FHO     .executeQuery("select * from FEEDBACK.COMMENTS");
      //FHO writeResultSet(resultSet);

      // PreparedStatements can use variables and are more efficient
      //FHO preparedStatement = connect
      //FHO     .prepareStatement("insert into  FEEDBACK.COMMENTS values (default, ?, ?, ?, ? , ?, ?)");
      // "myuser, webpage, datum, summary, COMMENTS from FEEDBACK.COMMENTS");
      // Parameters start with 1
      //FHO preparedStatement.setString(1, "Test");
      //FHO preparedStatement.setString(2, "TestEmail");
      //FHO preparedStatement.setString(3, "TestWebpage");
      //FHO preparedStatement.setDate(4, new java.sql.Date(2009, 12, 11));
      //FHO preparedStatement.setString(5, "TestSummary");
      //FHO preparedStatement.setString(6, "TestComment");
      //FHO preparedStatement.executeUpdate();

      preparedStatement = connect
          .prepareStatement("SELECT * from actor");
      resultSet = preparedStatement.executeQuery();
      writeResultSet(resultSet);

      // Remove again the insert comment
      //FHO preparedStatement = connect
      //FHO .prepareStatement("delete from FEEDBACK.COMMENTS where myuser= ? ; ");
      //FHO   preparedStatement.setString(1, "Test");
      //FHO   preparedStatement.executeUpdate();
      
      //FHO resultSet = statement
      //FHO .executeQuery("select * from FEEDBACK.COMMENTS");
      //FHO writeMetaData(resultSet);
      
    } catch (Exception e) {
      throw e;
    } finally {
      close();
    }

  }

  private void writeMetaData(ResultSet resultSet) throws SQLException {
    //   Now get some metadata from the database
    // Result set get the result of the SQL query
    
    System.out.println("The columns in the table are: ");
    
    System.out.println("Table: " + resultSet.getMetaData().getTableName(1));
    for  (int i = 1; i<= resultSet.getMetaData().getColumnCount(); i++){
      System.out.println("Column " +i  + " "+ resultSet.getMetaData().getColumnName(i));
    }
  }

  private void writeResultSet(ResultSet resultSet) throws SQLException {
    // ResultSet is initially before the first data set
    while (resultSet.next()) {
      // It is possible to get the columns via name
      // also possible to get the columns via the column number
      // which starts at 1
      // e.g. resultSet.getSTring(2);
      String first = resultSet.getString("first_name");
      String name = resultSet.getString("last_name");
      Date last_update = resultSet.getDate("last_update");
      System.out.println("Name: " + first + " " + name);
      System.out.println("Date/Time: " + last_update);
    }
  }

  // You need to close the resultSet
  private void close() {
    try {
      if (resultSet != null) {
        resultSet.close();
      }

      if (statement != null) {
        statement.close();
      }

      if (connect != null) {
        connect.close();
      }
    } catch (Exception e) {

    }
  }

} 
