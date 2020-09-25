/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package search.engine;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 *
 * @author VAIO
 */
public class SearchEngine {

    /**
     * @param args the command line arguments
     */
    
    private static final String DB_URL = "jdbc:sqlserver://localhost:1433;databaseName=SearchEngineIndex";
    private static final String USER = "sa";
    private static final String PASS = "p@ssword13";
    private static Connection conn = null;
    
    public static void main(String[] args) throws InterruptedException {
        
        if(!connectToIndex())
        {
            System.out.println("Connection failed");
            return;
        }
        else
            System.out.println("Connection successful");
        
        Crawler C=new Crawler(conn);
        Indexer I=new Indexer(conn,C);
        
        Thread tCrawler=new Thread(C);
        Thread tIndexer=new Thread(I);
        
        tCrawler.start();
        tIndexer.start();
        
        tCrawler.join();
        tIndexer.join();
        
        closeConnection();
    }

    private static boolean connectToIndex()
    {
        try{
            
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");

            System.out.println("Connecting to database...");
            conn = DriverManager.getConnection(DB_URL,USER,PASS);
            return true;
            
         }catch(SQLException se){
             System.out.println("Connection to index failed: Error in JDBC");
             return false;
         }catch(ClassNotFoundException e){
             System.out.println("Connection to index failed: Error in Class.forName");
             return false;
         }
    }
    
    private static void closeConnection()
    {
        try{
         if(conn!=null)
            conn.close();
        }catch(SQLException se){
            System.out.println("Closing the connection failed");
        }
    }
}
